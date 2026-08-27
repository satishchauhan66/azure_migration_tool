# Author: Satish Chauhan
"""CSV / flat-file → SQL Server import via BCP (normalize to pipe, then bcp in)."""

from __future__ import annotations

import csv
import logging
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from ..utils.bcp_tools import find_bcp_exe, format_bcp_data_path
from .bcp_migration import (
    BcpJobReport,
    TableResult,
    _auth_bcp_args,
    _log,
    _quote,
    _split_fqn,
    connect,
    count_rows,
    resolve_staging_dir,
)

LogFn = Callable[[str], None]
_CREATE_NO_WINDOW = 0x08000000 if sys.platform.startswith("win") else 0


@dataclass
class CsvColumnInfo:
    name: str
    index: int


def peek_csv_headers(
    path: Path,
    *,
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
) -> List[str]:
    """Read header names (or Col1..N from first data row)."""
    with path.open("r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        try:
            row = next(reader)
        except StopIteration:
            return []
    row = [(c or "").strip() for c in row]
    if has_header:
        return [c or f"Column{i+1}" for i, c in enumerate(row)]
    return [f"Column{i+1}" for i in range(len(row))]


def list_dest_columns(
    dest_role: Dict[str, Any],
    table: str,
    *,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """Return dest columns: name, type_name, max_length, is_nullable, is_identity, column_id."""
    schema, name = _split_fqn(table)
    with connect(dest_role, logger) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.name, ty.name, c.max_length, c.is_nullable, c.is_identity, c.column_id
            FROM sys.columns c
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            JOIN sys.tables t ON t.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?
            ORDER BY c.column_id
            """,
            schema,
            name,
        )
        return [
            {
                "name": r[0],
                "type_name": r[1],
                "max_length": int(r[2] or 0),
                "is_nullable": bool(r[3]),
                "is_identity": bool(r[4]),
                "column_id": int(r[5]),
            }
            for r in cur.fetchall()
        ]


def auto_map_columns(csv_headers: Sequence[str], dest_cols: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    """Map dest_col_name -> csv_header by case-insensitive name match."""
    by_lower = {h.lower(): h for h in csv_headers if h}
    mapping: Dict[str, str] = {}
    for col in dest_cols:
        if col.get("is_identity"):
            continue
        hit = by_lower.get((col["name"] or "").lower())
        if hit:
            mapping[col["name"]] = hit
    return mapping


def ensure_table_from_csv_headers(
    dest_role: Dict[str, Any],
    table: str,
    csv_headers: Sequence[str],
    *,
    log: Optional[LogFn] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Create a simple NVARCHAR(MAX) table from CSV headers if missing."""
    schema, name = _split_fqn(table)
    with connect(dest_role, logger) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?
            """,
            schema,
            name,
        )
        if cur.fetchone():
            return
        cur.execute("SELECT 1 FROM sys.schemas WHERE name = ?", schema)
        if not cur.fetchone() and schema.lower() != "dbo":
            cur.execute(f"CREATE SCHEMA {_quote(schema)}")
        cols_sql = []
        for h in csv_headers:
            safe = re.sub(r"[^\w]+", "_", (h or "col").strip()) or "col"
            if safe[0].isdigit():
                safe = "c_" + safe
            cols_sql.append(f"{_quote(safe)} NVARCHAR(MAX) NULL")
        ddl = f"CREATE TABLE {_quote(schema)}.{_quote(name)} (\n  " + ",\n  ".join(cols_sql) + "\n)"
        _log(log, f"Creating dest table from CSV headers: {schema}.{name}")
        cur.execute(ddl)


def _normalize_row(
    parts: List[str],
    *,
    expected_cols: int,
    wide_text_index: Optional[int],
) -> Optional[List[str]]:
    """Align a split row to expected_cols; optionally collapse extra fields into one wide text column."""
    if not parts and expected_cols == 0:
        return []
    if len(parts) == expected_cols:
        return parts
    if wide_text_index is None or expected_cols < 2:
        return None
    # wide text column absorbed delimiters
    w = wide_text_index
    if w < 0 or w >= expected_cols:
        return None
    trailing = expected_cols - w - 1
    if trailing < 0 or len(parts) < expected_cols:
        return None
    head = parts[:w]
    if trailing == 0:
        mid = ",".join(parts[w:])
        return head + [mid]
    mid = ",".join(parts[w : len(parts) - trailing])
    tail = parts[len(parts) - trailing :]
    return head + [mid] + tail


def convert_csv_files_to_pipe(
    files: Sequence[Path],
    out_file: Path,
    *,
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
    csv_headers: Sequence[str],
    dest_column_order: Sequence[str],
    mapping: Dict[str, str],
    wide_text_csv_column: Optional[str] = None,
    log: Optional[LogFn] = None,
    cancel_event: Optional[threading.Event] = None,
) -> int:
    """
    Convert one or more CSVs into a single pipe-delimited file for bcp -c -t|.

    mapping: dest_column_name -> csv_header_name
    dest_column_order: ordered dest columns to write (usually non-identity mapped cols)
    """
    header_index = {h: i for i, h in enumerate(csv_headers)}
    wide_idx: Optional[int] = None
    if wide_text_csv_column:
        wide_idx = header_index.get(wide_text_csv_column)
        if wide_idx is None:
            # try case-insensitive
            for h, i in header_index.items():
                if h.lower() == wide_text_csv_column.lower():
                    wide_idx = i
                    break

    expected = len(csv_headers)
    rows_out = 0
    out_file.parent.mkdir(parents=True, exist_ok=True)

    with out_file.open("w", encoding="utf-8", newline="\n") as fout:
        for path in files:
            if cancel_event and cancel_event.is_set():
                break
            _log(log, f"Reading {path.name}...")
            with path.open("r", encoding=encoding, errors="replace", newline="") as fin:
                # Manual split keeps broken unquoted commas recoverable via wide_text_index
                first = True
                for line in fin:
                    if cancel_event and cancel_event.is_set():
                        break
                    if first:
                        first = False
                        if has_header:
                            continue
                    raw = line.rstrip("\r\n")
                    if not raw:
                        continue
                    parts = raw.split(delimiter)
                    norm = _normalize_row(parts, expected_cols=expected, wide_text_index=wide_idx)
                    if norm is None:
                        # try Python csv for this line only when strict
                        try:
                            parsed = next(csv.reader([raw], delimiter=delimiter))
                            if len(parsed) == expected:
                                norm = parsed
                        except Exception:
                            norm = None
                    if norm is None:
                        continue

                    out_vals: List[str] = []
                    for dest_col in dest_column_order:
                        csv_name = mapping.get(dest_col)
                        if not csv_name:
                            out_vals.append("")
                            continue
                        idx = header_index.get(csv_name)
                        if idx is None:
                            # case-insensitive
                            idx = next(
                                (i for h, i in header_index.items() if h.lower() == csv_name.lower()),
                                None,
                            )
                        val = norm[idx] if idx is not None and idx < len(norm) else ""
                        val = (val or "").replace("\r", " ").replace("\n", " ").replace("|", "/")
                        out_vals.append(val)
                    fout.write("|".join(out_vals) + "\n")
                    rows_out += 1
                    if rows_out % 500000 == 0:
                        _log(log, f"  ... prepared {rows_out:,} rows")
    return rows_out


def write_bcp_format_file(
    path: Path,
    *,
    dest_cols: Sequence[Dict[str, Any]],
    write_order: Sequence[str],
) -> Path:
    """Non-XML BCP format for pipe-delimited character data mapped to selected columns."""
    by_name = {c["name"]: c for c in dest_cols}
    lines = ["14.0", str(len(write_order))]
    for i, col_name in enumerate(write_order, start=1):
        col = by_name[col_name]
        server_order = int(col["column_id"])
        term = "\\n" if i == len(write_order) else "|"
        # host_file_order data_type prefix_len data_len terminator server_col_order server_col_name collation
        lines.append(f'{i} SQLCHAR 0 0 "{term}" {server_order} {col_name} ""')
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def bcp_in_pipe_file(
    *,
    bcp_exe: str,
    dest_role: Dict[str, Any],
    table: str,
    data_file: Path,
    format_file: Optional[Path] = None,
    batch_size: int = 10000,
    log: Optional[LogFn] = None,
) -> None:
    schema, name = _split_fqn(table)
    err = data_file.with_suffix(data_file.suffix + ".err")
    args = [
        bcp_exe,
        f"{dest_role['db']}.{schema}.{name}",
        "in",
        format_bcp_data_path(str(data_file)),
        "-S",
        dest_role["server"],
        *_auth_bcp_args(dest_role),
        "-b",
        str(max(1, batch_size)),
        "-e",
        format_bcp_data_path(str(err)),
        "-F",
        "1",
    ]
    if format_file is not None:
        # Format file defines field/row terminators; do not also pass -c/-t/-r.
        args.extend(["-f", format_bcp_data_path(str(format_file))])
    else:
        args.extend(["-c", "-t", "|", "-r", "\n"])
    _log(log, "BCP IN " + table)
    proc = subprocess.run(args, capture_output=True, text=True, creationflags=_CREATE_NO_WINDOW)
    out = ((proc.stdout or "") + (proc.stderr or "")).strip()
    for line in out.splitlines():
        if line.strip():
            _log(log, "  " + line)
    if proc.returncode != 0:
        raise RuntimeError(f"bcp failed (exit {proc.returncode}): {out[-1500:]}")


def truncate_table(dest_role: Dict[str, Any], table: str, *, logger: Optional[logging.Logger] = None) -> None:
    schema, name = _split_fqn(table)
    with connect(dest_role, logger) as conn:
        cur = conn.cursor()
        try:
            cur.execute(f"TRUNCATE TABLE {_quote(schema)}.{_quote(name)}")
        except Exception:
            cur.execute(f"DELETE FROM {_quote(schema)}.{_quote(name)}")


def run_csv_bcp_import(
    cfg: Dict[str, Any],
    log: Optional[LogFn] = None,
    *,
    cancel_event: Optional[threading.Event] = None,
) -> BcpJobReport:
    """
    cfg keys:
      dest_server, dest_db, dest_auth, dest_user, dest_password
      table: schema.table
      csv_files: list[str]
      delimiter, encoding, has_header
      mapping: dict dest_col -> csv_header
      wide_text_csv_column: optional csv header that may contain delimiter unquoted
      create_missing, truncate_dest, batch_size, work_dir, verify_after_copy, dry_run
    """
    started = datetime.now(timezone.utc).isoformat()
    lg = logging.getLogger("csv_bcp_import")
    dest_role = {
        "server": cfg["dest_server"],
        "db": cfg["dest_db"],
        "auth": cfg.get("dest_auth") or "windows",
        "user": cfg.get("dest_user") or "",
        "password": cfg.get("dest_password"),
    }
    table = (cfg.get("table") or "").strip()
    if not table:
        raise ValueError("Destination table is required.")
    files = [Path(p) for p in (cfg.get("csv_files") or [])]
    if not files:
        raise ValueError("No CSV files selected.")
    for p in files:
        if not p.is_file():
            raise FileNotFoundError(f"CSV not found: {p}")

    delimiter = cfg.get("delimiter") or ","
    encoding = cfg.get("encoding") or "utf-8"
    has_header = bool(cfg.get("has_header", True))
    mapping: Dict[str, str] = dict(cfg.get("mapping") or {})

    work_root = resolve_staging_dir((cfg.get("work_dir") or "").strip() or None, log)
    run_dir = work_root / datetime.now().strftime("csv_run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    headers = peek_csv_headers(files[0], delimiter=delimiter, encoding=encoding, has_header=has_header)
    if not headers:
        raise ValueError("Could not read CSV headers / columns.")

    dest_cols = list_dest_columns(dest_role, table, logger=lg)
    if not dest_cols and bool(cfg.get("create_missing", True)):
        ensure_table_from_csv_headers(dest_role, table, headers, log=log, logger=lg)
        dest_cols = list_dest_columns(dest_role, table, logger=lg)
        mapping = auto_map_columns(headers, dest_cols)
        if not mapping:
            mapping = {
                dest_cols[i]["name"]: headers[i] for i in range(min(len(dest_cols), len(headers)))
            }
    elif not dest_cols:
        raise ValueError(f"Destination table not found: {table}")

    if not mapping:
        mapping = auto_map_columns(headers, dest_cols)
    if not mapping:
        raise ValueError("Column mapping is empty - map at least one CSV column to a dest column.")

    # Write only mapped non-identity columns; format file maps to correct server ordinals.
    write_order = [
        c["name"]
        for c in dest_cols
        if c["name"] in mapping and mapping[c["name"]] and not c.get("is_identity")
    ]
    if not write_order:
        raise ValueError("No destination columns selected for import.")

    if bool(cfg.get("dry_run", False)):
        _log(log, f"Dry-run: would import {len(files)} file(s) -> {table}")
        _log(log, f"Mapped columns: {len(write_order)}")
        finished = datetime.now(timezone.utc).isoformat()
        return BcpJobReport(
            ok=True,
            started_at=started,
            finished_at=finished,
            tables=[
                TableResult(
                    table=table,
                    status="ok",
                    message=f"dry-run: {len(files)} file(s), {len(headers)} csv cols -> {len(write_order)} dest cols",
                )
            ],
            work_dir=str(run_dir),
        )

    bcp_exe = find_bcp_exe()
    if not bcp_exe:
        raise RuntimeError("bcp.exe not found.")

    if bool(cfg.get("truncate_dest", False)):
        _log(log, f"Truncating {table}...")
        truncate_table(dest_role, table, logger=lg)

    pipe_file = run_dir / "import.bcp.txt"
    fmt_file = run_dir / "import.fmt"
    write_bcp_format_file(fmt_file, dest_cols=dest_cols, write_order=write_order)

    t0 = time.monotonic()
    rows = convert_csv_files_to_pipe(
        files,
        pipe_file,
        delimiter=delimiter,
        encoding=encoding,
        has_header=has_header,
        csv_headers=headers,
        dest_column_order=write_order,
        mapping=mapping,
        wide_text_csv_column=cfg.get("wide_text_csv_column") or None,
        log=log,
        cancel_event=cancel_event,
    )
    _log(log, f"[OK] Prepared {rows:,} rows -> {pipe_file}")

    if rows == 0:
        raise RuntimeError(
            "No data rows prepared from CSV (check delimiter / header / wide-text column)."
        )

    bcp_in_pipe_file(
        bcp_exe=bcp_exe,
        dest_role=dest_role,
        table=table,
        data_file=pipe_file,
        format_file=fmt_file,
        batch_size=int(cfg.get("batch_size") or 10000),
        log=log,
    )

    dest_n = count_rows(dest_role, table, logger=lg) if bool(cfg.get("verify_after_copy", True)) else rows
    ok = dest_n == rows if bool(cfg.get("verify_after_copy", True)) else True
    msg = f"imported {rows:,} rows" if ok else f"row mismatch prepared={rows:,} dest={dest_n:,}"
    finished = datetime.now(timezone.utc).isoformat()
    report = BcpJobReport(
        ok=ok,
        started_at=started,
        finished_at=finished,
        tables=[
            TableResult(
                table=table,
                status="ok" if ok else "fail",
                src_rows=rows,
                dest_rows=dest_n,
                duration_sec=time.monotonic() - t0,
                message=msg,
                error=None if ok else msg,
            )
        ],
        work_dir=str(run_dir),
    )
    _log(log, f"=== CSV BCP import {'SUCCEEDED' if ok else 'FAILED'} ===")
    return report
