# Author: Satish Chauhan
"""BCP table migration engine (list / create / out / in / verify / parallel / resume)."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from ..utils.bcp_tools import find_bcp_exe, format_bcp_data_path, get_app_base_dir
from ..utils.database import build_conn_str, connect_to_database, pick_sql_driver

LogFn = Callable[[str], None]
_CREATE_NO_WINDOW = 0x08000000 if sys.platform.startswith("win") else 0


@dataclass
class TableInfo:
    schema: str
    name: str
    src_rows: int = 0
    dest_exists: bool = False
    dest_rows: int = 0

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass
class TableResult:
    table: str
    status: str  # ok | fail | skipped
    src_rows: int = 0
    dest_rows: int = 0
    duration_sec: float = 0.0
    error: Optional[str] = None
    message: str = ""


@dataclass
class BcpJobReport:
    ok: bool
    started_at: str
    finished_at: str
    tables: List[TableResult] = field(default_factory=list)
    work_dir: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "work_dir": self.work_dir,
            "tables": [asdict(t) for t in self.tables],
        }


def _log(log: Optional[LogFn], msg: str) -> None:
    if log:
        log(msg)


def _quote(name: str) -> str:
    return "[" + (name or "").replace("]", "]]") + "]"


def _split_fqn(table: str) -> Tuple[str, str]:
    t = (table or "").strip()
    if "." in t:
        s, n = t.split(".", 1)
        return s.strip("[]"), n.strip("[]")
    return "dbo", t.strip("[]")


def connect(cfg_role: Dict[str, Any], logger: Optional[logging.Logger] = None):
    """Connect using role keys: server, db, auth, user, password."""
    log = logger or logging.getLogger("bcp_migration")
    server = (cfg_role.get("server") or "").strip()
    db = (cfg_role.get("db") or "").strip()
    auth = (cfg_role.get("auth") or "windows").strip().lower()
    user = cfg_role.get("user") or ""
    password = cfg_role.get("password")
    driver = pick_sql_driver(log)
    if auth == "entra_mfa":
        return connect_to_database(
            server, db, user, driver, auth, password, timeout=60, logger=log
        )
    cs = build_conn_str(server, db, user, driver, auth, password)
    import pyodbc

    return pyodbc.connect(cs, timeout=60, autocommit=True)


def default_work_dir() -> Path:
    env = (os.environ.get("AMT_BCP_WORK_DIR") or "").strip()
    if env:
        p = Path(env)
    else:
        p = get_app_base_dir() / "bcp_work"
    try:
        from ..utils.bcp_tools import verify_bcp_work_dir

        ok, msg = verify_bcp_work_dir(p)
        if ok:
            return Path(msg)
    except Exception:
        pass
    p.mkdir(parents=True, exist_ok=True)
    return p


def resolve_staging_dir(preferred: Optional[str], log: Optional[LogFn] = None) -> Path:
    """Resolve a writable BCP staging folder (local or UNC / Azure Files SMB)."""
    from ..utils.bcp_tools import resolve_bcp_work_root

    root = resolve_bcp_work_root(preferred)
    _log(log, f"Staging folder: {root}")
    return Path(root)


def list_tables(
    cfg_role: Dict[str, Any],
    *,
    logger: Optional[logging.Logger] = None,
) -> List[TableInfo]:
    """List user tables with approximate row counts from a SQL Server database."""
    with connect(cfg_role, logger) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT s.name, t.name,
                   ISNULL((
                       SELECT SUM(p.rows)
                       FROM sys.partitions p
                       WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)
                   ), 0)
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.is_ms_shipped = 0
            ORDER BY s.name, t.name
            """
        )
        return [
            TableInfo(schema=r[0], name=r[1], src_rows=int(r[2] or 0)) for r in cur.fetchall()
        ]


def enrich_with_dest(
    tables: Sequence[TableInfo],
    dest_role: Dict[str, Any],
    *,
    logger: Optional[logging.Logger] = None,
) -> List[TableInfo]:
    """Fill dest_exists / dest_rows for each table."""
    try:
        dest_map: Dict[str, int] = {}
        with connect(dest_role, logger) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT s.name, t.name,
                       ISNULL((
                           SELECT SUM(p.rows)
                           FROM sys.partitions p
                           WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)
                       ), 0)
                FROM sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE t.is_ms_shipped = 0
                """
            )
            for s, n, rows in cur.fetchall():
                dest_map[f"{s}.{n}".lower()] = int(rows or 0)
        out: List[TableInfo] = []
        for t in tables:
            key = t.fqn.lower()
            if key in dest_map:
                out.append(
                    TableInfo(
                        schema=t.schema,
                        name=t.name,
                        src_rows=t.src_rows,
                        dest_exists=True,
                        dest_rows=dest_map[key],
                    )
                )
            else:
                out.append(
                    TableInfo(
                        schema=t.schema,
                        name=t.name,
                        src_rows=t.src_rows,
                        dest_exists=False,
                        dest_rows=0,
                    )
                )
        return out
    except Exception:
        return list(tables)


def _sql_type(row) -> str:
    col_name, type_name, max_length, precision, scale, is_nullable, is_identity, coll_name = row
    t = (type_name or "").lower()
    if t in ("varchar", "char", "varbinary", "binary"):
        size = "max" if max_length == -1 else str(max_length)
        base = f"{t}({size})"
    elif t in ("nvarchar", "nchar"):
        size = "max" if max_length == -1 else str(max_length // 2)
        base = f"{t}({size})"
    elif t in ("decimal", "numeric"):
        base = f"{t}({precision},{scale})"
    elif t in ("datetime2", "time", "datetimeoffset"):
        base = f"{t}({scale})"
    elif t == "float":
        base = f"float({precision})" if precision else "float"
    else:
        base = t
    if t in ("varchar", "char", "nvarchar", "nchar", "text", "ntext") and coll_name:
        base += f" COLLATE {coll_name}"
    nulls = "NULL" if is_nullable else "NOT NULL"
    ident = " IDENTITY(1,1)" if is_identity else ""
    return f"{_quote(col_name)} {base}{ident} {nulls}"


def ensure_dest_table(
    src_role: Dict[str, Any],
    dest_role: Dict[str, Any],
    table: str,
    *,
    truncate_if_exists: bool = False,
    log: Optional[LogFn] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    schema, name = _split_fqn(table)
    with connect(dest_role, logger) as dest:
        cur = dest.cursor()
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
            if truncate_if_exists:
                _log(log, f"Truncating dest {schema}.{name}")
                try:
                    cur.execute(f"TRUNCATE TABLE {_quote(schema)}.{_quote(name)}")
                except Exception:
                    cur.execute(f"DELETE FROM {_quote(schema)}.{_quote(name)}")
            return
        # ensure schema
        cur.execute("SELECT 1 FROM sys.schemas WHERE name = ?", schema)
        if not cur.fetchone() and schema.lower() != "dbo":
            cur.execute(f"CREATE SCHEMA {_quote(schema)}")

    with connect(src_role, logger) as src:
        cur = src.cursor()
        cur.execute(
            """
            SELECT c.name, ty.name, c.max_length, c.precision, c.scale,
                   c.is_nullable, c.is_identity, c.collation_name
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
        cols = cur.fetchall()
        if not cols:
            raise RuntimeError(f"No columns found for source {table}")
        col_sql = ",\n  ".join(_sql_type(r) for r in cols)
        cur.execute(
            """
            SELECT c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic
              ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c
              ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            JOIN sys.tables t ON t.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ? AND i.is_primary_key = 1
            ORDER BY ic.key_ordinal
            """,
            schema,
            name,
        )
        pk_cols = [r[0] for r in cur.fetchall()]
        pk_sql = ""
        if pk_cols:
            pk_sql = (
                f",\n  CONSTRAINT {_quote('PK_' + name + '_bcp')} PRIMARY KEY ("
                + ", ".join(_quote(c) for c in pk_cols)
                + ")"
            )
        ddl = f"CREATE TABLE {_quote(schema)}.{_quote(name)} (\n  {col_sql}{pk_sql}\n)"

    with connect(dest_role, logger) as dest:
        _log(log, f"Creating dest table {schema}.{name}")
        dest.cursor().execute(ddl)


def count_rows(role: Dict[str, Any], table: str, *, logger: Optional[logging.Logger] = None) -> int:
    schema, name = _split_fqn(table)
    with connect(role, logger) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT_BIG(*) FROM {_quote(schema)}.{_quote(name)}")
        return int(cur.fetchone()[0])


def _auth_bcp_args(role: Dict[str, Any]) -> List[str]:
    auth = (role.get("auth") or "windows").strip().lower()
    if auth == "windows":
        return ["-T"]
    if auth == "sql":
        return ["-U", role.get("user") or "", "-P", role.get("password") or ""]
    # Entra MFA: Windows bcp cannot use tokens; caller should use SQL login helper.
    raise RuntimeError(
        f"Auth '{auth}' is not supported directly by bcp.exe on this path. "
        "Use Windows or SQL authentication, or create a temporary SQL login."
    )


def run_bcp_command(args: List[str], log: Optional[LogFn] = None) -> None:
    _log(log, "BCP: " + " ".join(a if i == 0 or args[i - 1] not in ("-P",) else "***" for i, a in enumerate(args)))
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        creationflags=_CREATE_NO_WINDOW,
    )
    out = ((proc.stdout or "") + (proc.stderr or "")).strip()
    for line in out.splitlines():
        if line.strip():
            _log(log, "  " + line)
    if proc.returncode != 0:
        raise RuntimeError(f"bcp failed (exit {proc.returncode}): {out[-1500:]}")


def bcp_out_in_table(
    *,
    bcp_exe: str,
    src_role: Dict[str, Any],
    dest_role: Dict[str, Any],
    table: str,
    work_dir: Path,
    batch_size: int = 5000,
    native: bool = True,
    keep_identity: bool = True,
    log: Optional[LogFn] = None,
) -> Tuple[Path, int]:
    """BCP OUT from source then IN to dest. Returns (data_file, bytes)."""
    schema, name = _split_fqn(table)
    safe = re.sub(r"[^\w.\-]+", "_", f"{schema}.{name}")
    data_file = work_dir / f"{safe}.bcp"
    err_out = work_dir / f"{safe}.out.err"
    err_in = work_dir / f"{safe}.in.err"
    fmt = "-n" if native else "-c"
    data_path = format_bcp_data_path(str(data_file))

    out_args = [
        bcp_exe,
        f"{src_role['db']}.{schema}.{name}",
        "out",
        data_path,
        "-S",
        src_role["server"],
        *_auth_bcp_args(src_role),
        fmt,
        "-b",
        str(max(1, batch_size)),
        "-e",
        format_bcp_data_path(str(err_out)),
    ]
    run_bcp_command(out_args, log)

    in_args = [
        bcp_exe,
        f"{dest_role['db']}.{schema}.{name}",
        "in",
        data_path,
        "-S",
        dest_role["server"],
        *_auth_bcp_args(dest_role),
        fmt,
        "-b",
        str(max(1, batch_size)),
        "-e",
        format_bcp_data_path(str(err_in)),
    ]
    if keep_identity:
        in_args.append("-E")
    run_bcp_command(in_args, log)
    size = data_file.stat().st_size if data_file.is_file() else 0
    return data_file, size


def _resume_path(work_dir: Path) -> Path:
    return work_dir / "bcp_resume.json"


def load_resume(work_dir: Path) -> Dict[str, Any]:
    p = _resume_path(work_dir)
    if not p.is_file():
        return {"completed": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"completed": []}


def save_resume(work_dir: Path, completed: Sequence[str]) -> None:
    _resume_path(work_dir).write_text(
        json.dumps({"completed": list(completed), "updated_at": datetime.now(timezone.utc).isoformat()}, indent=2),
        encoding="utf-8",
    )


def match_exclude(table: str, patterns: Sequence[str]) -> bool:
    for pat in patterns:
        pat = (pat or "").strip()
        if not pat:
            continue
        # simple glob: * and ?
        rx = re.escape(pat).replace(r"\*", ".*").replace(r"\?", ".")
        if re.fullmatch(rx, table, flags=re.IGNORECASE):
            return True
        schema, name = _split_fqn(table)
        if re.fullmatch(rx, name, flags=re.IGNORECASE):
            return True
    return False


def run_bcp_migration(
    cfg: Dict[str, Any],
    log: Optional[LogFn] = None,
    *,
    cancel_event: Optional[threading.Event] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> BcpJobReport:
    """
    Run BCP migration for selected tables.

    cfg keys:
      src_server, src_db, src_auth, src_user, src_password
      dest_server, dest_db, dest_auth, dest_user, dest_password
      tables: list[str] or comma-separated
      exclude: list/comma patterns
      create_missing, truncate_dest, keep_identity, native_format
      batch_size, parallel_tables, max_retries
      skip_if_equal, resume_enabled, keep_bcp_files
      verify_after_copy, dry_run, work_dir
    """
    started = datetime.now(timezone.utc).isoformat()
    lg = logging.getLogger("bcp_migration")
    src_role = {
        "server": cfg["src_server"],
        "db": cfg["src_db"],
        "auth": cfg.get("src_auth") or "windows",
        "user": cfg.get("src_user") or "",
        "password": cfg.get("src_password"),
    }
    dest_role = {
        "server": cfg["dest_server"],
        "db": cfg["dest_db"],
        "auth": cfg.get("dest_auth") or "windows",
        "user": cfg.get("dest_user") or "",
        "password": cfg.get("dest_password"),
    }

    work_root = resolve_staging_dir(
        (cfg.get("work_dir") or "").strip() or None,
        log,
    )
    run_dir = work_root / datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_tables = cfg.get("tables") or []
    if isinstance(raw_tables, str):
        tables = [t.strip() for t in raw_tables.split(",") if t.strip()]
    else:
        tables = [str(t).strip() for t in raw_tables if str(t).strip()]
    if not tables:
        raise ValueError("No tables selected for BCP migration.")

    exclude_raw = cfg.get("exclude") or []
    if isinstance(exclude_raw, str):
        exclude = [x.strip() for x in exclude_raw.split(",") if x.strip()]
    else:
        exclude = [str(x).strip() for x in exclude_raw if str(x).strip()]

    tables = [t if "." in t else f"dbo.{t}" for t in tables]
    tables = [t for t in tables if not match_exclude(t, exclude)]
    if not tables:
        raise ValueError("All selected tables were excluded.")

    bcp_exe = find_bcp_exe()
    if not bcp_exe:
        raise RuntimeError("bcp.exe not found. Install SQL Server Command Line Utilities.")

    create_missing = bool(cfg.get("create_missing", True))
    truncate_dest = bool(cfg.get("truncate_dest", False))
    keep_identity = bool(cfg.get("keep_identity", True))
    native = bool(cfg.get("native_format", True))
    batch_size = int(cfg.get("batch_size") or 5000)
    parallel = max(1, int(cfg.get("parallel_tables") or 1))
    max_retries = max(0, int(cfg.get("max_retries") or 0))
    skip_if_equal = bool(cfg.get("skip_if_equal", False))
    resume_enabled = bool(cfg.get("resume_enabled", False))
    keep_files = bool(cfg.get("keep_bcp_files", False))
    verify = bool(cfg.get("verify_after_copy", True))
    dry_run = bool(cfg.get("dry_run", False))

    completed = set(load_resume(work_root).get("completed") or []) if resume_enabled else set()
    results: List[TableResult] = []
    results_lock = threading.Lock()

    _log(log, f"BCP engine using {bcp_exe}")
    _log(log, f"Work dir: {run_dir}")
    _log(log, f"Tables to migrate: {len(tables)} (parallel={parallel})")

    def one_table(table: str) -> TableResult:
        if cancel_event and cancel_event.is_set():
            return TableResult(table=table, status="skipped", message="cancelled")
        if resume_enabled and table.lower() in {c.lower() for c in completed}:
            return TableResult(table=table, status="skipped", message="already completed (resume)")

        t0 = time.monotonic()
        try:
            src_n = count_rows(src_role, table, logger=lg)
            dest_n = 0
            dest_exists = False
            try:
                dest_n = count_rows(dest_role, table, logger=lg)
                dest_exists = True
            except Exception:
                dest_exists = False

            if skip_if_equal and dest_exists and src_n == dest_n:
                return TableResult(
                    table=table,
                    status="skipped",
                    src_rows=src_n,
                    dest_rows=dest_n,
                    duration_sec=time.monotonic() - t0,
                    message="src rows == dest rows",
                )

            if dry_run:
                action = "create+bcp" if not dest_exists else ("truncate+bcp" if truncate_dest else "bcp")
                return TableResult(
                    table=table,
                    status="ok",
                    src_rows=src_n,
                    dest_rows=dest_n,
                    duration_sec=time.monotonic() - t0,
                    message=f"dry-run: would {action}",
                )

            if create_missing or truncate_dest:
                ensure_dest_table(
                    src_role,
                    dest_role,
                    table,
                    truncate_if_exists=truncate_dest and dest_exists,
                    log=log,
                    logger=lg,
                )
            elif not dest_exists:
                raise RuntimeError(f"Destination table missing: {table} (enable Create missing tables)")

            last_err: Optional[str] = None
            for attempt in range(max_retries + 1):
                try:
                    data_file, size = bcp_out_in_table(
                        bcp_exe=bcp_exe,
                        src_role=src_role,
                        dest_role=dest_role,
                        table=table,
                        work_dir=run_dir,
                        batch_size=batch_size,
                        native=native,
                        keep_identity=keep_identity,
                        log=log,
                    )
                    dest_after = count_rows(dest_role, table, logger=lg) if verify else -1
                    if verify and dest_after != src_n:
                        raise RuntimeError(f"Row mismatch after BCP: src={src_n} dest={dest_after}")
                    if not keep_files and data_file.is_file():
                        try:
                            data_file.unlink()
                        except Exception:
                            pass
                    return TableResult(
                        table=table,
                        status="ok",
                        src_rows=src_n,
                        dest_rows=dest_after if dest_after >= 0 else src_n,
                        duration_sec=time.monotonic() - t0,
                        message=f"bcp ok ({size} bytes)",
                    )
                except Exception as ex:
                    last_err = str(ex)
                    _log(log, f"[WARN] {table} attempt {attempt + 1} failed: {ex}")
                    if attempt < max_retries:
                        time.sleep(1.5 * (attempt + 1))
            return TableResult(
                table=table,
                status="fail",
                src_rows=src_n,
                dest_rows=0,
                duration_sec=time.monotonic() - t0,
                error=last_err,
            )
        except Exception as ex:
            return TableResult(
                table=table,
                status="fail",
                duration_sec=time.monotonic() - t0,
                error=str(ex),
            )

    total = len(tables)
    done = 0
    if parallel <= 1:
        for table in tables:
            if cancel_event and cancel_event.is_set():
                break
            _log(log, f"--- {table} ---")
            res = one_table(table)
            results.append(res)
            done += 1
            if progress_callback:
                progress_callback(done, total, table)
            _log(log, f"[{res.status.upper()}] {table}: {res.message or res.error or ''}")
            if resume_enabled and res.status == "ok":
                completed.add(table)
                save_resume(work_root, sorted(completed))
    else:
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futs = {pool.submit(one_table, t): t for t in tables}
            for fut in as_completed(futs):
                table = futs[fut]
                res = fut.result()
                with results_lock:
                    results.append(res)
                    done += 1
                    d = done
                if progress_callback:
                    progress_callback(d, total, table)
                _log(log, f"[{res.status.upper()}] {table}: {res.message or res.error or ''}")
                if resume_enabled and res.status == "ok":
                    with results_lock:
                        completed.add(table)
                        save_resume(work_root, sorted(completed))

    finished = datetime.now(timezone.utc).isoformat()
    ok = all(r.status in ("ok", "skipped") for r in results) and not any(
        r.status == "fail" for r in results
    )
    report = BcpJobReport(
        ok=ok,
        started_at=started,
        finished_at=finished,
        tables=sorted(results, key=lambda r: r.table.lower()),
        work_dir=str(run_dir),
    )
    (run_dir / "report.json").write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    _log(log, f"=== BCP job {'SUCCEEDED' if ok else 'FAILED'} | {len(results)} table result(s) ===")
    return report
