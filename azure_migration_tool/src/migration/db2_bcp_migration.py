# Author: Satish Chauhan
"""DB2 -> SQL Server bulk path: auto EXPORT (large) or JDBC (small) -> BCP IN.

Designed for large-DB migration hosts (multi-TB disk, high RAM).
No ADF. Routing is automatic — no UI method picker.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dtime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

if sys.platform.startswith("win"):
    import winreg
else:
    winreg = None  # type: ignore[assignment]

from ..utils.bcp_tools import find_bcp_exe
from ..utils.database import connect_to_db2_jdbc, ensure_db2_jdbc_driver
from .bcp_migration import (
    BcpJobReport,
    TableInfo,
    TableResult,
    _log,
    _quote,
    _split_fqn,
    connect as connect_sql,
    count_rows,
    resolve_staging_dir,
)
from .csv_bcp_import import (
    bcp_in_pipe_file,
    list_dest_columns,
    write_bcp_format_file,
)

LogFn = Callable[[str], None]
_CREATE_NO_WINDOW = 0x08000000 if sys.platform.startswith("win") else 0

# JDBC only below this estimated CARD; at/above => native EXPORT (required for large DBs).
DEFAULT_JDBC_MAX_ROWS = 2_000_000
DEFAULT_JDBC_FETCH = 50_000
DEFAULT_PARALLEL = 4

_LOB_TYPES = {"BLOB", "CLOB", "DCLOB", "DBCLOB", "LONG VARCHAR", "XML"}

_TYPE_MAP = {
    "INTEGER": "INT",
    "INT": "INT",
    "SMALLINT": "SMALLINT",
    "BIGINT": "BIGINT",
    "VARCHAR": "NVARCHAR",
    "CHAR": "NCHAR",
    "CHARACTER": "NCHAR",
    "GRAPHIC": "NCHAR",
    "VARGRAPHIC": "NVARCHAR",
    "CLOB": "NVARCHAR(MAX)",
    "DCLOB": "NVARCHAR(MAX)",
    "DBCLOB": "NVARCHAR(MAX)",
    "BLOB": "VARBINARY(MAX)",
    "LONG VARCHAR": "NVARCHAR(MAX)",
    "TIMESTAMP": "DATETIME2",
    "DATE": "DATE",
    "TIME": "TIME",
    "DECIMAL": "DECIMAL",
    "NUMERIC": "NUMERIC",
    "DEC": "DECIMAL",
    "DOUBLE": "FLOAT",
    "FLOAT": "FLOAT",
    "DOUBLE PRECISION": "FLOAT",
    "REAL": "REAL",
    "DECFLOAT": "FLOAT",
    "BINARY": "VARBINARY",
    "VARBINARY": "VARBINARY",
    "XML": "NVARCHAR(MAX)",
    "BOOLEAN": "BIT",
}


def _py_str(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _py_int(val: Any) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def connect_db2(role: Dict[str, Any], logger: Optional[logging.Logger] = None):
    log = logger or logging.getLogger("db2_bcp")
    jar = ensure_db2_jdbc_driver(log)
    if not jar:
        raise RuntimeError(
            "DB2 JDBC driver (db2jcc4.jar) not found. Place it under drivers/ or install Java + driver."
        )
    host = (role.get("server") or "").strip()
    db = (role.get("db") or "").strip()
    user = (role.get("user") or "").strip()
    password = role.get("password") or ""
    port = int(role.get("port") or 50000)
    if not host or not db or not user:
        raise ValueError("DB2 host, database, and user are required.")
    return connect_to_db2_jdbc(host, port, db, user, password, timeout=60, logger=log)


def find_db2_clp() -> Optional[str]:
    """Locate db2.exe (full Data Server Client / SQLLIB — not the thin dsdriver)."""
    env = (os.environ.get("DB2_CLP") or os.environ.get("DB2_HOME") or "").strip()
    candidates: List[Path] = []
    if env:
        p = Path(env)
        candidates.extend([p, p / "bin" / "db2.exe", p / "db2.exe"])
    which = shutil.which("db2") or shutil.which("db2.exe")
    if which:
        candidates.append(Path(which))
    for base in (
        Path(r"C:\Program Files\IBM\SQLLIB\bin"),
        Path(r"C:\Program Files (x86)\IBM\SQLLIB\bin"),
        Path(r"C:\IBM\SQLLIB\bin"),
        Path(r"D:\IBM\SQLLIB\bin"),
        Path(r"E:\IBM\SQLLIB\bin"),
        Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "IBM" / "SQLLIB" / "bin",
    ):
        candidates.append(base / "db2.exe")
    # Registry InstalledCopies (Windows)
    if winreg is not None:
        try:
            key_path = r"SOFTWARE\IBM\DB2\InstalledCopies"
            try:
                with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path) as root:
                    i = 0
                    while True:
                        try:
                            copy_name = winreg.EnumKey(root, i)
                        except OSError:
                            break
                        i += 1
                        try:
                            with winreg.OpenKey(root, copy_name) as ck:
                                install, _ = winreg.QueryValueEx(ck, "DB2 Path")
                                if install:
                                    candidates.append(Path(install) / "bin" / "db2.exe")
                        except OSError:
                            pass
            except OSError:
                pass
        except Exception:
            pass
    for c in candidates:
        try:
            if c.is_file():
                return str(c.resolve())
        except Exception:
            continue
    return None


def find_db2cmd(db2_exe: Optional[str] = None) -> Optional[str]:
    """Locate db2cmd.exe (required to initialize CLP environment on Windows)."""
    which = shutil.which("db2cmd") or shutil.which("db2cmd.exe")
    if which:
        return which
    if db2_exe:
        sibling = Path(db2_exe).resolve().parent / "db2cmd.exe"
        if sibling.is_file():
            return str(sibling)
    for base in (
        Path(r"C:\Program Files\IBM\SQLLIB\bin"),
        Path(r"C:\Program Files (x86)\IBM\SQLLIB\bin"),
        Path(r"C:\IBM\SQLLIB\bin"),
    ):
        p = base / "db2cmd.exe"
        if p.is_file():
            return str(p)
    return None


def _clp_node_alias(host: str, port: int) -> str:
    """DB2 node names are limited to 8 characters."""
    digest = hashlib.md5(f"{host}:{port}".encode("utf-8")).hexdigest()[:7]
    return ("A" + digest)[:8].upper()


def _clp_db_alias(db: str) -> str:
    """Prefer real DB name when ≤8 chars; otherwise a stable short alias."""
    name = re.sub(r"[^A-Za-z0-9]", "", (db or "").strip()) or "DB2DB"
    if len(name) <= 8:
        return name.upper()
    return ("D" + hashlib.md5(name.encode("utf-8")).hexdigest()[:7]).upper()


def _escape_clp_password(password: str) -> str:
    """Quote password for CLP USING clause."""
    # Double any embedded double-quotes
    return '"' + (password or "").replace('"', '""') + '"'


def _run_db2_clp_script(
    db2_exe: str,
    script: str,
    *,
    log: Optional[LogFn] = None,
    work_dir: Optional[Path] = None,
) -> Tuple[int, str]:
    """
    Run a multi-statement CLP script.

    On Windows, db2.exe must be launched via db2cmd (sets instance env).
    Avoid CREATE_NO_WINDOW — it commonly causes DB21018E / silent CLP failure.
    """
    script_dir = work_dir or Path(os.environ.get("TEMP") or ".")
    script_dir.mkdir(parents=True, exist_ok=True)
    script_file = script_dir / f"amt_db2_clp_{os.getpid()}_{int(time.time() * 1000)}.sql"
    script_file.write_text(script, encoding="utf-8", errors="replace")
    try:
        db2cmd = find_db2cmd(db2_exe) if sys.platform.startswith("win") else None
        if db2cmd:
            # db2cmd /c /w /i  — close, wait, inherit console; then run db2 -tvf
            cmd = [db2cmd, "/c", "/w", "/i", db2_exe, "-tvf", str(script_file)]
        else:
            cmd = [db2_exe, "-tvf", str(script_file)]

        env = os.environ.copy()
        # Ensure SQLLIB bin is on PATH for dependent DLLs
        bin_dir = str(Path(db2_exe).resolve().parent)
        env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
        if "DB2INSTANCE" not in env:
            # Common default instance name for client installs
            env["DB2INSTANCE"] = env.get("DB2INSTANCE") or "DB2"

        _log(log, f"  CLP invoke: {' '.join(cmd[:5])}…")
        startupinfo = None
        creationflags = 0
        if sys.platform.startswith("win"):
            # Hide window without CREATE_NO_WINDOW (which breaks CLP)
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0  # SW_HIDE

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            cwd=bin_dir,
            startupinfo=startupinfo,
            creationflags=creationflags,
            timeout=None,
        )
        out = ((proc.stdout or "") + (proc.stderr or "")).strip()
        return int(proc.returncode or 0), out
    finally:
        try:
            script_file.unlink(missing_ok=True)
        except Exception:
            pass


def _build_clp_connect_script(role: Dict[str, Any]) -> Tuple[str, str]:
    """
    Build catalog + CONNECT statements for a remote TCPIP DB2.
    Returns (script_prefix_ending_with_CONNECT, db_alias_used).
    """
    db = (role.get("db") or "").strip()
    user = (role.get("user") or "").strip()
    password = role.get("password") or ""
    host = (role.get("server") or "").strip()
    port = int(role.get("port") or 50000)
    node = _clp_node_alias(host, port)
    alias = _clp_db_alias(db)
    pwd = _escape_clp_password(password)

    # Catalog is idempotent enough if we uncatalog first (ignore failures).
    lines = [
        f"UNCATALOG DATABASE {alias}",
        f"UNCATALOG NODE {node}",
        f"CATALOG TCPIP NODE {node} REMOTE {host} SERVER {port}",
        f"CATALOG DATABASE {db} AS {alias} AT NODE {node} AUTHENTICATION SERVER",
        "TERMINATE",
        f"CONNECT TO {alias} USER {user} USING {pwd}",
    ]
    return "\n".join(lines), alias


def export_db2_table_clp(
    role: Dict[str, Any],
    table: str,
    out_file: Path,
    *,
    write_order: Sequence[str],
    db2_exe: str,
    log: Optional[LogFn] = None,
) -> int:
    """Client-side DB2 CLP EXPORT to pipe-delimited DEL file (writes on this host)."""
    schema, name = _split_fqn(table)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    if out_file.exists():
        out_file.unlink()

    # Prefer a local staging path for EXPORT when UNC is used — CLP is more reliable
    # writing local then we can copy; but UNC often works from the client. Try direct first.
    out_path = str(out_file)
    try:
        out_path = str(out_file.resolve())
    except Exception:
        out_path = str(out_file)
    out_path = out_path.replace("/", "\\")

    select_sql = _export_select_sql(schema, name, write_order)
    # coldel0x7C = '|'; nochardel = no quote wrapping
    export_sql = (
        f'EXPORT TO "{out_path}" OF DEL MODIFIED BY coldel0x7C nochardel {select_sql}'
    )
    connect_prefix, alias = _build_clp_connect_script(role)
    host = (role.get("server") or "").strip()
    port = int(role.get("port") or 50000)
    db = (role.get("db") or "").strip()

    script = connect_prefix + "\n" + export_sql + "\nCONNECT RESET\nTERMINATE\n"
    _log(log, f"DB2 CLP EXPORT {table} -> {out_file.name}")
    _log(log, f"  (remote {host}:{port} db={db} alias={alias})")

    rc, out = _run_db2_clp_script(db2_exe, script, log=log, work_dir=out_file.parent)
    for line in out.splitlines()[-50:]:
        if line.strip():
            _log(log, "  " + line)

    # Soft-fail catalog noise is normal; require export file or clear EXPORT success
    if not out_file.exists():
        hint = ""
        if "SQL1024N" in out or "not cataloged" in out.lower():
            hint = (
                " Catalog/connect failed — ensure IBM Data Server *Client* "
                "(SQLLIB with db2.exe) is installed, not only the JDBC jar / dsdriver."
            )
        if "DB21018E" in out:
            hint = (
                " CLP failed to start (DB21018E). Run from a DB2 Command Window "
                "or ensure db2cmd.exe is next to db2.exe and DB2INSTANCE is set."
            )
        if "SQL30081N" in out:
            hint = f" Network error reaching {host}:{port} — check firewall / DB2 port."
        raise RuntimeError(
            f"DB2 CLP EXPORT produced no file (exit {rc}): {out[-2000:]}{hint}"
        )

    rows = _parse_rows_exported(out)
    if rows is None:
        rows = _count_file_lines(out_file, log)
    _log(log, f"[OK] CLP EXPORT {rows:,} rows -> {out_file.name}")
    return rows


def list_db2_tables(
    role: Dict[str, Any],
    *,
    schema: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> List[TableInfo]:
    """List DB2 tables with CARD and approximate size (NPAGES * tablespace pagesize)."""
    sch = (schema or role.get("schema") or "").strip() or None
    conn = connect_db2(role, logger)
    try:
        cur = conn.cursor()
        # NPAGES = used data pages; PAGESIZE from tablespace (default 4K if missing).
        # CARD / NPAGES can be -1 when RUNSTATS has not been collected.
        base_sql = """
            SELECT t.TABSCHEMA, t.TABNAME, t.CARD, t.NPAGES,
                   COALESCE(ts.PAGESIZE, 4096) AS PAGESIZE
            FROM SYSCAT.TABLES t
            LEFT JOIN SYSCAT.TABLESPACES ts ON t.TBSPACE = ts.TBSPACE
            WHERE t.TYPE = 'T'
        """
        if sch:
            cur.execute(
                base_sql + " AND t.TABSCHEMA = ? ORDER BY t.TABSCHEMA, t.TABNAME",
                [sch],
            )
        else:
            cur.execute(
                base_sql
                + " AND t.TABSCHEMA NOT LIKE 'SYS%' ORDER BY t.TABSCHEMA, t.TABNAME"
            )
        out: List[TableInfo] = []
        for row in cur.fetchall():
            card = _py_int(row[2])
            if card < 0:
                card = 0
            npages = _py_int(row[3])
            pagesize = _py_int(row[4]) or 4096
            size_bytes = max(0, npages) * max(0, pagesize) if npages > 0 else 0
            out.append(
                TableInfo(
                    schema=_py_str(row[0]),
                    name=_py_str(row[1]),
                    src_rows=card,
                    src_size_bytes=size_bytes,
                )
            )
        return out
    finally:
        try:
            _safe_close_db2(conn)
        except Exception:
            pass


def get_table_card(
    role: Dict[str, Any],
    table: str,
    *,
    logger: Optional[logging.Logger] = None,
) -> int:
    schema, name = _split_fqn(table)
    conn = connect_db2(role, logger)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT CARD FROM SYSCAT.TABLES
            WHERE TYPE = 'T' AND TABSCHEMA = ? AND TABNAME = ?
            """,
            [schema, name],
        )
        row = cur.fetchone()
        card = _py_int(row[0]) if row else 0
        return max(0, card)
    finally:
        try:
            _safe_close_db2(conn)
        except Exception:
            pass


def _fetch_db2_columns(cur, schema: str, table: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS, IDENTITY, COLNO
            FROM SYSCAT.COLUMNS
            WHERE TABSCHEMA = ? AND TABNAME = ?
            ORDER BY COLNO
            """,
            [schema, table],
        )
        has_identity = True
    except Exception:
        cur.execute(
            """
            SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS, 'N', COLNO
            FROM SYSCAT.COLUMNS
            WHERE TABSCHEMA = ? AND TABNAME = ?
            ORDER BY COLNO
            """,
            [schema, table],
        )
        has_identity = False
    cols = []
    for row in cur.fetchall():
        cols.append(
            {
                "name": _py_str(row[0]),
                "type_name": _py_str(row[1]).upper(),
                "length": _py_int(row[2]),
                "scale": _py_int(row[3]),
                "nullable": _py_str(row[4]).upper() == "Y",
                "is_identity": has_identity and _py_str(row[5]).upper() == "Y",
                "colno": _py_int(row[6]),
            }
        )
    return cols


def _db2_to_sql_ddl(col: Dict[str, Any]) -> str:
    raw = (col["type_name"] or "").upper()
    mapped = _TYPE_MAP.get(raw, "NVARCHAR(MAX)")
    length = int(col.get("length") or 0)
    scale = int(col.get("scale") or 0)
    null_sql = "NULL" if col.get("nullable", True) else "NOT NULL"
    name = _quote(col["name"])

    if col.get("is_identity"):
        # Must be IDENTITY on dest because EXPORT/BCP skip these columns
        id_type = mapped
        if id_type in ("NVARCHAR(MAX)", "NVARCHAR", "NCHAR", "VARCHAR", "CHAR"):
            id_type = "BIGINT"
        if id_type in ("DECIMAL", "NUMERIC"):
            id_type = "BIGINT"
        if "(" in id_type:
            id_type = id_type.split("(", 1)[0]
        return f"{name} {id_type} IDENTITY(1,1) NOT NULL"

    if mapped in ("NVARCHAR(MAX)", "VARBINARY(MAX)"):
        return f"{name} {mapped} {null_sql}"
    if mapped in ("DECIMAL", "NUMERIC"):
        p = max(1, min(length or 18, 38))
        s = max(0, min(scale, p))
        return f"{name} {mapped}({p},{s}) {null_sql}"
    if mapped in ("NVARCHAR", "NCHAR", "VARCHAR", "CHAR", "VARBINARY", "BINARY"):
        # DB2 LENGTH is often bytes / CCSID-dependent — use MAX on auto-create to avoid BCP truncation
        if mapped.startswith("VARBIN") or mapped == "BINARY":
            return f"{name} VARBINARY(MAX) {null_sql}"
        return f"{name} NVARCHAR(MAX) {null_sql}"
    return f"{name} {mapped} {null_sql}"


def ensure_dest_table_from_db2(
    dest_role: Dict[str, Any],
    db2_cols: Sequence[Dict[str, Any]],
    table: str,
    *,
    truncate_if_exists: bool = False,
    log: Optional[LogFn] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    schema, name = _split_fqn(table)
    with connect_sql(dest_role, logger) as dest:
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
        cur.execute("SELECT 1 FROM sys.schemas WHERE name = ?", schema)
        if not cur.fetchone() and schema.lower() != "dbo":
            cur.execute(f"CREATE SCHEMA {_quote(schema)}")
        col_sql = ",\n  ".join(_db2_to_sql_ddl(c) for c in db2_cols)
        ddl = f"CREATE TABLE {_quote(schema)}.{_quote(name)} (\n  {col_sql}\n)"
        _log(log, f"Creating dest table from DB2 columns: {schema}.{name}")
        cur.execute(ddl)


def _cell_to_str(val: Any, type_name: str) -> str:
    if val is None:
        return ""
    tn = (type_name or "").upper()
    if tn in ("BLOB",) or isinstance(val, (bytes, bytearray, memoryview)):
        return ""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, dtime):
        return val.strftime("%H:%M:%S")
    if isinstance(val, Decimal):
        return format(val, "f")
    if isinstance(val, bool):
        return "1" if val else "0"
    s = str(val)
    if s.startswith("java.") and hasattr(val, "toString"):
        try:
            s = str(val.toString())
        except Exception:
            pass
    return s.replace("\r", " ").replace("\n", " ").replace("|", "/")


def _safe_close_db2(conn: Any) -> None:
    """Commit/rollback then close so JCC does not raise -4471."""
    if conn is None:
        return
    try:
        jconn = getattr(conn, "jconn", None)
        if jconn is not None:
            try:
                if hasattr(jconn, "getAutoCommit") and not jconn.getAutoCommit():
                    try:
                        jconn.commit()
                    except Exception:
                        try:
                            jconn.rollback()
                        except Exception:
                            pass
            except Exception:
                pass
        else:
            try:
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


def _prepare_db2_jdbc_conn(conn: Any) -> None:
    """Disable autocommit / hold cursors so result sets stay readable with jaydebeapi."""
    try:
        jconn = getattr(conn, "jconn", None)
        if jconn is None:
            return
        try:
            jconn.setAutoCommit(False)
        except Exception:
            pass
        try:
            # java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT == 1
            jconn.setHoldability(1)
        except Exception:
            pass
    except Exception:
        pass


def extract_db2_table_to_pipe(
    role: Dict[str, Any],
    table: str,
    out_file: Path,
    *,
    columns: Sequence[Dict[str, Any]],
    write_order: Sequence[str],
    log: Optional[LogFn] = None,
    cancel_event: Optional[threading.Event] = None,
    fetch_size: int = DEFAULT_JDBC_FETCH,
    logger: Optional[logging.Logger] = None,
    row_hint: int = 0,
) -> int:
    """Stream DB2 rows to a pipe-delimited file (small/medium tables only)."""
    schema, name = _split_fqn(table)
    by_name = {c["name"]: c for c in columns}
    by_lower = {c["name"].lower(): c for c in columns}
    write_cols: List[Dict[str, Any]] = []
    for col_name in write_order:
        hit = by_name.get(col_name) or by_lower.get(col_name.lower())
        if hit is None:
            raise RuntimeError(f"DB2 column not found for export: {col_name}")
        write_cols.append(hit)
    if not write_cols:
        raise RuntimeError(f"No exportable columns for {table}")
    col_list = ", ".join(f'"{c["name"]}"' for c in write_cols)
    sql = f'SELECT {col_list} FROM "{schema}"."{name}"'
    out_file.parent.mkdir(parents=True, exist_ok=True)
    rows_out = 0
    conn = connect_db2(role, logger)
    _prepare_db2_jdbc_conn(conn)
    try:
        cur = conn.cursor()
        _log(log, f"DB2 JDBC extract {table} ...")
        cur.execute(sql)
        # Ask JCC for larger network fetch batches when possible
        try:
            jstmt = getattr(cur, "_statement", None) or getattr(cur, "statement", None)
            if jstmt is not None and hasattr(jstmt, "setFetchSize"):
                jstmt.setFetchSize(int(fetch_size))
        except Exception:
            pass

        def _write_row(row: Any) -> None:
            nonlocal rows_out
            vals = [
                _cell_to_str(row[i] if i < len(row) else None, write_cols[i]["type_name"])
                for i in range(len(write_cols))
            ]
            fout.write("|".join(vals) + "\n")
            rows_out += 1

        with out_file.open("w", encoding="utf-8", newline="\n", buffering=1024 * 1024) as fout:
            # jaydebeapi + DB2 JCC: fetchmany() after the last batch often raises
            # ERRORCODE=-4470 (result set is closed) instead of returning [].
            # Prefer fetchall for moderate sizes; otherwise fetchone loop.
            use_fetchall = row_hint <= 0 or row_hint <= 500_000
            if use_fetchall:
                try:
                    all_rows = cur.fetchall()
                except Exception as ex:
                    msg = str(ex).lower()
                    if "result set is closed" in msg or "-4470" in msg:
                        all_rows = []
                    else:
                        raise
                for row in all_rows or []:
                    if cancel_event and cancel_event.is_set():
                        break
                    _write_row(row)
                    if rows_out % 500000 == 0 and rows_out:
                        _log(log, f"  ... extracted {rows_out:,} rows")
            else:
                while True:
                    if cancel_event and cancel_event.is_set():
                        break
                    try:
                        row = cur.fetchone()
                    except Exception as ex:
                        msg = str(ex).lower()
                        if "result set is closed" in msg or "-4470" in msg:
                            break
                        raise
                    if row is None:
                        break
                    _write_row(row)
                    if rows_out % 500000 == 0 and rows_out:
                        _log(log, f"  ... extracted {rows_out:,} rows")
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        try:
            _safe_close_db2(conn)
        except Exception:
            pass
    _log(log, f"[OK] JDBC extracted {rows_out:,} rows -> {out_file.name}")
    return rows_out


def _export_select_sql(schema: str, name: str, write_order: Sequence[str]) -> str:
    cols = ", ".join(f'"{c}"' for c in write_order)
    return f'SELECT {cols} FROM "{schema}"."{name}"'


def _parse_rows_exported(text: str) -> Optional[int]:
    m = re.search(r"Number of rows exported[:\s]+([\d,]+)", text, re.I)
    if m:
        return int(m.group(1).replace(",", ""))
    m = re.search(r"rows exported[:\s]+([\d,]+)", text, re.I)
    if m:
        return int(m.group(1).replace(",", ""))
    return None


def _count_file_lines(path: Path, log: Optional[LogFn] = None) -> int:
    """Count lines efficiently for large DEL/pipe files."""
    n = 0
    with path.open("rb") as f:
        while True:
            chunk = f.read(8 * 1024 * 1024)
            if not chunk:
                break
            n += chunk.count(b"\n")
    return n


def export_db2_table_admin_cmd(
    role: Dict[str, Any],
    table: str,
    out_file: Path,
    *,
    write_order: Sequence[str],
    log: Optional[LogFn] = None,
    logger: Optional[logging.Logger] = None,
) -> int:
    """
    Server-side EXPORT via SYSPROC.ADMIN_CMD.
    out_file path must be visible to the DB2 server (local path on server or UNC both can mount).
    """
    schema, name = _split_fqn(table)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    # Quote path for ADMIN_CMD; wrap in double-quotes when spaces present
    path_for_server = str(out_file).replace("/", "\\")
    if " " in path_for_server:
        path_sql = '"' + path_for_server.replace('"', '') + '"'
    else:
        path_sql = path_for_server
    path_sql = path_sql.replace("'", "''")
    select_sql = _export_select_sql(schema, name, write_order)
    cmd = (
        f"EXPORT TO '{path_sql}' OF DEL MODIFIED BY coldel0x7C nochardel {select_sql}"
    )
    _log(log, f"DB2 ADMIN_CMD EXPORT {table} -> {out_file}")
    _log(log, "  (file path must be writable by the DB2 server process)")
    conn = connect_db2(role, logger)
    try:
        cur = conn.cursor()
        cur.execute(f"CALL SYSPROC.ADMIN_CMD(?)", [cmd])
        # Result set may contain rows_exported
        rows = None
        try:
            while True:
                try:
                    rset = cur.fetchall()
                except Exception:
                    rset = None
                if not rset:
                    if not cur.nextset():
                        break
                    continue
                for r in rset:
                    # common: (ROWS_EXPORTED, ...) or message text
                    for cell in r:
                        if isinstance(cell, (int, float)) and int(cell) >= 0:
                            rows = int(cell)
                        elif isinstance(cell, str):
                            parsed = _parse_rows_exported(cell)
                            if parsed is not None:
                                rows = parsed
                if not cur.nextset():
                    break
        except Exception:
            pass
    finally:
        try:
            _safe_close_db2(conn)
        except Exception:
            pass

    # File may appear on UNC from server's perspective
    if not out_file.exists():
        # Wait briefly for share sync
        for _ in range(30):
            time.sleep(1)
            if out_file.exists():
                break
    if not out_file.exists():
        raise RuntimeError(
            f"ADMIN_CMD EXPORT finished but file not visible at {out_file}. "
            "Staging UNC must be writable by the DB2 server, or install DB2 CLP on this host."
        )
    if rows is None:
        rows = _count_file_lines(out_file, log)
    _log(log, f"[OK] ADMIN_CMD EXPORT {rows:,} rows -> {out_file.name}")
    return rows


def export_db2_table_auto(
    role: Dict[str, Any],
    table: str,
    out_file: Path,
    *,
    write_order: Sequence[str],
    columns: Sequence[Dict[str, Any]],
    log: Optional[LogFn] = None,
    logger: Optional[logging.Logger] = None,
    cancel_event: Optional[threading.Event] = None,
    fetch_size: int = DEFAULT_JDBC_FETCH,
    row_hint: int = 0,
    allow_admin_cmd: bool = False,
) -> Tuple[int, str]:
    """
    Large-table extract order:
      1) DB2 CLP EXPORT (fastest when db2.exe is installed)
      2) Client-side JDBC stream to staging (this host writes the file — always works with UNC)
      3) Optional ADMIN_CMD only when allow_admin_cmd=True (needs DB2 *server* write ACL on path)
    """
    db2_exe = find_db2_clp()
    if db2_exe:
        try:
            n = export_db2_table_clp(
                role, table, out_file, write_order=write_order, db2_exe=db2_exe, log=log
            )
            return n, "clp"
        except Exception as ex:
            _log(log, f"[Note] CLP EXPORT failed, using client-side extract: {ex}")
            try:
                if out_file.exists():
                    out_file.unlink()
            except Exception:
                pass

    if allow_admin_cmd:
        try:
            n = export_db2_table_admin_cmd(
                role, table, out_file, write_order=write_order, log=log, logger=logger
            )
            return n, "admin_cmd"
        except Exception as ex:
            _log(log, f"[Note] ADMIN_CMD EXPORT not usable ({ex}); using client-side extract")
            try:
                if out_file.exists():
                    out_file.unlink()
            except Exception:
                pass

    # Client-side: JDBC reads from DB2, this machine writes staging (UNC/local).
    # This unblocks large tables when CLP is missing and ADMIN_CMD cannot see the share.
    _log(
        log,
        f"{table}: client-side extract -> staging (JDBC stream; no DB2 server file write required)",
    )
    n = extract_db2_table_to_pipe(
        role,
        table,
        out_file,
        columns=columns,
        write_order=write_order,
        log=log,
        cancel_event=cancel_event,
        fetch_size=fetch_size,
        logger=logger,
        row_hint=row_hint,
    )
    return n, "client_jdbc"


def choose_extract_method(card: int, jdbc_max: int, export_available_hint: bool) -> str:
    """Return 'jdbc' (small) or 'export' (large — CLP or client stream + BCP)."""
    if card < 0:
        card = 0
    if card >= jdbc_max:
        return "export"
    return "jdbc"


def run_db2_preflight(cfg: Dict[str, Any], log: Optional[LogFn] = None) -> Tuple[bool, List[str]]:
    """Checks for DB2 -> SQL large-migration path."""
    msgs: List[str] = []
    ok = True
    try:
        jar = ensure_db2_jdbc_driver(logging.getLogger("db2_bcp"))
        if jar:
            msgs.append(f"[OK] JDBC driver: {jar}")
        else:
            ok = False
            msgs.append("[FAIL] DB2 JDBC driver not found")
    except Exception as ex:
        ok = False
        msgs.append(f"[FAIL] JDBC driver: {ex}")

    bcp = find_bcp_exe()
    if bcp:
        msgs.append(f"[OK] bcp.exe: {bcp}")
    else:
        ok = False
        msgs.append("[FAIL] bcp.exe not found")

    clp = find_db2_clp()
    if clp:
        db2cmd = find_db2cmd(clp)
        msgs.append(f"[OK] DB2 CLP: {clp}")
        if sys.platform.startswith("win") and not db2cmd:
            msgs.append(
                "[WARN] db2cmd.exe not found next to db2.exe — CLP EXPORT may fail (DB21018E)"
            )
        else:
            msgs.append("[OK] Large tables prefer native CLP EXPORT (remote catalog + EXPORT)")
    else:
        msgs.append(
            "[WARN] db2.exe CLP not installed — large tables use slower client JDBC extract. "
            "Install IBM Data Server Client (SQLLIB with db2.exe + db2cmd.exe), "
            "not only the JDBC jar / Data Server Driver Package. "
            "Then set DB2_HOME or DB2_CLP to the SQLLIB folder and re-Validate."
        )

    src = {
        "server": cfg.get("src_server"),
        "db": cfg.get("src_db"),
        "user": cfg.get("src_user"),
        "password": cfg.get("src_password"),
        "port": cfg.get("src_port") or 50000,
        "schema": cfg.get("src_schema") or "",
    }
    try:
        conn = connect_db2(src)
        # Probe ADMIN_CMD exists
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM SYSCAT.ROUTINES WHERE ROUTINESCHEMA='SYSPROC' AND ROUTINENAME='ADMIN_CMD'"
            )
            if cur.fetchone():
                msgs.append("[OK] SYSPROC.ADMIN_CMD available")
            else:
                msgs.append("[WARN] SYSPROC.ADMIN_CMD not found in catalog")
        except Exception as ex:
            msgs.append(f"[WARN] Could not probe ADMIN_CMD: {ex}")
        _safe_close_db2(conn)
        msgs.append(f"[OK] DB2 connect {src['server']}:{src['port']}/{src['db']}")
    except Exception as ex:
        ok = False
        msgs.append(f"[FAIL] DB2 connect: {ex}")

    dest = {
        "server": cfg.get("dest_server"),
        "db": cfg.get("dest_db"),
        "auth": cfg.get("dest_auth") or "windows",
        "user": cfg.get("dest_user") or "",
        "password": cfg.get("dest_password"),
    }
    try:
        with connect_sql(dest) as c:
            c.cursor().execute("SELECT 1")
        msgs.append(f"[OK] Dest SQL {dest['server']} / {dest['db']}")
    except Exception as ex:
        ok = False
        msgs.append(f"[FAIL] Dest SQL: {ex}")

    try:
        root = resolve_staging_dir((cfg.get("work_dir") or "").strip() or None, log)
        msgs.append(f"[OK] Staging folder writable: {root}")
        jdbc_max = int(cfg.get("jdbc_max_rows") or DEFAULT_JDBC_MAX_ROWS)
        msgs.append(
            f"[OK] Auto-route: CARD < {jdbc_max:,} => JDBC; else => CLP/client extract + BCP"
        )
    except Exception as ex:
        ok = False
        msgs.append(f"[FAIL] Staging: {ex}")

    for m in msgs:
        _log(log, m)
    return ok, msgs


def _migrate_one_table(
    *,
    table: str,
    src_role: Dict[str, Any],
    dest_role: Dict[str, Any],
    run_dir: Path,
    bcp_exe: str,
    cfg: Dict[str, Any],
    log: Optional[LogFn],
    cancel_event: Optional[threading.Event],
    lg: logging.Logger,
    row_hint: int,
) -> TableResult:
    t0 = time.monotonic()
    if cancel_event and cancel_event.is_set():
        return TableResult(table=table, status="skipped", message="cancelled")

    create_missing = bool(cfg.get("create_missing", True))
    truncate_dest = bool(cfg.get("truncate_dest", False))
    verify = bool(cfg.get("verify_after_copy", True))
    batch_size = int(cfg.get("batch_size") or 50000)
    keep_files = bool(cfg.get("keep_bcp_files", False))
    jdbc_max = int(cfg.get("jdbc_max_rows") or DEFAULT_JDBC_MAX_ROWS)
    fetch_size = int(cfg.get("jdbc_fetch_size") or DEFAULT_JDBC_FETCH)

    schema, name = _split_fqn(table)
    conn = connect_db2(src_role, lg)
    try:
        cur = conn.cursor()
        db2_cols = _fetch_db2_columns(cur, schema, name)
    finally:
        _safe_close_db2(conn)
    if not db2_cols:
        raise RuntimeError(f"No columns on DB2 table {table}")

    if create_missing or truncate_dest:
        ensure_dest_table_from_db2(
            dest_role,
            db2_cols,
            table,
            truncate_if_exists=truncate_dest,
            log=log,
            logger=lg,
        )
    else:
        if not list_dest_columns(dest_role, table, logger=lg):
            raise RuntimeError(f"Dest table missing: {table} (enable Create missing)")

    dest_cols = list_dest_columns(dest_role, table, logger=lg)
    by_lower = {c["name"].lower(): c for c in dest_cols}
    write_order: List[str] = []
    for c in db2_cols:
        if c.get("is_identity"):
            continue
        # Skip BLOB in character EXPORT/BCP path
        if (c.get("type_name") or "").upper() == "BLOB":
            continue
        hit = by_lower.get(c["name"].lower())
        if hit and not hit.get("is_identity"):
            write_order.append(hit["name"])
    if not write_order:
        raise RuntimeError(f"No matching dest columns for {table}")

    card = row_hint
    if card <= 0:
        try:
            card = get_table_card(src_role, table, logger=lg)
        except Exception:
            card = 0

    method = choose_extract_method(card, jdbc_max, True)
    # Unknown CARD: use large path (CLP or client extract)
    if card == 0 and bool(cfg.get("prefer_export_when_unknown", True)):
        method = "export"

    pipe = run_dir / f"{schema}.{name}.bcp.txt"
    used = method
    allow_admin = bool(cfg.get("allow_admin_cmd_export", False))
    if method == "export":
        _log(
            log,
            f"{table}: CARD~{card:,} -> large path (CLP if present, else client extract + BCP)",
        )
        rows, used = export_db2_table_auto(
            src_role,
            table,
            pipe,
            write_order=write_order,
            columns=db2_cols,
            log=log,
            logger=lg,
            cancel_event=cancel_event,
            fetch_size=fetch_size,
            row_hint=card,
            allow_admin_cmd=allow_admin,
        )
        used = f"large/{used}"
    else:
        _log(log, f"{table}: CARD~{card:,} -> JDBC extract + BCP")
        rows = extract_db2_table_to_pipe(
            src_role,
            table,
            pipe,
            columns=db2_cols,
            write_order=write_order,
            log=log,
            cancel_event=cancel_event,
            fetch_size=fetch_size,
            logger=lg,
            row_hint=card,
        )
        used = "jdbc"

    if cancel_event and cancel_event.is_set():
        return TableResult(table=table, status="skipped", message="cancelled")

    fmt = run_dir / f"{schema}.{name}.fmt"
    write_bcp_format_file(fmt, dest_cols=dest_cols, write_order=write_order)

    if rows == 0:
        dest_n = count_rows(dest_role, table, logger=lg) if verify else 0
        if not keep_files:
            pipe.unlink(missing_ok=True)
            fmt.unlink(missing_ok=True)
        return TableResult(
            table=table,
            status="ok",
            src_rows=0,
            dest_rows=dest_n,
            duration_sec=time.monotonic() - t0,
            message=f"empty source ({used})",
        )

    bcp_in_pipe_file(
        bcp_exe=bcp_exe,
        dest_role=dest_role,
        table=table,
        data_file=pipe,
        format_file=fmt,
        batch_size=batch_size,
        log=log,
    )
    dest_n = count_rows(dest_role, table, logger=lg) if verify else rows
    ok = (dest_n == rows) if verify else True
    if ok:
        msg = f"{used}: imported {rows:,} rows"
        _log(log, f"[OK] {table}: {msg}")
    else:
        hint = ""
        if not truncate_dest and dest_n > rows:
            hint = (
                " — dest already had rows; enable 'Truncate before load' on re-run "
                "or clear the dest table"
            )
        msg = f"{used}: row mismatch extracted={rows:,} dest={dest_n:,}{hint}"
        _log(log, f"[FAIL] {table}: {msg}")
    if not keep_files:
        try:
            pipe.unlink(missing_ok=True)
            fmt.unlink(missing_ok=True)
        except Exception:
            pass
    return TableResult(
        table=table,
        status="ok" if ok else "fail",
        src_rows=rows,
        dest_rows=dest_n,
        duration_sec=time.monotonic() - t0,
        message=msg,
        error=None if ok else msg,
    )


def run_db2_bcp_migration(
    cfg: Dict[str, Any],
    log: Optional[LogFn] = None,
    *,
    cancel_event: Optional[threading.Event] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> BcpJobReport:
    """
    Auto large-DB path:
      CARD < jdbc_max_rows -> JDBC extract + BCP
      else -> CLP (if installed) or client-side JDBC extract + BCP
            (ADMIN_CMD only if allow_admin_cmd_export=True)
    Parallelism via parallel_tables (default 4).
    """
    started = datetime.now(timezone.utc).isoformat()
    lg = logging.getLogger("db2_bcp")
    src_role = {
        "server": cfg["src_server"],
        "db": cfg["src_db"],
        "user": cfg.get("src_user") or "",
        "password": cfg.get("src_password"),
        "port": int(cfg.get("src_port") or 50000),
        "schema": (cfg.get("src_schema") or "").strip(),
    }
    dest_role = {
        "server": cfg["dest_server"],
        "db": cfg["dest_db"],
        "auth": cfg.get("dest_auth") or "windows",
        "user": cfg.get("dest_user") or "",
        "password": cfg.get("dest_password"),
    }
    tables: List[str] = list(cfg.get("tables") or [])
    if not tables:
        raise ValueError("No tables selected.")

    work_root = resolve_staging_dir((cfg.get("work_dir") or "").strip() or None, log)
    run_dir = work_root / datetime.now().strftime("db2_run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    jdbc_max = int(cfg.get("jdbc_max_rows") or DEFAULT_JDBC_MAX_ROWS)
    parallel = max(1, int(cfg.get("parallel_tables") or DEFAULT_PARALLEL))
    # Cap parallelism for safety on shared hosts
    parallel = min(parallel, 16)
    row_hints: Dict[str, int] = dict(cfg.get("table_row_hints") or {})

    _log(
        log,
        f"DB2 large-migration mode: JDBC if CARD < {jdbc_max:,}; "
        f"else CLP/client extract + BCP. parallel={parallel}",
    )

    if bool(cfg.get("dry_run", False)):
        finished = datetime.now(timezone.utc).isoformat()
        return BcpJobReport(
            ok=True,
            started_at=started,
            finished_at=finished,
            tables=[
                TableResult(
                    table=t,
                    status="ok",
                    message=(
                        "dry-run EXPORT+BCP"
                        if row_hints.get(t, jdbc_max) >= jdbc_max
                        else "dry-run JDBC+BCP"
                    ),
                )
                for t in tables
            ],
            work_dir=str(run_dir),
        )

    bcp_exe = find_bcp_exe()
    if not bcp_exe:
        raise RuntimeError("bcp.exe not found.")

    results: List[TableResult] = []
    all_ok = True
    total = len(tables)
    done_count = 0
    lock = threading.Lock()

    def _run(table: str) -> TableResult:
        nonlocal done_count, all_ok
        try:
            tr = _migrate_one_table(
                table=table,
                src_role=src_role,
                dest_role=dest_role,
                run_dir=run_dir,
                bcp_exe=bcp_exe,
                cfg=cfg,
                log=log,
                cancel_event=cancel_event,
                lg=lg,
                row_hint=int(row_hints.get(table) or 0),
            )
        except Exception as ex:
            _log(log, f"[FAIL] {table}: {ex}")
            tr = TableResult(
                table=table,
                status="fail",
                error=str(ex),
                message=str(ex),
            )
        with lock:
            done_count += 1
            if tr.status == "fail":
                all_ok = False
            if progress_callback:
                progress_callback(done_count, total, table)
        return tr

    if parallel <= 1 or len(tables) == 1:
        for i, table in enumerate(tables):
            if cancel_event and cancel_event.is_set():
                results.append(TableResult(table=table, status="skipped", message="cancelled"))
                all_ok = False
                break
            if progress_callback:
                progress_callback(i, total, table)
            results.append(_run(table))
            if results[-1].status == "fail" and not bool(cfg.get("continue_on_error", True)):
                break
    else:
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futs = {pool.submit(_run, t): t for t in tables}
            for fut in as_completed(futs):
                results.append(fut.result())
                if cancel_event and cancel_event.is_set():
                    break

    # Stable order by input table list
    by_name = {r.table: r for r in results}
    ordered = [by_name[t] for t in tables if t in by_name]
    for r in results:
        if r.table not in {x.table for x in ordered}:
            ordered.append(r)

    finished = datetime.now(timezone.utc).isoformat()
    fails = [r for r in ordered if r.status == "fail"]
    if fails:
        _log(log, f"=== DB2 -> SQL BCP FINISHED WITH ERRORS ({len(fails)} table(s)) ===")
        for r in fails:
            _log(log, f"  - {r.table}: {r.message or r.error or 'failed'}")
    else:
        _log(log, "=== DB2 -> SQL BCP SUCCEEDED ===")
    return BcpJobReport(
        ok=all_ok,
        started_at=started,
        finished_at=finished,
        tables=ordered,
        work_dir=str(run_dir),
    )
