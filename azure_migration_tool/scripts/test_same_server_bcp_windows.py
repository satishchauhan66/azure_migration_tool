#!/usr/bin/env python
"""
Same-server BCP smoke test with Windows auth.

Creates (if needed) a tiny source table and a destination DB on the same instance,
then BCP OUTs and BCP INs one table until success.

Usage (from azure_migration_tool folder):
  python scripts/test_same_server_bcp_windows.py
  python scripts/test_same_server_bcp_windows.py --table dbo.SomeExistingTable --src-db testsatish
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

_APP_DIR = Path(__file__).resolve().parent.parent
_REPO_ROOT = _APP_DIR.parent
for p in (_REPO_ROOT, _APP_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import pyodbc

from src.utils.database import build_conn_str, pick_sql_driver

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("same_server_bcp")

DEFAULT_SERVER = r"gpitd-shir01.us.pressganey.com\i2022"
DEFAULT_SRC_DB = "testsatish"
DEFAULT_DEST_DB = "testdb_bcp"
DEFAULT_TABLE = "dbo.AMT_BCP_SMOKE"
_CREATE_NO_WINDOW = 0x08000000 if sys.platform.startswith("win") else 0


def find_bcp() -> str:
    candidates = [
        r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe",
        r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    found = shutil.which("bcp.exe")
    if not found:
        raise RuntimeError("bcp.exe not found")
    return found


def connect(server: str, db: str):
    driver = pick_sql_driver(log)
    cs = build_conn_str(server, db, "", driver, "windows", None)
    log.info("ODBC connect: server=%s db=%s driver=%s auth=windows", server, db, driver)
    return pyodbc.connect(cs, timeout=60, autocommit=True)


def ensure_dest_db(server: str, dest_db: str) -> None:
    with connect(server, "master") as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sys.databases WHERE name = ?", dest_db)
        if cur.fetchone():
            log.info("[OK] Destination DB already exists: %s", dest_db)
            return
        log.info("Creating database [%s]...", dest_db)
        cur.execute(f"CREATE DATABASE [{dest_db}]")
        log.info("[OK] Created database [%s]", dest_db)


def ensure_smoke_source(server: str, src_db: str, table: str) -> None:
    """Create a tiny source table if using the default smoke table name."""
    if table.upper() != DEFAULT_TABLE.upper():
        return
    schema, name = table.split(".", 1)
    with connect(server, src_db) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?
            """,
            schema,
            name,
        )
        if cur.fetchone():
            log.info("[OK] Source smoke table exists: %s.%s", schema, name)
            return
        log.info("Creating smoke source table %s.%s ...", schema, name)
        cur.execute(
            f"""
            CREATE TABLE [{schema}].[{name}] (
                id INT NOT NULL PRIMARY KEY,
                label NVARCHAR(100) NOT NULL,
                created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO [{schema}].[{name}] (id, label) VALUES
                (1, N'row-one'),
                (2, N'row-two'),
                (3, N'row-three')
            """
        )
        log.info("[OK] Created and seeded %s.%s", schema, name)


def ensure_dest_table(server: str, dest_db: str, table: str) -> None:
    schema, name = table.split(".", 1) if "." in table else ("dbo", table)
    with connect(server, dest_db) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?
            """,
            schema,
            name,
        )
        if cur.fetchone():
            log.info("[OK] Dest table exists; truncating: %s.%s", schema, name)
            cur.execute(f"TRUNCATE TABLE [{schema}].[{name}]")
            return
        log.info("Creating dest table %s.%s ...", schema, name)
        cur.execute(
            f"""
            CREATE TABLE [{schema}].[{name}] (
                id INT NOT NULL PRIMARY KEY,
                label NVARCHAR(100) NOT NULL,
                created_at DATETIME2 NOT NULL
            );
            """
        )
        log.info("[OK] Created dest table %s.%s", schema, name)


def table_exists(server: str, db: str, table: str) -> bool:
    schema, name = table.split(".", 1) if "." in table else ("dbo", table)
    with connect(server, db) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?
            """,
            schema,
            name,
        )
        return cur.fetchone() is not None


def count_rows(server: str, db: str, table: str) -> int:
    schema, name = table.split(".", 1) if "." in table else ("dbo", table)
    with connect(server, db) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT_BIG(*) FROM [{schema}].[{name}]")
        return int(cur.fetchone()[0])


def run_bcp(args: list[str]) -> None:
    log.info("BCP: %s", " ".join(args))
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        creationflags=_CREATE_NO_WINDOW,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    for line in out.splitlines():
        if line.strip():
            log.info("  %s", line)
    if proc.returncode != 0:
        raise RuntimeError(f"bcp failed with exit {proc.returncode}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=DEFAULT_SERVER)
    ap.add_argument("--src-db", default=DEFAULT_SRC_DB)
    ap.add_argument("--dest-db", default=DEFAULT_DEST_DB)
    ap.add_argument("--table", default=DEFAULT_TABLE, help="schema.table")
    ap.add_argument(
        "--search-analysis",
        action="store_true",
        help="Search for ANALYSIS_NORM_BASE on the instance before migrating",
    )
    args = ap.parse_args()

    table = args.table
    if "." not in table:
        table = f"dbo.{table}"

    bcp = find_bcp()
    log.info("Using bcp: %s", bcp)

    with connect(args.server, "master") as conn:
        cur = conn.cursor()
        cur.execute("SELECT @@SERVERNAME, SUSER_SNAME(), @@VERSION")
        row = cur.fetchone()
        log.info("[OK] Connected as %s on %s", row[1], row[0])
        log.info("Version: %s", (row[2] or "")[:120])

    if args.search_analysis:
        log.info("Searching online user DBs for ANALYSIS_NORM_BASE...")
        with connect(args.server, "master") as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM sys.databases WHERE state_desc='ONLINE' "
                "AND name NOT IN ('master','model','msdb','tempdb') ORDER BY name"
            )
            dbs = [r[0] for r in cur.fetchall()]
        hits = []
        for db in dbs:
            try:
                with connect(args.server, db) as c2:
                    cur2 = c2.cursor()
                    cur2.execute(
                        "SELECT s.name, t.name FROM sys.tables t "
                        "JOIN sys.schemas s ON s.schema_id=t.schema_id "
                        "WHERE t.name = 'ANALYSIS_NORM_BASE'"
                    )
                    for sname, tname in cur2.fetchall():
                        hits.append((db, sname, tname))
            except Exception as ex:
                log.info("  skip %s: %s", db, ex)
        if hits:
            log.info("Found ANALYSIS_NORM_BASE at: %s", hits)
        else:
            log.info("ANALYSIS_NORM_BASE not found on this SQL instance (expected if it lives on DB2).")

    ensure_dest_db(args.server, args.dest_db)
    ensure_smoke_source(args.server, args.src_db, table)

    if not table_exists(args.server, args.src_db, table):
        raise SystemExit(f"Source table not found: {args.src_db}.{table}")

    ensure_dest_table(args.server, args.dest_db, table)

    src_count = count_rows(args.server, args.src_db, table)
    log.info("Source row count: %s", src_count)

    work = Path(tempfile.mkdtemp(prefix="bcp_same_server_"))
    data_file = work / "data.bcp"
    err_out = work / "out.err"
    err_in = work / "in.err"
    log.info("Work dir: %s", work)

    # Native format, trusted connection (-T)
    run_bcp(
        [
            bcp,
            f"{args.src_db}.{table}",
            "out",
            str(data_file),
            "-S",
            args.server,
            "-T",
            "-n",
            "-b",
            "1000",
            "-e",
            str(err_out),
        ]
    )
    log.info("[OK] BCP OUT size=%s bytes", data_file.stat().st_size)

    run_bcp(
        [
            bcp,
            f"{args.dest_db}.{table}",
            "in",
            str(data_file),
            "-S",
            args.server,
            "-T",
            "-n",
            "-b",
            "1000",
            "-e",
            str(err_in),
            "-E",
        ]
    )
    dest_count = count_rows(args.server, args.dest_db, table)
    log.info("Destination row count: %s", dest_count)
    if dest_count != src_count:
        raise SystemExit(f"Row count mismatch: src={src_count} dest={dest_count}")
    log.info("[SUCCESS] Same-server Windows-auth BCP migrated %s rows (%s -> %s.%s)", dest_count, args.src_db, args.dest_db, table)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as ex:
        log.error("[FAIL] %s", ex)
        raise
