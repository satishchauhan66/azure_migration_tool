#!/usr/bin/env python
"""Migrate one existing table same-server with Windows auth BCP (schema + data)."""

from __future__ import annotations

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
log = logging.getLogger("migrate_one")

SERVER = r"gpitd-shir01.us.pressganey.com\i2022"
SRC_DB = "FUSION_ps_fus_db21q"
DEST_DB = "testdb_bcp"
TABLE = "dbo.ANALYSIS_NORM_BASE"
_CREATE_NO_WINDOW = 0x08000000 if sys.platform.startswith("win") else 0


def find_bcp() -> str:
    for c in (
        r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe",
        r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
    ):
        if os.path.isfile(c):
            return c
    found = shutil.which("bcp.exe")
    if not found:
        raise RuntimeError("bcp.exe not found")
    return found


def connect(db: str):
    driver = pick_sql_driver(log)
    cs = build_conn_str(SERVER, db, "", driver, "windows", None)
    return pyodbc.connect(cs, timeout=120, autocommit=True)


def ensure_dest_db() -> None:
    with connect("master") as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM sys.databases WHERE name = ?", DEST_DB)
        if not cur.fetchone():
            log.info("Creating [%s]...", DEST_DB)
            cur.execute(f"CREATE DATABASE [{DEST_DB}]")
        log.info("[OK] Dest DB %s", DEST_DB)


def quote_ident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def sql_type(row) -> str:
    (
        col_name,
        type_name,
        max_length,
        precision,
        scale,
        is_nullable,
        is_identity,
        coll_name,
    ) = row
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
    return f"{quote_ident(col_name)} {base}{ident} {nulls}"


def ensure_dest_table() -> None:
    schema, name = TABLE.split(".", 1)
    with connect(DEST_DB) as conn:
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
            log.info("[OK] Dest table exists; truncating %s", TABLE)
            cur.execute(f"TRUNCATE TABLE {quote_ident(schema)}.{quote_ident(name)}")
            return

    with connect(SRC_DB) as conn:
        cur = conn.cursor()
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
            raise RuntimeError(f"No columns for {SRC_DB}.{TABLE}")
        col_sql = ",\n  ".join(sql_type(r) for r in cols)
        # PK if present
        cur.execute(
            """
            SELECT c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
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
                f",\n  CONSTRAINT [PK_{name}_mig] PRIMARY KEY ("
                + ", ".join(quote_ident(c) for c in pk_cols)
                + ")"
            )
        ddl = f"CREATE TABLE {quote_ident(schema)}.{quote_ident(name)} (\n  {col_sql}{pk_sql}\n)"

    with connect(DEST_DB) as conn:
        log.info("Creating dest table with DDL from source...")
        conn.cursor().execute(ddl)
        log.info("[OK] Created %s.%s", DEST_DB, TABLE)


def count_rows(db: str) -> int:
    schema, name = TABLE.split(".", 1)
    with connect(db) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT_BIG(*) FROM {quote_ident(schema)}.{quote_ident(name)}")
        return int(cur.fetchone()[0])


def run_bcp(args: list[str]) -> None:
    log.info("BCP: %s", " ".join(args))
    proc = subprocess.run(args, capture_output=True, text=True, creationflags=_CREATE_NO_WINDOW)
    out = (proc.stdout or "") + (proc.stderr or "")
    for line in out.splitlines():
        if line.strip():
            log.info("  %s", line)
    if proc.returncode != 0:
        raise RuntimeError(f"bcp exit {proc.returncode}")


def main() -> int:
    import argparse

    global SERVER, SRC_DB, DEST_DB, TABLE
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=SERVER)
    ap.add_argument("--src-db", default=SRC_DB)
    ap.add_argument("--dest-db", default=DEST_DB)
    ap.add_argument("--table", default=TABLE)
    args = ap.parse_args()
    SERVER = args.server
    SRC_DB = args.src_db
    DEST_DB = args.dest_db
    TABLE = args.table if "." in args.table else f"dbo.{args.table}"

    bcp = find_bcp()
    log.info("bcp=%s server=%s", bcp, SERVER)
    with connect("master") as conn:
        cur = conn.cursor()
        cur.execute("SELECT @@SERVERNAME, SUSER_SNAME()")
        log.info("[OK] %s / %s", *cur.fetchone())

    ensure_dest_db()
    src_n = count_rows(SRC_DB)
    log.info("Source rows (%s.%s): %s", SRC_DB, TABLE, src_n)
    if src_n == 0:
        log.warning(
            "Source table has 0 rows — schema will still be copied. "
            "For ANALYSIS_NORM_BASE, real data may still live on DB2 PS-FUS-DB21Q / FUSION."
        )
    ensure_dest_table()

    work = Path(tempfile.mkdtemp(prefix="bcp_anb_"))
    data_file = work / "data.bcp"
    log.info("Work: %s", work)

    run_bcp(
        [
            bcp,
            f"{SRC_DB}.{TABLE}",
            "out",
            str(data_file),
            "-S",
            SERVER,
            "-T",
            "-n",
            "-b",
            "5000",
            "-e",
            str(work / "out.err"),
        ]
    )
    log.info("[OK] OUT size=%s bytes", data_file.stat().st_size)

    run_bcp(
        [
            bcp,
            f"{DEST_DB}.{TABLE}",
            "in",
            str(data_file),
            "-S",
            SERVER,
            "-T",
            "-n",
            "-b",
            "5000",
            "-e",
            str(work / "in.err"),
            "-E",
        ]
    )
    dest_n = count_rows(DEST_DB)
    log.info("Dest rows: %s", dest_n)
    if dest_n != src_n:
        raise SystemExit(f"Row mismatch src={src_n} dest={dest_n}")
    log.info(
        "[SUCCESS] Migrated %s rows: %s.%s -> %s.%s (Windows auth BCP)",
        dest_n,
        SRC_DB,
        TABLE,
        DEST_DB,
        TABLE,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as ex:
        log.error("[FAIL] %s", ex)
        raise
