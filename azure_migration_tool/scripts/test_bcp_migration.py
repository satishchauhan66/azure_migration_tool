#!/usr/bin/env python
"""
Test BCP export (source) and import (destination) for SurveyDesign -> Azure MI.

Usage (from azure_migration_tool folder, with venv/requirements installed):
  python scripts/test_bcp_migration.py --dest-user you@company.com
  python scripts/test_bcp_migration.py --dest-user you@company.com --table dbo.SomeTable
  python scripts/test_bcp_migration.py --connect-only

Defaults match the SurveyDesign_UAT -> testdemo26 scenario.
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

# Repo layout: scripts/ is under azure_migration_tool/
_APP_DIR = Path(__file__).resolve().parent.parent
_REPO_ROOT = _APP_DIR.parent
for p in (_REPO_ROOT, _APP_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import pyodbc

from src.utils.database import build_conn_str, connect_to_database, pick_sql_driver

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("test_bcp")

DEFAULT_SRC_SERVER = r"db-surveydesign-test\SurveyDesign"
DEFAULT_SRC_DB = "SurveyDesign_UAT"
DEFAULT_DEST_SERVER = "route66-qa-eus2-mi-02.313a5ac3664e.database.windows.net"
DEFAULT_DEST_DB = "testdemo26"

_CREATE_NO_WINDOW = 0x08000000 if sys.platform.startswith("win") else 0


def find_bcp() -> str | None:
    candidates = [
        r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe",
        r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
        r"C:\Program Files\Microsoft SQL Server\150\Tools\Binn\bcp.exe",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return shutil.which("bcp.exe")


def acquire_azure_token(username: str) -> str:
    import msal

    scopes = ["https://database.windows.net/.default"]
    authority = "https://login.microsoftonline.com/common"
    if "@" in username:
        authority = f"https://login.microsoftonline.com/{username.split('@')[1]}"

    client_ids = [
        ("04b07795-8ddf-4c3b-9f7f-8a4e6b7c2d7c", "Azure CLI"),
        ("872cd9fa-d31f-45e0-9eab-6e460a02d1f1", "Visual Studio"),
    ]
    last_err = None
    for client_id, name in client_ids:
        app = msal.PublicClientApplication(client_id, authority=authority)
        accounts = app.get_accounts(username=username)
        if accounts:
            r = app.acquire_token_silent(scopes, account=accounts[0])
            if r and "access_token" in r:
                log.info("[OK] Cached Azure AD token for %s", username)
                return r["access_token"]
        log.info("Interactive MFA sign-in (%s)...", name)
        r = app.acquire_token_interactive(scopes, login_hint=username, prompt="select_account")
        if r and "access_token" in r:
            log.info("[OK] Token acquired via %s", name)
            return r["access_token"]
        last_err = r.get("error_description", r.get("error", "unknown"))
    raise RuntimeError(f"Could not acquire Azure AD token: {last_err}")


def test_odbc(server: str, db: str, auth: str, user: str, password: str | None) -> None:
    driver = pick_sql_driver(log)
    log.info("ODBC test: server=%s db=%s auth=%s driver=%s", server, db, auth, driver)
    if (auth or "").lower() == "entra_mfa":
        conn = connect_to_database(server, db, user, driver, auth, password, timeout=30, logger=log)
    else:
        conn_str = build_conn_str(server, db, user or "", driver, auth, password)
        conn = pyodbc.connect(conn_str, timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT @@VERSION")
    ver = (cur.fetchone()[0] or "")[:120]
    cur.execute("SELECT DB_NAME()")
    log.info("[OK] Connected. DB=%s | %s...", cur.fetchone()[0], ver)
    conn.close()


def pick_smallest_table(server: str, db: str, auth: str, user: str, password: str | None) -> str:
    driver = pick_sql_driver(log)
    conn_str = build_conn_str(server, db, user or "", driver, auth, password)
    conn = pyodbc.connect(conn_str, timeout=60)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT TOP 1 s.name + '.' + t.name
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
        WHERE t.is_ms_shipped = 0
        GROUP BY s.name, t.name
        ORDER BY SUM(p.rows) ASC
        """
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise RuntimeError("No user tables found on source")
    return row[0]


def bcp_auth_args(server: str, db: str, auth: str, user: str, password: str | None, token: str | None) -> list[str]:
    args = ["-S", server, "-d", db]
    auth_l = (auth or "").lower()
    if auth_l == "windows":
        args.append("-T")
    elif auth_l in ("entra_mfa", "entra_password"):
        args.extend(["-G", "-U", user])
        if token:
            args.extend(["-P", token])
        elif password:
            args.extend(["-P", password])
    elif auth_l == "sql":
        args.extend(["-U", user, "-P", password or ""])
    else:
        args.append("-T")
    return args


def run_bcp(cmd: list[str], label: str) -> subprocess.CompletedProcess:
    safe = []
    for i, x in enumerate(cmd):
        if i > 0 and cmd[i - 1] == "-P":
            safe.append("***")
        else:
            safe.append(x)
    log.info("%s: %s", label, " ".join(safe))
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=3600,
        creationflags=_CREATE_NO_WINDOW if sys.platform.startswith("win") else 0,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Test BCP SurveyDesign_UAT -> testdemo26")
    ap.add_argument("--src-server", default=DEFAULT_SRC_SERVER)
    ap.add_argument("--src-db", default=DEFAULT_SRC_DB)
    ap.add_argument("--dest-server", default=DEFAULT_DEST_SERVER)
    ap.add_argument("--dest-db", default=DEFAULT_DEST_DB)
    ap.add_argument("--dest-user", help="Entra UPN for MFA (required unless --connect-only)")
    ap.add_argument("--table", help="Schema.Table to test (default: smallest table on source)")
    ap.add_argument("--connect-only", action="store_true", help="Only test ODBC connectivity")
    ap.add_argument("--export-only", action="store_true", help="Export only, no import")
    args = ap.parse_args()

    bcp = find_bcp()
    if not bcp:
        log.error("bcp.exe not found. Install SQL Server Command Line Utilities / ODBC tools.")
        return 1
    log.info("BCP: %s", bcp)

    log.info("--- Source ODBC (Windows) ---")
    try:
        test_odbc(args.src_server, args.src_db, "windows", "", None)
    except Exception as e:
        log.error("[FAIL] Source: %s", e)
        return 1

    if not args.dest_user and not args.connect_only:
        log.error("--dest-user is required for destination MFA (e.g. you@company.com)")
        return 1

    dest_token = None
    if args.dest_user:
        log.info("--- Destination token (MFA) ---")
        try:
            dest_token = acquire_azure_token(args.dest_user)
        except Exception as e:
            log.error("[FAIL] Token: %s", e)
            return 1

        log.info("--- Destination ODBC (Entra) ---")
        try:
            # ODBC via driver token attr (password arg unused; token acquired above for BCP -P)
            test_odbc(args.dest_server, args.dest_db, "entra_mfa", args.dest_user, None)
        except Exception as e:
            log.error("[FAIL] Destination ODBC: %s", e)
            log.error("BCP may still work if table exists; continuing for export test...")

    if args.connect_only:
        log.info("Connect-only mode: done.")
        return 0

    table = args.table
    if not table:
        log.info("--- Picking smallest user table on source ---")
        table = pick_smallest_table(args.src_server, args.src_db, "windows", "", None)
    schema, tname = table.split(".", 1)
    log.info("Test table: %s", table)

    try:
        from src.utils.bcp_tools import (
            bcp_query_for_export,
            bcp_table_name_for_import,
            create_bcp_migration_dir,
            format_bcp_data_path,
        )
    except ImportError:
        from azure_migration_tool.src.utils.bcp_tools import (
            bcp_query_for_export,
            bcp_table_name_for_import,
            create_bcp_migration_dir,
            format_bcp_data_path,
        )

    tmp = create_bcp_migration_dir()
    data_file = format_bcp_data_path(os.path.join(tmp, f"{schema}_{tname}.dat"))

    src_args = bcp_auth_args(args.src_server, args.src_db, "windows", "", None, None)
    export_cmd = [
        bcp,
        bcp_query_for_export(schema, tname).replace("SELECT *", "SELECT TOP 1000 *", 1),
        "queryout",
        data_file,
        *src_args,
        "-n",
        "-q",
    ]
    log.info("--- BCP export (source) ---")
    r = run_bcp(export_cmd, "export")
    if r.returncode != 0:
        log.error("[FAIL] export:\n%s", (r.stderr or r.stdout or "").strip())
        return 1
    size = os.path.getsize(data_file) if os.path.isfile(data_file) else 0
    log.info("[OK] Export: %s bytes -> %s", size, data_file)

    if args.export_only:
        return 0

    dest_args = bcp_auth_args(
        args.dest_server, args.dest_db, "entra_mfa", args.dest_user or "", None, dest_token
    )
    import_cmd = [
        bcp,
        bcp_table_name_for_import(schema, tname),
        "in",
        data_file,
        *dest_args,
        "-n",
        "-q",
        "-b",
        "5000",
    ]
    log.info("--- BCP import (destination) ---")
    log.info("NOTE: Table [%s].[%s] must already exist on %s", schema, tname, args.dest_db)
    r = run_bcp(import_cmd, "import")
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip()
        log.error("[FAIL] import:\n%s", err)
        if "Invalid object name" in err:
            log.error("Create the table on destination first (Schema tab) or pick a table that exists on both sides.")
        return 1

    log.info("[OK] BCP round-trip test completed for %s", table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
