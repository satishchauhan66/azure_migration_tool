# Author: Satish Chauhan
"""Smoke-test DB2 -> SQL small (JDBC) and large (EXPORT) paths using saved server config."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_APP = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_APP))

from src.migration.db2_bcp_migration import (  # noqa: E402
    choose_extract_method,
    DEFAULT_JDBC_MAX_ROWS,
    export_db2_table_admin_cmd,
    get_table_card,
    list_db2_tables,
    run_db2_bcp_migration,
    run_db2_preflight,
    _fetch_db2_columns,
    connect_db2,
    _safe_close_db2,
)
from src.migration.csv_bcp_import import (  # noqa: E402
    bcp_in_pipe_file,
    list_dest_columns,
    write_bcp_format_file,
)
from src.migration.db2_bcp_migration import ensure_dest_table_from_db2  # noqa: E402
from src.utils.bcp_tools import find_bcp_exe  # noqa: E402


def _load_roles():
    cfg_path = Path.home() / ".azure_migration_tool" / "saved_servers.json"
    servers = json.loads(cfg_path.read_text(encoding="utf-8"))
    db2 = next(s for s in servers if s.get("server", "").upper() == "PS-FUS-DB21Q")
    sql = next(
        s
        for s in servers
        if "gpitd-shir01" in (s.get("server") or "").lower() and s.get("db_type") != "db2"
    )
    src = {
        "server": db2["server"],
        "db": db2.get("database") or "FUSION",
        "user": db2.get("user") or "",
        "password": db2.get("password") or "",
        "port": int(db2.get("port") or 50000),
        "schema": db2.get("schema") or "USERID",
    }
    dest = {
        "server": sql["server"],
        "db": "testdb_bcp",
        "auth": "windows",
        "user": "",
        "password": None,
    }
    return src, dest


def main() -> int:
    src, dest = _load_roles()
    print(f"DB2: {src['server']}:{src['port']}/{src['db']} schema={src['schema']}")
    print(f"SQL: {dest['server']} / {dest['db']}")

    cfg = {
        "src_server": src["server"],
        "src_db": src["db"],
        "src_user": src["user"],
        "src_password": src["password"],
        "src_port": src["port"],
        "src_schema": src["schema"],
        "dest_server": dest["server"],
        "dest_db": dest["db"],
        "dest_auth": dest["auth"],
        "work_dir": r"\\gpitd-shir01.us.pressganey.com\sqlbackups\bcp log",
    }
    print("\n=== Preflight ===")
    ok, msgs = run_db2_preflight(cfg, print)
    if not ok:
        print("PREFLIGHT FAILED")
        return 1
    print("PREFLIGHT PASSED")

    print("\n=== List tables (USERID) ===")
    tables = list_db2_tables(src, schema=src["schema"])
    tables_sorted = sorted(tables, key=lambda t: t.src_rows)
    small = next((t for t in tables_sorted if 0 < t.src_rows < DEFAULT_JDBC_MAX_ROWS), None)
    large = next((t for t in reversed(tables_sorted) if t.src_rows >= DEFAULT_JDBC_MAX_ROWS), None)
    # Prefer a non-trivial small table if available (e.g. ACCT_DEFN)
    preferred_small = next(
        (t for t in tables_sorted if t.name.upper() == "ACCT_DEFN" and t.src_rows < DEFAULT_JDBC_MAX_ROWS),
        None,
    )
    if preferred_small:
        small = preferred_small
    elif not small:
        small = tables_sorted[0] if tables_sorted else None
    print(f"tables={len(tables)} jdbc_max={DEFAULT_JDBC_MAX_ROWS:,}")
    if small:
        print(
            f"small pick: {small.fqn} CARD~{small.src_rows:,} "
            f"route={choose_extract_method(small.src_rows, DEFAULT_JDBC_MAX_ROWS, True)}"
        )
    if large:
        print(
            f"large pick: {large.fqn} CARD~{large.src_rows:,} "
            f"route={choose_extract_method(large.src_rows, DEFAULT_JDBC_MAX_ROWS, True)}"
        )

    # --- Small: full JDBC migrate ---
    if not small:
        print("No small table found")
        return 1
    print(f"\n=== SMALL migrate (JDBC): {small.fqn} ===")
    report = run_db2_bcp_migration(
        {
            **cfg,
            "dest_auth": "windows",
            "tables": [small.fqn],
            "create_missing": True,
            "truncate_dest": True,
            "verify_after_copy": True,
            "batch_size": 50000,
            "parallel_tables": 1,
            "keep_bcp_files": False,
            "table_row_hints": {small.fqn: small.src_rows},
            "jdbc_max_rows": DEFAULT_JDBC_MAX_ROWS,
        },
        print,
    )
    print("SMALL RESULT", report.ok, report.tables[0].message if report.tables else "")
    if not report.ok:
        return 2

    # --- Large: smoke EXPORT of first 50k rows via ADMIN_CMD, then BCP ---
    if not large:
        print("\nNo large CARD table — skip bulk EXPORT smoke")
        return 0

    print(f"\n=== LARGE EXPORT smoke (FETCH FIRST 50000): {large.fqn} ===")
    work = Path(cfg["work_dir"]) / "db2_smoke_large"
    work.mkdir(parents=True, exist_ok=True)
    smoke_table = f"dbo.SMOKE_{large.name}"[:100]
    conn = connect_db2(src)
    try:
        cols = _fetch_db2_columns(conn.cursor(), large.schema, large.name)
    finally:
        _safe_close_db2(conn)
    write_order = [c["name"] for c in cols if not c.get("is_identity") and (c.get("type_name") or "").upper() != "BLOB"]
    write_order = write_order[:40]  # keep smoke narrow
    ensure_dest_table_from_db2(
        dest,
        [c for c in cols if c["name"] in write_order],
        smoke_table,
        truncate_if_exists=True,
        log=print,
    )
    # Build ADMIN_CMD-friendly select with row limit
    col_sql = ", ".join(f'"{c}"' for c in write_order)
    select_sql = f'SELECT {col_sql} FROM "{large.schema}"."{large.name}" FETCH FIRST 50000 ROWS ONLY'
    out_file = work / f"{large.name}_smoke.del"
    if out_file.exists():
        out_file.unlink()
    path_sql = str(out_file).replace("'", "''")
    cmd = f"EXPORT TO '{path_sql}' OF DEL MODIFIED BY coldel0x7C nochardel {select_sql}"
    print("ADMIN_CMD:", cmd[:180], "...")
    conn = connect_db2(src)
    try:
        cur = conn.cursor()
        cur.execute("CALL SYSPROC.ADMIN_CMD(?)", [cmd])
        try:
            while True:
                try:
                    cur.fetchall()
                except Exception:
                    pass
                if not cur.nextset():
                    break
        except Exception:
            pass
    finally:
        _safe_close_db2(conn)

    # Wait for UNC file
    import time

    for i in range(60):
        if out_file.exists() and out_file.stat().st_size > 0:
            break
        time.sleep(1)
    if not out_file.exists():
        print("BULK EXPORT SMOKE FAILED: file not visible on staging (ADMIN_CMD path / UNC ACL)")
        print("Small JDBC path still PASSED.")
        return 3

    # Count lines + BCP in
    raw = out_file.read_bytes()
    rows = raw.count(b"\n")
    print(f"[OK] EXPORT file {out_file.name} size={out_file.stat().st_size:,} lines≈{rows:,}")
    dest_cols = list_dest_columns(dest, smoke_table)
    by_lower = {c["name"].lower(): c for c in dest_cols}
    mapped = [by_lower[c.lower()]["name"] for c in write_order if c.lower() in by_lower]
    fmt = work / f"{large.name}_smoke.fmt"
    write_bcp_format_file(fmt, dest_cols=dest_cols, write_order=mapped)
    bcp = find_bcp_exe()
    bcp_in_pipe_file(
        bcp_exe=bcp,
        dest_role=dest,
        table=smoke_table,
        data_file=out_file,
        format_file=fmt,
        batch_size=50000,
        log=print,
    )
    print(f"BULK EXPORT SMOKE RESULT ok table={smoke_table} exported_rows≈{rows:,}")
    print("\nALL SMOKES DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
