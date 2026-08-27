#!/usr/bin/env python
"""End-to-end test of BCP migration engine (Windows auth, demo DBs)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_APP = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_APP))

from src.migration.bcp_migration import enrich_with_dest, list_tables, run_bcp_migration

logging.basicConfig(level=logging.INFO, format="%(message)s")


def main() -> int:
    server = r"gpitd-shir01.us.pressganey.com\i2022"
    src_db = "FUSION_ps_fus_db21q"
    dest_db = "testdb_bcp"
    tables = ["dbo.TOM_TEST", "dbo.AMT_BCP_SMOKE"]  # second may be only on testsatish

    src = {"server": server, "db": src_db, "auth": "windows", "user": "", "password": None}
    dest = {"server": server, "db": dest_db, "auth": "windows", "user": "", "password": None}

    print("Listing source tables…")
    all_tables = list_tables(src)
    print(f"  source table count: {len(all_tables)}")
    enriched = enrich_with_dest(all_tables[:5], dest)
    for t in enriched:
        print(f"  sample: {t.fqn} src={t.src_rows} dest_exists={t.dest_exists} dest_rows={t.dest_rows}")

    # Prefer tables that exist on source
    available = {t.fqn.lower() for t in all_tables}
    selected = [t for t in tables if t.lower() in available]
    if "dbo.tom_test" not in available:
        # pick two small tables with rows
        selected = [
            t.fqn
            for t in sorted(all_tables, key=lambda x: x.src_rows)[:2]
            if t.src_rows >= 0
        ][:2]
    if not selected:
        selected = [all_tables[0].fqn]

    # Also migrate smoke from testsatish if present there
    smoke_src = {"server": server, "db": "testsatish", "auth": "windows", "user": "", "password": None}
    try:
        smoke_tables = list_tables(smoke_src)
        if any(t.fqn.lower() == "dbo.amt_bcp_smoke" for t in smoke_tables):
            print("Migrating dbo.AMT_BCP_SMOKE from testsatish…")
            report_smoke = run_bcp_migration(
                {
                    "src_server": server,
                    "src_db": "testsatish",
                    "src_auth": "windows",
                    "dest_server": server,
                    "dest_db": dest_db,
                    "dest_auth": "windows",
                    "tables": ["dbo.AMT_BCP_SMOKE"],
                    "create_missing": True,
                    "truncate_dest": True,
                    "verify_after_copy": True,
                    "parallel_tables": 1,
                    "max_retries": 1,
                    "keep_bcp_files": False,
                },
                print,
            )
            print("SMOKE:", "OK" if report_smoke.ok else "FAIL", report_smoke.to_dict())
            if not report_smoke.ok:
                return 1
    except Exception as ex:
        print("Smoke optional path skipped:", ex)

    print(f"Migrating from {src_db}: {selected}")
    report = run_bcp_migration(
        {
            "src_server": server,
            "src_db": src_db,
            "src_auth": "windows",
            "dest_server": server,
            "dest_db": dest_db,
            "dest_auth": "windows",
            "tables": selected,
            "create_missing": True,
            "truncate_dest": True,
            "verify_after_copy": True,
            "parallel_tables": 2,
            "max_retries": 1,
            "keep_bcp_files": False,
            "exclude": "tmp_*",
        },
        print,
    )
    print("REPORT OK:", report.ok)
    for tr in report.tables:
        print(f"  {tr.status}: {tr.table} src={tr.src_rows} dest={tr.dest_rows} {tr.message or tr.error}")
    return 0 if report.ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
