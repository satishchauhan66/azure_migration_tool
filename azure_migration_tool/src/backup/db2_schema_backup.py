# Author: Satish Chauhan

"""
DB2 schema backup (read-only).
Connects to DB2 via JDBC, reads metadata from SYSCAT, and exports DDL to files.
Works with read-only users who have SELECT on the catalog.
"""

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..utils.logging import setup_logger
from ..utils.paths import safe_name, short_slug, utc_iso, utc_ts_compact
from .db2_backup_exporters import (
    build_db2_create_table_sql,
    export_db2_check_constraints,
    export_db2_foreign_keys,
    export_db2_indexes,
    export_db2_sequences,
    export_db2_views,
    export_db2_procedures,
    export_db2_functions,
    export_db2_triggers,
)


def _db2_ident(name: str) -> str:
    """Quote DB2 identifier (double quotes, escape double quote by doubling)."""
    if not name:
        return '""'
    return '"' + str(name).replace('"', '""') + '"'


def setup_run_folders(backup_root: Path, server: str, db: str, run_id: str) -> Dict[str, Any]:
    """Setup folder structure for DB2 backup run (same layout as SQL Server backup)."""
    server_tag = short_slug(server)
    db_tag = short_slug(db)
    base = backup_root / server_tag / db_tag / "runs" / run_id
    paths = {
        "run_root": base,
        "logs_dir": base / "logs",
        "meta_dir": base / "meta",
        "schema_dir": base / "schema",
        "tables_dir": base / "schema" / "01_tables",
        "prog_dir": base / "schema" / "02_programmables",
        "cx_dir": base / "schema" / "03_constraints_indexes",
    }
    for k in ["run_root", "logs_dir", "meta_dir", "schema_dir", "tables_dir", "prog_dir", "cx_dir"]:
        paths[k].mkdir(parents=True, exist_ok=True)
    paths.update({
        "summary_file": paths["meta_dir"] / "run_summary.json",
        "tables_all_file": paths["schema_dir"] / "01_tables_all.sql",
        "sequences_file": paths["prog_dir"] / "sequences.sql",
        "views_file": paths["prog_dir"] / "views.sql",
        "procedures_file": paths["prog_dir"] / "procedures.sql",
        "functions_file": paths["prog_dir"] / "functions.sql",
        "triggers_file": paths["prog_dir"] / "triggers.sql",
        "foreign_keys_file": paths["cx_dir"] / "foreign_keys.sql",
        "checks_file": paths["cx_dir"] / "check_constraints.sql",
        "indexes_file": paths["cx_dir"] / "indexes.sql",
    })
    return paths


def _sql_header(title: str, server: str, db: str, run_id: str) -> str:
    """Generate SQL script header comment for DB2 (no SET NOCOUNT; no GO)."""
    return "\n".join([
        f"-- {title}",
        f"-- Server: {server}",
        f"-- Database: {db}",
        f"-- Run: {run_id}",
        f"-- Generated (UTC): {utc_iso()}",
        "",
    ])


def run_db2_backup(cfg: dict) -> dict:
    """
    Run DB2 schema backup (read-only). Uses SYSCAT catalog only.
    cfg: host, port, database, user, password, backup_root, schema (optional), log_table_sample (optional).
    """
    run_id = utc_ts_compact()
    backup_root = Path(cfg["backup_root"])
    host = cfg.get("host") or cfg.get("server", "")
    port = int(cfg.get("port", 50000))
    database = cfg.get("database", "")
    schema_filter = (cfg.get("schema") or "").strip() or None

    paths = setup_run_folders(backup_root, host, database, run_id)
    log_file = paths["logs_dir"] / f"run_{run_id}.log"
    logger = setup_logger(log_file, "db2_schema_backup")

    start = time.time()
    summary = {
        "run_id": run_id,
        "server": host,
        "database": database,
        "source_type": "db2",
        "schema_filter": schema_filter,
        "user": cfg.get("user", ""),
        "started_utc": utc_iso(),
        "ended_utc": None,
        "duration_seconds": None,
        "status": "started",
        "errors": [],
        "counts": {"tables": 0, "views": 0, "procedures": 0, "functions": 0, "sequences": 0},
        "warnings": [],
        "files": {k: str(v) for k, v in paths.items() if isinstance(v, Path) and getattr(v, "suffix", "") == ".sql"},
        "tables": [],
    }

    logger.info("Starting DB2 schema backup run: %s", run_id)
    logger.info("Target: %s:%s / %s (schema=%s)", host, port, database, schema_filter or "all")

    try:
        from ..utils.database import connect_to_db2_jdbc
        with connect_to_db2_jdbc(
            host=host,
            port=port,
            database=database,
            user=cfg.get("user", ""),
            password=cfg.get("password") or "",
            timeout=30,
            logger=logger,
        ) as conn:
            conn.timeout = 0
            cur = conn.cursor()

            # Verify connection
            cur.execute("SELECT CURRENT SERVER, CURRENT USER, CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            row = cur.fetchone()
            logger.info("Connected. Server=%s User=%s Time=%s", *(str(x) for x in (row or (None, None, None))))

            from gui.utils.db2_schema import (
                fetch_db2_tables,
                fetch_db2_columns,
                fetch_db2_primary_keys,
                fetch_db2_views,
                fetch_db2_procedures,
                fetch_db2_functions,
                fetch_db2_triggers,
            )
            from .db2_backup_exporters import get_pk_columns_for_table

            # Fetch PKs once for all tables
            all_pks = fetch_db2_primary_keys(cur, schema_filter)

            # 1) Tables
            tables = fetch_db2_tables(cur, schema_filter)
            summary["counts"]["tables"] = len(tables)
            logger.info("Found %d user tables.", len(tables))

            log_table_sample = int(cfg.get("log_table_sample", 20))
            if tables:
                sample = [f"{t.schema_name}.{t.table_name}" for t in tables[:log_table_sample]]
                logger.info("Sample tables: %s%s", ", ".join(sample), " ..." if len(tables) > log_table_sample else "")

            tables_all = [_sql_header("01 - TABLES", host, database, run_id)]
            for idx, t in enumerate(tables, start=1):
                t0 = time.time()
                fqn = f"{t.schema_name}.{t.table_name}"
                logger.info("[%d/%d] Table: %s", idx, len(tables), fqn)
                cols = fetch_db2_columns(cur, t.schema_name, t.table_name)
                pk_for_table = [p for p in all_pks if p["schema_name"] == t.schema_name and p["table_name"] == t.table_name]
                pk_col_names = get_pk_columns_for_table(cur, t.schema_name, t.table_name, pk_for_table) if pk_for_table else []

                table_sql, warning = build_db2_create_table_sql(t.schema_name, t.table_name, cols, pk_col_names)
                if warning:
                    summary["warnings"].append(warning)
                if not table_sql.strip():
                    logger.warning("Table %s: skipped (no DDL). %s", fqn, warning or "")
                    continue
                tables_all.append(table_sql)
                per_file = paths["tables_dir"] / f"{safe_name(t.schema_name)}.{safe_name(t.table_name)}.sql"
                per_file.write_text(table_sql, encoding="utf-8")
                summary["tables"].append({
                    "schema": t.schema_name,
                    "table": t.table_name,
                    "columns": len(cols),
                    "file": str(per_file),
                    "duration_ms": int((time.time() - t0) * 1000),
                })

            path_tables_all = paths["tables_all_file"]
            path_tables_all.write_text("\n".join(tables_all), encoding="utf-8")
            logger.info("Wrote tables script: %s", str(path_tables_all.resolve()))

            # 2) Sequences
            logger.info("Exporting sequences...")
            seq_sql = _sql_header("01 - SEQUENCES", host, database, run_id) + export_db2_sequences(cur, schema_filter, logger)
            if seq_sql.strip():
                paths["sequences_file"].write_text(seq_sql, encoding="utf-8")
                summary["counts"]["sequences"] = len([x for x in seq_sql.splitlines() if "CREATE SEQUENCE" in x])
            else:
                logger.info("No sequences found.")

            # 3) Views
            views = fetch_db2_views(cur, schema_filter)
            summary["counts"]["views"] = len(views)
            view_sql = _sql_header("02 - VIEWS", host, database, run_id) + export_db2_views(cur, views, logger)
            paths["views_file"].write_text(view_sql, encoding="utf-8")

            # 4) Procedures
            procs = fetch_db2_procedures(cur, schema_filter)
            summary["counts"]["procedures"] = len(procs)
            proc_sql = _sql_header("02 - STORED PROCEDURES", host, database, run_id) + export_db2_procedures(cur, procs, logger)
            paths["procedures_file"].write_text(proc_sql, encoding="utf-8")

            # 5) Functions
            funcs = fetch_db2_functions(cur, schema_filter)
            summary["counts"]["functions"] = len(funcs)
            func_sql = _sql_header("02 - FUNCTIONS", host, database, run_id) + export_db2_functions(cur, funcs, logger)
            paths["functions_file"].write_text(func_sql, encoding="utf-8")

            # 6) Triggers
            logger.info("Exporting triggers...")
            trig_sql = _sql_header("02 - TRIGGERS", host, database, run_id) + export_db2_triggers(cur, schema_filter, logger)
            paths["triggers_file"].write_text(trig_sql, encoding="utf-8")

            # 7) Foreign keys
            logger.info("Exporting foreign keys...")
            fk_sql = _sql_header("03 - FOREIGN KEYS", host, database, run_id) + export_db2_foreign_keys(cur, schema_filter, logger)
            paths["foreign_keys_file"].write_text(fk_sql, encoding="utf-8")

            # 8) Check constraints
            logger.info("Exporting check constraints...")
            chk_sql = _sql_header("03 - CHECK CONSTRAINTS", host, database, run_id) + export_db2_check_constraints(cur, schema_filter)
            paths["checks_file"].write_text(chk_sql, encoding="utf-8")

            # 9) Indexes (non-PK)
            logger.info("Exporting indexes...")
            ix_sql = _sql_header("03 - INDEXES", host, database, run_id) + export_db2_indexes(cur, schema_filter, logger)
            paths["indexes_file"].write_text(ix_sql, encoding="utf-8")

        summary["status"] = "success"
    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("DB2 backup failed: %s", msg)
        summary["status"] = "failed"
        summary["errors"].append(msg)

    finally:
        summary["ended_utc"] = utc_iso()
        summary["duration_seconds"] = round(time.time() - start, 3)
        paths["summary_file"].write_text(json.dumps(summary, indent=2), encoding="utf-8")
        logger.info("Run status: %s", summary["status"])
        logger.info("Duration: %s seconds", summary["duration_seconds"])

    return summary
