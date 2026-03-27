# Author: Sa-tish Chauhan

"""Main schema backup functionality."""

import json
import sys
import time
from pathlib import Path

import pyodbc

from ..utils.database import build_conn_str, pick_sql_driver, connect_to_database
from ..utils.logging import setup_logger
from ..utils.paths import safe_name, safe_table_filename, short_slug, utc_iso, utc_ts_compact, win_safe_path
from ..utils.sql import sql_header
from .exporters import (
    build_create_table_sql,
    export_check_constraints,
    export_default_constraints,
    export_extended_properties,
    export_foreign_keys,
    export_indexes,
    export_primary_keys,
    export_sequences,
    export_synonyms,
    export_triggers,
    fetch_columns,
    fetch_objects,
    fetch_primary_key,
    fetch_row_count,
    fetch_tables,
    object_definition,
    wrap_create_or_alter,
)


def setup_run_folders(backup_root: Path, server: str, db: str, run_id: str):
    """Setup folder structure for backup run"""
    # Use SHORT tags to avoid Windows path issues
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
        win_safe_path(paths[k]).mkdir(parents=True, exist_ok=True)

    paths.update(
        {
            "summary_file": paths["meta_dir"] / "run_summary.json",
            "tables_all_file": paths["schema_dir"] / "01_tables_all.sql",
            "sequences_file": paths["prog_dir"] / "sequences.sql",
            "synonyms_file": paths["prog_dir"] / "synonyms.sql",
            "views_file": paths["prog_dir"] / "views.sql",
            "procedures_file": paths["prog_dir"] / "procedures.sql",
            "functions_file": paths["prog_dir"] / "functions.sql",
            "triggers_file": paths["prog_dir"] / "triggers.sql",
            "foreign_keys_file": paths["cx_dir"] / "foreign_keys.sql",
            "checks_file": paths["cx_dir"] / "check_constraints.sql",
            "defaults_file": paths["cx_dir"] / "default_constraints.sql",
            "indexes_file": paths["cx_dir"] / "indexes.sql",
            "primary_keys_file": paths["cx_dir"] / "primary_keys.sql",
            "extended_properties_file": paths["cx_dir"] / "extended_properties.sql",
        }
    )
    return paths


def run_backup(cfg: dict):
    """Run schema backup with provided configuration"""
    run_id = utc_ts_compact()
    backup_root = Path(cfg["backup_root"])
    paths = setup_run_folders(backup_root, cfg["server"], cfg["database"], run_id)
    run_root_resolved = str(paths["run_root"].resolve())

    log_file = paths["logs_dir"] / f"run_{run_id}.log"
    logger = setup_logger(log_file, "schema_backup")

    start = time.time()

    summary = {
        "run_id": run_id,
        "run_root": run_root_resolved,
        "backup_path": run_root_resolved,
        "server": cfg["server"],
        "database": cfg["database"],
        "auth": cfg["auth"],
        "user": cfg["user"],
        "driver": None,
        "python_exe": sys.executable,
        "started_utc": utc_iso(),
        "ended_utc": None,
        "duration_seconds": None,
        "status": "started",
        "errors": [],
        "counts": {
            "tables": 0,
            "views": 0,
            "procedures": 0,
            "functions": 0,
        },
        "validation": {
            "foreign_keys_skipped": 0,
            "indexes_skipped": 0,
            "tables_skipped": 0,
            "indexes_with_empty_filter": 0,
        },
        "files": {k: str(v) for k, v in paths.items() if isinstance(v, Path) and v.suffix},
        "tables": [],
        "warnings": [],
        "effective_config": {k: ("***" if "password" in k and cfg.get(k) else cfg.get(k)) for k in cfg},
    }

    logger.info("Starting full schema backup run: %s", run_id)
    logger.info("Target server: %s", cfg["server"])
    logger.info("Target database: %s", cfg["database"])
    logger.info("Auth type: %s", cfg["auth"])
    logger.info("Auth user: %s", cfg["user"])
    logger.info("Python exe: %s", sys.executable)
    logger.info("Output root: %s", str(paths["run_root"].resolve()))
    logger.info("Log file: %s", str(log_file.resolve()))

    # Default True: migration-friendly DDL (heaps + primary_keys.sql / default_constraints for post-data restore).
    raw_table_ddl = bool(cfg.get("raw_table_ddl", True))
    if raw_table_ddl:
        logger.info(
            "raw_table_ddl=True (default): CREATE TABLE omits inline PK and column DEFAULTs; "
            "primary_keys.sql is emitted for restore after data load. Set raw_table_ddl=False for inline PK/defaults."
        )
    else:
        logger.info("raw_table_ddl=False: inline PRIMARY KEY and column DEFAULTs in CREATE TABLE.")

    try:
        driver = pick_sql_driver(logger)
        summary["driver"] = driver

        password = cfg.get("password")
        
        logger.info("Connecting (auth=%s)...", cfg["auth"])
        # Use connect_to_database which handles MSAL token caching automatically
        with connect_to_database(
            server=cfg["server"],
            db=cfg["database"],
            user=cfg["user"],
            driver=driver,
            auth=cfg["auth"],
            password=password,
            timeout=30,
            logger=logger,
        ) as conn:
            conn.timeout = 0
            cur = conn.cursor()

            cur.execute("SELECT DB_NAME(), SUSER_SNAME(), GETDATE();")
            db_name, login_name, server_time = cur.fetchone()
            logger.info("Connected. DB=%s Login=%s ServerTime=%s", db_name, login_name, server_time)

            # 1) TABLES
            tables = fetch_tables(cur)
            summary["counts"]["tables"] = len(tables)
            logger.info("Found %d user tables.", len(tables))

            if tables:
                sample = [f"{t.schema_name}.{t.table_name}" for t in tables[: cfg["log_table_sample"]]]
                logger.info(
                    "Sample tables: %s%s",
                    ", ".join(sample),
                    " ..." if len(tables) > cfg["log_table_sample"] else "",
                )

            tables_all = [sql_header("01 - TABLES", cfg["server"], cfg["database"], run_id)]
            for idx, (schema_name, table_name) in enumerate(tables, start=1):
                t0 = time.time()
                fqn = f"{schema_name}.{table_name}"
                logger.info("[%d/%d] Tables: %s", idx, len(tables), fqn)

                cols = fetch_columns(cur, schema_name, table_name)
                pk = fetch_primary_key(cur, schema_name, table_name)
                row_count = fetch_row_count(cur, schema_name, table_name)

                table_sql, table_warning = build_create_table_sql(
                    schema_name,
                    table_name,
                    cols,
                    pk,
                    include_primary_key=not raw_table_ddl,
                    include_inline_defaults=not raw_table_ddl,
                )
                
                # Log warning if any
                if table_warning:
                    logger.warning("Table %s: %s", fqn, table_warning)
                    summary["warnings"].append(table_warning)
                
                # Skip table if SQL is empty (validation failed)
                if not table_sql.strip():
                    logger.error("Table %s: Skipped due to validation failure", fqn)
                    summary["warnings"].append(f"Table {fqn}: Skipped - validation failed")
                    summary["validation"]["tables_skipped"] += 1
                    continue
                
                tables_all.append(table_sql)

                per_file = paths["tables_dir"] / safe_table_filename(schema_name, table_name)
                win_safe_path(per_file).write_text(table_sql, encoding="utf-8")

                summary["tables"].append(
                    {
                        "schema": schema_name,
                        "table": table_name,
                        "row_count_estimate": row_count,
                        "columns": len(cols),
                        "has_primary_key": bool(pk),
                        "file": str(per_file),
                        "duration_ms": int((time.time() - t0) * 1000),
                        "warning": table_warning,
                    }
                )

            paths["tables_all_file"].write_text("\n".join(tables_all), encoding="utf-8")
            logger.info("Wrote tables script: %s", str(paths["tables_all_file"].resolve()))

            if raw_table_ddl:
                pk_sql = sql_header(
                    "03 - PRIMARY KEYS (run after data load)",
                    cfg["server"],
                    cfg["database"],
                    run_id,
                ) + export_primary_keys(cur)
                paths["primary_keys_file"].write_text(pk_sql, encoding="utf-8")
                logger.info("Wrote primary keys script: %s", str(paths["primary_keys_file"].resolve()))

            # 2) SEQUENCES AND SYNONYMS (run before tables/views that use them)
            logger.info("Exporting sequences...")
            seq_sql = sql_header("01 - SEQUENCES (run before tables)", cfg["server"], cfg["database"], run_id) + export_sequences(cur)
            if seq_sql.strip():
                paths["sequences_file"].write_text(seq_sql, encoding="utf-8")
                logger.info("Sequences exported: %s", str(paths["sequences_file"].resolve()))
            else:
                logger.info("No sequences found.")

            logger.info("Exporting synonyms...")
            syn_sql = sql_header("01 - SYNONYMS (run before objects that use them)", cfg["server"], cfg["database"], run_id) + export_synonyms(cur)
            if syn_sql.strip():
                paths["synonyms_file"].write_text(syn_sql, encoding="utf-8")
                logger.info("Synonyms exported: %s", str(paths["synonyms_file"].resolve()))
            else:
                logger.info("No synonyms found.")

            # 3) PROGRAMMABLES
            views = fetch_objects(cur, "V")
            summary["counts"]["views"] = len(views)
            logger.info("Found %d views.", len(views))
            view_out = [sql_header("02 - VIEWS (run after tables + data)", cfg["server"], cfg["database"], run_id)]
            for (s, n, _t, oid) in views:
                defn = object_definition(cur, oid)
                view_out.append(wrap_create_or_alter(s, n, defn, "VIEW"))
                if defn is None:
                    summary["warnings"].append(f"View {s}.{n} definition not available (maybe encrypted).")
            paths["views_file"].write_text("\n".join(view_out), encoding="utf-8")

            procs = fetch_objects(cur, "P")
            summary["counts"]["procedures"] = len(procs)
            logger.info("Found %d stored procedures.", len(procs))
            proc_out = [sql_header("02 - STORED PROCEDURES (run after tables + data)", cfg["server"], cfg["database"], run_id)]
            for (s, n, _t, oid) in procs:
                defn = object_definition(cur, oid)
                proc_out.append(wrap_create_or_alter(s, n, defn, "PROC"))
                if defn is None:
                    summary["warnings"].append(f"Proc {s}.{n} definition not available (maybe encrypted).")
            paths["procedures_file"].write_text("\n".join(proc_out), encoding="utf-8")

            funcs = fetch_objects(cur, "FN,TF,IF")
            summary["counts"]["functions"] = len(funcs)
            logger.info("Found %d functions.", len(funcs))
            func_out = [sql_header("02 - FUNCTIONS (run after tables + data)", cfg["server"], cfg["database"], run_id)]
            for (s, n, _t, oid) in funcs:
                defn = object_definition(cur, oid)
                func_out.append(wrap_create_or_alter(s, n, defn, "FUNCTION"))
                if defn is None:
                    summary["warnings"].append(f"Function {s}.{n} definition not available (maybe encrypted).")
            paths["functions_file"].write_text("\n".join(func_out), encoding="utf-8")

            # TRIGGERS (run after tables are created)
            logger.info("Exporting triggers...")
            trig_sql, trig_warnings = export_triggers(cur, logger)
            if trig_warnings:
                summary["warnings"].extend(trig_warnings)
            trig_sql = sql_header("02 - TRIGGERS (run after tables + data)", cfg["server"], cfg["database"], run_id) + trig_sql
            paths["triggers_file"].write_text(trig_sql, encoding="utf-8")
            logger.info("Triggers exported. Warnings: %d", len(trig_warnings))

            # 3) CONSTRAINTS + INDEXES
            logger.info("Exporting foreign keys...")
            fk_sql, fk_warnings = export_foreign_keys(cur, logger)
            if fk_warnings:
                summary["warnings"].extend(fk_warnings)
                # Count skipped foreign keys
                summary["validation"]["foreign_keys_skipped"] = len([w for w in fk_warnings if "skipped" in w.lower()])
            fk_sql = sql_header("03 - FOREIGN KEYS (run after data load)", cfg["server"], cfg["database"], run_id) + fk_sql
            paths["foreign_keys_file"].write_text(fk_sql, encoding="utf-8")
            logger.info("Foreign keys exported. Warnings: %d, Skipped: %d", len(fk_warnings), summary["validation"]["foreign_keys_skipped"])

            logger.info("Exporting check constraints...")
            chk_sql = sql_header("03 - CHECK CONSTRAINTS (run after data load)", cfg["server"], cfg["database"], run_id) + export_check_constraints(cur)
            paths["checks_file"].write_text(chk_sql, encoding="utf-8")

            if cfg["export_defaults_separately"]:
                logger.info("Exporting default constraints...")
                df_sql = sql_header("03 - DEFAULT CONSTRAINTS (optional)", cfg["server"], cfg["database"], run_id) + export_default_constraints(cur)
                paths["defaults_file"].write_text(df_sql, encoding="utf-8")

            logger.info("Exporting indexes...")
            ix_sql, ix_warnings = export_indexes(cur, logger)
            if ix_warnings:
                summary["warnings"].extend(ix_warnings)
                # Count skipped indexes and empty filters
                summary["validation"]["indexes_skipped"] = len([w for w in ix_warnings if "skipped" in w.lower()])
                summary["validation"]["indexes_with_empty_filter"] = len([w for w in ix_warnings if "filter_definition is empty" in w.lower()])
            ix_sql = sql_header("03 - INDEXES (run after data load)", cfg["server"], cfg["database"], run_id) + ix_sql
            paths["indexes_file"].write_text(ix_sql, encoding="utf-8")
            logger.info("Indexes exported. Warnings: %d, Skipped: %d, Empty filters: %d", 
                       len(ix_warnings), summary["validation"]["indexes_skipped"], summary["validation"]["indexes_with_empty_filter"])

            # 4) EXTENDED PROPERTIES (Comments/Metadata)
            logger.info("Exporting extended properties (MS_Description, comments)...")
            ep_sql = sql_header("04 - EXTENDED PROPERTIES (run after all objects are created)", cfg["server"], cfg["database"], run_id) + export_extended_properties(cur, logger)
            if ep_sql.strip():
                paths["extended_properties_file"].write_text(ep_sql, encoding="utf-8")
                logger.info("Extended properties exported: %s", str(paths["extended_properties_file"].resolve()))
            else:
                logger.info("No extended properties found.")

        summary["status"] = "success"

    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Backup failed: %s", msg)
        summary["status"] = "failed"
        summary["errors"].append(msg)

    finally:
        summary["ended_utc"] = utc_iso()
        summary["duration_seconds"] = round(time.time() - start, 3)
        # Run folder path (callers such as ADF Migration and restore expect this)
        run_root_str = str(paths["run_root"].resolve())
        summary["run_root"] = run_root_str
        summary["backup_path"] = run_root_str
        try:
            paths["summary_file"].write_text(json.dumps(summary, indent=2), encoding="utf-8")
        except (TypeError, ValueError) as ser_exc:
            logger.warning("Could not serialize run summary JSON: %s", ser_exc)

        logger.info("Run status: %s", summary["status"])
        logger.info("Duration: %s seconds", summary["duration_seconds"])
        logger.info("Summary JSON: %s", str(paths["summary_file"].resolve()))
        logger.info("Backup root: %s", str(paths["run_root"].resolve()))
        
        # Log validation summary
        validation = summary.get("validation", {})
        if any(validation.values()):
            logger.info("Validation Summary:")
            if validation.get("tables_skipped", 0) > 0:
                logger.info("  Tables skipped: %d", validation["tables_skipped"])
            if validation.get("foreign_keys_skipped", 0) > 0:
                logger.info("  Foreign keys skipped: %d", validation["foreign_keys_skipped"])
            if validation.get("indexes_skipped", 0) > 0:
                logger.info("  Indexes skipped: %d", validation["indexes_skipped"])
            if validation.get("indexes_with_empty_filter", 0) > 0:
                logger.info("  Indexes with empty filter (WHERE clause omitted): %d", validation["indexes_with_empty_filter"])
        
        if summary["warnings"]:
            logger.info("Warnings: %d (see run_summary.json)", len(summary["warnings"]))

    return summary

