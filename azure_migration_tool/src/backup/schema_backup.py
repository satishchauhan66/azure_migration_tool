# Author: Sa-tish Chauhan

"""Main schema backup functionality."""

import json
import sys
import time
from pathlib import Path

import pyodbc

from ..utils.azure_compat import detect_azure_sql_target, is_azure_sql_server
from ..utils.database import connect_to_database, pick_sql_driver, register_pyodbc_backup_converters
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
    export_unique_constraints,
    export_clr_modules,
    fetch_columns,
    fetch_objects,
    fetch_primary_key,
    fetch_row_count,
    fetch_tables,
    object_definition,
    object_module_session_options,
    SUPPLEMENTAL_PROCEDURE_NAME_LIKE,
    SUPPLEMENTAL_PROCEDURE_NAMES,
    format_module_sql_single,
    qident,
    wrap_create_or_alter,
)
from .export_inventory import build_gap_report
from .remaining_exporters import (
    export_always_encrypted,
    export_assemblies_full,
    export_clr_types,
    export_column_collation,
    export_column_permissions,
    export_data_masking,
    export_graph,
    export_index_options,
    export_memory_optimized_filegroup,
    export_replication,
    export_schema_authorization,
    export_sequence_current_values,
    export_specialized_indexes,
    export_table_storage,
)
from .full_exporters import (
    export_cdc,
    export_change_tracking,
    export_cryptographic_objects,
    export_database_audit_specifications,
    export_database_credentials,
    export_database_diagrams,
    export_database_options,
    export_external_tables,
    export_filegroups,
    export_fulltext,
    export_module_inventory,
    export_role_memberships,
    export_server_logins,
    export_service_broker,
    export_statistics,
    export_table_options,
    export_xml_schema_collections,
)
from .mirror_exporters import (
    export_database_permissions,
    export_database_principals,
    export_ddl_triggers,
    export_external_resources,
    export_legacy_rules_and_defaults,
    export_partitioning,
    export_plan_guides,
    export_schemas,
    export_security_policies,
    export_user_defined_types,
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
        "foundation_dir": base / "schema" / "00_foundation",
        "tables_dir": base / "schema" / "01_tables",
        "prog_dir": base / "schema" / "02_programmables",
        "procedures_dir": base / "schema" / "02_programmables" / "procedures",
        "cx_dir": base / "schema" / "03_constraints_indexes",
        "security_dir": base / "schema" / "04_security",
    }

    for k in [
        "run_root",
        "logs_dir",
        "meta_dir",
        "schema_dir",
        "foundation_dir",
        "tables_dir",
        "prog_dir",
        "procedures_dir",
        "cx_dir",
        "security_dir",
    ]:
        win_safe_path(paths[k]).mkdir(parents=True, exist_ok=True)

    paths.update(
        {
            "summary_file": paths["meta_dir"] / "run_summary.json",
            "restore_manifest_file": paths["meta_dir"] / "restore_order.json",
            "schemas_file": paths["foundation_dir"] / "schemas.sql",
            "user_defined_types_file": paths["foundation_dir"] / "user_defined_types.sql",
            "external_resources_file": paths["foundation_dir"] / "external_resources.sql",
            "partitioning_file": paths["foundation_dir"] / "partitioning.sql",
            "filegroups_file": paths["foundation_dir"] / "filegroups.sql",
            "database_credentials_file": paths["foundation_dir"] / "database_credentials.sql",
            "database_options_file": paths["foundation_dir"] / "database_options.sql",
            "xml_schema_collections_file": paths["foundation_dir"] / "xml_schema_collections.sql",
            "assemblies_file": paths["foundation_dir"] / "assemblies.sql",
            "server_logins_file": paths["meta_dir"] / "server_logins.sql",
            "database_diagrams_file": paths["meta_dir"] / "database_diagrams.sql",
            "encrypted_modules_file": paths["meta_dir"] / "encrypted_modules.sql",
            "tables_all_file": paths["schema_dir"] / "01_tables_all.sql",
            "sequences_file": paths["prog_dir"] / "sequences.sql",
            "synonyms_file": paths["prog_dir"] / "synonyms.sql",
            "views_file": paths["prog_dir"] / "views.sql",
            "procedures_file": paths["prog_dir"] / "procedures.sql",
            "clr_procedures_file": paths["prog_dir"] / "clr_procedures.sql",
            "functions_file": paths["prog_dir"] / "functions.sql",
            "triggers_file": paths["prog_dir"] / "triggers.sql",
            "ddl_triggers_file": paths["prog_dir"] / "ddl_triggers.sql",
            "security_policies_file": paths["prog_dir"] / "security_policies.sql",
            "plan_guides_file": paths["prog_dir"] / "plan_guides.sql",
            "legacy_rules_defaults_file": paths["prog_dir"] / "legacy_rules_defaults.sql",
            "service_broker_file": paths["prog_dir"] / "service_broker.sql",
            "external_tables_file": paths["prog_dir"] / "external_tables.sql",
            "change_tracking_file": paths["prog_dir"] / "change_tracking.sql",
            "cdc_file": paths["prog_dir"] / "cdc.sql",
            "foreign_keys_file": paths["cx_dir"] / "foreign_keys.sql",
            "checks_file": paths["cx_dir"] / "check_constraints.sql",
            "defaults_file": paths["cx_dir"] / "default_constraints.sql",
            "indexes_file": paths["cx_dir"] / "indexes.sql",
            "primary_keys_file": paths["cx_dir"] / "primary_keys.sql",
            "extended_properties_file": paths["cx_dir"] / "extended_properties.sql",
            "unique_constraints_file": paths["cx_dir"] / "unique_constraints.sql",
            "statistics_file": paths["cx_dir"] / "statistics.sql",
            "fulltext_file": paths["cx_dir"] / "fulltext.sql",
            "table_options_file": paths["cx_dir"] / "table_options.sql",
            "database_principals_file": paths["security_dir"] / "database_principals.sql",
            "permissions_file": paths["security_dir"] / "permissions.sql",
            "role_memberships_file": paths["security_dir"] / "role_memberships.sql",
            "cryptographic_objects_file": paths["security_dir"] / "cryptographic_objects.sql",
            "audit_specifications_file": paths["security_dir"] / "audit_specifications.sql",
            "column_collation_file": paths["cx_dir"] / "column_collation.sql",
            "table_storage_file": paths["cx_dir"] / "table_storage.sql",
            "specialized_indexes_file": paths["cx_dir"] / "specialized_indexes.sql",
            "index_options_file": paths["cx_dir"] / "index_options.sql",
            "always_encrypted_file": paths["security_dir"] / "always_encrypted.sql",
            "data_masking_file": paths["security_dir"] / "data_masking.sql",
            "column_permissions_file": paths["security_dir"] / "column_permissions.sql",
            "schema_authorization_file": paths["security_dir"] / "schema_authorization.sql",
            "replication_file": paths["prog_dir"] / "replication.sql",
            "graph_file": paths["prog_dir"] / "graph.sql",
            "memory_optimized_filegroup_file": paths["foundation_dir"] / "memory_optimized_filegroup.sql",
            "sequence_current_values_file": paths["prog_dir"] / "sequence_current_values.sql",
            "export_gap_report_file": paths["meta_dir"] / "export_gap_report.json",
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
            "schemas": 0,
            "user_defined_types": 0,
            "ddl_triggers": 0,
            "security_policies": 0,
        },
        "mirror_backup": True,
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
            # Backup-only: tolerate ODBC type codes pyodbc cannot map (does not affect other app connections).
            try:
                register_pyodbc_backup_converters(conn)
            except Exception:
                pass
            cur = conn.cursor()

            # Cast to NVARCHAR so ODBC/pyodbc never sees driver-specific datetime (-16 etc.) on column 3
            cur.execute(
                """
                SELECT CAST(DB_NAME() AS NVARCHAR(128)),
                       CAST(SUSER_SNAME() AS NVARCHAR(256)),
                       CONVERT(NVARCHAR(33), SYSUTCDATETIME(), 126);
                """
            )
            db_name, login_name, server_time = cur.fetchone()
            logger.info("Connected. DB=%s Login=%s ServerTime=%s", db_name, login_name, server_time)

            azure_target = cfg.get("azure_target")
            if azure_target is None:
                azure_target = detect_azure_sql_target(cur, cfg["server"])
            else:
                azure_target = bool(azure_target)
            if not azure_target and is_azure_sql_server(cfg["server"]):
                azure_target = True

            exported_files: dict = {}

            def _write_if_content(path_key: str, header_title: str, body: str, *, log_label: str):
                text = (body or "").strip()
                rel = str(paths[path_key].relative_to(paths["run_root"])).replace("\\", "/")
                if not text:
                    logger.info("No %s found.", log_label)
                    exported_files[rel] = False
                    return
                full = sql_header(header_title, cfg["server"], cfg["database"], run_id) + text
                paths[path_key].write_text(full, encoding="utf-8")
                exported_files[rel] = True
                logger.info("Wrote %s: %s", log_label, str(paths[path_key].resolve()))

            # 0) FOUNDATION (schemas, types, external, partitioning — before tables)
            logger.info("Exporting schemas...")
            _write_if_content("schemas_file", "00 - SCHEMAS", export_schemas(cur), log_label="schemas")

            logger.info("Exporting user-defined types...")
            udt_sql, udt_warnings = export_user_defined_types(cur, logger)
            if udt_warnings:
                summary["warnings"].extend(udt_warnings)
            _write_if_content("user_defined_types_file", "00 - USER-DEFINED TYPES", udt_sql, log_label="user-defined types")

            logger.info("Exporting external data sources and file formats...")
            _write_if_content(
                "external_resources_file",
                "00 - EXTERNAL RESOURCES",
                export_external_resources(cur),
                log_label="external resources",
            )

            logger.info("Exporting partition functions and schemes...")
            _write_if_content(
                "partitioning_file",
                "00 - PARTITIONING",
                export_partitioning(cur, logger),
                log_label="partitioning",
            )

            _write_if_content("filegroups_file", "00 - FILEGROUPS", export_filegroups(cur), log_label="filegroups")
            _write_if_content(
                "memory_optimized_filegroup_file",
                "00 - MEMORY-OPTIMIZED FILEGROUP",
                export_memory_optimized_filegroup(cur),
                log_label="memory-optimized filegroup",
            )
            _write_if_content(
                "database_credentials_file",
                "00 - DATABASE CREDENTIALS",
                export_database_credentials(cur),
                log_label="database credentials",
            )
            _write_if_content(
                "database_options_file",
                "00 - DATABASE OPTIONS",
                export_database_options(cur),
                log_label="database options",
            )
            _write_if_content(
                "xml_schema_collections_file",
                "00 - XML SCHEMA COLLECTIONS",
                export_xml_schema_collections(cur),
                log_label="XML schema collections",
            )
            asm_sql, asm_warnings = export_assemblies_full(cur, logger)
            if asm_warnings:
                summary["warnings"].extend(asm_warnings)
            asm_sql = (asm_sql or "") + "\n" + export_clr_types(cur, logger)
            _write_if_content("assemblies_file", "00 - ASSEMBLIES", asm_sql, log_label="assemblies")

            logger.info("Exporting legacy rules and defaults (if any)...")
            _write_if_content(
                "legacy_rules_defaults_file",
                "02 - LEGACY RULES AND DEFAULTS",
                export_legacy_rules_and_defaults(cur),
                log_label="legacy rules/defaults",
            )

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
            exported_files["schema/01_tables_all.sql"] = True
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
                session_opts = object_module_session_options(cur, oid)
                view_out.append(
                    wrap_create_or_alter(s, n, defn, "VIEW", session_options=session_opts)
                )
                if defn is None:
                    summary["warnings"].append(f"View {s}.{n} definition not available (maybe encrypted).")
            paths["views_file"].write_text("\n".join(view_out), encoding="utf-8")

            procs = fetch_objects(
                cur,
                "P",
                include_object_names=SUPPLEMENTAL_PROCEDURE_NAMES,
                include_name_like=SUPPLEMENTAL_PROCEDURE_NAME_LIKE,
            )
            summary["counts"]["procedures"] = len(procs)
            logger.info("Found %d stored procedures.", len(procs))
            proc_header = sql_header(
                "02 - STORED PROCEDURES (run after tables + data)",
                cfg["server"],
                cfg["database"],
                run_id,
            )
            proc_out = [proc_header]
            proc_manifest: List[str] = []
            for (s, n, _t, oid) in procs:
                defn = object_definition(cur, oid)
                session_opts = object_module_session_options(cur, oid)
                single_sql = format_module_sql_single(
                    s, n, defn, "PROC", session_options=session_opts
                )
                per_file = paths["procedures_dir"] / safe_table_filename(s, n)
                win_safe_path(per_file).write_text(single_sql, encoding="utf-8")
                proc_manifest.append(f"procedures/{per_file.name}")
                proc_out.append(
                    wrap_create_or_alter(s, n, defn, "PROC", session_options=session_opts)
                )
                if defn is None:
                    summary["warnings"].append(f"Proc {s}.{n} definition not available (maybe encrypted).")
            proc_text = "\n".join(proc_out)
            for (s, n, _t, oid) in procs:
                marker = f"{qident(s)}.{qident(n)}"
                if marker in proc_text:
                    continue
                defn = object_definition(cur, oid)
                session_opts = object_module_session_options(cur, oid)
                single_sql = format_module_sql_single(
                    s, n, defn, "PROC", session_options=session_opts
                )
                per_file = paths["procedures_dir"] / safe_table_filename(s, n)
                win_safe_path(per_file).write_text(single_sql, encoding="utf-8")
                proc_manifest.append(f"procedures/{per_file.name}")
                proc_out.append(
                    wrap_create_or_alter(s, n, defn, "PROC", session_options=session_opts)
                )
                summary["warnings"].append(f"Proc {s}.{n} was absent from procedures.sql — appended on verification pass.")
                if defn is None:
                    summary["warnings"].append(f"Proc {s}.{n} definition not available (maybe encrypted).")
            proc_out.append(
                "\n".join(
                    [
                        "-- Per-procedure files (one CREATE OR ALTER per file, preferred for restore):",
                        *[f"--   {line}" for line in proc_manifest],
                        "",
                    ]
                )
            )
            paths["procedures_file"].write_text("\n".join(proc_out), encoding="utf-8")
            logger.info(
                "Wrote %d procedure file(s) under %s",
                len(proc_manifest),
                str(paths["procedures_dir"].resolve()),
            )

            _write_if_content(
                "clr_procedures_file",
                "02 - CLR MODULES (inventory only — not restorable on Azure SQL)",
                export_clr_modules(cur, logger),
                log_label="CLR modules",
            )

            funcs = fetch_objects(cur, "FN,TF,IF")
            summary["counts"]["functions"] = len(funcs)
            logger.info("Found %d functions.", len(funcs))
            func_out = [sql_header("02 - FUNCTIONS (run after tables + data)", cfg["server"], cfg["database"], run_id)]
            for (s, n, _t, oid) in funcs:
                defn = object_definition(cur, oid)
                session_opts = object_module_session_options(cur, oid)
                func_out.append(
                    wrap_create_or_alter(s, n, defn, "FUNCTION", session_options=session_opts)
                )
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

            logger.info("Exporting DDL (database) triggers...")
            ddl_sql, ddl_warnings = export_ddl_triggers(cur, logger)
            if ddl_warnings:
                summary["warnings"].extend(ddl_warnings)
            _write_if_content("ddl_triggers_file", "02 - DDL TRIGGERS", ddl_sql, log_label="DDL triggers")

            logger.info("Exporting row-level security policies...")
            rls_sql, rls_warnings = export_security_policies(cur, logger)
            if rls_warnings:
                summary["warnings"].extend(rls_warnings)
            _write_if_content("security_policies_file", "02 - SECURITY POLICIES (RLS)", rls_sql, log_label="security policies")

            logger.info("Exporting plan guides...")
            _write_if_content("plan_guides_file", "02 - PLAN GUIDES", export_plan_guides(cur, logger), log_label="plan guides")

            _write_if_content(
                "service_broker_file",
                "02 - SERVICE BROKER",
                export_service_broker(cur, logger),
                log_label="service broker",
            )
            _write_if_content(
                "external_tables_file",
                "02 - EXTERNAL TABLES",
                export_external_tables(cur, logger),
                log_label="external tables",
            )
            _write_if_content(
                "change_tracking_file",
                "02 - CHANGE TRACKING",
                export_change_tracking(cur),
                log_label="change tracking",
            )
            _write_if_content("cdc_file", "02 - CDC", export_cdc(cur), log_label="CDC")

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

            logger.info("Exporting unique constraints...")
            _write_if_content(
                "unique_constraints_file",
                "03 - UNIQUE CONSTRAINTS",
                export_unique_constraints(cur, logger),
                log_label="unique constraints",
            )

            _write_if_content(
                "statistics_file",
                "03 - STATISTICS",
                export_statistics(cur, logger),
                log_label="statistics",
            )
            _write_if_content("fulltext_file", "03 - FULL-TEXT", export_fulltext(cur), log_label="full-text")
            _write_if_content(
                "table_options_file",
                "03 - TABLE OPTIONS (temporal / memory-optimized)",
                export_table_options(cur, logger),
                log_label="table options",
            )
            _write_if_content(
                "column_collation_file",
                "03 - COLUMN COLLATIONS",
                export_column_collation(cur),
                log_label="column collations",
            )
            _write_if_content(
                "table_storage_file",
                "03 - TABLE STORAGE (compression / partition placement)",
                export_table_storage(cur, logger),
                log_label="table storage",
            )
            spec_ix, spec_warn = export_specialized_indexes(cur, logger)
            if spec_warn:
                summary["warnings"].extend(spec_warn)
            _write_if_content(
                "specialized_indexes_file",
                "03 - SPECIALIZED INDEXES (columnstore / XML / spatial)",
                spec_ix,
                log_label="specialized indexes",
            )
            _write_if_content(
                "index_options_file",
                "03 - INDEX OPTIONS",
                export_index_options(cur),
                log_label="index options",
            )

            # 4) EXTENDED PROPERTIES (Comments/Metadata)
            logger.info("Exporting extended properties (MS_Description, comments)...")
            ep_sql = sql_header("04 - EXTENDED PROPERTIES (run after all objects are created)", cfg["server"], cfg["database"], run_id) + export_extended_properties(cur, logger)
            if ep_sql.strip():
                paths["extended_properties_file"].write_text(ep_sql, encoding="utf-8")
                logger.info("Extended properties exported: %s", str(paths["extended_properties_file"].resolve()))
            else:
                logger.info("No extended properties found.")

            # 5) SECURITY (users/roles + permissions — after objects exist on restore)
            logger.info("Exporting database principals (users and roles)...")
            _write_if_content(
                "database_principals_file",
                "04 - DATABASE PRINCIPALS",
                export_database_principals(cur),
                log_label="database principals",
            )

            logger.info("Exporting database permissions (GRANT/DENY)...")
            _write_if_content(
                "permissions_file",
                "04 - DATABASE PERMISSIONS",
                export_database_permissions(cur),
                log_label="database permissions",
            )

            _write_if_content(
                "role_memberships_file",
                "04 - ROLE MEMBERSHIPS",
                export_role_memberships(cur),
                log_label="role memberships",
            )
            _write_if_content(
                "cryptographic_objects_file",
                "04 - CRYPTOGRAPHIC OBJECTS",
                export_cryptographic_objects(cur, logger),
                log_label="cryptographic objects",
            )
            _write_if_content(
                "audit_specifications_file",
                "04 - DATABASE AUDIT SPECIFICATIONS",
                export_database_audit_specifications(cur),
                log_label="audit specifications",
            )
            _write_if_content(
                "always_encrypted_file",
                "04 - ALWAYS ENCRYPTED",
                export_always_encrypted(cur, logger),
                log_label="always encrypted",
            )
            _write_if_content(
                "data_masking_file",
                "04 - DYNAMIC DATA MASKING",
                export_data_masking(cur),
                log_label="data masking",
            )
            _write_if_content(
                "column_permissions_file",
                "04 - COLUMN PERMISSIONS",
                export_column_permissions(cur),
                log_label="column permissions",
            )
            _write_if_content(
                "schema_authorization_file",
                "04 - SCHEMA AUTHORIZATION",
                export_schema_authorization(cur),
                log_label="schema authorization",
            )
            _write_if_content(
                "replication_file",
                "02 - REPLICATION",
                export_replication(cur, logger),
                log_label="replication",
            )
            _write_if_content("graph_file", "02 - GRAPH TABLES", export_graph(cur, logger), log_label="graph")
            _write_if_content(
                "sequence_current_values_file",
                "02 - SEQUENCE CURRENT VALUES",
                export_sequence_current_values(cur),
                log_label="sequence current values",
            )

            _write_if_content(
                "database_diagrams_file",
                "META - DATABASE DIAGRAMS",
                export_database_diagrams(cur),
                log_label="database diagrams",
            )
            _write_if_content(
                "encrypted_modules_file",
                "META - ENCRYPTED MODULES",
                export_module_inventory(cur, logger),
                log_label="encrypted module inventory",
            )

            if cfg.get("export_server_logins", True):
                logger.info("Exporting server login inventory from master...")
                try:
                    with connect_to_database(
                        server=cfg["server"],
                        db="master",
                        user=cfg["user"],
                        driver=driver,
                        auth=cfg["auth"],
                        password=password,
                        timeout=30,
                        logger=logger,
                    ) as master_conn:
                        master_cur = master_conn.cursor()
                        login_sql = export_server_logins(master_cur)
                        if login_sql.strip():
                            paths["server_logins_file"].write_text(
                                sql_header("META - SERVER LOGINS (run in master)", cfg["server"], cfg["database"], run_id)
                                + login_sql,
                                encoding="utf-8",
                            )
                            logger.info("Wrote server logins: %s", paths["server_logins_file"])
                except Exception as login_ex:
                    summary["warnings"].append(f"Server login export skipped: {login_ex}")
                    logger.warning("Server login export skipped: %s", login_ex)

            gap_report = build_gap_report(cur, exported_files)
            paths["export_gap_report_file"].write_text(
                json.dumps(gap_report, indent=2),
                encoding="utf-8",
            )
            summary["export_gap_report"] = gap_report
            logger.info("Wrote export gap report: %s", paths["export_gap_report_file"])

            restore_manifest = {
                "description": "Recommended restore order for full mirror schema rebuild",
                "order": [
                    "meta/server_logins.sql (master)",
                    "00_foundation/database_options.sql",
                    "00_foundation/filegroups.sql",
                    "00_foundation/schemas.sql",
                    "00_foundation/user_defined_types.sql",
                    "00_foundation/memory_optimized_filegroup.sql",
                    "00_foundation/assemblies.sql",
                    "00_foundation/database_credentials.sql",
                    "00_foundation/external_resources.sql",
                    "00_foundation/partitioning.sql",
                    "00_foundation/xml_schema_collections.sql",
                    "02_programmables/legacy_rules_defaults.sql",
                    "02_programmables/service_broker.sql",
                    "02_programmables/sequences.sql",
                    "02_programmables/synonyms.sql",
                    "02_programmables/replication.sql",
                    "02_programmables/graph.sql",
                    "01_tables_all.sql",
                    "03_constraints_indexes/column_collation.sql",
                    "03_constraints_indexes/table_storage.sql",
                    "03_constraints_indexes/table_options.sql",
                    "03_constraints_indexes/primary_keys.sql",
                    "meta/database_diagrams.sql",
                    "02_programmables/views.sql",
                    "02_programmables/procedures/",
                    "02_programmables/procedures.sql",
                    "02_programmables/clr_procedures.sql",
                    "02_programmables/functions.sql",
                    "02_programmables/external_tables.sql",
                    "02_programmables/triggers.sql",
                    "02_programmables/ddl_triggers.sql",
                    "02_programmables/plan_guides.sql",
                    "03_constraints_indexes/indexes.sql",
                    "03_constraints_indexes/specialized_indexes.sql",
                    "03_constraints_indexes/index_options.sql",
                    "03_constraints_indexes/unique_constraints.sql",
                    "03_constraints_indexes/check_constraints.sql",
                    "03_constraints_indexes/default_constraints.sql",
                    "03_constraints_indexes/foreign_keys.sql",
                    "03_constraints_indexes/fulltext.sql",
                    "03_constraints_indexes/statistics.sql",
                    "02_programmables/security_policies.sql",
                    "02_programmables/change_tracking.sql",
                    "02_programmables/cdc.sql",
                    "04_security/database_principals.sql",
                    "04_security/role_memberships.sql",
                    "04_security/schema_authorization.sql",
                    "04_security/permissions.sql",
                    "04_security/column_permissions.sql",
                    "04_security/cryptographic_objects.sql",
                    "04_security/always_encrypted.sql",
                    "04_security/data_masking.sql",
                    "04_security/audit_specifications.sql",
                    "02_programmables/sequence_current_values.sql",
                    "03_constraints_indexes/extended_properties.sql",
                    "meta/encrypted_modules.sql",
                ],
                "expected_skips": [
                    "credential_secret_placeholder",
                    "server_login_password",
                    "encrypted_module_inventory",
                    "clr_assembly_azure",
                    "syslogins_master_azure",
                ],
                "notes": [
                    "See meta/export_gap_report.json for catalog counts vs exported files.",
                    "Credential SECRETS and login PASSWORDS are placeholders — set on target before restore.",
                    "Encrypted module definitions are listed in meta/encrypted_modules.sql only.",
                    "CLR modules are inventory-only in 02_programmables/clr_procedures.sql (skipped on Azure restore).",
                    "Table/data file paths and row data are not included (schema DDL only).",
                    "Use full_mirror=True (default in GUI) to restore all files in this order.",
                ],
            }
            paths["restore_manifest_file"].write_text(
                json.dumps(restore_manifest, indent=2),
                encoding="utf-8",
            )
            logger.info("Wrote restore manifest: %s", str(paths["restore_manifest_file"].resolve()))

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

