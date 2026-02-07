# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""Schema restore functionality."""

import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import pyodbc

from ..utils.azure_compat import (
    filter_azure_incompatible_batches,
    should_skip_already_exists_error,
    should_skip_azure_error,
    should_skip_default_constraint_error,
    should_skip_index_error,
)
from ..utils.database import build_conn_str, pick_sql_driver, resolve_password, connect_to_database
from ..utils.logging import setup_logger
from ..utils.paths import short_slug, utc_iso, utc_ts_compact
from ..utils.sql import split_sql_on_go
from .nullability_fix import apply_nullability_fixes


def find_latest_backup(backup_root: Path, server: str, db: str) -> Optional[Path]:
    """Find the latest backup run for a given server/database"""
    server_tag = short_slug(server)
    db_tag = short_slug(db)

    runs_dir = backup_root / server_tag / db_tag / "runs"
    if not runs_dir.exists():
        return None

    # Find all run folders (timestamp format: YYYYMMDD_HHMMSS)
    run_folders = []
    for item in runs_dir.iterdir():
        if item.is_dir() and re.match(r"^\d{8}_\d{6}$", item.name):
            run_folders.append((item.name, item))

    if not run_folders:
        return None

    # Sort by name (timestamp) descending
    run_folders.sort(key=lambda x: x[0], reverse=True)
    return run_folders[0][1]


def extract_schemas_from_sql(sql_text: str) -> set:
    """Extract schema names from SQL CREATE TABLE statements"""
    import re
    schemas = set()
    
    # Pattern 1: IF OBJECT_ID(N'schema.table', N'U')
    pattern1 = r"IF\s+OBJECT_ID\s*\(\s*N'(\w+)\.(\w+)'"
    for match in re.finditer(pattern1, sql_text, re.IGNORECASE):
        schema = match.group(1)
        if schema and schema.upper() not in ("dbo", "sys", "INFORMATION_SCHEMA"):
            schemas.add(schema)
    
    # Pattern 2: CREATE TABLE [schema].[table] or schema.table
    pattern2 = r"CREATE\s+TABLE\s+(?:\[?(\w+)\]?\.|(\w+)\.)"
    for match in re.finditer(pattern2, sql_text, re.IGNORECASE):
        schema = match.group(1) or match.group(2)
        if schema and schema.upper() not in ("dbo", "sys", "INFORMATION_SCHEMA"):
            schemas.add(schema)
    
    return schemas


def ensure_database_exists(server: str, db: str, user: str, driver: str, auth: str, password: Optional[str], logger) -> bool:
    """
    Check if database exists, create it if missing.
    Returns True if database exists or was created, False otherwise.
    """
    from ..utils.database import connect_to_database
    from ..utils.paths import qident
    
    try:
        # Connect to master database to check/create target database
        logger.info("Connecting to master database to check/create target database...")
        # Use connect_to_database which handles MSAL token caching automatically
        with connect_to_database(
            server=server,
            db="master",
            user=user,
            driver=driver,
            auth=auth,
            password=password,
            timeout=30,
            logger=logger,
        ) as master_conn:
            master_conn.timeout = 0
            master_cur = master_conn.cursor()
            
            # Check if database exists
            master_cur.execute(
                "SELECT COUNT(*) FROM sys.databases WHERE name = ?;",
                db
            )
            exists = master_cur.fetchone()[0] > 0
            
            if exists:
                logger.info("Database '%s' already exists", db)
                return True
            else:
                logger.info("Database '%s' does not exist. Creating it...", db)
                try:
                    master_cur.execute(f"CREATE DATABASE {qident(db)};")
                    master_conn.commit()
                    logger.info("Database '%s' created successfully", db)
                    return True
                except Exception as ex:
                    error_msg = str(ex)
                    # Check if it's an "already exists" error (race condition)
                    if "already exists" in error_msg.upper() or "2714" in error_msg:
                        logger.info("Database '%s' was created by another process", db)
                        return True
                    logger.error("Failed to create database '%s': %s", db, error_msg)
                    return False
                    
    except Exception as ex:
        logger.warning("Could not check/create database '%s': %s. Will attempt to connect anyway.", db, ex)
        return False  # Return False but don't fail - connection attempt will show real error


def ensure_schemas_exist(cur, conn, schemas: set, logger):
    """Create schemas if they don't exist"""
    from ..utils.paths import qident
    
    created = []
    already_existed = []
    failed = []
    
    for schema in schemas:
        try:
            # Check if schema exists
            cur.execute(
                "SELECT COUNT(*) FROM sys.schemas WHERE name = ?;",
                schema
            )
            exists = cur.fetchone()[0] > 0
            
            if not exists:
                logger.info("Creating schema: %s", schema)
                cur.execute(f"CREATE SCHEMA {qident(schema)};")
                conn.commit()
                logger.info("Schema created: %s", schema)
                created.append(schema)
            else:
                logger.debug("Schema already exists: %s", schema)
                already_existed.append(schema)
        except Exception as ex:
            error_msg = str(ex)
            # Check if it's an "already exists" error (race condition)
            if "already exists" in error_msg.upper() or "2714" in error_msg:
                logger.info("Schema '%s' was created by another process", schema)
                already_existed.append(schema)
            else:
                logger.warning("Failed to create schema %s: %s", schema, ex)
                failed.append(schema)
            try:
                conn.rollback()
            except Exception:
                pass
    
    if created:
        logger.info("Created %d schema(s): %s", len(created), ", ".join(created))
    if already_existed:
        logger.info("%d schema(s) already existed: %s", len(already_existed), ", ".join(already_existed))
    if failed:
        logger.warning("%d schema(s) failed to create: %s", len(failed), ", ".join(failed))


def get_backup_paths(backup_path: Path) -> dict:
    """Get all SQL file paths from backup folder structure"""
    schema_dir = backup_path / "schema"
    if not schema_dir.exists():
        return {}

    prog_dir = schema_dir / "02_programmables"
    cx_dir = schema_dir / "03_constraints_indexes"

    paths = {}
    
    # Tables files (two versions available):
    # - tables_no_pk_file: Tables without PKs (for faster data loading, PKs added later)
    # - tables_file: Tables with PKs (for direct restore)
    tables_no_pk_file = schema_dir / "01_tables_no_pk.sql"
    tables_file = schema_dir / "01_tables_all.sql"
    
    if tables_no_pk_file.exists():
        paths["tables_no_pk_file"] = tables_no_pk_file
    if tables_file.exists():
        paths["tables_file"] = tables_file
    
    if prog_dir.exists():
        paths["sequences_file"] = prog_dir / "sequences.sql"
        paths["synonyms_file"] = prog_dir / "synonyms.sql"
        paths["views_file"] = prog_dir / "views.sql"
        paths["procedures_file"] = prog_dir / "procedures.sql"
        paths["functions_file"] = prog_dir / "functions.sql"
        paths["triggers_file"] = prog_dir / "triggers.sql"

        if cx_dir.exists():
            paths["foreign_keys_file"] = cx_dir / "foreign_keys.sql"
            paths["check_constraints_file"] = cx_dir / "check_constraints.sql"
            paths["default_constraints_file"] = cx_dir / "default_constraints.sql"
            paths["indexes_file"] = cx_dir / "indexes.sql"
            # Primary keys (to add after tables, before foreign keys)
            primary_keys_file = cx_dir / "primary_keys.sql"
            if primary_keys_file.exists():
                paths["primary_keys_file"] = primary_keys_file
            # Extended properties (comments/metadata)
            extended_properties_file = cx_dir / "extended_properties.sql"
            if extended_properties_file.exists():
                paths["extended_properties_file"] = extended_properties_file

    return paths


def execute_sql_file(
    logger,
    cur,
    conn,
    sql_file: Path,
    file_type: str,
    continue_on_error: bool,
    dry_run: bool,
    preview_callback=None,  # Optional callback to preview SQL before execution
) -> dict:
    """Execute a SQL file. Returns dict with status, batches_executed, errors."""
    result = {
        "file": str(sql_file),
        "file_type": file_type,
        "status": "started",
        "batches_total": 0,
        "batches_filtered": 0,
        "batches_executed": 0,
        "batches_failed": 0,
        "batches_skipped": 0,
        "batches_already_existed": 0,  # Track objects that already existed
        "errors": [],
        "warnings": [],  # Track warnings (e.g., already exists)
        "duration_seconds": None,
    }

    if not sql_file.exists():
        result["status"] = "skipped"
        result["errors"].append(f"File not found: {sql_file}")
        logger.warning("File not found: %s", sql_file)
        return result

    t0 = time.time()

    try:
        sql_text = sql_file.read_text(encoding="utf-8")
        batches = split_sql_on_go(sql_text)
        result["batches_total"] = len(batches)

        if dry_run:
            logger.info("DRY RUN: Would execute %d batches from %s", len(batches), sql_file.name)
            result["status"] = "dry_run"
            result["duration_seconds"] = round(time.time() - t0, 3)
            return result

        if not batches:
            logger.warning("No SQL batches found in %s", sql_file.name)
            result["status"] = "skipped"
            result["duration_seconds"] = round(time.time() - t0, 3)
            return result

        logger.info("Executing %s: %d batches", sql_file.name, len(batches))

        # Filter out Azure SQL incompatible batches before execution
        compatible_batches = filter_azure_incompatible_batches(batches, logger)
        result["batches_filtered"] = len(batches) - len(compatible_batches)
        if result["batches_filtered"] > 0:
            logger.info("After Azure compatibility filter: %d/%d batches compatible (%d filtered)", 
                       len(compatible_batches), len(batches), result["batches_filtered"])
        
        for orig_idx, batch, issues in compatible_batches:
            # Show preview dialog if callback provided (GUI mode, not bulk/Excel)
            if preview_callback and not dry_run:
                # Only preview for certain object types (foreign keys, indexes, constraints)
                if file_type in ("FOREIGN_KEYS", "INDEXES", "CHECK_CONSTRAINTS", "DEFAULT_CONSTRAINTS"):
                    user_approved = preview_callback(
                        file_type=file_type,
                        batch_number=orig_idx,
                        total_batches=len(compatible_batches),
                        sql_batch=batch,
                        batch_index=orig_idx
                    )
                    if not user_approved:
                        # User cancelled or skipped this batch
                        logger.info("User skipped batch %d/%d in %s", orig_idx, len(compatible_batches), file_type)
                        result["batches_skipped"] += 1
                        continue
            
            try:
                cur.execute(batch)
                conn.commit()
                result["batches_executed"] += 1

                if orig_idx % 50 == 0:
                    logger.debug("Progress %s: batch %d/%d", sql_file.name, orig_idx, len(batches))

            except Exception as ex:
                error_msg = str(ex)
                error_str = f"{type(ex).__name__}: {ex}"
                
                # Check if this is an error we should skip (Azure incompatibility, already exists, etc.)
                should_skip = False
                skip_reason = None
                
                if should_skip_azure_error(error_str):
                    should_skip = True
                    skip_reason = "Azure SQL incompatible feature"
                elif file_type == "DEFAULT_CONSTRAINTS" and should_skip_default_constraint_error(error_str):
                    should_skip = True
                    skip_reason = "Default constraint already exists (included in table definition)"
                elif file_type == "INDEXES" and should_skip_index_error(error_str):
                    should_skip = True
                    skip_reason = "Index conflict (already exists or clustered index conflict)"
                elif should_skip_already_exists_error(error_str):
                    # Handle "already exists" errors for all object types (tables, views, SPs, functions, FKs, etc.)
                    should_skip = True
                    skip_reason = f"{file_type} object already exists"
                elif file_type == "FOREIGN_KEYS" and ("Incorrect syntax near ')'" in error_str or "syntax error" in error_str.lower()):
                    # Handle invalid foreign key SQL (empty column lists, etc.)
                    should_skip = True
                    skip_reason = "Invalid foreign key SQL (likely missing column information in backup)"
                elif file_type == "FOREIGN_KEYS" and ("no primary or candidate keys" in error_str.lower() or "1776" in error_str):
                    # Handle foreign key errors where referenced table doesn't have PK/unique constraint
                    # This can happen if:
                    # 1. Tables were restored without PKs (using tables_no_pk_file)
                    # 2. PKs weren't created properly during table restore
                    # 3. Tables already existed from a previous run without PKs
                    should_skip = True
                    skip_reason = "Referenced table missing primary key or unique constraint - ensure tables are restored with primary keys before creating foreign keys"
                    logger.warning(
                        "Foreign key creation failed: %s. This usually means the referenced table doesn't have a primary key. "
                        "Ensure tables are restored WITH primary keys (use tables_file, not tables_no_pk_file) before restoring foreign keys.",
                        error_str[:200]
                    )
                elif file_type == "INDEXES" and "Incorrect syntax near 'WHERE'" in error_str:
                    # Handle invalid index SQL (empty WHERE clause, etc.)
                    should_skip = True
                    skip_reason = "Invalid index SQL (likely empty or malformed WHERE clause in backup)"
                elif file_type == "INDEXES" and ("cannot specify included columns for a clustered index" in error_str.lower() or "10601" in error_str):
                    # Handle clustered index with INCLUDE columns (not allowed in SQL Server)
                    # This should be fixed in the backup, but handle gracefully if old backup is used
                    should_skip = True
                    skip_reason = "Clustered index with INCLUDE columns (not supported) - re-run backup with latest code to fix"
                    logger.warning(
                        "Index creation failed: %s. Clustered indexes cannot have INCLUDE columns. "
                        "Re-run schema backup with the latest code to automatically convert to nonclustered.",
                        error_str[:200]
                    )
                
                if should_skip:
                    # Track if this is an "already exists" skip vs other skip
                    is_already_exists = "already exists" in skip_reason.lower() or "already has" in skip_reason.lower()
                    
                    if is_already_exists:
                        logger.info(
                            "%s | Batch %d/%d - object already exists (%s): %s",
                            sql_file.name,
                            orig_idx,
                            len(batches),
                            skip_reason,
                            error_str[:200]  # Truncate long error messages
                        )
                        result["batches_already_existed"] += 1
                        result["warnings"].append(f"Batch {orig_idx}: {skip_reason}")
                    else:
                        logger.warning(
                            "%s | Batch %d/%d skipped (%s): %s",
                            sql_file.name,
                            orig_idx,
                            len(batches),
                            skip_reason,
                            error_str[:200]  # Truncate long error messages
                        )
                    
                    result["batches_skipped"] += 1
                    result["batches_executed"] += 1  # Count as executed (intentionally skipped)
                    continue
                
                # Real error - log and handle
                result["batches_failed"] += 1
                error_msg_full = f"Batch {orig_idx}/{len(batches)} failed: {error_str}"
                result["errors"].append(error_msg_full)
                logger.error("%s | %s", sql_file.name, error_msg_full)

                try:
                    conn.rollback()
                except Exception:
                    pass

                if not continue_on_error:
                    raise

        if result["batches_failed"] == 0:
            result["status"] = "success"
        elif result["batches_executed"] > 0:
            result["status"] = "completed_with_errors"
        else:
            result["status"] = "failed"

        result["duration_seconds"] = round(time.time() - t0, 3)
        
        # Build summary message
        summary_parts = [
            f"status={result['status']}",
            f"total={result['batches_total']}",
            f"executed={result['batches_executed'] - result['batches_skipped']}",  # Actually executed
            f"already_existed={result['batches_already_existed']}",
            f"filtered={result['batches_filtered']}",
            f"skipped={result['batches_skipped'] - result['batches_already_existed']}",  # Other skips
            f"failed={result['batches_failed']}",
            f"duration={result['duration_seconds']:.3f}s"
        ]
        
        logger.info(
            "Completed %s: %s",
            sql_file.name,
            " | ".join(summary_parts)
        )
        
        # Log summary of what happened
        if result["batches_already_existed"] > 0:
            logger.info(
                "  → %d object(s) already existed (skipped gracefully)",
                result["batches_already_existed"]
            )
        if result["batches_failed"] > 0:
            logger.warning(
                "  → %d batch(es) failed (see errors above)",
                result["batches_failed"]
            )

    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Failed to execute %s: %s", sql_file.name, msg)
        result["status"] = "failed"
        result["errors"].append(msg)
        result["duration_seconds"] = round(time.time() - t0, 3)

    return result


def run_restore(cfg: dict):
    """Run schema restore with provided configuration"""
    run_id = utc_ts_compact()

    # Determine backup path
    backup_path = None
    if cfg["backup_path"]:
        backup_path = Path(cfg["backup_path"])
        if not backup_path.exists():
            raise ValueError(f"Backup path does not exist: {backup_path}")
    else:
        # Try to find latest backup
        backup_root = Path("backups")
        if backup_root.exists():
            backup_path = find_latest_backup(backup_root, cfg["dest_server"], cfg["dest_db"])
            if backup_path:
                print(f"Found latest backup: {backup_path}")
            else:
                raise ValueError(
                    f"Could not find backup for {cfg['dest_server']}/{cfg['dest_db']}. "
                    f"Please specify --backup-path explicitly."
                )
        else:
            raise ValueError("No backup path specified and 'backups' folder not found. Use --backup-path.")

    # Setup logging
    restore_root = Path("restores") / run_id
    logs_dir = restore_root / "logs"
    meta_dir = restore_root / "meta"
    logs_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / f"restore_{run_id}.log"
    logger = setup_logger(log_file, "schema_restore")

    start = time.time()

    summary = {
        "run_id": run_id,
        "backup_path": str(backup_path),
        "dest_server": cfg["dest_server"],
        "dest_db": cfg["dest_db"],
        "dest_auth": cfg["dest_auth"],
        "dest_user": cfg["dest_user"],
        "started_utc": utc_iso(),
        "ended_utc": None,
        "duration_seconds": None,
        "status": "started",
        "errors": [],
        "files_restored": {},
        "effective_config": {k: ("***" if "password" in k and cfg.get(k) else cfg.get(k)) for k in cfg},
    }

    logger.info("Starting schema restore run: %s", run_id)
    logger.info("Backup path: %s", backup_path)
    logger.info("Destination: %s | %s | auth=%s | user=%s", cfg["dest_server"], cfg["dest_db"], cfg["dest_auth"], cfg["dest_user"])
    logger.info("Restore programmables: %s", cfg["restore_programmables"])
    logger.info("Restore constraints: %s", cfg["restore_constraints"])
    logger.info("Restore indexes: %s", cfg["restore_indexes"])
    logger.info("Continue on error: %s", cfg["continue_on_error"])
    logger.info("Dry run: %s", cfg["dry_run"])
    logger.info("Python exe: %s", sys.executable)
    logger.info("Log file: %s", str(log_file.resolve()))

    # Get backup file paths
    backup_paths = get_backup_paths(backup_path)
    if not backup_paths:
        raise ValueError(f"No SQL files found in backup path: {backup_path}")

    logger.info("Found backup files: %s", ", ".join(k for k in backup_paths.keys()))

    try:
        driver = pick_sql_driver(logger)
        password = cfg.get("dest_password")
        
        # Check if database exists, create if missing
        logger.info("Checking if destination database exists...")
        db_exists = ensure_database_exists(
            cfg["dest_server"], cfg["dest_db"], cfg["dest_user"], 
            driver, cfg["dest_auth"], password, logger
        )
        if not db_exists:
            logger.warning("Database may not exist. Connection attempt will show actual error.")
        
        logger.info("Connecting to destination (auth=%s)...", cfg["dest_auth"])
        # Use connect_to_database which handles MSAL token caching automatically
        with connect_to_database(
            server=cfg["dest_server"],
            db=cfg["dest_db"],
            user=cfg["dest_user"],
            driver=driver,
            auth=cfg["dest_auth"],
            password=password,
            timeout=30,
            logger=logger,
        ) as conn:
            conn.timeout = 0
            cur = conn.cursor()

            cur.execute("SELECT DB_NAME(), SUSER_SNAME(), GETDATE();")
            db_name, login_name, server_time = cur.fetchone()
            logger.info("Connected. DB=%s Login=%s ServerTime=%s", db_name, login_name, server_time)

            # If restoring tables, ensure all required schemas exist first
            if cfg.get("restore_tables", False) and "tables_file" in backup_paths:
                logger.info("Extracting schemas from tables SQL file...")
                tables_sql = backup_paths["tables_file"].read_text(encoding="utf-8")
                required_schemas = extract_schemas_from_sql(tables_sql)
                if required_schemas:
                    logger.info("Found schemas in backup: %s", ", ".join(sorted(required_schemas)))
                    ensure_schemas_exist(cur, conn, required_schemas, logger)
                else:
                    logger.info("No custom schemas found (using default schemas)")

            # Restore order (matching Red Gate SQL Compare approach):
            # 0. Sequences (before tables that use them)
            # 1. Synonyms (before objects that use them)
            # 2. Tables (if restore_tables is True - for initial setup)
            # 3. Primary Keys (must be before foreign keys and indexes)
            # 4. Programmables (views, procedures, functions)
            # 5. Indexes (non-clustered, before foreign keys)
            # 6. Constraints (check constraints, default constraints)
            # 7. Foreign Keys (LAST - after all tables and PKs exist)

            restore_order = []

            # Restore sequences first (before tables that use them)
            if "sequences_file" in backup_paths:
                restore_order.append(("sequences_file", "SEQUENCES", backup_paths["sequences_file"]))
                logger.info("Sequences will be restored before tables")

            # Restore synonyms early (before objects that use them)
            if "synonyms_file" in backup_paths:
                restore_order.append(("synonyms_file", "SYNONYMS", backup_paths["synonyms_file"]))
                logger.info("Synonyms will be restored before other objects")

            # Restore tables first if requested (for initial setup before data migration)
            # IMPORTANT: For foreign keys to work, tables MUST have primary keys.
            # Prefer tables_file (with PKs) over tables_no_pk_file (without PKs) unless explicitly requested.
            if cfg.get("restore_tables", False):
                # Check if we should use tables without PKs (for faster data loading)
                use_tables_no_pk = cfg.get("use_tables_no_pk", False)
                
                if use_tables_no_pk and "tables_no_pk_file" in backup_paths:
                    restore_order.append(("tables_no_pk_file", "TABLES (without PKs)", backup_paths["tables_no_pk_file"]))
                    logger.warning("Using tables_no_pk_file - primary keys will need to be restored separately before foreign keys")
                elif "tables_file" in backup_paths:
                    restore_order.append(("tables_file", "TABLES (with PKs)", backup_paths["tables_file"]))
                    logger.info("Using tables_file with primary keys included")
                elif "tables_no_pk_file" in backup_paths:
                    # Fallback to no_pk version if tables_file doesn't exist
                    restore_order.append(("tables_no_pk_file", "TABLES (without PKs)", backup_paths["tables_no_pk_file"]))
                    logger.warning("tables_file not found, using tables_no_pk_file - primary keys will need to be restored separately")
                    
                    # After tables are restored, fix nullability mismatches
                    # This compares source (backup) vs destination (current) and applies ALTER statements
                    if cfg.get("fix_nullability", True):  # Default to True
                        logger.info("Nullability fix enabled - will check and fix mismatches after table restore")
                        restore_order.append(("nullability_fix", "NULLABILITY_FIX", backup_paths["tables_file"]))

            # Restore Primary Keys BEFORE indexes and foreign keys
            # (PKs create clustered indexes which are needed for foreign key references)
            if "primary_keys_file" in backup_paths:
                restore_order.append(("primary_keys_file", "PRIMARY_KEYS", backup_paths["primary_keys_file"]))
                logger.info("Primary keys will be restored before indexes and foreign keys")

            if cfg["restore_programmables"]:
                if "views_file" in backup_paths:
                    restore_order.append(("views_file", "VIEWS", backup_paths["views_file"]))
                if "procedures_file" in backup_paths:
                    restore_order.append(("procedures_file", "PROCEDURES", backup_paths["procedures_file"]))
                if "functions_file" in backup_paths:
                    restore_order.append(("functions_file", "FUNCTIONS", backup_paths["functions_file"]))
                if "triggers_file" in backup_paths:
                    restore_order.append(("triggers_file", "TRIGGERS", backup_paths["triggers_file"]))

            # Restore indexes BEFORE foreign keys (indexes may be needed for FK performance)
            if cfg["restore_indexes"]:
                if "indexes_file" in backup_paths:
                    restore_order.append(("indexes_file", "INDEXES", backup_paths["indexes_file"]))

            # Restore constraints (check and default) BEFORE foreign keys
            if cfg["restore_constraints"]:
                if "check_constraints_file" in backup_paths:
                    restore_order.append(("check_constraints_file", "CHECK_CONSTRAINTS", backup_paths["check_constraints_file"]))
                if "default_constraints_file" in backup_paths:
                    restore_order.append(("default_constraints_file", "DEFAULT_CONSTRAINTS", backup_paths["default_constraints_file"]))
                # Foreign keys LAST - after all tables, PKs, and indexes exist
                if "foreign_keys_file" in backup_paths:
                    restore_order.append(("foreign_keys_file", "FOREIGN_KEYS", backup_paths["foreign_keys_file"]))

            # Restore extended properties (comments/metadata) AFTER all objects are created
            if "extended_properties_file" in backup_paths:
                restore_order.append(("extended_properties_file", "EXTENDED_PROPERTIES", backup_paths["extended_properties_file"]))

            logger.info("Restore order: %s", " -> ".join([name for _, name, _ in restore_order]))

            for file_key, file_type, sql_file in restore_order:
                # Special handling for nullability fix
                if file_key == "nullability_fix":
                    logger.info("=" * 80)
                    logger.info("FIXING NULLABILITY MISMATCHES")
                    logger.info("=" * 80)
                    nullability_result = apply_nullability_fixes(
                        cur, conn, backup_path, logger, dry_run=cfg.get("dry_run", False)
                    )
                    summary["nullability_fix"] = nullability_result
                    if nullability_result.get("errors"):
                        summary["errors"].extend(nullability_result["errors"])
                    logger.info(
                        "Nullability fix completed: %d tables checked, %d tables fixed, %d columns fixed",
                        nullability_result.get("tables_checked", 0),
                        nullability_result.get("tables_fixed", 0),
                        nullability_result.get("columns_fixed", 0)
                    )
                    continue
                
                logger.info("=== Restoring %s: %s ===", file_type, sql_file.name)
                result = execute_sql_file(
                    logger=logger,
                    cur=cur,
                    conn=conn,
                    sql_file=sql_file,
                    file_type=file_type,
                    continue_on_error=cfg["continue_on_error"],
                    dry_run=cfg.get("dry_run", False),
                    preview_callback=cfg.get("preview_callback"),  # Optional preview callback for GUI mode
                )
                summary["files_restored"][file_key] = result

                if result["status"] == "failed" and not cfg["continue_on_error"]:
                    raise RuntimeError(f"Stopping due to failure in {file_type}")

                if result["errors"]:
                    summary["errors"].extend([f"{file_type}: {e}" for e in result["errors"]])

        # Calculate overall statistics
        total_batches = 0
        total_executed = 0
        total_already_existed = 0
        total_failed = 0
        total_skipped = 0
        
        for file_result in summary["files_restored"].values():
            total_batches += file_result.get("batches_total", 0)
            total_executed += file_result.get("batches_executed", 0) - file_result.get("batches_skipped", 0)
            total_already_existed += file_result.get("batches_already_existed", 0)
            total_failed += file_result.get("batches_failed", 0)
            total_skipped += file_result.get("batches_skipped", 0)
        
        summary["statistics"] = {
            "total_batches": total_batches,
            "batches_executed": total_executed,
            "batches_already_existed": total_already_existed,
            "batches_failed": total_failed,
            "batches_skipped": total_skipped,
        }
        
        summary["status"] = "success" if not summary["errors"] else "completed_with_errors"

    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Restore failed: %s", msg)
        summary["status"] = "failed"
        summary["errors"].append(msg)

    finally:
        summary["ended_utc"] = utc_iso()
        summary["duration_seconds"] = round(time.time() - start, 3)

        summary_file = meta_dir / "restore_summary.json"
        summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        logger.info("Restore status: %s", summary["status"])
        logger.info("Duration: %s seconds", summary["duration_seconds"])
        
        # Log statistics if available
        if "statistics" in summary:
            stats = summary["statistics"]
            logger.info("Restore statistics:")
            logger.info("  Total batches: %d", stats["total_batches"])
            logger.info("  Successfully executed: %d", stats["batches_executed"])
            logger.info("  Already existed (skipped): %d", stats["batches_already_existed"])
            logger.info("  Failed: %d", stats["batches_failed"])
            logger.info("  Other skips: %d", stats["batches_skipped"] - stats["batches_already_existed"])
        
        logger.info("Summary JSON: %s", str(summary_file.resolve()))

        if summary["errors"]:
            logger.warning("Errors encountered: %d (see restore_summary.json)", len(summary["errors"]))

    return summary

