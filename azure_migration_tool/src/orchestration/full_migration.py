# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""Full migration orchestration."""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..backup import run_backup
from ..migration import run_migration
from ..restore import run_restore
from ..utils.logging import setup_logger
from ..utils.paths import short_slug, utc_iso


def find_latest_backup(backup_root: Path, server: str, db: str) -> Optional[Path]:
    """Find the latest backup run for a given server/database"""
    import re

    server_tag = short_slug(server)
    db_tag = short_slug(db)

    runs_dir = backup_root / server_tag / db_tag / "runs"
    if not runs_dir.exists():
        return None

    run_folders = []
    for item in runs_dir.iterdir():
        if item.is_dir() and re.match(r"^\d{8}_\d{6}$", item.name):
            run_folders.append((item.name, item))

    if not run_folders:
        return None

    run_folders.sort(key=lambda x: x[0], reverse=True)
    return run_folders[0][1]


def run_full_migration(cfg: dict):
    """Orchestrate the full migration process"""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    # Get log callback for streaming to GUI (optional)
    gui_log_callback = cfg.get("log_callback")

    # Setup logging
    logs_dir = Path("migration_runs") / run_id / "logs"
    meta_dir = Path("migration_runs") / run_id / "meta"
    logs_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / f"full_migration_{run_id}.log"
    logger = setup_logger(log_file, "full_migration")
    
    def log_msg(msg):
        """Log to both file and GUI callback if provided."""
        logger.info(msg)
        if gui_log_callback:
            try:
                gui_log_callback(msg)
            except:
                pass  # Don't let callback errors break migration

    start = time.time()

    summary = {
        "run_id": run_id,
        "started_utc": utc_iso(),
        "ended_utc": None,
        "duration_seconds": None,
        "status": "started",
        "steps": {
            "backup": {"status": "skipped", "run_id": None, "backup_path": None},
            "restore_tables": {"status": "skipped", "run_id": None},
            "migration": {"status": "skipped", "run_id": None},
            "restore": {"status": "skipped", "run_id": None},
        },
        "errors": [],
    }

    log_msg("=" * 60)
    log_msg(f"Starting FULL MIGRATION run: {run_id}")
    log_msg("=" * 60)
    log_msg(f"Skip backup: {cfg['skip_backup']}")
    log_msg(f"Skip migration: {cfg['skip_migration']}")
    log_msg(f"Skip restore: {cfg['skip_restore']}")
    logger.info("Log file: %s", str(log_file.resolve()))

    try:
        # ============================================================
        # STEP 1: SCHEMA BACKUP (from source)
        # ============================================================
        if not cfg["skip_backup"]:
            log_msg("")
            log_msg("=" * 60)
            log_msg("STEP 1: SCHEMA BACKUP")
            log_msg("=" * 60)

            backup_cfg = {
                "server": cfg["backup_src_server"],
                "database": cfg["backup_src_db"],
                "auth": cfg["backup_src_auth"],
                "user": cfg["backup_src_user"],
                "password": cfg["backup_src_password"],
                "backup_root": cfg.get("backup_root", "backups"),
                "log_table_sample": cfg.get("backup_log_table_sample", 20),
                "export_defaults_separately": cfg.get("backup_export_defaults_separately", True),
            }

            log_msg(f"Backing up schema from: {backup_cfg['server']} | {backup_cfg['database']}")
            backup_summary = run_backup(backup_cfg)

            summary["steps"]["backup"]["status"] = backup_summary["status"]
            summary["steps"]["backup"]["run_id"] = backup_summary["run_id"]
            summary["steps"]["backup"]["backup_path"] = str(
                Path("backups") / short_slug(backup_cfg["server"]) / short_slug(backup_cfg["database"]) / "runs" / backup_summary["run_id"]
            )

            if backup_summary["status"] != "success":
                summary["errors"].append(f"Backup failed: {backup_summary.get('errors', [])}")
                log_msg(f"✗ Backup failed. Status: {backup_summary['status']}")
                if not cfg.get("continue_on_backup_error", False):
                    raise RuntimeError("Backup failed. Stopping migration.")
            else:
                log_msg(f"✓ Backup completed successfully. Run ID: {backup_summary['run_id']}")
        else:
            log_msg("Skipping schema backup (--skip-backup)")

        # ============================================================
        # STEP 2: RESTORE TABLES ONLY (to destination, before data migration)
        # ============================================================
        if not cfg.get("skip_restore_tables", False) and not cfg["skip_backup"]:
            log_msg("")
            log_msg("=" * 60)
            log_msg("STEP 2: RESTORE TABLES (to destination)")
            log_msg("=" * 60)

            # Determine restore destination (defaults to migration destination)
            restore_dest_server = cfg.get("restore_dest_server") or cfg["migrate_dest_server"]
            restore_dest_db = cfg.get("restore_dest_db") or cfg["migrate_dest_db"]
            restore_dest_auth = cfg.get("restore_dest_auth") or cfg["migrate_dest_auth"]
            restore_dest_user = cfg.get("restore_dest_user") or cfg["migrate_dest_user"]
            restore_dest_password = cfg.get("restore_dest_password") or cfg["migrate_dest_password"]

            # Determine backup path (use the backup we just created)
            restore_backup_path = cfg.get("restore_backup_path")
            if not restore_backup_path and not cfg["skip_backup"]:
                # Use the backup we just created
                restore_backup_path = summary["steps"]["backup"]["backup_path"]
                log_msg(f"Using backup from Step 1: {restore_backup_path}")
            elif not restore_backup_path:
                # Auto-detect from latest backup
                backup_root = Path("backups")
                restore_backup_path = find_latest_backup(backup_root, cfg["backup_src_server"], cfg["backup_src_db"])
                if not restore_backup_path:
                    raise ValueError("Could not find backup path. Please specify --restore-backup-path.")
                log_msg(f"Auto-detected backup path: {restore_backup_path}")

            restore_tables_cfg = {
                "dest_server": restore_dest_server,
                "dest_db": restore_dest_db,
                "dest_auth": restore_dest_auth,
                "dest_user": restore_dest_user,
                "dest_password": restore_dest_password,
                "backup_path": str(restore_backup_path),
                "restore_tables": True,  # Only restore tables
                "restore_programmables": False,  # Skip for now
                "restore_constraints": False,  # Skip for now
                "restore_indexes": False,  # Skip for now
                "continue_on_error": True,  # Always continue on error for table restore (some tables may succeed)
                "dry_run": cfg.get("restore_dry_run", False),
            }

            log_msg(f"Restoring tables to: {restore_dest_server} | {restore_dest_db}")
            restore_tables_summary = run_restore(restore_tables_cfg)

            summary["steps"]["restore_tables"]["status"] = restore_tables_summary["status"]
            summary["steps"]["restore_tables"]["run_id"] = restore_tables_summary["run_id"]

            # Continue if status is success OR completed_with_errors (when continue_on_error is True)
            if restore_tables_summary["status"] == "success":
                log_msg("✓ Tables restored successfully")
            elif restore_tables_summary["status"] == "completed_with_errors":
                summary["errors"].extend(restore_tables_summary.get("errors", []))
                log_msg("⚠ Table restore completed with errors. Some tables may not have been created.")
                # Check if critical tables were created (at least some batches succeeded)
                tables_file_result = restore_tables_summary.get("files_restored", {}).get("tables_file", {})
                batches_executed = tables_file_result.get("batches_executed", 0)
                batches_total = tables_file_result.get("batches_total", 0)
                if batches_executed == 0:
                    log_msg("✗ No table batches were executed successfully. Cannot proceed with data migration.")
                    if not cfg.get("continue_on_restore_error", False):
                        raise RuntimeError("Table restore failed - no tables created. Stopping.")
                else:
                    log_msg(f"Table restore: {batches_executed}/{batches_total} batches succeeded. Continuing...")
            else:  # status == "failed"
                summary["errors"].extend(restore_tables_summary.get("errors", []))
                log_msg(f"✗ Table restore failed. Status: {restore_tables_summary['status']}")
                if not cfg.get("continue_on_restore_error", False):
                    raise RuntimeError("Table restore failed. Stopping.")
        else:
            log_msg("Skipping table restore (--skip-restore-tables or --skip-backup)")

        # ============================================================
        # STEP 3: DATA MIGRATION (from source to destination)
        # ============================================================
        if not cfg["skip_migration"]:
            log_msg("")
            log_msg("=" * 60)
            log_msg("STEP 3: DATA MIGRATION")
            log_msg("=" * 60)

            migrate_cfg = {
                "src_server": cfg["migrate_src_server"],
                "src_db": cfg["migrate_src_db"],
                "src_auth": cfg["migrate_src_auth"],
                "src_user": cfg["migrate_src_user"],
                "src_password": cfg["migrate_src_password"],
                "dest_server": cfg["migrate_dest_server"],
                "dest_db": cfg["migrate_dest_db"],
                "dest_auth": cfg["migrate_dest_auth"],
                "dest_user": cfg["migrate_dest_user"],
                "dest_password": cfg["migrate_dest_password"],
                "batch_size": cfg["migrate_batch_size"],
                "tables": cfg.get("migrate_tables", ""),
                "exclude": cfg.get("migrate_exclude", ""),
                "truncate_dest": cfg["migrate_truncate_dest"],
                "delete_dest": cfg["migrate_delete_dest"],
                "continue_on_error": cfg["migrate_continue_on_error"],
                "dry_run": cfg["migrate_dry_run"],
            }

            log_msg("Running data migration...")
            # Pass the log callback to run_migration for detailed streaming
            migrate_report = run_migration(migrate_cfg, log_callback=gui_log_callback)

            summary["steps"]["migration"]["status"] = migrate_report["status"]
            summary["steps"]["migration"]["run_id"] = migrate_report["run_id"]

            if migrate_report["status"] != "success":
                summary["errors"].extend(migrate_report.get("errors", []))
                log_msg(f"⚠ Migration completed with errors. Status: {migrate_report['status']}")
                if not cfg.get("continue_on_migration_error", False):
                    raise RuntimeError("Data migration failed. Stopping.")
            else:
                log_msg(f"✓ Data migration completed successfully. Tables: {migrate_report.get('tables_ok', 0)}")
        else:
            log_msg("Skipping data migration (--skip-migration)")

        # ============================================================
        # STEP 4: SCHEMA RESTORE (constraints, indexes, programmables - after data migration)
        # ============================================================
        if not cfg["skip_restore"]:
            log_msg("")
            log_msg("=" * 60)
            log_msg("STEP 4: SCHEMA RESTORE")
            log_msg("=" * 60)

            # Determine restore destination (defaults to migration destination)
            restore_dest_server = cfg["restore_dest_server"] or cfg["migrate_dest_server"]
            restore_dest_db = cfg["restore_dest_db"] or cfg["migrate_dest_db"]
            restore_dest_auth = cfg["restore_dest_auth"] or cfg["migrate_dest_auth"]
            restore_dest_user = cfg["restore_dest_user"] or cfg["migrate_dest_user"]
            restore_dest_password = cfg["restore_dest_password"] or cfg["migrate_dest_password"]

            # Determine backup path
            restore_backup_path = cfg["restore_backup_path"]
            if not restore_backup_path:
                # Auto-detect from latest backup
                backup_root = Path("backups")
                restore_backup_path = find_latest_backup(backup_root, cfg["backup_src_server"], cfg["backup_src_db"])
                if not restore_backup_path:
                    raise ValueError("Could not find backup path. Please specify --restore-backup-path.")
                log_msg(f"Auto-detected backup path: {restore_backup_path}")

            restore_cfg = {
                "dest_server": restore_dest_server,
                "dest_db": restore_dest_db,
                "dest_auth": restore_dest_auth,
                "dest_user": restore_dest_user,
                "dest_password": restore_dest_password,
                "backup_path": str(restore_backup_path),
                "restore_programmables": cfg["restore_programmables"],
                "restore_constraints": cfg["restore_constraints"],
                "restore_indexes": cfg["restore_indexes"],
                "continue_on_error": cfg["restore_continue_on_error"],
                "dry_run": cfg["restore_dry_run"],
            }

            log_msg(f"Restoring schema to: {restore_dest_server} | {restore_dest_db}")
            restore_summary = run_restore(restore_cfg)

            summary["steps"]["restore"]["status"] = restore_summary["status"]
            summary["steps"]["restore"]["run_id"] = restore_summary["run_id"]

            if restore_summary["status"] != "success":
                summary["errors"].extend(restore_summary.get("errors", []))
                log_msg(f"⚠ Restore completed with errors. Status: {restore_summary['status']}")
            else:
                log_msg("✓ Restore completed successfully")
        else:
            log_msg("Skipping schema restore (--skip-restore)")

        # Determine overall status
        all_steps = [s["status"] for s in summary["steps"].values() if s["status"] != "skipped"]
        if all(s == "success" for s in all_steps):
            summary["status"] = "success"
        elif any(s == "failed" for s in all_steps):
            summary["status"] = "failed"
        else:
            summary["status"] = "completed_with_errors"

    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Full migration failed: %s", msg)
        if gui_log_callback:
            try:
                gui_log_callback(f"✗ Full migration failed: {msg}")
            except:
                pass
        summary["status"] = "failed"
        summary["errors"].append(msg)

    finally:
        summary["ended_utc"] = utc_iso()
        summary["duration_seconds"] = round(time.time() - start, 3)

        summary_file = meta_dir / "full_migration_summary.json"
        summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        log_msg("")
        log_msg("=" * 60)
        log_msg("FULL MIGRATION COMPLETE")
        log_msg("=" * 60)
        log_msg(f"Status: {summary['status']}")
        log_msg(f"Duration: {summary['duration_seconds']} seconds")
        logger.info("Summary JSON: %s", str(summary_file.resolve()))
        logger.info("Log file: %s", str(log_file.resolve()))

        if summary["errors"]:
            log_msg(f"Errors encountered: {len(summary['errors'])}")
            for err in summary["errors"][:5]:  # Show first 5 errors
                log_msg(f"  - {err}")
            if len(summary["errors"]) > 5:
                log_msg(f"  ... and {len(summary['errors']) - 5} more errors")

    return summary

