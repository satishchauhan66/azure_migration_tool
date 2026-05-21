# Author: S@tish Chauhan

"""Schema restore functionality."""

import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import pyodbc

from ..utils.azure_compat import (
    EXPECTED_SKIP_REASONS,
    classify_expected_skip,
    detect_azure_sql_target,
    filter_azure_incompatible_batches,
    is_expected_skip_inventory_file,
    detect_azure_engine_edition,
    is_azure_managed_instance_edition,
    should_apply_azure_batch_filter,
    should_skip_already_exists_error,
    should_skip_azure_error,
    should_skip_default_constraint_error,
    should_skip_index_error,
    should_skip_windows_principal_error,
)
from ..utils.database import build_conn_str, pick_sql_driver, resolve_password, connect_to_database
from ..utils.logging import setup_logger
from ..utils.paths import app_data_dir, short_slug, utc_iso, utc_ts_compact
from ..utils.sql import normalize_alter_database_current, prepare_sql_batches
from .nullability_fix import apply_nullability_fixes


# Comprehensive restore sequence (file_key, file_type, cfg flag group or None = always in full mirror).
# flag group: tables | programmables | constraints | indexes | security
BUILTIN_RESTORE_ORDER: list[tuple[str, str, Optional[str]]] = [
    ("server_logins_file", "SERVER_LOGINS", None),
    ("database_options_file", "DATABASE_OPTIONS", None),
    ("filegroups_file", "FILEGROUPS", None),
    ("schemas_file", "SCHEMAS", None),
    ("user_defined_types_file", "USER_DEFINED_TYPES", None),
    ("memory_optimized_filegroup_file", "MEMORY_OPTIMIZED_FILEGROUP", None),
    ("assemblies_file", "ASSEMBLIES", None),
    ("database_credentials_file", "DATABASE_CREDENTIALS", None),
    ("external_resources_file", "EXTERNAL_RESOURCES", None),
    ("partitioning_file", "PARTITIONING", None),
    ("xml_schema_collections_file", "XML_SCHEMA_COLLECTIONS", None),
    ("legacy_rules_defaults_file", "LEGACY_RULES_DEFAULTS", None),
    ("service_broker_file", "SERVICE_BROKER", None),
    ("sequences_file", "SEQUENCES", None),
    ("synonyms_file", "SYNONYMS", None),
    ("replication_file", "REPLICATION", None),
    ("graph_file", "GRAPH", None),
    ("tables_file", "TABLES", "tables"),
    ("tables_no_pk_file", "TABLES (without PKs)", "tables"),
    ("column_collation_file", "COLUMN_COLLATIONS", "constraints"),
    ("table_storage_file", "TABLE_STORAGE", "constraints"),
    ("table_options_file", "TABLE_OPTIONS", "constraints"),
    ("primary_keys_file", "PRIMARY_KEYS", "constraints"),
    ("database_diagrams_file", "DATABASE_DIAGRAMS", "programmables"),
    ("views_file", "VIEWS", "programmables"),
    ("procedures_file", "PROCEDURES", "programmables"),
    ("clr_procedures_file", "CLR_PROCEDURES", "programmables"),
    ("functions_file", "FUNCTIONS", "programmables"),
    ("external_tables_file", "EXTERNAL_TABLES", "programmables"),
    ("triggers_file", "TRIGGERS", "programmables"),
    ("ddl_triggers_file", "DDL_TRIGGERS", "programmables"),
    ("plan_guides_file", "PLAN_GUIDES", "programmables"),
    ("indexes_file", "INDEXES", "indexes"),
    ("specialized_indexes_file", "SPECIALIZED_INDEXES", "indexes"),
    ("index_options_file", "INDEX_OPTIONS", "indexes"),
    ("unique_constraints_file", "UNIQUE_CONSTRAINTS", "indexes"),
    ("check_constraints_file", "CHECK_CONSTRAINTS", "constraints"),
    ("default_constraints_file", "DEFAULT_CONSTRAINTS", "constraints"),
    ("foreign_keys_file", "FOREIGN_KEYS", "constraints"),
    ("fulltext_file", "FULLTEXT", "indexes"),
    ("statistics_file", "STATISTICS", "indexes"),
    ("security_policies_file", "SECURITY_POLICIES", "programmables"),
    ("change_tracking_file", "CHANGE_TRACKING", "programmables"),
    ("cdc_file", "CDC", "programmables"),
    ("database_principals_file", "DATABASE_PRINCIPALS", "security"),
    ("role_memberships_file", "ROLE_MEMBERSHIPS", "security"),
    ("schema_authorization_file", "SCHEMA_AUTHORIZATION", "security"),
    ("permissions_file", "PERMISSIONS", "security"),
    ("column_permissions_file", "COLUMN_PERMISSIONS", "security"),
    ("always_encrypted_file", "ALWAYS_ENCRYPTED", "security"),
    ("data_masking_file", "DATA_MASKING", "security"),
    ("cryptographic_objects_file", "CRYPTOGRAPHIC_OBJECTS", "security"),
    ("audit_specifications_file", "AUDIT_SPECIFICATIONS", "security"),
    ("sequence_current_values_file", "SEQUENCE_VALUES", "programmables"),
    ("extended_properties_file", "EXTENDED_PROPERTIES", None),
    ("encrypted_modules_file", "ENCRYPTED_MODULES", None),
]

# Maps meta/restore_order.json path entries to get_backup_paths() keys.
_MANIFEST_PATH_TO_FILE_KEY: dict[str, str] = {
    "meta/server_logins.sql": "server_logins_file",
    "meta/server_logins.sql (master)": "server_logins_file",
    "meta/database_diagrams.sql": "database_diagrams_file",
    "meta/encrypted_modules.sql": "encrypted_modules_file",
    "00_foundation/database_options.sql": "database_options_file",
    "00_foundation/filegroups.sql": "filegroups_file",
    "00_foundation/schemas.sql": "schemas_file",
    "00_foundation/user_defined_types.sql": "user_defined_types_file",
    "00_foundation/memory_optimized_filegroup.sql": "memory_optimized_filegroup_file",
    "00_foundation/assemblies.sql": "assemblies_file",
    "00_foundation/database_credentials.sql": "database_credentials_file",
    "00_foundation/external_resources.sql": "external_resources_file",
    "00_foundation/partitioning.sql": "partitioning_file",
    "00_foundation/xml_schema_collections.sql": "xml_schema_collections_file",
    "01_tables_all.sql": "tables_file",
    "01_tables_no_pk.sql": "tables_no_pk_file",
    "schema/01_tables_all.sql": "tables_file",
    "schema/01_tables_no_pk.sql": "tables_no_pk_file",
    "02_programmables/sequences.sql": "sequences_file",
    "02_programmables/synonyms.sql": "synonyms_file",
    "02_programmables/service_broker.sql": "service_broker_file",
    "02_programmables/replication.sql": "replication_file",
    "02_programmables/graph.sql": "graph_file",
    "02_programmables/views.sql": "views_file",
    "02_programmables/procedures.sql": "procedures_file",
    "02_programmables/procedures/": "_procedures_dir_",
    "02_programmables/clr_procedures.sql": "clr_procedures_file",
    "02_programmables/functions.sql": "functions_file",
    "02_programmables/external_tables.sql": "external_tables_file",
    "02_programmables/triggers.sql": "triggers_file",
    "02_programmables/ddl_triggers.sql": "ddl_triggers_file",
    "02_programmables/plan_guides.sql": "plan_guides_file",
    "02_programmables/legacy_rules_defaults.sql": "legacy_rules_defaults_file",
    "02_programmables/security_policies.sql": "security_policies_file",
    "02_programmables/change_tracking.sql": "change_tracking_file",
    "02_programmables/cdc.sql": "cdc_file",
    "02_programmables/sequence_current_values.sql": "sequence_current_values_file",
    "03_constraints_indexes/column_collation.sql": "column_collation_file",
    "03_constraints_indexes/table_storage.sql": "table_storage_file",
    "03_constraints_indexes/table_options.sql": "table_options_file",
    "03_constraints_indexes/primary_keys.sql": "primary_keys_file",
    "03_constraints_indexes/indexes.sql": "indexes_file",
    "03_constraints_indexes/specialized_indexes.sql": "specialized_indexes_file",
    "03_constraints_indexes/index_options.sql": "index_options_file",
    "03_constraints_indexes/unique_constraints.sql": "unique_constraints_file",
    "03_constraints_indexes/check_constraints.sql": "check_constraints_file",
    "03_constraints_indexes/default_constraints.sql": "default_constraints_file",
    "03_constraints_indexes/foreign_keys.sql": "foreign_keys_file",
    "03_constraints_indexes/fulltext.sql": "fulltext_file",
    "03_constraints_indexes/statistics.sql": "statistics_file",
    "03_constraints_indexes/extended_properties.sql": "extended_properties_file",
    "04_security/database_principals.sql": "database_principals_file",
    "04_security/role_memberships.sql": "role_memberships_file",
    "04_security/schema_authorization.sql": "schema_authorization_file",
    "04_security/permissions.sql": "permissions_file",
    "04_security/column_permissions.sql": "column_permissions_file",
    "04_security/cryptographic_objects.sql": "cryptographic_objects_file",
    "04_security/always_encrypted.sql": "always_encrypted_file",
    "04_security/data_masking.sql": "data_masking_file",
    "04_security/audit_specifications.sql": "audit_specifications_file",
}


def normalize_full_mirror_cfg(cfg: dict) -> dict:
    """Enable all restore flags and mirror mode for a full sequential mirror restore."""
    if not cfg.get("full_mirror", False):
        return cfg
    merged = dict(cfg)
    merged["restore_tables"] = True
    merged["restore_programmables"] = True
    merged["restore_constraints"] = True
    merged["restore_indexes"] = True
    merged["restore_security"] = True
    merged["restore_primary_keys"] = True
    merged["mirror_source"] = True
    return merged


def run_full_mirror_restore(cfg: dict) -> dict:
    """Restore every discovered backup object in manifest order (expected Azure gaps logged as skips)."""
    return run_restore({**cfg, "full_mirror": True})


def _cfg_allows_restore_group(cfg: dict, group: Optional[str]) -> bool:
    if group is None:
        return True
    if group == "tables":
        return bool(cfg.get("restore_tables", False))
    if group == "programmables":
        return bool(cfg.get("restore_programmables", False))
    if group == "constraints":
        return bool(cfg.get("restore_constraints", False))
    if group == "indexes":
        return bool(cfg.get("restore_indexes", False))
    if group == "security":
        return bool(cfg.get("restore_security", True))
    return True


def load_manifest_restore_keys(backup_path: Path) -> Optional[list[str]]:
    """Load file_key order from meta/restore_order.json when present."""
    manifest_path = backup_path / "meta" / "restore_order.json"
    if not manifest_path.is_file():
        return None
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        order = data.get("order") or []
        keys: list[str] = []
        for entry in order:
            entry = (entry or "").strip()
            if not entry:
                continue
            key = _MANIFEST_PATH_TO_FILE_KEY.get(entry)
            if key:
                keys.append(key)
        return keys or None
    except Exception:
        return None


def build_restore_order(
    backup_path: Path,
    backup_paths: dict,
    cfg: dict,
    logger,
) -> list[tuple[str, str, Path]]:
    """Build ordered list of (file_key, file_type, path) to restore."""
    full_mirror = bool(cfg.get("full_mirror", False))
    restore_order: list[tuple[str, str, Path]] = []
    seen_keys: set[str] = set()

    def append_file(file_key: str, file_type: str, path: Path) -> None:
        if file_key in seen_keys:
            return
        seen_keys.add(file_key)
        restore_order.append((file_key, file_type, path))

    def append_from_spec(file_key: str, file_type: str, group: Optional[str]) -> None:
        if file_key in seen_keys:
            return
        if file_key == "primary_keys_file" and not effective_restore_primary_keys(cfg):
            return
        if not _cfg_allows_restore_group(cfg, group):
            return
        if file_key == "tables_file" and cfg.get("use_tables_no_pk", False):
            return
        if file_key == "tables_no_pk_file" and not cfg.get("use_tables_no_pk", False) and "tables_file" in backup_paths:
            return
        if file_key not in backup_paths:
            return
        path = backup_paths[file_key]
        if isinstance(path, Path) and path.is_file():
            append_file(file_key, file_type, path)

    manifest_keys = load_manifest_restore_keys(backup_path)
    spec_by_key = {fk: (fk, ft, grp) for fk, ft, grp in BUILTIN_RESTORE_ORDER}

    if manifest_keys:
        for file_key in manifest_keys:
            if file_key == "_procedures_dir_":
                proc_files = backup_paths.get("procedure_files")
                if proc_files and _cfg_allows_restore_group(cfg, "programmables"):
                    for proc_path in proc_files:
                        append_file(f"procedure:{proc_path.stem}", "PROCEDURE", proc_path)
                continue
            spec = spec_by_key.get(file_key)
            if spec:
                append_from_spec(*spec)
    else:
        for file_key, file_type, group in BUILTIN_RESTORE_ORDER:
            if file_key == "procedures_file" and backup_paths.get("procedure_files"):
                if _cfg_allows_restore_group(cfg, "programmables"):
                    for proc_path in backup_paths["procedure_files"]:
                        append_file(f"procedure:{proc_path.stem}", "PROCEDURE", proc_path)
                continue
            append_from_spec(file_key, file_type, group)

    # Per-procedure directory when not already expanded from manifest/builtin
    if backup_paths.get("procedure_files") and not any(k.startswith("procedure:") for k, _, _ in restore_order):
        if _cfg_allows_restore_group(cfg, "programmables"):
            for proc_path in backup_paths["procedure_files"]:
                append_file(f"procedure:{proc_path.stem}", "PROCEDURE", proc_path)
            logger.info(
                "Restoring %d procedure file(s) from %s",
                len(backup_paths["procedure_files"]),
                backup_paths.get("procedures_dir", "procedures/"),
            )

    # Nullability fix after tables when using tables_no_pk
    if (
        cfg.get("restore_tables", False)
        and cfg.get("use_tables_no_pk", False)
        and cfg.get("fix_nullability", True)
    ):
        ref_tables = backup_paths.get("tables_file") or backup_paths.get("tables_no_pk_file")
        if ref_tables and ("nullability_fix", "NULLABILITY_FIX", ref_tables) not in [
            (a, b, c) for a, b, c in restore_order
        ]:
            insert_at = next(
                (i + 1 for i, (k, _, _) in enumerate(restore_order) if k.startswith("tables")),
                len(restore_order),
            )
            restore_order.insert(insert_at, ("nullability_fix", "NULLABILITY_FIX", ref_tables))

    return restore_order


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

    foundation_dir = schema_dir / "00_foundation"
    prog_dir = schema_dir / "02_programmables"
    cx_dir = schema_dir / "03_constraints_indexes"
    security_dir = schema_dir / "04_security"

    paths = {}

    if foundation_dir.exists():
        for key, fname in (
            ("schemas_file", "schemas.sql"),
            ("user_defined_types_file", "user_defined_types.sql"),
            ("external_resources_file", "external_resources.sql"),
            ("partitioning_file", "partitioning.sql"),
            ("filegroups_file", "filegroups.sql"),
            ("database_credentials_file", "database_credentials.sql"),
            ("database_options_file", "database_options.sql"),
            ("xml_schema_collections_file", "xml_schema_collections.sql"),
            ("assemblies_file", "assemblies.sql"),
            ("memory_optimized_filegroup_file", "memory_optimized_filegroup.sql"),
        ):
            p = foundation_dir / fname
            if p.exists():
                paths[key] = p

    meta_dir = backup_path / "meta"
    if meta_dir.exists():
        for key, fname in (
            ("server_logins_file", "server_logins.sql"),
            ("database_diagrams_file", "database_diagrams.sql"),
            ("encrypted_modules_file", "encrypted_modules.sql"),
        ):
            p = meta_dir / fname
            if p.exists():
                paths[key] = p
    
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
        procedures_dir = prog_dir / "procedures"
        if procedures_dir.is_dir():
            proc_files = sorted(procedures_dir.glob("*.sql"))
            if proc_files:
                paths["procedures_dir"] = procedures_dir
                paths["procedure_files"] = proc_files
        procedures_file = prog_dir / "procedures.sql"
        if procedures_file.exists() and "procedure_files" not in paths:
            paths["procedures_file"] = procedures_file
        clr_procedures = prog_dir / "clr_procedures.sql"
        if clr_procedures.exists():
            paths["clr_procedures_file"] = clr_procedures
        paths["functions_file"] = prog_dir / "functions.sql"
        paths["triggers_file"] = prog_dir / "triggers.sql"
        for key, fname in (
            ("ddl_triggers_file", "ddl_triggers.sql"),
            ("security_policies_file", "security_policies.sql"),
            ("plan_guides_file", "plan_guides.sql"),
            ("legacy_rules_defaults_file", "legacy_rules_defaults.sql"),
            ("service_broker_file", "service_broker.sql"),
            ("external_tables_file", "external_tables.sql"),
            ("change_tracking_file", "change_tracking.sql"),
            ("cdc_file", "cdc.sql"),
            ("replication_file", "replication.sql"),
            ("graph_file", "graph.sql"),
            ("sequence_current_values_file", "sequence_current_values.sql"),
        ):
            p = prog_dir / fname
            if p.exists():
                paths[key] = p

    if cx_dir.exists():
        for key, fname in (
            ("foreign_keys_file", "foreign_keys.sql"),
            ("check_constraints_file", "check_constraints.sql"),
            ("default_constraints_file", "default_constraints.sql"),
            ("indexes_file", "indexes.sql"),
            ("primary_keys_file", "primary_keys.sql"),
            ("extended_properties_file", "extended_properties.sql"),
            ("unique_constraints_file", "unique_constraints.sql"),
            ("statistics_file", "statistics.sql"),
            ("fulltext_file", "fulltext.sql"),
            ("table_options_file", "table_options.sql"),
            ("column_collation_file", "column_collation.sql"),
            ("table_storage_file", "table_storage.sql"),
            ("specialized_indexes_file", "specialized_indexes.sql"),
            ("index_options_file", "index_options.sql"),
        ):
            p = cx_dir / fname
            if p.exists():
                paths[key] = p

    if security_dir.exists():
        for key, fname in (
            ("database_principals_file", "database_principals.sql"),
            ("permissions_file", "permissions.sql"),
            ("role_memberships_file", "role_memberships.sql"),
            ("cryptographic_objects_file", "cryptographic_objects.sql"),
            ("audit_specifications_file", "audit_specifications.sql"),
            ("always_encrypted_file", "always_encrypted.sql"),
            ("data_masking_file", "data_masking.sql"),
            ("column_permissions_file", "column_permissions.sql"),
            ("schema_authorization_file", "schema_authorization.sql"),
        ):
            p = security_dir / fname
            if p.exists():
                paths[key] = p

    # Discover any additional foundation SQL not in the static map (forward-compatible).
    if foundation_dir.exists():
        known_foundation = {
            foundation_dir / fname
            for _, fname in (
                ("schemas_file", "schemas.sql"),
                ("user_defined_types_file", "user_defined_types.sql"),
                ("external_resources_file", "external_resources.sql"),
                ("partitioning_file", "partitioning.sql"),
                ("filegroups_file", "filegroups.sql"),
                ("database_credentials_file", "database_credentials.sql"),
                ("database_options_file", "database_options.sql"),
                ("xml_schema_collections_file", "xml_schema_collections.sql"),
                ("assemblies_file", "assemblies.sql"),
                ("memory_optimized_filegroup_file", "memory_optimized_filegroup.sql"),
            )
        }
        extra = sorted(
            p for p in foundation_dir.glob("*.sql")
            if p.is_file() and p not in known_foundation and p not in paths.values()
        )
        if extra:
            paths["extra_foundation_files"] = extra

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
    mirror_source: bool = False,  # If True, attempt all batches (no Azure filter); get mirror of source
    azure_target: bool = False,  # Azure SQL DB/MI — filter platform-incompatible batches
    azure_engine_edition: Optional[int] = None,  # 5=Azure SQL DB, 8=Managed Instance (CLR on MI)
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
        "expected_skips": 0,
        "expected_skip_reasons": [],
        "errors": [],
        "warnings": [],  # Track warnings (e.g., already exists)
        "duration_seconds": None,
    }

    if is_expected_skip_inventory_file(file_type):
        result["status"] = "expected_skip_inventory"
        result["expected_skips"] = 1
        reason = classify_expected_skip(file_type=file_type) or "encrypted_module_inventory"
        result["expected_skip_reasons"].append(
            {"reason": reason, "detail": EXPECTED_SKIP_REASONS.get(reason, reason)}
        )
        logger.info(
            "Skipping %s (%s): %s",
            sql_file.name,
            file_type,
            EXPECTED_SKIP_REASONS.get(reason, reason),
        )
        result["duration_seconds"] = 0.0
        return result

    if not sql_file.exists():
        result["status"] = "skipped"
        result["errors"].append(f"File not found: {sql_file}")
        logger.warning("File not found: %s", sql_file)
        return result

    t0 = time.time()

    try:
        sql_text = sql_file.read_text(encoding="utf-8")
        if file_type == "DATABASE_OPTIONS":
            sql_text = normalize_alter_database_current(sql_text)
        batches = prepare_sql_batches(sql_text, file_type=file_type)
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

        logger.info(
            "Executing %s: %d batches (mirror_source=%s, azure_target=%s)",
            sql_file.name,
            len(batches),
            mirror_source,
            azure_target,
        )

        if should_apply_azure_batch_filter(
            mirror_source, azure_target, file_type, engine_edition=azure_engine_edition
        ):
            compatible_batches = filter_azure_incompatible_batches(
                batches, logger, file_type=file_type, engine_edition=azure_engine_edition
            )
            result["batches_filtered"] = len(batches) - len(compatible_batches)
            if result["batches_filtered"] > 0:
                result["expected_skips"] += result["batches_filtered"]
                reason = classify_expected_skip(file_type=file_type) or "azure_unsupported_feature"
                if reason not in [r.get("reason") for r in result["expected_skip_reasons"]]:
                    result["expected_skip_reasons"].append(
                        {"reason": reason, "detail": EXPECTED_SKIP_REASONS.get(reason, reason)}
                    )
                logger.info(
                    "After Azure compatibility filter: %d/%d batches compatible (%d expected skips)",
                    len(compatible_batches),
                    len(batches),
                    result["batches_filtered"],
                )
        else:
            compatible_batches = [
                (i, batch.strip(), [])
                for i, batch in enumerate(batches, 1)
                if batch and batch.strip()
            ]
            result["batches_filtered"] = 0

        use_autocommit = file_type == "DATABASE_OPTIONS"
        prev_autocommit = conn.autocommit
        if use_autocommit:
            conn.autocommit = True

        try:
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
                    if not use_autocommit:
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
                    elif should_skip_windows_principal_error(error_str):
                        should_skip = True
                        skip_reason = "Windows principal not portable to Azure SQL MI"
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
                    else:
                        expected_on_error = classify_expected_skip(
                            error_msg=error_str,
                            file_type=file_type,
                            batch_text=batch,
                        )
                        if expected_on_error:
                            should_skip = True
                            skip_reason = EXPECTED_SKIP_REASONS.get(
                                expected_on_error, expected_on_error
                            )

                    if should_skip:
                        # Track if this is an "already exists" skip vs other skip
                        is_already_exists = "already exists" in skip_reason.lower() or "already has" in skip_reason.lower()
                        expected_key = classify_expected_skip(
                            error_msg=error_str,
                            file_type=file_type,
                            skip_reason=skip_reason,
                            batch_text=batch,
                        )
                        is_expected = expected_key is not None

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
                        elif is_expected:
                            logger.info(
                                "%s | Batch %d/%d expected skip (%s): %s",
                                sql_file.name,
                                orig_idx,
                                len(batches),
                                EXPECTED_SKIP_REASONS.get(expected_key, expected_key),
                                error_str[:200],
                            )
                            result["expected_skips"] += 1
                            if expected_key not in [r.get("reason") for r in result["expected_skip_reasons"]]:
                                result["expected_skip_reasons"].append(
                                    {
                                        "reason": expected_key,
                                        "detail": EXPECTED_SKIP_REASONS.get(expected_key, expected_key),
                                    }
                                )
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

                    if not use_autocommit:
                        try:
                            conn.rollback()
                        except Exception:
                            pass

                    if not continue_on_error:
                        raise
        finally:
            if use_autocommit:
                conn.autocommit = prev_autocommit

        if result["batches_failed"] == 0:
            if result["expected_skips"] > 0 and result["batches_skipped"] == result["expected_skips"]:
                result["status"] = "success_with_expected_skips"
            else:
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
            f"expected_skips={result['expected_skips']}",
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
                "  -> %d object(s) already existed (skipped gracefully)",
                result["batches_already_existed"]
            )
        if result["batches_failed"] > 0:
            logger.warning(
                "  -> %d batch(es) failed (see errors above)",
                result["batches_failed"]
            )

    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Failed to execute %s: %s", sql_file.name, msg)
        result["status"] = "failed"
        result["errors"].append(msg)
        result["duration_seconds"] = round(time.time() - t0, 3)

    return result


def effective_restore_primary_keys(cfg: dict) -> bool:
    """
    Whether to run primary_keys.sql.

    If restore_primary_keys is set explicitly, that wins. Otherwise PKs are applied only when
    restoring constraints and/or indexes — so a tables-only restore leaves heaps for bulk load;
    a second restore (or full restore with those flags) adds PKs before FKs/index work.
    """
    if "restore_primary_keys" in cfg:
        return bool(cfg["restore_primary_keys"])
    return bool(cfg.get("restore_constraints", False) or cfg.get("restore_indexes", False))


def run_restore(cfg: dict):
    """Run schema restore with provided configuration"""
    cfg = normalize_full_mirror_cfg(cfg)
    run_id = utc_ts_compact()

    # Use project path if provided, otherwise user-writable app data dir
    data_root = Path(cfg["project_path"]) if cfg.get("project_path") else app_data_dir()

    # Determine backup path
    backup_path = None
    if cfg["backup_path"]:
        backup_path = Path(cfg["backup_path"])
        if not backup_path.exists():
            raise ValueError(f"Backup path does not exist: {backup_path}")
    else:
        # Try to find latest backup
        backup_root = data_root / "backups"
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
    restore_root = data_root / "restores" / run_id
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
    logger.info("Full mirror restore: %s", cfg.get("full_mirror", False))
    logger.info("Restore tables: %s", cfg.get("restore_tables", False))
    logger.info("Restore programmables: %s", cfg["restore_programmables"])
    logger.info("Restore constraints: %s", cfg["restore_constraints"])
    logger.info("Restore indexes: %s", cfg["restore_indexes"])
    logger.info("Restore security: %s", cfg.get("restore_security", True))
    logger.info("Restore primary keys: %s (effective)", effective_restore_primary_keys(cfg))
    logger.info("Continue on error: %s", cfg["continue_on_error"])
    logger.info("Mirror source (no Azure filter): %s", cfg.get("mirror_source", False))
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
        auth = (cfg.get("dest_auth") or "windows").strip().lower()

        # For MFA (entra_mfa), use a single connection: connect to target DB first. If that fails
        # (DB does not exist), connect to master to create it, then connect to target. This avoids
        # two connections in quick succession which can cause "connection forcibly closed" /
        # "Login failed for token-identified principal" on Azure SQL.
        conn = None
        if auth == "entra_mfa":
            logger.info("MFA auth: connecting to target database (single connection)...")
            try:
                conn = connect_to_database(
                    server=cfg["dest_server"],
                    db=cfg["dest_db"],
                    user=cfg["dest_user"],
                    driver=driver,
                    auth=cfg["dest_auth"],
                    password=password,
                    timeout=60,
                    logger=logger,
                )
                logger.info("Connected to target database.")
            except Exception as e:
                err_str = str(e).lower()
                if "4060" in err_str or "cannot open database" in err_str or "does not exist" in err_str:
                    logger.info("Target database not found; creating via master then reconnecting.")
                    ensure_database_exists(
                        cfg["dest_server"], cfg["dest_db"], cfg["dest_user"],
                        driver, cfg["dest_auth"], password, logger
                    )
                    time.sleep(2)
                    conn = connect_to_database(
                        server=cfg["dest_server"],
                        db=cfg["dest_db"],
                        user=cfg["dest_user"],
                        driver=driver,
                        auth=cfg["dest_auth"],
                        password=password,
                        timeout=60,
                        logger=logger,
                    )
                else:
                    raise

        if conn is None:
            # Non-MFA: ensure DB exists then connect
            logger.info("Checking if destination database exists...")
            ensure_database_exists(
                cfg["dest_server"], cfg["dest_db"], cfg["dest_user"],
                driver, cfg["dest_auth"], password, logger
            )
            logger.info("Connecting to destination (auth=%s)...", cfg["dest_auth"])
            conn = connect_to_database(
                server=cfg["dest_server"],
                db=cfg["dest_db"],
                user=cfg["dest_user"],
                driver=driver,
                auth=cfg["dest_auth"],
                password=password,
                timeout=30,
                logger=logger,
            )

        with conn:
            conn.timeout = 0
            cur = conn.cursor()

            cur.execute("SELECT DB_NAME(), SUSER_SNAME(), GETDATE();")
            db_name, login_name, server_time = cur.fetchone()
            logger.info("Connected. DB=%s Login=%s ServerTime=%s", db_name, login_name, server_time)

            azure_target = cfg.get("azure_target")
            if azure_target is None:
                azure_target = detect_azure_sql_target(cur, cfg["dest_server"])
            cfg["azure_target"] = bool(azure_target)
            cfg["azure_engine_edition"] = detect_azure_engine_edition(cur, cfg["dest_server"])
            if azure_target:
                if is_azure_managed_instance_edition(cfg.get("azure_engine_edition")):
                    logger.info(
                        "Azure SQL Managed Instance detected — CLR assemblies/types will be restored; "
                        "Windows principals and server-level batches remain filtered"
                    )
                else:
                    logger.info("Azure SQL target detected — CLR/assemblies and legacy DDL batches will be filtered")

            # If restoring tables, ensure all required schemas exist first
            tables_sql_path = None
            if cfg.get("restore_tables", False):
                use_no_pk = cfg.get("use_tables_no_pk", False)
                if use_no_pk and "tables_no_pk_file" in backup_paths:
                    tables_sql_path = backup_paths["tables_no_pk_file"]
                elif "tables_file" in backup_paths:
                    tables_sql_path = backup_paths["tables_file"]
                elif "tables_no_pk_file" in backup_paths:
                    tables_sql_path = backup_paths["tables_no_pk_file"]
            if tables_sql_path is not None:
                logger.info("Extracting schemas from tables SQL file...")
                tables_sql = tables_sql_path.read_text(encoding="utf-8")
                required_schemas = extract_schemas_from_sql(tables_sql)
                if required_schemas:
                    logger.info("Found schemas in backup: %s", ", ".join(sorted(required_schemas)))
                    ensure_schemas_exist(cur, conn, required_schemas, logger)
                else:
                    logger.info("No custom schemas found (using default schemas)")

            restore_order = build_restore_order(backup_path, backup_paths, cfg, logger)
            summary["restore_sequence"] = [f"{fk}:{ft}" for fk, ft, _ in restore_order]
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
                    mirror_source=cfg.get("mirror_source", False),
                    azure_target=cfg.get("azure_target", False),
                    azure_engine_edition=cfg.get("azure_engine_edition"),
                )
                summary["files_restored"][file_key] = result

                if result["status"] == "failed" and not cfg["continue_on_error"]:
                    raise RuntimeError(f"Stopping due to failure in {file_type}")

                if result["errors"] and result["status"] not in (
                    "expected_skip_inventory",
                    "success_with_expected_skips",
                ):
                    summary["errors"].extend([f"{file_type}: {e}" for e in result["errors"]])

        # Calculate overall statistics
        total_batches = 0
        total_executed = 0
        total_already_existed = 0
        total_failed = 0
        total_skipped = 0
        total_filtered = 0
        total_expected_skips = 0
        expected_skip_reasons: dict[str, str] = {}
        
        for file_result in summary["files_restored"].values():
            total_batches += file_result.get("batches_total", 0)
            total_executed += file_result.get("batches_executed", 0) - file_result.get("batches_skipped", 0)
            total_already_existed += file_result.get("batches_already_existed", 0)
            total_failed += file_result.get("batches_failed", 0)
            total_skipped += file_result.get("batches_skipped", 0)
            total_filtered += file_result.get("batches_filtered", 0)
            total_expected_skips += file_result.get("expected_skips", 0)
            for entry in file_result.get("expected_skip_reasons") or []:
                if entry.get("reason"):
                    expected_skip_reasons[entry["reason"]] = entry.get("detail") or EXPECTED_SKIP_REASONS.get(
                        entry["reason"], entry["reason"]
                    )
        
        summary["statistics"] = {
            "total_batches": total_batches,
            "batches_executed": total_executed,
            "batches_already_existed": total_already_existed,
            "batches_failed": total_failed,
            "batches_skipped": total_skipped,
            "batches_filtered_azure": total_filtered,
            "expected_skips": total_expected_skips,
        }
        if expected_skip_reasons:
            summary["expected_skips"] = {
                "count": total_expected_skips,
                "reasons": expected_skip_reasons,
            }
        if total_filtered > 0 or total_expected_skips > 0:
            summary["expected_skip_note"] = (
                f"{total_expected_skips} expected skip(s): passwords/login secrets, encrypted modules (inventory), "
                "CLR assemblies, and server/master batches not controllable on Azure SQL."
            )
        if total_filtered > 0:
            summary["azure_filter_note"] = (
                f"{total_filtered} batch(es) were filtered as Azure SQL incompatible (e.g. CREATE LOGIN, USE master, "
                "syslogins, CLR). These count as expected skips, not failures."
            )
        
        if not summary["errors"]:
            if total_failed > 0:
                summary["status"] = "completed_with_errors"
            elif total_expected_skips > 0:
                summary["status"] = "success_with_expected_skips"
            else:
                summary["status"] = "success"
        else:
            summary["status"] = "completed_with_errors"

    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Restore failed: %s", msg)
        summary["status"] = "failed"
        summary["errors"].append(msg)
        if "token-identified principal" in msg or ("18456" in msg and "Login failed" in msg):
            hint = (
                "Hint: For Azure SQL with Microsoft account (MFA), try: "
                "(1) ensure server firewall allows your IP, (2) retry (token may have expired), "
                "(3) use SQL Server authentication if available."
            )
            summary["errors"].append(hint)
            logger.warning("%s", hint)

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
            if stats.get("expected_skips", 0) > 0:
                logger.info("  Expected skips (not failures): %d", stats["expected_skips"])
                if summary.get("expected_skip_note"):
                    logger.info("  Note: %s", summary["expected_skip_note"])
            if stats.get("batches_filtered_azure", 0) > 0:
                logger.info("  Azure-incompatible (filtered): %d", stats["batches_filtered_azure"])
                if summary.get("azure_filter_note"):
                    logger.info("  Note: %s", summary["azure_filter_note"])
        
        logger.info("Summary JSON: %s", str(summary_file.resolve()))

        if summary["errors"]:
            logger.warning("Errors encountered: %d (see restore_summary.json)", len(summary["errors"]))

    return summary

