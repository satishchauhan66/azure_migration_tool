# Author: S@tish Chauhan

"""Catalog scan and gap report for schema backup coverage (user objects only)."""

from typing import Any, Dict, List, Tuple

from .sql_catalog_compat import ENCRYPTED_MODULE_WHERE


# User-relevant categories we aim to script (excludes MS-shipped / system)
EXPORT_CATEGORIES = [
    ("schemas", "sys.schemas", "schema_id > 4"),
    ("tables", "sys.tables", "is_ms_shipped = 0"),
    ("views", "sys.views", "is_ms_shipped = 0"),
    ("procedures", "sys.procedures", "is_ms_shipped = 0"),
    ("clr_modules", "sys.objects", "type IN ('PC','FS','FT','AF') AND is_ms_shipped = 0"),
    ("functions", "sys.objects", "type IN ('FN','IF','TF') AND is_ms_shipped = 0"),
    ("synonyms", "sys.synonyms", "1=1"),
    ("sequences", "sys.sequences", "1=1"),
    ("table_types", "sys.table_types", "1=1"),
    ("alias_types", "sys.types", "is_user_defined = 1 AND is_table_type = 0 AND is_assembly_type = 0"),
    ("dml_triggers", "sys.triggers", "parent_class = 1 AND is_ms_shipped = 0"),
    ("ddl_triggers", "sys.triggers", "parent_class = 0 AND is_ms_shipped = 0"),
    ("foreign_keys", "sys.foreign_keys", "1=1"),
    ("check_constraints", "sys.check_constraints", "parent_object_id > 0"),
    ("default_constraints", "sys.default_constraints", "parent_object_id > 0"),
    ("primary_keys", "sys.key_constraints", "type = 'PK'"),
    ("unique_constraints", "sys.key_constraints", "type = 'UQ'"),
    ("indexes_non_pk_uq", "sys.indexes", "index_id > 0 AND is_primary_key = 0 AND is_unique_constraint = 0"),
    ("security_policies", "sys.security_policies", "1=1"),
    ("database_users", "sys.database_principals", "type IN ('S','U','G','E','X') AND principal_id > 4"),
    ("database_roles", "sys.database_principals", "type = 'R' AND is_fixed_role = 0"),
    ("role_memberships", "sys.database_role_members", "1=1"),
    ("database_permissions", "sys.database_permissions", "class <> 0"),
    ("extended_properties", "sys.extended_properties", "class > 0"),
    ("external_data_sources", "sys.external_data_sources", "1=1"),
    ("external_file_formats", "sys.external_file_formats", "1=1"),
    ("external_tables", "sys.external_tables", "1=1"),
    ("partition_functions", "sys.partition_functions", "1=1"),
    ("partition_schemes", "sys.partition_schemes", "1=1"),
    ("filegroups", "sys.filegroups", "1=1"),
    ("database_credentials", "sys.database_credentials", "1=1"),
    ("assemblies", "sys.assemblies", "is_user_defined = 1"),
    ("xml_schema_collections", "sys.xml_schema_collections", "xml_collection_id > 1"),
    ("fulltext_catalogs", "sys.fulltext_catalogs", "1=1"),
    ("plan_guides", "sys.plan_guides", "is_ms_shipped = 0"),
    ("database_diagrams", "dbo.sysdiagrams", "1=1"),
    ("certificates", "sys.certificates", "1=1"),
    ("symmetric_keys", "sys.symmetric_keys", "1=1"),
    ("asymmetric_keys", "sys.asymmetric_keys", "1=1"),
    ("database_audit_specs", "sys.database_audit_specifications", "1=1"),
    ("encrypted_modules", "sys.sql_modules", ENCRYPTED_MODULE_WHERE),
]

# Maps category -> backup artifact (file stem). None = not yet scripted.
CATEGORY_EXPORT_MAP = {
    "schemas": "00_foundation/schemas.sql",
    "alias_types": "00_foundation/user_defined_types.sql",
    "table_types": "00_foundation/user_defined_types.sql",
    "assemblies": "00_foundation/assemblies.sql",
    "external_data_sources": "00_foundation/external_resources.sql",
    "external_file_formats": "00_foundation/external_resources.sql",
    "partition_functions": "00_foundation/partitioning.sql",
    "partition_schemes": "00_foundation/partitioning.sql",
    "filegroups": "00_foundation/filegroups.sql",
    "database_credentials": "00_foundation/database_credentials.sql",
    "xml_schema_collections": "00_foundation/xml_schema_collections.sql",
    "sequences": "02_programmables/sequences.sql",
    "synonyms": "02_programmables/synonyms.sql",
    "views": "02_programmables/views.sql",
    "procedures": "02_programmables/procedures.sql",
    "clr_modules": "02_programmables/clr_procedures.sql",
    "functions": "02_programmables/functions.sql",
    "dml_triggers": "02_programmables/triggers.sql",
    "ddl_triggers": "02_programmables/ddl_triggers.sql",
    "security_policies": "02_programmables/security_policies.sql",
    "plan_guides": "02_programmables/plan_guides.sql",
    "external_tables": "02_programmables/external_tables.sql",
    "tables": "01_tables_all.sql",
    "primary_keys": "03_constraints_indexes/primary_keys.sql",
    "foreign_keys": "03_constraints_indexes/foreign_keys.sql",
    "check_constraints": "03_constraints_indexes/check_constraints.sql",
    "default_constraints": "03_constraints_indexes/default_constraints.sql",
    "indexes_non_pk_uq": "03_constraints_indexes/indexes.sql",
    "unique_constraints": "03_constraints_indexes/unique_constraints.sql",
    "extended_properties": "03_constraints_indexes/extended_properties.sql",
    "database_users": "04_security/database_principals.sql",
    "database_roles": "04_security/database_principals.sql",
    "role_memberships": "04_security/role_memberships.sql",
    "database_permissions": "04_security/permissions.sql",
    "certificates": "04_security/cryptographic_objects.sql",
    "symmetric_keys": "04_security/cryptographic_objects.sql",
    "asymmetric_keys": "04_security/cryptographic_objects.sql",
    "database_audit_specs": "04_security/audit_specifications.sql",
    "database_diagrams": "meta/database_diagrams.sql",
    "encrypted_modules": "meta/encrypted_modules.sql",
}

# Gaps we script in remaining_exporters (new files)
REMAINING_EXPORT_FILES = {
    "column_collation": "03_constraints_indexes/column_collation.sql",
    "table_storage": "03_constraints_indexes/table_storage.sql",
    "specialized_indexes": "03_constraints_indexes/specialized_indexes.sql",
    "index_options": "03_constraints_indexes/index_options.sql",
    "always_encrypted": "04_security/always_encrypted.sql",
    "data_masking": "04_security/data_masking.sql",
    "column_permissions": "04_security/column_permissions.sql",
    "schema_authorization": "04_security/schema_authorization.sql",
    "replication": "02_programmables/replication.sql",
    "graph": "02_programmables/graph.sql",
    "memory_optimized_filegroup": "00_foundation/memory_optimized_filegroup.sql",
    "service_broker_full": "02_programmables/service_broker.sql",
    "sequence_restart": "02_programmables/sequence_current_values.sql",
}

# Cannot export (document only)
KNOWN_LIMITATIONS = [
    "login_passwords",
    "credential_secrets",
    "symmetric_key_passwords",
    "certificate_private_keys_without_password",
    "encrypted_module_definitions",
    "table_row_data",
    "physical_database_files",
]


def scan_catalog_counts(cur) -> Dict[str, int]:
    """Count user-relevant objects per category."""
    counts: Dict[str, int] = {}
    for key, table, where in EXPORT_CATEGORIES:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where};")
            counts[key] = int(cur.fetchone()[0] or 0)
        except Exception:
            counts[key] = -1
    return counts


def build_gap_report(cur, exported_files: Dict[str, bool]) -> Dict[str, Any]:
    """
    Build JSON-serializable gap report.
    exported_files: map of relative path -> whether file was written (non-empty).
    """
    counts = scan_catalog_counts(cur)
    still_missing_logic: List[Dict[str, str]] = []
    covered: List[str] = []

    for key, count in counts.items():
        if count < 0:
            still_missing_logic.append(
                {"category": key, "reason": "catalog_query_failed", "count": count}
            )
            continue
        if count == 0:
            continue
        artifact = CATEGORY_EXPORT_MAP.get(key)
        remaining = REMAINING_EXPORT_FILES.get(key)
        target = remaining or artifact
        if target and exported_files.get(target):
            covered.append(key)
        elif target:
            still_missing_logic.append(
                {
                    "category": key,
                    "count": count,
                    "expected_file": target,
                    "reason": "export_file_empty_or_missing",
                }
            )
        else:
            still_missing_logic.append(
                {"category": key, "count": count, "reason": "no_exporter_mapped"}
            )

    # Feature gaps detected in catalog (even if base export exists)
    feature_gaps = _detect_feature_gaps(cur)

    return {
        "catalog_counts": counts,
        "covered_categories": covered,
        "gaps_empty_or_missing_file": still_missing_logic,
        "feature_gaps_addressed_by_remaining_exporters": feature_gaps,
        "known_limitations_not_exportable": KNOWN_LIMITATIONS,
        "remaining_export_files": REMAINING_EXPORT_FILES,
    }


def _detect_feature_gaps(cur) -> List[Dict[str, Any]]:
    """Detect features present in DB that need dedicated exporters."""
    gaps: List[Dict[str, Any]] = []

    checks: List[Tuple[str, str, str]] = [
        (
            "column_collation",
            """
            SELECT COUNT(*) FROM sys.columns c
            JOIN sys.tables t ON t.object_id = c.object_id
            WHERE t.is_ms_shipped = 0 AND c.collation_name IS NOT NULL
              AND c.collation_name <> DATABASEPROPERTYEX(DB_NAME(), 'Collation');
            """,
        ),
        (
            "columnstore_indexes",
            """
            SELECT COUNT(*) FROM sys.indexes i
            JOIN sys.tables t ON t.object_id = i.object_id
            WHERE t.is_ms_shipped = 0 AND i.type IN (5, 6);
            """,
        ),
        (
            "xml_spatial_indexes",
            """
            SELECT COUNT(*) FROM sys.indexes i
            JOIN sys.objects o ON o.object_id = i.object_id
            WHERE o.is_ms_shipped = 0 AND i.type IN (3, 4);
            """,
        ),
        (
            "table_partition_scheme",
            """
            SELECT COUNT(*) FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
            JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
            WHERE t.is_ms_shipped = 0 AND ds.type = 'PS';
            """,
        ),
        (
            "data_compression",
            """
            SELECT COUNT(*) FROM sys.partitions p
            JOIN sys.tables t ON t.object_id = p.object_id
            WHERE t.is_ms_shipped = 0 AND p.data_compression > 0;
            """,
        ),
        (
            "always_encrypted",
            """
            SELECT COUNT(*) FROM sys.column_encryption_keys;
            """,
        ),
        (
            "dynamic_data_masking",
            """
            SELECT COUNT(*) FROM sys.masked_columns;
            """,
        ),
        (
            "graph_tables",
            """
            SELECT COUNT(*) FROM sys.tables WHERE is_node = 1 OR is_edge = 1;
            """,
        ),
        (
            "memory_optimized",
            """
            SELECT COUNT(*) FROM sys.tables WHERE is_memory_optimized = 1 AND is_ms_shipped = 0;
            """,
        ),
        (
            "replication_articles",
            """
            SELECT COUNT(*) FROM sys.articles;
            """,
        ),
        (
            "column_level_permissions",
            """
            SELECT COUNT(*) FROM sys.database_permissions
            WHERE minor_id > 0 AND class = 1;
            """,
        ),
    ]

    for name, sql in checks:
        try:
            cur.execute(sql)
            n = int(cur.fetchone()[0] or 0)
            if n > 0:
                gaps.append(
                    {
                        "feature": name,
                        "count": n,
                        "exporter_file": _feature_to_file(name),
                    }
                )
        except Exception:
            pass

    return gaps


def _feature_to_file(feature: str) -> str:
    mapping = {
        "column_collation": REMAINING_EXPORT_FILES.get("column_collation", ""),
        "columnstore_indexes": REMAINING_EXPORT_FILES["specialized_indexes"],
        "xml_spatial_indexes": REMAINING_EXPORT_FILES["specialized_indexes"],
        "table_partition_scheme": REMAINING_EXPORT_FILES["table_storage"],
        "data_compression": REMAINING_EXPORT_FILES["table_storage"],
        "always_encrypted": REMAINING_EXPORT_FILES["always_encrypted"],
        "dynamic_data_masking": REMAINING_EXPORT_FILES["data_masking"],
        "graph_tables": REMAINING_EXPORT_FILES["graph"],
        "memory_optimized": REMAINING_EXPORT_FILES["memory_optimized_filegroup"],
        "replication_articles": REMAINING_EXPORT_FILES["replication"],
        "column_level_permissions": REMAINING_EXPORT_FILES["column_permissions"],
    }
    return mapping.get(feature, "")
