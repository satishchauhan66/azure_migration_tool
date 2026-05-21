# Author: S@tish Chauhan

"""Full user-object schema exporters (mirror backup completeness)."""

import binascii
from typing import List, Optional, Tuple

from ..utils.paths import qident
from .exporters import _fetch_unique_constraint_columns, object_definition, wrap_create_or_alter
from .sql_catalog_compat import ENCRYPTED_MODULE_INVENTORY_SQL


def _go(lines: List[str]) -> str:
    return "\n".join(lines) + ("\n" if lines else "")


def export_filegroups(cur) -> str:
    cur.execute(
        """
        SELECT fg.name, fg.type_desc, fg.is_default
        FROM sys.filegroups fg
        WHERE fg.is_default = 0
        ORDER BY fg.name;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        out.append(f"-- Filegroup [{row.name}]")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.filegroups WHERE name = N'{row.name}')")
        out.append("BEGIN")
        out.append(f"    ALTER DATABASE CURRENT ADD FILEGROUP {qident(row.name)};")
        out.append("END")
        out.append("GO")
        out.append("")
    return _go(out)


def export_database_credentials(cur) -> str:
    cur.execute(
        """
        SELECT name, credential_identity, create_date
        FROM sys.database_credentials
        ORDER BY name;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        ident = (row.credential_identity or "").replace("'", "''")
        out.append(f"-- Database scoped credential [{row.name}]")
        out.append(
            f"-- Restore: set SECRET to match source (not exported). IDENTITY = N'{ident}'"
        )
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.database_credentials WHERE name = N'{row.name}')")
        out.append("BEGIN")
        out.append(
            f"    CREATE DATABASE SCOPED CREDENTIAL {qident(row.name)} "
            f"WITH IDENTITY = N'{ident}', SECRET = N'***REPLACE_SECRET***';"
        )
        out.append("END")
        out.append("GO")
        out.append("")
    return _go(out)


def export_database_options(cur) -> str:
    cur.execute(
        """
        SELECT
            d.name,
            d.compatibility_level,
            d.collation_name,
            d.recovery_model_desc,
            d.page_verify_option_desc,
            d.is_auto_close_on,
            d.is_auto_shrink_on,
            d.is_read_only,
            d.is_auto_create_stats_on,
            d.is_auto_update_stats_on
        FROM sys.databases d
        WHERE d.name = DB_NAME();
        """
    )
    row = cur.fetchone()
    if not row:
        return ""
    out = [
        f"-- Database options (source: [{row.name}]; applied to connected database)",
        "-- Azure SQL Database / Managed Instance: the following options are not supported",
        "-- via ALTER DATABASE and are skipped on restore (see database_options.sql comments).",
        f"-- COMPATIBILITY_LEVEL = {row.compatibility_level}",
        f"-- RECOVERY {row.recovery_model_desc}",
        f"-- PAGE_VERIFY {row.page_verify_option_desc}",
        f"-- is_read_only = {1 if row.is_read_only else 0}",
        f"-- is_auto_close_on = {1 if row.is_auto_close_on else 0}",
        f"-- is_auto_shrink_on = {1 if row.is_auto_shrink_on else 0}",
        f"-- Collation: {row.collation_name} (change only at CREATE DATABASE if needed)",
        "",
    ]
    if row.is_read_only:
        out.insert(
            4,
            "ALTER DATABASE CURRENT SET READ_ONLY ON;",
        )
        out.insert(5, "GO")
        out.insert(6, "")
    return _go(out)


def export_role_memberships(cur) -> str:
    cur.execute(
        """
        SELECT
            role_p.name AS role_name,
            member_p.name AS member_name
        FROM sys.database_role_members drm
        JOIN sys.database_principals role_p ON role_p.principal_id = drm.role_principal_id
        JOIN sys.database_principals member_p ON member_p.principal_id = drm.member_principal_id
        WHERE role_p.is_fixed_role = 0
          AND member_p.principal_id > 4
        ORDER BY role_p.name, member_p.name;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        out.append(f"-- {row.member_name} -> {row.role_name}")
        out.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.database_role_members drm "
            f"JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id "
            f"JOIN sys.database_principals m ON m.principal_id = drm.member_principal_id "
            f"WHERE r.name = N'{row.role_name}' AND m.name = N'{row.member_name}')"
        )
        out.append("BEGIN")
        out.append(f"    ALTER ROLE {qident(row.role_name)} ADD MEMBER {qident(row.member_name)};")
        out.append("END")
        out.append("GO")
        out.append("")
    return _go(out)


def export_cryptographic_objects(cur, logger=None) -> str:
    out: List[str] = []

    # Certificates (public only)
    try:
        cur.execute(
            """
            SELECT name, certificate_id
            FROM sys.certificates
            WHERE pvt_key_encryption_type IS NULL OR pvt_key_encryption_type = 'NA'
            ORDER BY name;
            """
        )
        for row in cur.fetchall():
            cur.execute(
                "SELECT CAST(CERTENCODED(?) AS VARBINARY(MAX));",
                row.certificate_id,
            )
            enc = cur.fetchone()
            if enc and enc[0]:
                hexval = "0x" + binascii.hexlify(enc[0]).decode("ascii")
                out.append(f"CREATE CERTIFICATE {qident(row.name)} FROM BINARY = {hexval};")
                out.append("GO")
                out.append("")
    except Exception as ex:
        if logger:
            logger.warning("Certificate export partial: %s", ex)

    # Symmetric / asymmetric keys — metadata only (secrets not extractable)
    for catalog, label in (
        ("sys.symmetric_keys", "SYMMETRIC KEY"),
        ("sys.asymmetric_keys", "ASYMMETRIC KEY"),
    ):
        try:
            cur.execute(f"SELECT name FROM {catalog} ORDER BY name;")
            for row in cur.fetchall():
                out.append(
                    f"-- {label} [{row.name}]: recreate manually with same algorithm and password on target"
                )
                out.append("")
        except Exception:
            pass

    return _go(out)


def export_database_audit_specifications(cur) -> str:
    try:
        cur.execute(
            """
            SELECT name, object_id, is_state_enabled
            FROM sys.database_audit_specifications
            ORDER BY name;
            """
        )
    except Exception:
        return ""
    out: List[str] = []
    for row in cur.fetchall():
        defn = object_definition(cur, row.object_id)
        if defn:
            out.append(defn.strip())
            out.append("GO")
            out.append("")
    return _go(out)


def export_assemblies_and_clr_types(cur, logger=None) -> Tuple[str, List[str]]:
    warnings: List[str] = []
    out: List[str] = []

    try:
        cur.execute(
            """
            SELECT a.name, a.assembly_id, af.file_id, af.name AS file_name
            FROM sys.assemblies a
            JOIN sys.assembly_files af ON af.assembly_id = a.assembly_id
            WHERE a.is_user_defined = 1
            ORDER BY a.name, af.file_id;
            """
        )
        current_asm = None
        for row in cur.fetchall():
            if row.name != current_asm:
                current_asm = row.name
                out.append(f"-- Assembly [{row.name}]")
            cur.execute(
                "SELECT CAST(content AS VARBINARY(MAX)) FROM sys.assembly_files WHERE assembly_id = ? AND file_id = ?;",
                row.assembly_id,
                row.file_id,
            )
            blob = cur.fetchone()
            if blob and blob[0]:
                hexval = "0x" + binascii.hexlify(blob[0]).decode("ascii")
                out.append(
                    f"-- CREATE ASSEMBLY {qident(row.name)} FROM {hexval} ... "
                    f"(permission_set / visibility — verify on restore)"
                )
                out.append("")
            else:
                warnings.append(f"Assembly {row.name}: binary not readable")
    except Exception as ex:
        if logger:
            logger.warning("Assembly export skipped: %s", ex)

    # CLR types
    try:
        cur.execute(
            """
            SELECT s.name AS schema_name, t.name AS type_name, t.user_type_id
            FROM sys.types t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.is_user_defined = 1 AND t.is_assembly_type = 1
            ORDER BY s.name, t.name;
            """
        )
        for row in cur.fetchall():
            out.append(f"-- CLR type {row.schema_name}.{row.type_name} (requires assembly on target)")
            out.append("")
    except Exception:
        pass

    return _go(out), warnings


def export_external_tables(cur, logger=None) -> str:
    try:
        cur.execute(
            """
            SELECT s.name AS schema_name, et.name AS table_name, et.object_id
            FROM sys.external_tables et
            JOIN sys.schemas s ON s.schema_id = et.schema_id
            ORDER BY s.name, et.name;
            """
        )
    except Exception:
        return ""
    out: List[str] = []
    for row in cur.fetchall():
        defn = object_definition(cur, row.object_id)
        if defn:
            out.append(defn.strip())
            out.append("GO")
            out.append("")
        elif logger:
            logger.warning("External table %s.%s: no definition", row.schema_name, row.table_name)
    return _go(out)


def export_xml_schema_collections(cur) -> str:
    try:
        cur.execute(
            """
            SELECT s.name AS schema_name, x.name AS collection_name, x.xml_collection_id
            FROM sys.xml_schema_collections x
            JOIN sys.schemas s ON s.schema_id = x.schema_id
            WHERE x.xml_collection_id > 1
            ORDER BY s.name, x.name;
            """
        )
    except Exception:
        return ""
    out: List[str] = []
    for row in cur.fetchall():
        cur.execute(
            """
            SELECT CAST(xml_component_xml AS NVARCHAR(MAX))
            FROM sys.xml_schema_components
            WHERE xml_collection_id = ?
            ORDER BY xml_component_id;
            """,
            row.xml_collection_id,
        )
        parts = [r[0] for r in cur.fetchall() if r[0]]
        if parts:
            xml_body = "\n".join(parts).replace("'", "''")
            out.append(f"-- XML schema collection {row.schema_name}.{row.collection_name}")
            out.append(
                f"CREATE XML SCHEMA COLLECTION {qident(row.schema_name)}.{qident(row.collection_name)} "
                f"AS N'{xml_body}';"
            )
            out.append("GO")
            out.append("")
    return _go(out)


def export_fulltext(cur) -> str:
    out: List[str] = []
    try:
        cur.execute("SELECT name, is_default FROM sys.fulltext_catalogs ORDER BY name;")
        for row in cur.fetchall():
            out.append(f"CREATE FULLTEXT CATALOG {qident(row.name)} AS DEFAULT;")
            out.append("GO")
            out.append("")
    except Exception:
        return ""

    try:
        cur.execute(
            """
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                i.name AS index_name,
                fc.name AS catalog_name
            FROM sys.fulltext_indexes fti
            JOIN sys.tables t ON t.object_id = fti.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.indexes i ON i.object_id = fti.object_id AND i.index_id = fti.unique_index_id
            JOIN sys.fulltext_catalogs fc ON fc.fulltext_catalog_id = fti.fulltext_catalog_id
            ORDER BY s.name, t.name;
            """
        )
        for row in cur.fetchall():
            cur.execute(
                """
                SELECT c.name
                FROM sys.fulltext_index_columns fic
                JOIN sys.columns c ON c.object_id = fic.object_id AND c.column_id = fic.column_id
                WHERE fic.object_id = OBJECT_ID(?, 'U')
                ORDER BY fic.language_id, c.column_id;
                """,
                f"{row.schema_name}.{row.table_name}",
            )
            cols = [qident(r.name) for r in cur.fetchall()]
            if cols:
                table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
                out.append(
                    f"CREATE FULLTEXT INDEX ON {table} ({', '.join(cols)}) "
                    f"KEY INDEX {qident(row.index_name)} ON {qident(row.catalog_name)};"
                )
                out.append("GO")
                out.append("")
    except Exception:
        pass
    return _go(out)


def export_statistics(cur, logger=None) -> str:
    cur.execute(
        """
        SELECT
            st.name AS stats_name,
            OBJECT_SCHEMA_NAME(st.object_id) AS schema_name,
            OBJECT_NAME(st.object_id) AS table_name,
            st.object_id,
            st.stats_id,
            st.user_created
        FROM sys.stats st
        INNER JOIN sys.tables t ON t.object_id = st.object_id
        WHERE t.is_ms_shipped = 0
          AND (st.user_created = 1 OR st.name NOT LIKE '_WA[_]%')
        ORDER BY schema_name, table_name, st.name;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        if (row.stats_name or "").startswith("_WA_") and not row.user_created:
            continue
        cur.execute(
            """
            SELECT c.name, sc.stats_column_id
            FROM sys.stats_columns sc
            JOIN sys.columns c ON c.object_id = sc.object_id AND c.column_id = sc.column_id
            WHERE sc.object_id = ? AND sc.stats_id = ?
            ORDER BY sc.stats_column_id;
            """,
            row.object_id,
            row.stats_id,
        )
        cols = [qident(r.name) for r in cur.fetchall() if r.name]
        if not cols:
            continue
        table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
        out.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE object_id = OBJECT_ID(N'{row.schema_name}.{row.table_name}') "
            f"AND name = N'{row.stats_name}')"
        )
        out.append("BEGIN")
        out.append(
            f"    CREATE STATISTICS {qident(row.stats_name)} ON {table} ({', '.join(cols)});"
        )
        out.append("END")
        out.append("GO")
        out.append("")
    return _go(out)


def export_table_options(cur, logger=None) -> str:
    """Temporal tables, memory-optimized, compression hints via ALTER."""
    try:
        cur.execute(
            """
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                t.temporal_type,
                t.history_table_id,
                t.is_memory_optimized,
                ht_s.name AS history_schema,
                ht.name AS history_table
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            LEFT JOIN sys.tables ht ON ht.object_id = t.history_table_id
            LEFT JOIN sys.schemas ht_s ON ht_s.schema_id = ht.schema_id
            WHERE t.is_ms_shipped = 0
              AND (t.temporal_type = 2 OR t.is_memory_optimized = 1);
            """
        )
    except Exception as ex:
        if logger:
            logger.info("Table options export skipped (catalog not available): %s", ex)
        return ""
    out: List[str] = []
    for row in cur.fetchall():
        table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
        if row.is_memory_optimized:
            out.append(f"-- Table {table} is memory-optimized; enable MEMORY_OPTIMIZED filegroup on target first")
            out.append(
                f"ALTER TABLE {table} SET (MEMORY_OPTIMIZED = ON);"
            )
            out.append("GO")
            out.append("")
        if row.temporal_type == 2 and row.history_table:
            try:
                cur.execute(
                    """
                    SELECT c.name
                    FROM sys.periods p
                    JOIN sys.columns c ON c.object_id = p.object_id AND c.column_id = p.start_column_id
                    WHERE p.object_id = OBJECT_ID(?, 'U');
                    """,
                    f"{row.schema_name}.{row.table_name}",
                )
                start_row = cur.fetchone()
                cur.execute(
                    """
                    SELECT c.name
                    FROM sys.periods p
                    JOIN sys.columns c ON c.object_id = p.object_id AND c.column_id = p.end_column_id
                    WHERE p.object_id = OBJECT_ID(?, 'U');
                    """,
                    f"{row.schema_name}.{row.table_name}",
                )
                end_row = cur.fetchone()
                if start_row and end_row:
                    hist = f"{qident(row.history_schema)}.{qident(row.history_table)}"
                    out.append(f"-- Enable system-versioning on {table}")
                    out.append(
                        f"ALTER TABLE {table} ADD PERIOD FOR SYSTEM_TIME "
                        f"({qident(start_row.name)}, {qident(end_row.name)});"
                    )
                    out.append(
                        f"ALTER TABLE {table} SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {hist}));"
                    )
                    out.append("GO")
                    out.append("")
            except Exception:
                if logger:
                    logger.warning("Temporal table %s: period columns not exported", table)
    return _go(out)


def export_service_broker(cur, logger=None) -> str:
    out: List[str] = []
    queries = [
        ("message types", "SELECT object_id, name FROM sys.service_message_types WHERE is_ms_shipped = 0"),
        ("contracts", "SELECT object_id, name FROM sys.service_contracts WHERE is_ms_shipped = 0"),
        ("queues", "SELECT object_id, SCHEMA_NAME(schema_id) AS schema_name, name FROM sys.service_queues WHERE is_ms_shipped = 0"),
        ("services", "SELECT object_id, name FROM sys.services WHERE is_ms_shipped = 0"),
        ("routes", "SELECT object_id, name FROM sys.routes"),
    ]
    for label, sql in queries:
        try:
            cur.execute(sql)
            for row in cur.fetchall():
                oid = row.object_id
                defn = object_definition(cur, oid)
                if defn:
                    out.append(f"-- Service Broker {label}: {getattr(row, 'name', '')}")
                    out.append(defn.strip())
                    out.append("GO")
                    out.append("")
        except Exception as ex:
            if logger:
                logger.debug("Service Broker %s: %s", label, ex)
    return _go(out)


_SYSDIAGRAMS_TABLE_DDL = """
-- dbo.sysdiagrams (required before diagram stored procedures)
IF OBJECT_ID(N'dbo.sysdiagrams', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.sysdiagrams
    (
        name sysname NOT NULL,
        principal_id int NOT NULL,
        diagram_id int IDENTITY(1,1) NOT NULL,
        version int NULL,
        definition varbinary(max) NULL,
        CONSTRAINT UK_principal_name UNIQUE NONCLUSTERED (principal_id, name),
        CONSTRAINT PK__sysdiagrams PRIMARY KEY CLUSTERED (diagram_id)
    );
END
GO
"""


def export_database_diagrams(cur) -> str:
    out: List[str] = [_SYSDIAGRAMS_TABLE_DDL.strip(), ""]
    try:
        cur.execute(
            """
            SELECT diagram_id, name, principal_id, version, definition
            FROM dbo.sysdiagrams
            ORDER BY diagram_id;
            """
        )
    except Exception:
        return _go(out)
    for row in cur.fetchall():
        if not row.definition:
            continue
        hexval = "0x" + binascii.hexlify(row.definition).decode("ascii")
        name = (row.name or "").replace("'", "''")
        out.append(f"-- Database diagram [{row.name}]")
        out.append(
            f"IF NOT EXISTS (SELECT 1 FROM dbo.sysdiagrams WHERE name = N'{name}')"
        )
        out.append("BEGIN")
        out.append(
            f"    INSERT INTO dbo.sysdiagrams (name, principal_id, version, definition) "
            f"VALUES (N'{name}', {row.principal_id}, {row.version}, {hexval});"
        )
        out.append("END")
        out.append("GO")
        out.append("")
    try:
        cur.execute(
            """
            SELECT kc.name AS constraint_name, kc.parent_object_id, kc.unique_index_id
            FROM sys.key_constraints kc
            WHERE kc.parent_object_id = OBJECT_ID(N'dbo.sysdiagrams') AND kc.type = 'UQ';
            """
        )
        for kc in cur.fetchall():
            cols = _fetch_unique_constraint_columns(cur, kc.parent_object_id, kc.unique_index_id)
            if not cols:
                continue
            out.append(f"-- UNIQUE dbo.sysdiagrams ({kc.constraint_name})")
            out.append(
                "IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'"
                f"{kc.constraint_name}' AND parent_object_id = OBJECT_ID(N'dbo.sysdiagrams'))"
            )
            out.append("BEGIN")
            out.append(
                f"    ALTER TABLE dbo.sysdiagrams ADD CONSTRAINT {qident(kc.constraint_name)} "
                f"UNIQUE ({', '.join(cols)});"
            )
            out.append("END")
            out.append("GO")
            out.append("")
    except Exception:
        pass
    return _go(out)


def export_change_tracking(cur) -> str:
    out: List[str] = []
    try:
        cur.execute(
            """
            SELECT d.is_change_tracking_on, d.change_tracking_retention_period,
                   d.change_tracking_retention_period_units_desc
            FROM sys.databases d WHERE d.name = DB_NAME();
            """
        )
        db = cur.fetchone()
        if db and db.is_change_tracking_on:
            out.append(
                f"ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON "
                f"(CHANGE_RETENTION = {db.change_tracking_retention_period} {db.change_tracking_retention_period_units_desc}, AUTO_CLEANUP = ON);"
            )
            out.append("GO")
            out.append("")
        cur.execute(
            """
            SELECT s.name AS schema_name, t.name AS table_name, t.is_track_columns_updated_on
            FROM sys.change_tracking_tables ct
            JOIN sys.tables t ON t.object_id = ct.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id;
            """
        )
        for row in cur.fetchall():
            table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
            track_cols = "ON" if row.is_track_columns_updated_on else "OFF"
            out.append(f"ALTER TABLE {table} ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = {track_cols});")
            out.append("GO")
            out.append("")
    except Exception:
        pass
    return _go(out)


def export_cdc(cur) -> str:
    out: List[str] = []
    try:
        cur.execute(
            """
            SELECT s.name AS schema_name, t.name AS table_name
            FROM cdc.change_tables ct
            JOIN sys.tables t ON t.object_id = ct.source_object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id;
            """
        )
        for row in cur.fetchall():
            out.append(
                f"EXEC sys.sp_cdc_enable_table @source_schema = N'{row.schema_name}', "
                f"@source_name = N'{row.table_name}', @role_name = NULL, @supports_net_changes = 1;"
            )
            out.append("GO")
            out.append("")
        cur.execute("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME();")
        r = cur.fetchone()
        if r and r.is_cdc_enabled:
            out.insert(0, "EXEC sys.sp_cdc_enable_db;\nGO\n\n")
    except Exception:
        pass
    return _go(out)


def export_server_logins(cur) -> str:
    """Requires connection to master."""
    cur.execute(
        """
        SELECT
            sp.name,
            sp.type_desc,
            sp.default_database_name,
            sp.is_disabled
        FROM sys.server_principals sp
        WHERE sp.type IN ('S', 'U', 'G')
          AND sp.name NOT LIKE '##%'
          AND sp.name NOT LIKE 'NT %'
        ORDER BY sp.name;
        """
    )
    out: List[str] = [
        "-- Server logins (run in master on target; passwords/secrets not exported)",
        "",
    ]
    for row in cur.fetchall():
        if row.type_desc == "SQL_LOGIN":
            out.append(
                f"-- CREATE LOGIN {qident(row.name)} WITH PASSWORD = N'***', "
                f"DEFAULT_DATABASE = {qident(row.default_database_name or 'master')}, CHECK_POLICY = OFF;"
            )
        else:
            out.append(f"-- Login {row.name} ({row.type_desc}): create manually on target")
        out.append("")
    return _go(out)


def export_module_inventory(cur, logger=None) -> str:
    """List encrypted modules that cannot be scripted (for manual follow-up)."""
    try:
        cur.execute(ENCRYPTED_MODULE_INVENTORY_SQL)
    except Exception as ex:
        if logger:
            logger.info("Encrypted module inventory skipped: %s", ex)
        return ""
    rows = cur.fetchall()
    if not rows:
        return ""
    out = ["-- Encrypted modules (definitions not exportable — deploy from source control)", ""]
    for row in rows:
        out.append(f"-- {row.type_desc}: {row.schema_name}.{row.object_name}")
    out.append("")
    return _go(out)
