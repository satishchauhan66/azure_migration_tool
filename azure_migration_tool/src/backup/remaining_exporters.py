# Author: S@tish Chauhan

"""Exporters for schema features not covered by base/mirror/full exporters."""

import binascii
from typing import List, Optional, Tuple

from ..utils.paths import qident
from .exporters import object_definition


def _go(lines: List[str]) -> str:
    return "\n".join(lines) + ("\n" if lines else "")


def export_column_collation(cur) -> str:
    """ALTER COLUMN COLLATE for columns that differ from database default."""
    cur.execute("SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS NVARCHAR(128));")
    db_collation = (cur.fetchone()[0] or "").strip()
    if not db_collation:
        return ""

    cur.execute(
        """
        SELECT s.name AS schema_name, t.name AS table_name, c.name AS column_name,
               c.collation_name, ty.name AS type_name
        FROM sys.columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        WHERE t.is_ms_shipped = 0
          AND c.collation_name IS NOT NULL
          AND c.collation_name <> ?
          AND ty.name IN ('char','varchar','nchar','nvarchar','text','ntext');
        """,
        db_collation,
    )
    out: List[str] = []
    for row in cur.fetchall():
        table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
        out.append(
            f"ALTER TABLE {table} ALTER COLUMN {qident(row.column_name)} "
            f"{row.type_name} COLLATE {row.collation_name};"
        )
        out.append("GO")
        out.append("")
    return _go(out)


def export_table_storage(cur, logger=None) -> str:
    """Partition scheme placement, data compression, lock escalation."""
    out: List[str] = []

    # Tables on partition schemes (clustered index / heap data_space)
    cur.execute(
        """
        SELECT s.name AS schema_name, t.name AS table_name, ps.name AS partition_scheme,
               c.name AS partition_column
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
        JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
        JOIN sys.partition_schemes ps ON ps.data_space_id = ds.data_space_id
        LEFT JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            AND ic.partition_ordinal = 1
        LEFT JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE t.is_ms_shipped = 0;
        """
    )
    for row in cur.fetchall():
        table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
        if row.partition_column:
            out.append(
                f"-- Recreate or move {table} onto partition scheme (run after tables exist)"
            )
            out.append(
                f"-- Intended: ON {qident(row.partition_scheme)}({qident(row.partition_column)})"
            )
            out.append("")

    # Data compression per partition
    try:
        cur.execute(
            """
            SELECT s.name AS schema_name, t.name AS table_name, p.partition_number,
                   p.data_compression_desc
            FROM sys.partitions p
            JOIN sys.tables t ON t.object_id = p.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.is_ms_shipped = 0 AND p.data_compression > 0
            ORDER BY s.name, t.name, p.partition_number;
            """
        )
        for row in cur.fetchall():
            table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
            comp = row.data_compression_desc
            if row.partition_number:
                out.append(
                    f"ALTER TABLE {table} REBUILD PARTITION = {row.partition_number} "
                    f"WITH (DATA_COMPRESSION = {comp});"
                )
            else:
                out.append(f"ALTER TABLE {table} REBUILD WITH (DATA_COMPRESSION = {comp});")
            out.append("GO")
            out.append("")
    except Exception as ex:
        if logger:
            logger.warning("Compression export: %s", ex)

    return _go(out)


def export_specialized_indexes(cur, logger=None) -> Tuple[str, List[str]]:
    """Columnstore, XML, and spatial indexes."""
    warnings: List[str] = []
    out: List[str] = []

    cur.execute(
        """
        SELECT s.name AS schema_name, o.name AS object_name, i.name AS index_name,
               i.type, i.type_desc, o.type AS obj_type
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE o.is_ms_shipped = 0 AND i.type IN (3, 4, 5, 6) AND i.name IS NOT NULL;
        """
    )
    for row in cur.fetchall():
        obj = f"{qident(row.schema_name)}.{qident(row.object_name)}"
        obj_kind = "V" if (row.obj_type or "").strip().upper() == "V" else "U"
        fqn = f"{row.schema_name}.{row.object_name}"
        cur.execute(
            """
            SELECT c.name, ic.is_included_column, ic.key_ordinal
            FROM sys.index_columns ic
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE ic.object_id = OBJECT_ID(?, ?)
              AND ic.index_id = (
                  SELECT index_id FROM sys.indexes
                  WHERE object_id = OBJECT_ID(?, ?) AND name = ?
              )
            ORDER BY ic.key_ordinal, ic.index_column_id;
            """,
            fqn,
            obj_kind,
            fqn,
            obj_kind,
            row.index_name,
        )
        cols = [qident(r.name) for r in cur.fetchall() if r.name and not r.is_included_column]
        if not cols and row.type not in (5, 6):
            warnings.append(f"Index {row.schema_name}.{row.object_name}.{row.index_name}: no columns")
            continue

        out.append(f"-- {row.type_desc} on {obj}")
        if row.type == 5:
            out.append(f"CREATE CLUSTERED COLUMNSTORE INDEX {qident(row.index_name)} ON {obj};")
        elif row.type == 6:
            out.append(
                f"CREATE NONCLUSTERED COLUMNSTORE INDEX {qident(row.index_name)} ON {obj} ({', '.join(cols)});"
            )
        elif row.type == 3:
            out.append(f"CREATE PRIMARY XML INDEX {qident(row.index_name)} ON {obj};")
        elif row.type == 4:
            out.append(
                f"CREATE SPATIAL INDEX {qident(row.index_name)} ON {obj}({cols[0]}) "
                f"USING GEOMETRY_GRID WITH (BOUNDING_BOX = (0,0,0,0)); -- adjust BOUNDING_BOX on target"
            )
        out.append("GO")
        out.append("")

    return _go(out), warnings


def export_index_options(cur) -> str:
    """Index properties: fillfactor, pad_index, ignore_dup_key, etc."""
    cur.execute(
        """
        SELECT s.name AS schema_name, t.name AS table_name, i.name AS index_name,
               i.fill_factor, i.is_padded, i.ignore_dup_key, i.allow_row_locks, i.allow_page_locks
        FROM sys.indexes i
        JOIN sys.tables t ON t.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.is_ms_shipped = 0 AND i.index_id > 0
          AND (i.fill_factor > 0 AND i.fill_factor < 100 OR i.is_padded = 1 OR i.ignore_dup_key = 1);
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
        opts = []
        if row.fill_factor and 0 < row.fill_factor < 100:
            opts.append(f"FILLFACTOR = {row.fill_factor}")
        if row.is_padded:
            opts.append("PAD_INDEX = ON")
        if row.ignore_dup_key:
            opts.append("IGNORE_DUP_KEY = ON")
        if opts:
            out.append(
                f"ALTER INDEX {qident(row.index_name)} ON {table} REBUILD WITH ({', '.join(opts)});"
            )
            out.append("GO")
            out.append("")
    return _go(out)


def export_always_encrypted(cur, logger=None) -> str:
    out: List[str] = []
    try:
        cur.execute(
            """
            SELECT name, key_store_provider_name, key_path
            FROM sys.column_master_keys ORDER BY name;
            """
        )
        for row in cur.fetchall():
            provider = row.key_store_provider_name or "MSSQL_CERTIFICATE_STORE"
            path = (row.key_path or "").replace("'", "''")
            out.append(f"-- Column master key [{row.name}]")
            out.append(
                f"-- CREATE COLUMN MASTER KEY {qident(row.name)} WITH "
                f"(KEY_STORE_PROVIDER_NAME = N'{provider}', KEY_PATH = N'{path}');"
            )
            out.append("")

        cur.execute(
            """
            SELECT name, column_master_key_id, key_encryption_type_desc
            FROM sys.column_encryption_keys ORDER BY name;
            """
        )
        for row in cur.fetchall():
            out.append(
                f"-- Column encryption key [{row.name}]: deploy CEK + encrypted value from source tooling"
            )
            out.append("")

        cur.execute(
            """
            SELECT
                SCHEMA_NAME(o.schema_id) AS schema_name,
                o.name AS table_name,
                c.name AS column_name,
                cek.name AS encryption_key_name,
                c.encryption_type_desc
            FROM sys.columns c
            JOIN sys.objects o ON o.object_id = c.object_id
            JOIN sys.column_encryption_keys cek ON cek.column_encryption_key_id = c.column_encryption_key_id
            WHERE o.is_ms_shipped = 0;
            """
        )
        for row in cur.fetchall():
            table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
            out.append(f"-- Always Encrypted: {table}.{row.column_name}")
            out.append(
                f"-- ALTER TABLE {table} ALTER COLUMN {qident(row.column_name)} "
                f"ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = {qident(row.encryption_key_name)}, "
                f"ENCRYPTION_TYPE = {row.encryption_type_desc});"
            )
            out.append("")
    except Exception as ex:
        if logger:
            logger.warning("Always Encrypted export: %s", ex)

    return _go(out)


def export_data_masking(cur) -> str:
    try:
        cur.execute(
            """
            SELECT
                SCHEMA_NAME(t.schema_id) AS schema_name,
                t.name AS table_name,
                c.name AS column_name,
                mc.masking_function
            FROM sys.masked_columns mc
            JOIN sys.columns c ON c.object_id = mc.object_id AND c.column_id = mc.column_id
            JOIN sys.tables t ON t.object_id = c.object_id
            WHERE t.is_ms_shipped = 0;
            """
        )
    except Exception:
        return ""
    out: List[str] = []
    for row in cur.fetchall():
        table = f"{qident(row.schema_name)}.{qident(row.table_name)}"
        fn = (row.masking_function or "").replace("'", "''")
        out.append(
            f"ALTER TABLE {table} ALTER COLUMN {qident(row.column_name)} "
            f"ADD MASKED WITH (FUNCTION = '{fn}');"
        )
        out.append("GO")
        out.append("")
    return _go(out)


def export_column_permissions(cur) -> str:
    cur.execute(
        """
        SELECT
            dp.state_desc,
            dp.permission_name,
            grantee.name AS grantee_name,
            SCHEMA_NAME(o.schema_id) AS object_schema,
            o.name AS object_name,
            c.name AS column_name
        FROM sys.database_permissions dp
        JOIN sys.database_principals grantee ON grantee.principal_id = dp.grantee_principal_id
        JOIN sys.objects o ON o.object_id = dp.major_id
        JOIN sys.columns c ON c.object_id = dp.major_id AND c.column_id = dp.minor_id
        WHERE dp.minor_id > 0 AND dp.class = 1
          AND grantee.principal_id > 4
        ORDER BY grantee.name, object_schema, object_name, column_name;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        action = row.state_desc.replace("_WITH_GRANT_OPTION", "").replace("_", " ")
        if "GRANT" in action and "OPTION" in row.state_desc:
            action = "GRANT"
            suffix = " WITH GRANT OPTION"
        else:
            suffix = ""
        target = f"{qident(row.object_schema)}.{qident(row.object_name)} ({qident(row.column_name)})"
        out.append(f"{action} {row.permission_name} ON {target} TO {qident(row.grantee_name)}{suffix};")
        out.append("GO")
        out.append("")
    return _go(out)


def export_schema_authorization(cur) -> str:
    from ..utils.azure_compat import is_exportable_schema_authorization

    cur.execute(
        """
        SELECT s.name AS schema_name, dp.name AS owner_name
        FROM sys.schemas s
        JOIN sys.database_principals dp ON dp.principal_id = s.principal_id
        WHERE s.schema_id > 4 AND dp.name <> 'dbo'
        ORDER BY s.name;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        if not is_exportable_schema_authorization(row.schema_name, row.owner_name):
            out.append(
                f"-- Skipped schema authorization [{row.schema_name}] -> [{row.owner_name}] "
                f"(principal default schema, not a portable T-SQL schema on Azure SQL)"
            )
            out.append("")
            continue
        out.append(f"ALTER AUTHORIZATION ON SCHEMA::{qident(row.schema_name)} TO {qident(row.owner_name)};")
        out.append("GO")
        out.append("")
    return _go(out)


def export_replication(cur, logger=None) -> str:
    out: List[str] = []
    try:
        cur.execute(
            """
            SELECT publication_id, name AS publication_name
            FROM syspublications
            ORDER BY name;
            """
        )
        for row in cur.fetchall():
            out.append(f"-- Replication publication [{row.publication_name}]")
            out.append(
                f"-- Recreate using replication wizard or sp_addpublication for publication_id={row.publication_id}"
            )
            out.append("")

        cur.execute(
            """
            SELECT a.article, OBJECT_SCHEMA_NAME(a.objid) AS schema_name,
                   OBJECT_NAME(a.objid) AS table_name, p.name AS publication_name
            FROM sysarticles a
            JOIN syspublications p ON p.pubid = a.pubid
            ORDER BY p.name, a.article;
            """
        )
        for row in cur.fetchall():
            out.append(
                f"-- Article [{row.article}] {row.schema_name}.{row.table_name} in [{row.publication_name}]"
            )
            out.append("")
    except Exception as ex:
        if logger:
            logger.debug("Replication not present: %s", ex)
    return _go(out)


def export_graph(cur, logger=None) -> str:
    out: List[str] = []
    try:
        cur.execute(
            """
            SELECT s.name AS schema_name, t.name AS table_name, t.is_node, t.is_edge, t.object_id
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE (t.is_node = 1 OR t.is_edge = 1) AND t.is_ms_shipped = 0;
            """
        )
        for row in cur.fetchall():
            kind = "NODE" if row.is_node else "EDGE"
            out.append(f"-- Graph {kind} table {row.schema_name}.{row.table_name}")
            defn = object_definition(cur, row.object_id)
            if defn:
                out.append(defn.strip())
                out.append("GO")
            else:
                out.append(
                    f"-- CREATE TABLE for graph {kind} â€” use SSMS or "
                    f"CREATE TABLE {qident(row.schema_name)}.{qident(row.table_name)} AS NODE/EDGE"
                )
            out.append("")
    except Exception as ex:
        if logger:
            logger.warning("Graph export: %s", ex)
    return _go(out)


def export_memory_optimized_filegroup(cur) -> str:
    try:
        cur.execute(
            """
            SELECT 1 FROM sys.filegroups WHERE type = 'FX';
            """
        )
        if not cur.fetchone():
            return ""
    except Exception:
        return ""
    return _go(
        [
            "-- Memory-optimized data filegroup",
            "IF NOT EXISTS (SELECT 1 FROM sys.filegroups WHERE type = 'FX')",
            "    ALTER DATABASE CURRENT ADD FILEGROUP [MEMORY_OPTIMIZED] CONTAINS MEMORY_OPTIMIZED_DATA;",
            "GO",
            "-- Add FILE to filegroup on target storage path manually",
            "",
        ]
    )


def export_sequence_current_values(cur) -> str:
    cur.execute(
        """
        SELECT s.name AS schema_name, seq.name AS sequence_name,
               CAST(seq.current_value AS NVARCHAR(100)) AS current_value
        FROM sys.sequences seq
        JOIN sys.schemas s ON s.schema_id = seq.schema_id;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        full = f"{qident(row.schema_name)}.{qident(row.sequence_name)}"
        out.append(f"ALTER SEQUENCE {full} RESTART WITH {row.current_value};")
        out.append("GO")
        out.append("")
    return _go(out)


def _assembly_permission_set(permission_set_desc: Optional[str]) -> str:
    """Map sys.assemblies.permission_set_desc to CREATE ASSEMBLY PERMISSION_SET value."""
    if not permission_set_desc:
        return "SAFE"
    desc = permission_set_desc.strip().upper()
    if desc in ("UNSAFE_ACCESS", "UNSAFE"):
        return "UNSAFE"
    if desc in ("SAFE_ACCESS", "SAFE"):
        return "SAFE"
    if desc == "EXTERNAL_ACCESS":
        return "EXTERNAL_ACCESS"
    if desc.endswith("_ACCESS"):
        return desc[: -len("_ACCESS")]
    return desc


def export_assemblies_full(cur, logger=None) -> Tuple[str, List[str]]:
    """Full CREATE ASSEMBLY statements."""
    warnings: List[str] = []
    out: List[str] = []
    try:
        cur.execute(
            """
            SELECT a.name, a.permission_set_desc, a.assembly_id
            FROM sys.assemblies a
            WHERE a.is_user_defined = 1
            ORDER BY a.name;
            """
        )
        assemblies = cur.fetchall()
        for asm in assemblies:
            cur.execute(
                """
                SELECT file_id, name, CAST(content AS VARBINARY(MAX)) AS content
                FROM sys.assembly_files WHERE assembly_id = ? ORDER BY file_id;
                """,
                asm.assembly_id,
            )
            files = cur.fetchall()
            if not files:
                warnings.append(f"Assembly {asm.name}: no files")
                continue
            hex_parts = []
            for f in files:
                if f.content:
                    hex_parts.append("0x" + binascii.hexlify(f.content).decode("ascii"))
            if not hex_parts:
                warnings.append(f"Assembly {asm.name}: empty content")
                continue
            perm = _assembly_permission_set(asm.permission_set_desc)
            out.append(f"-- Assembly {asm.name}")
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.assemblies WHERE name = N'{asm.name}')")
            out.append("BEGIN")
            if len(hex_parts) == 1:
                out.append(
                    f"    CREATE ASSEMBLY {qident(asm.name)} AUTHORIZATION dbo "
                    f"FROM {hex_parts[0]} WITH PERMISSION_SET = {perm};"
                )
            else:
                out.append(
                    f"    CREATE ASSEMBLY {qident(asm.name)} AUTHORIZATION dbo "
                    f"FROM {hex_parts[0]} WITH PERMISSION_SET = {perm};"
                )
                for i, hx in enumerate(hex_parts[1:], start=2):
                    out.append(f"    -- Additional file {i}: {hx[:40]}...")
            out.append("END")
            out.append("GO")
            out.append("")
    except Exception as ex:
        warnings.append(f"Assemblies: {ex}")
    return _go(out), warnings


def export_clr_types(cur, logger=None) -> str:
    out: List[str] = []
    try:
        cur.execute(
            """
            SELECT s.name AS schema_name, t.name AS type_name, a.name AS assembly_name
            FROM sys.types t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.assemblies a ON a.assembly_id = t.assembly_id
            WHERE t.is_user_defined = 1 AND t.is_assembly_type = 1
            ORDER BY s.name, t.name;
            """
        )
        for row in cur.fetchall():
            out.append(
                f"CREATE TYPE {qident(row.schema_name)}.{qident(row.type_name)} "
                f"EXTERNAL NAME {qident(row.assembly_name)}.[{row.type_name}];"
            )
            out.append("GO")
            out.append("")
    except Exception as ex:
        if logger:
            logger.warning("CLR types: %s", ex)
    return _go(out)
