# -*- coding: utf-8 -*-
"""
Build T-SQL to align an existing destination column with source metadata (type, length, collation, nullability).

Mirrors the practical ordering used by tools like SQL Compare / Red Gate: drop blocking dependencies on the
destination, ALTER COLUMN, then recreate PK / indexes / FKs from source definitions.
"""

from __future__ import annotations

from typing import Callable, List, Optional, Tuple

# (src_cur, dest_cur, schema, table, column) -> script or (None, error)


def _fetch_column_phys(cur, schema: str, table: str, column: str):
    qual = f"{schema}.{table}"
    cur.execute(
        """
        SELECT
            ty.name AS type_name,
            CAST(c.max_length AS INT) AS max_length,
            CAST(c.precision AS INT) AS precision,
            CAST(c.scale AS INT) AS scale,
            CASE WHEN c.is_nullable = 1 THEN 1 ELSE 0 END AS is_nullable,
            CAST(c.collation_name AS NVARCHAR(256)) AS collation_name,
            CASE WHEN c.is_identity = 1 THEN 1 ELSE 0 END AS is_identity
        FROM sys.columns c
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID(?, 'U') AND c.name = ?
        """,
        (qual, column),
    )
    return cur.fetchone()


def _dest_column_exists(dest_cur, schema: str, table: str, column: str) -> bool:
    dest_cur.execute(
        """
        SELECT 1 FROM sys.columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ? AND c.name = ?
        """,
        (schema, table, column),
    )
    return dest_cur.fetchone() is not None


def _indexes_touching_column(dest_cur, schema: str, table: str, column: str) -> List[Tuple]:
    """Rows: index_id, index_name, is_primary_key, is_unique_constraint."""
    dest_cur.execute(
        """
        SELECT DISTINCT i.index_id, i.name, i.is_primary_key, i.is_unique_constraint
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN sys.tables t ON t.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ? AND c.name = ? AND i.type > 0
        ORDER BY i.is_primary_key DESC, i.index_id
        """,
        (schema, table, column),
    )
    return list(dest_cur.fetchall())


def _fk_referencing_table(dest_cur, schema: str, table: str):
    dest_cur.execute(
        """
        SELECT fk.name AS fk_name,
               OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
               OBJECT_NAME(fk.parent_object_id) AS child_table
        FROM sys.foreign_keys fk
        JOIN sys.tables t ON t.object_id = fk.referenced_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ?
        """,
        (schema, table),
    )
    return list(dest_cur.fetchall())


def _default_on_column(dest_cur, schema: str, table: str, column: str) -> Optional[str]:
    qual = f"{schema}.{table}"
    dest_cur.execute(
        """
        SELECT dc.name
        FROM sys.default_constraints dc
        JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(?, 'U') AND c.name = ?
        """,
        (qual, column),
    )
    row = dest_cur.fetchone()
    return row[0] if row else None


def _source_index_names_ordered(src_cur, schema: str, table: str) -> List[str]:
    """PK first, then CLUSTERED, then NONCLUSTERED — by name within bucket."""
    src_cur.execute(
        """
        SELECT i.name, i.is_primary_key, i.type_desc
        FROM sys.indexes i
        JOIN sys.tables t ON t.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ? AND i.type > 0
          AND (i.is_primary_key = 1 OR i.is_unique_constraint = 0)
        """,
        (schema, table),
    )
    rows = list(src_cur.fetchall())
    def sort_key(r):
        name, is_pk, td = r[0], r[1], (r[2] or "").upper()
        bucket = 0 if is_pk else (1 if td == "CLUSTERED" else 2)
        return (bucket, name or "")
    rows.sort(key=sort_key)
    return [r[0] for r in rows if r[0]]


def build_column_mismatch_fix_script(
    src_cur,
    dest_cur,
    schema: str,
    table: str,
    column_name: str,
    get_object_code: Callable[..., Optional[str]],
    qident,
    type_sql,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (full_script, error_message). Script uses GO batch separators for pyodbc execution.
    get_object_code(cur, obj_name) -> SQL string or None (same contract as SchemaValidationTab._get_object_code).
    """
    src_row = _fetch_column_phys(src_cur, schema, table, column_name)
    if not src_row:
        return None, None  # Not a column (e.g. index/constraint name); caller uses normal DDL path

    type_name = (src_row.type_name or "").strip()
    max_length = src_row.max_length
    precision = src_row.precision
    scale = src_row.scale
    is_nullable = bool(src_row.is_nullable)
    collation_name = src_row.collation_name
    if hasattr(collation_name, "strip"):
        collation_name = collation_name.strip() if collation_name else None
    else:
        collation_name = None
    if bool(src_row.is_identity):
        return None, (
            "This column is an IDENTITY on source. Automatic alignment is not supported; "
            "use a manual script or table rebuild."
        )

    if not _dest_column_exists(dest_cur, schema, table, column_name):
        return None, None  # Caller should use ADD COLUMN path

    type_str = type_sql(type_name, max_length, precision, scale)
    tlower = type_name.lower()
    if tlower in ("varchar", "char", "nvarchar", "nchar", "text", "ntext") and collation_name:
        type_str = f"{type_str} COLLATE {collation_name}"
    nullable_sql = "NULL" if is_nullable else "NOT NULL"
    alter_line = (
        f"ALTER TABLE {qident(schema)}.{qident(table)} "
        f"ALTER COLUMN {qident(column_name)} {type_str} {nullable_sql};"
    )

    parts: List[str] = []
    parts.append(
        f"-- Align column [{schema}].[{table}].[{column_name}] on DESTINATION to match SOURCE "
        f"(type/collation/nullability)."
    )
    parts.append("-- Review in a lower environment; backup recommended.")
    parts.append("")

    fk_rows = _fk_referencing_table(dest_cur, schema, table)
    if fk_rows:
        parts.append("-- Drop FKs that reference this table (will be recreated from source)")
        for fk_name, child_schema, child_table in fk_rows:
            parts.append(
                f"ALTER TABLE {qident(child_schema)}.{qident(child_table)} "
                f"DROP CONSTRAINT IF EXISTS {qident(fk_name)};"
            )
        parts.append("GO")

    dc_name = _default_on_column(dest_cur, schema, table, column_name)
    if dc_name:
        parts.append("-- Drop default constraint on this column")
        parts.append(
            f"ALTER TABLE {qident(schema)}.{qident(table)} DROP CONSTRAINT IF EXISTS {qident(dc_name)};"
        )
        parts.append("GO")

    idx_rows = _indexes_touching_column(dest_cur, schema, table, column_name)
    dropped_index_names: List[str] = []

    if idx_rows:
        parts.append("-- Drop indexes / constraints that reference this column (destination)")
        for _iid, iname, is_pk, is_uq in idx_rows:
            if not iname:
                continue
            if is_pk:
                parts.append(
                    f"ALTER TABLE {qident(schema)}.{qident(table)} "
                    f"DROP CONSTRAINT IF EXISTS {qident(iname)};"
                )
            elif is_uq:
                parts.append(
                    f"ALTER TABLE {qident(schema)}.{qident(table)} "
                    f"DROP CONSTRAINT IF EXISTS {qident(iname)};"
                )
            else:
                parts.append(
                    f"DROP INDEX IF EXISTS {qident(iname)} ON {qident(schema)}.{qident(table)};"
                )
            dropped_index_names.append(iname)
        parts.append("GO")

    dropped_set = set(dropped_index_names)

    parts.append("-- ALTER COLUMN (match source)")
    parts.append(alter_line)
    parts.append("GO")

    parts.append("-- Recreate only indexes/constraints that were dropped (from SOURCE definitions)")
    for iname in _source_index_names_ordered(src_cur, schema, table):
        if iname not in dropped_set:
            continue
        full_name = f"{schema}.{table}.{iname}"
        code = get_object_code(src_cur, full_name)
        if not code or not str(code).strip():
            parts.append(f"-- [WARN] No script on source for {full_name}; recreate manually if needed.")
            continue
        lines = code.strip().split("\n")
        if lines and lines[0].strip().startswith("--"):
            lines = lines[1:]
        stmt = "\n".join(lines).strip()
        if stmt:
            parts.append(stmt)
            if not stmt.rstrip().endswith(";"):
                parts.append(";")
            parts.append("GO")

    if fk_rows:
        parts.append("-- Restore foreign keys from SOURCE")
        for fk_name, child_schema, child_table in fk_rows:
            full_name = f"{child_schema}.{child_table}.{fk_name}"
            code = get_object_code(src_cur, full_name)
            if not code or not str(code).strip():
                parts.append(
                    f"-- [WARN] Could not script FK {full_name}; re-add manually if required."
                )
                continue
            lines = code.strip().split("\n")
            if lines and lines[0].strip().startswith("--"):
                lines = lines[1:]
            stmt = "\n".join(lines).strip()
            if stmt:
                parts.append(stmt)
                if not stmt.rstrip().endswith(";"):
                    parts.append(";")
                parts.append("GO")

    return "\n".join(parts).strip(), None
