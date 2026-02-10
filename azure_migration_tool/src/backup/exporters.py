# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""Export functions for schema objects (tables, programmables, constraints, indexes)."""

import re
from typing import List, Tuple, Dict, Optional

from ..utils.paths import qident
from ..utils.sql import type_sql

# Re-export for external consumers
__all__ = [
    "qident", "type_sql", "parse_int_or_default",
    "fetch_tables", "fetch_columns", "fetch_primary_key", "build_create_table_sql",
    "fetch_objects", "object_definition", "wrap_create_or_alter",
    "fetch_triggers", "fetch_sequences", "fetch_synonyms",
    "export_indexes", "export_foreign_keys", "export_check_constraints",
    "export_default_constraints", "export_primary_keys",
]


def parse_int_or_default(s, default: int) -> int:
    """Parse integer or return default"""
    if s is None:
        return default
    s = str(s).strip()
    if not s:
        return default
    try:
        return int(float(s))
    except Exception:
        return default


# ----------------------------
# TABLES
# ----------------------------
def fetch_tables(cur):
    """Fetch all user tables from database"""
    cur.execute(
        """
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name;
        """
    )
    return cur.fetchall()


def fetch_columns(cur, schema_name: str, table_name: str):
    """Fetch column metadata for a table"""
    # Note: cast to NVARCHAR to avoid pyodbc "ODBC SQL type -16 not supported" issues
    cur.execute(
        """
        SELECT
            c.column_id,
            c.name AS column_name,
            ty.name AS type_name,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            CAST(ic.seed_value AS NVARCHAR(100)) AS seed_value_str,
            CAST(ic.increment_value AS NVARCHAR(100)) AS increment_value_str,
            CAST(dc.definition AS NVARCHAR(MAX)) AS default_definition_str,
            dc.name AS default_constraint_name
        FROM sys.columns c
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        LEFT JOIN sys.identity_columns ic
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        LEFT JOIN sys.default_constraints dc
            ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        WHERE c.object_id = OBJECT_ID(?, ?)
        ORDER BY c.column_id;
        """,
        f"{schema_name}.{table_name}",
        "U",
    )
    return cur.fetchall()


def fetch_primary_key(cur, schema_name: str, table_name: str):
    """Fetch primary key information for a table"""
    cur.execute(
        """
        SELECT kc.name AS pk_name, c.name AS col_name, ic.key_ordinal
        FROM sys.key_constraints kc
        JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE kc.parent_object_id = OBJECT_ID(?, ?) AND kc.type = 'PK'
        ORDER BY ic.key_ordinal;
        """,
        f"{schema_name}.{table_name}",
        "U",
    )
    return cur.fetchall()


def fetch_row_count(cur, schema_name: str, table_name: str) -> int:
    """Get estimated row count for a table"""
    cur.execute(
        """
        SELECT SUM(p.rows) AS row_count
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.partitions p ON p.object_id = t.object_id
        WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1);
        """,
        schema_name,
        table_name,
    )
    r = cur.fetchone()
    return int(r[0] or 0)


def build_create_table_sql(schema_name: str, table_name: str, cols, pk_rows) -> Tuple[str, Optional[str]]:
    """
    Build CREATE TABLE SQL statement
    
    Returns:
        Tuple of (sql_text, warning_message or None)
    """
    full_name = f"{qident(schema_name)}.{qident(table_name)}"
    lines = []
    lines.append(f"-- {full_name}")
    lines.append(f"IF OBJECT_ID(N'{schema_name}.{table_name}', N'U') IS NULL")
    lines.append("BEGIN")
    lines.append(f"    CREATE TABLE {full_name} (")

    col_lines = []
    warning = None
    
    # Validate we have columns
    if not cols:
        warning = f"Table {schema_name}.{table_name}: No columns found"
        return "", warning
    
    for r in cols:
        # Validate column name exists
        if not r.column_name or not r.column_name.strip():
            warning = f"Table {schema_name}.{table_name}: Column with empty name found (skipped)"
            continue
            
        # Validate type exists
        if not r.type_name or not r.type_name.strip():
            warning = f"Table {schema_name}.{table_name}.{r.column_name}: Missing type information"
            # Use a default type if missing
            type_str = "NVARCHAR(MAX)"
        else:
            type_str = type_sql(r.type_name, r.max_length, r.precision, r.scale)
        
        col_def = f"        {qident(r.column_name)} {type_str}"

        if r.is_identity:
            seed = parse_int_or_default(r.seed_value_str, 1)
            inc = parse_int_or_default(r.increment_value_str, 1)
            col_def += f" IDENTITY({seed},{inc})"

        col_def += " NULL" if r.is_nullable else " NOT NULL"

        # Defaults inline: use source constraint name so restore is a mirror (no "renamed" defaults)
        if r.default_definition_str and str(r.default_definition_str).strip():
            df_name = getattr(r, "default_constraint_name", None) or f"DF_{table_name}_{r.column_name}"
            if df_name and str(df_name).strip():
                col_def += f" CONSTRAINT {qident(str(df_name).strip())} DEFAULT {r.default_definition_str}"
            else:
                col_def += f" DEFAULT {r.default_definition_str}"

        col_lines.append(col_def)

    if not col_lines:
        warning = f"Table {schema_name}.{table_name}: No valid columns to create"
        return "", warning

    if pk_rows:
        # Validate primary key columns exist in table columns
        pk_col_names = [x.col_name for x in pk_rows if x.col_name]
        table_col_names = [r.column_name for r in cols if r.column_name]
        
        missing_pk_cols = [c for c in pk_col_names if c not in table_col_names]
        if missing_pk_cols:
            warning = f"Table {schema_name}.{table_name}: Primary key references missing columns: {', '.join(missing_pk_cols)}"
        else:
            pk_name = pk_rows[0].pk_name or f"PK_{table_name}"
            pk_cols = ", ".join(qident(x.col_name) for x in pk_rows if x.col_name)
            if pk_cols:
                col_lines.append(f"        CONSTRAINT {qident(pk_name)} PRIMARY KEY ({pk_cols})")

    lines.append(",\n".join(col_lines))
    lines.append("    );")
    lines.append("END")
    lines.append("GO")
    lines.append("")
    return "\n".join(lines), warning


# ----------------------------
# PROGRAMMABLES
# ----------------------------
def fetch_objects(cur, obj_types_csv: str):
    """Fetch database objects (views, procedures, functions)"""
    types = [t.strip() for t in obj_types_csv.split(",") if t.strip()]
    cur.execute(
        f"""
        SELECT
            s.name AS schema_name,
            o.name AS object_name,
            o.type AS object_type,
            o.object_id
        FROM sys.objects o
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN ({",".join(["?"] * len(types))})
        ORDER BY s.name, o.name;
        """,
        *types,
    )
    return cur.fetchall()


def fetch_triggers(cur):
    """Fetch database triggers"""
    cur.execute(
        """
        SELECT
            s.name AS schema_name,
            t.name AS trigger_name,
            OBJECT_NAME(t.parent_id) AS table_name,
            t.object_id,
            t.parent_id,
            t.is_disabled,
            t.is_instead_of_trigger
        FROM sys.triggers t
        JOIN sys.objects o ON o.object_id = t.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE t.is_ms_shipped = 0
          AND t.parent_class = 1  -- DML triggers only (not DDL)
        ORDER BY s.name, OBJECT_NAME(t.parent_id), t.name;
        """
    )
    return cur.fetchall()


def object_definition(cur, object_id: int):
    """Get object definition (view/procedure/function body)"""
    cur.execute("SELECT CAST(OBJECT_DEFINITION(?) AS NVARCHAR(MAX)) AS defn;", object_id)
    r = cur.fetchone()
    return r[0] if r else None


def wrap_create_or_alter(schema_name: str, object_name: str, definition: str, kind: str) -> str:
    """Wrap object definition with CREATE OR ALTER statement"""
    full_name = f"{qident(schema_name)}.{qident(object_name)}"
    if not definition:
        return f"-- {full_name} ({kind}) definition not available (maybe encrypted)\nGO\n\n"

    text = definition.strip()

    patterns = [
        (r"^\s*CREATE\s+VIEW\s+", "CREATE OR ALTER VIEW "),
        (r"^\s*CREATE\s+PROCEDURE\s+", "CREATE OR ALTER PROCEDURE "),
        (r"^\s*CREATE\s+PROC\s+", "CREATE OR ALTER PROC "),
        (r"^\s*CREATE\s+FUNCTION\s+", "CREATE OR ALTER FUNCTION "),
        (r"^\s*CREATE\s+TRIGGER\s+", "CREATE OR ALTER TRIGGER "),
    ]
    for pat, rep in patterns:
        text = re.sub(pat, rep, text, flags=re.IGNORECASE)

    return "\n".join(
        [
            f"-- {full_name} ({kind})",
            text,
            "GO",
            "",
        ]
    )


def fetch_sequences(cur):
    """Fetch database sequences"""
    cur.execute(
        """
        SELECT
            s.name AS schema_name,
            seq.name AS sequence_name,
            seq.start_value,
            seq.increment,
            seq.minimum_value,
            seq.maximum_value,
            seq.current_value,
            seq.is_cycling,
            seq.cache_size
        FROM sys.sequences seq
        JOIN sys.schemas s ON s.schema_id = seq.schema_id
        ORDER BY s.name, seq.name;
        """
    )
    return cur.fetchall()


def export_sequences(cur) -> str:
    """Export sequences as SQL"""
    sequences = fetch_sequences(cur)
    out = []
    
    for seq in sequences:
        schema_name = seq.schema_name
        sequence_name = seq.sequence_name
        full_name = f"{qident(schema_name)}.{qident(sequence_name)}"
        
        out.append(f"-- {full_name}")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE object_id = OBJECT_ID('{full_name}'))")
        out.append("BEGIN")
        
        # Build CREATE SEQUENCE statement
        seq_sql = f"    CREATE SEQUENCE {full_name}"
        
        # Add data type (defaults to BIGINT if not specified)
        # Note: SQL Server sequences don't expose the data type directly, default to BIGINT
        seq_sql += " AS BIGINT"
        
        # Start value
        if seq.start_value is not None:
            seq_sql += f"\n        START WITH {seq.start_value}"
        
        # Increment
        if seq.increment is not None:
            seq_sql += f"\n        INCREMENT BY {seq.increment}"
        
        # Min value
        if seq.minimum_value is not None:
            seq_sql += f"\n        MINVALUE {seq.minimum_value}"
        else:
            seq_sql += "\n        NO MINVALUE"
        
        # Max value
        if seq.maximum_value is not None:
            seq_sql += f"\n        MAXVALUE {seq.maximum_value}"
        else:
            seq_sql += "\n        NO MAXVALUE"
        
        # Cycling
        if seq.is_cycling:
            seq_sql += "\n        CYCLE"
        else:
            seq_sql += "\n        NO CYCLE"
        
        # Cache size
        if seq.cache_size is not None:
            seq_sql += f"\n        CACHE {seq.cache_size}"
        else:
            seq_sql += "\n        NO CACHE"
        
        out.append(seq_sql + ";")
        out.append("END")
        out.append("GO\n")
    
    return "\n".join(out)


def fetch_synonyms(cur):
    """Fetch database synonyms"""
    cur.execute(
        """
        SELECT
            s.name AS schema_name,
            syn.name AS synonym_name,
            syn.base_object_name
        FROM sys.synonyms syn
        JOIN sys.schemas s ON s.schema_id = syn.schema_id
        ORDER BY s.name, syn.name;
        """
    )
    return cur.fetchall()


def export_synonyms(cur) -> str:
    """Export synonyms as SQL"""
    synonyms = fetch_synonyms(cur)
    out = []
    
    for syn in synonyms:
        schema_name = syn.schema_name
        synonym_name = syn.synonym_name
        base_object_name = syn.base_object_name
        full_name = f"{qident(schema_name)}.{qident(synonym_name)}"
        
        out.append(f"-- {full_name} -> {base_object_name}")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.synonyms WHERE object_id = OBJECT_ID('{full_name}'))")
        out.append("BEGIN")
        out.append(f"    CREATE SYNONYM {full_name} FOR {base_object_name};")
        out.append("END")
        out.append("GO\n")
    
    return "\n".join(out)


def export_triggers(cur, logger=None) -> Tuple[str, List[str]]:
    """
    Export triggers as SQL
    
    Returns:
        Tuple of (sql_text, warnings_list)
    """
    triggers = fetch_triggers(cur)
    out = []
    warnings = []
    
    for trig in triggers:
        schema_name = trig.schema_name
        trigger_name = trig.trigger_name
        table_name = trig.table_name
        full_name = f"{qident(schema_name)}.{qident(trigger_name)}"
        table_full_name = f"{qident(schema_name)}.{qident(table_name)}"
        
        # Get trigger definition
        definition = object_definition(cur, trig.object_id)
        
        if not definition:
            warning_msg = f"Trigger {schema_name}.{trigger_name} on {table_name}: Definition not available (maybe encrypted, skipped)"
            warnings.append(warning_msg)
            if logger:
                logger.warning(warning_msg)
            out.append(f"-- WARNING: {full_name} on {table_full_name} - SKIPPED: Definition not available")
            out.append("GO\n")
            continue
        
        # Wrap with CREATE OR ALTER
        trigger_sql = wrap_create_or_alter(schema_name, trigger_name, definition, "TRIGGER")
        
        # If trigger is disabled, add DISABLE statement
        if trig.is_disabled:
            trigger_sql += f"\nALTER TABLE {table_full_name} DISABLE TRIGGER {qident(trigger_name)};\nGO\n"
        
        out.append(trigger_sql)
    
    if len(triggers) > 0 and logger:
        logger.info(f"Exported {len(triggers)} trigger(s)")
    
    return "\n".join(out), warnings


# ----------------------------
# CONSTRAINTS + INDEXES
# ----------------------------
def export_foreign_keys(cur, logger=None) -> Tuple[str, List[str]]:
    """
    Export foreign key constraints as SQL
    
    Returns:
        Tuple of (sql_text, warnings_list)
    """
    cur.execute(
        """
        SELECT
            fk.object_id,
            s1.name AS schema_name,
            t1.name AS table_name,
            fk.name AS fk_name,
            s2.name AS ref_schema_name,
            t2.name AS ref_table_name,
            fk.delete_referential_action_desc,
            fk.update_referential_action_desc
        FROM sys.foreign_keys fk
        JOIN sys.tables t1 ON t1.object_id = fk.parent_object_id
        JOIN sys.schemas s1 ON s1.schema_id = t1.schema_id
        JOIN sys.tables t2 ON t2.object_id = fk.referenced_object_id
        JOIN sys.schemas s2 ON s2.schema_id = t2.schema_id
        WHERE fk.is_ms_shipped = 0
        ORDER BY s1.name, t1.name, fk.name;
"""
    )
    fks = cur.fetchall()

    out = []
    warnings = []
    skipped_count = 0
    
    for fk in fks:
        parent = f"{qident(fk.schema_name)}.{qident(fk.table_name)}"
        ref = f"{qident(fk.ref_schema_name)}.{qident(fk.ref_table_name)}"
        
        cur.execute(
            """
            SELECT
                pc.name AS parent_column,
                rc.name AS ref_column,
                fkc.constraint_column_id
            FROM sys.foreign_key_columns fkc
            JOIN sys.columns pc
                ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
            JOIN sys.columns rc
                ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
            WHERE fkc.constraint_object_id = ?
            ORDER BY fkc.constraint_column_id;
            """,
            fk.object_id,
        )
        cols = cur.fetchall()
        
        # Validate that we have columns - skip if empty
        if not cols:
            skipped_count += 1
            warning_msg = f"Foreign key {fk.schema_name}.{fk.table_name}.{fk.fk_name} -> {fk.ref_schema_name}.{fk.ref_table_name}: Could not retrieve column information (skipped)"
            warnings.append(warning_msg)
            if logger:
                logger.warning(warning_msg)
            out.append(f"-- WARNING: {parent} -> {ref} ({fk.fk_name}) - SKIPPED: Could not retrieve column information")
            out.append("GO\n")
            continue
        
        # Validate columns are not empty
        parent_cols_list = [c.parent_column for c in cols if c.parent_column]
        ref_cols_list = [c.ref_column for c in cols if c.ref_column]
        
        if not parent_cols_list or not ref_cols_list:
            skipped_count += 1
            warning_msg = f"Foreign key {fk.schema_name}.{fk.table_name}.{fk.fk_name}: Empty column list (skipped)"
            warnings.append(warning_msg)
            if logger:
                logger.warning(warning_msg)
            out.append(f"-- WARNING: {parent} -> {ref} ({fk.fk_name}) - SKIPPED: Empty column list")
            out.append("GO\n")
            continue
        
        parent_cols = ", ".join(qident(c.parent_column) for c in cols if c.parent_column)
        ref_cols = ", ".join(qident(c.ref_column) for c in cols if c.ref_column)

        delete_action = fk.delete_referential_action_desc
        update_action = fk.update_referential_action_desc

        actions = []
        if delete_action and delete_action.upper() != "NO_ACTION":
            actions.append(f"ON DELETE {delete_action.replace('_', ' ')}")
        if update_action and update_action.upper() != "NO_ACTION":
            actions.append(f"ON UPDATE {update_action.replace('_', ' ')}")

        out.append(f"-- {parent} -> {ref} ({fk.fk_name})")
        out.append(f"ALTER TABLE {parent} WITH CHECK")
        out.append(
            f"ADD CONSTRAINT {qident(fk.fk_name)} FOREIGN KEY ({parent_cols}) "
            f"REFERENCES {ref} ({ref_cols})"
            + ((" " + " ".join(actions)) if actions else "")
            + ";"
        )
        out.append("GO\n")
    
    if skipped_count > 0 and logger:
        logger.warning(f"Skipped {skipped_count} foreign key(s) due to missing column information")

    return "\n".join(out), warnings


def export_check_constraints(cur) -> str:
    """Export check constraints as SQL"""
    cur.execute(
        """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            cc.name AS constraint_name,
            CAST(cc.definition AS NVARCHAR(MAX)) AS definition,
            cc.is_disabled
        FROM sys.check_constraints cc
        JOIN sys.tables t ON t.object_id = cc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name, cc.name;
        """
    )
    rows = cur.fetchall()

    out = []
    for r in rows:
        table = f"{qident(r.schema_name)}.{qident(r.table_name)}"
        out.append(f"-- {table} ({r.constraint_name})")
        out.append(f"ALTER TABLE {table} WITH CHECK ADD CONSTRAINT {qident(r.constraint_name)} CHECK {r.definition};")
        out.append("GO\n")
        if r.is_disabled:
            out.append(f"ALTER TABLE {table} NOCHECK CONSTRAINT {qident(r.constraint_name)};")
            out.append("GO\n")
    return "\n".join(out)


def export_default_constraints(cur) -> str:
    """Export default constraints as SQL"""
    cur.execute(
        """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            c.name AS column_name,
            dc.name AS constraint_name,
            CAST(dc.definition AS NVARCHAR(MAX)) AS definition
        FROM sys.default_constraints dc
        JOIN sys.tables t ON t.object_id = dc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name, dc.name;
        """
    )
    rows = cur.fetchall()
    out = []
    for r in rows:
        table = f"{qident(r.schema_name)}.{qident(r.table_name)}"
        out.append(f"-- {table}.{qident(r.column_name)} ({r.constraint_name})")
        out.append(
            f"ALTER TABLE {table} ADD CONSTRAINT {qident(r.constraint_name)} "
            f"DEFAULT {r.definition} FOR {qident(r.column_name)};"
        )
        out.append("GO\n")
    return "\n".join(out)


def export_primary_keys(cur) -> str:
    """
    Export PRIMARY KEY constraints as ALTER TABLE ADD CONSTRAINT statements.
    These are exported separately so they can be added AFTER data migration
    for faster bulk loading (inserting into a heap is faster than clustered table).
    """
    cur.execute(
        """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            kc.name AS pk_name,
            i.type_desc AS index_type
        FROM sys.key_constraints kc
        JOIN sys.tables t ON t.object_id = kc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
        WHERE kc.type = 'PK'
          AND t.is_ms_shipped = 0
        ORDER BY s.name, t.name;
        """
    )
    pks = cur.fetchall()

    out = []
    for r in pks:
        # Get PK columns
        cur.execute(
            """
            SELECT c.name AS col_name, ic.is_descending_key, ic.key_ordinal
            FROM sys.key_constraints kc
            JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE kc.parent_object_id = OBJECT_ID(?, 'U')
              AND kc.type = 'PK'
            ORDER BY ic.key_ordinal;
            """,
            f"{r.schema_name}.{r.table_name}",
        )
        cols = cur.fetchall()

        if not cols:
            continue

        table = f"{qident(r.schema_name)}.{qident(r.table_name)}"
        pk_name = qident(r.pk_name)

        col_parts = []
        for c in cols:
            col_str = qident(c.col_name)
            if c.is_descending_key:
                col_str += " DESC"
            col_parts.append(col_str)
        col_list = ", ".join(col_parts)

        index_type = "CLUSTERED" if (r.index_type or "").strip().upper() == "CLUSTERED" else "NONCLUSTERED"

        out.append(f"-- {table} ({pk_name})")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(N'{r.schema_name}.{r.table_name}') AND type = 'PK')")
        out.append(f"    ALTER TABLE {table} ADD CONSTRAINT {pk_name} PRIMARY KEY {index_type} ({col_list});")
        out.append("GO\n")

    return "\n".join(out)


def export_indexes(cur, logger=None) -> Tuple[str, List[str]]:
    """
    Export indexes as SQL
    
    Returns:
        Tuple of (sql_text, warnings_list)
    """
    cur.execute(
        """
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            i.name AS index_name,
            i.is_unique,
            i.type_desc,
            i.has_filter,
            CAST(i.filter_definition AS NVARCHAR(MAX)) AS filter_definition
        FROM sys.indexes i
        JOIN sys.tables t ON t.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.is_ms_shipped = 0
          AND i.is_hypothetical = 0
          AND i.name IS NOT NULL
          AND i.is_primary_key = 0
          AND i.is_unique_constraint = 0
        ORDER BY s.name, t.name, i.name;
        """
    )
    idx = cur.fetchall()

    out = []
    warnings = []
    skipped_count = 0
    
    for r in idx:
        cur.execute(
            """
            SELECT
                c.name AS col_name,
                ic.is_descending_key,
                ic.is_included_column,
                ic.key_ordinal,
                ic.index_column_id
            FROM sys.index_columns ic
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE ic.object_id = OBJECT_ID(?, 'U')
              AND ic.index_id = (SELECT index_id FROM sys.indexes WHERE object_id = OBJECT_ID(?, 'U') AND name = ?)
            ORDER BY ic.is_included_column, ic.key_ordinal, ic.index_column_id;
            """,
            f"{r.schema_name}.{r.table_name}",
            f"{r.schema_name}.{r.table_name}",
            r.index_name,
        )
        cols = cur.fetchall()

        key_cols = []
        inc_cols = []
        for c in cols:
            if c.col_name:  # Validate column name exists
                if c.is_included_column:
                    inc_cols.append(qident(c.col_name))
                else:
                    direction = " DESC" if c.is_descending_key else " ASC"
                    key_cols.append(qident(c.col_name) + direction)

        # Validate that we have at least one key column
        if not key_cols:
            skipped_count += 1
            warning_msg = f"Index {r.schema_name}.{r.table_name}.{r.index_name}: No key columns found (skipped)"
            warnings.append(warning_msg)
            if logger:
                logger.warning(warning_msg)
            out.append(f"-- WARNING: {qident(r.schema_name)}.{qident(r.table_name)} ({r.index_name}) - SKIPPED: No key columns found")
            out.append("GO\n")
            continue

        table = f"{qident(r.schema_name)}.{qident(r.table_name)}"
        unique = "UNIQUE " if r.is_unique else ""
        typ = "CLUSTERED" if (r.type_desc or "").strip().upper() == "CLUSTERED" else "NONCLUSTERED"

        # Clustered indexes cannot have INCLUDE columns in SQL Server
        # If clustered and has included columns, convert to nonclustered or remove INCLUDE
        if typ == "CLUSTERED" and inc_cols:
            warning_msg = f"Index {r.schema_name}.{r.table_name}.{r.index_name}: Clustered index with INCLUDE columns - converting to NONCLUSTERED (INCLUDE columns not allowed on clustered indexes)"
            warnings.append(warning_msg)
            if logger:
                logger.warning(warning_msg)
            typ = "NONCLUSTERED"  # Convert to nonclustered to allow INCLUDE columns

        stmt = f"CREATE {unique}{typ} INDEX {qident(r.index_name)} ON {table} ({', '.join(key_cols)})"
        if inc_cols:
            stmt += f" INCLUDE ({', '.join(inc_cols)})"
        
        # Validate WHERE clause - only add if filter_definition exists and is not empty/whitespace
        filter_def = str(r.filter_definition).strip() if r.filter_definition else ""
        if r.has_filter:
            if filter_def:
                stmt += f" WHERE {r.filter_definition}"
            else:
                # has_filter is True but filter_definition is empty - skip WHERE clause and warn
                warning_msg = f"Index {r.schema_name}.{r.table_name}.{r.index_name}: has_filter=True but filter_definition is empty (WHERE clause omitted)"
                warnings.append(warning_msg)
                if logger:
                    logger.warning(warning_msg)
        
        stmt += ";"

        out.append(f"-- {table} ({r.index_name})")
        out.append(stmt)
        out.append("GO\n")
    
    if skipped_count > 0 and logger:
        logger.warning(f"Skipped {skipped_count} index(es) due to missing column information")

    return "\n".join(out), warnings


# ----------------------------
# EXTENDED PROPERTIES (Comments/Metadata)
# ----------------------------
def fetch_extended_properties(cur, class_type: str = None):
    """
    Fetch extended properties (MS_Description and others) from database.
    
    Args:
        cur: Database cursor
        class_type: Optional filter by class type:
            - 'SCHEMA' (0)
            - 'OBJECT_OR_COLUMN' (1) - tables, views, columns, procedures, functions
            - 'PARAMETER' (2)
            - 'INDEX' (7)
            - 'CONSTRAINT' (6)
            - 'TRIGGER' (8)
    
    Returns:
        List of extended property rows with: major_id, minor_id, name, value, class_desc
    """
    query = """
        SELECT
            ep.major_id,
            ep.minor_id,
            ep.name,
            CAST(ep.value AS NVARCHAR(MAX)) AS value,
            CASE ep.class
                WHEN 0 THEN 'SCHEMA'
                WHEN 1 THEN 'OBJECT_OR_COLUMN'
                WHEN 2 THEN 'PARAMETER'
                WHEN 6 THEN 'CONSTRAINT'
                WHEN 7 THEN 'INDEX'
                WHEN 8 THEN 'TRIGGER'
                ELSE 'UNKNOWN'
            END AS class_desc,
            ep.class
        FROM sys.extended_properties ep
        WHERE ep.name IS NOT NULL
    """
    
    params = []
    if class_type:
        class_map = {
            'SCHEMA': 0,
            'OBJECT_OR_COLUMN': 1,
            'PARAMETER': 2,
            'CONSTRAINT': 6,
            'INDEX': 7,
            'TRIGGER': 8
        }
        if class_type.upper() in class_map:
            query += " AND ep.class = ?"
            params.append(class_map[class_type.upper()])
    
    query += " ORDER BY ep.major_id, ep.minor_id, ep.name;"
    
    cur.execute(query, *params)
    return cur.fetchall()


def get_object_name_for_extended_property(cur, major_id: int, minor_id: int, class_type: int) -> tuple:
    """
    Get object name and schema for an extended property.
    
    Returns:
        Tuple of (schema_name, object_name, column_name or None)
    """
    if class_type == 0:  # SCHEMA
        cur.execute("SELECT name FROM sys.schemas WHERE schema_id = ?;", major_id)
        row = cur.fetchone()
        if row:
            return (row[0], None, None)
    
    elif class_type == 1:  # OBJECT_OR_COLUMN
        # Check if it's a column (minor_id > 0) or object (minor_id = 0)
        if minor_id == 0:
            # It's an object (table, view, procedure, function)
            cur.execute("""
                SELECT s.name AS schema_name, o.name AS object_name, o.type
                FROM sys.objects o
                JOIN sys.schemas s ON s.schema_id = o.schema_id
                WHERE o.object_id = ?;
            """, major_id)
            row = cur.fetchone()
            if row:
                return (row[0], row[1], row[2] if len(row) > 2 else None)
        else:
            # It's a column
            cur.execute("""
                SELECT s.name AS schema_name, o.name AS object_name, c.name AS column_name
                FROM sys.objects o
                JOIN sys.schemas s ON s.schema_id = o.schema_id
                JOIN sys.columns c ON c.object_id = o.object_id
                WHERE o.object_id = ? AND c.column_id = ?;
            """, major_id, minor_id)
            row = cur.fetchone()
            if row:
                return (row[0], row[1], row[2])
    
    elif class_type == 6:  # CONSTRAINT
        cur.execute("""
            SELECT s.name AS schema_name, o.name AS object_name, kc.name AS constraint_name
            FROM sys.key_constraints kc
            JOIN sys.objects o ON o.object_id = kc.parent_object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE kc.object_id = ?;
        """, major_id)
        row = cur.fetchone()
        if row:
            return (row[0], row[1], row[2])
    
    elif class_type == 7:  # INDEX
        cur.execute("""
            SELECT s.name AS schema_name, o.name AS object_name, i.name AS index_name
            FROM sys.indexes i
            JOIN sys.objects o ON o.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE i.object_id = ? AND i.index_id = ?;
        """, major_id, minor_id)
        row = cur.fetchone()
        if row:
            return (row[0], row[1], row[2])
    
    elif class_type == 8:  # TRIGGER
        cur.execute("""
            SELECT s.name AS schema_name, t.name AS trigger_name, OBJECT_NAME(t.parent_id) AS table_name
            FROM sys.triggers t
            JOIN sys.objects o ON o.object_id = t.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE t.object_id = ?;
        """, major_id)
        row = cur.fetchone()
        if row:
            return (row[0], row[2], row[1])  # schema, table, trigger
    
    return (None, None, None)


def export_extended_properties(cur, logger=None) -> str:
    """
    Export extended properties as SQL statements (sp_addextendedproperty).
    
    Returns:
        SQL text with EXEC sp_addextendedproperty statements
    """
    props = fetch_extended_properties(cur)
    out = []
    
    for prop in props:
        major_id = prop.major_id
        minor_id = prop.minor_id
        name = prop.name
        value = prop.value
        class_type = getattr(prop, 'class')
        
        # Get object name
        schema_name, object_name, obj_type = get_object_name_for_extended_property(cur, major_id, minor_id, class_type)
        
        if not schema_name:
            # Skip if we can't identify the object
            if logger:
                logger.warning(f"Extended property {name} (major_id={major_id}, minor_id={minor_id}): Could not identify object, skipped")
            continue
        
        # Build sp_addextendedproperty statement
        # Escape single quotes in value
        escaped_value = str(value).replace("'", "''") if value else ""
        
        if class_type == 0:  # SCHEMA
            out.append(f"-- Extended property on SCHEMA [{schema_name}]")
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE major_id = SCHEMA_ID('{schema_name}') AND minor_id = 0 AND name = '{name}')")
            out.append("BEGIN")
            out.append(f"    EXEC sp_addextendedproperty @name = N'{name}', @value = N'{escaped_value}', @level0type = N'SCHEMA', @level0name = N'{schema_name}';")
            out.append("END")
            out.append("GO\n")
        
        elif class_type == 1:  # OBJECT_OR_COLUMN
            if minor_id == 0:
                # Object-level property - determine object type
                cur.execute("""
                    SELECT o.type
                    FROM sys.objects o
                    WHERE o.object_id = ?;
                """, major_id)
                obj_type_row = cur.fetchone()
                obj_type_char = obj_type_row[0] if obj_type_row else 'U'
                
                # Map object type to level1type
                type_map = {
                    'U': 'TABLE',
                    'V': 'VIEW',
                    'P': 'PROCEDURE',
                    'FN': 'FUNCTION',
                    'TF': 'FUNCTION',
                    'IF': 'FUNCTION'
                }
                level1type = type_map.get(obj_type_char, 'TABLE')  # Default to TABLE
                
                out.append(f"-- Extended property on {schema_name}.{object_name}")
                out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE major_id = OBJECT_ID('{schema_name}.{object_name}') AND minor_id = 0 AND name = '{name}')")
                out.append("BEGIN")
                out.append(f"    EXEC sp_addextendedproperty @name = N'{name}', @value = N'{escaped_value}', @level0type = N'SCHEMA', @level0name = N'{schema_name}', @level1type = N'{level1type}', @level1name = N'{object_name}';")
                out.append("END")
                out.append("GO\n")
            else:
                # Column-level property - sub_name is the column name
                schema_name, object_name, column_name = get_object_name_for_extended_property(cur, major_id, minor_id, class_type)
                if column_name:
                    out.append(f"-- Extended property on {schema_name}.{object_name}.{column_name}")
                    out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE major_id = OBJECT_ID('{schema_name}.{object_name}') AND minor_id = (SELECT column_id FROM sys.columns WHERE object_id = OBJECT_ID('{schema_name}.{object_name}') AND name = '{column_name}') AND name = '{name}')")
                    out.append("BEGIN")
                    out.append(f"    EXEC sp_addextendedproperty @name = N'{name}', @value = N'{escaped_value}', @level0type = N'SCHEMA', @level0name = N'{schema_name}', @level1type = N'TABLE', @level1name = N'{object_name}', @level2type = N'COLUMN', @level2name = N'{column_name}';")
                    out.append("END")
                    out.append("GO\n")
        
        elif class_type == 6:  # CONSTRAINT
            constraint_name = obj_type if obj_type else "Unknown"
            out.append(f"-- Extended property on CONSTRAINT {schema_name}.{object_name}.{constraint_name}")
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE major_id = OBJECT_ID('{schema_name}.{object_name}') AND minor_id = (SELECT object_id FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID('{schema_name}.{object_name}') AND name = '{constraint_name}') AND name = '{name}')")
            out.append("BEGIN")
            out.append(f"    EXEC sp_addextendedproperty @name = N'{name}', @value = N'{escaped_value}', @level0type = N'SCHEMA', @level0name = N'{schema_name}', @level1type = N'TABLE', @level1name = N'{object_name}', @level2type = N'CONSTRAINT', @level2name = N'{constraint_name}';")
            out.append("END")
            out.append("GO\n")
        
        elif class_type == 7:  # INDEX
            index_name = obj_type if obj_type else "Unknown"
            out.append(f"-- Extended property on INDEX {schema_name}.{object_name}.{index_name}")
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE major_id = OBJECT_ID('{schema_name}.{object_name}') AND minor_id = (SELECT index_id FROM sys.indexes WHERE object_id = OBJECT_ID('{schema_name}.{object_name}') AND name = '{index_name}') AND name = '{name}')")
            out.append("BEGIN")
            out.append(f"    EXEC sp_addextendedproperty @name = N'{name}', @value = N'{escaped_value}', @level0type = N'SCHEMA', @level0name = N'{schema_name}', @level1type = N'TABLE', @level1name = N'{object_name}', @level2type = N'INDEX', @level2name = N'{index_name}';")
            out.append("END")
            out.append("GO\n")
        
        elif class_type == 8:  # TRIGGER
            # For triggers, obj_type is the trigger name, object_name is the table name
            trigger_name = obj_type if obj_type else "Unknown"
            out.append(f"-- Extended property on TRIGGER {schema_name}.{object_name}.{trigger_name}")
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE major_id = OBJECT_ID('{schema_name}.{trigger_name}') AND minor_id = 0 AND name = '{name}')")
            out.append("BEGIN")
            out.append(f"    EXEC sp_addextendedproperty @name = N'{name}', @value = N'{escaped_value}', @level0type = N'SCHEMA', @level0name = N'{schema_name}', @level1type = N'TABLE', @level1name = N'{object_name}', @level2type = N'TRIGGER', @level2name = N'{trigger_name}';")
            out.append("END")
            out.append("GO\n")
    
    if logger and props:
        logger.info(f"Exported {len(props)} extended property(ies)")
    
    return "\n".join(out)