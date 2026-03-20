# Author: Satish Chauhan

"""
IDENTITY disable/restore for CDC and ADF data load.
Connect to Azure SQL, fetch all identity columns, disable (remove identity) and save state,
later restore identity from state file with optional reseed.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

def _qident(name: str) -> str:
    """Quote SQL identifier: [name] (handles ] by doubling)."""
    return "[" + (name or "").replace("]", "]]") + "]"


try:
    from src.utils.paths import qident, utc_iso, utc_ts_compact
except ImportError:
    try:
        from azure_migration_tool.src.utils.paths import qident, utc_iso, utc_ts_compact
    except ImportError:
        from datetime import datetime, timezone
        qident = _qident
        def utc_iso() -> str:
            return datetime.now(timezone.utc).isoformat()
        def utc_ts_compact() -> str:
            return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

try:
    from src.utils.sql import type_sql
except ImportError:
    try:
        from azure_migration_tool.src.utils.sql import type_sql
    except ImportError:
        def type_sql(type_name: str, max_length: int, precision: int, scale: int) -> str:
            t = (type_name or "").lower()
            if t in ("int", "bigint", "smallint", "tinyint"):
                return type_name or "INT"
            if t in ("decimal", "numeric"):
                return f"DECIMAL({precision or 18},{scale or 0})"
            if t in ("varchar", "nvarchar"):
                return f"{type_name}(MAX)" if max_length == -1 else f"{type_name}({int(max_length)//2 if 'n' in t else max_length})"
            return type_name or "INT"


def fetch_identity_columns(cur) -> List[Dict[str, Any]]:
    """
    Fetch all identity columns from the current database.
    Returns list of dicts: schema_name, table_name, column_name, type_name, max_length, precision, scale,
    seed_value, increment_value, last_value (IDENT_CURRENT).
    """
    cur.execute("""
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            c.name AS column_name,
            ty.name AS type_name,
            c.max_length,
            c.precision,
            c.scale,
            CAST(ic.seed_value AS NVARCHAR(100)) AS seed_value_str,
            CAST(ic.increment_value AS NVARCHAR(100)) AS increment_value_str
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.columns c ON c.object_id = t.object_id
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name, c.column_id
    """)
    rows = cur.fetchall()
    result = []
    for r in rows:
        schema_name = (r[0] or "").strip()
        table_name = (r[1] or "").strip()
        column_name = (r[2] or "").strip()
        type_name = (r[3] or "").strip()
        max_length = int(r[4]) if r[4] is not None else 0
        precision = int(r[5]) if r[5] is not None else 0
        scale = int(r[6]) if r[6] is not None else 0
        seed_str = (r[7] or "1").strip()
        inc_str = (r[8] or "1").strip()
        try:
            seed_value = int(float(seed_str))
        except (ValueError, TypeError):
            seed_value = 1
        try:
            increment_value = int(float(inc_str))
        except (ValueError, TypeError):
            increment_value = 1
        result.append({
            "schema_name": schema_name,
            "table_name": table_name,
            "column_name": column_name,
            "type_name": type_name,
            "max_length": max_length,
            "precision": precision,
            "scale": scale,
            "seed_value": seed_value,
            "increment_value": increment_value,
            "last_value": None,  # filled below with IDENT_CURRENT
        })
    # Get IDENT_CURRENT for each table (one identity column per table assumed for simplicity; we'll get max per table)
    for rec in result:
        try:
            cur.execute(
                "SELECT IDENT_CURRENT(?) AS last_val",
                [f"{rec['schema_name']}.{rec['table_name']}"]
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                try:
                    rec["last_value"] = int(float(row[0]))
                except (ValueError, TypeError):
                    rec["last_value"] = None
        except Exception:
            rec["last_value"] = None
    return result


def fetch_constraints_for_column(
    cur,
    schema_name: str,
    table_name: str,
    column_name: str,
) -> List[Dict[str, Any]]:
    """
    Fetch constraints that depend on the given table column (PK, FK, default, check)
    so they can be dropped before DROP COLUMN and re-created after.
    Returns list of dicts: type (pk|fk|default|check), name, and type-specific fields.
    """
    constraints: List[Dict[str, Any]] = []
    tid = f"{schema_name}.{table_name}"

    # Primary key that includes this column
    cur.execute("""
        SELECT k.name, ic.key_ordinal, c.name
        FROM sys.key_constraints k
        JOIN sys.index_columns ic ON ic.object_id = k.parent_object_id AND ic.index_id = k.unique_index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE k.type = 'PK' AND k.parent_object_id = OBJECT_ID(?)
        ORDER BY ic.key_ordinal
    """, [tid])
    pk_rows = cur.fetchall()
    if pk_rows:
        pk_name = pk_rows[0][0]
        pk_cols = [r[2] for r in pk_rows]
        if column_name in pk_cols:
            constraints.append({"type": "pk", "name": pk_name, "columns": pk_cols})

    # Foreign keys: (a) on this table using this column, (b) on other tables referencing this table.column
    cur.execute("""
        SELECT fk.name,
            OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id),
            OBJECT_SCHEMA_NAME(fk.referenced_object_id), OBJECT_NAME(fk.referenced_object_id),
            (SELECT STRING_AGG(pc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id)
             FROM sys.foreign_key_columns fkc
             JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
             WHERE fkc.constraint_object_id = fk.object_id),
            (SELECT STRING_AGG(rc.name, ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id)
             FROM sys.foreign_key_columns fkc
             JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
             WHERE fkc.constraint_object_id = fk.object_id)
        FROM sys.foreign_keys fk
        WHERE (fk.parent_object_id = OBJECT_ID(?) AND EXISTS (
            SELECT 1 FROM sys.foreign_key_columns fkc
            JOIN sys.columns c ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
            WHERE fkc.constraint_object_id = fk.object_id AND c.name = ?
        )) OR (fk.referenced_object_id = OBJECT_ID(?) AND EXISTS (
            SELECT 1 FROM sys.foreign_key_columns fkc
            JOIN sys.columns c ON c.object_id = fkc.referenced_object_id AND c.column_id = fkc.referenced_column_id
            WHERE fkc.constraint_object_id = fk.object_id AND c.name = ?
        ))
    """, [tid, column_name, tid, column_name])
    for r in cur.fetchall():
        constraints.append({
            "type": "fk",
            "name": r[0],
            "parent_schema": r[1] or "dbo",
            "parent_table": r[2],
            "parent_columns": (r[5] or "").split(",") if r[5] else [],
            "referenced_schema": r[3] or "dbo",
            "referenced_table": r[4],
            "referenced_columns": (r[6] or "").split(",") if r[6] else [],
        })

    # Default constraint on this column
    cur.execute("""
        SELECT dc.name, dc.definition
        FROM sys.default_constraints dc
        JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(?) AND c.name = ?
    """, [tid, column_name])
    for r in cur.fetchall():
        constraints.append({"type": "default", "name": r[0], "definition": (r[1] or "").strip()})

    # Check constraints on this table that reference this column (definition contains [ColumnName])
    cur.execute("""
        SELECT name, definition
        FROM sys.check_constraints
        WHERE parent_object_id = OBJECT_ID(?)
        AND definition LIKE ?
    """, [tid, "%[" + column_name + "]%"])
    for r in cur.fetchall():
        constraints.append({"type": "check", "name": r[0], "definition": (r[1] or "").strip()})

    return constraints


def enrich_identity_columns_with_constraints(
    cur,
    identity_columns: List[Dict[str, Any]],
) -> None:
    """Enrich each identity column with 'constraints' list (PK, FK, default, check) for script generation."""
    for rec in identity_columns:
        rec["constraints"] = fetch_constraints_for_column(
            cur,
            rec["schema_name"],
            rec["table_name"],
            rec["column_name"],
        )


def fetch_table_columns(cur, schema_name: str, table_name: str) -> List[str]:
    """Return ordered list of column names for the table (for restore INSERT/SELECT)."""
    tid = f"{schema_name}.{table_name}"
    cur.execute("""
        SELECT c.name
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ?
        ORDER BY c.column_id
    """, [schema_name, table_name])
    return [str(r[0]) for r in cur.fetchall() if r and r[0]]


def _split_sql_on_go(sql_text: str) -> List[str]:
    """Split SQL on GO statements."""
    try:
        from src.utils.sql import split_sql_on_go
        return split_sql_on_go(sql_text)
    except ImportError:
        try:
            from azure_migration_tool.src.utils.sql import split_sql_on_go
            return split_sql_on_go(sql_text)
        except ImportError:
            import re
            pattern = r"^\s*GO\s*;?\s*$"
            batches, current = [], []
            for line in sql_text.splitlines():
                if re.match(pattern, line, re.IGNORECASE):
                    batch = "\n".join(current).strip()
                    if batch:
                        batches.append(batch)
                    current = []
                else:
                    current.append(line)
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            return batches


def _type_sql_str(type_name: str, max_length: int, precision: int, scale: int) -> str:
    """Build SQL type string for column (no identity)."""
    try:
        return type_sql(type_name, max_length, precision, scale)
    except Exception:
        t = (type_name or "").lower()
        if t in ("int", "bigint", "smallint", "tinyint"):
            return type_name or "INT"
        if t in ("decimal", "numeric"):
            return f"DECIMAL({precision or 18},{scale or 0})"
        if t in ("varchar", "nvarchar", "char", "nchar"):
            if max_length == -1 or max_length > 8000:
                return f"{type_name}(MAX)" if "nvarchar" in t or "varchar" in t else f"{type_name}(8000)"
            if "nchar" in t or "nvarchar" in t:
                return f"{type_name}({int(max_length // 2)})"
            return f"{type_name}({max_length})"
        return type_name or "INT"


def _constraint_drop_table(c: Dict[str, Any], our_schema: str, our_table: str) -> Tuple[str, str]:
    """Return (schema, table) of the table that owns the constraint for DROP CONSTRAINT."""
    if c["type"] == "fk":
        return c["parent_schema"], c["parent_table"]
    return our_schema, our_table


def _emit_drop_constraint(c: Dict[str, Any], our_schema: str, our_table: str) -> str:
    sch, tbl = _constraint_drop_table(c, our_schema, our_table)
    return f"ALTER TABLE {qident(sch)}.{qident(tbl)} DROP CONSTRAINT {qident(c['name'])};"


def _emit_add_constraint(c: Dict[str, Any], our_schema: str, our_table: str, col: str) -> Optional[str]:
    """Return ADD CONSTRAINT statement; col is current name of the column (after rename)."""
    if c["type"] == "pk":
        cols = ", ".join(qident(x) for x in c["columns"])
        return f"ALTER TABLE {qident(our_schema)}.{qident(our_table)} ADD CONSTRAINT {qident(c['name'])} PRIMARY KEY ({cols});"
    if c["type"] == "fk":
        p_tbl = f"{qident(c['parent_schema'])}.{qident(c['parent_table'])}"
        r_tbl = f"{qident(c['referenced_schema'])}.{qident(c['referenced_table'])}"
        p_cols = ", ".join(qident(x) for x in c["parent_columns"])
        r_cols = ", ".join(qident(x) for x in c["referenced_columns"])
        return f"ALTER TABLE {p_tbl} ADD CONSTRAINT {qident(c['name'])} FOREIGN KEY ({p_cols}) REFERENCES {r_tbl} ({r_cols});"
    if c["type"] == "default":
        defn = (c.get("definition") or "").replace("'", "''")
        return f"ALTER TABLE {qident(our_schema)}.{qident(our_table)} ADD CONSTRAINT {qident(c['name'])} DEFAULT {defn} FOR {qident(col)};"
    if c["type"] == "check":
        defn = (c.get("definition") or "").replace("'", "''")
        return f"ALTER TABLE {qident(our_schema)}.{qident(our_table)} ADD CONSTRAINT {qident(c['name'])} CHECK {defn};"
    return None


def generate_disable_script(identity_columns: List[Dict[str, Any]], continue_on_error: bool = True) -> str:
    """
    Generate SQL script to remove IDENTITY from each column.
    Drops dependent constraints (PK, FK, default, check), then add new column, copy data, drop old column, rename new, re-add constraints.
    Uses GO after each statement so DDL is committed before DML.
    """
    lines = [
        "-- Disable IDENTITY: remove identity property from columns (drop/re-add dependent constraints)",
        "SET NOCOUNT ON;",
        "GO",
        "",
    ]
    for rec in identity_columns:
        schema = rec["schema_name"]
        table = rec["table_name"]
        col = rec["column_name"]
        type_str = _type_sql_str(
            rec["type_name"], rec["max_length"], rec["precision"], rec["scale"]
        )
        tbl = f"{qident(schema)}.{qident(table)}"
        new_col_name = col + "_noident"
        constraints = rec.get("constraints") or []

        # Drop order: FK where we are referenced, then FK on us, then PK, default, check
        fk_refs_us = [c for c in constraints if c["type"] == "fk" and c["referenced_schema"] == schema and c["referenced_table"] == table]
        fk_on_us = [c for c in constraints if c["type"] == "fk" and c["parent_schema"] == schema and c["parent_table"] == table]
        others = [c for c in constraints if c["type"] in ("pk", "default", "check")]

        has_pk = any(c["type"] == "pk" for c in constraints)
        lines.append(f"-- {tbl}.{qident(col)}")
        for c in fk_refs_us + fk_on_us + others:
            lines.append(_emit_drop_constraint(c, schema, table))
            lines.append("GO")
        # Add new column only if not already present (idempotent for resume/retry)
        schema_esc = (schema or "").replace("'", "''")
        table_esc = (table or "").replace("'", "''")
        new_col_esc = (new_col_name or "").replace("'", "''")
        lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON c.object_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = N'{schema_esc}' AND t.name = N'{table_esc}' AND c.name = N'{new_col_esc}')")
        lines.append(f"  ALTER TABLE {tbl} ADD {qident(new_col_name)} {type_str} NULL;")
        lines.append("GO")
        lines.append(f"UPDATE {tbl} SET {qident(new_col_name)} = {qident(col)};")
        lines.append("GO")
        # If column is part of PK, set NOT NULL before we drop old column so re-add PK succeeds
        if has_pk:
            lines.append(f"ALTER TABLE {tbl} ALTER COLUMN {qident(new_col_name)} {type_str} NOT NULL;")
            lines.append("GO")
        lines.append(f"ALTER TABLE {tbl} DROP COLUMN {qident(col)};")
        lines.append("GO")
        obj_name = f"{qident(schema)}.{qident(table)}.{qident(new_col_name)}".replace("'", "''")
        new_name_escaped = (col or "").replace("'", "''")
        lines.append(f"EXEC sp_rename N'{obj_name}', N'{new_name_escaped}', 'COLUMN';")
        lines.append("GO")
        # Re-add: PK, default, check on our table; then FK on us; then FK where we are referenced
        for c in others + fk_on_us + fk_refs_us:
            add_sql = _emit_add_constraint(c, schema, table, col)
            if add_sql:
                lines.append(add_sql)
                lines.append("GO")
        lines.append("")
    return "\n".join(lines)


def generate_restore_script(
    identity_columns: List[Dict[str, Any]],
    reseed_from_max: bool = True,
    continue_on_error: bool = True,
) -> str:
    """
    Generate SQL script to re-add IDENTITY to each column.
    Uses INSERT with IDENTITY_INSERT (not UPDATE, since identity columns cannot be updated).
    Flow: drop constraints, add identity column, copy to #temp, truncate, INSERT from #temp, drop old col, rename, re-add constraints, reseed.
    """
    lines = [
        "-- Restore IDENTITY: re-add identity property (INSERT with IDENTITY_INSERT; UPDATE not allowed on identity)",
        "SET NOCOUNT ON;",
        "GO",
        "",
    ]
    for rec in identity_columns:
        schema = rec["schema_name"]
        table = rec["table_name"]
        col = rec["column_name"]
        seed = rec.get("seed_value", 1)
        inc = rec.get("increment_value", 1)
        type_str = _type_sql_str(
            rec["type_name"], rec["max_length"], rec["precision"], rec["scale"]
        )
        tbl = f"{qident(schema)}.{qident(table)}"
        new_col_name = col + "_restore"
        full_name = f"{schema}.{table}"
        constraints = rec.get("constraints") or []
        table_columns = rec.get("table_columns") or []

        fk_refs_us = [c for c in constraints if c["type"] == "fk" and c["referenced_schema"] == schema and c["referenced_table"] == table]
        fk_on_us = [c for c in constraints if c["type"] == "fk" and c["parent_schema"] == schema and c["parent_table"] == table]
        others = [c for c in constraints if c["type"] in ("pk", "default", "check")]

        lines.append(f"-- {tbl}.{qident(col)}")
        for c in fk_refs_us + fk_on_us + others:
            lines.append(_emit_drop_constraint(c, schema, table))
            lines.append("GO")
        # Add new identity column
        lines.append(f"ALTER TABLE {tbl} ADD {qident(new_col_name)} {type_str} IDENTITY({seed},{inc}) NOT NULL;")
        lines.append("GO")

        if table_columns and col in table_columns:
            # Copy via temp table: SELECT INTO #temp, TRUNCATE, INSERT with IDENTITY_INSERT (UPDATE not allowed on identity)
            sel_list = ", ".join(qident(c) for c in table_columns)
            ins_cols = ", ".join(qident(new_col_name) if c == col else qident(c) for c in table_columns)
            sel_cols_from_temp = ", ".join(qident(col) if c == col else qident(c) for c in table_columns)
            temp_name = "#temp_restore_" + (schema + "_" + table).replace(" ", "_")[:80]
            lines.append(f"SELECT {sel_list} INTO {temp_name} FROM {tbl};")
            lines.append("GO")
            lines.append(f"TRUNCATE TABLE {tbl};")
            lines.append("GO")
            lines.append(f"SET IDENTITY_INSERT {tbl} ON;")
            lines.append(f"INSERT INTO {tbl} ({ins_cols}) SELECT {sel_cols_from_temp} FROM {temp_name};")
            lines.append(f"SET IDENTITY_INSERT {tbl} OFF;")
            lines.append("GO")
            lines.append(f"DROP TABLE {temp_name};")
            lines.append("GO")
        else:
            # Fallback: UPDATE (fails on identity; only used if table_columns not provided e.g. old state)
            lines.append(f"SET IDENTITY_INSERT {tbl} ON;")
            lines.append(f"UPDATE {tbl} SET {qident(new_col_name)} = {qident(col)};")
            lines.append(f"SET IDENTITY_INSERT {tbl} OFF;")
            lines.append("GO")

        lines.append(f"ALTER TABLE {tbl} DROP COLUMN {qident(col)};")
        lines.append("GO")
        obj_name = f"{qident(schema)}.{qident(table)}.{qident(new_col_name)}".replace("'", "''")
        new_name_escaped = (col or "").replace("'", "''")
        lines.append(f"EXEC sp_rename N'{obj_name}', N'{new_name_escaped}', 'COLUMN';")
        lines.append("GO")
        for c in others + fk_on_us + fk_refs_us:
            add_sql = _emit_add_constraint(c, schema, table, col)
            if add_sql:
                lines.append(add_sql)
                lines.append("GO")
        if reseed_from_max:
            lines.append(f"DECLARE @maxVal BIGINT = (SELECT ISNULL(MAX({qident(col)}), 0) FROM {tbl});")
            lines.append(f"DBCC CHECKIDENT ('{full_name}', RESEED, @maxVal);")
        elif rec.get("last_value") is not None:
            lines.append(f"DBCC CHECKIDENT ('{full_name}', RESEED, {rec['last_value']});")
        if reseed_from_max or rec.get("last_value") is not None:
            lines.append("GO")
        lines.append("")
    return "\n".join(lines)


def save_state_file(
    path: str,
    identity_columns: List[Dict[str, Any]],
    server: str = "",
    database: str = "",
) -> None:
    """Save identity state to JSON file for later restore."""
    state = {
        "version": 1,
        "server": server,
        "database": database,
        "created_utc": utc_iso(),
        "columns": identity_columns,
    }
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def load_state_file(path: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Load identity state from JSON file.
    Returns (columns list, full state dict with server, database, created_utc).
    """
    with open(path, "r", encoding="utf-8") as f:
        state = json.load(f)
    columns = state.get("columns", [])
    return columns, state


def run_disable(
    cur,
    identity_columns: List[Dict[str, Any]],
    state_path: str,
    server: str = "",
    database: str = "",
    execute: bool = True,
    continue_on_error: bool = True,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Save state file and optionally execute disable script.
    Enriches columns with dependent constraints (PK, FK, default, check) so they are dropped and re-added.
    Returns summary dict with status, errors, script (if not execute).
    """
    enrich_identity_columns_with_constraints(cur, identity_columns)
    save_state_file(state_path, identity_columns, server, database)
    script = generate_disable_script(identity_columns, continue_on_error)
    summary = {"status": "success", "errors": [], "state_file": state_path, "script": script}
    if not execute:
        return summary
    batches = _split_sql_on_go(script)
    for batch in batches:
        if not batch.strip():
            continue
        try:
            cur.execute(batch)
            cur.commit()
        except Exception as e:
            err_msg = str(e)
            summary["errors"].append(err_msg)
            if logger:
                logger.warning("Disable batch failed: %s", err_msg)
            if not continue_on_error:
                summary["status"] = "failed"
                return summary
    if summary["errors"]:
        summary["status"] = "completed_with_errors"
    return summary


def run_restore(
    cur,
    state_path: str,
    reseed_from_max: bool = True,
    execute: bool = True,
    continue_on_error: bool = True,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Load state file and optionally execute restore script.
    Enriches each column with table_columns (from live DB) so restore uses INSERT with IDENTITY_INSERT instead of UPDATE.
    Returns summary dict with status, errors, script (if not execute).
    """
    columns, state = load_state_file(state_path)
    if not columns:
        return {"status": "failed", "errors": ["No columns in state file"], "script": ""}
    # Fetch current table column list for each table so we can use INSERT path (UPDATE not allowed on identity)
    for rec in columns:
        try:
            rec["table_columns"] = fetch_table_columns(cur, rec["schema_name"], rec["table_name"])
        except Exception:
            rec["table_columns"] = []
    script = generate_restore_script(columns, reseed_from_max=reseed_from_max, continue_on_error=continue_on_error)
    summary = {"status": "success", "errors": [], "state_file": state_path, "script": script, "columns_restored": len(columns)}
    if not execute:
        return summary
    batches = _split_sql_on_go(script)
    for batch in batches:
        if not batch.strip():
            continue
        try:
            cur.execute(batch)
            cur.commit()
        except Exception as e:
            err_msg = str(e)
            summary["errors"].append(err_msg)
            if logger:
                logger.warning("Restore batch failed: %s", err_msg)
            if not continue_on_error:
                summary["status"] = "failed"
                return summary
    if summary["errors"]:
        summary["status"] = "completed_with_errors"
    return summary
