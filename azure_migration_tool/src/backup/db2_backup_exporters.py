# Author: Satish Chauhan

"""Export DDL for DB2 schema (read-only from SYSCAT)."""

import logging
from typing import Any, Dict, List, Optional, Tuple

# DB2 identifier quoting
def _q(s: str) -> str:
    if not s:
        return '""'
    return '"' + str(s).replace('"', '""') + '"'


def _db2_type_sql(typename: str, length: int, scale: int) -> str:
    """Build DB2 data type string from SYSCAT.COLUMNS."""
    t = (typename or "").strip().upper()
    if t in ("VARCHAR", "CHAR", "CHARACTER", "GRAPHIC", "VARGRAPHIC"):
        if length and length > 0:
            return f"{typename}({length})"
        return typename + "(1)" if t in ("CHAR", "CHARACTER") else typename + "(255)"
    if t in ("DECIMAL", "NUMERIC", "DEC"):
        if length and length > 0:
            return f"DECIMAL({length},{scale})" if scale is not None else f"DECIMAL({length},0)"
        return "DECIMAL(5,0)"
    if t in ("REAL", "DOUBLE", "FLOAT", "INTEGER", "INT", "SMALLINT", "BIGINT", "DATE", "TIME", "TIMESTAMP", "BLOB", "CLOB", "DBCLOB", "XML"):
        return t
    return typename or "VARCHAR(255)"


def get_pk_columns_for_table(cursor, schema: str, table_name: str, pk_rows: List[Dict]) -> List[str]:
    """Get ordered PK column names for a table from SYSCAT.KEYCOLUSE."""
    if not pk_rows:
        return []
    constname = pk_rows[0].get("constraint_name")
    if not constname:
        return []
    try:
        cursor.execute("""
            SELECT COLNAME FROM SYSCAT.KEYCOLUSE
            WHERE CONSTNAME = ? AND TABSCHEMA = ? AND TABNAME = ?
            ORDER BY COLSEQ
        """, [constname, schema, table_name])
        return [str(row[0]).strip() for row in cursor.fetchall() if row[0]]
    except Exception:
        return []


def build_db2_create_table_sql(
    schema_name: str,
    table_name: str,
    columns: List[Any],
    pk_col_names: List[str],
) -> Tuple[str, Optional[str]]:
    """Build CREATE TABLE statement for DB2. Returns (sql, warning_or_none)."""
    full = f"{_q(schema_name)}.{_q(table_name)}"
    lines = [f"-- {full}", f"CREATE TABLE {full} ("]
    col_defs = []
    for c in columns:
        name = getattr(c, "column_name", None) or (c.get("column_name") if isinstance(c, dict) else None)
        if not name or not str(name).strip():
            continue
        typ = _db2_type_sql(
            getattr(c, "data_type", None) or (c.get("data_type") if isinstance(c, dict) else ""),
            getattr(c, "length", 0) or (c.get("length", 0) if isinstance(c, dict) else 0),
            getattr(c, "scale", 0) or (c.get("scale", 0) if isinstance(c, dict) else 0),
        )
        nullable = getattr(c, "nullable", True)
        if hasattr(c, "nullable"):
            null_str = " NULL" if c.nullable else " NOT NULL"
        else:
            null_str = " NULL" if (c.get("nullable", True) if isinstance(c, dict) else True) else " NOT NULL"
        default = getattr(c, "default", None) or (c.get("default") if isinstance(c, dict) else None)
        default_str = ""
        if default and str(default).strip():
            default_str = " DEFAULT " + str(default).strip()
        col_defs.append(f"  {_q(str(name))} {typ}{null_str}{default_str}")
    if not col_defs:
        return "", f"Table {schema_name}.{table_name}: no columns"
    if pk_col_names:
        pk_list = ", ".join(_q(c) for c in pk_col_names)
        col_defs.append(f"  PRIMARY KEY ({pk_list})")
    lines.append(",\n".join(col_defs))
    lines.append(");")
    return "\n".join(lines), None


def export_db2_sequences(cursor, schema: Optional[str], logger: Optional[logging.Logger]) -> str:
    """Export CREATE SEQUENCE statements from SYSCAT.SEQUENCES."""
    try:
        from gui.utils.db2_schema import fetch_db2_sequence_details
        seqs = fetch_db2_sequence_details(cursor, schema)
    except Exception as e:
        if logger:
            logger.warning("Could not fetch sequence details: %s", e)
        return ""
    out = []
    for s in seqs:
        sch = s["schema_name"]
        name = s["sequence_name"]
        full = f"{_q(sch)}.{_q(name)}"
        line = f"CREATE SEQUENCE {full}"
        if s.get("start") is not None:
            line += f" START WITH {s['start']}"
        if s.get("increment") is not None:
            line += f" INCREMENT BY {s['increment']}"
        if s.get("minvalue") is not None:
            line += f" MINVALUE {s['minvalue']}"
        if s.get("maxvalue") is not None:
            line += f" MAXVALUE {s['maxvalue']}"
        if s.get("cycle") is True:
            line += " CYCLE"
        else:
            line += " NO CYCLE"
        if s.get("cache") is not None:
            line += f" CACHE {s['cache']}"
        out.append(line + ";")
    return "\n".join(out) + "\n" if out else ""


def export_db2_views(cursor, views: List[Tuple[str, str]], logger: Optional[logging.Logger]) -> str:
    """Export CREATE VIEW from view definitions (VIEWTEXT/TEXT)."""
    from gui.utils.db2_schema import fetch_db2_view_definition
    out = []
    for (sch, name) in views:
        defn = fetch_db2_view_definition(cursor, sch, name)
        if defn:
            out.append(f"-- {_q(sch)}.{_q(name)}")
            out.append(defn.strip())
            out.append("")
        else:
            if logger:
                logger.warning("View %s.%s: definition not available (no SELECT on TEXT or view encrypted).", sch, name)
            out.append(f"-- {_q(sch)}.{_q(name)} -- definition not available")
            out.append("")
    return "\n".join(out)


def export_db2_procedures(cursor, procs: List[Tuple[str, str]], logger: Optional[logging.Logger]) -> str:
    """Export CREATE PROCEDURE from SYSCAT.ROUTINES BODY."""
    from gui.utils.db2_schema import fetch_db2_routine_body
    out = []
    for (sch, name) in procs:
        body = fetch_db2_routine_body(cursor, sch, name, "P")
        if body:
            out.append(f"-- {_q(sch)}.{_q(name)}")
            out.append(body.strip())
            out.append("")
        else:
            if logger:
                logger.warning("Procedure %s.%s: body not available.", sch, name)
            out.append(f"-- {_q(sch)}.{_q(name)} -- body not available")
            out.append("")
    return "\n".join(out)


def export_db2_functions(cursor, funcs: List[Tuple[str, str]], logger: Optional[logging.Logger]) -> str:
    """Export CREATE FUNCTION from SYSCAT.ROUTINES BODY."""
    from gui.utils.db2_schema import fetch_db2_routine_body
    out = []
    for (sch, name) in funcs:
        body = fetch_db2_routine_body(cursor, sch, name, "F")
        if body:
            out.append(f"-- {_q(sch)}.{_q(name)}")
            out.append(body.strip())
            out.append("")
        else:
            if logger:
                logger.warning("Function %s.%s: body not available.", sch, name)
            out.append(f"-- {_q(sch)}.{_q(name)} -- body not available")
            out.append("")
    return "\n".join(out)


def export_db2_triggers(cursor, schema: Optional[str], logger: Optional[logging.Logger]) -> str:
    """Export CREATE TRIGGER from SYSCAT.TRIGGERS TRIGDEF."""
    from gui.utils.db2_schema import fetch_db2_triggers, fetch_db2_trigger_definition
    triggers = fetch_db2_triggers(cursor, schema)
    out = []
    for (sch, trigname, tabname) in triggers:
        defn = fetch_db2_trigger_definition(cursor, sch, trigname)
        if defn:
            out.append(f"-- {_q(sch)}.{_q(trigname)} ON {_q(sch)}.{_q(tabname)}")
            out.append(defn.strip())
            out.append("")
        else:
            if logger:
                logger.warning("Trigger %s.%s: definition not available.", sch, trigname)
            out.append(f"-- {_q(sch)}.{_q(trigname)} -- definition not available")
            out.append("")
    return "\n".join(out)


def export_db2_foreign_keys(cursor, schema: Optional[str], logger: Optional[logging.Logger]) -> str:
    """Export ALTER TABLE ... ADD FOREIGN KEY using SYSCAT.REFERENCES and KEYCOLUSE."""
    from gui.utils.db2_schema import fetch_db2_foreign_keys, fetch_db2_foreign_key_columns
    fks = fetch_db2_foreign_keys(cursor, schema)
    out = []
    for fk in fks:
        sch = fk["schema_name"]
        tab = fk["table_name"]
        ref_sch = fk["ref_schema"]
        ref_tab = fk["ref_table"]
        cname = fk["constraint_name"]
        cols = fetch_db2_foreign_key_columns(cursor, sch, tab, cname)
        if not cols:
            if logger:
                logger.warning("FK %s: no columns (skipped).", cname)
            out.append(f"-- {_q(sch)}.{_q(tab)} -> {_q(ref_sch)}.{_q(ref_tab)} ({cname}) -- skipped: no columns")
            out.append("")
            continue
        parent_cols = ", ".join(_q(p) for p, _ in cols)
        ref_cols = ", ".join(_q(r) for _, r in cols)
        tbl = f"{_q(sch)}.{_q(tab)}"
        ref = f"{_q(ref_sch)}.{_q(ref_tab)}"
        out.append(f"ALTER TABLE {tbl} ADD CONSTRAINT {_q(cname)} FOREIGN KEY ({parent_cols}) REFERENCES {ref} ({ref_cols});")
        out.append("")
    return "\n".join(out)


def export_db2_check_constraints(cursor, schema: Optional[str]) -> str:
    """Export ALTER TABLE ... ADD CHECK from SYSCAT.CHECKS."""
    from gui.utils.db2_schema import fetch_db2_check_constraints
    checks = fetch_db2_check_constraints(cursor, schema)
    out = []
    for c in checks:
        tbl = f"{_q(c['schema_name'])}.{_q(c['table_name'])}"
        out.append(f"ALTER TABLE {tbl} ADD CONSTRAINT {_q(c['constraint_name'])} CHECK {c['definition']};")
        out.append("")
    return "\n".join(out)


def export_db2_indexes(cursor, schema: Optional[str], logger: Optional[logging.Logger]) -> str:
    """Export CREATE INDEX from SYSCAT.INDEXES (excluding those backing PK/UK)."""
    from gui.utils.db2_schema import fetch_db2_indexes
    indexes = fetch_db2_indexes(cursor, schema)
    out = []
    for idx in indexes:
        # Skip if this index is the primary key (UNIQUERULE 'P')
        tbl = f"{_q(idx.schema_name)}.{_q(idx.table_name)}"
        cols = ", ".join(_q(c) for c in idx.columns)
        unq = "UNIQUE " if idx.unique else ""
        out.append(f"CREATE {unq}INDEX {_q(idx.index_name)} ON {tbl} ({cols});")
        out.append("")
    return "\n".join(out)
