"""
Thin layer: given a pyodbc connection, run SQL Server catalog queries
(sys.objects, sys.columns, sys.indexes, sys.foreign_keys, sys.check_constraints, etc.)
and return lists of dicts or pandas DataFrames.
"""

from typing import List, Optional

import pandas as pd


def _run(conn, sql: str, params: Optional[List] = None) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        columns = [d[0] for d in cur.description]
        raw_rows = cur.fetchall()
        # Convert to list of tuples so pandas gets (n_rows, n_cols); pyodbc Row can be interpreted as single value
        rows = [tuple(r) for r in raw_rows]
        return pd.DataFrame(rows, columns=columns)
    finally:
        cur.close()


def get_objects(
    conn, object_types: Optional[List[str]] = None, schema: Optional[str] = None
) -> pd.DataFrame:
    """Return objects (TABLE, VIEW, etc.) as schema_name, object_name, type."""
    type_map = {"TABLE": ["U"], "VIEW": ["V"], "PROCEDURE": ["P"], "FUNCTION": ["FN", "IF", "TF"]}
    if object_types:
        types = []
        for t in object_types:
            types.extend(type_map.get(t.upper(), []))
        if not types:
            types = ["U", "V", "P", "FN", "IF", "TF"]
    else:
        types = ["U", "V", "P", "FN", "IF", "TF"]
    placeholders = ",".join("?" for _ in types)
    q = f"SELECT RTRIM(SCHEMA_NAME(schema_id)) AS schema_name, RTRIM(name) AS object_name, type FROM sys.objects WHERE type IN ({placeholders})"
    params = list(types)
    if schema:
        q += " AND UPPER(SCHEMA_NAME(schema_id)) = UPPER(?)"
        params.append(schema)
    return _run(conn, q, params)


def get_tables(conn, schema: Optional[str] = None) -> pd.DataFrame:
    """Return tables as schema_name, object_name."""
    q = "SELECT RTRIM(SCHEMA_NAME(schema_id)) AS schema_name, RTRIM(name) AS object_name FROM sys.objects WHERE type = 'U'"
    params = []
    if schema:
        q += " AND UPPER(SCHEMA_NAME(schema_id)) = UPPER(?)"
        params.append(schema)
    return _run(conn, q, params if params else None)


def get_columns_with_types(conn, schema_list: Optional[List[str]] = None) -> pd.DataFrame:
    """Return columns with data_type, char_len, precision, scale. Columns: schema_name, table_name, column_name, data_type, char_len, precision, scale."""
    q = """
    SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, DATA_TYPE AS data_type,
           CAST(CHARACTER_MAXIMUM_LENGTH AS BIGINT) AS char_len,
           CAST(NUMERIC_PRECISION AS BIGINT) AS precision,
           CAST(NUMERIC_SCALE AS BIGINT) AS scale
    FROM INFORMATION_SCHEMA.COLUMNS
    """
    params = []
    if schema_list:
        q += " WHERE TABLE_SCHEMA IN (" + ",".join("?" for _ in schema_list) + ")"
        params.extend(schema_list)
    return _run(conn, q, params if params else None)


def get_column_defaults(conn, schema_list: Optional[List[str]] = None) -> pd.DataFrame:
    """Return schema_name, table_name, column_name, default_str."""
    q = """
    SELECT s.name AS schema_name, t.name AS table_name, c.name AS column_name,
           CAST(dc.definition AS NVARCHAR(4000)) AS default_str
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE 1=1
    """
    params = []
    if schema_list:
        q += " AND s.name IN (" + ",".join("?" for _ in schema_list) + ")"
        params.extend(schema_list)
    return _run(conn, q, params if params else None)


def get_index_columns(conn, schema_list: Optional[List[str]] = None) -> pd.DataFrame:
    """Return schema_name, table_name, idx_name, is_primary_key, is_unique, colseq, col_name, is_descending_key."""
    q = """
    SELECT s.name AS schema_name, t.name AS table_name, i.name AS idx_name,
           CAST(i.is_primary_key AS INT) AS is_primary_key, CAST(i.is_unique AS INT) AS is_unique,
           ic.key_ordinal AS colseq, c.name AS col_name, CAST(ic.is_descending_key AS INT) AS is_descending_key
    FROM sys.indexes i
    JOIN sys.tables t ON i.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
    WHERE i.is_hypothetical = 0 AND i.name IS NOT NULL
    """
    params = []
    if schema_list:
        q += " AND s.name IN (" + ",".join("?" for _ in schema_list) + ")"
        params.extend(schema_list)
    return _run(conn, q, params if params else None)


def get_primary_keys(conn, schema_list: Optional[List[str]] = None) -> pd.DataFrame:
    """Return schema_name, table_name, constraint_name for PKs."""
    q = """
    SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) AS table_name, RTRIM(kc.name) AS constraint_name
    FROM sys.key_constraints kc
    JOIN sys.tables t ON kc.parent_object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE kc.type = 'PK'
    """
    params = []
    if schema_list:
        q += " AND s.name IN (" + ",".join("?" for _ in schema_list) + ")"
        params.extend(schema_list)
    return _run(conn, q, params if params else None)


def get_foreign_keys(conn, schema_list: Optional[List[str]] = None) -> pd.DataFrame:
    """Return schema_name, table_name, fk_name, ref_schema_name, ref_table_name."""
    q = """
    SELECT s.name AS schema_name, t.name AS table_name, fk.name AS fk_name,
           rs.name AS ref_schema_name, rt.name AS ref_table_name
    FROM sys.foreign_keys fk
    JOIN sys.tables t ON fk.parent_object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
    JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
    WHERE fk.is_ms_shipped = 0
    """
    params = []
    if schema_list:
        q += " AND s.name IN (" + ",".join("?" for _ in schema_list) + ")"
        params.extend(schema_list)
    return _run(conn, q, params if params else None)


def get_check_constraints(conn, schema_list: Optional[List[str]] = None) -> pd.DataFrame:
    """Return schema_name, table_name, chk_name, chk_def."""
    q = """
    SELECT s.name AS schema_name, t.name AS table_name, cc.name AS chk_name, cc.definition AS chk_def
    FROM sys.check_constraints cc
    JOIN sys.objects o ON o.object_id = cc.parent_object_id
    JOIN sys.tables t ON t.object_id = o.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE o.type = 'U'
    """
    params = []
    if schema_list:
        q += " AND s.name IN (" + ",".join("?" for _ in schema_list) + ")"
        params.extend(schema_list)
    return _run(conn, q, params if params else None)


def get_columns_nullable(conn, schema_list: Optional[List[str]] = None) -> pd.DataFrame:
    """Return schema_name, table_name, column_name, is_nullable (YES/NO)."""
    q = """
    SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, IS_NULLABLE AS is_nullable
    FROM INFORMATION_SCHEMA.COLUMNS
    """
    params = []
    if schema_list:
        q += " WHERE TABLE_SCHEMA IN (" + ",".join("?" for _ in schema_list) + ")"
        params.extend(schema_list)
    return _run(conn, q, params if params else None)
