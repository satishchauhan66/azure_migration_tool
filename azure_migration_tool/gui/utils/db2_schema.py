"""
DB2-specific schema query utilities.
Uses SYSCAT views instead of INFORMATION_SCHEMA.

Also includes database-agnostic helper functions that work with both SQL Server and DB2.

JDBC/jaydebeapi returns Java objects (e.g. java.lang.String); convert to Python str/int
at read time so .upper(), .split(), etc. work correctly.
"""

import logging
from typing import List, Tuple, Optional, Any, Dict
from dataclasses import dataclass


def _py_str(val: Any) -> str:
    """Convert a cursor value to Python str (handles JDBC/Java types)."""
    if val is None:
        return ""
    return str(val).strip()


def _py_int(val: Any) -> int:
    """Convert a cursor value to Python int (handles JDBC/Java types)."""
    if val is None:
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


@dataclass
class DB2Table:
    """DB2 table info."""
    schema_name: str
    table_name: str


@dataclass
class DB2Column:
    """DB2 column info."""
    schema_name: str
    table_name: str
    column_name: str
    data_type: str
    length: int
    scale: int
    nullable: bool
    default: str


@dataclass
class DB2Index:
    """DB2 index info."""
    schema_name: str
    index_name: str
    table_name: str
    unique: bool
    columns: List[str]


@dataclass
class DB2Constraint:
    """DB2 constraint info."""
    schema_name: str
    constraint_name: str
    table_name: str
    constraint_type: str
    definition: str


def is_db2_connection(conn) -> bool:
    """Check if a connection is to a DB2 database."""
    # Check connection type
    conn_class_name = type(conn).__module__ + '.' + type(conn).__name__
    if 'jaydebeapi' in conn_class_name.lower():
        return True
    if 'db2' in conn_class_name.lower():
        return True
    # Try to detect by cursor behavior
    try:
        cur = conn.cursor()
        # Try a DB2-specific query
        cur.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        cur.fetchone()
        return True
    except:
        return False


def fetch_db2_tables(cursor, schema: Optional[str] = None) -> List[DB2Table]:
    """
    Fetch tables from DB2 database.
    
    Args:
        cursor: Database cursor
        schema: Optional schema name to filter (e.g., user's schema)
    """
    if schema:
        cursor.execute("""
            SELECT TABSCHEMA, TABNAME 
            FROM SYSCAT.TABLES 
            WHERE TYPE = 'T' 
            AND TABSCHEMA = ?
            ORDER BY TABSCHEMA, TABNAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT TABSCHEMA, TABNAME 
            FROM SYSCAT.TABLES 
            WHERE TYPE = 'T' 
            AND TABSCHEMA NOT LIKE 'SYS%'
            ORDER BY TABSCHEMA, TABNAME
        """)
    
    tables = []
    for row in cursor.fetchall():
        tables.append(DB2Table(
            schema_name=_py_str(row[0]),
            table_name=_py_str(row[1])
        ))
    return tables


def fetch_db2_views(cursor, schema: Optional[str] = None) -> List[Tuple[str, str]]:
    """Fetch views from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT VIEWSCHEMA, VIEWNAME 
            FROM SYSCAT.VIEWS 
            WHERE VIEWSCHEMA = ?
            ORDER BY VIEWSCHEMA, VIEWNAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT VIEWSCHEMA, VIEWNAME 
            FROM SYSCAT.VIEWS 
            WHERE VIEWSCHEMA NOT LIKE 'SYS%'
            ORDER BY VIEWSCHEMA, VIEWNAME
        """)
    
    return [(_py_str(row[0]), _py_str(row[1])) for row in cursor.fetchall()]


def fetch_db2_procedures(cursor, schema: Optional[str] = None) -> List[Tuple[str, str]]:
    """Fetch stored procedures from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT ROUTINESCHEMA, ROUTINENAME 
            FROM SYSCAT.ROUTINES 
            WHERE ROUTINETYPE = 'P'
            AND ROUTINESCHEMA = ?
            ORDER BY ROUTINESCHEMA, ROUTINENAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT ROUTINESCHEMA, ROUTINENAME 
            FROM SYSCAT.ROUTINES 
            WHERE ROUTINETYPE = 'P'
            AND ROUTINESCHEMA NOT LIKE 'SYS%'
            ORDER BY ROUTINESCHEMA, ROUTINENAME
        """)
    
    return [(_py_str(row[0]), _py_str(row[1])) for row in cursor.fetchall()]


def fetch_db2_functions(cursor, schema: Optional[str] = None) -> List[Tuple[str, str]]:
    """Fetch functions from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT ROUTINESCHEMA, ROUTINENAME 
            FROM SYSCAT.ROUTINES 
            WHERE ROUTINETYPE = 'F'
            AND ROUTINESCHEMA = ?
            ORDER BY ROUTINESCHEMA, ROUTINENAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT ROUTINESCHEMA, ROUTINENAME 
            FROM SYSCAT.ROUTINES 
            WHERE ROUTINETYPE = 'F'
            AND ROUTINESCHEMA NOT LIKE 'SYS%'
            ORDER BY ROUTINESCHEMA, ROUTINENAME
        """)
    
    return [(_py_str(row[0]), _py_str(row[1])) for row in cursor.fetchall()]


def fetch_db2_triggers(cursor, schema: Optional[str] = None) -> List[Tuple[str, str, str]]:
    """Fetch triggers from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT TRIGSCHEMA, TRIGNAME, TABNAME 
            FROM SYSCAT.TRIGGERS 
            WHERE TRIGSCHEMA = ?
            ORDER BY TRIGSCHEMA, TRIGNAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT TRIGSCHEMA, TRIGNAME, TABNAME 
            FROM SYSCAT.TRIGGERS 
            WHERE TRIGSCHEMA NOT LIKE 'SYS%'
            ORDER BY TRIGSCHEMA, TRIGNAME
        """)
    
    return [(_py_str(row[0]), _py_str(row[1]), _py_str(row[2])) for row in cursor.fetchall()]


def fetch_db2_sequences(cursor, schema: Optional[str] = None) -> List[Tuple[str, str]]:
    """Fetch sequences from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT SEQSCHEMA, SEQNAME 
            FROM SYSCAT.SEQUENCES 
            WHERE SEQSCHEMA = ?
            AND SEQTYPE = 'S'
            ORDER BY SEQSCHEMA, SEQNAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT SEQSCHEMA, SEQNAME 
            FROM SYSCAT.SEQUENCES 
            WHERE SEQSCHEMA NOT LIKE 'SYS%'
            AND SEQTYPE = 'S'
            ORDER BY SEQSCHEMA, SEQNAME
        """)
    
    return [(_py_str(row[0]), _py_str(row[1])) for row in cursor.fetchall()]


def fetch_db2_columns(cursor, schema: str, table_name: str) -> List[DB2Column]:
    """Fetch columns for a table from DB2 database."""
    cursor.execute("""
        SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS, DEFAULT
        FROM SYSCAT.COLUMNS
        WHERE TABSCHEMA = ? AND TABNAME = ?
        ORDER BY COLNO
    """, [schema, table_name])
    
    columns = []
    for row in cursor.fetchall():
        columns.append(DB2Column(
            schema_name=schema,
            table_name=table_name,
            column_name=_py_str(row[0]),
            data_type=_py_str(row[1]),
            length=_py_int(row[2]),
            scale=_py_int(row[3]),
            nullable=_py_str(row[4]) == 'Y',
            default=_py_str(row[5])
        ))
    return columns


def fetch_db2_indexes(cursor, schema: Optional[str] = None) -> List[DB2Index]:
    """Fetch indexes from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT INDSCHEMA, INDNAME, TABNAME, UNIQUERULE, COLNAMES
            FROM SYSCAT.INDEXES
            WHERE INDSCHEMA = ?
            ORDER BY INDSCHEMA, INDNAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT INDSCHEMA, INDNAME, TABNAME, UNIQUERULE, COLNAMES
            FROM SYSCAT.INDEXES
            WHERE INDSCHEMA NOT LIKE 'SYS%'
            ORDER BY INDSCHEMA, INDNAME
        """)
    
    indexes = []
    for row in cursor.fetchall():
        # Parse column names from the COLNAMES column (convert to Python str for .split())
        col_str = _py_str(row[4])
        columns = [c.strip().lstrip('+').lstrip('-') for c in col_str.split() if c.strip()]
        
        indexes.append(DB2Index(
            schema_name=_py_str(row[0]),
            index_name=_py_str(row[1]),
            table_name=_py_str(row[2]),
            unique=_py_str(row[3]) in ('U', 'P'),
            columns=columns
        ))
    return indexes


def fetch_db2_primary_keys(cursor, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch primary keys from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT CONSTNAME, TABSCHEMA, TABNAME
            FROM SYSCAT.TABCONST
            WHERE TYPE = 'P'
            AND TABSCHEMA = ?
            ORDER BY TABSCHEMA, TABNAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT CONSTNAME, TABSCHEMA, TABNAME
            FROM SYSCAT.TABCONST
            WHERE TYPE = 'P'
            AND TABSCHEMA NOT LIKE 'SYS%'
            ORDER BY TABSCHEMA, TABNAME
        """)
    
    pks = []
    for row in cursor.fetchall():
        pks.append({
            'constraint_name': _py_str(row[0]),
            'schema_name': _py_str(row[1]),
            'table_name': _py_str(row[2])
        })
    return pks


def fetch_db2_foreign_keys(cursor, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch foreign keys from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT CONSTNAME, TABSCHEMA, TABNAME, REFTABSCHEMA, REFTABNAME
            FROM SYSCAT.REFERENCES
            WHERE TABSCHEMA = ?
            ORDER BY TABSCHEMA, CONSTNAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT CONSTNAME, TABSCHEMA, TABNAME, REFTABSCHEMA, REFTABNAME
            FROM SYSCAT.REFERENCES
            WHERE TABSCHEMA NOT LIKE 'SYS%'
            ORDER BY TABSCHEMA, CONSTNAME
        """)
    
    fks = []
    for row in cursor.fetchall():
        fks.append({
            'constraint_name': _py_str(row[0]),
            'schema_name': _py_str(row[1]),
            'table_name': _py_str(row[2]),
            'ref_schema': _py_str(row[3]),
            'ref_table': _py_str(row[4])
        })
    return fks


def fetch_db2_check_constraints(cursor, schema: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch check constraints from DB2 database."""
    if schema:
        cursor.execute("""
            SELECT CONSTNAME, TABSCHEMA, TABNAME, TEXT
            FROM SYSCAT.CHECKS
            WHERE TABSCHEMA = ?
            ORDER BY TABSCHEMA, CONSTNAME
        """, [schema])
    else:
        cursor.execute("""
            SELECT CONSTNAME, TABSCHEMA, TABNAME, TEXT
            FROM SYSCAT.CHECKS
            WHERE TABSCHEMA NOT LIKE 'SYS%'
            ORDER BY TABSCHEMA, CONSTNAME
        """)
    
    checks = []
    for row in cursor.fetchall():
        checks.append({
            'constraint_name': _py_str(row[0]),
            'schema_name': _py_str(row[1]),
            'table_name': _py_str(row[2]),
            'definition': _py_str(row[3])
        })
    return checks


def get_db2_schema_objects(conn, object_types: List[str], schema: Optional[str] = None,
                           logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Fetch all schema objects from a DB2 database connection.
    
    Args:
        conn: Database connection (jaydebeapi or similar)
        object_types: List of object types to fetch
        schema: Optional schema to filter by
        logger: Optional logger
        
    Returns:
        Dictionary with object type as key and list of objects as value
    """
    cur = conn.cursor()
    result = {}
    
    if 'tables' in object_types:
        if logger:
            logger.info(f"Fetching DB2 tables (schema: {schema or 'all'})...")
        tables = fetch_db2_tables(cur, schema)
        result['tables'] = [(t.schema_name, t.table_name) for t in tables]
        if logger:
            logger.info(f"Found {len(result['tables'])} tables")
    
    if 'views' in object_types:
        if logger:
            logger.info("Fetching DB2 views...")
        result['views'] = fetch_db2_views(cur, schema)
        if logger:
            logger.info(f"Found {len(result['views'])} views")
    
    if 'procedures' in object_types:
        if logger:
            logger.info("Fetching DB2 procedures...")
        result['procedures'] = fetch_db2_procedures(cur, schema)
        if logger:
            logger.info(f"Found {len(result['procedures'])} procedures")
    
    if 'functions' in object_types:
        if logger:
            logger.info("Fetching DB2 functions...")
        result['functions'] = fetch_db2_functions(cur, schema)
        if logger:
            logger.info(f"Found {len(result['functions'])} functions")
    
    if 'triggers' in object_types:
        if logger:
            logger.info("Fetching DB2 triggers...")
        result['triggers'] = fetch_db2_triggers(cur, schema)
        if logger:
            logger.info(f"Found {len(result['triggers'])} triggers")
    
    if 'sequences' in object_types:
        if logger:
            logger.info("Fetching DB2 sequences...")
        result['sequences'] = fetch_db2_sequences(cur, schema)
        if logger:
            logger.info(f"Found {len(result['sequences'])} sequences")
    
    # DB2 doesn't have synonyms in the same way - skip or handle differently
    if 'synonyms' in object_types:
        result['synonyms'] = []  # DB2 uses aliases instead
    
    if 'indexes' in object_types:
        if logger:
            logger.info("Fetching DB2 indexes...")
        indexes = fetch_db2_indexes(cur, schema)
        # Convert to string representation for comparison
        idx_strs = [f"{idx.schema_name}.{idx.index_name} ON {idx.table_name}" for idx in indexes]
        result['indexes'] = '\n'.join(idx_strs)
        if logger:
            logger.info(f"Found {len(indexes)} indexes")
    
    if 'primary_keys' in object_types:
        if logger:
            logger.info("Fetching DB2 primary keys...")
        pks = fetch_db2_primary_keys(cur, schema)
        pk_strs = [f"{pk['schema_name']}.{pk['table_name']}.{pk['constraint_name']}" for pk in pks]
        result['primary_keys'] = '\n'.join(pk_strs)
        if logger:
            logger.info(f"Found {len(pks)} primary keys")
    
    if 'foreign_keys' in object_types:
        if logger:
            logger.info("Fetching DB2 foreign keys...")
        fks = fetch_db2_foreign_keys(cur, schema)
        fk_strs = [f"{fk['schema_name']}.{fk['table_name']}.{fk['constraint_name']} -> {fk['ref_schema']}.{fk['ref_table']}" for fk in fks]
        result['foreign_keys'] = '\n'.join(fk_strs)
        if logger:
            logger.info(f"Found {len(fks)} foreign keys")
    
    if 'check_constraints' in object_types:
        if logger:
            logger.info("Fetching DB2 check constraints...")
        checks = fetch_db2_check_constraints(cur, schema)
        chk_strs = [f"{chk['schema_name']}.{chk['table_name']}.{chk['constraint_name']}" for chk in checks]
        result['check_constraints'] = '\n'.join(chk_strs)
        if logger:
            logger.info(f"Found {len(checks)} check constraints")
    
    # DB2 doesn't have default constraints as separate objects
    if 'default_constraints' in object_types:
        result['default_constraints'] = ''  # Defaults are part of column definitions in DB2
    
    return result


# =============================================================================
# Database-agnostic helper functions
# These work with both SQL Server and DB2
# =============================================================================

def get_tables_query(db_type: str, schema: Optional[str] = None) -> Tuple[str, List]:
    """
    Get the SQL query and parameters to list tables.
    
    Args:
        db_type: 'sqlserver' or 'db2'
        schema: Optional schema to filter
        
    Returns:
        Tuple of (sql_query, parameters)
    """
    if db_type == 'db2':
        if schema:
            return ("""
                SELECT TABSCHEMA, TABNAME 
                FROM SYSCAT.TABLES 
                WHERE TYPE = 'T' AND TABSCHEMA = ?
                ORDER BY TABSCHEMA, TABNAME
            """, [schema])
        else:
            return ("""
                SELECT TABSCHEMA, TABNAME 
                FROM SYSCAT.TABLES 
                WHERE TYPE = 'T' AND TABSCHEMA NOT LIKE 'SYS%'
                ORDER BY TABSCHEMA, TABNAME
            """, [])
    else:  # SQL Server
        return ("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """, [])


def get_columns_query(db_type: str, schema: str, table_name: str) -> Tuple[str, List]:
    """
    Get the SQL query and parameters to list columns for a table.
    
    Args:
        db_type: 'sqlserver' or 'db2'
        schema: Schema name
        table_name: Table name
        
    Returns:
        Tuple of (sql_query, parameters)
    """
    if db_type == 'db2':
        return ("""
            SELECT COLNAME, TYPENAME, LENGTH, 
                   CASE WHEN NULLS = 'Y' THEN 'YES' ELSE 'NO' END
            FROM SYSCAT.COLUMNS
            WHERE TABSCHEMA = ? AND TABNAME = ?
            ORDER BY COLNO
        """, [schema, table_name])
    else:  # SQL Server
        return ("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, [schema, table_name])


def get_table_exists_query(db_type: str, schema: str, table_name: str) -> Tuple[str, List]:
    """
    Get the SQL query to check if a table exists.
    
    Args:
        db_type: 'sqlserver' or 'db2'
        schema: Schema name
        table_name: Table name
        
    Returns:
        Tuple of (sql_query, parameters)
    """
    if db_type == 'db2':
        return ("""
            SELECT COUNT(*) FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = ? AND TABNAME = ?
        """, [schema, table_name])
    else:  # SQL Server
        return ("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, [schema, table_name])


def fetch_tables_generic(cursor, db_type: str, schema: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    Fetch tables from either SQL Server or DB2.
    
    Args:
        cursor: Database cursor
        db_type: 'sqlserver' or 'db2'
        schema: Optional schema to filter
        
    Returns:
        List of (schema_name, table_name) tuples
    """
    sql, params = get_tables_query(db_type, schema)
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    
    tables = []
    for row in cursor.fetchall():
        schema_name = _py_str(row[0])
        table_name = _py_str(row[1])
        tables.append((schema_name, table_name))
    return tables


def fetch_columns_generic(cursor, db_type: str, schema: str, table_name: str) -> List[Dict[str, Any]]:
    """
    Fetch columns for a table from either SQL Server or DB2.
    
    Args:
        cursor: Database cursor
        db_type: 'sqlserver' or 'db2'
        schema: Schema name
        table_name: Table name
        
    Returns:
        List of column dictionaries with name, type, length, nullable
    """
    sql, params = get_columns_query(db_type, schema, table_name)
    cursor.execute(sql, params)
    
    columns = []
    for row in cursor.fetchall():
        col_name = _py_str(row[0])
        data_type = _py_str(row[1])
        columns.append({
            'name': col_name,
            'type': data_type,
            'length': _py_int(row[2]) if len(row) > 2 else 0,
            'nullable': _py_str(row[3]) if len(row) > 3 else ''
        })
    return columns


def table_exists_generic(cursor, db_type: str, schema: str, table_name: str) -> bool:
    """
    Check if a table exists in either SQL Server or DB2.
    
    Args:
        cursor: Database cursor
        db_type: 'sqlserver' or 'db2'
        schema: Schema name
        table_name: Table name
        
    Returns:
        True if table exists, False otherwise
    """
    sql, params = get_table_exists_query(db_type, schema, table_name)
    cursor.execute(sql, params)
    result = cursor.fetchone()
    return result[0] > 0 if result else False
