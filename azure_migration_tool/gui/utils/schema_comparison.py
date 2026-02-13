# Author: S@tish Chauhan

"""
Schema comparison utilities for comparing source and destination databases.
Supports both SQL Server and DB2.
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
import pyodbc

# Import from backup/restore modules
import sys
import os
from pathlib import Path

# Prefer local backup exporters (backup.exporters); fallback to src.backup.exporters
current_file = Path(__file__).resolve()
parent_dir = current_file.parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

_IMPORTS_SUCCESSFUL = False
_IMPORT_ERROR = None

try:
    from backup.exporters import (
        fetch_tables, fetch_columns, fetch_primary_key,
        fetch_objects, fetch_triggers, fetch_sequences, fetch_synonyms,
        export_foreign_keys, export_check_constraints, export_default_constraints,
        export_indexes, export_primary_keys, object_definition
    )
    _IMPORTS_SUCCESSFUL = True
except ImportError as e1:
    try:
        from src.backup.exporters import (
            fetch_tables, fetch_columns, fetch_primary_key,
            fetch_objects, fetch_triggers, fetch_sequences, fetch_synonyms,
            export_foreign_keys, export_check_constraints, export_default_constraints,
            export_indexes, export_primary_keys, object_definition
        )
        _IMPORTS_SUCCESSFUL = True
    except ImportError as e:
        _IMPORT_ERROR = str(e)
        fetch_tables = None
        fetch_columns = None
        fetch_primary_key = None
        fetch_objects = None
        fetch_triggers = None
        fetch_sequences = None
        fetch_synonyms = None
        export_foreign_keys = None
        export_check_constraints = None
        export_default_constraints = None
        export_indexes = None
        export_primary_keys = None
        object_definition = None

# Import DB2-specific schema utilities
try:
    from gui.utils.db2_schema import is_db2_connection, get_db2_schema_objects
    _DB2_AVAILABLE = True
except ImportError:
    _DB2_AVAILABLE = False
    is_db2_connection = None
    get_db2_schema_objects = None


def get_schema_objects(conn: pyodbc.Connection, object_types: List[str], logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Fetch all schema objects from a database connection.
    
    Args:
        conn: Database connection
        object_types: List of object types to fetch (e.g., ['tables', 'views', 'procedures'])
        logger: Optional logger
        
    Returns:
        Dictionary with object type as key and list of objects as value
    """
    if not fetch_tables:
        error_msg = "Backup exporters module not available."
        if not _IMPORTS_SUCCESSFUL:
            if _IMPORT_ERROR:
                error_msg += f" Import error: {_IMPORT_ERROR}. "
            error_msg += f" Parent dir: {parent_dir}. "
            error_msg += f" Current file: {current_file}. "
            # Check if file exists
            exporters_path = parent_dir / "src" / "backup" / "exporters.py"
            if exporters_path.exists():
                error_msg += f" File exists at: {exporters_path}. "
            else:
                error_msg += f" File NOT found at: {exporters_path}. "
        error_msg += " Please ensure backup.exporters (or src/backup/exporters.py) is available."
        raise ImportError(error_msg)
    
    cur = conn.cursor()
    result = {}
    
    if 'tables' in object_types:
        if logger:
            logger.info("Fetching tables...")
        tables = fetch_tables(cur)
        result['tables'] = [(t.schema_name, t.table_name) for t in tables]
        if logger:
            logger.info(f"Found {len(result['tables'])} tables")
    
    if 'views' in object_types:
        if logger:
            logger.info("Fetching views...")
        views = fetch_objects(cur, "V")
        result['views'] = [(v[0], v[1]) for v in views]  # (schema_name, object_name)
        if logger:
            logger.info(f"Found {len(result['views'])} views")
    
    if 'procedures' in object_types:
        if logger:
            logger.info("Fetching procedures...")
        procs = fetch_objects(cur, "P")
        result['procedures'] = [(p[0], p[1]) for p in procs]
        if logger:
            logger.info(f"Found {len(result['procedures'])} procedures")
    
    if 'functions' in object_types:
        if logger:
            logger.info("Fetching functions...")
        funcs = fetch_objects(cur, "FN,TF,IF")
        result['functions'] = [(f[0], f[1]) for f in funcs]
        if logger:
            logger.info(f"Found {len(result['functions'])} functions")
    
    if 'triggers' in object_types:
        if logger:
            logger.info("Fetching triggers...")
        triggers = fetch_triggers(cur)
        result['triggers'] = [(t.schema_name, t.trigger_name, t.table_name) for t in triggers]
        if logger:
            logger.info(f"Found {len(result['triggers'])} triggers")
    
    if 'sequences' in object_types:
        if logger:
            logger.info("Fetching sequences...")
        sequences = fetch_sequences(cur)
        result['sequences'] = [(s.schema_name, s.sequence_name) for s in sequences]
        if logger:
            logger.info(f"Found {len(result['sequences'])} sequences")
    
    if 'synonyms' in object_types:
        if logger:
            logger.info("Fetching synonyms...")
        synonyms = fetch_synonyms(cur)
        result['synonyms'] = [(s.schema_name, s.synonym_name) for s in synonyms]
        if logger:
            logger.info(f"Found {len(result['synonyms'])} synonyms")
    
    if 'foreign_keys' in object_types:
        if logger:
            logger.info("Fetching foreign keys...")
        # Get FK definitions as SQL for comparison
        fk_sql, _ = export_foreign_keys(cur, logger)
        result['foreign_keys'] = fk_sql
        if logger:
            logger.info("Foreign keys exported")
    
    if 'check_constraints' in object_types:
        if logger:
            logger.info("Fetching check constraints...")
        chk_sql = export_check_constraints(cur)
        result['check_constraints'] = chk_sql
        if logger:
            logger.info("Check constraints exported")
    
    if 'default_constraints' in object_types:
        if logger:
            logger.info("Fetching default constraints...")
        def_sql = export_default_constraints(cur)
        result['default_constraints'] = def_sql
        if logger:
            logger.info("Default constraints exported")
    
    if 'indexes' in object_types:
        if logger:
            logger.info("Fetching indexes...")
        idx_sql, _ = export_indexes(cur, logger)
        result['indexes'] = idx_sql
        if logger:
            logger.info("Indexes exported")
    
    if 'primary_keys' in object_types:
        if logger:
            logger.info("Fetching primary keys...")
        pk_sql = export_primary_keys(cur)
        result['primary_keys'] = pk_sql
        if logger:
            logger.info("Primary keys exported")
    
    return result


def normalize_object_key(obj: Tuple) -> str:
    """
    Normalize an object tuple to a comparable key.
    Handles whitespace stripping and case normalization.
    """
    if isinstance(obj, tuple):
        # Strip whitespace from each element and convert to uppercase for comparison
        normalized = tuple(
            str(item).strip().upper() if item is not None else ''
            for item in obj
        )
        return '.'.join(normalized)
    else:
        return str(obj).strip().upper()


def compare_object_lists(src_objects: List[Tuple], dest_objects: List[Tuple], object_type: str) -> Dict[str, Any]:
    """
    Compare two lists of objects and identify missing, extra, and matching objects.
    
    Args:
        src_objects: List of source objects (tuples like (schema, name) or (schema, name, table))
        dest_objects: List of destination objects (same format)
        object_type: Type of object being compared (for reporting)
        
    Returns:
        Dictionary with 'missing', 'extra', 'matching' lists
    """
    # Normalize to comparable keys (stripped, uppercase)
    src_map = {normalize_object_key(obj): obj for obj in src_objects}
    dest_map = {normalize_object_key(obj): obj for obj in dest_objects}
    
    src_keys = set(src_map.keys())
    dest_keys = set(dest_map.keys())
    
    # Find differences
    missing_keys = src_keys - dest_keys
    extra_keys = dest_keys - src_keys
    matching_keys = src_keys & dest_keys
    
    # Map back to original objects
    missing = [src_map[key] for key in missing_keys]
    extra = [dest_map[key] for key in extra_keys]
    matching = [src_map[key] for key in matching_keys]
    
    return {
        'missing': missing,
        'extra': extra,
        'matching': matching,
        'object_type': object_type
    }


def compare_object_definitions(src_conn: pyodbc.Connection, dest_conn: pyodbc.Connection,
                                schema_name: str, object_name: str, object_type: str,
                                logger: Optional[logging.Logger] = None) -> str:
    """
    Compare object definitions between source and destination.
    
    Returns:
        'match', 'different', 'missing', or 'extra'
    """
    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()
    
    # Get object IDs
    src_cur.execute("""
        SELECT o.object_id
        FROM sys.objects o
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = ? AND o.name = ?
    """, schema_name, object_name)
    src_row = src_cur.fetchone()
    
    dest_cur.execute("""
        SELECT o.object_id
        FROM sys.objects o
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = ? AND o.name = ?
    """, schema_name, object_name)
    dest_row = dest_cur.fetchone()
    
    if not src_row:
        return 'missing'
    if not dest_row:
        return 'extra'
    
    # Get definitions
    src_def = object_definition(src_cur, src_row[0])
    dest_def = object_definition(dest_cur, dest_row[0])
    
    if src_def == dest_def:
        return 'match'
    else:
        return 'different'


def compare_schemas(src_conn, dest_conn,
                   object_types: Optional[List[str]] = None,
                   src_schema: Optional[str] = None,
                   dest_schema: Optional[str] = None,
                   logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """
    Compare source and destination schemas.
    Supports both SQL Server and DB2 connections.
    
    Args:
        src_conn: Source database connection (pyodbc or jaydebeapi)
        dest_conn: Destination database connection (pyodbc or jaydebeapi)
        object_types: List of object types to compare (default: all)
        src_schema: Source schema name (for DB2 filtering)
        dest_schema: Destination schema name (for DB2/SQL Server filtering)
        logger: Optional logger
        
    Returns:
        Dictionary with comparison results for each object type
    """
    # Detect DB2 connections
    src_is_db2 = _DB2_AVAILABLE and is_db2_connection and is_db2_connection(src_conn)
    dest_is_db2 = _DB2_AVAILABLE and is_db2_connection and is_db2_connection(dest_conn)
    
    if logger:
        logger.info(f"Source is DB2: {src_is_db2}, Destination is DB2: {dest_is_db2}")
    
    # Check if SQL Server modules are available (only needed if not both are DB2)
    if not src_is_db2 or not dest_is_db2:
        if not fetch_tables:
            error_msg = "Backup exporters module not available for SQL Server."
            if not _IMPORTS_SUCCESSFUL:
                if _IMPORT_ERROR:
                    error_msg += f" Import error: {_IMPORT_ERROR}. "
                error_msg += f" Parent dir: {parent_dir}. "
                exporters_path = parent_dir / "src" / "backup" / "exporters.py"
                if exporters_path.exists():
                    error_msg += f" File exists at: {exporters_path}. "
                else:
                    error_msg += f" File NOT found at: {exporters_path}. "
            error_msg += " Please ensure backup.exporters (or src/backup/exporters.py) is available."
            raise ImportError(error_msg)
    
    if object_types is None:
        object_types = ['tables', 'views', 'procedures', 'functions', 'triggers',
                       'sequences', 'synonyms', 'foreign_keys', 'check_constraints',
                       'default_constraints', 'indexes', 'primary_keys']
    
    if logger:
        logger.info(f"Comparing schemas for object types: {', '.join(object_types)}")
    
    # Fetch objects from both databases using appropriate method
    if src_is_db2:
        if logger:
            logger.info(f"Fetching DB2 source objects (schema: {src_schema or 'all'})...")
        src_objects = get_db2_schema_objects(src_conn, object_types, src_schema, logger)
    else:
        if logger:
            logger.info("Fetching SQL Server source objects...")
        src_objects = get_schema_objects(src_conn, object_types, logger)
    
    if dest_is_db2:
        if logger:
            logger.info(f"Fetching DB2 destination objects (schema: {dest_schema or 'all'})...")
        dest_objects = get_db2_schema_objects(dest_conn, object_types, dest_schema, logger)
    else:
        if logger:
            logger.info("Fetching SQL Server destination objects...")
        dest_objects = get_schema_objects(dest_conn, object_types, logger)
    
    comparison = {}
    
    # Compare list-based objects (tables, views, procedures, etc.)
    list_types = ['tables', 'views', 'procedures', 'functions', 'triggers', 'sequences', 'synonyms']
    for obj_type in list_types:
        if obj_type in object_types:
            src_list = src_objects.get(obj_type, [])
            dest_list = dest_objects.get(obj_type, [])
            comparison[obj_type] = compare_object_lists(src_list, dest_list, obj_type)
    
    # Compare SQL-based objects (constraints, indexes) by comparing SQL text
    sql_types = ['foreign_keys', 'check_constraints', 'default_constraints', 'indexes', 'primary_keys']
    for obj_type in sql_types:
        if obj_type in object_types:
            src_sql = src_objects.get(obj_type, '')
            dest_sql = dest_objects.get(obj_type, '')
            
            if src_sql == dest_sql:
                status = 'match'
            elif not src_sql:
                status = 'missing'
            elif not dest_sql:
                status = 'extra'
            else:
                status = 'different'
            
            comparison[obj_type] = {
                'status': status,
                'object_type': obj_type,
                'src_sql': src_sql,
                'dest_sql': dest_sql
            }
    
    return comparison
