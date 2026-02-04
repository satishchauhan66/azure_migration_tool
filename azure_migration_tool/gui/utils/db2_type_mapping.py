"""
DB2 to SQL Server data type mapping validation.

This module provides type mapping rules for validating that DB2 column types
are correctly converted to SQL Server types during migration.
"""

from typing import Dict, Any, List, Optional, Tuple


# DB2 to SQL Server type mapping
# Based on standard migration patterns and the user's reference mapping table
DB2_TO_SQL_TYPE_MAP = {
    # Integer types
    'INTEGER': 'INT',
    'INT': 'INT',
    'SMALLINT': 'SMALLINT',
    'BIGINT': 'BIGINT',
    
    # Character types - VARCHAR can map to either VARCHAR or NVARCHAR
    'VARCHAR': 'VARCHAR',  # Changed: VARCHAR -> VARCHAR is the standard mapping
    'CHAR': 'CHAR',
    'CHARACTER': 'CHAR',
    'GRAPHIC': 'NCHAR',
    'VARGRAPHIC': 'NVARCHAR',
    
    # Large object types - these map to MAX types
    'CLOB': 'VARCHAR',  # CLOB -> VARCHAR(MAX)
    'DCLOB': 'NVARCHAR',  # DCLOB -> NVARCHAR(MAX) (double-byte CLOB)
    'DBCLOB': 'NVARCHAR',  # DBCLOB -> NVARCHAR(MAX)
    'BLOB': 'VARBINARY',  # BLOB -> VARBINARY(MAX)
    'LONG VARCHAR': 'VARCHAR',  # -> VARCHAR(MAX)
    
    # Date/Time types
    'TIMESTAMP': 'DATETIME2',
    'DATE': 'DATE',
    'TIME': 'TIME',
    
    # Numeric types
    'DECIMAL': 'DECIMAL',
    'NUMERIC': 'NUMERIC',  # Changed: NUMERIC -> NUMERIC (not DECIMAL)
    'DEC': 'DECIMAL',
    
    # Floating point types
    'DOUBLE': 'FLOAT',
    'FLOAT': 'FLOAT',
    'DOUBLE PRECISION': 'FLOAT',
    'REAL': 'REAL',
    'DECFLOAT': 'FLOAT',
    
    # Binary types
    'BINARY': 'BINARY',
    'VARBINARY': 'VARBINARY',
    'CHAR FOR BIT DATA': 'BINARY',
    'VARCHAR FOR BIT DATA': 'VARBINARY',
    
    # Other types
    'XML': 'XML',
    'BOOLEAN': 'BIT',
    'ROWID': 'VARBINARY',
}

# LOB types that should accept MAX (-1) length in SQL Server
LOB_TYPES = {'BLOB', 'CLOB', 'DCLOB', 'DBCLOB', 'LONG VARCHAR'}

# Alternative acceptable mappings (treated as SUCCESS, not just warnings)
# These are correct mappings even if they differ from the "primary" expected type
ACCEPTABLE_SUCCESS_MAPPINGS = {
    # VARCHAR can map to either VARCHAR or NVARCHAR (both are correct)
    ('VARCHAR', 'NVARCHAR'): True,
    ('VARCHAR', 'VARCHAR'): True,
    # CHAR can map to CHAR or NCHAR
    ('CHAR', 'NCHAR'): True,
    ('CHAR', 'CHAR'): True,
    # CLOB mappings
    ('CLOB', 'VARCHAR'): True,
    ('CLOB', 'NVARCHAR'): True,
    # BLOB mappings
    ('BLOB', 'VARBINARY'): True,
    # DECIMAL/NUMERIC are equivalent
    ('DECIMAL', 'NUMERIC'): True,
    ('NUMERIC', 'DECIMAL'): True,
}

# Alternative mappings that work but are warned about
ACCEPTABLE_WARNING_MAPPINGS = {
    ('VARCHAR', 'VARBINARY'): True,  # Binary data stored as varchar
    ('CLOB', 'TEXT'): True,  # Legacy type
    ('BLOB', 'IMAGE'): True,  # Legacy type
    ('TIMESTAMP', 'DATETIME'): True,  # Lower precision acceptable
    ('INTEGER', 'BIGINT'): True,  # Wider type acceptable
    ('SMALLINT', 'INT'): True,  # Wider type acceptable
    ('SMALLINT', 'BIGINT'): True,  # Wider type acceptable
    ('DOUBLE', 'REAL'): True,  # Lower precision
}


def normalize_db2_type(db2_type: str) -> str:
    """Normalize DB2 type name to uppercase and handle variations."""
    if not db2_type:
        return ''
    
    db2_type = str(db2_type).strip().upper()
    
    # Handle common variations
    if db2_type.startswith('CHARACTER VARYING'):
        return 'VARCHAR'
    if db2_type.startswith('CHAR VARYING'):
        return 'VARCHAR'
    if 'FOR BIT DATA' in db2_type:
        if 'VARCHAR' in db2_type:
            return 'VARCHAR FOR BIT DATA'
        return 'CHAR FOR BIT DATA'
    if db2_type.startswith('LONG VARCHAR'):
        return 'LONG VARCHAR'
    
    return db2_type


def normalize_sql_type(sql_type: str) -> str:
    """Normalize SQL Server type name to uppercase."""
    if not sql_type:
        return ''
    return str(sql_type).strip().upper()


def get_expected_sql_type(db2_type: str) -> str:
    """
    Get expected SQL Server type for a DB2 type.
    
    Args:
        db2_type: DB2 data type name
        
    Returns:
        Expected SQL Server data type name
    """
    db2_type_norm = normalize_db2_type(db2_type)
    return DB2_TO_SQL_TYPE_MAP.get(db2_type_norm, db2_type_norm)


def validate_type_mapping(
    db2_type: str, 
    db2_length: Optional[int], 
    db2_scale: Optional[int],
    sql_type: str, 
    sql_length: Optional[int], 
    sql_scale: Optional[int],
    column_name: str = ''
) -> Dict[str, Any]:
    """
    Validate if DB2 type correctly maps to SQL Server type.
    
    Args:
        db2_type: DB2 data type
        db2_length: DB2 column length/precision
        db2_scale: DB2 column scale
        sql_type: SQL Server data type
        sql_length: SQL Server column length/precision
        sql_scale: SQL Server column scale
        column_name: Optional column name for error messages
        
    Returns:
        Dictionary with:
        - status: 'SUCCESS' | 'WARNING' | 'ERROR'
        - expected_type: Expected SQL Server type
        - actual_type: Actual SQL Server type
        - message: Human-readable message
        - details: Additional details
    """
    db2_type_norm = normalize_db2_type(db2_type)
    sql_type_norm = normalize_sql_type(sql_type)
    expected_type = get_expected_sql_type(db2_type)
    
    # Handle None values
    db2_length = db2_length if db2_length is not None else 0
    db2_scale = db2_scale if db2_scale is not None else 0
    sql_length = sql_length if sql_length is not None else 0
    sql_scale = sql_scale if sql_scale is not None else 0
    
    col_info = f" for column '{column_name}'" if column_name else ""
    
    # Check if this is a LOB type - LOB types to MAX (-1) are always correct
    is_lob_type = db2_type_norm in LOB_TYPES
    
    # Check if the type mapping is correct (including acceptable alternatives)
    type_matches = (expected_type == sql_type_norm) or (db2_type_norm, sql_type_norm) in ACCEPTABLE_SUCCESS_MAPPINGS
    
    if type_matches:
        # Type name matches - now check size constraints
        
        # LOB types (BLOB, CLOB, etc.) - ANY size to MAX (-1) is correct
        if is_lob_type:
            if sql_length == -1:
                # MAX type - always correct for LOB
                return {
                    'status': 'SUCCESS',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'✓ Correct mapping{col_info}: {db2_type}({db2_length}) → {sql_type_norm}(MAX)',
                    'details': {'lob_to_max': True}
                }
            elif sql_length >= db2_length or sql_length > 8000:
                # Large enough
                return {
                    'status': 'SUCCESS',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'✓ Correct mapping{col_info}: {db2_type}({db2_length}) → {sql_type_norm}({sql_length})',
                    'details': {'lob_sized': True}
                }
            else:
                return {
                    'status': 'WARNING',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'⚠ Destination may be too small{col_info}: {db2_type}({db2_length}) → {sql_type_norm}({sql_length})',
                    'details': {'lob_warning': True}
                }
        
        # String/Binary types - check length
        elif sql_type_norm in ('NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'VARBINARY', 'BINARY'):
            # -1 means MAX in SQL Server - always acceptable
            if sql_length == -1:
                return {
                    'status': 'SUCCESS',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'✓ Correct mapping{col_info}: {db2_type}({db2_length}) → {sql_type_norm}(MAX)',
                    'details': {'length_match': True, 'max_type': True}
                }
            elif db2_length == sql_length:
                return {
                    'status': 'SUCCESS',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'✓ Correct mapping{col_info}: {db2_type}({db2_length}) → {sql_type_norm}({sql_length})',
                    'details': {'length_match': True}
                }
            elif sql_length >= db2_length and sql_length > 0:
                # SQL length is larger - acceptable
                return {
                    'status': 'SUCCESS',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'✓ Correct mapping{col_info}: {db2_type}({db2_length}) → {sql_type_norm}({sql_length})',
                    'details': {'length_match': True, 'larger_dest': True}
                }
            else:
                return {
                    'status': 'WARNING',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'⚠ Destination length smaller{col_info}: {db2_type}({db2_length}) → {sql_type_norm}({sql_length})',
                    'details': {'length_match': False, 'db2_length': db2_length, 'sql_length': sql_length}
                }
        
        # Decimal/Numeric types - check precision and scale
        elif sql_type_norm in ('DECIMAL', 'NUMERIC'):
            if db2_length == sql_length and db2_scale == sql_scale:
                return {
                    'status': 'SUCCESS',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'✓ Correct mapping{col_info}: {db2_type}({db2_length},{db2_scale}) → {sql_type_norm}({sql_length},{sql_scale})',
                    'details': {'precision_match': True, 'scale_match': True}
                }
            elif sql_length >= db2_length:
                # Larger precision is acceptable
                return {
                    'status': 'SUCCESS',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'✓ Correct mapping{col_info}: {db2_type}({db2_length},{db2_scale}) → {sql_type_norm}({sql_length},{sql_scale})',
                    'details': {'precision_match': True, 'scale_match': db2_scale == sql_scale}
                }
            else:
                return {
                    'status': 'WARNING',
                    'expected_type': expected_type,
                    'actual_type': sql_type_norm,
                    'message': f'⚠ Precision smaller{col_info}: {db2_type}({db2_length},{db2_scale}) → {sql_type_norm}({sql_length},{sql_scale})',
                    'details': {'precision_match': False, 'scale_match': False}
                }
        
        # DATETIME2 - TIMESTAMP mapping
        elif sql_type_norm == 'DATETIME2':
            # TIMESTAMP -> DATETIME2(7) is standard
            return {
                'status': 'SUCCESS',
                'expected_type': expected_type,
                'actual_type': sql_type_norm,
                'message': f'✓ Correct mapping{col_info}: {db2_type} → {sql_type_norm}',
                'details': {'scale': sql_scale}
            }
        
        # Integer types - exact match
        elif sql_type_norm in ('INT', 'SMALLINT', 'BIGINT', 'TINYINT'):
            return {
                'status': 'SUCCESS',
                'expected_type': expected_type,
                'actual_type': sql_type_norm,
                'message': f'✓ Correct mapping{col_info}: {db2_type} → {sql_type_norm}',
                'details': {}
            }
        
        # All other types - exact match is success
        else:
            return {
                'status': 'SUCCESS',
                'expected_type': expected_type,
                'actual_type': sql_type_norm,
                'message': f'✓ Correct mapping{col_info}: {db2_type} → {sql_type_norm}',
                'details': {}
            }
    
    # Check if it's an acceptable alternative mapping (with warning)
    if (db2_type_norm, sql_type_norm) in ACCEPTABLE_WARNING_MAPPINGS:
        return {
            'status': 'WARNING',
            'expected_type': expected_type,
            'actual_type': sql_type_norm,
            'message': f'⚠ Alternative mapping{col_info}: {db2_type} → {sql_type_norm} (expected {expected_type})',
            'details': {'alternative_mapping': True}
        }
    
    # Check for compatible string types
    if sql_type_norm in ('NVARCHAR', 'VARCHAR') and expected_type in ('NVARCHAR', 'VARCHAR'):
        return {
            'status': 'SUCCESS',
            'expected_type': expected_type,
            'actual_type': sql_type_norm,
            'message': f'✓ Correct mapping{col_info}: {db2_type} → {sql_type_norm}',
            'details': {'string_variant': True}
        }
    
    # Type mismatch - this is an actual error
    return {
        'status': 'ERROR',
        'expected_type': expected_type,
        'actual_type': sql_type_norm,
        'message': f'✗ Type mismatch{col_info}: {db2_type} → {sql_type_norm} (expected {expected_type})',
        'details': {'db2_type': db2_type}
    }


def compare_columns_with_type_mapping(
    src_columns: List[Dict[str, Any]], 
    dest_columns: List[Dict[str, Any]],
    src_db_type: str = 'db2',
    dest_db_type: str = 'sqlserver'
) -> Dict[str, Any]:
    """
    Compare columns with type mapping awareness.
    
    For DB2 to SQL Server comparisons, validates that types are correctly mapped
    and reports success for valid mappings.
    
    Args:
        src_columns: Source columns as list of dicts with 'name', 'type', 'length', 'scale', 'nullable'
        dest_columns: Destination columns as list of dicts
        src_db_type: Source database type ('db2' or 'sqlserver')
        dest_db_type: Destination database type ('db2' or 'sqlserver')
        
    Returns:
        Dictionary with:
        - matching: List of matching columns with SUCCESS status
        - missing_in_dest: Columns only in source
        - extra_in_dest: Columns only in destination
        - type_issues: Columns with type warnings or errors
        - summary: Statistics (total, matched, warnings, errors)
    """
    results = {
        'matching': [],
        'missing_in_dest': [],
        'extra_in_dest': [],
        'type_issues': [],
        'summary': {
            'total': 0,
            'matched': 0,
            'correctly_mapped': 0,
            'warnings': 0,
            'errors': 0
        }
    }
    
    # Create lookup maps (case-insensitive)
    src_col_map = {str(col.get('name', '')).upper().strip(): col for col in src_columns}
    dest_col_map = {str(col.get('name', '')).upper().strip(): col for col in dest_columns}
    
    all_col_names = set(src_col_map.keys()) | set(dest_col_map.keys())
    results['summary']['total'] = len(all_col_names)
    
    for col_name in all_col_names:
        src_col = src_col_map.get(col_name)
        dest_col = dest_col_map.get(col_name)
        
        if not src_col:
            # Column only in destination
            results['extra_in_dest'].append({
                'column': col_name,
                'dest_type': dest_col.get('type', ''),
                'status': 'EXTRA'
            })
            continue
        
        if not dest_col:
            # Column only in source
            results['missing_in_dest'].append({
                'column': col_name,
                'src_type': src_col.get('type', ''),
                'status': 'MISSING'
            })
            continue
        
        # Both exist - compare with type mapping if cross-database
        if src_db_type == 'db2' and dest_db_type == 'sqlserver':
            mapping_result = validate_type_mapping(
                db2_type=src_col.get('type', ''),
                db2_length=src_col.get('length'),
                db2_scale=src_col.get('scale'),
                sql_type=dest_col.get('type', ''),
                sql_length=dest_col.get('length'),
                sql_scale=dest_col.get('scale'),
                column_name=col_name
            )
            
            column_info = {
                'column': col_name,
                'src_type': src_col.get('type', ''),
                'dest_type': dest_col.get('type', ''),
                'src_length': src_col.get('length'),
                'dest_length': dest_col.get('length'),
                'status': mapping_result['status'],
                'message': mapping_result['message']
            }
            
            if mapping_result['status'] == 'SUCCESS':
                results['matching'].append(column_info)
                results['summary']['correctly_mapped'] += 1
                results['summary']['matched'] += 1
            elif mapping_result['status'] == 'WARNING':
                results['type_issues'].append(column_info)
                results['summary']['warnings'] += 1
            else:  # ERROR
                results['type_issues'].append(column_info)
                results['summary']['errors'] += 1
        else:
            # Same database type - direct comparison
            src_type = str(src_col.get('type', '')).upper().strip()
            dest_type = str(dest_col.get('type', '')).upper().strip()
            
            if src_type == dest_type:
                results['matching'].append({
                    'column': col_name,
                    'src_type': src_type,
                    'dest_type': dest_type,
                    'status': 'SUCCESS',
                    'message': f'✓ Types match: {src_type}'
                })
                results['summary']['matched'] += 1
            else:
                results['type_issues'].append({
                    'column': col_name,
                    'src_type': src_type,
                    'dest_type': dest_type,
                    'status': 'ERROR',
                    'message': f'✗ Type mismatch: {src_type} vs {dest_type}'
                })
                results['summary']['errors'] += 1
    
    return results


def get_type_mapping_summary(results: Dict[str, Any], use_unicode: bool = True) -> str:
    """
    Generate a human-readable summary of type mapping results.
    
    Args:
        results: Results from compare_columns_with_type_mapping
        use_unicode: Whether to use Unicode symbols (default True for GUI, False for console)
        
    Returns:
        Formatted summary string
    """
    summary = results.get('summary', {})
    total = summary.get('total', 0)
    matched = summary.get('matched', 0)
    correctly_mapped = summary.get('correctly_mapped', 0)
    warnings = summary.get('warnings', 0)
    errors = summary.get('errors', 0)
    
    if use_unicode:
        ok_sym = "✓"
        warn_sym = "⚠"
        err_sym = "✗"
    else:
        ok_sym = "[OK]"
        warn_sym = "[WARN]"
        err_sym = "[ERR]"
    
    lines = [
        f"Column Comparison Summary:",
        f"  Total columns: {total}",
        f"  {ok_sym} Correctly mapped: {correctly_mapped}",
        f"  {warn_sym} Warnings: {warnings}",
        f"  {err_sym} Errors: {errors}",
        f"  Missing in destination: {len(results.get('missing_in_dest', []))}",
        f"  Extra in destination: {len(results.get('extra_in_dest', []))}"
    ]
    
    return '\n'.join(lines)
