# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Signature-based schema matching utilities.

Provides intelligent matching for indexes, foreign keys, and constraints
that may have different names but identical definitions.

Also includes default value normalization for accurate comparison.
"""

from typing import Dict, Any, List, Optional, Tuple, Set
import re


# =============================================================================
# INDEX SIGNATURE MATCHING
# =============================================================================

def normalize_column_name(col: str) -> str:
    """Normalize a column name for comparison."""
    if not col:
        return ''
    # Strip whitespace, remove order indicators (+/-), uppercase
    col = str(col).strip().upper()
    col = col.lstrip('+').lstrip('-')
    return col


def build_index_signature(columns: List[str], is_unique: bool, is_primary: bool = False) -> str:
    """
    Build a signature for an index based on its definition.
    
    The signature uniquely identifies the index by its columns and properties,
    regardless of the index name.
    
    Args:
        columns: List of column names in order
        is_unique: Whether the index is unique
        is_primary: Whether this is a primary key
        
    Returns:
        Signature string like "COL1,COL2:UNIQUE" or "COL1,COL2:NONUNIQUE"
    """
    # Normalize and sort columns (order-insensitive for matching)
    norm_cols = sorted([normalize_column_name(c) for c in columns if c])
    cols_str = ','.join(norm_cols)
    
    if is_primary:
        return f"{cols_str}:PK"
    elif is_unique:
        return f"{cols_str}:UNIQUE"
    else:
        return f"{cols_str}:NONUNIQUE"


def compare_indexes_with_signatures(
    src_indexes: List[Dict[str, Any]],
    dest_indexes: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Compare indexes using signature-based matching.
    
    When an index has a different name but same columns and properties,
    it's considered a match with a rename notation.
    
    Args:
        src_indexes: Source indexes with 'name', 'table', 'columns', 'is_unique', 'is_primary'
        dest_indexes: Destination indexes with same structure
        
    Returns:
        Dictionary with:
        - matched: List of matched indexes (including renamed ones)
        - missing_in_dest: Indexes only in source
        - extra_in_dest: Indexes only in destination
        - summary: Statistics
    """
    results = {
        'matched': [],
        'matched_by_name': [],
        'matched_by_signature': [],  # Renamed indexes
        'missing_in_dest': [],
        'extra_in_dest': [],
        'summary': {
            'total_source': len(src_indexes),
            'total_dest': len(dest_indexes),
            'matched': 0,
            'matched_renamed': 0,
            'missing': 0,
            'extra': 0
        }
    }
    
    # Build signature maps
    src_by_name = {}
    src_by_sig = {}
    for idx in src_indexes:
        name = str(idx.get('name', '')).upper().strip()
        table = str(idx.get('table', '')).upper().strip()
        key = f"{table}.{name}"
        src_by_name[key] = idx
        
        # Build signature
        columns = idx.get('columns', [])
        if isinstance(columns, str):
            columns = [c.strip() for c in columns.split(',') if c.strip()]
        is_unique = idx.get('is_unique', False)
        is_primary = idx.get('is_primary', False)
        sig = build_index_signature(columns, is_unique, is_primary)
        sig_key = f"{table}:{sig}"
        if sig_key not in src_by_sig:
            src_by_sig[sig_key] = []
        src_by_sig[sig_key].append(idx)
    
    dest_by_name = {}
    dest_by_sig = {}
    for idx in dest_indexes:
        name = str(idx.get('name', '')).upper().strip()
        table = str(idx.get('table', '')).upper().strip()
        key = f"{table}.{name}"
        dest_by_name[key] = idx
        
        columns = idx.get('columns', [])
        if isinstance(columns, str):
            columns = [c.strip() for c in columns.split(',') if c.strip()]
        is_unique = idx.get('is_unique', False)
        is_primary = idx.get('is_primary', False)
        sig = build_index_signature(columns, is_unique, is_primary)
        sig_key = f"{table}:{sig}"
        if sig_key not in dest_by_sig:
            dest_by_sig[sig_key] = []
        dest_by_sig[sig_key].append(idx)
    
    matched_src = set()
    matched_dest = set()
    
    # First pass: match by name
    for key, src_idx in src_by_name.items():
        if key in dest_by_name:
            dest_idx = dest_by_name[key]
            results['matched'].append({
                'source': src_idx,
                'dest': dest_idx,
                'match_type': 'NAME',
                'status': 'SUCCESS',
                'message': f"Index matched by name: {src_idx.get('name')}"
            })
            results['matched_by_name'].append(src_idx)
            matched_src.add(key)
            matched_dest.add(key)
            results['summary']['matched'] += 1
    
    # Second pass: match by signature for unmatched indexes
    for sig_key, src_list in src_by_sig.items():
        if sig_key in dest_by_sig:
            dest_list = dest_by_sig[sig_key]
            
            for src_idx in src_list:
                src_name = str(src_idx.get('name', '')).upper().strip()
                src_table = str(src_idx.get('table', '')).upper().strip()
                src_key = f"{src_table}.{src_name}"
                
                if src_key in matched_src:
                    continue
                
                # Find an unmatched dest index with same signature
                for dest_idx in dest_list:
                    dest_name = str(dest_idx.get('name', '')).upper().strip()
                    dest_table = str(dest_idx.get('table', '')).upper().strip()
                    dest_key = f"{dest_table}.{dest_name}"
                    
                    if dest_key in matched_dest:
                        continue
                    
                    # Found a signature match with different name
                    results['matched'].append({
                        'source': src_idx,
                        'dest': dest_idx,
                        'match_type': 'SIGNATURE',
                        'status': 'SUCCESS',
                        'message': f"Index matched by definition: {src_idx.get('name')} -> {dest_idx.get('name')} (renamed)"
                    })
                    results['matched_by_signature'].append({
                        'source': src_idx,
                        'dest': dest_idx
                    })
                    matched_src.add(src_key)
                    matched_dest.add(dest_key)
                    results['summary']['matched'] += 1
                    results['summary']['matched_renamed'] += 1
                    break
    
    # Collect unmatched
    for key, src_idx in src_by_name.items():
        if key not in matched_src:
            results['missing_in_dest'].append(src_idx)
            results['summary']['missing'] += 1
    
    for key, dest_idx in dest_by_name.items():
        if key not in matched_dest:
            results['extra_in_dest'].append(dest_idx)
            results['summary']['extra'] += 1
    
    return results


# =============================================================================
# FOREIGN KEY SIGNATURE MATCHING
# =============================================================================

def build_fk_signature(
    columns: List[str],
    ref_table: str,
    ref_columns: List[str],
    on_delete: str = '',
    on_update: str = ''
) -> str:
    """
    Build a signature for a foreign key based on its definition.
    
    Args:
        columns: List of child column names
        ref_table: Referenced table name
        ref_columns: List of referenced column names
        on_delete: ON DELETE action
        on_update: ON UPDATE action
        
    Returns:
        Signature string
    """
    # Normalize columns
    norm_cols = sorted([normalize_column_name(c) for c in columns if c])
    norm_ref_cols = sorted([normalize_column_name(c) for c in ref_columns if c])
    ref_table_norm = normalize_column_name(ref_table)
    
    # Normalize actions
    on_delete = normalize_fk_action(on_delete)
    on_update = normalize_fk_action(on_update)
    
    return f"{','.join(norm_cols)}->{ref_table_norm}({','.join(norm_ref_cols)}):{on_delete}/{on_update}"


def normalize_fk_action(action: str) -> str:
    """Normalize FK action (ON DELETE/UPDATE) for comparison."""
    if not action:
        return 'NO ACTION'
    
    action = str(action).strip().upper().replace('_', ' ')
    
    # Map common variants
    action_map = {
        'A': 'NO ACTION',
        'R': 'NO ACTION',
        'N': 'SET NULL',
        'C': 'CASCADE',
        'NO ACTION': 'NO ACTION',
        'CASCADE': 'CASCADE',
        'SET NULL': 'SET NULL',
        'SET DEFAULT': 'SET DEFAULT',
        'RESTRICT': 'NO ACTION',
    }
    
    return action_map.get(action, action)


def compare_foreign_keys_with_signatures(
    src_fks: List[Dict[str, Any]],
    dest_fks: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Compare foreign keys using signature-based matching.
    
    Args:
        src_fks: Source FKs with 'name', 'table', 'columns', 'ref_table', 'ref_columns', 'on_delete', 'on_update'
        dest_fks: Destination FKs with same structure
        
    Returns:
        Dictionary with matched, missing, extra, and summary
    """
    results = {
        'matched': [],
        'matched_by_name': [],
        'matched_by_signature': [],
        'missing_in_dest': [],
        'extra_in_dest': [],
        'action_mismatches': [],
        'summary': {
            'total_source': len(src_fks),
            'total_dest': len(dest_fks),
            'matched': 0,
            'matched_renamed': 0,
            'action_warnings': 0,
            'missing': 0,
            'extra': 0
        }
    }
    
    # Build maps
    src_by_name = {}
    src_by_sig = {}
    for fk in src_fks:
        name = str(fk.get('name', '')).upper().strip()
        table = str(fk.get('table', '')).upper().strip()
        key = f"{table}.{name}"
        src_by_name[key] = fk
        
        columns = fk.get('columns', [])
        if isinstance(columns, str):
            columns = [c.strip() for c in columns.split(',') if c.strip()]
        ref_table = fk.get('ref_table', '')
        ref_columns = fk.get('ref_columns', [])
        if isinstance(ref_columns, str):
            ref_columns = [c.strip() for c in ref_columns.split(',') if c.strip()]
        
        # Build signature without actions (for column-only matching)
        sig_cols = build_fk_signature(columns, ref_table, ref_columns, '', '')
        sig_key = f"{table}:{sig_cols}"
        if sig_key not in src_by_sig:
            src_by_sig[sig_key] = []
        src_by_sig[sig_key].append(fk)
    
    dest_by_name = {}
    dest_by_sig = {}
    for fk in dest_fks:
        name = str(fk.get('name', '')).upper().strip()
        table = str(fk.get('table', '')).upper().strip()
        key = f"{table}.{name}"
        dest_by_name[key] = fk
        
        columns = fk.get('columns', [])
        if isinstance(columns, str):
            columns = [c.strip() for c in columns.split(',') if c.strip()]
        ref_table = fk.get('ref_table', '')
        ref_columns = fk.get('ref_columns', [])
        if isinstance(ref_columns, str):
            ref_columns = [c.strip() for c in ref_columns.split(',') if c.strip()]
        
        sig_cols = build_fk_signature(columns, ref_table, ref_columns, '', '')
        sig_key = f"{table}:{sig_cols}"
        if sig_key not in dest_by_sig:
            dest_by_sig[sig_key] = []
        dest_by_sig[sig_key].append(fk)
    
    matched_src = set()
    matched_dest = set()
    
    # First pass: match by name
    for key, src_fk in src_by_name.items():
        if key in dest_by_name:
            dest_fk = dest_by_name[key]
            
            # Check if actions match
            src_delete = normalize_fk_action(src_fk.get('on_delete', ''))
            dest_delete = normalize_fk_action(dest_fk.get('on_delete', ''))
            src_update = normalize_fk_action(src_fk.get('on_update', ''))
            dest_update = normalize_fk_action(dest_fk.get('on_update', ''))
            
            if src_delete == dest_delete and src_update == dest_update:
                status = 'SUCCESS'
                message = f"FK matched: {src_fk.get('name')}"
            else:
                status = 'WARNING'
                message = f"FK matched but actions differ: DELETE {src_delete}->{dest_delete}, UPDATE {src_update}->{dest_update}"
                results['action_mismatches'].append({
                    'source': src_fk,
                    'dest': dest_fk,
                    'src_delete': src_delete,
                    'dest_delete': dest_delete,
                    'src_update': src_update,
                    'dest_update': dest_update
                })
                results['summary']['action_warnings'] += 1
            
            results['matched'].append({
                'source': src_fk,
                'dest': dest_fk,
                'match_type': 'NAME',
                'status': status,
                'message': message
            })
            matched_src.add(key)
            matched_dest.add(key)
            results['summary']['matched'] += 1
    
    # Second pass: match by signature
    for sig_key, src_list in src_by_sig.items():
        if sig_key in dest_by_sig:
            dest_list = dest_by_sig[sig_key]
            
            for src_fk in src_list:
                src_name = str(src_fk.get('name', '')).upper().strip()
                src_table = str(src_fk.get('table', '')).upper().strip()
                src_key = f"{src_table}.{src_name}"
                
                if src_key in matched_src:
                    continue
                
                for dest_fk in dest_list:
                    dest_name = str(dest_fk.get('name', '')).upper().strip()
                    dest_table = str(dest_fk.get('table', '')).upper().strip()
                    dest_key = f"{dest_table}.{dest_name}"
                    
                    if dest_key in matched_dest:
                        continue
                    
                    results['matched'].append({
                        'source': src_fk,
                        'dest': dest_fk,
                        'match_type': 'SIGNATURE',
                        'status': 'SUCCESS',
                        'message': f"FK matched by definition: {src_fk.get('name')} -> {dest_fk.get('name')} (renamed)"
                    })
                    results['matched_by_signature'].append({
                        'source': src_fk,
                        'dest': dest_fk
                    })
                    matched_src.add(src_key)
                    matched_dest.add(dest_key)
                    results['summary']['matched'] += 1
                    results['summary']['matched_renamed'] += 1
                    break
    
    # Collect unmatched
    for key, src_fk in src_by_name.items():
        if key not in matched_src:
            results['missing_in_dest'].append(src_fk)
            results['summary']['missing'] += 1
    
    for key, dest_fk in dest_by_name.items():
        if key not in matched_dest:
            results['extra_in_dest'].append(dest_fk)
            results['summary']['extra'] += 1
    
    return results


# =============================================================================
# DEFAULT CONSTRAINT SIGNATURE MATCHING
# =============================================================================

def is_auto_generated_constraint_name(name: str) -> bool:
    """
    Check if a constraint name appears to be auto-generated.
    
    SQL Server auto-generates names like:
    - DF__TableName__Col__1234ABCD (default constraints)
    - PK__TableName__1234ABCD (primary keys)
    - UQ__TableName__1234ABCD (unique constraints)
    
    DB2 generates names like:
    - SQL1234567890123456 (auto-generated)
    """
    if not name:
        return False
    
    name_upper = str(name).strip().upper()
    
    # SQL Server auto-generated patterns (PREFIX__....__HEX8)
    # DF__ADVISE_IN__CLUST__17036CC0
    if re.match(r'^(DF|PK|UQ|CK|FK)__\w+__[0-9A-F]{8}$', name_upper):
        return True
    
    # DB2 auto-generated (SQL followed by many digits)
    if re.match(r'^SQL\d{10,}$', name_upper):
        return True
    
    return False


def build_default_constraint_signature(table: str, column: str) -> str:
    """
    Build a signature for a default constraint based on table+column.
    A column can only have one default constraint.
    """
    table_norm = str(table).strip().upper() if table else ''
    column_norm = str(column).strip().upper() if column else ''
    return f"{table_norm}.{column_norm}"


def compare_default_constraints_with_signatures(
    src_defaults: List[Dict[str, Any]],
    dest_defaults: List[Dict[str, Any]],
    cross_database: bool = False
) -> Dict[str, Any]:
    """
    Compare default constraints using signature-based matching (table.column).
    
    Auto-generated constraints in cross-database comparisons are marked as INFO
    (ignorable) rather than errors, since DB2 doesn't have named default constraints.
    
    Args:
        src_defaults: Source defaults with 'name', 'table', 'column', 'definition'
        dest_defaults: Destination defaults with same structure
        cross_database: True if comparing different database types
        
    Returns:
        Dictionary with matched, missing, extra, auto_generated, and summary
    """
    results = {
        'matched': [],
        'matched_by_name': [],
        'matched_by_signature': [],
        'missing_in_dest': [],
        'extra_in_dest': [],
        'auto_generated_in_dest': [],  # Auto-generated that can be safely ignored
        'summary': {
            'total_source': len(src_defaults),
            'total_dest': len(dest_defaults),
            'matched': 0,
            'matched_renamed': 0,
            'missing': 0,
            'extra': 0,
            'auto_generated_ignored': 0
        }
    }
    
    # Build maps by name and signature
    src_by_name = {}
    src_by_sig = {}
    for dc in src_defaults:
        name = str(dc.get('name', '')).upper().strip()
        table = str(dc.get('table', '')).upper().strip()
        column = str(dc.get('column', '')).upper().strip()
        
        key = f"{table}.{name}"
        src_by_name[key] = dc
        
        # Signature by table.column (a column can only have one default)
        sig = f"{table}.{column}"
        if sig and column and sig not in src_by_sig:
            src_by_sig[sig] = dc
    
    dest_by_name = {}
    dest_by_sig = {}
    for dc in dest_defaults:
        name = str(dc.get('name', '')).upper().strip()
        table = str(dc.get('table', '')).upper().strip()
        column = str(dc.get('column', '')).upper().strip()
        
        key = f"{table}.{name}"
        dest_by_name[key] = dc
        
        sig = f"{table}.{column}"
        if sig and column and sig not in dest_by_sig:
            dest_by_sig[sig] = dc
    
    matched_src = set()
    matched_dest = set()
    
    # First pass: match by name
    for key, src_dc in src_by_name.items():
        if key in dest_by_name:
            dest_dc = dest_by_name[key]
            results['matched'].append({
                'source': src_dc,
                'dest': dest_dc,
                'match_type': 'NAME',
                'message': f"Matched by name: {src_dc.get('name')}"
            })
            results['matched_by_name'].append(src_dc)
            matched_src.add(key)
            
            dest_table = str(dest_dc.get('table', '')).upper().strip()
            dest_name = str(dest_dc.get('name', '')).upper().strip()
            matched_dest.add(f"{dest_table}.{dest_name}")
            results['summary']['matched'] += 1
    
    # Second pass: match by signature (table.column)
    for sig, src_dc in src_by_sig.items():
        src_name = str(src_dc.get('name', '')).upper().strip()
        src_table = str(src_dc.get('table', '')).upper().strip()
        src_key = f"{src_table}.{src_name}"
        
        if src_key in matched_src:
            continue
        
        if sig in dest_by_sig:
            dest_dc = dest_by_sig[sig]
            dest_name = str(dest_dc.get('name', '')).upper().strip()
            dest_table = str(dest_dc.get('table', '')).upper().strip()
            dest_key = f"{dest_table}.{dest_name}"
            
            if dest_key in matched_dest:
                continue
            
            results['matched'].append({
                'source': src_dc,
                'dest': dest_dc,
                'match_type': 'SIGNATURE',
                'message': f"Matched by column: {src_dc.get('column')} (renamed)"
            })
            results['matched_by_signature'].append({'source': src_dc, 'dest': dest_dc})
            matched_src.add(src_key)
            matched_dest.add(dest_key)
            results['summary']['matched'] += 1
            results['summary']['matched_renamed'] += 1
    
    # Collect unmatched source (truly missing in dest)
    for key, src_dc in src_by_name.items():
        if key not in matched_src:
            results['missing_in_dest'].append(src_dc)
            results['summary']['missing'] += 1
    
    # Collect unmatched dest - check if auto-generated
    for key, dest_dc in dest_by_name.items():
        if key not in matched_dest:
            name = str(dest_dc.get('name', '')).strip()
            
            # For cross-database, auto-generated constraints can be ignored
            if cross_database and is_auto_generated_constraint_name(name):
                results['auto_generated_in_dest'].append(dest_dc)
                results['summary']['auto_generated_ignored'] += 1
            else:
                results['extra_in_dest'].append(dest_dc)
                results['summary']['extra'] += 1
    
    return results


# =============================================================================
# DEFAULT VALUE NORMALIZATION
# =============================================================================

def normalize_default_value(value: str, db_type: str = 'sqlserver') -> str:
    """
    Normalize a default value for comparison.
    
    Handles differences like:
    - ((0)) vs (0) vs 0
    - ('value') vs 'value'
    - GETDATE() vs (GETDATE())
    - NULL vs (NULL)
    
    Args:
        value: The default value expression
        db_type: 'sqlserver' or 'db2'
        
    Returns:
        Normalized default value string
    """
    if value is None:
        return ''
    
    value = str(value).strip()
    
    if not value:
        return ''
    
    # Uppercase for comparison
    value_upper = value.upper()
    
    # Handle NULL
    if value_upper in ('NULL', '(NULL)', '((NULL))'):
        return 'NULL'
    
    # Remove outer parentheses (multiple layers)
    while value.startswith('(') and value.endswith(')'):
        inner = value[1:-1]
        # Make sure we're not breaking function calls like GETDATE()
        if inner.count('(') == inner.count(')'):
            value = inner
        else:
            break
    
    # Uppercase again after stripping
    value_upper = value.upper()
    
    # Normalize common function names
    func_map = {
        'GETDATE()': 'GETDATE()',
        'CURRENT_TIMESTAMP': 'GETDATE()',
        'CURRENT TIMESTAMP': 'GETDATE()',
        'CURRENT_DATE': 'GETDATE()',
        'CURRENT DATE': 'GETDATE()',
        'SYSDATETIME()': 'GETDATE()',
        'GETUTCDATE()': 'GETUTCDATE()',
        'NEWID()': 'NEWID()',
        'NEWSEQUENTIALID()': 'NEWSEQUENTIALID()',
        'USER': 'USER',
        'CURRENT_USER': 'USER',
        'SYSTEM_USER': 'USER',
        'SUSER_SNAME()': 'USER',
    }
    
    if value_upper in func_map:
        return func_map[value_upper]
    
    # Remove quotes from string literals for comparison
    if (value.startswith("'") and value.endswith("'")) or \
       (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    
    # Try to normalize numeric values
    try:
        # Handle integers
        if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
            return str(int(value))
        # Handle decimals
        float_val = float(value)
        if float_val == int(float_val):
            return str(int(float_val))
        return str(float_val)
    except (ValueError, TypeError):
        pass
    
    return value


def compare_default_values(
    src_default: str,
    dest_default: str,
    src_db_type: str = 'db2',
    dest_db_type: str = 'sqlserver'
) -> Dict[str, Any]:
    """
    Compare default values with normalization.
    
    Args:
        src_default: Source default value
        dest_default: Destination default value
        src_db_type: Source database type
        dest_db_type: Destination database type
        
    Returns:
        Dictionary with status, message, and details
    """
    src_norm = normalize_default_value(src_default, src_db_type)
    dest_norm = normalize_default_value(dest_default, dest_db_type)
    
    if src_norm == dest_norm:
        return {
            'status': 'SUCCESS',
            'message': f'Default values match: {src_norm or "(none)"}',
            'src_normalized': src_norm,
            'dest_normalized': dest_norm
        }
    
    # Check for equivalent but different representations
    if not src_norm and not dest_norm:
        return {
            'status': 'SUCCESS',
            'message': 'Both have no default',
            'src_normalized': src_norm,
            'dest_normalized': dest_norm
        }
    
    if not src_norm or not dest_norm:
        return {
            'status': 'WARNING',
            'message': f'Default value differs: "{src_default or "(none)"}" vs "{dest_default or "(none)"}"',
            'src_normalized': src_norm,
            'dest_normalized': dest_norm
        }
    
    return {
        'status': 'ERROR',
        'message': f'Default value mismatch: "{src_default}" vs "{dest_default}"',
        'src_normalized': src_norm,
        'dest_normalized': dest_norm
    }


def compare_columns_with_defaults(
    src_columns: List[Dict[str, Any]],
    dest_columns: List[Dict[str, Any]],
    src_db_type: str = 'db2',
    dest_db_type: str = 'sqlserver'
) -> Dict[str, Any]:
    """
    Compare columns including their default values with normalization.
    
    Args:
        src_columns: Source columns with 'name', 'type', 'default'
        dest_columns: Destination columns with same structure
        
    Returns:
        Dictionary with default value comparison results
    """
    results = {
        'matched_defaults': [],
        'different_defaults': [],
        'summary': {
            'total': 0,
            'matched': 0,
            'different': 0
        }
    }
    
    # Build column maps
    src_col_map = {str(col.get('name', '')).upper().strip(): col for col in src_columns}
    dest_col_map = {str(col.get('name', '')).upper().strip(): col for col in dest_columns}
    
    # Compare defaults for matching columns
    for col_name in src_col_map:
        if col_name in dest_col_map:
            results['summary']['total'] += 1
            
            src_col = src_col_map[col_name]
            dest_col = dest_col_map[col_name]
            
            src_default = src_col.get('default', '')
            dest_default = dest_col.get('default', '')
            
            comparison = compare_default_values(
                src_default, dest_default,
                src_db_type, dest_db_type
            )
            
            if comparison['status'] == 'SUCCESS':
                results['matched_defaults'].append({
                    'column': col_name,
                    **comparison
                })
                results['summary']['matched'] += 1
            else:
                results['different_defaults'].append({
                    'column': col_name,
                    'src_default': src_default,
                    'dest_default': dest_default,
                    **comparison
                })
                results['summary']['different'] += 1
    
    return results


# =============================================================================
# SUMMARY FUNCTIONS
# =============================================================================

def get_index_matching_summary(results: Dict[str, Any], use_unicode: bool = True) -> str:
    """Generate a summary string for index matching results."""
    summary = results.get('summary', {})
    
    ok = "✓" if use_unicode else "[OK]"
    warn = "⚠" if use_unicode else "[WARN]"
    err = "✗" if use_unicode else "[ERR]"
    
    lines = [
        "Index Comparison Summary:",
        f"  Source indexes: {summary.get('total_source', 0)}",
        f"  Destination indexes: {summary.get('total_dest', 0)}",
        f"  {ok} Matched: {summary.get('matched', 0)}",
        f"    - By name: {len(results.get('matched_by_name', []))}",
        f"    - By signature (renamed): {summary.get('matched_renamed', 0)}",
        f"  {err} Missing in dest: {summary.get('missing', 0)}",
        f"  {warn} Extra in dest: {summary.get('extra', 0)}"
    ]
    
    return '\n'.join(lines)


def get_fk_matching_summary(results: Dict[str, Any], use_unicode: bool = True) -> str:
    """Generate a summary string for FK matching results."""
    summary = results.get('summary', {})
    
    ok = "✓" if use_unicode else "[OK]"
    warn = "⚠" if use_unicode else "[WARN]"
    err = "✗" if use_unicode else "[ERR]"
    
    lines = [
        "Foreign Key Comparison Summary:",
        f"  Source FKs: {summary.get('total_source', 0)}",
        f"  Destination FKs: {summary.get('total_dest', 0)}",
        f"  {ok} Matched: {summary.get('matched', 0)}",
        f"    - By name: {len(results.get('matched_by_name', []))}",
        f"    - By signature (renamed): {summary.get('matched_renamed', 0)}",
        f"  {warn} Action differences: {summary.get('action_warnings', 0)}",
        f"  {err} Missing in dest: {summary.get('missing', 0)}",
        f"  {warn} Extra in dest: {summary.get('extra', 0)}"
    ]
    
    return '\n'.join(lines)
