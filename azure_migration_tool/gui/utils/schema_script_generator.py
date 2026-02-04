"""
Script generator for selected schema objects.
"""

import logging
from typing import List, Tuple, Dict, Optional, Any
import pyodbc

# Import from backup/restore modules
import sys
from pathlib import Path

# Add parent directories to path
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

try:
    from src.backup.exporters import (
        fetch_tables, fetch_columns, fetch_primary_key,
        fetch_objects, fetch_triggers, fetch_sequences, fetch_synonyms,
        export_foreign_keys, export_check_constraints, export_default_constraints,
        export_indexes, export_primary_keys, object_definition,
        wrap_create_or_alter, build_create_table_sql, export_sequences, export_synonyms
    )
    from src.utils.sql import sql_header
    from src.utils.paths import qident
except ImportError:
    # Fallback if modules not available
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
    wrap_create_or_alter = None
    build_create_table_sql = None
    export_sequences = None
    export_synonyms = None
    sql_header = None


def generate_script_for_objects(
    conn: pyodbc.Connection,
    selected_objects: List[Tuple[str, str, str]],  # (object_type, schema, name)
    logger: Optional[logging.Logger] = None
) -> str:
    """
    Generate SQL script for selected objects.
    
    Args:
        conn: Source database connection
        selected_objects: List of (object_type, schema, name) tuples
        logger: Optional logger
        
    Returns:
        SQL script as string
    """
    if not fetch_tables:
        raise ImportError("Backup exporters module not available")
    
    cur = conn.cursor()
    scripts = []
    
    # Group objects by type for proper ordering
    objects_by_type = {}
    for obj_type, schema, name in selected_objects:
        if obj_type not in objects_by_type:
            objects_by_type[obj_type] = []
        # For SQL-based objects (constraints, indexes), name might be the obj_type itself
        # This indicates the entire category is selected
        if obj_type in ['foreign_keys', 'check_constraints', 'default_constraints', 'indexes', 'primary_keys']:
            # If name matches obj_type, it means the category checkbox was selected
            if name == obj_type or not name:
                objects_by_type[obj_type].append((schema, obj_type))  # Use obj_type as identifier
            else:
                objects_by_type[obj_type].append((schema, name))
        else:
            objects_by_type[obj_type].append((schema, name))
    
    # Generate scripts in dependency order
    # 1. Sequences (before tables)
    if 'sequences' in objects_by_type:
        if logger:
            logger.info(f"Generating script for {len(objects_by_type['sequences'])} sequence(s)...")
        # For sequences, we need to filter the export
        all_sequences = fetch_sequences(cur)
        selected_sequences = [s for s in all_sequences 
                            if (s.schema_name, s.sequence_name) in objects_by_type['sequences']]
        if selected_sequences:
            # Create filtered export
            seq_sql = _export_selected_sequences(selected_sequences)
            scripts.append(seq_sql)
    
    # 2. Synonyms (before other objects)
    if 'synonyms' in objects_by_type:
        if logger:
            logger.info(f"Generating script for {len(objects_by_type['synonyms'])} synonym(s)...")
        all_synonyms = fetch_synonyms(cur)
        selected_synonyms = [s for s in all_synonyms
                            if (s.schema_name, s.synonym_name) in objects_by_type['synonyms']]
        if selected_synonyms:
            syn_sql = _export_selected_synonyms(selected_synonyms)
            scripts.append(syn_sql)
    
    # 3. Tables
    if 'tables' in objects_by_type:
        if logger:
            logger.info(f"Generating script for {len(objects_by_type['tables'])} table(s)...")
        for schema, name in objects_by_type['tables']:
            cols = fetch_columns(cur, schema, name)
            pk = fetch_primary_key(cur, schema, name)
            table_sql = build_create_table_sql(schema, name, cols, pk, include_pk=True)
            scripts.append(table_sql)
    
    # 4. Views
    if 'views' in objects_by_type:
        if logger:
            logger.info(f"Generating script for {len(objects_by_type['views'])} view(s)...")
        all_views = fetch_objects(cur, "V")
        for schema, name in objects_by_type['views']:
            view = next((v for v in all_views if v[0] == schema and v[1] == name), None)
            if view:
                defn = object_definition(cur, view[3])  # object_id is at index 3
                view_sql = wrap_create_or_alter(schema, name, defn, "VIEW")
                scripts.append(view_sql)
    
    # 5. Procedures
    if 'procedures' in objects_by_type:
        if logger:
            logger.info(f"Generating script for {len(objects_by_type['procedures'])} procedure(s)...")
        all_procs = fetch_objects(cur, "P")
        for schema, name in objects_by_type['procedures']:
            proc = next((p for p in all_procs if p[0] == schema and p[1] == name), None)
            if proc:
                defn = object_definition(cur, proc[3])
                proc_sql = wrap_create_or_alter(schema, name, defn, "PROC")
                scripts.append(proc_sql)
    
    # 6. Functions
    if 'functions' in objects_by_type:
        if logger:
            logger.info(f"Generating script for {len(objects_by_type['functions'])} function(s)...")
        all_funcs = fetch_objects(cur, "FN,TF,IF")
        for schema, name in objects_by_type['functions']:
            func = next((f for f in all_funcs if f[0] == schema and f[1] == name), None)
            if func:
                defn = object_definition(cur, func[3])
                func_sql = wrap_create_or_alter(schema, name, defn, "FUNCTION")
                scripts.append(func_sql)
    
    # 7. Triggers
    if 'triggers' in objects_by_type:
        if logger:
            logger.info(f"Generating script for {len(objects_by_type['triggers'])} trigger(s)...")
        all_triggers = fetch_triggers(cur)
        for schema, name in objects_by_type['triggers']:
            trigger = next((t for t in all_triggers 
                          if t.schema_name == schema and t.trigger_name == name), None)
            if trigger:
                defn = object_definition(cur, trigger.object_id)
                if defn:
                    trigger_sql = wrap_create_or_alter(schema, name, defn, "TRIGGER")
                    scripts.append(trigger_sql)
    
    # 8. Primary Keys (after tables, before other constraints)
    if 'primary_keys' in objects_by_type:
        if logger:
            logger.info("Generating script for primary keys...")
        # For primary keys, if category is selected, export all PKs
        # Otherwise, filter by selected tables
        if objects_by_type['primary_keys']:  # If list is not empty, category was selected
            pk_sql = export_primary_keys(cur)
            if pk_sql.strip():
                scripts.append(pk_sql)
    
    # 9. Foreign Keys (after tables and PKs)
    if 'foreign_keys' in objects_by_type:
        if logger:
            logger.info("Generating script for foreign keys...")
        # If category is selected, export all FKs
        if objects_by_type['foreign_keys']:  # Category selected
            fk_sql, _ = export_foreign_keys(cur, logger)
            if fk_sql.strip():
                scripts.append(fk_sql)
    
    # 10. Check Constraints
    if 'check_constraints' in objects_by_type:
        if logger:
            logger.info("Generating script for check constraints...")
        if objects_by_type['check_constraints']:  # Category selected
            chk_sql = export_check_constraints(cur)
            if chk_sql.strip():
                scripts.append(chk_sql)
    
    # 11. Default Constraints
    if 'default_constraints' in objects_by_type:
        if logger:
            logger.info("Generating script for default constraints...")
        if objects_by_type['default_constraints']:  # Category selected
            def_sql = export_default_constraints(cur)
            if def_sql.strip():
                scripts.append(def_sql)
    
    # 12. Indexes (after tables, before foreign keys)
    if 'indexes' in objects_by_type:
        if logger:
            logger.info("Generating script for indexes...")
        if objects_by_type['indexes']:  # Category selected
            idx_sql, _ = export_indexes(cur, logger)
            if idx_sql.strip():
                scripts.append(idx_sql)
    
    # Combine all scripts
    full_script = "\n\n".join(scripts)
    
    return full_script


def _export_selected_sequences(sequences) -> str:
    """Export selected sequences as SQL."""
    
    out = []
    for seq in sequences:
        schema_name = seq.schema_name
        sequence_name = seq.sequence_name
        full_name = f"{qident(schema_name)}.{qident(sequence_name)}"
        
        out.append(f"-- {full_name}")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE object_id = OBJECT_ID('{full_name}'))")
        out.append("BEGIN")
        
        seq_sql = f"    CREATE SEQUENCE {full_name} AS BIGINT"
        if seq.start_value is not None:
            seq_sql += f"\n        START WITH {seq.start_value}"
        if seq.increment is not None:
            seq_sql += f"\n        INCREMENT BY {seq.increment}"
        if seq.minimum_value is not None:
            seq_sql += f"\n        MINVALUE {seq.minimum_value}"
        else:
            seq_sql += "\n        NO MINVALUE"
        if seq.maximum_value is not None:
            seq_sql += f"\n        MAXVALUE {seq.maximum_value}"
        else:
            seq_sql += "\n        NO MAXVALUE"
        if seq.is_cycling:
            seq_sql += "\n        CYCLE"
        else:
            seq_sql += "\n        NO CYCLE"
        if seq.cache_size is not None:
            seq_sql += f"\n        CACHE {seq.cache_size}"
        else:
            seq_sql += "\n        NO CACHE"
        
        out.append(seq_sql + ";")
        out.append("END")
        out.append("GO\n")
    
    return "\n".join(out)


def _export_selected_synonyms(synonyms) -> str:
    """Export selected synonyms as SQL."""
    
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
