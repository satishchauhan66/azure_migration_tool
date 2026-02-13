# Author: Satish Ch@uhan

"""Functions to fix nullability mismatches during restore."""

import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from ..utils.paths import qident
from ..utils.sql import type_sql


def parse_table_definition(sql_text: str) -> Dict[str, Dict[str, dict]]:
    """
    Parse CREATE TABLE SQL to extract column definitions.
    
    Returns:
        Dict mapping "schema.table" -> {"column_name": {"is_nullable": bool, "type": str, ...}}
    """
    tables = {}
    
    # Split by GO statements first to handle multiple CREATE TABLE statements
    batches = re.split(r'^\s*GO\s*$', sql_text, flags=re.MULTILINE | re.IGNORECASE)
    
    for batch in batches:
        batch = batch.strip()
        if not batch:
            continue
        
        # Pattern to match CREATE TABLE statements
        # Handle both: CREATE TABLE schema.table and CREATE TABLE [schema].[table]
        # Also handle IF OBJECT_ID wrapper
        pattern = r"(?:IF\s+OBJECT_ID.*?BEGIN\s+)?CREATE\s+TABLE\s+(\[?(\w+)\]?\.\[?(\w+)\]?)\s*\((.*?)\)"
        
        for match in re.finditer(pattern, batch, re.IGNORECASE | re.DOTALL):
            schema = match.group(2)
            table = match.group(3)
            columns_text = match.group(4)
            
            table_key = f"{schema}.{table}"
            if table_key not in tables:
                tables[table_key] = {}
            
            # Parse column definitions - handle multi-line and constraints
            # Split by comma, but be careful with nested parentheses
            col_defs = []
            current_col = []
            paren_depth = 0
            
            for char in columns_text:
                if char == '(':
                    paren_depth += 1
                    current_col.append(char)
                elif char == ')':
                    paren_depth -= 1
                    current_col.append(char)
                elif char == ',' and paren_depth == 0:
                    # End of column definition
                    col_defs.append(''.join(current_col).strip())
                    current_col = []
                else:
                    current_col.append(char)
            
            # Add last column
            if current_col:
                col_defs.append(''.join(current_col).strip())
            
            # Parse each column definition
            for col_def in col_defs:
                col_def = col_def.strip()
                if not col_def or col_def.startswith('CONSTRAINT'):
                    # Skip constraint definitions
                    continue
                
                # Extract column name (first identifier)
                col_name_match = re.match(r'\[?(\w+)\]?', col_def)
                if not col_name_match:
                    continue
                
                col_name = col_name_match.group(1)
                
                # Extract nullability - look for NULL or NOT NULL
                is_nullable = True  # Default in SQL Server
                if "NOT NULL" in col_def.upper():
                    is_nullable = False
                elif "NULL" in col_def.upper() and "NOT NULL" not in col_def.upper():
                    is_nullable = True
                
                # Extract type - get everything between column name and NULL/NOT NULL/IDENTITY
                # This is complex, so we'll use the destination type when applying fixes
                type_match = re.search(
                    r'\[?\w+\]?\s+(\w+(?:\([^)]+\))?(?:\([^)]+\))?)',
                    col_def,
                    re.IGNORECASE
                )
                col_type = type_match.group(1) if type_match else None
                
                tables[table_key][col_name] = {
                    "is_nullable": is_nullable,
                    "type": col_type,
                    "full_definition": col_def
                }
    
    return tables


def get_destination_columns(cur, schema_name: str, table_name: str) -> Dict[str, dict]:
    """
    Get column definitions from destination database.
    
    Returns:
        Dict mapping column_name -> {"is_nullable": bool, "type": str, ...}
    """
    cur.execute(
        """
        SELECT
            c.name AS column_name,
            ty.name AS type_name,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable
        FROM sys.columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        WHERE s.name = ? AND t.name = ?
        ORDER BY c.column_id;
        """,
        schema_name,
        table_name
    )
    
    columns = {}
    for row in cur.fetchall():
        col_name = row.column_name
        type_str = type_sql(row.type_name, row.max_length, row.precision, row.scale)
        columns[col_name] = {
            "is_nullable": bool(row.is_nullable),
            "type": type_str,
            "type_name": row.type_name,
            "max_length": row.max_length,
            "precision": row.precision,
            "scale": row.scale
        }
    
    return columns


def generate_nullability_fixes(
    cur,
    backup_tables: Dict[str, Dict[str, dict]],
    logger
) -> List[Tuple[str, str]]:
    """
    Compare source (backup) and destination (current) column definitions
    and generate ALTER TABLE statements to fix nullability mismatches.
    
    Returns:
        List of (table_key, alter_sql) tuples
    """
    fixes = []
    
    for table_key, source_columns in backup_tables.items():
        # Parse schema.table from table_key
        parts = table_key.split('.')
        if len(parts) != 2:
            continue
        
        schema_name, table_name = parts
        
        # Check if table exists in destination
        cur.execute(
            """
            SELECT COUNT(*) 
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?;
            """,
            schema_name,
            table_name
        )
        
        if cur.fetchone()[0] == 0:
            # Table doesn't exist, will be created by restore - skip
            continue
        
        # Get destination columns
        try:
            dest_columns = get_destination_columns(cur, schema_name, table_name)
        except Exception as ex:
            logger.warning("Could not get columns for %s: %s", table_key, ex)
            continue
        
        # Compare and generate ALTER statements
        alter_statements = []
        for col_name, source_col in source_columns.items():
            if col_name not in dest_columns:
                # Column doesn't exist in destination - will be handled by table creation/alter
                continue
            
            dest_col = dest_columns[col_name]
            
            # Check nullability mismatch
            if source_col["is_nullable"] != dest_col["is_nullable"]:
                # Need to fix nullability
                nullable_clause = "NULL" if source_col["is_nullable"] else "NOT NULL"
                
                # Use destination type (preserve existing type)
                type_str = dest_col["type"]
                
                alter_sql = (
                    f"ALTER TABLE {qident(schema_name)}.{qident(table_name)} "
                    f"ALTER COLUMN {qident(col_name)} {type_str} {nullable_clause};"
                )
                alter_statements.append(alter_sql)
                
                logger.info(
                    "Nullability mismatch: %s.%s.%s - Source: %s, Dest: %s",
                    schema_name, table_name, col_name,
                    "NULL" if source_col["is_nullable"] else "NOT NULL",
                    "NULL" if dest_col["is_nullable"] else "NOT NULL"
                )
        
        if alter_statements:
            # Combine all ALTER statements for this table
            combined_sql = "\n".join(alter_statements)
            fixes.append((table_key, combined_sql))
    
    return fixes


def apply_nullability_fixes(
    cur,
    conn,
    backup_path: Path,
    logger,
    dry_run: bool = False
) -> dict:
    """
    Apply nullability fixes by comparing backup and destination.
    
    Args:
        cur: Database cursor
        conn: Database connection
        backup_path: Path to backup folder (to read summary JSON and SQL files)
        logger: Logger instance
        dry_run: If True, only log what would be done
    
    Returns:
        Dict with statistics about fixes applied
    """
    import json
    
    result = {
        "tables_checked": 0,
        "tables_fixed": 0,
        "columns_fixed": 0,
        "errors": [],
        "warnings": []
    }
    
    try:
        # Read backup summary to get table list
        summary_file = backup_path / "meta" / "run_summary.json"
        if not summary_file.exists():
            logger.warning("Backup summary not found, cannot get table list")
            return result
        
        with open(summary_file, 'r', encoding='utf-8') as f:
            backup_summary = json.load(f)
        
        tables_list = backup_summary.get("tables", [])
        if not tables_list:
            logger.info("No tables found in backup summary")
            return result
        
        # Read tables SQL file
        tables_sql_file = backup_path / "schema" / "01_tables_all.sql"
        if not tables_sql_file.exists():
            logger.warning("Tables SQL file not found")
            return result
        
        backup_tables_sql = tables_sql_file.read_text(encoding="utf-8")
        
        # Parse backup table definitions
        backup_tables = parse_table_definition(backup_tables_sql)
        result["tables_checked"] = len(backup_tables)
        
        if not backup_tables:
            logger.info("No table definitions found in backup SQL")
            return result
        
        logger.info("Checking nullability for %d tables...", len(backup_tables))
        
        # Generate fixes
        fixes = generate_nullability_fixes(cur, backup_tables, logger)
        
        if not fixes:
            logger.info("No nullability mismatches found")
            return result
        
        logger.info("Found nullability mismatches in %d tables", len(fixes))
        
        # Apply fixes
        for table_key, alter_sql in fixes:
            try:
                if dry_run:
                    logger.info("DRY RUN: Would execute for %s:\n%s", table_key, alter_sql)
                    result["tables_fixed"] += 1
                    # Count columns in the ALTER statement
                    result["columns_fixed"] += alter_sql.count("ALTER COLUMN")
                else:
                    logger.info("Fixing nullability for %s...", table_key)
                    cur.execute(alter_sql)
                    conn.commit()
                    result["tables_fixed"] += 1
                    result["columns_fixed"] += alter_sql.count("ALTER COLUMN")
                    logger.info("Fixed nullability for %s", table_key)
            except Exception as ex:
                error_msg = str(ex)
                result["errors"].append(f"{table_key}: {error_msg}")
                logger.error("Failed to fix nullability for %s: %s", table_key, ex)
                try:
                    conn.rollback()
                except Exception:
                    pass
        
        if result["tables_fixed"] > 0:
            logger.info(
                "Nullability fixes applied: %d tables, %d columns",
                result["tables_fixed"],
                result["columns_fixed"]
            )
        
    except Exception as ex:
        error_msg = f"Error during nullability fix process: {ex}"
        result["errors"].append(error_msg)
        logger.error(error_msg)
    
    return result

