"""
Excel utility functions for bulk processing.
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
import tkinter.filedialog as filedialog


def normalize_column_name(col_name: str) -> Optional[str]:
    """Normalize Excel column name to our standard key"""
    col_lower = col_name.strip().lower()
    
    column_mapping = {
        # Source
        "src_server": [
            "staging db server", "staging_db_server", "staging server", "staging_server",
            "source server", "src_server", "source_server", "backup server", "backup_server",
            "src server", "source db server", "server"
        ],
        "src_db": [
            "staging db name", "staging_db_name", "staging database", "staging_db", "staging db",
            "source database", "src_db", "source_db", "src_database", "source_database",
            "backup database", "backup_db", "backup database name", "source db", "src database", "database"
        ],
        "src_auth": [
            "source auth", "src_auth", "source_authentication", "backup auth",
            "source authentication", "src authentication"
        ],
        "src_user": [
            "source user", "src_user", "source_user", "staging user", "backup user", "src user"
        ],
        "src_password": [
            "source password", "src_password", "backup password", "src password"
        ],
        
        # Destination
        "dest_server": [
            "destination db server", "destination_db_server", "destination server", "destination_server",
            "dest server", "dest_server", "restore server", "restore_server", "dest db server"
        ],
        "dest_db": [
            "destination db name", "destination_db_name", "destination database", "destination_db", "destination db",
            "dest database", "dest_db", "dest_database", "restore database", "restore_db", "dest db"
        ],
        "dest_auth": [
            "destination auth", "dest_auth", "destination_authentication", "restore auth",
            "destination authentication", "dest authentication"
        ],
        "dest_user": [
            "destination user", "dest_user", "destination_user", "restore user", "dest user"
        ],
        "dest_password": [
            "destination password", "dest_password", "restore password", "dest password"
        ],
        
        # Common - these are used when source/dest have the same user/password
        "user": ["user", "username", "account", "entra_user", "mfa_user", "assgined", "assigned"],
        "password": ["password", "pwd", "pass"],  # Generic password for single-source scenarios
        "auth": ["auth", "authentication", "auth_type"],
        
        # Migration settings
        "batch_size": ["batch_size", "batch size", "migrate_batch_size", "migrate batch size"],
        "truncate_dest": ["truncate_dest", "truncate dest", "truncate_destination", "migrate_truncate_dest", "truncate"],
        "delete_dest": ["delete_dest", "delete dest", "delete_destination", "migrate_delete_dest"],
        
        # Constraint/Index options
        "disable_fk": ["disable_fk", "disable fk", "disable foreign keys", "disable_foreign_keys"],
        "disable_indexes": ["disable_indexes", "disable indexes", "disable_index"],
        "disable_triggers": ["disable_triggers", "disable triggers"],
        
        # Restore settings
        "restore_tables": ["restore_tables", "restore tables", "restore tables"],
        "restore_views": ["restore_views", "restore views", "restore views"],
        "restore_procedures": ["restore_procedures", "restore procedures", "restore procedures"],
        "restore_functions": ["restore_functions", "restore functions", "restore functions"],
        "restore_constraints": ["restore_constraints", "restore constraints", "restore constraints"],
        "restore_indexes": ["restore_indexes", "restore indexes", "restore indexes"],
        "backup_path": ["backup_path", "backup path", "backup folder"],
        
        # Backup settings
        "backup_tables": ["backup_tables", "backup tables", "backup tables"],
        "backup_views": ["backup_views", "backup views", "backup views"],
        "backup_procedures": ["backup_procedures", "backup procedures", "backup procedures"],
        "backup_functions": ["backup_functions", "backup functions", "backup functions"],
        "backup_constraints": ["backup_constraints", "backup constraints", "backup constraints"],
        "backup_indexes": ["backup_indexes", "backup indexes", "backup indexes"],
        
        # Validation settings
        "validate_tables": ["validate_tables", "validate tables", "validate tables"],
        "validate_columns": ["validate_columns", "validate columns", "validate columns"],
        "validate_indexes": ["validate_indexes", "validate indexes", "validate indexes"],
        "validate_constraints": ["validate_constraints", "validate constraints", "validate constraints"],
        "validate_programmables": ["validate_programmables", "validate programmables", "validate programmables"],
        
        # Skip options
        "skip_backup": ["skip_backup", "skip backup"],
        "skip_migration": ["skip_migration", "skip migration"],
        "skip_restore": ["skip_restore", "skip restore"],
        
        # Tables filtering
        "tables": ["tables", "table_list", "include_tables"],
        "exclude": ["exclude", "exclude_tables", "excluded_tables"],
        
        # Performance & Resilience options
        "parallel_tables": ["parallel_tables", "parallel tables", "parallel_workers", "max_parallel"],
        "skip_completed": ["skip_completed", "skip completed", "skip_if_done"],
        "resume_enabled": ["resume_enabled", "resume enabled", "enable_resume", "resume"],
        "max_retries": ["max_retries", "max retries", "retries", "retry_count"],
        "verify_after_copy": ["verify_after_copy", "verify after copy", "verify", "verify_rows"],
        
        # Chunking options
        "enable_chunking": ["enable_chunking", "enable chunking", "chunking", "use_chunking"],
        "chunk_threshold": ["chunk_threshold", "chunk threshold", "chunking_threshold"],
        "num_chunks": ["num_chunks", "num chunks", "number_of_chunks", "chunks"],
        "chunk_workers": ["chunk_workers", "chunk workers", "chunking_workers", "chunk_parallel"],
    }
    
    for key, possible_names in column_mapping.items():
        if col_lower in [n.lower() for n in possible_names]:
            return key
    return None


def clean_server_name(server: str) -> str:
    """Clean server name - remove port numbers and trailing semicolons/commas"""
    if not server or pd.isna(server):
        return ""
    server = str(server).strip()
    # Remove trailing semicolons and commas
    server = server.rstrip(';,')
    # Remove port number if present (e.g., "server,1433" -> "server")
    if ',' in server:
        parts = server.split(',')
        # Check if second part is a number (port)
        if len(parts) == 2 and parts[1].strip().isdigit():
            server = parts[0].strip()
    return server


def read_excel_file(excel_file: str, required_columns: List[str], default_user: Optional[str] = None) -> List[Dict]:
    """
    Read database configuration from Excel file.
    
    Args:
        excel_file: Path to Excel file
        required_columns: List of required column keys (e.g., ["src_server", "src_db"])
        default_user: Default user if not in Excel
    
    Returns:
        List of dictionaries, each containing configuration
    """
    excel_path = Path(excel_file)
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_file}")
    
    df = pd.read_excel(excel_path)
    
    # Normalize column names
    column_map = {}
    for col in df.columns:
        normalized = normalize_column_name(col)
        if normalized:
            column_map[normalized] = col
    
    # Check required columns
    missing = [req for req in required_columns if req not in column_map]
    if missing:
        raise ValueError(
            f"Missing required columns in Excel file: {missing}\n"
            f"Available columns: {df.columns.tolist()}\n"
            f"Please ensure your Excel file has columns for: {required_columns}"
        )
    
    # Check for user columns - we support three patterns:
    # 1. Single "user" column (applies to both src and dest)
    # 2. Separate "src_user" and "dest_user" columns
    # 3. Any combination
    has_user_column = "user" in column_map
    has_src_user_column = "src_user" in column_map
    has_dest_user_column = "dest_user" in column_map
    
    # Check for password columns - same patterns as user
    has_password_column = "password" in column_map
    has_src_password_column = "src_password" in column_map
    has_dest_password_column = "dest_password" in column_map
    
    # Extract configurations
    configs = []
    for idx, row in df.iterrows():
        config = {}
        
        # Extract all mapped columns first
        for key, excel_col in column_map.items():
            val = row[excel_col]
            if pd.notna(val):
                val_str = str(val).strip()
                if val_str.lower() not in ['nan', 'none', '']:
                    if key in ["src_server", "dest_server"]:
                        config[key] = clean_server_name(val_str)
                    elif key in ["batch_size", "parallel_tables", "max_retries", "chunk_threshold", "num_chunks", "chunk_workers"]:
                        try:
                            config[key] = int(float(val))
                        except (ValueError, TypeError):
                            pass
                    elif key in ["truncate_dest", "delete_dest", "restore_tables", "restore_programmables",
                                "restore_constraints", "restore_indexes", "skip_backup", "skip_migration", "skip_restore",
                                "disable_fk", "disable_indexes", "disable_triggers", "skip_completed", "resume_enabled",
                                "verify_after_copy", "enable_chunking"]:
                        val_lower = val_str.lower()
                        config[key] = val_lower in ['true', 'yes', '1', 'y', 't']
                    else:
                        config[key] = val_str
        
        # Handle user columns - priority: specific columns > generic "user" > default_user
        # Get src_user
        src_user = config.get("src_user")
        if not src_user and has_user_column:
            src_user = config.get("user")
        if not src_user:
            src_user = default_user
        config["src_user"] = src_user
        
        # Get dest_user
        dest_user = config.get("dest_user")
        if not dest_user and has_user_column:
            dest_user = config.get("user")
        if not dest_user:
            dest_user = default_user
        config["dest_user"] = dest_user
        
        # Also set generic user for backwards compatibility
        if not config.get("user"):
            config["user"] = src_user or dest_user
        
        # Handle password columns - priority: specific columns > generic "password"
        # Get src_password
        src_password = config.get("src_password")
        if not src_password and has_password_column:
            src_password = config.get("password")
        config["src_password"] = src_password
        
        # Get dest_password
        dest_password = config.get("dest_password")
        if not dest_password and has_password_column:
            dest_password = config.get("password")
        config["dest_password"] = dest_password
        
        # Also set generic password for backwards compatibility
        if not config.get("password"):
            config["password"] = src_password or dest_password
        
        # Skip rows with empty required fields
        if not all(config.get(f) for f in required_columns if f != "user"):
            continue
        
        configs.append(config)
    
    if not configs:
        raise ValueError("No valid configurations found in Excel file")
    
    return configs


def create_sample_excel(template_type: str, output_path: Optional[str] = None) -> str:
    """
    Create a sample Excel template file.
    
    Args:
        template_type: Type of template ("full_migration", "schema_backup", "schema_restore", 
                      "data_migration", "data_validation", "schema_validation")
        output_path: Optional output path, if None will use file dialog
    
    Returns:
        Path to created file
    """
    if template_type == "full_migration":
        data = {
            "Source Server": ["source1.database.windows.net", "source2.database.windows.net"],
            "Source Database": ["SourceDB1", "SourceDB2"],
            "Source Authentication": ["entra_mfa", "entra_mfa"],
            "Source User": ["user@domain.com", "user@domain.com"],
            "Source Password": ["", ""],
            "Destination Server": ["dest1.database.windows.net", "dest2.database.windows.net"],
            "Destination Database": ["DestDB1", "DestDB2"],
            "Destination Authentication": ["entra_mfa", "entra_mfa"],
            "Destination User": ["user@domain.com", "user@domain.com"],
            "Destination Password": ["", ""],
            "Batch Size": [20000, 20000],
            "Truncate Dest": [False, False],
            "Skip Backup": [False, False],
            "Skip Migration": [False, False],
            "Skip Restore": [False, False],
        }
    elif template_type == "schema_backup":
        data = {
            "Source Server": ["server1.database.windows.net", "server2.database.windows.net"],
            "Source Database": ["DB1", "DB2"],
            "Source Authentication": ["entra_mfa", "entra_mfa"],
            "Source User": ["user@domain.com", "user@domain.com"],
            "Source Password": ["", ""],  # Optional: for SQL auth or entra_password
        }
    elif template_type == "schema_restore":
        data = {
            "Backup Path": ["backups/.../runs/20260101_120000", "backups/.../runs/20260101_130000"],
            "Destination Server": ["dest1.database.windows.net", "dest2.database.windows.net"],
            "Destination Database": ["DestDB1", "DestDB2"],
            "Destination Authentication": ["entra_mfa", "entra_mfa"],
            "Destination User": ["user@domain.com", "user@domain.com"],
            "Destination Password": ["", ""],  # Optional: for SQL auth or entra_password
            "Restore Tables": [True, True],
            "Restore Programmables": [True, True],
            "Restore Constraints": [True, True],
            "Restore Indexes": [True, True],
        }
    elif template_type == "data_migration":
        data = {
            "Source Server": ["src1.database.windows.net", "src2.database.windows.net"],
            "Source Database": ["SrcDB1", "SrcDB2"],
            "Source Authentication": ["entra_mfa", "entra_mfa"],
            "Source User": ["user@domain.com", "user@domain.com"],
            "Source Password": ["", ""],
            "Destination Server": ["dest1.database.windows.net", "dest2.database.windows.net"],
            "Destination Database": ["DestDB1", "DestDB2"],
            "Destination Authentication": ["entra_mfa", "entra_mfa"],
            "Destination User": ["user@domain.com", "user@domain.com"],
            "Destination Password": ["", ""],
            "Batch Size": [20000, 20000],
            "Truncate Dest": [False, False],
            "Tables": ["", ""],  # Empty for all tables, or comma-separated list
            "Exclude": ["", ""],  # Comma-separated list of tables to exclude
            "Disable FK": [False, False],
            "Disable Indexes": [False, False],
            "Disable Triggers": [False, False],
            "Parallel Tables": [1, 1],
            "Skip Completed": [False, False],
            "Resume Enabled": [True, True],
            "Max Retries": [3, 3],
            "Verify After Copy": [False, False],
            "Enable Chunking": [False, False],
            "Chunk Threshold": [10000000, 10000000],
            "Num Chunks": [10, 10],
            "Chunk Workers": [4, 4],
        }
    elif template_type == "data_validation":
        data = {
            "Source Server": ["src1.database.windows.net", "src2.database.windows.net"],
            "Source Database": ["SrcDB1", "SrcDB2"],
            "Source Authentication": ["entra_mfa", "entra_mfa"],
            "Source User": ["user@domain.com", "user@domain.com"],
            "Source Password": ["", ""],  # Optional: for SQL auth or entra_password
            "Destination Server": ["dest1.database.windows.net", "dest2.database.windows.net"],
            "Destination Database": ["DestDB1", "DestDB2"],
            "Destination Authentication": ["entra_mfa", "entra_mfa"],
            "Destination User": ["user@domain.com", "user@domain.com"],
            "Destination Password": ["", ""],  # Optional: for SQL auth or entra_password
            "Table Name": ["", ""],  # Empty for all tables
        }
    elif template_type == "schema_validation":
        data = {
            "Source Server": ["src1.database.windows.net", "src2.database.windows.net"],
            "Source Database": ["SrcDB1", "SrcDB2"],
            "Source Authentication": ["entra_mfa", "entra_mfa"],
            "Source User": ["user@domain.com", "user@domain.com"],
            "Source Password": ["", ""],
            "Destination Server": ["dest1.database.windows.net", "dest2.database.windows.net"],
            "Destination Database": ["DestDB1", "DestDB2"],
            "Destination Authentication": ["entra_mfa", "entra_mfa"],
            "Destination User": ["user@domain.com", "user@domain.com"],
            "Destination Password": ["", ""],
        }
    else:
        raise ValueError(f"Unknown template type: {template_type}")
    
    df = pd.DataFrame(data)
    
    if output_path is None:
        output_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            title=f"Save {template_type} Template"
        )
        if not output_path:
            return None
    
    df.to_excel(output_path, index=False)
    return output_path


