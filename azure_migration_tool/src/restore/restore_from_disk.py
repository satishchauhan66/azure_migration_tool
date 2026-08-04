# Author: S@tish Ch@uhan

"""
Restore SQL Server database from local or network disk path.

This module handles:
1. Restoring from local paths (C:\SQLBackups\*.bak)
2. Restoring from network UNC paths (\\server\share\*.bak)
3. Querying SQL Server backup history to find available backups
4. Automatic file relocation for RESTORE WITH MOVE
"""

import logging
import time
import threading
from pathlib import Path
from typing import Optional, Callable, Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


def _q(name: str) -> str:
    """Quote SQL identifier."""
    return "[" + name.replace("]", "]]") + "]"


def get_recent_backups_from_history(
    *,
    server: str,
    database: str = None,
    auth: str = "windows",
    user: str = "",
    password: str = "",
    driver: str = "ODBC Driver 18 for SQL Server",
    limit: int = 10,
    log: Optional[Callable[[str], None]] = None,
) -> List[Dict[str, Any]]:
    """
    Query SQL Server backup history to find recent backup files.
    
    Args:
        server: SQL Server instance
        database: Optional - filter by database name
        auth: 'windows' or 'sql'
        user: SQL auth username
        password: SQL auth password
        driver: ODBC driver name
        limit: Max number of backups to return
        log: Optional logging callback
        
    Returns:
        List of dicts with: database_name, backup_path, backup_date, size_mb, type
    """
    if log is None:
        log = logger.info
    
    try:
        import pyodbc
        from azure_migration_tool.src.utils.database import connect_to_database
    except ImportError as e:
        raise ImportError(f"Missing required dependency: {e}")
    
    log(f"Querying backup history on {server}...")
    
    conn = connect_to_database(
        server=server,
        db="master",
        user=user,
        driver=driver,
        auth=auth,
        password=password,
        timeout=30,
        logger=logger,
    )
    
    cur = conn.cursor()
    
    # Query backup history
    sql = """
        SELECT TOP (?)
            bs.database_name,
            bmf.physical_device_name,
            bs.backup_finish_date,
            bs.backup_size / 1024.0 / 1024.0 AS size_mb,
            bs.type,
            bs.compressed_backup_size / 1024.0 / 1024.0 AS compressed_size_mb
        FROM msdb.dbo.backupset bs
        JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
        WHERE bs.type = 'D'  -- Full database backups only
    """
    
    params = [limit]
    
    if database:
        sql += " AND bs.database_name = ?"
        params.append(database)
    
    sql += " ORDER BY bs.backup_finish_date DESC"
    
    cur.execute(sql, params)
    
    backups = []
    for row in cur:
        backup_type = {
            'D': 'Full',
            'I': 'Differential',
            'L': 'Log'
        }.get(row[4], 'Unknown')
        
        backups.append({
            'database_name': row[0],
            'backup_path': row[1],
            'backup_date': row[2],
            'size_mb': round(row[3], 2),
            'compressed_size_mb': round(row[5], 2) if row[5] else None,
            'type': backup_type,
        })
    
    cur.close()
    conn.close()
    
    log(f"Found {len(backups)} backup(s)")
    return backups


def restore_database_from_disk(
    *,
    server: str,
    backup_file_path: str,
    target_database_name: str = None,
    auth: str = "windows",
    user: str = "",
    password: str = "",
    driver: str = "ODBC Driver 18 for SQL Server",
    data_file_path: str = None,
    log_file_path: str = None,
    replace_existing: bool = False,
    recovery: bool = True,
    log: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[float], None]] = None,
) -> Dict[str, Any]:
    """
    Restore SQL Server database from a local or network .bak file.
    
    Args:
        server: SQL Server instance
        backup_file_path: Full path to .bak file (local or UNC)
        target_database_name: Database name to restore as (defaults to original name)
        auth: 'windows' or 'sql'
        user: SQL auth username
        password: SQL auth password
        driver: ODBC driver name
        data_file_path: Optional custom path for .mdf file
        log_file_path: Optional custom path for .ldf file
        replace_existing: If True, use WITH REPLACE to overwrite existing database
        recovery: If True, database is brought online after restore
        log: Optional logging callback
        
    Returns:
        Dict with: success, database_name, restore_time_sec, message
    """
    if log is None:
        log = logger.info
    
    result = {
        "success": False,
        "database_name": "",
        "restore_time_sec": 0,
        "message": "",
    }
    
    try:
        import pyodbc
        from azure_migration_tool.src.utils.database import connect_to_database
    except ImportError as e:
        error_msg = f"Missing required dependency: {e}"
        log(f"ERROR: {error_msg}")
        result["message"] = error_msg
        return result
    
    try:
        log(f"Connecting to SQL Server: {server}")
        conn = connect_to_database(
            server=server,
            db="master",
            user=user,
            driver=driver,
            auth=auth,
            password=password,
            timeout=60,
            logger=logger,
        )
        
        # Enable autocommit - RESTORE cannot run in a transaction
        conn.autocommit = True
        cur = conn.cursor()
        
        # Step 1: Read backup file header to get original database name and file list
        log(f"Reading backup file header from: {backup_file_path}")
        log("This may take a moment for network paths...")
        
        # Get backup set info
        cur.execute(f"RESTORE HEADERONLY FROM DISK = ?", (backup_file_path,))
        header = cur.fetchone()
        if not header:
            raise ValueError(f"Cannot read backup file: {backup_file_path}")
        
        original_db_name = header[0]  # DatabaseName is first column
        log(f"Original database name: {original_db_name}")
        
        # Determine target database name
        if not target_database_name:
            target_database_name = original_db_name
        
        log(f"Target database name: {target_database_name}")
        
        # Get file list from backup
        cur.execute(f"RESTORE FILELISTONLY FROM DISK = ?", (backup_file_path,))
        file_list = cur.fetchall()
        
        if not file_list:
            raise ValueError(f"No files found in backup: {backup_file_path}")
        
        log(f"Backup contains {len(file_list)} file(s)")
        
        # Build RESTORE command
        restore_sql = f"RESTORE DATABASE {_q(target_database_name)} FROM DISK = ?"
        
        # Build WITH clause
        with_clauses = []
        
        # Add file relocations
        for file_info in file_list:
            logical_name = file_info[0]
            file_type = file_info[2]  # 'D' for data, 'L' for log
            original_physical_path = file_info[1]
            
            # Determine new physical path
            if file_type == 'D' and data_file_path:
                new_path = data_file_path
            elif file_type == 'L' and log_file_path:
                new_path = log_file_path
            else:
                # Use SQL Server's default data directory
                cur.execute("""
                    DECLARE @DefaultData NVARCHAR(512)
                    EXEC master.dbo.xp_instance_regread 
                        N'HKEY_LOCAL_MACHINE',
                        N'Software\\Microsoft\\MSSQLServer\\MSSQLServer',
                        N'DefaultData',
                        @DefaultData OUTPUT
                    SELECT ISNULL(@DefaultData, SERVERPROPERTY('InstanceDefaultDataPath'))
                """)
                default_dir_row = cur.fetchone()
                default_dir = default_dir_row[0] if default_dir_row and default_dir_row[0] else None
                
                if not default_dir:
                    # Fallback: use original path's directory
                    import os
                    default_dir = os.path.dirname(original_physical_path)
                
                # Generate new filename
                import os
                original_filename = os.path.basename(original_physical_path)
                # Replace original DB name with target DB name in filename
                new_filename = original_filename.replace(original_db_name, target_database_name)
                new_path = os.path.join(default_dir, new_filename)
            
            with_clauses.append(f"MOVE '{logical_name}' TO '{new_path}'")
            log(f"  {logical_name} ({file_type}): {new_path}")
        
        # Add other options
        if replace_existing:
            with_clauses.append("REPLACE")
        
        with_clauses.append("STATS = 10")
        
        if recovery:
            with_clauses.append("RECOVERY")
        else:
            with_clauses.append("NORECOVERY")
        
        if with_clauses:
            restore_sql += " WITH " + ", ".join(with_clauses)
        
        log("")
        log(f"Starting restore of {target_database_name}...")
        log("This may take several minutes for large databases...")
        log("")
        
        restore_start = time.time()

        # Best-effort progress monitor (second connection polls percent_complete).
        try:
            from azure_migration_tool.src.restore.restore_from_blob import (
                _get_spid,
                _monitor_request_progress,
            )
        except ImportError:
            try:
                from src.restore.restore_from_blob import _get_spid, _monitor_request_progress
            except ImportError:
                _get_spid = None
                _monitor_request_progress = None

        stop_event = threading.Event()
        monitor = None
        spid = _get_spid(cur) if _get_spid else None
        if spid and _monitor_request_progress:
            monitor_connect_kwargs = dict(
                server=server,
                db="master",
                user=user or "",
                driver=driver,
                auth=auth or "windows",
                password=password,
                timeout=60,
                logger=logger,
            )
            monitor = threading.Thread(
                target=_monitor_request_progress,
                kwargs=dict(
                    connect_kwargs=monitor_connect_kwargs,
                    spid=spid,
                    log=log,
                    stop_event=stop_event,
                    poll_sec=5.0,
                    progress_callback=progress_callback,
                ),
                daemon=True,
            )
            log(f"Monitoring restore progress (SPID {spid}) — % complete will appear below...")
            monitor.start()

        try:
            try:
                cur.execute(restore_sql, (backup_file_path,))

                # Fetch progress messages
                while cur.nextset():
                    pass
            finally:
                stop_event.set()
                if monitor is not None:
                    monitor.join(timeout=6)

            restore_elapsed = time.time() - restore_start
            
            log("")
            log(f"✓ Restore completed successfully in {restore_elapsed:.1f}s")
            log(f"  Database: {target_database_name}")
            
            result["success"] = True
            result["database_name"] = target_database_name
            result["restore_time_sec"] = round(restore_elapsed, 2)
            result["message"] = f"Successfully restored {target_database_name}"
            
        except Exception as restore_error:
            restore_elapsed = time.time() - restore_start
            error_str = str(restore_error)
            
            # Check for common errors
            if "already exists" in error_str.lower():
                raise RuntimeError(
                    f"Database '{target_database_name}' already exists.\n\n"
                    f"Solutions:\n"
                    f"1. Check 'Replace existing database' option\n"
                    f"2. Choose a different target database name\n"
                    f"3. Manually drop the existing database first\n\n"
                    f"Original error: {restore_error}"
                )
            elif "access is denied" in error_str.lower() or "cannot open" in error_str.lower():
                raise RuntimeError(
                    f"Cannot access backup file: {backup_file_path}\n\n"
                    f"Solutions:\n"
                    f"1. Verify the path is correct\n"
                    f"2. Ensure SQL Server service account has read permission\n"
                    f"3. For network paths, verify the UNC path is accessible\n\n"
                    f"Original error: {restore_error}"
                )
            else:
                raise restore_error
        
        cur.close()
        conn.close()
        
    except Exception as e:
        error_msg = str(e)
        log("")
        log(f"ERROR: {error_msg}")
        result["message"] = error_msg
        return result
    
    return result
