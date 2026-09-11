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


def disk_restore_unsupported_for_host(server: str) -> bool:
    """True when hostname is Azure SQL Database or Managed Instance (*.database.windows.net)."""
    host = (server or "").split(",")[0].strip().lower()
    return ".database.windows.net" in host


def disk_restore_unsupported_message(
    server: str = "",
    engine_edition: Optional[int] = None,
) -> Optional[str]:
    """
    Return a user-facing message when RESTORE FROM DISK is not supported on the target.

    Azure SQL Managed Instance and Azure SQL Database require blob URL restores only.
    """
    try:
        from azure_migration_tool.src.utils.azure_compat import (
            AZURE_MANAGED_INSTANCE_EDITION,
            AZURE_SQL_DATABASE_EDITION,
            detect_azure_engine_edition,
            is_azure_sql_server,
        )
    except ImportError:
        from src.utils.azure_compat import (
            AZURE_MANAGED_INSTANCE_EDITION,
            AZURE_SQL_DATABASE_EDITION,
            detect_azure_engine_edition,
            is_azure_sql_server,
        )

    edition = engine_edition
    if edition is None and disk_restore_unsupported_for_host(server):
        edition = AZURE_MANAGED_INSTANCE_EDITION

    if edition == AZURE_MANAGED_INSTANCE_EDITION:
        target = "Azure SQL Managed Instance"
    elif edition == AZURE_SQL_DATABASE_EDITION:
        target = "Azure SQL Database"
    elif is_azure_sql_server(server):
        target = "Azure SQL"
    else:
        return None

    return (
        f"{target} cannot restore from a local or UNC .bak path (RESTORE FROM DISK).\n\n"
        "Use the 'Restore from Blob' tab instead:\n"
        "  1. Upload the .bak to Azure Blob Storage (Local Backup tab or AzCopy)\n"
        "  2. Restore to this server from the blob URL\n\n"
        "SQL Server error 41902 (when attempted): URI backup device only."
    )


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
    
    # Query backup history (one row per stripe; grouped below by backup_set_id)
    sql = """
        SELECT
            bs.database_name,
            bs.backup_set_id,
            bmf.physical_device_name,
            bs.backup_finish_date,
            bs.backup_size / 1024.0 / 1024.0 AS size_mb,
            bs.type,
            bs.compressed_backup_size / 1024.0 / 1024.0 AS compressed_size_mb
        FROM msdb.dbo.backupset bs
        JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
        WHERE bs.type = 'D'
    """
    params: List = []
    if database:
        sql += " AND bs.database_name = ?"
        params.append(database)
    sql += " ORDER BY bs.backup_finish_date DESC"

    cur.execute(sql, params)

    grouped: Dict[Any, Dict[str, Any]] = {}
    for row in cur:
        backup_type = {"D": "Full", "I": "Differential", "L": "Log"}.get(row[5], "Unknown")
        key = row[1]
        entry = grouped.get(key)
        if entry is None:
            entry = {
                "database_name": row[0],
                "backup_set_id": row[1],
                "paths": [],
                "backup_date": row[3],
                "size_mb": round(float(row[4] or 0), 2),
                "compressed_size_mb": round(float(row[6]), 2) if row[6] is not None else None,
                "type": backup_type,
            }
            grouped[key] = entry
        path = (row[2] or "").strip()
        if path and path not in entry["paths"]:
            entry["paths"].append(path)

    backups = []
    for entry in sorted(grouped.values(), key=lambda e: e["backup_date"], reverse=True)[:limit]:
        try:
            from ..backup.backup_path_utils import (
                discover_disk_stripe_set,
                format_backup_paths_for_ui,
            )
        except ImportError:
            from src.backup.backup_path_utils import (
                discover_disk_stripe_set,
                format_backup_paths_for_ui,
            )
        primary = entry["paths"][0] if entry["paths"] else ""
        paths = discover_disk_stripe_set(primary) if primary else list(entry["paths"])
        if len(paths) == 1 and len(entry["paths"]) > 1:
            paths = list(entry["paths"])
        backups.append(
            {
                "database_name": entry["database_name"],
                "backup_path": format_backup_paths_for_ui(paths),
                "backup_paths": paths,
                "stripe_count": len(paths),
                "backup_date": entry["backup_date"],
                "size_mb": entry["size_mb"],
                "compressed_size_mb": entry["compressed_size_mb"],
                "type": entry["type"],
            }
        )
    
    cur.close()
    conn.close()
    
    log(f"Found {len(backups)} backup(s)")
    return backups


def restore_database_from_disk(
    *,
    server: str,
    backup_file_path: str = "",
    backup_file_paths: Optional[List[str]] = None,
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
    cancel_event: Optional[Any] = None,
    on_connect: Optional[Callable[[Any], None]] = None,
) -> Dict[str, Any]:
    """
    Restore SQL Server database from a local or network .bak file.
    
    Args:
        server: SQL Server instance
        backup_file_path: Full path to .bak file (local or UNC), or first stripe when using backup_file_paths
        backup_file_paths: Optional list of .bak paths (striped backup set)
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
        paths: List[str] = []
        if backup_file_paths:
            paths = [p.strip() for p in backup_file_paths if (p or "").strip()]
        elif backup_file_path:
            raw = backup_file_path.strip()
            if ";" in raw:
                paths = [p.strip() for p in raw.split(";") if p.strip()]
            else:
                paths = [raw]
        if not paths:
            raise ValueError("No backup file path(s) provided.")

        try:
            from ..backup.backup_path_utils import discover_disk_stripe_set
        except ImportError:
            from src.backup.backup_path_utils import discover_disk_stripe_set
        if len(paths) == 1:
            paths = discover_disk_stripe_set(paths[0])
        primary_path = paths[0]

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

        if on_connect:
            try:
                on_connect(conn)
            except Exception:
                pass

        try:
            from azure_migration_tool.src.utils.azure_compat import detect_azure_engine_edition
        except ImportError:
            from src.utils.azure_compat import detect_azure_engine_edition
        engine_edition = detect_azure_engine_edition(cur, server)
        unsupported = disk_restore_unsupported_message(server, engine_edition)
        if unsupported:
            raise ValueError(unsupported)

        _watcher_stop = False
        if cancel_event is not None:
            def _watch_cancel() -> None:
                while not _watcher_stop:
                    if cancel_event.wait(0.5):
                        try:
                            conn.cancel()
                            log("Cancellation requested — aborting RESTORE…")
                        except Exception:
                            pass
                        return

            threading.Thread(target=_watch_cancel, daemon=True).start()
        
        # Step 1: Read backup file header to get original database name and file list
        log(f"Reading backup file header from: {primary_path}")
        if len(paths) > 1:
            log(f"Striped restore: {len(paths)} file(s)")
            for p in paths:
                log(f"  - {p}")
        log("This may take a moment for network paths...")
        
        # Get backup set info (first stripe is sufficient for metadata)
        cur.execute("RESTORE HEADERONLY FROM DISK = ?", (primary_path,))
        header = cur.fetchone()
        if not header:
            raise ValueError(f"Cannot read backup file: {primary_path}")
        
        original_db_name = header[0]  # DatabaseName is first column
        log(f"Original database name: {original_db_name}")
        
        # Determine target database name
        if not target_database_name:
            target_database_name = original_db_name
        
        log(f"Target database name: {target_database_name}")
        
        # Get file list from backup
        cur.execute("RESTORE FILELISTONLY FROM DISK = ?", (primary_path,))
        file_list = cur.fetchall()
        
        if not file_list:
            raise ValueError(f"No files found in backup: {primary_path}")
        
        log(f"Backup contains {len(file_list)} file(s)")
        
        # Build RESTORE command (one DISK per stripe when needed)
        if len(paths) == 1:
            restore_sql = f"RESTORE DATABASE {_q(target_database_name)} FROM DISK = ?"
            restore_params: tuple = (paths[0],)
        else:
            disk_clause = ", ".join("DISK = ?" for _ in paths)
            restore_sql = f"RESTORE DATABASE {_q(target_database_name)} FROM {disk_clause}"
            restore_params = tuple(paths)
        
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
                cur.execute(restore_sql, restore_params)

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
            _watcher_stop = True
            
        except Exception as restore_error:
            restore_elapsed = time.time() - restore_start
            _watcher_stop = True
            if cancel_event is not None and cancel_event.is_set():
                result["cancelled"] = True
                result["message"] = "Restore cancelled by user."
                log("Restore cancelled by user.")
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass
                return result
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
                    f"Cannot access backup file: {primary_path}\n\n"
                    f"Solutions:\n"
                    f"1. Verify the path is correct\n"
                    f"2. Ensure SQL Server service account has read permission\n"
                    f"3. For network paths, verify the UNC path is accessible\n\n"
                    f"Original error: {restore_error}"
                )
            elif "41902" in error_str or "uri backup device" in error_str.lower():
                raise ValueError(
                    disk_restore_unsupported_message(server)
                    or (
                        "This SQL Server target cannot restore from DISK/UNC paths. "
                        "Use Restore from Blob instead."
                    )
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
