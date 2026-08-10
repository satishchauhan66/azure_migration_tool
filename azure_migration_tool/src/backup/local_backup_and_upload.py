# Author: Satish Chauhan

"""
Local SQL Server backup + upload to Azure Blob Storage.

This is an alternative to BACKUP TO URL for environments where:
- SQL Server version < 2012 (no BACKUP TO URL support)
- Network restrictions prevent direct blob access from SQL Server
- Compliance requires separation of backup and upload steps

Flow:
1. BACKUP DATABASE to local disk path
2. Upload .bak file to Azure Blob Storage (using managed identity or SAS)
3. Optionally delete local file after successful upload

Limitations:
- Requires local disk space (database size + overhead)
- Slower than direct BACKUP TO URL (sequential vs parallel)
- Not suitable for very large databases (>5TB) without substantial local storage
"""

import os
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Dict, Any

logger = logging.getLogger(__name__)

# Small probe file for "create folder + test write" from this Windows host (not SQL Server).
_FOLDER_WRITE_PROBE_NAME = "._amt_folder_write_probe.tmp"

# Matches local backup filenames: MyDb_20260810_031718.bak or MyDb_20260810_031718_part01of04.bak
_BACKUP_FILENAME_RUN_RE = re.compile(
    r"^(.+)_(\d{8}_\d{6})(?:_part\d+of\d+)?$",
    re.IGNORECASE,
)


def _safe_db_folder_name(database: str) -> str:
    """Sanitize database name for blob folder segments (same rules as .bak to Blob tab)."""
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", (database or "").strip())[:128]


def _parse_backup_filename(filename: str) -> tuple[str, str]:
    """
    Parse a local backup filename into (database_folder, run_id).

    Examples:
      NICU_ss_sld_db22u_20260810_031718.bak -> (NICU_ss_sld_db22u, 20260810_031718)
      NICU_ss_sld_db22u_20260810_031718_part01of04.bak -> same run folder for all stripes
    """
    stem = Path(filename).stem
    m = _BACKUP_FILENAME_RUN_RE.match(stem)
    if m:
        return _safe_db_folder_name(m.group(1)), m.group(2)
    if "_part" in stem.lower():
        base, _, _rest = stem.partition("_part")
        m2 = _BACKUP_FILENAME_RUN_RE.match(base)
        if m2:
            return _safe_db_folder_name(m2.group(1)), m2.group(2)
    try:
        from ..utils.paths import utc_ts_compact
    except ImportError:
        from src.utils.paths import utc_ts_compact
    return _safe_db_folder_name(stem), utc_ts_compact()


def build_blob_upload_path(
    *,
    local_filename: str,
    blob_folder: str = "",
    database: str = "",
    run_id: str = "",
    structured_layout: bool = True,
) -> str:
    """
    Build the blob path (prefix inside container) for a local .bak upload.

    Structured layout (default, matches .bak to Blob tab):
      [optional_root/]database_name/run_id/filename.bak

    Flat layout (legacy):
      [optional_root/]filename.bak
    """
    name = Path(local_filename).name
    prefix = (blob_folder or "").strip().strip("/")

    if not structured_layout:
        parts = [p for p in (prefix, name) if p]
        return "/".join(parts)

    safe_db = _safe_db_folder_name(database) if database else ""
    resolved_run = (run_id or "").strip()
    if not safe_db or not resolved_run:
        inferred_db, inferred_run = _parse_backup_filename(name)
        safe_db = safe_db or inferred_db
        resolved_run = resolved_run or inferred_run

    segments = [p for p in (prefix, safe_db, resolved_run, name) if p]
    return "/".join(segments)


def normalize_backup_input_path(raw: Optional[str]) -> str:
    """Strip quotes/whitespace; normalize slashes for Windows (UNC and drive paths)."""
    s = (raw or "").strip().strip('"').strip("'")
    return s.replace("/", "\\")


def ensure_dir_and_probe_write(
    local_backup_path: str,
    *,
    apply_icacls_everyone: bool = False,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Create the backup folder (and parents) on this PC using os.makedirs, then verify
    write access with a tiny temp file. Use for UNC/local paths before relying on SQL Server.

    If local_backup_path ends with .bak, the parent directory is created and tested.

    Returns dict: success, message, resolved_directory, normalized_path, explicit_bak_file.
    """
    _log = log or logger.info
    out: Dict[str, Any] = {
        "success": False,
        "message": "",
        "resolved_directory": "",
        "normalized_path": "",
        "explicit_bak_file": False,
    }
    raw = normalize_backup_input_path(local_backup_path)
    out["normalized_path"] = raw
    if not raw:
        out["message"] = "Path is empty."
        return out

    explicit = raw.lower().endswith(".bak")
    out["explicit_bak_file"] = explicit
    target_dir = os.path.dirname(raw) if explicit else raw
    out["resolved_directory"] = target_dir
    if not target_dir:
        out["message"] = "Could not resolve a folder (invalid path or bare filename)."
        return out

    try:
        os.makedirs(target_dir, exist_ok=True)
    except OSError as e:
        out["message"] = f"Cannot create folder: {e}"
        return out

    probe = Path(target_dir) / _FOLDER_WRITE_PROBE_NAME
    try:
        probe.write_text("azure_migration_tool", encoding="ascii")
    except OSError as e:
        out["message"] = (
            f"Folder exists or was created, but this Windows account cannot write there: {e}\n"
            "Grant share + NTFS modify to your login for the share, or use a path writable from this PC."
        )
        return out
    finally:
        try:
            if probe.exists():
                probe.unlink()
        except OSError:
            pass

    if apply_icacls_everyone:
        try:
            from ..utils.subprocess_utils import run_silent

            icacls_result = run_silent(
                ["icacls", target_dir, "/grant", "Everyone:(OI)(CI)F", "/T"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if icacls_result.returncode == 0:
                _log(f"Set permissions on {target_dir} (Everyone: Full Control)")
            else:
                _log(f"Warning: icacls returned {icacls_result.returncode}: {icacls_result.stderr or icacls_result.stdout}")
        except Exception as e:
            _log(f"Warning: could not run icacls: {e}")

    out["success"] = True
    out["message"] = f"Folder is ready and writable from this PC:\n{target_dir}"
    _log(out["message"])
    return out


def run_local_backup_and_upload(
    *,
    server: str,
    database: str,
    auth: str = "windows",
    user: str = "",
    password: str = "",
    driver: str = "ODBC Driver 18 for SQL Server",
    local_backup_path: str,
    blob_auth_mode: str = "connection_string",
    blob_connection_string: str = "",
    blob_account_url: str = "",
    blob_container: str = "",
    blob_folder: str = "",
    run_id: str = "",
    structured_blob_paths: bool = True,
    delete_local_after_upload: bool = False,
    compression: bool = True,
    skip_upload: bool = False,
    stripes: int = 1,
    cancel_event: Optional[Any] = None,
    on_connect: Optional[Callable[[Any], None]] = None,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Backup SQL Server database to local disk, optionally upload to Azure Blob Storage.

    stripes: split the backup across N files (BACKUP ... TO DISK=f1, DISK=f2, ...). N>1 can be
        faster for large databases. Files are named ``<db>_<ts>_partNNofMM.bak``.
    cancel_event: optional threading.Event; if set (e.g. via a Stop button), a watcher cancels
        the running BACKUP on the connection.
    on_connect: optional callback given the live pyodbc connection right after connecting, so a
        caller can cancel the in-progress BACKUP (connection.cancel()).
    
    Args:
        server: SQL Server instance (e.g., 'localhost', 'server\\instance')
        database: Database name to backup
        auth: 'windows' or 'sql'
        user: SQL auth username (if auth='sql')
        password: SQL auth password (if auth='sql')
        driver: ODBC driver name
        local_backup_path: Backup folder (e.g. 'C:\\Backups' or '\\\\fileserver\\share'),
            or a full path ending in .bak for a fixed filename (e.g. '\\\\server\\share\\MyDb.bak').
        blob_auth_mode: 'connection_string' or 'managed_identity'
        blob_connection_string: Azure Storage connection string (if mode=connection_string)
        blob_account_url: Storage account URL (if mode=managed_identity)
        blob_container: Blob container name
        blob_folder: Optional folder path in container used as optional root prefix.
            With structured paths (default): root/database_name/run_id/file.bak
        run_id: Optional run folder (YYYYMMDD_HHMMSS). Inferred from filename when omitted.
        structured_blob_paths: When True, upload to database/run_id/ (same layout as .bak to Blob).
        delete_local_after_upload: If True, delete local .bak after successful upload
        compression: If True, use SQL Server backup compression
        skip_upload: If True, only backup locally (no cloud upload)
        log: Optional logging callback function
        
    Returns:
        Dict with keys: success, local_file, blob_url, backup_time_sec, upload_time_sec, message
    """
    if log is None:
        log = logger.info
    try:
        from ..utils.redact_secrets import redact_sensitive_text
    except ImportError:
        try:
            from src.utils.redact_secrets import redact_sensitive_text
        except ImportError:
            def redact_sensitive_text(t: str) -> str:  # type: ignore[misc]
                return t

    _emit = log

    def log(msg: str) -> None:
        _emit(redact_sensitive_text(str(msg)))

    result = {
        "success": False,
        "local_file": "",
        "blob_url": "",
        "backup_time_sec": 0,
        "upload_time_sec": 0,
        "message": "",
    }
    
    try:
        # Import dependencies
        try:
            from ..utils.database import connect_to_database
        except ImportError:
            from azure_migration_tool.src.utils.database import connect_to_database
        
        import pyodbc
        
        # Step 1: Validate backup path — folder, or explicit file path ending in .bak
        raw_path = normalize_backup_input_path(local_backup_path)
        if not raw_path:
            raise ValueError("Backup path is empty")

        explicit_bak_file = raw_path.lower().endswith(".bak")
        if explicit_bak_file:
            local_file = Path(raw_path)
            local_backup_dir = local_file.parent
            backup_filename = local_file.name
            log(f"Using explicit backup file path: {local_file}")
        else:
            local_backup_dir = Path(raw_path)
            backup_filename = None

        is_unc_dir = str(local_backup_dir).startswith("\\\\") or str(local_backup_dir).startswith("//")

        try:
            # os.makedirs tends to behave more predictably than Path.mkdir on deep UNC paths.
            os.makedirs(str(local_backup_dir), exist_ok=True)
            if local_backup_dir.exists() and local_backup_dir.is_dir():
                log(f"Ensured backup directory exists: {local_backup_dir}")
        except OSError as e:
            if explicit_bak_file and is_unc_dir:
                log(
                    "Warning: Could not create or verify UNC folder from this PC: "
                    f"{e}. SQL Server may still succeed if the share exists for the service account."
                )
            else:
                raise ValueError(f"Cannot create backup directory {local_backup_dir}: {e}") from e

        if local_backup_dir.exists():
            if not local_backup_dir.is_dir():
                raise ValueError(f"Backup path is not a directory: {local_backup_dir}")
        elif not (explicit_bak_file and is_unc_dir):
            raise ValueError(f"Backup directory does not exist: {local_backup_dir}")

        # Grant Everyone full control (for SQL Server service account access); only if folder is visible here
        if local_backup_dir.exists() and local_backup_dir.is_dir():
            try:
                from ..utils.subprocess_utils import run_silent
                icacls_result = run_silent(
                    ['icacls', str(local_backup_dir), '/grant', 'Everyone:(OI)(CI)F', '/T'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if icacls_result.returncode == 0:
                    log(f"Set permissions on {local_backup_dir} (Everyone: Full Control)")
                else:
                    log(f"Warning: Could not set permissions (icacls returned {icacls_result.returncode})")
            except Exception as e:
                log(f"Warning: Could not set permissions: {e}")

        # Test write access from Python (optional for UNC not mapped on this host)
        if local_backup_dir.exists() and local_backup_dir.is_dir():
            try:
                test_file = local_backup_dir / ".test_write"
                test_file.write_text("test")
                test_file.unlink()
                log("Python write test: OK")
            except Exception as e:
                log(f"WARNING: Python cannot write to {local_backup_dir}: {e}")
        elif explicit_bak_file and is_unc_dir:
            log("Skipping Python write test (UNC folder not visible from this PC).")

        try:
            n_stripes = max(1, int(stripes or 1))
        except (TypeError, ValueError):
            n_stripes = 1

        if explicit_bak_file:
            if n_stripes > 1:
                stem = local_file.stem  # filename without .bak
                local_files = [
                    local_file.parent / f"{stem}_part{i:02d}of{n_stripes:02d}.bak"
                    for i in range(1, n_stripes + 1)
                ]
            else:
                local_files = [local_file]
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if n_stripes > 1:
                local_files = [
                    local_backup_dir / f"{database}_{timestamp}_part{i:02d}of{n_stripes:02d}.bak"
                    for i in range(1, n_stripes + 1)
                ]
            else:
                local_files = [local_backup_dir / f"{database}_{timestamp}.bak"]

        # Keep single-file variables pointing at the first stripe for existing logic below.
        local_file = local_files[0]
        backup_filename = local_file.name
        result["local_file"] = str(local_file)
        result["local_files"] = [str(f) for f in local_files]
        if n_stripes > 1:
            log(f"Striped backup: {n_stripes} files")
        
        # Step 2: Connect to SQL Server and perform local backup
        log(f"Connecting to SQL Server: {server}")
        
        # Build connection string for reuse
        if auth == "windows":
            conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE=master;Trusted_Connection=yes;TrustServerCertificate=yes;"
        else:
            conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE=master;UID={user};PWD={password};TrustServerCertificate=yes;"
        
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
        
        # Enable autocommit - BACKUP cannot run in a transaction
        conn.autocommit = True
        cur = conn.cursor()

        # Expose the live connection so a Stop button can cancel the running BACKUP.
        if on_connect:
            try:
                on_connect(conn)
            except Exception:
                pass

        # If a cancel_event is provided, watch it and cancel the connection when set.
        _watcher_stop = False
        if cancel_event is not None:
            def _watch_cancel():
                while not _watcher_stop:
                    if cancel_event.wait(0.5):
                        try:
                            conn.cancel()
                            log("Cancellation requested — aborting BACKUP…")
                        except Exception:
                            pass
                        return
            import threading as _t
            _t.Thread(target=_watch_cancel, daemon=True).start()
        
        # Check if database exists
        cur.execute(
            "SELECT database_id FROM sys.databases WHERE name = ?",
            (database,)
        )
        if not cur.fetchone():
            raise ValueError(f"Database '{database}' not found on server {server}")
        
        # Try to get SQL Server default backup directory for informational purposes
        sql_backup_dir = None
        try:
            cur.execute(
                """
                DECLARE @BackupDirectory NVARCHAR(512)
                EXEC master.dbo.xp_instance_regread 
                    N'HKEY_LOCAL_MACHINE',
                    N'Software\\Microsoft\\MSSQLServer\\MSSQLServer',
                    N'BackupDirectory',
                    @BackupDirectory OUTPUT
                SELECT @BackupDirectory AS BackupDirectory
                """
            )
            row = cur.fetchone()
            sql_backup_dir = row[0] if row and row[0] else None
            if sql_backup_dir:
                log(f"Info: SQL Server's default backup directory is: {sql_backup_dir}")
        except Exception:
            pass  # xp_instance_regread might not be available or user lacks permissions
        
        # Get database size for logging
        cur.execute(
            """
            SELECT 
                CAST(SUM(size) * 8.0 / 1024 / 1024 AS DECIMAL(10,2)) AS size_gb
            FROM sys.master_files
            WHERE database_id = DB_ID(?)
            """,
            (database,)
        )
        row = cur.fetchone()
        db_size = row[0] if row and row[0] is not None else 0
        log(f"Database size: ~{db_size} GB")
        
        # Build BACKUP DATABASE command (one DISK per stripe)
        disk_clause = ", ".join("DISK = ?" for _ in local_files)
        backup_sql = f"BACKUP DATABASE [{database}] TO {disk_clause}"
        if compression:
            backup_sql += " WITH COMPRESSION, STATS = 10"
        else:
            backup_sql += " WITH STATS = 10"
        
        if len(local_files) > 1:
            log(f"Starting striped local backup ({len(local_files)} files):")
            for f in local_files:
                log(f"  - {f}")
        else:
            log(f"Starting local backup to: {local_file}")
        log("This may take several minutes for large databases...")
        log("")
        
        # Detect if this is a network (UNC) path
        is_network_path = str(local_file).startswith("\\\\") or str(local_file).startswith("//")
        if is_network_path:
            log("⚠️ Network path detected - SQL Server may succeed, but Python cannot verify the file")
            log("")
        
        backup_start = time.time()
        try:
            log(f"Executing: BACKUP DATABASE [{database}] TO {disk_clause}")
            log("")
            cur.execute(backup_sql, tuple(str(f) for f in local_files))
            
            # Fetch all result sets (SQL Server returns progress messages)
            messages_received = False
            while cur.nextset():
                messages_received = True
                pass
            
            if not messages_received:
                log("Note: No progress messages received from SQL Server (this is normal for small databases)")
            
            backup_elapsed = time.time() - backup_start
            result["backup_time_sec"] = round(backup_elapsed, 2)
            _watcher_stop = True
            
            log("")
            log(f"SQL Server backup command completed in {backup_elapsed:.1f}s")
            
        except Exception as backup_error:
            backup_elapsed = time.time() - backup_start
            _watcher_stop = True
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

            # If the user pressed Stop, report a clean cancellation instead of a raw error.
            if cancel_event is not None and cancel_event.is_set():
                result["cancelled"] = True
                result["message"] = "Backup cancelled by user."
                log("Backup cancelled by user.")
                return result

            # Check if it's a permissions error
            error_str = str(backup_error)
            if "operating system error 3" in error_str.lower() or "cannot find the path" in error_str.lower():
                raise RuntimeError(
                    f"SQL Server cannot access: {local_file}\n\n"
                    f"Solutions:\n"
                    f"1. Use C:\\Temp (recommended)\n"
                    f"2. Click 'Use SQL Server Default' button\n"
                    f"3. Grant SQL Server service account write access to this path\n\n"
                    f"Original error: {backup_error}"
                )
            else:
                raise backup_error
        
        cur.close()
        conn.close()
        
        # Give the file system a moment to flush
        time.sleep(0.5)
        
        # Verify backup file exists (skip for UNC paths - Python may not have network access)
        is_network_path = str(local_file).startswith("\\\\") or str(local_file).startswith("//")
        
        file_accessible_for_upload = True
        if is_network_path:
            # For network paths, try to verify if Python can also access the file.
            log(f"✓ Local backup completed in {backup_elapsed:.1f}s")
            log(f"  File: {local_file}")
            try:
                if local_file.exists():
                    file_size_mb = local_file.stat().st_size / (1024 * 1024)
                    log(f"  Network file is accessible to Python (size: {file_size_mb:.1f} MB)")
                    result["file_size_mb"] = round(file_size_mb, 2)
                else:
                    file_accessible_for_upload = False
                    log("  Note: Network file is not visible to Python from this host.")
            except Exception as network_access_error:
                file_accessible_for_upload = False
                log(f"  Note: Python cannot read network file for upload: {network_access_error}")
        else:
            # For local paths, verify file exists and get size
            log(f"Verifying backup file...")
            log(f"Expected location: {local_file}")
            log(f"Checking directory: {local_backup_dir}")
            
            # List all .bak files in the directory
            try:
                all_files = os.listdir(local_backup_dir)
                bak_files = [f for f in all_files if f.endswith('.bak')]
                log(f"Found {len(bak_files)} .bak file(s) in directory:")
                for f in bak_files[-5:]:
                    full_path = local_backup_dir / f
                    try:
                        size_mb = full_path.stat().st_size / (1024 * 1024)
                        log(f"  - {f} ({size_mb:.2f} MB)")
                    except:
                        log(f"  - {f}")
            except Exception as e:
                log(f"Could not list directory: {e}")
            
            if not local_file.exists():
                # Query SQL Server backup history to see where it actually wrote the file
                log("")
                log("File not found at expected location. Querying SQL Server backup history...")
                try:
                    conn_verify = pyodbc.connect(conn_str, autocommit=True, timeout=30)
                    cur_verify = conn_verify.cursor()
                    cur_verify.execute("""
                        SELECT TOP 1 
                            physical_device_name,
                            backup_finish_date
                        FROM msdb.dbo.backupset bs
                        JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
                        WHERE database_name = ?
                        ORDER BY backup_finish_date DESC
                    """, (database,))
                    
                    recent_backup = cur_verify.fetchone()
                    cur_verify.close()
                    conn_verify.close()
                    
                    if recent_backup:
                        actual_path = recent_backup[0]
                        backup_time = recent_backup[1]
                        log(f"SQL Server reports last backup was written to:")
                        log(f"  {actual_path}")
                        log(f"  at {backup_time}")
                        
                        # Check if that file exists
                        actual_file = Path(actual_path)
                        if actual_file.exists():
                            log(f"[OK] Found backup file at SQL Server's reported location!")
                            local_file = actual_file
                            result["local_file"] = str(local_file)
                        else:
                            log(f"[X] File also not found at SQL Server's reported location")
                except Exception as e:
                    log(f"Could not query backup history: {e}")
                
                if not local_file.exists():
                    raise FileNotFoundError(
                        f"Backup file not found: {local_file}\n\n"
                        f"SQL Server reported success, but file is not accessible.\n"
                        f"Check SQL Server's error log and the backup history above."
                    )
            
            file_size_mb = local_file.stat().st_size / (1024 * 1024)
            log(f"✓ Local backup completed in {backup_elapsed:.1f}s")
            log(f"  File: {local_file}")
            log(f"  Size: {file_size_mb:.1f} MB")
            result["file_size_mb"] = round(file_size_mb, 2)
        
        # Step 3: Upload to Azure Blob Storage (optional)
        blob_url = ""
        upload_elapsed = 0
        if skip_upload:
            log("")
            log("Skipping cloud upload (local-only mode)")
            result["success"] = True
            result["message"] = f"Local backup completed successfully in {backup_elapsed:.1f}s"
        else:
            # Check if we can access the file for upload (network paths may not be accessible)
            if is_network_path and not file_accessible_for_upload:
                log("")
                log("WARNING: Backup created on network path (UNC share)")
                log("Python cannot access this network file from the app host, so upload cannot continue.")
                log("Options:")
                log("  1. Run app on a host that can read the share")
                log("  2. Use a shared path both SQL Server and app host can access")
                log("  3. Manually upload .bak file from network share to cloud")
                log("")
                result["success"] = True
                result["message"] = f"Local backup completed in {backup_elapsed:.1f}s (upload skipped - network path)"
            else:
                log("")
                log(f"Uploading to Azure Blob Storage...")
                log(f"  Container: {blob_container}")
                log(f"  Auth mode: {blob_auth_mode}")

                blob_path = build_blob_upload_path(
                    local_filename=backup_filename,
                    blob_folder=blob_folder,
                    database=database,
                    run_id=run_id,
                    structured_layout=structured_blob_paths,
                )
                log(f"  Blob path: {blob_container}/{blob_path}")
                
                upload_start = time.time()
                
                if blob_auth_mode == "managed_identity":
                    blob_url = _upload_with_managed_identity(
                        local_file=local_file,
                        account_url=blob_account_url,
                        container=blob_container,
                        blob_path=blob_path,
                        log=log,
                    )
                else:
                    blob_url = _upload_with_connection_string(
                        local_file=local_file,
                        connection_string=blob_connection_string,
                        container=blob_container,
                        blob_path=blob_path,
                        log=log,
                    )
                
                upload_elapsed = time.time() - upload_start
                result["upload_time_sec"] = round(upload_elapsed, 2)
                result["blob_url"] = blob_url
                
                log(f"✓ Upload completed in {upload_elapsed:.1f}s")
                log(f"  Blob URL: {blob_url}")
                
                # Step 4: Optionally delete local file
                if delete_local_after_upload:
                    try:
                        local_file.unlink()
                        log(f"✓ Deleted local file: {local_file}")
                    except Exception as e:
                        log(f"Warning: Could not delete local file: {e}")
                
                result["success"] = True
                result["message"] = f"Backup and upload completed successfully in {backup_elapsed + upload_elapsed:.1f}s"
        
        log("")
        log("=" * 60)
        log("Summary:")
        log(f"  Backup time: {backup_elapsed:.1f}s")
        if not skip_upload:
            log(f"  Upload time: {upload_elapsed:.1f}s")
            log(f"  Total time: {backup_elapsed + upload_elapsed:.1f}s")
        else:
            log(f"  Total time: {backup_elapsed:.1f}s")
        log(f"  Local file: {local_file}")
        if blob_url:
            log(f"  Blob URL: {blob_url}")
        log("=" * 60)
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        log(f"ERROR: {error_msg}")
        result["message"] = redact_sensitive_text(error_msg)
        return result


def upload_existing_bak_to_blob(
    *,
    local_file_path: str,
    blob_auth_mode: str = "connection_string",
    blob_connection_string: str = "",
    blob_account_url: str = "",
    blob_container: str = "",
    blob_folder: str = "",
    database: str = "",
    run_id: str = "",
    structured_blob_paths: bool = True,
    delete_local_after_upload: bool = False,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Upload an EXISTING .bak file (local or an accessible UNC path) to Azure Blob Storage.

    Unlike run_local_backup_and_upload, this does NOT run a backup — it just uploads a file
    that already exists (e.g. one created by 'Create Local Backup', a DBA, or a SQL Agent job).

    Returns dict: success, local_file, blob_url, upload_time_sec, file_size_mb, message.
    """
    if log is None:
        log = logger.info
    try:
        from ..utils.redact_secrets import redact_sensitive_text
    except ImportError:
        try:
            from src.utils.redact_secrets import redact_sensitive_text
        except ImportError:
            def redact_sensitive_text(t: str) -> str:  # type: ignore[misc]
                return t

    _emit = log

    def log(msg: str) -> None:  # noqa: F811 - intentional local shadow to redact
        _emit(redact_sensitive_text(str(msg)))

    result: Dict[str, Any] = {
        "success": False,
        "local_file": local_file_path,
        "blob_url": "",
        "upload_time_sec": 0,
        "file_size_mb": 0,
        "message": "",
    }
    try:
        raw = normalize_backup_input_path(local_file_path)
        if not raw:
            raise ValueError("No file selected to upload.")
        if not raw.lower().endswith(".bak"):
            raise ValueError("The file to upload must be a .bak file.")
        local_file = Path(raw)
        result["local_file"] = str(local_file)

        is_network = raw.startswith("\\\\") or raw.startswith("//")
        try:
            if not local_file.exists():
                raise FileNotFoundError(
                    f"File not found or not accessible from this PC: {local_file}"
                    + ("\n(UNC path — run the app on a host that can read the share.)" if is_network else "")
                )
            size_mb = local_file.stat().st_size / (1024 * 1024)
            result["file_size_mb"] = round(size_mb, 2)
        except OSError as e:
            raise RuntimeError(f"Cannot read the file for upload: {e}")

        if not blob_container:
            raise ValueError("Container name is required.")

        log(f"Uploading existing backup file: {local_file} ({size_mb:.1f} MB)")
        log(f"  Container: {blob_container}")
        log(f"  Auth mode: {blob_auth_mode}")

        blob_path = build_blob_upload_path(
            local_filename=local_file.name,
            blob_folder=blob_folder,
            database=database,
            run_id=run_id,
            structured_layout=structured_blob_paths,
        )
        log(f"  Blob path: {blob_container}/{blob_path}")

        upload_start = time.time()
        if blob_auth_mode == "managed_identity":
            if not blob_account_url:
                raise ValueError("Storage account URL is required for Managed Identity mode.")
            blob_url = _upload_with_managed_identity(
                local_file=local_file,
                account_url=blob_account_url,
                container=blob_container,
                blob_path=blob_path,
                log=log,
            )
        else:
            if not blob_connection_string:
                raise ValueError("Blob connection string is required for connection-string mode.")
            blob_url = _upload_with_connection_string(
                local_file=local_file,
                connection_string=blob_connection_string,
                container=blob_container,
                blob_path=blob_path,
                log=log,
            )
        upload_elapsed = time.time() - upload_start
        result["upload_time_sec"] = round(upload_elapsed, 2)
        result["blob_url"] = blob_url
        log(f"✓ Upload completed in {upload_elapsed:.1f}s")
        log(f"  Blob URL: {blob_url}")

        if delete_local_after_upload:
            try:
                local_file.unlink()
                log(f"✓ Deleted local file: {local_file}")
            except Exception as e:
                log(f"Warning: Could not delete local file: {e}")

        result["success"] = True
        result["message"] = f"Uploaded to blob in {upload_elapsed:.1f}s"
        return result
    except Exception as e:
        error_msg = str(e)
        log(f"ERROR: {error_msg}")
        hint = _diagnose_blob_upload_error(error_msg, blob_auth_mode)
        if hint:
            log(hint)
            error_msg = f"{error_msg}{hint}"
        result["message"] = redact_sensitive_text(error_msg)
        return result


def _diagnose_blob_upload_error(err_text: str, blob_auth_mode: str) -> str:
    """Append user-facing hints for common Azure blob upload auth failures."""
    try:
        from .bak_to_blob import _diagnose_precheck_sdk_error
    except ImportError:
        try:
            from src.backup.bak_to_blob import _diagnose_precheck_sdk_error
        except ImportError:
            return ""
    return _diagnose_precheck_sdk_error(err_text, blob_auth_mode)


def _parse_storage_account_url_for_upload(storage_account_url: str, container: str) -> str:
    """Normalize storage account URL to account root (same rules as .bak to Blob tab)."""
    try:
        from .bak_to_blob import _parse_storage_account_url
    except ImportError:
        from src.backup.bak_to_blob import _parse_storage_account_url
    account_root, _ = _parse_storage_account_url(storage_account_url or "", container or "")
    return account_root


def _connection_string_uses_account_key(connection_string: str) -> bool:
    """True when the connection string includes a storage account key (full write access)."""
    lower = (connection_string or "").lower()
    return "accountkey=" in lower


def _get_tool_blob_service_client(
    *,
    blob_auth_mode: str,
    blob_connection_string: str,
    blob_account_url: str,
    container: str,
    log: Callable[[str], None],
):
    """
    BlobServiceClient for Python-side uploads from this PC.

    Uses the same auth patterns as Browse Azure / the rest of the app:
      * connection_string — storage account key (from Browse Azure or portal)
      * managed_identity — shared Azure AD credential (az login / browser cache),
        NOT host-only VM MI (listing blobs can work with Reader; upload needs Contributor)
    """
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        raise ImportError(
            "azure-storage-blob required for blob upload.\n"
            "Install: pip install azure-storage-blob"
        )

    if blob_auth_mode == "managed_identity":
        if not blob_account_url:
            raise ValueError("Storage account URL is required for Managed Identity mode.")
        account_url = _parse_storage_account_url_for_upload(blob_account_url, container)
        try:
            from utils.azure_shared_credential import get_shared_azure_credential
        except ImportError:
            try:
                from azure_migration_tool.utils.azure_shared_credential import (
                    get_shared_azure_credential,
                )
            except ImportError:
                get_shared_azure_credential = None  # type: ignore[misc, assignment]
        if get_shared_azure_credential is None:
            raise ImportError(
                "azure-identity is required for Managed Identity upload.\n"
                "Install: pip install azure-identity"
            )
        log(
            "  Authenticating with Azure AD (shared credential — same chain as Browse Azure)..."
        )
        credential = get_shared_azure_credential(log)
        return BlobServiceClient(account_url=account_url, credential=credential)

    if not blob_connection_string or not blob_connection_string.strip():
        raise ValueError("Blob connection string is required for connection-string mode.")
    lower = blob_connection_string.lower()
    if "sharedaccesssignature=" in lower and not _connection_string_uses_account_key(
        blob_connection_string
    ):
        raise ValueError(
            "The connection string looks like a SAS-only string (read/list may work, upload often "
            "does not). Use Browse Azure in 'Connection String (storage account key)' mode, or paste "
            "the full AccountKey= connection string from the Azure portal."
        )
    log("  Authenticating with storage account key (connection string)...")
    return BlobServiceClient.from_connection_string(blob_connection_string)


def _upload_file_to_blob(
    *,
    local_file: Path,
    blob_auth_mode: str,
    blob_connection_string: str,
    blob_account_url: str,
    container: str,
    blob_path: str,
    log: Callable[[str], None],
) -> str:
    """Upload a local file to blob storage; return the blob URL."""
    blob_service = _get_tool_blob_service_client(
        blob_auth_mode=blob_auth_mode,
        blob_connection_string=blob_connection_string,
        blob_account_url=blob_account_url,
        container=container,
        log=log,
    )
    blob_client = blob_service.get_blob_client(container=container, blob=blob_path)
    log(f"  Uploading {local_file.name} to {container}/{blob_path}...")
    with open(local_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True, max_concurrency=4)
    account_url = str(blob_service.url).rstrip("/")
    return f"{account_url}/{container}/{blob_path}"


def _upload_with_managed_identity(
    local_file: Path,
    account_url: str,
    container: str,
    blob_path: str,
    log: Callable[[str], None],
) -> str:
    """Upload file to blob storage (Managed Identity / Azure AD mode)."""
    return _upload_file_to_blob(
        local_file=local_file,
        blob_auth_mode="managed_identity",
        blob_connection_string="",
        blob_account_url=account_url,
        container=container,
        blob_path=blob_path,
        log=log,
    )


def _upload_with_connection_string(
    local_file: Path,
    connection_string: str,
    container: str,
    blob_path: str,
    log: Callable[[str], None],
) -> str:
    """Upload file to blob storage using connection string (account key)."""
    return _upload_file_to_blob(
        local_file=local_file,
        blob_auth_mode="connection_string",
        blob_connection_string=connection_string,
        blob_account_url="",
        container=container,
        blob_path=blob_path,
        log=log,
    )
