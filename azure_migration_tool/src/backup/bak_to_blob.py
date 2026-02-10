# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
On-prem SQL Server .bak backup to Azure Blob Storage (BACKUP TO URL).
Uses BACKUP DATABASE ... TO URL so the backup is written directly to blob.
Folder structure: container / db_name / run_id / db_name.bak (readable, mirror-like).
"""

import re
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

from ..utils.paths import utc_ts_compact

logger = logging.getLogger(__name__)


def _parse_storage_connection_string(conn_str: str) -> Dict[str, str]:
    """Parse Azure Storage connection string into parts. Returns dict with AccountName, AccountKey, EndpointSuffix (optional)."""
    conn_str = (conn_str or "").strip()
    if not conn_str:
        raise ValueError("Blob connection string is required.")
    parts = {}
    for segment in conn_str.split(";"):
        segment = segment.strip()
        if not segment:
            continue
        if "=" in segment:
            k, v = segment.split("=", 1)
            parts[k.strip().lower()] = v.strip()
    if "accountname" not in parts:
        raise ValueError("Connection string must include AccountName=...")
    if "accountkey" not in parts:
        raise ValueError("Connection string must include AccountKey=... (needed for SAS)")
    return parts


def _container_sas_and_url(
    account_name: str,
    account_key: str,
    container: str,
    endpoint_suffix: str = "core.windows.net",
    expiry_hours: int = 48,
):
    """
    Generate container-level SAS for SQL Server credential and the backup URL (no SAS in URL).
    Returns (backup_base_url, blob_path, sas_token, credential_name).
    credential_name must be used in CREATE CREDENTIAL; SECRET = sas_token (no leading ?).
    """
    try:
        from azure.storage.blob import generate_container_sas, ContainerSasPermissions
    except ImportError:
        raise ImportError("azure-storage-blob is required for .bak to blob. Install: pip install azure-storage-blob")

    sas_token = generate_container_sas(
        account_name=account_name,
        container_name=container,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True, write=True),
        expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
    )
    # Credential name must be exactly: https://account.blob.core.windows.net/container (no trailing slash)
    credential_name = f"https://{account_name}.blob.{endpoint_suffix}/{container}"
    return sas_token, credential_name


def run_bak_backup_to_blob(
    server: str,
    database: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
    blob_connection_string: str,
    container: str = "db2-stage",
    run_id: Optional[str] = None,
    log_callback: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Backup on-prem SQL Server database to Azure Blob as .bak (BACKUP DATABASE ... TO URL).

    Args:
        server: On-prem SQL Server instance (e.g. db-sentimentanalysis-test\\SurveySolutions).
        database: Database name (e.g. SentimentAnalysis_QA).
        auth: windows | sql (Windows recommended for on-prem).
        user: For SQL auth; can be None for Windows.
        password: For SQL auth; can be None for Windows.
        blob_connection_string: Azure Storage connection string (AccountName=...;AccountKey=...).
        container: Blob container name (default db2-stage).
        run_id: Optional run id (default: YYYYMMDD_HHMMSS).
        log_callback: Optional callable(msg) for progress.

    Returns:
        dict with status, run_id, blob_path, url (masked), error message if failed.
    """
    import pyodbc

    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            try:
                log_callback(msg)
            except Exception:
                pass

    result = {
        "status": "failed",
        "run_id": run_id or utc_ts_compact(),
        "blob_path": None,
        "server": server,
        "database": database,
        "container": container,
        "error": None,
    }

    run_id = result["run_id"]
    # Folder structure: db_name / run_id / db_name.bak (readable)
    safe_db = re.sub(r"[^a-zA-Z0-9._-]+", "_", database)[:128]
    blob_path = f"{safe_db}/{run_id}/{safe_db}.bak"
    result["blob_path"] = blob_path

    try:
        parts = _parse_storage_connection_string(blob_connection_string)
        account_name = parts.get("accountname", "")
        account_key = parts.get("accountkey", "")
        endpoint_suffix = parts.get("endpointsuffix", "core.windows.net")
        if not account_name or not account_key:
            result["error"] = "Connection string missing AccountName or AccountKey."
            return result

        log(f"Generating container SAS for credential (container={container})")
        sas_token, credential_name = _container_sas_and_url(
            account_name, account_key, container, endpoint_suffix=endpoint_suffix, expiry_hours=48
        )
        # URL without SAS - SQL Server uses the credential to authenticate
        backup_url = f"https://{account_name}.blob.{endpoint_suffix}/{container}/{blob_path}"
        log(f"Backup URL prepared (container={container}, path={blob_path}); using SQL credential")

        # Connect to on-prem (master for BACKUP and CREATE CREDENTIAL)
        try:
            from ..utils.database import connect_to_database, pick_sql_driver
        except ImportError:
            try:
                from src.utils.database import connect_to_database, pick_sql_driver
            except ImportError:
                from utils.database import connect_to_database, pick_sql_driver
        driver = pick_sql_driver(logger)
        conn = connect_to_database(
            server=server,
            db="master",
            user=user or "",
            driver=driver,
            auth=auth or "windows",
            password=password,
            timeout=120,
            logger=logger,
        )
        # Long timeout for BACKUP (e.g. 700 MB can take several minutes)
        conn.timeout = 600
        conn.autocommit = True
        cur = conn.cursor()

        # All SQL is built as literal strings (no ODBC ? params) because
        # DDL (CREATE/DROP CREDENTIAL) and BACKUP don't support parameterised queries.
        cred_bracket = credential_name.replace("]", "]]")

        # Drop credential if it already exists
        log("Creating SQL Server credential for blob container...")
        drop_sql = (
            "IF EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'"
            + credential_name.replace("'", "''")
            + "') DROP CREDENTIAL ["
            + cred_bracket
            + "]"
        )
        try:
            cur.execute(drop_sql)
        except Exception:
            pass

        # Create credential (SAS-based: IDENTITY = 'SHARED ACCESS SIGNATURE')
        create_cred_sql = (
            f"CREATE CREDENTIAL [{cred_bracket}] "
            f"WITH IDENTITY = N'SHARED ACCESS SIGNATURE', "
            f"SECRET = N'{sas_token.replace(chr(39), chr(39)+chr(39))}'"
        )
        cur.execute(create_cred_sql)
        log("Credential created. Running BACKUP DATABASE ... TO URL ...")
        log(f"BACKUP TO URL = {backup_url}")

        # BACKUP TO URL — no WITH CREDENTIAL (SAS credentials are matched by URL automatically).
        # Options aligned with working SSMS example: NOFORMAT, NOINIT, NAME, NOSKIP, NOREWIND, NOUNLOAD.
        backup_name_esc = f"{database}-Full Database Backup".replace("'", "''")
        backup_sql = (
            f"BACKUP DATABASE {_q(database)} "
            f"TO URL = N'{backup_url.replace(chr(39), chr(39)+chr(39))}' "
            f"WITH NOFORMAT, COMPRESSION, NOINIT, "
            f"NAME = N'{backup_name_esc}', NOSKIP, NOREWIND, NOUNLOAD, STATS = 10"
        )
        t0 = time.perf_counter()
        cur.execute(backup_sql)
        # BACKUP returns multiple result sets (progress messages). We must consume all of them
        # or the driver may return before the backup has actually finished (causing 0-byte blob).
        while True:
            try:
                for _ in cur.fetchall():
                    pass
            except Exception:
                pass
            if not cur.nextset():
                break
        elapsed = time.perf_counter() - t0
        log(f"BACKUP command completed in {elapsed:.1f} s")

        # Confirm SQL Server recorded the backup (msdb.dbo.backupset)
        try:
            cur.execute(
                "SELECT TOP 1 backup_finish_date, backup_size, type FROM msdb.dbo.backupset "
                "WHERE database_name = ? ORDER BY backup_finish_date DESC",
                (database,),
            )
            row = cur.fetchone()
            if row:
                finish_date, backup_size_msdb, backup_type = row
                size_mb_msdb = (backup_size_msdb or 0) / (1024 * 1024)
                log(f"SQL Server backup set: finish={finish_date}, size={size_mb_msdb:.1f} MB, type={backup_type}")
                if backup_size_msdb and backup_size_msdb > 0:
                    result["backup_size_msdb"] = backup_size_msdb
            else:
                log("No backup set found in msdb.dbo.backupset for this database.")
        except Exception as e:
            log(f"Could not read backup set: {e}")
        cur.close()
        conn.close()

        # Verify blob was written (not 0 bytes). Wait and retry: SQL Server may return
        # before Azure has committed the block list, so we can see 0 bytes if we check too soon.
        blob_size = None
        log(f"Verifying blob size: {container}/{blob_path}")
        try:
            from azure.storage.blob import BlobServiceClient
            blob_client = BlobServiceClient.from_connection_string(blob_connection_string)
            blob_client = blob_client.get_container_client(container).get_blob_client(blob_path)
            total_waited = 0
            max_wait_sec = 90
            while total_waited < max_wait_sec:
                if total_waited > 0:
                    log(f"Waiting for blob to be committed ({total_waited}s)...")
                time.sleep(5)
                total_waited += 5
                props = blob_client.get_blob_properties()
                blob_size = props.size
                if blob_size and blob_size > 0:
                    break
        except Exception as e:
            log(f"Could not verify blob size: {e}")
        if blob_size is not None:
            result["blob_size"] = blob_size
            if blob_size == 0:
                result["status"] = "failed"
                result["error"] = "Backup file in blob storage is 0 bytes after waiting. The backup may have failed; check SQL Server and network."
                log(result["error"])
                return result
            size_mb = blob_size / (1024 * 1024)
            log(f"Backup completed. Blob: {container}/{blob_path} ({size_mb:.1f} MB)")
        else:
            log(f"Backup completed. Blob: {container}/{blob_path}")

        result["status"] = "success"
        return result
    except Exception as e:
        result["error"] = str(e)
        log(f"Backup failed: {e}")
        return result


def _q(name: str) -> str:
    """Quote SQL identifier."""
    return "[" + name.replace("]", "]]") + "]"
