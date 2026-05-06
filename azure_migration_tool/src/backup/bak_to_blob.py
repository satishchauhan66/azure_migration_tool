# Author: Satish Ch@uhan

"""
On-prem SQL Server .bak backup to Azure Blob Storage (BACKUP TO URL).

Supports striped backups (multi-URL) for large databases. Each stripe is a
separate block blob; SQL Server writes them in parallel which both:
  * avoids the per-blob block-count limit (~50,000 blocks * MAXTRANSFERSIZE),
    which is the usual cause of error 3203 / 3013 / 1117 on big databases, and
  * improves throughput by using multiple network streams.

Layout in container:
    container / db_name / run_id / db_name.bak                  (single)
    container / db_name / run_id / db_name_part01of04.bak       (striped)
    container / db_name / run_id / db_name_part02of04.bak
    ...
"""

import re
import time
import logging
import math
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

from ..utils.paths import utc_ts_compact

logger = logging.getLogger(__name__)


# Recommended for BACKUP TO URL with block blobs (per Microsoft docs):
#   * 4 MB transfer size keeps block count low while still streaming efficiently
#   * 50,000 blocks per blob -> 4 MB * 50,000 = ~200 GB max per stripe
DEFAULT_MAX_TRANSFER_SIZE = 4 * 1024 * 1024     # 4 MB
DEFAULT_BLOCK_SIZE = 65536                       # 64 KB

# Stripe auto-sizing thresholds (compressed-ish, MB)
# Each stripe target ~150 GB so we stay comfortably below the 200 GB ceiling
_STRIPE_TARGET_GB = 150


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
    Generate container-level SAS for SQL Server credential and the credential name.
    Returns (sas_token, credential_name).
    credential_name is the URL prefix used in CREATE CREDENTIAL; SECRET = sas_token (no leading ?).
    """
    try:
        from azure.storage.blob import generate_container_sas, ContainerSasPermissions
    except ImportError:
        raise ImportError("azure-storage-blob is required for .bak to blob. Install: pip install azure-storage-blob")

    sas_token = generate_container_sas(
        account_name=account_name,
        container_name=container,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True, write=True, list=True, delete=True),
        expiry=datetime.utcnow() + timedelta(hours=expiry_hours),
    )
    credential_name = f"https://{account_name}.blob.{endpoint_suffix}/{container}"
    return sas_token, credential_name


def _get_database_size_mb(cur, database: str) -> Optional[float]:
    """Return total data+log size of the database in MB, or None if it can't be read."""
    try:
        cur.execute(
            """
            SELECT CAST(SUM(CAST(size AS BIGINT)) * 8.0 / 1024.0 AS FLOAT) AS size_mb
            FROM sys.master_files
            WHERE database_id = DB_ID(?)
            """,
            (database,),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return None


def _recommend_stripes(size_mb: Optional[float]) -> int:
    """Pick a sensible stripe count based on database size."""
    if size_mb is None or size_mb <= 0:
        return 1
    size_gb = size_mb / 1024.0
    if size_gb < 50:
        return 1
    # Aim for roughly _STRIPE_TARGET_GB per stripe; clamp to power-of-two-ish values
    n = max(1, math.ceil(size_gb / _STRIPE_TARGET_GB))
    if n <= 1:
        return 1
    if n <= 2:
        return 2
    if n <= 4:
        return 4
    if n <= 8:
        return 8
    if n <= 16:
        return 16
    return 32  # SQL Server supports up to 64 URLs


def _build_stripe_paths(safe_db: str, run_id: str, stripes: int) -> List[str]:
    """Return the blob paths (no scheme/host) for each stripe."""
    if stripes <= 1:
        return [f"{safe_db}/{run_id}/{safe_db}.bak"]
    width = max(2, len(str(stripes)))
    return [
        f"{safe_db}/{run_id}/{safe_db}_part{str(i + 1).zfill(width)}of{str(stripes).zfill(width)}.bak"
        for i in range(stripes)
    ]


def _q(name: str) -> str:
    """Quote SQL identifier."""
    return "[" + name.replace("]", "]]") + "]"


def _esc_sql(s: str) -> str:
    """Escape a single-quoted SQL literal."""
    return s.replace("'", "''")


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
    stripes: Optional[int] = None,
    max_transfer_size: int = DEFAULT_MAX_TRANSFER_SIZE,
    block_size: int = DEFAULT_BLOCK_SIZE,
) -> Dict[str, Any]:
    """
    Backup on-prem SQL Server database to Azure Blob as one or more .bak stripes
    (BACKUP DATABASE ... TO URL = N'...' [, URL = N'...' ...] ).

    Args:
        server: On-prem SQL Server instance.
        database: Database name.
        auth: windows | sql.
        user/password: For SQL auth; None for Windows.
        blob_connection_string: Azure Storage connection string (AccountName=...;AccountKey=...).
        container: Blob container name (default db2-stage).
        run_id: Optional run id (default: YYYYMMDD_HHMMSS).
        log_callback: Optional callable(msg) for progress.
        stripes: Number of parallel blob stripes (1..64). None or 0 = auto-pick from DB size.
        max_transfer_size: BACKUP MAXTRANSFERSIZE in bytes (default 4 MB).
        block_size: BACKUP BLOCKSIZE in bytes (default 64 KB).

    Returns:
        dict with status, run_id, blob_paths (list), stripes, error, etc.
    """
    import pyodbc  # noqa: F401  (driver init handled elsewhere)

    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            try:
                log_callback(msg)
            except Exception:
                pass

    result: Dict[str, Any] = {
        "status": "failed",
        "run_id": run_id or utc_ts_compact(),
        "blob_path": None,
        "blob_paths": [],
        "stripes": stripes or 0,
        "server": server,
        "database": database,
        "container": container,
        "error": None,
    }
    run_id = result["run_id"]
    safe_db = re.sub(r"[^a-zA-Z0-9._-]+", "_", database)[:128]

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

        # Connect to source (master is required for BACKUP and CREATE CREDENTIAL)
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
        # Long timeout for BACKUP — striping helps but very large DBs still take a while
        conn.timeout = 7200
        conn.autocommit = True
        cur = conn.cursor()

        # Auto-pick stripe count if not provided
        if not stripes or stripes <= 0:
            size_mb = _get_database_size_mb(cur, database)
            stripes = _recommend_stripes(size_mb)
            if size_mb is not None:
                log(f"Database size ~ {size_mb / 1024.0:.1f} GB -> using {stripes} stripe(s)")
            else:
                log(f"Database size unknown -> using {stripes} stripe(s)")
        else:
            stripes = max(1, min(64, int(stripes)))
            log(f"Using {stripes} stripe(s) (user-specified)")
        result["stripes"] = stripes

        blob_paths = _build_stripe_paths(safe_db, run_id, stripes)
        result["blob_paths"] = blob_paths
        result["blob_path"] = blob_paths[0]  # backwards-compat: first stripe / single file

        backup_urls = [
            f"https://{account_name}.blob.{endpoint_suffix}/{container}/{p}" for p in blob_paths
        ]
        for u in backup_urls:
            log(f"BACKUP TO URL = {u}")

        cred_bracket = credential_name.replace("]", "]]")

        # Drop credential if it already exists, then recreate (DDL: literal SQL only)
        log("Creating SQL Server credential for blob container...")
        drop_sql = (
            "IF EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'"
            + _esc_sql(credential_name)
            + "') DROP CREDENTIAL ["
            + cred_bracket
            + "]"
        )
        try:
            cur.execute(drop_sql)
        except Exception:
            pass

        create_cred_sql = (
            f"CREATE CREDENTIAL [{cred_bracket}] "
            f"WITH IDENTITY = N'SHARED ACCESS SIGNATURE', "
            f"SECRET = N'{_esc_sql(sas_token)}'"
        )
        cur.execute(create_cred_sql)
        log("Credential created. Running BACKUP DATABASE ... TO URL ...")

        # Build the multi-URL BACKUP statement.
        # MAXTRANSFERSIZE + BLOCKSIZE keep block usage low and avoid the
        # ~50,000-block-per-blob ceiling that triggers I/O error 3203/1117.
        url_clauses = ", ".join(f"URL = N'{_esc_sql(u)}'" for u in backup_urls)
        backup_name_esc = _esc_sql(f"{database}-Full Database Backup")
        backup_sql = (
            f"BACKUP DATABASE {_q(database)} "
            f"TO {url_clauses} "
            f"WITH NOFORMAT, COMPRESSION, NOINIT, "
            f"NAME = N'{backup_name_esc}', "
            f"NOSKIP, NOREWIND, NOUNLOAD, "
            f"MAXTRANSFERSIZE = {int(max_transfer_size)}, "
            f"BLOCKSIZE = {int(block_size)}, "
            f"STATS = 5"
        )

        t0 = time.perf_counter()
        cur.execute(backup_sql)
        # BACKUP returns multiple result sets (progress messages). Drain all of them
        # or the driver may return before Azure has actually committed the blobs.
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

        # Confirm the backup set in msdb (best-effort)
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

        # Verify each stripe blob is present and non-zero. SQL may return slightly
        # before Azure commits the final block list; retry briefly.
        try:
            from azure.storage.blob import BlobServiceClient
            container_client = BlobServiceClient.from_connection_string(
                blob_connection_string
            ).get_container_client(container)

            sizes: List[Tuple[str, Optional[int]]] = []
            for path in blob_paths:
                blob_client = container_client.get_blob_client(path)
                size_val: Optional[int] = None
                total_waited = 0
                max_wait_sec = 90
                while total_waited < max_wait_sec:
                    if total_waited > 0:
                        log(f"Waiting for {path} to be committed ({total_waited}s)...")
                    time.sleep(5)
                    total_waited += 5
                    try:
                        props = blob_client.get_blob_properties()
                        size_val = props.size
                    except Exception as e:
                        log(f"  blob not visible yet: {e}")
                        size_val = None
                    if size_val and size_val > 0:
                        break
                sizes.append((path, size_val))

            total = 0
            failed_any = False
            for path, sz in sizes:
                if sz is None:
                    log(f"WARN: could not read size for {path}")
                    failed_any = True
                elif sz == 0:
                    log(f"FAIL: {path} is 0 bytes after waiting")
                    failed_any = True
                else:
                    total += sz
                    log(f"OK:   {path} = {sz / (1024 * 1024):.1f} MB")

            result["blob_size"] = total or None
            if failed_any:
                result["status"] = "failed"
                result["error"] = (
                    "One or more stripe blobs are missing or 0 bytes after waiting. "
                    "The backup may have failed; check SQL Server error log."
                )
                log(result["error"])
                return result

            log(f"Backup completed. Total {len(blob_paths)} stripe(s), {total / (1024 * 1024):.1f} MB total.")
        except Exception as e:
            log(f"Could not verify blob sizes: {e}")

        result["status"] = "success"
        return result
    except Exception as e:
        result["error"] = str(e)
        log(f"Backup failed: {e}")
        return result
