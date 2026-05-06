# Author: Satish Ch@uhan

"""
Restore SQL Server database from Azure Blob Storage (.bak) via RESTORE DATABASE FROM URL.

Handles both single-file and striped backups. If the selected blob path matches
the striped naming convention `<db>_partNNofMM.bak`, all sibling stripes in the
same folder are auto-discovered and used in the RESTORE statement.

Uses the same credential pattern as backup (container SAS); no WITH CREDENTIAL for SAS.
"""

import re
import time
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)


_STRIPE_RE = re.compile(r"_part(\d+)of(\d+)\.bak$", re.IGNORECASE)


def _q(name: str) -> str:
    """Quote SQL identifier."""
    return "[" + name.replace("]", "]]") + "]"


def _esc_sql(s: str) -> str:
    return s.replace("'", "''")


def _discover_stripe_set(
    blob_connection_string: str,
    container: str,
    blob_path: str,
    log: Optional[Any] = None,
) -> List[str]:
    """
    Given any blob path that ends in .bak, return the full ordered list of
    stripe blob paths. For a single file the result is just `[blob_path]`.

    Striped naming: <prefix>_partNNofMM.bak (zero-padded). Sibling stripes
    must live in the same folder.
    """
    def _say(msg: str) -> None:
        if log:
            try:
                log(msg)
            except Exception:
                pass

    blob_path = blob_path.replace("\\", "/").lstrip("/")
    folder, fname = blob_path.rsplit("/", 1) if "/" in blob_path else ("", blob_path)

    # IMPORTANT: regex is matched against the file name (not the full path),
    # otherwise m.start() is an offset into the path and `fname[: m.start()]`
    # silently returns the entire filename (Python clamps slice indices), which
    # makes the list-by-prefix search match nothing and only one stripe ends
    # up being passed to RESTORE.
    m = _STRIPE_RE.search(fname)
    if not m:
        return [blob_path]

    total = int(m.group(2))
    prefix = fname[: m.start()]  # everything in the filename before "_partNN..."

    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        return [blob_path]

    client = BlobServiceClient.from_connection_string(blob_connection_string)
    container_client = client.get_container_client(container)
    list_prefix = (folder + "/" if folder else "") + prefix + "_part"

    found: Dict[int, str] = {}
    for b in container_client.list_blobs(name_starts_with=list_prefix):
        # Match against just the filename portion of the listed blob too.
        bname = b.name.rsplit("/", 1)[-1]
        mm = _STRIPE_RE.search(bname)
        if not mm:
            continue
        if int(mm.group(2)) != total:
            continue
        found[int(mm.group(1))] = b.name

    if len(found) != total:
        ordered = [found[k] for k in sorted(found)]
        _say(
            f"Stripe discovery found {len(found)} of {total} expected stripes "
            f"under prefix '{list_prefix}'. RESTORE will fail unless all stripes are present."
        )
        # Return what we have so RESTORE produces a clear "media family missing" error
        # rather than us silently restoring from the selected stripe alone.
        return ordered or [blob_path]

    return [found[i] for i in range(1, total + 1)]


def run_restore_from_blob(
    server: str,
    database: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
    blob_connection_string: str,
    container: str,
    blob_path: str,
    log_callback: Optional[Any] = None,
    target_managed_instance: bool = False,
) -> Dict[str, Any]:
    """
    Restore a SQL Server database from one or more .bak stripes in Azure Blob.

    Args:
        server: Target SQL Server instance.
        database: Target database name (will be created or replaced).
        auth: windows | sql.
        user/password: For SQL auth; None for Windows.
        blob_connection_string: Azure Storage connection string (AccountName=...;AccountKey=...).
        container: Blob container name (e.g. db2-stage).
        blob_path: Path within container to the .bak. May be the single file or any
            one stripe of a striped set; sibling stripes are auto-discovered.
        log_callback: Optional callable(msg) for progress.
        target_managed_instance: If True, omit REPLACE/STATS (required for Azure SQL MI).

    Returns:
        dict with status, error message if failed.
    """
    import pyodbc  # noqa: F401

    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            try:
                log_callback(msg)
            except Exception:
                pass

    result: Dict[str, Any] = {"status": "failed", "error": None}

    try:
        from ..backup.bak_to_blob import _parse_storage_connection_string, _container_sas_and_url
    except ImportError:
        try:
            from src.backup.bak_to_blob import _parse_storage_connection_string, _container_sas_and_url
        except ImportError:
            result["error"] = "Backup module not available (needed for credential/SAS)."
            return result

    try:
        parts = _parse_storage_connection_string(blob_connection_string)
        account_name = parts.get("accountname", "")
        account_key = parts.get("accountkey", "")
        endpoint_suffix = parts.get("endpointsuffix", "core.windows.net")
        if not account_name or not account_key:
            result["error"] = "Connection string missing AccountName or AccountKey."
            return result

        blob_path = (blob_path or "").strip().replace("\\", "/").lstrip("/")
        if not blob_path or not blob_path.endswith(".bak"):
            result["error"] = "Blob path must be set and end with .bak."
            return result

        # Discover stripe set (returns a single-element list for non-striped backups)
        stripe_paths = _discover_stripe_set(blob_connection_string, container, blob_path, log=log)
        if len(stripe_paths) > 1:
            log(f"Detected striped backup: {len(stripe_paths)} stripe(s)")
        else:
            log("Single-file backup (no stripes detected)")
        result["stripes"] = len(stripe_paths)

        log(f"Generating container SAS for credential (container={container})")
        sas_token, credential_name = _container_sas_and_url(
            account_name, account_key, container, endpoint_suffix=endpoint_suffix, expiry_hours=48
        )
        restore_urls = [
            f"https://{account_name}.blob.{endpoint_suffix}/{container}/{p}" for p in stripe_paths
        ]
        for u in restore_urls:
            log(f"Restore URL: {u}")

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
        conn.timeout = 7200
        conn.autocommit = True
        cur = conn.cursor()

        cred_bracket = credential_name.replace("]", "]]")

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
        log("Credential created. Running RESTORE DATABASE ... FROM URL ...")

        url_clauses = ", ".join(f"URL = N'{_esc_sql(u)}'" for u in restore_urls)
        if target_managed_instance:
            restore_sql = f"RESTORE DATABASE {_q(database)} FROM {url_clauses}"
        else:
            restore_sql = f"RESTORE DATABASE {_q(database)} FROM {url_clauses} WITH REPLACE, STATS = 5"

        t0 = time.perf_counter()
        cur.execute(restore_sql)
        while True:
            try:
                for _ in cur.fetchall():
                    pass
            except Exception:
                pass
            if not cur.nextset():
                break
        elapsed = time.perf_counter() - t0
        log(f"RESTORE command completed in {elapsed:.1f} s")

        cur.close()
        conn.close()
        result["status"] = "success"
        log(f"Restore completed. Database: {database}")
        return result
    except Exception as e:
        result["error"] = str(e)
        log(f"Restore failed: {e}")
        return result
