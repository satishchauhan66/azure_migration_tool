# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Restore SQL Server database from Azure Blob Storage (.bak) via RESTORE DATABASE FROM URL.
Uses the same credential pattern as backup (container SAS); no WITH CREDENTIAL for SAS.
"""

import re
import time
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def _q(name: str) -> str:
    """Quote SQL identifier."""
    return "[" + name.replace("]", "]]") + "]"


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
    Restore a SQL Server database from a .bak file in Azure Blob (RESTORE DATABASE ... FROM URL).

    Args:
        server: Target SQL Server instance.
        database: Target database name (will be created or replaced).
        auth: windows | sql.
        user: For SQL auth; can be None for Windows.
        password: For SQL auth; can be None for Windows.
        blob_connection_string: Azure Storage connection string (AccountName=...;AccountKey=...).
        container: Blob container name (e.g. db2-stage).
        blob_path: Path within container to the .bak (e.g. SentimentAnalysis_QA/20260210_124044/SentimentAnalysis_QA.bak).
        log_callback: Optional callable(msg) for progress.
        target_managed_instance: If True, use RESTORE without REPLACE/STATS (required for Azure SQL Managed Instance).

    Returns:
        dict with status, error message if failed.
    """
    import pyodbc

    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            try:
                log_callback(msg)
            except Exception:
                pass

    result = {"status": "failed", "error": None}

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
            result["error"] = "Blob path must be set and end with .bak (e.g. DbName/run_id/DbName.bak)."
            return result

        log(f"Generating container SAS for credential (container={container})")
        sas_token, credential_name = _container_sas_and_url(
            account_name, account_key, container, endpoint_suffix=endpoint_suffix, expiry_hours=48
        )
        restore_url = f"https://{account_name}.blob.{endpoint_suffix}/{container}/{blob_path}"
        log(f"Restore URL: {restore_url}")

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
        conn.timeout = 600
        conn.autocommit = True
        cur = conn.cursor()

        cred_bracket = credential_name.replace("]", "]]")

        # Drop credential if exists, then create
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

        create_cred_sql = (
            f"CREATE CREDENTIAL [{cred_bracket}] "
            f"WITH IDENTITY = N'SHARED ACCESS SIGNATURE', "
            f"SECRET = N'{sas_token.replace(chr(39), chr(39)+chr(39))}'"
        )
        cur.execute(create_cred_sql)
        log("Credential created. Running RESTORE DATABASE ... FROM URL ...")

        # RESTORE FROM URL — no WITH CREDENTIAL (SAS credentials matched by URL).
        # Azure SQL Managed Instance does not support WITH REPLACE or STATS; on-prem supports them.
        if target_managed_instance:
            restore_sql = (
                f"RESTORE DATABASE {_q(database)} "
                f"FROM URL = N'{restore_url.replace(chr(39), chr(39)+chr(39))}'"
            )
        else:
            # On-prem: REPLACE overwrites if DB exists; STATS = 10 for progress.
            restore_sql = (
                f"RESTORE DATABASE {_q(database)} "
                f"FROM URL = N'{restore_url.replace(chr(39), chr(39)+chr(39))}' "
                f"WITH REPLACE, STATS = 10"
            )
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
