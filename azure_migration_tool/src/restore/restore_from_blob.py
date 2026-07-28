# Author: S@tish Ch@uhan

"""
Restore SQL Server database from Azure Blob Storage (.bak) via RESTORE DATABASE FROM URL.

Handles both single-file and striped backups. If the selected blob path matches
the striped naming convention `<db>_partNNofMM.bak`, all sibling stripes in the
same folder are auto-discovered and used in the RESTORE statement.

Uses the same credential pattern as backup (container SAS); no WITH CREDENTIAL for SAS.

Diagnostic helpers:
  * run_test_blob_sdk_read — Azure SDK get_blob_properties on **this PC** (DefaultAzureCredential).
  * run_test_blob_headeronly_via_sql_odbc — ODBC to SQL Server, CREATE CREDENTIAL, RESTORE HEADERONLY
    FROM URL (same blob access path as full RESTORE).
"""

import re
import time
import logging
from typing import Optional, Dict, Any, List, Tuple

try:
    from ..utils.redact_secrets import redact_sensitive_text
except ImportError:
    try:
        from src.utils.redact_secrets import redact_sensitive_text
    except ImportError:
        def redact_sensitive_text(t: str) -> str:  # type: ignore[misc]
            return t

logger = logging.getLogger(__name__)


_STRIPE_RE = re.compile(r"_part(\d+)of(\d+)\.bak$", re.IGNORECASE)


def _log_sql_engine_context_for_mi_blob(cur, server: str, log) -> None:
    """Log SQL Server version and Windows service account — helps explain MI vs laptop identity."""
    log("--- SQL Server host (identity used for Managed Identity blob access) ---")
    log(f"  Connected instance: {server}")
    try:
        cur.execute(
            """
            SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT),
                   CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(64)),
                   CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256)),
                   CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(128))
            """
        )
        row = cur.fetchone()
        if row:
            maj, ver, ed, mach = row[0], row[1], row[2], row[3]
            log(f"  ProductMajorVersion: {maj} (SQL Server 2022 = 16)")
            log(f"  ProductVersion / Edition: {ver} / {ed}")
            if mach:
                log(f"  MachineName (SERVERPROPERTY): {mach}")
    except Exception as ex:
        log(f"  (Could not read SERVERPROPERTY: {ex})")
    try:
        cur.execute(
            """
            SELECT servicename, service_account
            FROM sys.dm_server_services
            WHERE servicename LIKE N'SQL Server (%'
            """
        )
        for r in cur.fetchall() or []:
            log(f"  Windows service: {r[0]} → runs as: {r[1]}")
    except Exception as ex:
        log(f"  (Could not read sys.dm_server_services: {ex})")
    log(
        "  For RESTORE/BACKUP … TO/FROM URL with IDENTITY = 'Managed Identity', Azure Storage "
        "RBAC must be granted to the **managed identity of this host** (Azure VM / MI), not to "
        "your workstation user."
    )
    log("--- (end SQL host context) ---")


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
    blob_service_client=None,
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

    if blob_service_client is not None:
        client = blob_service_client
    elif blob_connection_string:
        client = BlobServiceClient.from_connection_string(blob_connection_string)
    else:
        return [blob_path]
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


def _import_bak_to_blob_helpers():
    """Import helpers from bak_to_blob (package-relative)."""
    try:
        from ..backup import bak_to_blob as _b
    except ImportError:
        try:
            from src.backup import bak_to_blob as _b
        except ImportError:
            from azure_migration_tool.src.backup import bak_to_blob as _b
    return _b


def _prepare_blob_restore_urls(
    *,
    blob_path: str,
    container: str,
    blob_connection_string: str,
    storage_account_url: str,
    blob_auth_mode: str,
    log,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Resolve account URL, container SAS or MI credential name, stripe paths, full HTTPS URLs.

    Returns:
        (error_dict, None) on validation / import failure — same shape as run_restore_from_blob result.
        (None, context) on success. context keys: acct_url, container, credential_name, sas_token,
        stripe_paths, restore_urls (list of full blob URLs).
    """
    fail = lambda msg: ({"status": "failed", "error": msg, "diagnostic": None}, None)

    try:
        _b = _import_bak_to_blob_helpers()
    except Exception as e:
        return fail(f"Backup module not available (needed for credential/SAS): {e}")

    _parse_storage_connection_string = _b._parse_storage_connection_string
    _container_sas_and_url = _b._container_sas_and_url
    _parse_storage_account_url = _b._parse_storage_account_url
    _get_mi_blob_service_client = _b._get_mi_blob_service_client
    normalize_storage_blob_account_url = _b.normalize_storage_blob_account_url

    blob_path = (blob_path or "").strip().replace("\\", "/").lstrip("/")
    container = (container or "").strip()
    if not blob_path or not blob_path.endswith(".bak"):
        return fail("Blob path must be set and end with .bak.")

    if blob_auth_mode == "managed_identity":
        if not storage_account_url:
            return fail(
                "Storage account URL is required for Managed Identity mode. "
                "Enter it in the format: https://myaccount.blob.core.windows.net"
            )
        _raw_u = (storage_account_url or "").strip().rstrip("/")
        acct_url, container = _parse_storage_account_url(storage_account_url, container)
        if normalize_storage_blob_account_url(_raw_u).rstrip("/") != _raw_u:
            log(
                "Adjusted storage URL to Azure Blob endpoint "
                f"(use .blob.core.windows.net): {acct_url}"
            )
        credential_name = f"{acct_url}/{container}"
        log(f"Using Managed Identity auth (credential = {credential_name})")
        sas_token = None
    else:
        if not container:
            return fail("Container name is required in Connection String mode.")
        parts = _parse_storage_connection_string(blob_connection_string)
        account_name = parts.get("accountname", "")
        account_key = parts.get("accountkey", "")
        endpoint_suffix = parts.get("endpointsuffix", "core.windows.net")
        if not account_name or not account_key:
            return fail("Connection string missing AccountName or AccountKey.")
        acct_url = f"https://{account_name}.blob.{endpoint_suffix}"
        log(f"Generating container SAS for credential (container={container})")
        sas_token, credential_name = _container_sas_and_url(
            account_name,
            account_key,
            container,
            endpoint_suffix=endpoint_suffix,
            expiry_hours=48,
        )

    if blob_auth_mode == "managed_identity":
        log(
            "Next: listing blobs / stripe detection uses **this PC’s** DefaultAzureCredential "
            "(your Azure AD user or this machine’s managed identity) — not SQL Server."
        )

    if blob_auth_mode == "managed_identity":
        _mi_client = _get_mi_blob_service_client(acct_url)
        stripe_paths = _discover_stripe_set(
            "", container, blob_path, log=log, blob_service_client=_mi_client
        )
    else:
        stripe_paths = _discover_stripe_set(
            blob_connection_string, container, blob_path, log=log
        )
    if len(stripe_paths) > 1:
        log(f"Detected striped backup: {len(stripe_paths)} stripe(s)")
    else:
        log("Single-file backup (no stripes detected)")

    restore_urls = [f"{acct_url}/{container}/{p}" for p in stripe_paths]
    for u in restore_urls:
        log(f"Restore URL: {u}")

    ctx: Dict[str, Any] = {
        "acct_url": acct_url,
        "container": container,
        "credential_name": credential_name,
        "sas_token": sas_token,
        "stripe_paths": stripe_paths,
        "restore_urls": restore_urls,
    }
    return None, ctx


def run_test_blob_sdk_read(
    *,
    blob_connection_string: str = "",
    container: str = "",
    blob_path: str = "",
    log_callback: Optional[Any] = None,
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
) -> Dict[str, Any]:
    """
    Read blob properties via Azure SDK on **this machine** (same credential path as list-backups).
    Does not prove SQL Server can read the blob.
    """
    result: Dict[str, Any] = {"status": "failed", "error": None}

    def log(msg: str) -> None:
        safe = redact_sensitive_text(str(msg))
        logger.info(safe)
        if log_callback:
            try:
                log_callback(safe)
            except Exception:
                pass

    try:
        _b = _import_bak_to_blob_helpers()
        _get_mi_blob_service_client = _b._get_mi_blob_service_client
    except Exception as e:
        result["error"] = str(e)
        return result

    prep_err, ctx = _prepare_blob_restore_urls(
        blob_path=blob_path,
        container=container,
        blob_connection_string=blob_connection_string,
        storage_account_url=storage_account_url,
        blob_auth_mode=blob_auth_mode,
        log=log,
    )
    if prep_err:
        return prep_err

    assert ctx is not None
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        result["error"] = "Install azure-storage-blob (pip install azure-storage-blob)."
        return result

    first_rel = ctx["stripe_paths"][0]
    log(f"SDK test: get_blob_properties for container={ctx['container']!r} blob={first_rel!r}")
    try:
        if blob_auth_mode == "managed_identity":
            svc = _get_mi_blob_service_client(ctx["acct_url"])
        else:
            svc = BlobServiceClient.from_connection_string(blob_connection_string)
        bc = svc.get_blob_client(ctx["container"], first_rel)
        p = bc.get_blob_properties()
        log(f"SDK OK: size_bytes={p.size}, etag={p.etag!r}, last_modified={p.last_modified}")
        result["status"] = "success"
        result["size_bytes"] = p.size
        return result
    except Exception as e:
        err = str(e)
        result["error"] = redact_sensitive_text(err)
        log(f"SDK blob read failed: {err}")
        return result


def run_test_blob_headeronly_via_sql_odbc(
    server: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
    blob_connection_string: str = "",
    container: str = "",
    blob_path: str = "",
    log_callback: Optional[Any] = None,
    target_managed_instance: bool = False,
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
) -> Dict[str, Any]:
    """
    Same path as RESTORE FROM URL: ODBC to SQL Server, CREATE CREDENTIAL, then
    RESTORE HEADERONLY FROM URL (read-only). Validates the **SQL host** can open the backup device.
    """
    import pyodbc  # noqa: F401

    def log(msg: str) -> None:
        safe = redact_sensitive_text(str(msg))
        logger.info(safe)
        if log_callback:
            try:
                log_callback(safe)
            except Exception:
                pass

    result: Dict[str, Any] = {"status": "failed", "error": None}

    try:
        _b = _import_bak_to_blob_helpers()
        _diagnose_backup_error = _b._diagnose_backup_error
        _mi_credential_sql = _b._mi_credential_sql
        _check_mi_backup_supported = _b._check_mi_backup_supported
    except Exception as e:
        result["error"] = f"Backup module not available: {e}"
        return result

    prep_err, ctx = _prepare_blob_restore_urls(
        blob_path=blob_path,
        container=container,
        blob_connection_string=blob_connection_string,
        storage_account_url=storage_account_url,
        blob_auth_mode=blob_auth_mode,
        log=log,
    )
    if prep_err:
        return prep_err
    assert ctx is not None

    acct_url = ctx["acct_url"]
    container = ctx["container"]
    credential_name = ctx["credential_name"]
    sas_token = ctx.get("sas_token")
    restore_urls = ctx["restore_urls"]
    result["stripes"] = len(ctx["stripe_paths"])

    try:
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

        if blob_auth_mode == "managed_identity":
            _log_sql_engine_context_for_mi_blob(cur, server, log)

        if blob_auth_mode == "managed_identity" and not target_managed_instance:
            mi_err = _check_mi_backup_supported(cur, log)
            if mi_err:
                result["error"] = redact_sensitive_text(mi_err)
                log(mi_err)
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass
                return result

        cred_bracket = credential_name.replace("]", "]]")
        log("Creating SQL Server credential for blob container (same as full restore)...")
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

        if blob_auth_mode == "managed_identity":
            create_cred_sql = _mi_credential_sql(credential_name)
            log("CREATE CREDENTIAL (IDENTITY = 'Managed Identity').")
        else:
            create_cred_sql = (
                f"CREATE CREDENTIAL [{cred_bracket}] "
                f"WITH IDENTITY = N'SHARED ACCESS SIGNATURE', "
                f"SECRET = N'{_esc_sql(sas_token or '')}'"
            )
            log("CREATE CREDENTIAL (SHARED ACCESS SIGNATURE).")
        cur.execute(create_cred_sql)

        url_clauses = ", ".join(f"URL = N'{_esc_sql(u)}'" for u in restore_urls)
        header_sql = f"RESTORE HEADERONLY FROM {url_clauses}"
        log("Running RESTORE HEADERONLY FROM URL … (read-only; same blob access path as RESTORE DATABASE)")
        t0 = time.perf_counter()
        cur.execute(header_sql)
        cols = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall() or []
        while True:
            try:
                for _ in cur.fetchall():
                    pass
            except Exception:
                pass
            if not cur.nextset():
                break
        elapsed = time.perf_counter() - t0

        if rows and cols:
            preview = [str(x)[:120] for x in rows[0][: min(8, len(rows[0]))]]
            log(f"HEADERONLY OK in {elapsed:.1f}s. Columns (first 8): {cols[:8]}")
            log(f"First row (first 8 values, truncated): {preview}")
        else:
            log(f"HEADERONLY completed in {elapsed:.1f}s (no rows returned — unusual for a valid .bak).")

        cur.close()
        conn.close()
        result["status"] = "success"
        result["header_rows"] = len(rows)
        return result
    except Exception as e:
        err_text = str(e)
        try:
            if getattr(e, "args", None):
                log(f"(ODBC/SQL raw exception.args) {e.args!r}")
        except Exception:
            pass
        try:
            diagnostic = _diagnose_backup_error(
                err_text,
                blob_auth_mode=blob_auth_mode,
                storage_account_url=acct_url,
                container_name=container,
            )
        except Exception:
            diagnostic = ""
        result["error"] = redact_sensitive_text(err_text)
        if diagnostic:
            result["diagnostic"] = diagnostic.strip()
        log(f"HEADERONLY test failed: {err_text}")
        if diagnostic:
            log(diagnostic.strip())
        return result


def run_restore_from_blob(
    server: str,
    database: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
    blob_connection_string: str = "",
    container: str = "",
    blob_path: str = "",
    log_callback: Optional[Any] = None,
    target_managed_instance: bool = False,
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
) -> Dict[str, Any]:
    """
    Restore a SQL Server database from one or more .bak stripes in Azure Blob.

    Args:
        server: Target SQL Server instance.
        database: Target database name (will be created or replaced).
        auth: windows | sql.
        user/password: For SQL auth; None for Windows.
        blob_connection_string: Azure Storage connection string (required for
            blob_auth_mode='connection_string').
        container: Blob container name (no default; required unless URL contains it in MI mode).
        blob_path: Path within container to the .bak. May be the single file or any
            one stripe of a striped set; sibling stripes are auto-discovered.
        log_callback: Optional callable(msg) for progress.
        target_managed_instance: If True, omit REPLACE/STATS (required for Azure SQL MI).
        blob_auth_mode: 'connection_string' (default, SAS) or 'managed_identity'.
        storage_account_url: Required for blob_auth_mode='managed_identity'.

    Returns:
        dict with status, error message if failed.
    """
    import pyodbc  # noqa: F401

    def log(msg: str) -> None:
        safe = redact_sensitive_text(str(msg))
        logger.info(safe)
        if log_callback:
            try:
                log_callback(safe)
            except Exception:
                pass

    result: Dict[str, Any] = {"status": "failed", "error": None}

    try:
        try:
            _b = _import_bak_to_blob_helpers()
            _diagnose_backup_error = _b._diagnose_backup_error
            _mi_credential_sql = _b._mi_credential_sql
            _check_mi_backup_supported = _b._check_mi_backup_supported
        except Exception as e:
            result["error"] = f"Backup module not available (needed for credential/SAS): {e}"
            return result

        prep_err, ctx = _prepare_blob_restore_urls(
            blob_path=blob_path,
            container=container,
            blob_connection_string=blob_connection_string,
            storage_account_url=storage_account_url,
            blob_auth_mode=blob_auth_mode,
            log=log,
        )
        if prep_err:
            return prep_err
        assert ctx is not None
        acct_url = ctx["acct_url"]
        container = ctx["container"]
        credential_name = ctx["credential_name"]
        sas_token = ctx.get("sas_token")
        restore_urls = ctx["restore_urls"]
        result["stripes"] = len(ctx["stripe_paths"])

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

        if blob_auth_mode == "managed_identity":
            _log_sql_engine_context_for_mi_blob(cur, server, log)
            log(
                "RESTORE … FROM URL uses the **SQL host** managed identity from the block above — "
                "grant that identity **Storage Blob Data Reader** (or Contributor) on the storage account if you see OS error 5."
            )

        if blob_auth_mode == "managed_identity" and not target_managed_instance:
            mi_err = _check_mi_backup_supported(cur, log)
            if mi_err:
                result["error"] = redact_sensitive_text(mi_err)
                log(mi_err)
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass
                return result

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

        if blob_auth_mode == "managed_identity":
            create_cred_sql = _mi_credential_sql(credential_name)
            log("Credential created (IDENTITY = 'Managed Identity'). Running RESTORE ...")
        else:
            create_cred_sql = (
                f"CREATE CREDENTIAL [{cred_bracket}] "
                f"WITH IDENTITY = N'SHARED ACCESS SIGNATURE', "
                f"SECRET = N'{_esc_sql(sas_token)}'"
            )
            log("Credential created (SAS). Running RESTORE DATABASE ... FROM URL ...")
        cur.execute(create_cred_sql)

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
        err_text = str(e)
        try:
            if getattr(e, "args", None):
                log(f"(ODBC/SQL raw exception.args) {e.args!r}")
        except Exception:
            pass
        acct_url_hint = locals().get("acct_url") or ""
        container_hint = locals().get("container") or ""
        try:
            diagnostic = _diagnose_backup_error(
                err_text,
                blob_auth_mode=blob_auth_mode,
                storage_account_url=acct_url_hint,
                container_name=container_hint,
            )
        except Exception:
            diagnostic = ""
        result["error"] = redact_sensitive_text(err_text)
        if diagnostic:
            result["diagnostic"] = diagnostic.strip()
        log(f"Restore failed: {err_text}")
        if diagnostic:
            log(diagnostic.strip())
        return result
