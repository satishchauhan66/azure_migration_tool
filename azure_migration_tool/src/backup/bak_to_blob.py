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

Blob authentication modes
--------------------------
blob_auth_mode = "connection_string"  (default)
    The tool adds a short-lived *stored access policy* on the container, issues
    a SAS bound to that policy, creates a SQL Server credential
    (IDENTITY = 'SHARED ACCESS SIGNATURE'), runs BACKUP, then **removes the
    policy** so the SAS stops working immediately (ad-hoc SAS cannot be revoked
    server-side). Orphan policies from interrupted runs are removed on the next
    backup; each policy also has a bounded lifetime so SAS self-expires if revoke
    fails. Requires a free policy slot (max 5 per container). Works on any SQL
    Server (on-prem, Azure VM, MI).

blob_auth_mode = "managed_identity"
    Uses the Azure Managed Identity of the SQL Server host VM / Azure SQL MI.
    The tool creates a SQL Server credential using IDENTITY = 'Managed Identity'
    (no secret needed). The tool itself uses DefaultAzureCredential to probe the
    container (precheck and post-backup size verification).

    Requirements:
      - SQL Server 2022 on Azure VM / Azure SQL MI / Arc-enabled SQL Server 2022.
      - The MI must have Storage Blob Data Contributor on the container (or account).
      - azure-identity must be installed (pip install azure-identity).
"""

import re
import time
import uuid
import logging
import math
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urlparse

from ..utils.paths import utc_ts_compact

try:
    from ..utils.redact_secrets import redact_sensitive_text
except ImportError:
    try:
        from src.utils.redact_secrets import redact_sensitive_text
    except ImportError:
        def redact_sensitive_text(t: str) -> str:  # type: ignore[misc]
            return t

logger = logging.getLogger(__name__)


# Recommended for BACKUP TO URL with block blobs (per Microsoft docs):
#   * 4 MB transfer size keeps block count low while still streaming efficiently
#   * 50,000 blocks per blob -> 4 MB * 50,000 = ~200 GB max per stripe
DEFAULT_MAX_TRANSFER_SIZE = 4 * 1024 * 1024     # 4 MB
DEFAULT_BLOCK_SIZE = 65536                       # 64 KB

# Stored access policy lifetime for backup SAS (hours). Must cover longest backup;
# if post-backup revoke fails, the SAS stops working when this expires.
_TOOL_BACKUP_SAS_POLICY_MAX_HOURS = 72

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


def _container_signed_identifiers_as_dict(raw) -> Dict[str, Any]:
    """
    Normalize get_container_access_policy()['signed_identifiers'] to dict[str, AccessPolicy].

    azure-storage-blob returns a list of SignedIdentifier objects; set_container_access_policy
    expects dict[str, AccessPolicy]. Using dict(list) raises TypeError on that list.
    """
    if not raw:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, list):
        out: Dict[str, Any] = {}
        for entry in raw:
            if entry is None:
                continue
            if isinstance(entry, dict):
                sid = entry.get("id") or entry.get("Id")
                ap = entry.get("access_policy") or entry.get("AccessPolicy")
            else:
                sid = getattr(entry, "id", None)
                ap = getattr(entry, "access_policy", None)
            if sid and ap is not None:
                out[str(sid)] = ap
        return out
    return {}


def _is_tool_temp_backup_policy_id(policy_id: str) -> bool:
    """True for policy ids issued by this tool for backup SAS (amt + 32 lowercase hex)."""
    s = policy_id or ""
    return (
        len(s) == 35
        and s.startswith("amt")
        and all(c in "0123456789abcdef" for c in s[3:])
    )


def _prune_stale_tool_backup_access_policies(identifiers: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    """
    Remove tool-issued backup policies from the in-memory map.

    Orphan policies (crash / failed revoke before finally) are cleared on the next
    backup run in the same set_container_access_policy call as the new policy, so
    old SAS URLs stop working without manual portal cleanup.
    """
    remove_keys = [k for k in identifiers if _is_tool_temp_backup_policy_id(k)]
    if not remove_keys:
        return identifiers, 0
    rk = set(remove_keys)
    return {k: v for k, v in identifiers.items() if k not in rk}, len(remove_keys)


def _delete_container_access_policy_id(container_client, policy_id: str) -> None:
    """Remove one stored access policy id from the container (no-op if missing)."""
    props = container_client.get_container_access_policy()
    identifiers = _container_signed_identifiers_as_dict(props.get("signed_identifiers"))
    if policy_id not in identifiers:
        return
    del identifiers[policy_id]
    container_client.set_container_access_policy(
        signed_identifiers=identifiers,
        public_access=props.get("public_access"),
    )


def _register_temp_sas_policy_and_token(
    blob_connection_string: str,
    container: str,
    policy_expiry_hours: int = _TOOL_BACKUP_SAS_POLICY_MAX_HOURS,
    log_callback: Optional[Any] = None,
) -> Tuple[str, str, str, str]:
    """
    Create a stored access policy on the container and a SAS that references it.

    Any prior tool-issued backup policies (same id pattern) on this container are
    removed first so leftover SAS from crashed runs are invalidated in one update.

    If SAS generation fails after the policy was written, the policy is removed.

    Returns:
        (sas_token, credential_name, account_blob_root_url, policy_id)
    """
    from azure.storage.blob import (
        AccessPolicy,
        BlobServiceClient,
        ContainerSasPermissions,
        generate_container_sas,
    )

    parts = _parse_storage_connection_string(blob_connection_string)
    account_name = parts.get("accountname", "")
    account_key = parts.get("accountkey", "")
    endpoint_suffix = parts.get("endpointsuffix", "core.windows.net")
    if not account_name or not account_key:
        raise ValueError("Connection string missing AccountName or AccountKey.")

    policy_id = f"amt{uuid.uuid4().hex}"
    bsc = BlobServiceClient.from_connection_string(blob_connection_string)
    cc = bsc.get_container_client(container)

    props = cc.get_container_access_policy()
    identifiers = _container_signed_identifiers_as_dict(props.get("signed_identifiers"))
    identifiers, n_pruned = _prune_stale_tool_backup_access_policies(identifiers)
    if n_pruned:
        msg = (
            f"Removed {n_pruned} stale backup access polic{'y' if n_pruned == 1 else 'ies'} "
            f"on container {container!r} (orphaned SAS from interrupted runs cleared)."
        )
        safe_msg = redact_sensitive_text(msg)
        logger.info(safe_msg)
        if log_callback:
            try:
                log_callback(safe_msg)
            except Exception:
                pass
    if policy_id in identifiers:
        del identifiers[policy_id]
    if len(identifiers) >= 5:
        raise ValueError(
            "This blob container already has the maximum of 5 stored access policies. "
            "Remove unused policies in Azure Portal (container: Access policy), "
            "or use another container, then retry."
        )

    perms = ContainerSasPermissions(read=True, write=True, list=True, delete=True)
    now = datetime.utcnow()
    identifiers[policy_id] = AccessPolicy(
        permission=perms,
        expiry=now + timedelta(hours=policy_expiry_hours),
        start=now - timedelta(minutes=5),
    )
    cc.set_container_access_policy(
        signed_identifiers=identifiers,
        public_access=props.get("public_access"),
    )

    try:
        sas_token = generate_container_sas(
            account_name=account_name,
            container_name=container,
            account_key=account_key,
            policy_id=policy_id,
        )
    except Exception:
        try:
            _delete_container_access_policy_id(cc, policy_id)
        except Exception:
            pass
        raise

    credential_name = f"https://{account_name}.blob.{endpoint_suffix}/{container}"
    acct_url = f"https://{account_name}.blob.{endpoint_suffix}"
    return sas_token, credential_name, acct_url, policy_id


def _revoke_temp_sas_policy(
    blob_connection_string: str,
    container: str,
    policy_id: str,
    log,
) -> None:
    """Remove the stored access policy so any SAS signed with it stops working."""
    from azure.storage.blob import BlobServiceClient

    last_err: Optional[Exception] = None
    for attempt in range(2):
        try:
            cc = BlobServiceClient.from_connection_string(blob_connection_string).get_container_client(
                container
            )
            _delete_container_access_policy_id(cc, policy_id)
            log(
                f"Revoked temporary container access policy {policy_id!r}; "
                "the backup SAS is no longer valid."
            )
            return
        except Exception as e:
            last_err = e
            if attempt == 0:
                log(
                    f"WARN: could not revoke backup SAS policy {policy_id!r} ({e}); retrying once..."
                )
                time.sleep(1.0)
    assert last_err is not None
    log(
        f"WARN: could not revoke temporary SAS access policy {policy_id!r} after retry: {last_err}. "
        f"The policy expires within {_TOOL_BACKUP_SAS_POLICY_MAX_HOURS}h; the next backup clears "
        "leftover amt* policies automatically."
    )


def normalize_storage_blob_account_url(url: str) -> str:
    """
    Fix the common typo ``https://account.core.windows.net`` (missing ``blob``).
    Azure Blob endpoints must be ``https://account.blob.core.windows.net``.
    Connection strings avoid this because they use AccountName + EndpointSuffix.
    """
    from urllib.parse import urlparse, urlunparse

    raw = (url or "").strip().rstrip("/")
    if not raw.startswith("https://"):
        return raw
    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    # Wrong:  myaccount.core.windows.net
    # Right:  myaccount.blob.core.windows.net
    if host.endswith(".core.windows.net") and ".blob." not in host:
        account = host[: -len(".core.windows.net")]
        if account:
            fixed = urlunparse(
                parsed._replace(netloc=f"{account}.blob.core.windows.net")
            ).rstrip("/")
            return fixed
    return raw


def _parse_storage_account_url(account_url: str, container: str) -> Tuple[str, str]:
    """
    Accept either a full container URL or a bare account URL and return
    (account_url_normalized, container_name).
    - If `container` is provided, it wins.
    - Otherwise, the first URL path segment is used as container.
    """
    url = normalize_storage_blob_account_url((account_url or "").strip().rstrip("/"))
    if not url.startswith("https://"):
        raise ValueError(
            "Storage account URL must start with https://, e.g. "
            "https://myaccount.blob.core.windows.net"
        )
    parts = url.split("/")
    path_parts = [p for p in parts[3:] if p]
    resolved_container = (container or "").strip() or (path_parts[0] if path_parts else "")
    if not resolved_container:
        raise ValueError(
            "Container is required. Set the container field, or include it in the URL "
            "(e.g. https://myaccount.blob.core.windows.net/mycontainer)."
        )
    # Always return account root URL (without container path).
    account_root = "/".join(parts[:3])
    return account_root, resolved_container


def _get_mi_blob_service_client(account_url: str):
    """Return a BlobServiceClient authenticated with host Managed Identity only."""
    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        raise ImportError(
            "azure-identity and azure-storage-blob are required for Managed Identity mode. "
            "Install: pip install azure-identity azure-storage-blob"
        )
    # In managed_identity mode we intentionally restrict credential sources to
    # avoid silently using a developer/user login from CLI/IDE on the host.
    credential = DefaultAzureCredential(
        exclude_environment_credential=True,
        exclude_workload_identity_credential=True,
        exclude_shared_token_cache_credential=True,
        exclude_visual_studio_code_credential=True,
        exclude_cli_credential=True,
        exclude_powershell_credential=True,
        exclude_developer_cli_credential=True,
        exclude_interactive_browser_credential=True,
    )
    return BlobServiceClient(account_url=account_url, credential=credential)


def _mi_credential_sql(credential_name: str) -> str:
    """
    SQL to create a SQL Server credential for Managed Identity BACKUP TO URL.
    No SECRET is needed — the identity is taken from the host VM / SQL MI.
    """
    cred_bracket = credential_name.replace("]", "]]")
    return (
        f"CREATE CREDENTIAL [{cred_bracket}] "
        f"WITH IDENTITY = N'Managed Identity'"
    )


def _get_sql_major_version(cur) -> Optional[int]:
    """Return SQL Server major version (e.g. 13 for 2016, 16 for 2022), or None."""
    try:
        cur.execute("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)")
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    try:
        cur.execute("SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(64))")
        row = cur.fetchone()
        if row and row[0]:
            head = str(row[0]).split(".", 1)[0]
            return int(head) if head.isdigit() else None
    except Exception:
        pass
    return None


def _check_mi_backup_supported(cur, log) -> Optional[str]:
    """
    Managed Identity for BACKUP/RESTORE TO/FROM URL requires SQL Server 2022+.
    Return an error string if MI is unsupported on this instance, else None.
    """
    major = _get_sql_major_version(cur)
    if major is None:
        log("Could not detect SQL Server version; proceeding with Managed Identity (will fail if unsupported).")
        return None
    if major < 16:
        names = {11: "2012", 12: "2014", 13: "2016", 14: "2017", 15: "2019"}
        ver = names.get(major, f"v{major}")
        return (
            f"Managed Identity for BACKUP TO URL is not supported on SQL Server {ver} "
            f"(detected major={major}). It requires SQL Server 2022+ (or Azure SQL MI / Arc-enabled 2022). "
            "Switch to 'Connection String (storage account key)' mode for backup/restore on this server."
        )
    return None


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


def _diagnose_backup_error(
    err_text: str,
    blob_auth_mode: str = "connection_string",
    *,
    storage_account_url: str = "",
    container_name: str = "",
) -> str:
    """
    Translate common cryptic SQL Server BACKUP TO URL errors into actionable hints.
    Returns an empty string when no specific pattern is recognised.
    """
    upper = (err_text or "").upper()
    hints: List[str] = []

    acct_hint = ""
    if storage_account_url:
        try:
            host = urlparse(storage_account_url.strip()).netloc or ""
            acct_hint = host.split(".")[0] if host else ""
        except Exception:
            acct_hint = ""

    is_3201 = "3201" in upper or "CANNOT OPEN BACKUP DEVICE" in upper
    if is_3201 and ("OPERATING SYSTEM ERROR 50" in upper or "ERROR 50(" in upper):
        hints.append(
            "OS error 50 ('The request is not supported') from BACKUP TO URL means the storage "
            "account or target blob rejects the API call SQL Server makes. Most likely causes:\n"
            "  1. The storage account has HIERARCHICAL NAMESPACE enabled (ADLS Gen2). "
            "BACKUP TO URL is NOT supported on ADLS Gen2 - use a plain Blob / GPv2 account.\n"
            "  2. A blob already exists at the target URL but is a different blob type "
            "(e.g. page blob from a previous run that used a storage-key credential, "
            "instead of the SAS-based block blob this tool writes).\n"
            "  3. The container has an immutability / WORM policy that blocks the write.\n"
            "  4. Premium block blob account with constraints on MAXTRANSFERSIZE.\n\n"
            "Quick checks:\n"
            "  - Azure portal -> storage account -> Configuration: 'Hierarchical namespace' must be Disabled.\n"
            "  - Browse to the target folder in the container: delete any pre-existing .bak there.\n"
            "  - Try a different (plain GPv2) storage account."
        )
        if blob_auth_mode == "managed_identity":
            hints.append(
                "Managed Identity note: if your app/VM can list/upload blobs with MI but SQL BACKUP TO URL "
                "still fails with OS error 50, the SQL host likely does not support MI BACKUP TO URL in this "
                "environment. Use Connection String (SAS) mode for backup/restore, or move to a SQL 2022 "
                "supported host configuration for MI backup."
            )
    elif is_3201 and ("OPERATING SYSTEM ERROR 5(" in upper or " ERROR 5 " in upper):
        if blob_auth_mode == "managed_identity":
            hints.append(
                "OS error 5 (Access denied) on RESTORE/FROM URL almost always means the **identity used by "
                "SQL Server on the database host** is not allowed to read the blob — not a SQL 2022 version bug.\n\n"
                "Important: two different principals are involved:\n"
                "  1) **This app / your PC** — used DefaultAzureCredential to list blobs and discover stripes. "
                "That can succeed even when RESTORE fails.\n"
                "  2) **The machine running SQL Server** — when the credential uses IDENTITY = 'Managed Identity', "
                "SQL Server 2022+ reads Azure Storage using the **Azure Managed Identity attached to that host** "
                "(system-assigned or user-assigned on the Azure VM, or the identity Azure SQL MI / Arc uses).\n\n"
                "What to fix in Azure (ask cloud / identity team if needed):\n"
                "  • Portal → your **storage account**"
                + (f" (`{acct_hint}`)" if acct_hint else "")
                + " → **Access control (IAM)**.\n"
                "  • **Add role assignment** → role **Storage Blob Data Reader** (minimum for RESTORE; use "
                "**Contributor** if you also need backup writes).\n"
                "  • **Assign access to** → **Managed identity** → pick the **subscription** and the identity "
                "that belongs to the **same VM (or MI) where this SQL instance runs** — not your user, not the "
                "laptop running this tool.\n"
                "  • Scope can be the storage account or the container"
                + (f" `{container_name}`" if container_name else "")
                + ". Wait several minutes for RBAC to propagate, then retry RESTORE.\n\n"
                "Also verify on the SQL host VM: **Identity** blade shows the managed identity you granted; "
                "and storage firewall allows trusted Azure services / the host’s access as required."
            )
        else:
            hints.append(
                "OS error 5 = access denied. The SAS / credential does not have permission to write "
                "to this container. Verify the storage account key is correct and the container exists."
            )
    elif is_3201 and ("OPERATING SYSTEM ERROR 86" in upper or "AUTHENTICATIONFAILED" in upper):
        hints.append(
            "Authentication failed. The SAS token may be expired or the account key is wrong. "
            "Re-paste the storage connection string and retry."
        )
    elif "1117" in upper or "I/O DEVICE ERROR" in upper:
        hints.append(
            "I/O device error during BACKUP TO URL is usually the per-blob 50,000-block ceiling "
            "(default ~50 GB / stripe with 1 MB transfers, ~200 GB with 4 MB). Increase the "
            "Stripes value on the .bak to Blob tab (try 4, 8, or 16) to split the backup across "
            "more blobs."
        )

    if not hints:
        return ""
    return "\n\n--- Likely cause ---\n" + "\n\n".join(hints)


def _diagnose_precheck_sdk_error(err_text: str, blob_auth_mode: str) -> str:
    """
    Explain Azure SDK errors from container precheck (IAM / firewall / wrong auth).
    """
    upper = (err_text or "").upper().replace(" ", "").replace("_", "")
    hints: List[str] = []

    if "AUTHORIZATIONPERMISSIONMISMATCH" in upper:
        hints.append(
            "Azure returned AuthorizationPermissionMismatch: an identity signed in successfully, "
            "but it lacks permission for this blob API call.\n\n"
            "With Azure AD / OAuth (Managed Identity mode or az login), you need a **data-plane** "
            "role — **Owner** or **Contributor** on the storage account are **not** enough.\n\n"
            "Assign **Storage Blob Data Contributor** (read/write blobs) or "
            "**Storage Blob Data Reader** (read/list only) on this storage account (or container scope).\n\n"
            "Two identities may matter:\n"
            "  • **This PC / tool** — precheck and list-backups use DefaultAzureCredential here "
            "(your Azure AD user if you ran `az login`, or this machine's managed identity). "
            "Grant **Storage Blob Data Contributor** to that principal.\n"
            "  • **SQL Server host** — BACKUP/RESTORE with `IDENTITY = 'Managed Identity'` uses the "
            "**SQL Server VM or Managed Instance** identity. Grant the **same** Blob Data role "
            "to that identity too, or backups will fail later even if precheck passes.\n\n"
            "Portal: Storage account → Access control (IAM) → Add role assignment → pick the role "
            "above → Members → choose the managed identity or user. Allow 5–15 minutes for IAM to propagate.\n"
        )
    elif blob_auth_mode == "managed_identity" and (
        "AUTHORIZATIONFAILED" in upper or "AUTHENTICATIONFAILED" in upper
    ):
        hints.append(
            "Login / managed identity could not authorize to storage. "
            "Confirm the VM has MI enabled or run `az login`, and assign "
            "**Storage Blob Data Contributor** on this storage account.\n"
        )

    if not hints:
        return ""
    return "\n\n--- How to fix ---\n" + "\n".join(hints)


def _precheck_storage_account(
    container: str,
    blob_paths: List[str],
    log,
    *,
    blob_connection_string: str = "",
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
) -> Optional[str]:
    """
    Fast sanity checks before we run BACKUP. Returns an error string if a hard
    blocker is detected (HNS-enabled account, pre-existing incompatible blob,
    container missing) so the caller can fail fast with a clear message.
    Supports both connection-string and managed-identity auth modes.
    """
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        return None

    try:
        if not (container or "").strip():
            return "Container name is required."
        if blob_auth_mode == "managed_identity":
            if not storage_account_url:
                return "Storage account URL is required for Managed Identity mode."
            acct_url, _ = _parse_storage_account_url(storage_account_url, container)
            client = _get_mi_blob_service_client(acct_url)
        else:
            if not blob_connection_string:
                return "Blob connection string is required."
            client = BlobServiceClient.from_connection_string(blob_connection_string)

        container_client = client.get_container_client(container)

        # 1) Container must exist and be accessible.
        try:
            container_client.get_container_properties()
        except Exception as e:
            err_text = str(e)
            hint = _diagnose_precheck_sdk_error(err_text, blob_auth_mode)
            log(f"Precheck failed — container '{container}': {err_text}")
            if hint:
                log(hint.strip())
            # Short message for dialogs; details stay in the Log panel only.
            return (
                f"Cannot access blob container '{container}'. "
                "See the Log panel for the full error and how to fix it."
            )

        # 2) Refuse to start if a blob already exists at any target stripe path
        #    (would cause OS error 50 / NOINIT mismatch).
        existing = []
        for p in blob_paths:
            try:
                bc = container_client.get_blob_client(p)
                props = bc.get_blob_properties()
                existing.append((p, props.size, getattr(props, "blob_type", "?")))
            except Exception:
                pass
        if existing:
            lines = ["Target blob path(s) already exist:"]
            for p, size, btype in existing:
                lines.append(f"  - {p}  ({btype}, {size} bytes)")
            lines.append(
                "BACKUP TO URL with NOFORMAT/NOINIT will fail. Delete these blobs or use a new run folder."
            )
            log("\n".join(lines))
            return (
                "Backup target blob path(s) already exist. "
                "See the Log for paths to delete, or start backup again (new timestamp folder)."
            )

    except Exception as e:
        log(f"Precheck warning (non-fatal): {e}")
        return None
    return None


def run_bak_backup_to_blob(
    server: str,
    database: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
    blob_connection_string: str = "",
    container: str = "",
    run_id: Optional[str] = None,
    log_callback: Optional[Any] = None,
    stripes: Optional[int] = None,
    max_transfer_size: int = DEFAULT_MAX_TRANSFER_SIZE,
    block_size: int = DEFAULT_BLOCK_SIZE,
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
) -> Dict[str, Any]:
    """
    Backup on-prem SQL Server database to Azure Blob as one or more .bak stripes.

    Args:
        server: On-prem SQL Server instance.
        database: Database name.
        auth: windows | sql (for the SQL Server connection).
        user/password: For SQL auth; None for Windows.
        blob_connection_string: Azure Storage connection string (required for
            blob_auth_mode='connection_string').
        container: Blob container name (no default; required unless URL contains it in MI mode).
        run_id: Optional run id (default: YYYYMMDD_HHMMSS).
        log_callback: Optional callable(msg) for progress.
        stripes: Number of parallel blob stripes (1..64). None or 0 = auto-pick from DB size.
        max_transfer_size: BACKUP MAXTRANSFERSIZE in bytes (default 4 MB).
        block_size: BACKUP BLOCKSIZE in bytes (default 64 KB).
        blob_auth_mode: 'connection_string' (default, revocable SAS via stored policy)
            or 'managed_identity'.
        storage_account_url: Required when blob_auth_mode='managed_identity'.
            e.g. https://myaccount.blob.core.windows.net

    Returns:
        dict with status, run_id, blob_paths (list), stripes, error, etc.
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

    result: Dict[str, Any] = {
        "status": "failed",
        "run_id": run_id or utc_ts_compact(),
        "blob_path": None,
        "blob_paths": [],
        "stripes": stripes or 0,
        "server": server,
        "database": database,
        "container": container,
        "blob_auth_mode": blob_auth_mode,
        "error": None,
    }
    run_id = result["run_id"]
    safe_db = re.sub(r"[^a-zA-Z0-9._-]+", "_", database)[:128]
    container = (container or "").strip()
    temp_sas_policy_id: Optional[str] = None

    try:
        # ------------------------------------------------------------------ #
        # Resolve storage coordinates + credential material
        # ------------------------------------------------------------------ #
        if blob_auth_mode == "managed_identity":
            if not storage_account_url:
                result["error"] = (
                    "Storage account URL is required for Managed Identity mode. "
                    "Enter it in the format: https://myaccount.blob.core.windows.net"
                )
                return result
            _raw_u = (storage_account_url or "").strip().rstrip("/")
            acct_url, container = _parse_storage_account_url(storage_account_url, container)
            result["container"] = container
            if normalize_storage_blob_account_url(_raw_u).rstrip("/") != _raw_u:
                log(
                    "Adjusted storage URL to Azure Blob endpoint "
                    f"(hostname must be account.blob.core.windows.net, not account.core.windows.net): {acct_url}"
                )
            credential_name = f"{acct_url}/{container}"
            log(f"Using Managed Identity auth (credential = {credential_name})")
        else:
            if not container:
                result["error"] = "Container name is required in Connection String mode."
                return result
            log(
                f"Creating revocable SAS for backup (container={container}): "
                "temporary stored access policy + SAS (policy removed after backup)."
            )
            try:
                sas_token, credential_name, acct_url, temp_sas_policy_id = (
                    _register_temp_sas_policy_and_token(
                        blob_connection_string, container, log_callback=log
                    )
                )
            except ValueError as e:
                msg = str(e)
                result["error"] = redact_sensitive_text(msg)
                log(msg)
                return result

        # ------------------------------------------------------------------ #
        # Connect to SQL Server (BACKUP must run from master)
        # ------------------------------------------------------------------ #
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
            mi_err = _check_mi_backup_supported(cur, log)
            if mi_err:
                result["error"] = redact_sensitive_text(mi_err)
                log(mi_err)
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass
                return result

        # Auto-pick stripe count
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
        result["blob_path"] = blob_paths[0]

        # ------------------------------------------------------------------ #
        # Pre-check: container accessible? target blobs absent?
        # ------------------------------------------------------------------ #
        precheck_err = _precheck_storage_account(
            container, blob_paths, log,
            blob_connection_string=blob_connection_string,
            blob_auth_mode=blob_auth_mode,
            storage_account_url=storage_account_url,
        )
        if precheck_err:
            result["error"] = redact_sensitive_text(precheck_err)
            try:
                cur.close(); conn.close()
            except Exception:
                pass
            return result

        backup_urls = [f"{acct_url}/{container}/{p}" for p in blob_paths]
        for u in backup_urls:
            log(f"BACKUP TO URL = {u}")

        # ------------------------------------------------------------------ #
        # Create / refresh the SQL Server credential
        # ------------------------------------------------------------------ #
        cred_bracket = credential_name.replace("]", "]]")
        log("Creating SQL Server credential for blob container...")
        drop_sql = (
            "IF EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'"
            + _esc_sql(credential_name)
            + "') DROP CREDENTIAL ["
            + cred_bracket + "]"
        )
        try:
            cur.execute(drop_sql)
        except Exception:
            pass

        if blob_auth_mode == "managed_identity":
            create_cred_sql = _mi_credential_sql(credential_name)
            log("Credential created (IDENTITY = 'Managed Identity', no secret). Running BACKUP ...")
        else:
            create_cred_sql = (
                f"CREATE CREDENTIAL [{cred_bracket}] "
                f"WITH IDENTITY = N'SHARED ACCESS SIGNATURE', "
                f"SECRET = N'{_esc_sql(sas_token)}'"
            )
            log("Credential created (SAS). Running BACKUP DATABASE ... TO URL ...")
        cur.execute(create_cred_sql)

        # ------------------------------------------------------------------ #
        # Execute BACKUP
        # MAXTRANSFERSIZE + BLOCKSIZE keep block count low, avoiding the
        # ~50,000-block-per-blob ceiling (error 3203 / 1117).
        # ------------------------------------------------------------------ #
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

        # Best-effort: confirm the backup set in msdb
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

        # ------------------------------------------------------------------ #
        # Verify each stripe blob is non-zero (Azure commits slightly after SQL returns)
        # ------------------------------------------------------------------ #
        try:
            if blob_auth_mode == "managed_identity":
                container_client = _get_mi_blob_service_client(acct_url).get_container_client(container)
            else:
                from azure.storage.blob import BlobServiceClient
                container_client = BlobServiceClient.from_connection_string(
                    blob_connection_string
                ).get_container_client(container)

            sizes: List[Tuple[str, Optional[int]]] = []
            for path in blob_paths:
                blob_client = container_client.get_blob_client(path)
                size_val: Optional[int] = None
                total_waited = 0
                while total_waited < 90:
                    if total_waited > 0:
                        log(f"Waiting for {path} to be committed ({total_waited}s)...")
                    time.sleep(5)
                    total_waited += 5
                    try:
                        size_val = blob_client.get_blob_properties().size
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
                result["error"] = redact_sensitive_text(
                    "One or more stripe blobs are missing or 0 bytes after waiting. "
                    "The backup may have failed; check SQL Server error log."
                )
                log(result["error"])
                return result

            log(f"Backup completed. {len(blob_paths)} stripe(s), {total / (1024 * 1024):.1f} MB total.")
        except Exception as e:
            log(f"Could not verify blob sizes: {e}")

        result["status"] = "success"
        return result
    except Exception as e:
        err_text = str(e)
        diagnostic = _diagnose_backup_error(
            err_text,
            blob_auth_mode=blob_auth_mode,
            storage_account_url=locals().get("acct_url") or (storage_account_url or ""),
            container_name=container or "",
        )
        # Dialog: short; Log: full SQL error + troubleshooting notes
        result["error"] = redact_sensitive_text(err_text)
        log(f"Backup failed: {err_text}")
        if diagnostic:
            log(diagnostic.strip())
        return result
    finally:
        if temp_sas_policy_id:
            _revoke_temp_sas_policy(blob_connection_string, container, temp_sas_policy_id, log)
