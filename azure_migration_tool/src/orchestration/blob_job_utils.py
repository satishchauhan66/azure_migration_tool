# Author: Satish Chauhan

"""Blob/storage helpers for headless jobs (no Tk dependency)."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple


def is_azure_sql_managed_instance_host(server: str) -> bool:
    return ".database.windows.net" in (server or "").lower()


def normalize_blob_account_url(url: str) -> str:
    try:
        from ..backup.bak_to_blob import normalize_storage_blob_account_url
    except ImportError:
        from azure_migration_tool.src.backup.bak_to_blob import normalize_storage_blob_account_url
    normalized = normalize_storage_blob_account_url(url or "")
    parts = normalized.split("/")
    return "/".join(parts[:3]).rstrip("/")


def resolve_blob_container(blob_auth_mode: str, container: str, storage_account_url: str) -> str:
    c = (container or "").strip()
    if blob_auth_mode != "managed_identity":
        return c
    if c:
        return c
    try:
        from ..backup.bak_to_blob import _parse_storage_account_url
    except ImportError:
        from azure_migration_tool.src.backup.bak_to_blob import _parse_storage_account_url
    _, resolved = _parse_storage_account_url(storage_account_url or "", "")
    return resolved


def resolve_stripes_value(raw: Any) -> Optional[int]:
    """None for auto; else 1..64."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() == "auto":
        return None
    try:
        return max(1, min(64, int(s)))
    except ValueError:
        return None


def resolve_max_parallel_uploads(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if s in ("", "max", "all"):
        return None
    try:
        return max(1, int(s))
    except ValueError:
        return None


def merge_blob_config(blob_section: Dict[str, Any]) -> Tuple[Dict[str, str], Optional[str]]:
    """
    Build normalized blob settings from a job ``blob`` section.

    Returns (storage_dict, error_message).
    storage_dict keys: blob_auth_mode, conn_str, storage_account_url, container,
    blob_folder, structured_blob_paths, delete_local_after_upload, max_parallel_uploads
    """
    blob = dict(blob_section or {})
    if blob.get("use_saved_settings") is not False:
        blob["use_saved_settings"] = True
    if blob.get("use_saved_settings"):
        try:
            from gui.utils.blob_config import load_blob_settings
        except ImportError:
            from azure_migration_tool.gui.utils.blob_config import load_blob_settings
        conn_s, cont_s, mode_s, url_s = load_blob_settings()
        blob.setdefault("auth_mode", mode_s or "connection_string")
        if conn_s and not blob.get("connection_string"):
            blob["connection_string"] = conn_s
        if cont_s and not blob.get("container"):
            blob["container"] = cont_s
        if url_s and not blob.get("storage_account_url"):
            blob["storage_account_url"] = url_s

    blob_auth_mode = (blob.get("auth_mode") or "connection_string").strip()
    conn_str = (blob.get("connection_string") or "").strip()
    storage_account_url = (blob.get("storage_account_url") or "").strip()
    container = (blob.get("container") or "").strip()
    blob_folder = (blob.get("folder") or blob.get("blob_folder") or "").strip()
    structured = bool(blob.get("structured_paths", blob.get("structured_blob_paths", True)))
    delete_local = bool(blob.get("delete_local_after_upload", False))
    max_parallel = resolve_max_parallel_uploads(blob.get("max_parallel_uploads"))

    try:
        container = resolve_blob_container(blob_auth_mode, container, storage_account_url)
    except Exception as exc:
        return {}, str(exc)

    if blob_auth_mode == "managed_identity":
        if not storage_account_url:
            return {}, "Storage account URL is required for managed_identity blob auth."
        storage_account_url = normalize_blob_account_url(storage_account_url)
    elif not conn_str:
        return (
            {},
            "Azure Blob is not configured. In the app: Tools → Settings → save connection string "
            "and container once (same PC that runs worker/MCP jobs).",
        )

    if not container:
        return {}, "Blob container name is required."

    return (
        {
            "blob_auth_mode": blob_auth_mode,
            "conn_str": conn_str,
            "storage_account_url": storage_account_url,
            "container": container,
            "blob_folder": blob_folder,
            "structured_blob_paths": structured,
            "delete_local_after_upload": delete_local,
            "max_parallel_uploads": max_parallel,
        },
        None,
    )
