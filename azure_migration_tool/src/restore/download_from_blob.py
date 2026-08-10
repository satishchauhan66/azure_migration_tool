# Author: Satish Chauhan

"""
Download SQL Server .bak backup file(s) from Azure Blob Storage to a local or UNC path.

Used by the Blob → Local Restore tab: download stripe sets to disk, then RESTORE FROM DISK.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = __import__("logging").getLogger(__name__)


def _resolve_local_download_paths(local_destination: str, blob_paths: List[str]) -> List[str]:
    """
    Map blob paths to local file paths.

    * Folder destination → each blob keeps its file name in that folder.
    * Single .bak path → one file only; striped sets use the parent folder + stripe names.
    """
    raw = (local_destination or "").strip().strip('"').strip("'")
    if not raw:
        raise ValueError("Local download path is required.")
    raw = raw.replace("/", "\\")

    if len(blob_paths) == 1 and raw.lower().endswith(".bak"):
        parent = os.path.dirname(raw)
        if parent:
            os.makedirs(parent, exist_ok=True)
        return [raw]

    folder = raw
    if raw.lower().endswith(".bak"):
        folder = os.path.dirname(raw) or raw
    os.makedirs(folder, exist_ok=True)
    return [os.path.join(folder, Path(bp.replace("\\", "/")).name) for bp in blob_paths]


def download_backup_from_blob(
    *,
    blob_path: str,
    blob_auth_mode: str = "connection_string",
    blob_connection_string: str = "",
    blob_account_url: str = "",
    blob_container: str = "",
    local_destination: str,
    log: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[float], None]] = None,
    cancel_event: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Download a .bak blob (or full striped set) from Azure Blob Storage to local/UNC disk.

    Returns dict: success, local_files, local_file (first), blob_paths, download_time_sec, message.
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

    result: Dict[str, Any] = {
        "success": False,
        "local_files": [],
        "local_file": "",
        "blob_paths": [],
        "download_time_sec": 0,
        "message": "",
    }

    try:
        blob_path = (blob_path or "").strip().replace("\\", "/").lstrip("/")
        if not blob_path or not blob_path.lower().endswith(".bak"):
            raise ValueError("Select a .bak blob path to download.")
        if not (blob_container or "").strip():
            raise ValueError("Container name is required.")

        try:
            from ..backup.local_backup_and_upload import _get_tool_blob_service_client
        except ImportError:
            from src.backup.local_backup_and_upload import _get_tool_blob_service_client

        try:
            from .restore_from_blob import _discover_stripe_set
        except ImportError:
            from src.restore.restore_from_blob import _discover_stripe_set

        client = _get_tool_blob_service_client(
            blob_auth_mode=blob_auth_mode,
            blob_connection_string=blob_connection_string,
            blob_account_url=blob_account_url,
            container=blob_container,
            log=log,
        )
        container = blob_container.strip()

        conn_for_discover = blob_connection_string if blob_auth_mode != "managed_identity" else ""
        blob_paths = _discover_stripe_set(
            conn_for_discover,
            container,
            blob_path,
            log=log,
            blob_service_client=client,
        )
        result["blob_paths"] = list(blob_paths)

        local_paths = _resolve_local_download_paths(local_destination, blob_paths)
        if len(local_paths) != len(blob_paths):
            raise RuntimeError("Internal error: blob/local path count mismatch.")

        log(f"Downloading {len(blob_paths)} file(s) from container '{container}'")
        for bp, lp in zip(blob_paths, local_paths):
            log(f"  blob: {bp}")
            log(f"  ->   {lp}")

        start = time.time()
        total_bytes = 0
        downloaded_bytes = 0

        container_client = client.get_container_client(container)
        for bp in blob_paths:
            props = container_client.get_blob_client(bp).get_blob_properties()
            total_bytes += props.size or 0

        for idx, (bp, lp) in enumerate(zip(blob_paths, local_paths), 1):
            if cancel_event is not None and cancel_event.is_set():
                result["message"] = "Download cancelled by user."
                log("Download cancelled.")
                return result

            blob_client = container_client.get_blob_client(bp)
            props = blob_client.get_blob_properties()
            size = props.size or 0
            log(f"[{idx}/{len(blob_paths)}] Downloading {Path(bp).name} ({size / (1024 * 1024):.1f} MB)...")

            Path(lp).parent.mkdir(parents=True, exist_ok=True)
            downloader = blob_client.download_blob(max_concurrency=4)
            with open(lp, "wb") as out:
                for chunk in downloader.chunks():
                    if cancel_event is not None and cancel_event.is_set():
                        result["message"] = "Download cancelled by user."
                        log("Download cancelled.")
                        return result
                    out.write(chunk)
                    downloaded_bytes += len(chunk)
                    if progress_callback and total_bytes:
                        pct = min(99.0, downloaded_bytes / total_bytes * 100.0)
                        progress_callback(pct)

            log(f"  ✓ Saved {lp}")

        elapsed = time.time() - start
        result["download_time_sec"] = round(elapsed, 2)
        result["local_files"] = local_paths
        result["local_file"] = local_paths[0] if local_paths else ""
        result["success"] = True
        result["message"] = f"Downloaded {len(local_paths)} file(s) in {elapsed:.1f}s"
        log(f"✓ Download complete ({elapsed:.1f}s)")
        if progress_callback:
            progress_callback(100.0)
        return result

    except Exception as e:
        msg = redact_sensitive_text(str(e))
        log(f"ERROR: {msg}")
        result["message"] = msg
        return result
