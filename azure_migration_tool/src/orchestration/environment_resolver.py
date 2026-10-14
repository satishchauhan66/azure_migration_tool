# Author: Satish Chauhan

"""Apply environment profiles (dev/qa/uat/prod) to worker jobs."""

from __future__ import annotations

from typing import Any, Dict


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _first_database(job: Dict[str, Any]) -> str:
    src = job.get("source") or {}
    dbs = src.get("databases") or []
    if dbs:
        return str(dbs[0]).strip()
    runs = job.get("backup_runs") or []
    if runs and isinstance(runs[0], dict):
        return str(runs[0].get("database") or "").strip()
    return ""


def resolve_environment_for_job(job: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from gui.utils.environment_profiles import detect_environment_from_hints, get_profile
    except ImportError:
        from azure_migration_tool.gui.utils.environment_profiles import (
            detect_environment_from_hints,
            get_profile,
        )

    explicit = (job.get("environment") or job.get("profile") or "").strip()
    blob = job.get("blob") or {}
    if not explicit and isinstance(blob, dict):
        explicit = (blob.get("profile") or blob.get("environment") or "").strip()
    tgt = job.get("restore_target") or {}
    restore_server = (tgt.get("server") or "").strip()
    storage_url = ""
    if isinstance(blob, dict):
        storage_url = (blob.get("storage_account_url") or "").strip()

    detection = detect_environment_from_hints(
        environment=explicit,
        database=_first_database(job),
        restore_server=restore_server,
        storage_url=storage_url,
    )
    env = detection.get("environment") or "uat"
    profile = get_profile(env) or {}
    return {
        "environment": env,
        "detection": detection,
        "profile": {
            "mi_server": (profile.get("mi_server") or "").strip(),
            "storage_account_url": (profile.get("storage_account_url") or "").strip(),
            "container": (profile.get("container") or "sqlbackups").strip(),
            "blob_auth_mode": (profile.get("blob_auth_mode") or "managed_identity").strip(),
            "connection_string": (profile.get("connection_string") or "").strip(),
            "blob_folder_prefix": (profile.get("blob_folder_prefix") or "").strip(),
            "target_managed_instance": bool(profile.get("target_managed_instance", True)),
            "restore_user": (profile.get("restore_user") or "").strip(),
            "owner_team": (profile.get("owner_team") or "").strip(),
            "application": (profile.get("application") or "").strip(),
        },
    }


def apply_environment_to_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fill missing restore_target / blob fields from the resolved environment profile.
    Explicit payload values are never overwritten.
    """
    resolved = resolve_environment_for_job(job)
    env = resolved["environment"]
    prof = resolved["profile"]
    detection = resolved["detection"]

    if _is_empty(job.get("environment")):
        job["environment"] = env
    job["_environment_resolution"] = {
        "environment": env,
        "confidence": detection.get("confidence"),
        "ambiguous": detection.get("ambiguous"),
        "candidates": detection.get("candidates"),
        "scores": detection.get("scores"),
    }

    if detection.get("ambiguous") and _is_empty((job.get("environment") or "").strip()):
        job["_environment_resolution"]["warning"] = (
            "Multiple environments matched; using first candidate. "
            "Set job.environment explicitly in the payload."
        )

    blob = job.setdefault("blob", {})
    if not isinstance(blob, dict):
        blob = {}
        job["blob"] = blob

    use_profile = blob.get("use_profile")
    if use_profile is None:
        use_profile = not blob.get("use_saved_settings", False)

    tgt = job.setdefault("restore_target", {})
    if not isinstance(tgt, dict):
        tgt = {}
        job["restore_target"] = tgt

    if use_profile and prof.get("mi_server") and _is_empty(tgt.get("server")):
        tgt["server"] = prof["mi_server"]
    if "target_managed_instance" not in tgt and prof.get("target_managed_instance") is not None:
        tgt["target_managed_instance"] = prof["target_managed_instance"]
    if _is_empty(tgt.get("user")) and prof.get("restore_user"):
        tgt["user"] = prof["restore_user"]

    blob_keys_from_profile = (
        "auth_mode",
        "storage_account_url",
        "connection_string",
        "container",
        "folder",
    )
    has_any_blob_payload = any(not _is_empty(blob.get(k)) for k in blob_keys_from_profile) or blob.get(
        "connection_string"
    )

    if use_profile and (prof.get("storage_account_url") or prof.get("connection_string")):
        if not has_any_blob_payload:
            blob["use_saved_settings"] = False
        if _is_empty(blob.get("auth_mode")):
            blob["auth_mode"] = prof.get("blob_auth_mode") or "managed_identity"
        if _is_empty(blob.get("storage_account_url")) and prof.get("storage_account_url"):
            blob["storage_account_url"] = prof["storage_account_url"]
        if _is_empty(blob.get("connection_string")) and prof.get("connection_string"):
            blob["connection_string"] = prof["connection_string"]
        if _is_empty(blob.get("container")) and prof.get("container"):
            blob["container"] = prof["container"]
        if _is_empty(blob.get("folder")) and prof.get("blob_folder_prefix"):
            blob["folder"] = prof["blob_folder_prefix"]

    return resolved
