# Author: Satish Chauhan

"""Merge saved defaults with explicit job payload (payload wins when set)."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict


def _apply_organization_defaults(job: Dict[str, Any]) -> None:
    try:
        from gui.utils.org_settings import load_org_settings
    except ImportError:
        from azure_migration_tool.gui.utils.org_settings import load_org_settings

    org = load_org_settings()
    local = job.setdefault("local_backup", {})
    if _is_empty(local.get("path")) and _is_empty(local.get("local_backup_path")):
        unc = (org.get("default_backup_unc_path") or "").strip()
        if unc:
            local["path"] = unc
    blob = job.setdefault("blob", {})
    if _is_empty(blob.get("folder")) and _is_empty(blob.get("blob_folder")):
        prefix = (org.get("default_blob_folder_prefix") or "").strip()
        if prefix:
            blob["folder"] = prefix
    if _is_empty(job.get("project_path")):
        pp = (org.get("default_project_path") or "").strip()
        if pp:
            job["project_path"] = pp
            out = job.setdefault("output", {})
            if isinstance(out, dict) and _is_empty(out.get("project_path")):
                out["project_path"] = pp


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def prepare_job_with_defaults(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply organization + environment defaults; never overwrite non-empty payload fields.
    Adds ``_effective`` summary for MCP / auditing.
    """
    work = deepcopy(job)
    payload_snapshot = deepcopy(job)

    _apply_organization_defaults(work)

    try:
        from .environment_resolver import apply_environment_to_job
    except ImportError:
        from azure_migration_tool.src.orchestration.environment_resolver import (
            apply_environment_to_job,
        )
    resolution = apply_environment_to_job(work)

    work["_effective"] = {
        "payload_provided": _summarize_provided(payload_snapshot),
        "defaults_applied": _summarize_defaults_applied(payload_snapshot, work),
        "environment_resolution": work.get("_environment_resolution"),
        "resolved_environment": resolution.get("environment"),
    }
    return work


def _summarize_provided(job: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in (
        "job_id",
        "environment",
        "source",
        "local_backup",
        "blob",
        "restore_target",
        "steps",
        "backup_runs",
        "project_path",
        "output",
    ):
        val = job.get(key)
        if not _is_empty(val):
            out[key] = val
    return out


def _summarize_defaults_applied(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    applied: Dict[str, Any] = {}
    for section in ("local_backup", "blob", "restore_target", "output", "project_path"):
        if section == "project_path":
            if _is_empty(before.get("project_path")) and not _is_empty(after.get("project_path")):
                applied["project_path"] = after.get("project_path")
            continue
        b = before.get(section) if section != "output" else before.get("output") or {}
        a = after.get(section) if section != "output" else after.get("output") or {}
        if not isinstance(b, dict):
            b = {}
        if not isinstance(a, dict):
            a = {}
        filled = {}
        for k, v in a.items():
            if k.startswith("_"):
                continue
            if _is_empty(b.get(k)) and not _is_empty(v):
                filled[k] = v
        if filled:
            applied[section] = filled
    if _is_empty(before.get("environment")) and not _is_empty(after.get("environment")):
        applied["environment"] = after.get("environment")
    return applied
