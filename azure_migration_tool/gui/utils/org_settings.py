# Author: Satish Chauhan

"""Organization / one-time defaults (backup UNC, project path, blob prefix)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

try:
    from src.backup.backup_path_utils import DEFAULT_SHARED_BACKUP_ROOT
except ImportError:
    from azure_migration_tool.src.backup.backup_path_utils import DEFAULT_SHARED_BACKUP_ROOT


def get_org_settings_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", os.path.expanduser("~"))) / "AzureMigrationTool"
    else:
        base = Path(os.path.expanduser("~")) / ".azure_migration_tool"
    base.mkdir(parents=True, exist_ok=True)
    return base / "organization_settings.json"


def load_org_settings() -> Dict[str, Any]:
    path = get_org_settings_path()
    if not path.is_file():
        return {
            "default_backup_unc_path": DEFAULT_SHARED_BACKUP_ROOT,
            "default_blob_folder_prefix": "",
            "default_project_path": "",
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("default_backup_unc_path", DEFAULT_SHARED_BACKUP_ROOT)
            data.setdefault("default_blob_folder_prefix", "")
            data.setdefault("default_project_path", "")
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {
        "default_backup_unc_path": DEFAULT_SHARED_BACKUP_ROOT,
        "default_blob_folder_prefix": "",
        "default_project_path": "",
    }


def save_org_settings(
    *,
    default_backup_unc_path: str = "",
    default_blob_folder_prefix: str = "",
    default_project_path: str = "",
) -> None:
    data = load_org_settings()
    if default_backup_unc_path is not None:
        data["default_backup_unc_path"] = (default_backup_unc_path or "").strip()
    if default_blob_folder_prefix is not None:
        data["default_blob_folder_prefix"] = (default_blob_folder_prefix or "").strip()
    if default_project_path is not None:
        data["default_project_path"] = (default_project_path or "").strip()
    get_org_settings_path().write_text(json.dumps(data, indent=2), encoding="utf-8")


def org_backup_path_configured() -> bool:
    return bool((load_org_settings().get("default_backup_unc_path") or "").strip())


def org_settings_summary() -> str:
    o = load_org_settings()
    parts = []
    unc = (o.get("default_backup_unc_path") or "").strip()
    if unc:
        parts.append(f"Backup UNC: {unc}")
    else:
        parts.append("Backup UNC: not set")
    prefix = (o.get("default_blob_folder_prefix") or "").strip()
    if prefix:
        parts.append(f"Blob prefix: {prefix}")
    proj = (o.get("default_project_path") or "").strip()
    if proj:
        parts.append(f"Projects: {proj}")
    return " · ".join(parts)
