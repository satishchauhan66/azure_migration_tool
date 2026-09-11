# Author: Satish Chauhan
"""Persist per-database local backup preferences (stripe mode/count)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _settings_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", os.path.expanduser("~"))) / "AzureMigrationTool"
    else:
        base = Path(os.path.expanduser("~")) / ".azure_migration_tool"
    base.mkdir(parents=True, exist_ok=True)
    return base / "local_backup_settings.json"


def _db_key(server: str, database: str) -> str:
    return f"{(server or '').strip().upper()}|{(database or '').strip().upper()}"


def load_all() -> Dict[str, Any]:
    path = _settings_path()
    if not path.is_file():
        return {"stripes_by_database": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("stripes_by_database", {})
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return {"stripes_by_database": {}}


def save_all(data: Dict[str, Any]) -> None:
    path = _settings_path()
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def get_stripe_preference(server: str, database: str) -> Optional[Dict[str, Any]]:
    key = _db_key(server, database)
    if not key or key == "|":
        return None
    entry = (load_all().get("stripes_by_database") or {}).get(key)
    return entry if isinstance(entry, dict) else None


def save_stripe_preference(
    server: str,
    database: str,
    *,
    mode: str,
    stripes_value: str,
    last_size_gb: Optional[float] = None,
    last_auto_stripes: Optional[int] = None,
) -> None:
    key = _db_key(server, database)
    if not key or key == "|":
        return
    data = load_all()
    stripes_map = data.setdefault("stripes_by_database", {})
    entry: Dict[str, Any] = {
        "mode": (mode or "auto").strip().lower(),
        "stripes_value": (stripes_value or "Auto").strip(),
    }
    if last_size_gb is not None:
        entry["last_size_gb"] = round(float(last_size_gb), 2)
    if last_auto_stripes is not None:
        entry["last_auto_stripes"] = int(last_auto_stripes)
    stripes_map[key] = entry
    save_all(data)


def resolve_saved_stripes_ui(server: str, database: str) -> Tuple[str, str]:
    """
    Return (mode, combobox_value) for the Local Backup stripes control.

    mode is 'auto' or 'manual'. combobox_value is 'Auto' or a numeric string.
    """
    pref = get_stripe_preference(server, database)
    if not pref:
        return "auto", "Auto"
    mode = str(pref.get("mode") or "auto").lower()
    val = str(pref.get("stripes_value") or "Auto").strip()
    if mode == "manual" and val.lower() != "auto":
        return "manual", val
    return "auto", "Auto"
