# Author: Satish Chauhan
"""Saved prerequisite row profiles and blob storage presets."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from gui.utils.server_config import CONFIG_DIR, ensure_config_dir
except ImportError:
    from azure_migration_tool.gui.utils.server_config import CONFIG_DIR, ensure_config_dir

logger = logging.getLogger(__name__)

PROFILES_FILE = CONFIG_DIR / "prerequisite_profiles.json"
BLOB_PRESETS_FILE = CONFIG_DIR / "prerequisite_blob_presets.json"

ORG_BLOB_PRESET_ID = "__organization_default__"
CUSTOM_BLOB_PRESET_ID = "__custom__"


def _load_json_list(path: Path) -> List[Dict[str, Any]]:
    ensure_config_dir()
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning("Could not load %s: %s", path.name, exc)
        return []


def _save_json_list(path: Path, items: List[Dict[str, Any]]) -> bool:
    ensure_config_dir()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2, ensure_ascii=False)
        return True
    except Exception as exc:
        logger.warning("Could not save %s: %s", path.name, exc)
        return False


def list_profile_names() -> List[str]:
    return [
        str(p.get("name") or "").strip()
        for p in _load_json_list(PROFILES_FILE)
        if str(p.get("name") or "").strip()
    ]


def get_profile(name: str) -> Optional[Dict[str, Any]]:
    key = (name or "").strip()
    if not key:
        return None
    for p in _load_json_list(PROFILES_FILE):
        if (p.get("name") or "").strip() == key:
            row = dict(p.get("row") or {})
            row["row_label"] = row.get("row_label") or key
            return row
    return None


def save_profile(name: str, row: Dict[str, Any]) -> bool:
    key = (name or "").strip()
    if not key:
        return False
    items = _load_json_list(PROFILES_FILE)
    payload = {"name": key, "row": {k: v for k, v in row.items() if k != "name"}}
    found = False
    for i, p in enumerate(items):
        if (p.get("name") or "").strip() == key:
            items[i] = payload
            found = True
            break
    if not found:
        items.append(payload)
    return _save_json_list(PROFILES_FILE, items)


def delete_profile(name: str) -> bool:
    key = (name or "").strip()
    if not key:
        return False
    items = _load_json_list(PROFILES_FILE)
    new_items = [p for p in items if (p.get("name") or "").strip() != key]
    if len(new_items) == len(items):
        return False
    return _save_json_list(PROFILES_FILE, new_items)


def list_blob_preset_names() -> List[str]:
    names = [
        str(p.get("name") or "").strip()
        for p in _load_json_list(BLOB_PRESETS_FILE)
        if str(p.get("name") or "").strip()
    ]
    return [ORG_BLOB_PRESET_ID] + names + [CUSTOM_BLOB_PRESET_ID]


def blob_preset_display(name: str) -> str:
    if name == ORG_BLOB_PRESET_ID:
        return "(Organization default)"
    if name == CUSTOM_BLOB_PRESET_ID:
        return "(Custom — edit fields)"
    return name


def resolve_blob_preset_id(display: str) -> str:
    text = (display or "").strip()
    if text == "(Organization default)":
        return ORG_BLOB_PRESET_ID
    if text == "(Custom — edit fields)":
        return CUSTOM_BLOB_PRESET_ID
    return text


def get_blob_preset(name: str) -> Optional[Dict[str, str]]:
    key = (name or "").strip()
    if not key or key == ORG_BLOB_PRESET_ID:
        return None
    for p in _load_json_list(BLOB_PRESETS_FILE):
        if (p.get("name") or "").strip() == key:
            return {
                "blob_auth_mode": (p.get("blob_auth_mode") or "connection_string").strip(),
                "blob_connection_string": (p.get("blob_connection_string") or "").strip(),
                "blob_account_url": (p.get("blob_account_url") or "").strip(),
                "blob_container": (p.get("blob_container") or "").strip(),
            }
    return None


def save_blob_preset(name: str, fields: Dict[str, str]) -> bool:
    key = (name or "").strip()
    if not key or key == ORG_BLOB_PRESET_ID:
        return False
    items = _load_json_list(BLOB_PRESETS_FILE)
    payload = {
        "name": key,
        "blob_auth_mode": (fields.get("blob_auth_mode") or "connection_string").strip(),
        "blob_connection_string": (fields.get("blob_connection_string") or "").strip(),
        "blob_account_url": (fields.get("blob_account_url") or "").strip(),
        "blob_container": (fields.get("blob_container") or "").strip(),
    }
    found = False
    for i, p in enumerate(items):
        if (p.get("name") or "").strip() == key:
            items[i] = payload
            found = True
            break
    if not found:
        items.append(payload)
    return _save_json_list(BLOB_PRESETS_FILE, items)


def delete_blob_preset(name: str) -> bool:
    key = (name or "").strip()
    if not key or key == ORG_BLOB_PRESET_ID:
        return False
    items = _load_json_list(BLOB_PRESETS_FILE)
    new_items = [p for p in items if (p.get("name") or "").strip() != key]
    if len(new_items) == len(items):
        return False
    return _save_json_list(BLOB_PRESETS_FILE, new_items)
