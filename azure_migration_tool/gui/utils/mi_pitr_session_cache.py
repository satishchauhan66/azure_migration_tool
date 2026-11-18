# Author: Satish Chauhan

"""Persist MI PITR subscription list and picker selections across app restarts."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

TAB_SINGLE = "single"
TAB_BULK = "bulk"
_FILENAME = "mi_pitr_session.json"
_MAX_SUB_AGE_DAYS = 14


def _cache_path() -> Path:
    base = Path(os.environ.get("APPDATA", os.path.expanduser("~"))) / "AzureMigrationTool"
    base.mkdir(parents=True, exist_ok=True)
    return base / _FILENAME


def _load_all() -> Dict[str, Any]:
    path = _cache_path()
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_all(data: Dict[str, Any]) -> None:
    try:
        _cache_path().write_text(json.dumps(data, indent=2), encoding="utf-8")
    except OSError:
        pass


def load_tab_session(tab_key: str) -> Dict[str, Any]:
    block = _load_all().get(tab_key)
    return dict(block) if isinstance(block, dict) else {}


def save_tab_session(tab_key: str, patch: Dict[str, Any]) -> None:
    data = _load_all()
    cur = dict(data.get(tab_key) or {})
    cur.update(patch)
    cur["saved_utc"] = datetime.now(timezone.utc).isoformat()
    data[tab_key] = cur
    _save_all(data)


def subscriptions_still_fresh(sess: Dict[str, Any]) -> bool:
    rows = sess.get("subscriptions")
    if not isinstance(rows, list) or not rows:
        return False
    ts = sess.get("subscriptions_saved_utc") or sess.get("saved_utc")
    if not ts:
        return False
    try:
        saved = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - saved
        return age.total_seconds() < _MAX_SUB_AGE_DAYS * 86400
    except ValueError:
        return False


def cached_subscriptions(sess: Dict[str, Any]) -> List[Dict[str, str]]:
    rows = sess.get("subscriptions") or []
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, str]] = []
    for r in rows:
        if isinstance(r, dict) and r.get("subscription_id"):
            out.append(
                {
                    "display_name": str(r.get("display_name") or ""),
                    "subscription_id": str(r.get("subscription_id") or ""),
                }
            )
    return out