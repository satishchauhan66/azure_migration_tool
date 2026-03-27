# Author: Satish Chauhan

"""
App-wide MRU lists for SQL server hostnames and usernames (per machine, local JSON).
"""

from __future__ import annotations

import json
import logging
from typing import List

try:
    from gui.utils.server_config import CONFIG_DIR, ensure_config_dir
except ImportError:
    from azure_migration_tool.gui.utils.server_config import CONFIG_DIR, ensure_config_dir

logger = logging.getLogger(__name__)

HISTORY_FILE = CONFIG_DIR / "input_history.json"
MAX_ITEMS = 30


def _load_raw() -> dict:
    ensure_config_dir()
    if not HISTORY_FILE.exists():
        return {"servers": [], "usernames": []}
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"servers": [], "usernames": []}
        return {
            "servers": data.get("servers") if isinstance(data.get("servers"), list) else [],
            "usernames": data.get("usernames") if isinstance(data.get("usernames"), list) else [],
        }
    except Exception as exc:
        logger.warning("Could not load input history: %s", exc)
        return {"servers": [], "usernames": []}


def _save_raw(servers: List[str], usernames: List[str]) -> None:
    ensure_config_dir()
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {"servers": servers[:MAX_ITEMS], "usernames": usernames[:MAX_ITEMS]},
                f,
                indent=2,
                ensure_ascii=False,
            )
    except Exception as exc:
        logger.warning("Could not save input history: %s", exc)


def _mru_add(items: List[str], value: str) -> List[str]:
    v = (value or "").strip()
    if not v:
        return items
    lower = v.lower()
    rest = [x for x in items if (x or "").strip().lower() != lower]
    return [v] + rest


def record_server(hostname: str) -> None:
    """Remember a server / host string after a successful use."""
    data = _load_raw()
    servers = _mru_add([str(x) for x in data["servers"] if isinstance(x, str)], hostname)
    _save_raw(servers[:MAX_ITEMS], [str(x) for x in data["usernames"] if isinstance(x, str)][:MAX_ITEMS])


def record_username(username: str) -> None:
    """Remember a login / UPN after a successful use."""
    data = _load_raw()
    users = _mru_add([str(x) for x in data["usernames"] if isinstance(x, str)], username)
    _save_raw(
        [str(x) for x in data["servers"] if isinstance(x, str)][:MAX_ITEMS],
        users[:MAX_ITEMS],
    )


def get_servers() -> List[str]:
    data = _load_raw()
    out = []
    seen = set()
    for x in data["servers"]:
        if not isinstance(x, str):
            continue
        t = x.strip()
        if not t or t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
    return out[:MAX_ITEMS]


def get_usernames() -> List[str]:
    data = _load_raw()
    out = []
    seen = set()
    for x in data["usernames"]:
        if not isinstance(x, str):
            continue
        t = x.strip()
        if not t or t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
    return out[:MAX_ITEMS]
