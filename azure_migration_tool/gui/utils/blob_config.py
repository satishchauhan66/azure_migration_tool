# Author: Satish Chauhan

"""Persisted Azure Blob settings (connection string + container) shared app-wide."""

from __future__ import annotations

import json
import os
from pathlib import Path


def get_blob_settings_path() -> Path:
    if os.name == "nt":
        base = os.environ.get("APPDATA", os.path.expanduser("~"))
        base = Path(base) / "AzureMigrationTool"
    else:
        base = Path(os.path.expanduser("~")) / ".azure_migration_tool"
    base.mkdir(parents=True, exist_ok=True)
    return base / "blob_settings.json"


def load_blob_settings() -> tuple[str, str]:
    """Return ``(connection_string, container)`` with defaults if missing."""
    path = get_blob_settings_path()
    try:
        if not path.exists():
            return "", "db2-stage"
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        conn = data.get("blob_connection_string")
        cont = data.get("container")
        conn_s = conn.strip() if isinstance(conn, str) else ""
        cont_s = cont.strip() if isinstance(cont, str) else ""
        return conn_s, (cont_s or "db2-stage")
    except Exception:
        return "", "db2-stage"


def save_blob_settings(blob_connection_string: str, container: str) -> None:
    path = get_blob_settings_path()
    data = {
        "blob_connection_string": (blob_connection_string or "").strip(),
        "container": (container or "db2-stage").strip() or "db2-stage",
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
