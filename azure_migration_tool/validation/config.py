# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Load database config (from file or dict) and expose output directory.
No pydantic; plain json.load(). Normalize user -> username for db2 and azure_sql.
Config is not required to be stored on disk; callers can pass a dict in memory.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union


def normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize 'user' -> 'username' for db2 and azure_sql. Returns a copy."""
    cfg = dict(config)
    for side in ("db2", "azure_sql"):
        if side in cfg and isinstance(cfg[side], dict):
            cfg[side] = dict(cfg[side])
            if "user" in cfg[side] and "username" not in cfg[side]:
                cfg[side]["username"] = cfg[side].pop("user")
    return cfg


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load database config from a JSON file. Normalize 'user' -> 'username' for db2 and azure_sql.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return normalize_config(config)


def get_output_dir(config_path: Optional[str] = None, output_dir_override: Optional[str] = None) -> str:
    """
    Get output directory: override > VALIDATION_OUTPUT_DIR env > config file's directory > cwd.
    """
    if output_dir_override and str(output_dir_override).strip():
        return str(output_dir_override).strip()
    out = os.environ.get("VALIDATION_OUTPUT_DIR", "").strip()
    if out:
        return out
    if config_path and os.path.isfile(config_path):
        return os.path.dirname(os.path.abspath(config_path))
    return os.getcwd()
