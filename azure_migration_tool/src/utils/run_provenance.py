# Author: Satish Chauhan

"""Build and version metadata for run traceability (no secrets)."""

from __future__ import annotations

import logging
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _app_version() -> str:
    try:
        import importlib

        m = importlib.import_module("azure_migration_tool")
        ver = getattr(m, "__version__", None)
        if ver:
            return str(ver)
    except ImportError:
        pass
    return ""


def _git_root() -> Optional[Path]:
    p = Path(__file__).resolve()
    for parent in (p, *p.parents):
        if (parent / ".git").is_dir():
            return parent
    return None


def _git_revision() -> str:
    root = _git_root()
    if not root:
        return ""
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=str(root),
        )
        if proc.returncode == 0 and (proc.stdout or "").strip():
            return proc.stdout.strip()
    except Exception:
        pass
    return ""


def _odbc_driver() -> str:
    try:
        from ..utils.database import pick_sql_driver

        return pick_sql_driver(logger)
    except Exception:
        return ""


def _azcopy_version() -> str:
    try:
        from ..utils.azcopy_utils import get_azcopy_version

        return get_azcopy_version()
    except Exception:
        return ""


def collect_run_provenance() -> Dict[str, Any]:
    return {
        "app_version": _app_version(),
        "git_revision": _git_revision(),
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "odbc_driver": _odbc_driver(),
        "azcopy_version": _azcopy_version(),
    }