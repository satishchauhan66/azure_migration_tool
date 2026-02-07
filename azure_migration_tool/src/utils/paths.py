# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""Path and naming utilities."""

import hashlib
import re
from datetime import datetime, timezone


def utc_ts_compact() -> str:
    """Return compact UTC timestamp: YYYYMMDD_HHMMSS"""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    """Return ISO format UTC timestamp"""
    return datetime.now(timezone.utc).isoformat()


def safe_name(s: str) -> str:
    """Convert string to safe filename (removes special chars, limits length)"""
    s = (s or "").strip()
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s)
    return s[:200] if len(s) > 200 else s


def short_slug(s: str, max_prefix: int = 28) -> str:
    """
    Generate short stable folder name: <prefix>_<8charhash>
    Keeps Windows paths short to avoid MAX_PATH issues.
    """
    s = (s or "").strip()
    prefix = safe_name(s)[:max_prefix]
    h = hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()[:8]
    return f"{prefix}_{h}" if prefix else h


def qident(name: str) -> str:
    """Quote SQL identifier: [name] (handles ] by doubling)"""
    return "[" + name.replace("]", "]]") + "]"

