# Author: S@tish Chauhan

"""Path and naming utilities."""

import hashlib
import os
import re
from datetime import datetime, timezone
from pathlib import Path


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


def safe_table_filename(schema: str, table: str, ext: str = ".sql",
                        max_len: int = 80) -> str:
    """Build a per-table filename that stays within *max_len* characters.

    Returns ``schema.table.sql`` when it fits, otherwise truncates
    the table name and appends a hash suffix to prevent collisions.
    """
    raw = f"{safe_name(schema)}.{safe_name(table)}{ext}"
    if len(raw) <= max_len:
        return raw
    h = hashlib.sha1(f"{schema}.{table}".encode("utf-8", "ignore")).hexdigest()[:8]
    budget = max_len - len(safe_name(schema)) - len(ext) - 1 - 1 - len(h)
    # schema '.' truncated_table '_' hash ext
    trunc = safe_name(table)[:max(budget, 10)]
    return f"{safe_name(schema)}.{trunc}_{h}{ext}"


def short_slug(s: str, max_prefix: int = 28) -> str:
    """
    Generate short stable folder name: <prefix>_<8charhash>
    Keeps Windows paths short to avoid MAX_PATH issues.
    """
    s = (s or "").strip()
    prefix = safe_name(s)[:max_prefix]
    h = hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()[:8]
    return f"{prefix}_{h}" if prefix else h


def win_safe_path(p: Path) -> Path:
    """On Windows, return an extended-length ``\\\\?\\`` path to bypass the
    260-char MAX_PATH limit.  On other OSes return *p* unchanged."""
    if os.name != "nt":
        return p
    s = str(p.resolve())
    if not s.startswith("\\\\?\\"):
        s = "\\\\?\\" + s
    return Path(s)


def qident(name: str) -> str:
    """Quote SQL identifier: [name] (handles ] by doubling)"""
    return "[" + name.replace("]", "]]") + "]"

