# Author: Satish Chauhan
"""Shared helpers for local/UNC .bak paths (stripes, folder layout, DB name inference)."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import List, Optional, Tuple

_STRIPE_RE = re.compile(r"_part(\d+)of(\d+)\.bak$", re.IGNORECASE)
_RUN_ID_RE = re.compile(r"^\d{8}_\d{6}$", re.IGNORECASE)
_BACKUP_FILENAME_RUN_RE = re.compile(
    r"^(.+)_(\d{8}_\d{6})(?:_part\d+of\d+)?$",
    re.IGNORECASE,
)


def _safe_db_folder_name(database: str) -> str:
    """Sanitize database name for folder segments."""
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", (database or "").strip())[:128]


def parse_backup_filename(filename: str) -> Tuple[str, str]:
    """
    Parse a backup filename into (database_folder, run_id).

    Examples:
      NICU_ss_sld_db22u_20260810_031718.bak -> (NICU_ss_sld_db22u, 20260810_031718)
      NICU_ss_sld_db22u_20260810_031718_part01of04.bak -> same run folder for all stripes
    """
    stem = Path(filename).stem
    m = _BACKUP_FILENAME_RUN_RE.match(stem)
    if m:
        return _safe_db_folder_name(m.group(1)), m.group(2)
    if "_part" in stem.lower():
        base, _, _rest = stem.partition("_part")
        m2 = _BACKUP_FILENAME_RUN_RE.match(base)
        if m2:
            return _safe_db_folder_name(m2.group(1)), m2.group(2)
    return _safe_db_folder_name(stem), ""


def normalize_backup_path(raw: str) -> str:
    """Normalize slashes and strip quotes (UNC, drive, or //server/share paths)."""
    s = (raw or "").strip().strip('"').strip("'")
    if s.startswith("//") and not s.startswith("\\\\"):
        s = "\\\\" + s[2:]
    return s.replace("/", "\\")


def infer_database_name_from_backup_path(backup_path: str) -> Optional[str]:
    """
    Infer backed-up database name from path/filename.

    Supports structured folders ``.../MyDb/20260903_031135/MyDb_20260903_031135.bak``
    and flat names like ``URG2K_ps_x_db22q_20260903_031135.bak``.
    """
    norm = normalize_backup_path(backup_path)
    if not norm:
        return None
    p = Path(norm)
    parts = p.parts
    if len(parts) >= 3 and _RUN_ID_RE.match(parts[-2]):
        return parts[-3]
    db, _run = parse_backup_filename(p.name)
    return db or None


def discover_disk_stripe_set(backup_path: str) -> List[str]:
    """
    Given any .bak path, return the full ordered stripe set on disk/UNC.

    Striped naming: ``<prefix>_partNNofMM.bak`` (siblings in the same folder).
    Single-file backups return a one-element list.
    """
    norm = normalize_backup_path(backup_path)
    if not norm:
        return []
    p = Path(norm)
    fname = p.name
    folder = p.parent

    m = _STRIPE_RE.search(fname)
    if not m:
        return [norm]

    total = int(m.group(2))
    prefix = fname[: m.start()]
    width = max(2, len(str(total)))

    def _stripe_path(index: int) -> str:
        name = f"{prefix}_part{str(index).zfill(width)}of{str(total).zfill(width)}.bak"
        return normalize_backup_path(str(folder / name))

    constructed = [_stripe_path(i) for i in range(1, total + 1)]

    found: dict[int, str] = {}
    if folder.exists():
        for candidate in folder.glob(f"{prefix}_part*of*.bak"):
            mm = _STRIPE_RE.search(candidate.name)
            if not mm or int(mm.group(2)) != total:
                continue
            found[int(mm.group(1))] = normalize_backup_path(str(candidate))

    if len(found) == total:
        return [found[i] for i in range(1, total + 1)]

    if found:
        for i in range(1, total + 1):
            if i not in found:
                found[i] = constructed[i - 1]
        return [found[i] for i in range(1, total + 1)]

    return constructed


def build_structured_local_backup_dir(
    backup_root: str | Path,
    database: str,
    run_id: str,
) -> Path:
    """``backup_root / database / run_id`` for readable on-disk layout."""
    root = Path(normalize_backup_path(str(backup_root)))
    safe_db = _safe_db_folder_name(database)
    run = (run_id or "").strip()
    if not safe_db or not run:
        return root
    return root / safe_db / run


def format_backup_paths_for_ui(paths: List[str]) -> str:
    """Semicolon-separated paths for multi-stripe display in entry fields."""
    return "; ".join(p for p in paths if p)
