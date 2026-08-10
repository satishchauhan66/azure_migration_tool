# Author: Satish Chauhan

"""
Discover database folders and .bak backups in an Azure Blob container.

Supports all layouts used by this tool:
  * Structured (default): database/run_id/filename.bak
  * With optional root prefix: root/database/run_id/filename.bak
  * Flat folder: database/filename.bak
  * Container root: filename.bak (database parsed from filename)
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

_RUN_ID_RE = re.compile(r"^\d{8}_\d{6}$", re.IGNORECASE)
_STRIPE_RE = re.compile(r"_part(\d+)of(\d+)\.bak$", re.IGNORECASE)


def _import_parse_backup_filename():
    try:
        from .local_backup_and_upload import _parse_backup_filename
    except ImportError:
        from src.backup.local_backup_and_upload import _parse_backup_filename
    return _parse_backup_filename


def normalize_blob_name(name: str) -> str:
    return (name or "").replace("\\", "/").strip().lstrip("/")


def is_bak_blob(name: str) -> bool:
    return normalize_blob_name(name).lower().endswith(".bak")


def database_folder_from_blob_path(blob_name: str) -> Optional[str]:
    """
    Return the backed-up database folder name for a .bak blob path.

    Examples:
      OUT2K_ss_sld_db22u/20260810_054126/OUT2K_ss_sld_db22u_20260810_054126.bak -> OUT2K_ss_sld_db22u
      backups/OUT2K_ss_sld_db22u/20260810_054126/file.bak -> OUT2K_ss_sld_db22u
      OUT2K_ss_sld_db22u/OUT2K_ss_sld_db22u.bak -> OUT2K_ss_sld_db22u
      OUT2K_ss_sld_db22u_20260810_054126.bak -> OUT2K_ss_sld_db22u
    """
    name = normalize_blob_name(blob_name)
    if not is_bak_blob(name):
        return None
    parts = [p for p in name.split("/") if p]
    if len(parts) >= 3 and _RUN_ID_RE.match(parts[-2]):
        return parts[-3]
    if len(parts) >= 2:
        return parts[0]
    parse_backup_filename = _import_parse_backup_filename()
    db, _run = parse_backup_filename(parts[0])
    return db or None


def discover_database_names(blobs: Iterable[Any]) -> List[str]:
    """Collect sorted unique database folder names from container blobs."""
    seen: set[str] = set()
    for blob in blobs:
        name = getattr(blob, "name", blob)
        db = database_folder_from_blob_path(str(name))
        if db:
            seen.add(db)
    return sorted(seen)


def blob_matches_database(blob_name: str, db_name: str) -> bool:
    """True when this .bak blob belongs to the selected database folder."""
    db_name = (db_name or "").strip().rstrip("/")
    if not db_name or not is_bak_blob(blob_name):
        return False
    extracted = database_folder_from_blob_path(blob_name)
    if extracted and extracted == db_name:
        return True
    norm = normalize_blob_name(blob_name)
    if norm.startswith(db_name + "/"):
        return True
    return False


def backup_blobs_for_database(blobs: Iterable[Any], db_name: str) -> List[Any]:
    """Filter blob items to .bak files for the given database."""
    return [
        b
        for b in blobs
        if blob_matches_database(str(getattr(b, "name", b)), db_name)
    ]


def _fmt_size(n: int) -> str:
    gb = n / (1024 ** 3)
    if gb >= 1.0:
        return f"{gb:,.1f} GB"
    return f"{n / (1024 ** 2):,.1f} MB"


def build_backup_list_display(blobs: Iterable[Any]) -> Tuple[List[str], Dict[str, str]]:
    """
    Build listbox labels and label->blob_path map for .bak blobs.

    Striped sets collapse to one row; restore/download discovers siblings.
    """
    all_baks = [b for b in blobs if is_bak_blob(str(getattr(b, "name", b)))]
    groups: dict = {}
    singles: list = []

    for b in all_baks:
        name = normalize_blob_name(str(b.name))
        folder, fname = name.rsplit("/", 1) if "/" in name else ("", name)
        size = int(getattr(b, "size", 0) or 0)
        m = _STRIPE_RE.search(fname)
        if not m:
            singles.append((name, size))
            continue
        pref = fname[: m.start()]
        key = (folder, pref, int(m.group(2)))
        groups.setdefault(key, []).append((int(m.group(1)), name, size))

    display: list = []
    for name, size in singles:
        label = f"{name}    [single, {_fmt_size(size)}]"
        display.append((name, label, name))

    for (_folder, _pref, total), parts in groups.items():
        parts.sort()
        first_name = parts[0][1]
        total_size = sum(p[2] for p in parts)
        have = len(parts)
        status = f"{have}/{total} stripe(s)" + ("" if have == total else "  MISSING!")
        label = f"{first_name}    [{status}, total {_fmt_size(total_size)}]"
        display.append((first_name, label, first_name))

    display.sort(key=lambda t: t[0], reverse=True)
    labels = [t[1] for t in display]
    label_to_path = {t[1]: t[2] for t in display}
    return labels, label_to_path


def list_all_container_blobs(container_client) -> List[Any]:
    """List every blob in a container (handles pagination)."""
    return list(container_client.list_blobs(name_starts_with=None))
