# Author: S@tish Chauhan

"""Schema restore module."""

from .schema_restore import (
    BUILTIN_RESTORE_ORDER,
    build_restore_order,
    effective_restore_primary_keys,
    get_backup_paths,
    normalize_full_mirror_cfg,
    run_full_mirror_restore,
    run_restore,
)

try:
    from ..compare.apply_repair import apply_schema_repair
except ImportError:
    apply_schema_repair = None  # type: ignore

__all__ = [
    "BUILTIN_RESTORE_ORDER",
    "build_restore_order",
    "effective_restore_primary_keys",
    "get_backup_paths",
    "normalize_full_mirror_cfg",
    "run_full_mirror_restore",
    "run_restore",
    "apply_schema_repair",
]

