# Author: Sa-tish Chauhan

"""Orchestration module for full migration workflow."""

from __future__ import annotations

from typing import Any

__all__ = ["run_full_migration", "run_bulk_local_backup_blob_restore_job"]


def __getattr__(name: str) -> Any:
    if name == "run_full_migration":
        from .full_migration import run_full_migration

        return run_full_migration
    if name == "run_bulk_local_backup_blob_restore_job":
        from .bulk_local_blob_pipeline import run_bulk_local_backup_blob_restore_job

        return run_bulk_local_backup_blob_restore_job
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
