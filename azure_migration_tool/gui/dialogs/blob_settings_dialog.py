# Author: Satish Chauhan

"""Blob settings summary + entry to organization setup wizard."""

from __future__ import annotations

from typing import Any

import tkinter as tk


def blob_settings_summary() -> str:
    try:
        from gui.utils.blob_config import blob_settings_configured, load_blob_settings
    except ImportError:
        from azure_migration_tool.gui.utils.blob_config import (
            blob_settings_configured,
            load_blob_settings,
        )
    if not blob_settings_configured():
        return "Blob: not configured"
    conn, cont, mode, url = load_blob_settings()
    if mode == "managed_identity":
        return f"Blob: MI · {url or '?'} · {cont or 'container from URL'}"
    return f"Blob: connection string · container {cont or '?'}"


def open_blob_settings_dialog(main_window: Any, *, parent: tk.Misc | None = None) -> None:
    """Organization setup wizard (step 1 = Azure Blob)."""
    try:
        from gui.dialogs.organization_setup_wizard import open_organization_setup_wizard
    except ImportError:
        from azure_migration_tool.gui.dialogs.organization_setup_wizard import (
            open_organization_setup_wizard,
        )
    open_organization_setup_wizard(main_window, parent=parent)
