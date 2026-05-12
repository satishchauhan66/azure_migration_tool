# Author: Sa-tish Chauhan

"""
Backup & Restore tab: .bak to Azure Blob and Restore from Blob.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from pathlib import Path
import threading
import subprocess
import sys
import os
import logging
from typing import Any, Dict, Optional

parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.widgets.connection_widget import ConnectionWidget
from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll

logger = logging.getLogger(__name__)


def _compact_dialog_error(msg: str, max_len: int = 600) -> str:
    """Keep error popups short; strip secrets; full detail stays in the Log panel (redacted)."""
    try:
        from azure_migration_tool.src.utils.redact_secrets import redact_sensitive_text
    except ImportError:
        from src.utils.redact_secrets import redact_sensitive_text
    m = redact_sensitive_text((msg or "").strip())
    if len(m) <= max_len:
        return m
    return m[: max_len].rstrip() + "\n\n(Full message is in the Log panel.)"


def _normalize_blob_account_url_for_gui(url: str) -> str:
    """Normalize to account-level blob URL for GUI list/read operations."""
    try:
        from src.backup.bak_to_blob import normalize_storage_blob_account_url
        normalized = normalize_storage_blob_account_url(url or "")
        parts = normalized.split("/")
        return "/".join(parts[:3]).rstrip("/")
    except ImportError:
        try:
            from azure_migration_tool.src.backup.bak_to_blob import normalize_storage_blob_account_url
            normalized = normalize_storage_blob_account_url(url or "")
            parts = normalized.split("/")
            return "/".join(parts[:3]).rstrip("/")
        except ImportError:
            return (url or "").strip().rstrip("/")


def _resolve_container_for_gui(blob_auth_mode: str, container: str, storage_account_url: str) -> str:
    """Resolve container from field, or URL path in managed-identity mode."""
    c = (container or "").strip()
    if blob_auth_mode != "managed_identity":
        return c
    if c:
        return c
    try:
        from src.backup.bak_to_blob import _parse_storage_account_url
    except ImportError:
        from azure_migration_tool.src.backup.bak_to_blob import _parse_storage_account_url
    _, resolved = _parse_storage_account_url(storage_account_url or "", "")
    return resolved


class BackupRestoreTab:
    """Tab for .bak backup to Azure Blob and restore from Blob."""

    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None

        # Blob settings: app-wide (MainWindow) so every screen uses the same settings
        self.blob_conn_var = self.main_window.shared_blob_connection_string
        self.blob_container_var = self.main_window.shared_blob_container
        self.blob_auth_mode_var = self.main_window.shared_blob_auth_mode
        self.blob_account_url_var = self.main_window.shared_blob_account_url

        # listbox-label -> real blob path (set in _list_restore_backups)
        self._restore_label_to_path: dict = {}

        # Masked blob connection entries (shared var; may be built twice — backup + restore panes)
        self._blob_conn_entry_widgets: list = []
        self.blob_conn_show_plain = tk.BooleanVar(value=False)

        # .bak to Blob: Step 1 must be validated before Browse Azure / backup
        self._bak_step1_validated = False
        self._bak_server_caps: Optional[Dict[str, Any]] = None

        self._create_widgets()

    def set_project_path(self, project_path):
        """Set the current project path."""
        self.project_path = project_path

    def _create_widgets(self):
        """Create notebook with .bak to Blob and Restore from Blob sub-tabs (scrollable)."""
        canvas = tk.Canvas(self.frame)
        scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )

        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        def _on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)

        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)

        bind_canvas_vertical_scroll(canvas, scrollable_frame)

        title_label = tk.Label(
            scrollable_frame,
            text="Backup & Restore (.bak <-> Azure Blob)",
            font=("Arial", 16, "bold"),
        )
        title_label.pack(pady=10)

        notebook = ttk.Notebook(scrollable_frame)
        notebook.pack(fill=tk.X, padx=10, pady=10)

        bak_frame = ttk.Frame(notebook)
        notebook.add(bak_frame, text=".bak to Blob (Direct)")
        self._create_bak_to_blob_widgets(bak_frame)

        local_backup_frame = ttk.Frame(notebook)
        notebook.add(local_backup_frame, text="Local Backup")
        self._create_local_backup_widgets(local_backup_frame)

        restore_disk_frame = ttk.Frame(notebook)
        notebook.add(restore_disk_frame, text="Restore from Disk")
        self._create_restore_from_disk_widgets(restore_disk_frame)

        restore_frame = ttk.Frame(notebook)
        notebook.add(restore_frame, text="Restore from Blob")
        self._create_restore_from_blob_widgets(restore_frame)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _create_bak_to_blob_widgets(self, parent):
        """On-prem .bak backup to Azure Blob (BACKUP TO URL)."""
        tk.Label(parent, text=".bak Backup to Azure Blob (on-prem to blob)", font=("Arial", 12, "bold")).pack(
            pady=(0, 10)
        )
        tk.Label(
            parent,
            text="Full database backup (including data) to blob. Folder: container / db_name / run_id / db_name.bak",
            fg="gray",
            wraplength=600,
        ).pack(anchor=tk.W, pady=(0, 5))
        tk.Label(
            parent,
            text="RoundhouseE: Backup includes all data. To skip RoundhouseE, drop that schema after restore.",
            fg="gray",
            wraplength=600,
        ).pack(anchor=tk.W, pady=(0, 10))

        step1 = ttk.LabelFrame(parent, text="Step 1: On-prem source database", padding=10)
        step1.pack(fill=tk.X, padx=5, pady=5)
        self.bak_server_var = self.main_window.shared_src_server
        self.bak_db_var = self.main_window.shared_src_db
        self.bak_auth_var = tk.StringVar(value="windows")
        self.bak_user_var = tk.StringVar()
        self.bak_password_var = tk.StringVar()
        self.bak_conn_widget = ConnectionWidget(
            parent=step1,
            server_var=self.bak_server_var,
            db_var=self.bak_db_var,
            auth_var=self.bak_auth_var,
            user_var=self.bak_user_var,
            password_var=self.bak_password_var,
            label_text="",
            row_start=0,
        )

        # ConnectionWidget uses grid on ``step1``; all siblings must use grid too (not pack).
        _r1 = self.bak_conn_widget.grid_last_row + 1
        step1.columnconfigure(0, weight=0)
        step1.columnconfigure(1, weight=1)

        step1_actions = ttk.Frame(step1)
        step1_actions.grid(row=_r1, column=0, columnspan=2, sticky=tk.EW, pady=(10, 0))
        self.bak_validate_step1_btn = ttk.Button(
            step1_actions,
            text="Validate Step 1",
            command=self._validate_bak_step1,
        )
        self.bak_validate_step1_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.bak_change_step1_btn = ttk.Button(
            step1_actions,
            text="Change connection",
            command=self._unlock_bak_step1,
            state=tk.DISABLED,
        )
        self.bak_change_step1_btn.pack(side=tk.LEFT)
        
        self.bak_step1_status = tk.Label(
            step1_actions, text="", fg="green", font=("Segoe UI", 9)
        )
        self.bak_step1_status.pack(side=tk.LEFT, padx=(12, 0))

        step2 = ttk.LabelFrame(parent, text="Step 2: Azure Blob storage", padding=10)
        step2.pack(fill=tk.X, padx=5, pady=5)
        browse_row = ttk.Frame(step2)
        browse_row.pack(fill=tk.X, pady=(0, 4))
        tk.Label(
            browse_row,
            text="Pick storage after Step 1 is validated.",
            fg="gray",
        ).pack(side=tk.LEFT)
        self.bak_browse_azure_btn = ttk.Button(
            browse_row,
            text="Browse Azure...",
            command=self._open_azure_blob_browser_for_backup,
            state=tk.DISABLED,
        )
        self.bak_browse_azure_btn.pack(side=tk.RIGHT)

        self.bak_blob_conn_var = self.blob_conn_var
        self.bak_container_var = self.blob_container_var
        self._create_blob_auth_widgets(
            step2,
            save_command=self._save_bak_blob_settings,
            prefix="bak",
            show_browse_azure=False,
        )

        # Step 3: backup options (stripes)
        step3 = ttk.LabelFrame(parent, text="Step 3: Backup options", padding=10)
        step3.pack(fill=tk.X, padx=5, pady=5)
        stripes_row = ttk.Frame(step3)
        stripes_row.pack(fill=tk.X)
        tk.Label(stripes_row, text="Stripes (parallel .bak files in blob):").pack(side=tk.LEFT)
        self.bak_stripes_var = tk.StringVar(value="Auto")
        self.bak_stripes_combo = ttk.Combobox(
            stripes_row,
            textvariable=self.bak_stripes_var,
            values=["Auto", "1", "2", "4", "8", "16", "32"],
            width=8,
            state="readonly",
        )
        self.bak_stripes_combo.pack(side=tk.LEFT, padx=(8, 0))
        tk.Label(
            step3,
            text=(
                "Auto picks 1 stripe for DBs < 50 GB, more for larger ones (~150 GB / stripe). "
                "Striping avoids the per-blob 50,000-block limit (error 3203 / 1117) and "
                "speeds up large backups via parallel streams."
            ),
            fg="gray",
            wraplength=700,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(6, 0))

        btn_frame = ttk.Frame(parent)
        btn_frame.pack(pady=10)
        self.bak_to_blob_btn = ttk.Button(
            btn_frame, text="Start .bak Backup to Blob", command=self._start_bak_to_blob, width=25
        )
        self.bak_to_blob_btn.pack(side=tk.LEFT, padx=5)

        log_frame = ttk.LabelFrame(parent, text="Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.bak_to_blob_log = scrolledtext.ScrolledText(log_frame, height=10, wrap=tk.WORD)
        self.bak_to_blob_log.pack(fill=tk.BOTH, expand=True)

    def _create_local_backup_widgets(self, parent):
        """Local backup to disk + optional upload to Azure Blob."""
        tk.Label(
            parent,
            text="Local Backup (with Optional Cloud Upload)",
            font=("Arial", 12, "bold")
        ).pack(pady=(0, 10))
        tk.Label(
            parent,
            text="BACKUP TO DISK writes on the SQL Server host (drive or UNC). Optionally upload the .bak to Azure Blob.",
            fg="gray",
            wraplength=600,
        ).pack(anchor=tk.W, pady=(0, 5))
        tk.Label(
            parent,
            text="Use when: you need a server-side .bak, compliance, or network limits on direct cloud backup.",
            fg="gray",
            wraplength=600,
        ).pack(anchor=tk.W, pady=(0, 10))

        # Step 1: Source database
        step1 = ttk.LabelFrame(parent, text="Step 1: Source SQL Server database", padding=10)
        step1.pack(fill=tk.X, padx=5, pady=5)
        
        self.local_server_var = self.main_window.shared_src_server
        self.local_db_var = self.main_window.shared_src_db
        self.local_auth_var = tk.StringVar(value="windows")
        self.local_user_var = tk.StringVar()
        self.local_password_var = tk.StringVar()
        
        self.local_conn_widget = ConnectionWidget(
            parent=step1,
            server_var=self.local_server_var,
            db_var=self.local_db_var,
            auth_var=self.local_auth_var,
            user_var=self.local_user_var,
            password_var=self.local_password_var,
            label_text="",
            row_start=0,
        )

        # Step 2: Backup folder on the SQL Server host (not necessarily this PC)
        step2 = ttk.LabelFrame(parent, text="Step 2: Backup folder or full .bak path (SQL Server / UNC)", padding=10)
        step2.pack(fill=tk.X, padx=5, pady=5)
        tk.Label(
            step2,
            text="Enter a folder the SQL Server instance can write to, or a full path ending in .bak for a fixed filename "
            "(e.g. \\\\fileserver\\share\\MyDb.bak — matches BACKUP ... TO DISK). "
            "Use \"Use SQL Server Default\" for the server's backup folder.",
            fg="gray",
            wraplength=650,
        ).pack(anchor=tk.W, pady=(0, 6))
        
        path_row = ttk.Frame(step2)
        path_row.pack(fill=tk.X)
        tk.Label(path_row, text="Backup folder or .bak file:").pack(side=tk.LEFT)
        
        self.local_backup_path_var = tk.StringVar(value="")
        
        ttk.Entry(path_row, textvariable=self.local_backup_path_var, width=45).pack(
            side=tk.LEFT, padx=(8, 4)
        )
        ttk.Button(path_row, text="Browse...", command=self._browse_local_backup_path, width=10).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(path_row, text="Use SQL Server Default", command=self._use_sql_default_backup_path, width=20).pack(side=tk.LEFT)

        capacity_row = ttk.Frame(step2)
        capacity_row.pack(fill=tk.X, pady=(6, 0))
        self.local_check_capacity_btn = ttk.Button(
            capacity_row,
            text="Check Path Capacity",
            command=self._check_local_backup_capacity,
            width=20,
        )
        self.local_check_capacity_btn.pack(side=tk.LEFT)
        self.local_prepare_folder_btn = ttk.Button(
            capacity_row,
            text="Create folder + test write",
            command=self._prepare_local_backup_folder_test,
            width=22,
        )
        self.local_prepare_folder_btn.pack(side=tk.LEFT, padx=(8, 0))
        tk.Label(
            capacity_row,
            text="Prepare: nested mkdir + probe write (+ icacls Everyone on folder)",
            fg="gray",
        ).pack(side=tk.LEFT, padx=(10, 0))

        capacity_hint = ttk.Frame(step2)
        capacity_hint.pack(fill=tk.X, pady=(2, 0))
        tk.Label(
            capacity_hint,
            text="Check Path Capacity: free space from this PC when the path is reachable; backup size from SQL (msdb) when server + database are set.",
            fg="gray",
            wraplength=650,
        ).pack(anchor=tk.W)
        
        tk.Label(
            step2,
            text="⚠️ The SQL Server service account needs write access. For a folder, the app can create it and set permissions; "
            "for a UNC .bak path, ensure the share is writable from the server even if this PC cannot see it.",
            fg="orange",
            wraplength=650,
        ).pack(anchor=tk.W, pady=(4, 0))
        
        options_row = ttk.Frame(step2)
        options_row.pack(fill=tk.X, pady=(8, 0))
        self.local_compression_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            options_row,
            text="Use backup compression",
            variable=self.local_compression_var
        ).pack(side=tk.LEFT, padx=(0, 16))
        
        self.local_delete_after_upload_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            options_row,
            text="Delete local file after upload",
            variable=self.local_delete_after_upload_var
        ).pack(side=tk.LEFT)
        
        self.local_skip_upload_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            options_row,
            text="Skip cloud upload (local-only backup)",
            variable=self.local_skip_upload_var,
            command=self._toggle_local_blob_section
        ).pack(side=tk.LEFT, padx=(16, 0))

        # Step 3: Azure Blob destination (initially hidden if skip_upload is True)
        self.local_step3_frame = ttk.LabelFrame(parent, text="Step 3: Azure Blob destination", padding=10)
        self.local_step3_frame.pack(fill=tk.X, padx=5, pady=5)
        self.local_step3_frame.pack_forget()  # Hide initially
        
        self._create_blob_auth_widgets(
            self.local_step3_frame,
            save_command=self._save_bak_blob_settings,
            prefix="local",
            show_browse_azure=True,
        )
        
        folder_row = ttk.Frame(self.local_step3_frame)
        folder_row.pack(fill=tk.X, pady=(8, 0))
        tk.Label(folder_row, text="Blob folder path (optional):").pack(side=tk.LEFT)
        self.local_blob_folder_var = tk.StringVar(value="backups")
        ttk.Entry(folder_row, textvariable=self.local_blob_folder_var, width=40).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        tk.Label(
            self.local_step3_frame,
            text="Optional folder/prefix in container (e.g., 'backups/prod'). Leave empty to upload to container root.",
            fg="gray",
            wraplength=650,
        ).pack(anchor=tk.W, pady=(4, 0))

        # Start button
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(pady=10)
        self.local_backup_btn = ttk.Button(
            btn_frame,
            text="Start Local Backup + Upload",
            command=self._start_local_backup_and_upload,
            width=30
        )
        self.local_backup_btn.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            btn_frame,
            text="Open Backup Folder",
            command=self._open_local_backup_folder,
            width=20
        ).pack(side=tk.LEFT, padx=5)

        # Log
        log_frame = ttk.LabelFrame(parent, text="Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.local_backup_log = scrolledtext.ScrolledText(log_frame, height=10, wrap=tk.WORD)
        self.local_backup_log.pack(fill=tk.BOTH, expand=True)

    def _create_restore_from_disk_widgets(self, parent):
        """Restore database from local or network .bak file."""
        tk.Label(parent, text="Restore from Local/Network Disk", font=("Arial", 12, "bold")).pack(
            pady=(0, 10)
        )
        tk.Label(
            parent,
            text="Restore a SQL Server database from a .bak file on local disk or network share.\n"
                 "Works with files from 'Local Backup' tab or SQL Server's default backup location.",
            wraplength=700,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, padx=5, pady=(0, 10))

        # Step 1: Target SQL Server
        step1 = ttk.LabelFrame(parent, text="Step 1: Target SQL Server", padding=10)
        step1.pack(fill=tk.X, padx=5, pady=5)
        
        # Create connection variables
        self.restore_disk_server_var = tk.StringVar()
        self.restore_disk_db_var = tk.StringVar()  # Not used for database selection, but required
        self.restore_disk_auth_var = tk.StringVar(value="windows")
        self.restore_disk_user_var = tk.StringVar()
        self.restore_disk_password_var = tk.StringVar()
        
        self.restore_disk_conn_widget = ConnectionWidget(
            parent=step1,
            server_var=self.restore_disk_server_var,
            db_var=self.restore_disk_db_var,
            auth_var=self.restore_disk_auth_var,
            user_var=self.restore_disk_user_var,
            password_var=self.restore_disk_password_var,
            label_text="",
            row_start=0,
        )

        # Step 2: Backup File Source
        step2 = ttk.LabelFrame(parent, text="Step 2: Backup File", padding=10)
        step2.pack(fill=tk.X, padx=5, pady=5)
        
        # Option 1: Browse for file
        browse_frame = ttk.Frame(step2)
        browse_frame.pack(fill=tk.X, pady=(0, 10))
        tk.Label(browse_frame, text="Backup file:").pack(side=tk.LEFT)
        self.restore_disk_file_var = tk.StringVar()
        ttk.Entry(browse_frame, textvariable=self.restore_disk_file_var, width=60).pack(
            side=tk.LEFT, padx=(8, 4)
        )
        ttk.Button(browse_frame, text="Browse...", command=self._browse_restore_disk_file, width=10).pack(side=tk.LEFT)
        
        tk.Label(
            step2,
            text="💡 Tip: SQL Server can access network paths like \\\\server\\share\\backup.bak",
            fg="blue",
            wraplength=650,
        ).pack(anchor=tk.W)
        
        # Option 2: Load from SQL Server backup history
        ttk.Separator(step2, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)
        
        history_source_frame = ttk.Frame(step2)
        history_source_frame.pack(fill=tk.X, pady=(0, 6))
        tk.Label(history_source_frame, text="History source server:").pack(side=tk.LEFT)
        self.restore_disk_history_server_var = tk.StringVar()
        ttk.Entry(
            history_source_frame,
            textvariable=self.restore_disk_history_server_var,
            width=42
        ).pack(side=tk.LEFT, padx=(8, 8))
        tk.Label(history_source_frame, text="(optional; defaults to target server)", fg="gray").pack(side=tk.LEFT)

        history_filter_frame = ttk.Frame(step2)
        history_filter_frame.pack(fill=tk.X, pady=(0, 6))
        tk.Label(history_filter_frame, text="History DB filter:").pack(side=tk.LEFT)
        self.restore_disk_history_db_filter_var = tk.StringVar()
        ttk.Entry(
            history_filter_frame,
            textvariable=self.restore_disk_history_db_filter_var,
            width=42
        ).pack(side=tk.LEFT, padx=(8, 8))
        tk.Label(history_filter_frame, text="(optional, e.g. MassEditHangFire_UAT)", fg="gray").pack(side=tk.LEFT)

        history_frame = ttk.Frame(step2)
        history_frame.pack(fill=tk.X)
        tk.Label(history_frame, text="Or load from SQL Server backup history:").pack(side=tk.LEFT)
        ttk.Button(
            history_frame,
            text="Load Recent Backups",
            command=self._load_backup_history,
            width=20
        ).pack(side=tk.LEFT, padx=(8, 0))
        
        # Backup history listbox
        history_list_frame = ttk.Frame(step2)
        history_list_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        tk.Label(history_list_frame, text="Recent backups:").pack(anchor=tk.W)
        
        list_scroll_frame = ttk.Frame(history_list_frame)
        list_scroll_frame.pack(fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(list_scroll_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.restore_disk_history_list = tk.Listbox(
            list_scroll_frame,
            height=6,
            yscrollcommand=scrollbar.set
        )
        self.restore_disk_history_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.restore_disk_history_list.yview)
        
        self.restore_disk_history_list.bind('<<ListboxSelect>>', self._on_backup_history_select)
        
        # Store backup history data (list index -> full path)
        self.restore_disk_history_data = {}

        # Step 3: Restore Options
        step3 = ttk.LabelFrame(parent, text="Step 3: Restore Options", padding=10)
        step3.pack(fill=tk.X, padx=5, pady=5)
        
        # Target database name
        name_row = ttk.Frame(step3)
        name_row.pack(fill=tk.X, pady=(0, 8))
        tk.Label(name_row, text="Target database name:").pack(side=tk.LEFT)
        self.restore_disk_target_db_var = tk.StringVar()
        ttk.Entry(name_row, textvariable=self.restore_disk_target_db_var, width=30).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        tk.Label(
            name_row,
            text="(Leave empty to use original name from backup)",
            fg="gray"
        ).pack(side=tk.LEFT, padx=(8, 0))
        
        # Replace existing checkbox
        self.restore_disk_replace_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            step3,
            text="Replace existing database (if target exists)",
            variable=self.restore_disk_replace_var
        ).pack(anchor=tk.W, pady=(0, 8))
        
        # Custom file locations (advanced)
        advanced_frame = ttk.LabelFrame(step3, text="Advanced: Custom File Locations (optional)", padding=5)
        advanced_frame.pack(fill=tk.X, pady=(8, 0))
        
        tk.Label(
            advanced_frame,
            text="Leave empty to use SQL Server's default data directory",
            fg="gray"
        ).pack(anchor=tk.W, pady=(0, 5))
        
        # Data file path
        data_row = ttk.Frame(advanced_frame)
        data_row.pack(fill=tk.X, pady=(0, 4))
        tk.Label(data_row, text="Data file (.mdf):").pack(side=tk.LEFT)
        self.restore_disk_data_file_var = tk.StringVar()
        ttk.Entry(data_row, textvariable=self.restore_disk_data_file_var, width=50).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        
        # Log file path
        log_row = ttk.Frame(advanced_frame)
        log_row.pack(fill=tk.X)
        tk.Label(log_row, text="Log file (.ldf):").pack(side=tk.LEFT)
        self.restore_disk_log_file_var = tk.StringVar()
        ttk.Entry(log_row, textvariable=self.restore_disk_log_file_var, width=50).pack(
            side=tk.LEFT, padx=(8, 0)
        )

        # Start restore button
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, padx=5, pady=10)
        
        self.restore_disk_btn = ttk.Button(
            btn_frame,
            text="Start Restore",
            command=self._start_restore_from_disk,
            width=20
        )
        self.restore_disk_btn.pack(side=tk.LEFT, padx=5)

        # Log
        log_frame = ttk.LabelFrame(parent, text="Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.restore_disk_log = scrolledtext.ScrolledText(log_frame, height=10, wrap=tk.WORD)
        self.restore_disk_log.pack(fill=tk.BOTH, expand=True)

    # ------------------------------------------------------------------ #
    # Shared blob-auth widget builder
    # ------------------------------------------------------------------ #
    def _sync_blob_conn_show(self):
        """Toggle masking for all blob connection string entries (shared StringVar)."""
        show = "" if self.blob_conn_show_plain.get() else "*"
        for w in self._blob_conn_entry_widgets:
            try:
                w.config(show=show)
            except Exception:
                pass

    def _create_blob_auth_widgets(
        self,
        parent,
        *,
        save_command,
        prefix: str,
        show_browse_azure: bool = True,
    ):
        """
        Build the blob storage auth section inside `parent` (a LabelFrame).
        Radio buttons toggle between Connection String (storage account key) and Managed Identity.

        ``prefix`` must be ``bak`` or ``restore``. Each section keeps its own
        frame refs so toggling updates BOTH tabs (backup and restore share the
        same StringVars but must not overwrite each other's widgets).

        When ``show_browse_azure`` is False (``.bak to Blob`` tab), omit Browse here;
        that tab uses ``bak_browse_azure_btn`` after Step 1 validation.
        """
        # Auth-mode radios (only one row needed — shared vars; duplicate radios are OK)
        mode_row = ttk.Frame(parent)
        mode_row.pack(fill=tk.X, pady=(0, 6))
        tk.Label(mode_row, text="Auth mode:").pack(side=tk.LEFT)
        rb_conn = ttk.Radiobutton(
            mode_row,
            text="Connection String (storage account key)",
            variable=self.blob_auth_mode_var, value="connection_string",
            command=self._on_blob_auth_mode_change,
        )
        rb_conn.pack(side=tk.LEFT, padx=(8, 0))
        rb_mi = ttk.Radiobutton(
            mode_row, text="Managed Identity",
            variable=self.blob_auth_mode_var, value="managed_identity",
            command=self._on_blob_auth_mode_change,
        )
        rb_mi.pack(side=tk.LEFT, padx=(12, 0))
        setattr(self, f"_{prefix}_rb_conn", rb_conn)
        setattr(self, f"_{prefix}_rb_mi", rb_mi)
        if show_browse_azure:
            ttk.Button(
                mode_row,
                text="Browse Azure...",
                command=self._open_azure_blob_browser,
            ).pack(side=tk.RIGHT)

        # Swap area: only one of conn_str vs MI visible at a time (per section)
        cred_swap = ttk.Frame(parent)
        cred_swap.pack(fill=tk.X)

        conn_f = ttk.Frame(cred_swap)
        tk.Label(
            conn_f,
            text="Azure storage connection string (masked by default; not echoed in logs):",
        ).pack(anchor=tk.W)
        ent = tk.Entry(conn_f, textvariable=self.blob_conn_var, width=70, show="*")
        ent.pack(fill=tk.X, pady=2)
        self._blob_conn_entry_widgets.append(ent)
        show_row = ttk.Frame(conn_f)
        show_row.pack(anchor=tk.W, pady=(0, 2))
        ttk.Checkbutton(
            show_row,
            text="Show connection string (sensitive — avoid on shared screens)",
            variable=self.blob_conn_show_plain,
            command=self._sync_blob_conn_show,
        ).pack(side=tk.LEFT)
        tk.Label(
            conn_f,
            text=(
                "The storage account key in this string does not rotate when you run backup; "
                "backup uses a short-lived SAS. Logs and error dialogs redact keys and secrets."
            ),
            fg="gray",
            wraplength=700,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(2, 0))

        mi_f = ttk.Frame(cred_swap)
        tk.Label(
            mi_f,
            text="Storage account URL (you can include /container at the end):",
        ).pack(anchor=tk.W)
        tk.Label(
            mi_f,
            text=(
                "Examples:\n"
                "  https://myaccount.blob.core.windows.net/sqlbackups   (container in URL — container field below can be empty)\n"
                "  https://myaccount.blob.core.windows.net               (then fill the container field below)"
            ),
            fg="gray",
            wraplength=700,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(0, 2))
        ttk.Entry(mi_f, textvariable=self.blob_account_url_var, width=70).pack(fill=tk.X, pady=2)
        tk.Label(
            mi_f,
            text=(
                "Requires: SQL Server 2022 on Azure VM / Azure SQL MI / Arc-enabled SQL 2022. "
                "Earlier versions (SQL 2016/2017/2019) do NOT support Managed Identity for BACKUP TO URL — "
                "use Connection String (account key) mode for those. "
                "The host MI must have 'Storage Blob Data Contributor' on the container."
            ),
            fg="gray", wraplength=700, justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(2, 0))

        setattr(self, f"_{prefix}_cred_swap", cred_swap)
        setattr(self, f"_{prefix}_conn_str_frame", conn_f)
        setattr(self, f"_{prefix}_mi_frame", mi_f)

        container_label = tk.Label(parent, text="Container name:")
        container_label.pack(anchor=tk.W, pady=(8, 0))
        ttk.Entry(parent, textvariable=self.blob_container_var, width=30).pack(anchor=tk.W, pady=2)
        container_hint = tk.Label(
            parent,
            text="",
            fg="gray",
            wraplength=700,
            justify=tk.LEFT,
        )
        container_hint.pack(anchor=tk.W, pady=(0, 2))

        setattr(self, f"_{prefix}_container_label", container_label)
        setattr(self, f"_{prefix}_container_hint", container_hint)

        # Save button
        btn_row = ttk.Frame(parent)
        btn_row.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(btn_row, text="Save settings", command=save_command).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_row, text="Clear settings", command=self._clear_blob_settings).pack(side=tk.LEFT, padx=5)

        # Apply initial visibility for this section
        self._apply_blob_auth_visibility(prefix)

    def _apply_blob_auth_visibility(self, prefix: str) -> None:
        """Show connection-string OR MI fields for one blob-auth section."""
        conn_f = getattr(self, f"_{prefix}_conn_str_frame", None)
        mi_f = getattr(self, f"_{prefix}_mi_frame", None)
        if not conn_f or not mi_f:
            return
        mode = self.blob_auth_mode_var.get()
        if mode == "managed_identity":
            conn_f.pack_forget()
            mi_f.pack(fill=tk.X)
        else:
            mi_f.pack_forget()
            conn_f.pack(fill=tk.X)

        container_label = getattr(self, f"_{prefix}_container_label", None)
        container_hint = getattr(self, f"_{prefix}_container_hint", None)
        if container_label is not None and container_hint is not None:
            if mode == "managed_identity":
                container_label.config(text="Container name (optional if included in URL):")
                container_hint.config(
                    text=(
                        "Leave empty if the storage URL above already ends with /<container>. "
                        "Otherwise, type the container name here."
                    ),
                )
            else:
                container_label.config(text="Container name:")
                container_hint.config(text="Required in Connection String mode.")

    def _on_blob_auth_mode_change(self, *_):
        """Show/hide credential fields in every blob-auth section (backup + restore)."""
        for prefix in ("bak", "restore"):
            self._apply_blob_auth_visibility(prefix)

    def _apply_bak_blob_auth_locks(self, *, mi_allowed: bool) -> None:
        """After Step 1 validation: allow both blob auth modes, or only connection string."""
        rb_conn = getattr(self, "_bak_rb_conn", None)
        rb_mi = getattr(self, "_bak_rb_mi", None)
        if rb_conn is None or rb_mi is None:
            return
        if mi_allowed:
            rb_conn.configure(state=tk.NORMAL)
            rb_mi.configure(state=tk.NORMAL)
        else:
            self.blob_auth_mode_var.set("connection_string")
            rb_conn.configure(state=tk.NORMAL)
            rb_mi.configure(state="disabled")
        self._on_blob_auth_mode_change()

    def _validate_bak_step1(self) -> None:
        if self._bak_step1_validated:
            return
        server = (self.bak_server_var.get() or "").strip()
        database = (self.bak_db_var.get() or "").strip()
        if not server or not database:
            messagebox.showerror("Step 1", "Enter server and database before validating.")
            return
        if self.bak_conn_widget.db_type_var.get() != "sqlserver":
            messagebox.showerror(
                "Step 1",
                ".bak backup to Azure Blob requires Database Type: SQL Server / Azure SQL.",
            )
            return
        auth = self.bak_auth_var.get() or "windows"
        if auth == "sql" and not (self.bak_user_var.get() or "").strip():
            messagebox.showerror("Step 1", "SQL authentication requires a user name.")
            return

        self.bak_validate_step1_btn.configure(state=tk.DISABLED)

        def run() -> None:
            caps: Dict[str, Any] = {}
            err: Optional[str] = None
            try:
                try:
                    from src.utils.database import connect_to_database, pick_sql_driver
                except ImportError:
                    from azure_migration_tool.src.utils.database import (
                        connect_to_database,
                        pick_sql_driver,
                    )
                import logging

                log = logging.getLogger(__name__)
                driver = pick_sql_driver(log)
                conn = connect_to_database(
                    server=server,
                    db="master",
                    user=self.bak_user_var.get() or "",
                    driver=driver,
                    auth=auth,
                    password=self.bak_password_var.get() or None,
                    timeout=60,
                    logger=log,
                )
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT),
                           CAST(SERVERPROPERTY('ProductMinorVersion') AS INT),
                           CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256)),
                           CAST(@@SERVERNAME AS NVARCHAR(256))
                    """
                )
                row = cur.fetchone()
                maj = int(row[0]) if row and row[0] is not None else 0
                minor = int(row[1]) if row and row[1] is not None else 0
                edition = (row[2] or "").strip() if row else ""
                inst = (row[3] or "").strip() if row else ""
                cur.close()
                conn.close()
                caps = {
                    "major": maj,
                    "minor": minor,
                    "edition": edition,
                    "instance": inst,
                    "server_input": server,
                    "database": database,
                    "supports_sas_backup_to_url": maj >= 11,
                    "supports_mi_backup_to_url": maj >= 16,
                }
            except Exception as exc:
                err = str(exc)

            self.frame.after(0, lambda c=caps, e=err: self._finish_validate_bak_step1(c, e))

        threading.Thread(target=run, daemon=True).start()

    def _finish_validate_bak_step1(
        self, caps: Dict[str, Any], err: Optional[str]
    ) -> None:
        self.bak_validate_step1_btn.configure(state=tk.NORMAL)
        if err:
            messagebox.showerror("Validation failed", _compact_dialog_error(err))
            return
        if not caps.get("supports_sas_backup_to_url"):
            messagebox.showerror(
                "Unsupported SQL version",
                "BACKUP TO URL requires SQL Server 2012 or later (detected major version < 11).",
            )
            return

        self._bak_server_caps = caps
        self._bak_step1_validated = True
        self.bak_conn_widget.set_connection_fields_locked(True)
        self.bak_validate_step1_btn.configure(state=tk.DISABLED)
        self.bak_change_step1_btn.configure(state=tk.NORMAL)
        self.bak_browse_azure_btn.configure(state=tk.NORMAL)

        mi_ok = bool(caps.get("supports_mi_backup_to_url"))
        self._apply_bak_blob_auth_locks(mi_allowed=mi_ok)
        
        status_msg = f"✓ Connected to {caps.get('instance', caps.get('server_input', 'server'))}"
        if not mi_ok:
            status_msg += " (SQL < 2022: only connection string / account key mode available)"
        self.bak_step1_status.config(text=status_msg)

    def _unlock_bak_step1(self) -> None:
        if not self._bak_step1_validated:
            return
        self._bak_step1_validated = False
        self._bak_server_caps = None
        self.bak_conn_widget.set_connection_fields_locked(False)
        self.bak_validate_step1_btn.configure(state=tk.NORMAL)
        self.bak_change_step1_btn.configure(state=tk.DISABLED)
        self.bak_browse_azure_btn.configure(state=tk.DISABLED)
        self.bak_step1_status.config(text="")
        rb_conn = getattr(self, "_bak_rb_conn", None)
        rb_mi = getattr(self, "_bak_rb_mi", None)
        if rb_conn is not None and rb_mi is not None:
            rb_conn.configure(state=tk.NORMAL)
            rb_mi.configure(state=tk.NORMAL)

    def _open_azure_blob_browser_for_backup(self) -> None:
        if not self._bak_step1_validated:
            messagebox.showwarning(
                "Step 1 required",
                "Validate Step 1 first. Then you can browse Azure storage.",
            )
            return
        mi_allowed = bool(self._bak_server_caps and self._bak_server_caps.get("supports_mi_backup_to_url"))
        self._open_azure_blob_browser(mi_allowed=mi_allowed)

    def _open_azure_blob_browser(self, mi_allowed: bool = True):
        """Open the Subscription/Storage/Container picker dialog."""
        try:
            try:
                from gui.widgets.azure_blob_browser import (
                    AzureBlobBrowser,
                    azure_browse_dependency_error,
                )
            except ImportError:
                from azure_migration_tool.gui.widgets.azure_blob_browser import (
                    AzureBlobBrowser,
                    azure_browse_dependency_error,
                )
        except ImportError as e:
            messagebox.showerror(
                "Missing module",
                "Could not load the Browse Azure dialog.\n\n"
                f"Details: {e}",
            )
            return

        dep_err = azure_browse_dependency_error()
        if dep_err:
            messagebox.showerror("Browse Azure", dep_err)
            return

        def on_apply(*, mode: str, connection_string: str, container: str, account_url: str):
            self.blob_auth_mode_var.set(mode)
            self.blob_container_var.set(container)
            if mode == "managed_identity":
                self.blob_account_url_var.set(account_url)
            else:
                self.blob_conn_var.set(connection_string)
                self.blob_account_url_var.set(account_url)
            self._on_blob_auth_mode_change()

        default_mode = self.blob_auth_mode_var.get() or "managed_identity"
        if not mi_allowed and default_mode == "managed_identity":
            default_mode = "connection_string"
        
        AzureBlobBrowser(
            self.frame,
            on_apply=on_apply,
            default_mode=default_mode,
            mi_allowed=mi_allowed,
        )

    def _save_bak_blob_settings(self):
        """Save current blob auth settings to disk."""
        try:
            try:
                from gui.utils.blob_config import save_blob_settings
            except ImportError:
                from azure_migration_tool.gui.utils.blob_config import save_blob_settings

            save_blob_settings(
                self.blob_conn_var.get(),
                self.blob_container_var.get(),
                blob_auth_mode=self.blob_auth_mode_var.get(),
                storage_account_url=self.blob_account_url_var.get(),
            )
            messagebox.showinfo(
                "Saved",
                "Blob settings saved. They load automatically next app start."
                + (" On Windows the connection string is stored encrypted (DPAPI)." if os.name == "nt" else ""),
            )
        except Exception as e:
            messagebox.showerror("Save failed", _compact_dialog_error(str(e)))

    def _clear_blob_settings(self):
        """Clear saved blob settings and reset form fields."""
        try:
            try:
                from gui.utils.blob_config import clear_blob_settings
            except ImportError:
                from azure_migration_tool.gui.utils.blob_config import clear_blob_settings

            clear_blob_settings()
            
            self.blob_conn_var.set("")
            self.blob_container_var.set("")
            self.blob_account_url_var.set("")
            self.blob_auth_mode_var.set("connection_string")
            self._on_blob_auth_mode_change()
            self.blob_conn_show_plain.set(False)
            self._sync_blob_conn_show()

            messagebox.showinfo(
                "Cleared",
                "Blob settings cleared. Form fields reset to empty.",
            )
        except Exception as e:
            messagebox.showerror("Clear failed", _compact_dialog_error(str(e)))

    def _start_bak_to_blob(self):
        """Run .bak backup to Azure Blob (BACKUP TO URL)."""
        try:
            from src.backup.bak_to_blob import run_bak_backup_to_blob
        except ImportError:
            try:
                from azure_migration_tool.src.backup.bak_to_blob import run_bak_backup_to_blob
            except ImportError:
                run_bak_backup_to_blob = None
        if not run_bak_backup_to_blob:
            messagebox.showerror("Error", "Backup to blob module not available. Install: pip install azure-storage-blob")
            return
        if not self._bak_step1_validated:
            messagebox.showerror(
                "Step 1 not validated",
                'Click "Validate Step 1 & detect capabilities" before starting backup.',
            )
            return
        server = (self.bak_server_var.get() or "").strip()
        database = (self.bak_db_var.get() or "").strip()
        if not server or not database:
            messagebox.showerror("Error", "Server and database are required.")
            return
        conn_str = (self.blob_conn_var.get() or "").strip()
        container = (self.blob_container_var.get() or "").strip()
        blob_auth_mode = self.blob_auth_mode_var.get() or "connection_string"
        storage_account_url = (self.blob_account_url_var.get() or "").strip()
        try:
            container = _resolve_container_for_gui(blob_auth_mode, container, storage_account_url)
        except Exception as e:
            messagebox.showerror("Error", _compact_dialog_error(str(e)))
            return
        if blob_auth_mode == "managed_identity":
            if not storage_account_url:
                messagebox.showerror(
                    "Error",
                    "Storage account URL is required for Managed Identity "
                    "(e.g. https://myaccount.blob.core.windows.net).",
                )
                return
        elif not conn_str:
            messagebox.showerror("Error", "Blob connection string is required.")
            return
        if not container:
            messagebox.showerror(
                "Error",
                "Container name is required. Enter container field or include it in storage URL path.",
            )
            return

        stripes_raw = (self.bak_stripes_var.get() or "Auto").strip()
        if stripes_raw.lower() == "auto":
            stripes_arg = None
        else:
            try:
                stripes_arg = max(1, min(64, int(stripes_raw)))
            except ValueError:
                stripes_arg = None

        self.bak_to_blob_btn.config(state=tk.DISABLED)
        self.bak_to_blob_log.delete("1.0", tk.END)

        def run():
            def log(msg):
                self.bak_to_blob_log.insert(tk.END, msg + "\n")
                self.bak_to_blob_log.see(tk.END)

            try:
                summary = run_bak_backup_to_blob(
                    server=server,
                    database=database,
                    auth=self.bak_auth_var.get() or "windows",
                    user=self.bak_user_var.get() or None,
                    password=self.bak_password_var.get() or None,
                    blob_connection_string=conn_str,
                    container=container,
                    log_callback=log,
                    stripes=stripes_arg,
                    blob_auth_mode=blob_auth_mode,
                    storage_account_url=storage_account_url,
                )
                if summary.get("status") == "success":
                    paths = summary.get("blob_paths") or [summary.get("blob_path")]
                    for p in paths:
                        if p:
                            log(f"Blob path: {summary.get('container')}/{p}")
                    size_mb = summary.get("blob_size")
                    size_msg = f" ({size_mb / (1024*1024):.1f} MB)" if size_mb is not None else ""
                    stripe_msg = f" {summary.get('stripes', 1)} stripe(s)." if summary.get("stripes") else ""
                    self.frame.after(
                        0,
                        lambda m=size_msg, s=stripe_msg: messagebox.showinfo(
                            "Success", f"Backup to blob completed successfully.{s}{m}"
                        ),
                    )
                else:
                    err = summary.get("error") or "Unknown error"
                    self.frame.after(
                        0,
                        lambda e=err: messagebox.showerror("Backup failed", _compact_dialog_error(e)),
                    )
            except Exception as e:
                log(str(e))
                self.frame.after(
                    0,
                    lambda x=str(e): messagebox.showerror("Error", _compact_dialog_error(x)),
                )
            finally:
                self.frame.after(0, lambda: self.bak_to_blob_btn.config(state=tk.NORMAL))

        threading.Thread(target=run, daemon=True).start()

    def _toggle_local_blob_section(self):
        """Show/hide Azure Blob section based on skip_upload checkbox."""
        if self.local_skip_upload_var.get():
            # Local-only mode: hide Azure blob section
            self.local_step3_frame.pack_forget()
        else:
            # Upload mode: show Azure blob section
            self.local_step3_frame.pack(fill=tk.X, padx=5, pady=5)

    def _open_local_backup_folder(self):
        """Open the backup folder in Windows Explorer."""
        import subprocess
        
        folder_path = (self.local_backup_path_var.get() or "").strip()
        if not folder_path:
            messagebox.showwarning(
                "No Path",
                "Enter a backup directory or full .bak path first."
            )
            return
        
        # Expand user path if needed
        folder_path = os.path.expanduser(folder_path)
        if folder_path.lower().endswith(".bak"):
            folder_path = str(Path(folder_path).parent)
            if not folder_path or folder_path == ".":
                messagebox.showwarning("Invalid Path", "Could not determine folder from the .bak path.")
                return
        
        if not os.path.exists(folder_path):
            response = messagebox.askyesno(
                "Folder Not Found",
                f"Folder doesn't exist:\n{folder_path}\n\nCreate it now?"
            )
            if response:
                try:
                    os.makedirs(folder_path, exist_ok=True)
                except Exception as e:
                    messagebox.showerror("Error", f"Could not create folder:\n{_compact_dialog_error(str(e))}")
                    return
            else:
                return
        
        try:
            subprocess.Popen(['explorer', folder_path])
        except Exception as e:
            messagebox.showerror("Error", f"Could not open folder:\n{_compact_dialog_error(str(e))}")

    def _browse_local_backup_path(self):
        """Open file dialog to select local backup directory."""
        from tkinter import filedialog
        
        initial_dir = (self.local_backup_path_var.get() or "").strip()
        if not initial_dir or not os.path.isdir(initial_dir):
            initial_dir = os.environ.get("USERPROFILE") or os.environ.get("SystemDrive", "C:") + "\\"
        directory = filedialog.askdirectory(
            title="Select backup directory (this PC)",
            initialdir=initial_dir
        )
        if directory:
            self.local_backup_path_var.set(directory)
    
    def _use_sql_default_backup_path(self):
        """Query SQL Server for its default backup directory and use it."""
        server = (self.local_server_var.get() or "").strip()
        if not server:
            messagebox.showwarning(
                "Server Required",
                "Enter SQL Server name first, then click this button to detect its default backup path."
            )
            return
        
        def query_sql_default():
            try:
                try:
                    from src.utils.database import connect_to_database
                except ImportError:
                    from azure_migration_tool.src.utils.database import connect_to_database
                
                auth = self.local_auth_var.get() or "windows"
                conn = connect_to_database(
                    server=server,
                    db="master",
                    user=self.local_user_var.get() or "",
                    driver="ODBC Driver 18 for SQL Server",
                    auth=auth,
                    password=self.local_password_var.get() or "",
                    timeout=30,
                    logger=logger,
                )
                
                cur = conn.cursor()
                cur.execute(
                    """
                    DECLARE @BackupDirectory NVARCHAR(512)
                    EXEC master.dbo.xp_instance_regread 
                        N'HKEY_LOCAL_MACHINE',
                        N'Software\\Microsoft\\MSSQLServer\\MSSQLServer',
                        N'BackupDirectory',
                        @BackupDirectory OUTPUT
                    SELECT @BackupDirectory AS BackupDirectory
                    """
                )
                row = cur.fetchone()
                sql_backup_dir = row[0] if row and row[0] else None
                cur.close()
                conn.close()
                
                if sql_backup_dir:
                    self.frame.after(
                        0,
                        lambda path=sql_backup_dir: self._apply_sql_default_path(path)
                    )
                else:
                    self.frame.after(
                        0,
                        lambda: messagebox.showwarning(
                            "Not Found",
                            "Could not detect SQL Server's default backup directory.\n\n"
                            "The registry query might not be available or you lack permissions."
                        )
                    )
            except Exception as e:
                self.frame.after(
                    0,
                    lambda err=_compact_dialog_error(str(e)): messagebox.showerror(
                        "Query Failed",
                        f"Could not query SQL Server:\n\n{err}"
                    )
                )
        
        threading.Thread(target=query_sql_default, daemon=True).start()
    
    def _apply_sql_default_path(self, path: str):
        """Apply SQL Server's default backup path to the UI."""
        self.local_backup_path_var.set(path)
        messagebox.showinfo(
            "Path Updated",
            f"Using SQL Server's default backup directory:\n\n{path}\n\n"
            "This path is guaranteed to have correct permissions."
        )

    def _check_local_backup_capacity(self):
        """Check path reachability and capacity from app host perspective."""
        backup_path = (self.local_backup_path_var.get() or "").strip()
        if not backup_path:
            messagebox.showwarning("Path Required", "Enter a backup directory path first.")
            return

        server = (self.local_server_var.get() or "").strip()
        database = (self.local_db_var.get() or "").strip()
        use_compression = bool(self.local_compression_var.get())

        self.local_check_capacity_btn.config(state=tk.DISABLED)

        def ui_log(msg: str):
            def _append(m: str = msg):
                self.local_backup_log.insert(tk.END, m + "\n")
                self.local_backup_log.see(tk.END)

            self.frame.after(0, _append)

        def run():
            try:
                try:
                    from gui.utils.backup_capacity import run_local_backup_capacity_report
                except ImportError:
                    from azure_migration_tool.gui.utils.backup_capacity import run_local_backup_capacity_report
                try:
                    from src.utils.database import connect_to_database
                except ImportError:
                    from azure_migration_tool.src.utils.database import connect_to_database

                run_local_backup_capacity_report(
                    backup_path=backup_path,
                    server=server,
                    database=database,
                    auth=self.local_auth_var.get() or "windows",
                    user=self.local_user_var.get() or "",
                    password=self.local_password_var.get() or "",
                    use_compression=use_compression,
                    log=ui_log,
                    connect_to_database=connect_to_database,
                    logger=logger,
                )
            finally:
                self.frame.after(0, lambda: self.local_check_capacity_btn.config(state=tk.NORMAL))

        threading.Thread(target=run, daemon=True).start()

    def _prepare_local_backup_folder_test(self):
        """Create backup folder (and parents) on this PC, probe write, optional icacls — same idea as pre-backup prep."""
        backup_path = (self.local_backup_path_var.get() or "").strip()
        if not backup_path:
            messagebox.showwarning("Path Required", "Enter a backup folder or full .bak path first.")
            return

        self.local_prepare_folder_btn.config(state=tk.DISABLED)

        def ui_log(msg: str):
            def _append(m: str = msg):
                self.local_backup_log.insert(tk.END, m + "\n")
                self.local_backup_log.see(tk.END)

            self.frame.after(0, _append)

        def run():
            try:
                try:
                    from src.backup.local_backup_and_upload import ensure_dir_and_probe_write
                except ImportError:
                    from azure_migration_tool.src.backup.local_backup_and_upload import ensure_dir_and_probe_write

                result = ensure_dir_and_probe_write(
                    backup_path,
                    apply_icacls_everyone=True,
                    log=ui_log,
                )

                def finish():
                    self.local_prepare_folder_btn.config(state=tk.NORMAL)
                    if result.get("success"):
                        messagebox.showinfo(
                            "Create folder + test write",
                            result.get("message", "OK"),
                        )
                    else:
                        messagebox.showerror(
                            "Create folder + test write",
                            _compact_dialog_error(result.get("message", "Unknown error")),
                        )

                self.frame.after(0, finish)
            except Exception as e:
                err = _compact_dialog_error(str(e))

                def finish_err():
                    self.local_prepare_folder_btn.config(state=tk.NORMAL)
                    messagebox.showerror("Create folder + test write", err)

                self.frame.after(0, finish_err)

        threading.Thread(target=run, daemon=True).start()

    def _start_local_backup_and_upload(self):
        """Run local backup + optional upload to blob."""
        try:
            from src.backup.local_backup_and_upload import run_local_backup_and_upload
        except ImportError:
            try:
                from azure_migration_tool.src.backup.local_backup_and_upload import run_local_backup_and_upload
            except ImportError:
                run_local_backup_and_upload = None
        
        if not run_local_backup_and_upload:
            messagebox.showerror(
                "Error",
                "Local backup module not available. Ensure azure-storage-blob is installed."
            )
            return
        
        # Validate inputs
        server = (self.local_server_var.get() or "").strip()
        database = (self.local_db_var.get() or "").strip()
        local_path = (self.local_backup_path_var.get() or "").strip()
        skip_upload = self.local_skip_upload_var.get()
        
        if not server or not database:
            messagebox.showerror("Error", "Server and database are required.")
            return
        
        if not local_path:
            messagebox.showerror("Error", "Local backup path is required.")
            return
        
        # Validate blob settings only if uploading
        blob_auth_mode = self.blob_auth_mode_var.get() or "connection_string"
        conn_str = (self.blob_conn_var.get() or "").strip()
        storage_account_url = (self.blob_account_url_var.get() or "").strip()
        container = (self.blob_container_var.get() or "").strip()
        blob_folder = (self.local_blob_folder_var.get() or "").strip()
        
        if not skip_upload:
            if blob_auth_mode == "managed_identity":
                if not storage_account_url:
                    messagebox.showerror(
                        "Error",
                        "Storage account URL is required for Managed Identity (or enable 'Skip cloud upload')."
                    )
                    return
            elif not conn_str:
                messagebox.showerror("Error", "Blob connection string is required (or enable 'Skip cloud upload').")
                return
            
            if not container:
                messagebox.showerror("Error", "Container name is required (or enable 'Skip cloud upload').")
                return
        
        # Disable button and clear log
        self.local_backup_btn.config(state=tk.DISABLED)
        self.local_backup_log.delete("1.0", tk.END)
        
        def run():
            def log(msg):
                self.local_backup_log.insert(tk.END, msg + "\n")
                self.local_backup_log.see(tk.END)
            
            try:
                result = run_local_backup_and_upload(
                    server=server,
                    database=database,
                    auth=self.local_auth_var.get() or "windows",
                    user=self.local_user_var.get() or "",
                    password=self.local_password_var.get() or "",
                    local_backup_path=local_path,
                    blob_auth_mode=blob_auth_mode,
                    blob_connection_string=conn_str,
                    blob_account_url=storage_account_url,
                    blob_container=container,
                    blob_folder=blob_folder,
                    delete_local_after_upload=self.local_delete_after_upload_var.get(),
                    compression=self.local_compression_var.get(),
                    skip_upload=skip_upload,
                    log=log,
                )
                
                if result.get("success"):
                    local_file = result.get('local_file', 'N/A')
                    is_network = local_file.startswith("\\\\") or local_file.startswith("//")
                    
                    if skip_upload:
                        msg = (
                            f"Local backup completed!\n\n"
                            f"Time: {result.get('backup_time_sec', 0):.1f}s\n"
                            f"File: {local_file}"
                        )
                        if is_network:
                            msg += "\n\nNote: Backup saved to network share (UNC path).\nSQL Server has access, but you may need network permissions to access it."
                    else:
                        upload_time = result.get('upload_time_sec', 0)
                        if upload_time > 0:
                            msg = (
                                f"Backup and upload completed!\n\n"
                                f"Backup time: {result.get('backup_time_sec', 0):.1f}s\n"
                                f"Upload time: {upload_time:.1f}s\n"
                                f"Total: {result.get('backup_time_sec', 0) + upload_time:.1f}s\n\n"
                                f"Blob URL: {result.get('blob_url', 'N/A')}"
                            )
                        else:
                            # Upload was skipped (network path)
                            msg = (
                                f"Backup completed (upload skipped)!\n\n"
                                f"Time: {result.get('backup_time_sec', 0):.1f}s\n"
                                f"File: {local_file}\n\n"
                                f"Note: Network paths (UNC) cannot be uploaded by Python.\n"
                                f"Use a local path (e.g., C:\\Temp) if cloud upload is needed."
                            )
                    
                    self.frame.after(0, lambda m=msg: messagebox.showinfo("Success", m))
                else:
                    self.frame.after(
                        0,
                        lambda: messagebox.showerror(
                            "Failed",
                            result.get("message", "Unknown error")
                        )
                    )
            except Exception as e:
                log(f"ERROR: {str(e)}")
                self.frame.after(
                    0,
                    lambda: messagebox.showerror("Error", _compact_dialog_error(str(e)))
                )
            finally:
                self.frame.after(0, lambda: self.local_backup_btn.config(state=tk.NORMAL))
        
        threading.Thread(target=run, daemon=True).start()

    def _create_restore_from_blob_widgets(self, parent):
        """Restore database from Azure Blob (.bak)."""
        tk.Label(parent, text="Restore from Azure Blob (.bak to SQL Server)", font=("Arial", 12, "bold")).pack(
            pady=(0, 5)
        )
        tk.Label(
            parent,
            text="Pick the database (backed-up name), then list backups for that DB only. Select one and restore to target server.",
            fg="gray",
            wraplength=600,
        ).pack(anchor=tk.W, pady=(0, 10))

        step1 = ttk.LabelFrame(parent, text="Step 1: Azure Blob storage", padding=10)
        step1.pack(fill=tk.X, padx=5, pady=5)
        self.restore_blob_conn_var = self.blob_conn_var
        self.restore_container_var = self.blob_container_var
        self._create_blob_auth_widgets(
            step1,
            save_command=self._save_restore_blob_settings,
            prefix="restore",
        )

        tk.Label(
            step1,
            text="Database to restore (backed-up name, e.g. SentimentAnalysis_QA):",
        ).pack(anchor=tk.W, pady=(12, 0))
        db_filter_row = ttk.Frame(step1)
        db_filter_row.pack(fill=tk.X, pady=2)
        self.restore_db_filter_var = tk.StringVar()
        self.restore_db_filter_combo = ttk.Combobox(db_filter_row, textvariable=self.restore_db_filter_var, width=40)
        self.restore_db_filter_combo.pack(side=tk.LEFT, padx=(0, 5))
        self.restore_db_filter_combo.bind("<<ComboboxSelected>>", self._on_restore_db_filter_selected)
        ttk.Button(db_filter_row, text="List databases", command=self._list_restore_databases).pack(
            side=tk.LEFT, padx=2
        )
        tk.Label(
            step1,
            text="(Lists top-level folders in container; pick one to see only that database's backups.)",
            fg="gray",
        ).pack(anchor=tk.W, pady=(0, 4))

        tk.Label(step1, text="Backups for this database (pick one):").pack(anchor=tk.W, pady=(8, 0))
        tk.Label(
            step1,
            text=(
                "Striped backups appear as ONE row tagged [N/M stripe(s), total ...]. "
                "Pick that single row and the tool restores from all stripes automatically."
            ),
            fg="gray",
            wraplength=700,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(0, 4))
        list_frame = ttk.Frame(step1)
        list_frame.pack(fill=tk.X, pady=2)
        self.restore_backups_listbox = tk.Listbox(list_frame, height=6, width=70, selectmode=tk.SINGLE)
        scroll_list = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.restore_backups_listbox.yview)
        self.restore_backups_listbox.configure(yscrollcommand=scroll_list.set)
        self.restore_backups_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_list.pack(side=tk.RIGHT, fill=tk.Y)
        self.restore_backups_listbox.bind("<<ListboxSelect>>", self._on_restore_backup_selected)
        btn_list_frame = ttk.Frame(step1)
        btn_list_frame.pack(fill=tk.X, pady=(4, 0))
        ttk.Button(btn_list_frame, text="List backups", command=self._list_restore_backups).pack(
            side=tk.LEFT, padx=5
        )
        self.restore_blob_path_var = tk.StringVar()
        tk.Label(step1, textvariable=self.restore_blob_path_var, fg="gray").pack(anchor=tk.W, pady=(2, 0))

        step2 = ttk.LabelFrame(parent, text="Step 2: Target SQL Server (Staging MI)", padding=10)
        step2.pack(fill=tk.X, padx=5, pady=5)
        self.restore_blob_server_var = self.main_window.shared_dest_server
        self.restore_blob_db_var = self.main_window.shared_dest_db
        self.restore_blob_auth_var = self.main_window.shared_dest_auth
        self.restore_blob_user_var = self.main_window.shared_dest_user
        self.restore_blob_password_var = self.main_window.shared_dest_password
        self.restore_blob_managed_instance_var = tk.BooleanVar(value=False)
        ConnectionWidget(
            parent=step2,
            server_var=self.restore_blob_server_var,
            db_var=self.restore_blob_db_var,
            auth_var=self.restore_blob_auth_var,
            user_var=self.restore_blob_user_var,
            password_var=self.restore_blob_password_var,
            label_text="Target database (auto-filled from selection; created if not present, replaced if present):",
            row_start=0,
        )
        ttk.Checkbutton(
            step2,
            text="Target is Azure SQL Managed Instance (use RESTORE without REPLACE/STATS)",
            variable=self.restore_blob_managed_instance_var,
        ).grid(row=8, column=0, columnspan=2, sticky=tk.W, padx=5, pady=(8, 0))

        btn_frame = ttk.Frame(parent)
        btn_frame.pack(pady=10)
        self.restore_from_blob_btn = ttk.Button(
            btn_frame, text="Start Restore from Blob", command=self._start_restore_from_blob, width=25
        )
        self.restore_from_blob_btn.pack(side=tk.LEFT, padx=5)
        self.test_restore_blob_sdk_btn = ttk.Button(
            btn_frame,
            text="Test: SDK read (this PC)",
            command=self._test_restore_blob_sdk_read,
            width=22,
        )
        self.test_restore_blob_sdk_btn.pack(side=tk.LEFT, padx=5)
        self.test_restore_blob_sql_headeronly_btn = ttk.Button(
            btn_frame,
            text="Test: SQL HEADERONLY (ODBC)",
            command=self._test_restore_blob_sql_headeronly,
            width=26,
        )
        self.test_restore_blob_sql_headeronly_btn.pack(side=tk.LEFT, padx=5)

        log_frame = ttk.LabelFrame(parent, text="Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.restore_from_blob_log = scrolledtext.ScrolledText(log_frame, height=8, wrap=tk.WORD)
        self.restore_from_blob_log.pack(fill=tk.BOTH, expand=True)
        tk.Label(
            parent,
            text="Tests: SDK read uses this PC’s identity. SQL HEADERONLY uses the same path as full restore (credential + RESTORE HEADERONLY FROM URL on the target SQL instance).",
            fg="gray",
            wraplength=720,
        ).pack(anchor=tk.W, padx=5, pady=(0, 4))

    def _save_restore_blob_settings(self):
        """Save restore blob auth settings (same file as .bak to Blob)."""
        try:
            try:
                from gui.utils.blob_config import save_blob_settings
            except ImportError:
                from azure_migration_tool.gui.utils.blob_config import save_blob_settings

            save_blob_settings(
                self.blob_conn_var.get(),
                self.blob_container_var.get(),
                blob_auth_mode=self.blob_auth_mode_var.get(),
                storage_account_url=self.blob_account_url_var.get(),
            )
            messagebox.showinfo(
                "Saved",
                "Blob settings saved."
                + (" On Windows the connection string is stored encrypted (DPAPI)." if os.name == "nt" else ""),
            )
        except Exception as e:
            messagebox.showerror("Save failed", _compact_dialog_error(str(e)))

    def _on_restore_db_filter_selected(self, event=None):
        """Sync target database to the selected backup database name."""
        db = (self.restore_db_filter_var.get() or "").strip()
        if db and not db.startswith("(") and db != "Listing...":
            self.restore_blob_db_var.set(db)

    def _on_restore_backup_selected(self, event):
        """Set blob path when user selects a backup from the list."""
        sel = self.restore_backups_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        items = self.restore_backups_listbox.get(0, tk.END)
        if idx >= len(items):
            return
        label = items[idx]
        # Prefer the lookup map populated during listing; fall back to splitting the label.
        path_map = getattr(self, "_restore_label_to_path", {}) or {}
        path = path_map.get(label) or label.split("    [", 1)[0].strip()
        self.restore_blob_path_var.set(path)
        if "/" in path:
            self.restore_blob_db_var.set(path.split("/", 1)[0])

    def _list_restore_databases(self):
        """Discover top-level folder names in container and populate combobox."""
        conn_str = (self.blob_conn_var.get() or "").strip()
        container = (self.blob_container_var.get() or "").strip()
        blob_auth_mode = self.blob_auth_mode_var.get()
        storage_account_url = (self.blob_account_url_var.get() or "").strip()
        try:
            container = _resolve_container_for_gui(blob_auth_mode, container, storage_account_url)
        except Exception as e:
            messagebox.showerror("Error", _compact_dialog_error(str(e)))
            return
        if blob_auth_mode != "managed_identity" and not conn_str:
            messagebox.showerror("Error", "Enter blob connection string first (or switch to Managed Identity).")
            return
        if blob_auth_mode == "managed_identity" and not storage_account_url:
            messagebox.showerror("Error", "Enter storage account URL first.")
            return
        self.restore_db_filter_var.set("Listing...")

        def run():
            try:
                from azure.storage.blob import BlobServiceClient

                if blob_auth_mode == "managed_identity":
                    try:
                        from src.backup.bak_to_blob import _get_mi_blob_service_client
                    except ImportError:
                        from azure_migration_tool.src.backup.bak_to_blob import _get_mi_blob_service_client
                    acct_url = _normalize_blob_account_url_for_gui(storage_account_url)
                    client = _get_mi_blob_service_client(acct_url)
                else:
                    client = BlobServiceClient.from_connection_string(conn_str)

                container_client = client.get_container_client(container)
                seen = set()
                for b in container_client.list_blobs(name_starts_with=None):
                    if "/" in b.name:
                        top = b.name.split("/", 1)[0]
                        if top and top not in seen:
                            seen.add(top)
                names = sorted(seen)
                self.frame.after(0, lambda: self._populate_restore_databases_combo(names))
            except Exception as e:
                self.frame.after(
                    0,
                    lambda msg=str(e): self._populate_restore_databases_combo([], msg),
                )

        threading.Thread(target=run, daemon=True).start()

    def _populate_restore_databases_combo(self, names, error=None):
        """Update database filter combobox (called on UI thread)."""
        if error:
            self.restore_db_filter_var.set("")
            self.restore_db_filter_combo["values"] = []
            messagebox.showerror("List databases failed", _compact_dialog_error(str(error)))
            return
        self.restore_db_filter_combo["values"] = names
        if names:
            self.restore_db_filter_var.set(names[0])
            self.restore_blob_db_var.set(names[0])
        else:
            self.restore_db_filter_var.set("(no folders found)")

    def _list_restore_backups(self):
        """List .bak blobs under the chosen database folder only.

        Striped sets (`<db>_partNNofMM.bak`) are collapsed to a single entry
        showing any one stripe (the restore module discovers siblings).
        """
        conn_str = (self.blob_conn_var.get() or "").strip()
        container = (self.blob_container_var.get() or "").strip()
        blob_auth_mode = self.blob_auth_mode_var.get()
        storage_account_url = (self.blob_account_url_var.get() or "").strip()
        try:
            container = _resolve_container_for_gui(blob_auth_mode, container, storage_account_url)
        except Exception as e:
            messagebox.showerror("Error", _compact_dialog_error(str(e)))
            return
        db_name = (self.restore_db_filter_var.get() or "").strip()
        if blob_auth_mode != "managed_identity" and not conn_str:
            messagebox.showerror("Error", "Enter blob connection string first.")
            return
        if not db_name or db_name.startswith("(") or db_name == "Listing...":
            messagebox.showerror(
                "Error",
                "Choose a database first: click 'List databases' and pick one (or type the backed-up DB name).",
            )
            return
        self.restore_backups_listbox.delete(0, tk.END)
        self.restore_backups_listbox.insert(tk.END, "Listing...")
        prefix = db_name.strip().rstrip("/") + "/"

        def run():
            try:
                import re as _re
                from azure.storage.blob import BlobServiceClient

                if blob_auth_mode == "managed_identity":
                    try:
                        from src.backup.bak_to_blob import _get_mi_blob_service_client
                    except ImportError:
                        from azure_migration_tool.src.backup.bak_to_blob import _get_mi_blob_service_client
                    _acct = _normalize_blob_account_url_for_gui(storage_account_url)
                    client = _get_mi_blob_service_client(_acct)
                else:
                    client = BlobServiceClient.from_connection_string(conn_str)
                container_client = client.get_container_client(container)
                # Pull each blob's size so we can show MB/GB next to the run.
                blob_iter = list(container_client.list_blobs(name_starts_with=prefix))
                all_baks = [b for b in blob_iter if b.name.endswith(".bak")]

                # Group striped sets: key = (folder, filename_prefix_before_part, total).
                # The regex MUST be matched against the file name (not the full path),
                # otherwise m.start() is an offset into the path and `fname[: m.start()]`
                # silently returns the entire filename.
                stripe_re = _re.compile(r"_part(\d+)of(\d+)\.bak$", _re.IGNORECASE)
                groups: dict = {}
                singles: list = []
                for b in all_baks:
                    name = b.name
                    folder, fname = name.rsplit("/", 1) if "/" in name else ("", name)
                    m = stripe_re.search(fname)
                    if not m:
                        singles.append((name, b.size or 0))
                        continue
                    pref = fname[: m.start()]
                    key = (folder, pref, int(m.group(2)))
                    groups.setdefault(key, []).append((int(m.group(1)), name, b.size or 0))

                def _fmt_size(n: int) -> str:
                    gb = n / (1024 ** 3)
                    if gb >= 1.0:
                        return f"{gb:,.1f} GB"
                    mb = n / (1024 ** 2)
                    return f"{mb:,.1f} MB"

                # `display` items are (sort_key, label, payload_path)
                display: list = []
                for name, size in singles:
                    label = f"{name}    [single, {_fmt_size(size)}]"
                    display.append((name, label, name))

                for (folder, pref, total), parts in groups.items():
                    parts.sort()
                    first_name = parts[0][1]
                    total_size = sum(p[2] for p in parts)
                    have = len(parts)
                    status = f"{have}/{total} stripe(s)" + ("" if have == total else "  MISSING!")
                    label = f"{first_name}    [{status}, total {_fmt_size(total_size)}]"
                    # Sort by folder so newest run_id (highest timestamp) sorts last
                    display.append((first_name, label, first_name))

                display.sort(key=lambda t: t[0], reverse=True)
                labels = [t[1] for t in display]
                self._restore_label_to_path = {t[1]: t[2] for t in display}
                self.frame.after(0, lambda: self._populate_restore_backups_list(labels))
            except Exception as e:
                self._restore_label_to_path = {}
                self.frame.after(
                    0,
                    lambda msg=_compact_dialog_error(str(e)): self._populate_restore_backups_list([], msg),
                )

        threading.Thread(target=run, daemon=True).start()

    def _populate_restore_backups_list(self, names, error=None):
        """Update listbox with backup names (called on UI thread)."""
        self.restore_backups_listbox.delete(0, tk.END)
        if error:
            self.restore_backups_listbox.insert(tk.END, f"Error: {_compact_dialog_error(str(error))}")
            return
        if not names:
            self.restore_backups_listbox.insert(tk.END, "(no .bak files found)")
            return
        for n in names:
            self.restore_backups_listbox.insert(tk.END, n)

    def _collect_restore_blob_tab_inputs(self, require_database: bool) -> Optional[dict]:
        """Validate Restore-from-blob tab fields; return dict or None after messagebox."""
        blob_path = (self.restore_blob_path_var.get() or "").strip()
        if not blob_path or blob_path.startswith("(") or blob_path.startswith("Error"):
            messagebox.showerror("Error", "Select a backup from the list (click 'List backups' then select a .bak).")
            return None
        conn_str = (self.blob_conn_var.get() or "").strip()
        container = (self.blob_container_var.get() or "").strip()
        blob_auth_mode = self.blob_auth_mode_var.get() or "connection_string"
        storage_account_url = (self.blob_account_url_var.get() or "").strip()
        try:
            container = _resolve_container_for_gui(blob_auth_mode, container, storage_account_url)
        except Exception as e:
            messagebox.showerror("Error", _compact_dialog_error(str(e)))
            return None
        server = (self.restore_blob_server_var.get() or "").strip()
        database = (self.restore_blob_db_var.get() or "").strip()
        if blob_auth_mode == "connection_string" and not conn_str:
            messagebox.showerror("Error", "Blob connection string is required (Connection String mode).")
            return None
        if blob_auth_mode == "managed_identity" and not storage_account_url:
            messagebox.showerror("Error", "Storage account URL is required (Managed Identity mode).")
            return None
        if not container:
            messagebox.showerror(
                "Error",
                "Container name is required. Enter container field or include it in storage URL path.",
            )
            return None
        if not server:
            messagebox.showerror("Error", "Target SQL Server is required.")
            return None
        if require_database and not database:
            messagebox.showerror("Error", "Target database name is required for restore.")
            return None
        return {
            "blob_path": blob_path,
            "conn_str": conn_str,
            "container": container,
            "blob_auth_mode": blob_auth_mode,
            "storage_account_url": storage_account_url,
            "server": server,
            "database": database,
        }

    def _restore_blob_tab_set_busy(self, busy: bool) -> None:
        st = tk.DISABLED if busy else tk.NORMAL
        self.restore_from_blob_btn.config(state=st)
        self.test_restore_blob_sdk_btn.config(state=st)
        self.test_restore_blob_sql_headeronly_btn.config(state=st)

    def _test_restore_blob_sdk_read(self):
        """Azure SDK get_blob_properties on this PC (same identity as list-backups)."""
        try:
            from src.restore.restore_from_blob import run_test_blob_sdk_read
        except ImportError:
            try:
                from azure_migration_tool.src.restore.restore_from_blob import run_test_blob_sdk_read
            except ImportError:
                run_test_blob_sdk_read = None
        if not run_test_blob_sdk_read:
            messagebox.showerror("Error", "Restore module not available.")
            return
        p = self._collect_restore_blob_tab_inputs(require_database=False)
        if not p:
            return
        self._restore_blob_tab_set_busy(True)
        self.restore_from_blob_log.delete("1.0", tk.END)

        def log(msg):
            self.frame.after(0, lambda m=msg: self.restore_from_blob_log.insert(tk.END, m + "\n"))
            self.frame.after(0, lambda: self.restore_from_blob_log.see(tk.END))

        def run():
            try:
                log("=== Test: Azure SDK get_blob_properties (this PC) ===")
                summary = run_test_blob_sdk_read(
                    blob_connection_string=p["conn_str"],
                    container=p["container"],
                    blob_path=p["blob_path"],
                    log_callback=log,
                    blob_auth_mode=p["blob_auth_mode"],
                    storage_account_url=p["storage_account_url"],
                )
                if summary.get("status") == "success":
                    sz = summary.get("size_bytes")
                    self.frame.after(
                        0,
                        lambda: messagebox.showinfo(
                            "SDK test OK",
                            f"This PC can read blob metadata.\nSize (bytes): {sz}\n\n"
                            "This does not prove SQL Server can read the blob; use "
                            "'Test: SQL HEADERONLY (ODBC)' for that.",
                        ),
                    )
                else:
                    err = summary.get("error") or "Unknown error"
                    self.frame.after(
                        0,
                        lambda e=err: messagebox.showerror("SDK test failed", _compact_dialog_error(e)),
                    )
            except Exception as e:
                log(str(e))
                self.frame.after(
                    0,
                    lambda x=str(e): messagebox.showerror("Error", _compact_dialog_error(x)),
                )
            finally:
                self.frame.after(0, lambda: self._restore_blob_tab_set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _test_restore_blob_sql_headeronly(self):
        """RESTORE HEADERONLY FROM URL via ODBC — same credential + blob path as full restore."""
        try:
            from src.restore.restore_from_blob import run_test_blob_headeronly_via_sql_odbc
        except ImportError:
            try:
                from azure_migration_tool.src.restore.restore_from_blob import (
                    run_test_blob_headeronly_via_sql_odbc,
                )
            except ImportError:
                run_test_blob_headeronly_via_sql_odbc = None
        if not run_test_blob_headeronly_via_sql_odbc:
            messagebox.showerror("Error", "Restore module not available.")
            return
        p = self._collect_restore_blob_tab_inputs(require_database=False)
        if not p:
            return
        self._restore_blob_tab_set_busy(True)
        self.restore_from_blob_log.delete("1.0", tk.END)

        def log(msg):
            self.frame.after(0, lambda m=msg: self.restore_from_blob_log.insert(tk.END, m + "\n"))
            self.frame.after(0, lambda: self.restore_from_blob_log.see(tk.END))

        def run():
            try:
                log("=== Test: RESTORE HEADERONLY FROM URL (ODBC → SQL Server → blob) ===")
                summary = run_test_blob_headeronly_via_sql_odbc(
                    server=p["server"],
                    auth=self.restore_blob_auth_var.get() or "windows",
                    user=self.restore_blob_user_var.get() or None,
                    password=self.restore_blob_password_var.get() or None,
                    blob_connection_string=p["conn_str"],
                    container=p["container"],
                    blob_path=p["blob_path"],
                    log_callback=log,
                    target_managed_instance=self.restore_blob_managed_instance_var.get(),
                    blob_auth_mode=p["blob_auth_mode"],
                    storage_account_url=p["storage_account_url"],
                )
                if summary.get("status") == "success":
                    n = summary.get("header_rows", 0)
                    self.frame.after(
                        0,
                        lambda: messagebox.showinfo(
                            "SQL blob test OK",
                            f"SQL Server read the backup header from blob URL(s).\nHeader row(s): {n}\n\n"
                            "If this succeeds but full restore fails later, the issue is likely restore options "
                            "or destination DB, not blob read permissions.",
                        ),
                    )
                else:
                    err = summary.get("error") or "Unknown error"
                    self.frame.after(
                        0,
                        lambda e=err: messagebox.showerror(
                            "SQL HEADERONLY failed", _compact_dialog_error(e)
                        ),
                    )
            except Exception as e:
                log(str(e))
                self.frame.after(
                    0,
                    lambda x=str(e): messagebox.showerror("Error", _compact_dialog_error(x)),
                )
            finally:
                self.frame.after(0, lambda: self._restore_blob_tab_set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _start_restore_from_blob(self):
        """Run RESTORE DATABASE FROM URL (Azure Blob)."""
        try:
            from src.restore.restore_from_blob import run_restore_from_blob
        except ImportError:
            try:
                from azure_migration_tool.src.restore.restore_from_blob import run_restore_from_blob
            except ImportError:
                run_restore_from_blob = None
        if not run_restore_from_blob:
            messagebox.showerror("Error", "Restore module not available. Install: pip install azure-storage-blob")
            return
        p = self._collect_restore_blob_tab_inputs(require_database=True)
        if not p:
            return

        self._restore_blob_tab_set_busy(True)
        self.restore_from_blob_log.delete("1.0", tk.END)

        def log(msg):
            self.frame.after(0, lambda m=msg: self.restore_from_blob_log.insert(tk.END, m + "\n"))
            self.frame.after(0, lambda: self.restore_from_blob_log.see(tk.END))

        def run():
            try:
                summary = run_restore_from_blob(
                    server=p["server"],
                    database=p["database"],
                    auth=self.restore_blob_auth_var.get() or "windows",
                    user=self.restore_blob_user_var.get() or None,
                    password=self.restore_blob_password_var.get() or None,
                    blob_connection_string=p["conn_str"],
                    container=p["container"],
                    blob_path=p["blob_path"],
                    log_callback=log,
                    target_managed_instance=self.restore_blob_managed_instance_var.get(),
                    blob_auth_mode=p["blob_auth_mode"],
                    storage_account_url=p["storage_account_url"],
                )
                if summary.get("status") == "success":
                    self.frame.after(
                        0, lambda: messagebox.showinfo("Success", "Restore from blob completed successfully.")
                    )
                else:
                    err = summary.get("error") or "Unknown error"
                    self.frame.after(
                        0,
                        lambda e=err: messagebox.showerror("Restore failed", _compact_dialog_error(e)),
                    )
            except Exception as e:
                log(str(e))
                self.frame.after(
                    0,
                    lambda x=str(e): messagebox.showerror("Error", _compact_dialog_error(x)),
                )
            finally:
                self.frame.after(0, lambda: self._restore_blob_tab_set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    # ------------------------------------------------------------------ #
    # Restore from Disk handlers
    # ------------------------------------------------------------------ #
    def _save_restore_disk_connection(self):
        """Save restore disk connection settings."""
        # Connection settings are already stored in the ConnectionWidget
        messagebox.showinfo("Saved", "Connection settings saved.")

    def _browse_restore_disk_file(self):
        """Browse for .bak file."""
        from tkinter import filedialog
        
        filename = filedialog.askopenfilename(
            title="Select Backup File",
            filetypes=[("Backup files", "*.bak"), ("All files", "*.*")],
            initialdir="C:\\"
        )
        
        if filename:
            self.restore_disk_file_var.set(filename)

    def _load_backup_history(self):
        """Load recent backups from SQL Server backup history."""
        try:
            target_server = self.restore_disk_server_var.get().strip()
            history_server = self.restore_disk_history_server_var.get().strip() or target_server
            database_filter = self.restore_disk_history_db_filter_var.get().strip() or None

            if not history_server:
                messagebox.showwarning("Incomplete", "Please enter SQL Server instance first.")
                return
            
            self.restore_disk_log.delete('1.0', tk.END)
            self.restore_disk_log.insert(tk.END, f"Loading backup history from {history_server}...\n")
            if database_filter:
                self.restore_disk_log.insert(tk.END, f"Filtering by database: {database_filter}\n")
            self.restore_disk_log.update()
            
            def run():
                try:
                    from azure_migration_tool.src.restore.restore_from_disk import get_recent_backups_from_history
                    
                    auth = self.restore_disk_auth_var.get()
                    user = self.restore_disk_user_var.get()
                    password = self.restore_disk_password_var.get()
                    
                    def log(msg):
                        self.frame.after(0, lambda: self.restore_disk_log.insert(tk.END, msg + "\n"))
                        self.frame.after(0, lambda: self.restore_disk_log.see(tk.END))
                    
                    backups = get_recent_backups_from_history(
                        server=history_server,
                        database=database_filter,
                        auth=auth,
                        user=user,
                        password=password,
                        limit=20,
                        log=log
                    )
                    
                    # Update listbox on main thread
                    def update_list():
                        self.restore_disk_history_list.delete(0, tk.END)
                        self.restore_disk_history_data.clear()
                        
                        for idx, backup in enumerate(backups):
                            db_name = backup['database_name']
                            backup_date = backup['backup_date'].strftime("%Y-%m-%d %H:%M:%S")
                            size_mb = backup['size_mb']
                            backup_path = backup['backup_path']
                            
                            display_text = f"{db_name} | {backup_date} | {size_mb} MB | {backup_path}"
                            self.restore_disk_history_list.insert(tk.END, display_text)
                            self.restore_disk_history_data[idx] = backup_path

                        if not backups:
                            log("\nNo matching backups found. Try clearing the filter or checking the source server.")
                        else:
                            log(f"\nLoaded {len(backups)} backup(s). Select one to populate the Backup file path.")
                    
                    self.frame.after(0, update_list)
                    
                except Exception as e:
                    error_msg = _compact_dialog_error(str(e))
                    self.frame.after(
                        0,
                        lambda m=error_msg: messagebox.showerror(
                            "Error", f"Failed to load backup history:\n\n{m}"
                        ),
                    )
                    self.frame.after(
                        0,
                        lambda m=error_msg: self.restore_disk_log.insert(tk.END, f"\nERROR: {m}\n"),
                    )
            
            threading.Thread(target=run, daemon=True).start()
            
        except Exception as e:
            messagebox.showerror("Error", _compact_dialog_error(str(e)))

    def _on_backup_history_select(self, event):
        """Handle selection from backup history listbox."""
        selection = self.restore_disk_history_list.curselection()
        if selection:
            idx = selection[0]
            backup_path = self.restore_disk_history_data.get(idx, "")
            if backup_path:
                self.restore_disk_file_var.set(backup_path)

    def _start_restore_from_disk(self):
        """Start restore from disk operation."""
        server = self.restore_disk_server_var.get().strip()
        backup_file = self.restore_disk_file_var.get().strip()
        target_db = self.restore_disk_target_db_var.get().strip()
        
        if not server:
            messagebox.showwarning("Incomplete", "Please enter SQL Server instance.")
            return
        
        if not backup_file:
            messagebox.showwarning("Incomplete", "Please select a backup file.")
            return
        
        auth = self.restore_disk_auth_var.get()
        user = self.restore_disk_user_var.get()
        password = self.restore_disk_password_var.get()
        
        replace_existing = self.restore_disk_replace_var.get()
        data_file = self.restore_disk_data_file_var.get().strip() or None
        log_file = self.restore_disk_log_file_var.get().strip() or None
        
        # Confirm if replacing
        if replace_existing and target_db:
            if not messagebox.askyesno(
                "Confirm Replace",
                f"Are you sure you want to REPLACE the existing database '{target_db}'?\n\n"
                f"This will overwrite all data in that database!"
            ):
                return
        
        self.restore_disk_log.delete('1.0', tk.END)
        self.restore_disk_btn.config(state=tk.DISABLED)
        
        def log(msg):
            self.frame.after(0, lambda: self.restore_disk_log.insert(tk.END, msg + "\n"))
            self.frame.after(0, lambda: self.restore_disk_log.see(tk.END))
        
        def run():
            try:
                from azure_migration_tool.src.restore.restore_from_disk import restore_database_from_disk
                
                result = restore_database_from_disk(
                    server=server,
                    backup_file_path=backup_file,
                    target_database_name=target_db or None,
                    auth=auth,
                    user=user,
                    password=password,
                    data_file_path=data_file,
                    log_file_path=log_file,
                    replace_existing=replace_existing,
                    recovery=True,
                    log=log
                )
                
                if result.get("success"):
                    self.frame.after(
                        0, lambda: messagebox.showinfo(
                            "Success",
                            f"Database '{result['database_name']}' restored successfully in {result['restore_time_sec']}s!"
                        )
                    )
                else:
                    err = result.get("message", "Unknown error")
                    self.frame.after(
                        0,
                        lambda: messagebox.showerror("Restore Failed", _compact_dialog_error(err))
                    )
            except Exception as e:
                error_msg = str(e)
                log(f"\nERROR: {error_msg}")
                self.frame.after(
                    0,
                    lambda: messagebox.showerror("Error", _compact_dialog_error(error_msg))
                )
            finally:
                self.frame.after(0, lambda: self.restore_disk_btn.config(state=tk.NORMAL))
        
        threading.Thread(target=run, daemon=True).start()
