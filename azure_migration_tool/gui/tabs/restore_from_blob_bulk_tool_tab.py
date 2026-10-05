# Author: Satish Chauhan
"""Tools > Restore from Blob — RESTORE FROM URL for multiple databases."""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
from typing import Any, Dict, List, Optional

try:
    from gui.tabs.backup_restore_tab import BackupRestoreTab, _compact_dialog_error
except ImportError:
    from azure_migration_tool.gui.tabs.backup_restore_tab import (
        BackupRestoreTab,
        _compact_dialog_error,
    )

try:
    from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll
except ImportError:
    from azure_migration_tool.gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll

try:
    from gui.widgets.connection_widget import ConnectionWidget
except ImportError:
    from azure_migration_tool.gui.widgets.connection_widget import ConnectionWidget


class RestoreFromBlobBulkToolTab(BackupRestoreTab):
    """Restore many databases from Blob (latest .bak per database)."""

    _CHECKED = "\u2611"
    _UNCHECKED = "\u2610"

    def _create_widgets(self) -> None:
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

        tk.Label(
            scrollable_frame,
            text="Restore from Blob (multiple databases)",
            font=("Arial", 12, "bold"),
        ).pack(pady=(0, 8))

        self._create_bulk_restore_blob_step1(scrollable_frame)
        self._create_bulk_restore_target_section(scrollable_frame)

        btn_frame = ttk.Frame(scrollable_frame)
        btn_frame.pack(pady=10)
        ttk.Button(
            btn_frame,
            text="Restore selected databases",
            command=self._start_bulk_restore_from_blob,
            width=28,
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            btn_frame,
            text="Stop",
            command=self._stop_bulk_restore_from_blob,
            width=8,
        ).pack(side=tk.LEFT, padx=5)

        self.bulk_progress_var = tk.StringVar(value="")
        tk.Label(
            scrollable_frame,
            textvariable=self.bulk_progress_var,
            anchor=tk.W,
        ).pack(fill=tk.X, padx=8, pady=(0, 4))

        log_frame = ttk.LabelFrame(scrollable_frame, text="Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.restore_from_blob_log = scrolledtext.ScrolledText(log_frame, height=12, wrap=tk.WORD)
        self.restore_from_blob_log.pack(fill=tk.BOTH, expand=True)

        self._bulk_blob_db_names: List[str] = []
        self._bulk_blob_db_checked: Dict[str, bool] = {}
        self._bulk_blob_path_by_db: Dict[str, str] = {}

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _create_bulk_restore_blob_step1(self, parent) -> None:
        step1 = ttk.LabelFrame(parent, text="Step 1: Blob storage", padding=10)
        step1.pack(fill=tk.X, padx=5, pady=5)
        self._create_blob_auth_widgets(
            step1,
            save_command=self._save_restore_blob_settings,
            prefix="bulk_restore",
            show_browse_azure=True,
        )

        db_frame = ttk.LabelFrame(step1, text="Databases in container (latest backup each)", padding=8)
        db_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        filter_row = ttk.Frame(db_frame)
        filter_row.pack(fill=tk.X, pady=(0, 4))
        tk.Label(filter_row, text="Filter:").pack(side=tk.LEFT)
        self.bulk_blob_db_filter_var = tk.StringVar()
        self.bulk_blob_db_filter_var.trace_add("write", lambda *_: self._bulk_rebuild_blob_db_tree())
        ttk.Entry(filter_row, textvariable=self.bulk_blob_db_filter_var, width=42).pack(
            side=tk.LEFT, padx=(8, 0), fill=tk.X, expand=True
        )

        tree_frame = ttk.Frame(db_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(tree_frame)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.bulk_blob_db_tree = ttk.Treeview(
            tree_frame,
            columns=("check", "database"),
            show="headings",
            height=10,
            yscrollcommand=sb.set,
        )
        self.bulk_blob_db_tree.heading("check", text="")
        self.bulk_blob_db_tree.heading("database", text="Database")
        self.bulk_blob_db_tree.column("check", width=32, anchor=tk.CENTER, stretch=False)
        self.bulk_blob_db_tree.column("database", width=420, anchor=tk.W)
        sb.config(command=self.bulk_blob_db_tree.yview)
        self.bulk_blob_db_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.bulk_blob_db_tree.bind("<Button-1>", self._on_bulk_blob_db_tree_click)

        self.bulk_blob_db_selection_var = tk.StringVar(value="0 selected")
        ttk.Label(db_frame, textvariable=self.bulk_blob_db_selection_var).pack(anchor=tk.W, pady=(4, 0))

        btn_row = ttk.Frame(db_frame)
        btn_row.pack(fill=tk.X, pady=(6, 0))
        ttk.Button(
            btn_row, text="List databases", command=self._refresh_bulk_blob_database_list, width=14
        ).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_row, text="Select all", command=self._bulk_blob_select_all, width=12).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(btn_row, text="Clear all", command=self._bulk_blob_clear_all, width=12).pack(
            side=tk.LEFT
        )

    def _create_bulk_restore_target_section(self, parent) -> None:
        step2 = ttk.LabelFrame(parent, text="Step 2: Target SQL Server", padding=10)
        step2.pack(fill=tk.X, padx=5, pady=5)

        self.restore_blob_server_var = self.main_window.shared_dest_server
        self.restore_blob_db_var = self.main_window.shared_dest_db
        self.restore_blob_auth_var = self.main_window.shared_dest_auth
        self.restore_blob_user_var = self.main_window.shared_dest_user
        self.restore_blob_password_var = self.main_window.shared_dest_password
        self.restore_blob_managed_instance_var = tk.BooleanVar(value=False)

        step2.columnconfigure(0, weight=0)
        step2.columnconfigure(1, weight=1)
        self._bulk_restore_conn_widget = ConnectionWidget(
            parent=step2,
            server_var=self.restore_blob_server_var,
            db_var=self.restore_blob_db_var,
            auth_var=self.restore_blob_auth_var,
            user_var=self.restore_blob_user_var,
            password_var=self.restore_blob_password_var,
            label_text="",
            row_start=0,
            server_only=True,
        )
        mi_row = self._bulk_restore_conn_widget.grid_last_row + 1
        ttk.Checkbutton(
            step2,
            text="Target is Azure SQL Managed Instance",
            variable=self.restore_blob_managed_instance_var,
        ).grid(row=mi_row, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))

    def _bulk_blob_update_selection_label(self) -> None:
        n = sum(1 for name in self._bulk_blob_db_names if self._bulk_blob_db_checked.get(name))
        self.bulk_blob_db_selection_var.set(f"{n} of {len(self._bulk_blob_db_names)} selected")

    def _bulk_rebuild_blob_db_tree(self) -> None:
        q = (self.bulk_blob_db_filter_var.get() or "").strip().lower()
        for item in self.bulk_blob_db_tree.get_children():
            self.bulk_blob_db_tree.delete(item)
        for name in self._bulk_blob_db_names:
            if q and q not in name.lower():
                continue
            mark = self._CHECKED if self._bulk_blob_db_checked.get(name) else self._UNCHECKED
            self.bulk_blob_db_tree.insert("", tk.END, values=(mark, name))
        self._bulk_blob_update_selection_label()

    def _on_bulk_blob_db_tree_click(self, event: tk.Event) -> Optional[str]:
        if self.bulk_blob_db_tree.identify_region(event.x, event.y) != "cell":
            return None
        item = self.bulk_blob_db_tree.identify_row(event.y)
        if not item:
            return None
        name = (self.bulk_blob_db_tree.set(item, "database") or "").strip()
        if not name or name not in self._bulk_blob_db_checked:
            return None
        self._bulk_blob_db_checked[name] = not self._bulk_blob_db_checked.get(name, False)
        mark = self._CHECKED if self._bulk_blob_db_checked[name] else self._UNCHECKED
        self.bulk_blob_db_tree.set(item, "check", mark)
        self._bulk_blob_update_selection_label()
        return "break"

    def _bulk_blob_select_all(self) -> None:
        for name in self._bulk_blob_db_names:
            self._bulk_blob_db_checked[name] = True
        self._bulk_rebuild_blob_db_tree()

    def _bulk_blob_clear_all(self) -> None:
        for name in self._bulk_blob_db_names:
            self._bulk_blob_db_checked[name] = False
        self._bulk_rebuild_blob_db_tree()

    def _selected_bulk_blob_databases(self) -> List[str]:
        return [n for n in self._bulk_blob_db_names if self._bulk_blob_db_checked.get(n)]

    def _refresh_bulk_blob_database_list(self) -> None:
        storage = self._restore_blob_storage_inputs()
        if not storage:
            return

        self.bulk_progress_var.set("Listing blob databases…")

        def run() -> None:
            err = ""
            names: List[str] = []
            path_by_db: Dict[str, str] = {}
            try:
                from gui.tabs.backup_restore_tab import (
                    _get_gui_blob_service_client,
                    _import_blob_backup_catalog,
                )
            except ImportError:
                from azure_migration_tool.gui.tabs.backup_restore_tab import (
                    _get_gui_blob_service_client,
                    _import_blob_backup_catalog,
                )
            try:
                client = _get_gui_blob_service_client(
                    blob_auth_mode=storage["blob_auth_mode"],
                    blob_connection_string=storage["conn_str"],
                    blob_account_url=storage["storage_account_url"],
                    container=storage["container"],
                )
                catalog = _import_blob_backup_catalog()
                all_blobs = catalog.list_all_container_blobs(
                    client.get_container_client(storage["container"])
                )
                names = catalog.discover_database_names(all_blobs)
                for db in names:
                    path = catalog.latest_backup_blob_path_for_database(all_blobs, db)
                    if path:
                        path_by_db[db] = path
                names = [n for n in names if n in path_by_db]
            except Exception as exc:
                err = str(exc)

            def finish() -> None:
                if err:
                    messagebox.showerror(
                        "List databases failed",
                        _compact_dialog_error(err),
                    )
                    self.bulk_progress_var.set("")
                    return
                self._bulk_blob_db_names = list(names)
                self._bulk_blob_path_by_db = dict(path_by_db)
                self._bulk_blob_db_checked = {n: True for n in names}
                self.bulk_blob_db_filter_var.set("")
                self._bulk_rebuild_blob_db_tree()
                self.bulk_progress_var.set(f"Found {len(names)} database(s) with backups")

            self.frame.after(0, finish)

        threading.Thread(target=run, daemon=True).start()

    def _stop_bulk_restore_from_blob(self) -> None:
        self._restore_blob_stop_event.set()
        self._append_restore_log("Stop requested.")

    def _start_bulk_restore_from_blob(self) -> None:
        databases = self._selected_bulk_blob_databases()
        if not databases:
            messagebox.showerror("Error", "Select at least one database (List databases, then check rows).")
            return
        jobs: List[Dict[str, str]] = []
        for db in databases:
            blob_path = (self._bulk_blob_path_by_db.get(db) or "").strip()
            if blob_path:
                jobs.append({"database": db, "blob_path": blob_path})
        if not jobs:
            messagebox.showerror(
                "Error",
                "No backup paths for the selected databases. Click 'List databases' again.",
            )
            return
        self.restore_from_blob_log.delete("1.0", tk.END)
        self._start_restore_multiple_from_blob(jobs)
