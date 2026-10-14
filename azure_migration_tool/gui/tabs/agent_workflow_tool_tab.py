# Author: Satish Chauhan

"""Tools > Agent Workflow — test headless bulk backup → blob → restore jobs (CLI/MCP)."""

from __future__ import annotations

import json
import logging
import threading
import uuid
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk
import tkinter as tk
from typing import Any, Dict, List, Optional

try:
    from gui.widgets.connection_widget import ConnectionWidget
except ImportError:
    from azure_migration_tool.gui.widgets.connection_widget import ConnectionWidget

try:
    from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll
except ImportError:
    from azure_migration_tool.gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll

try:
    from gui.utils.database_utils import list_databases
except ImportError:
    from azure_migration_tool.gui.utils.database_utils import list_databases

try:
    from gui.tabs.backup_restore_tab import _compact_dialog_error
except ImportError:
    from azure_migration_tool.gui.tabs.backup_restore_tab import _compact_dialog_error


class AgentWorkflowToolTab:
    """Build, validate, and run worker jobs for agent/MCP integration."""

    _CHECKED = "\u2611"
    _UNCHECKED = "\u2610"

    def __init__(self, parent: tk.Widget, main_window: Any) -> None:
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self._project_path: Optional[str] = None
        self._run_thread: Optional[threading.Thread] = None
        self._last_run_summary: Dict[str, Any] = {}
        self._agent_db_names: List[str] = []
        self._agent_db_checked: Dict[str, bool] = {}
        self._create_widgets()

    def set_project_path(self, project_path: Optional[str]) -> None:
        self._project_path = project_path
        if project_path:
            self.project_path_var.set(project_path)

    def _create_widgets(self) -> None:
        paned = ttk.PanedWindow(self.frame, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(paned)
        right = ttk.Frame(paned)
        paned.add(left, weight=3)
        paned.add(right, weight=2)

        canvas = tk.Canvas(left)
        sb = ttk.Scrollbar(left, orient=tk.VERTICAL, command=canvas.yview)
        form = ttk.Frame(canvas)
        form.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        win_id = canvas.create_window((0, 0), window=form, anchor=tk.NW)

        def _resize(event: tk.Event) -> None:
            canvas.itemconfig(win_id, width=event.width)

        canvas.bind("<Configure>", _resize)
        canvas.configure(yscrollcommand=sb.set)
        bind_canvas_vertical_scroll(canvas, form)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        hdr = ttk.Frame(form)
        hdr.grid(row=0, column=0, columnspan=2, sticky=tk.EW, pady=(0, 8))
        tk.Label(
            hdr,
            text="Agent Workflow — run jobs",
            font=("Arial", 12, "bold"),
        ).pack(side=tk.LEFT)
        ttk.Button(
            hdr,
            text="AgentOps run history",
            command=lambda: getattr(self.main_window, "_open_agent_ops_dashboard", lambda: None)(),
            width=18,
        ).pack(side=tk.RIGHT)

        self.job_id_var = tk.StringVar(value=f"job-{uuid.uuid4().hex[:8]}")
        self.project_path_var = tk.StringVar(
            value=getattr(self.main_window, "_project_path", None) or ""
        )

        meta = ttk.LabelFrame(form, text="Job", padding=8)
        meta.grid(row=1, column=0, columnspan=2, sticky=tk.EW, pady=4)
        ttk.Label(meta, text="Job ID:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(meta, textvariable=self.job_id_var, width=36).grid(
            row=0, column=1, sticky=tk.EW, padx=(8, 0)
        )
        ttk.Label(meta, text="Project path (results):").grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
        ttk.Entry(meta, textvariable=self.project_path_var, width=48).grid(
            row=1, column=1, sticky=tk.EW, padx=(8, 0), pady=(6, 0)
        )
        meta.columnconfigure(1, weight=1)

        steps = ttk.LabelFrame(form, text="Steps", padding=8)
        steps.grid(row=2, column=0, columnspan=2, sticky=tk.EW, pady=4)
        self.step_backup_var = tk.BooleanVar(value=True)
        self.step_upload_var = tk.BooleanVar(value=True)
        self.step_restore_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(steps, text="1. Local backup", variable=self.step_backup_var).pack(
            anchor=tk.W
        )
        ttk.Checkbutton(steps, text="2. Upload to blob", variable=self.step_upload_var).pack(
            anchor=tk.W
        )
        ttk.Checkbutton(steps, text="3. Restore on target", variable=self.step_restore_var).pack(
            anchor=tk.W
        )

        src = ttk.LabelFrame(form, text="Source SQL Server", padding=8)
        src.grid(row=3, column=0, columnspan=2, sticky=tk.EW, pady=4)
        self.src_auth_var = tk.StringVar(value="windows")
        self.src_user_var = tk.StringVar()
        self.src_password_var = tk.StringVar()
        self.src_conn = ConnectionWidget(
            parent=src,
            server_var=self.main_window.shared_src_server,
            db_var=self.main_window.shared_src_db,
            auth_var=self.src_auth_var,
            user_var=self.src_user_var,
            password_var=self.src_password_var,
            label_text="",
            row_start=0,
            server_only=True,
        )
        db_row = self.src_conn.grid_last_row + 1
        db_frame = ttk.LabelFrame(src, text="Databases to include", padding=6)
        db_frame.grid(row=db_row, column=0, columnspan=2, sticky=tk.EW, pady=(8, 0))
        filter_row = ttk.Frame(db_frame)
        filter_row.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(filter_row, text="Filter:").pack(side=tk.LEFT)
        self.agent_db_filter_var = tk.StringVar()
        self.agent_db_filter_var.trace_add("write", lambda *_: self._agent_rebuild_db_tree())
        ttk.Entry(filter_row, textvariable=self.agent_db_filter_var, width=36).pack(
            side=tk.LEFT, padx=(6, 0), fill=tk.X, expand=True
        )
        list_row = ttk.Frame(db_frame)
        list_row.pack(fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(list_row)
        self.agent_db_tree = ttk.Treeview(
            list_row,
            columns=("check", "database"),
            show="headings",
            selectmode="browse",
            height=8,
            yscrollcommand=sb.set,
        )
        self.agent_db_tree.heading("check", text="")
        self.agent_db_tree.heading("database", text="Database")
        self.agent_db_tree.column("check", width=32, anchor=tk.CENTER, stretch=False)
        self.agent_db_tree.column("database", width=360, anchor=tk.W)
        sb.config(command=self.agent_db_tree.yview)
        self.agent_db_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.agent_db_tree.bind("<Button-1>", self._on_agent_db_tree_click)
        self.agent_db_selection_var = tk.StringVar(value="0 selected")
        ttk.Label(db_frame, textvariable=self.agent_db_selection_var).pack(anchor=tk.W, pady=(4, 0))
        self.list_all_db_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            db_frame,
            text="Backup all user databases (ignore selection)",
            variable=self.list_all_db_var,
        ).pack(anchor=tk.W, pady=(4, 0))
        btn_row = ttk.Frame(db_frame)
        btn_row.pack(fill=tk.X, pady=(6, 0))
        ttk.Button(btn_row, text="Refresh list", command=self._refresh_databases, width=12).pack(
            side=tk.LEFT
        )
        ttk.Button(btn_row, text="Select all", command=self._agent_select_all_databases, width=10).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Button(btn_row, text="Clear all", command=self._agent_clear_database_selection, width=10).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        src.columnconfigure(1, weight=1)

        local = ttk.LabelFrame(form, text="Backup path (SQL Server / UNC)", padding=8)
        local.grid(row=4, column=0, columnspan=2, sticky=tk.EW, pady=4)
        self.local_path_var = tk.StringVar(value="")
        self.local_compression_var = tk.BooleanVar(value=True)
        self.local_structured_var = tk.BooleanVar(value=True)
        self.local_stripes_var = tk.StringVar(value="auto")
        ttk.Label(
            local,
            text="Folder or .bak on the SQL Server host\n(prefer UNC both server and this PC can read):",
        ).grid(row=0, column=0, sticky=tk.NW)
        ttk.Entry(local, textvariable=self.local_path_var, width=48).grid(
            row=0, column=1, sticky=tk.EW, padx=(8, 0)
        )
        path_btns = ttk.Frame(local)
        path_btns.grid(row=1, column=1, sticky=tk.W, padx=(8, 0), pady=(6, 0))
        self.agent_sql_default_btn = ttk.Button(
            path_btns,
            text="Use SQL Server default",
            command=self._use_sql_default_backup_path,
            width=22,
        )
        self.agent_sql_default_btn.pack(side=tk.LEFT)
        self.agent_check_capacity_btn = ttk.Button(
            path_btns,
            text="Check path capacity",
            command=self._check_local_backup_capacity,
            width=18,
        )
        self.agent_check_capacity_btn.pack(side=tk.LEFT, padx=(6, 0))
        self.agent_prepare_folder_btn = ttk.Button(
            path_btns,
            text="Create folder + test write",
            command=self._prepare_local_backup_folder_test,
            width=22,
        )
        self.agent_prepare_folder_btn.pack(side=tk.LEFT, padx=(6, 0))
        tk.Label(
            local,
            text="BACKUP writes on the SQL instance. Use a share/UNC (e.g. \\\\server\\SqlBackups), "
            "not this PC's D: drive unless SQL runs on this machine.",
            fg="gray",
            wraplength=520,
            justify=tk.LEFT,
        ).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))
        ttk.Checkbutton(local, text="Compression", variable=self.local_compression_var).grid(
            row=3, column=0, columnspan=2, sticky=tk.W, pady=(6, 0)
        )
        ttk.Checkbutton(
            local, text="Structured local paths", variable=self.local_structured_var
        ).grid(row=4, column=0, columnspan=2, sticky=tk.W)
        stripe_row = ttk.Frame(local)
        stripe_row.grid(row=5, column=0, columnspan=2, sticky=tk.W, pady=(6, 0))
        ttk.Label(stripe_row, text="Stripes:").pack(side=tk.LEFT)
        ttk.Combobox(
            stripe_row,
            textvariable=self.local_stripes_var,
            values=("auto", "1", "2", "4", "8"),
            width=8,
            state="readonly",
        ).pack(side=tk.LEFT, padx=(8, 0))
        local.columnconfigure(1, weight=1)

        blob = ttk.LabelFrame(form, text="Azure Blob (one-time org setup)", padding=8)
        blob.grid(row=5, column=0, columnspan=2, sticky=tk.EW, pady=4)
        self.blob_folder_var = tk.StringVar()
        self.blob_delete_local_var = tk.BooleanVar(value=False)
        self.blob_status_var = tk.StringVar(value="")
        self._refresh_blob_status_label()
        tk.Label(
            blob,
            textvariable=self.blob_status_var,
            wraplength=520,
            justify=tk.LEFT,
            fg="#1a5276",
        ).grid(row=0, column=0, columnspan=2, sticky=tk.W)
        blob_btn_row = ttk.Frame(blob)
        blob_btn_row.grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))
        ttk.Button(
            blob_btn_row,
            text="Organization setup (Blob → UNC → paths)…",
            command=self._open_blob_settings,
            width=28,
        ).pack(side=tk.LEFT)
        ttk.Label(blob, text="Optional blob folder prefix:").grid(
            row=2, column=0, sticky=tk.W, pady=(10, 0)
        )
        ttk.Entry(blob, textvariable=self.blob_folder_var, width=32).grid(
            row=2, column=1, sticky=tk.W, padx=(8, 0), pady=(10, 0)
        )
        ttk.Checkbutton(
            blob, text="Delete local .bak after upload", variable=self.blob_delete_local_var
        ).grid(row=3, column=0, columnspan=2, sticky=tk.W, pady=(6, 0))

        tgt = ttk.LabelFrame(form, text="Restore target", padding=8)
        tgt.grid(row=6, column=0, columnspan=2, sticky=tk.EW, pady=4)
        self.tgt_mi_var = tk.BooleanVar(value=True)
        self.tgt_conn = ConnectionWidget(
            parent=tgt,
            server_var=self.main_window.shared_dest_server,
            db_var=self.main_window.shared_dest_db,
            auth_var=self.main_window.shared_dest_auth,
            user_var=self.main_window.shared_dest_user,
            password_var=self.main_window.shared_dest_password,
            label_text="Target server (each DB keeps its name)",
            row_start=0,
            server_only=True,
        )
        mi_row = self.tgt_conn.grid_last_row + 1
        ttk.Checkbutton(
            tgt,
            text="Target is Azure SQL Managed Instance",
            variable=self.tgt_mi_var,
        ).grid(row=mi_row, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))

        actions = ttk.Frame(form)
        actions.grid(row=7, column=0, columnspan=2, sticky=tk.EW, pady=12)
        self.run_btn = ttk.Button(actions, text="Run job", command=self._start_run, width=12)
        self.run_btn.pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(actions, text="Validate", command=self._validate, width=10).pack(side=tk.LEFT, padx=3)
        ttk.Button(actions, text="Load JSON…", command=self._load_json, width=12).pack(
            side=tk.LEFT, padx=3
        )
        ttk.Button(actions, text="Save JSON…", command=self._save_json, width=12).pack(
            side=tk.LEFT, padx=3
        )
        ttk.Button(actions, text="Sync form → JSON", command=self._sync_json_preview, width=14).pack(
            side=tk.LEFT, padx=3
        )
        ttk.Button(
            actions, text="Import CSV/Excel…", command=self._import_spreadsheet, width=16
        ).pack(side=tk.LEFT, padx=3)
        ttk.Button(
            actions, text="Export CSV/Excel…", command=self._export_spreadsheet, width=16
        ).pack(side=tk.LEFT, padx=3)
        ttk.Button(
            actions, text="Sample template…", command=self._save_sample_template, width=14
        ).pack(side=tk.LEFT, padx=3)
        ttk.Button(
            actions, text="Export migration brief…", command=self._export_migration_brief, width=18
        ).pack(side=tk.LEFT, padx=3)

        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(form, textvariable=self.status_var).grid(row=8, column=0, columnspan=2, sticky=tk.W)

        tk.Label(
            form,
            text="MCP: starts with the app at http://127.0.0.1:7823/mcp (Tools → Copy agent MCP URL)",
            fg="gray",
            wraplength=520,
            justify=tk.LEFT,
        ).grid(row=9, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))
        tk.Label(
            form,
            text="Bulk input: use Sample template… for CSV/Excel columns (one row per database; "
            "leave paths empty to use Organization defaults).",
            fg="gray",
            wraplength=520,
            justify=tk.LEFT,
        ).grid(row=10, column=0, columnspan=2, sticky=tk.W, pady=(4, 0))

        form.columnconfigure(0, weight=1)

        ttk.Label(right, text="Job JSON").pack(anchor=tk.W)
        self.json_preview = scrolledtext.ScrolledText(right, height=16, width=42, font=("Consolas", 9))
        self.json_preview.pack(fill=tk.BOTH, expand=True, pady=(4, 8))

        ttk.Label(right, text="Log").pack(anchor=tk.W)
        self.log_text = scrolledtext.ScrolledText(right, height=18, width=42, font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)

        self._apply_org_defaults_to_form()
        self._sync_json_preview()

    def _apply_org_defaults_to_form(self) -> None:
        try:
            from gui.utils.org_settings import load_org_settings
        except ImportError:
            from azure_migration_tool.gui.utils.org_settings import load_org_settings
        org = load_org_settings()
        if not (self.local_path_var.get() or "").strip():
            self.local_path_var.set((org.get("default_backup_unc_path") or "").strip())
        if not (self.project_path_var.get() or "").strip():
            pp = (org.get("default_project_path") or "").strip()
            if pp:
                self.project_path_var.set(pp)
            elif getattr(self.main_window, "_project_path", None):
                self.project_path_var.set(self.main_window._project_path)
        if not (self.blob_folder_var.get() or "").strip():
            self.blob_folder_var.set((org.get("default_blob_folder_prefix") or "").strip())
        self._refresh_blob_status_label()

    def _log(self, msg: str) -> None:
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def _refresh_blob_status_label(self) -> None:
        try:
            from gui.dialogs.blob_settings_dialog import blob_settings_summary
            from gui.utils.org_settings import org_settings_summary
        except ImportError:
            from azure_migration_tool.gui.dialogs.blob_settings_dialog import blob_settings_summary
            from azure_migration_tool.gui.utils.org_settings import org_settings_summary
        self.blob_status_var.set(f"{blob_settings_summary()}\n{org_settings_summary()}")

    def _open_blob_settings(self) -> None:
        try:
            from gui.dialogs.blob_settings_dialog import open_blob_settings_dialog
        except ImportError:
            from azure_migration_tool.gui.dialogs.blob_settings_dialog import (
                open_blob_settings_dialog,
            )
        open_blob_settings_dialog(self.main_window, parent=self.frame.winfo_toplevel())
        self._apply_org_defaults_to_form()
        self._sync_json_preview()

    def _use_sql_default_backup_path(self) -> None:
        server = (self.main_window.shared_src_server.get() or "").strip()
        if not server:
            messagebox.showwarning(
                "Server required",
                "Enter source SQL Server first, then detect its default backup path.",
            )
            return

        def query_sql_default() -> None:
            sql_backup_dir = None
            query_error: Optional[BaseException] = None
            try:
                try:
                    from src.utils.database import connect_to_database
                except ImportError:
                    from azure_migration_tool.src.utils.database import connect_to_database

                auth = (self.src_auth_var.get() or "windows").strip()
                conn = connect_to_database(
                    server=server,
                    db="master",
                    user=(self.src_user_var.get() or ""),
                    driver="ODBC Driver 18 for SQL Server",
                    auth=auth,
                    password=(self.src_password_var.get() or "") or None,
                    timeout=30,
                    logger=logging.getLogger(__name__),
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
            except Exception as exc:
                query_error = exc

            try:
                from src.backup.backup_path_utils import resolve_sql_default_backup_path
            except ImportError:
                from azure_migration_tool.src.backup.backup_path_utils import (
                    resolve_sql_default_backup_path,
                )

            path, reason = resolve_sql_default_backup_path(sql_backup_dir)
            self.frame.after(
                0,
                lambda: self._apply_sql_default_path(
                    path, reason, detected_sql_path=sql_backup_dir, query_error=query_error
                ),
            )

        threading.Thread(target=query_sql_default, daemon=True).start()

    def _apply_sql_default_path(
        self,
        path: str,
        reason: str = "sql_unc",
        detected_sql_path: Optional[str] = None,
        query_error: Optional[BaseException] = None,
    ) -> None:
        self.local_path_var.set(path)
        self._sync_json_preview()
        if reason == "sql_unc":
            messagebox.showinfo(
                "Path updated",
                f"Using SQL Server default backup directory:\n\n{path}\n\n"
                "Prefer a UNC path reachable from both SQL Server and this PC.",
            )
            return
        if reason == "sql_local":
            messagebox.showinfo(
                "Path updated",
                "SQL Server reported a host-local directory "
                f"(often not readable from this PC):\n\n{detected_sql_path}\n\n"
                f"Using shared location instead:\n\n{path}",
            )
            return
        detail = f"\n\n(Registry query: {query_error})" if query_error else ""
        messagebox.showinfo(
            "Path updated",
            f"Using shared backup location:\n\n{path}{detail}",
        )

    def _check_local_backup_capacity(self) -> None:
        backup_path = (self.local_path_var.get() or "").strip()
        if not backup_path:
            messagebox.showwarning("Path required", "Enter a SQL Server / UNC backup path first.")
            return
        server = (self.main_window.shared_src_server.get() or "").strip()
        dbs = self._parse_databases()
        database = dbs[0] if dbs else (self.main_window.shared_src_db.get() or "").strip()
        self.agent_check_capacity_btn.config(state=tk.DISABLED)

        def ui_log(msg: str) -> None:
            self.frame.after(0, lambda m=msg: self._log(m))

        def run() -> None:
            try:
                try:
                    from gui.utils.backup_capacity import run_local_backup_capacity_report
                except ImportError:
                    from azure_migration_tool.gui.utils.backup_capacity import (
                        run_local_backup_capacity_report,
                    )
                try:
                    from src.utils.database import connect_to_database
                except ImportError:
                    from azure_migration_tool.src.utils.database import connect_to_database

                run_local_backup_capacity_report(
                    backup_path=backup_path,
                    server=server,
                    database=database,
                    auth=(self.src_auth_var.get() or "windows").strip(),
                    user=(self.src_user_var.get() or ""),
                    password=(self.src_password_var.get() or ""),
                    use_compression=bool(self.local_compression_var.get()),
                    log=ui_log,
                    connect_to_database=connect_to_database,
                    logger=logging.getLogger(__name__),
                )
            finally:
                self.frame.after(0, lambda: self.agent_check_capacity_btn.config(state=tk.NORMAL))

        threading.Thread(target=run, daemon=True).start()

    def _prepare_local_backup_folder_test(self) -> None:
        backup_path = (self.local_path_var.get() or "").strip()
        if not backup_path:
            messagebox.showwarning("Path required", "Enter a backup folder or .bak path first.")
            return
        self.agent_prepare_folder_btn.config(state=tk.DISABLED)

        def ui_log(msg: str) -> None:
            self.frame.after(0, lambda m=msg: self._log(m))

        def run() -> None:
            try:
                try:
                    from src.backup.local_backup_and_upload import ensure_dir_and_probe_write
                except ImportError:
                    from azure_migration_tool.src.backup.local_backup_and_upload import (
                        ensure_dir_and_probe_write,
                    )

                result = ensure_dir_and_probe_write(
                    backup_path,
                    apply_icacls_everyone=True,
                    log=ui_log,
                )

                def finish() -> None:
                    self.agent_prepare_folder_btn.config(state=tk.NORMAL)
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
            except Exception as exc:
                err = _compact_dialog_error(str(exc))

                def finish_err() -> None:
                    self.agent_prepare_folder_btn.config(state=tk.NORMAL)
                    messagebox.showerror("Create folder + test write", err)

                self.frame.after(0, finish_err)

        threading.Thread(target=run, daemon=True).start()

    def _agent_update_selection_label(self) -> None:
        n = sum(1 for name in self._agent_db_names if self._agent_db_checked.get(name))
        self.agent_db_selection_var.set(f"{n} of {len(self._agent_db_names)} selected")

    def _agent_rebuild_db_tree(self) -> None:
        if not hasattr(self, "agent_db_tree"):
            return
        q = (self.agent_db_filter_var.get() or "").strip().lower()
        for item in self.agent_db_tree.get_children():
            self.agent_db_tree.delete(item)
        for name in self._agent_db_names:
            if q and q not in name.lower():
                continue
            mark = self._CHECKED if self._agent_db_checked.get(name) else self._UNCHECKED
            self.agent_db_tree.insert("", tk.END, values=(mark, name))
        self._agent_update_selection_label()

    def _on_agent_db_tree_click(self, event: tk.Event) -> Optional[str]:
        if self.agent_db_tree.identify_region(event.x, event.y) != "cell":
            return None
        item = self.agent_db_tree.identify_row(event.y)
        if not item:
            return None
        name = (self.agent_db_tree.set(item, "database") or "").strip()
        if not name or name not in self._agent_db_checked:
            return None
        self._agent_db_checked[name] = not self._agent_db_checked.get(name, False)
        mark = self._CHECKED if self._agent_db_checked[name] else self._UNCHECKED
        self.agent_db_tree.set(item, "check", mark)
        self._agent_update_selection_label()
        self._sync_json_preview()
        return "break"

    def _agent_select_all_databases(self) -> None:
        for name in self._agent_db_names:
            self._agent_db_checked[name] = True
        self._agent_rebuild_db_tree()
        self._sync_json_preview()

    def _agent_clear_database_selection(self) -> None:
        for name in self._agent_db_names:
            self._agent_db_checked[name] = False
        self._agent_rebuild_db_tree()
        self._sync_json_preview()

    def _parse_databases(self) -> List[str]:
        return [n for n in self._agent_db_names if self._agent_db_checked.get(n)]

    def _set_databases(self, names: List[str]) -> None:
        selected = {n.strip() for n in names if (n or "").strip()}
        for name in self._agent_db_names:
            self._agent_db_checked[name] = name in selected
        for name in selected:
            if name not in self._agent_db_checked:
                self._agent_db_names.append(name)
                self._agent_db_checked[name] = True
        self._agent_db_names.sort()
        self._agent_rebuild_db_tree()

    def _result_path(self, job_id: str) -> str:
        base = (self.project_path_var.get() or "").strip()
        if not base:
            return ""
        return str(Path(base) / "worker_jobs" / f"{job_id}_result.json")

    def build_job_dict(self) -> Dict[str, Any]:
        job_id = (self.job_id_var.get() or "").strip() or str(uuid.uuid4())
        project = (self.project_path_var.get() or "").strip() or None
        job: Dict[str, Any] = {
            "job_id": job_id,
            "operation": "bulk_local_backup_blob_restore",
            "project_path": project,
            "steps": {
                "backup": bool(self.step_backup_var.get()),
                "upload": bool(self.step_upload_var.get()),
                "restore": bool(self.step_restore_var.get()),
            },
            "source": {
                "server": (self.main_window.shared_src_server.get() or "").strip(),
                "auth": (self.src_auth_var.get() or "windows").strip(),
                "user": (self.src_user_var.get() or "").strip(),
                "password": (self.src_password_var.get() or "").strip(),
                "databases": self._parse_databases(),
                "list_all_databases": bool(self.list_all_db_var.get()),
            },
            "local_backup": {
                "path": (self.local_path_var.get() or "").strip(),
                "compression": bool(self.local_compression_var.get()),
                "structured_paths": bool(self.local_structured_var.get()),
                "stripes": (self.local_stripes_var.get() or "auto").strip(),
            },
            "blob": {
                "use_saved_settings": True,
                "folder": (self.blob_folder_var.get() or "").strip(),
                "delete_local_after_upload": bool(self.blob_delete_local_var.get()),
            },
            "restore_target": {
                "server": (self.main_window.shared_dest_server.get() or "").strip(),
                "auth": (self.main_window.shared_dest_auth.get() or "entra_mfa").strip(),
                "user": (self.main_window.shared_dest_user.get() or "").strip(),
                "password": (self.main_window.shared_dest_password.get() or "").strip(),
                "target_managed_instance": bool(self.tgt_mi_var.get()),
            },
        }
        if project:
            job["output"] = {
                "project_path": project,
                "result_path": self._result_path(job_id),
            }
        return job

    def apply_job_dict(self, job: Dict[str, Any]) -> None:
        self.job_id_var.set((job.get("job_id") or "").strip() or self.job_id_var.get())
        proj = (job.get("project_path") or (job.get("output") or {}).get("project_path") or "")
        if proj:
            self.project_path_var.set(str(proj))
        steps = job.get("steps") or {}
        self.step_backup_var.set(bool(steps.get("backup", True)))
        self.step_upload_var.set(bool(steps.get("upload", True)))
        self.step_restore_var.set(bool(steps.get("restore", True)))
        src = job.get("source") or {}
        if src.get("server"):
            self.main_window.shared_src_server.set(src["server"])
        if src.get("auth"):
            self.src_auth_var.set(src["auth"])
        self.src_user_var.set(src.get("user") or "")
        self.src_password_var.set(src.get("password") or "")
        self._set_databases(list(src.get("databases") or []))
        self.list_all_db_var.set(bool(src.get("list_all_databases")))
        local = job.get("local_backup") or {}
        if local.get("path"):
            self.local_path_var.set(local["path"])
        self.local_compression_var.set(bool(local.get("compression", True)))
        self.local_structured_var.set(bool(local.get("structured_paths", True)))
        if local.get("stripes") is not None:
            self.local_stripes_var.set(str(local.get("stripes")))
        blob = job.get("blob") or {}
        self.blob_folder_var.set(blob.get("folder") or blob.get("blob_folder") or "")
        self.blob_delete_local_var.set(bool(blob.get("delete_local_after_upload")))
        tgt = job.get("restore_target") or {}
        if tgt.get("server"):
            self.main_window.shared_dest_server.set(tgt["server"])
        if tgt.get("auth"):
            self.main_window.shared_dest_auth.set(tgt["auth"])
        self.main_window.shared_dest_user.set(tgt.get("user") or "")
        self.main_window.shared_dest_password.set(tgt.get("password") or "")
        if "target_managed_instance" in tgt:
            self.tgt_mi_var.set(bool(tgt["target_managed_instance"]))

    def _sync_json_preview(self) -> None:
        try:
            text = json.dumps(self.build_job_dict(), indent=2)
        except Exception as exc:
            text = f"{{\"error\": \"{exc}\"}}"
        self.json_preview.delete("1.0", tk.END)
        self.json_preview.insert(tk.END, text)

    def _job_from_preview(self) -> Dict[str, Any]:
        raw = self.json_preview.get("1.0", tk.END).strip()
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Job JSON must be an object.")
        return data

    def _refresh_databases(self) -> None:
        server = (self.main_window.shared_src_server.get() or "").strip()
        if not server:
            messagebox.showwarning("Server required", "Enter a source server first.")
            return
        auth = (self.src_auth_var.get() or "windows").strip()
        user = (self.src_user_var.get() or "").strip()
        password = (self.src_password_var.get() or "").strip()

        def run() -> None:
            err: Optional[str] = None
            names: List[str] = []
            try:
                names = list_databases(
                    server=server,
                    auth=auth,
                    user=user,
                    password=password or None,
                    timeout=30,
                )
            except Exception as exc:
                err = str(exc)

            def finish() -> None:
                if err:
                    messagebox.showerror("List databases failed", _compact_dialog_error(err))
                    return
                self._agent_db_names = list(names)
                self._agent_db_checked = {n: True for n in names}
                self.agent_db_filter_var.set("")
                self._agent_rebuild_db_tree()
                self._sync_json_preview()

            self.frame.after(0, finish)

        threading.Thread(target=run, daemon=True).start()

    def _validate(self) -> None:
        try:
            from azure_migration_tool.worker.__main__ import _validate_job
        except ImportError:
            from worker.__main__ import _validate_job  # type: ignore[no-redef]

        try:
            job = self._job_from_preview()
        except Exception as exc:
            messagebox.showerror("Invalid JSON", _compact_dialog_error(str(exc)))
            return
        issues = _validate_job(job)
        if issues:
            messagebox.showwarning(
                "Validation",
                "Issues found:\n\n" + "\n".join(f"• {i}" for i in issues),
            )
        else:
            messagebox.showinfo("Validation", "Job looks valid.")

    def _examples_jobs_dir(self) -> Path:
        return Path(__file__).resolve().parents[2] / "examples" / "jobs"

    def _apply_prepared_job_to_ui(self, job: Dict[str, Any]) -> None:
        self.apply_job_dict(job)
        self.json_preview.delete("1.0", tk.END)
        self.json_preview.insert(tk.END, json.dumps(job, indent=2))

    def _import_spreadsheet(self) -> None:
        path = filedialog.askopenfilename(
            title="Import worker job CSV or Excel",
            filetypes=[
                ("CSV", "*.csv"),
                ("Excel", "*.xlsx *.xls"),
                ("All", "*.*"),
            ],
        )
        if not path:
            return
        try:
            try:
                from azure_migration_tool.src.orchestration.job_spreadsheet import read_job_spreadsheet
            except ImportError:
                from src.orchestration.job_spreadsheet import read_job_spreadsheet

            jobs = read_job_spreadsheet(path)
            if not jobs:
                raise ValueError("No jobs found in file.")
            if len(jobs) > 1:
                if not messagebox.askyesno(
                    "Multiple jobs",
                    f"Found {len(jobs)} jobs in the file.\n\nLoad the first job only?",
                ):
                    out_dir = filedialog.askdirectory(title="Folder to save all job JSON files")
                    if not out_dir:
                        return
                    folder = Path(out_dir)
                    folder.mkdir(parents=True, exist_ok=True)
                    try:
                        from azure_migration_tool.worker.__main__ import _prepare_job
                    except ImportError:
                        from worker.__main__ import _prepare_job  # type: ignore[no-redef]
                    for raw in jobs:
                        prepared = _prepare_job(dict(raw))
                        jid = (prepared.get("job_id") or "job").strip()
                        dest = folder / f"{jid}.json"
                        dest.write_text(json.dumps(prepared, indent=2), encoding="utf-8")
                    messagebox.showinfo("Exported", f"Saved {len(jobs)} JSON files to:\n{folder}")
                    self._apply_prepared_job_to_ui(_prepare_job(dict(jobs[0])))
                    self._log(f"Imported {len(jobs)} jobs; saved JSON to {folder}")
                    return
            try:
                from azure_migration_tool.worker.__main__ import _prepare_job
            except ImportError:
                from worker.__main__ import _prepare_job  # type: ignore[no-redef]
            prepared = _prepare_job(dict(jobs[0]))
            self._apply_prepared_job_to_ui(prepared)
            self._log(f"Imported job from {path}")
            messagebox.showinfo(
                "Import complete",
                "Spreadsheet loaded. Review Job JSON (defaults merged). Use Validate before Run.",
            )
        except Exception as exc:
            messagebox.showerror("Import failed", _compact_dialog_error(str(exc)))

    def _export_spreadsheet(self) -> None:
        self._sync_json_preview()
        try:
            job = self._job_from_preview()
        except Exception as exc:
            messagebox.showerror("Invalid JSON", _compact_dialog_error(str(exc)))
            return
        path = filedialog.asksaveasfilename(
            title="Export job as CSV or Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv"), ("All", "*.*")],
            initialfile=f"{self.job_id_var.get() or 'job'}_template.xlsx",
        )
        if not path:
            return
        try:
            try:
                from azure_migration_tool.src.orchestration.job_spreadsheet import write_job_spreadsheet
            except ImportError:
                from src.orchestration.job_spreadsheet import write_job_spreadsheet
            write_job_spreadsheet(path, [job], sample=False)
            self._log(f"Exported spreadsheet to {path}")
            messagebox.showinfo("Exported", f"Saved:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", _compact_dialog_error(str(exc)))

    def _save_sample_template(self) -> None:
        try:
            try:
                from azure_migration_tool.src.orchestration.job_spreadsheet import (
                    bundled_template_dir,
                    ensure_bundled_templates,
                    write_job_spreadsheet,
                )
            except ImportError:
                from src.orchestration.job_spreadsheet import (
                    bundled_template_dir,
                    ensure_bundled_templates,
                    write_job_spreadsheet,
                )
            ensure_bundled_templates()
            path = filedialog.asksaveasfilename(
                title="Save sample job template",
                defaultextension=".xlsx",
                initialdir=str(bundled_template_dir()),
                initialfile="worker_job_template.xlsx",
                filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv")],
            )
            if not path:
                bundled = bundled_template_dir()
                if messagebox.askyesno(
                    "Open samples folder",
                    f"Templates are also in:\n{bundled}\n\nOpen that folder?",
                ):
                    import os
                    import subprocess

                    subprocess.Popen(["explorer", str(bundled)])
                return
            write_job_spreadsheet(path, [], sample=True)
            self._log(f"Saved sample template to {path}")
            messagebox.showinfo(
                "Sample template",
                f"Saved sample with column headers and example row:\n{path}\n\n"
                "Use one row per database; same job_id groups databases into one job.",
            )
        except Exception as exc:
            messagebox.showerror("Sample template", _compact_dialog_error(str(exc)))

    def _export_migration_brief(self) -> None:
        summary = self._last_run_summary or {}
        result_path = (summary.get("result_path") or "").strip()
        if not result_path:
            messagebox.showinfo(
                "Migration brief",
                "Run a job first (or export from AgentOps run history).",
            )
            return
        job_id = (summary.get("job_id") or self.job_id_var.get() or "migration").strip()
        path = filedialog.asksaveasfilename(
            title="Export migration brief",
            defaultextension=".md",
            initialfile=f"migration_brief_{job_id}.md",
            filetypes=[("Markdown", "*.md"), ("JSON", "*.json")],
        )
        if not path:
            return
        log_tail = self.log_text.get("1.0", tk.END).strip().splitlines()
        try:
            try:
                from src.orchestration.migration_brief import export_migration_brief
            except ImportError:
                from azure_migration_tool.src.orchestration.migration_brief import (
                    export_migration_brief,
                )
            fmt = "json" if path.lower().endswith(".json") else "markdown"
            out = export_migration_brief(
                path,
                result_path=result_path,
                log_tail=log_tail,
                format=fmt,
            )
            messagebox.showinfo("Migration brief", f"Saved:\n{out}")
        except Exception as exc:
            messagebox.showerror("Migration brief", _compact_dialog_error(str(exc)))

    def _load_json(self) -> None:
        path = filedialog.askopenfilename(
            title="Load worker job JSON",
            filetypes=[("JSON", "*.json"), ("All", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                job = json.load(f)
            if not isinstance(job, dict):
                raise ValueError("Expected a JSON object.")
            self.apply_job_dict(job)
            self._sync_json_preview()
            self._log(f"Loaded job from {path}")
        except Exception as exc:
            messagebox.showerror("Load failed", _compact_dialog_error(str(exc)))

    def _save_json(self) -> None:
        self._sync_json_preview()
        default_dir = (self.project_path_var.get() or "").strip()
        initial = ""
        if default_dir:
            Path(default_dir).mkdir(parents=True, exist_ok=True)
            initial = str(Path(default_dir) / "worker_jobs" / f"{self.job_id_var.get()}.json")
        path = filedialog.asksaveasfilename(
            title="Save worker job JSON",
            defaultextension=".json",
            initialfile=Path(initial).name if initial else "job.json",
            initialdir=str(Path(initial).parent) if initial else "",
            filetypes=[("JSON", "*.json")],
        )
        if not path:
            return
        try:
            job = self._job_from_preview()
            Path(path).write_text(json.dumps(job, indent=2), encoding="utf-8")
            self._log(f"Saved job to {path}")
            messagebox.showinfo("Saved", f"Job saved:\n{path}")
        except Exception as exc:
            messagebox.showerror("Save failed", _compact_dialog_error(str(exc)))

    def _start_run(self) -> None:
        if self._run_thread and self._run_thread.is_alive():
            messagebox.showinfo("Busy", "A job is already running.")
            return
        self._apply_org_defaults_to_form()
        if self.step_backup_var.get() and not (self.local_path_var.get() or "").strip():
            if messagebox.askyesno(
                "Backup path missing",
                "Set the SQL backup UNC in Organization setup (step 2).\n\nOpen wizard now?",
            ):
                self._open_blob_settings()
            return
        if self.step_upload_var.get() or self.step_restore_var.get():
            try:
                from gui.utils.blob_config import blob_settings_configured
            except ImportError:
                from azure_migration_tool.gui.utils.blob_config import blob_settings_configured
            if not blob_settings_configured():
                if messagebox.askyesno(
                    "Blob not configured",
                    "Save Azure Blob storage once (org default) before upload/restore.\n\n"
                    "Open setup now?",
                ):
                    self._open_blob_settings()
                return
        self._sync_json_preview()
        try:
            job = self._job_from_preview()
        except Exception as exc:
            messagebox.showerror("Invalid JSON", _compact_dialog_error(str(exc)))
            return
        job["_run_origin"] = "gui-agent"

        try:
            from azure_migration_tool.worker.__main__ import _validate_job
        except ImportError:
            from worker.__main__ import _validate_job  # type: ignore[no-redef]
        issues = _validate_job(job)
        if issues:
            if not messagebox.askyesno(
                "Validation warnings",
                "Job has issues:\n\n"
                + "\n".join(issues[:8])
                + ("\n…" if len(issues) > 8 else "")
                + "\n\nRun anyway?",
            ):
                return

        self.run_btn.config(state=tk.DISABLED)
        self.status_var.set("Running…")
        self.log_text.delete("1.0", tk.END)

        def log(msg: str) -> None:
            self.frame.after(0, lambda m=msg: self._log(m))

        def run() -> None:
            try:
                from azure_migration_tool.src.orchestration.bulk_local_blob_pipeline import (
                    run_bulk_local_backup_blob_restore_job,
                )
            except ImportError:
                from src.orchestration.bulk_local_blob_pipeline import (
                    run_bulk_local_backup_blob_restore_job,
                )
            summary: Dict[str, Any] = {}
            try:
                summary = run_bulk_local_backup_blob_restore_job(job, log=log)
            except Exception as exc:
                log(f"ERROR: {exc}")
                summary = {"status": "failed", "errors": [str(exc)]}

            def done() -> None:
                self.run_btn.config(state=tk.NORMAL)
                self._last_run_summary = dict(summary)
                st = summary.get("status", "failed")
                self.status_var.set(f"Finished: {st}")
                if summary.get("result_path"):
                    self._log(f"Result file: {summary['result_path']}")
                preview = json.dumps(summary, indent=2)
                self.json_preview.delete("1.0", tk.END)
                self.json_preview.insert(tk.END, preview)
                if st == "success":
                    messagebox.showinfo("Job complete", f"Status: {st}")
                elif st == "completed_with_errors":
                    messagebox.showwarning("Job complete", f"Status: {st}\nCheck log and result JSON.")
                else:
                    messagebox.showerror("Job failed", f"Status: {st}\nCheck log.")

            self.frame.after(0, done)

        self._run_thread = threading.Thread(target=run, daemon=True)
        self._run_thread.start()
