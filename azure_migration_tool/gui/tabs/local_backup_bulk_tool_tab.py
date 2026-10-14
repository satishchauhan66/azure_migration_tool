# Author: Satish Chauhan
"""Tools > Local Backup — BACKUP TO DISK for multiple databases on one server."""

from __future__ import annotations

import os
import threading
import tkinter as tk
from tkinter import messagebox, ttk
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

try:
    from gui.widgets.bulk_pipeline_status import BulkPipelineStatusPanel, make_ui_progress_callback
except ImportError:
    from azure_migration_tool.gui.widgets.bulk_pipeline_status import (
        BulkPipelineStatusPanel,
        make_ui_progress_callback,
    )

try:
    from gui.utils.database_utils import list_databases
except ImportError:
    from azure_migration_tool.gui.utils.database_utils import list_databases


class LocalBackupBulkToolTab(BackupRestoreTab):
    """Local backup + optional blob upload; backs up many databases in one run."""

    _CHECKED = "\u2611"  # ☑
    _UNCHECKED = "\u2610"  # ☐

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
            text="Local Backup (multiple databases)",
            font=("Arial", 12, "bold"),
        ).pack(pady=(0, 8))

        self._create_bulk_local_step1(scrollable_frame)
        self._create_bulk_restore_target_section(scrollable_frame)
        self._bulk_pipeline_panel = BulkPipelineStatusPanel(scrollable_frame)
        self._create_local_backup_shared_sections(scrollable_frame, blob_prefix="bulk")

        self.local_backup_btn.configure(
            text="Run pipeline",
            command=self._start_bulk_local_create_backup,
            width=18,
        )
        self.local_upload_btn.configure(text="Upload only (session)", width=20)
        btn_row = self.local_backup_btn.master
        self.local_restore_all_blob_btn = ttk.Button(
            btn_row,
            text="Restore only (session)",
            command=self._start_bulk_restore_from_completed_runs,
            width=20,
        )
        self.local_restore_all_blob_btn.pack(side=tk.LEFT, padx=5)
        tk.Label(
            btn_row,
            text="Each DB: backup → upload → restore as soon as the prior step finishes.",
            fg="gray",
        ).pack(side=tk.LEFT, padx=(12, 0))
        self.bulk_progress_var = tk.StringVar(value="")

        self._bulk_completed_runs: List[Dict[str, Any]] = []

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _create_bulk_restore_target_section(self, parent) -> None:
        step4 = ttk.LabelFrame(
            parent,
            text="Pipeline options (per database, no wait for other DBs)",
            padding=10,
        )
        step4.pack(fill=tk.X, padx=5, pady=5)

        self.restore_blob_server_var = self.main_window.shared_dest_server
        self.restore_blob_db_var = self.main_window.shared_dest_db
        self.restore_blob_auth_var = self.main_window.shared_dest_auth
        self.restore_blob_user_var = self.main_window.shared_dest_user
        self.restore_blob_password_var = self.main_window.shared_dest_password
        self.restore_blob_managed_instance_var = tk.BooleanVar(value=False)

        step4.columnconfigure(0, weight=0)
        step4.columnconfigure(1, weight=1)
        self._bulk_restore_conn_widget = ConnectionWidget(
            parent=step4,
            server_var=self.restore_blob_server_var,
            db_var=self.restore_blob_db_var,
            auth_var=self.restore_blob_auth_var,
            user_var=self.restore_blob_user_var,
            password_var=self.restore_blob_password_var,
            label_text="Target SQL Server (each DB restores under its own name)",
            row_start=0,
            server_only=True,
        )
        opt_row = self._bulk_restore_conn_widget.grid_last_row + 1
        ttk.Checkbutton(
            step4,
            text="Target is Azure SQL Managed Instance",
            variable=self.restore_blob_managed_instance_var,
        ).grid(row=opt_row, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))
        self.bulk_auto_upload_after_backup_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            step4,
            text="Step 2 — Upload to blob immediately after that database’s backup",
            variable=self.bulk_auto_upload_after_backup_var,
        ).grid(row=opt_row + 1, column=0, columnspan=2, sticky=tk.W, pady=(6, 0))
        self.bulk_auto_restore_after_upload_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            step4,
            text="Step 3 — Restore immediately after that database’s upload",
            variable=self.bulk_auto_restore_after_upload_var,
        ).grid(row=opt_row + 2, column=0, columnspan=2, sticky=tk.W, pady=(6, 0))
        self.bulk_auto_countdown_var = tk.StringVar(value="10")
        tk.Label(
            step4,
            text="Use Upload only / Restore only for databases already finished in this session.",
            fg="gray",
            wraplength=680,
            justify=tk.LEFT,
        ).grid(row=opt_row + 3, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))

    def _create_bulk_local_step1(self, parent) -> None:
        step1 = ttk.LabelFrame(parent, text="Step 1: SQL Server and databases", padding=10)
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
            server_only=True,
        )

        # ConnectionWidget uses grid on ``step1``; database list must use grid too (not pack).
        step1.columnconfigure(0, weight=0)
        step1.columnconfigure(1, weight=1)
        _db_row = self.local_conn_widget.grid_last_row + 1
        db_frame = ttk.LabelFrame(step1, text="Databases to back up", padding=8)
        db_frame.grid(row=_db_row, column=0, columnspan=2, sticky=tk.EW, pady=(10, 0))

        self._bulk_db_names: List[str] = []
        self._bulk_db_checked: Dict[str, bool] = {}

        filter_row = ttk.Frame(db_frame)
        filter_row.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(filter_row, text="Filter:").pack(side=tk.LEFT)
        self.bulk_db_filter_var = tk.StringVar()
        self.bulk_db_filter_var.trace_add("write", lambda *_: self._bulk_rebuild_db_tree())
        ttk.Entry(filter_row, textvariable=self.bulk_db_filter_var, width=42).pack(
            side=tk.LEFT, padx=(6, 0), fill=tk.X, expand=True
        )

        list_row = ttk.Frame(db_frame)
        list_row.pack(fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(list_row)
        self.bulk_db_tree = ttk.Treeview(
            list_row,
            columns=("check", "database"),
            show="headings",
            selectmode="browse",
            height=12,
            yscrollcommand=sb.set,
        )
        self.bulk_db_tree.heading("check", text="")
        self.bulk_db_tree.heading("database", text="Database")
        self.bulk_db_tree.column("check", width=32, anchor=tk.CENTER, stretch=False)
        self.bulk_db_tree.column("database", width=420, anchor=tk.W)
        sb.config(command=self.bulk_db_tree.yview)
        self.bulk_db_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.bulk_db_tree.bind("<Button-1>", self._on_bulk_db_tree_click)

        self.bulk_db_selection_var = tk.StringVar(value="0 selected")
        ttk.Label(db_frame, textvariable=self.bulk_db_selection_var).pack(anchor=tk.W, pady=(4, 0))

        btn_row = ttk.Frame(db_frame)
        btn_row.pack(fill=tk.X, pady=(6, 0))
        ttk.Button(btn_row, text="Refresh list", command=self._refresh_bulk_database_list, width=14).pack(
            side=tk.LEFT
        )
        ttk.Button(btn_row, text="Select all", command=self._bulk_select_all_databases, width=12).pack(
            side=tk.LEFT, padx=(6, 0)
        )
        ttk.Button(btn_row, text="Clear all", command=self._bulk_clear_database_selection, width=12).pack(
            side=tk.LEFT, padx=(6, 0)
        )

        self._local_stripe_refresh_after_id = None
        self.local_server_var.trace_add("write", self._schedule_local_stripe_refresh)

    def _bulk_update_selection_label(self) -> None:
        n = sum(1 for name in self._bulk_db_names if self._bulk_db_checked.get(name))
        self.bulk_db_selection_var.set(f"{n} of {len(self._bulk_db_names)} selected")

    def _bulk_rebuild_db_tree(self) -> None:
        if not hasattr(self, "bulk_db_tree"):
            return
        q = (self.bulk_db_filter_var.get() or "").strip().lower()
        for item in self.bulk_db_tree.get_children():
            self.bulk_db_tree.delete(item)
        for name in self._bulk_db_names:
            if q and q not in name.lower():
                continue
            mark = self._CHECKED if self._bulk_db_checked.get(name) else self._UNCHECKED
            self.bulk_db_tree.insert("", tk.END, values=(mark, name))
        self._bulk_update_selection_label()

    def _on_bulk_db_tree_click(self, event: tk.Event) -> Optional[str]:
        if "disabled" in self.bulk_db_tree.state():
            return None
        if self.bulk_db_tree.identify_region(event.x, event.y) != "cell":
            return None
        item = self.bulk_db_tree.identify_row(event.y)
        if not item:
            return None
        name = (self.bulk_db_tree.set(item, "database") or "").strip()
        if not name or name not in self._bulk_db_checked:
            return None
        current = self._bulk_db_checked.get(name, False)
        self._bulk_db_checked[name] = not current
        mark = self._CHECKED if self._bulk_db_checked[name] else self._UNCHECKED
        self.bulk_db_tree.set(item, "check", mark)
        self._bulk_update_selection_label()
        return "break"

    def _bulk_select_all_databases(self) -> None:
        for name in self._bulk_db_names:
            self._bulk_db_checked[name] = True
        self._bulk_rebuild_db_tree()

    def _bulk_clear_database_selection(self) -> None:
        for name in self._bulk_db_names:
            self._bulk_db_checked[name] = False
        self._bulk_rebuild_db_tree()

    def _selected_bulk_databases(self) -> List[str]:
        return [n for n in self._bulk_db_names if self._bulk_db_checked.get(n)]

    def _refresh_bulk_database_list(self) -> None:
        server = (self.local_server_var.get() or "").strip()
        if not server:
            messagebox.showwarning("Server required", "Enter a server name first.")
            return
        auth = (self.local_auth_var.get() or "windows").strip()
        user = (self.local_user_var.get() or "").strip()
        password = (self.local_password_var.get() or "").strip()

        def run() -> None:
            err = None
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
                    messagebox.showerror(
                        "List databases failed",
                        _compact_dialog_error(err),
                    )
                    return
                self._bulk_db_names = list(names)
                self._bulk_db_checked = {n: True for n in names}
                self.bulk_db_filter_var.set("")
                self._bulk_rebuild_db_tree()

            self.frame.after(0, finish)

        threading.Thread(target=run, daemon=True).start()

    def _start_bulk_local_create_backup(self) -> None:
        try:
            from src.orchestration.bulk_local_blob_pipeline import (
                run_bulk_local_backup_blob_restore_job,
            )
        except ImportError:
            try:
                from azure_migration_tool.src.orchestration.bulk_local_blob_pipeline import (
                    run_bulk_local_backup_blob_restore_job,
                )
            except ImportError:
                run_bulk_local_backup_blob_restore_job = None
        if not run_bulk_local_backup_blob_restore_job:
            messagebox.showerror("Error", "Local backup module not available.")
            return

        server = (self.local_server_var.get() or "").strip()
        databases = self._selected_bulk_databases()
        local_path = (self.local_backup_path_var.get() or "").strip()
        if not server:
            messagebox.showerror("Error", "Server is required.")
            return
        if not databases:
            messagebox.showerror(
                "Error",
                "Select at least one database (Refresh list, then check the databases to back up).",
            )
            return
        if not local_path:
            messagebox.showerror("Error", "Backup path is required.")
            return

        self._local_stop_event.clear()
        self._local_active_conn = None
        self._bulk_completed_runs = []
        self._local_set_busy(True)
        self.local_backup_log.delete("1.0", tk.END)
        auto_upload = bool(self.bulk_auto_upload_after_backup_var.get())
        auto_restore = auto_upload and bool(self.bulk_auto_restore_after_upload_var.get())
        self._bulk_pipeline_panel.reset(
            databases,
            do_backup=True,
            do_upload=auto_upload,
            do_restore=auto_restore,
        )
        self.local_upload_status_var.set(
            f"Pipeline running — up to {self._bulk_parallel_databases()} databases in parallel"
        )

        def log(msg: str) -> None:
            self.frame.after(
                0,
                lambda m=msg: (
                    self.local_backup_log.insert(tk.END, m + "\n"),
                    self.local_backup_log.see(tk.END),
                ),
            )

        progress_cb = make_ui_progress_callback(
            self._bulk_pipeline_panel,
            lambda fn: self.frame.after(0, fn),
        )

        def run() -> None:
            if auto_upload and self._pipeline_storage_dict() is None:
                self.frame.after(0, lambda: self._local_set_busy(False))
                return
            if auto_restore:
                ready, reason = self._restore_target_server_ready()
                if not ready:
                    self.frame.after(
                        0,
                        lambda: messagebox.showerror(
                            "Restore target",
                            reason + "\n\nUncheck auto-restore or set Step 4 target server.",
                        ),
                    )
                    self.frame.after(0, lambda: self._local_set_busy(False))
                    return

            val = (self.local_stripes_var.get() or "Auto").strip()
            stripes_job = "auto" if val.lower() == "auto" else val
            job: Dict[str, Any] = {
                "_run_origin": "gui-bulk",
                "pipeline": {"max_parallel_databases": self._bulk_parallel_databases()},
                "steps": {
                    "backup": True,
                    "upload": auto_upload,
                    "restore": auto_restore,
                },
                "source": {
                    "server": server,
                    "auth": self.local_auth_var.get() or "windows",
                    "user": self.local_user_var.get() or "",
                    "password": self.local_password_var.get() or "",
                    "databases": databases,
                },
                "local_backup": {
                    "path": local_path,
                    "compression": self.local_compression_var.get(),
                    "structured_paths": self.local_structured_paths_var.get(),
                    "stripes": stripes_job,
                },
                "blob": self._gui_blob_job_section(),
                "restore_target": self._pipeline_restore_target() if auto_restore else {},
            }
            log(
                f"=== Per-database pipeline: {len(databases)} DB(s), "
                f"up to {self._bulk_parallel_databases()} in parallel ==="
            )
            summary = run_bulk_local_backup_blob_restore_job(
                job,
                log=log,
                cancel_event=self._local_stop_event,
                progress_callback=progress_cb,
            )
            successful_runs: List[Dict[str, Any]] = list(summary.get("backup_runs") or [])
            backup_step = (summary.get("steps") or {}).get("backup") or {}
            ok_count = int(backup_step.get("ok") or len(successful_runs))
            fail_count = int(backup_step.get("failed") or 0)
            if summary.get("errors"):
                for err in summary["errors"][:20]:
                    log(f"ERROR: {err}")

            def done() -> None:
                pipe_rows = ((summary.get("steps") or {}).get("pipeline") or {}).get("databases")
                if pipe_rows:
                    self._bulk_pipeline_panel.apply_pipeline_rows(pipe_rows)
                self.local_upload_status_var.set(
                    f"Pipeline done — backup OK: {ok_count}, failed: {fail_count}"
                )
                self._bulk_completed_runs = list(successful_runs)
                if successful_runs:
                    try:
                        from src.backup.backup_path_utils import format_run_folder_for_ui
                    except ImportError:
                        from azure_migration_tool.src.backup.backup_path_utils import (
                            format_run_folder_for_ui,
                        )
                    last_files = successful_runs[-1]["files"]
                    self.local_upload_file_var.set(
                        format_run_folder_for_ui(last_files)
                        + (f"  (+{len(successful_runs) - 1} more DB)" if len(successful_runs) > 1 else "")
                    )
                    self._refresh_local_upload_status()
                msg = f"Backups finished.\n\nSucceeded: {ok_count}\nFailed: {fail_count}"
                if job["steps"].get("upload"):
                    st = (summary.get("status") or "finished").replace("_", " ")
                    messagebox.showinfo("Pipeline", f"Finished with status: {st}")
                elif fail_count and ok_count:
                    messagebox.showwarning("Bulk backup", msg)
                elif fail_count:
                    messagebox.showwarning("Bulk backup", msg)
                elif ok_count:
                    messagebox.showinfo("Bulk backup", msg)
                self._local_set_busy(False)

            self.frame.after(0, done)

        threading.Thread(target=run, daemon=True).start()

    def _stop_local_process(self) -> None:
        self._restore_blob_stop_event.set()
        super()._stop_local_process()

    def _bulk_upload_runs(self) -> List[Dict[str, Any]]:
        return list(getattr(self, "_bulk_completed_runs", None) or [])

    def _resolve_local_upload_paths(self) -> List[str]:
        runs = self._bulk_upload_runs()
        if len(runs) > 1:
            paths: List[str] = []
            for spec in runs:
                paths.extend(f for f in (spec.get("files") or []) if f)
            if paths:
                return paths
        return super()._resolve_local_upload_paths()

    def _start_bulk_restore_from_completed_runs(self) -> None:
        self._cancel_bulk_auto_restore_prompt()
        runs = self._bulk_upload_runs()
        jobs = self._bulk_restore_jobs_from_backup_runs(runs)
        if not jobs:
            messagebox.showerror(
                "Error",
                "No completed backups in this session. Run backup (and upload) first, "
                "or use Tools → Restore from Blob (multiple DBs).",
            )
            return
        self._start_restore_multiple_from_blob(jobs)

    def _refresh_local_upload_status(self) -> None:
        tracker = self._local_upload_tracker
        if tracker is not None:
            self.local_upload_status_var.set(tracker.format_status_line())
            return
        runs = self._bulk_upload_runs()
        if len(runs) > 1:
            paths: List[str] = []
            for spec in runs:
                paths.extend(spec.get("files") or [])
            if paths:
                total = 0
                for p in paths:
                    try:
                        total += os.path.getsize(os.path.normpath(p))
                    except OSError:
                        pass
                gb = total / (1024**3) if total else 0.0
                self.local_upload_status_var.set(
                    f"Ready: {len(runs)} database(s), {len(paths)} .bak file(s), ~{gb:.2f} GB"
                )
                return
        super()._refresh_local_upload_status()

    def _start_local_upload_to_blob(self) -> None:
        self._cancel_local_auto_upload_prompt()
        runs = self._bulk_upload_runs()
        if len(runs) > 1:
            self._start_local_upload_multiple_runs_to_blob(runs)
            return
        if len(runs) == 1:
            spec = runs[0]
            self.local_db_var.set(spec.get("database") or "")
            if spec.get("run_id"):
                self._local_last_run_id = spec["run_id"]
            files = spec.get("files") or []
            if files:
                try:
                    from src.backup.backup_path_utils import format_run_folder_for_ui
                except ImportError:
                    from azure_migration_tool.src.backup.backup_path_utils import (
                        format_run_folder_for_ui,
                    )
                self.local_upload_file_var.set(format_run_folder_for_ui(files))
                self._local_last_backup_files = list(files)
        super()._start_local_upload_to_blob()

    def _on_bulk_blob_upload_finished(
        self,
        runs: List[Dict[str, Any]],
        uploaded: int,
        failed: int,
    ) -> bool:
        if failed or uploaded <= 0:
            return False
        if not self.bulk_auto_restore_after_upload_var.get():
            return False
        jobs = self._bulk_restore_jobs_from_backup_runs(runs)
        if not jobs:
            return False
        ready, reason = self._restore_target_server_ready()
        if not ready:
            messagebox.showinfo(
                "Upload complete",
                f"Uploaded {uploaded} file(s) for {len(jobs)} database(s).\n\n"
                "Automatic restore was not started:\n"
                f"{reason}\n\n"
                "Set Step 4 target server, then click '3. Restore all from Blob'.",
            )
            return True
        if self._restore_blob_storage_inputs() is None:
            return True
        summary = (
            f"Upload completed.\n\n{uploaded} file(s) uploaded for {len(jobs)} database(s)."
        )
        self._prompt_auto_restore_after_bulk_blob(
            summary,
            jobs,
            countdown_sec=self._bulk_auto_countdown_seconds(),
        )
        return True
