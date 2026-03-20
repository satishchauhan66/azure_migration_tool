# Author: Sa-tish Chauhan

"""
Backup & Restore tab: .bak to Azure Blob and Restore from Blob.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from pathlib import Path
import threading
import sys
import os
import json

parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.widgets.connection_widget import ConnectionWidget
from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll


class BackupRestoreTab:
    """Tab for .bak backup to Azure Blob and restore from Blob."""

    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None
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
        notebook.add(bak_frame, text=".bak to Blob")
        self._create_bak_to_blob_widgets(bak_frame)

        restore_frame = ttk.Frame(notebook)
        notebook.add(restore_frame, text="Restore from Blob")
        self._create_restore_from_blob_widgets(restore_frame)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _get_bak_blob_config_path(self):
        """Path to saved blob connection string (per-user app data)."""
        if os.name == "nt":
            base = os.environ.get("APPDATA", os.path.expanduser("~"))
            base = Path(base) / "AzureMigrationTool"
        else:
            base = Path(os.path.expanduser("~")) / ".azure_migration_tool"
        base.mkdir(parents=True, exist_ok=True)
        return base / "blob_settings.json"

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
        self.bak_server_var = tk.StringVar()
        self.bak_db_var = tk.StringVar()
        self.bak_auth_var = tk.StringVar(value="windows")
        self.bak_user_var = tk.StringVar()
        self.bak_password_var = tk.StringVar()
        ConnectionWidget(
            parent=step1,
            server_var=self.bak_server_var,
            db_var=self.bak_db_var,
            auth_var=self.bak_auth_var,
            user_var=self.bak_user_var,
            password_var=self.bak_password_var,
            label_text="",
            row_start=0,
        )

        step2 = ttk.LabelFrame(parent, text="Step 2: Azure Blob storage", padding=10)
        step2.pack(fill=tk.X, padx=5, pady=5)
        tk.Label(step2, text="Connection string (AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net):").pack(
            anchor=tk.W
        )
        self.bak_blob_conn_var = tk.StringVar()
        ttk.Entry(step2, textvariable=self.bak_blob_conn_var, width=70).pack(fill=tk.X, pady=2)
        tk.Label(step2, text="Container name (e.g. db2-stage):").pack(anchor=tk.W, pady=(8, 0))
        self.bak_container_var = tk.StringVar(value="db2-stage")
        ttk.Entry(step2, textvariable=self.bak_container_var, width=30).pack(fill=tk.X, pady=2)
        blob_btn_row = ttk.Frame(step2)
        blob_btn_row.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(blob_btn_row, text="Save connection string", command=self._save_bak_blob_settings).pack(
            side=tk.LEFT, padx=5
        )
        self._load_bak_blob_settings()

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

    def _load_bak_blob_settings(self):
        """Load saved blob connection string and container into the form."""
        try:
            path = self._get_bak_blob_config_path()
            if not path.exists():
                return
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data.get("blob_connection_string"), str):
                self.bak_blob_conn_var.set(data["blob_connection_string"])
            if isinstance(data.get("container"), str):
                self.bak_container_var.set(data["container"])
        except Exception:
            pass

    def _save_bak_blob_settings(self):
        """Save current blob connection string and container to disk."""
        try:
            path = self._get_bak_blob_config_path()
            data = {
                "blob_connection_string": self.bak_blob_conn_var.get().strip(),
                "container": (self.bak_container_var.get() or "db2-stage").strip() or "db2-stage",
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            messagebox.showinfo(
                "Saved",
                "Blob connection string and container have been saved.\nThey will be loaded next time you open this tab.",
            )
        except Exception as e:
            messagebox.showerror("Save failed", str(e))

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
        server = (self.bak_server_var.get() or "").strip()
        database = (self.bak_db_var.get() or "").strip()
        if not server or not database:
            messagebox.showerror("Error", "Server and database are required.")
            return
        conn_str = (self.bak_blob_conn_var.get() or "").strip()
        if not conn_str:
            messagebox.showerror("Error", "Blob connection string is required.")
            return
        container = (self.bak_container_var.get() or "db2-stage").strip()

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
                )
                if summary.get("status") == "success":
                    log(f"Blob path: {summary.get('container')}/{summary.get('blob_path')}")
                    size_mb = summary.get("blob_size")
                    size_msg = f" ({size_mb / (1024*1024):.1f} MB)" if size_mb is not None else ""
                    self.frame.after(
                        0, lambda m=size_msg: messagebox.showinfo("Success", f"Backup to blob completed successfully.{m}")
                    )
                else:
                    err = summary.get("error") or "Unknown error"
                    self.frame.after(0, lambda msg=err: messagebox.showerror("Backup failed", msg))
            except Exception as e:
                log(str(e))
                self.frame.after(0, lambda m=str(e): messagebox.showerror("Error", m))
            finally:
                self.frame.after(0, lambda: self.bak_to_blob_btn.config(state=tk.NORMAL))

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
        tk.Label(step1, text="Connection string (same as .bak to Blob; load/save shared):").pack(anchor=tk.W)
        self.restore_blob_conn_var = tk.StringVar()
        ttk.Entry(step1, textvariable=self.restore_blob_conn_var, width=70).pack(fill=tk.X, pady=2)
        tk.Label(step1, text="Container name (e.g. db2-stage):").pack(anchor=tk.W, pady=(8, 0))
        self.restore_container_var = tk.StringVar(value="db2-stage")
        ttk.Entry(step1, textvariable=self.restore_container_var, width=30).pack(fill=tk.X, pady=2)
        blob_btn_row = ttk.Frame(step1)
        blob_btn_row.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(blob_btn_row, text="Save connection string", command=self._save_restore_blob_settings).pack(
            side=tk.LEFT, padx=5
        )
        self._load_restore_blob_settings()

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

        step2 = ttk.LabelFrame(parent, text="Step 2: Target SQL Server", padding=10)
        step2.pack(fill=tk.X, padx=5, pady=5)
        self.restore_blob_server_var = tk.StringVar()
        self.restore_blob_db_var = tk.StringVar()
        self.restore_blob_auth_var = tk.StringVar(value="windows")
        self.restore_blob_user_var = tk.StringVar()
        self.restore_blob_password_var = tk.StringVar()
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

        log_frame = ttk.LabelFrame(parent, text="Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.restore_from_blob_log = scrolledtext.ScrolledText(log_frame, height=8, wrap=tk.WORD)
        self.restore_from_blob_log.pack(fill=tk.BOTH, expand=True)

    def _load_restore_blob_settings(self):
        """Load blob connection string and container into restore fields (same file as .bak to Blob)."""
        try:
            path = self._get_bak_blob_config_path()
            if not path.exists():
                return
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data.get("blob_connection_string"), str):
                self.restore_blob_conn_var.set(data["blob_connection_string"])
            if isinstance(data.get("container"), str):
                self.restore_container_var.set(data["container"])
        except Exception:
            pass

    def _save_restore_blob_settings(self):
        """Save restore blob connection string and container (same file as .bak to Blob)."""
        try:
            path = self._get_bak_blob_config_path()
            data = {
                "blob_connection_string": self.restore_blob_conn_var.get().strip(),
                "container": (self.restore_container_var.get() or "db2-stage").strip() or "db2-stage",
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            messagebox.showinfo("Saved", "Blob connection string and container have been saved.")
        except Exception as e:
            messagebox.showerror("Save failed", str(e))

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
        if idx < len(items):
            path = items[idx]
            self.restore_blob_path_var.set(path)
            if "/" in path:
                self.restore_blob_db_var.set(path.split("/", 1)[0])

    def _list_restore_databases(self):
        """Discover top-level folder names in container and populate combobox."""
        conn_str = (self.restore_blob_conn_var.get() or "").strip()
        container = (self.restore_container_var.get() or "db2-stage").strip()
        if not conn_str:
            messagebox.showerror("Error", "Enter blob connection string first.")
            return
        self.restore_db_filter_var.set("Listing...")

        def run():
            try:
                from azure.storage.blob import BlobServiceClient

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
                self.frame.after(0, lambda: self._populate_restore_databases_combo([], str(e)))

        threading.Thread(target=run, daemon=True).start()

    def _populate_restore_databases_combo(self, names, error=None):
        """Update database filter combobox (called on UI thread)."""
        if error:
            self.restore_db_filter_var.set("")
            self.restore_db_filter_combo["values"] = []
            messagebox.showerror("List databases failed", error)
            return
        self.restore_db_filter_combo["values"] = names
        if names:
            self.restore_db_filter_var.set(names[0])
            self.restore_blob_db_var.set(names[0])
        else:
            self.restore_db_filter_var.set("(no folders found)")

    def _list_restore_backups(self):
        """List .bak blobs under the chosen database folder only."""
        conn_str = (self.restore_blob_conn_var.get() or "").strip()
        container = (self.restore_container_var.get() or "db2-stage").strip()
        db_name = (self.restore_db_filter_var.get() or "").strip()
        if not conn_str:
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
                from azure.storage.blob import BlobServiceClient

                client = BlobServiceClient.from_connection_string(conn_str)
                container_client = client.get_container_client(container)
                names = [
                    b.name for b in container_client.list_blobs(name_starts_with=prefix) if b.name.endswith(".bak")
                ]
                names.sort(reverse=True)
                self.frame.after(0, lambda: self._populate_restore_backups_list(names))
            except Exception as e:
                self.frame.after(0, lambda: self._populate_restore_backups_list([], str(e)))

        threading.Thread(target=run, daemon=True).start()

    def _populate_restore_backups_list(self, names, error=None):
        """Update listbox with backup names (called on UI thread)."""
        self.restore_backups_listbox.delete(0, tk.END)
        if error:
            self.restore_backups_listbox.insert(tk.END, f"Error: {error}")
            return
        if not names:
            self.restore_backups_listbox.insert(tk.END, "(no .bak files found)")
            return
        for n in names:
            self.restore_backups_listbox.insert(tk.END, n)

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
        blob_path = (self.restore_blob_path_var.get() or "").strip()
        if not blob_path or blob_path.startswith("(") or blob_path.startswith("Error"):
            messagebox.showerror("Error", "Select a backup from the list (click 'List backups' then select a .bak).")
            return
        conn_str = (self.restore_blob_conn_var.get() or "").strip()
        container = (self.restore_container_var.get() or "db2-stage").strip()
        server = (self.restore_blob_server_var.get() or "").strip()
        database = (self.restore_blob_db_var.get() or "").strip()
        if not conn_str or not server or not database:
            messagebox.showerror("Error", "Blob connection string, target server, and target database are required.")
            return

        self.restore_from_blob_btn.config(state=tk.DISABLED)
        self.restore_from_blob_log.delete("1.0", tk.END)

        def log(msg):
            self.frame.after(0, lambda: self.restore_from_blob_log.insert(tk.END, msg + "\n"))
            self.frame.after(0, lambda: self.restore_from_blob_log.see(tk.END))

        def run():
            try:
                summary = run_restore_from_blob(
                    server=server,
                    database=database,
                    auth=self.restore_blob_auth_var.get() or "windows",
                    user=self.restore_blob_user_var.get() or None,
                    password=self.restore_blob_password_var.get() or None,
                    blob_connection_string=conn_str,
                    container=container,
                    blob_path=blob_path,
                    log_callback=log,
                    target_managed_instance=self.restore_blob_managed_instance_var.get(),
                )
                if summary.get("status") == "success":
                    self.frame.after(
                        0, lambda: messagebox.showinfo("Success", "Restore from blob completed successfully.")
                    )
                else:
                    err = summary.get("error") or "Unknown error"
                    self.frame.after(0, lambda msg=err: messagebox.showerror("Restore failed", msg))
            except Exception as e:
                log(str(e))
                self.frame.after(0, lambda m=str(e): messagebox.showerror("Error", m))
            finally:
                self.frame.after(0, lambda: self.restore_from_blob_btn.config(state=tk.NORMAL))

        threading.Thread(target=run, daemon=True).start()
