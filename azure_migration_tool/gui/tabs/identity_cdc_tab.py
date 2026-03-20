# Author: Satish Chauhan

"""
IDENTITY (CDC) tab: connect to Azure SQL, fetch all identity columns,
disable identity (save to run folder: state + script) for ADF data load,
then restore identity from run folder with optional reseed.
"""

import os
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from pathlib import Path
import threading
import sys

parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

try:
    from gui.widgets.connection_widget import ConnectionWidget
except ImportError:
    from azure_migration_tool.gui.widgets.connection_widget import ConnectionWidget

try:
    from utils.identity_cdc import (
        fetch_identity_columns,
        save_state_file,
        load_state_file,
        generate_disable_script,
        generate_restore_script,
        run_disable,
        run_restore,
        _split_sql_on_go,
    )
except ImportError:
    from azure_migration_tool.utils.identity_cdc import (
        fetch_identity_columns,
        save_state_file,
        load_state_file,
        generate_disable_script,
        generate_restore_script,
        run_disable,
        run_restore,
        _split_sql_on_go,
    )

try:
    from src.utils.paths import short_slug, utc_ts_compact
except ImportError:
    try:
        from azure_migration_tool.src.utils.paths import short_slug, utc_ts_compact
    except ImportError:
        import re
        import hashlib
        from datetime import datetime, timezone
        def safe_name(s):
            s = (s or "").strip()
            s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s)
            return s[:200] if len(s) > 200 else s
        def short_slug(s, max_prefix=28):
            s = (s or "").strip()
            prefix = safe_name(s)[:max_prefix]
            h = hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()[:8]
            return f"{prefix}_{h}" if prefix else h
        def utc_ts_compact():
            return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# Run folder file names
STATE_FILENAME = "identity_state.json"
DISABLE_SCRIPT_NAME = "disable_identity.sql"
RESTORE_SCRIPT_NAME = "restore_identity.sql"
LAST_RUN_FOLDER_FILENAME = "last_identity_run_folder.txt"


class IdentityCDCTab:
    """Tab for disabling and restoring IDENTITY columns (CDC / ADF workflow)."""

    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None
        self._identity_columns = []
        self._conn = None
        self._last_script = None
        self._last_run_folder = ""
        self._create_widgets()
        self._load_last_run_folder()

    def set_project_path(self, project_path):
        """Set project root path for default run folder (called from main/project tab)."""
        self.project_path = Path(project_path) if project_path else None

    def _get_default_run_folder(self) -> str:
        """Default run folder: project_path/identity_cdc_runs/<server>_<db>_<timestamp> or cwd fallback."""
        base = None
        if self.project_path and getattr(self.project_path, "exists", lambda: False)():
            base = Path(self.project_path) / "identity_cdc_runs"
        if not base or not getattr(base, "exists", lambda: False)():
            base = Path(os.getcwd()) / "identity_cdc_runs"
        server = (self.server_var.get() or "server").strip()
        database = (self.db_var.get() or "db").strip()
        slug_s = short_slug(server)[:20]
        slug_d = short_slug(database)[:20]
        ts = utc_ts_compact()
        folder_name = f"{slug_s}_{slug_d}_{ts}"
        return str(base / folder_name)

    def _last_run_folder_file(self) -> Path:
        """Path to persist last run folder (project or APPDATA)."""
        if self.project_path is not None:
            p = Path(self.project_path) if not isinstance(self.project_path, Path) else self.project_path
            return p / "identity_cdc_runs" / LAST_RUN_FOLDER_FILENAME
        appdata = os.environ.get("APPDATA") or os.path.expanduser("~")
        return Path(appdata) / "AzureMigrationTool" / LAST_RUN_FOLDER_FILENAME

    def _save_last_run_folder(self, folder: str):
        """Remember last run folder and persist it."""
        folder = (folder or "").strip()
        if not folder:
            return
        self._last_run_folder = folder
        try:
            fpath = self._last_run_folder_file()
            fpath.parent.mkdir(parents=True, exist_ok=True)
            fpath.write_text(folder, encoding="utf-8")
        except Exception:
            pass

    def _load_last_run_folder(self):
        """Load last run folder from file and set restore folder entry if widget exists."""
        try:
            fpath = self._last_run_folder_file()
            if getattr(fpath, "exists", lambda: False)() and fpath.is_file():
                self._last_run_folder = fpath.read_text(encoding="utf-8").strip()
                if hasattr(self, "restore_run_folder_var") and self._last_run_folder:
                    self.restore_run_folder_var.set(self._last_run_folder)
        except Exception:
            pass

    def _log(self, msg: str):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def _create_widgets(self):
        # Scrollable container (so tab content can scroll)
        canvas = tk.Canvas(self.frame)
        scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        def _update_scroll_region(event=None):
            canvas.update_idletasks()
            w = max(canvas.winfo_width(), scrollable_frame.winfo_reqwidth())
            h = max(canvas.winfo_height(), scrollable_frame.winfo_reqheight())
            canvas.configure(scrollregion=(0, 0, w, h))

        scrollable_frame.bind("<Configure>", _update_scroll_region)
        canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        def _on_canvas_configure(event):
            canvas.itemconfig(canvas_window, width=event.width)
            _update_scroll_region()
        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind("<Enter>", lambda e: canvas.bind_all("<MouseWheel>", _on_mousewheel))
        canvas.bind("<Leave>", lambda e: canvas.unbind_all("<MouseWheel>"))

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # All content goes into scrollable_frame
        content = scrollable_frame

        title = tk.Label(
            content,
            text="IDENTITY (CDC) — Disable / Restore",
            font=("Arial", 16, "bold"),
        )
        title.pack(pady=10)

        help_frame = ttk.LabelFrame(content, text="About", padding=10)
        help_frame.pack(fill=tk.X, padx=20, pady=5)
        help_text = (
            "Connect to Azure SQL, fetch all identity columns, then disable identity (save state file) before ADF data load. "
            "After migration, restore identity from the state file with optional reseed. Use when ADF cannot fill identity columns correctly."
        )
        tk.Label(help_frame, text=help_text, font=("Arial", 9), fg="gray", wraplength=800, justify=tk.LEFT).pack(anchor=tk.W)

        # Connection
        conn_frame = ttk.LabelFrame(content, text="1. Connection (Azure SQL)", padding=10)
        conn_frame.pack(fill=tk.X, padx=20, pady=5)

        self.server_var = tk.StringVar()
        self.db_var = tk.StringVar()
        self.auth_var = tk.StringVar(value="entra_mfa")
        self.user_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.db_type_var = tk.StringVar(value="sqlserver")
        self.port_var = tk.StringVar(value="")
        self.schema_var = tk.StringVar(value="")

        self.connection_widget = ConnectionWidget(
            parent=conn_frame,
            server_var=self.server_var,
            db_var=self.db_var,
            auth_var=self.auth_var,
            user_var=self.user_var,
            password_var=self.password_var,
            db_type_var=self.db_type_var,
            port_var=self.port_var,
            schema_var=self.schema_var,
            label_text="",
            row_start=0,
        )

        btn_conn = ttk.Button(conn_frame, text="Connect", command=self._connect)
        btn_conn.grid(row=8, column=1, pady=5, sticky=tk.W)
        self.conn_status = tk.Label(conn_frame, text="Not connected", fg="gray")
        self.conn_status.grid(row=9, column=1, sticky=tk.W)

        # Fetch
        fetch_frame = ttk.LabelFrame(content, text="2. Fetch identity columns", padding=10)
        fetch_frame.pack(fill=tk.X, padx=20, pady=5)
        ttk.Button(fetch_frame, text="Fetch all identity columns", command=self._fetch).pack(side=tk.LEFT, padx=(0, 10))
        self.fetch_count_var = tk.StringVar(value="")
        tk.Label(fetch_frame, textvariable=self.fetch_count_var, fg="gray").pack(side=tk.LEFT)

        list_frame = ttk.Frame(content)
        list_frame.pack(fill=tk.X, padx=20, pady=5)
        tk.Label(list_frame, text="Identity columns (Schema.Table.Column — Type, Seed, Increment, Last value):").pack(anchor=tk.W)
        self.listbox = tk.Listbox(list_frame, height=6, selectmode=tk.EXTENDED, font=("Consolas", 9))
        scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.listbox.yview)
        self.listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.listbox.config(yscrollcommand=scroll.set)

        # Options
        opt_frame = ttk.LabelFrame(content, text="3. Options", padding=10)
        opt_frame.pack(fill=tk.X, padx=20, pady=5)
        self.continue_on_error_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(opt_frame, text="Continue on error (skip failed columns, log and continue)", variable=self.continue_on_error_var).pack(anchor=tk.W)
        self.reseed_from_max_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(opt_frame, text="Restore: Reseed identity from MAX(column) after restore (recommended after ADF load)", variable=self.reseed_from_max_var).pack(anchor=tk.W)

        # Disable
        disable_frame = ttk.LabelFrame(content, text="4. Disable identity (save to run folder)", padding=10)
        disable_frame.pack(fill=tk.X, padx=20, pady=5)
        row_d = ttk.Frame(disable_frame)
        row_d.pack(fill=tk.X)
        tk.Label(row_d, text="Run folder:").pack(side=tk.LEFT, padx=(0, 5))
        self.run_folder_disable_var = tk.StringVar()
        ttk.Entry(row_d, textvariable=self.run_folder_disable_var, width=50).pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        ttk.Button(row_d, text="Browse...", command=lambda: self._browse_folder(self.run_folder_disable_var)).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(row_d, text="Use default", command=self._set_default_run_folder).pack(side=tk.LEFT)
        btn_row_d = ttk.Frame(disable_frame)
        btn_row_d.pack(anchor=tk.W, pady=5)
        ttk.Button(btn_row_d, text="Save & run now", command=lambda: self._run_disable(execute=True)).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(btn_row_d, text="Save script only", command=lambda: self._run_disable(execute=False)).pack(side=tk.LEFT)
        tk.Label(disable_frame, text="Saves identity_state.json and disable_identity.sql to the run folder. Use this folder later for Restore.", fg="gray").pack(anchor=tk.W)

        # Restore
        restore_frame = ttk.LabelFrame(content, text="5. Restore identity (from run folder)", padding=10)
        restore_frame.pack(fill=tk.X, padx=20, pady=5)
        row_r = ttk.Frame(restore_frame)
        row_r.pack(fill=tk.X)
        tk.Label(row_r, text="Run folder:").pack(side=tk.LEFT, padx=(0, 5))
        self.restore_run_folder_var = tk.StringVar()
        ttk.Entry(row_r, textvariable=self.restore_run_folder_var, width=50).pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        ttk.Button(row_r, text="Use last run", command=self._use_last_run_folder).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(row_r, text="Browse...", command=lambda: self._browse_folder(self.restore_run_folder_var)).pack(side=tk.LEFT)
        btn_row_r = ttk.Frame(restore_frame)
        btn_row_r.pack(anchor=tk.W, pady=5)
        ttk.Button(btn_row_r, text="Restore now", command=lambda: self._run_restore(execute=True)).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(btn_row_r, text="Generate script only", command=lambda: self._run_restore(execute=False)).pack(side=tk.LEFT)
        run_script_row = ttk.Frame(restore_frame)
        run_script_row.pack(anchor=tk.W, pady=3)
        ttk.Button(run_script_row, text="Run disable script from folder", command=lambda: self._run_script_from_folder(DISABLE_SCRIPT_NAME)).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(run_script_row, text="Run restore script from folder", command=lambda: self._run_script_from_folder(RESTORE_SCRIPT_NAME)).pack(side=tk.LEFT)
        tk.Label(restore_frame, text="Folder must contain identity_state.json. Restore re-adds identity; optionally reseeds from MAX(column).", fg="gray").pack(anchor=tk.W)

        # Save script / Execute script file
        script_btn_frame = ttk.Frame(content)
        script_btn_frame.pack(fill=tk.X, padx=20, pady=2)
        self.save_script_btn = ttk.Button(script_btn_frame, text="Save last script to file...", command=self._save_last_script, state=tk.DISABLED)
        self.save_script_btn.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(script_btn_frame, text="Execute script file...", command=self._execute_script_file).pack(side=tk.LEFT)
        tk.Label(script_btn_frame, text="(Pick a .sql file to run against current connection)", fg="gray").pack(side=tk.LEFT, padx=5)

        # Log
        log_frame = ttk.LabelFrame(content, text="Log", padding=10)
        log_frame.pack(fill=tk.X, padx=20, pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=8, wrap=tk.WORD, font=("Consolas", 9))
        self.log_text.pack(fill=tk.X)

        # Ensure scroll region is updated after layout (fixes hidden content)
        content.after(100, _update_scroll_region)

    def _browse_folder(self, var: tk.StringVar):
        parent = self.frame.winfo_toplevel()
        path = filedialog.askdirectory(parent=parent, title="Select run folder")
        if path:
            var.set(path)

    def _set_default_run_folder(self):
        self.run_folder_disable_var.set(self._get_default_run_folder())
        self._log("Run folder set to default (use this folder for Restore later).")

    def _use_last_run_folder(self):
        if self._last_run_folder:
            self.restore_run_folder_var.set(self._last_run_folder)
            self._log(f"Using last run folder: {self._last_run_folder}")
        else:
            messagebox.showinfo("Info", "No last run folder. Run Disable first or browse for a folder.")

    def _run_script_from_folder(self, script_name: str):
        """Execute disable_identity.sql or restore_identity.sql from the run folder."""
        folder = (self.restore_run_folder_var.get() or "").strip()
        if not folder:
            messagebox.showwarning("Warning", "Enter or select a run folder first.")
            return
        path = Path(folder) / script_name
        if not path.exists():
            messagebox.showerror("Error", f"Script not found: {path}")
            return
        if not self._conn:
            messagebox.showwarning("Warning", "Connect first.")
            return

        def do_run():
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                self.frame.after(0, lambda: messagebox.showerror("Error", str(e)))
                return
            batches = _split_sql_on_go(text)
            batches = [b.strip() for b in batches if b.strip()]
            if not batches:
                self.frame.after(0, lambda: self._log(f"No batches in {script_name}"))
                return
            self.frame.after(0, lambda: self._log(f"Executing {len(batches)} batch(es) from {path}..."))
            errors = []
            executed = 0
            for i, batch in enumerate(batches):
                if not batch.strip():
                    continue
                try:
                    cur = self._conn.cursor()
                    cur.execute(batch)
                    self._conn.commit()
                    executed += 1
                    self.frame.after(0, lambda n=i + 1, t=len(batches): self._log(f"  Batch {n}/{t} OK"))
                except Exception as e:
                    errors.append(str(e))
                    self.frame.after(0, lambda msg=str(e): self._log(f"  Error: {msg}"))
            def done():
                self._log(f"Done. Executed {executed} batch(es). Errors: {len(errors)}")
                if errors:
                    messagebox.showwarning("Completed with errors", f"Executed {executed} batch(es).\n{len(errors)} error(s). See log.")
                else:
                    messagebox.showinfo("Done", f"Script executed successfully.\n{executed} batch(es).")
            self.frame.after(0, done)

        threading.Thread(target=do_run, daemon=True).start()

    def _connect(self):
        def do_connect():
            try:
                from gui.utils.database_utils import connect_to_any_database
            except ImportError:
                from azure_migration_tool.gui.utils.database_utils import connect_to_any_database
            server = self.server_var.get()
            database = self.db_var.get()
            if not server or not database:
                self.frame.after(0, lambda: messagebox.showerror("Error", "Server and Database are required."))
                return
            auth = self.auth_var.get()
            user = self.user_var.get()
            password = self.password_var.get() or None
            db_type = self.db_type_var.get()
            if db_type != "sqlserver":
                self.frame.after(0, lambda: messagebox.showwarning("Warning", "This tab supports Azure SQL / SQL Server only. Use SQL Server connection."))
            try:
                conn = connect_to_any_database(
                    server=server,
                    database=database,
                    auth=auth,
                    user=user,
                    password=password,
                    db_type="sqlserver",
                    port=int(self.port_var.get() or 0) or None,
                    timeout=30,
                )
                self._conn = conn
                self.frame.after(0, lambda: self.conn_status.config(text="Connected", fg="green"))
                self.frame.after(0, lambda: self._log("Connected to " + server + " / " + database))
            except Exception as e:
                self._conn = None
                self.frame.after(0, lambda: self.conn_status.config(text="Failed", fg="red"))
                self.frame.after(0, lambda: messagebox.showerror("Connection Error", str(e)))

        threading.Thread(target=do_connect, daemon=True).start()

    def _fetch(self):
        if not self._conn:
            messagebox.showwarning("Warning", "Please connect first.")
            return

        def do_fetch():
            try:
                cur = self._conn.cursor()
                cols = fetch_identity_columns(cur)
                self._identity_columns = cols
                lines = []
                for c in cols:
                    last = c.get("last_value")
                    last_str = str(last) if last is not None else "N/A"
                    line = f"{c['schema_name']}.{c['table_name']}.{c['column_name']} — {c['type_name']}, seed={c.get('seed_value',1)}, inc={c.get('increment_value',1)}, last={last_str}"
                    lines.append(line)
                def update():
                    self.listbox.delete(0, tk.END)
                    for line in lines:
                        self.listbox.insert(tk.END, line)
                    self.fetch_count_var.set(f"Found {len(cols)} identity column(s)")
                    self._log(f"Fetched {len(cols)} identity column(s).")
                self.frame.after(0, update)
            except Exception as e:
                self.frame.after(0, lambda: messagebox.showerror("Fetch Error", str(e)))
                self.frame.after(0, lambda: self._log("Fetch failed: " + str(e)))

        threading.Thread(target=do_fetch, daemon=True).start()

    def _run_disable(self, execute: bool = True):
        if not self._conn:
            messagebox.showwarning("Warning", "Please connect first.")
            return
        run_folder = self.run_folder_disable_var.get().strip()
        if not run_folder:
            run_folder = self._get_default_run_folder()
            self.run_folder_disable_var.set(run_folder)
        cols = self._identity_columns
        if not cols:
            messagebox.showwarning("Warning", "Fetch identity columns first.")
            return

        continue_on_error = self.continue_on_error_var.get()
        server = self.server_var.get()
        database = self.db_var.get()
        state_path = str(Path(run_folder) / STATE_FILENAME)
        script_path = Path(run_folder) / DISABLE_SCRIPT_NAME

        def do_disable():
            try:
                Path(run_folder).mkdir(parents=True, exist_ok=True)
                cur = self._conn.cursor()
                summary = run_disable(
                    cur,
                    cols,
                    state_path,
                    server=server,
                    database=database,
                    execute=execute,
                    continue_on_error=continue_on_error,
                )
                script = summary.get("script") or ""
                if script:
                    script_path.write_text(script, encoding="utf-8")

                def update():
                    self._save_last_run_folder(run_folder)
                    self.restore_run_folder_var.set(run_folder)
                    self._log(f"Run folder: {run_folder}")
                    self._log(f"State: {state_path}")
                    self._log(f"Script: {script_path}")
                    if execute:
                        self._log(f"Status: {summary['status']}")
                        for err in summary.get("errors", []):
                            self._log("  Error: " + err)
                        if summary["status"] == "success":
                            messagebox.showinfo("Done", f"Identity disabled. Run folder:\n{run_folder}")
                        elif summary["status"] == "completed_with_errors":
                            messagebox.showwarning("Done with errors", "Some columns failed. Check log.")
                        else:
                            messagebox.showerror("Failed", summary.get("errors", ["Unknown error"])[0] if summary.get("errors") else "Failed")
                    else:
                        self._last_script = script
                        self.save_script_btn.config(state=tk.NORMAL)
                        self._log("Script-only: SQL saved to run folder, not executed.")
                        messagebox.showinfo("Done", f"State and script saved to run folder.\n{run_folder}")
                self.frame.after(0, update)
            except Exception as e:
                self.frame.after(0, lambda: messagebox.showerror("Error", str(e)))
                self.frame.after(0, lambda: self._log("Disable failed: " + str(e)))

        self._log("Disabling identity..." + (" (script only)" if not execute else " (executing)"))
        threading.Thread(target=do_disable, daemon=True).start()

    def _run_restore(self, execute: bool = True):
        if not self._conn:
            messagebox.showwarning("Warning", "Please connect first.")
            return
        run_folder = self.restore_run_folder_var.get().strip()
        if not run_folder:
            messagebox.showwarning("Warning", "Enter run folder or click 'Use last run' / Browse.")
            return
        state_path = str(Path(run_folder) / STATE_FILENAME)
        if not Path(state_path).exists():
            messagebox.showerror("Error", f"State file not found: {state_path}\nRun folder must contain {STATE_FILENAME}")
            return

        continue_on_error = self.continue_on_error_var.get()
        reseed_from_max = self.reseed_from_max_var.get()
        script_path = Path(run_folder) / RESTORE_SCRIPT_NAME

        def do_restore():
            try:
                cur = self._conn.cursor()
                summary = run_restore(
                    cur,
                    state_path,
                    reseed_from_max=reseed_from_max,
                    execute=execute,
                    continue_on_error=continue_on_error,
                )
                script = summary.get("script") or ""
                if script:
                    script_path.write_text(script, encoding="utf-8")

                def update():
                    if summary["status"] == "failed" and "No columns" in str(summary.get("errors", [])):
                        messagebox.showerror("Error", "No columns in state file.")
                        return
                    self._log(f"Restore status: {summary['status']}")
                    for err in summary.get("errors", []):
                        self._log("  Error: " + err)
                    if execute:
                        if summary["status"] == "success":
                            messagebox.showinfo("Done", "Identity restored successfully.")
                        elif summary["status"] == "completed_with_errors":
                            messagebox.showwarning("Done with errors", "Some columns failed. Check log.")
                        else:
                            messagebox.showerror("Failed", summary.get("errors", ["Unknown error"])[0] if summary.get("errors") else "Failed")
                    else:
                        self._last_script = script
                        self.save_script_btn.config(state=tk.NORMAL)
                        self._log(f"Script-only: restore script saved to {script_path}")
                        messagebox.showinfo("Done", f"Restore script saved to run folder.\n{script_path}")
                self.frame.after(0, update)
            except Exception as e:
                self.frame.after(0, lambda: messagebox.showerror("Error", str(e)))
                self.frame.after(0, lambda: self._log("Restore failed: " + str(e)))

        self._log("Restoring identity..." + (" (script only)" if not execute else " (executing)"))
        threading.Thread(target=do_restore, daemon=True).start()

    def _save_last_script(self):
        if not getattr(self, "_last_script", None):
            messagebox.showinfo("Info", "No script to save. Run Disable or Restore in script-only mode first.")
            return
        parent = self.frame.winfo_toplevel()
        path = filedialog.asksaveasfilename(
            parent=parent,
            title="Save script",
            defaultextension=".sql",
            filetypes=[("SQL", "*.sql"), ("All", "*.*")],
        )
        if path:
            try:
                Path(path).write_text(self._last_script, encoding="utf-8")
                messagebox.showinfo("Saved", f"Script saved to:\n{path}")
                self._log("Script saved to " + path)
            except Exception as e:
                messagebox.showerror("Error", str(e))

    def _execute_script_file(self):
        """Browse for a .sql file and execute it against the current connection (batches split on GO)."""
        if not self._conn:
            messagebox.showwarning("Warning", "Please connect first.")
            return
        parent = self.frame.winfo_toplevel()
        path = filedialog.askopenfilename(
            parent=parent,
            title="Select SQL script to execute",
            filetypes=[("SQL", "*.sql"), ("All", "*.*")],
        )
        if not path:
            return
        path = Path(path)
        if not path.exists():
            messagebox.showerror("Error", f"File not found: {path}")
            return

        def do_execute():
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                self.frame.after(0, lambda: messagebox.showerror("Error", f"Cannot read file: {e}"))
                return
            batches = _split_sql_on_go(text)
            batches = [b.strip() for b in batches if b.strip()]
            if not batches:
                self.frame.after(0, lambda: self._log("No executable batches found in script."))
                return
            self.frame.after(0, lambda: self._log(f"Executing {len(batches)} batch(es) from {path.name}..."))
            errors = []
            executed = 0
            for i, batch in enumerate(batches):
                if not batch.strip():
                    continue
                try:
                    cur = self._conn.cursor()
                    cur.execute(batch)
                    self._conn.commit()
                    executed += 1
                    self.frame.after(0, lambda n=i+1, t=len(batches): self._log(f"  Batch {n}/{t} OK"))
                except Exception as e:
                    err_msg = str(e)
                    errors.append(err_msg)
                    self.frame.after(0, lambda msg=err_msg: self._log(f"  Error: {msg}"))
            def done():
                self._log(f"Done. Executed {executed} batch(es). Errors: {len(errors)}")
                if errors:
                    messagebox.showwarning("Completed with errors", f"Executed {executed} batch(es).\n{len(errors)} error(s). See log.")
                else:
                    messagebox.showinfo("Done", f"Script executed successfully.\n{executed} batch(es).")
            self.frame.after(0, done)
        threading.Thread(target=do_execute, daemon=True).start()
