# Author: Satish Chauhan

"""
ADF Migration Tab — three-phase workflow:
  Phase 1  Database Prep: backup source schema, restore table skeletons to target
  Phase 2  Metadata Setup: populate ADF_Lookup in the control DB
  Phase 3  Trigger & Finalize: call ADF pipeline, wait, then restore indexes/constraints/programmables
"""

import json
import logging
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from pathlib import Path
import threading
import time
import sys

parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll

_sql_logger = logging.getLogger(__name__)

# Lazy imports for backend modules
_schema_backup = None
_schema_restore = None


def _ensure_backend():
    global _schema_backup, _schema_restore
    if _schema_backup is None:
        try:
            from src.backup.schema_backup import run_backup
            _schema_backup = run_backup
        except ImportError:
            from azure_migration_tool.src.backup.schema_backup import run_backup
            _schema_backup = run_backup
    if _schema_restore is None:
        try:
            from src.restore.schema_restore import run_restore
            _schema_restore = run_restore
        except ImportError:
            from azure_migration_tool.src.restore.schema_restore import run_restore
            _schema_restore = run_restore


# Default ADF constants
DEFAULT_SUBSCRIPTION_ID = "0b7f6570-6f27-48da-8ad4-7ae96052f5d2"
DEFAULT_RESOURCE_GROUP = "pgsurveys-core-sbx-eus2-rg"
DEFAULT_FACTORY_NAME = "pgs-mi-to-sql-sbx-eus2-adf"
DEFAULT_PIPELINE_NAME = "MasterPipelineTableLoad_LACERA"
DEFAULT_CONTROL_DB = "SQLMigrationGenericDB"

# MasterPipelineTableLoad_LACERA declares these names; Lookup passes them to the SP as TargetDBName / Environment.
ADF_PIPELINE_PARAM_TARGET_DB = "SourceDatabaseName"
ADF_PIPELINE_PARAM_ENVIRONMENT = "EnvironmentName"


class ADFTriggerTab:
    """Three-phase ADF migration tab."""

    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None
        self._cancel_requested = False
        self._running = False
        self._create_widgets()

    def set_project_path(self, project_path):
        self.project_path = project_path

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _create_widgets(self):
        canvas = tk.Canvas(self.frame)
        scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=canvas.yview)
        scrollable = ttk.Frame(canvas)
        scrollable.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        cw = canvas.create_window((0, 0), window=scrollable, anchor="nw")
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(cw, width=e.width))
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        bind_canvas_vertical_scroll(canvas, scrollable)

        # Title
        tk.Label(scrollable, text="ADF Migration", font=("Arial", 16, "bold")).pack(pady=10)
        tk.Label(
            scrollable,
            text=(
                "Phase 1: Backup source schema & create table skeletons on target  |  "
                "Phase 2: Populate ADF_Lookup in control DB  |  "
                "Phase 3: Trigger ADF pipeline & restore indexes/constraints"
            ),
            font=("Arial", 9), fg="gray", wraplength=900, justify=tk.LEFT,
        ).pack(pady=(0, 5))

        # ---------- Key Vault ----------
        self._build_keyvault_section(scrollable)

        # ---------- connections (side-by-side) ----------
        conn_frame = ttk.Frame(scrollable)
        conn_frame.pack(fill=tk.X, padx=10, pady=5)

        self._build_source_connection(conn_frame)
        self._build_target_connection(conn_frame)

        tk.Label(
            scrollable,
            text="Tip: After a short pause typing Source user, empty Target & Control fill once automatically.",
            fg="gray",
            font=("Arial", 8),
        ).pack(anchor=tk.W, padx=14, pady=(0, 4))

        # ---------- control DB ----------
        self._build_control_db_section(scrollable)

        self._adf_username_bootstrapped = False
        self._adf_bootstrap_after_id = None
        self.main_window.shared_src_user.trace_add(
            "write", self._adf_schedule_username_bootstrap
        )
        # Pre-filled Source (e.g. from another tab) — copy immediately, no debounce
        self._adf_apply_username_bootstrap_now()

        # ---------- ADF config ----------
        self._build_adf_config_section(scrollable)

        # ---------- phase options ----------
        self._build_phase_options(scrollable)

        # ---------- buttons ----------
        btn_frame = ttk.Frame(scrollable)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        self.run_btn = ttk.Button(btn_frame, text="Run ADF Migration", command=self._on_run)
        self.run_btn.pack(side=tk.LEFT, padx=5)

        self.cancel_btn = ttk.Button(btn_frame, text="Cancel", state=tk.DISABLED, command=self._on_cancel)
        self.cancel_btn.pack(side=tk.LEFT, padx=5)

        self.progress_var = tk.StringVar(value="Idle")
        tk.Label(btn_frame, textvariable=self.progress_var, fg="blue").pack(side=tk.LEFT, padx=15)

        self.progress_bar = ttk.Progressbar(btn_frame, mode="indeterminate", length=200)
        self.progress_bar.pack(side=tk.LEFT, padx=5)

        # ---------- log ----------
        log_frame = ttk.LabelFrame(scrollable, text="Output", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=18, font=("Consolas", 9), wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        self._log("Ready.  Configure connections above, then click 'Run ADF Migration'.")

    # ---- source ----
    def _build_source_connection(self, parent):
        lf = ttk.LabelFrame(parent, text="Source Database (MI)", padding=10)
        lf.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        from gui.widgets.connection_widget import ConnectionWidget

        self.src_server_var = self.main_window.shared_src_server
        self.src_db_var = self.main_window.shared_src_db
        self.src_auth_var = self.main_window.shared_src_auth
        self.src_user_var = self.main_window.shared_src_user
        self.src_password_var = self.main_window.shared_src_password
        ConnectionWidget(lf, self.src_server_var, self.src_db_var, self.src_auth_var,
                         self.src_user_var, self.src_password_var, row_start=0)

    # ---- target ----
    def _build_target_connection(self, parent):
        lf = ttk.LabelFrame(parent, text="Target Database (Azure SQL)", padding=10)
        lf.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))
        from gui.widgets.connection_widget import ConnectionWidget

        self.dest_server_var = self.main_window.shared_dest_server
        self.dest_db_var = self.main_window.shared_dest_db
        self.dest_auth_var = self.main_window.shared_dest_auth
        self.dest_user_var = self.main_window.shared_dest_user
        self.dest_password_var = self.main_window.shared_dest_password
        ConnectionWidget(lf, self.dest_server_var, self.dest_db_var, self.dest_auth_var,
                         self.dest_user_var, self.dest_password_var, row_start=0)

    # ---- control DB ----
    def _build_control_db_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Phase 2 — Control Database (ADF_Lookup)", padding=10)
        lf.pack(fill=tk.X, padx=10, pady=5)

        row = 0
        tk.Label(lf, text="Control Server:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.ctrl_server_var = tk.StringVar()
        self.ctrl_server_combo = ttk.Combobox(
            lf,
            textvariable=self.ctrl_server_var,
            width=52,
            state="normal",
            postcommand=self._adf_ctrl_server_history_postcommand,
        )
        self.ctrl_server_combo.grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)
        tk.Label(lf, text="(e.g. route66-sbx-eus2-mi.xxx.database.windows.net)", fg="gray", font=("Arial", 8)).grid(
            row=row, column=2, sticky=tk.W, padx=5)

        row += 1
        tk.Label(lf, text="Control Database:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.ctrl_db_var = tk.StringVar(value=DEFAULT_CONTROL_DB)
        ttk.Entry(lf, textvariable=self.ctrl_db_var, width=55).grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)

        row += 1
        tk.Label(lf, text="Auth:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.ctrl_auth_var = tk.StringVar(value="entra_mfa")
        ttk.Combobox(lf, textvariable=self.ctrl_auth_var, width=52, state="readonly",
                     values=["entra_mfa", "entra_password", "sql", "windows"]).grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)

        row += 1
        tk.Label(lf, text="User:").grid(row=row, column=0, sticky=tk.W, pady=3)
        u0 = (self.main_window.shared_src_user.get() or self.main_window.shared_dest_user.get() or "").strip()
        self.ctrl_user_var = tk.StringVar(value=u0)
        self.ctrl_user_combo = ttk.Combobox(
            lf,
            textvariable=self.ctrl_user_var,
            width=52,
            state="normal",
            postcommand=self._adf_ctrl_user_history_postcommand,
        )
        self.ctrl_user_combo.grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)

        row += 1
        self.ctrl_password_label = tk.Label(lf, text="Password:")
        self.ctrl_password_label.grid(row=row, column=0, sticky=tk.W, pady=3)
        self.ctrl_password_var = tk.StringVar()
        self.ctrl_password_entry = ttk.Entry(lf, textvariable=self.ctrl_password_var, width=55, show="*")
        self.ctrl_password_entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)
        self.ctrl_auth_var.trace_add("write", lambda *_: self._update_ctrl_password_visibility())
        self._update_ctrl_password_visibility()

        row += 1
        tk.Label(lf, text="Environment:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.environment_var = tk.StringVar(value="UAT")
        ttk.Combobox(lf, textvariable=self.environment_var, width=52,
                     values=["DEV", "SBX", "QA", "UAT", "PROD"]).grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)

        row += 1
        tk.Label(lf, text="Load Type:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.load_type_var = tk.StringVar(value="FullLoad")
        ttk.Combobox(lf, textvariable=self.load_type_var, width=52, state="readonly",
                     values=["FullLoad", "IncrementalLoad"]).grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)

        lf.columnconfigure(1, weight=1)

    def _adf_schedule_username_bootstrap(self, *_args):
        """Defer bootstrap until typing pauses so we copy the full username, not the first character."""
        if self._adf_username_bootstrapped:
            return
        if self._adf_bootstrap_after_id is not None:
            try:
                self.frame.after_cancel(self._adf_bootstrap_after_id)
            except tk.TclError:
                pass
            self._adf_bootstrap_after_id = None
        self._adf_bootstrap_after_id = self.frame.after(
            450, self._adf_apply_username_bootstrap_now
        )

    def _adf_apply_username_bootstrap_now(self):
        """First time Source user is non-empty, copy into Target/Control only where still empty."""
        self._adf_bootstrap_after_id = None
        if self._adf_username_bootstrapped:
            return
        u = (self.src_user_var.get() or "").strip()
        if not u:
            return
        self._adf_username_bootstrapped = True
        if not (self.dest_user_var.get() or "").strip():
            self.dest_user_var.set(u)
        if not (self.ctrl_user_var.get() or "").strip():
            self.ctrl_user_var.set(u)

    def _adf_ctrl_server_history_postcommand(self):
        try:
            from gui.utils import input_history as ih
        except ImportError:
            from azure_migration_tool.gui.utils import input_history as ih
        self.ctrl_server_combo["values"] = tuple(ih.get_servers())

    def _adf_ctrl_user_history_postcommand(self):
        try:
            from gui.utils import input_history as ih
        except ImportError:
            from azure_migration_tool.gui.utils import input_history as ih
        self.ctrl_user_combo["values"] = tuple(ih.get_usernames())

    def _record_adf_connection_history(self):
        """Remember servers/users after a successful full ADF migration run."""
        try:
            from gui.utils import input_history as ih
        except ImportError:
            from azure_migration_tool.gui.utils import input_history as ih
        for getter in (
            self.src_server_var.get,
            self.dest_server_var.get,
            self.ctrl_server_var.get,
        ):
            s = (getter() or "").strip()
            if s:
                ih.record_server(s)
        for getter in (
            self.src_user_var.get,
            self.dest_user_var.get,
            self.ctrl_user_var.get,
        ):
            u = (getter() or "").strip()
            if u:
                ih.record_username(u)

    def _update_ctrl_password_visibility(self):
        """Hide password when MFA or Windows auth (no SQL password)."""
        if not getattr(self, "ctrl_password_label", None):
            return
        auth = (self.ctrl_auth_var.get() or "").strip()
        if auth in ("entra_mfa", "windows"):
            self.ctrl_password_label.grid_remove()
            self.ctrl_password_entry.grid_remove()
        else:
            self.ctrl_password_label.grid()
            self.ctrl_password_entry.grid()

    # ---- ADF ----
    def _build_adf_config_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Phase 3 — Azure Data Factory", padding=10)
        lf.pack(fill=tk.X, padx=10, pady=5)

        fields = [
            ("Subscription ID:", "subscription_var", DEFAULT_SUBSCRIPTION_ID),
            ("Resource Group:", "resource_group_var", DEFAULT_RESOURCE_GROUP),
            ("Factory Name:", "factory_var", DEFAULT_FACTORY_NAME),
            ("Pipeline Name:", "pipeline_var", DEFAULT_PIPELINE_NAME),
        ]
        for row, (label, attr, default) in enumerate(fields):
            tk.Label(lf, text=label).grid(row=row, column=0, sticky=tk.W, pady=3)
            var = tk.StringVar(value=default)
            setattr(self, attr, var)
            ttk.Entry(lf, textvariable=var, width=55).grid(row=row, column=1, sticky=tk.W, padx=5, pady=3)

        row += 1
        tk.Label(lf, text="Poll interval (s):").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.poll_interval_var = tk.IntVar(value=15)
        ttk.Spinbox(lf, from_=5, to=120, textvariable=self.poll_interval_var, width=10).grid(
            row=row, column=1, sticky=tk.W, padx=5, pady=3)

        row += 1
        tk.Label(lf, text="Timeout (min, 0 = none):").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.timeout_var = tk.IntVar(value=0)
        ttk.Spinbox(lf, from_=0, to=1440, textvariable=self.timeout_var, width=10).grid(
            row=row, column=1, sticky=tk.W, padx=5, pady=3)

        row += 1
        tk.Label(lf, text="Extra pipeline params (JSON):").grid(row=row, column=0, sticky=tk.NW, pady=3)
        self.pipeline_extra_params_var = tk.StringVar()
        ttk.Entry(lf, textvariable=self.pipeline_extra_params_var, width=55).grid(
            row=row, column=1, sticky=tk.W, padx=5, pady=3)
        tk.Label(
            lf,
            text=(
                "Optional JSON merged on top of trigger params. "
                f"LACERA master pipeline expects @pipeline().parameters.{ADF_PIPELINE_PARAM_TARGET_DB} "
                f"and .{ADF_PIPELINE_PARAM_ENVIRONMENT} (mapped to the lookup SP’s TargetDBName / Environment)."
            ),
            fg="gray",
            font=("Arial", 8),
            wraplength=620,
            justify=tk.LEFT,
        ).grid(row=row + 1, column=0, columnspan=2, sticky=tk.W, padx=5, pady=(0, 6))

        lf.columnconfigure(1, weight=1)

    # ---- phase skip options ----
    def _build_phase_options(self, parent):
        lf = ttk.LabelFrame(parent, text="Phase Options", padding=10)
        lf.pack(fill=tk.X, padx=10, pady=5)

        self.skip_phase1_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(lf, text="Skip Phase 1 (tables already exist on target)",
                        variable=self.skip_phase1_var).pack(side=tk.LEFT, padx=10)

        self.skip_phase2_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(lf, text="Skip Phase 2 (ADF_Lookup already populated)",
                        variable=self.skip_phase2_var).pack(side=tk.LEFT, padx=10)

        self.skip_phase3_trigger_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(lf, text="Skip Phase 3 trigger (run post-data only)",
                        variable=self.skip_phase3_trigger_var).pack(side=tk.LEFT, padx=10)

        self.restore_programmables_var = tk.BooleanVar(value=True)
        self.restore_constraints_var = tk.BooleanVar(value=True)
        self.restore_indexes_var = tk.BooleanVar(value=True)

        lf2 = ttk.LabelFrame(parent, text="Post-Data Restore (Phase 3, after ADF completes)", padding=10)
        lf2.pack(fill=tk.X, padx=10, pady=5)
        ttk.Checkbutton(lf2, text="Restore Programmables (Views, Procs, Functions)",
                        variable=self.restore_programmables_var).pack(side=tk.LEFT, padx=10)
        ttk.Checkbutton(lf2, text="Restore Constraints (FKs, Check)",
                        variable=self.restore_constraints_var).pack(side=tk.LEFT, padx=10)
        ttk.Checkbutton(lf2, text="Restore Indexes",
                        variable=self.restore_indexes_var).pack(side=tk.LEFT, padx=10)

    # ------------------------------------------------------------------
    # Key Vault
    # ------------------------------------------------------------------
    def _build_keyvault_section(self, parent):
        lf = ttk.LabelFrame(parent, text="Key Vault (ADF + control DB)", padding=8)
        lf.pack(fill=tk.X, padx=10, pady=5)

        row = ttk.Frame(lf)
        row.pack(fill=tk.X)
        tk.Label(row, text="Vault URL:").pack(side=tk.LEFT)
        self.kv_url_var = tk.StringVar()
        ttk.Entry(row, textvariable=self.kv_url_var, width=52).pack(side=tk.LEFT, padx=6, fill=tk.X, expand=True)
        self.kv_fetch_btn = ttk.Button(row, text="Load secrets", command=self._on_fetch_keyvault)
        self.kv_fetch_btn.pack(side=tk.LEFT, padx=(4, 0))
        ttk.Button(row, text="Secret names…", width=12, command=self._show_keyvault_secret_help).pack(
            side=tk.LEFT, padx=(4, 0)
        )

    def _show_keyvault_secret_help(self):
        try:
            from utils.keyvault_client import DEFAULT_SECRET_MAP, SECRET_NAME_LABELS
        except ImportError:
            from azure_migration_tool.utils.keyvault_client import DEFAULT_SECRET_MAP, SECRET_NAME_LABELS
        lines = "\n".join(
            f"  • {name} — {SECRET_NAME_LABELS.get(name, '')}"
            for name in DEFAULT_SECRET_MAP
        )
        messagebox.showinfo(
            "Key Vault secret names",
            "Store these names in your vault. Missing secrets are skipped.\n\n" + lines,
        )

    def _on_fetch_keyvault(self):
        vault_url = (self.kv_url_var.get() or "").strip()
        if not vault_url:
            messagebox.showwarning("Key Vault", "Please enter the Key Vault URL.")
            return
        if not vault_url.startswith("https://"):
            vault_url = "https://" + vault_url

        self.kv_fetch_btn.config(state=tk.DISABLED)
        self._log("Fetching secrets from Key Vault...")
        threading.Thread(target=self._do_fetch_keyvault, args=(vault_url,), daemon=True).start()

    def _do_fetch_keyvault(self, vault_url: str):
        try:
            from utils.keyvault_client import fetch_secrets, DEFAULT_SECRET_MAP
        except ImportError:
            try:
                from azure_migration_tool.utils.keyvault_client import fetch_secrets, DEFAULT_SECRET_MAP
            except ImportError:
                self._log_safe("[FAIL] keyvault_client module not found.")
                self.frame.after(0, lambda: self.kv_fetch_btn.config(state=tk.NORMAL))
                return

        try:
            results = fetch_secrets(vault_url, secret_names=DEFAULT_SECRET_MAP, logger=self._log_safe)
        except Exception as exc:
            self._log_safe(f"[FAIL] Key Vault fetch failed: {exc}")
            self.frame.after(0, lambda: self.kv_fetch_btn.config(state=tk.NORMAL))
            return

        # Map results to UI fields
        field_var_map = {
            "adf_subscription_id": self.subscription_var,
            "adf_resource_group":  self.resource_group_var,
            "adf_factory_name":    self.factory_var,
            "adf_pipeline_name":   self.pipeline_var,
            "ctrl_server":         self.ctrl_server_var,
            "ctrl_db":             self.ctrl_db_var,
            "ctrl_user":           self.ctrl_user_var,
            "ctrl_password":       self.ctrl_password_var,
            "ctrl_auth":           self.ctrl_auth_var,
            "environment":         self.environment_var,
        }

        populated = 0
        for field_key, value in results.items():
            var = field_var_map.get(field_key)
            if var is not None and value:
                self.frame.after(0, lambda v=var, val=value: v.set(val))
                populated += 1

        self._log_safe(f"[OK] {populated} field(s) populated from Key Vault.")
        self.frame.after(0, lambda: self.kv_fetch_btn.config(state=tk.NORMAL))

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------
    def _log(self, msg: str):
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def _log_safe(self, msg: str):
        self.frame.after(0, lambda: self._log(msg))

    # ------------------------------------------------------------------
    # Run / Cancel
    # ------------------------------------------------------------------
    def _on_cancel(self):
        self._cancel_requested = True
        self._log_safe("Cancel requested — will stop after the current step.")

    def _on_run(self):
        # Basic validation
        src_server = (self.src_server_var.get() or "").strip()
        src_db = (self.src_db_var.get() or "").strip()
        dest_server = (self.dest_server_var.get() or "").strip()
        dest_db = (self.dest_db_var.get() or "").strip()

        if not src_server or not src_db:
            messagebox.showwarning("Missing", "Please fill Source server and database.")
            return
        if not dest_server or not dest_db:
            messagebox.showwarning("Missing", "Please fill Target server and database.")
            return

        if not self.skip_phase2_var.get():
            ctrl_server = (self.ctrl_server_var.get() or "").strip()
            ctrl_db = (self.ctrl_db_var.get() or "").strip()
            if not ctrl_server or not ctrl_db:
                messagebox.showwarning("Missing", "Please fill Control server and database for Phase 2.")
                return

        if not self.skip_phase3_trigger_var.get():
            for attr in ("subscription_var", "resource_group_var", "factory_var", "pipeline_var"):
                if not (getattr(self, attr).get() or "").strip():
                    messagebox.showwarning("Missing", f"ADF field '{attr.replace('_var','')}' is empty.")
                    return

        self._cancel_requested = False
        self._running = True
        self.run_btn.config(state=tk.DISABLED)
        self.cancel_btn.config(state=tk.NORMAL)
        self.progress_bar.start(12)
        self.log_text.delete("1.0", tk.END)

        threading.Thread(target=self._execute_all_phases, daemon=True).start()

    # ------------------------------------------------------------------
    # Phase orchestration
    # ------------------------------------------------------------------
    def _execute_all_phases(self):
        t0 = time.time()
        backup_path = None
        tables = []
        ok = True

        try:
            # Phase 1
            if not self.skip_phase1_var.get() and not self._cancel_requested:
                self._set_progress("Phase 1 — Database Prep")
                ok, backup_path, tables = self._phase1_database_prep()
                if not ok:
                    return

            if self._cancel_requested:
                self._log_safe("Cancelled.")
                return

            # If phase 1 was skipped but we need tables for phase 2, discover them
            if not tables and not self.skip_phase2_var.get():
                self._log_safe("Discovering tables from source...")
                tables = self._discover_tables_from_source()
                if not tables:
                    self._log_safe("[WARN] No tables found on source.")

            # Phase 2
            if not self.skip_phase2_var.get() and not self._cancel_requested:
                self._set_progress("Phase 2 — Metadata Setup")
                ok = self._phase2_metadata_setup(tables)
                if not ok:
                    return

            if self._cancel_requested:
                self._log_safe("Cancelled.")
                return

            # Phase 3
            if not self._cancel_requested:
                self._set_progress("Phase 3 — Trigger & Finalize")
                ok = self._phase3_trigger_and_finalize(backup_path)
                if not ok:
                    return

            self._record_adf_connection_history()
            elapsed = round(time.time() - t0, 1)
            self._log_safe(f"\nAll phases completed in {elapsed}s.")
            self._set_progress("Done")
            self.frame.after(0, lambda: messagebox.showinfo("ADF Migration", f"All phases completed in {elapsed}s."))

        except Exception as exc:
            import traceback
            self._log_safe(f"\n[FATAL] {exc}\n{traceback.format_exc()}")
            self._set_progress("Failed")
            self.frame.after(0, lambda: messagebox.showerror("ADF Migration", str(exc)))
        finally:
            self._running = False
            self.frame.after(0, lambda: self.run_btn.config(state=tk.NORMAL))
            self.frame.after(0, lambda: self.cancel_btn.config(state=tk.DISABLED))
            self.frame.after(0, lambda: self.progress_bar.stop())

    def _set_progress(self, msg: str):
        self.frame.after(0, lambda: self.progress_var.set(msg))
        self._log_safe(f"\n{'='*60}\n{msg}\n{'='*60}")

    # ------------------------------------------------------------------
    # Phase 1  —  Database Prep
    # ------------------------------------------------------------------
    def _phase1_database_prep(self):
        """Backup source schema, restore only table skeletons to target.

        Returns (success, backup_path, table_list).
        """
        _ensure_backend()
        src_server = self.src_server_var.get().strip()
        src_db = self.src_db_var.get().strip()
        src_auth = self.src_auth_var.get().strip()
        src_user = self.src_user_var.get().strip()
        src_password = self.src_password_var.get() or None
        dest_server = self.dest_server_var.get().strip()
        dest_db = self.dest_db_var.get().strip()
        dest_auth = self.dest_auth_var.get().strip()
        dest_user = self.dest_user_var.get().strip()
        dest_password = self.dest_password_var.get() or None
        project = str(self.project_path) if self.project_path else None

        # Resolve backup root
        if project:
            backup_root = str(Path(project) / "backups")
        else:
            try:
                from src.utils.paths import app_data_dir
            except ImportError:
                from azure_migration_tool.src.utils.paths import app_data_dir
            backup_root = str(app_data_dir() / "backups")

        # Step 1a — Backup source schema
        self._log_safe(f"Backing up schema from {src_server} / {src_db} ...")
        backup_cfg = {
            "server": src_server,
            "database": src_db,
            "auth": src_auth,
            "user": src_user,
            "password": src_password,
            "backup_root": backup_root,
            "log_table_sample": 20,
            "export_defaults_separately": True,
        }
        try:
            backup_summary = _schema_backup(backup_cfg)
        except Exception as exc:
            self._log_safe(f"[FAIL] Backup failed: {exc}")
            return False, None, []

        status = backup_summary.get("status", "unknown")
        if status != "success":
            self._log_safe(f"[FAIL] Backup status: {status}")
            errs = backup_summary.get("errors", [])
            for e in errs:
                self._log_safe(f"  - {e}")
            return False, None, []

        backup_path = (
            backup_summary.get("backup_path")
            or backup_summary.get("run_root")
            or backup_summary.get("run_dir")
        )
        if isinstance(backup_path, Path):
            backup_path = str(backup_path.resolve())
        elif backup_path:
            backup_path = str(backup_path).strip() or None

        rid = backup_summary.get("run_id")
        if rid:
            try:
                from src.utils.paths import short_slug
            except ImportError:
                from azure_migration_tool.src.utils.paths import short_slug
            reconstructed = str(
                Path(backup_root)
                / short_slug(src_server)
                / short_slug(src_db)
                / "runs"
                / rid
            )
            if not backup_path or not Path(backup_path).exists():
                if Path(reconstructed).exists():
                    backup_path = reconstructed
                elif not backup_path:
                    backup_path = reconstructed

        if not backup_path or not Path(backup_path).exists():
            self._log_safe(
                f"[FAIL] Backup run folder not found. Expected under: {backup_root} "
                f"(run_id={rid!r})."
            )
            return False, None, []

        self._log_safe(f"[OK] Backup succeeded: {backup_path}")

        # Collect table list from backup folder and/or run summary JSON
        tables = self._tables_from_backup(backup_path, backup_summary)
        self._log_safe(f"  Tables found: {len(tables)}")

        if self._cancel_requested:
            return False, backup_path, tables

        # Step 1b — Restore table skeletons only to target
        self._log_safe(f"\nRestoring table skeletons to {dest_server} / {dest_db} ...")
        restore_cfg = {
            "project_path": project,
            "backup_path": str(backup_path),
            "dest_server": dest_server,
            "dest_db": dest_db,
            "dest_auth": dest_auth,
            "dest_user": dest_user,
            "dest_password": dest_password,
            "restore_tables": True,
            "restore_programmables": False,
            "restore_constraints": False,
            "restore_indexes": False,
            "continue_on_error": True,
            "dry_run": False,
        }
        try:
            restore_summary = _schema_restore(restore_cfg)
        except Exception as exc:
            self._log_safe(f"[FAIL] Table restore failed: {exc}")
            return False, backup_path, tables

        r_status = restore_summary.get("status", "unknown")
        if r_status in ("success", "completed_with_errors"):
            stats = restore_summary.get("statistics", {})
            self._log_safe(f"[OK] Table restore: {r_status}  (batches={stats.get('batches_executed', '?')}, "
                           f"errors={stats.get('errors', '?')})")
        else:
            self._log_safe(f"[FAIL] Table restore status: {r_status}")
            return False, backup_path, tables

        return True, backup_path, tables

    def _phase2_isactive_value(self, cursor):
        """Match dbo.ADF_Lookup.isActive type (bit vs varchar/char)."""
        try:
            cursor.execute(
                """
                SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = N'dbo' AND TABLE_NAME = N'ADF_Lookup' AND COLUMN_NAME = N'isActive'
                """
            )
            row = cursor.fetchone()
            if row and row[0]:
                dt = (row[0] or "").lower()
                if dt in ("bit", "tinyint", "smallint", "int"):
                    return 1
        except Exception:
            pass
        return "Y"

    # ------------------------------------------------------------------
    # Phase 2  —  Metadata Setup (ADF_Lookup)
    # ------------------------------------------------------------------
    def _phase2_metadata_setup(self, tables):
        """Insert or merge rows into dbo.ADF_Lookup in the control database."""
        ctrl_server = self.ctrl_server_var.get().strip()
        ctrl_db = self.ctrl_db_var.get().strip()
        ctrl_auth = self.ctrl_auth_var.get().strip()
        ctrl_user = self.ctrl_user_var.get().strip()
        ctrl_password = self.ctrl_password_var.get() or None
        environment = self.environment_var.get().strip() or "UAT"
        load_type = self.load_type_var.get().strip() or "FullLoad"

        src_server = self.src_server_var.get().strip()
        src_db = self.src_db_var.get().strip()
        dest_server = self.dest_server_var.get().strip()
        dest_db = self.dest_db_var.get().strip()

        if not tables:
            self._log_safe("[WARN] No tables to insert into ADF_Lookup.")
            return True

        self._log_safe(f"Connecting to control DB: {ctrl_server} / {ctrl_db} ...")

        try:
            from src.utils.database import connect_to_database, pick_sql_driver
        except ImportError:
            from azure_migration_tool.src.utils.database import connect_to_database, pick_sql_driver

        try:
            driver = pick_sql_driver(_sql_logger)
            conn = connect_to_database(
                server=ctrl_server,
                db=ctrl_db,
                user=ctrl_user,
                driver=driver,
                auth=ctrl_auth,
                password=ctrl_password,
                timeout=30,
                logger=_sql_logger,
            )
        except Exception as exc:
            self._log_safe(f"[FAIL] Cannot connect to control DB: {exc}")
            return False

        conn.autocommit = False
        cursor = conn.cursor()
        is_active = self._phase2_isactive_value(cursor)

        # One MERGE per table: upsert on natural key (matches typical ADF_Lookup usage).
        merge_sql = """
MERGE [dbo].[ADF_Lookup] AS tgt
USING (
    SELECT
        ? AS [SrcSchemaName], ? AS [SrcTableName],
        ? AS [TargetSchemaName], ? AS [TargetTableName],
        ? AS [SQLQuerytoFetch], ? AS [LoadType], ? AS [isActive],
        ? AS [SourceDbServerName], ? AS [SourceDatabaseName],
        ? AS [TargetAzureSQLServer], ? AS [TargetAzureDatabaseName], ? AS [EnvironmentName]
) AS src
ON tgt.[SrcSchemaName] = src.[SrcSchemaName]
   AND tgt.[SrcTableName] = src.[SrcTableName]
   AND tgt.[SourceDatabaseName] = src.[SourceDatabaseName]
   AND tgt.[TargetAzureDatabaseName] = src.[TargetAzureDatabaseName]
   AND tgt.[EnvironmentName] = src.[EnvironmentName]
WHEN MATCHED THEN UPDATE SET
    [TargetSchemaName] = src.[TargetSchemaName],
    [TargetTableName] = src.[TargetTableName],
    [SQLQuerytoFetch] = src.[SQLQuerytoFetch],
    [LoadType] = src.[LoadType],
    [isActive] = src.[isActive],
    [SourceDbServerName] = src.[SourceDbServerName],
    [TargetAzureSQLServer] = src.[TargetAzureSQLServer]
WHEN NOT MATCHED THEN INSERT (
    [SrcSchemaName], [SrcTableName],
    [TargetSchemaName], [TargetTableName],
    [SQLQuerytoFetch], [LoadType], [isActive],
    [SourceDbServerName], [SourceDatabaseName],
    [TargetAzureSQLServer], [TargetAzureDatabaseName], [EnvironmentName],
    [AuditDateColumnName], [StartTimeStamp], [EndTimeStamp],
    [StartKey], [EndKey], [TargetSPName], [TargetSPParm]
)
VALUES (
    src.[SrcSchemaName], src.[SrcTableName],
    src.[TargetSchemaName], src.[TargetTableName],
    src.[SQLQuerytoFetch], src.[LoadType], src.[isActive],
    src.[SourceDbServerName], src.[SourceDatabaseName],
    src.[TargetAzureSQLServer], src.[TargetAzureDatabaseName], src.[EnvironmentName],
    NULL, NULL, NULL, NULL, NULL, '', ''
);
"""

        errors: list[str] = []

        for schema, table in tables:
            if self._cancel_requested:
                self._log_safe("Cancelled during Phase 2 inserts.")
                conn.rollback()
                conn.close()
                return False

            fqn = f"{schema}.{table}"
            query = f"SELECT * FROM [{schema}].[{table}]"
            params = (
                schema,
                table,
                schema,
                table,
                query,
                load_type,
                is_active,
                src_server,
                src_db,
                dest_server,
                dest_db,
                environment,
            )

            try:
                cursor.execute(merge_sql, params)
            except Exception as exc:
                errors.append(f"{fqn}: {exc}")

        if errors:
            conn.rollback()
            conn.close()
            self._log_safe(f"[FAIL] ADF_Lookup MERGE: {len(errors)} error(s); transaction rolled back.")
            for e in errors[:20]:
                self._log_safe(f"  - {e}")
            if len(errors) > 20:
                self._log_safe(f"  ... and {len(errors) - 20} more")
            return False

        conn.commit()
        self._log_safe(f"[OK] ADF_Lookup: MERGE completed for {len(tables)} table(s).")

        try:
            cursor.execute(
                """
                SELECT COUNT(*) FROM [dbo].[ADF_Lookup]
                WHERE [SourceDatabaseName] = ? AND [TargetAzureDatabaseName] = ? AND [EnvironmentName] = ?
                """,
                (src_db, dest_db, environment),
            )
            cnt = cursor.fetchone()[0]
            self._log_safe(
                f"  Verify: {cnt} row(s) in ADF_Lookup for source DB={src_db!r}, "
                f"target DB={dest_db!r}, env={environment!r}."
            )
            if cnt == 0 and len(tables) > 0:
                self._log_safe(
                    "[FAIL] Verify COUNT is 0 — filters may not match existing rows "
                    "(check SourceDatabaseName / TargetAzureDatabaseName / EnvironmentName spelling)."
                )
                conn.close()
                return False
        except Exception as ver_exc:
            self._log_safe(f"  (Could not run verify COUNT: {ver_exc})")

        conn.close()
        return True

    # ------------------------------------------------------------------
    # Phase 3  —  Trigger ADF & Finalize
    # ------------------------------------------------------------------
    def _phase3_trigger_and_finalize(self, backup_path):
        """Trigger ADF pipeline, wait for success, then restore remaining schema objects."""
        if not self.skip_phase3_trigger_var.get():
            ok = self._trigger_adf_and_wait()
            if not ok:
                return False
        else:
            self._log_safe("Skipping ADF trigger (user requested skip).")

        if self._cancel_requested:
            return False

        # Post-data: restore programmables / constraints / indexes
        do_programmables = self.restore_programmables_var.get()
        do_constraints = self.restore_constraints_var.get()
        do_indexes = self.restore_indexes_var.get()

        if not any([do_programmables, do_constraints, do_indexes]):
            self._log_safe("No post-data restore options selected. Skipping.")
            return True

        if not backup_path:
            self._log_safe("[WARN] No backup path available for post-data restore. Skipping.")
            return True

        _ensure_backend()
        dest_server = self.dest_server_var.get().strip()
        dest_db = self.dest_db_var.get().strip()
        dest_auth = self.dest_auth_var.get().strip()
        dest_user = self.dest_user_var.get().strip()
        dest_password = self.dest_password_var.get() or None
        project = str(self.project_path) if self.project_path else None

        self._log_safe(f"\nRestoring post-data objects to {dest_server} / {dest_db} ...")
        self._log_safe(
            f"  Programmables={do_programmables}  Constraints={do_constraints}  Indexes={do_indexes}  "
            f"(primary keys from backup run when Constraints or Indexes is enabled)"
        )

        restore_cfg = {
            "project_path": project,
            "backup_path": str(backup_path),
            "dest_server": dest_server,
            "dest_db": dest_db,
            "dest_auth": dest_auth,
            "dest_user": dest_user,
            "dest_password": dest_password,
            "restore_tables": False,
            "restore_programmables": do_programmables,
            "restore_constraints": do_constraints,
            "restore_indexes": do_indexes,
            "continue_on_error": True,
            "dry_run": False,
        }
        try:
            summary = _schema_restore(restore_cfg)
        except Exception as exc:
            self._log_safe(f"[FAIL] Post-data restore failed: {exc}")
            return False

        status = summary.get("status", "unknown")
        stats = summary.get("statistics", {})
        self._log_safe(f"[OK] Post-data restore: {status}  (batches={stats.get('batches_executed', '?')}, "
                       f"errors={stats.get('errors', '?')})")
        return True

    def _trigger_adf_and_wait(self):
        """Authenticate with ADF, trigger the pipeline, poll until done."""
        sub = self.subscription_var.get().strip()
        rg = self.resource_group_var.get().strip()
        factory = self.factory_var.get().strip()
        pipeline = self.pipeline_var.get().strip()
        poll = self.poll_interval_var.get()
        timeout_min = self.timeout_var.get()
        timeout_sec = timeout_min * 60 if timeout_min > 0 else None

        dest_db = self.dest_db_var.get().strip()
        environment = self.environment_var.get().strip() or "UAT"

        self._log_safe(f"Triggering pipeline: {pipeline}  (factory={factory})")

        try:
            from azure_migration_tool.utils.adf_client import ADFClient
        except ImportError:
            try:
                from utils.adf_client import ADFClient
            except ImportError:
                self._log_safe("[FAIL] ADFClient not available. Install: pip install azure-identity azure-mgmt-datafactory")
                return False

        try:
            client = ADFClient(
                factory_name=factory,
                resource_group=rg,
                subscription_id=sub,
                logger=self._log_safe,
            )
        except Exception as exc:
            self._log_safe(f"[FAIL] ADF authentication failed: {exc}")
            return False

        try:
            client.warn_if_execute_pipeline_does_not_wait_for_children(pipeline)
        except Exception as exc:
            self._log_safe(f"  (waitOnCompletion inspection skipped: {exc})")

        # Must match pipeline JSON "parameters" keys or Azure ignores them (defaults apply).
        parameters = {
            ADF_PIPELINE_PARAM_TARGET_DB: dest_db,
            ADF_PIPELINE_PARAM_ENVIRONMENT: environment,
        }
        raw_extra = (self.pipeline_extra_params_var.get() or "").strip()
        if raw_extra:
            try:
                extra = json.loads(raw_extra)
                if not isinstance(extra, dict):
                    self._log_safe(
                        "[FAIL] Extra pipeline params must be a JSON object, "
                        f'e.g. {{"{ADF_PIPELINE_PARAM_TARGET_DB}":"MyDb"}}'
                    )
                    return False
                parameters.update(extra)
            except json.JSONDecodeError as je:
                self._log_safe(f"[FAIL] Invalid JSON in extra pipeline params: {je}")
                return False

        try:
            declared = client.get_declared_pipeline_parameters(pipeline)
            if declared:
                self._log_safe(f"  ADF pipeline parameters (declared): {list(declared.keys())}")
                for key in parameters:
                    if key not in declared:
                        self._log_safe(
                            f"  [WARN] Trigger key {key!r} is not declared on the pipeline — Azure will ignore it."
                        )
            else:
                self._log_safe(
                    "  [WARN] Could not read declared pipeline parameters (empty). "
                    "Trigger values may still apply only where activities use @pipeline().parameters.* ."
                )
        except Exception as exc:
            self._log_safe(f"  (Could not inspect pipeline parameters: {exc})")

        self._log_safe(
            f"  Mapping: Target DB field → pipeline parameter {ADF_PIPELINE_PARAM_TARGET_DB!r} "
            f"(feeds SP arg TargetDBName); Environment → {ADF_PIPELINE_PARAM_ENVIRONMENT!r}."
        )
        try:
            self._log_safe("  Trigger payload:\n" + json.dumps(parameters, indent=2, default=str))
        except Exception:
            self._log_safe(f"  Trigger payload (repr): {parameters!r}")

        try:
            run_id = client.trigger_pipeline(pipeline, parameters=parameters)
        except Exception as exc:
            self._log_safe(f"[FAIL] Pipeline trigger failed: {exc}")
            return False

        self._log_safe(f"Pipeline triggered.  Run ID: {run_id}")
        self._log_safe(f"Polling every {poll}s (timeout={'none' if not timeout_sec else f'{timeout_min}min'}) ...")

        def _status_cb(info):
            s = info.get("status", "?")
            dur = info.get("duration_ms")
            dur_str = f"  duration={dur}ms" if dur else ""
            self._log_safe(f"  Pipeline status: {s}{dur_str}")

        try:
            final = client.wait_for_completion(
                run_id,
                timeout=timeout_sec,
                callback=_status_cb,
                poll_interval=poll,
            )
        except TimeoutError:
            self._log_safe("[FAIL] Pipeline timed out.")
            return False
        except Exception as exc:
            self._log_safe(f"[FAIL] Error polling pipeline: {exc}")
            return False

        final_status = final.get("status", "Unknown")
        if final_status != "Succeeded":
            msg = final.get("message", "")
            self._log_safe(f"[FAIL] Pipeline ended with status: {final_status}  {msg}")
            return False

        self._log_safe(f"[OK] Master pipeline run completed: {final_status}")

        child_result = client.wait_for_all_child_pipelines(
            master_run_id=run_id,
            master_run_start=final.get("run_start"),
            poll_interval=max(5, poll),
            timeout_seconds=timeout_sec,
            callback=self._log_safe,
        )
        if not child_result.get("ok", False):
            reason = child_result.get("reason") or "child pipeline wait failed"
            self._log_safe(f"[FAIL] {reason}")
            return False
        if child_result.get("child_run_ids"):
            self._log_safe("[OK] All discovered child pipeline runs finished successfully.")
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _tables_from_backup(self, backup_path, backup_summary=None):
        """Table list from 01_tables/*.sql filenames, or from run_summary tables array."""
        tables = []
        if backup_path:
            bp = Path(str(backup_path))
            tables_dir = bp / "schema" / "01_tables"
            if tables_dir.exists():
                for f in sorted(tables_dir.glob("*.sql")):
                    name = f.stem
                    if "." in name:
                        parts = name.split(".", 1)
                        tables.append((parts[0], parts[1]))
                    else:
                        tables.append(("dbo", name))
        if not tables and backup_summary:
            for row in backup_summary.get("tables") or []:
                if not isinstance(row, dict):
                    continue
                s, t = row.get("schema"), row.get("table")
                if s and t:
                    tables.append((str(s), str(t)))
        return tables

    def _discover_tables_from_source(self):
        """Query source database for user tables."""
        src_server = self.src_server_var.get().strip()
        src_db = self.src_db_var.get().strip()
        src_auth = self.src_auth_var.get().strip()
        src_user = self.src_user_var.get().strip()
        src_password = self.src_password_var.get() or None

        try:
            from src.utils.database import connect_to_database, pick_sql_driver
        except ImportError:
            from azure_migration_tool.src.utils.database import connect_to_database, pick_sql_driver

        try:
            driver = pick_sql_driver(_sql_logger)
            conn = connect_to_database(
                server=src_server, db=src_db, user=src_user,
                driver=driver, auth=src_auth, password=src_password, timeout=30,
                logger=_sql_logger,
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_SCHEMA, TABLE_NAME"
            )
            tables = [(row[0], row[1]) for row in cursor.fetchall()]
            conn.close()
            self._log_safe(f"  Discovered {len(tables)} tables from source.")
            return tables
        except Exception as exc:
            self._log_safe(f"  [WARN] Could not discover tables: {exc}")
            return []
