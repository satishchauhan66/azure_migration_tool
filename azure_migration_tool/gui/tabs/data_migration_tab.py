# Author: S@tish Chauhan

"""
Data Migration Tab
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from pathlib import Path
import threading
import time
import sys
import subprocess
import tempfile

try:
    from src.utils.subprocess_utils import run_silent as _run_silent
except ImportError:
    try:
        from azure_migration_tool.src.utils.subprocess_utils import run_silent as _run_silent
    except ImportError:
        _run_silent = subprocess.run
import os
import secrets
import string
import shutil
import urllib.request
import webbrowser

# Add parent directories to path
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.utils.excel_utils import read_excel_file, create_sample_excel
from gui.widgets.connection_widget import ConnectionWidget
from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll

try:
    import pyodbc
except ImportError:
    pyodbc = None

# Use real data migration module (no top-level migrate_data package)
migrate_data = None
try:
    from azure_migration_tool.src.migration import data_migration as migrate_data
except ImportError:
    try:
        from src.migration import data_migration as migrate_data
    except ImportError:
        try:
            import sys
            sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
            from src.migration import data_migration as migrate_data
        except ImportError:
            migrate_data = None


def _data_migration_module():
    """Return the loaded ``src.migration.data_migration`` module (the global ``migrate_data``)."""
    if migrate_data is None:
        raise RuntimeError("Data migration module not available.")
    return migrate_data


try:
    import msal
except ImportError:
    msal = None

try:
    # Try importing from utils (when running from azure_migration_tool directory)
    try:
        from utils.adf_client import ADFClient
        ADF_AVAILABLE = True
    except ImportError:
        # Try importing from azure_migration_tool.utils (when running from parent directory)
        from azure_migration_tool.utils.adf_client import ADFClient
        ADF_AVAILABLE = True
except ImportError:
    ADF_AVAILABLE = False
    ADFClient = None


class DataMigrationTab:
    """Data migration tab."""
    
    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None
        
        self._create_widgets()
        
    def set_project_path(self, project_path):
        """Set the current project path."""
        self.project_path = project_path
        
    def _create_widgets(self):
        """Create UI widgets."""
        # Create scrollable canvas
        canvas = tk.Canvas(self.frame)
        scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas_frame = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        
        def on_canvas_configure(event):
            canvas_width = event.width
            canvas.itemconfig(canvas_frame, width=canvas_width)
        
        canvas.bind('<Configure>', on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Title
        title_label = tk.Label(
            scrollable_frame,
            text="Data Migration",
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=10)
        
        # Create two-column layout
        main_frame = ttk.Frame(scrollable_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        bind_canvas_vertical_scroll(canvas, scrollable_frame)

        # Store reference
        self.scrollable_frame = scrollable_frame
        
        # Left column - Source
        left_frame = ttk.LabelFrame(main_frame, text="Source Database", padding=10)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        # Use shared variables from main window
        self.src_server_var = self.main_window.shared_src_server
        self.src_db_var = self.main_window.shared_src_db
        self.src_auth_var = self.main_window.shared_src_auth
        self.src_user_var = self.main_window.shared_src_user
        self.src_password_var = self.main_window.shared_src_password
        
        # Create connection widget for source
        self.src_connection_widget = ConnectionWidget(
            parent=left_frame,
            server_var=self.src_server_var,
            db_var=self.src_db_var,
            auth_var=self.src_auth_var,
            user_var=self.src_user_var,
            password_var=self.src_password_var,
            label_text="",  # Frame already has label
            row_start=0
        )
        self.src_connection_widget.frame.pack(fill=tk.BOTH, expand=True)
        
        # Right column - Destination
        right_frame = ttk.LabelFrame(main_frame, text="Destination Database", padding=10)
        right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        # Use shared variables from main window
        self.dest_server_var = self.main_window.shared_dest_server
        self.dest_db_var = self.main_window.shared_dest_db
        self.dest_auth_var = self.main_window.shared_dest_auth
        self.dest_user_var = self.main_window.shared_dest_user
        self.dest_password_var = self.main_window.shared_dest_password
        
        # Create connection widget for destination
        self.dest_connection_widget = ConnectionWidget(
            parent=right_frame,
            server_var=self.dest_server_var,
            db_var=self.dest_db_var,
            auth_var=self.dest_auth_var,
            user_var=self.dest_user_var,
            password_var=self.dest_password_var,
            label_text="",  # Frame already has label
            row_start=0
        )
        self.dest_connection_widget.frame.pack(fill=tk.BOTH, expand=True)
        
        # Options frame
        options_frame = ttk.LabelFrame(scrollable_frame, text="Migration Options", padding=10)
        options_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Migration method selection
        method_frame = ttk.LabelFrame(options_frame, text="Migration Method", padding=5)
        method_frame.grid(row=0, column=0, columnspan=2, sticky=tk.W+tk.E, pady=5)
        
        self.migration_method_var = tk.StringVar(value="standard")
        ttk.Radiobutton(method_frame, text="Standard Migration (pyodbc)", 
                       variable=self.migration_method_var, value="standard",
                       command=self._on_migration_method_change).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(method_frame, text="BCP (Bulk Copy Program)", 
                       variable=self.migration_method_var, value="bcp",
                       command=self._on_migration_method_change).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(method_frame, text="Azure Data Factory (ADF)", 
                       variable=self.migration_method_var, value="adf",
                       command=self._on_migration_method_change).pack(side=tk.LEFT, padx=10)
        
        # Batch size (auto-tuned, but can set initial) - only for standard
        self.batch_size_label = tk.Label(options_frame, text="Initial Batch Size (auto-tuned):")
        self.batch_size_label.grid(row=1, column=0, sticky=tk.W, pady=5)
        self.batch_size_var = tk.StringVar(value="20000")
        self.batch_size_entry = ttk.Entry(options_frame, textvariable=self.batch_size_var, width=20)
        self.batch_size_entry.grid(row=1, column=1, sticky=tk.W, pady=5, padx=5)
        
        # Truncate destination
        self.truncate_dest_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(options_frame, text="Truncate Destination Tables", 
                       variable=self.truncate_dest_var).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # Disable foreign keys during migration
        self.disable_fk_var = tk.BooleanVar(value=False)
        fk_check = ttk.Checkbutton(options_frame, text="Disable Foreign Keys (required for truncate with FK references)", 
                       variable=self.disable_fk_var)
        fk_check.grid(row=3, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # Disable indexes during migration (for performance)
        self.disable_indexes_var = tk.BooleanVar(value=False)
        idx_check = ttk.Checkbutton(options_frame, text="Disable Non-Clustered Indexes during migration (faster bulk insert)", 
                       variable=self.disable_indexes_var)
        idx_check.grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # Disable triggers during migration
        self.disable_triggers_var = tk.BooleanVar(value=False)
        trig_check = ttk.Checkbutton(options_frame, text="Disable Triggers during migration", 
                       variable=self.disable_triggers_var)
        trig_check.grid(row=5, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # Continue on error
        self.continue_on_error_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Continue on Error", 
                       variable=self.continue_on_error_var).grid(row=6, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # BCP-specific options (initially hidden)
        self.bcp_options_frame = ttk.LabelFrame(options_frame, text="BCP Options", padding=5)
        self.bcp_options_frame.grid(row=7, column=0, columnspan=2, sticky=tk.W+tk.E, pady=5)
        
        info_label = tk.Label(
            self.bcp_options_frame,
            text="[OK] MFA prompts once for pyodbc checks. On Windows, BCP import to Entra/Azure "
                 "uses a temporary SQL login on the destination (created automatically).\n"
                 "   Or enable 'Use SQL Login' to control the login name yourself.",
            font=("Arial", 9),
            fg="green",
            justify=tk.LEFT,
        )
        info_label.pack(anchor=tk.W, pady=2)
        
        # Single table option
        single_table_frame = ttk.Frame(self.bcp_options_frame)
        single_table_frame.pack(anchor=tk.W, fill=tk.X, pady=5)
        
        tk.Label(single_table_frame, text="Single Table (optional):").pack(side=tk.LEFT, padx=5)
        self.bcp_single_table_var = tk.StringVar(value="")
        single_table_entry = ttk.Entry(single_table_frame, textvariable=self.bcp_single_table_var, width=40)
        single_table_entry.pack(side=tk.LEFT, padx=5)
        tk.Label(single_table_frame, text="e.g. dbo.MyTable or schema.table", 
                font=("Arial", 8), fg="gray").pack(side=tk.LEFT, padx=5)
        
        # Clear button for single table
        ttk.Button(single_table_frame, text="Clear", width=6,
                  command=lambda: self.bcp_single_table_var.set("")).pack(side=tk.LEFT, padx=2)

        work_dir_frame = ttk.Frame(self.bcp_options_frame)
        work_dir_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(work_dir_frame, text="BCP work folder (optional):").pack(side=tk.LEFT, padx=5)
        self.bcp_work_dir_var = tk.StringVar(value="")
        ttk.Entry(work_dir_frame, textvariable=self.bcp_work_dir_var, width=48).pack(
            side=tk.LEFT, padx=5, fill=tk.X, expand=True
        )
        tk.Label(
            self.bcp_options_frame,
            text="Blank = auto. Use a drive with plenty of space (e.g. D:\\BCPWork) for large DBs.",
            fg="gray",
            font=("Arial", 8),
        ).pack(anchor=tk.W, padx=5)
        
        # SQL login creation to avoid MFA prompts (optional, since we have token caching)
        self.bcp_use_sql_login_var = tk.BooleanVar(value=False)  # Default OFF - token caching handles MFA
        ttk.Checkbutton(self.bcp_options_frame, 
                       text="Use SQL Login instead of Entra (optional - for service accounts)", 
                       variable=self.bcp_use_sql_login_var).pack(anchor=tk.W, pady=2)
        
        sql_login_frame = ttk.Frame(self.bcp_options_frame)
        sql_login_frame.pack(anchor=tk.W, pady=2, padx=20)
        
        tk.Label(sql_login_frame, text="SQL Login Name:").pack(side=tk.LEFT, padx=5)
        self.bcp_login_name_var = tk.StringVar(value="svc_azdm_bcp")
        ttk.Entry(sql_login_frame, textvariable=self.bcp_login_name_var, width=20).pack(side=tk.LEFT, padx=5)
        
        self.bcp_cleanup_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(self.bcp_options_frame, text="Clean up SQL login after migration (if created)", 
                       variable=self.bcp_cleanup_var).pack(anchor=tk.W, pady=2)
        
        # Exclude backup tables option
        self.bcp_exclude_backup_var = tk.BooleanVar(value=True)  # Default ON
        ttk.Checkbutton(self.bcp_options_frame, 
                       text="Exclude backup tables (Bak_*, _backup, _bak, _old)", 
                       variable=self.bcp_exclude_backup_var).pack(anchor=tk.W, pady=2)
        
        # Check table exists in destination before importing
        self.bcp_check_table_exists_var = tk.BooleanVar(value=True)  # Default ON
        ttk.Checkbutton(self.bcp_options_frame, 
                       text="Skip tables that don't exist in destination", 
                       variable=self.bcp_check_table_exists_var).pack(anchor=tk.W, pady=2)

        # Pre-flight checklist (BCP)
        pf_frame = ttk.LabelFrame(self.bcp_options_frame, text="Pre-flight checklist", padding=5)
        pf_frame.pack(fill=tk.BOTH, expand=True, pady=8)

        pf_hint = tk.Label(
            pf_frame,
            text="Validates ODBC, BCP, connections, schema, and estimates disk space "
                 "(largest table + recommended free GB) before migration.",
            font=("Arial", 8),
            fg="gray",
            justify=tk.LEFT,
            wraplength=560,
        )
        pf_hint.pack(anchor=tk.W, pady=(0, 4))

        pf_table_frame = ttk.Frame(pf_frame)
        pf_table_frame.pack(fill=tk.BOTH, expand=True)

        pf_cols = ("status", "category", "check", "detail")
        self.bcp_preflight_tree = ttk.Treeview(
            pf_table_frame, columns=pf_cols, show="headings", height=7
        )
        self.bcp_preflight_tree.heading("status", text="")
        self.bcp_preflight_tree.heading("category", text="Area")
        self.bcp_preflight_tree.heading("check", text="Check")
        self.bcp_preflight_tree.heading("detail", text="Result")
        self.bcp_preflight_tree.column("status", width=28, anchor=tk.CENTER, stretch=False)
        self.bcp_preflight_tree.column("category", width=72, stretch=False)
        self.bcp_preflight_tree.column("check", width=130, stretch=False)
        self.bcp_preflight_tree.column("detail", width=720, minwidth=320, stretch=True)

        pf_v_scroll = ttk.Scrollbar(
            pf_table_frame, orient=tk.VERTICAL, command=self.bcp_preflight_tree.yview
        )
        pf_h_scroll = ttk.Scrollbar(
            pf_table_frame, orient=tk.HORIZONTAL, command=self.bcp_preflight_tree.xview
        )
        self.bcp_preflight_tree.configure(
            yscrollcommand=pf_v_scroll.set, xscrollcommand=pf_h_scroll.set
        )
        self.bcp_preflight_tree.grid(row=0, column=0, sticky="nsew")
        pf_v_scroll.grid(row=0, column=1, sticky="ns")
        pf_h_scroll.grid(row=1, column=0, sticky="ew")
        pf_table_frame.grid_rowconfigure(0, weight=1)
        pf_table_frame.grid_columnconfigure(0, weight=1)

        self._bcp_preflight_messages: dict[str, str] = {}
        self.bcp_preflight_tree.bind("<<TreeviewSelect>>", self._on_bcp_preflight_select)

        tk.Label(
            pf_frame,
            text="Full result (selected row) — scroll to read:",
            font=("Arial", 8),
            fg="gray",
        ).pack(anchor=tk.W, pady=(4, 0))
        self.bcp_preflight_detail = scrolledtext.ScrolledText(
            pf_frame, height=4, wrap=tk.WORD, font=("Consolas", 9), state=tk.DISABLED
        )
        self.bcp_preflight_detail.pack(fill=tk.BOTH, expand=False, pady=(2, 0))

        self._bcp_preflight_passed = False
        self._bcp_preflight_then_migrate = False
        pf_btn_row = ttk.Frame(pf_frame)
        pf_btn_row.pack(fill=tk.X, pady=(6, 0))
        self.bcp_preflight_btn = ttk.Button(
            pf_btn_row,
            text="Run pre-flight validation",
            command=self._run_bcp_preflight_validation,
            width=28,
        )
        self.bcp_preflight_btn.pack(side=tk.LEFT)
        self.bcp_preflight_status_var = tk.StringVar(value="Not validated yet")
        tk.Label(pf_btn_row, textvariable=self.bcp_preflight_status_var, fg="gray").pack(
            side=tk.LEFT, padx=10
        )
        
        # Hide BCP options initially
        self.bcp_options_frame.grid_remove()
        
        # ADF-specific options (initially hidden)
        self.adf_options_frame = ttk.LabelFrame(options_frame, text="ADF Options", padding=5)
        self.adf_options_frame.grid(row=8, column=0, columnspan=2, sticky=tk.W+tk.E, pady=5)
        
        info_label = tk.Label(self.adf_options_frame, 
                             text="Requires pre-configured ADF pipeline. Pipeline should accept parameters for source/dest and table list.",
                             font=("Arial", 9), fg="blue", justify=tk.LEFT, wraplength=600)
        info_label.pack(anchor=tk.W, pady=2)
        
        # ADF Factory Name
        factory_frame = ttk.Frame(self.adf_options_frame)
        factory_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(factory_frame, text="ADF Factory Name:").pack(side=tk.LEFT, padx=5)
        self.adf_factory_name_var = tk.StringVar(value="")
        ttk.Entry(factory_frame, textvariable=self.adf_factory_name_var, width=40).pack(side=tk.LEFT, padx=5)
        
        # ADF Pipeline Name
        pipeline_frame = ttk.Frame(self.adf_options_frame)
        pipeline_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(pipeline_frame, text="ADF Pipeline Name:").pack(side=tk.LEFT, padx=5)
        self.adf_pipeline_name_var = tk.StringVar(value="")
        ttk.Entry(pipeline_frame, textvariable=self.adf_pipeline_name_var, width=40).pack(side=tk.LEFT, padx=5)
        
        # Resource Group
        rg_frame = ttk.Frame(self.adf_options_frame)
        rg_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(rg_frame, text="Resource Group:").pack(side=tk.LEFT, padx=5)
        self.adf_resource_group_var = tk.StringVar(value="")
        ttk.Entry(rg_frame, textvariable=self.adf_resource_group_var, width=40).pack(side=tk.LEFT, padx=5)
        
        # Subscription ID (optional)
        sub_frame = ttk.Frame(self.adf_options_frame)
        sub_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(sub_frame, text="Subscription ID (optional):").pack(side=tk.LEFT, padx=5)
        self.adf_subscription_id_var = tk.StringVar(value="")
        ttk.Entry(sub_frame, textvariable=self.adf_subscription_id_var, width=40).pack(side=tk.LEFT, padx=5)
        tk.Label(sub_frame, text="(auto-detected if not provided)", 
                font=("Arial", 8), fg="gray").pack(side=tk.LEFT, padx=5)
        
        # Pipeline Parameters section
        params_label = tk.Label(self.adf_options_frame, text="Pipeline Parameter Names (defaults shown):",
                               font=("Arial", 9, "bold"))
        params_label.pack(anchor=tk.W, pady=(10, 2))
        
        # Table list parameter
        table_param_frame = ttk.Frame(self.adf_options_frame)
        table_param_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(table_param_frame, text="Table List Parameter:").pack(side=tk.LEFT, padx=5)
        self.adf_table_list_param_var = tk.StringVar(value="TableList")
        ttk.Entry(table_param_frame, textvariable=self.adf_table_list_param_var, width=30).pack(side=tk.LEFT, padx=5)
        
        # Truncate parameter
        truncate_param_frame = ttk.Frame(self.adf_options_frame)
        truncate_param_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(truncate_param_frame, text="Truncate Parameter:").pack(side=tk.LEFT, padx=5)
        self.adf_truncate_param_var = tk.StringVar(value="TruncateDestination")
        ttk.Entry(truncate_param_frame, textvariable=self.adf_truncate_param_var, width=30).pack(side=tk.LEFT, padx=5)
        
        # Source/Dest parameter names (optional, with defaults)
        src_param_frame = ttk.Frame(self.adf_options_frame)
        src_param_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(src_param_frame, text="Source Server Param:").pack(side=tk.LEFT, padx=5)
        self.adf_src_server_param_var = tk.StringVar(value="SourceServer")
        ttk.Entry(src_param_frame, textvariable=self.adf_src_server_param_var, width=20).pack(side=tk.LEFT, padx=5)
        tk.Label(src_param_frame, text="Source DB Param:").pack(side=tk.LEFT, padx=5)
        self.adf_src_db_param_var = tk.StringVar(value="SourceDatabase")
        ttk.Entry(src_param_frame, textvariable=self.adf_src_db_param_var, width=20).pack(side=tk.LEFT, padx=5)
        
        dest_param_frame = ttk.Frame(self.adf_options_frame)
        dest_param_frame.pack(anchor=tk.W, fill=tk.X, pady=2)
        tk.Label(dest_param_frame, text="Dest Server Param:").pack(side=tk.LEFT, padx=5)
        self.adf_dest_server_param_var = tk.StringVar(value="DestServer")
        ttk.Entry(dest_param_frame, textvariable=self.adf_dest_server_param_var, width=20).pack(side=tk.LEFT, padx=5)
        tk.Label(dest_param_frame, text="Dest DB Param:").pack(side=tk.LEFT, padx=5)
        self.adf_dest_db_param_var = tk.StringVar(value="DestDatabase")
        ttk.Entry(dest_param_frame, textvariable=self.adf_dest_db_param_var, width=20).pack(side=tk.LEFT, padx=5)
        
        # Hide ADF options initially
        self.adf_options_frame.grid_remove()
        
        # Performance & Resilience Options
        perf_frame = ttk.LabelFrame(scrollable_frame, text="Performance & Resilience", padding=10)
        perf_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Row 1: Parallel tables
        parallel_frame = ttk.Frame(perf_frame)
        parallel_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(parallel_frame, text="Parallel Tables:").pack(side=tk.LEFT, padx=5)
        self.parallel_tables_var = tk.IntVar(value=1)
        parallel_spin = ttk.Spinbox(parallel_frame, from_=1, to=8, width=5, 
                                    textvariable=self.parallel_tables_var)
        parallel_spin.pack(side=tk.LEFT, padx=5)
        tk.Label(parallel_frame, text="(1-8 tables migrated simultaneously)", 
                fg="gray").pack(side=tk.LEFT, padx=5)
        
        # Row 2: Skip completed tables
        self.skip_completed_var = tk.BooleanVar(value=False)
        skip_check = ttk.Checkbutton(perf_frame, 
                                     text="Skip Completed Tables (compare src/dest row counts, skip if match)",
                                     variable=self.skip_completed_var)
        skip_check.pack(anchor=tk.W, pady=2)
        
        # Row 3: Resume on failure
        self.resume_enabled_var = tk.BooleanVar(value=True)
        resume_check = ttk.Checkbutton(perf_frame,
                                       text="Enable Resume (checkpoint progress, resume after failures)",
                                       variable=self.resume_enabled_var)
        resume_check.pack(anchor=tk.W, pady=2)
        
        # Row 4: Max retries
        retry_frame = ttk.Frame(perf_frame)
        retry_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(retry_frame, text="Max Retries per Table:").pack(side=tk.LEFT, padx=5)
        self.max_retries_var = tk.IntVar(value=3)
        retry_spin = ttk.Spinbox(retry_frame, from_=1, to=10, width=5,
                                 textvariable=self.max_retries_var)
        retry_spin.pack(side=tk.LEFT, padx=5)
        tk.Label(retry_frame, text="(with exponential backoff: 1s, 2s, 4s...)",
                fg="gray").pack(side=tk.LEFT, padx=5)
        
        # Row 5: Verify after copy
        self.verify_after_copy_var = tk.BooleanVar(value=False)
        verify_check = ttk.Checkbutton(perf_frame,
                                       text="Verify After Copy (compare row counts after each table)",
                                       variable=self.verify_after_copy_var)
        verify_check.pack(anchor=tk.W, pady=2)
        
        # Row 6: Chunked migration (same as Full Migration – for ODBC/Standard method)
        chunk_row = ttk.Frame(perf_frame)
        chunk_row.pack(fill=tk.X, pady=8)
        self.enable_chunking_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(chunk_row, text="Enable chunked migration (parallel slices, retry on fail)",
                       variable=self.enable_chunking_var).pack(side=tk.LEFT, padx=(0, 10))
        tk.Label(chunk_row, text="Threshold (rows):").pack(side=tk.LEFT, padx=(0, 5))
        self.chunk_threshold_var = tk.IntVar(value=500_000)
        ttk.Spinbox(chunk_row, from_=100_000, to=500_000_000, increment=100_000,
                    width=12, textvariable=self.chunk_threshold_var).pack(side=tk.LEFT, padx=2)
        tk.Label(chunk_row, text="Chunks (max 32):").pack(side=tk.LEFT, padx=(10, 5))
        self.num_chunks_var = tk.IntVar(value=10)
        ttk.Spinbox(chunk_row, from_=2, to=32, width=5,
                    textvariable=self.num_chunks_var).pack(side=tk.LEFT, padx=2)
        tk.Label(chunk_row, text="Workers (max 32):").pack(side=tk.LEFT, padx=(10, 5))
        self.chunk_workers_var = tk.IntVar(value=4)
        ttk.Spinbox(chunk_row, from_=1, to=32, width=5,
                    textvariable=self.chunk_workers_var).pack(side=tk.LEFT, padx=2)
        tk.Label(chunk_row, text="(ODBC/Standard method only)", fg="gray", font=("Arial", 8)).pack(side=tk.LEFT, padx=5)
        
        # Progress frame
        progress_frame = ttk.LabelFrame(scrollable_frame, text="Progress", padding=10)
        progress_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.progress_var = tk.StringVar(value="Ready")
        tk.Label(progress_frame, textvariable=self.progress_var, font=("Arial", 10)).pack(anchor=tk.W)
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress_bar.pack(fill=tk.X, pady=5)
        
        # Excel support frame
        excel_frame = ttk.LabelFrame(scrollable_frame, text="Bulk Processing (Excel)", padding=10)
        excel_frame.pack(fill=tk.X, padx=10, pady=10)
        
        excel_btn_frame = ttk.Frame(excel_frame)
        excel_btn_frame.pack(fill=tk.X)
        
        ttk.Button(excel_btn_frame, text="Download Sample Template", 
                  command=lambda: self._download_template("data_migration")).pack(side=tk.LEFT, padx=5)
        ttk.Button(excel_btn_frame, text="Upload Excel File", 
                  command=self._upload_excel).pack(side=tk.LEFT, padx=5)
        
        self.excel_file_var = tk.StringVar()
        tk.Label(excel_frame, textvariable=self.excel_file_var, fg="gray").pack(anchor=tk.W, pady=5)
        
        self.excel_configs = []
        
        # Buttons
        btn_frame = ttk.Frame(scrollable_frame)
        btn_frame.pack(pady=10)
        
        self.migrate_btn = ttk.Button(btn_frame, text="Start Migration", command=self._start_migration, width=20)
        self.migrate_btn.pack(side=tk.LEFT, padx=5)

        self.bcp_validate_btn = ttk.Button(
            btn_frame,
            text="Validate (BCP)",
            command=self._run_bcp_preflight_validation,
            width=16,
        )
        self.bcp_validate_btn.pack(side=tk.LEFT, padx=5)
        
        self.bulk_migrate_btn = ttk.Button(btn_frame, text="Start Bulk Migration", command=self._start_bulk_migration, 
                                           width=20, state=tk.DISABLED)
        self.bulk_migrate_btn.pack(side=tk.LEFT, padx=5)
        
        # Log output
        log_frame = ttk.LabelFrame(scrollable_frame, text="Migration Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.migration_log = scrolledtext.ScrolledText(log_frame, height=15, wrap=tk.WORD)
        self.migration_log.pack(fill=tk.BOTH, expand=True)

        self._on_migration_method_change()
        
    def _on_migration_method_change(self):
        """Show/hide options based on selected migration method."""
        method = self.migration_method_var.get()
        if method == "bcp":
            self.batch_size_label.grid_remove()
            self.batch_size_entry.grid_remove()
            self.bcp_options_frame.grid()
            self.adf_options_frame.grid_remove()
            if hasattr(self, "bcp_validate_btn"):
                self.bcp_validate_btn.pack(side=tk.LEFT, padx=5, before=self.bulk_migrate_btn)
        elif method == "adf":
            self.batch_size_label.grid_remove()
            self.batch_size_entry.grid_remove()
            self.bcp_options_frame.grid_remove()
            self.adf_options_frame.grid()
            if hasattr(self, "bcp_validate_btn"):
                self.bcp_validate_btn.pack_forget()
        else:  # standard
            self.batch_size_label.grid()
            self.batch_size_entry.grid()
            self.bcp_options_frame.grid_remove()
            self.adf_options_frame.grid_remove()
            if hasattr(self, "bcp_validate_btn"):
                self.bcp_validate_btn.pack_forget()
    
    def _find_bcp_exe(self, *, prefer_odbc_18: bool = False):
        """Find bcp.exe (bundled tools folder, install dir, or system paths)."""
        try:
            from src.utils.bcp_tools import find_bcp_exe
            return find_bcp_exe(prefer_odbc_18=prefer_odbc_18)
        except ImportError:
            try:
                from azure_migration_tool.src.utils.bcp_tools import find_bcp_exe
                return find_bcp_exe(prefer_odbc_18=prefer_odbc_18)
            except ImportError:
                return shutil.which("bcp.exe")

    def _resolve_auth_var(self, auth_var) -> str:
        """Return internal auth key (entra_mfa, windows, sql, ...); sync from combo display if needed."""
        raw = (auth_var.get() or "").strip()
        auth = raw.lower()
        if auth in ("entra_mfa", "entra_password", "sql", "windows"):
            return auth
        try:
            from gui.widgets.connection_widget import AUTH_TO_INTERNAL
        except ImportError:
            from azure_migration_tool.gui.widgets.connection_widget import AUTH_TO_INTERNAL
        internal = AUTH_TO_INTERNAL.get(raw)
        if internal:
            auth_var.set(internal)
            return internal
        return auth

    def _build_migration_cfg(self) -> dict:
        """Collect migration settings from the form."""
        bcp_single_table = ""
        if hasattr(self, "bcp_single_table_var"):
            bcp_single_table = (self.bcp_single_table_var.get() or "").strip()
        bcp_work_dir = ""
        if hasattr(self, "bcp_work_dir_var"):
            bcp_work_dir = (self.bcp_work_dir_var.get() or "").strip()
        return {
            "src_server": self.src_server_var.get(),
            "src_db": self.src_db_var.get(),
            "src_auth": self._resolve_auth_var(self.src_auth_var),
            "src_user": self.src_user_var.get(),
            "src_password": self.src_password_var.get() or None,
            "dest_server": self.dest_server_var.get(),
            "dest_db": self.dest_db_var.get(),
            "dest_auth": self._resolve_auth_var(self.dest_auth_var),
            "dest_user": self.dest_user_var.get(),
            "dest_password": self.dest_password_var.get() or None,
            "batch_size": int(self.batch_size_var.get()) if self.batch_size_var.get() else 20000,
            "truncate_dest": self.truncate_dest_var.get(),
            "delete_dest": False,
            "continue_on_error": self.continue_on_error_var.get(),
            "dry_run": False,
            "tables": bcp_single_table,
            "bcp_work_dir": bcp_work_dir,
            "exclude": "",
            "disable_fk": self.disable_fk_var.get(),
            "disable_indexes": self.disable_indexes_var.get(),
            "disable_triggers": self.disable_triggers_var.get(),
            "parallel_tables": self.parallel_tables_var.get(),
            "skip_completed": self.skip_completed_var.get(),
            "resume_enabled": self.resume_enabled_var.get(),
            "max_retries": self.max_retries_var.get(),
            "verify_after_copy": self.verify_after_copy_var.get(),
            "enable_chunking": self.enable_chunking_var.get(),
            "chunk_threshold": self.chunk_threshold_var.get(),
            "num_chunks": self.num_chunks_var.get(),
            "chunk_workers": self.chunk_workers_var.get(),
        }

    def _run_bcp_preflight_validation(self):
        """Run BCP pre-flight checks and populate the checklist UI."""
        if not self.src_server_var.get() or not self.src_db_var.get():
            messagebox.showerror("Pre-flight", "Source server and database are required.")
            return
        if not self.dest_server_var.get() or not self.dest_db_var.get():
            messagebox.showerror("Pre-flight", "Destination server and database are required.")
            return

        self.bcp_preflight_btn.config(state=tk.DISABLED)
        self.bcp_validate_btn.config(state=tk.DISABLED)
        self.bcp_preflight_status_var.set("Validating...")
        self.migration_log.insert(tk.END, "\n--- BCP pre-flight validation ---\n")
        self.migration_log.see(tk.END)

        src_db_type = (
            getattr(getattr(self, "src_connection_widget", None), "db_type_var", None)
            and self.src_connection_widget.db_type_var.get()
            or "sqlserver"
        ).strip().lower()
        if src_db_type == "db2":
            msg = (
                "BCP data migration requires a SQL Server / Azure SQL source. "
                "Your source Database Type is IBM DB2 (host like PS-FUS-DB21Q). "
                "Switch Source Database Type to SQL Server / Azure SQL and point at a SQL "
                "database that already has the table (e.g. gpitd-shir01...\\i2022 / "
                "FUSION_ps_fus_db21q), or use a DB2→SQL path — do not run BCP against DB2."
            )
            self.migration_log.insert(tk.END, f"[FAIL] {msg}\n")
            self.migration_log.see(tk.END)
            self.bcp_preflight_status_var.set("FAILED — source is DB2")
            self.bcp_preflight_btn.config(state=tk.NORMAL)
            self.bcp_validate_btn.config(state=tk.NORMAL)
            messagebox.showerror("BCP pre-flight", msg)
            return

        cfg = self._build_migration_cfg()

        def worker():
            try:
                try:
                    from src.migration.bcp_preflight import preflight_passed, run_bcp_preflight
                except ImportError:
                    from azure_migration_tool.src.migration.bcp_preflight import (
                        preflight_passed,
                        run_bcp_preflight,
                    )

                def log_cb(msg):
                    self.frame.after(0, lambda m=msg: self._append_preflight_log(m))

                items = run_bcp_preflight(cfg, log_cb, install_bcp_if_missing=True)
                passed = preflight_passed(items)
                self.frame.after(0, lambda: self._show_preflight_results(items, passed))
            except Exception as e:
                self.frame.after(
                    0,
                    lambda: messagebox.showerror("Pre-flight", str(e)),
                )
            finally:
                self.frame.after(0, self._preflight_buttons_idle)

        threading.Thread(target=worker, daemon=True).start()

    def _append_preflight_log(self, msg: str):
        self.migration_log.insert(tk.END, msg + "\n")
        self.migration_log.see(tk.END)

    def _preflight_buttons_idle(self):
        self.bcp_preflight_btn.config(state=tk.NORMAL)
        self.bcp_validate_btn.config(state=tk.NORMAL)

    def _on_bcp_preflight_select(self, _event=None) -> None:
        sel = self.bcp_preflight_tree.selection()
        if not sel:
            return
        self._set_bcp_preflight_detail(self._bcp_preflight_messages.get(sel[0], ""))

    def _set_bcp_preflight_detail(self, text: str) -> None:
        self.bcp_preflight_detail.config(state=tk.NORMAL)
        self.bcp_preflight_detail.delete("1.0", tk.END)
        self.bcp_preflight_detail.insert(tk.END, text or "")
        self.bcp_preflight_detail.config(state=tk.DISABLED)
        self.bcp_preflight_detail.see("1.0")

    def _show_preflight_results(self, items, passed: bool):
        for row in self.bcp_preflight_tree.get_children():
            self.bcp_preflight_tree.delete(row)
        self._bcp_preflight_messages.clear()
        first_iid = None
        for item in items:
            icon = "OK" if item.passed else ("!" if not item.blocking else "X")
            # Short preview in grid; full text in detail panel below
            preview = item.message.replace("\n", " | ")
            if len(preview) > 120:
                preview = preview[:117] + "..."
            iid = self.bcp_preflight_tree.insert(
                "",
                tk.END,
                values=(icon, item.category, item.name, preview),
            )
            self._bcp_preflight_messages[iid] = item.message
            if first_iid is None:
                first_iid = iid
        if first_iid:
            self.bcp_preflight_tree.selection_set(first_iid)
            self.bcp_preflight_tree.focus(first_iid)
            self._set_bcp_preflight_detail(self._bcp_preflight_messages[first_iid])
        else:
            self._set_bcp_preflight_detail("")
        self._bcp_preflight_passed = passed
        if passed:
            self.bcp_preflight_status_var.set("Passed — ready to migrate")
        else:
            blocking = sum(1 for i in items if not i.passed and i.blocking)
            self.bcp_preflight_status_var.set(f"Failed — {blocking} blocking issue(s)")
        self.migration_log.insert(
            tk.END,
            f"Pre-flight: {'PASSED' if passed else 'FAILED'}\n",
        )
        self.migration_log.see(tk.END)
        if not passed:
            self._bcp_preflight_then_migrate = False
            messagebox.showwarning(
                "Pre-flight failed",
                "Fix blocking items in the checklist (marked X), then run validation again.",
            )
        elif getattr(self, "_bcp_preflight_then_migrate", False):
            self._bcp_preflight_then_migrate = False
            self.migration_log.insert(tk.END, "Pre-flight passed — starting migration...\n")
            self.migration_log.see(tk.END)
            self._start_migration()
    
    def _install_bcp_via_powershell(self):
        """Attempt to install BCP using PowerShell."""
        try:
            try:
                from src.utils.bcp_tools import ensure_bcp_installed, find_bundled_sqlcmd_msi
            except ImportError:
                from azure_migration_tool.src.utils.bcp_tools import ensure_bcp_installed, find_bundled_sqlcmd_msi

            if find_bundled_sqlcmd_msi():
                self.migration_log.insert(tk.END, "Installing BCP from bundled MSI...\n")
                self.migration_log.see(tk.END)
                ok, msg = ensure_bcp_installed(
                    lambda m: self.migration_log.insert(tk.END, m + "\n"),
                    allow_download=False,
                )
                if ok and self._find_bcp_exe():
                    return True, None
                if not ok:
                    self.migration_log.insert(tk.END, f"Bundled install: {msg}\n")

            self.migration_log.insert(tk.END, "Installing BCP using PowerShell...\n")
            self.migration_log.insert(tk.END, "Note: This may require administrator privileges.\n")
            self.migration_log.see(tk.END)
            
            # PowerShell script to download and install BCP
            # Using base64 encoding to avoid quote/escape issues
            ps_script = '''
$ErrorActionPreference = "Continue"
$ProgressPreference = "SilentlyContinue"

# Download URLs for SQL Server Command Line Utilities
$downloadUrls = @(
    "https://go.microsoft.com/fwlink/?linkid=2230791",
    "https://go.microsoft.com/fwlink/?linkid=2142258"
)

$tempDir = $env:TEMP
$installerPath = Join-Path $tempDir "SqlCmdLnUtils.msi"

# Remove existing installer if present
if (Test-Path $installerPath) {
    Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
}

# Try to download from each URL
$downloaded = $false
foreach ($url in $downloadUrls) {
    try {
        Write-Host "Attempting to download from: $url"
        $ProgressPreference = "SilentlyContinue"
        Invoke-WebRequest -Uri $url -OutFile $installerPath -UseBasicParsing -TimeoutSec 300
        if (Test-Path $installerPath -PathType Leaf) {
            $fileSize = (Get-Item $installerPath).Length
            if ($fileSize -gt 1000000) {  # At least 1MB
                Write-Host "Download complete: $installerPath ($([math]::Round($fileSize/1MB, 2)) MB)"
                $downloaded = $true
                break
            } else {
                Write-Host "Downloaded file too small, trying next URL..."
                Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
            }
        }
    } catch {
        Write-Host "Failed to download from $url : $($_.Exception.Message)"
        if (Test-Path $installerPath) {
            Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
        }
        continue
    }
}

if (-not $downloaded) {
    Write-Host "ERROR: Failed to download installer from all URLs"
    exit 1
}

# Install MSI silently
Write-Host "Installing BCP (this may take a few minutes, please wait)..."
$logPath = Join-Path $tempDir "bcp_install.log"
$installArgs = @(
    "/i",
    "`"$installerPath`"",
    "/quiet",
    "/norestart",
    "/L*v",
    "`"$logPath`""
)

try {
    $process = Start-Process -FilePath "msiexec.exe" -ArgumentList $installArgs -Wait -PassThru -NoNewWindow
    
    if ($process.ExitCode -eq 0 -or $process.ExitCode -eq 3010) {
        Write-Host "Installation completed (exit code: $($process.ExitCode))"
        # Wait for files to be registered
        Start-Sleep -Seconds 5
    } else {
        Write-Host "ERROR: Installation failed with exit code: $($process.ExitCode)"
        if (Test-Path $logPath) {
            Write-Host "Check log file: $logPath"
        }
        exit $process.ExitCode
    }
} catch {
    Write-Host "ERROR: Failed to run installer: $($_.Exception.Message)"
    exit 1
}

# Clean up installer
if (Test-Path $installerPath) {
    Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
    Write-Host "Cleaned up installer file"
}

Write-Host "Installation process completed successfully"
exit 0
'''
            
            # Run PowerShell script
            # Use -EncodedCommand for better reliability with special characters
            import base64
            ps_bytes = ps_script.encode('utf-16-le')
            ps_encoded = base64.b64encode(ps_bytes).decode('ascii')
            
            ps_cmd = [
                "powershell.exe",
                "-ExecutionPolicy", "Bypass",
                "-NoProfile",
                "-NonInteractive",
                "-EncodedCommand", ps_encoded
            ]
            
            self.migration_log.insert(tk.END, "Running PowerShell installation script...\n")
            self.migration_log.see(tk.END)
            
            result = _run_silent(
                ps_cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minutes
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            )
            
            # Display output
            if result.stdout:
                self.migration_log.insert(tk.END, result.stdout)
            if result.stderr:
                self.migration_log.insert(tk.END, f"PowerShell output: {result.stderr}\n")
            self.migration_log.see(tk.END)
            
            if result.returncode == 0:
                # Wait a moment for installation to complete
                import time
                time.sleep(3)
                
                # Verify installation
                bcp_path = self._find_bcp_exe()
                if bcp_path:
                    self.migration_log.insert(tk.END, f"[OK] BCP installed successfully at: {bcp_path}\n")
                    self.migration_log.see(tk.END)
                    return True, None
                else:
                    # Check installation log
                    log_path = os.path.join(tempfile.gettempdir(), "bcp_install.log")
                    if os.path.exists(log_path):
                        self.migration_log.insert(tk.END, f"Installation may have completed. Check log: {log_path}\n")
                        self.migration_log.insert(tk.END, "If BCP is still not found, please restart the application.\n")
                    return False, "BCP installed but not found. Please restart the application."
            else:
                error_msg = result.stderr or result.stdout or f"Exit code: {result.returncode}"
                # Check if it's a permission issue
                if "access" in error_msg.lower() or "permission" in error_msg.lower() or result.returncode == 5:
                    return False, "Installation requires administrator privileges. Please run the application as Administrator."
                return False, f"PowerShell installation failed: {error_msg}"
                
        except subprocess.TimeoutExpired:
            return False, "Installation timed out. Please try installing manually."
        except Exception as e:
            return False, f"Installation error: {str(e)}"
    
    def _download_bcp_installer(self):
        """Download BCP installer from Microsoft."""
        import urllib.request
        import tempfile
        
        # Microsoft download URL for SQL Server Command Line Utilities
        # Note: This URL may need to be updated - Microsoft changes download links
        download_urls = [
            "https://go.microsoft.com/fwlink/?linkid=2230791",  # SQL Server 2022 Command Line Utilities
            "https://go.microsoft.com/fwlink/?linkid=2142258",  # SQL Server 2019 Command Line Utilities
        ]
        
        temp_dir = tempfile.gettempdir()
        installer_path = os.path.join(temp_dir, "SqlCmdLnUtils.msi")
        
        try:
            self.migration_log.insert(tk.END, "Downloading SQL Server Command Line Utilities installer...\n")
            self.migration_log.see(tk.END)
            
            for url in download_urls:
                try:
                    urllib.request.urlretrieve(url, installer_path)
                    if os.path.exists(installer_path) and os.path.getsize(installer_path) > 0:
                        self.migration_log.insert(tk.END, f"[OK] Download complete: {installer_path}\n")
                        self.migration_log.see(tk.END)
                        return installer_path
                except Exception as e:
                    self.migration_log.insert(tk.END, f"  Failed to download from {url}: {str(e)}\n")
                    continue
            
            return None
            
        except Exception as e:
            self.migration_log.insert(tk.END, f"[X] Download failed: {str(e)}\n")
            return None
    
    def _install_bcp_installer(self, installer_path):
        """Run the BCP installer MSI file."""
        try:
            self.migration_log.insert(tk.END, "Running installer (this may take a few minutes)...\n")
            self.migration_log.see(tk.END)
            
            # Run installer silently
            install_cmd = [
                "msiexec",
                "/i", installer_path,
                "/quiet",  # Silent installation
                "/norestart"  # Don't restart
            ]
            
            result = _run_silent(
                install_cmd,
                capture_output=True,
                text=True,
                timeout=600  # 10 minutes
            )
            
            if result.returncode == 0:
                # Wait for installation to complete
                import time
                time.sleep(3)
                
                # Verify installation
                bcp_path = self._find_bcp_exe()
                if bcp_path:
                    self.migration_log.insert(tk.END, f"[OK] BCP installed successfully at: {bcp_path}\n")
                    self.migration_log.see(tk.END)
                    return True, None
                else:
                    return False, "Installer completed but BCP not found. Please restart the application."
            else:
                error_msg = result.stderr or result.stdout
                return False, f"Installation failed: {error_msg}"
                
        except subprocess.TimeoutExpired:
            return False, "Installation timed out."
        except Exception as e:
            return False, f"Installation error: {str(e)}"
    
    def _handle_bcp_not_found(self):
        """Handle BCP not found - offer installation options."""
        response = messagebox.askyesnocancel(
            "BCP Not Found",
            "BCP (Bulk Copy Program) utility is not installed.\n\n"
            "Would you like to install it automatically?\n\n"
            "Yes: Try automatic installation via PowerShell\n"
            "No: Show manual installation instructions\n"
            "Cancel: Cancel migration"
        )
        
        if response is None:  # Cancel
            return False
        
        if response:  # Yes - Try automatic installation
            self.migration_log.insert(tk.END, "\nAttempting to install BCP using PowerShell...\n")
            self.migration_log.see(tk.END)
            
            # Try PowerShell installation
            success, error = self._install_bcp_via_powershell()
            if success:
                messagebox.showinfo("Success", "BCP installed successfully! You can now proceed with migration.")
                return True
            else:
                # If PowerShell installation failed, try direct download method
                self.migration_log.insert(tk.END, f"PowerShell installation failed: {error}\n")
                self.migration_log.insert(tk.END, "Trying alternative download method...\n")
                self.migration_log.see(tk.END)
            
            installer_path = self._download_bcp_installer()
            if installer_path:
                if messagebox.askyesno(
                    "Installer Downloaded",
                    f"Installer downloaded to:\n{installer_path}\n\n"
                    "Would you like to run it now?\n\n"
                    "Note: Installation requires administrator privileges."
                ):
                    success, error = self._install_bcp_installer(installer_path)
                    if success:
                        messagebox.showinfo("Success", "BCP installed successfully! You can now proceed with migration.")
                        return True
                    else:
                        messagebox.showerror("Installation Failed", f"Failed to install BCP:\n{error}")
                        self._show_bcp_manual_instructions()
                        return False
                else:
                    messagebox.showinfo(
                        "Installer Ready",
                        f"Installer saved to:\n{installer_path}\n\n"
                        "Please run it manually to install BCP."
                    )
                    return False
            else:
                # Show manual instructions
                self._show_bcp_manual_instructions()
                return False
        else:  # No - Show manual instructions
            self._show_bcp_manual_instructions()
            return False
    
    def _show_bcp_manual_instructions(self):
        """Show manual installation instructions."""
        instructions = """
BCP (Bulk Copy Program) Installation Instructions:

1. Download SQL Server Command Line Utilities:
   https://learn.microsoft.com/en-us/sql/tools/bcp-utility

2. Or install via PowerShell (run as Administrator):
   # Download and install in one command
   $url = "https://go.microsoft.com/fwlink/?linkid=2230791"
   $installer = "$env:TEMP\SqlCmdLnUtils.msi"
   Invoke-WebRequest -Uri $url -OutFile $installer
   Start-Process msiexec.exe -ArgumentList "/i $installer /quiet /norestart" -Wait

3. Or download directly:
   https://go.microsoft.com/fwlink/?linkid=2230791

4. After installation, restart this application.

Alternative: Install SQL Server Management Studio (SSMS) which includes BCP.
        """
        
        # Create a new window with instructions
        inst_window = tk.Toplevel(self.frame.winfo_toplevel())
        inst_window.title("BCP Installation Instructions")
        inst_window.geometry("600x400")
        
        text_widget = scrolledtext.ScrolledText(inst_window, wrap=tk.WORD, padx=10, pady=10)
        text_widget.pack(fill=tk.BOTH, expand=True)
        text_widget.insert("1.0", instructions)
        text_widget.config(state=tk.DISABLED)
        
        btn_frame = ttk.Frame(inst_window)
        btn_frame.pack(pady=10)
        
        def open_download():
            import webbrowser
            webbrowser.open("https://learn.microsoft.com/en-us/sql/tools/bcp-utility")
        
        ttk.Button(btn_frame, text="Open Download Page", command=open_download).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Close", command=inst_window.destroy).pack(side=tk.LEFT, padx=5)
    
    def _generate_bcp_password(self, length=16):
        """Generate a secure random password for BCP login that meets SQL Server requirements."""
        # SQL Server password requirements:
        # - At least 8 characters (we use 16)
        # - Contains uppercase, lowercase, digit, and special character
        if length < 8:
            length = 16
        
        # Ensure we have at least one of each required character type
        uppercase = secrets.choice(string.ascii_uppercase)
        lowercase = secrets.choice(string.ascii_lowercase)
        digit = secrets.choice(string.digits)
        special = secrets.choice("!@#$%^&*")
        
        # Fill the rest randomly
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        remaining = ''.join(secrets.choice(alphabet) for _ in range(length - 4))
        
        # Combine and shuffle
        password_chars = list(uppercase + lowercase + digit + special + remaining)
        secrets.SystemRandom().shuffle(password_chars)
        password = ''.join(password_chars)
        
        return password
    
    def _create_sql_login_for_bcp(self, conn, db_name, login_name, password, logger=None):
        """
        Create SQL login and user for BCP operations.
        Returns: (success, error_message)
        """
        try:
            cur = conn.cursor()
            
            # Check if login exists
            cur.execute("SELECT name FROM sys.server_principals WHERE name = ?", login_name)
            if cur.fetchone():
                # Login exists, update password
                if logger:
                    logger(f"Login {login_name} already exists, updating password...")
                cur.execute(f"ALTER LOGIN [{login_name}] WITH PASSWORD = '{password}'")
            else:
                # Create login in master
                if logger:
                    logger(f"Creating login {login_name} in master database...")
                cur.execute("USE master")
                cur.execute(f"""
                    CREATE LOGIN [{login_name}] 
                    WITH PASSWORD = '{password}'
                """)
            
            # Switch to target database
            cur.execute(f"USE [{db_name}]")
            
            # Check if user exists
            cur.execute("SELECT name FROM sys.database_principals WHERE name = ?", login_name)
            if cur.fetchone():
                # User exists, ensure role membership
                if logger:
                    logger(f"User {login_name} already exists, ensuring db_owner role...")
                try:
                    cur.execute(f"ALTER ROLE db_owner ADD MEMBER [{login_name}]")
                except:
                    pass  # Already a member
            else:
                # Create user
                if logger:
                    logger(f"Creating user {login_name} in database {db_name}...")
                cur.execute(f"CREATE USER [{login_name}] FOR LOGIN [{login_name}]")
                cur.execute(f"ALTER ROLE db_owner ADD MEMBER [{login_name}]")
            
            conn.commit()
            return (True, None)
            
        except Exception as e:
            conn.rollback()
            return (False, str(e))
    
    def _cleanup_sql_login(self, conn, db_name, login_name, logger=None):
        """Clean up SQL login and user after migration."""
        try:
            cur = conn.cursor()
            
            # Remove user from database
            cur.execute(f"USE [{db_name}]")
            cur.execute(f"IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '{login_name}') DROP USER [{login_name}]")
            
            # Remove login from server
            cur.execute("USE master")
            cur.execute(f"IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '{login_name}') DROP LOGIN [{login_name}]")
            
            conn.commit()
            if logger:
                logger(f"Cleaned up login {login_name}")
            return True
        except Exception as e:
            if logger:
                logger(f"Error cleaning up login: {str(e)}")
            return False
    
    def _acquire_azure_ad_token(self, username, logger=None):
        """
        Acquire Azure AD access token for SQL Database using MSAL.
        This prompts for MFA once and caches the token for reuse.
        
        Used by pyodbc (SQL_COPT_SS_ACCESS_TOKEN). On Windows, bcp.exe cannot use this
        token (-P with -G is Linux/macOS only); use SQL login or Entra interactive for BCP.
        """
        if msal is None:
            if logger:
                logger("[WARN] MSAL library not installed. Install with: pip install msal")
            return None
        
        # Azure SQL Database resource ID
        scopes = ["https://database.windows.net//.default"]
        
        # Try multiple well-known client IDs (some may not be available in all tenants)
        # Order: Azure PowerShell, Azure CLI, Visual Studio
        client_ids = [
            ("1950a258-227b-4e31-a9cf-717495945fc2", "Azure PowerShell"),
            ("04b07795-8ddf-4c3b-9f7f-8a4e6b7c2d7c", "Azure CLI"),
            ("872cd9fa-d31f-45e0-9eab-6e460a02d1f1", "Visual Studio"),
        ]
        
        # Extract tenant from username (email) if possible
        # e.g., user@company.com -> company.com
        authority = "https://login.microsoftonline.com/common"
        if username and "@" in username:
            domain = username.split("@")[1]
            # Use the domain as tenant hint
            authority = f"https://login.microsoftonline.com/{domain}"
            if logger:
                logger(f"Using tenant: {domain}")
        
        last_error = None
        
        for client_id, client_name in client_ids:
            try:
                if logger:
                    logger(f"Trying authentication with {client_name}...")
                
                # Create a public client application with token cache
                app = msal.PublicClientApplication(
                    client_id,
                    authority=authority,
                )
                
                # First, try to get token from cache silently
                accounts = app.get_accounts(username=username)
                if accounts:
                    if logger:
                        logger(f"Found cached token for {username}, attempting silent acquisition...")
                    result = app.acquire_token_silent(scopes, account=accounts[0])
                    if result and "access_token" in result:
                        if logger:
                            logger("[OK] Using cached Azure AD token (no MFA prompt needed)")
                        return result["access_token"]
                
                # No cached token, need interactive auth
                if logger:
                    logger("Acquiring Azure AD token interactively (MFA prompt will appear once)...")
                    logger("Please complete MFA authentication in the browser window...")
                
                # Use interactive flow with browser
                result = app.acquire_token_interactive(
                    scopes=scopes,
                    login_hint=username,
                    prompt="select_account"
                )
                
                if result and "access_token" in result:
                    if logger:
                        logger(f"[OK] Azure AD token acquired successfully via {client_name}")
                    return result["access_token"]
                else:
                    error = result.get("error_description", result.get("error", "Unknown error"))
                    last_error = error
                    if "AADSTS700016" in str(error):
                        # App not found in tenant, try next client ID
                        if logger:
                            logger(f"  {client_name} not available in tenant, trying next...")
                        continue
                    else:
                        if logger:
                            logger(f"[X] Failed to acquire token: {error}")
                        return None
                        
            except Exception as e:
                last_error = str(e)
                if "AADSTS700016" in str(e):
                    # App not found in tenant, try next client ID
                    if logger:
                        logger(f"  {client_name} not available in tenant, trying next...")
                    continue
                else:
                    if logger:
                        logger(f"[X] Error with {client_name}: {str(e)}")
                    # Try next client ID
                    continue
        
        # All client IDs failed
        if logger:
            logger(f"[X] Failed to acquire token with all available methods")
            logger(f"  Last error: {last_error}")
            logger("  Tip: Try using SQL authentication instead of Entra MFA for BCP")
        return None
    
    def _build_bcp_auth_args(self, server, db, auth, user, password, access_token=None):
        """
        Build BCP authentication arguments based on auth type.
        
        If access_token is provided for Entra MFA, it will be used instead of 
        prompting for MFA interactively. This allows token reuse across multiple BCP calls.
        """
        args = ["-S", server, "-d", db]
        
        auth_lower = (auth or "").strip().lower()
        
        if auth_lower == "entra_mfa":
            args.extend(["-G", "-U", user])
            if access_token:
                try:
                    from src.utils.bcp_tools import bcp_entra_token_via_cli_supported
                except ImportError:
                    from azure_migration_tool.src.utils.bcp_tools import (
                        bcp_entra_token_via_cli_supported,
                    )
                if bcp_entra_token_via_cli_supported():
                    args.extend(["-P", access_token])
                # Windows: omit -P; bcp uses Entra interactive (ODBC 18) or caller uses SQL login
        elif auth_lower == "entra_password":
            # Entra with password (no MFA)
            args.extend(["-G", "-U", user])
            if password:
                args.extend(["-P", password])
        elif auth_lower == "sql":
            # SQL authentication
            if user and password:
                args.extend(["-U", user, "-P", password])
            else:
                raise ValueError("SQL authentication requires username and password")
        elif auth_lower == "windows":
            # Windows authentication
            args.append("-T")
        else:
            # Default to SQL auth if credentials provided
            if user and password:
                args.extend(["-U", user, "-P", password])
            else:
                args.append("-T")  # Fallback to Windows auth
        
        return args

    def _connect_migration_db(self, md, cfg, side: str, *, logger=None):
        """
        Open pyodbc connection for source or dest using correct build_conn_str argument order.

        build_conn_str(server, db, user, driver, auth, password) — not (server, db, driver, auth, user).
        Entra MFA uses connect_to_database (token / interactive) instead of a plain connection string.
        """
        import logging

        temp_logger = logger or logging.getLogger("bcp_migration_connect")
        driver = md.pick_sql_driver(temp_logger)
        prefix = "src" if side == "src" else "dest"
        server = (cfg.get(f"{prefix}_server") or "").strip()
        db = (cfg.get(f"{prefix}_db") or "").strip()
        auth = (cfg.get(f"{prefix}_auth") or "").strip().lower()
        user = (cfg.get(f"{prefix}_user") or "").strip()
        password = cfg.get(f"{prefix}_password")

        if not auth:
            raise ValueError(
                f"{prefix.capitalize()} authentication is not set. "
                "Select an auth type on the connection panel (e.g. Windows login or Microsoft account)."
            )

        if auth == "entra_mfa":
            return md.connect_to_database(
                server,
                db,
                user,
                driver,
                auth,
                password,
                timeout=60,
                logger=temp_logger,
            )
        conn_str = md.build_conn_str(server, db, user, driver, auth, password)
        return pyodbc.connect(conn_str, timeout=60)

    def _thread_safe_log(self, msg: str) -> None:
        """Append to migration log from a worker thread without freezing the UI."""
        text = (msg or "").rstrip()
        if not text:
            return

        def _append() -> None:
            self.migration_log.insert(tk.END, text + "\n")
            self.migration_log.see(tk.END)
            try:
                self.migration_log.update_idletasks()
            except tk.TclError:
                pass

        try:
            self.frame.after(0, _append)
        except tk.TclError:
            pass

    def _set_bcp_progress_ui(self, current: int, total: int, message: str) -> None:
        """Update progress label and bar (call from worker via after)."""

        def _ui() -> None:
            try:
                self.progress_var.set(message)
                if total > 0:
                    try:
                        self.progress_bar.stop()
                    except tk.TclError:
                        pass
                    self.progress_bar.config(mode="determinate", maximum=total, value=min(current, total))
                self.frame.update_idletasks()
            except tk.TclError:
                pass

        try:
            self.frame.after(0, _ui)
        except tk.TclError:
            pass

    def _run_bcp_subprocess(
        self,
        cmd: list,
        logger_callback,
        phase_label: str,
        *,
        watch_path: str | None = None,
        work_dir: str | None = None,
        timeout: int = 7200,
    ) -> subprocess.CompletedProcess:
        """
        Run bcp.exe with periodic heartbeat logs so the UI does not look frozen.
        BCP only prints when finished unless we poll output file size.
        """
        stop = threading.Event()
        t0 = time.perf_counter()

        def heartbeat() -> None:
            while not stop.wait(12):
                elapsed = int(time.perf_counter() - t0)
                extra = ""
                if watch_path and os.path.isfile(watch_path):
                    mb = os.path.getsize(watch_path) / (1024 * 1024)
                    extra = f", data file {mb:.1f} MB"
                logger_callback(f"  ... {phase_label} still running ({elapsed}s{extra})")

        logger_callback(f"  {phase_label} started...")
        hb = threading.Thread(target=heartbeat, daemon=True)
        hb.start()
        run_kwargs: dict = {"capture_output": True, "text": True, "timeout": timeout}
        if work_dir:
            run_kwargs["cwd"] = work_dir
            env = os.environ.copy()
            env["TEMP"] = work_dir
            env["TMP"] = work_dir
            run_kwargs["env"] = env
        try:
            result = _run_silent(cmd, **run_kwargs)
        finally:
            stop.set()
            hb.join(timeout=1)

        elapsed = time.perf_counter() - t0
        if watch_path and os.path.isfile(watch_path):
            mb = os.path.getsize(watch_path) / (1024 * 1024)
            logger_callback(f"  {phase_label} finished in {elapsed:.0f}s ({mb:.1f} MB on disk)")
        else:
            logger_callback(f"  {phase_label} finished in {elapsed:.0f}s")
        if result.stdout and result.stdout.strip():
            for line in result.stdout.strip().splitlines()[-3:]:
                if line.strip():
                    logger_callback(f"  bcp: {line.strip()}")
        return result
    
    def _migrate_with_bcp(self, cfg, logger_callback):
        """Migrate data using BCP with native MFA support."""
        md = _data_migration_module()
        dest_auth_early = (cfg.get("dest_auth") or "").strip().lower()
        bcp_exe = self._find_bcp_exe(prefer_odbc_18=(dest_auth_early == "entra_mfa"))
        if not bcp_exe:
            raise RuntimeError("BCP utility not found. Please install SQL Server Command Line Utilities.")
        
        logger_callback(f"Using BCP: {bcp_exe}")
        logger_callback("Preparing: connecting and building table list (large databases may take a few minutes)...")
        
        # Check if user wants to use SQL login instead
        use_sql_login = self.bcp_use_sql_login_var.get()
        auto_dest_sql_login = (
            sys.platform.startswith("win")
            and dest_auth_early == "entra_mfa"
            and not use_sql_login
        )
        bcp_login_name = None
        bcp_password = None
        bcp_dest_login_name = None
        bcp_dest_password = None
        
        if use_sql_login:
            # Generate password and create SQL login
            bcp_password = self._generate_bcp_password()
            bcp_login_name = self.bcp_login_name_var.get() or "svc_azdm_bcp"
            
            # Build connection strings for MFA to create logins
            import logging
            
            log_handler = logging.StreamHandler()
            log_handler.setFormatter(logging.Formatter('%(message)s'))
            temp_logger = logging.getLogger('bcp_migration')
            temp_logger.addHandler(log_handler)
            temp_logger.setLevel(logging.INFO)
            
            # Connect with MFA to create SQL logins
            logger_callback("Connecting to create SQL logins (if needed)...")
            src_conn = self._connect_migration_db(md, cfg, "src", logger=temp_logger)
            dest_conn = self._connect_migration_db(md, cfg, "dest", logger=temp_logger)
            
            try:
                logger_callback(f"Creating SQL login '{bcp_login_name}' on source database...")
                success, error = self._create_sql_login_for_bcp(
                    src_conn, cfg['src_db'], bcp_login_name, bcp_password,
                    lambda msg: logger_callback(f"  {msg}")
                )
                if not success:
                    raise RuntimeError(f"Failed to create login on source: {error}")
                
                logger_callback(f"Creating SQL login '{bcp_login_name}' on destination database...")
                success, error = self._create_sql_login_for_bcp(
                    dest_conn, cfg['dest_db'], bcp_login_name, bcp_password,
                    lambda msg: logger_callback(f"  {msg}")
                )
                if not success:
                    raise RuntimeError(f"Failed to create login on destination: {error}")
                
                logger_callback("[OK] SQL logins created successfully")
                
                # Get list of tables to migrate before closing connection
                src_cur = src_conn.cursor()
                # Dictionary to store source row counts for skip/resume logic
                self._bcp_source_row_counts = {}
                
                if cfg.get('tables'):
                    # Specific tables - still need to get row counts
                    table_list = [t.strip() for t in cfg['tables'].split(',')]
                    for tbl in table_list:
                        schema, tname = tbl.split('.') if '.' in tbl else ('dbo', tbl)
                        src_cur.execute(f"""
                            SELECT SUM(p.rows) FROM sys.partitions p 
                            JOIN sys.tables t ON p.object_id = t.object_id
                            JOIN sys.schemas s ON t.schema_id = s.schema_id
                            WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                        """, (schema, tname))
                        row = src_cur.fetchone()
                        self._bcp_source_row_counts[tbl] = row[0] or 0 if row else 0
                else:
                    # All tables with row counts, sorted by row count (LARGER tables first for BCP)
                    logger_callback("Getting table row counts (larger tables first for BCP)...")
                    src_cur.execute("""
                        SELECT s.name, t.name,
                               (SELECT SUM(p.rows) FROM sys.partitions p 
                                WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as row_count
                        FROM sys.tables t
                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                        WHERE t.is_ms_shipped = 0
                        ORDER BY row_count DESC, s.name, t.name
                    """)
                    rows = src_cur.fetchall()
                    table_list = [f"{row[0]}.{row[1]}" for row in rows]
                    
                    # Store row counts for skip/resume logic
                    for row in rows:
                        self._bcp_source_row_counts[f"{row[0]}.{row[1]}"] = row[2] or 0
                    
                    # Log the sorted order
                    logger_callback("Migration order (larger tables first):")
                    for i, row in enumerate(rows[:5], 1):
                        logger_callback(f"  {i}. {row[0]}.{row[1]}: {row[2] or 0:,} rows")
                    if len(rows) > 5:
                        logger_callback(f"  ... and {len(rows) - 5} more tables")
            finally:
                src_conn.close()
                dest_conn.close()
        else:
            # Not using SQL login - get table list via BCP query or pyodbc
            # For simplicity, we'll use pyodbc to get table list
            import logging
            
            log_handler = logging.StreamHandler()
            log_handler.setFormatter(logging.Formatter('%(message)s'))
            temp_logger = logging.getLogger('bcp_migration')
            temp_logger.addHandler(log_handler)
            temp_logger.setLevel(logging.INFO)
            
            temp_src_conn = self._connect_migration_db(md, cfg, "src", logger=temp_logger)
            try:
                src_cur = temp_src_conn.cursor()
                
                # Dictionary to store source row counts for skip/resume logic
                self._bcp_source_row_counts = {}
                
                if cfg.get('tables'):
                    # Specific tables - still need to get row counts
                    table_list = [t.strip() for t in cfg['tables'].split(',')]
                    for tbl in table_list:
                        schema, tname = tbl.split('.') if '.' in tbl else ('dbo', tbl)
                        src_cur.execute(f"""
                            SELECT SUM(p.rows) FROM sys.partitions p 
                            JOIN sys.tables t ON p.object_id = t.object_id
                            JOIN sys.schemas s ON t.schema_id = s.schema_id
                            WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                        """, (schema, tname))
                        row = src_cur.fetchone()
                        self._bcp_source_row_counts[tbl] = row[0] or 0 if row else 0
                else:
                    # All tables with row counts, sorted by row count (LARGER tables first for BCP)
                    logger_callback("Getting table row counts (larger tables first for BCP)...")
                    src_cur.execute("""
                        SELECT s.name, t.name,
                               (SELECT SUM(p.rows) FROM sys.partitions p 
                                WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) as row_count
                        FROM sys.tables t
                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                        WHERE t.is_ms_shipped = 0
                        ORDER BY row_count DESC, s.name, t.name
                    """)
                    rows = src_cur.fetchall()
                    table_list = [f"{row[0]}.{row[1]}" for row in rows]
                    
                    # Store row counts for skip/resume logic
                    for row in rows:
                        self._bcp_source_row_counts[f"{row[0]}.{row[1]}"] = row[2] or 0
                    
                    # Log the sorted order
                    logger_callback("Migration order (larger tables first):")
                    for i, row in enumerate(rows[:5], 1):
                        logger_callback(f"  {i}. {row[0]}.{row[1]}: {row[2] or 0:,} rows")
                    if len(rows) > 5:
                        logger_callback(f"  ... and {len(rows) - 5} more tables")
            finally:
                temp_src_conn.close()

        if auto_dest_sql_login:
            bcp_dest_password = self._generate_bcp_password()
            bcp_dest_login_name = self.bcp_login_name_var.get() or "svc_azdm_bcp"
            import logging

            log_handler = logging.StreamHandler()
            log_handler.setFormatter(logging.Formatter("%(message)s"))
            temp_logger = logging.getLogger("bcp_migration_dest_sql")
            if not temp_logger.handlers:
                temp_logger.addHandler(log_handler)
            temp_logger.setLevel(logging.INFO)
            logger_callback(
                "Destination uses Entra MFA: Windows bcp.exe cannot use cached access tokens. "
                f"Creating SQL login '{bcp_dest_login_name}' on destination for BCP import..."
            )
            dest_conn = self._connect_migration_db(md, cfg, "dest", logger=temp_logger)
            try:
                success, error = self._create_sql_login_for_bcp(
                    dest_conn,
                    cfg["dest_db"],
                    bcp_dest_login_name,
                    bcp_dest_password,
                    lambda msg: logger_callback(f"  {msg}"),
                )
                if not success:
                    raise RuntimeError(f"Failed to create SQL login on destination: {error}")
                logger_callback("[OK] Destination SQL login ready for BCP import")
            finally:
                dest_conn.close()
        
        # Store source row counts for skip/resume logic
        source_row_counts = {}
        if hasattr(self, '_bcp_source_row_counts'):
            source_row_counts = self._bcp_source_row_counts
        
        # Filter out backup tables if option is enabled
        original_count = len(table_list)
        exclude_backup = self.bcp_exclude_backup_var.get() if hasattr(self, 'bcp_exclude_backup_var') else True
        
        if exclude_backup:
            backup_patterns = ['bak_', '_backup', '_bak', '_old', 'backup_', 'old_']
            filtered_list = []
            excluded_backup = []
            for tbl in table_list:
                table_name_lower = tbl.lower()
                is_backup = any(pattern in table_name_lower for pattern in backup_patterns)
                if is_backup:
                    excluded_backup.append(tbl)
                else:
                    filtered_list.append(tbl)
            
            if excluded_backup:
                logger_callback(f"Excluding {len(excluded_backup)} backup table(s):")
                for tbl in excluded_backup[:5]:
                    logger_callback(f"  - {tbl}")
                if len(excluded_backup) > 5:
                    logger_callback(f"  ... and {len(excluded_backup) - 5} more")
            
            table_list = filtered_list
        
        # Check if tables exist in destination (if option enabled)
        check_exists = self.bcp_check_table_exists_var.get() if hasattr(self, 'bcp_check_table_exists_var') else True
        
        if check_exists and table_list:
            logger_callback("Checking which tables exist in destination...")
            import logging
            
            log_handler = logging.StreamHandler()
            log_handler.setFormatter(logging.Formatter('%(message)s'))
            temp_logger = logging.getLogger('bcp_check_tables')
            if not temp_logger.handlers:
                temp_logger.addHandler(log_handler)
            temp_logger.setLevel(logging.INFO)
            
            try:
                check_conn = self._connect_migration_db(md, cfg, "dest", logger=temp_logger)
                check_cur = check_conn.cursor()
                
                # Get all tables in destination
                check_cur.execute("""
                    SELECT s.name + '.' + t.name 
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                """)
                dest_tables = set(row[0] for row in check_cur.fetchall())
                check_conn.close()
                
                # Filter to only tables that exist in destination
                existing_tables = []
                missing_tables = []
                for tbl in table_list:
                    if tbl in dest_tables:
                        existing_tables.append(tbl)
                    else:
                        missing_tables.append(tbl)
                
                if missing_tables:
                    logger_callback(f"[WARN] Skipping {len(missing_tables)} table(s) not in destination:")
                    for tbl in missing_tables[:5]:
                        logger_callback(f"  - {tbl}")
                    if len(missing_tables) > 5:
                        logger_callback(f"  ... and {len(missing_tables) - 5} more")
                
                table_list = existing_tables
            except Exception as e:
                logger_callback(f"[WARN] Could not check destination tables: {e}")
                logger_callback("  Proceeding with all tables...")
        
        total_tables = len(table_list)
        logger_callback(f"Migrating {total_tables} table(s) using BCP...")
        if total_tables == 0:
            logger_callback("[WARN] No tables to migrate after filters. Check schema on destination.")
            return {"status": "success", "tables_ok": 0, "tables_failed": 0}

        self._set_bcp_progress_ui(0, max(total_tables, 1), f"Starting BCP — {total_tables} table(s)")
        
        # Acquire Azure AD access tokens if using Entra MFA authentication
        # This allows us to authenticate once and reuse the token for all BCP calls
        src_access_token = None
        dest_access_token = None
        
        src_auth = (cfg.get('src_auth') or '').strip().lower()
        dest_auth = (cfg.get('dest_auth') or '').strip().lower()
        
        if not use_sql_login:  # Only acquire tokens if NOT using SQL login override
            if src_auth == 'entra_mfa':
                logger_callback("Acquiring Azure AD token for source (MFA prompt will appear once)...")
                src_access_token = self._acquire_azure_ad_token(
                    cfg.get('src_user'), 
                    logger=logger_callback
                )
                if not src_access_token:
                    raise RuntimeError("Failed to acquire Azure AD token for source. Please check your credentials.")
            
            if dest_auth == "entra_mfa" and not auto_dest_sql_login:
                if src_auth == "entra_mfa" and cfg.get("src_user") == cfg.get("dest_user") and src_access_token:
                    logger_callback("Reusing Azure AD token for destination (same user)...")
                    dest_access_token = src_access_token
                else:
                    logger_callback("Acquiring Azure AD token for destination (MFA prompt will appear once)...")
                    dest_access_token = self._acquire_azure_ad_token(
                        cfg.get("dest_user"),
                        logger=logger_callback,
                    )
                    if not dest_access_token:
                        raise RuntimeError(
                            "Failed to acquire Azure AD token for destination. "
                            "Check credentials or enable 'Use SQL Login' for BCP."
                        )
        
        try:
            from src.utils.bcp_tools import (
                bcp_host_data_file_error_hint,
                bcp_query_for_export,
                bcp_table_name_for_import,
                create_bcp_migration_dir,
                ensure_bcp_output_file,
                format_bcp_data_path,
            )
        except ImportError:
            from azure_migration_tool.src.utils.bcp_tools import (
                bcp_host_data_file_error_hint,
                bcp_query_for_export,
                bcp_table_name_for_import,
                create_bcp_migration_dir,
                ensure_bcp_output_file,
                format_bcp_data_path,
            )

        preferred_work = (cfg.get("bcp_work_dir") or "").strip() or None
        try:
            from src.migration.bcp_preflight import estimate_bcp_disk_space, log_bcp_disk_estimate
            from src.utils.bcp_tools import resolve_bcp_work_root
        except ImportError:
            from azure_migration_tool.src.migration.bcp_preflight import (
                estimate_bcp_disk_space,
                log_bcp_disk_estimate,
            )
            from azure_migration_tool.src.utils.bcp_tools import resolve_bcp_work_root

        work_root = resolve_bcp_work_root(preferred_work)
        try:
            disk_est = estimate_bcp_disk_space(
                cfg,
                table_list,
                work_root=work_root,
                bcp_exe=bcp_exe,
                log=logger_callback,
                calibrate_with_bcp=True,
            )
            log_bcp_disk_estimate(disk_est, logger_callback)
            if not disk_est.sufficient:
                logger_callback(
                    "[WARN] Work disk may be too small for the largest table. "
                    "Change BCP work folder or free space before continuing."
                )
        except Exception as est_exc:
            logger_callback(f"[WARN] Could not estimate disk space: {est_exc}")

        temp_dir = create_bcp_migration_dir(preferred_work)
        logger_callback(f"Using BCP work directory: {temp_dir}")
        logger_callback(
            "One .dat per table; each file is removed after a successful import."
        )
        
        success_count = 0
        error_count = 0
        skipped_count = 0  # Initialize for resume logic
        
        # Disable constraints/indexes/triggers if requested
        fk_disabled = False
        indexes_disabled = False
        triggers_disabled = False
        dest_conn_for_constraints = None
        dest_conn_for_resume = None  # Initialize for resume logic
        
        try:
            # Connect to destination for constraint management
            if cfg.get('disable_fk') or cfg.get('disable_indexes') or cfg.get('disable_triggers'):
                import logging
                
                log_handler = logging.StreamHandler()
                log_handler.setFormatter(logging.Formatter('%(message)s'))
                temp_logger = logging.getLogger('bcp_migration_constraints')
                if not temp_logger.handlers:
                    temp_logger.addHandler(log_handler)
                temp_logger.setLevel(logging.INFO)
                
                dest_conn_for_constraints = self._connect_migration_db(
                    md, cfg, "dest", logger=temp_logger
                )
                dest_cur = dest_conn_for_constraints.cursor()
                
                if cfg.get('disable_fk'):
                    logger_callback("Disabling foreign key constraints...")
                    md.disable_all_foreign_keys(dest_cur, temp_logger)
                    dest_conn_for_constraints.commit()
                    fk_disabled = True
                    logger_callback("[OK] Foreign key constraints disabled")
                
                if cfg.get('disable_indexes'):
                    logger_callback("Disabling non-clustered indexes...")
                    md.disable_nonclustered_indexes(dest_cur, temp_logger)
                    dest_conn_for_constraints.commit()
                    indexes_disabled = True
                    logger_callback("[OK] Non-clustered indexes disabled")
                
                if cfg.get('disable_triggers'):
                    logger_callback("Disabling triggers...")
                    md.disable_all_triggers(dest_cur, temp_logger)
                    dest_conn_for_constraints.commit()
                    triggers_disabled = True
                    logger_callback("[OK] Triggers disabled")
        
            # Create a connection to destination for row count checks (resume logic)
            resume_enabled = cfg.get('resume_enabled', True)  # Default to resume enabled
            skip_completed = cfg.get('skip_completed', True)  # Default to skip completed
            
            if resume_enabled or skip_completed:
                import logging
                log_handler = logging.StreamHandler()
                log_handler.setFormatter(logging.Formatter('%(message)s'))
                temp_logger = logging.getLogger('bcp_resume')
                if not temp_logger.handlers:
                    temp_logger.addHandler(log_handler)
                temp_logger.setLevel(logging.INFO)
                dest_conn_for_resume = self._connect_migration_db(
                    md, cfg, "dest", logger=temp_logger
                )
            
            for table_idx, table_fqn in enumerate(table_list, start=1):
                schema, table_name = table_fqn.split('.')
                data_file = format_bcp_data_path(
                    os.path.join(temp_dir, f"{schema}_{table_name}.dat")
                )
                self._set_bcp_progress_ui(
                    table_idx - 1,
                    total_tables,
                    f"Table {table_idx}/{total_tables}: {table_fqn}",
                )
                logger_callback(f"\n--- Table {table_idx}/{total_tables}: {table_fqn} ---")
                
                try:
                    # Resume/Skip logic: Check if table is already migrated
                    if (resume_enabled or skip_completed) and dest_conn_for_resume:
                        src_row_count = source_row_counts.get(table_fqn, 0)
                        
                        # Get destination row count
                        dest_cur = dest_conn_for_resume.cursor()
                        dest_cur.execute(f"""
                            SELECT SUM(p.rows) FROM sys.partitions p 
                            JOIN sys.tables t ON p.object_id = t.object_id
                            JOIN sys.schemas s ON t.schema_id = s.schema_id
                            WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                        """, (schema, table_name))
                        row = dest_cur.fetchone()
                        dest_row_count = row[0] or 0 if row else 0
                        
                        # If row counts match, skip this table
                        if src_row_count > 0 and src_row_count == dest_row_count:
                            logger_callback(f"[SKIP] Skipping {table_fqn} (already migrated: {src_row_count:,} rows match)")
                            skipped_count += 1
                            success_count += 1
                            continue
                        elif dest_row_count > 0 and dest_row_count != src_row_count:
                            # Partial migration - truncate and reload
                            logger_callback(f"[WARN] {table_fqn}: Row count mismatch (src: {src_row_count:,}, dest: {dest_row_count:,}) - will truncate and reload")
                    
                    # Truncate destination if requested or if row count mismatch
                    if self.truncate_dest_var.get():
                        logger_callback(f"Truncating {table_fqn} in destination...")
                        if dest_conn_for_resume:
                            dest_cur = dest_conn_for_resume.cursor()
                            dest_cur.execute(f"TRUNCATE TABLE [{schema}].[{table_name}]")
                            dest_conn_for_resume.commit()
                        else:
                            # Need to create a connection for truncate
                            import logging
                            
                            log_handler = logging.StreamHandler()
                            log_handler.setFormatter(logging.Formatter('%(message)s'))
                            temp_logger = logging.getLogger('bcp_migration')
                            temp_logger.addHandler(log_handler)
                            temp_logger.setLevel(logging.INFO)
                            
                            temp_dest_conn = self._connect_migration_db(
                                md, cfg, "dest", logger=temp_logger
                            )
                            try:
                                dest_cur = temp_dest_conn.cursor()
                                dest_cur.execute(f"TRUNCATE TABLE [{schema}].[{table_name}]")
                                temp_dest_conn.commit()
                            finally:
                                temp_dest_conn.close()
                    
                    # Export from source
                    logger_callback(f"Exporting {table_fqn} from source...")
                    
                    # Build authentication arguments
                    src_auth_type = cfg.get('src_auth', '').lower()
                    if use_sql_login and bcp_login_name and bcp_password:
                        # Use SQL login
                        logger_callback(f"  Auth: SQL login ({bcp_login_name})")
                        src_auth_args = ["-S", cfg['src_server'], "-d", cfg['src_db'], 
                                       "-U", bcp_login_name, "-P", bcp_password]
                    elif src_auth_type == 'sql':
                        # Direct SQL auth
                        logger_callback(f"  Auth: SQL ({cfg.get('src_user', 'N/A')})")
                        src_auth_args = self._build_bcp_auth_args(
                            cfg['src_server'], cfg['src_db'],
                            cfg['src_auth'], cfg['src_user'], cfg.get('src_password'),
                            access_token=None
                        )
                    elif src_auth_type == 'entra_mfa' and src_access_token:
                        # MFA with cached token
                        logger_callback(f"  Auth: Entra MFA with cached token")
                        src_auth_args = self._build_bcp_auth_args(
                            cfg['src_server'], cfg['src_db'],
                            cfg['src_auth'], cfg['src_user'], cfg.get('src_password'),
                            access_token=src_access_token
                        )
                    else:
                        # Native auth
                        logger_callback(f"  Auth: {src_auth_type} (native)")
                        src_auth_args = self._build_bcp_auth_args(
                            cfg['src_server'], cfg['src_db'],
                            cfg['src_auth'], cfg['src_user'], cfg.get('src_password'),
                            access_token=None
                        )
                    
                    export_cmd = [
                        bcp_exe,
                        bcp_query_for_export(schema, table_name),
                        "queryout",
                        data_file
                    ] + src_auth_args + [
                        "-n",  # Native format (fastest)
                        "-q"   # Quoted identifiers
                    ]
                    
                    # Log the command (without password/token for security)
                    safe_cmd = [x if x not in [bcp_password, src_access_token] and not (src_access_token and x == src_access_token) else "***" for x in export_cmd]
                    logger_callback(f"  Command: {' '.join(safe_cmd[:8])}...")
                    logger_callback(f"  Data file: {data_file}")

                    try:
                        ensure_bcp_output_file(data_file)
                    except OSError as file_exc:
                        logger_callback(f"[X] BCP export failed for {table_fqn}:")
                        logger_callback(f"  Cannot create data file: {file_exc}")
                        logger_callback(f"  {bcp_host_data_file_error_hint()}")
                        error_count += 1
                        if not self.continue_on_error_var.get():
                            raise RuntimeError(f"Cannot create BCP data file: {file_exc}")
                        continue

                    result = self._run_bcp_subprocess(
                        export_cmd,
                        logger_callback,
                        f"Export {table_fqn}",
                        watch_path=data_file,
                        work_dir=temp_dir,
                    )
                    if result.returncode != 0:
                        error_msg = result.stderr or result.stdout
                        
                        # Check for token-related errors
                        if "Invalid value specified for connection string attribute 'PWD'" in error_msg:
                            logger_callback(f"[X] BCP export failed for {table_fqn}:")
                            logger_callback(f"  Error: Access token not supported by ODBC driver")
                            logger_callback(f"  Solution: Use SQL authentication or upgrade to ODBC Driver 18")
                        elif "Unable to open BCP host data-file" in (error_msg or ""):
                            logger_callback(f"[X] BCP export failed for {table_fqn}:")
                            logger_callback(f"  {error_msg.strip()}")
                            logger_callback(f"  {bcp_host_data_file_error_hint()}")
                        else:
                            logger_callback(f"[X] BCP export failed for {table_fqn}:")
                            logger_callback(f"  {error_msg.strip()}")
                        
                        error_count += 1
                        if not self.continue_on_error_var.get():
                            raise RuntimeError(f"BCP export failed: {error_msg}")
                        continue
                    
                    # Check if file was created and has data
                    if not os.path.exists(data_file) or os.path.getsize(data_file) == 0:
                        logger_callback(f"[WARN] No data to migrate for {table_fqn}")
                        try:
                            if os.path.isfile(data_file):
                                os.remove(data_file)
                        except OSError:
                            pass
                        continue
                    
                    # Import to destination
                    logger_callback(f"Importing {table_fqn} to destination...")
                    
                    # Build authentication arguments
                    dest_auth_type = cfg.get('dest_auth', '').lower()
                    dest_sql_user = bcp_dest_login_name if auto_dest_sql_login else bcp_login_name
                    dest_sql_pass = bcp_dest_password if auto_dest_sql_login else bcp_password
                    if (use_sql_login or auto_dest_sql_login) and dest_sql_user and dest_sql_pass:
                        logger_callback(f"  Auth: SQL login ({dest_sql_user})")
                        dest_auth_args = [
                            "-S",
                            cfg["dest_server"],
                            "-d",
                            cfg["dest_db"],
                            "-U",
                            dest_sql_user,
                            "-P",
                            dest_sql_pass,
                        ]
                    elif dest_auth_type == 'sql':
                        # Direct SQL auth
                        logger_callback(f"  Auth: SQL ({cfg.get('dest_user', 'N/A')})")
                        dest_auth_args = self._build_bcp_auth_args(
                            cfg['dest_server'], cfg['dest_db'],
                            cfg['dest_auth'], cfg['dest_user'], cfg.get('dest_password'),
                            access_token=None
                        )
                    elif dest_auth_type == "entra_mfa" and dest_access_token:
                        logger_callback("  Auth: Entra MFA (interactive via bcp -G)")
                        dest_auth_args = self._build_bcp_auth_args(
                            cfg["dest_server"],
                            cfg["dest_db"],
                            cfg["dest_auth"],
                            cfg["dest_user"],
                            cfg.get("dest_password"),
                            access_token=dest_access_token,
                        )
                    else:
                        # Native auth (will prompt for MFA each time if needed)
                        logger_callback(f"  Auth: {dest_auth_type} (native)")
                        dest_auth_args = self._build_bcp_auth_args(
                            cfg['dest_server'], cfg['dest_db'],
                            cfg['dest_auth'], cfg['dest_user'], cfg.get('dest_password'),
                            access_token=None
                        )
                    
                    import_cmd = [
                        bcp_exe,
                        bcp_table_name_for_import(schema, table_name),
                        "in",
                        data_file
                    ] + dest_auth_args + [
                        "-n",  # Native format
                        "-q",  # Quoted identifiers
                        "-b", "10000"  # Batch size
                    ]
                    
                    # Log the command (without password/token for security)
                    redact = {bcp_password, bcp_dest_password, dest_access_token} - {None}
                    safe_cmd = [x if x not in redact else "***" for x in import_cmd]
                    logger_callback(f"  Command: {' '.join(safe_cmd[:8])}...")
                    
                    result = self._run_bcp_subprocess(
                        import_cmd,
                        logger_callback,
                        f"Import {table_fqn}",
                        work_dir=temp_dir,
                    )
                    if result.returncode != 0:
                        error_msg = result.stderr or result.stdout
                        
                        # Check for specific error types and provide helpful messages
                        if "Invalid value specified for connection string attribute 'PWD'" in error_msg:
                            logger_callback(f"[X] BCP import failed for {table_fqn}:")
                            logger_callback(
                                "  Error: Entra access tokens cannot be passed to bcp.exe on Windows."
                            )
                            logger_callback(
                                "  Solution: Enable 'Use SQL Login' in BCP Options, or restart migration "
                                "(auto SQL login on destination is applied when Entra MFA is selected)."
                            )
                        elif "Invalid object name" in error_msg or "S0002" in error_msg:
                            logger_callback(f"[X] BCP import failed for {table_fqn}:")
                            logger_callback(
                                f"  Error: Table {bcp_table_name_for_import(schema, table_name)!r} "
                                f"not found in {cfg.get('dest_db', 'destination')}."
                            )
                            logger_callback(
                                "  Solution: Run Schema migration on the destination first, "
                                "or disable 'Skip tables not in destination' and verify the table exists."
                            )
                        elif "permission" in error_msg.lower() or "denied" in error_msg.lower():
                            logger_callback(f"[X] BCP import failed for {table_fqn}:")
                            logger_callback(f"  Error: Permission denied on destination table")
                            logger_callback(f"  Solution: Grant INSERT permission to the user")
                        elif "foreign key" in error_msg.lower() or "constraint" in error_msg.lower():
                            logger_callback(f"[X] BCP import failed for {table_fqn}:")
                            logger_callback(f"  Error: Foreign key or constraint violation")
                            logger_callback(f"  Solution: Enable 'Disable Foreign Keys' option")
                        else:
                            logger_callback(f"[X] BCP import failed for {table_fqn}:")
                            # Show first 3 lines of error for clarity
                            error_lines = error_msg.strip().split('\n')[:3]
                            for line in error_lines:
                                if line.strip():
                                    logger_callback(f"  {line.strip()}")
                        
                        error_count += 1
                        if not self.continue_on_error_var.get():
                            raise RuntimeError(f"BCP import failed: {error_msg}")
                        continue
                    
                    # Parse BCP output for row count
                    output = result.stdout
                    rows_imported = 0
                    for line in output.split('\n'):
                        line_lower = line.lower()
                        if 'rows copied' in line_lower or 'rows affected' in line_lower or 'copied' in line_lower:
                            # Try to extract number from lines like "1000 rows copied" or "1000 rows affected"
                            parts = line.split()
                            for i, part in enumerate(parts):
                                try:
                                    if part.isdigit() and i + 1 < len(parts):
                                        rows_imported = int(part)
                                        break
                                except:
                                    pass
                    
                    if rows_imported > 0:
                        logger_callback(f"[OK] Migrated {table_fqn} ({rows_imported:,} rows)")
                    else:
                        logger_callback(f"[OK] Migrated {table_fqn}")
                    success_count += 1
                    try:
                        freed = os.path.getsize(data_file)
                        os.remove(data_file)
                        try:
                            from src.utils.bcp_tools import disk_free_gb, format_size_mb
                        except ImportError:
                            from azure_migration_tool.src.utils.bcp_tools import (
                                disk_free_gb,
                                format_size_mb,
                            )
                        logger_callback(
                            f"  Removed .dat ({format_size_mb(freed)}); "
                            f"{disk_free_gb(temp_dir):.1f} GB free on work disk"
                        )
                    except OSError:
                        pass
                
                except Exception as e:
                    logger_callback(f"[X] Error migrating {table_fqn}: {str(e)}")
                    error_count += 1
                    if not self.continue_on_error_var.get():
                        raise
        
        finally:
            # Close resume connection if open
            try:
                if dest_conn_for_resume:
                    dest_conn_for_resume.close()
            except:
                pass
            
            # Log summary with skipped count
            if skipped_count > 0:
                logger_callback(f"")
                logger_callback(f"Resume Summary: {skipped_count} table(s) skipped (already migrated)")
            
            # Re-enable constraints/indexes/triggers
            try:
                if dest_conn_for_constraints:
                    import logging
                    
                    temp_logger = logging.getLogger('bcp_migration_constraints')
                    dest_cur = dest_conn_for_constraints.cursor()
                    
                    if triggers_disabled:
                        logger_callback("Re-enabling triggers...")
                        md.enable_all_triggers(dest_cur, temp_logger)
                        dest_conn_for_constraints.commit()
                        logger_callback("[OK] Triggers re-enabled")
                    
                    if indexes_disabled:
                        logger_callback("Rebuilding non-clustered indexes (this may take a while)...")
                        # Note: For BCP we don't track which indexes were disabled, so we rebuild all
                        dest_cur.execute("""
                            DECLARE @sql NVARCHAR(MAX) = N'';
                            SELECT @sql = @sql + N'ALTER INDEX ' + QUOTENAME(i.name) + 
                                   ' ON ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ' REBUILD;' + CHAR(13)
                            FROM sys.indexes i
                            JOIN sys.tables t ON i.object_id = t.object_id
                            JOIN sys.schemas s ON t.schema_id = s.schema_id
                            WHERE i.type_desc = 'NONCLUSTERED'
                              AND i.is_disabled = 1
                              AND i.name IS NOT NULL;
                            EXEC sp_executesql @sql;
                        """)
                        dest_conn_for_constraints.commit()
                        logger_callback("[OK] Non-clustered indexes rebuilt")
                    
                    if fk_disabled:
                        logger_callback("Re-enabling foreign key constraints...")
                        md.enable_all_foreign_keys(dest_cur, temp_logger)
                        dest_conn_for_constraints.commit()
                        logger_callback("[OK] Foreign key constraints re-enabled")
                    
                    dest_conn_for_constraints.close()
            except Exception as cleanup_ex:
                logger_callback(f"[WARN] Warning during constraint cleanup: {cleanup_ex}")
            
            # Clean up temp files
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger_callback(f"Cleaned up temporary directory")
            except:
                pass
            
            cleanup_dest_login = (
                auto_dest_sql_login and bcp_dest_login_name
            ) or (self.bcp_cleanup_var.get() and use_sql_login and bcp_login_name)
            cleanup_src_login = self.bcp_cleanup_var.get() and use_sql_login and bcp_login_name
            if cleanup_dest_login or cleanup_src_login:
                dest_login = bcp_dest_login_name if auto_dest_sql_login else bcp_login_name
                logger_callback(f"Cleaning up SQL login '{dest_login}'...")
                import logging

                log_handler = logging.StreamHandler()
                log_handler.setFormatter(logging.Formatter("%(message)s"))
                temp_logger = logging.getLogger("bcp_migration")
                if not temp_logger.handlers:
                    temp_logger.addHandler(log_handler)
                temp_logger.setLevel(logging.INFO)

                dest_conn = self._connect_migration_db(md, cfg, "dest", logger=temp_logger)
                try:
                    self._cleanup_sql_login(
                        dest_conn,
                        cfg["dest_db"],
                        dest_login,
                        lambda msg: logger_callback(f"  {msg}"),
                    )
                finally:
                    dest_conn.close()
                if cleanup_src_login:
                    src_conn = self._connect_migration_db(md, cfg, "src", logger=temp_logger)
                    try:
                        self._cleanup_sql_login(
                            src_conn,
                            cfg["src_db"],
                            bcp_login_name,
                            lambda msg: logger_callback(f"  {msg}"),
                        )
                    finally:
                        src_conn.close()
            
            logger_callback(f"\n{'='*60}")
            logger_callback(f"BCP Migration Complete: {success_count} succeeded, {error_count} failed")
            
            return {
                "status": "success" if error_count == 0 else "completed_with_errors",
                "tables_ok": success_count,
                "tables_failed": error_count,
                "total_tables": len(table_list)
            }
    
    def _migrate_with_adf(self, cfg, logger_callback):
        """Migrate data using Azure Data Factory."""
        if not ADF_AVAILABLE or ADFClient is None:
            raise RuntimeError(
                "Azure Data Factory SDK not available.\n\n"
                "Install with: pip install azure-identity azure-mgmt-datafactory"
            )
        
        # Get ADF configuration from UI
        factory_name = self.adf_factory_name_var.get().strip()
        pipeline_name = self.adf_pipeline_name_var.get().strip()
        resource_group = self.adf_resource_group_var.get().strip()
        subscription_id = self.adf_subscription_id_var.get().strip() or None
        
        # Get parameter names
        table_list_param = self.adf_table_list_param_var.get().strip() or "TableList"
        truncate_param = self.adf_truncate_param_var.get().strip() or "TruncateDestination"
        src_server_param = self.adf_src_server_param_var.get().strip() or "SourceServer"
        src_db_param = self.adf_src_db_param_var.get().strip() or "SourceDatabase"
        dest_server_param = self.adf_dest_server_param_var.get().strip() or "DestServer"
        dest_db_param = self.adf_dest_db_param_var.get().strip() or "DestDatabase"
        
        logger_callback(f"Connecting to Azure Data Factory...")
        logger_callback(f"  Factory: {factory_name}")
        logger_callback(f"  Pipeline: {pipeline_name}")
        logger_callback(f"  Resource Group: {resource_group}")
        if subscription_id:
            logger_callback(f"  Subscription: {subscription_id}")
        
        try:
            # Initialize ADF client
            adf_client = ADFClient(
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=subscription_id,
                credentials=None,  # Will use DefaultAzureCredential/InteractiveBrowserCredential
                logger=logger_callback
            )
            
            # Validate pipeline exists
            logger_callback("Validating pipeline exists...")
            if not adf_client.pipeline_exists(pipeline_name):
                raise ValueError(f"Pipeline '{pipeline_name}' not found in factory '{factory_name}'")
            logger_callback(f"[OK] Pipeline '{pipeline_name}' found")
            
            # Get table list
            table_list = []
            if cfg.get("tables"):
                # Single table specified
                table_list = [cfg["tables"].strip()]
                logger_callback(f"Migrating single table: {table_list[0]}")
            else:
                # Get all tables from source
                logger_callback("Getting table list from source database...")
                try:
                    import pyodbc
                    md_adf = _data_migration_module()
                    import logging
                    
                    log_handler = logging.StreamHandler()
                    log_handler.setFormatter(logging.Formatter('%(message)s'))
                    temp_logger = logging.getLogger('adf_migration')
                    temp_logger.addHandler(log_handler)
                    temp_logger.setLevel(logging.INFO)
                    
                    src_conn = self._connect_migration_db(md_adf, cfg, "src", logger=temp_logger)
                    src_cur = src_conn.cursor()
                    
                    tables = md_adf.fetch_tables(src_cur)
                    table_list = [f"{s}.{t}" for s, t in tables]
                    
                    src_conn.close()
                    logger_callback(f"Found {len(table_list)} table(s) to migrate")
                except Exception as e:
                    logger_callback(f"[WARN] Could not get table list: {e}")
                    logger_callback("  Using '*' to migrate all tables")
                    table_list = ["*"]  # Use wildcard for all tables
            
            # Build pipeline parameters
            parameters = {
                src_server_param: cfg['src_server'],
                src_db_param: cfg['src_db'],
                dest_server_param: cfg['dest_server'],
                dest_db_param: cfg['dest_db'],
                table_list_param: ",".join(table_list) if table_list != ["*"] else "*",
                truncate_param: cfg.get('truncate_dest', False)
            }
            
            logger_callback(f"\nPipeline Parameters:")
            for key, value in parameters.items():
                if key == truncate_param:
                    logger_callback(f"  {key}: {value}")
                elif key == table_list_param:
                    if table_list == ["*"]:
                        logger_callback(f"  {key}: * (all tables)")
                    else:
                        logger_callback(f"  {key}: {len(table_list)} table(s)")
                else:
                    logger_callback(f"  {key}: {value}")
            
            # Trigger pipeline
            logger_callback(f"\nTriggering pipeline '{pipeline_name}'...")
            run_id = adf_client.trigger_pipeline(pipeline_name, parameters)
            logger_callback(f"[OK] Pipeline triggered. Run ID: {run_id}")
            logger_callback(f"  Monitor at: https://adf.azure.com/monitoring/pipelineruns/{run_id}")
            
            # Monitor pipeline execution
            logger_callback(f"\nMonitoring pipeline execution...")
            logger_callback(f"  (Polling every 10 seconds)")
            
            def status_callback(status_info):
                """Callback for status updates."""
                status = status_info['status']
                elapsed = ""
                if status_info.get('run_start'):
                    from datetime import datetime, timezone
                    if isinstance(status_info['run_start'], str):
                        run_start = datetime.fromisoformat(status_info['run_start'].replace('Z', '+00:00'))
                    else:
                        run_start = status_info['run_start']
                    if run_start.tzinfo is None:
                        run_start = run_start.replace(tzinfo=timezone.utc)
                    elapsed_seconds = int((datetime.now(timezone.utc) - run_start).total_seconds())
                    elapsed = f" (elapsed: {elapsed_seconds}s)"
                
                logger_callback(f"  Status: {status}{elapsed}")
                
                if status == 'Failed' and status_info.get('message'):
                    logger_callback(f"  Error: {status_info['message']}")
            
            # Wait for completion (no timeout for large migrations)
            try:
                final_status = adf_client.wait_for_completion(
                    run_id,
                    timeout=None,  # No timeout - let it run as long as needed
                    callback=status_callback,
                    poll_interval=10
                )
                
                # Final status
                status = final_status['status']
                duration_ms = final_status.get('duration_ms', 0)
                duration_sec = duration_ms / 1000 if duration_ms else 0
                
                logger_callback(f"\n{'='*60}")
                if status == 'Succeeded':
                    logger_callback(f"[OK] Pipeline completed successfully!")
                    logger_callback(f"  Duration: {duration_sec:.1f} seconds")
                    return {
                        "status": "success",
                        "run_id": run_id,
                        "duration_seconds": duration_sec
                    }
                elif status == 'Failed':
                    error_msg = final_status.get('message', 'Unknown error')
                    logger_callback(f"[X] Pipeline failed!")
                    logger_callback(f"  Error: {error_msg}")
                    return {
                        "status": "failed",
                        "run_id": run_id,
                        "error": error_msg,
                        "duration_seconds": duration_sec
                    }
                else:
                    logger_callback(f"[WARN] Pipeline status: {status}")
                    return {
                        "status": "unknown",
                        "run_id": run_id,
                        "error": f"Pipeline ended with status: {status}",
                        "duration_seconds": duration_sec
                    }
                    
            except TimeoutError as e:
                logger_callback(f"\n[WARN] Timeout waiting for pipeline completion")
                logger_callback(f"  Run ID: {run_id}")
                logger_callback(f"  Check status in Azure Portal")
                return {
                    "status": "timeout",
                    "run_id": run_id,
                    "error": str(e)
                }
            except Exception as e:
                logger_callback(f"\n[X] Error monitoring pipeline: {e}")
                return {
                    "status": "error",
                    "run_id": run_id,
                    "error": str(e)
                }
                
        except Exception as e:
            error_msg = str(e)
            logger_callback(f"\n[X] Error: {error_msg}")
            return {
                "status": "failed",
                "error": error_msg
            }
        
    def _ensure_bcp_ready_for_migration(self) -> bool:
        """Ensure BCP module, pre-flight, and bcp.exe are ready. Returns False to abort."""
        if not migrate_data:
            messagebox.showerror("Error", "Data migration module not available!")
            return False
        if not getattr(self, "_bcp_preflight_passed", False):
            self._bcp_preflight_then_migrate = True
            self._run_bcp_preflight_validation()
            return False
        bcp_exe = self._find_bcp_exe()
        if bcp_exe:
            return True
        try:
            from src.utils.bcp_tools import ensure_bcp_installed
        except ImportError:
            from azure_migration_tool.src.utils.bcp_tools import ensure_bcp_installed

        ok, msg = ensure_bcp_installed(
            lambda m: self.migration_log.insert(tk.END, m + "\n"),
            allow_download=True,
        )
        if ok:
            return True
        if self._handle_bcp_not_found():
            return bool(self._find_bcp_exe())
        messagebox.showerror(
            "Error",
            "BCP utility is still not found.\n\n"
            f"{msg}\n\n"
            "Use the installer with BCP tools or install SQL Command Line Utilities.",
        )
        return False

    def _start_migration(self):
        """Start data migration in a separate thread."""
        migration_method = self.migration_method_var.get()

        src_db_type = (
            getattr(getattr(self, "src_connection_widget", None), "db_type_var", None)
            and self.src_connection_widget.db_type_var.get()
            or "sqlserver"
        ).strip().lower()
        if migration_method == "bcp" and src_db_type == "db2":
            messagebox.showerror(
                "BCP migration",
                "BCP requires a SQL Server / Azure SQL source.\n\n"
                "Source is set to IBM DB2. Switch Database Type to SQL Server and use a SQL "
                "database that has the table (for example gpitd-shir01...\\i2022 / "
                "FUSION_ps_fus_db21q), or use another migration method for DB2.",
            )
            return
        
        if migration_method == "bcp":
            if not self._ensure_bcp_ready_for_migration():
                return
        elif migration_method == "adf":
            # Check if ADF client is available
            if not ADF_AVAILABLE or ADFClient is None:
                messagebox.showerror("Error", 
                    "Azure Data Factory SDK not available.\n\n"
                    "Install with: pip install azure-identity azure-mgmt-datafactory")
                return
            # Validate ADF configuration
            if not self.adf_factory_name_var.get():
                messagebox.showerror("Error", "ADF Factory Name is required!")
                return
            if not self.adf_pipeline_name_var.get():
                messagebox.showerror("Error", "ADF Pipeline Name is required!")
                return
            if not self.adf_resource_group_var.get():
                messagebox.showerror("Error", "Resource Group is required!")
                return
        else:
            if not migrate_data:
                messagebox.showerror("Error", "Data migration module not available!")
                return
            
        # Validate inputs
        if not self.src_server_var.get():
            messagebox.showerror("Error", "Source server is required!")
            return
        if not self.src_db_var.get():
            messagebox.showerror("Error", "Source database is required!")
            return
        if not self.dest_server_var.get():
            messagebox.showerror("Error", "Destination server is required!")
            return
        if not self.dest_db_var.get():
            messagebox.showerror("Error", "Destination database is required!")
            return
            
        self.migrate_btn.config(state=tk.DISABLED)
        self.progress_bar.start()
        self.progress_var.set("Migration in progress...")
        self.migration_log.delete("1.0", tk.END)
        self.migration_log.insert(tk.END, "Starting data migration...\n")
        
        def run_migration():
            try:
                cfg = self._build_migration_cfg()
                bcp_single_table = (cfg.get("tables") or "").strip()
                
                # Check migration method
                migration_method = self.migration_method_var.get()
                
                if migration_method == "bcp":
                    # Use BCP migration
                    if bcp_single_table:
                        self.migration_log.insert(tk.END, f"Using BCP (Bulk Copy Program) for single table: {bcp_single_table}\n")
                    else:
                        self.migration_log.insert(tk.END, f"Using BCP (Bulk Copy Program) for migration...\n")
                    self.migration_log.see(tk.END)
                    
                    def log_callback(msg):
                        self._thread_safe_log(msg)
                    
                    report = self._migrate_with_bcp(cfg, log_callback)
                    
                    if report["status"] == "success":
                        self.migration_log.insert(tk.END, f"\n[OK] BCP Migration completed successfully!\n")
                        self.migration_log.insert(tk.END, f"Tables migrated: {report.get('tables_ok', 0)}\n")
                        messagebox.showinfo("Success", "BCP migration completed successfully!")
                    else:
                        self.migration_log.insert(tk.END, f"\n[X] BCP Migration completed with errors!\n")
                        self.migration_log.insert(tk.END, f"Tables succeeded: {report.get('tables_ok', 0)}\n")
                        self.migration_log.insert(tk.END, f"Tables failed: {report.get('tables_failed', 0)}\n")
                        messagebox.showwarning("Warning", "BCP migration completed with errors! Check log for details.")
                elif migration_method == "adf":
                    # Use ADF migration
                    self.migration_log.insert(tk.END, f"Using Azure Data Factory for migration...\n")
                    self.migration_log.see(tk.END)
                    
                    def log_callback(msg):
                        self.migration_log.insert(tk.END, f"{msg}\n")
                        self.migration_log.see(tk.END)
                        self.frame.update_idletasks()
                    
                    report = self._migrate_with_adf(cfg, log_callback)
                    
                    if report["status"] == "success":
                        self.migration_log.insert(tk.END, f"\n[OK] ADF Migration completed successfully!\n")
                        self.migration_log.insert(tk.END, f"Pipeline Run ID: {report.get('run_id', 'N/A')}\n")
                        messagebox.showinfo("Success", "ADF migration completed successfully!")
                    else:
                        self.migration_log.insert(tk.END, f"\n[X] ADF Migration completed with errors!\n")
                        self.migration_log.insert(tk.END, f"Error: {report.get('error', 'Unknown error')}\n")
                        messagebox.showwarning("Warning", "ADF migration completed with errors! Check log for details.")
                else:
                    # Use standard migration with real-time log streaming
                    def stream_log(msg):
                        """Stream log messages to the GUI in real-time."""
                        self.migration_log.insert(tk.END, f"{msg}\n")
                        self.migration_log.see(tk.END)
                        self.frame.update_idletasks()
                    
                    report = migrate_data.run_migration(cfg, log_callback=stream_log)
                    
                    if report["status"] == "success":
                        self.migration_log.insert(tk.END, f"\n{'='*50}\n")
                        self.migration_log.insert(tk.END, f"[OK] Migration completed successfully!\n")
                        self.migration_log.insert(tk.END, f"Tables migrated: {report.get('tables_ok', 0)}\n")
                        self.migration_log.insert(tk.END, f"Tables failed: {report.get('tables_failed', 0)}\n")
                        messagebox.showinfo("Success", "Data migration completed successfully!")
                    else:
                        self.migration_log.insert(tk.END, f"\n{'='*50}\n")
                        self.migration_log.insert(tk.END, f"[X] Migration completed with errors!\n")
                        self.migration_log.insert(tk.END, f"Errors: {report.get('errors', [])}\n")
                        messagebox.showwarning("Warning", "Migration completed with errors! Check log for details.")
                    
            except Exception as e:
                self.migration_log.insert(tk.END, f"\n[X] Error: {str(e)}\n")
                messagebox.showerror("Error", f"Migration failed: {str(e)}")
            finally:
                def _migration_finished_ui() -> None:
                    try:
                        self.progress_bar.stop()
                        self.progress_bar.config(mode="indeterminate")
                    except tk.TclError:
                        pass
                    self.progress_var.set("Ready")
                    self.migrate_btn.config(state=tk.NORMAL)

                self.frame.after(0, _migration_finished_ui)
                
        threading.Thread(target=run_migration, daemon=True).start()
        
    def _download_template(self, template_type: str):
        """Download sample Excel template."""
        try:
            file_path = create_sample_excel(template_type)
            if file_path:
                messagebox.showinfo("Success", f"Template saved to:\n{file_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create template: {str(e)}")
            
    def _upload_excel(self):
        """Upload Excel file for bulk migration."""
        file_path = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )
        if not file_path:
            return
            
        try:
            configs = read_excel_file(
                file_path,
                required_columns=["src_server", "src_db", "dest_server", "dest_db"],
                default_user=self.src_user_var.get() or None
            )
            
            self.excel_configs = configs
            self.excel_file_var.set(f"Loaded {len(configs)} configuration(s) from {Path(file_path).name}")
            self.bulk_migrate_btn.config(state=tk.NORMAL)
            messagebox.showinfo("Success", f"Loaded {len(configs)} configuration(s) from Excel file!")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read Excel file: {str(e)}")
            
    def _start_bulk_migration(self):
        """Start bulk migration from Excel configurations."""
        if not self.excel_configs:
            messagebox.showwarning("Warning", "No configurations loaded! Please upload an Excel file first.")
            return
        
        migration_method = self.migration_method_var.get()
        
        if migration_method == "bcp":
            if not self._ensure_bcp_ready_for_migration():
                return
        elif migration_method == "adf":
            # Check if ADF client is available
            if not ADF_AVAILABLE or ADFClient is None:
                messagebox.showerror("Error", 
                    "Azure Data Factory SDK not available.\n\n"
                    "Install with: pip install azure-identity azure-mgmt-datafactory")
                return
            # Validate ADF configuration
            if not self.adf_factory_name_var.get():
                messagebox.showerror("Error", "ADF Factory Name is required!")
                return
            if not self.adf_pipeline_name_var.get():
                messagebox.showerror("Error", "ADF Pipeline Name is required!")
                return
            if not self.adf_resource_group_var.get():
                messagebox.showerror("Error", "Resource Group is required!")
                return
        else:
            if not migrate_data:
                messagebox.showerror("Error", "Data migration module not available!")
                return
            
        self.bulk_migrate_btn.config(state=tk.DISABLED)
        self.progress_bar.start()
        self.progress_var.set("Bulk migration in progress...")
        self.migration_log.delete("1.0", tk.END)
        self.migration_log.insert(tk.END, f"Starting bulk migration for {len(self.excel_configs)} configuration(s)...\n")
        
        def run_bulk():
            success_count = 0
            fail_count = 0
            
            for idx, cfg in enumerate(self.excel_configs, 1):
                self.migration_log.insert(tk.END, f"\n{'='*60}\n")
                self.migration_log.insert(tk.END, f"[{idx}/{len(self.excel_configs)}] Processing {cfg.get('src_db')} -> {cfg.get('dest_db')}...\n")
                self.migration_log.see(tk.END)
                
                try:
                    migrate_cfg = {
                        "src_server": cfg.get("src_server"),
                        "src_db": cfg.get("src_db"),
                        "src_auth": cfg.get("src_auth", self.src_auth_var.get()),
                        "src_user": cfg.get("src_user", cfg.get("user", self.src_user_var.get())),
                        "src_password": cfg.get("src_password", self.src_password_var.get() or None),
                        "dest_server": cfg.get("dest_server"),
                        "dest_db": cfg.get("dest_db"),
                        "dest_auth": cfg.get("dest_auth", self.dest_auth_var.get()),
                        "dest_user": cfg.get("dest_user", cfg.get("user", self.dest_user_var.get())),
                        "dest_password": cfg.get("dest_password", self.dest_password_var.get() or None),
                        "batch_size": cfg.get("batch_size", int(self.batch_size_var.get()) if self.batch_size_var.get() else 20000),
                        "truncate_dest": cfg.get("truncate_dest", self.truncate_dest_var.get()),
                        "delete_dest": False,
                        "continue_on_error": cfg.get("continue_on_error", self.continue_on_error_var.get()),
                        "dry_run": False,
                        "tables": cfg.get("tables", ""),  # Empty means all tables
                        "exclude": cfg.get("exclude", ""),  # No tables excluded
                        "disable_fk": cfg.get("disable_fk", self.disable_fk_var.get()),
                        "disable_indexes": cfg.get("disable_indexes", self.disable_indexes_var.get()),
                        "disable_triggers": cfg.get("disable_triggers", self.disable_triggers_var.get()),
                        # Performance & Resilience options
                        "parallel_tables": cfg.get("parallel_tables", self.parallel_tables_var.get()),
                        "skip_completed": cfg.get("skip_completed", self.skip_completed_var.get()),
                        "resume_enabled": cfg.get("resume_enabled", self.resume_enabled_var.get()),
                        "max_retries": cfg.get("max_retries", self.max_retries_var.get()),
                        "verify_after_copy": cfg.get("verify_after_copy", self.verify_after_copy_var.get()),
                        # Chunking options (ADF-style)
                        "enable_chunking": cfg.get("enable_chunking", self.enable_chunking_var.get()),
                        "chunk_threshold": cfg.get("chunk_threshold", self.chunk_threshold_var.get()),
                        "num_chunks": cfg.get("num_chunks", self.num_chunks_var.get()),
                        "chunk_workers": cfg.get("chunk_workers", self.chunk_workers_var.get()),
                    }
                    
                    migration_method = self.migration_method_var.get()
                    
                    if migration_method == "bcp":
                        def log_callback(msg):
                            self.migration_log.insert(tk.END, f"  {msg}\n")
                            self.migration_log.see(tk.END)
                        
                        report = self._migrate_with_bcp(migrate_cfg, log_callback)
                        
                        if report["status"] == "success":
                            self.migration_log.insert(tk.END, f"[OK] Success: {cfg.get('src_db')} -> {cfg.get('dest_db')}\n")
                            self.migration_log.insert(tk.END, f"  Tables migrated: {report.get('tables_ok', 0)}\n")
                            success_count += 1
                        else:
                            self.migration_log.insert(tk.END, f"[X] Completed with errors: {cfg.get('src_db')} -> {cfg.get('dest_db')}\n")
                            self.migration_log.insert(tk.END, f"  Tables succeeded: {report.get('tables_ok', 0)}, failed: {report.get('tables_failed', 0)}\n")
                            fail_count += 1
                    elif migration_method == "adf":
                        def log_callback(msg):
                            self.migration_log.insert(tk.END, f"  {msg}\n")
                            self.migration_log.see(tk.END)
                            self.frame.update_idletasks()
                        
                        report = self._migrate_with_adf(migrate_cfg, log_callback)
                        
                        if report["status"] == "success":
                            self.migration_log.insert(tk.END, f"[OK] Success: {cfg.get('src_db')} -> {cfg.get('dest_db')}\n")
                            self.migration_log.insert(tk.END, f"  Pipeline Run ID: {report.get('run_id', 'N/A')}\n")
                            success_count += 1
                        else:
                            self.migration_log.insert(tk.END, f"[X] Failed: {cfg.get('src_db')} -> {cfg.get('dest_db')}\n")
                            self.migration_log.insert(tk.END, f"  Error: {report.get('error', 'Unknown error')}\n")
                            fail_count += 1
                    else:
                        # Use standard migration with log streaming
                        def stream_log(msg):
                            self.migration_log.insert(tk.END, f"  {msg}\n")
                            self.migration_log.see(tk.END)
                            self.frame.update_idletasks()
                        
                        report = migrate_data.run_migration(migrate_cfg, log_callback=stream_log)
                        
                        if report["status"] == "success":
                            self.migration_log.insert(tk.END, f"[OK] Success: {cfg.get('src_db')} -> {cfg.get('dest_db')}\n")
                            self.migration_log.insert(tk.END, f"  Tables migrated: {report.get('tables_ok', 0)}\n")
                            success_count += 1
                        else:
                            self.migration_log.insert(tk.END, f"[X] Failed: {cfg.get('src_db')} -> {cfg.get('dest_db')}\n")
                            self.migration_log.insert(tk.END, f"  Errors: {report.get('errors', [])}\n")
                            fail_count += 1
                except Exception as e:
                    self.migration_log.insert(tk.END, f"[X] Error: {cfg.get('src_db')} -> {cfg.get('dest_db')} - {str(e)}\n")
                    fail_count += 1
                    
            self.progress_bar.stop()
            self.progress_var.set("Ready")
            self.migration_log.insert(tk.END, f"\n{'='*60}\n")
            self.migration_log.insert(tk.END, f"Bulk migration completed: {success_count} succeeded, {fail_count} failed\n")
            messagebox.showinfo("Bulk Migration Complete", f"Completed: {success_count} succeeded, {fail_count} failed")
            self.bulk_migrate_btn.config(state=tk.NORMAL)
            
        threading.Thread(target=run_bulk, daemon=True).start()

