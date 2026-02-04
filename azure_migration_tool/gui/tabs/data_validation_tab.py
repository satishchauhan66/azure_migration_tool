"""
Data Validation Tab - Row Comparison
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from pathlib import Path
import threading
import sys
import pyodbc
import json
from datetime import datetime

# Add parent directories to path
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.utils.excel_utils import read_excel_file, create_sample_excel
from gui.utils.database_utils import connect_with_msal_cache, connect_to_any_database

# Import log console for streaming logs
try:
    from gui.utils.log_console import log_to_console
    LOG_CONSOLE_AVAILABLE = True
except ImportError:
    LOG_CONSOLE_AVAILABLE = False
    log_to_console = None

# Import driver utilities for handling missing driver errors
try:
    from utils.driver_utils import (
        check_sql_server_odbc_driver,
        install_odbc_via_powershell,
        get_manual_install_instructions,
    )
    DRIVER_UTILS_AVAILABLE = True
except ImportError:
    DRIVER_UTILS_AVAILABLE = False

import logging


def is_driver_missing_error(error_msg: str) -> bool:
    """Check if an error is related to missing ODBC driver."""
    error_str = str(error_msg).upper()
    driver_error_indicators = [
        "IM002",  # Data source name not found
        "IM003",  # Driver not found
        "IM004",  # Driver's SQLAllocHandle failed
        "01000",  # Driver not capable
        "DATA SOURCE NAME NOT FOUND",
        "NO DEFAULT DRIVER",
        "DRIVER NOT FOUND",
        "ODBC DRIVER",
    ]
    return any(indicator in error_str for indicator in driver_error_indicators)


class DataValidationTab:
    """Data validation (row comparison) tab."""
    
    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None
        
        self._create_widgets()
        
    def set_project_path(self, project_path):
        """Set the current project path."""
        self.project_path = project_path
    
    def _log(self, message: str, level: int = logging.INFO, context: dict = None):
        """Log message to both the widget and the streaming console with context."""
        # Build context string
        context_str = ""
        if context:
            parts = []
            if 'db' in context:
                parts.append(f"DB:{context['db']}")
            if 'table' in context:
                parts.append(f"Table:{context['table']}")
            if 'config_idx' in context:
                parts.append(f"Config#{context['config_idx']}")
            if 'process' in context:
                parts.append(f"Process:{context['process']}")
            if parts:
                context_str = f"[{', '.join(parts)}] "
        
        full_message = f"{context_str}{message}"
        
        # Log to widget (thread-safe using after())
        def update_widget():
            try:
                if hasattr(self, 'validation_log') and self.validation_log:
                    self.validation_log.insert(tk.END, f"{full_message}\n")
                    self.validation_log.see(tk.END)
            except Exception:
                pass  # Widget might be destroyed
        
        # Schedule widget update on main thread
        try:
            self.frame.after(0, update_widget)
        except Exception:
            pass  # Frame might be destroyed
        
        # Log to streaming console
        if LOG_CONSOLE_AVAILABLE and log_to_console:
            log_to_console(f"[DataValidation] {full_message}", level)
    
    def _update_status(self, status: str, color: str = "black"):
        """Update status label (thread-safe)."""
        def update():
            try:
                self.status_label.config(text=status, fg=color)
            except Exception:
                pass
        try:
            self.frame.after(0, update)
        except Exception:
            pass
    
    def _update_progress(self, current: int, total: int, text: str = ""):
        """Update progress bar (thread-safe)."""
        def update():
            try:
                if total > 0:
                    percent = (current / total) * 100
                    self.progress_var.set(percent)
                    if text:
                        self.progress_text.config(text=text)
                    else:
                        self.progress_text.config(text=f"Progress: {current}/{total} ({percent:.1f}%)")
            except Exception:
                pass
        try:
            self.frame.after(0, update)
        except Exception:
            pass
    
    def _update_stats(self, success: int, failed: int, total: int = None):
        """Update statistics label (thread-safe)."""
        def update():
            try:
                if total is not None:
                    stats_text = f"✓ Completed: {success} | ✗ Failed: {failed} | Total: {total}"
                else:
                    stats_text = f"✓ Completed: {success} | ✗ Failed: {failed}"
                self.stats_label.config(text=stats_text)
            except Exception:
                pass
        try:
            self.frame.after(0, update)
        except Exception:
            pass
    
    def _reset_progress(self):
        """Reset progress bar and status."""
        def reset():
            try:
                self.progress_var.set(0)
                self.progress_text.config(text="")
                self.stats_label.config(text="")
                self.status_label.config(text="Ready", fg="gray")
            except Exception:
                pass
        try:
            self.frame.after(0, reset)
        except Exception:
            pass
        
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
            text="Data Validation - Row Comparison",
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=10)
        
        # Create two-column layout
        main_frame = ttk.Frame(scrollable_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Enable mouse wheel scrolling
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
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
        
        # DB type, port, and schema for DB2 support
        self.src_db_type_var = tk.StringVar(value="sqlserver")
        self.src_port_var = tk.StringVar(value="50000")
        self.src_schema_var = tk.StringVar(value="")
        
        # Create connection widget for source
        from gui.widgets.connection_widget import ConnectionWidget
        self.src_connection_widget = ConnectionWidget(
            parent=left_frame,
            server_var=self.src_server_var,
            db_var=self.src_db_var,
            auth_var=self.src_auth_var,
            user_var=self.src_user_var,
            password_var=self.src_password_var,
            label_text="",
            row_start=0,
            db_type_var=self.src_db_type_var,
            port_var=self.src_port_var,
            schema_var=self.src_schema_var
        )
        
        # Right column - Destination (using shared variables)
        right_frame = ttk.LabelFrame(main_frame, text="Destination Database", padding=10)
        right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        # Use shared variables from main window
        self.dest_server_var = self.main_window.shared_dest_server
        self.dest_db_var = self.main_window.shared_dest_db
        self.dest_auth_var = self.main_window.shared_dest_auth
        self.dest_user_var = self.main_window.shared_dest_user
        self.dest_password_var = self.main_window.shared_dest_password
        
        # DB type, port, and schema for DB2 support
        self.dest_db_type_var = tk.StringVar(value="sqlserver")
        self.dest_port_var = tk.StringVar(value="50000")
        self.dest_schema_var = tk.StringVar(value="")
        
        # Create connection widget for destination
        from gui.widgets.connection_widget import ConnectionWidget
        self.dest_connection_widget = ConnectionWidget(
            parent=right_frame,
            server_var=self.dest_server_var,
            db_var=self.dest_db_var,
            auth_var=self.dest_auth_var,
            user_var=self.dest_user_var,
            password_var=self.dest_password_var,
            label_text="",
            row_start=0,
            db_type_var=self.dest_db_type_var,
            port_var=self.dest_port_var,
            schema_var=self.dest_schema_var
        )
        
        # Options frame
        options_frame = ttk.LabelFrame(scrollable_frame, text="Validation Options", padding=10)
        options_frame.pack(fill=tk.X, padx=10, pady=10)
        
        tk.Label(options_frame, text="Table Name (optional, leave empty for all tables):").pack(anchor=tk.W)
        self.table_name_var = tk.StringVar()
        ttk.Entry(options_frame, textvariable=self.table_name_var, width=50).pack(anchor=tk.W, pady=5)
        
        self.sample_rows_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Sample Row Differences (first 100)", 
                       variable=self.sample_rows_var).pack(anchor=tk.W, pady=5)
        
        self.use_exact_count_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(options_frame, text="Use Exact COUNT(*) (slower but more accurate)", 
                       variable=self.use_exact_count_var).pack(anchor=tk.W, pady=5)
        
        # Excel support frame
        excel_frame = ttk.LabelFrame(scrollable_frame, text="Bulk Processing (Excel)", padding=10)
        excel_frame.pack(fill=tk.X, padx=10, pady=10)
        
        excel_btn_frame = ttk.Frame(excel_frame)
        excel_btn_frame.pack(fill=tk.X)
        
        ttk.Button(excel_btn_frame, text="📥 Download Sample Template", 
                  command=lambda: self._download_template("data_validation")).pack(side=tk.LEFT, padx=5)
        ttk.Button(excel_btn_frame, text="📤 Upload Excel File", 
                  command=self._upload_excel).pack(side=tk.LEFT, padx=5)
        
        self.excel_file_var = tk.StringVar()
        tk.Label(excel_frame, textvariable=self.excel_file_var, fg="gray").pack(anchor=tk.W, pady=5)
        
        self.excel_configs = []
        
        # Buttons
        btn_frame = ttk.Frame(scrollable_frame)
        btn_frame.pack(pady=10)
        
        self.validate_btn = ttk.Button(btn_frame, text="Start Validation", command=self._start_validation, width=20)
        self.validate_btn.pack(side=tk.LEFT, padx=5)
        
        self.bulk_validate_btn = ttk.Button(btn_frame, text="Start Bulk Validation", command=self._start_bulk_validation, 
                                            width=20, state=tk.DISABLED)
        self.bulk_validate_btn.pack(side=tk.LEFT, padx=5)
        
        self.export_btn = ttk.Button(btn_frame, text="Export Report", command=self._export_report, width=20, state=tk.DISABLED)
        self.export_btn.pack(side=tk.LEFT, padx=5)
        
        # Progress and Status Frame
        status_frame = ttk.LabelFrame(scrollable_frame, text="Progress & Status", padding=10)
        status_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Current status label
        self.status_label = tk.Label(status_frame, text="Ready", font=("Arial", 10, "bold"), fg="gray")
        self.status_label.pack(anchor=tk.W, pady=(0, 5))
        
        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(status_frame, variable=self.progress_var, maximum=100, length=400, mode='determinate')
        self.progress_bar.pack(fill=tk.X, pady=5)
        
        # Progress text
        self.progress_text = tk.Label(status_frame, text="", fg="blue", font=("Arial", 9))
        self.progress_text.pack(anchor=tk.W, pady=2)
        
        # Stats frame
        stats_frame = ttk.Frame(status_frame)
        stats_frame.pack(fill=tk.X, pady=5)
        
        self.stats_label = tk.Label(stats_frame, text="", font=("Arial", 9), fg="darkgreen")
        self.stats_label.pack(anchor=tk.W)
        
        # Results frame
        results_frame = ttk.LabelFrame(scrollable_frame, text="Validation Results", padding=10)
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Filter frame
        filter_frame = ttk.Frame(results_frame)
        filter_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Label(filter_frame, text="Filter by Status:").pack(side=tk.LEFT, padx=5)
        
        self.status_filter_var = tk.StringVar(value="All")
        status_filter_combo = ttk.Combobox(filter_frame, textvariable=self.status_filter_var,
                                          values=["All", "✓ Match", "✗ Mismatch", "✗ Error", "✗ Missing"],
                                          state="readonly", width=15)
        status_filter_combo.pack(side=tk.LEFT, padx=5)
        status_filter_combo.bind("<<ComboboxSelected>>", lambda e: self._filter_results())
        
        tk.Label(filter_frame, text="Search:").pack(side=tk.LEFT, padx=(20, 5))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(filter_frame, textvariable=self.search_var, width=20)
        search_entry.pack(side=tk.LEFT, padx=5)
        search_entry.bind("<KeyRelease>", lambda e: self._filter_results())
        
        ttk.Button(filter_frame, text="Clear Filter", command=self._clear_filter).pack(side=tk.LEFT, padx=5)
        
        # Store all items for filtering
        self.all_tree_items = []
        
        # Treeview for results
        tree_frame = ttk.Frame(results_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(tree_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.results_tree = ttk.Treeview(tree_frame, columns=("DB", "Source", "Destination", "Status", "Differences"), 
                                         show="tree headings", yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.results_tree.yview)
        
        self.results_tree.heading("#0", text="Table", command=lambda: self._sort_treeview("#0"))
        self.results_tree.heading("DB", text="Database", command=lambda: self._sort_treeview("DB"))
        self.results_tree.heading("Source", text="Source Rows", command=lambda: self._sort_treeview("Source"))
        self.results_tree.heading("Destination", text="Dest Rows", command=lambda: self._sort_treeview("Destination"))
        self.results_tree.heading("Status", text="Status", command=lambda: self._sort_treeview("Status"))
        self.results_tree.heading("Differences", text="Differences", command=lambda: self._sort_treeview("Differences"))
        
        self.results_tree.column("#0", width=200)
        self.results_tree.column("DB", width=250)  # Increased width for database names
        self.results_tree.column("Source", width=120)
        self.results_tree.column("Destination", width=120)
        self.results_tree.column("Status", width=100)
        self.results_tree.column("Differences", width=150)
        
        self.treeview_sort_reverse = {}
        
        self.results_tree.pack(fill=tk.BOTH, expand=True)
        
        # Log output
        log_frame = ttk.LabelFrame(scrollable_frame, text="Validation Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.validation_log = scrolledtext.ScrolledText(log_frame, height=10, wrap=tk.WORD)
        self.validation_log.pack(fill=tk.BOTH, expand=True)
        
        self.validation_results = {}
        self._cached_driver = None  # Cache the detected driver
    
    def _get_odbc_driver(self):
        """Auto-detect the best available ODBC driver."""
        if self._cached_driver:
            self._log(f"Using cached ODBC driver: {self._cached_driver}", logging.DEBUG, {"process": "DriverDetection"})
            return self._cached_driver
        
        self._log("Detecting ODBC driver...", logging.DEBUG, {"process": "DriverDetection"})
        drivers = pyodbc.drivers()
        self._log(f"Available drivers: {', '.join(drivers)}", logging.DEBUG, {"process": "DriverDetection"})
        
        preferred = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "SQL Server",
        ]
        
        for name in preferred:
            if name in drivers:
                self._cached_driver = name
                self._log(f"✓ Selected ODBC driver: {name}", logging.INFO, {"process": "DriverDetection"})
                return name
        
        # Fallback: find any SQL Server driver
        for d in drivers:
            if "SQL Server" in d:
                self._cached_driver = d
                self._log(f"✓ Selected fallback ODBC driver: {d}", logging.WARNING, {"process": "DriverDetection"})
                return d
        
        # Last resort - return the most common one and hope it works
        self._log(f"⚠ No ODBC driver found, using default: ODBC Driver 17 for SQL Server", logging.WARNING, {"process": "DriverDetection"})
        return "ODBC Driver 17 for SQL Server"
        
    def _get_connection_string(self, server, database, auth, user, password):
        """Build connection string with auto-detected driver."""
        driver_name = self._get_odbc_driver()
        driver = "{" + driver_name + "}"
        
        # Log password status (but not the actual password)
        password_status = "provided" if password else "missing"
        self._log(f"Building connection string: Server={server}, Database={database}, Auth={auth}, User={user}, Password={password_status}", 
                 logging.DEBUG, {"process": "ConnectionString"})
        
        if auth == "entra_mfa":
            conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Authentication=ActiveDirectoryInteractive;UID={user}"
            self._log(f"  Using Entra MFA authentication", logging.DEBUG, {"process": "ConnectionString"})
        elif auth == "entra_password":
            conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Authentication=ActiveDirectoryPassword;UID={user};PWD={password or ''}"
            self._log(f"  Using Entra Password authentication (password: {'***' if password else '(empty)'})", logging.DEBUG, {"process": "ConnectionString"})
        elif auth == "sql":
            conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password or ''}"
            self._log(f"  Using SQL authentication (password: {'***' if password else '(empty)'})", logging.DEBUG, {"process": "ConnectionString"})
        else:  # windows
            conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes"
            self._log(f"  Using Windows authentication", logging.DEBUG, {"process": "ConnectionString"})
            
        return conn_str
        
    def _start_validation(self):
        """Start data validation in a separate thread."""
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
            
        self.validate_btn.config(state=tk.DISABLED)
        self.export_btn.config(state=tk.DISABLED)
        self.results_tree.delete(*self.results_tree.get_children())
        self.all_tree_items = []  # Clear filter items list
        self.validation_log.delete("1.0", tk.END)
        self._reset_progress()
        self._log("Starting data validation...")
        self.validation_results = {}
        self._update_status("Starting validation...", "blue")
        
        def run_validation():
            src_db = self.src_db_var.get() or "Source"
            dest_db = self.dest_db_var.get() or "Destination"
            context = {"db": f"{src_db}->{dest_db}", "process": "SingleValidation"}
            
            try:
                self._log("Building connection strings...", logging.DEBUG, context)
                src_conn_str = self._get_connection_string(
                    self.src_server_var.get(),
                    self.src_db_var.get(),
                    self.src_auth_var.get(),
                    self.src_user_var.get(),
                    self.src_password_var.get()
                )
                self._log(f"Source connection: {self.src_server_var.get()}/{src_db}", logging.INFO, context)
                
                dest_conn_str = self._get_connection_string(
                    self.dest_server_var.get(),
                    self.dest_db_var.get(),
                    self.dest_auth_var.get(),
                    self.dest_user_var.get(),
                    self.dest_password_var.get()
                )
                self._log(f"Destination connection: {self.dest_server_var.get()}/{dest_db}", logging.INFO, context)
                
                self._update_status(f"Connecting to {src_db}...", "orange")
                self._log("Connecting to source database...", logging.INFO, context)
                src_conn = connect_to_any_database(
                    server=self.src_server_var.get(),
                    database=self.src_db_var.get(),
                    auth=self.src_auth_var.get(),
                    user=self.src_user_var.get(),
                    password=self.src_password_var.get() or None,
                    db_type=self.src_db_type_var.get(),
                    port=int(self.src_port_var.get() or 50000),
                    timeout=30
                )
                self._log(f"[OK] Connected to source: {src_db}", logging.INFO, context)
                
                self._update_status(f"Connecting to {dest_db}...", "orange")
                self._log("Connecting to destination database...", logging.INFO, context)
                dest_conn = connect_to_any_database(
                    server=self.dest_server_var.get(),
                    database=self.dest_db_var.get(),
                    auth=self.dest_auth_var.get(),
                    user=self.dest_user_var.get(),
                    password=self.dest_password_var.get() or None,
                    db_type=self.dest_db_type_var.get(),
                    port=int(self.dest_port_var.get() or 50000),
                    timeout=30
                )
                self._log(f"[OK] Connected to destination: {dest_db}", logging.INFO, context)
                
                src_cur = src_conn.cursor()
                dest_cur = dest_conn.cursor()
                
                # Get list of tables
                table_name = self.table_name_var.get().strip()
                src_is_db2 = self.src_db_type_var.get().lower() == "db2"
                src_schema = self.src_schema_var.get().strip().upper() if src_is_db2 else None
                
                if table_name:
                    self._log(f"Using specified table: {table_name}", logging.INFO, context)
                    tables = [table_name]
                else:
                    self._update_status("Querying for table list...", "blue")
                    self._log("Querying source database for table list...", logging.INFO, context)
                    
                    if src_is_db2:
                        # DB2 uses SYSCAT.TABLES
                        if src_schema:
                            src_cur.execute("""
                                SELECT TABSCHEMA, TABNAME
                                FROM SYSCAT.TABLES
                                WHERE TYPE = 'T' AND TABSCHEMA = ?
                                ORDER BY TABSCHEMA, TABNAME
                            """, (src_schema,))
                        else:
                            src_cur.execute("""
                                SELECT TABSCHEMA, TABNAME
                                FROM SYSCAT.TABLES
                                WHERE TYPE = 'T' AND TABSCHEMA NOT LIKE 'SYS%'
                                ORDER BY TABSCHEMA, TABNAME
                            """)
                    else:
                        # SQL Server uses INFORMATION_SCHEMA
                        src_cur.execute("""
                            SELECT TABLE_SCHEMA, TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE'
                            ORDER BY TABLE_SCHEMA, TABLE_NAME
                        """)
                    tables = [f"{row[0]}.{row[1]}" for row in src_cur.fetchall()]
                    self._log(f"Found {len(tables)} table(s) in source database", logging.INFO, context)
                
                self._update_status(f"Validating {len(tables)} table(s)...", "green")
                self._update_progress(0, len(tables), f"Starting validation of {len(tables)} tables...")
                self._log(f"Starting validation of {len(tables)} table(s)...", logging.INFO, context)
                
                for idx, table in enumerate(tables, 1):
                    schema, name = table.split('.') if '.' in table else ('dbo', table)
                    table_context = {**context, "table": table}
                    
                    # Update progress every 10 tables or for first/last
                    if idx % 10 == 0 or idx == 1 or idx == len(tables):
                        self._update_progress(idx, len(tables), f"Validating table {idx}/{len(tables)}: {table}")
                        self._update_status(f"Validating table {idx}/{len(tables)}: {table}", "green")
                    
                    self._log(f"[{idx}/{len(tables)}] Validating table: {table}", logging.INFO, table_context)
                    
                    try:
                        use_exact = self.use_exact_count_var.get()
                        
                        # Build table references based on database type
                        if src_is_db2:
                            src_table_ref = f'"{schema}"."{name}"'  # DB2 uses double quotes
                        else:
                            src_table_ref = f"[{schema}].[{name}]"  # SQL Server uses brackets
                        
                        # Destination is always SQL Server
                        dest_table_ref = f"[{schema}].[{name}]"
                        
                        if use_exact:
                            # EXACT MODE: Use direct COUNT(*) for both databases
                            self._log(f"  Executing COUNT(*) on source: {src_table_ref}", logging.DEBUG, table_context)
                            src_cur.execute(f"SELECT COUNT(*) FROM {src_table_ref}")
                            src_count = src_cur.fetchone()[0]
                            self._log(f"  Source count: {src_count:,}", logging.INFO, table_context)
                            
                            self._log(f"  Executing COUNT(*) on destination: {dest_table_ref}", logging.DEBUG, table_context)
                            dest_cur.execute(f"SELECT COUNT(*) FROM {dest_table_ref}")
                            dest_count = dest_cur.fetchone()[0]
                            self._log(f"  Destination count: {dest_count:,}", logging.INFO, table_context)
                            
                            # Compare counts
                            status = "✓ Match" if src_count == dest_count else "✗ Mismatch"
                            diff = abs(src_count - dest_count)
                            
                            if src_count == dest_count:
                                self._log(f"  ✓ MATCH: Both have {src_count:,} rows", logging.INFO, table_context)
                            else:
                                self._log(f"  ✗ MISMATCH: Difference of {diff:,} rows (Source: {src_count:,}, Dest: {dest_count:,})", 
                                         logging.WARNING, table_context)
                            
                            differences = []
                            if self.sample_rows_var.get() and src_count != dest_count:
                                differences.append(f"Row count difference: {diff}")
                        elif src_is_db2:
                            # FAST MODE FOR DB2: Use SYSCAT.TABLES.CARD (cardinality metadata)
                            self._log(f"  Fast check: Querying SYSCAT.TABLES.CARD for row count (source)...", logging.DEBUG, table_context)
                            src_cur.execute("""
                                SELECT CARD FROM SYSCAT.TABLES 
                                WHERE TABSCHEMA = ? AND TABNAME = ?
                            """, (schema, name))
                            result = src_cur.fetchone()
                            src_count_fast = int(result[0]) if result and result[0] is not None and result[0] >= 0 else -1
                            
                            # For destination (SQL Server), use sys.partitions
                            self._log(f"  Fast check: Querying sys.partitions for row count (destination)...", logging.DEBUG, table_context)
                            dest_cur.execute("""
                                SELECT SUM(p.rows) 
                                FROM sys.partitions p 
                                JOIN sys.tables t ON p.object_id = t.object_id
                                JOIN sys.schemas s ON t.schema_id = s.schema_id
                                WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                            """, (schema, name))
                            dest_count_fast = dest_cur.fetchone()[0] or 0
                            
                            # If DB2 CARD is invalid (-1), fall back to COUNT(*)
                            if src_count_fast < 0:
                                self._log(f"  DB2 CARD unavailable, using COUNT(*)...", logging.DEBUG, table_context)
                                src_cur.execute(f"SELECT COUNT(*) FROM {src_table_ref}")
                                src_count = src_cur.fetchone()[0]
                            else:
                                src_count = src_count_fast
                                self._log(f"  Source fast count (CARD): {src_count:,}", logging.INFO, table_context)
                            
                            dest_count = int(dest_count_fast)
                            self._log(f"  Destination fast count: {dest_count:,}", logging.INFO, table_context)
                            
                            # Compare fast counts
                            if src_count == dest_count:
                                status = "✓ Match"
                                diff = 0
                                differences = []
                                self._log(f"  ✓ MATCH (fast check): Both have {src_count:,} rows", logging.INFO, table_context)
                            else:
                                # Fast counts differ - verify with exact COUNT(*) for accuracy
                                self._log(f"  ⚠ Fast check mismatch ({src_count:,} vs {dest_count:,}), verifying...", logging.WARNING, table_context)
                                
                                # Only do exact count if difference is significant (>1% or >100 rows)
                                diff_pct = abs(src_count - dest_count) / max(src_count, dest_count, 1) * 100
                                if diff_pct > 1 or abs(src_count - dest_count) > 100:
                                    self._log(f"  Executing exact COUNT(*) for verification...", logging.DEBUG, table_context)
                                    src_cur.execute(f"SELECT COUNT(*) FROM {src_table_ref}")
                                    src_count = src_cur.fetchone()[0]
                                    dest_cur.execute(f"SELECT COUNT(*) FROM {dest_table_ref}")
                                    dest_count = dest_cur.fetchone()[0]
                                    self._log(f"  Exact counts: Source={src_count:,}, Dest={dest_count:,}", logging.INFO, table_context)
                                
                                status = "✓ Match" if src_count == dest_count else "✗ Mismatch"
                                diff = abs(src_count - dest_count)
                                
                                if src_count == dest_count:
                                    self._log(f"  ✓ MATCH (exact check): Both have {src_count:,} rows", logging.INFO, table_context)
                                else:
                                    self._log(f"  ✗ MISMATCH: Difference of {diff:,} rows", logging.WARNING, table_context)
                                
                                differences = [f"Row count difference: {diff}"] if diff > 0 else []
                        else:
                            # FAST CHECK: Get row counts using sys.partitions (metadata, < 1 second)
                            # Only for SQL Server to SQL Server comparisons
                            self._log(f"  Fast check: Querying sys.partitions for row count (source)...", logging.DEBUG, table_context)
                            src_cur.execute("""
                                SELECT SUM(p.rows) 
                                FROM sys.partitions p 
                                JOIN sys.tables t ON p.object_id = t.object_id
                                JOIN sys.schemas s ON t.schema_id = s.schema_id
                                WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                            """, (schema, name))
                            src_count_fast = src_cur.fetchone()[0] or 0
                            self._log(f"  Source fast count: {src_count_fast:,}", logging.DEBUG, table_context)
                            
                            self._log(f"  Fast check: Querying sys.partitions for row count (destination)...", logging.DEBUG, table_context)
                            dest_cur.execute("""
                                SELECT SUM(p.rows) 
                                FROM sys.partitions p 
                                JOIN sys.tables t ON p.object_id = t.object_id
                                JOIN sys.schemas s ON t.schema_id = s.schema_id
                                WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                            """, (schema, name))
                            dest_count_fast = dest_cur.fetchone()[0] or 0
                            self._log(f"  Destination fast count: {dest_count_fast:,}", logging.DEBUG, table_context)
                            
                            # If fast counts match, we're done (most common case)
                            if src_count_fast == dest_count_fast:
                                src_count = src_count_fast
                                dest_count = dest_count_fast
                                status = "✓ Match"
                                diff = 0
                                differences = []
                                self._log(f"  ✓ MATCH (fast check): Both have {src_count:,} rows", logging.INFO, table_context)
                            else:
                                # PRECISE CHECK: Counts differ, do exact COUNT(*) to verify
                                self._log(f"  ⚠ Fast check mismatch detected, verifying with exact COUNT(*)...", logging.WARNING, table_context)
                                
                                self._log(f"  Executing exact COUNT(*) on source: {src_table_ref}", logging.DEBUG, table_context)
                                src_cur.execute(f"SELECT COUNT(*) FROM {src_table_ref}")
                                src_count = src_cur.fetchone()[0]
                                self._log(f"  Source exact count: {src_count:,}", logging.INFO, table_context)
                                
                                self._log(f"  Executing exact COUNT(*) on destination: {dest_table_ref}", logging.DEBUG, table_context)
                                dest_cur.execute(f"SELECT COUNT(*) FROM {dest_table_ref}")
                                dest_count = dest_cur.fetchone()[0]
                                self._log(f"  Destination exact count: {dest_count:,}", logging.INFO, table_context)
                                
                                # Compare precise counts
                                status = "✓ Match" if src_count == dest_count else "✗ Mismatch"
                                diff = abs(src_count - dest_count)
                                
                                if src_count == dest_count:
                                    self._log(f"  ✓ MATCH (exact check): Both have {src_count:,} rows", logging.INFO, table_context)
                                else:
                                    self._log(f"  ✗ MISMATCH: Difference of {diff:,} rows (Source: {src_count:,}, Dest: {dest_count:,})", 
                                             logging.WARNING, table_context)
                                
                                # Sample row differences if requested
                                differences = []
                                if self.sample_rows_var.get() and src_count != dest_count:
                                    differences.append(f"Row count difference: {diff}")
                        
                        result = {
                            "table": table,
                            "src_count": src_count,
                            "dest_count": dest_count,
                            "status": status,
                            "differences": differences
                        }
                        
                        self.validation_results[table] = result
                        
                        db_name = f"{src_db} vs {dest_db}"
                        # Thread-safe treeview update
                        def add_result(tbl=table, db=db_name, sc=src_count, dc=dest_count, st=status, d=diff, mismatch=(src_count != dest_count)):
                            try:
                                item = self.results_tree.insert("", tk.END, text=tbl,
                                                               values=(db, sc, dc, st, f"{d} rows"))
                                self.all_tree_items.append(item)
                                if mismatch:
                                    self.results_tree.set(item, "Status", "✗ Mismatch")
                            except Exception:
                                pass
                        self.frame.after(0, add_result)
                        
                    except Exception as e:
                        error_msg = str(e)
                        error_type = type(e).__name__
                        self._log(f"  ✗ ERROR validating {table}: {error_msg}", logging.ERROR, table_context)
                        self._log(f"  Error type: {error_type}", logging.DEBUG, table_context)
                        
                        db_name = f"{src_db} vs {dest_db}"
                        # Thread-safe treeview update for errors
                        def add_error(tbl=table, db=db_name, err=error_msg):
                            try:
                                item = self.results_tree.insert("", tk.END, text=tbl,
                                                       values=(db, "Error", "Error", "✗ Error", err))
                                self.all_tree_items.append(item)
                            except Exception:
                                pass
                        self.frame.after(0, add_error)
                
                # Close connections
                self._log("Closing database connections...", logging.DEBUG, context)
                src_conn.close()
                dest_conn.close()
                self._log("✓ Database connections closed", logging.DEBUG, context)
                
                self._log("\n✓ Validation completed!", logging.INFO, context)
                
                # Thread-safe UI updates
                def on_success():
                    try:
                        self.export_btn.config(state=tk.NORMAL)
                        self._update_progress(len(tables), len(tables), f"Completed: {len(tables)} tables validated")
                        self._update_status(f"✓ Validation completed! {len(tables)} tables validated", "darkgreen")
                        messagebox.showinfo("Success", "Data validation completed!")
                    except Exception:
                        pass
                self.frame.after(0, on_success)
                    
            except Exception as e:
                error_str = str(e)
                error_type = type(e).__name__
                self._log(f"\n✗ FATAL ERROR: {error_str}", logging.ERROR, context)
                self._log(f"  Error type: {error_type}", logging.DEBUG, context)
                self._log(f"  Source: {self.src_server_var.get()}/{src_db}", logging.DEBUG, context)
                self._log(f"  Destination: {self.dest_server_var.get()}/{dest_db}", logging.DEBUG, context)
                
                # Check if this is a driver-related error (thread-safe)
                def show_error(err=error_str):
                    try:
                        self._update_status(f"✗ Validation failed: {err[:50]}...", "red")
                        if is_driver_missing_error(err):
                            self._handle_driver_missing_error(err)
                        else:
                            messagebox.showerror("Error", f"Validation failed: {err}")
                    except Exception:
                        pass
                self.frame.after(0, show_error)
            finally:
                # Thread-safe button re-enable
                self.frame.after(0, lambda: self.validate_btn.config(state=tk.NORMAL))
                
        threading.Thread(target=run_validation, daemon=True).start()
    
    def _handle_driver_missing_error(self, error_str: str):
        """Handle ODBC driver missing error - offer installation."""
        if not DRIVER_UTILS_AVAILABLE:
            messagebox.showerror("ODBC Driver Not Found", 
                f"SQL Server ODBC Driver is not installed.\n\n"
                f"Please install Microsoft ODBC Driver 17 or 18 for SQL Server.\n\n"
                f"Download from:\n"
                f"https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server\n\n"
                f"Error: {error_str}")
            return
        
        # Check current driver status
        driver_ok, driver_name = check_sql_server_odbc_driver()
        
        if driver_ok:
            # Driver exists but there's still an error - might be a different issue
            messagebox.showerror("Connection Error", 
                f"ODBC Driver is installed ({driver_name}) but connection failed.\n\n"
                f"This might be a configuration issue.\n\n"
                f"Error: {error_str}")
            return
        
        # Offer to install driver
        response = messagebox.askyesnocancel(
            "ODBC Driver Not Found",
            "SQL Server ODBC Driver is not installed on this machine.\n\n"
            "This driver is required for database connections.\n\n"
            "Would you like to install it now?\n\n"
            "Yes: Install automatically (requires admin rights)\n"
            "No: Show manual installation instructions\n"
            "Cancel: Close this dialog"
        )
        
        if response is True:
            # Yes - Try automatic installation
            self._install_odbc_driver()
        elif response is False:
            # No - Show manual instructions
            self._show_manual_install_instructions()
    
    def _install_odbc_driver(self):
        """Install ODBC driver with progress dialog."""
        progress_win = tk.Toplevel(self.frame)
        progress_win.title("Installing ODBC Driver")
        progress_win.geometry("400x150")
        progress_win.resizable(False, False)
        progress_win.transient(self.frame.winfo_toplevel())
        progress_win.grab_set()
        
        progress_win.update_idletasks()
        parent = self.frame.winfo_toplevel()
        x = parent.winfo_x() + (parent.winfo_width() // 2) - 200
        y = parent.winfo_y() + (parent.winfo_height() // 2) - 75
        progress_win.geometry(f"+{x}+{y}")
        
        tk.Label(progress_win, text="Installing ODBC Driver 18 for SQL Server...", 
                 font=("Segoe UI", 10)).pack(pady=20)
        tk.Label(progress_win, text="This may take a few minutes.\nA UAC prompt may appear.", 
                 font=("Segoe UI", 9)).pack()
        
        progress = ttk.Progressbar(progress_win, mode='indeterminate', length=300)
        progress.pack(pady=20)
        progress.start(10)
        
        def run_install():
            try:
                success, message = install_odbc_via_powershell()
                self.frame.after(0, lambda: self._finish_driver_install(progress_win, success, message))
            except Exception as e:
                self.frame.after(0, lambda: self._finish_driver_install(progress_win, False, str(e)))
        
        threading.Thread(target=run_install, daemon=True).start()
    
    def _finish_driver_install(self, progress_win, success, message):
        """Finish driver installation and show result."""
        try:
            progress_win.destroy()
        except:
            pass
        
        if success:
            driver_ok, driver_name = check_sql_server_odbc_driver()
            if driver_ok:
                messagebox.showinfo("Success", 
                    f"ODBC Driver installed successfully!\n\n"
                    f"Driver: {driver_name}\n\n"
                    f"You can now retry the validation.")
                self.validation_log.insert(tk.END, f"\n✓ ODBC Driver installed: {driver_name}\n")
            else:
                messagebox.showinfo("Installation Complete", 
                    message + "\n\nPlease restart the application to use the new driver.")
        else:
            messagebox.showerror("Installation Failed", 
                f"Failed to install ODBC Driver:\n\n{message}\n\n"
                "Please try manual installation.")
            self._show_manual_install_instructions()
    
    def _show_manual_install_instructions(self):
        """Show manual installation instructions for ODBC driver."""
        if DRIVER_UTILS_AVAILABLE:
            instructions = get_manual_install_instructions()
        else:
            is_64bit = sys.maxsize > 2**32
            arch = "64-bit" if is_64bit else "32-bit"
            instructions = f"""
ODBC Driver Installation Instructions
=====================================

Your Python installation is {arch}, so you need the matching ODBC driver.

Option 1: Download from Microsoft
---------------------------------
1. Go to: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
2. Download "ODBC Driver 18 for SQL Server" ({arch})
3. Run the installer and follow the prompts
4. Restart this application

Option 2: Using winget (Windows 11/10)
--------------------------------------
Open Command Prompt as Administrator and run:
    winget install Microsoft.msodbcsql18

After installation, restart this application.
"""
        
        dialog = tk.Toplevel(self.frame)
        dialog.title("ODBC Driver Installation Instructions")
        dialog.geometry("600x400")
        dialog.transient(self.frame.winfo_toplevel())
        
        text = tk.Text(dialog, wrap=tk.WORD, font=("Consolas", 10))
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text.insert(tk.END, instructions)
        text.config(state=tk.DISABLED)
        
        tk.Button(dialog, text="Close", command=dialog.destroy).pack(pady=10)
        
    def _export_report(self):
        """Export validation report to Excel."""
        if not self.validation_results and len(self.results_tree.get_children()) == 0:
            messagebox.showwarning("Warning", "No validation results to export!")
            return
            
        filename = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if not filename:
            return
            
        try:
            import pandas as pd
            
            # Collect data from treeview
            data = []
            for item in self.results_tree.get_children():
                values = self.results_tree.item(item)
                table = values['text']
                cols = values['values']
                
                # Column order: (DB, Source, Destination, Status, Differences)
                if len(cols) >= 5:
                    data.append({
                        "Database": cols[0] if cols[0] else f"{self.src_db_var.get()} vs {self.dest_db_var.get()}",
                        "Table": table,
                        "Source Rows": cols[1] if len(cols) > 1 else "",
                        "Destination Rows": cols[2] if len(cols) > 2 else "",
                        "Status": cols[3] if len(cols) > 3 else "",
                        "Differences": cols[4] if len(cols) > 4 else ""
                    })
                elif len(cols) > 0:
                    # Handle cases with fewer columns (backward compatibility)
                    data.append({
                        "Database": cols[0] if len(cols) > 0 else f"{self.src_db_var.get()} vs {self.dest_db_var.get()}",
                        "Table": table,
                        "Source Rows": cols[1] if len(cols) > 1 else "",
                        "Destination Rows": cols[2] if len(cols) > 2 else "",
                        "Status": cols[3] if len(cols) > 3 else "",
                        "Differences": cols[4] if len(cols) > 4 else ""
                    })
            
            if filename.endswith('.xlsx'):
                # Export to Excel
                df = pd.DataFrame(data)
                df.to_excel(filename, index=False, sheet_name="Validation Results")
                messagebox.showinfo("Success", f"Report exported to Excel:\n{filename}")
            else:
                # Export to JSON
                report = {
                    "timestamp": datetime.now().isoformat(),
                    "source": {
                        "server": self.src_server_var.get(),
                        "database": self.src_db_var.get()
                    },
                    "destination": {
                        "server": self.dest_server_var.get(),
                        "database": self.dest_db_var.get()
                    },
                    "results": data
                }
                
                with open(filename, 'w') as f:
                    json.dump(report, f, indent=2)
                    
                messagebox.showinfo("Success", f"Report exported to JSON:\n{filename}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export report: {str(e)}")
            
    def _sort_treeview(self, col):
        """Sort treeview by column."""
        # Get all items with their values
        items = []
        for item in self.results_tree.get_children(''):
            if col == "#0":
                # For tree column, use the text
                val = self.results_tree.item(item, "text")
            else:
                # For other columns, use the column value
                val = self.results_tree.set(item, col)
            items.append((val, item))
        
        # Determine sort direction
        reverse = self.treeview_sort_reverse.get(col, False)
        self.treeview_sort_reverse[col] = not reverse
        
        # Sort items
        try:
            # Try numeric sort first (for numeric columns)
            items.sort(key=lambda t: int(str(t[0])) if str(t[0]).isdigit() else (float(str(t[0])) if str(t[0]).replace('.', '').replace('-', '').isdigit() else float('inf')), reverse=reverse)
        except:
            # Fall back to string sort
            items.sort(key=lambda t: str(t[0]).lower() if t[0] else "", reverse=reverse)
        
        # Rearrange items
        for index, (val, item) in enumerate(items):
            self.results_tree.move(item, '', index)
            
    def _download_template(self, template_type: str):
        """Download sample Excel template."""
        try:
            file_path = create_sample_excel(template_type)
            if file_path:
                messagebox.showinfo("Success", f"Template saved to:\n{file_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create template: {str(e)}")
            
    def _upload_excel(self):
        """Upload Excel file for bulk validation."""
        file_path = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )
        if not file_path:
            self._log("Excel file selection cancelled", logging.DEBUG, {"process": "ExcelUpload"})
            return
            
        try:
            self._log(f"Reading Excel file: {Path(file_path).name}", logging.INFO, {"process": "ExcelUpload"})
            configs = read_excel_file(
                file_path,
                required_columns=["src_server", "src_db", "dest_server", "dest_db"],
                default_user=self.src_user_var.get() or None
            )
            
            self._log(f"Successfully parsed {len(configs)} configuration(s) from Excel", logging.INFO, {"process": "ExcelUpload"})
            for idx, cfg in enumerate(configs, 1):
                self._log(f"  Config #{idx}: {cfg.get('src_db')} -> {cfg.get('dest_db')}", logging.DEBUG, 
                         {"process": "ExcelUpload", "config_idx": idx, "db": f"{cfg.get('src_db')}->{cfg.get('dest_db')}"})
            
            self.excel_configs = configs
            self.excel_file_var.set(f"Loaded {len(configs)} configuration(s) from {Path(file_path).name}")
            self.bulk_validate_btn.config(state=tk.NORMAL)
            messagebox.showinfo("Success", f"Loaded {len(configs)} configuration(s) from Excel file!")
        except Exception as e:
            self._log(f"Failed to read Excel file: {str(e)}", logging.ERROR, {"process": "ExcelUpload"})
            messagebox.showerror("Error", f"Failed to read Excel file: {str(e)}")
            
    def _start_bulk_validation(self):
        """Start bulk validation from Excel configurations."""
        if not self.excel_configs:
            messagebox.showwarning("Warning", "No configurations loaded! Please upload an Excel file first.")
            return
            
        self.bulk_validate_btn.config(state=tk.DISABLED)
        self.results_tree.delete(*self.results_tree.get_children())
        self.all_tree_items = []  # Clear filter items list
        self.validation_log.delete("1.0", tk.END)
        self._reset_progress()
        self._log(f"Starting bulk validation for {len(self.excel_configs)} configuration(s)...")
        self.validation_results = {}
        
        total_configs = len(self.excel_configs)
        self._update_status("Starting bulk validation...", "blue")
        self._update_progress(0, total_configs, f"Initializing... (0/{total_configs})")
        
        def run_bulk():
            total_success = 0
            total_fail = 0
            
            self._log(f"Bulk validation thread started", logging.DEBUG, {"process": "BulkValidation"})
            
            for idx, cfg in enumerate(self.excel_configs, 1):
                src_db = cfg.get('src_db') or "Source"
                dest_db = cfg.get('dest_db') or "Destination"
                context = {"config_idx": idx, "db": f"{src_db}->{dest_db}", "process": "BulkValidation"}
                
                self._log(f"\n{'='*60}", logging.INFO, context)
                self._log(f"Processing configuration #{idx}/{len(self.excel_configs)}", logging.INFO, context)
                self._log(f"  Source: {cfg.get('src_server')}/{src_db}", logging.INFO, context)
                self._log(f"  Destination: {cfg.get('dest_server')}/{dest_db}", logging.INFO, context)
                self._log(f"  Source Auth: {cfg.get('src_auth', self.src_auth_var.get())}", logging.DEBUG, context)
                self._log(f"  Dest Auth: {cfg.get('dest_auth', self.dest_auth_var.get())}", logging.DEBUG, context)
                
                # Update status UI
                self._update_status(f"Processing Config #{idx}/{total_configs}: {src_db} → {dest_db}", "blue")
                self._update_progress(idx - 1, total_configs, f"Config {idx}/{total_configs}: {src_db} → {dest_db}")
                
                try:
                    # Build connection strings
                    self._log("Building source connection string...", logging.DEBUG, context)
                    src_conn_str = self._get_connection_string(
                        cfg.get("src_server"),
                        cfg.get("src_db"),
                        cfg.get("src_auth", self.src_auth_var.get()),
                        cfg.get("src_user", cfg.get("user", self.src_user_var.get())),
                        cfg.get("src_password", self.src_password_var.get())
                    )
                    self._log(f"Source connection string built (server: {cfg.get('src_server')})", logging.DEBUG, context)
                    
                    self._log("Building destination connection string...", logging.DEBUG, context)
                    dest_conn_str = self._get_connection_string(
                        cfg.get("dest_server"),
                        cfg.get("dest_db"),
                        cfg.get("dest_auth", self.dest_auth_var.get()),
                        cfg.get("dest_user", cfg.get("user", self.dest_user_var.get())),
                        cfg.get("dest_password", self.dest_password_var.get())
                    )
                    self._log(f"Destination connection string built (server: {cfg.get('dest_server')})", logging.DEBUG, context)
                    
                    # Connect to databases
                    self._update_status(f"Connecting to {src_db}...", "orange")
                    self._log("Connecting to source database...", logging.INFO, context)
                    src_conn = connect_to_any_database(
                        server=cfg.get("src_server"),
                        database=cfg.get("src_db"),
                        auth=cfg.get("src_auth", self.src_auth_var.get()),
                        user=cfg.get("src_user", cfg.get("user", self.src_user_var.get())),
                        password=cfg.get("src_password", self.src_password_var.get()) or None,
                        db_type=cfg.get("src_db_type", self.src_db_type_var.get()),
                        port=int(cfg.get("src_port", self.src_port_var.get()) or 50000),
                        timeout=30
                    )
                    self._log(f"[OK] Connected to source database: {src_db}", logging.INFO, context)
                    
                    self._update_status(f"Connecting to {dest_db}...", "orange")
                    self._log("Connecting to destination database...", logging.INFO, context)
                    dest_conn = connect_to_any_database(
                        server=cfg.get("dest_server"),
                        database=cfg.get("dest_db"),
                        auth=cfg.get("dest_auth", self.dest_auth_var.get()),
                        user=cfg.get("dest_user", cfg.get("user", self.dest_user_var.get())),
                        password=cfg.get("dest_password", self.dest_password_var.get()) or None,
                        db_type=cfg.get("dest_db_type", self.dest_db_type_var.get()),
                        port=int(cfg.get("dest_port", self.dest_port_var.get()) or 50000),
                        timeout=30
                    )
                    self._log(f"[OK] Connected to destination database: {dest_db}", logging.INFO, context)
                    
                    src_cur = src_conn.cursor()
                    dest_cur = dest_conn.cursor()
                    
                    # Get table list
                    table_name = cfg.get("table_name", "").strip() or self.table_name_var.get().strip()
                    src_is_db2 = cfg.get("src_db_type", self.src_db_type_var.get()).lower() == "db2"
                    src_schema_cfg = cfg.get("src_schema", self.src_schema_var.get()).strip().upper() if src_is_db2 else None
                    
                    if table_name:
                        self._log(f"Using specified table: {table_name}", logging.INFO, context)
                        tables = [table_name]
                    else:
                        self._log("Querying source database for table list...", logging.INFO, context)
                        
                        if src_is_db2:
                            # DB2 uses SYSCAT.TABLES
                            if src_schema_cfg:
                                src_cur.execute("""
                                    SELECT TABSCHEMA, TABNAME
                                    FROM SYSCAT.TABLES
                                    WHERE TYPE = 'T' AND TABSCHEMA = ?
                                    ORDER BY TABSCHEMA, TABNAME
                                """, (src_schema_cfg,))
                            else:
                                src_cur.execute("""
                                    SELECT TABSCHEMA, TABNAME
                                    FROM SYSCAT.TABLES
                                    WHERE TYPE = 'T' AND TABSCHEMA NOT LIKE 'SYS%'
                                    ORDER BY TABSCHEMA, TABNAME
                                """)
                        else:
                            # SQL Server uses INFORMATION_SCHEMA
                            src_cur.execute("""
                                SELECT TABLE_SCHEMA, TABLE_NAME
                                FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_TYPE = 'BASE TABLE'
                                ORDER BY TABLE_SCHEMA, TABLE_NAME
                            """)
                        tables = [f"{row[0]}.{row[1]}" for row in src_cur.fetchall()]
                        self._log(f"Found {len(tables)} table(s) in source database", logging.INFO, context)
                        for i, tbl in enumerate(tables[:10], 1):  # Log first 10
                            self._log(f"  Table {i}: {tbl}", logging.DEBUG, context)
                        if len(tables) > 10:
                            self._log(f"  ... and {len(tables) - 10} more tables", logging.DEBUG, context)
                    
                    # Validate each table
                    self._update_status(f"Validating {len(tables)} table(s) in {src_db}...", "green")
                    self._log(f"Starting validation of {len(tables)} table(s)...", logging.INFO, context)
                    validated_count = 0
                    error_count = 0
                    
                    for table_idx, table in enumerate(tables, 1):
                        schema, name = table.split('.') if '.' in table else ('dbo', table)
                        table_context = {**context, "table": table}
                        
                        # Update status for each table
                        if table_idx % 10 == 0 or table_idx == 1:  # Update every 10 tables or first one
                            self._update_status(f"Validating table {table_idx}/{len(tables)}: {table}", "green")
                        
                        self._log(f"[{table_idx}/{len(tables)}] Validating table: {table}", logging.INFO, table_context)
                        
                        try:
                            use_exact = self.use_exact_count_var.get()
                            
                            # Build table references based on database type
                            if src_is_db2:
                                src_table_ref = f'"{schema}"."{name}"'  # DB2 uses double quotes
                            else:
                                src_table_ref = f"[{schema}].[{name}]"  # SQL Server uses brackets
                            
                            # Destination is always SQL Server
                            dest_table_ref = f"[{schema}].[{name}]"
                            
                            if use_exact:
                                # EXACT MODE: Use direct COUNT(*)
                                self._log(f"  Executing COUNT(*) on source: {src_table_ref}", logging.DEBUG, table_context)
                                src_cur.execute(f"SELECT COUNT(*) FROM {src_table_ref}")
                                src_count = src_cur.fetchone()[0]
                                self._log(f"  Source row count: {src_count:,}", logging.INFO, table_context)
                                
                                self._log(f"  Executing COUNT(*) on destination: {dest_table_ref}", logging.DEBUG, table_context)
                                dest_cur.execute(f"SELECT COUNT(*) FROM {dest_table_ref}")
                                dest_count = dest_cur.fetchone()[0]
                                self._log(f"  Destination row count: {dest_count:,}", logging.INFO, table_context)
                            elif src_is_db2:
                                # FAST MODE FOR DB2: Use SYSCAT.TABLES.CARD
                                src_cur.execute("""
                                    SELECT CARD FROM SYSCAT.TABLES 
                                    WHERE TABSCHEMA = ? AND TABNAME = ?
                                """, (schema, name))
                                result = src_cur.fetchone()
                                src_count = int(result[0]) if result and result[0] is not None and result[0] >= 0 else -1
                                
                                # Destination uses sys.partitions
                                dest_cur.execute("""
                                    SELECT SUM(p.rows) 
                                    FROM sys.partitions p 
                                    JOIN sys.tables t ON p.object_id = t.object_id
                                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                                    WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                                """, (schema, name))
                                dest_count = int(dest_cur.fetchone()[0] or 0)
                                
                                # If DB2 CARD unavailable, fall back to COUNT(*)
                                if src_count < 0:
                                    src_cur.execute(f"SELECT COUNT(*) FROM {src_table_ref}")
                                    src_count = src_cur.fetchone()[0]
                                
                                self._log(f"  Fast counts: Source={src_count:,}, Dest={dest_count:,}", logging.INFO, table_context)
                            else:
                                # Use fast count from sys.partitions (SQL Server to SQL Server only)
                                self._log(f"  Fast check: Querying sys.partitions for row count (source)...", logging.DEBUG, table_context)
                                src_cur.execute("""
                                    SELECT SUM(p.rows) 
                                    FROM sys.partitions p 
                                    JOIN sys.tables t ON p.object_id = t.object_id
                                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                                    WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                                """, (schema, name))
                                src_count_fast = src_cur.fetchone()[0] or 0
                                self._log(f"  Source fast count: {src_count_fast:,}", logging.DEBUG, table_context)
                                
                                self._log(f"  Fast check: Querying sys.partitions for row count (destination)...", logging.DEBUG, table_context)
                                dest_cur.execute("""
                                    SELECT SUM(p.rows) 
                                    FROM sys.partitions p 
                                    JOIN sys.tables t ON p.object_id = t.object_id
                                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                                    WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)
                                """, (schema, name))
                                dest_count_fast = dest_cur.fetchone()[0] or 0
                                self._log(f"  Destination fast count: {dest_count_fast:,}", logging.DEBUG, table_context)
                                
                                # If fast counts match, use them; otherwise verify with exact count
                                if src_count_fast == dest_count_fast:
                                    src_count = src_count_fast
                                    dest_count = dest_count_fast
                                    self._log(f"  Fast check match: {src_count:,} rows", logging.INFO, table_context)
                                else:
                                    # Verify with exact COUNT(*)
                                    self._log(f"  ⚠ Fast check mismatch, verifying with exact COUNT(*)...", logging.WARNING, table_context)
                                    src_cur.execute(f"SELECT COUNT(*) FROM {src_table_ref}")
                                    src_count = src_cur.fetchone()[0]
                                    dest_cur.execute(f"SELECT COUNT(*) FROM {dest_table_ref}")
                                    dest_count = dest_cur.fetchone()[0]
                                    self._log(f"  Source exact count: {src_count:,}, Dest exact count: {dest_count:,}", logging.INFO, table_context)
                            
                            # Compare
                            status = "✓ Match" if src_count == dest_count else "✗ Mismatch"
                            diff = abs(src_count - dest_count)
                            
                            if src_count == dest_count:
                                self._log(f"  ✓ MATCH: Both databases have {src_count:,} rows", logging.INFO, table_context)
                            else:
                                self._log(f"  ✗ MISMATCH: Difference of {diff:,} rows (Source: {src_count:,}, Dest: {dest_count:,})", 
                                         logging.WARNING, table_context)
                            
                            result = {
                                "table": table,
                                "src_count": src_count,
                                "dest_count": dest_count,
                                "status": status,
                                "differences": [f"Row count difference: {diff}"]
                            }
                            
                            self.validation_results[f"{cfg.get('src_db')}.{table}"] = result
                            
                            db_name = f"{src_db} vs {dest_db}"
                            # Thread-safe treeview update
                            def add_result(tbl=table, db=db_name, sc=src_count, dc=dest_count, st=status, d=diff):
                                try:
                                    item = self.results_tree.insert("", tk.END, text=tbl,
                                                                   values=(db, sc, dc, st, f"{d} rows"))
                                    self.all_tree_items.append(item)
                                except Exception:
                                    pass
                            self.frame.after(0, add_result)
                            validated_count += 1
                            
                        except Exception as e:
                            error_count += 1
                            error_msg = str(e)
                            self._log(f"  ✗ ERROR validating {table}: {error_msg}", logging.ERROR, table_context)
                            self._log(f"  Error type: {type(e).__name__}", logging.DEBUG, table_context)
                            
                            db_name = f"{src_db} vs {dest_db}"
                            # Thread-safe treeview update for errors
                            def add_error(tbl=table, db=db_name, err=error_msg):
                                try:
                                    item = self.results_tree.insert("", tk.END, text=tbl,
                                                           values=(db, "Error", "Error", "✗ Error", err))
                                    self.all_tree_items.append(item)
                                except Exception:
                                    pass
                            self.frame.after(0, add_error)
                    
                    # Close connections
                    self._log("Closing database connections...", logging.DEBUG, context)
                    src_conn.close()
                    dest_conn.close()
                    self._log("✓ Database connections closed", logging.DEBUG, context)
                    
                    total_success += 1
                    self._log(f"✓ Configuration #{idx} completed: {validated_count} tables validated, {error_count} errors", 
                             logging.INFO, context)
                    self._update_status(f"✓ Config #{idx} completed: {validated_count} tables validated", "darkgreen")
                    self._update_stats(total_success, total_fail, total_configs)
                    
                except Exception as e:
                    error_str = str(e)
                    error_type = type(e).__name__
                    self._log(f"✗ FATAL ERROR in configuration #{idx}: {error_str}", logging.ERROR, context)
                    self._log(f"  Error type: {error_type}", logging.DEBUG, context)
                    self._log(f"  Source: {cfg.get('src_server')}/{cfg.get('src_db')}", logging.DEBUG, context)
                    self._log(f"  Destination: {cfg.get('dest_server')}/{cfg.get('dest_db')}", logging.DEBUG, context)
                    total_fail += 1
                    self._update_status(f"✗ Config #{idx} failed: {error_str[:50]}...", "red")
                    self._update_stats(total_success, total_fail, total_configs)
                    
                    # Check if this is a driver error - only show once
                    if is_driver_missing_error(error_str) and total_fail == 1:
                        self.frame.after(0, lambda err=error_str: self._handle_driver_missing_error(err))
                    
            self._log(f"\n{'='*60}")
            self._log(f"Bulk validation completed: {total_success} succeeded, {total_fail} failed")
            
            # Thread-safe final UI updates
            def finalize(success=total_success, fail=total_fail, total=total_configs):
                try:
                    self.export_btn.config(state=tk.NORMAL)
                    self.bulk_validate_btn.config(state=tk.NORMAL)
                    self._update_progress(total, total, f"Completed: {success} succeeded, {fail} failed")
                    if fail == 0:
                        self._update_status(f"✓ All {success} configurations completed successfully!", "darkgreen")
                    else:
                        self._update_status(f"Completed: {success} succeeded, {fail} failed", "orange")
                    self._update_stats(success, fail, total)
                    messagebox.showinfo("Bulk Validation Complete", f"Completed: {success} succeeded, {fail} failed")
                except Exception:
                    pass
            self.frame.after(0, finalize)
            
        threading.Thread(target=run_bulk, daemon=True).start()
        
    def _filter_results(self):
        """Filter treeview results by status and search term."""
        status_filter = self.status_filter_var.get()
        search_term = self.search_var.get().lower()
        
        # Show/hide items based on filter
        for item in self.all_tree_items:
            try:
                values = self.results_tree.item(item)
                status = self.results_tree.set(item, "Status")
                table = values['text']
                db_name = self.results_tree.set(item, "DB")
                
                # Check status filter
                status_match = (status_filter == "All" or status in status_filter)
                
                # Check search filter
                search_match = (not search_term or 
                               search_term in table.lower() or 
                               search_term in db_name.lower() or
                               search_term in status.lower())
                
                # Show or hide item
                if status_match and search_match:
                    try:
                        self.results_tree.reattach(item, "", 0)  # Show item
                    except:
                        pass  # Item already attached
                else:
                    try:
                        self.results_tree.detach(item)  # Hide item
                    except:
                        pass  # Item already detached
            except:
                pass  # Item might have been deleted
                
    def _clear_filter(self):
        """Clear all filters and show all results."""
        self.status_filter_var.set("All")
        self.search_var.set("")
        
        # Re-attach all items
        for item in self.all_tree_items:
            try:
                self.results_tree.reattach(item, "", 0)
            except:
                pass

