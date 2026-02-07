# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Legacy Schema Validation Tab - Python-only validation (DB2 vs Azure SQL).
Uses azure_migration_tool.validation; no PySpark required.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from pathlib import Path
import threading
import sys
import os
import json
from datetime import datetime
import logging

# Add parent directories to path
if getattr(sys, 'frozen', False):
    # Running as compiled exe
    if hasattr(sys, '_MEIPASS'):
        base_dir = Path(sys._MEIPASS)
    else:
        base_dir = Path(sys.executable).parent
    sys.path.insert(0, str(base_dir))
else:
    # Running as script
    parent_dir = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(parent_dir))

# Import log console for streaming logs
try:
    from gui.utils.log_console import log_to_console
    LOG_CONSOLE_AVAILABLE = True
except ImportError:
    LOG_CONSOLE_AVAILABLE = False
    log_to_console = None

# Import ConnectionWidget
from gui.widgets.connection_widget import ConnectionWidget


class LegacySchemaValidationTab:
    """Legacy schema validation tab using Python-only validation (no PySpark)."""
    
    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None
        self.validation_results = {}
        self.output_dir = None
        
        self._create_widgets()
        
    def set_project_path(self, project_path):
        """Set the current project path."""
        self.project_path = project_path
    
    def _log(self, message: str, level: int = logging.INFO, context: dict = None):
        """Log message to both the widget and the streaming console with context."""
        context_str = ""
        if context:
            parts = []
            if 'db' in context:
                parts.append(f"DB:{context['db']}")
            if 'schema' in context:
                parts.append(f"Schema:{context['schema']}")
            if 'step' in context:
                parts.append(f"Step:{context['step']}")
            if parts:
                context_str = f"[{', '.join(parts)}] "
        
        full_message = f"{context_str}{message}"
        
        def update_widget():
            try:
                if hasattr(self, 'validation_log') and self.validation_log:
                    self.validation_log.insert(tk.END, f"{full_message}\n")
                    self.validation_log.see(tk.END)
            except Exception:
                pass
        
        try:
            self.frame.after(0, update_widget)
        except Exception:
            pass
        
        if LOG_CONSOLE_AVAILABLE and log_to_console:
            log_to_console(f"[LegacySchemaValidation] {full_message}", level)
    
    def _update_status(self, status: str, color: str = "black"):
        """Update status label (thread-safe)."""
        def update():
            try:
                self.status_label.config(text=status, foreground=color)
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
    
    def _reset_progress(self):
        """Reset progress bar and status."""
        def reset():
            try:
                self.progress_var.set(0)
                self.progress_text.config(text="")
                self.status_label.config(text="Ready", foreground="gray")
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
            text="Compare DB2 to Azure (Schema)",
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=10)
        
        # Info label (user-friendly)
        info_label = tk.Label(
            scrollable_frame,
            text="Checks that tables, views, and other objects match between IBM DB2 and Azure SQL. DB2 connection requires Java and db2jcc4.jar. If you don't have DB2, use the \"Schema Validation\" tab instead.",
            font=("Arial", 9),
            fg="gray",
            wraplength=700,
            justify=tk.LEFT
        )
        info_label.pack(pady=(0, 10), anchor=tk.W, padx=0)
        
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
        
        self.scrollable_frame = scrollable_frame
        
        # Left column - Source Database
        left_frame = ttk.LabelFrame(main_frame, text="Source Database", padding=10)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        # Source connection variables
        self.src_server_var = tk.StringVar()
        self.src_db_var = tk.StringVar()
        self.src_auth_var = tk.StringVar(value="sql")
        self.src_user_var = tk.StringVar()
        self.src_password_var = tk.StringVar()
        self.src_db_type_var = tk.StringVar(value="db2")
        self.src_port_var = tk.StringVar(value="50000")
        self.src_schema_var = tk.StringVar()
        
        # Create connection widget for source
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
        
        # Right column - Destination Database
        right_frame = ttk.LabelFrame(main_frame, text="Destination Database", padding=10)
        right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        # Destination connection variables
        self.dest_server_var = tk.StringVar()
        self.dest_db_var = tk.StringVar()
        self.dest_auth_var = tk.StringVar(value="sql")
        self.dest_user_var = tk.StringVar()
        self.dest_password_var = tk.StringVar()
        self.dest_db_type_var = tk.StringVar(value="sqlserver")
        self.dest_port_var = tk.StringVar(value="1433")
        self.dest_schema_var = tk.StringVar()
        
        # Create connection widget for destination
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
        
        # Validation options frame
        options_frame = ttk.LabelFrame(scrollable_frame, text="Schema Validation Checks", padding=10)
        options_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Object types
        obj_frame = ttk.Frame(options_frame)
        obj_frame.pack(fill=tk.X, pady=5)
        tk.Label(obj_frame, text="Object Types:").pack(side=tk.LEFT)
        
        self.tables_var = tk.BooleanVar(value=True)
        self.views_var = tk.BooleanVar(value=True)
        self.procs_var = tk.BooleanVar(value=True)
        self.funcs_var = tk.BooleanVar(value=True)
        self.triggers_var = tk.BooleanVar(value=True)
        self.indexes_var = tk.BooleanVar(value=True)
        self.constraints_var = tk.BooleanVar(value=True)
        self.sequences_var = tk.BooleanVar(value=True)
        
        ttk.Checkbutton(obj_frame, text="Tables", variable=self.tables_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(obj_frame, text="Views", variable=self.views_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(obj_frame, text="Procedures", variable=self.procs_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(obj_frame, text="Functions", variable=self.funcs_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(obj_frame, text="Triggers", variable=self.triggers_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(obj_frame, text="Indexes", variable=self.indexes_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(obj_frame, text="Constraints", variable=self.constraints_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(obj_frame, text="Sequences", variable=self.sequences_var).pack(side=tk.LEFT, padx=5)
        
        # Validation checks
        checks_frame = ttk.Frame(options_frame)
        checks_frame.pack(fill=tk.X, pady=5)
        tk.Label(checks_frame, text="Checks:").pack(side=tk.LEFT)
        
        self.presence_var = tk.BooleanVar(value=True)
        self.datatypes_var = tk.BooleanVar(value=True)
        self.defaults_var = tk.BooleanVar(value=True)
        self.indexes_check_var = tk.BooleanVar(value=True)
        self.fk_var = tk.BooleanVar(value=True)
        self.nullable_var = tk.BooleanVar(value=True)
        self.check_constraints_var = tk.BooleanVar(value=True)
        
        ttk.Checkbutton(checks_frame, text="Object Presence", variable=self.presence_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(checks_frame, text="Data Types", variable=self.datatypes_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(checks_frame, text="Default Values", variable=self.defaults_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(checks_frame, text="Indexes", variable=self.indexes_check_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(checks_frame, text="Foreign Keys", variable=self.fk_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(checks_frame, text="Nullable", variable=self.nullable_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(checks_frame, text="Check Constraints", variable=self.check_constraints_var).pack(side=tk.LEFT, padx=5)
        
        # Output directory
        output_frame = ttk.Frame(options_frame)
        output_frame.pack(fill=tk.X, pady=5)
        tk.Label(output_frame, text="Output Directory:").pack(side=tk.LEFT)
        self.output_dir_var = tk.StringVar(value=str(Path.home() / "Desktop" / "validation_outputs"))
        ttk.Entry(output_frame, textvariable=self.output_dir_var, width=50).pack(side=tk.LEFT, padx=5)
        ttk.Button(output_frame, text="Browse...", command=self._browse_output_dir).pack(side=tk.LEFT)
        
        # Buttons frame
        buttons_frame = ttk.Frame(scrollable_frame)
        buttons_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.run_btn = ttk.Button(buttons_frame, text="▶ Run Schema Validation", command=self._run_validation)
        self.run_btn.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(buttons_frame, text="📂 Open Output Folder", command=self._open_output_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="📊 Export to Excel", command=self._export_to_excel).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="🔄 Clear Results", command=self._clear_results).pack(side=tk.LEFT, padx=5)
        
        # Progress frame
        progress_frame = ttk.Frame(scrollable_frame)
        progress_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=2)
        
        self.progress_text = tk.Label(progress_frame, text="", font=("Arial", 9))
        self.progress_text.pack(anchor=tk.W)
        
        self.status_label = ttk.Label(progress_frame, text="Ready", foreground="gray")
        self.status_label.pack(anchor=tk.W)
        
        # Results frame with notebook for different result types
        results_frame = ttk.LabelFrame(scrollable_frame, text="Validation Results", padding=10)
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Results notebook
        self.results_notebook = ttk.Notebook(results_frame)
        self.results_notebook.pack(fill=tk.BOTH, expand=True)
        
        # Object Presence tab
        presence_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(presence_frame, text="Object Presence")
        columns = ("ObjectType", "SourceSchema", "SourceObject", "DestSchema", "DestObject", "ChangeType", "ElementPath")
        self.presence_tree = self._create_treeview(presence_frame, columns)
        
        # Data Types tab
        datatypes_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(datatypes_frame, text="Data Types")
        columns = ("ObjectType", "SourceSchema", "SourceObject", "Column", "SourceType", "DestType", "ExpectedType", "Status")
        self.datatypes_tree = self._create_treeview(datatypes_frame, columns)
        
        # Default Values tab
        defaults_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(defaults_frame, text="Default Values")
        columns = ("ObjectType", "SourceSchema", "SourceObject", "Column", "SourceDefault", "DestDefault", "Match", "Status")
        self.defaults_tree = self._create_treeview(defaults_frame, columns)
        
        # Indexes tab
        indexes_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(indexes_frame, text="Indexes")
        columns = ("ObjectType", "SourceSchema", "SourceObject", "IndexName", "SourceCols", "DestCols", "Status")
        self.indexes_tree = self._create_treeview(indexes_frame, columns)
        
        # Foreign Keys tab
        fk_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(fk_frame, text="Foreign Keys")
        columns = ("ObjectType", "SourceSchema", "SourceObject", "FkName", "SourceRefTable", "DestRefTable", "Status")
        self.fk_tree = self._create_treeview(fk_frame, columns)
        
        # Nullable tab
        nullable_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(nullable_frame, text="Nullable")
        columns = ("ObjectType", "SourceSchema", "SourceObject", "Column", "SourceNullable", "DestNullable", "Status")
        self.nullable_tree = self._create_treeview(nullable_frame, columns)
        
        # Summary tab
        summary_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(summary_frame, text="Summary")
        self.summary_text = scrolledtext.ScrolledText(summary_frame, height=10, state=tk.DISABLED)
        self.summary_text.pack(fill=tk.BOTH, expand=True)
        
        # Log frame
        log_frame = ttk.LabelFrame(scrollable_frame, text="Validation Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.validation_log = scrolledtext.ScrolledText(log_frame, height=8)
        self.validation_log.pack(fill=tk.BOTH, expand=True)
    
    def _create_treeview(self, parent, columns):
        """Create a treeview with given columns."""
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=8)
        
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=100, minwidth=50)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        return tree
    
    def _browse_output_dir(self):
        """Browse for output directory."""
        directory = filedialog.askdirectory(title="Select Output Directory")
        if directory:
            self.output_dir_var.set(directory)
    
    def _open_output_folder(self):
        """Open the output folder in file explorer."""
        output_dir = self.output_dir_var.get()
        if os.path.exists(output_dir):
            os.startfile(output_dir)
        else:
            messagebox.showwarning("Warning", f"Output directory does not exist: {output_dir}")
    
    def _clear_results(self):
        """Clear all results."""
        for tree in [self.presence_tree, self.datatypes_tree, self.defaults_tree, 
                     self.indexes_tree, self.fk_tree, self.nullable_tree]:
            for item in tree.get_children():
                tree.delete(item)
        
        self.summary_text.config(state=tk.NORMAL)
        self.summary_text.delete(1.0, tk.END)
        self.summary_text.config(state=tk.DISABLED)
        
        self.validation_log.delete(1.0, tk.END)
        self.validation_results = {}
        self._reset_progress()
    
    def _map_auth_to_pyspark(self, auth_type: str) -> str:
        """Map GUI auth type to PySpark JDBC auth type."""
        mapping = {
            "sql": "SqlPassword",
            "entra_password": "ActiveDirectoryPassword",
            "entra_mfa": "ActiveDirectoryInteractive",
            "windows": "IntegratedSecurity",
        }
        return mapping.get(auth_type, "SqlPassword")
    
    def _create_database_config(self):
        """Create database_config.json from user inputs."""
        # Map auth types
        src_auth = self._map_auth_to_pyspark(self.src_auth_var.get())
        dest_auth = self._map_auth_to_pyspark(self.dest_auth_var.get())
        
        src_db_type = self.src_db_type_var.get().strip().lower()
        dest_db_type = self.dest_db_type_var.get().strip().lower()
        
        config = {
            "source_db_type": src_db_type,
            "destination_db_type": dest_db_type,
        }
        
        # Build source config based on type
        if src_db_type == "db2":
            config["db2"] = {
                "host": self.src_server_var.get().strip(),
                "port": int(self.src_port_var.get().strip() or "50000"),
                "database": self.src_db_var.get().strip(),
                "username": self.src_user_var.get().strip(),
                "password": self.src_password_var.get()
            }
        else:
            # SQL Server source
            config["source_sql"] = {
                "server": self.src_server_var.get().strip(),
                "port": int(self.src_port_var.get().strip() or "1433"),
                "database": self.src_db_var.get().strip(),
                "username": self.src_user_var.get().strip(),
                "password": self.src_password_var.get(),
                "authentication": src_auth,
                "encrypt": "yes",
                "trust_server_certificate": "yes"
            }
        
        # Build destination config based on type
        if dest_db_type == "db2":
            config["dest_db2"] = {
                "host": self.dest_server_var.get().strip(),
                "port": int(self.dest_port_var.get().strip() or "50000"),
                "database": self.dest_db_var.get().strip(),
                "username": self.dest_user_var.get().strip(),
                "password": self.dest_password_var.get()
            }
        else:
            # SQL Server / Azure SQL destination
            config["azure_sql"] = {
                "server": self.dest_server_var.get().strip(),
                "port": int(self.dest_port_var.get().strip() or "1433"),
                "database": self.dest_db_var.get().strip(),
                "username": self.dest_user_var.get().strip(),
                "password": self.dest_password_var.get(),
                "authentication": dest_auth,
                "encrypt": "yes",
                "trust_server_certificate": "yes"
            }
        
        return config
    
    def _validate_inputs(self):
        """Validate user inputs."""
        errors = []
        
        if not self.src_server_var.get().strip():
            errors.append("Source server is required")
        if not self.src_db_var.get().strip():
            errors.append("Source database is required")
        if not self.src_user_var.get().strip():
            errors.append("Source user is required")
        if not self.src_password_var.get():
            errors.append("Source password is required")
            
        if not self.dest_server_var.get().strip():
            errors.append("Destination server is required")
        if not self.dest_db_var.get().strip():
            errors.append("Destination database is required")
        
        checks = [self.presence_var, self.datatypes_var, self.defaults_var, 
                  self.indexes_check_var, self.fk_var, self.nullable_var, self.check_constraints_var]
        if not any(v.get() for v in checks):
            errors.append("At least one validation check must be selected")
        
        return errors
    
    def _get_object_types(self):
        """Get list of selected object types."""
        types = []
        if self.tables_var.get():
            types.append("TABLE")
        if self.views_var.get():
            types.append("VIEW")
        if self.procs_var.get():
            types.append("PROCEDURE")
        if self.funcs_var.get():
            types.append("FUNCTION")
        if self.triggers_var.get():
            types.append("TRIGGER")
        if self.indexes_var.get():
            types.append("INDEX")
        if self.constraints_var.get():
            types.append("CONSTRAINT")
        if self.sequences_var.get():
            types.append("SEQUENCE")
        return types
    
    def _run_validation(self):
        """Run the validation in a background thread."""
        errors = self._validate_inputs()
        if errors:
            messagebox.showerror("Validation Error", "\n".join(errors))
            return
        
        self.run_btn.config(state=tk.DISABLED)
        self._clear_results()
        
        thread = threading.Thread(target=self._validation_worker, daemon=True)
        thread.start()
    
    def _validation_worker(self):
        """Background worker for validation."""
        try:
            self._update_status("Initializing...", "blue")
            self._log("Starting legacy schema validation...")
            
            # Output directory for CSV results only (no config file stored)
            output_dir = self.output_dir_var.get().strip() or str(Path.home() / "Desktop" / "validation_outputs")
            os.makedirs(output_dir, exist_ok=True)
            config = self._create_database_config()
            os.environ["VALIDATION_OUTPUT_DIR"] = output_dir
            
            # Log source connection info
            if config.get("source_db_type", "db2").lower() == "db2":
                src_info = config.get("db2", {})
                self._log(f"Source: {src_info.get('host', '')}:{src_info.get('port', '')}/{src_info.get('database', '')}")
            else:
                src_info = config.get("source_sql", {})
                self._log(f"Source: {src_info.get('server', '')}:{src_info.get('port', '')}/{src_info.get('database', '')}")
            
            # Log destination connection info
            if config.get("destination_db_type", "sqlserver").lower() == "db2":
                dest_info = config.get("dest_db2", {})
                self._log(f"Destination: {dest_info.get('host', '')}:{dest_info.get('port', '')}/{dest_info.get('database', '')}")
            else:
                dest_info = config.get("azure_sql", {})
                self._log(f"Destination: {dest_info.get('server', '')}/{dest_info.get('database', '')}")
            
            self._log(f"Results will be saved to: {output_dir}")
            
            # Import the validation service (Python-only, no PySpark)
            self._log("Importing validation module...")
            try:
                from azure_migration_tool.validation.schema_service import LegacySchemaValidationService
            except ImportError:
                try:
                    from validation.schema_service import LegacySchemaValidationService
                except ImportError as e2:
                    error_msg = str(e2)
                    self._log(f"ERROR: Failed to import module: {error_msg}", logging.ERROR)
                    self._update_status(f"Import error: {e2}", "red")
                    self._show_error(
                        "Could not load the validation module.\n\n"
                        "This tab compares IBM DB2 to Azure SQL (no PySpark required). "
                        "If you're comparing two SQL Server/Azure databases, use the \"Schema Validation\" tab instead.\n\n"
                        f"Details: {error_msg}"
                    )
                    return
            
            # Initialize service
            self._log("Initializing LegacySchemaValidationService...")
            
            # Log config info for debugging
            src_type = config.get("source_db_type", "unknown")
            dest_type = config.get("destination_db_type", "unknown")
            self._log(f"Source database type: {src_type}")
            self._log(f"Destination database type: {dest_type}")
            
            try:
                service = LegacySchemaValidationService(config=config, output_dir=output_dir)
            except Exception as e:
                import traceback
                self._log(f"ERROR: Failed to initialize service: {e}", logging.ERROR)
                self._log(traceback.format_exc())
                self._update_status(f"Init error: {e}", "red")
                self._show_error(f"Failed to initialize validation service.\n\n{e}")
                return
            
            source_schema = self.src_schema_var.get().strip() or None
            target_schema = self.dest_schema_var.get().strip() or None
            object_types = self._get_object_types()
            
            self._log(f"Source schema: {source_schema or 'ALL'}")
            self._log(f"Target schema: {target_schema or 'ALL'}")
            self._log(f"Object types: {object_types}")
            
            # Count total checks
            checks = [
                (self.presence_var, "presence"),
                (self.datatypes_var, "datatypes"),
                (self.defaults_var, "defaults"),
                (self.indexes_check_var, "indexes"),
                (self.fk_var, "foreign_keys"),
                (self.nullable_var, "nullable"),
                (self.check_constraints_var, "check_constraints"),
            ]
            total_steps = sum(1 for v, _ in checks if v.get())
            current_step = 0
            results = []
            
            # 1. Schema Presence
            if self.presence_var.get():
                current_step += 1
                self._update_progress(current_step, total_steps, "Checking object presence...")
                self._log("Running object presence comparison...", context={"step": "presence"})
                
                try:
                    df = service.compare_schema_presence(
                        source_schema=source_schema,
                        target_schema=target_schema,
                        object_types=object_types
                    )
                    count = len(df)
                    # Count unique objects being compared
                    if not df.empty:
                        src_objects = df[df["SourceObjectName"] != ""]["SourceObjectName"].nunique()
                        dest_objects = df[df["DestinationObjectName"] != ""]["DestinationObjectName"].nunique()
                        missing_target = len(df[df["ChangeType"] == "MISSING_IN_TARGET"]) if "ChangeType" in df.columns else 0
                        missing_source = len(df[df["ChangeType"] == "MISSING_IN_SOURCE"]) if "ChangeType" in df.columns else 0
                        self._log(f"Object presence check: {src_objects} source objects, {dest_objects} destination objects")
                        self._log(f"  Found {count} differences: {missing_target} missing in target, {missing_source} missing in source")
                        if count > 0:
                            # Show first few examples
                            sample = df.head(3)
                            for _, row in sample.iterrows():
                                change = row.get("ChangeType", "")
                                src_obj = row.get("SourceObjectName", "")
                                dest_obj = row.get("DestinationObjectName", "")
                                self._log(f"  Example: {change} - Source: {src_obj}, Dest: {dest_obj}")
                    else:
                        self._log(f"Object presence check: DataFrame is empty - no objects found or no differences")
                        self._log(f"  This might indicate: 1) No objects in databases, 2) All objects match, or 3) Connection issue")
                    
                    self._load_presence_results(df)
                    results.append(("Object Presence", count, None))
                    self.validation_results["presence"] = df
                    self._log(f"Stored presence results: {len(df)} rows in validation_results['presence']")
                except Exception as e:
                    import traceback
                    self._log(f"ERROR in presence check: {e}", logging.ERROR)
                    self._log(traceback.format_exc())
                    results.append(("Object Presence", "ERROR", str(e)))
            
            # 2. Data Types
            if self.datatypes_var.get():
                current_step += 1
                self._update_progress(current_step, total_steps, "Checking column data types...")
                self._log("Running data types comparison...", context={"step": "datatypes"})
                
                try:
                    df = service.compare_column_datatypes_mapped(
                        source_schema=source_schema,
                        target_schema=target_schema,
                        object_types=["TABLE"] if "TABLE" in object_types else object_types
                    )
                    count = len(df)
                    # Count unique tables being compared
                    if not df.empty:
                        unique_tables = df.groupby(["SourceSchemaName", "SourceObjectName"]).size().shape[0]
                        unique_columns = df["ColumnName"].nunique()
                        self._log(f"Data types check: Compared {unique_tables} table(s), {unique_columns} column(s), found {count} differences")
                    else:
                        self._log(f"Data types check: No tables matched or no differences found")
                    
                    self._load_datatypes_results(df)
                    results.append(("Data Types", count, None))
                    self.validation_results["datatypes"] = df
                except Exception as e:
                    import traceback
                    self._log(f"ERROR in datatypes check: {e}", logging.ERROR)
                    self._log(traceback.format_exc())
                    results.append(("Data Types", "ERROR", str(e)))
            
            # 3. Default Values
            if self.defaults_var.get():
                current_step += 1
                self._update_progress(current_step, total_steps, "Checking default values...")
                self._log("Running default values comparison...", context={"step": "defaults"})
                
                try:
                    df = service.compare_column_default_values(
                        source_schema=source_schema,
                        target_schema=target_schema,
                        object_types=["TABLE"] if "TABLE" in object_types else object_types
                    )
                    count = len(df)
                    self._log(f"Default values check found {count} differences")
                    
                    csv_path = service.save_comparison_to_csv(df, "default_values")
                    self._log(f"Saved to: {csv_path}")
                    
                    self._load_defaults_results(df)
                    results.append(("Default Values", count, csv_path))
                    self.validation_results["defaults"] = df
                except Exception as e:
                    self._log(f"ERROR in defaults check: {e}", logging.ERROR)
                    results.append(("Default Values", "ERROR", str(e)))
            
            # 4. Indexes
            if self.indexes_check_var.get():
                current_step += 1
                self._update_progress(current_step, total_steps, "Checking index definitions...")
                self._log("Running index comparison...", context={"step": "indexes"})
                
                try:
                    df = service.compare_index_definitions(
                        source_schema=source_schema,
                        target_schema=target_schema,
                        object_types=["TABLE"] if "TABLE" in object_types else object_types
                    )
                    count = len(df)
                    self._log(f"Index check found {count} differences")
                    
                    self._load_indexes_results(df)
                    results.append(("Indexes", count, None))
                    self.validation_results["indexes"] = df
                except Exception as e:
                    self._log(f"ERROR in index check: {e}", logging.ERROR)
                    results.append(("Indexes", "ERROR", str(e)))
            
            # 5. Foreign Keys
            if self.fk_var.get():
                current_step += 1
                self._update_progress(current_step, total_steps, "Checking foreign keys...")
                self._log("Running foreign key comparison...", context={"step": "foreign_keys"})
                
                try:
                    df = service.compare_foreign_keys(
                        source_schema=source_schema,
                        target_schema=target_schema,
                        object_types=["TABLE"] if "TABLE" in object_types else object_types
                    )
                    count = len(df)
                    self._log(f"Foreign key check found {count} differences")
                    
                    self._load_fk_results(df)
                    results.append(("Foreign Keys", count, None))
                    self.validation_results["foreign_keys"] = df
                except Exception as e:
                    self._log(f"ERROR in FK check: {e}", logging.ERROR)
                    results.append(("Foreign Keys", "ERROR", str(e)))
            
            # 6. Nullable
            if self.nullable_var.get():
                current_step += 1
                self._update_progress(current_step, total_steps, "Checking nullable constraints...")
                self._log("Running nullable comparison...", context={"step": "nullable"})
                
                try:
                    df = service.compare_column_nullable_constraints(
                        source_schema=source_schema,
                        target_schema=target_schema,
                        object_types=["TABLE"] if "TABLE" in object_types else object_types
                    )
                    count = len(df)
                    self._log(f"Nullable check found {count} differences")
                    
                    self._load_nullable_results(df)
                    results.append(("Nullable", count, None))
                    self.validation_results["nullable"] = df
                except Exception as e:
                    self._log(f"ERROR in nullable check: {e}", logging.ERROR)
                    results.append(("Nullable", "ERROR", str(e)))
            
            # 7. Check Constraints
            if self.check_constraints_var.get():
                current_step += 1
                self._update_progress(current_step, total_steps, "Checking check constraints...")
                self._log("Running check constraints comparison...", context={"step": "check_constraints"})
                
                try:
                    df = service.compare_check_constraints(
                        source_schema=source_schema,
                        target_schema=target_schema,
                        object_types=["TABLE"] if "TABLE" in object_types else object_types
                    )
                    count = len(df)
                    self._log(f"Check constraints found {count} differences")
                    
                    results.append(("Check Constraints", count, None))
                    self.validation_results["check_constraints"] = df
                except Exception as e:
                    self._log(f"ERROR in check constraints: {e}", logging.ERROR)
                    results.append(("Check Constraints", "ERROR", str(e)))
            
            # Write single consolidated file (errors only) to output_dir
            try:
                import uuid
                
                # Log summary of what was compared BEFORE building consolidated errors
                total_validations = len([k for k in self.validation_results.keys() if self.validation_results[k] is not None])
                self._log(f"Completed {total_validations} validation check(s)")
                for val_type, df in self.validation_results.items():
                    if df is not None:
                        count = len(df) if hasattr(df, '__len__') else 0
                        self._log(f"  {val_type}: {count} results stored")
                        if count > 0 and val_type == "presence":
                            # Show sample of presence results
                            sample = df.head(3) if hasattr(df, 'head') else []
                            for _, row in sample.iterrows():
                                change = row.get("ChangeType", "N/A")
                                self._log(f"    Sample: {change}")
                
                errors_df = self._build_consolidated_errors_df()
                self._log(f"Consolidated errors DataFrame: {len(errors_df)} rows")
                if not errors_df.empty:
                    self._log(f"  Error types: {errors_df['ValidationType'].value_counts().to_dict() if 'ValidationType' in errors_df.columns else 'N/A'}")
                
                db_name = (self.dest_db_var.get() or "validation").strip().replace("/", "_").replace("\\", "_")
                host_name = (self.src_server_var.get() or "host").strip().replace(".", "_").replace(":", "_")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                unique_id = uuid.uuid4().hex[:6]
                single_filename = f"schema_validate_all_{db_name}_{host_name}_{timestamp}_{unique_id}.csv"
                single_path = os.path.join(output_dir, single_filename)
                
                # Always create the file, even if empty (no errors)
                errors_df.to_csv(single_path, index=False)
                if not errors_df.empty:
                    self._log(f"Saved report (errors only) to: {single_path}")
                    self._log(f"Found {len(errors_df)} error(s) in consolidated report")
                    results = [("Schema validation report (errors only)", len(errors_df), single_path)]
                else:
                    self._log(f"Saved report (no errors found) to: {single_path}")
                    self._log("No validation errors found - schemas match!")
                    results = [("Schema validation report (errors only)", 0, single_path)]
            except Exception as e:
                import traceback
                self._log(f"Failed to write consolidated report: {e}", logging.ERROR)
                self._log(traceback.format_exc())
                results = [("Schema validation report", 0, f"ERROR: {e}")]
            
            # Update summary
            self._update_summary(results)
            
            self._update_progress(total_steps, total_steps, "Validation complete!")
            self._update_status("Validation completed successfully!", "green")
            self._log("=" * 50)
            self._log("SCHEMA VALIDATION COMPLETE")
            self._log("=" * 50)
            
            # No Spark to stop (Python-only validation)
            
        except Exception as e:
            self._log(f"CRITICAL ERROR: {e}", logging.ERROR)
            self._update_status(f"Error: {e}", "red")
            import traceback
            self._log(traceback.format_exc())
            self._show_error(f"Validation failed.\n\n{e}\n\nCheck the log for details.")
        finally:
            self._enable_run_button()
    
    def _load_presence_results(self, spark_df):
        """Load presence results into treeview (accepts pandas or Spark DataFrame)."""
        def update():
            try:
                pdf = spark_df.toPandas() if hasattr(spark_df, 'toPandas') else spark_df
                for _, row in pdf.iterrows():
                    values = (
                        row.get("ObjectType", ""),
                        row.get("SourceSchemaName", ""),
                        row.get("SourceObjectName", ""),
                        row.get("DestinationSchemaName", ""),
                        row.get("DestinationObjectName", ""),
                        row.get("ChangeType", ""),
                        row.get("ElementPath", "")
                    )
                    self.presence_tree.insert("", tk.END, values=values)
            except Exception as e:
                self._log(f"Error loading presence results: {e}")
        self.frame.after(0, update)
    
    def _load_datatypes_results(self, df):
        """Load datatypes results into treeview (accepts pandas DataFrame)."""
        if hasattr(df, 'toPandas'):
            df = df.toPandas()
        def update():
            try:
                for _, row in df.iterrows():
                    values = (
                        row.get("ObjectType", ""),
                        row.get("SourceSchemaName", ""),
                        row.get("SourceObjectName", ""),
                        row.get("ColumnName", ""),
                        row.get("SourceDataType", ""),
                        row.get("DestinationDataType", ""),
                        row.get("ExpectedAzureType", ""),
                        row.get("Status", "")
                    )
                    self.datatypes_tree.insert("", tk.END, values=values)
            except Exception as e:
                self._log(f"Error loading datatypes results: {e}")
        self.frame.after(0, update)
    
    def _load_defaults_results(self, spark_df):
        """Load defaults results into treeview (accepts pandas or Spark DataFrame)."""
        def update():
            try:
                pdf = spark_df.toPandas() if hasattr(spark_df, 'toPandas') else spark_df
                for _, row in pdf.iterrows():
                    values = (
                        row.get("ObjectType", ""),
                        row.get("SourceSchemaName", ""),
                        row.get("SourceObjectName", ""),
                        row.get("ColumnName", ""),
                        row.get("SourceDefault", ""),
                        row.get("DestinationDefault", ""),
                        row.get("DefaultMatch", ""),
                        row.get("Status", "")
                    )
                    self.defaults_tree.insert("", tk.END, values=values)
            except Exception as e:
                self._log(f"Error loading defaults results: {e}")
        self.frame.after(0, update)
    
    def _load_indexes_results(self, df):
        """Load indexes results into treeview (accepts pandas DataFrame)."""
        if hasattr(df, 'toPandas'):
            df = df.toPandas()
        def update():
            try:
                for _, row in df.iterrows():
                    values = (
                        row.get("ObjectType", ""),
                        row.get("SourceSchemaName", ""),
                        row.get("SourceObjectName", ""),
                        row.get("IndexName", ""),
                        row.get("SourceCols", ""),
                        row.get("DestinationCols", ""),
                        row.get("Status", "")
                    )
                    self.indexes_tree.insert("", tk.END, values=values)
            except Exception as e:
                self._log(f"Error loading indexes results: {e}")
        self.frame.after(0, update)
    
    def _load_fk_results(self, df):
        """Load foreign key results into treeview (accepts pandas DataFrame)."""
        if hasattr(df, 'toPandas'):
            df = df.toPandas()
        def update():
            try:
                for _, row in df.iterrows():
                    values = (
                        row.get("ObjectType", ""),
                        row.get("SourceSchemaName", ""),
                        row.get("SourceObjectName", ""),
                        row.get("FkName", ""),
                        row.get("SourceRefTable", ""),
                        row.get("DestinationRefTable", ""),
                        row.get("Status", "")
                    )
                    self.fk_tree.insert("", tk.END, values=values)
            except Exception as e:
                self._log(f"Error loading FK results: {e}")
        self.frame.after(0, update)
    
    def _load_nullable_results(self, df):
        """Load nullable results into treeview (accepts pandas DataFrame)."""
        if hasattr(df, 'toPandas'):
            df = df.toPandas()
        def update():
            try:
                for _, row in df.iterrows():
                    values = (
                        row.get("ObjectType", ""),
                        row.get("SourceSchemaName", ""),
                        row.get("SourceObjectName", ""),
                        row.get("ColumnName", ""),
                        row.get("SourceNullable", ""),
                        row.get("DestinationNullable", ""),
                        row.get("Status", "")
                    )
                    self.nullable_tree.insert("", tk.END, values=values)
            except Exception as e:
                self._log(f"Error loading nullable results: {e}")
        self.frame.after(0, update)
    
    def _update_summary(self, results):
        """Update the summary tab."""
        def update():
            self.summary_text.config(state=tk.NORMAL)
            self.summary_text.delete(1.0, tk.END)
            
            summary = []
            summary.append("=" * 60)
            summary.append("SCHEMA VALIDATION SUMMARY")
            summary.append("=" * 60)
            summary.append(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            summary.append(f"\nSource: {self.src_server_var.get()}/{self.src_db_var.get()}")
            summary.append(f"Source Schema: {self.src_schema_var.get() or 'ALL'}")
            summary.append(f"Destination: {self.dest_server_var.get()}/{self.dest_db_var.get()}")
            summary.append(f"Destination Schema: {self.dest_schema_var.get() or 'ALL'}")
            summary.append(f"\nOutput Directory: {self.output_dir_var.get()}")
            summary.append("\n" + "-" * 60)
            summary.append(f"\n{'Validation Type':<25} {'Results':<15} {'Output File'}")
            summary.append("-" * 60)
            
            total_issues = 0
            for item in results:
                if not isinstance(item, (list, tuple)) or len(item) != 3:
                    continue
                name, count, path = item[0], item[1], item[2]
                if path and not str(path).startswith("ERROR"):
                    filename = os.path.basename(str(path))
                else:
                    filename = str(path)
                summary.append(f"{name:<25} {str(count):<15} {filename}")
                if isinstance(count, int):
                    total_issues += count
            
            summary.append("-" * 60)
            summary.append(f"{'TOTAL ISSUES':<25} {total_issues}")
            
            self.summary_text.insert(tk.END, "\n".join(summary))
            self.summary_text.config(state=tk.DISABLED)
        
        self.frame.after(0, update)
    
    def _enable_run_button(self):
        """Re-enable the run button."""
        def enable():
            self.run_btn.config(state=tk.NORMAL)
        self.frame.after(0, enable)
    
    def _show_error(self, message):
        """Show error message box (thread-safe)."""
        def show():
            messagebox.showerror("Error", message)
        self.frame.after(0, show)
    
    def _build_consolidated_errors_df(self):
        """Build consolidated DataFrame with only Status=error rows. Returns empty DataFrame if no errors."""
        import pandas as pd
        all_rows = []
        for validation_type, df in self.validation_results.items():
                if df is None:
                    continue
                pdf = df.toPandas() if hasattr(df, 'toPandas') else df
                if not hasattr(pdf, "iterrows"):
                    continue
                for _, row in pdf.iterrows():
                    try:
                        # Extract base info
                        src_schema = str(row.get('SourceSchemaName', '') or '')
                        dest_schema = str(row.get('DestinationSchemaName', '') or '')
                        src_obj = str(row.get('SourceObjectName', row.get('SourceTableName', '')) or '')
                        dest_obj = str(row.get('DestinationObjectName', row.get('DestinationTableName', '')) or '')
                        obj_type = str(row.get('ObjectType', 'TABLE') or 'TABLE')
                        status = str(row.get('Status', 'error') or 'error').lower()
                        error_desc = str(row.get('ErrorDescription', '') or '')
                        element_path = str(row.get('ElementPath', '') or '')
                        # Fallback: build schema.object when service does not return ElementPath
                        if not element_path and (src_schema or src_obj):
                            element_path = ".".join(p for p in (src_schema, src_obj) if p)
                        
                        # Determine ValidationType and ErrorCode based on validation_type and error description
                        if validation_type == 'presence':
                            val_type = 'presence'
                            # Service returns ChangeType, not ErrorDescription; match reference file wording
                            change_type = str(row.get('ChangeType', '') or '').strip().upper()
                            if change_type == 'MISSING_IN_TARGET' or not dest_obj:
                                error_code = 'PRESENCE_MISSING_IN_TARGET'
                                error_desc = error_desc or 'Object exists in source but not in target'
                                status = 'error'
                            elif change_type == 'MISSING_IN_SOURCE' or not src_obj:
                                error_code = 'PRESENCE_MISSING_IN_SOURCE'
                                error_desc = error_desc or 'Object exists in Azure SQL but not in DB2'
                                status = 'error'
                            elif 'source but not in target' in (error_desc or '').lower():
                                error_code = 'PRESENCE_MISSING_IN_TARGET'
                                error_desc = error_desc or 'Object exists in source but not in target'
                            elif 'Azure SQL but not in DB2' in (error_desc or '') or 'but not in source' in (error_desc or '').lower():
                                error_code = 'PRESENCE_MISSING_IN_SOURCE'
                                error_desc = error_desc or 'Object exists in Azure SQL but not in DB2'
                            else:
                                error_code = 'PRESENCE_MISMATCH'
                                error_desc = error_desc or 'Presence mismatch'
                            details = {}
                        
                        elif validation_type == 'datatypes':
                            val_type = 'datatype_mapping'
                            error_code = 'DATATYPE_NAME_MISMATCH'
                            col_name = str(row.get('ColumnName', '') or '')
                            src_type = str(row.get('SourceDataType', row.get('SourceType', '')) or '')
                            dest_type = str(row.get('DestinationDataType', row.get('DestinationType', '')) or '')
                            expected = str(row.get('ExpectedAzureType', '') or '')
                            actual = str(row.get('ActualAzureType', dest_type) or '')
                            details = {
                                "column_name": col_name,
                                "source_data_type": src_type,
                                "destination_data_type": dest_type,
                                "expected_azure_type": expected or src_type.upper(),
                                "actual_azure_type": actual.upper()
                            }
                            # Add precision/scale if available
                            if pd.notna(row.get('SourcePrecision')):
                                details['source_precision'] = int(row.get('SourcePrecision', 0))
                            if pd.notna(row.get('SourceScale')):
                                details['source_scale'] = int(row.get('SourceScale', 0))
                            if pd.notna(row.get('DestinationPrecision')):
                                details['destination_precision'] = int(row.get('DestinationPrecision', 0))
                            # Build or update element path (schema.table.column)
                            if not element_path and (src_schema or src_obj or col_name):
                                element_path = ".".join(p for p in (src_schema, src_obj, col_name) if p)
                            elif col_name and element_path and col_name not in element_path:
                                element_path = f"{element_path}.{col_name}"
                        
                        elif validation_type == 'defaults':
                            val_type = 'default_values'
                            status = 'warning'  # Original uses warning for default value differences
                            error_code = 'DEFAULT_VALUE_MISMATCH'
                            # Override error description to match original exactly
                            error_desc = 'Default value difference (treated as warning)'
                            col_name = str(row.get('ColumnName', '') or '')
                            src_def = str(row.get('SourceDefault', '') or '')
                            dest_def = str(row.get('DestinationDefault', '') or '')
                            details = {
                                "column_name": col_name,
                                "source_default": src_def,
                                "destination_default": dest_def
                            }
                            # Build or update element path (schema.table.column)
                            if not element_path and (src_schema or src_obj or col_name):
                                element_path = ".".join(p for p in (src_schema, src_obj, col_name) if p)
                            elif col_name and element_path and col_name not in element_path:
                                element_path = f"{element_path}.{col_name}"
                        
                        elif validation_type == 'indexes':
                            val_type = 'indexes'
                            idx_name = str(row.get('IndexName', '') or '')
                            src_cols = str(row.get('SourceCols', '') or '')
                            dest_cols = str(row.get('DestinationCols', '') or '')
                            src_unique = row.get('SourceUnique', None)
                            dest_unique = row.get('DestinationUnique', None)
                            # Service returns Status (e.g. MISSING_IN_TARGET), not ErrorDescription; match reference wording
                            row_status = str(row.get('Status', '') or '').strip().upper()
                            if row_status == 'MISSING_IN_TARGET' or 'missing in target' in (error_desc or '').lower():
                                error_code = 'INDEX_MISSING_IN_TARGET'
                                error_desc = error_desc or 'Index missing in target'
                                status = 'warning'
                            elif row_status == 'MISSING_IN_SOURCE' or 'missing in source' in (error_desc or '').lower():
                                error_code = 'INDEX_MISSING_IN_SOURCE'
                                error_desc = error_desc or 'Index missing in source'
                                status = 'warning'
                            elif 'column order' in (error_desc or '').lower():
                                error_code = 'INDEX_MISMATCH'
                                error_desc = error_desc or 'Index column order mismatch'
                                status = 'warning'
                            else:
                                error_code = 'INDEX_MISMATCH'
                                error_desc = error_desc or 'Index mismatch'
                                status = 'error'
                            # ObjectType should be TABLE for all index records (matching reference)
                            obj_type = 'TABLE'
                            # Build ElementPath as schema.table.indexname (reference format)
                            if not element_path and (src_schema or src_obj or idx_name):
                                element_path = ".".join(p for p in (src_schema, src_obj, idx_name) if p)
                            elif idx_name and element_path and idx_name not in element_path:
                                element_path = f"{element_path}.{idx_name}"
                            # Build details - always include index_name, source_cols, destination_cols
                            details = {"index_name": idx_name}
                            if src_unique is not None and pd.notna(src_unique):
                                details['source_unique'] = bool(src_unique)
                            if dest_unique is not None and pd.notna(dest_unique):
                                details['destination_unique'] = bool(dest_unique)
                            details['source_cols'] = src_cols
                            details['destination_cols'] = dest_cols
                        
                        elif validation_type == 'foreign_keys':
                            val_type = 'foreign_keys'
                            # Note: service uses 'FkName' not 'FKName'
                            fk_name = str(row.get('FkName', row.get('FKName', '')) or '')
                            
                            # Determine error code and status based on error description
                            if 'missing in source' in error_desc.lower():
                                error_code = 'FK_MISSING_IN_SOURCE'
                                status = 'error'
                            elif 'missing in target' in error_desc.lower():
                                error_code = 'FK_MISSING_IN_TARGET'
                                status = 'error'
                            elif 'delete action' in error_desc.lower():
                                error_code = 'FK_DELETE_ACTION_MISMATCH'
                                status = 'warning'
                            elif 'update action' in error_desc.lower():
                                error_code = 'FK_UPDATE_ACTION_MISMATCH'
                                status = 'warning'
                            else:
                                error_code = 'FK_MISMATCH'
                                status = 'warning'
                            
                            details = {"fk_name": fk_name}
                            # Add all FK details from row
                            for col in ['SourceRefSchema', 'SourceRefTable', 'DestinationRefSchema', 'DestinationRefTable',
                                       'SourcePairs', 'DestinationPairs', 'SourceDelete', 'DestinationDelete',
                                       'SourceUpdate', 'DestinationUpdate']:
                                val = row.get(col, '')
                                if val and pd.notna(val):
                                    # Convert column name to snake_case for JSON
                                    key = ''.join(['_' + c.lower() if c.isupper() else c for c in col]).lstrip('_')
                                    details[key] = str(val)
                            
                            # Add status field to details to match original format
                            details['status'] = status
                            # Build or update element path (schema.table.fk_name)
                            if not element_path and (src_schema or src_obj or fk_name):
                                element_path = ".".join(p for p in (src_schema, src_obj, fk_name) if p)
                            elif fk_name and element_path and fk_name not in element_path:
                                element_path = f"{element_path}.{fk_name}"
                        
                        elif validation_type == 'nullable':
                            val_type = 'nullable'
                            error_code = 'NULLABLE_MISMATCH'
                            col_name = str(row.get('ColumnName', '') or '')
                            details = {
                                "column_name": col_name,
                                "source_nullable": str(row.get('SourceNullable', '')),
                                "destination_nullable": str(row.get('DestinationNullable', ''))
                            }
                            # Build element path (schema.table.column)
                            if not element_path and (src_schema or src_obj or col_name):
                                element_path = ".".join(p for p in (src_schema, src_obj, col_name) if p)
                        
                        elif validation_type == 'check_constraints':
                            val_type = 'check_constraints'
                            error_code = 'CHECK_CONSTRAINT_MISMATCH'
                            constraint_name = str(row.get('ConstraintName', '') or '')
                            details = {"constraint_name": constraint_name} if constraint_name else {}
                            # Build element path (schema.table.constraint_name)
                            if not element_path and (src_schema or src_obj or constraint_name):
                                element_path = ".".join(p for p in (src_schema, src_obj, constraint_name) if p)
                        
                        else:
                            val_type = validation_type
                            error_code = 'SCHEMA_MISMATCH'
                            details = {}
                        
                        # Only include error rows (no warnings)
                        if status != 'error':
                            continue
                        all_rows.append({
                            'ValidationType': val_type,
                            'Status': status,
                            'ObjectType': obj_type,
                            'SourceObjectName': src_obj,
                            'SourceSchemaName': src_schema,
                            'DestinationObjectName': dest_obj,
                            'DestinationSchemaName': dest_schema,
                            'ElementPath': element_path,
                            'ErrorCode': error_code,
                            'ErrorDescription': error_desc,
                            # Use separators=(',',':') for compact JSON without spaces
                            'DetailsJson': json.dumps(details, separators=(',', ':')) if details else '{}'
                        })
                    except Exception as row_err:
                        self._log(f"Export skipped one row ({validation_type}): {row_err}")
        return pd.DataFrame(all_rows)

    def _export_to_excel(self):
        """Export consolidated report (errors only) to a single CSV file."""
        if not self.validation_results:
            messagebox.showwarning("Warning", "No results to export. Run validation first.")
            return
        try:
            import pandas as pd
            import uuid
            result_df = self._build_consolidated_errors_df()
            if result_df.empty:
                messagebox.showinfo("No Errors", "No schema validation errors found. Only errors are included in the report.")
                return
            db_name = self.dest_db_var.get().strip().replace("/", "_").replace("\\", "_") or "validation"
            host_name = self.src_server_var.get().strip().replace(".", "_").replace(":", "_") or "host"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            unique_id = uuid.uuid4().hex[:6]
            default_name = f"schema_validate_all_{db_name}_{host_name}_{timestamp}_{unique_id}.csv"
            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile=default_name,
                title="Export Consolidated Report (errors only)"
            )
            if not file_path:
                return
            result_df.to_csv(file_path, index=False)
            messagebox.showinfo("Success", f"Exported {len(result_df)} errors to:\n{file_path}")
            self._log(f"CSV report exported: {file_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {e}")
            import traceback
            self._log(f"Export error: {traceback.format_exc()}")
