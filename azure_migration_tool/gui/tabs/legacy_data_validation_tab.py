# Author: S@tish Chauhan

"""
Legacy Data Validation Tab - Python-only (no PySpark).
Compares DB2 to Azure SQL; same report format as before.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from pathlib import Path
import multiprocessing
import threading
import sys
import os
import json
from datetime import datetime
import logging

# Add parent directories to path
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
from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll


class LegacyDataValidationTab:
    """Legacy data validation tab using Python-only validation (no PySpark)."""
    
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
    
    def _console_log(self, message: str):
        """Print to CMD/stdout so progress is visible when running from terminal (helps when UI hangs)."""
        try:
            from datetime import datetime
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] [LegacyData] {message}", flush=True)
        except Exception:
            pass

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
            log_to_console(f"[LegacyDataValidation] {full_message}", level)
    
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
    
    def _is_driver_or_jvm_error(self, e: Exception) -> bool:
        """Return True if the exception is a DB2 driver or JVM classpath error."""
        msg = str(e)
        if isinstance(e, FileNotFoundError):
            return "JAR" in msg or "db2" in msg.lower() or "driver" in msg.lower()
        if isinstance(e, RuntimeError) and "JVM was already started" in msg:
            return True
        if "DB2Driver" in msg and "is not found" in msg.lower():
            return True
        if "DB2 JDBC driver" in msg or "driver JAR not found" in msg:
            return True
        return False

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
            text="Legacy Data Validation",
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=10)
        
        # Info label
        info_label = tk.Label(
            scrollable_frame,
            text="Python-only validation (no PySpark). DB2 source requires Java and db2jcc4.jar in the drivers folder.",
            font=("Arial", 9, "italic"),
            fg="gray"
        )
        info_label.pack(pady=(0, 10))
        
        # Create two-column layout
        main_frame = ttk.Frame(scrollable_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        bind_canvas_vertical_scroll(canvas, scrollable_frame)

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
        options_frame = ttk.LabelFrame(scrollable_frame, text="Validation Options", padding=10)
        options_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Object types
        tk.Label(options_frame, text="Object Types:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.tables_var = tk.BooleanVar(value=True)
        self.views_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(options_frame, text="Tables", variable=self.tables_var).grid(row=0, column=1, sticky=tk.W, padx=5)
        ttk.Checkbutton(options_frame, text="Views", variable=self.views_var).grid(row=0, column=2, sticky=tk.W, padx=5)
        
        # Validation checks
        tk.Label(options_frame, text="Checks:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.row_counts_var = tk.BooleanVar(value=True)
        self.null_empty_var = tk.BooleanVar(value=True)
        self.distinct_key_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Row Counts", variable=self.row_counts_var).grid(row=1, column=1, sticky=tk.W, padx=5)
        ttk.Checkbutton(options_frame, text="Null Value Counts", variable=self.null_empty_var).grid(row=1, column=2, sticky=tk.W, padx=5)
        ttk.Checkbutton(options_frame, text="Distinct Key (PK Check)", variable=self.distinct_key_var).grid(row=1, column=3, sticky=tk.W, padx=5)
        
        # Output directory
        output_frame = ttk.Frame(options_frame)
        output_frame.grid(row=2, column=0, columnspan=4, sticky=tk.W, pady=5)
        tk.Label(output_frame, text="Output Directory:").pack(side=tk.LEFT)
        self.output_dir_var = tk.StringVar(value=str(Path.home() / "Desktop" / "validation_outputs"))
        ttk.Entry(output_frame, textvariable=self.output_dir_var, width=50).pack(side=tk.LEFT, padx=5)
        ttk.Button(output_frame, text="Browse...", command=self._browse_output_dir).pack(side=tk.LEFT)
        
        # Buttons frame
        buttons_frame = ttk.Frame(scrollable_frame)
        buttons_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.run_btn = ttk.Button(buttons_frame, text="Run Data Validation", command=self._run_validation)
        self.run_btn.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(buttons_frame, text="Open Output Folder", command=self._open_output_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Export to Excel", command=self._export_to_excel).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Clear Results", command=self._clear_results).pack(side=tk.LEFT, padx=5)
        
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
        
        # Row Counts tab
        row_counts_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(row_counts_frame, text="Row Counts")
        
        columns = ("ObjectType", "SourceSchema", "SourceObject", "DestSchema", "DestObject", 
                   "SourceCount", "DestCount", "Match", "ErrorDescription")
        self.row_counts_tree = self._create_treeview(row_counts_frame, columns)
        
        # Null Values tab
        null_empty_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(null_empty_frame, text="Null Value Counts")
        
        columns = ("ObjectType", "SourceSchema", "SourceObject", "Column", 
                   "SourceNull", "DestNull", "SourceEmpty", "DestEmpty", "NullMatch", "EmptyMatch")
        self.null_empty_tree = self._create_treeview(null_empty_frame, columns)
        
        # Distinct Key tab (Primary Key check)
        distinct_key_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(distinct_key_frame, text="Distinct Key")
        
        columns = ("ObjectType", "SourceSchema", "SourceObject", "DestSchema", "DestObject", 
                   "ElementPath", "ErrorCode", "ErrorDescription")
        self.distinct_key_tree = self._create_treeview(distinct_key_frame, columns)
        
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
        
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=10)
        
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
        for tree in [self.row_counts_tree, self.null_empty_tree, self.distinct_key_tree]:
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
        
        if not self.row_counts_var.get() and not self.null_empty_var.get():
            errors.append("At least one validation check must be selected")
        
        return errors
    
    def _run_validation(self):
        """Run validation in a subprocess so the JVM (DB2) does not freeze the UI."""
        errors = self._validate_inputs()
        if errors:
            messagebox.showerror("Validation Error", "\n".join(errors))
            return

        self.run_btn.config(state=tk.DISABLED)
        self._clear_results()

        output_dir = self.output_dir_var.get().strip() or str(Path.home() / "Desktop" / "validation_outputs")
        os.makedirs(output_dir, exist_ok=True)
        config = self._create_database_config()
        source_schema = self.src_schema_var.get().strip() or None
        target_schema = self.dest_schema_var.get().strip() or None

        self._update_status("Starting validation (subprocess)...", "blue")
        self._log("Starting legacy data validation in subprocess (UI stays responsive)...")
        self._console_log(f"Output: {output_dir}")
        self._console_log(f"Source schema: {source_schema or 'ALL'}, Target schema: {target_schema or 'ALL'}")

        try:
            from azure_migration_tool.validation.run_subprocess import run_legacy_data_validation_subprocess
        except ImportError:
            from validation.run_subprocess import run_legacy_data_validation_subprocess

        queue = multiprocessing.Queue()
        process = multiprocessing.Process(
            target=run_legacy_data_validation_subprocess,
            args=(
                queue,
                config,
                output_dir,
                source_schema,
                target_schema,
                self.row_counts_var.get(),
                self.null_empty_var.get(),
                self.distinct_key_var.get(),
            ),
            daemon=True,
        )
        process.start()
        self._validation_process = process
        reader = threading.Thread(target=self._subprocess_queue_reader, args=(queue, process), daemon=True)
        reader.start()

    def _subprocess_queue_reader(self, queue: multiprocessing.Queue, process: multiprocessing.Process) -> None:
        """Read progress and results from subprocess; schedule UI updates on main thread."""
        try:
            while True:
                try:
                    msg = queue.get()
                except Exception:
                    break
                if msg[0] == "done":
                    self.frame.after(0, lambda p=msg[1]: self._on_subprocess_done(p))
                    break
                if msg[0] == "error":
                    self.frame.after(0, lambda e=msg[1]: self._on_subprocess_error(e))
                    break
                self.frame.after(0, lambda m=msg: self._handle_subprocess_msg(m))
        except Exception:
            pass
        try:
            process.join(timeout=1.0)
        except Exception:
            pass

    def _handle_subprocess_msg(self, msg: tuple) -> None:
        """Handle progress or result message from subprocess (called on main thread)."""
        try:
            if msg[0] == "progress":
                step, current, total, schema, table = msg[1], msg[2], msg[3], msg[4], msg[5]
                labels = {"row_counts": "Row counts", "null_values": "Null values", "distinct_key": "Distinct key"}
                step_label = labels.get(step, step)
                if total and (schema or table):
                    pct = (current / total * 100) if total else 0
                    self.progress_text.config(text=f"{step_label}: {current}/{total} ({pct:.0f}%) - {schema}.{table}")
                    if current % 10 == 0 or current == total:
                        self.validation_log.insert(tk.END, f"  [{current}/{total}] {schema}.{table}\n")
                        self.validation_log.see(tk.END)
                    self._console_log(f"  {step} {current}/{total} {schema}.{table}")
                else:
                    self.progress_text.config(text=f"Comparing {step_label}...")
                self.frame.update_idletasks()
            elif msg[0] == "result":
                step, df = msg[1], msg[2]
                self.validation_results[step] = df
                count = len(df)
                if step == "row_counts":
                    self._load_row_counts_results(df)
                    self._log(f"Row counts: {count} results")
                elif step == "null_values":
                    self._load_null_empty_results(df)
                    self._log(f"Null values: {count} results")
                elif step == "distinct_key":
                    self._load_distinct_key_results(df)
                    self._log(f"Distinct key: {count} results")
        except Exception as e:
            self._log(f"Error handling subprocess msg: {e}", logging.ERROR)

    def _on_subprocess_done(self, single_csv_path: str) -> None:
        """Validation subprocess finished successfully (called on main thread)."""
        try:
            if getattr(self, "_validation_process", None):
                self._validation_process.join(timeout=2.0)
        except Exception:
            pass
        total_steps = sum([self.row_counts_var.get(), self.null_empty_var.get(), self.distinct_key_var.get()])
        results = []
        
        def _safe_len(df):
            """Get length of DataFrame or return 0 if None/empty."""
            if df is None:
                return 0
            try:
                return len(df)
            except Exception:
                return 0
        
        if self.row_counts_var.get():
            n = _safe_len(self.validation_results.get("row_counts"))
            results.append(("Row Counts", n, None))
        if self.null_empty_var.get():
            n = _safe_len(self.validation_results.get("null_values"))
            results.append(("Null Values", n, None))
        if self.distinct_key_var.get():
            n = _safe_len(self.validation_results.get("distinct_key"))
            results.append(("Distinct Key", n, None))
        self._update_summary(results, single_csv_path=single_csv_path)
        self._update_progress(total_steps, total_steps, "Validation complete!")
        self._update_status("Validation completed successfully!", "green")
        self._log("VALIDATION COMPLETE")
        self._console_log("VALIDATION COMPLETE")
        if single_csv_path:
            self._console_log(f"Saved: {single_csv_path}")
        self._enable_run_button()

    def _on_subprocess_error(self, err_msg: str) -> None:
        """Validation subprocess failed (called on main thread)."""
        try:
            if getattr(self, "_validation_process", None):
                self._validation_process.join(timeout=2.0)
        except Exception:
            pass
        self._log(f"CRITICAL ERROR: {err_msg}", logging.ERROR)
        self._console_log(f"CRITICAL ERROR: {err_msg}")
        self._update_status("Error", "red")
        if "DB2Driver" in err_msg or "JVM" in err_msg or "classpath" in err_msg.lower():
            self._show_error(
                "DB2 driver or JVM error.\n\n"
                "Restart the app and try again; ensure db2jcc4.jar is in the drivers folder."
            )
        else:
            self._show_error(f"Validation failed.\n\n{err_msg}\n\nCheck the log for details.")
        self._enable_run_button()
    
    def _load_row_counts_results(self, df):
        """Load row counts results into treeview (accepts pandas DataFrame)."""
        try:
            if hasattr(df, 'toPandas'):
                pdf = df.toPandas()
            else:
                pdf = df
            self._log(f"Row counts DataFrame columns: {list(pdf.columns)}")
            self._log(f"Row counts DataFrame shape: {pdf.shape}")
        except Exception as e:
            self._log(f"Error converting row counts to pandas: {e}")
            return
        
        def update():
            try:
                for _, row in pdf.iterrows():
                    values = (
                        str(row.get("ObjectType", "") or ""),
                        str(row.get("SourceSchemaName", "") or ""),
                        str(row.get("SourceObjectName", "") or ""),
                        str(row.get("DestinationSchemaName", "") or ""),
                        str(row.get("DestinationObjectName", "") or ""),
                        str(row.get("SourceRowCount", "") or ""),
                        str(row.get("DestinationRowCount", "") or ""),
                        str(row.get("RowCountMatch", "") or ""),
                        str(row.get("ErrorDescription", "") or "")
                    )
                    self.row_counts_tree.insert("", tk.END, values=values)
                self._log(f"Loaded {len(pdf)} row count results to UI")
            except Exception as e:
                self._log(f"Error loading row counts to UI: {e}")
        
        self.frame.after(0, update)
    
    def _load_null_empty_results(self, spark_df):
        """Load null values results into treeview (accepts pandas or Spark DataFrame)."""
        try:
            if hasattr(spark_df, "toPandas"):
                pdf = spark_df.toPandas()
            else:
                pdf = spark_df
            self._log(f"Null values DataFrame columns: {list(pdf.columns)}")
            self._log(f"Null values DataFrame shape: {pdf.shape}")
        except Exception as e:
            self._log(f"Error converting null values to pandas: {e}")
            return
        
        def update():
            try:
                for _, row in pdf.iterrows():
                    values = (
                        str(row.get("ObjectType", "") or ""),
                        str(row.get("SourceSchemaName", "") or ""),
                        str(row.get("SourceObjectName", "") or ""),
                        str(row.get("ColumnName", "") or ""),
                        str(row.get("SourceNullCount", "") or ""),
                        str(row.get("DestinationNullCount", "") or ""),
                        str(row.get("SourceEmptyCount", "") or ""),
                        str(row.get("DestinationEmptyCount", "") or ""),
                        str(row.get("NullCountMatch", "") or ""),
                        str(row.get("EmptyCountMatch", "") or "")
                    )
                    self.null_empty_tree.insert("", tk.END, values=values)
                self._log(f"Loaded {len(pdf)} null value results to UI")
            except Exception as e:
                self._log(f"Error loading null values to UI: {e}")
        
        self.frame.after(0, update)
    
    def _load_distinct_key_results(self, df):
        """Load distinct key results into treeview (accepts pandas DataFrame)."""
        try:
            if hasattr(df, 'toPandas'):
                pdf = df.toPandas()
            else:
                pdf = df
            self._log(f"Distinct key DataFrame columns: {list(pdf.columns)}")
            self._log(f"Distinct key DataFrame shape: {pdf.shape}")
        except Exception as e:
            self._log(f"Error converting distinct key to pandas: {e}")
            return
        
        def update():
            try:
                for _, row in pdf.iterrows():
                    # Map to distinct_key format
                    src_schema = str(row.get("SourceSchemaName", "") or "")
                    src_obj = str(row.get("SourceObjectName", "") or "")
                    element_path = f"{src_schema}.{src_obj}"
                    
                    values = (
                        "TABLE",  # ObjectType
                        src_schema,
                        src_obj,
                        str(row.get("DestinationSchemaName", "") or ""),
                        str(row.get("DestinationObjectName", "") or ""),
                        element_path,
                        "KEY_NOT_FOUND",  # ErrorCode
                        "No primary key detected on either side"  # ErrorDescription
                    )
                    self.distinct_key_tree.insert("", tk.END, values=values)
                self._log(f"Loaded {len(pdf)} distinct key results to UI")
            except Exception as e:
                self._log(f"Error loading distinct key to UI: {e}")
        
        self.frame.after(0, update)
    
    def _update_summary(self, results, single_csv_path=None):
        """Update the summary tab. single_csv_path: one consolidated CSV with standard columns."""
        def update():
            self.summary_text.config(state=tk.NORMAL)
            self.summary_text.delete(1.0, tk.END)
            
            summary = []
            summary.append("=" * 60)
            summary.append("DATA VALIDATION SUMMARY")
            summary.append("=" * 60)
            summary.append(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            summary.append(f"\nSource: {self.src_server_var.get()}/{self.src_db_var.get()}")
            summary.append(f"Source Schema: {self.src_schema_var.get() or 'ALL'}")
            summary.append(f"Destination: {self.dest_server_var.get()}/{self.dest_db_var.get()}")
            summary.append(f"Destination Schema: {self.dest_schema_var.get() or 'ALL'}")
            summary.append(f"\nOutput Directory: {self.output_dir_var.get()}")
            if single_csv_path:
                summary.append(f"\nAll results (single file): {single_csv_path}")
            summary.append("\n" + "-" * 60)
            summary.append(f"\n{'Validation Type':<25} {'Results':<15}")
            summary.append("-" * 60)
            
            for name, count, _ in results:
                summary.append(f"{name:<25} {str(count):<15}")
            
            summary.append("-" * 60)
            
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
    
    def _export_to_excel(self):
        """Export all results to consolidated CSV (same format as validation reports)."""
        if not self.validation_results:
            messagebox.showwarning("Warning", "No results to export. Run validation first.")
            return
        
        try:
            import pandas as pd
            import uuid
            import json
            
            # Build filename matching original format: data_validate_all_{db}_{host}_{timestamp}_{uuid}.csv
            db_name = self.dest_db_var.get().strip().replace("/", "_").replace("\\", "_") or "validation"
            host_name = self.src_server_var.get().strip().replace(".", "_").replace(":", "_") or "host"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            unique_id = uuid.uuid4().hex[:6]
            default_name = f"data_validate_all_{db_name}_{host_name}_{timestamp}_{unique_id}.csv"
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile=default_name,
                title="Export Consolidated Report (CSV)"
            )
            
            if not file_path:
                return
            
            # Build consolidated DataFrame matching original format
            # Columns: ValidationType,Status,ObjectType,SourceObjectName,SourceSchemaName,
            #          DestinationObjectName,DestinationSchemaName,ElementPath,ErrorCode,ErrorDescription,DetailsJson
            
            all_rows = []
            
            for validation_type, df in self.validation_results.items():
                for _, row in df.iterrows():
                    # Determine if this is an error based on match columns
                    is_error = False
                    error_code = ""
                    error_desc = ""
                    details = {}
                    
                    # Handle distinct_key validation type - always include all records
                    if validation_type == 'distinct_key':
                        is_error = True
                        error_code = "KEY_NOT_FOUND"
                        error_desc = "No primary key detected on either side"
                        details = {"key_columns": []}
                    elif 'RowCountMatch' in df.columns:
                        if str(row.get('RowCountMatch', '')).lower() != 'true':
                            is_error = True
                            error_code = "ROW_COUNT_MISMATCH"
                            src_count = row.get('SourceRowCount', 0)
                            dest_count = row.get('DestinationRowCount', 0)
                            error_desc = f"Row count mismatch: Source={src_count}, Destination={dest_count}"
                            details = {
                                "source_count": int(src_count) if pd.notna(src_count) else 0,
                                "destination_count": int(dest_count) if pd.notna(dest_count) else 0
                            }
                    elif 'NullCountMatch' in df.columns:
                        if str(row.get('NullCountMatch', '')).lower() != 'true':
                            is_error = True
                            error_code = "NULL_COUNT_MISMATCH"
                            src_nulls = row.get('SourceNullCount', 0)
                            dest_nulls = row.get('DestinationNullCount', 0)
                            error_desc = f"Null count mismatch: Source={src_nulls}, Destination={dest_nulls}"
                            details = {
                                "source_null_count": int(src_nulls) if pd.notna(src_nulls) else 0,
                                "destination_null_count": int(dest_nulls) if pd.notna(dest_nulls) else 0
                            }
                    
                    # Only include errors in the export (matching original format)
                    if not is_error:
                        continue
                    
                    # Extract object info from row
                    src_schema = row.get('SourceSchemaName', self.src_schema_var.get() or '')
                    dest_schema = row.get('DestinationSchemaName', self.dest_schema_var.get() or '')
                    src_obj = row.get('SourceObjectName', row.get('SourceTableName', ''))
                    dest_obj = row.get('DestinationObjectName', row.get('DestinationTableName', ''))
                    obj_type = row.get('ObjectType', 'TABLE')
                    element_path = row.get('ElementPath', f"{src_schema}.{src_obj}")
                    
                    all_rows.append({
                        'ValidationType': validation_type,
                        'Status': 'error',
                        'ObjectType': obj_type,
                        'SourceObjectName': src_obj,
                        'SourceSchemaName': src_schema,
                        'DestinationObjectName': dest_obj,
                        'DestinationSchemaName': dest_schema,
                        'ElementPath': element_path,
                        'ErrorCode': error_code,
                        'ErrorDescription': error_desc,
                        'DetailsJson': json.dumps(details) if details else ''
                    })
            
            # Create and save DataFrame
            result_df = pd.DataFrame(all_rows)
            
            if result_df.empty:
                messagebox.showinfo("No Issues", "No validation issues found. All records matched successfully!")
                return
            
            result_df.to_csv(file_path, index=False)
            
            messagebox.showinfo("Success", f"Exported {len(result_df)} issues to:\n{file_path}")
            self._log(f"CSV report exported: {file_path}")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {e}")
            import traceback
            self._log(f"Export error: {traceback.format_exc()}")
