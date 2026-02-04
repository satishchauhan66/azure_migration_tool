"""
Full Migration Tab - Orchestrates all 4 steps together
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from pathlib import Path
import threading
import sys

# Add parent directories to path
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.utils.excel_utils import read_excel_file, create_sample_excel

try:
    import run_full_migration
except ImportError:
    run_full_migration = None


class FullMigrationTab:
    """Full migration orchestration tab."""
    
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
            text="Full Migration (All Steps)",
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=10)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Enable mouse wheel scrolling
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        # Store reference for later use
        self.scrollable_frame = scrollable_frame
        
        # Description
        desc_label = tk.Label(
            scrollable_frame,
            text="Orchestrates complete migration: Backup → Restore Tables → Migrate Data → Restore Complete Schema",
            font=("Arial", 10),
            fg="gray"
        )
        desc_label.pack(pady=5)
        
        # Main connection frame
        main_frame = ttk.Frame(scrollable_frame)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Left column - Source Database (used for backup and migration)
        left_frame = ttk.LabelFrame(main_frame, text="Source Database (for Backup & Migration)", padding=10)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        # Use shared variables from main window
        self.src_server_var = self.main_window.shared_src_server
        self.src_db_var = self.main_window.shared_src_db
        self.src_auth_var = self.main_window.shared_src_auth
        self.src_user_var = self.main_window.shared_src_user
        self.src_password_var = self.main_window.shared_src_password
        
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
            row_start=0
        )
        
        # Right column - Destination Database (using shared variables)
        right_frame = ttk.LabelFrame(main_frame, text="Destination Database (for Migration & Restore)", padding=10)
        right_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        # Use shared variables from main window
        self.dest_server_var = self.main_window.shared_dest_server
        self.dest_db_var = self.main_window.shared_dest_db
        self.dest_auth_var = self.main_window.shared_dest_auth
        self.dest_user_var = self.main_window.shared_dest_user
        self.dest_password_var = self.main_window.shared_dest_password
        
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
            row_start=0
        )
        
        # Restore options frame (moved here for better visibility)
        restore_options_frame = ttk.LabelFrame(scrollable_frame, text="Restore Options (applied after data migration)", padding=10)
        restore_options_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.restore_programmables_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(restore_options_frame, text="Restore Programmables (Views, Procedures, Functions)", 
                       variable=self.restore_programmables_var).pack(side=tk.LEFT, padx=10)
        
        self.restore_constraints_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(restore_options_frame, text="Restore Constraints (Foreign Keys, Check Constraints)", 
                       variable=self.restore_constraints_var).pack(side=tk.LEFT, padx=10)
        
        self.restore_indexes_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(restore_options_frame, text="Restore Indexes", 
                       variable=self.restore_indexes_var).pack(side=tk.LEFT, padx=10)
        
        # Migration options frame
        migration_options_frame = ttk.LabelFrame(scrollable_frame, text="Migration Options (disable constraints during data migration)", padding=10)
        migration_options_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.disable_fk_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(migration_options_frame, text="Disable Foreign Keys (during migration)", 
                       variable=self.disable_fk_var).pack(side=tk.LEFT, padx=10)
        
        self.disable_indexes_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(migration_options_frame, text="Disable Non-clustered Indexes", 
                       variable=self.disable_indexes_var).pack(side=tk.LEFT, padx=10)
        
        self.disable_triggers_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(migration_options_frame, text="Disable Triggers", 
                       variable=self.disable_triggers_var).pack(side=tk.LEFT, padx=10)
        
        self.restore_continue_on_error_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(restore_options_frame, text="Continue on Error", 
                       variable=self.restore_continue_on_error_var).pack(side=tk.LEFT, padx=10)
        
        # Options frame
        options_frame = ttk.LabelFrame(scrollable_frame, text="Migration Options", padding=10)
        options_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Skip options
        skip_frame = ttk.Frame(options_frame)
        skip_frame.pack(fill=tk.X, pady=5)
        
        self.skip_backup_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(skip_frame, text="Skip Backup (if already done)", 
                       variable=self.skip_backup_var).pack(side=tk.LEFT, padx=10)
        
        self.skip_migration_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(skip_frame, text="Skip Data Migration (if already done)", 
                       variable=self.skip_migration_var).pack(side=tk.LEFT, padx=10)
        
        self.skip_restore_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(skip_frame, text="Skip Schema Restore (if already done)", 
                       variable=self.skip_restore_var).pack(side=tk.LEFT, padx=10)
        
        # Migration settings
        settings_frame = ttk.Frame(options_frame)
        settings_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(settings_frame, text="Initial Batch Size (auto-tuned):").pack(side=tk.LEFT, padx=5)
        self.batch_size_var = tk.StringVar(value="20000")
        ttk.Entry(settings_frame, textvariable=self.batch_size_var, width=15).pack(side=tk.LEFT, padx=5)
        
        # Performance & Resilience frame
        perf_frame = ttk.LabelFrame(scrollable_frame, text="Performance & Resilience", padding=10)
        perf_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Row 1: Parallel tables and retries
        perf_row1 = ttk.Frame(perf_frame)
        perf_row1.pack(fill=tk.X, pady=5)
        
        tk.Label(perf_row1, text="Parallel Tables:").pack(side=tk.LEFT, padx=5)
        self.parallel_tables_var = tk.IntVar(value=1)
        ttk.Spinbox(perf_row1, from_=1, to=8, width=5, 
                   textvariable=self.parallel_tables_var).pack(side=tk.LEFT, padx=5)
        
        tk.Label(perf_row1, text="Max Retries:").pack(side=tk.LEFT, padx=20)
        self.max_retries_var = tk.IntVar(value=3)
        ttk.Spinbox(perf_row1, from_=1, to=10, width=5,
                   textvariable=self.max_retries_var).pack(side=tk.LEFT, padx=5)
        
        # Row 2: Checkboxes
        perf_row2 = ttk.Frame(perf_frame)
        perf_row2.pack(fill=tk.X, pady=5)
        
        self.skip_completed_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(perf_row2, text="Skip Completed Tables (row counts match)",
                       variable=self.skip_completed_var).pack(side=tk.LEFT, padx=10)
        
        self.resume_enabled_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(perf_row2, text="Enable Resume (checkpoint progress)",
                       variable=self.resume_enabled_var).pack(side=tk.LEFT, padx=10)
        
        self.verify_after_copy_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(perf_row2, text="Verify After Copy",
                       variable=self.verify_after_copy_var).pack(side=tk.LEFT, padx=10)
        
        # Row 3: Chunking options
        perf_row3 = ttk.Frame(perf_frame)
        perf_row3.pack(fill=tk.X, pady=5)
        
        self.enable_chunking_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(perf_row3, text="Enable Chunking (ADF-style)",
                       variable=self.enable_chunking_var).pack(side=tk.LEFT, padx=10)
        
        tk.Label(perf_row3, text="Threshold:").pack(side=tk.LEFT, padx=5)
        self.chunk_threshold_var = tk.IntVar(value=10_000_000)
        ttk.Spinbox(perf_row3, from_=1_000_000, to=500_000_000, increment=1_000_000, 
                   width=12, textvariable=self.chunk_threshold_var).pack(side=tk.LEFT, padx=2)
        
        tk.Label(perf_row3, text="Chunks:").pack(side=tk.LEFT, padx=5)
        self.num_chunks_var = tk.IntVar(value=10)
        ttk.Spinbox(perf_row3, from_=2, to=50, width=5,
                   textvariable=self.num_chunks_var).pack(side=tk.LEFT, padx=2)
        
        tk.Label(perf_row3, text="Workers:").pack(side=tk.LEFT, padx=5)
        self.chunk_workers_var = tk.IntVar(value=4)
        ttk.Spinbox(perf_row3, from_=1, to=16, width=5,
                   textvariable=self.chunk_workers_var).pack(side=tk.LEFT, padx=2)
        
        self.truncate_dest_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(settings_frame, text="Truncate Destination Tables", 
                       variable=self.truncate_dest_var).pack(side=tk.LEFT, padx=10)
        
        # Excel support frame
        excel_frame = ttk.LabelFrame(scrollable_frame, text="Bulk Processing (Excel)", padding=10)
        excel_frame.pack(fill=tk.X, padx=10, pady=10)
        
        excel_btn_frame = ttk.Frame(excel_frame)
        excel_btn_frame.pack(fill=tk.X)
        
        ttk.Button(excel_btn_frame, text="📥 Download Sample Template", 
                  command=lambda: self._download_template("full_migration")).pack(side=tk.LEFT, padx=5)
        ttk.Button(excel_btn_frame, text="📤 Upload Excel File", 
                  command=self._upload_excel).pack(side=tk.LEFT, padx=5)
        
        self.excel_file_var = tk.StringVar()
        tk.Label(excel_frame, textvariable=self.excel_file_var, fg="gray").pack(anchor=tk.W, pady=5)
        
        self.excel_configs = []
        
        # Buttons
        btn_frame = ttk.Frame(scrollable_frame)
        btn_frame.pack(pady=10)
        
        self.start_btn = ttk.Button(btn_frame, text="Start Full Migration", command=self._start_full_migration, width=25)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        
        self.bulk_btn = ttk.Button(btn_frame, text="Start Bulk Migration", command=self._start_bulk_migration, 
                                   width=25, state=tk.DISABLED)
        self.bulk_btn.pack(side=tk.LEFT, padx=5)
        
        # Progress
        progress_frame = ttk.LabelFrame(scrollable_frame, text="Progress", padding=10)
        progress_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.progress_var = tk.StringVar(value="Ready")
        tk.Label(progress_frame, textvariable=self.progress_var, font=("Arial", 10)).pack(anchor=tk.W)
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress_bar.pack(fill=tk.X, pady=5)
        
        # Log output
        log_frame = ttk.LabelFrame(scrollable_frame, text="Migration Log", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.full_migration_log = scrolledtext.ScrolledText(log_frame, height=20, wrap=tk.WORD)
        self.full_migration_log.pack(fill=tk.BOTH, expand=True)
    
    def _stream_log(self, msg):
        """Stream log messages to the GUI in real-time."""
        try:
            self.full_migration_log.insert(tk.END, f"{msg}\n")
            self.full_migration_log.see(tk.END)
            self.frame.update_idletasks()
        except Exception:
            pass  # Ignore GUI update errors
        
    def _start_full_migration(self):
        """Start full migration in a separate thread."""
        if not run_full_migration:
            messagebox.showerror("Error", "Full migration module not available!")
            return
            
        # Validate inputs
        if not self.skip_backup_var.get() or not self.skip_migration_var.get():
            if not self.src_server_var.get():
                messagebox.showerror("Error", "Source server is required!")
                return
            if not self.src_db_var.get():
                messagebox.showerror("Error", "Source database is required!")
                return
                
        if not self.skip_migration_var.get():
            if not self.dest_server_var.get():
                messagebox.showerror("Error", "Destination server is required!")
                return
            if not self.dest_db_var.get():
                messagebox.showerror("Error", "Destination database is required!")
                return
                
        self.start_btn.config(state=tk.DISABLED)
        self.progress_bar.start()
        self.progress_var.set("Full migration in progress...")
        self.full_migration_log.delete("1.0", tk.END)
        self.full_migration_log.insert(tk.END, "Starting full migration...\n")
        
        def run_full():
            try:
                # Define log streaming callback
                def stream_log(msg):
                    """Stream log messages to the GUI in real-time."""
                    self.full_migration_log.insert(tk.END, f"{msg}\n")
                    self.full_migration_log.see(tk.END)
                    self.frame.update_idletasks()
                
                # Get and validate all connection parameters
                src_server = (self.src_server_var.get() or "").strip()
                src_db = (self.src_db_var.get() or "").strip()
                src_auth = (self.src_auth_var.get() or "entra_mfa").strip()
                src_user = (self.src_user_var.get() or "").strip()
                src_password = (self.src_password_var.get() or "").strip() or None
                
                dest_server = (self.dest_server_var.get() or "").strip()
                dest_db = (self.dest_db_var.get() or "").strip()
                dest_auth = (self.dest_auth_var.get() or "entra_mfa").strip()
                dest_user = (self.dest_user_var.get() or "").strip()
                dest_password = (self.dest_password_var.get() or "").strip() or None
                
                # Validate server names
                if not src_server:
                    messagebox.showerror("Error", "Source server is required!")
                    return
                if not dest_server:
                    messagebox.showerror("Error", "Destination server is required!")
                    return
                
                # Validate server names don't look like email domains
                if "@" in src_server or (src_server.endswith(".com") and "." not in src_server.split(".")[0]):
                    messagebox.showerror("Error", f"Invalid source server name: '{src_server}'. Please enter a valid SQL Server address.")
                    return
                if "@" in dest_server or (dest_server.endswith(".com") and "." not in dest_server.split(".")[0]):
                    messagebox.showerror("Error", f"Invalid destination server name: '{dest_server}'. Please enter a valid SQL Server address.")
                    return
                
                cfg = {
                    # Backup (uses source database)
                    "backup_src_server": src_server,
                    "backup_src_db": src_db,
                    "backup_src_auth": src_auth,
                    "backup_src_user": src_user,
                    "backup_src_password": src_password,
                    
                    # Migration
                    "migrate_src_server": src_server,
                    "migrate_src_db": src_db,
                    "migrate_src_auth": src_auth,
                    "migrate_src_user": src_user,
                    "migrate_src_password": src_password,
                    
                    "migrate_dest_server": dest_server,
                    "migrate_dest_db": dest_db,
                    "migrate_dest_auth": dest_auth,
                    "migrate_dest_user": dest_user,
                    "migrate_dest_password": dest_password,
                    
                    "migrate_batch_size": int(self.batch_size_var.get()) if self.batch_size_var.get() else 20000,
                    "migrate_truncate_dest": self.truncate_dest_var.get(),
                    "migrate_delete_dest": False,
                    "migrate_continue_on_error": True,
                    "migrate_dry_run": False,
                    "migrate_tables": "",  # All tables
                    "migrate_exclude": "",  # No exclusions
                    "migrate_disable_fk": self.disable_fk_var.get(),
                    "migrate_disable_indexes": self.disable_indexes_var.get(),
                    "migrate_disable_triggers": self.disable_triggers_var.get(),
                    
                    # Performance & Resilience options
                    "migrate_parallel_tables": self.parallel_tables_var.get(),
                    "migrate_skip_completed": self.skip_completed_var.get(),
                    "migrate_resume_enabled": self.resume_enabled_var.get(),
                    "migrate_max_retries": self.max_retries_var.get(),
                    "migrate_verify_after_copy": self.verify_after_copy_var.get(),
                    
                    # Chunking options (ADF-style for large tables)
                    "migrate_enable_chunking": self.enable_chunking_var.get(),
                    "migrate_chunk_threshold": self.chunk_threshold_var.get(),
                    "migrate_num_chunks": self.num_chunks_var.get(),
                    "migrate_chunk_workers": self.chunk_workers_var.get(),
                    
                    # Restore
                    "restore_dest_server": None,  # Will default to migrate_dest_server
                    "restore_dest_db": None,  # Will default to migrate_dest_db
                    "restore_dest_auth": None,  # Will default to migrate_dest_auth
                    "restore_dest_user": None,  # Will default to migrate_dest_user
                    "restore_dest_password": None,  # Will default to migrate_dest_password
                    
                    "restore_backup_path": None,  # Will auto-detect
                    "restore_programmables": self.restore_programmables_var.get(),
                    "restore_constraints": self.restore_constraints_var.get(),
                    "restore_indexes": self.restore_indexes_var.get(),
                    "restore_continue_on_error": self.restore_continue_on_error_var.get(),
                    "restore_dry_run": False,
                    
                    # Skip options
                    "skip_backup": self.skip_backup_var.get(),
                    "skip_migration": self.skip_migration_var.get(),
                    "skip_restore": self.skip_restore_var.get(),
                    
                    # Log callback for streaming
                    "log_callback": stream_log,
                }
                
                self.full_migration_log.insert(tk.END, "Running full migration...\n")
                self.full_migration_log.see(tk.END)
                
                summary = run_full_migration.run_full_migration(cfg)
                
                if summary["status"] == "success":
                    self.full_migration_log.insert(tk.END, f"\n✓ Full migration completed successfully!\n")
                    self.full_migration_log.insert(tk.END, f"Run ID: {summary.get('run_id', 'N/A')}\n")
                    messagebox.showinfo("Success", "Full migration completed successfully!")
                else:
                    self.full_migration_log.insert(tk.END, f"\n✗ Full migration completed with errors!\n")
                    self.full_migration_log.insert(tk.END, f"Errors: {summary.get('errors', [])}\n")
                    messagebox.showwarning("Warning", "Full migration completed with errors! Check log for details.")
                    
            except Exception as e:
                self.full_migration_log.insert(tk.END, f"\n✗ Error: {str(e)}\n")
                messagebox.showerror("Error", f"Full migration failed: {str(e)}")
            finally:
                self.progress_bar.stop()
                self.progress_var.set("Ready")
                self.start_btn.config(state=tk.NORMAL)
                
        threading.Thread(target=run_full, daemon=True).start()
        
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
            # Use the existing read_excel_config from run_full_migration if available
            if hasattr(run_full_migration, 'read_excel_config'):
                configs = run_full_migration.read_excel_config(file_path)
            else:
                configs = read_excel_file(
                    file_path,
                    required_columns=["src_server", "src_db", "dest_server", "dest_db"],
                    default_user=None
                )
            
            self.excel_configs = configs
            self.excel_file_var.set(f"Loaded {len(configs)} configuration(s) from {Path(file_path).name}")
            self.bulk_btn.config(state=tk.NORMAL)
            messagebox.showinfo("Success", f"Loaded {len(configs)} configuration(s) from Excel file!")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read Excel file: {str(e)}")
            
    def _start_bulk_migration(self):
        """Start bulk migration from Excel configurations."""
        if not self.excel_configs:
            messagebox.showwarning("Warning", "No configurations loaded! Please upload an Excel file first.")
            return
            
        if not run_full_migration:
            messagebox.showerror("Error", "Full migration module not available!")
            return
            
        self.bulk_btn.config(state=tk.DISABLED)
        self.progress_bar.start()
        self.progress_var.set("Bulk migration in progress...")
        self.full_migration_log.delete("1.0", tk.END)
        self.full_migration_log.insert(tk.END, f"Starting bulk migration for {len(self.excel_configs)} configuration(s)...\n")
        
        def run_bulk():
            success_count = 0
            fail_count = 0
            
            for idx, cfg in enumerate(self.excel_configs, 1):
                self.full_migration_log.insert(tk.END, f"\n{'='*60}\n")
                self.full_migration_log.insert(tk.END, f"[{idx}/{len(self.excel_configs)}] Processing {cfg.get('src_db')} -> {cfg.get('dest_db')}...\n")
                self.full_migration_log.see(tk.END)
                
                try:
                    # Build full migration config from Excel row
                    full_cfg = {
                        "backup_src_server": cfg.get("src_server"),
                        "backup_src_db": cfg.get("src_db"),
                        "backup_src_auth": cfg.get("src_auth", cfg.get("auth", "entra_mfa")),
                        "backup_src_user": cfg.get("src_user", cfg.get("user")),
                        "backup_src_password": cfg.get("src_password"),
                        
                        "migrate_src_server": cfg.get("src_server"),
                        "migrate_src_db": cfg.get("src_db"),
                        "migrate_src_auth": cfg.get("src_auth", cfg.get("auth", "entra_mfa")),
                        "migrate_src_user": cfg.get("src_user", cfg.get("user")),
                        "migrate_src_password": cfg.get("src_password"),
                        
                        "migrate_dest_server": cfg.get("dest_server"),
                        "migrate_dest_db": cfg.get("dest_db"),
                        "migrate_dest_auth": cfg.get("dest_auth", cfg.get("auth", "entra_mfa")),
                        "migrate_dest_user": cfg.get("dest_user", cfg.get("user")),
                        "migrate_dest_password": cfg.get("dest_password"),
                        
                        "migrate_batch_size": cfg.get("batch_size", int(self.batch_size_var.get()) if self.batch_size_var.get() else 20000),
                        "migrate_truncate_dest": cfg.get("truncate_dest", self.truncate_dest_var.get()),
                        "migrate_delete_dest": False,
                        "migrate_continue_on_error": True,
                        "migrate_dry_run": False,
                        "migrate_tables": cfg.get("tables", ""),
                        "migrate_exclude": cfg.get("exclude", ""),
                        "migrate_disable_fk": cfg.get("disable_fk", False),
                        "migrate_disable_indexes": cfg.get("disable_indexes", False),
                        "migrate_disable_triggers": cfg.get("disable_triggers", False),
                        
                        # Performance & Resilience options
                        "migrate_parallel_tables": cfg.get("parallel_tables", self.parallel_tables_var.get()),
                        "migrate_skip_completed": cfg.get("skip_completed", self.skip_completed_var.get()),
                        "migrate_resume_enabled": cfg.get("resume_enabled", self.resume_enabled_var.get()),
                        "migrate_max_retries": cfg.get("max_retries", self.max_retries_var.get()),
                        "migrate_verify_after_copy": cfg.get("verify_after_copy", self.verify_after_copy_var.get()),
                        
                        # Chunking options (ADF-style for large tables)
                        "migrate_enable_chunking": cfg.get("enable_chunking", self.enable_chunking_var.get()),
                        "migrate_chunk_threshold": cfg.get("chunk_threshold", self.chunk_threshold_var.get()),
                        "migrate_num_chunks": cfg.get("num_chunks", self.num_chunks_var.get()),
                        "migrate_chunk_workers": cfg.get("chunk_workers", self.chunk_workers_var.get()),
                        
                        "restore_dest_server": None,
                        "restore_dest_db": None,
                        "restore_dest_auth": None,
                        "restore_dest_user": None,
                        "restore_dest_password": None,
                        
                        "restore_backup_path": cfg.get("backup_path"),
                        "restore_programmables": cfg.get("restore_programmables", self.restore_programmables_var.get()),
                        "restore_constraints": cfg.get("restore_constraints", self.restore_constraints_var.get()),
                        "restore_indexes": cfg.get("restore_indexes", self.restore_indexes_var.get()),
                        "restore_continue_on_error": self.restore_continue_on_error_var.get(),
                        "restore_dry_run": False,
                        
                        "skip_backup": cfg.get("skip_backup", self.skip_backup_var.get()),
                        "skip_migration": cfg.get("skip_migration", self.skip_migration_var.get()),
                        "skip_restore": cfg.get("skip_restore", self.skip_restore_var.get()),
                        
                        # Log callback for streaming logs to UI
                        "log_callback": lambda msg: self._stream_log(msg),
                    }
                    
                    summary = run_full_migration.run_full_migration(full_cfg)
                    
                    if summary["status"] == "success":
                        self.full_migration_log.insert(tk.END, f"✓ Success: {cfg.get('src_db')} -> {cfg.get('dest_db')}\n")
                        success_count += 1
                    else:
                        self.full_migration_log.insert(tk.END, f"✗ Failed: {cfg.get('src_db')} -> {cfg.get('dest_db')} - {summary.get('errors', [])}\n")
                        fail_count += 1
                except Exception as e:
                    self.full_migration_log.insert(tk.END, f"✗ Error: {cfg.get('src_db')} -> {cfg.get('dest_db')} - {str(e)}\n")
                    fail_count += 1
                    
            self.progress_bar.stop()
            self.progress_var.set("Ready")
            self.full_migration_log.insert(tk.END, f"\n{'='*60}\n")
            self.full_migration_log.insert(tk.END, f"Bulk migration completed: {success_count} succeeded, {fail_count} failed\n")
            messagebox.showinfo("Bulk Migration Complete", f"Completed: {success_count} succeeded, {fail_count} failed")
            self.bulk_btn.config(state=tk.NORMAL)
            
        threading.Thread(target=run_bulk, daemon=True).start()

