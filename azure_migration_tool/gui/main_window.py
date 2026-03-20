# Author: S@tish Chauhan

"""
Main application window with tabbed interface.
"""

import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
import sys
import os
import threading

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Tab classes are imported lazily when each tab is first selected (see _on_tab_changed).

# Import log console
try:
    from gui.utils.log_console import show_log_console, log_to_console
    LOG_CONSOLE_AVAILABLE = True
except ImportError:
    LOG_CONSOLE_AVAILABLE = False
    show_log_console = None
    log_to_console = None

# Import driver utilities
try:
    from utils.driver_utils import (
        check_sql_server_odbc_driver,
        check_all_dependencies,
        install_odbc_via_powershell,
        get_manual_install_instructions,
        download_odbc_driver,
        install_odbc_driver_elevated,
    )
    DRIVER_UTILS_AVAILABLE = True
except ImportError:
    DRIVER_UTILS_AVAILABLE = False


class MainWindow:
    """Main application window."""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Azure Migration Tool")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 600)
        
        # Set application icon (if available)
        try:
            # You can add an icon file later
            pass
        except:
            pass
        
        # Create SHARED connection variables across all tabs
        # Source connection
        self.shared_src_server = tk.StringVar()
        self.shared_src_db = tk.StringVar()
        self.shared_src_auth = tk.StringVar(value="entra_mfa")
        self.shared_src_user = tk.StringVar()
        self.shared_src_password = tk.StringVar()
        
        # Destination connection
        self.shared_dest_server = tk.StringVar()
        self.shared_dest_db = tk.StringVar()
        self.shared_dest_auth = tk.StringVar(value="entra_mfa")
        self.shared_dest_user = tk.StringVar()
        self.shared_dest_password = tk.StringVar()
        
        # Create menu bar
        self._create_menu()
        
        # Lazy-loaded tabs: create content only when tab is first selected
        # Order: Projects, Backup & Restore, ... Data Validation
        # ADF Pipeline and IDENTITY (CDC) are under Tools > Experiments (POC) — separate windows.
        self._project_path = None
        self._poc_experiment_tabs = []  # tab instances opened from Experiments menu (for set_project_path)
        self._tab_created = {i: False for i in range(7)}
        self._tab_instances = {}
        self._tab_labels = [
            "Projects",
            "Backup & Restore",
            "Full Migration",
            "Schema Backup/Migration",
            "Data Migration",
            "Schema Validation",
            "Data Validation",
        ]
        
        # Create notebook (tabs)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Add 7 placeholder frames (one per tab)
        self._tab_placeholders = []
        for i in range(7):
            ph = ttk.Frame(self.notebook)
            lbl = tk.Label(ph, text="Loading...", font=("Arial", 10), fg="gray")
            lbl.pack(expand=True, pady=50)
            self._tab_placeholders.append(ph)
            self.notebook.add(ph, text=self._tab_labels[i])
        
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)
        
        # Check for required drivers on startup (after window is shown)
        self.root.after(500, self._check_drivers_on_startup)
        
        # Handle window close - cleanup Spark and other resources
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # Status bar
        self.status_bar = tk.Label(
            self.root,
            text="Ready",
            bd=1,
            relief=tk.SUNKEN,
            anchor=tk.W,
            padx=5
        )
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Center window
        self._center_window()
        
        # Show welcome once for new users (after a short delay so window is visible)
        self.root.after(800, self._maybe_show_welcome)
    
    def _on_tab_changed(self, event=None):
        """Create tab content when a tab is selected for the first time."""
        try:
            idx = self.notebook.index(self.notebook.select())
        except Exception:
            return
        if idx is None or idx < 0 or idx >= 7 or self._tab_created.get(idx, False):
            return
        self._ensure_tab_created(idx)
    
    def _ensure_tab_created(self, idx):
        """Create the tab at index idx if not yet created; put real content inside placeholder (no forget/insert)."""
        if self._tab_created.get(idx, False):
            tab = self._tab_instances.get(idx)
            if tab is not None:
                return tab
        # Map index to (module_path, class_name); try azure_migration_tool first, then gui for path compatibility
        # ADF Pipeline / IDENTITY (CDC): Tools > Experiments (POC)
        tab_specs = [
            ("azure_migration_tool.gui.tabs.project_tab", "ProjectTab"),
            ("azure_migration_tool.gui.tabs.backup_restore_tab", "BackupRestoreTab"),
            ("azure_migration_tool.gui.tabs.full_migration_tab", "FullMigrationTab"),
            ("azure_migration_tool.gui.tabs.schema_tab", "SchemaTab"),
            ("azure_migration_tool.gui.tabs.data_migration_tab", "DataMigrationTab"),
            ("azure_migration_tool.gui.tabs.schema_validation_tab", "SchemaValidationTab"),
            ("azure_migration_tool.gui.tabs.data_validation_tab", "DataValidationTab"),
        ]
        mod_name, class_name = tab_specs[idx]
        try:
            try:
                mod = __import__(mod_name, fromlist=[class_name])
            except ImportError:
                # Fallback when run from inside azure_migration_tool (e.g. gui is on path)
                short_name = "gui.tabs." + mod_name.split(".tabs.")[-1]
                mod = __import__(short_name, fromlist=[class_name])
            cls = getattr(mod, class_name)
            # Build tab inside the existing placeholder so we never call notebook.forget/insert
            # (avoids TclError: Slave index out of bounds on some Tk/ttk versions)
            placeholder = self._tab_placeholders[idx]
            # Clear "Loading..." label first; then create tab (so we don't destroy tab.frame)
            for child in placeholder.winfo_children():
                child.destroy()
            tab = cls(placeholder, self)
            self._tab_instances[idx] = tab
            self._tab_created[idx] = True
            tab.frame.pack(fill=tk.BOTH, expand=True)
            # Propagate project path if set
            if hasattr(self, "_project_path") and self._project_path and hasattr(tab, "set_project_path"):
                tab.set_project_path(self._project_path)
            return tab
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None
        
    def _maybe_show_welcome(self):
        """Show a short welcome / getting started message once per user."""
        try:
            welcome_dir = os.path.join(os.environ.get("APPDATA", os.path.expanduser("~")), "AzureMigrationTool")
            flag_file = os.path.join(welcome_dir, "welcome_shown")
            if os.path.exists(flag_file):
                return
            msg = (
                "Welcome to Azure Migration Tool.\n\n"
                "To get started:\n"
                "1. Create or open a project (Projects tab).\n"
                "2. Go to the \"Full Migration\" tab.\n"
                "3. Enter your source and destination database details.\n"
                "4. Click Run to migrate.\n\n"
                "Need help? Use Help > About or Tools > Check what's installed.\n\n"
                "POC features: Tools > Experiments (POC) — ADF Pipeline, IDENTITY (CDC)."
            )
            messagebox.showinfo("Getting started", msg)
            os.makedirs(welcome_dir, exist_ok=True)
            with open(flag_file, "w") as f:
                f.write("1")
        except Exception:
            pass
    
    def _create_menu(self):
        """Create menu bar."""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="New Project...", command=self._new_project)
        file_menu.add_command(label="Open Project...", command=self._open_project)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        
        # View menu
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="View", menu=view_menu)
        view_menu.add_command(label="Technical log (Ctrl+L)", command=self._show_log_console, accelerator="Ctrl+L")
        
        # Bind keyboard shortcut
        self.root.bind("<Control-l>", lambda e: self._show_log_console())
        self.root.bind("<Control-L>", lambda e: self._show_log_console())
        
        # Tools menu
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="Check what's installed...", command=self._show_dependency_check)
        tools_menu.add_command(label="Install database driver...", command=self._install_odbc_driver)
        tools_menu.add_separator()
        tools_menu.add_command(label="Legacy Data Validation...", command=self._open_legacy_data_window)
        tools_menu.add_command(label="Compare DB2 (Schema)...", command=self._open_compare_db2_window)
        tools_menu.add_separator()
        experiments_menu = tk.Menu(tools_menu, tearoff=0)
        tools_menu.add_cascade(label="Experiments (POC)", menu=experiments_menu)
        experiments_menu.add_command(
            label="ADF Pipeline...",
            command=self._open_adf_pipeline_experiment,
        )
        experiments_menu.add_command(
            label="IDENTITY (CDC)...",
            command=self._open_identity_cdc_experiment,
        )
        tools_menu.add_separator()
        tools_menu.add_command(label="Settings...", command=self._show_settings)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="Getting started", command=lambda: messagebox.showinfo("Getting started", 
            "1. Create or open a project (Projects tab).\n2. Go to Full Migration tab.\n3. Enter source and destination databases.\n4. Click Run."))
        help_menu.add_command(label="About", command=self._show_about)
        
    def _new_project(self):
        """Create a new project."""
        self._ensure_tab_created(0)
        self.notebook.select(0)
        tab = self._tab_instances.get(0)
        if tab and hasattr(tab, "create_new_project"):
            tab.create_new_project()
        
    def _open_project(self):
        """Open an existing project."""
        self._ensure_tab_created(0)
        self.notebook.select(0)
        tab = self._tab_instances.get(0)
        if tab and hasattr(tab, "browse_project"):
            tab.browse_project()
        
    def _show_settings(self):
        """Show settings dialog."""
        messagebox.showinfo("Settings", "Settings dialog coming soon!")

    def _open_legacy_data_window(self):
        """Open Legacy Data Validation in a separate window (Tools menu)."""
        self._open_tool_window(
            "azure_migration_tool.gui.tabs.legacy_data_validation_tab",
            "LegacyDataValidationTab",
            "Legacy Data Validation",
        )

    def _open_compare_db2_window(self):
        """Open Compare DB2 (Schema) in a separate window (Tools menu)."""
        self._open_tool_window(
            "azure_migration_tool.gui.tabs.legacy_schema_validation_tab",
            "LegacySchemaValidationTab",
            "Compare DB2 (Schema)",
        )

    def _open_adf_pipeline_experiment(self):
        """Open ADF Pipeline in a separate window (Tools > Experiments POC)."""
        self._open_tool_window(
            "azure_migration_tool.gui.tabs.adf_trigger_tab",
            "ADFTriggerTab",
            "ADF Pipeline [POC]",
            poc_experiment=True,
        )

    def _open_identity_cdc_experiment(self):
        """Open IDENTITY (CDC) in a separate window (Tools > Experiments POC)."""
        self._open_tool_window(
            "azure_migration_tool.gui.tabs.identity_cdc_tab",
            "IdentityCDCTab",
            "IDENTITY (CDC) [POC]",
            poc_experiment=True,
        )

    def _unregister_poc_experiment_tab(self, tab):
        try:
            self._poc_experiment_tabs.remove(tab)
        except ValueError:
            pass

    def notify_poc_experiment_tabs_project_path(self, project_path):
        """When a project is loaded, update any open Experiments (POC) windows."""
        self._project_path = project_path
        for tab in list(self._poc_experiment_tabs):
            if tab is not None and hasattr(tab, "set_project_path"):
                try:
                    tab.set_project_path(project_path)
                except Exception:
                    pass

    def _open_tool_window(self, mod_name, class_name, title, poc_experiment=False):
        """Open a tab class in a new Toplevel window."""
        try:
            try:
                mod = __import__(mod_name, fromlist=[class_name])
            except ImportError:
                short_name = "gui.tabs." + mod_name.split(".tabs.")[-1]
                mod = __import__(short_name, fromlist=[class_name])
            cls = getattr(mod, class_name)
            win = tk.Toplevel(self.root)
            win.title(title)
            win.geometry("1000x700")
            win.minsize(800, 500)
            placeholder = ttk.Frame(win)
            placeholder.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            tab = cls(placeholder, self)
            tab.frame.pack(fill=tk.BOTH, expand=True)
            if hasattr(self, "_project_path") and self._project_path and hasattr(tab, "set_project_path"):
                tab.set_project_path(self._project_path)
            if poc_experiment:
                self._poc_experiment_tabs.append(tab)

                def _on_destroy(event):
                    if event.widget == win:
                        self._unregister_poc_experiment_tab(tab)

                win.bind("<Destroy>", _on_destroy)
        except Exception as e:
            messagebox.showerror("Error", f"Could not open {title}: {e}")
    
    def _show_log_console(self):
        """Show the streaming log console window."""
        if LOG_CONSOLE_AVAILABLE and show_log_console:
            show_log_console(self.root)
        else:
            messagebox.showerror("Error", "Log Console module not available.")
        
    def _show_about(self):
        """Show about dialog."""
        try:
            from azure_migration_tool import __version__
        except ImportError:
            __version__ = "1.0"
        about_text = f"""Azure Migration Tool v{__version__}

A comprehensive tool for SQL Server/Azure SQL migrations.

Features:
• Schema Backup and Migration
• Data Migration with Auto-tuning
• Data Validation (Row Comparison)
• Schema Validation
"""
        messagebox.showinfo("About", about_text)
        
    def _center_window(self):
        """Center the window on screen."""
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')
    
    def _on_closing(self):
        """Handle application close - cleanup all resources."""
        try:
            # Stop any active Spark sessions
            self._cleanup_spark()
        except Exception as e:
            print(f"Cleanup error: {e}")
        
        # Destroy all windows and exit
        self.root.quit()
        self.root.destroy()
        
        # Force exit to kill any remaining background processes
        import os
        os._exit(0)
    
    def _cleanup_spark(self):
        """No-op: Legacy validation uses Python-only path (no PySpark). Kept for API compatibility."""
        pass
        
    def update_status(self, message):
        """Update status bar message."""
        self.status_bar.config(text=message)
        self.root.update_idletasks()
    
    def _check_drivers_on_startup(self):
        """Check for required drivers on application startup."""
        if not DRIVER_UTILS_AVAILABLE:
            return
        
        driver_ok, driver_name = check_sql_server_odbc_driver()
        
        if driver_ok:
            self.update_status("Ready — database driver installed.")
        else:
            # Show driver missing dialog
            self._show_driver_missing_dialog()
    
    def _show_driver_missing_dialog(self):
        """Show dialog when ODBC driver is missing."""
        response = messagebox.askyesnocancel(
            "Database driver needed",
            "The tool can't connect to SQL Server yet. It needs a small driver from Microsoft.\n\n"
            "Would you like to install it now?\n\n"
            "Yes — Install automatically (you may need administrator rights)\n"
            "No — Show manual installation instructions\n"
            "Cancel — Continue without it (you won't be able to connect to databases)"
        )
        
        if response is True:
            # Yes - Try automatic installation
            self._install_odbc_driver()
        elif response is False:
            # No - Show manual instructions
            self._show_manual_install_instructions()
        # Cancel - do nothing
    
    def _install_odbc_driver(self):
        """Install ODBC driver with user choice of method."""
        if not DRIVER_UTILS_AVAILABLE:
            messagebox.showerror("Error", "Driver utilities not available!")
            return
        
        # First check if already installed
        driver_ok, driver_name = check_sql_server_odbc_driver()
        if driver_ok:
            messagebox.showinfo("Already Installed", 
                f"ODBC Driver is already installed:\n{driver_name}")
            return
        
        # Ask for installation method
        response = messagebox.askyesno(
            "Install ODBC Driver",
            "Choose installation method:\n\n"
            "Yes: PowerShell automatic install (recommended)\n"
            "No: Download and run installer manually\n\n"
            "Note: Installation requires administrator privileges."
        )
        
        if response:
            # PowerShell install
            self._install_via_powershell()
        else:
            # Download and manual install
            self._download_and_install()
    
    def _install_via_powershell(self):
        """Install ODBC driver via PowerShell."""
        # Create progress dialog
        progress_win = tk.Toplevel(self.root)
        progress_win.title("Installing ODBC Driver")
        progress_win.geometry("400x150")
        progress_win.resizable(False, False)
        progress_win.transient(self.root)
        progress_win.grab_set()
        
        # Center on parent
        progress_win.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - (200)
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - (75)
        progress_win.geometry(f"+{x}+{y}")
        
        tk.Label(progress_win, text="Installing ODBC Driver 18 for SQL Server...", 
                 font=("Segoe UI", 10)).pack(pady=20)
        tk.Label(progress_win, text="This may take a few minutes.\nA UAC prompt may appear.", 
                 font=("Segoe UI", 9)).pack()
        
        progress = ttk.Progressbar(progress_win, mode='indeterminate', length=300)
        progress.pack(pady=20)
        progress.start(10)
        
        status_label = tk.Label(progress_win, text="Please wait...", font=("Segoe UI", 9))
        status_label.pack()
        
        def run_install():
            try:
                success, message = install_odbc_via_powershell()
                
                self.root.after(0, lambda: self._finish_install(progress_win, success, message))
            except Exception as e:
                self.root.after(0, lambda: self._finish_install(progress_win, False, str(e)))
        
        threading.Thread(target=run_install, daemon=True).start()
    
    def _download_and_install(self):
        """Download ODBC driver and run installer."""
        # Create progress dialog
        progress_win = tk.Toplevel(self.root)
        progress_win.title("Downloading ODBC Driver")
        progress_win.geometry("400x150")
        progress_win.resizable(False, False)
        progress_win.transient(self.root)
        progress_win.grab_set()
        
        # Center on parent
        progress_win.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - (200)
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - (75)
        progress_win.geometry(f"+{x}+{y}")
        
        tk.Label(progress_win, text="Downloading ODBC Driver 18...", 
                 font=("Segoe UI", 10)).pack(pady=20)
        
        progress = ttk.Progressbar(progress_win, mode='determinate', length=300)
        progress.pack(pady=10)
        
        status_label = tk.Label(progress_win, text="Starting download...", font=("Segoe UI", 9))
        status_label.pack()
        
        def update_progress(downloaded, total):
            if total > 0:
                pct = (downloaded / total) * 100
                self.root.after(0, lambda: progress.configure(value=pct))
                self.root.after(0, lambda: status_label.configure(
                    text=f"Downloaded {downloaded // 1024} KB of {total // 1024} KB"))
        
        def run_download():
            try:
                success, result = download_odbc_driver(progress_callback=update_progress)
                
                if success:
                    # Close progress and run installer
                    self.root.after(0, progress_win.destroy)
                    self.root.after(100, lambda: self._run_installer(result))
                else:
                    self.root.after(0, lambda: self._finish_install(progress_win, False, result))
            except Exception as e:
                self.root.after(0, lambda: self._finish_install(progress_win, False, str(e)))
        
        threading.Thread(target=run_download, daemon=True).start()
    
    def _run_installer(self, msi_path):
        """Run the downloaded MSI installer."""
        response = messagebox.askyesno(
            "Run Installer",
            f"ODBC Driver downloaded to:\n{msi_path}\n\n"
            "Run the installer now?\n\n"
            "Note: A UAC prompt will appear."
        )
        
        if response:
            success, message = install_odbc_driver_elevated(msi_path)
            if success:
                messagebox.showinfo("Installation Started", 
                    message + "\n\nPlease complete the installation wizard,\n"
                    "then restart this application.")
            else:
                messagebox.showerror("Installation Failed", message)
    
    def _finish_install(self, progress_win, success, message):
        """Finish installation and show result."""
        try:
            progress_win.destroy()
        except:
            pass
        
        if success:
            # Verify installation
            driver_ok, driver_name = check_sql_server_odbc_driver()
            if driver_ok:
                messagebox.showinfo("Success", 
                    f"ODBC Driver installed successfully!\n\n"
                    f"Driver: {driver_name}")
                self.update_status("Ready — database driver installed.")
            else:
                messagebox.showinfo("Installation Complete", 
                    message + "\n\nPlease restart the application to use the new driver.")
        else:
            messagebox.showerror("Installation Failed", 
                f"Failed to install ODBC Driver:\n\n{message}\n\n"
                "Please try manual installation.")
            self._show_manual_install_instructions()
    
    def _show_manual_install_instructions(self):
        """Show manual installation instructions."""
        if not DRIVER_UTILS_AVAILABLE:
            instructions = """
Please install Microsoft ODBC Driver 17 or 18 for SQL Server.

Download from:
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

Make sure to download the version matching your Python architecture (32-bit or 64-bit).
"""
        else:
            instructions = get_manual_install_instructions()
        
        # Show in a scrollable dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("ODBC Driver Installation Instructions")
        dialog.geometry("600x400")
        dialog.transient(self.root)
        
        text = tk.Text(dialog, wrap=tk.WORD, font=("Consolas", 10))
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text.insert(tk.END, instructions)
        text.config(state=tk.DISABLED)
        
        tk.Button(dialog, text="Close", command=dialog.destroy).pack(pady=10)
    
    def _show_dependency_check(self):
        """Show comprehensive dependency check dialog."""
        import platform as plat
        import sys
        
        # Create dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Dependency Check")
        dialog.geometry("600x500")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center on parent
        dialog.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - 300
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - 250
        dialog.geometry(f"+{x}+{y}")
        
        # Header
        ttk.Label(dialog, text="Dependency Check", font=("Segoe UI", 14, "bold")).pack(pady=10)
        
        # Results frame with scrollbar
        frame = ttk.Frame(dialog)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        text = tk.Text(frame, wrap=tk.WORD, font=("Consolas", 10), state=tk.NORMAL)
        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=text.yview)
        text.configure(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Check dependencies
        lines = []
        lines.append("=" * 50)
        lines.append("SYSTEM INFORMATION")
        lines.append("=" * 50)
        lines.append(f"OS: {plat.system()} {plat.release()}")
        lines.append(f"Architecture: {plat.machine()}")
        lines.append(f"Python: {sys.version.split()[0]}")
        lines.append(f"Running as: {'Executable' if getattr(sys, 'frozen', False) else 'Script'}")
        lines.append("")
        
        lines.append("=" * 50)
        lines.append("CORE DEPENDENCIES")
        lines.append("=" * 50)
        
        missing_deps = []
        
        # Check ODBC Driver
        try:
            from setup.auto_setup import DependencyChecker
            checker = DependencyChecker()
            
            # ODBC
            odbc_ok, odbc_msg = checker.check_odbc_driver()
            if odbc_ok:
                lines.append(f"[OK] ODBC Driver: {odbc_msg}")
            else:
                lines.append(f"[MISSING] ODBC Driver: {odbc_msg}")
                missing_deps.append("ODBC")
            
            # Java
            java_ok, java_msg = checker.check_java()
            if java_ok:
                lines.append(f"[OK] Java: {java_msg}")
            else:
                lines.append(f"[MISSING] Java: {java_msg}")
                missing_deps.append("Java")
            
            # Exe build: Python/PySpark not required (Legacy validation is bundled)
            if getattr(sys, 'frozen', False):
                lines.append("[OK] This build: no system Python or PySpark required")
            
            # PySpark not required (Legacy validation uses Python-only path)
            lines.append("[OK] Legacy validation: Python-only (no PySpark required)")
            
            # JDBC
            jdbc_ok, jdbc_msg = checker.check_jdbc_drivers()
            if jdbc_ok:
                lines.append(f"[OK] JDBC Drivers: {jdbc_msg}")
            else:
                lines.append(f"[OPTIONAL] JDBC: {jdbc_msg}")
                lines.append("         (Only needed for DB2 source connections)")
        
        except ImportError:
            # Fallback if auto_setup not available
            if DRIVER_UTILS_AVAILABLE:
                driver_ok, driver_name = check_sql_server_odbc_driver()
                if driver_ok:
                    lines.append(f"[OK] ODBC Driver: {driver_name}")
                else:
                    lines.append("[MISSING] ODBC Driver")
                    missing_deps.append("ODBC")
            
            # PySpark not required
            lines.append("[OK] Legacy validation: Python-only (no PySpark required)")
        
        lines.append("")
        
        # Installation instructions
        if missing_deps:
            lines.append("=" * 50)
            lines.append("INSTALLATION INSTRUCTIONS")
            lines.append("=" * 50)
            
            if "ODBC" in missing_deps:
                lines.append("")
                lines.append("ODBC Driver:")
                lines.append("  Download: https://aka.ms/downloadmsodbcsql")
                lines.append("  Or use Tools > Install ODBC Driver")
            
            if "Java" in missing_deps:
                lines.append("")
                lines.append("Java 11+ (for DB2 Compare tabs only):")
                lines.append("  Download: https://adoptium.net/")
                lines.append("  Install and set JAVA_HOME environment variable")
            
            lines.append("")
            lines.append("-" * 50)
            lines.append("NOTE: Legacy Schema/Data Validation uses Python only (no PySpark).")
            lines.append("      DB2 source requires Java + db2jcc4.jar in the drivers folder.")
            lines.append("-" * 50)
        else:
            lines.append("=" * 50)
            lines.append("All dependencies are installed!")
            lines.append("=" * 50)
        
        # Display results
        text.insert(tk.END, "\n".join(lines))
        text.config(state=tk.DISABLED)
        
        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=10)
        
        if missing_deps:
            ttk.Button(btn_frame, text="Install Missing Dependencies", 
                      command=lambda: self._install_missing_deps(dialog)).pack(side=tk.LEFT, padx=5)
        
        if "ODBC" in missing_deps:
            ttk.Button(btn_frame, text="Install ODBC Only", 
                      command=lambda: [dialog.destroy(), self._install_odbc_driver()]).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(btn_frame, text="Refresh", 
                  command=lambda: [dialog.destroy(), self._show_dependency_check()]).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
    
    def _install_missing_deps(self, parent_dialog=None):
        """Install all missing dependencies with progress UI."""
        if parent_dialog:
            parent_dialog.destroy()
        
        # Create progress dialog
        progress_win = tk.Toplevel(self.root)
        progress_win.title("Installing Dependencies")
        progress_win.geometry("500x400")
        progress_win.resizable(False, False)
        progress_win.transient(self.root)
        progress_win.grab_set()
        
        # Center on parent
        progress_win.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - 250
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - 200
        progress_win.geometry(f"+{x}+{y}")
        
        # Header
        ttk.Label(progress_win, text="Installing Dependencies", 
                  font=("Segoe UI", 12, "bold")).pack(pady=10)
        
        # Progress bar
        progress = ttk.Progressbar(progress_win, mode='indeterminate', length=400)
        progress.pack(pady=10)
        progress.start(10)
        
        # Status text
        status_frame = ttk.Frame(progress_win)
        status_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        status_text = tk.Text(status_frame, wrap=tk.WORD, font=("Consolas", 9), 
                              state=tk.NORMAL, height=15)
        scrollbar = ttk.Scrollbar(status_frame, orient=tk.VERTICAL, command=status_text.yview)
        status_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        status_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        def add_status(msg):
            self.root.after(0, lambda: _update_status(msg))
        
        def _update_status(msg):
            status_text.config(state=tk.NORMAL)
            status_text.insert(tk.END, msg + "\n")
            status_text.see(tk.END)
            status_text.config(state=tk.DISABLED)
        
        def run_install():
            try:
                from setup.auto_setup import DependencyChecker
                checker = DependencyChecker()
                
                add_status("Starting dependency installation...\n")
                
                success, messages = checker.install_all_missing(progress_callback=add_status)
                
                add_status("\n" + "=" * 40)
                if success:
                    add_status("All dependencies installed successfully!")
                else:
                    add_status("Some dependencies could not be installed.")
                    add_status("Please check the messages above.")
                
                self.root.after(0, lambda: finish_install(success))
                
            except Exception as e:
                add_status(f"\nError: {e}")
                self.root.after(0, lambda: finish_install(False))
        
        def finish_install(success):
            progress.stop()
            
            # Add close button
            btn_frame = ttk.Frame(progress_win)
            btn_frame.pack(pady=10)
            
            ttk.Button(btn_frame, text="Check Again", 
                      command=lambda: [progress_win.destroy(), self._show_dependency_check()]).pack(side=tk.LEFT, padx=5)
            ttk.Button(btn_frame, text="Close", 
                      command=progress_win.destroy).pack(side=tk.LEFT, padx=5)
            
            if success:
                self.update_status("Dependencies installed - please restart for full effect")
        
        # Run in background thread
        threading.Thread(target=run_install, daemon=True).start()

