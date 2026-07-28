#!/usr/bin/env python
# Author: S@tish Ch@uhan

"""
Azure Migration Tool - Main Application
A comprehensive GUI application for SQL Server/Azure SQL migrations.

Features:
1. Schema Migration/Backup
2. Data Migration
3. Row Comparison (Data Validation)
4. Schema Validation
"""

import sys
import os
from pathlib import Path


def _suppress_subprocess_console_windows() -> None:
    """Globally prevent every subprocess.Popen from flashing a console window.

    When a PyInstaller-built *windowed* app spawns any subprocess (icacls,
    powershell, az.cmd, java, bcp, ...), Windows creates a visible cmd.exe
    console because there is no parent console to inherit.  This monkey-patch
    injects CREATE_NO_WINDOW + SW_HIDE into every Popen call process-wide,
    covering our own code *and* third-party libs (azure-identity, msal, etc.).
    """
    if not sys.platform.startswith("win"):
        return
    import subprocess
    _CREATE_NO_WINDOW = 0x08000000
    _real_init = subprocess.Popen.__init__

    def _patched_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        kwargs.setdefault("creationflags", 0)
        kwargs["creationflags"] |= _CREATE_NO_WINDOW
        if kwargs.get("startupinfo") is None:
            si = subprocess.STARTUPINFO()
            si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            si.wShowWindow = 0  # SW_HIDE
            kwargs["startupinfo"] = si
        _real_init(self, *args, **kwargs)

    subprocess.Popen.__init__ = _patched_init  # type: ignore[method-assign]


_suppress_subprocess_console_windows()

# Handle frozen exe (PyInstaller) vs normal execution
if getattr(sys, 'frozen', False):
    # Running as compiled exe
    if hasattr(sys, '_MEIPASS'):
        app_dir = Path(sys._MEIPASS)
    else:
        app_dir = Path(sys.executable).parent
    
    # Add paths for bundled modules
    sys.path.insert(0, str(app_dir))
    
    # Add db2_azure_validation if bundled
    db2_val_dir = app_dir / 'db2_azure_validation'
    if db2_val_dir.exists():
        sys.path.insert(0, str(db2_val_dir.parent))
    
    # Add gui, setup, src, backup modules
    for subdir in ['gui', 'setup', 'utils', 'src', 'backup']:
        subdir_path = app_dir / subdir
        if subdir_path.exists():
            sys.path.insert(0, str(subdir_path.parent))
    
    # Note: PySpark Python path is set by DependencyChecker.check_python()
    # which runs during startup dependency check
else:
    # Running as script
    app_dir = Path(__file__).parent
    parent_dir = app_dir.parent
    sys.path.insert(0, str(parent_dir))
    sys.path.insert(0, str(app_dir))

import tkinter as tk
from tkinter import ttk, messagebox
import threading


def main():
    """Launch the Azure Migration Tool application."""
    # Create root window first (hidden initially for setup)
    root = tk.Tk()
    root.withdraw()  # Hide until setup is complete
    
    # Run auto-setup on first launch or if dependencies are missing
    try:
        from setup.auto_setup import DependencyChecker
        checker = DependencyChecker()
        
        # quick_check returns (all_ok, blocking_issues, optional_notes)
        result = checker.quick_check()
        all_ok, issues = result[0], result[1]
        optional_notes = result[2] if len(result) > 2 else []
        
        if not all_ok:
            msg = "This tool needs a few things installed to work.\n\n"
            
            for issue in issues:
                msg += f"• {issue}\n"
            
            msg += "\n--- What to do ---\n\n"
            
            if any("ODBC" in i for i in issues):
                msg += "• Database driver (for SQL Server): use Tools > Install database driver, or https://aka.ms/downloadmsodbcsql\n"
            if any("Python" in i for i in issues):
                msg += "• Python: https://python.org/downloads/\n"
            
            if optional_notes:
                msg += "\n--- Optional (DB2 source only) ---\n\n"
                for note in optional_notes:
                    msg += f"• {note}\n"
                msg += "\nThese are NOT required for SQL Server / Azure SQL migrations.\n"
            
            msg += "\nTip: For normal SQL Server migration you only need the database driver."
            
            messagebox.showwarning("Setup needed", msg)
    except ImportError:
        # Setup module not available (running from source without it)
        pass
    except Exception as e:
        # Don't block startup on setup errors
        print(f"Setup check failed: {e}")
    
    # Now show and run the main application
    root.deiconify()  # Show the window
    
    from gui.main_window import MainWindow
    app = MainWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()


