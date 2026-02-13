#!/usr/bin/env python
# Author: Satish Ch@uhan

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
        
        # Check all dependencies
        all_ok, issues = checker.quick_check()
        
        if not all_ok:
            # Build user-friendly message (plain English, not jargon)
            msg = "This tool needs a few things installed to work. We'll help you install them.\n\n"
            
            for issue in issues:
                msg += f"• {issue}\n"
            
            msg += "\n--- What to do ---\n\n"
            
            if any("Java" in i for i in issues):
                msg += "• Java (for DB2 connection only): https://adoptium.net/\n"
            if any("ODBC" in i for i in issues):
                msg += "• Database driver (for SQL Server): use Tools > Install database driver, or https://aka.ms/downloadmsodbcsql\n"
            if any("Python" in i for i in issues):
                msg += "• Python: https://python.org/downloads/\n"
            if any("JDBC" in i for i in issues):
                msg += "• DB2 driver file: place db2jcc4.jar in the app's drivers folder (for Compare DB2 tabs)\n"
            
            msg += "\nTip: For normal SQL Server migration you only need the database driver. Legacy DB2 comparisons run without PySpark."
            
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


