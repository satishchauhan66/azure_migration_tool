#!/usr/bin/env python
# Author: S@tish Chauhan

"""
Smoke tests for Azure Migration Tool (no GUI, no DB).
Run from repo root: python -m azure_migration_tool.run_smoke_tests
Or: cd azure_migration_tool && python run_smoke_tests.py
"""

import sys
import os
from pathlib import Path

# Ensure package and parent are on path
repo_root = Path(__file__).resolve().parent.parent
app_dir = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

def test_imports():
    """Test that main modules can be imported."""
    errors = []
    # Main (no GUI yet)
    try:
        import azure_migration_tool.main as main_mod
        assert hasattr(main_mod, "main")
    except Exception as e:
        errors.append(("azure_migration_tool.main", e))
    # GUI modules (tk may not be available in headless)
    try:
        import tkinter
    except ImportError:
        print("SKIP: tkinter not available (no GUI tests)")
        return errors
    try:
        from azure_migration_tool.gui.main_window import MainWindow
    except Exception as e:
        errors.append(("MainWindow", e))
    try:
        from azure_migration_tool.gui.widgets.connection_widget import ConnectionWidget, DB_TYPE_DISPLAY, AUTH_DISPLAY
        assert "SQL Server" in str(DB_TYPE_DISPLAY.values())
        assert "Microsoft" in str(AUTH_DISPLAY.values())
    except Exception as e:
        errors.append(("ConnectionWidget", e))
    try:
        from azure_migration_tool.gui.tabs.legacy_schema_validation_tab import LegacySchemaValidationTab
    except Exception as e:
        errors.append(("LegacySchemaValidationTab", e))
    try:
        from azure_migration_tool.gui.utils.tooltip import add_tooltip
    except Exception as e:
        errors.append(("tooltip", e))
    return errors


def test_pyspark_optional():
    """Test PySpark import (optional)."""
    try:
        import pyspark
        print(f"  PySpark {pyspark.__version__} found.")
        return None
    except ImportError as e:
        print(f"  PySpark not installed (optional): {e}")
        return None


def test_db2_azure_validation_optional():
    """Test db2_azure_validation import (optional)."""
    try:
        from db2_azure_validation.services.schema_validation_service import PySparkSchemaValidationService
        print("  db2_azure_validation found.")
        return None
    except ImportError as e:
        print(f"  db2_azure_validation not found (optional): {e}")
        return None


def main():
    print("=" * 60)
    print("Azure Migration Tool – smoke tests")
    print("=" * 60)
    print("\n1. Core imports")
    import_errors = test_imports()
    if import_errors:
        for name, err in import_errors:
            print(f"  FAIL {name}: {err}")
        print("\nResult: FAIL (import errors)")
        return 1
    print("  OK")
    print("\n2. Optional: PySpark")
    test_pyspark_optional()
    print("\n3. Optional: db2_azure_validation")
    test_db2_azure_validation_optional()
    print("\n" + "=" * 60)
    print("Result: PASS")
    print("=" * 60)
    print("For E2E with real DBs: python -m azure_migration_tool.test_legacy_data_validation --config path/to/database_config.json --e2e")
    print("For schema E2E: python -m azure_migration_tool.test_legacy_schema_validation --config path/to/database_config.json --e2e")
    return 0


if __name__ == "__main__":
    sys.exit(main())
