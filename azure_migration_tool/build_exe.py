#!/usr/bin/env python
# Author: S@tish Chauhan

"""
Build Azure Migration Tool - Creates small exe with all dependencies.

Usage:
    python build_exe.py           # Build exe
    python build_exe.py --clean   # Clean and rebuild
    python build_exe.py --debug   # Build with console window
"""

import os
import sys
import shutil
import subprocess
import argparse
from pathlib import Path
from typing import Optional


def print_header(text):
    print("\n" + "=" * 60)
    print(f" {text}")
    print("=" * 60)


def _write_ico_from_png(png_path: Path, ico_path: Path, app_dir: Path) -> bool:
    """Create a multi-resolution Windows .ico from a PNG (needs Pillow)."""
    try:
        from PIL import Image
    except ImportError:
        print("  Installing Pillow for icon generation...")
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "pillow", "-q"],
            capture_output=True,
            text=True,
        )
        try:
            from PIL import Image
        except ImportError:
            print(
                "  [WARN] Pillow not available — install with: pip install pillow\n"
                "         Exe/installer will build without a custom icon until app.ico exists."
            )
            return False
    try:
        img = Image.open(png_path).convert("RGBA")
        sizes = [
            (16, 16),
            (24, 24),
            (32, 32),
            (48, 48),
            (64, 64),
            (128, 128),
            (256, 256),
        ]
        ico_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(ico_path, format="ICO", sizes=sizes)
        rel = ico_path.relative_to(app_dir)
        print(f"  [OK] Wrote {rel} (from {png_path.name})")
        return True
    except Exception as e:
        print(f"  [WARN] Could not create app.ico: {e}")
        return False


def ensure_app_ico(app_dir: Path) -> Optional[Path]:
    """
    Resolve resources/logo.png (or legacy build/logo.png), refresh resources/app.ico.
    Returns path to app.ico if it exists or was created.
    """
    resources = app_dir / "resources"
    png_res = resources / "logo.png"
    png_build = app_dir / "build" / "logo.png"
    png = png_res if png_res.exists() else png_build
    if not png.exists():
        return None
    resources.mkdir(parents=True, exist_ok=True)
    if png == png_build and not png_res.exists():
        try:
            shutil.copy2(png, png_res)
            png = png_res
            print(f"  Copied build/logo.png -> resources/logo.png")
        except OSError as e:
            print(f"  [WARN] Could not copy logo to resources/: {e}")
    ico = resources / "app.ico"
    need = not ico.exists() or ico.stat().st_mtime < png.stat().st_mtime
    if need:
        if not _write_ico_from_png(png, ico, app_dir):
            return ico if ico.exists() else None
    return ico if ico.exists() else None


def build_pyinstaller(app_dir: Path, console: bool = False) -> bool:
    """Build exe with PyInstaller."""
    
    print("  Checking PyInstaller...")
    try:
        import PyInstaller
    except ImportError:
        print("  Installing PyInstaller...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', 'pyinstaller', '-q'])
    
    project_root = app_dir.parent
    drivers_dir = app_dir / 'drivers'
    
    # Build datas list (Legacy tabs use azure_migration_tool.validation only – no db2_azure_validation)
    datas_list = []
    
    # Add drivers folder (includes db2jcc4.jar for DB2 connections)
    if drivers_dir.exists():
        datas_list.append((str(drivers_dir), 'drivers'))
    
    # Add gui, setup, backup, and src folders
    for subdir in ['gui', 'setup', 'backup', 'src']:
        subdir_path = app_dir / subdir
        if subdir_path.exists():
            datas_list.append((str(subdir_path), subdir))
    
    datas_str = repr(datas_list)
    console_str = "True" if console else "False"

    icon_path = ensure_app_ico(app_dir)
    icon_line = ""
    if icon_path and icon_path.is_file():
        # Forward slashes work in PyInstaller spec on Windows
        icon_spec = icon_path.resolve().as_posix()
        icon_line = f'\n    icon=r"{icon_spec}",'

    # Create spec file
    spec_content = f'''# -*- mode: python ; coding: utf-8 -*-

# Small exe - PySpark NOT bundled (users install it via: pip install pyspark)
# DB2 JDBC driver IS bundled in drivers folder

app_datas = {datas_str}

a = Analysis(
    [r"{app_dir / 'main.py'}"],
    pathex=[r"{project_root}", r"{app_dir}"],
    binaries=[],
    datas=app_datas,
    hiddenimports=[
        # App modules - gui
        "gui", "gui.main_window",
        "gui.tabs", "gui.tabs.schema_tab", "gui.tabs.data_migration_tab",
        "gui.tabs.data_validation_tab", "gui.tabs.schema_validation_tab",
        "gui.tabs.mi_pitr_restore_tab",
        "gui.tabs.legacy_schema_validation_tab", "gui.tabs.legacy_data_validation_tab",
        "gui.tabs.full_migration_tab", "gui.tabs.project_tab",
        "gui.utils", "gui.utils.schema_comparison", "gui.utils.log_console",
        "gui.utils.database_utils", "gui.utils.db2_type_mapping", "gui.utils.db2_schema",
        "gui.utils.schema_matching", "gui.utils.schema_remap", "gui.utils.compare_keys",
        "gui.utils.schema_script_generator",
        "gui.utils.excel_utils", "gui.utils.scrollable_frame", "gui.utils.canvas_mousewheel",
        "gui.utils.server_config",
        "gui.widgets", "gui.widgets.connection_widget", "gui.widgets.schema_tree",
        "gui.widgets.schema_diff_viewer",
        "gui.dialogs", "gui.dialogs.restore_preview_dialog",
        # Setup
        "setup", "setup.auto_setup",
        # Validation (Legacy schema/data comparison – Python only)
        "validation", "validation.schema_service", "validation.data_service",
        "validation.run_subprocess", "validation.connections", "validation.config",
        "validation.azure_catalog",
        # Local backup module
        "backup", "backup.exporters",
        # src module (schema backup/restore/migration)
        "src", "src.backup", "src.backup.exporters", "src.backup.schema_backup",
        "src.restore", "src.restore.schema_restore", "src.restore.nullability_fix",
        "src.migration", "src.migration.data_migration",
        "src.orchestration", "src.orchestration.full_migration",
        "src.utils", "src.utils.database", "src.utils.sql", "src.utils.paths",
        "src.azure_mgmt", "src.azure_mgmt.mi_pitr_restore",
        "utils.azure_shared_credential",
        "src.utils.config", "src.utils.logging", "src.utils.azure_compat",
        # External modules (Legacy tabs: no PySpark, no db2_azure_validation)
        "jaydebeapi", "jpype1", "jpype", "jpype.imports", "pyodbc", "pandas",
        "openpyxl", "requests",
        "azure.identity", "azure.core.credentials",
        "tkinter", "tkinter.ttk", "tkinter.messagebox", "tkinter.filedialog",
        "tkinter.scrolledtext",
    ],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=[
        "matplotlib", "scipy", "numpy.testing", "pytest",
        # Exclude pyspark completely - users install it separately
        "pyspark", "pyspark.sql", "pyspark.errors", "pyspark.util",
        "py4j",
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="AzureMigrationTool",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    runtime_tmpdir=None,
    console={console_str},{icon_line}
)
'''
    
    spec_file = app_dir / 'AzureMigrationTool.spec'
    spec_file.write_text(spec_content)
    print(f"  Created spec file")
    
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--noconfirm',
        '--distpath', str(app_dir / 'dist'),
        '--workpath', str(app_dir / 'build'),
        str(spec_file),
    ]
    
    print("  Building exe (this takes a few minutes)...")
    result = subprocess.run(cmd, cwd=str(app_dir), capture_output=True, text=True)
    
    exe = app_dir / 'dist' / 'AzureMigrationTool.exe'
    if exe.exists():
        size = exe.stat().st_size / (1024 * 1024)
        # Get version and rename to single versioned file (AzureMigrationTool_1.2.0.exe)
        version = None
        init_py = app_dir / '__init__.py'
        if init_py.exists():
            try:
                text = init_py.read_text(encoding='utf-8')
                import re
                m = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', text)
                if m:
                    version = m.group(1).strip()
            except Exception:
                pass
        if version:
            versioned = app_dir / 'dist' / f'AzureMigrationTool_{version}.exe'
            exe.rename(versioned)
            print(f"  Built: {versioned.name} ({size:.0f} MB)")
        else:
            print(f"  Built: {exe.name} ({size:.0f} MB)")
        return True
    else:
        print(f"  Build failed!")
        print("  STDOUT:", result.stdout[-2000:] if result.stdout else "None")
        print("  STDERR:", result.stderr[-2000:] if result.stderr else "None")
        return False


def main():
    parser = argparse.ArgumentParser(description='Build Azure Migration Tool')
    parser.add_argument('--clean', action='store_true', help='Clean before building')
    parser.add_argument('--debug', action='store_true', help='Build with console window')
    args = parser.parse_args()
    
    # All paths relative to this script (azure_migration_tool folder)
    app_dir = Path(__file__).parent.resolve()
    dist_dir = app_dir / 'dist'
    build_dir = app_dir / 'build'
    drivers_dir = app_dir / 'drivers'
    
    print_header("Azure Migration Tool - Build System")
    print(f"App Directory: {app_dir}")
    
    # Clean
    if args.clean:
        print("\nCleaning...")
        for d in [dist_dir, build_dir]:
            if d.exists():
                shutil.rmtree(d)
                print(f"  Removed {d.name}")
    
    # Check drivers
    print("\nChecking bundled drivers...")
    drivers_dir.mkdir(exist_ok=True)
    
    db2_jar = drivers_dir / 'db2jcc4.jar'
    if db2_jar.exists():
        size = db2_jar.stat().st_size / (1024 * 1024)
        print(f"  [OK] db2jcc4.jar ({size:.1f} MB)")
    else:
        print(f"  [MISSING] db2jcc4.jar - DB2 connections won't work")
        print(f"  Place it in: {drivers_dir}")
    
    # Build
    print_header("Building Executable")
    if not build_pyinstaller(app_dir, args.debug):
        print("\nBuild failed!")
        return 1
    
    # Summary (exe is versioned: AzureMigrationTool_<version>.exe)
    version = None
    init_py = app_dir / '__init__.py'
    if init_py.exists():
        try:
            import re
            text = init_py.read_text(encoding='utf-8')
            m = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', text)
            if m:
                version = m.group(1).strip()
        except Exception:
            pass
    exe_path = (dist_dir / f'AzureMigrationTool_{version}.exe') if version else (dist_dir / 'AzureMigrationTool.exe')
    if exe_path.exists():
        size = exe_path.stat().st_size / (1024 * 1024)
        print_header("BUILD SUCCESSFUL!")
        print(f"\nExecutable: {exe_path}")
        print(f"Size: {size:.0f} MB")
        
        print("\n" + "-" * 60)
        print("REQUIRED ON DESTINATION (target PC):")
        print("-" * 60)
        print("• Windows (this exe is Windows-only)")
        print("• ODBC Driver for SQL Server (for Azure SQL / SQL Server)")
        print("  Install: https://aka.ms/downloadmsodbcsql or use Tools > Install database driver")
        print("• Java 11+ ONLY if you use Compare DB2 (Schema/Data) tabs (for DB2 connection)")
        print("  Install: https://adoptium.net/")
        print("• No Python or PySpark needed on target – everything is bundled in the exe")
        print("\nTO USE: Copy the exe to target PC and double-click to run.")
        scripts_dir = app_dir.parent / 'scripts'
        if scripts_dir.exists():
            print("\nFor target PCs that show dependency errors (e.g. VCRUNTIME140.dll missing),")
            print("  copy the 'scripts' folder next to the exe and run Install-Dependencies-and-Run.bat")
            print(f"  Scripts: {scripts_dir}")
        
        print("\nBUNDLED DRIVERS:")
        if db2_jar.exists():
            print("  - db2jcc4.jar (DB2 JDBC driver)")
        else:
            print("  - None (add db2jcc4.jar to drivers folder)")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
