# Author: Satish Ch@uhan

"""
Auto-Setup Module for Azure Migration Tool
Automatically configures the host computer with necessary drivers and dependencies.
Runs on first startup or when dependencies are missing.
"""

import os
import sys
import subprocess
import platform
import tempfile
import shutil
import urllib.request
import zipfile
import ctypes
import logging
from pathlib import Path
from typing import Tuple, Optional, List

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def is_admin() -> bool:
    """Check if running with administrator privileges."""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False


def get_app_data_dir() -> Path:
    """Get application data directory for storing bundled dependencies."""
    if getattr(sys, 'frozen', False):
        # Running as compiled exe
        app_dir = Path(os.environ.get('LOCALAPPDATA', '')) / 'AzureMigrationTool'
    else:
        # Running as script
        app_dir = Path(__file__).parent.parent / 'app_data'
    
    app_dir.mkdir(parents=True, exist_ok=True)
    return app_dir


def get_bundled_dir() -> Path:
    """Get directory where bundled files are stored (in exe or alongside it)."""
    if getattr(sys, 'frozen', False):
        # Running as compiled exe - check _MEIPASS (PyInstaller temp dir)
        if hasattr(sys, '_MEIPASS'):
            return Path(sys._MEIPASS)
        else:
            return Path(sys.executable).parent
    else:
        return Path(__file__).parent.parent


def get_exe_dir() -> Path:
    """Get directory where the exe is located (for bundled resources next to exe)."""
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent
    else:
        return Path(__file__).parent.parent.parent


class DependencyChecker:
    """Check and install dependencies automatically."""
    
    def __init__(self, progress_callback=None):
        self.progress_callback = progress_callback or (lambda msg, pct: logger.info(f"{pct}% - {msg}"))
        self.app_data = get_app_data_dir()
        self.bundled_dir = get_bundled_dir()
        
    def _report(self, message: str, percent: int = 0):
        """Report progress."""
        self.progress_callback(message, percent)
        logger.info(message)
    
    # =========================================================================
    # Java Runtime Check/Install
    # =========================================================================
    
    def check_java(self) -> Tuple[bool, str]:
        """Check if Java 11+ is available."""
        # First check bundled Java (in exe directory or _MEIPASS)
        exe_dir = get_exe_dir()
        bundled_locations = [
            exe_dir / 'java' / 'bin' / 'java.exe',  # Portable/Installer layout
            exe_dir / 'bundle' / 'java' / 'bin' / 'java.exe',
            self.bundled_dir / 'bundle' / 'java' / 'bin' / 'java.exe',
            self.app_data / 'java' / 'bin' / 'java.exe',
        ]
        
        # Also check AMT_JAVA_HOME environment variable (set by installer)
        amt_java = os.environ.get('AMT_JAVA_HOME', '')
        if amt_java:
            bundled_locations.insert(0, Path(amt_java) / 'bin' / 'java.exe')
        
        for bundled_java in bundled_locations:
            if bundled_java.exists():
                version = self._get_java_version(str(bundled_java))
                if version and version >= 11:
                    # Set JAVA_HOME to bundled Java
                    java_home = str(bundled_java.parent.parent)
                    os.environ['JAVA_HOME'] = java_home
                    # Also add to PATH
                    java_bin = str(bundled_java.parent)
                    if java_bin not in os.environ.get('PATH', ''):
                        os.environ['PATH'] = java_bin + os.pathsep + os.environ.get('PATH', '')
                    return True, f"Bundled Java {version} found"
        
        # Check JAVA_HOME
        java_home = os.environ.get('JAVA_HOME', '')
        if java_home and os.path.exists(java_home):
            java_exe = os.path.join(java_home, 'bin', 'java.exe')
            if os.path.exists(java_exe):
                version = self._get_java_version(java_exe)
                if version and version >= 11:
                    return True, f"Java {version} found at JAVA_HOME"
        
        # Check system PATH
        try:
            result = subprocess.run(['java', '-version'], capture_output=True, text=True)
            output = result.stderr or result.stdout
            if 'version' in output.lower():
                # Parse version
                import re
                match = re.search(r'version "(\d+)', output)
                if match:
                    version = int(match.group(1))
                    if version >= 11:
                        return True, f"Java {version} found in PATH"
        except FileNotFoundError:
            pass
        
        return False, "Java 11+ not found"
    
    def _get_java_version(self, java_exe: str) -> Optional[int]:
        """Get Java version number."""
        try:
            result = subprocess.run([java_exe, '-version'], capture_output=True, text=True)
            output = result.stderr or result.stdout
            import re
            match = re.search(r'version "(\d+)', output)
            if match:
                return int(match.group(1))
        except:
            pass
        return None
    
    def install_java(self) -> bool:
        """Download and install portable Java (Adoptium/Eclipse Temurin)."""
        self._report("Downloading Java 17 (Eclipse Temurin)...", 10)
        
        # Determine architecture
        is_64bit = platform.machine().endswith('64')
        arch = 'x64' if is_64bit else 'x86'
        
        # Adoptium API URL for Java 17 LTS
        api_url = f"https://api.adoptium.net/v3/binary/latest/17/ga/windows/{arch}/jdk/hotspot/normal/eclipse"
        
        java_dir = self.app_data / 'java'
        temp_zip = self.app_data / 'java_temp.zip'
        
        try:
            self._report("Downloading Java runtime (~180MB)...", 15)
            
            # Download with progress
            def download_progress(block_num, block_size, total_size):
                if total_size > 0:
                    pct = min(int(block_num * block_size * 100 / total_size), 100)
                    self._report(f"Downloading Java: {pct}%", 15 + int(pct * 0.3))
            
            urllib.request.urlretrieve(api_url, str(temp_zip), download_progress)
            
            self._report("Extracting Java...", 50)
            
            # Extract
            with zipfile.ZipFile(str(temp_zip), 'r') as zip_ref:
                # Get the root folder name in the zip
                root_name = zip_ref.namelist()[0].split('/')[0]
                zip_ref.extractall(str(self.app_data))
            
            # Rename extracted folder to 'java'
            extracted_dir = self.app_data / root_name
            if java_dir.exists():
                shutil.rmtree(java_dir)
            extracted_dir.rename(java_dir)
            
            # Clean up
            temp_zip.unlink()
            
            # Set JAVA_HOME
            os.environ['JAVA_HOME'] = str(java_dir)
            
            self._report("Java installed successfully!", 55)
            return True
            
        except Exception as e:
            self._report(f"Failed to install Java: {e}", 0)
            if temp_zip.exists():
                temp_zip.unlink()
            return False
    
    # =========================================================================
    # ODBC Driver Check/Install
    # =========================================================================
    
    def check_odbc_driver(self) -> Tuple[bool, str]:
        """Check if SQL Server ODBC Driver is installed."""
        import winreg
        
        driver_names = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server Native Client 11.0",
        ]
        
        try:
            # Check installed ODBC drivers
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                                r"SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers")
            i = 0
            installed_drivers = []
            while True:
                try:
                    name, value, _ = winreg.EnumValue(key, i)
                    installed_drivers.append(name)
                    i += 1
                except OSError:
                    break
            winreg.CloseKey(key)
            
            for driver in driver_names:
                if driver in installed_drivers:
                    return True, f"Found: {driver}"
            
        except Exception as e:
            logger.warning(f"Could not check ODBC drivers: {e}")
        
        return False, "SQL Server ODBC Driver not found"
    
    def install_odbc_driver(self) -> bool:
        """Install SQL Server ODBC Driver 18 from bundled or download."""
        if not is_admin():
            self._report("ODBC driver installation requires administrator privileges.", 0)
            self._report("Please run the application as Administrator for first-time setup.", 0)
            return False
        
        # Check for bundled ODBC installer first
        exe_dir = get_exe_dir()
        bundled_locations = [
            exe_dir / 'bundle' / 'odbc' / 'msodbcsql18.msi',
            self.bundled_dir / 'bundle' / 'odbc' / 'msodbcsql18.msi',
            self.app_data / 'odbc' / 'msodbcsql18.msi',
        ]
        
        msi_path = None
        for loc in bundled_locations:
            if loc.exists():
                msi_path = loc
                self._report("Using bundled ODBC installer...", 60)
                break
        
        if msi_path is None:
            # Download if not bundled
            self._report("Downloading SQL Server ODBC Driver 18...", 60)
            odbc_url = "https://go.microsoft.com/fwlink/?linkid=2249006"
            msi_path = self.app_data / 'msodbcsql.msi'
            
            try:
                urllib.request.urlretrieve(odbc_url, str(msi_path))
            except Exception as e:
                self._report(f"Failed to download ODBC Driver: {e}", 0)
                return False
        
        try:
            self._report("Installing ODBC Driver (requires admin)...", 70)
            
            # Silent install
            result = subprocess.run(
                ['msiexec', '/i', str(msi_path), '/quiet', '/norestart', 'IACCEPTMSODBCSQLLICENSETERMS=YES'],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                self._report("ODBC Driver installed successfully!", 75)
                return True
            else:
                self._report(f"ODBC installation failed (code {result.returncode})", 0)
                return False
                
        except Exception as e:
            self._report(f"Failed to install ODBC Driver: {e}", 0)
            return False
    
    # =========================================================================
    # Python/PySpark Check (for Legacy Schema Validation)
    # =========================================================================
    
    def check_python(self) -> Tuple[bool, str]:
        """Check if Python is available on the system (needed for PySpark workers)."""
        if not getattr(sys, 'frozen', False):
            # Running from source - Python is obviously available
            return True, f"Python {sys.version.split()[0]}"
        
        # Running as exe - check for system Python
        candidates = [
            'python',
            'python3',
            os.path.expandvars(r'%LOCALAPPDATA%\Programs\Python\Python311\python.exe'),
            os.path.expandvars(r'%LOCALAPPDATA%\Programs\Python\Python310\python.exe'),
            os.path.expandvars(r'%LOCALAPPDATA%\Programs\Python\Python39\python.exe'),
            r'C:\Python311\python.exe',
            r'C:\Python310\python.exe',
        ]
        
        for py in candidates:
            try:
                result = subprocess.run([py, '--version'], capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    version = result.stdout.strip() or result.stderr.strip()
                    # Set environment for PySpark
                    os.environ['PYSPARK_PYTHON'] = py
                    os.environ['PYSPARK_DRIVER_PYTHON'] = py
                    return True, version
            except:
                pass
        
        return False, "Python not found on system"
    
    def check_pyspark(self) -> Tuple[bool, str]:
        """Check if PySpark is installed."""
        try:
            # First check if we can import pyspark
            import pyspark
            return True, f"PySpark {pyspark.__version__}"
        except ImportError:
            pass
        
        # If running as exe, check if pyspark is installed in system Python
        if getattr(sys, 'frozen', False):
            python_ok, _ = self.check_python()
            if python_ok:
                py = os.environ.get('PYSPARK_PYTHON', 'python')
                try:
                    result = subprocess.run(
                        [py, '-c', 'import pyspark; print(pyspark.__version__)'],
                        capture_output=True, text=True, timeout=10
                    )
                    if result.returncode == 0:
                        version = result.stdout.strip()
                        return True, f"PySpark {version} (system)"
                except:
                    pass
        
        return False, "PySpark not installed (pip install pyspark)"
    
    # =========================================================================
    # Install Missing Dependencies
    # =========================================================================
    
    def install_pyspark(self, progress_callback=None) -> Tuple[bool, str]:
        """Install PySpark using pip."""
        def report(msg):
            if progress_callback:
                progress_callback(msg)
            self._report(msg, 50)
        
        # Find Python/pip
        if getattr(sys, 'frozen', False):
            python_ok, _ = self.check_python()
            if not python_ok:
                return False, "Python not found. Please install Python first."
            py = os.environ.get('PYSPARK_PYTHON', 'python')
        else:
            py = sys.executable
        
        report("Installing PySpark (this may take a few minutes)...")
        
        try:
            result = subprocess.run(
                [py, '-m', 'pip', 'install', 'pyspark', '--upgrade'],
                capture_output=True, text=True, timeout=300
            )
            
            if result.returncode == 0:
                return True, "PySpark installed successfully!"
            else:
                return False, f"Installation failed: {result.stderr[:500]}"
        except subprocess.TimeoutExpired:
            return False, "Installation timed out. Please run manually: pip install pyspark"
        except Exception as e:
            return False, f"Installation error: {e}"
    
    def install_all_missing(self, progress_callback=None) -> Tuple[bool, List[str]]:
        """Install all missing dependencies."""
        results = []
        all_ok = True
        
        def report(msg):
            if progress_callback:
                progress_callback(msg)
            results.append(msg)
        
        # Check and install Java
        java_ok, java_msg = self.check_java()
        if not java_ok:
            report("Java not found - please install manually from https://adoptium.net/")
            all_ok = False
        else:
            report(f"Java: OK ({java_msg})")
        
        # Check and install ODBC
        odbc_ok, odbc_msg = self.check_odbc_driver()
        if not odbc_ok:
            report("Installing ODBC Driver...")
            if self.install_odbc_driver():
                report("ODBC Driver: Installed successfully")
            else:
                report("ODBC Driver: Installation failed (requires admin)")
                all_ok = False
        else:
            report(f"ODBC Driver: OK ({odbc_msg})")
        
        # Check and install PySpark
        pyspark_ok, pyspark_msg = self.check_pyspark()
        if not pyspark_ok:
            report("Installing PySpark...")
            success, msg = self.install_pyspark(progress_callback)
            if success:
                report("PySpark: Installed successfully")
            else:
                report(f"PySpark: {msg}")
                all_ok = False
        else:
            report(f"PySpark: OK ({pyspark_msg})")
        
        return all_ok, results
    
    # =========================================================================
    # JDBC Drivers Check
    # =========================================================================
    
    def check_jdbc_drivers(self) -> Tuple[bool, str]:
        """Check if JDBC drivers are available."""
        exe_dir = get_exe_dir()
        script_dir = Path(__file__).parent.parent  # azure_migration_tool folder
        
        # Check for DB2 JDBC driver in multiple locations
        db2_locations = [
            exe_dir / 'drivers' / 'db2jcc4.jar',  # When running as exe
            self.bundled_dir / 'drivers' / 'db2jcc4.jar',  # PyInstaller _MEIPASS
            script_dir / 'drivers' / 'db2jcc4.jar',  # When running from source
            self.app_data / 'drivers' / 'db2jcc4.jar',
            Path(os.environ.get('DB2_JDBC_DRIVER_PATH', '')) / 'db2jcc4.jar',
        ]
        
        db2_found = False
        # Set environment variable if found
        for loc in db2_locations:
            if loc.name and loc.exists():
                os.environ['DB2_JDBC_DRIVER_PATH'] = str(loc.parent)
                db2_found = True
                break
        
        # SQL Server JDBC is typically bundled with PySpark or downloaded automatically
        
        if db2_found:
            return True, "JDBC drivers found"
        else:
            return False, "DB2 JDBC driver (db2jcc4.jar) not found"
    
    def setup_jdbc_drivers(self) -> bool:
        """Setup JDBC drivers directory."""
        drivers_dir = self.app_data / 'drivers'
        drivers_dir.mkdir(parents=True, exist_ok=True)
        
        # Check if bundled drivers exist and copy them
        bundled_drivers = self.bundled_dir / 'drivers'
        if bundled_drivers.exists():
            for jar in bundled_drivers.glob('*.jar'):
                dest = drivers_dir / jar.name
                if not dest.exists():
                    shutil.copy(jar, dest)
                    self._report(f"Copied {jar.name} to drivers directory", 80)
        
        # Set environment variable for driver path
        os.environ['DB2_JDBC_DRIVER_PATH'] = str(drivers_dir)
        
        return True
    
    # =========================================================================
    # Main Setup Flow
    # =========================================================================
    
    def run_full_setup(self) -> Tuple[bool, List[str]]:
        """
        Run complete dependency setup.
        Returns (success, list of messages).
        """
        messages = []
        all_ok = True
        
        self._report("Checking dependencies...", 0)
        
        # 1. Check Java
        self._report("Checking Java Runtime...", 5)
        java_ok, java_msg = self.check_java()
        if not java_ok:
            self._report("Java not found. Installing...", 10)
            if self.install_java():
                messages.append("Java 17 installed successfully")
            else:
                messages.append("WARNING: Java installation failed. PySpark features will not work.")
                messages.append("Please install Java 11 or 17 manually from https://adoptium.net/")
                all_ok = False
        else:
            messages.append(f"Java: {java_msg}")
        
        # 2. Check ODBC Driver
        self._report("Checking ODBC Driver...", 55)
        odbc_ok, odbc_msg = self.check_odbc_driver()
        if not odbc_ok:
            self._report("ODBC Driver not found...", 60)
            if is_admin():
                if self.install_odbc_driver():
                    messages.append("SQL Server ODBC Driver 18 installed successfully")
                else:
                    messages.append("WARNING: ODBC driver installation failed.")
                    all_ok = False
            else:
                messages.append("NOTE: ODBC driver not installed. Run as Administrator for auto-install,")
                messages.append("or download from: https://go.microsoft.com/fwlink/?linkid=2249006")
        else:
            messages.append(f"ODBC Driver: {odbc_msg}")
        
        # 3. Setup JDBC Drivers
        self._report("Setting up JDBC drivers...", 78)
        self.setup_jdbc_drivers()
        jdbc_ok, jdbc_msg = self.check_jdbc_drivers()
        if jdbc_ok:
            messages.append(f"JDBC Drivers: {jdbc_msg}")
        else:
            messages.append("NOTE: DB2 JDBC driver not found. Place db2jcc4.jar in the drivers folder")
            messages.append(f"Drivers folder: {self.app_data / 'drivers'}")
        
        # 4. Create config marker
        self._report("Finalizing setup...", 95)
        setup_marker = self.app_data / '.setup_complete'
        setup_marker.write_text(f"Setup completed on {platform.node()}")
        
        self._report("Setup complete!", 100)
        
        return all_ok, messages
    
    def is_setup_complete(self) -> bool:
        """Check if initial setup has been completed."""
        setup_marker = self.app_data / '.setup_complete'
        return setup_marker.exists()
    
    def quick_check(self) -> Tuple[bool, List[str]]:
        """Quick check of all dependencies without installing."""
        issues = []
        
        java_ok, java_msg = self.check_java()
        if not java_ok:
            issues.append(f"Java: {java_msg}")
        
        odbc_ok, odbc_msg = self.check_odbc_driver()
        if not odbc_ok:
            issues.append(f"ODBC: {odbc_msg}")
        
        # Do NOT require Python/PySpark when running as frozen exe: the built exe uses
        # Legacy validation (Python-only, no PySpark). Requiring them would show false
        # "dependency" errors on every PC without Python installed.
        # (Python/PySpark checks are still used by the full setup flow if user opts in.)
        
        jdbc_ok, jdbc_msg = self.check_jdbc_drivers()
        if not jdbc_ok:
            issues.append(f"JDBC: {jdbc_msg}")
        
        return len(issues) == 0, issues


def show_setup_dialog(parent=None):
    """Show a setup dialog with progress."""
    import tkinter as tk
    from tkinter import ttk, messagebox
    
    if parent is None:
        root = tk.Tk()
        root.withdraw()
    else:
        root = parent
    
    # Create setup window
    setup_win = tk.Toplevel(root) if parent else tk.Tk()
    setup_win.title("Azure Migration Tool - First Time Setup")
    setup_win.geometry("500x350")
    setup_win.resizable(False, False)
    
    # Center window
    setup_win.update_idletasks()
    x = (setup_win.winfo_screenwidth() - 500) // 2
    y = (setup_win.winfo_screenheight() - 350) // 2
    setup_win.geometry(f"+{x}+{y}")
    
    # Title
    ttk.Label(
        setup_win, 
        text="Setting up Azure Migration Tool",
        font=('Arial', 14, 'bold')
    ).pack(pady=20)
    
    ttk.Label(
        setup_win,
        text="Installing required dependencies...",
        font=('Arial', 10)
    ).pack(pady=5)
    
    # Progress bar
    progress_var = tk.DoubleVar()
    progress_bar = ttk.Progressbar(setup_win, variable=progress_var, maximum=100, length=400)
    progress_bar.pack(pady=20)
    
    # Status label
    status_var = tk.StringVar(value="Initializing...")
    status_label = ttk.Label(setup_win, textvariable=status_var, wraplength=450)
    status_label.pack(pady=10)
    
    # Log area
    log_text = tk.Text(setup_win, height=8, width=60, state='disabled')
    log_text.pack(pady=10, padx=20)
    
    def log_message(msg):
        log_text.config(state='normal')
        log_text.insert('end', msg + '\n')
        log_text.see('end')
        log_text.config(state='disabled')
        setup_win.update()
    
    def update_progress(message: str, percent: int):
        status_var.set(message)
        progress_var.set(percent)
        setup_win.update()
    
    def run_setup():
        checker = DependencyChecker(progress_callback=update_progress)
        success, messages = checker.run_full_setup()
        
        for msg in messages:
            log_message(msg)
        
        if success:
            log_message("\nSetup completed successfully!")
            setup_win.after(2000, setup_win.destroy)
        else:
            log_message("\nSetup completed with warnings. Some features may not work.")
            ttk.Button(setup_win, text="Continue", command=setup_win.destroy).pack(pady=10)
    
    # Run setup after window is shown
    setup_win.after(500, run_setup)
    
    if parent is None:
        setup_win.mainloop()
    else:
        setup_win.wait_window()


def ensure_dependencies(parent=None) -> bool:
    """
    Ensure all dependencies are available.
    Call this at application startup.
    Returns True if all dependencies are ready.
    """
    checker = DependencyChecker()
    
    # Quick check first
    all_ok, issues = checker.quick_check()
    
    if all_ok:
        return True
    
    # Check if this is first run
    if not checker.is_setup_complete():
        # Show setup dialog
        show_setup_dialog(parent)
        return checker.quick_check()[0]
    else:
        # Setup was done before but something is missing
        # Just log warnings
        for issue in issues:
            logger.warning(issue)
        return False


if __name__ == "__main__":
    # Run setup directly for testing
    show_setup_dialog()
