# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Driver utilities for checking and installing required drivers.
Handles ODBC drivers and other dependencies for SQL Server connectivity.
"""

import os
import sys
import subprocess
import tempfile
import urllib.request
import platform
import ctypes
from pathlib import Path
from typing import Tuple, Optional, List


def is_admin() -> bool:
    """Check if the current process has administrator privileges."""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except:
        return False


def get_installed_odbc_drivers() -> List[str]:
    """Get list of installed ODBC drivers."""
    try:
        import pyodbc
        return pyodbc.drivers()
    except ImportError:
        return []


def check_sql_server_odbc_driver() -> Tuple[bool, Optional[str]]:
    """
    Check if a SQL Server ODBC driver is installed.
    
    Returns:
        Tuple of (is_installed, driver_name or None)
    """
    drivers = get_installed_odbc_drivers()
    
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server",
    ]
    
    for name in preferred:
        if name in drivers:
            return True, name
    
    # Check for any SQL Server driver
    for driver in drivers:
        if "SQL Server" in driver:
            return True, driver
    
    return False, None


def get_odbc_download_url() -> Tuple[str, str]:
    """
    Get the download URL for ODBC Driver based on system architecture.
    
    Returns:
        Tuple of (url, filename)
    """
    # Determine architecture
    is_64bit = sys.maxsize > 2**32
    
    # Microsoft ODBC Driver 18 download URLs
    if is_64bit:
        url = "https://go.microsoft.com/fwlink/?linkid=2249006"  # ODBC 18 x64
        filename = "msodbcsql18_x64.msi"
    else:
        url = "https://go.microsoft.com/fwlink/?linkid=2249005"  # ODBC 18 x86
        filename = "msodbcsql18_x86.msi"
    
    return url, filename


def download_odbc_driver(progress_callback=None) -> Tuple[bool, str]:
    """
    Download the ODBC driver installer.
    
    Args:
        progress_callback: Optional callback function(downloaded, total) for progress
        
    Returns:
        Tuple of (success, file_path or error_message)
    """
    url, filename = get_odbc_download_url()
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, filename)
    
    try:
        if progress_callback:
            progress_callback(0, 100)
        
        # Download with progress
        def report_progress(block_num, block_size, total_size):
            if progress_callback and total_size > 0:
                downloaded = block_num * block_size
                progress_callback(min(downloaded, total_size), total_size)
        
        urllib.request.urlretrieve(url, file_path, reporthook=report_progress)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            return True, file_path
        else:
            return False, "Downloaded file is empty or missing"
            
    except Exception as e:
        return False, f"Download failed: {str(e)}"


def install_odbc_driver_silent(msi_path: str, log_callback=None) -> Tuple[bool, str]:
    """
    Install ODBC driver silently using msiexec.
    Requires administrator privileges.
    
    Args:
        msi_path: Path to the MSI installer
        log_callback: Optional callback for logging messages
        
    Returns:
        Tuple of (success, message)
    """
    if not os.path.exists(msi_path):
        return False, f"Installer not found: {msi_path}"
    
    if not is_admin():
        return False, "Administrator privileges required for installation"
    
    try:
        log_path = os.path.join(tempfile.gettempdir(), "odbc_install.log")
        
        # Silent install with logging
        cmd = [
            "msiexec", "/i", msi_path,
            "/quiet", "/norestart",
            "IACCEPTMSODBCSQLLICENSETERMS=YES",
            f"/log", log_path
        ]
        
        if log_callback:
            log_callback(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes
        )
        
        if result.returncode == 0:
            return True, "Installation completed successfully"
        elif result.returncode == 3010:
            # Reboot required
            return True, "Installation completed. A restart may be required."
        else:
            error_msg = f"Installation failed with code {result.returncode}"
            if os.path.exists(log_path):
                error_msg += f"\nSee log: {log_path}"
            return False, error_msg
            
    except subprocess.TimeoutExpired:
        return False, "Installation timed out"
    except Exception as e:
        return False, f"Installation error: {str(e)}"


def install_odbc_driver_elevated(msi_path: str) -> Tuple[bool, str]:
    """
    Install ODBC driver with UAC elevation prompt.
    
    Args:
        msi_path: Path to the MSI installer
        
    Returns:
        Tuple of (success, message)
    """
    if not os.path.exists(msi_path):
        return False, f"Installer not found: {msi_path}"
    
    try:
        # Use ShellExecute to run with elevation
        import ctypes
        
        # Create a batch script that runs msiexec
        batch_path = os.path.join(tempfile.gettempdir(), "install_odbc.bat")
        log_path = os.path.join(tempfile.gettempdir(), "odbc_install.log")
        
        with open(batch_path, 'w') as f:
            f.write(f'@echo off\n')
            f.write(f'msiexec /i "{msi_path}" /quiet /norestart IACCEPTMSODBCSQLLICENSETERMS=YES /log "{log_path}"\n')
            f.write(f'echo Installation exit code: %ERRORLEVEL%\n')
            f.write(f'pause\n')
        
        # Run with elevation
        ret = ctypes.windll.shell32.ShellExecuteW(
            None,
            "runas",
            "cmd.exe",
            f'/c "{batch_path}"',
            None,
            1  # SW_SHOWNORMAL
        )
        
        if ret > 32:
            return True, "Installation started with elevation. Please wait for it to complete."
        else:
            return False, f"Failed to start elevated installation (error code: {ret})"
            
    except Exception as e:
        return False, f"Elevation error: {str(e)}"


def install_odbc_via_powershell() -> Tuple[bool, str]:
    """
    Install ODBC driver using PowerShell with automatic download.
    
    Returns:
        Tuple of (success, message)
    """
    is_64bit = sys.maxsize > 2**32
    
    if is_64bit:
        download_url = "https://go.microsoft.com/fwlink/?linkid=2249006"
    else:
        download_url = "https://go.microsoft.com/fwlink/?linkid=2249005"
    
    ps_script = f'''
$ErrorActionPreference = "Stop"
$tempDir = [System.IO.Path]::GetTempPath()
$installerPath = Join-Path $tempDir "msodbcsql18.msi"

Write-Host "Downloading ODBC Driver 18 for SQL Server..."
try {{
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    Invoke-WebRequest -Uri "{download_url}" -OutFile $installerPath -UseBasicParsing
}} catch {{
    # Fallback to WebClient
    $webClient = New-Object System.Net.WebClient
    $webClient.DownloadFile("{download_url}", $installerPath)
}}

if (Test-Path $installerPath) {{
    Write-Host "Installing ODBC Driver..."
    $logPath = Join-Path $tempDir "odbc_install.log"
    $process = Start-Process -FilePath "msiexec.exe" -ArgumentList "/i", "`"$installerPath`"", "/quiet", "/norestart", "IACCEPTMSODBCSQLLICENSETERMS=YES", "/log", "`"$logPath`"" -Wait -PassThru -Verb RunAs
    
    if ($process.ExitCode -eq 0 -or $process.ExitCode -eq 3010) {{
        Write-Host "Installation completed successfully!"
        exit 0
    }} else {{
        Write-Host "Installation failed with exit code: $($process.ExitCode)"
        exit 1
    }}
}} else {{
    Write-Host "Download failed - installer not found"
    exit 1
}}
'''
    
    try:
        result = subprocess.run(
            ["powershell", "-ExecutionPolicy", "Bypass", "-Command", ps_script],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes
        )
        
        if result.returncode == 0:
            return True, "ODBC Driver installed successfully"
        else:
            error_msg = result.stderr or result.stdout or "Unknown error"
            return False, f"Installation failed: {error_msg}"
            
    except subprocess.TimeoutExpired:
        return False, "Installation timed out"
    except Exception as e:
        return False, f"PowerShell error: {str(e)}"


def get_manual_install_instructions() -> str:
    """Get manual installation instructions for ODBC driver."""
    is_64bit = sys.maxsize > 2**32
    arch = "64-bit" if is_64bit else "32-bit"
    
    return f"""
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

Option 3: Using Chocolatey
--------------------------
Open Command Prompt as Administrator and run:
    choco install sqlserver-odbcdriver

After installation, restart this application.
"""


def check_pyodbc_installed() -> Tuple[bool, str]:
    """
    Check if pyodbc is installed and working.
    
    Returns:
        Tuple of (is_installed, version or error_message)
    """
    try:
        import pyodbc
        return True, pyodbc.version
    except ImportError as e:
        return False, f"pyodbc not installed: {str(e)}"
    except Exception as e:
        return False, f"pyodbc error: {str(e)}"


def check_all_dependencies() -> dict:
    """
    Check all required dependencies.
    
    Returns:
        Dictionary with dependency status
    """
    results = {
        "pyodbc": {"installed": False, "version": None, "error": None},
        "odbc_driver": {"installed": False, "name": None, "error": None},
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "architecture": "64-bit" if sys.maxsize > 2**32 else "32-bit",
            "python_version": sys.version
        }
    }
    
    # Check pyodbc
    pyodbc_ok, pyodbc_info = check_pyodbc_installed()
    results["pyodbc"]["installed"] = pyodbc_ok
    if pyodbc_ok:
        results["pyodbc"]["version"] = pyodbc_info
    else:
        results["pyodbc"]["error"] = pyodbc_info
    
    # Check ODBC driver
    if pyodbc_ok:
        driver_ok, driver_name = check_sql_server_odbc_driver()
        results["odbc_driver"]["installed"] = driver_ok
        if driver_ok:
            results["odbc_driver"]["name"] = driver_name
        else:
            results["odbc_driver"]["error"] = "No SQL Server ODBC driver found"
    else:
        results["odbc_driver"]["error"] = "Cannot check - pyodbc not available"
    
    return results

