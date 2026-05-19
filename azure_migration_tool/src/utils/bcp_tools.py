# Author: Satish Chauhan

"""Locate BCP and install SQL Server Command Line Utilities (bundled MSI or download)."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path
from typing import Callable, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Override BCP .dat file location (must be writable by the same user as bcp.exe).
BCP_WORK_DIR_ENV = "AMT_BCP_WORK_DIR"

_BCP_DOWNLOAD_URLS = (
    "https://go.microsoft.com/fwlink/?linkid=2230791",
    "https://go.microsoft.com/fwlink/?linkid=2142258",
)

_SYSTEM_BCP_PATHS = [
    r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe",
    r"C:\Program Files (x86)\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe",
    r"C:\Program Files\Microsoft SQL Server\160\Tools\Binn\bcp.exe",
    r"C:\Program Files (x86)\Microsoft SQL Server\160\Tools\Binn\bcp.exe",
    r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
    r"C:\Program Files (x86)\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
    r"C:\Program Files\Microsoft SQL Server\150\Tools\Binn\bcp.exe",
    r"C:\Program Files (x86)\Microsoft SQL Server\150\Tools\Binn\bcp.exe",
]


def get_app_base_dir() -> Path:
    """Directory next to exe (frozen) or azure_migration_tool package root (dev)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parent.parent.parent


def get_bundled_resource_dir() -> Path:
    """PyInstaller _MEIPASS when frozen, else package root."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent.parent.parent


def bcp_entra_token_via_cli_supported() -> bool:
    """Entra access-token file (-G -P token) for bcp is supported on Linux/macOS only."""
    return not sys.platform.startswith("win")


def find_bcp_exe(*, prefer_odbc_18: bool = False) -> Optional[str]:
    """Return path to bcp.exe: bundled tools folder, install dir, then system paths."""
    bases: List[Path] = [
        get_app_base_dir() / "tools" / "bcp",
        get_bundled_resource_dir() / "tools" / "bcp",
    ]
    for base in bases:
        for name in ("bcp.exe", "Binn/bcp.exe", "Binn\\bcp.exe"):
            p = base / name.replace("/", os.sep)
            if p.is_file():
                return str(p.resolve())

    paths = list(_SYSTEM_BCP_PATHS)
    if prefer_odbc_18:
        paths = [p for p in paths if "\\180\\" in p or "\\160\\" in p] + [
            p for p in paths if "\\180\\" not in p and "\\160\\" not in p
        ]
    for path in paths:
        if os.path.isfile(path):
            return path
    found = shutil.which("bcp.exe")
    return found


def find_bundled_sqlcmd_msi() -> Optional[Path]:
    """MSI shipped with app/installer for silent BCP install."""
    names = ("SqlCmdLnUtils.msi", "MsSqlCmdLnUtils.msi")
    dirs = [
        get_app_base_dir() / "tools",
        get_bundled_resource_dir() / "tools",
        get_app_base_dir() / "bcp",
        get_bundled_resource_dir() / "bcp",
        Path(__file__).resolve().parent.parent.parent / "installer" / "tools",
    ]
    for d in dirs:
        if not d.is_dir():
            continue
        for name in names:
            p = d / name
            if p.is_file() and p.stat().st_size > 500_000:
                return p.resolve()
    return None


def _run_msi_install(msi_path: Path, log: Optional[Callable[[str], None]] = None) -> Tuple[bool, str]:
    _log = log or (lambda _m: None)
    if not sys.platform.startswith("win"):
        return False, "BCP install is supported on Windows only"
    log_file = Path(tempfile.gettempdir()) / "amt_bcp_install.log"
    cmd = [
        "msiexec.exe",
        "/i",
        str(msi_path),
        "/quiet",
        "/norestart",
        "/L*v",
        str(log_file),
    ]
    _log(f"Installing SQL Command Line Utilities from {msi_path.name}...")
    try:
        from .subprocess_utils import run_silent

        r = run_silent(cmd, capture_output=True, text=True, timeout=600)
    except ImportError:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if r.returncode in (0, 3010):
        return True, f"Installed (exit {r.returncode}). Log: {log_file}"
    return False, (r.stderr or r.stdout or f"msiexec exit {r.returncode}")[:500]


def download_sqlcmd_msi(dest: Path, log: Optional[Callable[[str], None]] = None) -> bool:
    _log = log or (lambda _m: None)
    for url in _BCP_DOWNLOAD_URLS:
        try:
            _log(f"Downloading BCP tools from Microsoft...")
            urllib.request.urlretrieve(url, dest)
            if dest.is_file() and dest.stat().st_size > 500_000:
                _log(f"Downloaded {dest.stat().st_size // (1024 * 1024)} MB")
                return True
        except Exception as e:
            _log(f"Download failed ({url}): {e}")
    return False


def ensure_bcp_installed(
    log: Optional[Callable[[str], None]] = None,
    *,
    allow_download: bool = True,
) -> Tuple[bool, str]:
    """
    Ensure bcp.exe is available. Uses bundled MSI, then download+install, then system paths.

    Returns (success, message_or_path).
    """
    existing = find_bcp_exe()
    if existing:
        return True, existing

    _log = log or (lambda _m: None)
    msi = find_bundled_sqlcmd_msi()
    if msi:
        ok, msg = _run_msi_install(msi, _log)
        if ok:
            found = find_bcp_exe()
            if found:
                return True, found
            return False, "MSI installed but bcp.exe not found. Restart the app or reboot."
        _log(f"Bundled MSI install failed: {msg}")

    if allow_download:
        tools_dir = get_app_base_dir() / "tools"
        tools_dir.mkdir(parents=True, exist_ok=True)
        dest = tools_dir / "SqlCmdLnUtils.msi"
        if download_sqlcmd_msi(dest, _log):
            ok, msg = _run_msi_install(dest, _log)
            if ok:
                found = find_bcp_exe()
                if found:
                    return True, found
                return False, "Downloaded MSI installed but bcp.exe not found. Restart the app."

    return False, (
        "BCP not found. Install Microsoft SQL Server Command Line Utilities "
        "(ODBC tools) or run the app installer with BCP components."
    )


def _bcp_work_root_candidates(preferred: Optional[str] = None) -> List[Path]:
    """Directories to try for BCP bulk data files (stable, writable, not session-scoped)."""
    roots: List[Path] = []
    if preferred and str(preferred).strip():
        roots.append(Path(str(preferred).strip()))
    env_dir = (os.environ.get(BCP_WORK_DIR_ENV) or "").strip()
    if env_dir:
        roots.append(Path(env_dir))
    roots.append(get_app_base_dir() / "bcp_work")
    program_data = os.environ.get("ProgramData") or r"C:\ProgramData"
    roots.append(Path(program_data) / "AzureMigrationTool" / "bcp_work")
    # Last resort: system temp (can be %LOCALAPPDATA%\\Temp\\<session> on RDP).
    roots.append(Path(tempfile.gettempdir()) / "AzureMigrationTool" / "bcp_work")
    return roots


def verify_bcp_work_dir(path: Path) -> Tuple[bool, str]:
    """Create dir if needed and confirm the current process can write files there."""
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".bcp_write_probe"
        probe.write_bytes(b"ok")
        probe.unlink(missing_ok=True)
        return True, str(path.resolve())
    except OSError as exc:
        return False, str(exc)


def resolve_bcp_work_root(preferred: Optional[str] = None) -> str:
    """
    Pick a writable root for BCP migration folders.

    Prefers app/ProgramData locations over per-RDP-session Temp\\\\<id> paths, which
    can fail with "Unable to open BCP host data-file" for some process contexts.
    """
    errors: List[str] = []
    for candidate in _bcp_work_root_candidates(preferred):
        ok, msg = verify_bcp_work_dir(candidate)
        if ok:
            return msg
        errors.append(f"{candidate} ({msg})")
    raise RuntimeError(
        "No writable BCP work directory. Tried:\n  "
        + "\n  ".join(errors)
        + f"\nSet {BCP_WORK_DIR_ENV} or choose a folder in BCP Options (e.g. D:\\BCPWork)."
    )


def create_bcp_migration_dir(preferred_root: Optional[str] = None) -> str:
    """Create a unique subdirectory under a verified BCP work root."""
    root = resolve_bcp_work_root(preferred_root)
    return tempfile.mkdtemp(prefix="bcp_migration_", dir=root)


def format_bcp_data_path(path: str) -> str:
    """Absolute normalized path for bcp.exe; use 8.3 short path on Windows when possible."""
    path = os.path.normpath(os.path.abspath(path))
    if not sys.platform.startswith("win"):
        return path
    try:
        import ctypes

        buf = ctypes.create_unicode_buffer(32768)
        if ctypes.windll.kernel32.GetShortPathNameW(path, buf, 32768):
            return buf.value
    except Exception:
        pass
    return path


def ensure_bcp_output_file(path: str) -> None:
    """Ensure parent exists and the output file can be created (fail before calling bcp)."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "wb"):
        pass


def bcp_table_name_for_import(schema: str, table: str) -> str:
    """
    Table identifier for ``bcp ... in`` (not ``queryout``).

    Use schema.table without square brackets; combine with -q so bcp quotes identifiers.
    Bracketed form [schema].[table] is mis-parsed and yields Invalid object name errors.
    """
    return f"{schema}.{table}"


def bcp_query_for_export(schema: str, table: str) -> str:
    """SELECT for ``bcp ... queryout``."""
    return f"SELECT * FROM [{schema}].[{table}]"


def disk_free_gb(path: str) -> float:
    """Free space on the volume containing path."""
    return shutil.disk_usage(path).free / (1024**3)


def format_size_mb(num_bytes: int) -> str:
    if num_bytes >= 1024**3:
        return f"{num_bytes / (1024**3):.2f} GB"
    return f"{num_bytes / (1024**2):.1f} MB"


def bcp_host_data_file_error_hint() -> str:
    return (
        "BCP could not open the .dat file for writing. Use BCP Options to set a work folder "
        f"(e.g. D:\\BCPWork), set {BCP_WORK_DIR_ENV}, or check antivirus/UAC on the temp path."
    )
