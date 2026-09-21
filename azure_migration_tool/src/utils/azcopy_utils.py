# Author: Satish Chauhan
"""
AzCopy + Azure CLI helpers for large .bak uploads to blob storage.

AzCopy is preferred for multi-GB backup files (parallel blocks, resume).
Managed-identity / Azure AD uploads run ``azcopy login`` once per app session, then a
single AzCopy job per stripe folder (avoids dozens of parallel Azure CLI token calls).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

try:
    from .subprocess_utils import popen_silent, run_silent
except ImportError:
    from src.utils.subprocess_utils import popen_silent, run_silent

_AZCOPY_WIN_CANDIDATES = (
    r"C:\Program Files\AzCopy\azcopy.exe",
    r"C:\Program Files (x86)\Microsoft SDKs\Azure\AzCopy\AzCopy.exe",
)

# Per-file block upload parallelism inside each AzCopy process (I/O routines).
DEFAULT_AZCOPY_CONCURRENCY_VALUE = 32
# Max simultaneous AzCopy processes when uploading multiple .bak stripes.
DEFAULT_MAX_PARALLEL_FILE_UPLOADS = 16
MAX_PARALLEL_FILE_UPLOADS_CAP = 64
# Many simultaneous AzCopy + AZCLI auto-login calls fail (AzureCLICredential status 1).
MAX_PARALLEL_ENTRA_FILE_UPLOADS = 4

_azcopy_entra_lock = threading.Lock()
_azcopy_entra_session_ready = False

_AZ_CLI_WIN_CANDIDATES = (
    r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
    r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
    r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.exe",
)


def _app_install_dirs() -> List[Path]:
    """Install dir (next to exe) and PyInstaller bundle dir."""
    dirs: List[Path] = []
    try:
        from .bcp_tools import get_app_base_dir, get_bundled_resource_dir
    except ImportError:
        try:
            from src.utils.bcp_tools import get_app_base_dir, get_bundled_resource_dir
        except ImportError:
            return dirs
    dirs.append(get_app_base_dir())
    bundled = get_bundled_resource_dir()
    if bundled not in dirs:
        dirs.append(bundled)
    return dirs


def find_azcopy_executable() -> Optional[str]:
    """Return path to azcopy.exe: bundled with app first, then PATH / system install."""
    for base in _app_install_dirs():
        bundled = base / "tools" / "azcopy" / "azcopy.exe"
        if bundled.is_file():
            return str(bundled.resolve())

    found = shutil.which("azcopy")
    if found:
        return found
    if sys.platform.startswith("win"):
        for candidate in _AZCOPY_WIN_CANDIDATES:
            if os.path.isfile(candidate):
                return candidate
        local = os.environ.get("LOCALAPPDATA", "")
        if local:
            guess = os.path.join(local, "Microsoft", "AzureStorage", "AzCopy", "azcopy.exe")
            if os.path.isfile(guess):
                return guess
    return None


def augment_path_for_azure_tools() -> None:
    """Prepend bundled AzCopy and common Azure CLI install dirs to PATH (GUI/frozen exe)."""
    if not sys.platform.startswith("win"):
        return
    extras: List[str] = []
    for base in _app_install_dirs():
        azcopy_dir = base / "tools" / "azcopy"
        if azcopy_dir.is_dir():
            extras.append(str(azcopy_dir))
    for candidate in _AZ_CLI_WIN_CANDIDATES:
        wbin = os.path.dirname(candidate)
        if wbin and os.path.isdir(wbin) and wbin not in extras:
            extras.append(wbin)
    path = os.environ.get("PATH", "")
    for entry in extras:
        if entry and entry not in path.split(os.pathsep):
            path = entry + os.pathsep + path
    os.environ["PATH"] = path


def find_az_cli_executable() -> Optional[str]:
    """Return path to ``az`` CLI (PATH, then standard Windows install locations)."""
    augment_path_for_azure_tools()
    found = shutil.which("az")
    if found and os.path.isfile(found):
        return found
    if sys.platform.startswith("win"):
        for candidate in _AZ_CLI_WIN_CANDIDATES:
            if os.path.isfile(candidate):
                return candidate
    return None


def _az_cli_command(*cli_args: str) -> List[str]:
    """Build argv for Azure CLI (handles az.cmd on Windows)."""
    az = find_az_cli_executable()
    if not az:
        raise FileNotFoundError("Azure CLI (az) is not installed.")
    if sys.platform.startswith("win") and az.lower().endswith(".cmd"):
        return ["cmd.exe", "/c", az, *cli_args]
    return [az, *cli_args]


def _run_cmd(
    args: List[str],
    *,
    env: Optional[Dict[str, str]] = None,
    timeout: int = 120,
    visible_console: bool = False,
) -> Tuple[int, str, str]:
    """Run a subprocess; optionally show a console window (for interactive az login)."""
    kwargs: Dict[str, Any] = {
        "capture_output": True,
        "text": True,
        "timeout": timeout,
        "env": env,
    }
    if visible_console and sys.platform.startswith("win"):
        kwargs.pop("capture_output", None)
        kwargs["creationflags"] = subprocess.CREATE_NEW_CONSOLE
        proc = subprocess.Popen(args, env=env)
        proc.wait(timeout=timeout)
        return proc.returncode or 0, "", ""
    result = run_silent(args, **kwargs)
    return result.returncode, (result.stdout or "").strip(), (result.stderr or "").strip()


def get_azcopy_version() -> str:
    exe = find_azcopy_executable()
    if not exe:
        return ""
    try:
        rc, out, err = _run_cmd([exe, "--version"], timeout=30)
        text = out or err
        if rc == 0 and text:
            return text.splitlines()[0].strip()
    except Exception:
        pass
    return ""


def _extract_device_code_message(stdout: str, stderr: str) -> str:
    """Pull device-login URL/code lines for a GUI message box."""
    lines: List[str] = []
    for raw in (stdout + "\n" + stderr).splitlines():
        line = raw.strip()
        if not line:
            continue
        lower = line.lower()
        if "microsoft.com/devicelogin" in lower or "to sign in, use a web browser" in lower:
            lines.append(line)
        if "code" in lower and any(ch.isdigit() for ch in line):
            lines.append(line)
    return "\n".join(lines[:6])


def get_azure_cli_account() -> Dict[str, Any]:
    """
    Return Azure CLI status: installed, logged_in, user, subscription, tenant.

    Does not prompt; uses existing ``az login`` session only.
    """
    az = find_az_cli_executable()
    out: Dict[str, Any] = {
        "installed": bool(az),
        "path": az or "",
        "logged_in": False,
        "user": "",
        "subscription": "",
        "tenant": "",
        "message": "",
    }
    if not az:
        out["message"] = "Azure CLI (az) is not installed or not on PATH."
        return out
    try:
        rc, stdout, stderr = _run_cmd(
            _az_cli_command("account", "show", "--output", "json"),
            timeout=45,
        )
        if rc != 0:
            out["message"] = (
                stderr or stdout or "Not signed in. Run 'Sign in to Azure' in the app."
            )
            return out
        import json

        data = json.loads(stdout or "{}")
        out["logged_in"] = True
        user = data.get("user") or {}
        out["user"] = str(user.get("name") or user.get("userPrincipalName") or "")
        out["subscription"] = str(data.get("name") or data.get("id") or "")
        out["tenant"] = str(data.get("tenantId") or "")
        out["message"] = f"Signed in as {out['user']}" if out["user"] else "Signed in."
        return out
    except Exception as exc:
        out["message"] = str(exc)
        return out


def format_azure_tools_status() -> str:
    """One-line human-readable status for the GUI."""
    azcopy_ver = get_azcopy_version()
    azcopy_ok = bool(find_azcopy_executable())
    cli = get_azure_cli_account()
    parts: List[str] = []
    if azcopy_ok:
        parts.append(f"AzCopy: {azcopy_ver or 'installed'}")
    else:
        parts.append("AzCopy: not installed")
    if not cli.get("installed"):
        parts.append("Azure CLI: not installed")
    elif cli.get("logged_in"):
        user = cli.get("user") or "signed in"
        parts.append(f"Azure CLI: {user}")
    else:
        parts.append("Azure CLI: not signed in")
    return " | ".join(parts)


def install_instructions() -> str:
    bundled = find_azcopy_executable()
    azcopy_line = (
        "AzCopy is bundled with the Azure Migration Tool installer (tools\\azcopy).\n"
        if bundled
        else "Re-run the Azure Migration Tool Setup installer, or:\n"
        "  winget install Microsoft.Azure.AzCopy\n"
    )
    return (
        azcopy_line
        + "\nFor Azure AD blob upload mode, Azure CLI is also required:\n"
        "  winget install Microsoft.AzureCLI\n"
        "Then click 'Sign in to Azure' in this app (or run: az login --use-device-code).\n"
        "Restart the app after installing so PATH is refreshed."
    )


def run_az_login(
    log: Optional[Callable[[str], None]] = None,
    *,
    use_device_code: bool = True,
    on_device_code: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    Run ``az login``. Returns True when an account is available afterward.

    Device code is default so GUI apps without a console still work; output is
    written to ``log``. Set ``use_device_code=False`` to open the system browser
    in a new console window (Windows).
    """
    _log = log or (lambda _m: None)
    az = find_az_cli_executable()
    if not az:
        _log("Azure CLI (az) is not installed.")
        _log(install_instructions())
        return False

    before = get_azure_cli_account()
    if before.get("logged_in"):
        _log(f"Already signed in: {before.get('user')}")
        return True

    _log("Starting Azure sign-in (az login)...")
    try:
        if use_device_code:
            rc, stdout, stderr = _run_cmd(
                _az_cli_command("login", "--use-device-code"),
                timeout=600,
            )
            for line in (stdout + "\n" + stderr).splitlines():
                line = line.strip()
                if line:
                    _log(line)
            device_hint = _extract_device_code_message(stdout, stderr)
            if device_hint and on_device_code:
                on_device_code(device_hint)
            if rc != 0:
                _log(f"az login failed (exit {rc}).")
                return False
        else:
            _log("Opening a console window for az login (complete sign-in there)...")
            flags = subprocess.CREATE_NEW_CONSOLE if sys.platform.startswith("win") else 0
            proc = subprocess.Popen(
                _az_cli_command("login"),
                creationflags=flags,
            )
            proc.wait(timeout=600)
            if proc.returncode != 0:
                _log(f"az login failed (exit {proc.returncode}).")
                return False
    except Exception as exc:
        _log(f"az login error: {exc}")
        return False

    after = get_azure_cli_account()
    if after.get("logged_in"):
        _log(f"[OK] Signed in as {after.get('user')}")
        return True
    _log("Sign-in did not complete. Try again or run 'az login' in a terminal.")
    return False


def build_blob_destination_url(
    *,
    account_url: str,
    container: str,
    blob_path: str,
) -> str:
    """Build https://account.blob.../container/path for AzCopy."""
    raw = (account_url or "").strip().rstrip("/")
    if not raw:
        raise ValueError("Storage account URL is required.")
    # Normalize when caller passed account/container in the URL (reuse bak_to_blob when available).
    if raw.count("/") > 3:
        try:
            try:
                from ..backup.bak_to_blob import _parse_storage_account_url
            except ImportError:
                from src.backup.bak_to_blob import _parse_storage_account_url
            raw, _ = _parse_storage_account_url(raw, container or "")
        except Exception:
            parts = raw.split("/")
            raw = "/".join(parts[:3])
    cont = (container or "").strip().strip("/")
    path = (blob_path or "").strip().lstrip("/")
    if not cont:
        raise ValueError("Container name is required for AzCopy upload.")
    encoded_path = "/".join(quote(part, safe="") for part in path.split("/") if part)
    if encoded_path:
        return f"{raw}/{cont}/{encoded_path}"
    return f"{raw}/{cont}"


def _parse_storage_parts(connection_string: str) -> Tuple[str, str, str]:
    try:
        from ..backup.bak_to_blob import _parse_storage_connection_string
    except ImportError:
        from src.backup.bak_to_blob import _parse_storage_connection_string

    parts = _parse_storage_connection_string(connection_string)
    account = parts.get("accountname", "")
    key = parts.get("accountkey", "")
    suffix = parts.get("endpointsuffix", "core.windows.net")
    return account, key, suffix


def resolve_parallel_upload_workers(file_count: int, requested: Optional[int] = None) -> int:
    """
    How many .bak files to upload at once with separate AzCopy jobs.

    ``requested`` None or <= 0 means all stripe files in parallel (up to MAX_PARALLEL_FILE_UPLOADS_CAP).
    """
    n = max(1, int(file_count or 1))
    if requested is None or requested <= 0:
        return min(n, MAX_PARALLEL_FILE_UPLOADS_CAP)
    return max(1, min(int(requested), n, MAX_PARALLEL_FILE_UPLOADS_CAP))


def azcopy_concurrency_for_parallel_jobs(parallel_file_jobs: int) -> int:
    """Tune per-file AzCopy concurrency when multiple uploads run in parallel."""
    jobs = max(1, int(parallel_file_jobs or 1))
    base = DEFAULT_AZCOPY_CONCURRENCY_VALUE
    per_job = max(4, min(base, base // jobs))
    return per_job


def apply_azcopy_performance_env(
    env: Dict[str, str],
    *,
    parallel_file_jobs: int = 1,
    concurrency_value: Optional[int] = None,
) -> Dict[str, str]:
    """Set AzCopy env vars for high-throughput upload (respect existing env overrides)."""
    out = dict(env)
    if "AZCOPY_CONCURRENCY_VALUE" not in out:
        cv = concurrency_value
        if cv is None:
            cv = azcopy_concurrency_for_parallel_jobs(parallel_file_jobs)
        out["AZCOPY_CONCURRENCY_VALUE"] = str(max(1, int(cv)))
    if parallel_file_jobs > 1 and "AZCOPY_CONCURRENT_FILES" not in out:
        out["AZCOPY_CONCURRENT_FILES"] = str(
            min(MAX_PARALLEL_FILE_UPLOADS_CAP, parallel_file_jobs)
        )
    return out


def apply_azcopy_entra_auth_env(
    env: Dict[str, str],
    *,
    tenant_id: str = "",
    use_azcopy_login_cache: bool = False,
) -> Dict[str, str]:
    """
    Configure AzCopy for Microsoft Entra ID.

    After ``azcopy login``, leave env unchanged so AzCopy uses its cached token.
    Otherwise set ``AZCOPY_AUTO_LOGIN_TYPE=AZCLI`` (one AzCopy process at a time is safest).
    """
    out = dict(env)
    if use_azcopy_login_cache:
        return out
    if "AZCOPY_AUTO_LOGIN_TYPE" not in out:
        out["AZCOPY_AUTO_LOGIN_TYPE"] = "AZCLI"
    tid = (tenant_id or "").strip()
    if tid and "AZCOPY_TENANT_ID" not in out:
        out["AZCOPY_TENANT_ID"] = tid
    return out


def run_azcopy_login(
    log: Callable[[str], None],
    *,
    tenant_id: str = "",
) -> bool:
    """Run ``azcopy login`` using the existing ``az login`` session (non-interactive)."""
    azcopy = find_azcopy_executable()
    if not azcopy:
        log("AzCopy executable not found.")
        return False
    env = dict(os.environ)
    env["AZCOPY_AUTO_LOGIN_TYPE"] = "AZCLI"
    tid = (tenant_id or "").strip()
    if tid:
        env["AZCOPY_TENANT_ID"] = tid
    cmd = [azcopy, "login", "--login-type", "AZCLI"]
    if tid:
        cmd.extend(["--tenant-id", tid])
    log("AzCopy: establishing Entra ID session (azcopy login, uses your az login)...")
    try:
        rc, stdout, stderr = _run_cmd(cmd, timeout=180, env=env)
        for line in (stdout + "\n" + stderr).splitlines():
            line = line.strip()
            if line:
                log(line)
        if rc != 0:
            log(f"azcopy login failed (exit {rc}).")
            return False
        return True
    except Exception as exc:
        log(f"azcopy login error: {exc}")
        return False


def prepare_azcopy_entra_auth(
    log: Callable[[str], None],
    *,
    auto_login: bool = True,
) -> Optional[str]:
    """
    One-time Entra ID setup for AzCopy uploads on this process.

    Returns an error message, or None when ready.
    """
    global _azcopy_entra_session_ready
    with _azcopy_entra_lock:
        if _azcopy_entra_session_ready:
            return None
        cli_err = ensure_azure_cli_login_for_azcopy(log, auto_login=auto_login)
        if cli_err:
            return cli_err
        cli = get_azure_cli_account()
        if not run_azcopy_login(log, tenant_id=str(cli.get("tenant") or "")):
            return (
                "AzCopy could not sign in with your Azure CLI session.\n\n"
                "In a terminal run: az login\n"
                "Then: azcopy login --login-type AZCLI\n"
                "Or use Blob connection string (account key) mode for uploads."
            )
        _azcopy_entra_session_ready = True
        user = cli.get("user") or "signed in"
        log(f"AzCopy Entra ID session ready ({user})")
        return None


def cap_parallel_workers_for_entra(blob_auth_mode: str, workers: int) -> int:
    mode = (blob_auth_mode or "").strip().lower()
    if mode != "managed_identity":
        return workers
    return max(1, min(workers, MAX_PARALLEL_ENTRA_FILE_UPLOADS))


def _stream_process_output(
    proc,
    log: Callable[[str], None],
    on_line: Optional[Callable[[str], None]] = None,
) -> int:
    assert proc.stdout is not None
    for line in proc.stdout:
        text = line.rstrip()
        if text:
            log(text)
            if on_line:
                on_line(text)
    proc.wait()
    return int(proc.returncode or 0)


def upload_file_with_azcopy(
    local_file: Path,
    *,
    blob_auth_mode: str,
    blob_connection_string: str,
    blob_account_url: str,
    container: str,
    blob_path: str,
    log: Callable[[str], None],
    parallel_file_jobs: int = 1,
    concurrency_value: Optional[int] = None,
    on_azcopy_line: Optional[Callable[[str], None]] = None,
) -> str:
    """
    Upload a local file with AzCopy. Returns the blob URL on success.

    Raises RuntimeError / ValueError with user-facing messages on failure.
    """
    azcopy = find_azcopy_executable()
    if not azcopy:
        raise RuntimeError(
            "AzCopy is not installed on this host.\n\n" + install_instructions()
        )

    mode = (blob_auth_mode or "connection_string").strip().lower()
    env = apply_azcopy_performance_env(
        dict(os.environ),
        parallel_file_jobs=parallel_file_jobs,
        concurrency_value=concurrency_value,
    )
    resolved_account_url = (blob_account_url or "").strip()

    if mode == "managed_identity":
        prep_err = prepare_azcopy_entra_auth(log, auto_login=True)
        if prep_err:
            raise RuntimeError(prep_err)
        cli = get_azure_cli_account()
        if not resolved_account_url:
            raise ValueError("Storage account URL is required for Managed Identity / AzCopy login mode.")
        env = apply_azcopy_entra_auth_env(
            env,
            tenant_id=str(cli.get("tenant") or ""),
            use_azcopy_login_cache=_azcopy_entra_session_ready,
        )
        log(
            f"  AzCopy auth: Microsoft Entra ID ({cli.get('user') or 'signed in'})"
        )
    else:
        if not blob_connection_string or not blob_connection_string.strip():
            raise ValueError("Blob connection string is required for key-based AzCopy upload.")
        account, key, suffix = _parse_storage_parts(blob_connection_string)
        env["AZCOPY_ACCOUNT_NAME"] = account
        env["AZCOPY_ACCOUNT_KEY"] = key
        log("  AzCopy auth: storage account key (AZCOPY_ACCOUNT_NAME / KEY)")
        if not resolved_account_url:
            resolved_account_url = f"https://{account}.blob.{suffix}"

    dest_url = build_blob_destination_url(
        account_url=resolved_account_url,
        container=container,
        blob_path=blob_path,
    )
    cmd = [
        azcopy,
        "copy",
        str(local_file),
        dest_url,
        "--overwrite=true",
        "--check-length=true",
        "--log-level=INFO",
    ]

    log(
        f"  AzCopy: {local_file.name} -> {dest_url} "
        f"(concurrency={env.get('AZCOPY_CONCURRENCY_VALUE', '?')})"
    )
    proc = popen_silent(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1,
    )
    rc = _stream_process_output(proc, log, on_line=on_azcopy_line)
    if rc != 0:
        raise RuntimeError(f"AzCopy failed (exit code {rc}). See log above.")
    return dest_url


def upload_directory_with_azcopy(
    local_dir: Path,
    *,
    blob_auth_mode: str,
    blob_connection_string: str,
    blob_account_url: str,
    container: str,
    blob_dest_prefix: str,
    log: Callable[[str], None],
    parallel_file_jobs: int = 1,
    concurrency_value: Optional[int] = None,
    on_azcopy_line: Optional[Callable[[str], None]] = None,
) -> str:
    """
    Upload all files under ``local_dir`` to ``container/blob_dest_prefix/`` in one AzCopy job.

    Uses ``--recursive`` and ``--as-subdir=false`` so stripe files land directly under the prefix.
    Returns the destination folder URL (no trailing file name).
    """
    azcopy = find_azcopy_executable()
    if not azcopy:
        raise RuntimeError(
            "AzCopy is not installed on this host.\n\n" + install_instructions()
        )
    if not local_dir.is_dir():
        raise ValueError(f"Not a directory: {local_dir}")

    mode = (blob_auth_mode or "connection_string").strip().lower()
    env = apply_azcopy_performance_env(
        dict(os.environ),
        parallel_file_jobs=parallel_file_jobs,
        concurrency_value=concurrency_value,
    )
    resolved_account_url = (blob_account_url or "").strip()

    if mode == "managed_identity":
        prep_err = prepare_azcopy_entra_auth(log, auto_login=True)
        if prep_err:
            raise RuntimeError(prep_err)
        cli = get_azure_cli_account()
        if not resolved_account_url:
            raise ValueError("Storage account URL is required for Managed Identity / AzCopy login mode.")
        env = apply_azcopy_entra_auth_env(
            env,
            tenant_id=str(cli.get("tenant") or ""),
            use_azcopy_login_cache=_azcopy_entra_session_ready,
        )
        log(
            f"  AzCopy auth: Microsoft Entra ID ({cli.get('user') or 'signed in'})"
        )
    else:
        if not blob_connection_string or not blob_connection_string.strip():
            raise ValueError("Blob connection string is required for key-based AzCopy upload.")
        account, key, suffix = _parse_storage_parts(blob_connection_string)
        env["AZCOPY_ACCOUNT_NAME"] = account
        env["AZCOPY_ACCOUNT_KEY"] = key
        log("  AzCopy auth: storage account key (AZCOPY_ACCOUNT_NAME / KEY)")
        if not resolved_account_url:
            resolved_account_url = f"https://{account}.blob.{suffix}"

    prefix = (blob_dest_prefix or "").strip().strip("/")
    dest_url = build_blob_destination_url(
        account_url=resolved_account_url,
        container=container,
        blob_path=prefix,
    )
    if not dest_url.endswith("/"):
        dest_url += "/"

    cmd = [
        azcopy,
        "copy",
        str(local_dir),
        dest_url,
        "--recursive=true",
        "--as-subdir=false",
        "--overwrite=true",
        "--check-length=true",
        "--log-level=INFO",
    ]
    log(
        f"  AzCopy folder: {local_dir} -> {dest_url} "
        f"(concurrency={env.get('AZCOPY_CONCURRENCY_VALUE', '?')})"
    )
    proc = popen_silent(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1,
    )
    rc = _stream_process_output(proc, log, on_line=on_azcopy_line)
    if rc != 0:
        raise RuntimeError(f"AzCopy failed (exit code {rc}). See log above.")
    return dest_url.rstrip("/")


def run_azcopy_smoke_check(log: Callable[[str], None]) -> Optional[str]:
    """
    Verify AzCopy runs (``azcopy --version``). Returns error message or None.
    """
    azcopy = find_azcopy_executable()
    if not azcopy:
        return "AzCopy executable not found."
    ver = get_azcopy_version()
    if not ver:
        return "AzCopy did not return a version string."
    log(f"AzCopy smoke check OK: {ver.splitlines()[0] if ver else ver}")
    return None


def ensure_azure_cli_login_for_azcopy(
    log: Optional[Callable[[str], None]] = None,
    *,
    auto_login: bool = False,
) -> Optional[str]:
    """
    For AzCopy Entra ID uploads (``AZCOPY_AUTO_LOGIN_TYPE=AZCLI``). Returns error message,
    or None when CLI is signed in.
    """
    _log = log or (lambda _m: None)
    cli = get_azure_cli_account()
    if not cli.get("installed"):
        return "Azure CLI is not installed.\n\n" + install_instructions()
    if cli.get("logged_in"):
        _log(f"Azure CLI session OK ({cli.get('user')})")
        return None
    if auto_login:
        _log("Azure CLI not signed in — starting az login for AzCopy...")
        if run_az_login(_log, use_device_code=True):
            return None
        return (
            "Azure CLI sign-in failed. Click 'Sign in to Azure' in Step 3, "
            "or run: az login --use-device-code"
        )
    return (
        "Azure CLI is not signed in. AzCopy uploads in Managed Identity / Azure AD mode "
        "require az login on this host. Click 'Sign in to Azure' in Step 3."
    )


def ensure_azcopy_ready_for_upload(
    blob_auth_mode: str,
    log: Optional[Callable[[str], None]] = None,
    *,
    auto_login: bool = False,
) -> Optional[str]:
    """
    Pre-flight for AzCopy uploads. Returns an error message string, or None if ready.
    """
    _log = log or (lambda _m: None)
    if not find_azcopy_executable():
        msg = "AzCopy is not installed.\n\n" + install_instructions()
        _log(msg)
        return msg
    mode = (blob_auth_mode or "").strip().lower()
    smoke = run_azcopy_smoke_check(_log)
    if smoke:
        _log(smoke)
        return smoke
    if mode == "managed_identity":
        login_err = ensure_azure_cli_login_for_azcopy(_log, auto_login=auto_login)
        if login_err:
            _log(login_err)
            return login_err
        prep = prepare_azcopy_entra_auth(_log, auto_login=auto_login)
        if prep:
            _log(prep)
            return prep
        cli = get_azure_cli_account()
        _log(f"AzCopy ready (Azure AD: {cli.get('user')})")
    else:
        _log("AzCopy ready (storage account key mode)")
    return None
