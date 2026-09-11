# Author: Satish Chauhan
"""
AzCopy + Azure CLI helpers for large .bak uploads to blob storage.

AzCopy is preferred for multi-GB backup files (parallel blocks, resume).
Managed-identity / Azure AD uploads use ``azcopy --auth-mode Login`` which
requires an ``az login`` session on the CDC / app host.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
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


def find_azcopy_executable() -> Optional[str]:
    """Return path to azcopy.exe / azcopy on PATH or common install locations."""
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


def find_az_cli_executable() -> Optional[str]:
    """Return path to ``az`` CLI on PATH."""
    return shutil.which("az")


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
            [az, "account", "show", "--output", "json"],
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
    return (
        "Install on this Windows host (PowerShell as admin):\n"
        "  winget install Microsoft.AzureCLI\n"
        "  winget install Microsoft.Azure.AzCopy\n\n"
        "Then click 'Sign in to Azure' in this app (or run: az login).\n"
        "Restart the app after installing so PATH is refreshed."
    )


def run_az_login(
    log: Optional[Callable[[str], None]] = None,
    *,
    use_device_code: bool = True,
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
                [az, "login", "--use-device-code"],
                timeout=600,
            )
            for line in (stdout + "\n" + stderr).splitlines():
                line = line.strip()
                if line:
                    _log(line)
            if rc != 0:
                _log(f"az login failed (exit {rc}).")
                return False
        else:
            _log("Opening a console window for az login (complete sign-in there)...")
            flags = subprocess.CREATE_NEW_CONSOLE if sys.platform.startswith("win") else 0
            proc = subprocess.Popen([az, "login"], creationflags=flags)
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


def _stream_process_output(proc, log: Callable[[str], None]) -> int:
    assert proc.stdout is not None
    for line in proc.stdout:
        text = line.rstrip()
        if text:
            log(text)
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
    env = dict(os.environ)
    resolved_account_url = (blob_account_url or "").strip()

    if mode == "managed_identity":
        cli = get_azure_cli_account()
        if not cli.get("installed"):
            raise RuntimeError(
                "Azure CLI is required for AzCopy login auth.\n\n" + install_instructions()
            )
        if not cli.get("logged_in"):
            raise RuntimeError(
                "Azure CLI is not signed in. Click 'Sign in to Azure' in the app, "
                "then retry the upload."
            )
        if not resolved_account_url:
            raise ValueError("Storage account URL is required for Managed Identity / AzCopy login mode.")
        cmd_auth = ["--auth-mode", "Login"]
        log(f"  AzCopy auth: Azure AD (az login as {cli.get('user')})")
    else:
        if not blob_connection_string or not blob_connection_string.strip():
            raise ValueError("Blob connection string is required for key-based AzCopy upload.")
        account, key, suffix = _parse_storage_parts(blob_connection_string)
        env["AZCOPY_ACCOUNT_NAME"] = account
        env["AZCOPY_ACCOUNT_KEY"] = key
        cmd_auth = ["--auth-mode", "Key"]
        log("  AzCopy auth: storage account key (connection string)")
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
        *cmd_auth,
    ]

    log(f"  AzCopy: {local_file.name} -> {dest_url}")
    proc = popen_silent(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1,
    )
    rc = _stream_process_output(proc, log)
    if rc != 0:
        raise RuntimeError(f"AzCopy failed (exit code {rc}). See log above.")
    return dest_url


def ensure_azcopy_ready_for_upload(
    blob_auth_mode: str,
    log: Optional[Callable[[str], None]] = None,
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
    if mode == "managed_identity":
        cli = get_azure_cli_account()
        if not cli.get("installed"):
            msg = "Azure CLI is not installed.\n\n" + install_instructions()
            _log(msg)
            return msg
        if not cli.get("logged_in"):
            msg = (
                "Azure CLI is not signed in. Click 'Sign in to Azure' in Step 3, "
                "then retry the upload."
            )
            _log(msg)
            return msg
        _log(f"AzCopy ready (Azure AD: {cli.get('user')})")
    else:
        _log("AzCopy ready (storage account key mode)")
    return None
