# Author: S@tish Ch@uhan

"""Check GitHub Releases and apply silent background updates (Windows frozen exe)."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Tuple

import requests

DEFAULT_GITHUB_REPO = "satishchauhan66/azure_migration_tool"
_GITHUB_API = "https://api.github.com/repos/{repo}/releases/latest"
_USER_AGENT = "AzureMigrationTool-UpdateCheck"
_VERSION_FROM_TAG = re.compile(r"^(?:build/|v)?(.+)$", re.IGNORECASE)
ProgressCallback = Callable[[int, int], None]


@dataclass(frozen=True)
class UpdateInfo:
    """Result of comparing the running app to the latest GitHub release."""

    current_version: str
    latest_version: str
    release_page_url: str
    portable_download_url: Optional[str]
    setup_download_url: Optional[str]
    release_notes: str

    @property
    def download_url(self) -> Optional[str]:
        """Preferred asset URL (portable exe, else installer)."""
        return self.portable_download_url or self.setup_download_url

    @property
    def is_update_available(self) -> bool:
        return version_tuple(self.latest_version) > version_tuple(self.current_version)

    @property
    def can_apply_silently(self) -> bool:
        return bool(self.portable_download_url)


def version_tuple(version: str) -> Tuple[int, ...]:
    """Parse semver-like strings (e.g. 1.2.456, v1.2, build/1.0.1) for ordering."""
    text = (version or "").strip()
    m = _VERSION_FROM_TAG.match(text)
    if m:
        text = m.group(1).strip()
    parts: list[int] = []
    for segment in text.split("."):
        segment = segment.strip()
        if not segment:
            continue
        num = ""
        for ch in segment:
            if ch.isdigit():
                num += ch
            else:
                break
        parts.append(int(num) if num else 0)
    return tuple(parts) if parts else (0,)


def summarize_release_notes_for_user(body: str, max_len: int = 200) -> str:
    """
    Short user-facing blurb only — not the full CI-generated GitHub release body.
    """
    text = (body or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    ci_markers = (
        "workflow run",
        "github latest",
        "actions artifacts",
        "standalone executable",
        "| **version** |",
        "build from `main`",
    )
    if any(marker in lowered for marker in ci_markers):
        return ""
    line = text.splitlines()[0].strip()
    line = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", line)
    line = re.sub(r"`([^`]+)`", r"\1", line)
    if len(line) > max_len:
        line = line[: max_len - 3].rstrip() + "..."
    return line


def normalize_release_version(tag_or_name: str) -> str:
    """Extract a display/compare version from a GitHub tag or release title."""
    text = (tag_or_name or "").strip()
    m = _VERSION_FROM_TAG.match(text)
    if m:
        return m.group(1).strip()
    if text.lower().startswith("v"):
        return text[1:].split("(", 1)[0].strip()
    return text


def _app_data_dir() -> str:
    base = os.environ.get("APPDATA") or os.path.expanduser("~")
    path = os.path.join(base, "AzureMigrationTool")
    os.makedirs(path, exist_ok=True)
    return path


def updates_dir() -> Path:
    path = Path(_app_data_dir()) / "updates"
    path.mkdir(parents=True, exist_ok=True)
    return path


def staged_manifest_path() -> Path:
    return updates_dir() / "staged_update.json"


def dismissed_update_version_path() -> str:
    return os.path.join(_app_data_dir(), "dismissed_update_version.txt")


def read_dismissed_update_version() -> Optional[str]:
    path = dismissed_update_version_path()
    try:
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                value = f.read().strip()
                return value or None
    except OSError:
        pass
    return None


def write_dismissed_update_version(version: str) -> None:
    try:
        with open(dismissed_update_version_path(), "w", encoding="utf-8") as f:
            f.write(version.strip())
    except OSError:
        pass


def _classify_asset_urls(assets: list) -> Tuple[Optional[str], Optional[str]]:
    setup_url: Optional[str] = None
    portable_url: Optional[str] = None
    for asset in assets or []:
        name = (asset.get("name") or "").lower()
        url = asset.get("browser_download_url")
        if not url or not name.endswith(".exe"):
            continue
        if "setup" in name:
            setup_url = url
        elif name.startswith("azuremigrationtool"):
            portable_url = url
    return portable_url, setup_url


def staged_exe_path(version: str) -> Path:
    safe = re.sub(r"[^\w.\-]+", "_", version.strip())
    return updates_dir() / f"AzureMigrationTool_{safe}.exe"


def read_staged_update() -> Optional[dict]:
    path = staged_manifest_path()
    if not path.is_file():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        return data
    except (OSError, json.JSONDecodeError):
        return None


def write_staged_update(version: str, exe_path: Path) -> None:
    payload = {
        "version": version.strip(),
        "exe_path": str(exe_path.resolve()),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(staged_manifest_path(), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def clear_staged_update() -> None:
    try:
        staged_manifest_path().unlink(missing_ok=True)
    except OSError:
        pass


def get_ready_staged_exe(current_version: str) -> Optional[Tuple[str, Path]]:
    """Return (version, path) when a newer build is already downloaded."""
    data = read_staged_update()
    if not data:
        return None
    version = str(data.get("version") or "").strip()
    exe_raw = str(data.get("exe_path") or "").strip()
    if not version or not exe_raw:
        return None
    if version_tuple(version) <= version_tuple(current_version):
        return None
    exe_path = Path(exe_raw)
    if not exe_path.is_file() or exe_path.stat().st_size < 1024 * 1024:
        return None
    return version, exe_path


def auto_update_allowed() -> bool:
    if os.environ.get("AZURE_MIGRATION_TOOL_SKIP_UPDATE_CHECK"):
        return False
    if os.environ.get("AZURE_MIGRATION_TOOL_DISABLE_AUTO_UPDATE"):
        return False
    if getattr(sys, "frozen", False):
        return True
    return bool(os.environ.get("AZURE_MIGRATION_TOOL_ENABLE_AUTO_UPDATE"))


def auto_restart_after_download() -> bool:
    return bool(os.environ.get("AZURE_MIGRATION_TOOL_AUTO_RESTART"))


def running_exe_path() -> Optional[Path]:
    if not getattr(sys, "frozen", False):
        return None
    try:
        return Path(sys.executable).resolve()
    except OSError:
        return None


def fetch_latest_release(
    repo: str = DEFAULT_GITHUB_REPO,
    timeout: float = 20.0,
) -> dict:
    """Return the JSON body for GET /releases/latest."""
    url = _GITHUB_API.format(repo=repo)
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": _USER_AGENT,
    }
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def check_for_update(
    current_version: str,
    repo: str = DEFAULT_GITHUB_REPO,
    timeout: float = 20.0,
) -> Optional[UpdateInfo]:
    """
    Compare current_version to the latest GitHub release.

    Returns None if the check fails (network, no releases, etc.).
    """
    try:
        release = fetch_latest_release(repo=repo, timeout=timeout)
    except requests.RequestException:
        return None

    tag = release.get("tag_name") or ""
    latest = normalize_release_version(tag)
    if not latest:
        name = release.get("name") or ""
        latest = normalize_release_version(name)
    if not latest:
        return None

    notes = summarize_release_notes_for_user(release.get("body") or "")

    page_url = release.get("html_url") or f"https://github.com/{repo}/releases/latest"
    portable_url, setup_url = _classify_asset_urls(release.get("assets") or [])

    return UpdateInfo(
        current_version=current_version,
        latest_version=latest,
        release_page_url=page_url,
        portable_download_url=portable_url,
        setup_download_url=setup_url,
        release_notes=notes,
    )


def should_prompt_for_update(info: UpdateInfo) -> bool:
    """True if user has not dismissed this latest version."""
    if not info.is_update_available:
        return False
    dismissed = read_dismissed_update_version()
    return dismissed != info.latest_version


def should_silent_download(info: UpdateInfo) -> bool:
    if not info.is_update_available or not info.can_apply_silently:
        return False
    dismissed = read_dismissed_update_version()
    if dismissed == info.latest_version:
        return False
    ready = get_ready_staged_exe(info.current_version)
    if ready and ready[0] == info.latest_version:
        return False
    return True


def download_release_asset(
    url: str,
    dest: Path,
    timeout: float = 900.0,
    progress_callback: Optional[ProgressCallback] = None,
) -> None:
    """Download a release asset to dest (atomic via .part file)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".part")
    headers = {"User-Agent": _USER_AGENT}
    with requests.get(url, stream=True, timeout=timeout, headers=headers) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length") or 0)
        downloaded = 0
        with open(partial, "wb") as handle:
            for chunk in response.iter_content(chunk_size=256 * 1024):
                if not chunk:
                    continue
                handle.write(chunk)
                downloaded += len(chunk)
                if progress_callback:
                    progress_callback(downloaded, total)
    if partial.stat().st_size < 1024 * 1024:
        partial.unlink(missing_ok=True)
        raise ValueError("Downloaded file is too small to be a valid build.")
    partial.replace(dest)


def download_and_stage_update(
    info: UpdateInfo,
    progress_callback: Optional[ProgressCallback] = None,
) -> Path:
    """Download the portable exe and record it in the staged manifest."""
    url = info.portable_download_url
    if not url:
        raise ValueError("No portable executable is available for silent update.")
    dest = staged_exe_path(info.latest_version)
    download_release_asset(url, dest, progress_callback=progress_callback)
    write_staged_update(info.latest_version, dest)
    return dest


def spawn_apply_update_when_process_exits(
    pid: int,
    target_exe: Path,
    staged_exe: Path,
    restart: bool = True,
) -> None:
    """Replace target_exe with staged_exe after pid exits (Windows only)."""
    if not sys.platform.startswith("win"):
        return

    script_dir = Path(_app_data_dir()) / "update"
    script_dir.mkdir(parents=True, exist_ok=True)
    ps1 = script_dir / "apply_update.ps1"
    ps1.write_text(
        _APPLY_UPDATE_PS1,
        encoding="utf-8",
    )

    target = str(target_exe.resolve())
    staged = str(staged_exe.resolve())
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    subprocess.Popen(
        [
            "powershell.exe",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-WindowStyle",
            "Hidden",
            "-File",
            str(ps1),
            "-ProcessId",
            str(pid),
            "-Target",
            target,
            "-Source",
            staged,
            "-Restart",
            "1" if restart else "0",
        ],
        creationflags=creationflags,
        close_fds=True,
    )


_APPLY_UPDATE_PS1 = r"""param(
    [int]$ProcessId,
    [string]$Target,
    [string]$Source,
    [string]$Restart = "1"
)
$ErrorActionPreference = "Stop"
if ($ProcessId -gt 0) {
    Wait-Process -Id $ProcessId -ErrorAction SilentlyContinue
}
Start-Sleep -Seconds 2
if (-not (Test-Path -LiteralPath $Source)) { exit 2 }
$backup = "$Target.prev"
if (Test-Path -LiteralPath $Target) {
    Move-Item -LiteralPath $Target -Destination $backup -Force
}
try {
    Copy-Item -LiteralPath $Source -Destination $Target -Force
    if (Test-Path -LiteralPath $backup) {
        Remove-Item -LiteralPath $backup -Force -ErrorAction SilentlyContinue
    }
} catch {
    if (Test-Path -LiteralPath $backup) {
        Move-Item -LiteralPath $backup -Destination $Target -Force
    }
    exit 1
}
if ($Restart -eq "1") {
    Start-Process -FilePath $Target
}
exit 0
"""


def resolve_github_repo() -> str:
    """Repo slug owner/name for API calls (override via env for forks)."""
    override = os.environ.get("AZURE_MIGRATION_TOOL_GITHUB_REPO", "").strip()
    if override:
        return override
    try:
        from azure_migration_tool import __github_repo__  # type: ignore[attr-defined]

        if __github_repo__:
            return str(__github_repo__).strip()
    except (ImportError, AttributeError):
        pass
    return DEFAULT_GITHUB_REPO
