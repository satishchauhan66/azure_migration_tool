# Author: Satish Chauhan
"""
Subprocess helpers that suppress console windows on Windows.

When a PyInstaller-built GUI app (console=False) spawns a subprocess,
Windows opens a visible cmd.exe window for each call unless CREATE_NO_WINDOW
is passed via creationflags (Popen) or startupinfo (run/Popen).

This module provides drop-in wrappers that do this automatically.
"""

from __future__ import annotations

import subprocess
import sys
from typing import Any

_IS_WINDOWS = sys.platform.startswith("win")
_CREATE_NO_WINDOW = 0x08000000 if _IS_WINDOWS else 0


def _startupinfo() -> Any:
    """Return a STARTUPINFO that hides the console, or None on non-Windows."""
    if not _IS_WINDOWS:
        return None
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = 0  # SW_HIDE
    return si


def run_silent(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess:
    """subprocess.run() that never flashes a console window on Windows."""
    if _IS_WINDOWS:
        kwargs.setdefault("creationflags", _CREATE_NO_WINDOW)
        kwargs.setdefault("startupinfo", _startupinfo())
    return subprocess.run(*args, **kwargs)


def popen_silent(*args: Any, **kwargs: Any) -> subprocess.Popen:
    """subprocess.Popen() that never flashes a console window on Windows."""
    if _IS_WINDOWS:
        kwargs.setdefault("creationflags", _CREATE_NO_WINDOW)
        kwargs.setdefault("startupinfo", _startupinfo())
    return subprocess.Popen(*args, **kwargs)
