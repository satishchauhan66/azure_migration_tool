# Author: Satish Chauhan

"""Persisted Azure Blob settings (connection string + container) shared app-wide."""

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

# Windows DPAPI: encrypt connection string at rest (not plain AccountKey in JSON).
_BLOB_CS_ENC_MARKER = "dpapi_v1"
_CRYPTPROTECT_UI_FORBIDDEN = 0x1


def get_blob_settings_path() -> Path:
    if os.name == "nt":
        base = os.environ.get("APPDATA", os.path.expanduser("~"))
        base = Path(base) / "AzureMigrationTool"
    else:
        base = Path(os.path.expanduser("~")) / ".azure_migration_tool"
    base.mkdir(parents=True, exist_ok=True)
    return base / "blob_settings.json"


def _dpapi_available() -> bool:
    return os.name == "nt" and sys.platform == "win32"


def _dpapi_protect(plain: bytes) -> bytes:
    if not plain:
        return b""
    if not _dpapi_available():
        raise OSError("DPAPI is only available on Windows.")
    import ctypes
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

    buf_t = ctypes.c_char * len(plain)
    buf = buf_t.from_buffer_copy(plain)
    blob_in = DATA_BLOB(len(plain), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    blob_out = DATA_BLOB()
    if not ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(blob_out),
    ):
        raise OSError("CryptProtectData failed")
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        if blob_out.pbData:
            ctypes.windll.kernel32.LocalFree(blob_out.pbData)


def _dpapi_unprotect(enc: bytes) -> bytes:
    if not enc:
        return b""
    if not _dpapi_available():
        raise OSError("DPAPI is only available on Windows.")
    import ctypes
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

    buf_t = ctypes.c_char * len(enc)
    buf = buf_t.from_buffer_copy(enc)
    blob_in = DATA_BLOB(len(enc), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
    blob_out = DATA_BLOB()
    if not ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(blob_in),
        None,
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(blob_out),
    ):
        raise OSError("CryptUnprotectData failed")
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        if blob_out.pbData:
            ctypes.windll.kernel32.LocalFree(blob_out.pbData)


def _decode_connection_string_from_saved(data: Dict[str, Any]) -> str:
    """Resolve connection string from JSON (supports DPAPI-wrapped and legacy plaintext)."""
    enc_marker = (data.get("_blob_cs_enc") or "").strip()
    prot_b64 = (data.get("blob_connection_string_protected") or "").strip()
    plain = (data.get("blob_connection_string") or "").strip()

    if enc_marker == _BLOB_CS_ENC_MARKER and prot_b64 and _dpapi_available():
        try:
            raw = _dpapi_unprotect(base64.b64decode(prot_b64.encode("ascii")))
            return raw.decode("utf-8").strip()
        except Exception:
            return ""
    return plain


def load_blob_settings() -> tuple[str, str, str, str]:
    """Return ``(connection_string, container, blob_auth_mode, storage_account_url)``."""
    path = get_blob_settings_path()
    try:
        if not path.exists():
            return "", "", "connection_string", ""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        conn_s = _decode_connection_string_from_saved(data)
        cont_s = (data.get("container") or "").strip()
        mode_s = (data.get("blob_auth_mode") or "connection_string").strip()
        url_s = (data.get("storage_account_url") or "").strip()
        return conn_s, cont_s, mode_s, url_s
    except Exception:
        return "", "", "connection_string", ""


def save_blob_settings(
    blob_connection_string: str,
    container: str,
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
) -> None:
    path = get_blob_settings_path()
    raw = (blob_connection_string or "").strip()
    data: Dict[str, Any] = {
        "container": (container or "").strip(),
        "blob_auth_mode": (blob_auth_mode or "connection_string").strip(),
        "storage_account_url": (storage_account_url or "").strip(),
    }

    if raw and _dpapi_available():
        try:
            enc = _dpapi_protect(raw.encode("utf-8"))
            data["blob_connection_string"] = ""
            data["blob_connection_string_protected"] = base64.b64encode(enc).decode("ascii")
            data["_blob_cs_enc"] = _BLOB_CS_ENC_MARKER
        except Exception:
            data["blob_connection_string"] = raw
    else:
        data["blob_connection_string"] = raw

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def clear_blob_settings() -> None:
    """Delete saved blob settings file."""
    path = get_blob_settings_path()
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass
