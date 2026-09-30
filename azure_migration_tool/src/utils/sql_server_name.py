# Author: Satish Chauhan
"""Normalize and validate SQL Server host names for GUI and ODBC connections."""

from __future__ import annotations


def sanitize_sql_server_name(server: str) -> str:
    """
    Clean user/pasted server values (same rules as Excel import clean_server_name).

    - Trim whitespace and surrounding quotes
    - Strip trailing ``;`` / ``,``
    - Remove ``tcp:`` / ``np:`` / ``lpc:`` prefixes
    - Drop ``,1433``-style port suffix (ODBC uses Server=host only here)
    """
    s = (server or "").strip().strip('"').strip("'")
    s = s.rstrip(";,")
    lower = s.lower()
    for prefix in ("tcp:", "np:", "lpc:"):
        if lower.startswith(prefix):
            s = s[len(prefix) :].lstrip()
            break
    if "," in s:
        host, rest = s.split(",", 1)
        if rest.strip().isdigit():
            s = host.strip()
    return s


def validate_sql_server_name(server: str) -> str:
    """
    Sanitize then reject values that look like an email domain, not a SQL host.

    Returns the sanitized server string.
    """
    s = sanitize_sql_server_name(server)
    if not s:
        raise ValueError("Server name cannot be empty")
    if "@" in s or (s.endswith(".com") and len(s.split(".")) < 3):
        raise ValueError(
            f"Invalid server name: '{s}'. "
            "Server name appears to be an email domain, not a SQL Server address."
        )
    return s
