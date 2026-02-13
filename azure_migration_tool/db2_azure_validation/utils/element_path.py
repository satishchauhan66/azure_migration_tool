# Author: Sa-tish Chauhan

"""
Utilities for generating consistent ElementPath values across APIs.
"""

from __future__ import annotations

from typing import Any


def format_element_path(*parts: Any) -> str:
    """
    Join schema/object fragments with '.' after stripping whitespace.

    Empty or None fragments are ignored to avoid duplicate separators.
    
    Args:
        *parts: Variable number of path components (schema, object, column, etc.)
    
    Returns:
        Dot-separated path string.
    
    Examples:
        >>> format_element_path("SCHEMA", "TABLE", "COLUMN")
        'SCHEMA.TABLE.COLUMN'
        >>> format_element_path("SCHEMA", None, "TABLE")
        'SCHEMA.TABLE'
        >>> format_element_path("  SCHEMA  ", "  TABLE  ")
        'SCHEMA.TABLE'
    """
    cleaned: list[str] = []
    for part in parts:
        if part is None:
            continue
        text = str(part).strip()
        if text:
            cleaned.append(text)
    return ".".join(cleaned)


__all__ = ["format_element_path"]
