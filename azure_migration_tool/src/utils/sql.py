# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""SQL generation and parsing utilities."""

import re
from typing import List

from .paths import qident, utc_iso


def type_sql(type_name: str, max_length: int, precision: int, scale: int) -> str:
    """Convert SQL type metadata to SQL type string"""
    t = (type_name or "").lower()
    if t in ("varchar", "char", "varbinary", "binary"):
        if max_length == -1:
            return f"{type_name}(max)"
        return f"{type_name}({max_length})"
    if t in ("nvarchar", "nchar"):
        if max_length == -1:
            return f"{type_name}(max)"
        return f"{type_name}({int(max_length / 2)})"
    if t in ("decimal", "numeric"):
        return f"{type_name}({precision},{scale})"
    if t in ("datetime2", "datetimeoffset", "time"):
        return f"{type_name}({scale})"
    return type_name


def sql_header(title: str, server: str, db: str, run_id: str) -> str:
    """Generate SQL script header comment"""
    return "\n".join(
        [
            f"-- {title}",
            f"-- Server: {server}",
            f"-- Database: {db}",
            f"-- Run: {run_id}",
            f"-- Generated (UTC): {utc_iso()}",
            "SET NOCOUNT ON;",
            "GO",
            "",
        ]
    )


def split_sql_on_go(sql_text: str) -> List[str]:
    """
    Split SQL text on GO statements (case-insensitive, handles GO on its own line).
    Returns list of SQL batches (without GO statements).
    """
    # Pattern: GO on its own line (with optional whitespace before/after)
    # Also handle GO with semicolon: GO;
    pattern = r"^\s*GO\s*;?\s*$"
    batches = []
    current_batch = []

    for line in sql_text.splitlines():
        if re.match(pattern, line, re.IGNORECASE):
            # Found GO - save current batch if non-empty
            batch_text = "\n".join(current_batch).strip()
            if batch_text:
                batches.append(batch_text)
            current_batch = []
        else:
            current_batch.append(line)

    # Add final batch if any
    batch_text = "\n".join(current_batch).strip()
    if batch_text:
        batches.append(batch_text)

    return batches

