# Author: Satish Chauhan

"""Redact secrets (connection strings, SAS tokens, keys, passwords) from log text."""

from __future__ import annotations

import re

_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # Azure Storage connection string values
    (re.compile(r"(AccountKey\s*=\s*)[^\s;]+", re.IGNORECASE), r"\1***"),
    (re.compile(r"(SharedAccessSignature\s*=\s*)[^\s;]+", re.IGNORECASE), r"\1***"),
    # SAS tokens in URLs  (?sv=...&sig=...&se=...)
    (re.compile(r"(\?[^\"'\s]*?(?:sig|sv|se|sp|spr|srt|ss)\s*=)[^&\"'\s]+", re.IGNORECASE), r"\1***"),
    # Inline passwords / secrets
    (re.compile(r"((?:password|pwd|secret|token)\s*=\s*)[^\s;\"']+", re.IGNORECASE), r"\1***"),
    # Bearer / auth headers
    (re.compile(r"(Bearer\s+)[A-Za-z0-9\-._~+/]+=*", re.IGNORECASE), r"\1***"),
]


def redact_sensitive_text(text: str) -> str:
    """Return *text* with secrets replaced by ``***``."""
    if not text:
        return text
    result = text
    for pattern, replacement in _PATTERNS:
        result = pattern.sub(replacement, result)
    return result
