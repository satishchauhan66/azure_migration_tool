# Author: Sa-tish Chauhan

"""Normalize module text for apples-to-apples schema comparison."""

import hashlib
import re
from typing import Optional

from ..backup.exporters import _normalize_module_definition
from ..utils.sql import strip_module_session_set_options, strip_standalone_go_lines

# Strip comments and bracket noise for stable hashes.
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"--[^\n]*")
_WHITESPACE = re.compile(r"\s+")


def normalize_module_text_for_compare(definition: Optional[str], kind: str = "PROC") -> str:
    """
    Canonical text for diffing procedures/views/functions.

    - CREATE OR ALTER vs CREATE vs ALTER PROCEDURE → same body
    - Collapse whitespace; drop comments and GO lines
    """
    if not definition:
        return ""

    text = strip_module_session_set_options(definition.strip())
    text = strip_standalone_go_lines(text)
    text = _normalize_module_definition(text, kind, normalize_literals=True)
    text = _BLOCK_COMMENT.sub("", text)
    text = _LINE_COMMENT.sub("", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _WHITESPACE.sub(" ", text).strip().upper()
    return text


def definition_hash(definition: Optional[str], kind: str = "PROC") -> str:
    """SHA-256 hex of normalized module text (empty string if no definition)."""
    normalized = normalize_module_text_for_compare(definition, kind)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def module_compare_fingerprint(entry: dict, kind: str = "PROC") -> str:
    """
    Hash body plus session options (uses_ansi_nulls / uses_quoted_identifier).

    Redgate still emits ALTER when only QUOTED_IDENTIFIER metadata differs.
    """
    body_hash = entry.get("hash") or definition_hash(entry.get("definition"), kind)
    ansi = entry.get("uses_ansi_nulls")
    qi = entry.get("uses_quoted_identifier")
    if ansi is None and qi is None:
        return body_hash
    return f"{body_hash}|ansi={int(bool(ansi))}|qi={int(bool(qi))}"


def object_key(schema_name: str, object_name: str, object_type: str) -> str:
    """Stable key: TYPE.schema.name (schema/name lowercased for compare sets)."""
    return f"{object_type}.{schema_name}.{object_name}".lower()
