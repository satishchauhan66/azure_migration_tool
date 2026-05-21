# Author: S@tish Chauhan

"""SQL generation and parsing utilities."""

import re
from typing import List, Optional, Tuple

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


_GO_LINE = re.compile(r"^\s*GO\s*;?\s*$", re.IGNORECASE)

_MODULE_SESSION_SET_LINE = re.compile(
    r"^\s*SET\s+(?:ANSI_NULLS|QUOTED_IDENTIFIER)\s+(?:ON|OFF)\s*;?\s*$",
    re.IGNORECASE,
)

_ASSIGN_DOUBLE_QUOTED_LITERAL = re.compile(r'(=\s*)"([^"]+)"')


def _line_in_single_quoted_string(line: str, in_string: bool) -> bool:
    """Track whether we end the line inside a single-quoted T-SQL string literal."""
    i = 0
    n = len(line)
    while i < n:
        if in_string:
            if line[i] == "'":
                if i + 1 < n and line[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
        else:
            if line[i] == "'":
                in_string = True
                i += 1
            elif line[i : i + 2] == "--":
                break
            else:
                i += 1
    return in_string


def format_module_session_preamble(
    uses_ansi_nulls: bool,
    uses_quoted_identifier: bool,
) -> str:
    """SSMS-style SET batches that must precede CREATE/ALTER for the module body."""
    ansi = "ON" if uses_ansi_nulls else "OFF"
    qi = "ON" if uses_quoted_identifier else "OFF"
    return f"SET ANSI_NULLS {ansi}\nGO\nSET QUOTED_IDENTIFIER {qi}\nGO"


def strip_module_session_set_options(sql_text: str) -> str:
    """Remove SET ANSI_NULLS / QUOTED_IDENTIFIER lines (and GO) from module text."""
    if not sql_text:
        return sql_text
    kept: List[str] = []
    for line in sql_text.splitlines():
        if _MODULE_SESSION_SET_LINE.match(line) or _GO_LINE.match(line):
            continue
        kept.append(line)
    return "\n".join(kept).strip()


def normalize_double_quoted_string_literals(sql_text: str) -> str:
    """
    Convert simple assignment double-quoted literals to single-quoted (e.g. = "2" -> = '2').
    Safe under QUOTED_IDENTIFIER ON; does not alter delimited identifiers outside = "..." .
    """
    if not sql_text or '"' not in sql_text:
        return sql_text

    def _repl(match: re.Match) -> str:
        prefix, value = match.group(1), match.group(2)
        if not value or not re.fullmatch(r"[\w .\-]+", value):
            return match.group(0)
        escaped = value.replace("'", "''")
        return f"{prefix}'{escaped}'"

    return _ASSIGN_DOUBLE_QUOTED_LITERAL.sub(_repl, sql_text)


def _module_batch_preamble_start(lines: List[str], header_idx: int) -> int:
    """Include SET ANSI_NULLS / QUOTED_IDENTIFIER lines immediately before a module header."""
    start = header_idx
    idx = header_idx - 1
    saw_session_set = False
    while idx >= 0:
        stripped = lines[idx].strip()
        if not stripped:
            if saw_session_set:
                start = idx
            idx -= 1
            continue
        if _GO_LINE.match(lines[idx]):
            if saw_session_set:
                start = idx
            idx -= 1
            continue
        if _MODULE_SESSION_SET_LINE.match(lines[idx]):
            saw_session_set = True
            start = idx
            idx -= 1
            continue
        if stripped.startswith("--") and saw_session_set:
            start = idx
            idx -= 1
            continue
        break
    return start if saw_session_set else header_idx


def strip_standalone_go_lines(sql_text: str) -> str:
    """Remove GO lines embedded in module bodies (OBJECT_DEFINITION / SSMS scripting)."""
    if not sql_text:
        return sql_text
    kept = [
        line
        for line in sql_text.splitlines()
        if not _GO_LINE.match(line)
    ]
    return "\n".join(kept)


def normalize_alter_database_current(sql_text: str) -> str:
    """Use ALTER DATABASE CURRENT so restore works when target DB name differs from backup."""
    return re.sub(
        r"\bALTER\s+DATABASE\s+(?:\[[^\]]+\]|\w+)",
        "ALTER DATABASE CURRENT",
        sql_text,
        flags=re.IGNORECASE,
    )


_MODULE_HEADER_LINE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+ALTER\s+)?(?:PROC(?:EDURE)?|FUNCTION)\s+"
    r"(?:\[?\w+\]?\.)?\[?\w+\]?",
    re.IGNORECASE,
)

_PROCEDURE_HEADER_LINE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+ALTER\s+)?PROC(?:EDURE)?\s+",
    re.IGNORECASE,
)

_CREATE_MODULE_FRAGMENT = re.compile(
    r"^\s*CREATE\s+(?:OR\s+ALTER\s+)?(?:PROC(?:EDURE)?|FUNCTION)\s+",
    re.IGNORECASE,
)


def _is_executable_module_batch(text: str) -> bool:
    """Drop orphan fragments from bad splits (e.g. SELECT @ver = '2' split to bare 2)."""
    stripped = (text or "").strip()
    if not stripped:
        return False
    if _CREATE_MODULE_FRAGMENT.match(stripped):
        return True
    if len(stripped) < 24:
        return False
    return True


def _module_header_line_indices(lines: List[str]) -> List[int]:
    """Line indices of top-level CREATE (OR ALTER) PROCEDURE/FUNCTION headers."""
    indices: List[int] = []
    in_string = False
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            in_string = _line_in_single_quoted_string(line, in_string)
            continue
        if stripped.startswith("--"):
            in_string = _line_in_single_quoted_string(line, in_string)
            continue
        if not in_string and _MODULE_HEADER_LINE.match(line):
            indices.append(idx)
        in_string = _line_in_single_quoted_string(line, in_string)
    return indices


def _first_executable_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if _MODULE_SESSION_SET_LINE.match(line):
            continue
        return stripped
    return ""


def _is_procedure_batch(text: str) -> bool:
    """True when batch starts with CREATE (OR ALTER) PROCEDURE."""
    return bool(_PROCEDURE_HEADER_LINE.match(_first_executable_line(text)))


def split_procedure_batches(sql_text: str) -> List[str]:
    """
    Split SQL into one batch per CREATE (OR ALTER) PROCEDURE.

    Uses a line-by-line state machine so CREATE inside string literals is ignored.
    Drops batches that do not start with a procedure header (e.g. SET NOCOUNT preamble).
    """
    if not sql_text or not sql_text.strip():
        return []

    lines = sql_text.splitlines()
    header_indices: List[int] = []
    in_string = False

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            in_string = _line_in_single_quoted_string(line, in_string)
            continue
        if stripped.startswith("--"):
            in_string = _line_in_single_quoted_string(line, in_string)
            continue
        if not in_string and _PROCEDURE_HEADER_LINE.match(line):
            header_indices.append(idx)
        in_string = _line_in_single_quoted_string(line, in_string)

    if not header_indices:
        return []

    header_indices.append(len(lines))
    result: List[str] = []
    for i in range(len(header_indices) - 1):
        start = _module_batch_preamble_start(lines, header_indices[i])
        chunk_lines = lines[start : header_indices[i + 1]]
        chunk = strip_standalone_go_lines("\n".join(chunk_lines)).strip()
        if chunk and _is_procedure_batch(chunk):
            result.append(chunk)
    return result


def split_batches_on_create_module(batches: List[str]) -> List[str]:
    """
    Split batches that still contain multiple CREATE (OR ALTER) PROCEDURE/FUNCTION
    statements (e.g. merged ASPstate diagram procs when GO was missing in backup).

    Ignores CREATE PROCEDURE lines inside string literals (dynamic SQL) so bodies like
    TempGetVersion (SELECT @ver = '2') are not broken into orphan fragments.
    """
    result: List[str] = []
    for batch in batches:
        text = batch.strip()
        if not text:
            continue
        lines = text.splitlines()
        header_lines = _module_header_line_indices(lines)
        if len(header_lines) <= 1:
            cleaned = strip_standalone_go_lines(text).strip()
            if cleaned and _is_executable_module_batch(cleaned):
                result.append(cleaned)
            continue
        header_lines.append(len(lines))
        for i in range(len(header_lines) - 1):
            start = _module_batch_preamble_start(lines, header_lines[i])
            chunk_lines = lines[start : header_lines[i + 1]]
            chunk = strip_standalone_go_lines("\n".join(chunk_lines)).strip()
            if chunk and _is_executable_module_batch(chunk):
                result.append(chunk)
    return result


def split_sql_on_go(sql_text: str) -> List[str]:
    """
    Split SQL text on GO statements (case-insensitive, handles GO on its own line).
    Ignores GO inside single-quoted string literals. Returns batches without GO lines.
    """
    batches = []
    current_batch = []
    in_string = False

    for line in sql_text.splitlines():
        if _GO_LINE.match(line) and not in_string:
            batch_text = "\n".join(current_batch).strip()
            if batch_text:
                batches.append(strip_standalone_go_lines(batch_text).strip())
            current_batch = []
        else:
            current_batch.append(line)
            in_string = _line_in_single_quoted_string(line, in_string)

    batch_text = "\n".join(current_batch).strip()
    if batch_text:
        batches.append(strip_standalone_go_lines(batch_text).strip())

    return [b for b in batches if b]


def expand_module_session_set_batches(batches: List[str]) -> List[str]:
    """
    ODBC requires CREATE/ALTER PROCEDURE to be first in its batch; SET ANSI_NULLS and
    QUOTED_IDENTIFIER must run in separate batches immediately before the module body.
    """
    expanded: List[str] = []
    for batch in batches:
        text = (batch or "").strip()
        if not text:
            continue
        lines = text.splitlines()
        idx = 0
        while idx < len(lines):
            stripped = lines[idx].strip()
            if not stripped or _GO_LINE.match(lines[idx]):
                idx += 1
                continue
            if stripped.startswith("--"):
                idx += 1
                continue
            if _MODULE_SESSION_SET_LINE.match(lines[idx]):
                m = re.match(
                    r"^\s*SET\s+(ANSI_NULLS|QUOTED_IDENTIFIER)\s+(ON|OFF)\s*;?\s*$",
                    lines[idx],
                    re.IGNORECASE,
                )
                if m:
                    expanded.append(f"SET {m.group(1).upper()} {m.group(2).upper()}")
                idx += 1
                continue
            break
        body = "\n".join(lines[idx:]).strip()
        if body:
            expanded.append(body)
    return expanded


def _finalize_procedure_batches(batches: List[str]) -> List[str]:
    """Split session SET options, then normalize legacy double-quoted literals."""
    expanded = expand_module_session_set_batches(batches)
    return [normalize_double_quoted_string_literals(b) for b in expanded]


def prepare_sql_batches(sql_text: str, file_type: Optional[str] = None) -> List[str]:
    """Split SQL into executable batches for restore."""
    if file_type == "SCHEMA_REPAIR":
        result: List[str] = []
        for go_batch in split_sql_on_go(sql_text):
            go_batch = (go_batch or "").strip()
            if not go_batch:
                continue
            if _is_procedure_batch(go_batch):
                proc_batches = split_procedure_batches(go_batch) or [go_batch]
                result.extend(_finalize_procedure_batches(proc_batches))
            else:
                expanded = expand_module_session_set_batches([go_batch])
                result.extend(
                    [b for b in expanded if b.strip()] or [go_batch]
                )
        return result

    if file_type in ("PROCEDURES", "PROCEDURE"):
        batches = split_procedure_batches(sql_text)
        if batches:
            return _finalize_procedure_batches(batches)
        batches = []
        for go_batch in split_sql_on_go(sql_text):
            batches.extend(split_procedure_batches(go_batch))
        if batches:
            return _finalize_procedure_batches(batches)
        single = strip_standalone_go_lines(sql_text).strip()
        if single and _is_procedure_batch(single):
            return _finalize_procedure_batches([single])
        return []

    batches = split_sql_on_go(sql_text)
    if file_type == "FUNCTIONS":
        batches = split_batches_on_create_module(batches)
    return batches

