"""
Resolve primary-key and comparison columns for cross-database sample row comparison.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple


def pair_columns_case_insensitive(
    src_cols: List[str], dest_cols: List[str]
) -> List[Tuple[str, str]]:
    """(src_name, dest_name) pairs with case-insensitive match; order follows src_cols."""
    dest_lower: Dict[str, str] = {str(c).lower(): str(c) for c in dest_cols}
    out: List[Tuple[str, str]] = []
    seen_dest: set = set()
    for sc in src_cols:
        scs = str(sc)
        dc = dest_lower.get(scs.lower())
        if dc is not None and dc.lower() not in seen_dest:
            seen_dest.add(dc.lower())
            out.append((scs, dc))
    return out


def align_pairs_to_cursor_columns(
    pairs: List[Tuple[str, str]],
    src_cols: List[str],
    dest_cols: List[str],
) -> List[Tuple[str, str]]:
    """Map logical names to actual cursor.description names (case)."""
    smap = {str(c).lower(): str(c) for c in src_cols}
    dmap = {str(c).lower(): str(c) for c in dest_cols}
    out: List[Tuple[str, str]] = []
    for sc, dc in pairs:
        sa = smap.get(str(sc).lower())
        da = dmap.get(str(dc).lower())
        if sa is not None and da is not None:
            out.append((sa, da))
    return out


def fetch_sqlserver_column_names(cursor, schema: str, table: str) -> List[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, table),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def fetch_db2_column_names(cursor, schema: str, table: str) -> List[str]:
    cursor.execute(
        """
        SELECT COLNAME
        FROM SYSCAT.COLUMNS
        WHERE TABSCHEMA = ? AND TABNAME = ?
        ORDER BY COLNO
        """,
        (schema, table),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def fetch_sqlserver_pk_columns(cursor, schema: str, table: str) -> List[str]:
    """Ordered PK column names on SQL Server."""
    cursor.execute(
        """
        SELECT c.name
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.is_primary_key = 1
        INNER JOIN sys.index_columns ic
            ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE s.name = ? AND t.name = ?
        ORDER BY ic.key_ordinal
        """,
        (schema, table),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def fetch_db2_pk_columns(cursor, schema: str, table: str) -> List[str]:
    """Ordered PK column names on DB2 (SYSCAT)."""
    cursor.execute(
        """
        SELECT k.COLNAME
        FROM SYSCAT.TABCONST tc
        INNER JOIN SYSCAT.KEYCOLUSE k
            ON tc.TABSCHEMA = k.TABSCHEMA
            AND tc.TABNAME = k.TABNAME
            AND tc.CONSTNAME = k.CONSTNAME
        WHERE tc.TYPE = 'P'
          AND tc.TABSCHEMA = ?
          AND tc.TABNAME = ?
        ORDER BY k.COLSEQ
        """,
        (schema, table),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _sqlserver_bracket_ident(ident: str) -> str:
    s = str(ident).replace("]", "]]")
    return f"[{s}]"


def _db2_quote_ident(ident: str) -> str:
    s = str(ident).replace('"', '""')
    return f'"{s}"'


def sqlserver_order_by_clause(key_dest_cols: List[str]) -> str:
    if not key_dest_cols:
        return ""
    parts = ", ".join(_sqlserver_bracket_ident(c) for c in key_dest_cols)
    return f" ORDER BY {parts}"


def normalize_compare_key_tuple(key_tuple: tuple) -> tuple:
    """
    Normalize key parts so dict lookup matches across DB2/JDBC source rows and
    pyodbc destination rows (same logical value, same Python type).
    """
    return tuple(_coerce_scalar_for_odbc(x) for x in key_tuple)


def _coerce_scalar_for_odbc(val: Any) -> Any:
    """
    Normalize values from DB2/JDBC or other drivers so pyodbc binds reliably
    to SQL Server (avoids invalid / mis-inferred parameter types).
    """
    if val is None:
        return None
    if isinstance(val, (str, int, float, bool, bytes, bytearray, datetime, date, Decimal)):
        return val
    # java.lang.String / JPype objects from JDBC cursors
    try:
        tname = type(val).__name__
        mod = getattr(type(val), "__module__", "") or ""
        if "java" in mod.lower() or tname == "java.lang.String":
            return str(val)
    except Exception:
        pass
    try:
        return str(val)
    except Exception:
        return val


def _sqlserver_col_pred_and_param(bcol: str, val: Any) -> Tuple[str, List[Any]]:
    """
    One column predicate with explicit CASTs so ODBC Driver 18 can bind types
    (avoids 'Cannot find data type' when values look like identifiers, e.g. L.E.).
    """
    v = _coerce_scalar_for_odbc(val)
    if v is None:
        return f"{bcol} IS NULL", []
    if isinstance(v, datetime):
        return f"{bcol} = CAST(? AS DATETIME2)", [v]
    if isinstance(v, date):
        return f"{bcol} = CAST(? AS DATE)", [v]
    if isinstance(v, bool):
        return f"{bcol} = CAST(? AS BIT)", [1 if v else 0]
    if isinstance(v, int) and not isinstance(v, bool):
        return f"{bcol} = CAST(? AS BIGINT)", [v]
    if isinstance(v, float):
        return f"{bcol} = CAST(? AS FLOAT(53))", [v]
    if isinstance(v, Decimal):
        return f"{bcol} = CAST(? AS DECIMAL(38, 19))", [v]
    if isinstance(v, (bytes, bytearray)):
        return f"{bcol} = CAST(? AS VARBINARY(MAX))", [bytes(v)]
    # strings and anything else: force nvarchar so dotted names are not parsed as types
    return f"{bcol} = CAST(? AS NVARCHAR(MAX))", [str(v)]


def sqlserver_where_key_matches_tuple(
    dest_key_cols: List[str], key_tuple: tuple
) -> Tuple[str, List[Any]]:
    """
    Build a NULL-safe AND clause for one composite key (SQL Server).
    Returns (sql_fragment, parameters_for_placeholders_only).
    """
    parts: List[str] = []
    params: List[Any] = []
    for i, col in enumerate(dest_key_cols):
        bcol = _sqlserver_bracket_ident(col)
        val = key_tuple[i] if i < len(key_tuple) else None
        pred, extra = _sqlserver_col_pred_and_param(bcol, val)
        parts.append(pred)
        params.extend(extra)
    return "(" + " AND ".join(parts) + ")", params


def fetch_sqlserver_rows_matching_composite_keys(
    cursor,
    dest_schema: str,
    dest_table: str,
    dest_key_cols: List[str],
    key_tuples: Sequence[tuple],
    batch_size: int = 28,
) -> Tuple[List[str], List[tuple]]:
    """
    Load destination rows whose composite key appears in key_tuples (deduped).
    Batches OR-of-key-clauses to stay under SQL Server parameter limits.

    Returns (column_names, rows).
    """
    if not dest_key_cols:
        return [], []
    tref = (
        f"{_sqlserver_bracket_ident(dest_schema)}."
        f"{_sqlserver_bracket_ident(dest_table)}"
    )
    unique_keys = list(dict.fromkeys(key_tuples))
    if not unique_keys:
        cursor.execute(f"SELECT TOP 0 * FROM {tref}")
        desc = cursor.description or []
        return [str(d[0]) if d[0] is not None else "" for d in desc], []

    all_rows: List[tuple] = []
    col_names: Optional[List[str]] = None
    for start in range(0, len(unique_keys), batch_size):
        batch = unique_keys[start : start + batch_size]
        or_parts: List[str] = []
        params: List[Any] = []
        for kt in batch:
            frag, p = sqlserver_where_key_matches_tuple(dest_key_cols, kt)
            or_parts.append(frag)
            params.extend(p)
        sql = f"SELECT * FROM {tref} WHERE " + " OR ".join(or_parts)
        cursor.execute(sql, tuple(params))
        if col_names is None:
            col_names = [
                str(d[0]) if d[0] is not None else ""
                for d in (cursor.description or [])
            ]
        all_rows.extend(cursor.fetchall())
    return col_names or [], all_rows


def db2_order_by_clause(key_src_cols: List[str]) -> str:
    if not key_src_cols:
        return ""
    parts = ", ".join(_db2_quote_ident(c) for c in key_src_cols)
    return f" ORDER BY {parts}"


def resolve_compare_key_pairs(
    col_pairs: List[Tuple[str, str]],
    dest_pk: List[str],
    src_pk: List[str],
    override_raw: Optional[str],
) -> Tuple[List[Tuple[str, str]], str]:
    """
    Return ((src_col, dest_col), ...), human-readable key source label.
    """
    if not col_pairs:
        return [], "no common columns"

    if override_raw and override_raw.strip():
        parts = [p.strip() for p in override_raw.split(",") if p.strip()]
        key_pairs: List[Tuple[str, str]] = []
        for p in parts:
            pl = p.lower()
            for sc, dc in col_pairs:
                if str(sc).lower() == pl or str(dc).lower() == pl:
                    key_pairs.append((sc, dc))
                    break
        if key_pairs:
            return key_pairs, "user override"

    dest_to_pair = {str(dc).lower(): (sc, dc) for sc, dc in col_pairs}
    key_pairs = []
    for dcol in dest_pk:
        tup = dest_to_pair.get(str(dcol).lower())
        if tup:
            key_pairs.append(tup)
    if dest_pk and len(key_pairs) == len(dest_pk):
        return key_pairs, "PK (destination)"

    src_to_pair = {str(sc).lower(): (sc, dc) for sc, dc in col_pairs}
    key_pairs = []
    for scol in src_pk:
        tup = src_to_pair.get(str(scol).lower())
        if tup:
            key_pairs.append(tup)
    if src_pk and len(key_pairs) == len(src_pk):
        return key_pairs, "PK (source)"

    for sc, dc in col_pairs:
        sl = str(sc).lower()
        dl = str(dc).lower()
        if sl.endswith("_id") or dl.endswith("_id") or sl == "id" or dl == "id":
            return [(sc, dc)], "heuristic (id-like column)"

    # TABLE_NAME alone is rarely unique (audit/history tables); prefer another column.
    for sc, dc in col_pairs:
        sl = str(sc).lower()
        dl = str(dc).lower()
        if sl != "table_name" and dl != "table_name":
            return [(sc, dc)], "heuristic (first non-TABLE_NAME column)"

    return [col_pairs[0]], "heuristic (first common column)"


def row_key_tuple(row: dict, key_pairs: List[Tuple[str, str]], side: str) -> tuple:
    """side: 'src' uses first element of each pair, 'dest' uses second."""
    if side == "src":
        return tuple(row.get(sc) for sc, _ in key_pairs)
    return tuple(row.get(dc) for _, dc in key_pairs)


def format_key_for_display(key_tuple: tuple) -> str:
    if len(key_tuple) == 1:
        return str(key_tuple[0])
    return " | ".join(str(x) for x in key_tuple)


def sample_key_duplicate_stats(
    row_dicts: List[dict],
    key_pairs: List[Tuple[str, str]],
    side: str,
) -> Tuple[bool, int, int, int]:
    """
    Detect non-unique comparison keys in a fetched sample.

    Returns:
        has_duplicates, row_count, unique_key_count, keys_that_have_more_than_one_row
    """
    if not row_dicts or not key_pairs:
        return False, 0, 0, 0
    keys = [row_key_tuple(r, key_pairs, side) for r in row_dicts]
    n = len(keys)
    counter = Counter(keys)
    unique = len(counter)
    multi = sum(1 for c in counter.values() if c > 1)
    return (n > unique), n, unique, multi


def format_duplicate_key_examples(
    row_dicts: List[dict],
    key_pairs: List[Tuple[str, str]],
    side: str,
    limit: int = 4,
) -> List[str]:
    """Example key values that appear more than once in the sample (for UI hints)."""
    if not row_dicts or not key_pairs:
        return []
    keys = [
        normalize_compare_key_tuple(row_key_tuple(r, key_pairs, side))
        for r in row_dicts
    ]
    counter = Counter(keys)
    out: List[str] = []
    for k, cnt in counter.items():
        if cnt > 1:
            out.append(f"{format_key_for_display(k)} ({cnt}×)")
            if len(out) >= limit:
                break
    return out


def _pair_sig(p: Tuple[str, str]) -> Tuple[str, str]:
    return (str(p[0]).lower(), str(p[1]).lower())


def distinct_key_count_in_sample(
    src_rows: List[dict], key_pairs: List[Tuple[str, str]]
) -> int:
    if not src_rows:
        return 0
    if not key_pairs:
        return 1
    keys = [
        normalize_compare_key_tuple(row_key_tuple(r, key_pairs, "src"))
        for r in src_rows
    ]
    return len(set(keys))


def greedy_expand_key_until_unique_in_sample(
    base_pairs: List[Tuple[str, str]],
    all_pairs: List[Tuple[str, str]],
    src_rows: List[dict],
) -> Tuple[List[Tuple[str, str]], bool, str]:
    """
    Greedily add columns from all_pairs (not already in the key) to maximize
    distinct composite keys on src_rows until each row is unique or no column
    improves distinctness.

    Returns:
        suggested_pairs, is_unique_in_sample, status_tag
        status_tag: "already_unique" | "expanded" | "partial"
    """
    n_rows = len(src_rows)
    if n_rows <= 1:
        out = list(base_pairs) if base_pairs else (list(all_pairs[:1]) if all_pairs else [])
        return out, True, "already_unique"

    if not all_pairs:
        return list(base_pairs), False, "partial"

    pair_by_sig = {_pair_sig(p): p for p in all_pairs}
    selected: List[Tuple[str, str]] = []
    seen_sig: set = set()
    for p in base_pairs:
        sig = _pair_sig(p)
        if sig in pair_by_sig and sig not in seen_sig:
            selected.append(pair_by_sig[sig])
            seen_sig.add(sig)

    remaining_ordered = [
        p for p in all_pairs if _pair_sig(p) not in seen_sig
    ]

    current = distinct_key_count_in_sample(src_rows, selected)
    if current == n_rows:
        return selected, True, "already_unique"

    while remaining_ordered:
        best_pair: Optional[Tuple[str, str]] = None
        best_score = current
        for p in remaining_ordered:
            trial = selected + [p]
            score = distinct_key_count_in_sample(src_rows, trial)
            if score > best_score:
                best_score = score
                best_pair = p
        if best_pair is None:
            break
        selected.append(best_pair)
        seen_sig.add(_pair_sig(best_pair))
        remaining_ordered = [
            p for p in remaining_ordered if _pair_sig(p) != _pair_sig(best_pair)
        ]
        current = best_score
        if current == n_rows:
            return selected, True, "expanded"

    return selected, current == n_rows, "partial"


def format_compare_key_override(suggested_pairs: List[Tuple[str, str]]) -> str:
    """Comma-separated destination column names for the override text field."""
    return ", ".join(str(dc) for _, dc in suggested_pairs)
