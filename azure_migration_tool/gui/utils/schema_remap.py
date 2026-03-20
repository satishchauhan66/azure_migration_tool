# Author: Satish Chauhan
"""
Pair source/destination tables when one schema name maps to another (e.g. userid -> dbo).
"""

from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


def pair_tables_for_schema_remap(
    src_tables_list: List[Tuple[Any, Any]],
    dest_tables_list: List[Tuple[Any, Any]],
    remap_from: str,
    remap_to: str,
) -> Tuple[Set[str], Set[str], Set[str], Dict[str, str]]:
    """
    Match tables: if source schema equals remap_from (case-insensitive), look for
    remap_to.table on destination; otherwise require same schema.table on both sides.

    Returns:
        common_tables: set of source-side full names "Schema.Table"
        missing_in_dest: source tables with no paired destination table
        extra_in_dest: destination tables not consumed by any source table
        cross_map: source "Sch.Tbl" -> destination "Sch.Tbl" (physical dest name)
    """
    rf = (remap_from or "").strip().upper()
    rt = (remap_to or "").strip().upper()
    if not rf or not rt:
        raise ValueError("remap_from and remap_to must be non-empty")

    def norm_cell(x: Any) -> str:
        return str(x).strip() if x is not None else ""

    dest_lookup: Dict[Tuple[str, str], str] = {}
    for ds, dn in dest_tables_list:
        ds_s, dn_s = norm_cell(ds), norm_cell(dn)
        dest_lookup[(ds_s.upper(), dn_s.upper())] = f"{ds_s}.{dn_s}"

    matched_dest: Set[str] = set()
    cross_map: Dict[str, str] = {}
    common_tables: Set[str] = set()

    for ss, sn in src_tables_list:
        ss_s, sn_s = norm_cell(ss), norm_cell(sn)
        src_full = f"{ss_s}.{sn_s}"
        if ss_s.upper() == rf:
            dkey = (rt, sn_s.upper())
        else:
            dkey = (ss_s.upper(), sn_s.upper())
        if dkey in dest_lookup:
            dest_full = dest_lookup[dkey]
            common_tables.add(src_full)
            cross_map[src_full] = dest_full
            matched_dest.add(dest_full)

    src_full_set = {f"{norm_cell(a)}.{norm_cell(b)}" for a, b in src_tables_list}
    dest_full_set = {f"{norm_cell(a)}.{norm_cell(b)}" for a, b in dest_tables_list}
    missing_in_dest = src_full_set - common_tables
    extra_in_dest = dest_full_set - matched_dest

    return common_tables, missing_in_dest, extra_in_dest, cross_map


def physical_dest_schema_table(
    src_schema: str,
    table_name: str,
    *,
    remap_enabled: bool,
    remap_from: str,
    remap_to: str,
) -> Tuple[str, str]:
    """Return (schema, table) to use on the destination for queries."""
    ss = (src_schema or "").strip()
    tn = (table_name or "").strip()
    if not remap_enabled:
        return ss, tn
    rf = (remap_from or "").strip().upper()
    rt = (remap_to or "").strip()
    if not rf or not rt:
        return ss, tn
    if ss.upper() == rf:
        return rt, tn
    return ss, tn
