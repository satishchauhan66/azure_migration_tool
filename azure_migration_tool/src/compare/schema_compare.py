# Author: Sa-tish Chauhan

"""Compare source vs target schema catalogs object-by-object."""

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from ..utils.azure_compat import (
    EXPECTED_SKIP_REASONS,
    expected_azure_gap_note,
    is_azure_nonportable_module,
    is_mi_blocked_framework_assembly_name,
    is_windows_principal_name,
)
from .catalog import fetch_live_catalog, load_backup_catalog
from .normalize import module_compare_fingerprint, object_key

# Re-export for GUI / CLI
__all__ = [
    "compare_schema_catalogs",
    "compare_live_databases",
    "compare_backup_to_live",
    "fetch_live_catalog",
    "load_backup_catalog",
    "summarize_diff_report",
]


def _diff_name_sets(
    src_names: Set[str],
    tgt_names: Set[str],
    object_type: str,
) -> Dict[str, List[str]]:
    missing = sorted(src_names - tgt_names)
    extra = sorted(tgt_names - src_names)
    common = src_names & tgt_names
    return {
        "object_type": object_type,
        "missing": missing,
        "extra": extra,
        "match": sorted(common),
        "missing_count": len(missing),
        "extra_count": len(extra),
        "match_count": len(common),
    }


def _diff_modules(
    src_mods: Dict[str, Dict[str, Any]],
    tgt_mods: Dict[str, Dict[str, Any]],
    object_type: str,
    dest_is_azure: bool,
) -> Dict[str, Any]:
    src_keys = set(src_mods.keys())
    tgt_keys = set(tgt_mods.keys())
    base = _diff_name_sets(src_keys, tgt_keys, object_type)

    different: List[Dict[str, str]] = []
    for key in sorted(src_keys & tgt_keys):
        sh = src_mods[key].get("fingerprint") or module_compare_fingerprint(
            src_mods[key], object_type
        )
        th = tgt_mods[key].get("fingerprint") or module_compare_fingerprint(
            tgt_mods[key], object_type
        )
        if sh != th:
            entry = {
                "key": key,
                "schema": src_mods[key].get("schema", ""),
                "name": src_mods[key].get("name", ""),
                "source_hash": sh,
                "target_hash": th,
            }
            note = expected_azure_gap_note(key, "programmable", dest_is_azure)
            skip_reason = is_azure_nonportable_module(src_mods[key]) if dest_is_azure else None
            if skip_reason:
                entry["expected_skip_reason"] = skip_reason
                entry["expected_skip_note"] = EXPECTED_SKIP_REASONS.get(
                    skip_reason, skip_reason
                )
            elif note:
                entry["expected_skip_note"] = note
            different.append(entry)

    base["different"] = different

    proc_expected_skip: List[Dict[str, str]] = []
    if dest_is_azure:
        for key in base["missing"]:
            entry = src_mods.get(key)
            if not entry:
                continue
            reason = is_azure_nonportable_module(entry)
            if reason:
                proc_expected_skip.append(
                    {
                        "key": key,
                        "name": entry.get("name", key),
                        "reason": reason,
                        "note": EXPECTED_SKIP_REASONS.get(reason, reason),
                    }
                )

    base["expected_skip"] = proc_expected_skip
    base["expected_skip_count"] = len(proc_expected_skip)
    base["different_count"] = len(different)
    return base


def _diff_users(
    src_users: Dict[str, Dict[str, Any]],
    tgt_users: Dict[str, Dict[str, Any]],
    dest_is_azure: bool,
) -> Dict[str, Any]:
    src_keys = {k.lower() for k in src_users}
    tgt_keys = {k.lower() for k in tgt_users}
    base = _diff_name_sets(src_keys, tgt_keys, "USER")

    expected_skip: List[Dict[str, str]] = []
    for key in base["missing"]:
        src_entry = src_users.get(key) or next(
            (v for k, v in src_users.items() if k.lower() == key),
            None,
        )
        if not src_entry:
            continue
        name = src_entry.get("name", key)
        if dest_is_azure and (
            src_entry.get("windows") or is_windows_principal_name(name)
        ):
            expected_skip.append(
                {
                    "key": key,
                    "name": name,
                    "reason": "windows_principal_azure",
                    "note": "Map Entra ID group/user on target; do not emit CREATE USER FROM WINDOWS on MI",
                }
            )

    base["expected_skip"] = expected_skip
    base["expected_skip_count"] = len(expected_skip)
    return base


def _diff_principal_schemas(
    src_schemas: Dict[str, Dict[str, Any]],
    tgt_schemas: Dict[str, Dict[str, Any]],
    dest_is_azure: bool,
) -> Dict[str, Any]:
    src_keys = set(src_schemas.keys())
    tgt_keys = set(tgt_schemas.keys())
    base = _diff_name_sets(src_keys, tgt_keys, "SCHEMA")
    expected_skip: List[Dict[str, str]] = []
    if dest_is_azure:
        for key in base["missing"]:
            entry = src_schemas.get(key) or {}
            name = entry.get("name", key)
            expected_skip.append(
                {
                    "key": key,
                    "name": name,
                    "reason": "windows_principal_azure",
                    "note": (
                        "Principal-named schema requires Entra user on target; "
                        "CREATE SCHEMA not emitted on MI"
                    ),
                }
            )
    base["expected_skip"] = expected_skip
    base["expected_skip_count"] = len(expected_skip)
    return base


def _normalize_permission_batch(batch: str) -> str:
    s = " ".join((batch or "").split()).upper().rstrip(";")
    # TYPE::[schema].[name] — ignore stray space after TYPE::
    return re.sub(r"TYPE::\s+\[", "TYPE::[", s)


def _diff_permission_batches(
    src_batches: List[str],
    tgt_batches: List[str],
) -> Dict[str, Any]:
    src_set = {_normalize_permission_batch(b) for b in src_batches if b and b.strip()}
    tgt_set = {_normalize_permission_batch(b) for b in tgt_batches if b and b.strip()}
    missing_norm = sorted(src_set - tgt_set)
    # Preserve source batch text for repair (first match per normalized form).
    src_by_norm: Dict[str, str] = {}
    for batch in src_batches:
        norm = _normalize_permission_batch(batch)
        if norm and norm not in src_by_norm:
            src_by_norm[norm] = batch.strip()
    missing_batches = [src_by_norm[n] for n in missing_norm if n in src_by_norm]
    return {
        "object_type": "PERMISSION",
        "missing": missing_batches,
        "missing_count": len(missing_batches),
        "extra_count": len(tgt_set - src_set),
    }


def _diff_assemblies(
    src_names: Set[str],
    tgt_names: Set[str],
    dest_is_azure: bool,
) -> Dict[str, Any]:
    base = _diff_name_sets(src_names, tgt_names, "ASSEMBLY")
    expected_skip: List[Dict[str, str]] = []
    if dest_is_azure:
        for name in base["missing"]:
            if is_mi_blocked_framework_assembly_name(name):
                expected_skip.append(
                    {
                        "name": name,
                        "reason": "clr_framework_policy_azure",
                        "note": EXPECTED_SKIP_REASONS["clr_framework_policy_azure"],
                    }
                )
    base["expected_skip"] = expected_skip
    base["expected_skip_count"] = len(expected_skip)
    return base


def compare_schema_catalogs(
    source_catalog: Dict[str, Any],
    target_catalog: Dict[str, Any],
    *,
    source_label: str = "source",
    target_label: str = "target",
    dest_is_azure: bool = False,
) -> Dict[str, Any]:
    """
    Compare two catalogs and return a structured diff report (JSON-serializable).
    """
    src_tables = set(source_catalog.get("tables") or [])
    tgt_tables = set(target_catalog.get("tables") or [])

    report: Dict[str, Any] = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source": source_label,
        "target": target_label,
        "dest_is_azure": dest_is_azure,
        "tables": _diff_name_sets(src_tables, tgt_tables, "TABLE"),
        "procedures": _diff_modules(
            source_catalog.get("procedures") or {},
            target_catalog.get("procedures") or {},
            "PROC",
            dest_is_azure,
        ),
        "views": _diff_modules(
            source_catalog.get("views") or {},
            target_catalog.get("views") or {},
            "VIEW",
            dest_is_azure,
        ),
        "functions": _diff_modules(
            source_catalog.get("functions") or {},
            target_catalog.get("functions") or {},
            "FUNCTION",
            dest_is_azure,
        ),
        "assemblies": _diff_assemblies(
            set(source_catalog.get("assemblies") or []),
            set(target_catalog.get("assemblies") or []),
            dest_is_azure,
        ),
        "users": _diff_users(
            source_catalog.get("users") or {},
            target_catalog.get("users") or {},
            dest_is_azure,
        ),
        "principal_schemas": _diff_principal_schemas(
            source_catalog.get("principal_schemas") or {},
            target_catalog.get("principal_schemas") or {},
            dest_is_azure,
        ),
        "permissions": _diff_permission_batches(
            list(source_catalog.get("permission_batches") or []),
            list(target_catalog.get("permission_batches") or []),
        ),
    }

    report["summary"] = summarize_diff_report(report)
    return report


def summarize_diff_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """Human-oriented counts for logging and GUI."""
    parts = (
        "tables",
        "procedures",
        "views",
        "functions",
        "assemblies",
        "users",
        "principal_schemas",
        "permissions",
    )
    lines: List[str] = []
    totals = {"missing": 0, "extra": 0, "different": 0, "expected_skip": 0}

    for part in parts:
        block = report.get(part) or {}
        mc = block.get("missing_count", len(block.get("missing") or []))
        ec = block.get("extra_count", len(block.get("extra") or []))
        dc = block.get("different_count", len(block.get("different") or []))
        esc = block.get("expected_skip_count", len(block.get("expected_skip") or []))
        totals["missing"] += mc
        totals["extra"] += ec
        totals["different"] += dc
        totals["expected_skip"] += esc
        if mc or ec or dc:
            lines.append(f"{part}: missing={mc} extra={ec} different={dc}")

    if report.get("users", {}).get("expected_skip"):
        lines.append(
            f"users: {len(report['users']['expected_skip'])} Windows/AD principals (expected_skip on Azure)"
        )
    asm_esc = (report.get("assemblies") or {}).get("expected_skip_count", 0)
    if asm_esc:
        lines.append(f"assemblies: {asm_esc} blocked .NET Framework CLR (expected_skip on MI)")

    return {
        "lines": lines,
        "text": "; ".join(lines) if lines else "No differences",
        **totals,
    }


def compare_live_databases(
    source_cur,
    target_cur,
    *,
    source_label: str = "source",
    target_label: str = "target",
    dest_is_azure: bool = False,
    return_source_catalog: bool = False,
) -> Union[Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Compare two connected databases (tables, modules, assemblies, users)."""
    src_cat = fetch_live_catalog(
        source_cur,
        include_permissions=True,
        include_assembly_batches=True,
        for_schema_compare=True,
    )
    tgt_cat = fetch_live_catalog(
        target_cur,
        include_permissions=True,
        for_schema_compare=True,
        include_diagram_supplemental=True,
    )
    report = compare_schema_catalogs(
        src_cat,
        tgt_cat,
        source_label=source_label,
        target_label=target_label,
        dest_is_azure=dest_is_azure,
    )
    if return_source_catalog:
        return report, src_cat
    return report


def compare_backup_to_live(
    backup_path,
    target_cur,
    *,
    target_label: str = "target",
    dest_is_azure: bool = False,
) -> Dict[str, Any]:
    """Compare mirror backup folder as source vs live target database."""
    from pathlib import Path

    src_cat = load_backup_catalog(Path(backup_path))
    tgt_cat = fetch_live_catalog(target_cur)
    return compare_schema_catalogs(
        src_cat,
        tgt_cat,
        source_label=str(backup_path),
        target_label=target_label,
        dest_is_azure=dest_is_azure,
    )
