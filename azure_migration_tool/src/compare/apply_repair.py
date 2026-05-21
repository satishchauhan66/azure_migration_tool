# Author: Sa-tish Chauhan

"""Apply generated schema repair scripts on a target database."""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..restore.schema_restore import execute_sql_file
from ..utils.azure_compat import (
    classify_expected_skip,
    detect_azure_engine_edition,
    detect_azure_sql_target,
    should_skip_already_exists_error,
    should_skip_azure_error,
    should_skip_windows_principal_error,
    EXPECTED_SKIP_REASONS,
)
from ..utils.sql import prepare_sql_batches


def apply_redgate_deployment_script(
    logger,
    cur,
    conn,
    script_path: Path,
    *,
    server: str = "",
    continue_on_error: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Apply a Redgate SQL Compare deployment script (GO-separated) on the target database.

    Uses the same batch splitting, SET/CREATE procedure rules, and Azure expected-skip
    handling as schema restore/repair.
    """
    azure_target = detect_azure_sql_target(cur, server)
    engine_edition = detect_azure_engine_edition(cur, server)
    return execute_sql_file(
        logger,
        cur,
        conn,
        Path(script_path),
        file_type="SCHEMA_REPAIR",
        continue_on_error=continue_on_error,
        dry_run=dry_run,
        mirror_source=False,
        azure_target=azure_target,
        azure_engine_edition=engine_edition,
    )


def apply_schema_repair(
    logger,
    cur,
    conn,
    fix_sql_path: Path,
    *,
    server: str = "",
    continue_on_error: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Execute a repair script using the same batch splitting and Azure rules as schema restore.
    """
    azure_target = detect_azure_sql_target(cur, server)
    engine_edition = detect_azure_engine_edition(cur, server)
    return execute_sql_file(
        logger,
        cur,
        conn,
        Path(fix_sql_path),
        file_type="SCHEMA_REPAIR",
        continue_on_error=continue_on_error,
        dry_run=dry_run,
        mirror_source=False,
        azure_target=azure_target,
        azure_engine_edition=engine_edition,
    )


def _expand_item_batches(batches: List[str]) -> List[str]:
    """Ensure each batch is ODBC-safe (PRINT/DROP/SET/CREATE never merged)."""
    expanded: List[str] = []
    for batch in batches:
        text = (batch or "").strip()
        if not text:
            continue
        sub = prepare_sql_batches(text, file_type="SCHEMA_REPAIR")
        expanded.extend(sub if sub else [text])
    return [b for b in expanded if b and b.strip()]


def _item_sql_text(item: Dict[str, Any]) -> str:
    batches: List[str] = list(item.get("sql_batches") or [])
    if batches:
        return "\nGO\n".join(b.strip() for b in batches if b and str(b).strip())
    return ""


def _execute_repair_batches(
    logger,
    cur,
    conn,
    batches: List[str],
    *,
    continue_on_error: bool = True,
) -> Dict[str, Any]:
    """Run pre-split repair batches one at a time with commit after each."""
    result = {
        "status": "started",
        "batches_total": len(batches),
        "batches_executed": 0,
        "batches_failed": 0,
        "batches_skipped": 0,
        "expected_skips": 0,
        "errors": [],
    }
    t0 = time.time()
    for idx, batch in enumerate(batches, 1):
        try:
            cur.execute(batch)
            conn.commit()
            result["batches_executed"] += 1
        except Exception as ex:
            error_str = f"{type(ex).__name__}: {ex}"
            should_skip = False
            skip_reason = None
            if should_skip_azure_error(error_str):
                should_skip = True
                skip_reason = "Azure SQL incompatible feature"
            elif should_skip_windows_principal_error(error_str):
                should_skip = True
                skip_reason = "Windows principal not portable to Azure SQL MI"
            elif should_skip_already_exists_error(error_str):
                should_skip = True
                skip_reason = "Object already exists"
            else:
                expected_on_error = classify_expected_skip(error_msg=error_str, batch_text=batch)
                if expected_on_error:
                    should_skip = True
                    skip_reason = EXPECTED_SKIP_REASONS.get(
                        expected_on_error, expected_on_error
                    )
            if should_skip:
                result["batches_skipped"] += 1
                result["expected_skips"] += 1
                logger.warning("Repair batch %d skipped: %s — %s", idx, skip_reason, error_str[:200])
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue
            result["batches_failed"] += 1
            result["errors"].append({"batch": idx, "error": error_str})
            logger.error("Repair batch %d failed: %s", idx, error_str[:500])
            if not continue_on_error:
                result["status"] = "failed"
                result["duration_seconds"] = round(time.time() - t0, 3)
                return result
            try:
                conn.rollback()
            except Exception:
                pass
    result["status"] = "failed" if result["batches_failed"] else "completed"
    result["duration_seconds"] = round(time.time() - t0, 3)
    return result


def apply_repair_item(
    logger,
    cur,
    conn,
    item: Dict[str, Any],
    *,
    server: str = "",
    continue_on_error: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Apply a single structured repair item to the target database."""
    item_id = item.get("id", "")
    if item.get("expected_skip"):
        return {
            "item_id": item_id,
            "status": "skipped",
            "reason": item.get("reason") or "expected_skip",
            "batches_executed": 0,
            "batches_failed": 0,
            "batches_skipped": 0,
        }

    raw_batches = list(item.get("sql_batches") or [])
    if not raw_batches:
        sql_text = _item_sql_text(item)
        if not sql_text.strip():
            return {
                "item_id": item_id,
                "status": "skipped",
                "reason": "no_sql",
                "batches_executed": 0,
                "batches_failed": 0,
                "batches_skipped": 0,
            }
        raw_batches = prepare_sql_batches(sql_text, file_type="SCHEMA_REPAIR")

    batches = _expand_item_batches(raw_batches)
    if not batches:
        return {
            "item_id": item_id,
            "status": "skipped",
            "reason": "no_sql",
            "batches_executed": 0,
            "batches_failed": 0,
            "batches_skipped": 0,
        }

    if dry_run:
        return {
            "item_id": item_id,
            "status": "dry_run",
            "batches_total": len(batches),
            "batches_executed": 0,
            "batches_failed": 0,
            "batches_skipped": 0,
        }

    result = _execute_repair_batches(
        logger, cur, conn, batches, continue_on_error=continue_on_error
    )
    result["item_id"] = item_id
    return result


def apply_repair_items(
    logger,
    cur,
    conn,
    items: List[Dict[str, Any]],
    *,
    server: str = "",
    continue_on_error: bool = True,
    dry_run: bool = False,
    skip_expected: bool = True,
) -> Dict[str, Any]:
    """Apply multiple repair items; returns per-item results."""
    results: List[Dict[str, Any]] = []
    for item in items:
        if skip_expected and item.get("expected_skip"):
            results.append(
                {
                    "item_id": item.get("id"),
                    "status": "skipped",
                    "reason": "expected_skip",
                }
            )
            continue
        results.append(
            apply_repair_item(
                logger,
                cur,
                conn,
                item,
                server=server,
                continue_on_error=continue_on_error,
                dry_run=dry_run,
            )
        )
    failed = sum(1 for r in results if r.get("status") not in ("completed", "skipped", "dry_run"))
    return {"status": "failed" if failed else "completed", "items": results}
