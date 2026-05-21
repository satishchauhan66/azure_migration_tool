# Author: Sa-tish Chauhan

"""Apply generated schema repair scripts on a target database."""

import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..restore.schema_restore import execute_sql_file
from ..utils.azure_compat import detect_azure_engine_edition, detect_azure_sql_target


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


def _item_sql_text(item: Dict[str, Any]) -> str:
    batches: List[str] = list(item.get("sql_batches") or [])
    if batches:
        return "\nGO\n".join(b.strip() for b in batches if b and str(b).strip())
    return ""


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

    if dry_run:
        return {
            "item_id": item_id,
            "status": "dry_run",
            "batches_executed": 0,
            "batches_failed": 0,
            "batches_skipped": 0,
        }

    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".sql",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            tmp.write(sql_text)
            tmp_path = Path(tmp.name)
        result = apply_schema_repair(
            logger,
            cur,
            conn,
            tmp_path,
            server=server,
            continue_on_error=continue_on_error,
            dry_run=False,
        )
        result["item_id"] = item_id
        return result
    finally:
        if tmp_path:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass


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
