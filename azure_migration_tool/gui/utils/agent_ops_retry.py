# Author: Satish Chauhan

"""Build and run partial pipeline retries from AgentOps run records."""

from __future__ import annotations

import json
import re
import threading
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_STEPS = ("backup", "upload", "restore")
_FAIL_STATUSES = frozenset({"failed", "error", "cancelled"})
_PARTIAL_OK = frozenset({"success", "ok", "skipped", "partial"})


def _norm_status(value: Any) -> str:
    return (str(value or "")).strip().lower()


def failed_steps_from_run(
    result: Dict[str, Any],
    *,
    step_state: Optional[Dict[str, Any]] = None,
) -> List[str]:
    failed: Set[str] = set()
    state = step_state if isinstance(step_state, dict) else {}
    for step in _STEPS:
        slot = state.get(step) if isinstance(state.get(step), dict) else {}
        st = _norm_status(slot.get("status"))
        if st in _FAIL_STATUSES or st == "failed":
            failed.add(step)
    steps_root = result.get("steps") if isinstance(result.get("steps"), dict) else {}
    for step in _STEPS:
        block = steps_root.get(step)
        if isinstance(block, dict):
            st = _norm_status(block.get("status"))
            if st in _FAIL_STATUSES or st == "failed":
                failed.add(step)
            elif st == "partial":
                failed.add(step)
    pipe = steps_root.get("pipeline") if isinstance(steps_root.get("pipeline"), dict) else {}
    for row in pipe.get("databases") or []:
        if not isinstance(row, dict):
            continue
        for step in _STEPS:
            st = _norm_status(row.get(step))
            if st in _FAIL_STATUSES or st == "failed":
                failed.add(step)
    ordered = [s for s in _STEPS if s in failed]
    return ordered


def step_success(
    result: Dict[str, Any],
    step: str,
    *,
    step_state: Optional[Dict[str, Any]] = None,
) -> bool:
    state = step_state if isinstance(step_state, dict) else {}
    slot = state.get(step) if isinstance(state.get(step), dict) else {}
    st = _norm_status(slot.get("status"))
    if st in ("success", "ok"):
        return True
    block = (result.get("steps") or {}).get(step)
    if isinstance(block, dict):
        bst = _norm_status(block.get("status"))
        if bst in ("success", "skipped", "partial"):
            return bst == "success" or bst == "skipped"
    return False


def load_run_artifacts(
    history_row: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[str]]:
    """Return (effective_job, result, error_message)."""
    result: Dict[str, Any] = {}
    job: Dict[str, Any] = {}
    rp = (history_row.get("result_path") or "").strip()
    if rp and Path(rp).is_file():
        try:
            data = json.loads(Path(rp).read_text(encoding="utf-8"))
            if isinstance(data, dict):
                result = data
        except (OSError, json.JSONDecodeError) as exc:
            return {}, {}, str(exc)
    elif rp:
        return {}, {}, "Result file not found."

    ep = (history_row.get("effective_job_path") or result.get("effective_job_path") or "").strip()
    if not ep and rp and result.get("job_id"):
        guess = Path(rp).parent / f"{result['job_id']}_job.effective.json"
        if guess.is_file():
            ep = str(guess)
    if ep and Path(ep).is_file():
        try:
            data = json.loads(Path(ep).read_text(encoding="utf-8"))
            if isinstance(data, dict):
                job = data
        except (OSError, json.JSONDecodeError) as exc:
            return {}, result, f"Job file: {exc}"

    if not job and not result:
        return {}, {}, "No result or effective job JSON for this run."
    return job, result, None


def build_retry_job(
    base_job: Dict[str, Any],
    result: Dict[str, Any],
    retry_steps: List[str],
    *,
    step_state: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    steps = {s for s in retry_steps if s in _STEPS}
    if not steps:
        return None, "No steps selected."

    job = deepcopy(base_job)
    for meta in ("_effective", "_environment_resolution"):
        job.pop(meta, None)

    old_id = (result.get("job_id") or job.get("job_id") or "run").strip()
    safe = re.sub(r"[^\w\-]+", "-", old_id)[:40]
    suffix = "-".join(sorted(steps))
    job["job_id"] = f"{safe}-retry-{suffix}-{uuid.uuid4().hex[:6]}"
    job["retried_from_job_id"] = old_id
    job["_run_origin"] = "agentops_retry"

    job["steps"] = {
        "backup": "backup" in steps,
        "upload": "upload" in steps,
        "restore": "restore" in steps,
    }
    job.pop("skip_backup", None)
    job.pop("skip_upload", None)
    job.pop("skip_restore", None)

    runs = list(result.get("backup_runs") or job.get("backup_runs") or [])
    if not job["steps"]["backup"]:
        if not runs and (job["steps"]["upload"] or job["steps"]["restore"]):
            return None, "Cannot retry without backup_runs from the previous result (re-run backup first)."
        job["backup_runs"] = runs
    if job["steps"]["upload"] and not job["steps"]["backup"]:
        if not runs:
            return None, "No backup runs available to upload."
        job["backup_runs"] = runs

    if job["steps"]["restore"] and not job["steps"]["backup"]:
        job["backup_runs"] = runs
        if not runs:
            return None, "No backup runs available to restore."

    for prereq in _STEPS:
        idx = _STEPS.index(prereq)
        if prereq in steps:
            for prior in _STEPS[:idx]:
                if prior not in steps and not step_success(result, prior, step_state=step_state):
                    if prior == "backup" and runs:
                        continue
                    return (
                        None,
                        f"Cannot retry {prereq} only — prior step “{prior}” did not succeed. "
                        f"Include {prior} in the retry or fix the earlier step first.",
                    )
    return job, None


def run_retry_job_async(job: Dict[str, Any], on_done: Optional[Any] = None) -> None:
    """Queue retry on a background thread (same path as MCP)."""
    try:
        from gui.utils.run_history import record_pipeline_run_failed, record_pipeline_run_started
    except ImportError:
        from azure_migration_tool.gui.utils.run_history import (
            record_pipeline_run_failed,
            record_pipeline_run_started,
        )

    def work() -> None:
        try:
            record_pipeline_run_started(job, origin=job.get("_run_origin") or "agentops_retry")
            try:
                from azure_migration_tool.mcp_server import _run_job_dict
            except ImportError:
                from mcp_server import _run_job_dict  # type: ignore[no-redef]
            _run_job_dict(job)
        except Exception as exc:
            try:
                record_pipeline_run_failed(job, str(exc), origin="agentops_retry")
            except Exception:
                pass
        finally:
            if on_done:
                try:
                    on_done()
                except Exception:
                    pass

    threading.Thread(target=work, daemon=True).start()