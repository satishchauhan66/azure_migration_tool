# Author: Satish Chauhan

"""Persist pipeline run summaries for AgentOps dashboard."""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_MAX_ENTRIES = 300
_FILENAME = "run_history.json"
_history_lock = threading.Lock()
_ACTIVE_STATUSES = frozenset({"running", "started", "queued"})


def _history_path() -> Path:
    base = os.environ.get("APPDATA") or os.path.expanduser("~")
    folder = Path(base) / "AzureMigrationTool"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / _FILENAME


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _duration_seconds(started: Optional[str], ended: Optional[str]) -> Optional[float]:
    a, b = _parse_iso(started), _parse_iso(ended)
    if not a or not b:
        return None
    return max(0.0, (b - a).total_seconds())


def load_run_history(limit: int = 200) -> List[Dict[str, Any]]:
    path = _history_path()
    if not path.is_file():
        return []
    with _history_lock:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
    rows = data if isinstance(data, list) else data.get("runs") or []
    return list(rows)[:limit]


def save_run_history(rows: List[Dict[str, Any]]) -> None:
    path = _history_path()
    trimmed = rows[:_MAX_ENTRIES]
    with _history_lock:
        path.write_text(json.dumps(trimmed, indent=2), encoding="utf-8")


def clear_run_history() -> None:
    path = _history_path()
    if path.is_file():
        path.unlink()


def _metadata_from_job(job: Dict[str, Any]) -> Dict[str, str]:
    source = job.get("source") or {}
    tgt = job.get("restore_target") or {}
    dbs = list(source.get("databases") or [])
    src_db = (str(job.get("src_db") or (dbs[0] if dbs else "") or "")).strip()
    dest_db = (str(job.get("dest_db") or src_db) or "").strip()
    return {
        "source_database": src_db,
        "dest_database": dest_db,
        "dest_server": (tgt.get("server") or "").strip(),
    }


def summarize_result_progress(result: Optional[Dict[str, Any]]) -> str:
    if not result or not isinstance(result, dict):
        return ""
    steps = result.get("steps") or {}
    parts: List[str] = []
    for name in ("backup", "upload", "restore"):
        block = steps.get(name)
        if isinstance(block, dict):
            st = (block.get("status") or "").strip()
            if st:
                parts.append(f"{name}:{st}")
    pipe = steps.get("pipeline") or {}
    for dbrow in pipe.get("databases") or []:
        if not isinstance(dbrow, dict):
            continue
        db = (dbrow.get("database") or "?").strip()
        for step in ("backup", "upload", "restore"):
            st = (dbrow.get(step) or "").strip()
            if st and st not in ("pending", "skipped"):
                parts.append(f"{db} {step}={st}")
    if not parts:
        return (result.get("status") or "").strip()
    text = " · ".join(parts[:10])
    if len(parts) > 10:
        text += " …"
    return text


def _find_running_row(rows: List[Dict[str, Any]], job_id: str) -> Optional[Dict[str, Any]]:
    return _find_active_row(rows, job_id)


def _find_active_row(rows: List[Dict[str, Any]], job_id: str) -> Optional[Dict[str, Any]]:
    jid = (job_id or "").strip()
    if not jid:
        return None
    active: Optional[Dict[str, Any]] = None
    fallback: Optional[Dict[str, Any]] = None
    for row in rows:
        if (row.get("job_id") or "").strip() != jid:
            continue
        if fallback is None:
            fallback = row
        st = (row.get("status") or "").lower()
        if st in _ACTIVE_STATUSES:
            return row
        if st not in ("success", "failed", "completed_with_errors"):
            active = row
    if active is not None:
        return active
    return fallback


def _mutate_history(mutator) -> None:
    with _history_lock:
        path = _history_path()
        if path.is_file():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return
        else:
            data = []
        rows = data if isinstance(data, list) else data.get("runs") or []
        if not mutator(rows):
            return
        path.write_text(json.dumps(rows[:_MAX_ENTRIES], indent=2), encoding="utf-8")


def append_run_history_log(job_id: str, message: str) -> None:
    jid = (job_id or "").strip()
    line = (message or "").strip()
    if not jid or not line:
        return
    try:
        from gui.utils.agent_ops_progress import (
            default_step_state,
            format_step_chips,
            latest_percent_from_logs,
        )
    except ImportError:
        from azure_migration_tool.gui.utils.agent_ops_progress import (
            default_step_state,
            format_step_chips,
            latest_percent_from_logs,
        )

    def mutate(rows: List[Dict[str, Any]]) -> bool:
        row = _find_active_row(rows, jid)
        if row is None:
            return False
        logs = list(row.get("log_lines") or [])
        logs.append(line)
        row["log_lines"] = logs[-400:]
        state = dict(row.get("step_state") or default_step_state())
        changed = False
        for step_key, log_step in (("restore", "restore"), ("backup", "backup")):
            pct = latest_percent_from_logs(logs, log_step)
            if pct is None:
                continue
            slot = state.get(step_key) if isinstance(state.get(step_key), dict) else {}
            st = (slot.get("status") or "pending").lower()
            if st in ("running", "pending", ""):
                slot = dict(slot)
                slot["status"] = "running"
                cur = slot.get("percent")
                try:
                    cur_f = float(cur) if cur is not None else None
                except (TypeError, ValueError):
                    cur_f = None
                if cur_f is None or pct > cur_f:
                    slot["percent"] = pct
                state[step_key] = slot
                row["current_step"] = step_key
                changed = True
        if changed:
            row["step_state"] = state
            row["progress"] = format_step_chips(state)
        return True

    _mutate_history(mutate)


def update_run_history_step(
    job_id: str,
    step: str,
    status: str,
    *,
    detail: str = "",
    percent: Optional[float] = None,
    database: str = "",
) -> None:
    jid = (job_id or "").strip()
    step_key = (step or "").strip().lower()
    if not jid or step_key not in ("backup", "upload", "restore"):
        return
    try:
        from gui.utils.agent_ops_progress import default_step_state, format_step_chips, parse_percent
    except ImportError:
        from azure_migration_tool.gui.utils.agent_ops_progress import (
            default_step_state,
            format_step_chips,
            parse_percent,
        )
    def mutate(rows: List[Dict[str, Any]]) -> bool:
        row = _find_active_row(rows, jid)
        if row is None:
            return False
        state = dict(row.get("step_state") or default_step_state())
        for name in ("backup", "upload", "restore"):
            if name not in state or not isinstance(state.get(name), dict):
                state[name] = {"status": "pending", "percent": None}
        st = (status or "").strip().lower()
        resolved_pct = percent
        if resolved_pct is None and st == "running":
            resolved_pct = parse_percent(detail)
        prev = state.get(step_key) if isinstance(state.get(step_key), dict) else {}
        if st == "running" and resolved_pct is None:
            prev_pct = prev.get("percent")
            if prev_pct is not None:
                resolved_pct = prev_pct
        if st == "success":
            resolved_pct = 100.0
        state[step_key] = {"status": st or "running", "percent": resolved_pct}
        row["step_state"] = state
        row["current_step"] = step_key
        chips = format_step_chips(state)
        if database:
            row["progress"] = f"{chips}  ({database})"
        else:
            row["progress"] = chips
        if detail and st == "failed":
            row["error_preview"] = str(detail)[:500]
        return True

    _mutate_history(mutate)


def update_run_history_progress(
    job_id: str,
    progress: str,
    *,
    current_step: str = "",
) -> None:
    jid = (job_id or "").strip()
    if not jid:
        return

    def mutate(rows: List[Dict[str, Any]]) -> bool:
        row = _find_active_row(rows, jid)
        if row is None:
            return False
        row["progress"] = (progress or "").strip()
        if current_step:
            row["current_step"] = current_step.strip()
        return True

    _mutate_history(mutate)


def record_pipeline_run_started(
    job: Dict[str, Any],
    *,
    origin: str = "mcp",
) -> Dict[str, Any]:
    """Log a run as soon as it is queued (shown in AgentOps while work is in progress)."""
    source = job.get("source") or {}
    dbs = list(source.get("databases") or [])
    meta = _metadata_from_job(job)
    entry: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "recorded_utc": datetime.now(timezone.utc).isoformat(),
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "job_id": (job.get("job_id") or "").strip(),
        "status": "running",
        "environment": job.get("environment") or "",
        "source_server": (source.get("server") or "").strip(),
        "source_database": meta["source_database"],
        "dest_server": meta["dest_server"],
        "dest_database": meta["dest_database"],
        "database_count": len(dbs),
        "databases_preview": ", ".join(dbs[:8]),
        "progress": "Queued",
        "current_step": "",
        "step_state": {
            "backup": {"status": "pending", "percent": None},
            "upload": {"status": "pending", "percent": None},
            "restore": {"status": "pending", "percent": None},
        },
        "log_lines": [],
        "ended_utc": None,
        "duration_sec": None,
        "result_path": "",
        "project_path": (job.get("project_path") or (job.get("output") or {}).get("project_path") or ""),
        "origin": (job.get("_run_origin") or origin or "mcp").strip(),
        "error_preview": "",
        "operation": job.get("operation") or "bulk_local_backup_blob_restore",
        "run_id": "",
        "app_version": "",
        "effective_job_path": "",
        "brief_path": "",
    }

    def mutate(rows: List[Dict[str, Any]]) -> bool:
        rows.insert(0, entry)
        return True

    _mutate_history(mutate)
    return entry


def record_pipeline_run_failed(
    job: Dict[str, Any],
    error: str,
    *,
    origin: str = "mcp",
) -> Dict[str, Any]:
    """Record a job that failed before or outside the pipeline runner."""
    now = datetime.now(timezone.utc).isoformat()
    result = {
        "job_id": job.get("job_id") or "",
        "status": "failed",
        "started_utc": now,
        "ended_utc": now,
        "errors": [error or "Unknown error"],
        "environment": job.get("environment") or "",
        "operation": job.get("operation") or "bulk_local_backup_blob_restore",
    }
    return record_pipeline_run(result, job, origin=origin)


def record_pipeline_run(
    result: Dict[str, Any],
    job: Optional[Dict[str, Any]] = None,
    *,
    origin: str = "worker",
) -> Dict[str, Any]:
    """Append one run record; returns the stored entry."""
    job = job or {}
    source = job.get("source") or {}
    dbs = list(source.get("databases") or [])
    if not dbs:
        dbs = [
            (r.get("database") or "").strip()
            for r in (result.get("backup_runs") or [])
            if isinstance(r, dict)
        ]
    preview = ", ".join(dbs[:8])
    if len(dbs) > 8:
        preview += f" (+{len(dbs) - 8} more)"

    errors = result.get("errors") or []
    meta = _metadata_from_job(job)
    progress = summarize_result_progress(result)
    entry: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "recorded_utc": datetime.now(timezone.utc).isoformat(),
        "job_id": result.get("job_id") or job.get("job_id") or "",
        "status": result.get("status") or "unknown",
        "environment": job.get("environment") or result.get("environment") or "",
        "source_server": (source.get("server") or result.get("source_server") or "").strip(),
        "source_database": meta["source_database"],
        "dest_server": meta["dest_server"],
        "dest_database": meta["dest_database"],
        "database_count": len(dbs),
        "databases_preview": preview,
        "progress": progress,
        "current_step": "",
        "started_utc": result.get("started_utc"),
        "ended_utc": result.get("ended_utc"),
        "duration_sec": _duration_seconds(result.get("started_utc"), result.get("ended_utc")),
        "result_path": result.get("result_path") or "",
        "project_path": (job.get("project_path") or (job.get("output") or {}).get("project_path") or ""),
        "origin": (job.get("_run_origin") or origin or "worker").strip(),
        "error_preview": (errors[0] if errors else "")[:500],
        "operation": result.get("operation") or "bulk_local_backup_blob_restore",
        "run_id": result.get("run_id") or "",
        "app_version": (result.get("provenance") or {}).get("app_version") or "",
        "effective_job_path": result.get("effective_job_path") or "",
        "brief_path": (result.get("artifact_paths") or {}).get("brief_markdown") or "",
    }

    jid = (entry.get("job_id") or "").strip()

    def mutate(rows: List[Dict[str, Any]]) -> bool:
        running = _find_running_row(rows, jid) if jid else None
        if running:
            entry["log_lines"] = list(running.get("log_lines") or [])[-400:]
            entry["step_state"] = running.get("step_state") or entry.get("step_state")
            entry["id"] = running.get("id") or entry.get("id")
            if not (entry.get("progress") or "").strip():
                try:
                    from gui.utils.agent_ops_progress import format_step_chips
                except ImportError:
                    from azure_migration_tool.gui.utils.agent_ops_progress import format_step_chips
                entry["progress"] = format_step_chips(entry.get("step_state"))
        if jid:
            rows[:] = [
                r
                for r in rows
                if not (
                    (r.get("job_id") or "").strip() == jid
                    and (r.get("status") or "").lower() in ("running", "started", "queued")
                )
            ]
        rows.insert(0, entry)
        return True

    _mutate_history(mutate)
    return entry


def summarize_history(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    total = len(rows)
    success = sum(1 for r in rows if (r.get("status") or "").lower() == "success")
    failed = sum(1 for r in rows if (r.get("status") or "").lower() == "failed")
    partial = sum(
        1
        for r in rows
        if (r.get("status") or "").lower() in ("completed_with_errors", "partial")
    )
    return {"total": total, "success": success, "failed": failed, "partial": partial}
