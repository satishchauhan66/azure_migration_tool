# Author: Satish Chauhan

"""Persist pipeline run summaries for AgentOps dashboard."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_MAX_ENTRIES = 300
_FILENAME = "run_history.json"


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
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    rows = data if isinstance(data, list) else data.get("runs") or []
    return list(rows)[:limit]


def save_run_history(rows: List[Dict[str, Any]]) -> None:
    path = _history_path()
    trimmed = rows[:_MAX_ENTRIES]
    path.write_text(json.dumps(trimmed, indent=2), encoding="utf-8")


def clear_run_history() -> None:
    path = _history_path()
    if path.is_file():
        path.unlink()


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
    entry: Dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "recorded_utc": datetime.now(timezone.utc).isoformat(),
        "job_id": result.get("job_id") or job.get("job_id") or "",
        "status": result.get("status") or "unknown",
        "environment": job.get("environment") or result.get("environment") or "",
        "source_server": (source.get("server") or result.get("source_server") or "").strip(),
        "database_count": len(dbs),
        "databases_preview": preview,
        "started_utc": result.get("started_utc"),
        "ended_utc": result.get("ended_utc"),
        "duration_sec": _duration_seconds(result.get("started_utc"), result.get("ended_utc")),
        "result_path": result.get("result_path") or "",
        "project_path": (job.get("project_path") or (job.get("output") or {}).get("project_path") or ""),
        "origin": (job.get("_run_origin") or origin or "worker").strip(),
        "error_preview": (errors[0] if errors else "")[:500],
        "operation": result.get("operation") or "bulk_local_backup_blob_restore",
    }

    rows = load_run_history(_MAX_ENTRIES)
    rows.insert(0, entry)
    save_run_history(rows)
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
