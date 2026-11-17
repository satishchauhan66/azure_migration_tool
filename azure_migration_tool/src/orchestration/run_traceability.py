# Author: Satish Chauhan

"""Persist redacted job effective config and failure briefs next to result JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

def _sanitize_job(job: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from .migration_brief import _sanitize
    except ImportError:
        from azure_migration_tool.src.orchestration.migration_brief import _sanitize
    return _sanitize(dict(job))


def _log_tail_for_job(job_id: str) -> List[str]:
    jid = (job_id or "").strip()
    if not jid:
        return []
    try:
        try:
            from gui.utils.run_history import load_run_history
        except ImportError:
            from azure_migration_tool.gui.utils.run_history import load_run_history
        for row in load_run_history(100):
            if (row.get("job_id") or "").strip() == jid:
                return list(row.get("log_lines") or [])
    except Exception:
        pass
    return []


def attach_run_traceability(
    result: Dict[str, Any],
    job: Dict[str, Any],
    *,
    log_tail: Optional[List[str]] = None,
) -> None:
    """
    Write ``{job_id}_job.effective.json``; on failure also ``migration_brief`` .md + .json.
    Updates ``result`` with ``artifact_paths`` (mutates in place).
    """
    result_path = (result.get("result_path") or "").strip()
    if not result_path:
        return
    rp = Path(result_path)
    out_dir = rp.parent
    job_id = (result.get("job_id") or job.get("job_id") or rp.stem.replace("_result", "")).strip()
    if not job_id:
        return

    effective_path = out_dir / f"{job_id}_job.effective.json"
    sanitized = _sanitize_job(job)
    effective_path.write_text(json.dumps(sanitized, indent=2), encoding="utf-8")

    artifacts: Dict[str, str] = {
        "result_json": str(rp),
        "effective_job_json": str(effective_path),
    }

    status = (result.get("status") or "").strip().lower()
    tail = list(log_tail or []) or _log_tail_for_job(job_id)
    needs_brief = status != "success" or bool(result.get("errors"))
    if needs_brief:
        try:
            from .migration_brief import export_migration_brief
        except ImportError:
            from azure_migration_tool.src.orchestration.migration_brief import (
                export_migration_brief,
            )
        brief_md = out_dir / f"{job_id}_migration_brief.md"
        brief_json = out_dir / f"{job_id}_migration_brief.json"
        export_migration_brief(
            str(brief_md),
            result_path=str(rp),
            job_path=str(effective_path),
            log_tail=tail,
            format="markdown",
        )
        export_migration_brief(
            str(brief_json),
            result_path=str(rp),
            job_path=str(effective_path),
            log_tail=tail,
            format="json",
        )
        artifacts["brief_markdown"] = str(brief_md)
        artifacts["brief_json"] = str(brief_json)

    result["artifact_paths"] = artifacts
    result["effective_job_path"] = str(effective_path)