# Author: Satish Chauhan

"""Migration brief — context pack for agents and PMs (markdown + JSON)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_SENSITIVE_KEYS = frozenset(
    {
        "password",
        "connection_string",
        "conn_str",
        "blob_connection_string",
        "secret",
        "token",
        "access_key",
    }
)


def _redact_text(text: str) -> str:
    try:
        from ..utils.redact_secrets import redact_sensitive_text
    except ImportError:
        from azure_migration_tool.src.utils.redact_secrets import redact_sensitive_text
    return redact_sensitive_text(text)


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            if str(k).lower() in _SENSITIVE_KEYS:
                out[k] = "[REDACTED]"
            else:
                out[k] = _sanitize(v)
        return out
    if isinstance(value, list):
        return [_sanitize(v) for v in value]
    if isinstance(value, str):
        return _redact_text(value)
    return value


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _guess_job_path(result_path: Path, job_id: str) -> Optional[Path]:
    parent = result_path.parent
    for name in (f"{job_id}.json", f"{job_id}_job.json"):
        p = parent / name
        if p.is_file():
            return p
    return None


def _environment_profile(environment: str) -> Dict[str, Any]:
    env = (environment or "").strip()
    if not env:
        return {}
    try:
        from gui.utils.environment_profiles import get_profile
    except ImportError:
        from azure_migration_tool.gui.utils.environment_profiles import get_profile
    prof = get_profile(env) or {}
    return _sanitize(dict(prof))


def _org_context() -> Dict[str, Any]:
    try:
        from gui.utils.org_settings import load_org_settings
    except ImportError:
        from azure_migration_tool.gui.utils.org_settings import load_org_settings
    return _sanitize(load_org_settings())


def build_migration_brief(
    *,
    result_path: Optional[str] = None,
    job_path: Optional[str] = None,
    history_entry: Optional[Dict[str, Any]] = None,
    log_tail: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Assemble a redacted brief from result JSON, optional job file, and org/env context."""
    result: Dict[str, Any] = {}
    job: Dict[str, Any] = {}
    rp = Path(result_path).expanduser().resolve() if result_path else None
    if rp and rp.is_file():
        result = _sanitize(_load_json(rp) or {})
    if job_path:
        jp = Path(job_path).expanduser().resolve()
        job = _sanitize(_load_json(jp) or {})
    elif rp and result.get("job_id"):
        guess = _guess_job_path(rp, str(result["job_id"]))
        if guess:
            job = _sanitize(_load_json(guess) or {})

    env = (
        (history_entry or {}).get("environment")
        or result.get("environment")
        or job.get("environment")
        or ""
    )
    status = result.get("status") or (history_entry or {}).get("status") or "unknown"
    job_id = result.get("job_id") or job.get("job_id") or (history_entry or {}).get("job_id") or ""
    dbs = result.get("backup_runs") or []
    db_names = [
        (r.get("database") or "").strip() for r in dbs if isinstance(r, dict) and r.get("database")
    ]
    if not db_names and history_entry:
        preview = (history_entry.get("databases_preview") or "").strip()
        if preview:
            db_names = [p.strip() for p in preview.split(",") if p.strip()]

    pipe_rows = ((result.get("steps") or {}).get("pipeline") or {}).get("databases") or []

    catalog_entries: List[Dict[str, Any]] = []
    try:
        from gui.utils.environment_profiles import resolve_database_catalog
    except ImportError:
        from azure_migration_tool.gui.utils.environment_profiles import resolve_database_catalog
    src_server = (
        result.get("source_server")
        or (job.get("source") or {}).get("server")
        or (history_entry or {}).get("source_server")
        or ""
    )
    for db_name in db_names:
        if db_name:
            catalog_entries.append(
                resolve_database_catalog(db_name, restore_server=str(src_server))
            )

    brief: Dict[str, Any] = {
        "brief_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "job_id": job_id,
            "status": status,
            "environment": env,
            "source_server": result.get("source_server")
            or (job.get("source") or {}).get("server")
            or (history_entry or {}).get("source_server"),
            "database_count": len(db_names) or (history_entry or {}).get("database_count"),
            "databases": db_names,
            "started_utc": result.get("started_utc") or (history_entry or {}).get("started_utc"),
            "ended_utc": result.get("ended_utc") or (history_entry or {}).get("ended_utc"),
            "duration_sec": (history_entry or {}).get("duration_sec"),
            "origin": (history_entry or {}).get("origin"),
            "result_path": str(rp) if rp else (history_entry or {}).get("result_path"),
            "project_path": (history_entry or {}).get("project_path") or job.get("project_path"),
        },
        "errors": list(result.get("errors") or [])[:30],
        "per_database_pipeline": pipe_rows,
        "effective_job": job,
        "run_result": result,
        "organization_defaults": _org_context(),
        "environment_profile": _environment_profile(str(env)),
        "database_catalog": catalog_entries,
        "history_snapshot": _sanitize(history_entry) if history_entry else None,
        "log_tail": [_redact_text(str(line)) for line in (log_tail or [])[-80:]],
        "agent_hints": [
            "Use amt_prepare_job_payload to preview merged defaults before re-run.",
            "Use amt_resolve_environment when environment is ambiguous.",
            "Check per_database_pipeline for which step failed per DB.",
        ],
    }
    return brief


def brief_to_markdown(brief: Dict[str, Any]) -> str:
    s = brief.get("summary") or {}
    lines = [
        "# Migration brief",
        "",
        f"Generated: {brief.get('generated_utc', '')}",
        "",
        "## Summary",
        "",
        f"- **Job ID:** {s.get('job_id', '')}",
        f"- **Status:** {s.get('status', '')}",
        f"- **Environment:** {s.get('environment') or '—'}",
        f"- **Source server:** {s.get('source_server') or '—'}",
        f"- **Databases ({s.get('database_count', 0)}):** "
        + ", ".join(s.get("databases") or [])[:2000],
        f"- **Started:** {s.get('started_utc') or '—'}",
        f"- **Ended:** {s.get('ended_utc') or '—'}",
        f"- **Duration:** {s.get('duration_sec', '—')}s",
        f"- **Origin:** {s.get('origin') or '—'}",
        f"- **Result file:** `{s.get('result_path') or ''}`",
        "",
    ]
    errors = brief.get("errors") or []
    if errors:
        lines.extend(["## Errors", ""])
        for e in errors:
            lines.append(f"- {e}")
        lines.append("")

    pipe = brief.get("per_database_pipeline") or []
    if pipe:
        lines.extend(["## Per-database pipeline", "", "| Database | Backup | Upload | Restore |", "|---|---|---|---|"])
        for row in pipe:
            if not isinstance(row, dict):
                continue
            db = row.get("database") or ""
            lines.append(
                f"| {db} | {row.get('backup', '')} | {row.get('upload', '')} | {row.get('restore', '')} |"
            )
        lines.append("")

    hints = brief.get("agent_hints") or []
    if hints:
        lines.extend(["## Agent hints", ""])
        for h in hints:
            lines.append(f"- {h}")
        lines.append("")

    org = brief.get("organization_defaults") or {}
    if org:
        lines.extend(["## Organization defaults (redacted)", "", "```json", json.dumps(org, indent=2), "```", ""])

    prof = brief.get("environment_profile") or {}
    if prof:
        lines.extend(["## Environment profile (redacted)", "", "```json", json.dumps(prof, indent=2), "```", ""])

    tail = brief.get("log_tail") or []
    if tail:
        lines.extend(["## Log tail", "", "```"])
        lines.extend(tail)
        lines.extend(["```", ""])

    lines.extend(
        [
            "## Full run result (redacted JSON)",
            "",
            "```json",
            json.dumps(brief.get("run_result") or {}, indent=2),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def export_migration_brief(
    output_path: str,
    *,
    result_path: Optional[str] = None,
    job_path: Optional[str] = None,
    history_entry: Optional[Dict[str, Any]] = None,
    log_tail: Optional[List[str]] = None,
    format: str = "markdown",
) -> Path:
    brief = build_migration_brief(
        result_path=result_path,
        job_path=job_path,
        history_entry=history_entry,
        log_tail=log_tail,
    )
    out = Path(output_path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    fmt = (format or "markdown").strip().lower()
    if fmt in ("md", "markdown"):
        if out.suffix.lower() not in (".md", ".markdown"):
            out = out.with_suffix(".md")
        out.write_text(brief_to_markdown(brief), encoding="utf-8")
    else:
        if out.suffix.lower() != ".json":
            out = out.with_suffix(".json")
        out.write_text(json.dumps(brief, indent=2), encoding="utf-8")
    return out
