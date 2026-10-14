# Author: Satish Chauhan

"""Declarative multi-step agent playbooks (resolve → prepare → run → brief)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

_VAR_PATTERN = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")

_RUN_TOOLS = frozenset({"run_bulk_job_json", "run_bulk_job_file"})


def bundled_playbooks_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "examples" / "playbooks"


def user_playbooks_dir() -> Path:
    import os

    base = os.environ.get("APPDATA") or os.path.expanduser("~")
    path = Path(base) / "AzureMigrationTool" / "playbooks"
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_playbook_paths() -> List[Path]:
    paths: List[Path] = []
    for folder in (bundled_playbooks_dir(), user_playbooks_dir()):
        if folder.is_dir():
            paths.extend(sorted(folder.glob("*.json")))
    seen = set()
    unique: List[Path] = []
    for p in paths:
        key = p.name.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(p)
    return unique


def load_playbook(path: str | Path) -> Dict[str, Any]:
    p = Path(path).expanduser().resolve()
    if not p.is_file():
        raise FileNotFoundError(str(p))
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Playbook must be a JSON object.")
    if not data.get("steps"):
        raise ValueError("Playbook must include a steps array.")
    return data


def _get_path(context: Dict[str, Any], dotted: str) -> Any:
    parts = dotted.strip().split(".")
    cur: Any = context
    for part in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _substitute_string(text: str, context: Dict[str, Any]) -> Any:
    if "{{" not in text:
        return text

    def repl(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        val = _get_path(context, key)
        if val is None:
            return match.group(0)
        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return str(val)

    whole = _VAR_PATTERN.sub(repl, text)
    if whole.startswith("{") and whole.endswith("}"):
        try:
            return json.loads(whole)
        except json.JSONDecodeError:
            pass
    return whole


def _substitute(value: Any, context: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        out = _substitute_string(value, context)
        if isinstance(out, str) and out == value and "{{" in value:
            key = value.strip().strip("{}").strip()
            resolved = _get_path(context, key)
            if resolved is not None:
                return resolved
        return out
    if isinstance(value, dict):
        return {k: _substitute(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute(v, context) for v in value]
    return value


def _tool_resolve_environment(
    database: str = "",
    restore_server: str = "",
    storage_url: str = "",
    environment: str = "",
) -> Dict[str, Any]:
    try:
        from gui.utils.environment_profiles import detect_environment_from_hints, get_profile
    except ImportError:
        from azure_migration_tool.gui.utils.environment_profiles import (
            detect_environment_from_hints,
            get_profile,
        )
    detection = detect_environment_from_hints(
        environment=environment,
        database=database,
        restore_server=restore_server,
        storage_url=storage_url,
    )
    env = detection.get("environment") or "uat"
    prof = get_profile(env) or {}
    return {
        "environment": env,
        "detection": detection,
        "restore_target": {
            "server": (prof.get("mi_server") or "").strip(),
            "target_managed_instance": bool(prof.get("target_managed_instance", True)),
        },
        "blob": {
            "profile": env,
            "use_saved_settings": False,
            "auth_mode": (prof.get("blob_auth_mode") or "managed_identity").strip(),
            "storage_account_url": (prof.get("storage_account_url") or "").strip(),
            "container": (prof.get("container") or "sqlbackups").strip(),
            "folder": (prof.get("blob_folder_prefix") or "").strip(),
        },
    }


def _tool_prepare_job_payload(job_json: Any) -> Dict[str, Any]:
    from azure_migration_tool.worker.__main__ import _prepare_job

    if isinstance(job_json, dict):
        raw = job_json
    else:
        raw = json.loads(str(job_json))
    if not isinstance(raw, dict):
        raise ValueError("job_json must be a JSON object.")
    effective = _prepare_job(raw)
    return {"effective_job": effective}


def _tool_run_bulk_job_json(job_json: Any, result_path: str = "") -> Dict[str, Any]:
    from azure_migration_tool.worker.__main__ import _prepare_job
    from azure_migration_tool.src.orchestration.bulk_local_blob_pipeline import (
        run_bulk_local_backup_blob_restore_job,
    )

    if isinstance(job_json, dict):
        raw = job_json
    else:
        raw = json.loads(str(job_json))
    job = _prepare_job(raw)
    job["_run_origin"] = "playbook"
    if result_path:
        out = job.setdefault("output", {})
        if isinstance(out, dict):
            out["result_path"] = str(Path(result_path).expanduser().resolve())
    return run_bulk_local_backup_blob_restore_job(job, log=lambda _m: None)


def _tool_validate_job(job_path: str) -> Dict[str, Any]:
    from azure_migration_tool.worker.__main__ import _load_job, _prepare_job, _validate_job

    path = Path(job_path).expanduser().resolve()
    job = _prepare_job(_load_job(path))
    issues = _validate_job(job)
    return {"job_path": str(path), "valid": not issues, "issues": issues}


def _tool_import_job_spreadsheet(file_path: str, prepare_defaults: bool = True) -> Dict[str, Any]:
    from azure_migration_tool.src.orchestration.job_spreadsheet import read_job_spreadsheet
    from azure_migration_tool.worker.__main__ import _prepare_job

    jobs = read_job_spreadsheet(file_path)
    if prepare_defaults:
        jobs = [_prepare_job(dict(j)) for j in jobs]
    return {"job_count": len(jobs), "jobs": jobs}


def _tool_export_migration_brief(
    output_path: str,
    result_path: str = "",
    job_path: str = "",
) -> Dict[str, Any]:
    from azure_migration_tool.src.orchestration.migration_brief import export_migration_brief

    fmt = "json" if output_path.lower().endswith(".json") else "markdown"
    out = export_migration_brief(
        output_path,
        result_path=result_path or None,
        job_path=job_path or None,
        format=fmt,
    )
    return {"brief_path": str(out)}


def _tool_list_run_history(limit: int = 50) -> Dict[str, Any]:
    try:
        from gui.utils.run_history import load_run_history, summarize_history
    except ImportError:
        from azure_migration_tool.gui.utils.run_history import load_run_history, summarize_history
    rows = load_run_history(max(1, min(200, int(limit))))
    return {"summary": summarize_history(rows), "runs": rows}


TOOL_HANDLERS: Dict[str, Callable[..., Dict[str, Any]]] = {
    "resolve_environment": _tool_resolve_environment,
    "prepare_job_payload": _tool_prepare_job_payload,
    "run_bulk_job_json": _tool_run_bulk_job_json,
    "run_bulk_job_file": lambda job_path, result_path="": _tool_run_bulk_job_json(
        Path(job_path).expanduser().read_text(encoding="utf-8"),
        result_path=result_path,
    ),
    "validate_job": _tool_validate_job,
    "import_job_spreadsheet": _tool_import_job_spreadsheet,
    "export_migration_brief": _tool_export_migration_brief,
    "list_run_history": _tool_list_run_history,
}


def run_playbook(
    playbook: Dict[str, Any],
    *,
    variables: Optional[Dict[str, Any]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Execute playbook steps in order. Context: vars, steps (outputs by save_as).
    dry_run skips run_* tools.
    """
    context: Dict[str, Any] = {
        "vars": dict(variables or {}),
        "steps": {},
        "playbook_id": playbook.get("playbook_id") or playbook.get("id") or "",
        "dry_run": dry_run,
    }
    playbook_vars = playbook.get("variables") or {}
    if isinstance(playbook_vars, dict):
        for k, v in playbook_vars.items():
            context["vars"].setdefault(k, v)

    trace: List[Dict[str, Any]] = []
    for step in playbook.get("steps") or []:
        if not isinstance(step, dict):
            continue
        step_id = (step.get("id") or "").strip() or f"step_{len(trace)}"
        tool = (step.get("tool") or "").strip()
        if not tool:
            trace.append({"id": step_id, "status": "skipped", "reason": "no tool"})
            continue
        if dry_run and tool in _RUN_TOOLS:
            trace.append({"id": step_id, "tool": tool, "status": "skipped", "reason": "dry_run"})
            continue
        handler = TOOL_HANDLERS.get(tool)
        if not handler:
            trace.append({"id": step_id, "tool": tool, "status": "failed", "error": "unknown tool"})
            break
        args = _substitute(step.get("args") or {}, context)
        if tool in ("prepare_job_payload", "run_bulk_job_json") and isinstance(args.get("job_json"), dict):
            args["job_json"] = json.dumps(args["job_json"])
        try:
            result = handler(**args)
        except TypeError as exc:
            trace.append({"id": step_id, "tool": tool, "status": "failed", "error": str(exc)})
            break
        except Exception as exc:
            trace.append({"id": step_id, "tool": tool, "status": "failed", "error": str(exc)})
            if not step.get("optional"):
                break
            continue
        save_as = (step.get("save_as") or "").strip()
        if save_as:
            context["steps"][save_as] = result
        trace.append({"id": step_id, "tool": tool, "status": "ok", "save_as": save_as or None})
        if step.get("stop_on_status"):
            want = step["stop_on_status"]
            if isinstance(result, dict) and result.get("status") == want:
                break

    return {
        "playbook_id": context["playbook_id"],
        "dry_run": dry_run,
        "trace": trace,
        "vars": context["vars"],
        "steps": context["steps"],
    }
