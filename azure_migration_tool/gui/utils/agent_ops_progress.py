# Author: Satish Chauhan

"""Wire pipeline log/progress into AgentOps run history."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Tuple

LogFn = Callable[[str], None]
ProgressFn = Callable[[str, str, str, str], None]

_STEP_NAMES = ("backup", "upload", "restore")
_MAX_LOG_LINES = 400


def default_step_state() -> Dict[str, Dict[str, Any]]:
    return {name: {"status": "pending", "percent": None} for name in _STEP_NAMES}


def parse_percent(detail: str) -> Optional[float]:
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", detail or "")
    if not m:
        return None
    try:
        return max(0.0, min(100.0, float(m.group(1))))
    except ValueError:
        return None


_RESTORE_LOG_PROGRESS = re.compile(
    r"\[progress\].*?(\d+(?:\.\d+)?)\s*%\s*complete",
    re.IGNORECASE,
)
_BACKUP_LOG_PROGRESS = re.compile(
    r"\[backup progress\].*?(\d+(?:\.\d+)?)\s*%\s*complete",
    re.IGNORECASE,
)
_PERCENT_ANYWHERE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def latest_percent_from_logs(log_lines: Optional[List[str]], step: str) -> Optional[float]:
    if step not in ("restore", "backup"):
        return None
    best: Optional[float] = None
    for line in log_lines or []:
        text = str(line)
        if step == "backup":
            m = _BACKUP_LOG_PROGRESS.search(text)
        else:
            m = _RESTORE_LOG_PROGRESS.search(text)
        if not m:
            if step == "backup" and "[backup progress]" not in text.lower():
                continue
            if step == "restore" and "restore" not in text.lower() and "[progress]" not in text.lower():
                continue
            m = _PERCENT_ANYWHERE.search(text)
        if not m:
            continue
        try:
            val = max(0.0, min(100.0, float(m.group(1))))
        except ValueError:
            continue
        if best is None or val > best:
            best = val
    return best


def enrich_step_state(
    step_state: Optional[Dict[str, Any]],
    log_lines: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    state = deepcopy(step_state) if isinstance(step_state, dict) else default_step_state()
    for name in _STEP_NAMES:
        if name not in state or not isinstance(state.get(name), dict):
            state[name] = {"status": "pending", "percent": None}
    logs = log_lines or []
    for key in _STEP_NAMES:
        slot = state[key]
        st = (slot.get("status") or "pending").lower()
        if st != "running":
            continue
        log_pct = latest_percent_from_logs(logs, key)
        if log_pct is None:
            continue
        cur = slot.get("percent")
        try:
            cur_f = float(cur) if cur is not None else None
        except (TypeError, ValueError):
            cur_f = None
        if cur_f is None or log_pct > cur_f:
            slot["percent"] = log_pct
    return state


def format_step_chips(step_state: Optional[Dict[str, Any]]) -> str:
    step_state = step_state or {}
    parts: List[str] = []
    labels = {"backup": "Backup", "upload": "Upload", "restore": "Restore"}
    for key in _STEP_NAMES:
        slot = step_state.get(key) if isinstance(step_state.get(key), dict) else {}
        st = (slot.get("status") or "pending").lower()
        pct = slot.get("percent")
        label = labels[key]
        if st == "success":
            parts.append(f"{label}:OK")
        elif st == "failed":
            parts.append(f"{label}:FAIL")
        elif st == "skipped":
            parts.append(f"{label}:skip")
        elif st == "running":
            if pct is not None:
                parts.append(f"{label}:{int(round(float(pct)))}%")
            else:
                parts.append(f"{label}:…")
        else:
            parts.append(f"{label}:—")
    return " | ".join(parts)


def make_agent_ops_hooks(
    job: Dict[str, Any],
    *,
    include_history_log: bool = True,
) -> Tuple[LogFn, ProgressFn]:
    """Return log + progress callbacks that update AgentOps run history for this job."""
    jid = (job.get("job_id") or "").strip()

    def log(msg: str) -> None:
        if not jid or not include_history_log:
            return
        try:
            try:
                from gui.utils.run_history import append_run_history_log
            except ImportError:
                from azure_migration_tool.gui.utils.run_history import append_run_history_log
            append_run_history_log(jid, str(msg))
        except Exception:
            pass

    def progress_cb(database: str, step: str, status: str, detail: str = "") -> None:
        if not jid:
            return
        percent = parse_percent(detail) if (status or "").lower() == "running" else None
        try:
            try:
                from gui.utils.run_history import update_run_history_step
            except ImportError:
                from azure_migration_tool.gui.utils.run_history import update_run_history_step
            update_run_history_step(
                jid,
                step,
                status,
                detail=detail,
                percent=percent,
                database=database,
            )
        except Exception:
            pass

    return log, progress_cb


def chain_log(base: LogFn, extra: LogFn) -> LogFn:
    def combined(msg: str) -> None:
        base(msg)
        extra(msg)

    return combined
