# Author: Satish Chauhan

"""
Run migration worker jobs from JSON (for Cursor, scripts, CI).

Usage:
  python -m azure_migration_tool.worker run --job path/to/job.json
  python -m azure_migration_tool.worker run --job job.json --result out.json
  python -m azure_migration_tool.worker validate --job job.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

# Repo layout: package lives under azure_migration_tool/
_PKG_ROOT = Path(__file__).resolve().parent.parent
_REPO_ROOT = _PKG_ROOT.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_job(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Job file must be a JSON object.")
    return data


def _prepare_job(job: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from azure_migration_tool.src.orchestration.job_defaults import prepare_job_with_defaults
    except ImportError:
        from src.orchestration.job_defaults import prepare_job_with_defaults
    prepared = prepare_job_with_defaults(job)
    job.clear()
    job.update(prepared)
    return job


def _validate_job(job: Dict[str, Any]) -> list[str]:
    issues: list[str] = []
    op = (job.get("operation") or "bulk_local_backup_blob_restore").strip()
    if op != "bulk_local_backup_blob_restore":
        issues.append(f"Unsupported operation: {op}")
    steps = job.get("steps") or {}
    do_backup = bool(steps.get("backup", not job.get("skip_backup", False)))
    if do_backup:
        src = job.get("source") or {}
        if not (src.get("server") or "").strip():
            issues.append("source.server is required when backup step is enabled.")
        local = job.get("local_backup") or {}
        if not (local.get("path") or local.get("local_backup_path") or "").strip():
            issues.append("local_backup.path is required when backup step is enabled.")
        if not (src.get("databases") or []) and not src.get("list_all_databases"):
            issues.append("source.databases or source.list_all_databases is required for backup.")
    do_upload = bool(steps.get("upload", not job.get("skip_upload", False)))
    do_restore = bool(steps.get("restore", not job.get("skip_restore", False)))
    if (do_upload or do_restore) and not (job.get("backup_runs") or do_backup):
        issues.append("backup_runs or backup step required for upload/restore.")
    if do_restore:
        tgt = job.get("restore_target") or {}
        if not (tgt.get("server") or "").strip():
            issues.append("restore_target.server is required when restore step is enabled.")
    return issues


def cmd_validate(args: argparse.Namespace) -> int:
    job_path = Path(args.job).expanduser().resolve()
    job = _prepare_job(_load_job(job_path))
    issues = _validate_job(job)
    if issues:
        for line in issues:
            print(f"[invalid] {line}", file=sys.stderr)
        return 2
    eff = job.get("_effective") or {}
    applied = eff.get("defaults_applied") or {}
    if applied:
        print("[ok] Job structure looks valid (defaults filled for missing fields).")
        print(json.dumps({"defaults_applied": applied}, indent=2))
    else:
        print("[ok] Job structure looks valid (all required fields from payload).")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    job_path = Path(args.job).expanduser().resolve()
    job = _prepare_job(_load_job(job_path))
    job["_run_origin"] = "cli"
    issues = _validate_job(job)
    if issues and not args.force:
        for line in issues:
            print(f"[invalid] {line}", file=sys.stderr)
        return 2

    if args.result:
        out = job.setdefault("output", {})
        if isinstance(out, dict):
            out["result_path"] = str(Path(args.result).expanduser().resolve())

    try:
        from azure_migration_tool.src.orchestration.bulk_local_blob_pipeline import (
            run_bulk_local_backup_blob_restore_job,
        )
    except ImportError:
        from src.orchestration.bulk_local_blob_pipeline import (
            run_bulk_local_backup_blob_restore_job,
        )

    def log(msg: str) -> None:
        text = str(msg).replace("\u2713", "[OK]").replace("\u2717", "[FAIL]")
        try:
            print(text, flush=True)
        except UnicodeEncodeError:
            print(text.encode("ascii", errors="replace").decode("ascii"), flush=True)

    summary = run_bulk_local_backup_blob_restore_job(job, log=log)
    if args.json_stdout:
        print(json.dumps(summary, indent=2))
    else:
        print(f"Status: {summary.get('status')}")
        if summary.get("result_path"):
            print(f"Result file: {summary['result_path']}")
        if summary.get("errors"):
            print("Errors:")
            for err in summary["errors"][:10]:
                print(f"  - {err}")

    status = (summary.get("status") or "failed").lower()
    if status == "success":
        return 0
    if status == "completed_with_errors":
        return 1
    return 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Azure Migration Tool headless worker (bulk local backup → blob → restore)."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Execute a job file")
    run_p.add_argument("--job", required=True, help="Path to job JSON")
    run_p.add_argument("--result", help="Write result JSON to this path")
    run_p.add_argument("--json-stdout", action="store_true", help="Print full result JSON to stdout")
    run_p.add_argument(
        "--force",
        action="store_true",
        help="Run even if validate would fail (not recommended)",
    )
    run_p.set_defaults(func=cmd_run)

    val_p = sub.add_parser("validate", help="Validate job JSON without running")
    val_p.add_argument("--job", required=True, help="Path to job JSON")
    val_p.set_defaults(func=cmd_validate)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
