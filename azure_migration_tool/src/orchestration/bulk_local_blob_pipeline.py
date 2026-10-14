# Author: Satish Chauhan

"""
Headless bulk workflow: local BACKUP TO DISK (multiple DBs) → upload to Blob → restore on target.

Mirrors Tools → Local Backup (multiple databases) steps 1–3 without the GUI.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .blob_job_utils import (
    is_azure_sql_managed_instance_host,
    merge_blob_config,
    resolve_stripes_value,
    resolve_max_parallel_uploads,
)

logger = logging.getLogger(__name__)

LogFn = Callable[[str], None]
PipelineProgressFn = Callable[[str, str, str, str], None]
RunSpec = Dict[str, Any]


def _default_log(msg: str) -> None:
    logger.info("%s", msg)


def _import_backup_upload():
    try:
        from ..backup.local_backup_and_upload import (
            build_blob_upload_path,
            run_blob_upload_preflight,
            run_local_backup_and_upload,
            upload_existing_bak_files_parallel,
        )
    except ImportError:
        from azure_migration_tool.src.backup.local_backup_and_upload import (
            build_blob_upload_path,
            run_blob_upload_preflight,
            run_local_backup_and_upload,
            upload_existing_bak_files_parallel,
        )
    return (
        build_blob_upload_path,
        run_blob_upload_preflight,
        run_local_backup_and_upload,
        upload_existing_bak_files_parallel,
    )


def _import_restore():
    try:
        from ..restore.restore_from_blob import run_restore_from_blob
    except ImportError:
        from azure_migration_tool.src.restore.restore_from_blob import run_restore_from_blob
    return run_restore_from_blob


def _import_list_databases():
    try:
        from gui.utils.database_utils import list_databases
    except ImportError:
        from azure_migration_tool.gui.utils.database_utils import list_databases
    return list_databases


def _resolve_pipeline_parallelism(job: Dict[str, Any], database_count: int) -> int:
    pipe = job.get("pipeline") or {}
    if pipe.get("phase_batches") or job.get("phase_batches"):
        return 0
    raw = pipe.get("max_parallel_databases") or job.get("max_parallel_databases")
    if raw is None:
        return max(1, min(4, database_count))
    s = str(raw).strip().lower()
    if s in ("max", "all"):
        return max(1, database_count)
    try:
        return max(1, min(database_count, int(raw)))
    except ValueError:
        return max(1, min(4, database_count))


def backup_single_database(
    *,
    server: str,
    database: str,
    local_backup_path: str,
    auth: str = "windows",
    user: str = "",
    password: str = "",
    compression: bool = True,
    structured_local_paths: bool = True,
    stripes: Optional[int] = None,
    cancel_event: Optional[threading.Event] = None,
    log: Optional[LogFn] = None,
) -> Tuple[Optional[RunSpec], Optional[str]]:
    """Backup one database; returns (run spec, error message)."""
    log = log or _default_log
    _, _, run_local_backup_and_upload, _ = _import_backup_upload()
    if cancel_event and cancel_event.is_set():
        return None, "cancelled"
    try:
        result = run_local_backup_and_upload(
            server=server,
            database=database,
            auth=auth,
            user=user,
            password=password,
            local_backup_path=local_backup_path,
            compression=compression,
            skip_upload=True,
            structured_local_paths=structured_local_paths,
            stripes=stripes,
            cancel_event=cancel_event,
            log=log,
        )
    except Exception as exc:
        return None, str(exc)

    if result.get("cancelled"):
        return None, "cancelled"
    if not result.get("success"):
        return None, result.get("message") or "Unknown backup error"

    files = result.get("local_files") or ([result.get("local_file", "")])
    files = [f for f in files if f]
    if not files:
        return None, "No backup files produced."

    run: RunSpec = {
        "database": database,
        "files": files,
        "run_id": (result.get("run_id") or "").strip(),
    }
    try:
        from gui.utils.local_backup_settings import save_stripe_preference
    except ImportError:
        from azure_migration_tool.gui.utils.local_backup_settings import save_stripe_preference
    mode = "auto" if stripes is None else "manual"
    save_stripe_preference(
        server,
        database,
        mode=mode,
        stripes_value=str(stripes) if stripes is not None else "Auto",
        last_auto_stripes=int(result.get("stripes") or 1),
    )
    return run, None


def backup_runs_from_spec(
    *,
    server: str,
    databases: List[str],
    local_backup_path: str,
    auth: str = "windows",
    user: str = "",
    password: str = "",
    compression: bool = True,
    structured_local_paths: bool = True,
    stripes: Optional[int] = None,
    cancel_event: Optional[threading.Event] = None,
    log: Optional[LogFn] = None,
) -> Dict[str, Any]:
    """Step 1: backup each database locally (skip_upload=True)."""
    log = log or _default_log
    ok_count = 0
    fail_count = 0
    runs: List[RunSpec] = []
    errors: List[str] = []
    total = len(databases)

    for idx, database in enumerate(databases, start=1):
        if cancel_event and cancel_event.is_set():
            log("Cancelled before backup.")
            break
        log(f"--- [{idx}/{total}] backup {database} ---")
        run, err = backup_single_database(
            server=server,
            database=database,
            local_backup_path=local_backup_path,
            auth=auth,
            user=user,
            password=password,
            compression=compression,
            structured_local_paths=structured_local_paths,
            stripes=stripes,
            cancel_event=cancel_event,
            log=log,
        )
        if err == "cancelled":
            log("Backup cancelled.")
            break
        if run:
            ok_count += 1
            runs.append(run)
        else:
            fail_count += 1
            msg = f"{database}: {err or 'backup failed'}"
            errors.append(msg)
            log(f"Failed: {msg}")

    status = "success" if fail_count == 0 and ok_count > 0 else "failed"
    if ok_count and fail_count:
        status = "partial"
    if ok_count == 0 and fail_count:
        status = "failed"
    return {
        "status": status,
        "ok": ok_count,
        "failed": fail_count,
        "runs": runs,
        "errors": errors,
    }


def upload_single_backup_run(
    spec: RunSpec,
    storage: Dict[str, Any],
    *,
    cancel_event: Optional[threading.Event] = None,
    log: Optional[LogFn] = None,
) -> Tuple[bool, Optional[str]]:
    """Upload one database's backup files. Returns (success, error)."""
    log = log or _default_log
    _, run_blob_upload_preflight, _, upload_existing_bak_files_parallel = _import_backup_upload()

    database = (spec.get("database") or "").strip()
    run_id = (spec.get("run_id") or "").strip()
    run_files = [f for f in (spec.get("files") or []) if f and str(f).strip()]
    if not run_files:
        return False, "no files"

    blob_auth_mode = storage["blob_auth_mode"]
    conn_str = storage.get("conn_str") or ""
    storage_account_url = storage.get("storage_account_url") or ""
    container = storage["container"]
    blob_folder = storage.get("blob_folder") or ""
    structured_blob_paths = bool(storage.get("structured_blob_paths", True))
    delete_local = bool(storage.get("delete_local_after_upload", False))
    max_parallel = resolve_max_parallel_uploads(storage.get("max_parallel_uploads"))

    pre = run_blob_upload_preflight(
        run_files,
        blob_auth_mode=blob_auth_mode,
        blob_connection_string=conn_str,
        blob_account_url=storage_account_url,
        blob_container=container,
        log=log,
    )
    if not pre.get("ok"):
        return False, pre.get("message") or "Upload preflight failed."

    readable = {os.path.normpath(os.path.expanduser(p)) for p in (pre.get("files") or [])}
    run_files = [
        p for p in run_files if os.path.normpath(os.path.expanduser(p)) in readable
    ]
    if not run_files:
        return False, "no readable files"

    if cancel_event and cancel_event.is_set():
        return False, "cancelled"

    batch = upload_existing_bak_files_parallel(
        local_file_paths=run_files,
        max_parallel_uploads=max_parallel,
        cancel_event=cancel_event,
        blob_auth_mode=blob_auth_mode,
        blob_connection_string=conn_str,
        blob_account_url=storage_account_url,
        blob_container=container,
        blob_folder=blob_folder,
        database=database,
        run_id=run_id,
        structured_blob_paths=structured_blob_paths,
        delete_local_after_upload=delete_local,
        require_azcopy=True,
        log=log,
    )
    uploaded = int(batch.get("uploaded", 0))
    failed = int(batch.get("failed", 0))
    if failed or uploaded == 0:
        return False, f"upload failed ({failed} failed)"
    return True, None


def upload_backup_runs(
    runs: List[RunSpec],
    storage: Dict[str, Any],
    *,
    cancel_event: Optional[threading.Event] = None,
    log: Optional[LogFn] = None,
) -> Dict[str, Any]:
    """Step 2: upload completed local backup runs to blob."""
    log = log or _default_log
    (
        _build_blob_upload_path,
        run_blob_upload_preflight,
        _run_local,
        upload_existing_bak_files_parallel,
    ) = _import_backup_upload()

    normalized_runs: List[RunSpec] = []
    for spec in runs or []:
        files = [f for f in (spec.get("files") or []) if f and str(f).strip()]
        if not files:
            continue
        normalized_runs.append(
            {
                "database": (spec.get("database") or "").strip(),
                "files": files,
                "run_id": (spec.get("run_id") or "").strip(),
            }
        )
    if not normalized_runs:
        return {
            "status": "failed",
            "uploaded": 0,
            "failed": 0,
            "errors": ["No backup files to upload."],
        }

    blob_auth_mode = storage["blob_auth_mode"]
    conn_str = storage.get("conn_str") or ""
    storage_account_url = storage.get("storage_account_url") or ""
    container = storage["container"]
    blob_folder = storage.get("blob_folder") or ""
    structured_blob_paths = bool(storage.get("structured_blob_paths", True))
    delete_local = bool(storage.get("delete_local_after_upload", False))
    max_parallel = resolve_max_parallel_uploads(storage.get("max_parallel_uploads"))

    all_paths: List[str] = []
    for spec in normalized_runs:
        all_paths.extend(spec["files"])

    pre = run_blob_upload_preflight(
        all_paths,
        blob_auth_mode=blob_auth_mode,
        blob_connection_string=conn_str,
        blob_account_url=storage_account_url,
        blob_container=container,
        log=log,
    )
    if not pre.get("ok"):
        err = pre.get("message") or "Upload preflight failed."
        return {"status": "failed", "uploaded": 0, "failed": len(all_paths), "errors": [err]}

    readable = {
        os.path.normpath(os.path.expanduser(p)) for p in (pre.get("files") or [])
    }

    uploaded = 0
    failed = 0
    errors: List[str] = []
    last_url = ""
    total_runs = len(normalized_runs)

    log(f"=== Uploading {len(readable)} file(s) across {total_runs} database(s) ===")

    for run_idx, spec in enumerate(normalized_runs, start=1):
        if cancel_event and cancel_event.is_set():
            log("Upload cancelled.")
            break
        database = spec["database"]
        run_id = spec["run_id"]
        run_files = [
            p
            for p in spec["files"]
            if os.path.normpath(os.path.expanduser(p)) in readable
        ]
        if not run_files:
            log(f"--- [{run_idx}/{total_runs}] {database}: skipped (no readable files) ---")
            failed += len(spec["files"])
            continue
        log(f"--- [{run_idx}/{total_runs}] {database}: {len(run_files)} file(s) ---")
        ok, err = upload_single_backup_run(
            spec,
            storage,
            cancel_event=cancel_event,
            log=log,
        )
        if ok:
            uploaded += len(run_files)
        else:
            failed += len(run_files)
            if err:
                errors.append(f"{database}: {err}")

    status = "success" if failed == 0 and uploaded > 0 else "failed"
    if uploaded and failed:
        status = "partial"
    return {
        "status": status,
        "uploaded": uploaded,
        "failed": failed,
        "last_blob_url": last_url,
        "runs": normalized_runs,
        "errors": errors,
    }


def restore_jobs_from_runs(
    runs: List[RunSpec],
    storage: Dict[str, Any],
) -> List[Dict[str, str]]:
    build_blob_upload_path, _, _, _ = _import_backup_upload()
    blob_folder = storage.get("blob_folder") or ""
    structured = bool(storage.get("structured_blob_paths", True))
    jobs: List[Dict[str, str]] = []
    for spec in runs or []:
        database = (spec.get("database") or "").strip()
        files = [f for f in (spec.get("files") or []) if f]
        if not database or not files:
            continue
        blob_path = build_blob_upload_path(
            local_filename=Path(files[0]).name,
            blob_folder=blob_folder,
            database=database,
            run_id=(spec.get("run_id") or "").strip(),
            structured_layout=structured,
        )
        jobs.append({"database": database, "blob_path": blob_path})
    return jobs


def restore_single_database_from_blob(
    database: str,
    blob_path: str,
    storage: Dict[str, Any],
    target: Dict[str, Any],
    *,
    cancel_event: Optional[threading.Event] = None,
    log: Optional[LogFn] = None,
) -> Tuple[bool, Optional[str]]:
    log = log or _default_log
    run_restore_from_blob = _import_restore()
    server = (target.get("server") or "").strip()
    if not server:
        return False, "restore_target.server is required."

    auth = (target.get("auth") or "windows").strip()
    user = target.get("user") or None
    password = target.get("password") or None
    target_mi = target.get("target_managed_instance")
    if target_mi is None:
        target_mi = is_azure_sql_managed_instance_host(server)
    else:
        target_mi = bool(target_mi)

    if cancel_event and cancel_event.is_set():
        return False, "cancelled"

    try:
        summary = run_restore_from_blob(
            server=server,
            database=database,
            auth=auth,
            user=user,
            password=password,
            blob_connection_string=storage.get("conn_str") or "",
            container=storage["container"],
            blob_path=blob_path,
            log_callback=log,
            target_managed_instance=target_mi,
            blob_auth_mode=storage["blob_auth_mode"],
            storage_account_url=storage.get("storage_account_url") or "",
            cancel_event=cancel_event,
        )
    except Exception as exc:
        return False, str(exc)

    status = (summary or {}).get("status", "failed")
    if status in ("success", "client_lost"):
        return True, None
    return False, (summary or {}).get("error") or "Unknown error"


def restore_databases_from_blob(
    jobs: List[Dict[str, str]],
    storage: Dict[str, Any],
    target: Dict[str, Any],
    *,
    cancel_event: Optional[threading.Event] = None,
    log: Optional[LogFn] = None,
) -> Dict[str, Any]:
    """Step 3: sequential RESTORE FROM URL for each database."""
    log = log or _default_log
    server = (target.get("server") or "").strip()
    if not server:
        return {
            "status": "failed",
            "ok": 0,
            "failed": 0,
            "errors": ["restore_target.server is required."],
        }

    normalized: List[Dict[str, str]] = []
    for job in jobs or []:
        database = (job.get("database") or "").strip()
        blob_path = (job.get("blob_path") or "").strip()
        if database and blob_path:
            normalized.append({"database": database, "blob_path": blob_path})

    if not normalized:
        return {
            "status": "failed",
            "ok": 0,
            "failed": 0,
            "errors": ["No restore jobs (database + blob_path)."],
        }

    container = storage["container"]

    ok_count = 0
    fail_count = 0
    errors: List[str] = []
    total = len(normalized)

    log(f"=== Restore from Blob: {total} database(s) → {server} ===")
    for idx, job in enumerate(normalized, start=1):
        if cancel_event and cancel_event.is_set():
            log("Restore cancelled.")
            break
        database = job["database"]
        blob_path = job["blob_path"]
        log(f"--- [{idx}/{total}] {database} ---")
        log(f"  Blob: {container}/{blob_path}")
        ok, err = restore_single_database_from_blob(
            database,
            blob_path,
            storage,
            target,
            cancel_event=cancel_event,
            log=log,
        )
        if ok:
            ok_count += 1
            log(f"OK {database}: restore finished")
        else:
            fail_count += 1
            errors.append(f"{database}: {err or 'restore failed'}")
            log(f"FAIL {database}: {err}")

    st = "success" if fail_count == 0 and ok_count > 0 else "failed"
    if ok_count and fail_count:
        st = "partial"
    return {"status": st, "ok": ok_count, "failed": fail_count, "errors": errors}


def _aggregate_pipeline_results(
    per_db: List[Dict[str, Any]],
    *,
    do_backup: bool,
    do_upload: bool,
    do_restore: bool,
) -> Tuple[List[RunSpec], Dict[str, Any], List[str]]:
    runs: List[RunSpec] = []
    errors: List[str] = []
    for row in per_db:
        errors.extend(row.get("errors") or [])
        run = row.get("run")
        if isinstance(run, dict) and run.get("files"):
            runs.append(run)

    def _count(step: str) -> Tuple[int, int]:
        ok = sum(1 for r in per_db if r.get(step) == "success")
        fail = sum(1 for r in per_db if r.get(step) == "failed")
        return ok, fail

    steps: Dict[str, Any] = {"pipeline": {"mode": "per_database", "databases": per_db}}
    if do_backup:
        ok, fail = _count("backup")
        st = "success" if fail == 0 and ok else ("partial" if ok and fail else "failed")
        steps["backup"] = {"status": st, "ok": ok, "failed": fail, "runs": runs}
    if do_upload:
        ok, fail = _count("upload")
        st = "success" if fail == 0 and ok else ("partial" if ok and fail else "failed")
        steps["upload"] = {"status": st, "uploaded": ok, "failed": fail, "runs": runs}
    if do_restore:
        ok, fail = _count("restore")
        st = "success" if fail == 0 and ok else ("partial" if ok and fail else "failed")
        steps["restore"] = {"status": st, "ok": ok, "failed": fail}
    return runs, steps, errors


def _process_database_pipeline(
    database: str,
    initial_run: Optional[RunSpec],
    *,
    server: str,
    auth: str,
    user: str,
    password: str,
    local_path: str,
    local_cfg: Dict[str, Any],
    stripes: Optional[int],
    storage_dict: Dict[str, Any],
    target: Dict[str, Any],
    do_backup: bool,
    do_upload: bool,
    do_restore: bool,
    cancel_event: Optional[threading.Event],
    log: LogFn,
    log_lock: threading.Lock,
    progress_callback: Optional[PipelineProgressFn] = None,
) -> Dict[str, Any]:
    def tlog(msg: str) -> None:
        with log_lock:
            log(f"[{database}] {msg}")

    def progress(step: str, status: str, detail: str = "") -> None:
        if progress_callback:
            progress_callback(database, step, status, detail)

    def skip_remaining_after_failure() -> None:
        if progress_callback:
            if do_upload:
                progress("upload", "skipped", "")
            if do_restore:
                progress("restore", "skipped", "")

    row: Dict[str, Any] = {"database": database, "errors": []}
    run: Optional[RunSpec] = initial_run

    if do_backup:
        progress("backup", "running", "backup to disk")
        tlog("Step 1/3 backup started")
        run, err = backup_single_database(
            server=server,
            database=database,
            local_backup_path=local_path,
            auth=auth,
            user=user,
            password=password,
            compression=bool(local_cfg.get("compression", True)),
            structured_local_paths=bool(
                local_cfg.get("structured_paths", local_cfg.get("structured_local_paths", True))
            ),
            stripes=stripes,
            cancel_event=cancel_event,
            log=tlog,
        )
        if not run:
            row["backup"] = "failed"
            row["errors"].append(err or "backup failed")
            progress("backup", "failed", err or "backup failed")
            skip_remaining_after_failure()
            return row
        row["backup"] = "success"
        progress("backup", "success", "backup complete")
        tlog("Step 1/3 backup complete")
    elif run:
        row["backup"] = "skipped"
        progress("backup", "skipped", "using existing run")
    else:
        row["backup"] = "failed"
        row["errors"].append("no backup run for database")
        progress("backup", "failed", "no backup run")
        skip_remaining_after_failure()
        return row

    row["run"] = run

    if do_upload:
        progress("upload", "running", "upload to blob")
        tlog("Step 2/3 upload started")
        ok, err = upload_single_backup_run(
            run,
            storage_dict,
            cancel_event=cancel_event,
            log=tlog,
        )
        row["upload"] = "success" if ok else "failed"
        if not ok:
            row["errors"].append(err or "upload failed")
            progress("upload", "failed", err or "upload failed")
            if do_restore:
                progress("restore", "skipped", "")
            return row
        progress("upload", "success", "upload complete")
        tlog("Step 2/3 upload complete")

    if do_restore:
        restore_jobs = restore_jobs_from_runs([run], storage_dict)
        if not restore_jobs:
            row["restore"] = "failed"
            row["errors"].append("could not build blob path for restore")
            progress("restore", "failed", "no blob path")
            return row
        blob_path = restore_jobs[0]["blob_path"]
        progress("restore", "running", blob_path)
        tlog(f"Step 3/3 restore started ({blob_path})")
        ok, err = restore_single_database_from_blob(
            database,
            blob_path,
            storage_dict,
            target,
            cancel_event=cancel_event,
            log=tlog,
        )
        row["restore"] = "success" if ok else "failed"
        if ok:
            progress("restore", "success", "restore complete")
            tlog("Step 3/3 restore complete")
        else:
            row["errors"].append(err or "restore failed")
            progress("restore", "failed", err or "restore failed")

    return row


def _execute_database_pipelines(
    work_items: List[Tuple[str, Optional[RunSpec]]],
    *,
    job: Dict[str, Any],
    server: str,
    auth: str,
    user: str,
    password: str,
    local_path: str,
    local_cfg: Dict[str, Any],
    stripes: Optional[int],
    storage_dict: Dict[str, Any],
    target: Dict[str, Any],
    do_backup: bool,
    do_upload: bool,
    do_restore: bool,
    cancel_event: Optional[threading.Event],
    log: LogFn,
    progress_callback: Optional[PipelineProgressFn] = None,
) -> Tuple[List[RunSpec], Dict[str, Any], List[str]]:
    max_workers = _resolve_pipeline_parallelism(job, len(work_items))
    log_lock = threading.Lock()
    log(f"=== Per-database pipeline: {len(work_items)} DB(s), up to {max_workers} in parallel ===")

    per_db: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _process_database_pipeline,
                database,
                initial_run,
                server=server,
                auth=auth,
                user=user,
                password=password,
                local_path=local_path,
                local_cfg=local_cfg,
                stripes=stripes,
                storage_dict=storage_dict,
                target=target,
                do_backup=do_backup,
                do_upload=do_upload,
                do_restore=do_restore,
                cancel_event=cancel_event,
                log=log,
                log_lock=log_lock,
                progress_callback=progress_callback,
            ): database
            for database, initial_run in work_items
        }
        for fut in as_completed(futures):
            db = futures[fut]
            try:
                per_db.append(fut.result())
            except Exception as exc:
                per_db.append(
                    {"database": db, "errors": [str(exc)], "backup": "failed"}
                )

    per_db.sort(key=lambda r: r.get("database") or "")
    return _aggregate_pipeline_results(
        per_db, do_backup=do_backup, do_upload=do_upload, do_restore=do_restore
    )


def _apply_organization_defaults(job: Dict[str, Any]) -> None:
    try:
        from gui.utils.org_settings import load_org_settings
    except ImportError:
        from azure_migration_tool.gui.utils.org_settings import load_org_settings
    org = load_org_settings()
    local = job.setdefault("local_backup", {})
    if not (local.get("path") or local.get("local_backup_path") or "").strip():
        unc = (org.get("default_backup_unc_path") or "").strip()
        if unc:
            local["path"] = unc
    blob = job.setdefault("blob", {})
    if not (blob.get("folder") or blob.get("blob_folder") or "").strip():
        prefix = (org.get("default_blob_folder_prefix") or "").strip()
        if prefix:
            blob["folder"] = prefix
    if not (job.get("project_path") or "").strip():
        pp = (org.get("default_project_path") or "").strip()
        if pp:
            job["project_path"] = pp
            out = job.setdefault("output", {})
            if isinstance(out, dict) and not (out.get("project_path") or "").strip():
                out["project_path"] = pp


def run_bulk_local_backup_blob_restore_job(
    job: Dict[str, Any],
    *,
    log: Optional[LogFn] = None,
    cancel_event: Optional[threading.Event] = None,
    progress_callback: Optional[PipelineProgressFn] = None,
) -> Dict[str, Any]:
    """
    Execute a job document (see examples/jobs/bulk_local_blob_restore.example.json).

    Steps are controlled by job["steps"] or skip_backup / skip_upload / skip_restore flags.
    """
    log = log or _default_log
    if "_effective" not in job:
        try:
            from .job_defaults import prepare_job_with_defaults
        except ImportError:
            from azure_migration_tool.src.orchestration.job_defaults import prepare_job_with_defaults
        merged = prepare_job_with_defaults(job)
        job.clear()
        job.update(merged)
    job_id = (job.get("job_id") or "").strip() or str(uuid.uuid4())
    started = datetime.now(timezone.utc).isoformat()

    steps_cfg = job.get("steps") or {}
    do_backup = bool(steps_cfg.get("backup", not job.get("skip_backup", False)))
    do_upload = bool(steps_cfg.get("upload", not job.get("skip_upload", False)))
    do_restore = bool(steps_cfg.get("restore", not job.get("skip_restore", False)))

    result: Dict[str, Any] = {
        "job_id": job_id,
        "operation": job.get("operation") or "bulk_local_backup_blob_restore",
        "started_utc": started,
        "ended_utc": None,
        "status": "started",
        "steps": {},
        "errors": [],
        "backup_runs": [],
    }

    runs: List[RunSpec] = list(job.get("backup_runs") or [])

    source = job.get("source") or {}
    server = (source.get("server") or "").strip()
    auth = (source.get("auth") or "windows").strip()
    user = source.get("user") or ""
    password = source.get("password") or ""

    local_cfg = job.get("local_backup") or {}
    local_path = (local_cfg.get("path") or local_cfg.get("local_backup_path") or "").strip()

    storage_dict: Dict[str, Any] = {}
    if do_upload or do_restore:
        storage_dict, blob_err = merge_blob_config(job.get("blob") or {})
        if blob_err:
            result["status"] = "failed"
            result["errors"].append(blob_err)
            result["ended_utc"] = datetime.now(timezone.utc).isoformat()
            _try_record_run_history(result, job)
            return result

    try:
        databases: List[str] = []
        if do_backup:
            databases = list(source.get("databases") or [])
            if not databases and source.get("list_all_databases"):
                list_databases = _import_list_databases()
                databases = list_databases(
                    server=server,
                    auth=auth,
                    user=user,
                    password=password or None,
                    timeout=int(source.get("timeout", 30)),
                )
            if not server:
                raise ValueError("source.server is required for backup.")
            if not local_path:
                raise ValueError("local_backup.path is required for backup.")
            if not databases:
                raise ValueError("source.databases is empty (set list_all_databases or name databases).")

        stripes = resolve_stripes_value(local_cfg.get("stripes", "auto"))
        work_items: List[Tuple[str, Optional[RunSpec]]] = []
        if do_backup:
            work_items = [(db, None) for db in databases]
        elif runs:
            for spec in runs:
                db = (spec.get("database") or "").strip()
                if db:
                    work_items.append((db, spec))
        elif do_upload or do_restore:
            raise ValueError("No backup runs available for upload/restore.")

        use_pipeline = work_items and _resolve_pipeline_parallelism(job, len(work_items)) > 0
        if use_pipeline and len(work_items) >= 1:
            runs, step_parts, pipe_errors = _execute_database_pipelines(
                work_items,
                job=job,
                server=server,
                auth=auth,
                user=user,
                password=password,
                local_path=local_path,
                local_cfg=local_cfg,
                stripes=stripes,
                storage_dict=storage_dict,
                target=job.get("restore_target") or {},
                do_backup=do_backup,
                do_upload=do_upload,
                do_restore=do_restore,
                cancel_event=cancel_event,
                log=log,
                progress_callback=progress_callback,
            )
            result["steps"].update(step_parts)
            result["backup_runs"] = runs
            result["errors"].extend(pipe_errors)
        else:
            if do_backup:
                backup_out = backup_runs_from_spec(
                    server=server,
                    databases=databases,
                    local_backup_path=local_path,
                    auth=auth,
                    user=user,
                    password=password,
                    compression=bool(local_cfg.get("compression", True)),
                    structured_local_paths=bool(
                        local_cfg.get("structured_paths", local_cfg.get("structured_local_paths", True))
                    ),
                    stripes=stripes,
                    cancel_event=cancel_event,
                    log=log,
                )
                result["steps"]["backup"] = backup_out
                runs = backup_out.get("runs") or []
                result["backup_runs"] = runs
                result["errors"].extend(backup_out.get("errors") or [])
                if backup_out.get("status") == "failed" and do_upload:
                    log("Skipping upload/restore because backup had no successful runs.")
                    do_upload = False
                    do_restore = False
            elif runs:
                result["backup_runs"] = runs
                result["steps"]["backup"] = {"status": "skipped", "runs": runs}

            if do_upload:
                if not runs:
                    raise ValueError("No backup runs available for upload.")
                upload_out = upload_backup_runs(
                    runs,
                    storage_dict,
                    cancel_event=cancel_event,
                    log=log,
                )
                result["steps"]["upload"] = upload_out
                runs = upload_out.get("runs") or runs
                result["backup_runs"] = runs
                result["errors"].extend(upload_out.get("errors") or [])
                if upload_out.get("status") == "failed":
                    do_restore = False

            if do_restore:
                if not runs:
                    raise ValueError("No backup runs available for restore.")
                jobs = restore_jobs_from_runs(runs, storage_dict)
                restore_out = restore_databases_from_blob(
                    jobs,
                    storage_dict,
                    job.get("restore_target") or {},
                    cancel_event=cancel_event,
                    log=log,
                )
                result["steps"]["restore"] = restore_out
                result["errors"].extend(restore_out.get("errors") or [])

        step_statuses = [
            s.get("status")
            for s in result["steps"].values()
            if isinstance(s, dict) and s.get("status") not in (None, "skipped")
        ]
        if not step_statuses:
            result["status"] = "failed"
            result["errors"].append("No steps were executed.")
        elif all(s == "success" for s in step_statuses):
            result["status"] = "success"
        elif any(s == "failed" for s in step_statuses) and not any(
            s in ("success", "partial") for s in step_statuses
        ):
            result["status"] = "failed"
        elif any(s in ("partial", "failed") for s in step_statuses):
            result["status"] = "completed_with_errors"
        else:
            result["status"] = "success"

    except Exception as exc:
        result["status"] = "failed"
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        log(f"Job failed: {exc}")

    result["ended_utc"] = datetime.now(timezone.utc).isoformat()
    result["environment"] = job.get("environment") or ""
    result["source_server"] = server

    output = job.get("output") or {}
    project_path = (output.get("project_path") or job.get("project_path") or "").strip()
    result_path = (output.get("result_path") or job.get("result_path") or "").strip()
    if not result_path and project_path:
        out_dir = Path(project_path) / "worker_jobs"
        out_dir.mkdir(parents=True, exist_ok=True)
        result_path = str(out_dir / f"{job_id}_result.json")
    if result_path:
        Path(result_path).parent.mkdir(parents=True, exist_ok=True)
        Path(result_path).write_text(json.dumps(result, indent=2), encoding="utf-8")
        result["result_path"] = result_path
        log(f"Wrote result: {result_path}")

    _try_record_run_history(result, job)
    return result


def _try_record_run_history(result: Dict[str, Any], job: Dict[str, Any]) -> None:
    try:
        try:
            from gui.utils.run_history import record_pipeline_run
        except ImportError:
            from azure_migration_tool.gui.utils.run_history import record_pipeline_run
        record_pipeline_run(
            result,
            job,
            origin=str(job.get("_run_origin") or "worker"),
        )
    except Exception:
        pass
