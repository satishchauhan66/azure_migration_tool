# Author: Satish Chauhan

"""Import/export worker jobs from CSV or Excel (one row per database; rows grouped into jobs)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

CANONICAL_COLUMNS: Tuple[str, ...] = (
    "job_id",
    "environment",
    "step_backup",
    "step_upload",
    "step_restore",
    "source_server",
    "source_auth",
    "source_user",
    "source_password",
    "database",
    "databases",
    "list_all_databases",
    "local_backup_path",
    "local_compression",
    "local_structured_paths",
    "local_stripes",
    "blob_use_profile",
    "blob_use_saved_settings",
    "blob_folder",
    "blob_storage_account_url",
    "blob_container",
    "blob_auth_mode",
    "restore_server",
    "restore_auth",
    "restore_user",
    "restore_password",
    "restore_target_mi",
    "project_path",
    "result_path",
)

# Friendly headers in sample files map to canonical names (normalized: lower, non-alnum -> _)
_HEADER_ALIASES: Dict[str, str] = {
    "job_id": "job_id",
    "jobid": "job_id",
    "environment": "environment",
    "env": "environment",
    "profile": "environment",
    "step_backup": "step_backup",
    "backup": "step_backup",
    "step_upload": "step_upload",
    "upload": "step_upload",
    "step_restore": "step_restore",
    "restore": "step_restore",
    "source_server": "source_server",
    "source_sql_server": "source_server",
    "sql_server": "source_server",
    "server": "source_server",
    "source_auth": "source_auth",
    "source_user": "source_user",
    "source_password": "source_password",
    "database": "database",
    "db": "database",
    "database_name": "database",
    "databases": "databases",
    "list_all_databases": "list_all_databases",
    "local_backup_path": "local_backup_path",
    "backup_unc_path": "local_backup_path",
    "unc_backup_path": "local_backup_path",
    "local_compression": "local_compression",
    "local_structured_paths": "local_structured_paths",
    "local_stripes": "local_stripes",
    "blob_use_profile": "blob_use_profile",
    "blob_use_saved_settings": "blob_use_saved_settings",
    "blob_folder": "blob_folder",
    "blob_storage_account_url": "blob_storage_account_url",
    "storage_account_url": "blob_storage_account_url",
    "blob_container": "blob_container",
    "container": "blob_container",
    "blob_auth_mode": "blob_auth_mode",
    "restore_server": "restore_server",
    "target_server": "restore_server",
    "mi_server": "restore_server",
    "restore_auth": "restore_auth",
    "restore_user": "restore_user",
    "restore_password": "restore_password",
    "restore_target_mi": "restore_target_mi",
    "target_managed_instance": "restore_target_mi",
    "project_path": "project_path",
    "result_path": "result_path",
}


def _normalize_header(name: Any) -> str:
    s = str(name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return _HEADER_ALIASES.get(s, s)


def _parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    try:
        import pandas as pd

        if pd.isna(value):
            return None
    except Exception:
        pass
    s = str(value).strip().lower()
    if not s:
        return None
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return None


def _cell_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        import pandas as pd

        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _split_databases(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[;,|\n]+", raw)
    return [p.strip() for p in parts if p.strip()]


def _read_csv_stdlib(path: Path) -> List[Dict[str, str]]:
    import csv

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")
        rows: List[Dict[str, str]] = []
        for raw in reader:
            rows.append({str(k): _cell_str(v) for k, v in raw.items() if k})
        return rows


def _rows_to_canonical_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    if not rows:
        raise ValueError("Spreadsheet is empty.")
    canon_rows: List[Dict[str, str]] = []
    for raw in rows:
        mapped: Dict[str, str] = {c: "" for c in CANONICAL_COLUMNS}
        for key, val in raw.items():
            canon = _normalize_header(key)
            if canon in CANONICAL_COLUMNS and _cell_str(val):
                mapped[canon] = _cell_str(val)
        canon_rows.append(mapped)
    return canon_rows


def _read_dataframe(path: Path) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _rows_to_canonical_rows(_read_csv_stdlib(path))
    if suffix in (".xlsx", ".xls"):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas and openpyxl are required for Excel. Install: pip install pandas openpyxl"
            ) from None
        raw = pd.read_excel(path, dtype=str)
        rows = [{str(k): _cell_str(v) for k, v in rec.items()} for rec in raw.to_dict(orient="records")]
        return _rows_to_canonical_rows(rows)
    raise ValueError(f"Unsupported file type: {suffix} (use .csv, .xlsx, or .xls)")


def _canonicalize_dataframe(df: Any) -> Any:
    import pandas as pd

    if df is None or df.empty:
        raise ValueError("Spreadsheet is empty.")
    canon_to_series: Dict[str, Any] = {}
    for col in df.columns:
        canon = _normalize_header(col)
        if canon not in CANONICAL_COLUMNS:
            continue
        series = df[col].map(_cell_str)
        if canon in canon_to_series:
            prev = canon_to_series[canon]
            canon_to_series[canon] = prev.where(prev.str.strip() != "", series)
        else:
            canon_to_series[canon] = series
    n = len(df)
    return pd.DataFrame(
        {c: canon_to_series.get(c, pd.Series([""] * n)) for c in CANONICAL_COLUMNS}
    )


def _row_dict(row: Any) -> Dict[str, str]:
    return {col: _cell_str(row.get(col, "")) for col in CANONICAL_COLUMNS}


def _first_non_empty(rows: Sequence[Dict[str, str]], key: str) -> str:
    for row in rows:
        v = (row.get(key) or "").strip()
        if v:
            return v
    return ""


def _first_bool(rows: Sequence[Dict[str, str]], key: str, default: bool) -> bool:
    for row in rows:
        b = _parse_bool(row.get(key))
        if b is not None:
            return b
    return default


def _collect_databases(rows: Sequence[Dict[str, str]]) -> List[str]:
    names: List[str] = []
    seen = set()
    for row in rows:
        for token in _split_databases(row.get("databases") or ""):
            if token not in seen:
                seen.add(token)
                names.append(token)
        one = (row.get("database") or "").strip()
        if one and one not in seen:
            seen.add(one)
            names.append(one)
    return names


def _merge_rows_to_job(rows: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    if not rows:
        raise ValueError("No rows to merge.")
    job_id = _first_non_empty(rows, "job_id") or "imported-job"
    environment = _first_non_empty(rows, "environment")
    project = _first_non_empty(rows, "project_path")
    result_path = _first_non_empty(rows, "result_path")

    job: Dict[str, Any] = {
        "job_id": job_id,
        "operation": "bulk_local_backup_blob_restore",
        "steps": {
            "backup": _first_bool(rows, "step_backup", True),
            "upload": _first_bool(rows, "step_upload", True),
            "restore": _first_bool(rows, "step_restore", True),
        },
        "source": {
            "server": _first_non_empty(rows, "source_server"),
            "auth": _first_non_empty(rows, "source_auth") or "windows",
            "user": _first_non_empty(rows, "source_user"),
            "password": _first_non_empty(rows, "source_password"),
            "databases": _collect_databases(rows),
            "list_all_databases": _first_bool(rows, "list_all_databases", False),
        },
        "local_backup": {
            "path": _first_non_empty(rows, "local_backup_path"),
            "compression": _first_bool(rows, "local_compression", True),
            "structured_paths": _first_bool(rows, "local_structured_paths", True),
            "stripes": _first_non_empty(rows, "local_stripes") or "auto",
        },
        "blob": {},
        "restore_target": {
            "server": _first_non_empty(rows, "restore_server"),
            "auth": _first_non_empty(rows, "restore_auth") or "entra_mfa",
            "user": _first_non_empty(rows, "restore_user"),
            "password": _first_non_empty(rows, "restore_password"),
            "target_managed_instance": _first_bool(rows, "restore_target_mi", True),
        },
    }
    if environment:
        job["environment"] = environment

    blob_folder = _first_non_empty(rows, "blob_folder")
    blob_url = _first_non_empty(rows, "blob_storage_account_url")
    blob_container = _first_non_empty(rows, "blob_container")
    blob_auth = _first_non_empty(rows, "blob_auth_mode")
    use_profile = _parse_bool(rows[0].get("blob_use_profile"))
    use_saved = _parse_bool(rows[0].get("blob_use_saved_settings"))
    if use_profile is True:
        job["blob"]["use_profile"] = True
    elif use_profile is False:
        job["blob"]["use_profile"] = False
    if use_saved is not None:
        job["blob"]["use_saved_settings"] = use_saved
    elif not any((blob_url, blob_container, blob_auth)):
        job["blob"]["use_saved_settings"] = True
    if blob_folder:
        job["blob"]["folder"] = blob_folder
    if blob_url:
        job["blob"]["storage_account_url"] = blob_url
    if blob_container:
        job["blob"]["container"] = blob_container
    if blob_auth:
        job["blob"]["auth_mode"] = blob_auth

    if project:
        job["project_path"] = project
        job["output"] = {
            "project_path": project,
            "result_path": result_path or "",
        }
    elif result_path:
        job["output"] = {"result_path": result_path}

    return job


def canonical_rows_to_jobs(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Group rows by job_id + source_server; merge databases per group."""
    groups: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for row in rows:
        key = (row.get("job_id") or "", row.get("source_server") or "")
        groups.setdefault(key, []).append(row)
    return [_merge_rows_to_job(group) for group in groups.values()]


def dataframe_to_jobs(df: Any) -> List[Dict[str, Any]]:
    """Group rows by job_id + source_server; merge databases per group."""
    if isinstance(df, list):
        return canonical_rows_to_jobs(df)
    canon = _canonicalize_dataframe(df)
    groups: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for _, series in canon.iterrows():
        row = _row_dict(series)
        key = (row.get("job_id") or "", row.get("source_server") or "")
        groups.setdefault(key, []).append(row)
    return [_merge_rows_to_job(rows) for rows in groups.values()]


def read_job_spreadsheet(path: str | Path) -> List[Dict[str, Any]]:
    """Read CSV or Excel and return one or more worker job dicts."""
    p = Path(path).expanduser().resolve()
    if not p.is_file():
        raise FileNotFoundError(str(p))
    df = _read_dataframe(p)
    return dataframe_to_jobs(df)


def job_to_template_rows(job: Dict[str, Any]) -> List[Dict[str, str]]:
    """Flatten a job to one row per database (for export/template)."""
    src = job.get("source") or {}
    dbs = list(src.get("databases") or [])
    if not dbs:
        dbs = [""]
    local = job.get("local_backup") or {}
    blob = job.get("blob") or {}
    tgt = job.get("restore_target") or {}
    steps = job.get("steps") or {}
    out = job.get("output") or {}
    base = {
        "job_id": str(job.get("job_id") or ""),
        "environment": str(job.get("environment") or ""),
        "step_backup": str(steps.get("backup", True)),
        "step_upload": str(steps.get("upload", True)),
        "step_restore": str(steps.get("restore", True)),
        "source_server": str(src.get("server") or ""),
        "source_auth": str(src.get("auth") or ""),
        "source_user": str(src.get("user") or ""),
        "source_password": "",
        "database": "",
        "databases": "",
        "list_all_databases": str(src.get("list_all_databases", False)),
        "local_backup_path": str(local.get("path") or ""),
        "local_compression": str(local.get("compression", True)),
        "local_structured_paths": str(local.get("structured_paths", True)),
        "local_stripes": str(local.get("stripes") or "auto"),
        "blob_use_profile": str(blob.get("use_profile", "")),
        "blob_use_saved_settings": str(blob.get("use_saved_settings", "")),
        "blob_folder": str(blob.get("folder") or ""),
        "blob_storage_account_url": str(blob.get("storage_account_url") or ""),
        "blob_container": str(blob.get("container") or ""),
        "blob_auth_mode": str(blob.get("auth_mode") or ""),
        "restore_server": str(tgt.get("server") or ""),
        "restore_auth": str(tgt.get("auth") or ""),
        "restore_user": str(tgt.get("user") or ""),
        "restore_password": "",
        "restore_target_mi": str(tgt.get("target_managed_instance", True)),
        "project_path": str(job.get("project_path") or out.get("project_path") or ""),
        "result_path": str(out.get("result_path") or ""),
    }
    rows: List[Dict[str, str]] = []
    for db in dbs:
        row = dict(base)
        row["database"] = db
        rows.append(row)
    return rows


def write_job_spreadsheet(path: str | Path, jobs: Sequence[Dict[str, Any]], *, sample: bool = False) -> Path:
    """Write jobs to CSV or Excel (.csv / .xlsx)."""
    rows: List[Dict[str, str]] = []
    if sample:
        rows.append(_sample_row())
    else:
        for job in jobs:
            rows.extend(job_to_template_rows(job))
    if not rows:
        rows.append({c: "" for c in CANONICAL_COLUMNS})
    out = Path(path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.suffix.lower() == ".csv":
        import csv

        with out.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(CANONICAL_COLUMNS))
            writer.writeheader()
            writer.writerows(rows)
    elif out.suffix.lower() in (".xlsx", ".xls"):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas and openpyxl are required for Excel export. Install: pip install pandas openpyxl"
            ) from None
        pd.DataFrame(rows, columns=list(CANONICAL_COLUMNS)).to_excel(
            out, index=False, engine="openpyxl"
        )
    else:
        raise ValueError("Output path must end with .csv or .xlsx")
    return out


def _sample_row() -> Dict[str, str]:
    return {
        "job_id": "my-migration-job",
        "environment": "qa",
        "step_backup": "true",
        "step_upload": "true",
        "step_restore": "true",
        "source_server": r"SOURCE-SQL\INSTANCE",
        "source_auth": "windows",
        "source_user": "",
        "source_password": "",
        "database": "SampleApp_ss_sld_db22q",
        "databases": "",
        "list_all_databases": "false",
        "local_backup_path": "",
        "local_compression": "true",
        "local_structured_paths": "true",
        "local_stripes": "auto",
        "blob_use_profile": "true",
        "blob_use_saved_settings": "",
        "blob_folder": "",
        "blob_storage_account_url": "",
        "blob_container": "",
        "blob_auth_mode": "",
        "restore_server": "",
        "restore_auth": "entra_mfa",
        "restore_user": "you@example.com",
        "restore_password": "",
        "restore_target_mi": "true",
        "project_path": r"C:\MigrationProjects\my-run",
        "result_path": "",
    }


def bundled_template_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "examples" / "jobs"


def ensure_bundled_templates() -> Tuple[Path, Path]:
    """Create/update sample CSV and XLSX under examples/jobs if missing."""
    folder = bundled_template_dir()
    folder.mkdir(parents=True, exist_ok=True)
    csv_path = folder / "worker_job_template.csv"
    xlsx_path = folder / "worker_job_template.xlsx"
    write_job_spreadsheet(csv_path, [], sample=True)
    write_job_spreadsheet(xlsx_path, [], sample=True)
    return csv_path, xlsx_path
