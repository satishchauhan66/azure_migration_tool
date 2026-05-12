# Author: Satish Chauhan
"""
Estimate backup disk needs and read free space for Local Backup UI.

- Windows: uses shutil.disk_usage when possible; falls back to GetDiskFreeSpaceExW
  (helps some UNC cases where exists/list fails but quota API works).
- Backup size: prefers last full backup row in msdb (realistic .bak size); falls back
  to data-file logical size with compression heuristics.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional, Tuple

LogFn = Callable[[str], None]


def _noop_log(_: str) -> None:
    pass


def normalize_backup_dir_path(raw: str) -> str:
    """Strip quotes/whitespace; expand ~ on Windows; normalize slashes for UNC."""
    p = (raw or "").strip().strip('"').strip("'")
    p = os.path.expanduser(p)
    return p.replace("/", "\\")


def unc_share_root(unc_path: str) -> Optional[str]:
    """
    For \\\\server\\share\\a\\b return \\\\server\\share (first two path segments after \\\\).
    """
    p = unc_path.replace("/", "\\")
    if not p.startswith("\\\\"):
        return None
    rest = p[2:]
    parts = [x for x in rest.split("\\") if x]
    if len(parts) < 2:
        return None
    return "\\\\" + parts[0] + "\\" + parts[1]


def candidate_paths_for_disk_query(path: str) -> List[str]:
    """Ordered list of paths to try for free-space APIs."""
    p = normalize_backup_dir_path(path)
    out: List[str] = [p]
    root = unc_share_root(p)
    if root and root.lower() != p.lower():
        out.append(root)
    return out


def disk_usage_shutil(path: str) -> Optional[Tuple[int, int, int]]:
    """Returns (total_bytes, used_bytes, free_bytes) or None."""
    try:
        if not os.path.exists(path):
            return None
        import shutil

        t, u, f = shutil.disk_usage(path)
        return int(t), int(u), int(f)
    except OSError:
        return None


def disk_usage_windows_api(path: str) -> Optional[Tuple[int, int, int]]:
    """
    Use GetDiskFreeSpaceExW. Returns (total, used, free_available_to_caller) in bytes.
    """
    if os.name != "nt":
        return None
    try:
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        GetDiskFreeSpaceExW = kernel32.GetDiskFreeSpaceExW
        GetDiskFreeSpaceExW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.POINTER(ctypes.c_ulonglong),
            ctypes.POINTER(ctypes.c_ulonglong),
            ctypes.POINTER(ctypes.c_ulonglong),
        ]
        GetDiskFreeSpaceExW.restype = ctypes.c_int

        free_avail = ctypes.c_ulonglong(0)
        total_bytes = ctypes.c_ulonglong(0)
        total_free = ctypes.c_ulonglong(0)

        ok = GetDiskFreeSpaceExW(
            ctypes.c_wchar_p(path),
            ctypes.byref(free_avail),
            ctypes.byref(total_bytes),
            ctypes.byref(total_free),
        )
        if not ok:
            return None
        total = int(total_bytes.value)
        total_free_on_volume = int(total_free.value)
        free_to_caller = int(free_avail.value)
        used = max(0, total - total_free_on_volume)
        # Match shutil semantics: "free" is space available to this caller (quota-aware).
        return total, used, free_to_caller
    except Exception:
        return None


def best_disk_usage(path: str, log: LogFn = _noop_log) -> Tuple[Optional[Tuple[int, int, int]], str]:
    """
    Try multiple path variants and APIs. Returns ((total, used, free), method_label).
    """
    for cand in candidate_paths_for_disk_query(path):
        du = disk_usage_shutil(cand)
        if du:
            return du, f"shutil.disk_usage({cand!r})"
        du = disk_usage_windows_api(cand)
        if du:
            return du, f"GetDiskFreeSpaceExW({cand!r})"
        log(f"Tried {cand!r}: no free-space data from app host.")
    return None, "none"


def fetch_last_full_backup_stats(cur: Any, database: str) -> Optional[Dict[str, Any]]:
    """
    Read last successful full backup size from msdb (closest to next .bak size).
    """
    cur.execute(
        """
        SELECT TOP 1
            CAST(bs.backup_size / 1024.0 / 1024.0 AS DECIMAL(14,2)) AS backup_size_mb,
            CAST(bs.compressed_backup_size / 1024.0 / 1024.0 AS DECIMAL(14,2)) AS compressed_size_mb,
            bs.backup_finish_date,
            bmf.physical_device_name
        FROM msdb.dbo.backupset bs
        JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
        WHERE bs.database_name = ?
          AND bs.type = 'D'
          AND bs.backup_finish_date IS NOT NULL
        ORDER BY bs.backup_finish_date DESC
        """,
        (database,),
    )
    row = cur.fetchone()
    if not row:
        return None
    backup_mb = float(row[0]) if row[0] is not None else None
    comp_mb = float(row[1]) if row[1] is not None else None
    # compressed_backup_size can be NULL on very old rows; use backup_size
    effective_mb = comp_mb if comp_mb and comp_mb > 0 else backup_mb
    if not effective_mb or effective_mb <= 0:
        return None
    return {
        "effective_mb": effective_mb,
        "backup_size_mb": backup_mb,
        "compressed_size_mb": comp_mb,
        "finish": row[2],
        "device": row[3],
    }


def fetch_data_file_size_gb(cur: Any, database: str) -> float:
    cur.execute(
        """
        SELECT CAST(SUM(size) * 8.0 / 1024 / 1024 AS DECIMAL(14,2))
        FROM sys.master_files
        WHERE database_id = DB_ID(?)
        """,
        (database,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return 0.0
    return float(row[0])


def estimate_backup_gb(
    *,
    data_file_gb: float,
    use_compression: bool,
    last_backup_stats: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Produce planning estimate in GB with method + components.
    """
    out: Dict[str, Any] = {
        "gb": 0.0,
        "method": "unknown",
        "detail": "",
    }
    if last_backup_stats:
        gb = max(0.01, last_backup_stats["effective_mb"] / 1024.0)
        out["gb"] = gb
        out["method"] = "msdb_last_full_backup"
        dev = last_backup_stats.get("device") or ""
        out["detail"] = (
            f"Last full backup ~{gb:.2f} GB (msdb; device={dev!r}, "
            f"finished={last_backup_stats.get('finish')})"
        )
        return out

    # Heuristic fallback: logical data size * ratio (compression varies widely)
    ratio = 0.55 if use_compression else 1.0
    gb = max(0.01, data_file_gb * ratio)
    out["gb"] = gb
    out["method"] = "logical_data_heuristic"
    out["detail"] = (
        f"Logical data files ~{data_file_gb:.2f} GB * {ratio:.2f} "
        f"({'compression on' if use_compression else 'no compression'}) "
        f"~ {gb:.2f} GB (heuristic; use msdb history when available)"
    )
    return out


def run_local_backup_capacity_report(
    *,
    backup_path: str,
    server: str,
    database: str,
    auth: str,
    user: str,
    password: str,
    use_compression: bool,
    log: LogFn,
    connect_to_database: Any,
    logger: Any,
) -> None:
    """
    Full report for UI: path access, free space, backup estimate, status.
    """
    raw = normalize_backup_dir_path(backup_path)
    path = os.path.dirname(raw) if raw.lower().endswith(".bak") else raw
    log("")
    log("=== Path Capacity Check ===")
    log(f"Path (normalized): {raw}")
    if raw.lower().endswith(".bak"):
        log(f"Full .bak path: using parent folder for free-space check: {path}")

    path_exists = os.path.exists(path)
    log(f"App can see path (os.path.exists): {'yes' if path_exists else 'no'}")

    du, method = best_disk_usage(path, log=log)
    if du:
        total_b, used_b, free_b = du
        total_gb = total_b / (1024**3)
        used_gb = used_b / (1024**3)
        free_gb = free_b / (1024**3)
        log(f"Free-space source: {method}")
        log(f"Capacity: total={total_gb:.2f} GB, used={used_gb:.2f} GB, free~{free_gb:.2f} GB (caller view)")
    else:
        log("Free space: could not be read from this app host for this path.")
        if unc_share_root(path):
            log(
                "Tip: UNC shares often require your Windows account to have share+NTFS read. "
                "SQL Server may still backup there even if this PC cannot list free space."
            )

    if not (server and database):
        log("Note: Enter server + database for backup size estimate (msdb + data files).")
        return

    try:
        conn = connect_to_database(
            server=server,
            db="master",
            user=user or "",
            driver="ODBC Driver 18 for SQL Server",
            auth=auth or "windows",
            password=password or "",
            timeout=15,
            logger=logger,
        )
        cur = conn.cursor()
        data_gb = fetch_data_file_size_gb(cur, database)
        log(f"Logical data+logs (sys.master_files): {data_gb:.2f} GB")

        last_stats = fetch_last_full_backup_stats(cur, database)
        if last_stats:
            log(
                f"Last full backup row: raw={last_stats.get('backup_size_mb')} MB, "
                f"compressed={last_stats.get('compressed_size_mb')} MB "
                f"(using effective {last_stats['effective_mb']:.2f} MB for planning)"
            )
        else:
            log("No prior full backup in msdb for this database (first backup estimate is heuristic only).")

        est = estimate_backup_gb(
            data_file_gb=data_gb,
            use_compression=use_compression,
            last_backup_stats=last_stats,
        )
        log(f"Planned backup size estimate: {est['gb']:.2f} GB")
        log(f"Estimate method: {est['method']}")
        log(est["detail"])

        buffer = 1.2
        recommended = est["gb"] * buffer
        log(f"Recommended free space (×{buffer:.0%} buffer): {recommended:.2f} GB")

        if du:
            free_gb = du[2] / (1024**3)
            if free_gb >= recommended:
                log("Status: OK - free space looks sufficient for this estimate.")
            else:
                log("Status: WARNING - free space may be too low for this estimate.")
        else:
            log("Status: UNKNOWN - cannot compare free space from app host; validate on file server / DBA side.")

        cur.close()
        conn.close()
    except Exception as e:
        log(f"Note: SQL size query failed: {e}")
