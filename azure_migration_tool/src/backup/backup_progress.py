# Author: Satish Chauhan

"""Poll sys.dm_exec_requests for BACKUP DATABASE / BACKUP LOG progress."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

LogFn = Callable[[str], None]
ProgressFn = Callable[[float], None]


_BACKUP_DMV_SQL = """
DECLARE @TargetDB NVARCHAR(128) = ?;

SELECT
    r.session_id AS SPID,
    DB_NAME(r.database_id) AS database_name,
    r.command,
    CONVERT(DECIMAL(5, 2), r.percent_complete) AS percent_complete,
    r.start_time,
    CONVERT(DECIMAL(10, 2), r.total_elapsed_time / 60000.0) AS elapsed_minutes,
    CONVERT(DECIMAL(10, 2), r.estimated_completion_time / 60000.0) AS est_remaining_minutes,
    DATEADD(SECOND, r.estimated_completion_time / 1000, GETDATE()) AS estimated_completion_time,
    r.wait_type,
    r.status,
    a.text AS query_text
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) a
WHERE r.command IN ('BACKUP DATABASE', 'BACKUP LOG')
  AND (
        DB_NAME(r.database_id) = @TargetDB
        OR a.text LIKE '%' + @TargetDB + '%'
  );
"""


@dataclass
class BackupProgressSnapshot:
    database: str
    session_id: Optional[int] = None
    command: str = ""
    percent_complete: Optional[float] = None
    start_time: Any = None
    elapsed_minutes: Optional[float] = None
    est_minutes_remaining: Optional[float] = None
    estimated_completion_at: Any = None
    wait_type: str = ""
    request_status: str = ""

    def format_log_line(self) -> str:
        pct = (
            f"{float(self.percent_complete):.1f}%"
            if self.percent_complete is not None
            else "—"
        )
        parts = [
            f"  [backup progress] SPID {self.session_id}: [{self.database}] "
            f"{self.command or 'BACKUP'}: {pct} complete"
        ]
        if self.start_time is not None:
            st = self.start_time
            parts.append(
                f"started {st.strftime('%Y-%m-%d %H:%M:%S') if hasattr(st, 'strftime') else st}"
            )
        if self.elapsed_minutes is not None:
            parts.append(f"elapsed {self.elapsed_minutes:.1f} min")
        if self.est_minutes_remaining is not None and self.est_minutes_remaining > 0:
            parts.append(f"remaining {self.est_minutes_remaining:.1f} min")
        if self.estimated_completion_at is not None:
            eta = self.estimated_completion_at
            parts.append(
                "est. finish "
                + (
                    eta.strftime("%Y-%m-%d %H:%M:%S")
                    if hasattr(eta, "strftime")
                    else str(eta)
                )
            )
        if self.request_status or self.wait_type:
            parts.append(f"{self.request_status} | wait {self.wait_type}")
        return " | ".join(parts)


def _snapshot_from_row(database: str, row: Tuple[Any, ...]) -> BackupProgressSnapshot:
    snap = BackupProgressSnapshot(database=database)
    snap.session_id = int(row[0]) if row[0] is not None else None
    if row[1]:
        snap.database = str(row[1])
    snap.command = str(row[2] or "BACKUP DATABASE")
    snap.percent_complete = float(row[3]) if row[3] is not None else None
    snap.start_time = row[4]
    snap.elapsed_minutes = float(row[5]) if row[5] is not None else None
    snap.est_minutes_remaining = float(row[6]) if row[6] is not None else None
    snap.estimated_completion_at = row[7]
    snap.wait_type = str(row[8] or "")
    snap.request_status = str(row[9] or "") if len(row) > 9 else ""
    return snap


def _pick_best_backup_row(
    rows: List[Tuple[Any, ...]],
    database: str,
) -> Optional[Tuple[Any, ...]]:
    if not rows:
        return None
    db_key = (database or "").strip().lower()
    scored: List[Tuple[float, Tuple[Any, ...]]] = []
    for row in rows:
        db_name = (str(row[1] or "") if len(row) > 1 else "").lower()
        if db_key and db_name and db_name != db_key:
            continue
        pct = float(row[3]) if row[3] is not None else 0.0
        scored.append((pct, row))
    if not scored:
        pct_row = max(rows, key=lambda r: float(r[3] or 0))
        return pct_row
    return max(scored, key=lambda t: t[0])[1]


def fetch_backup_progress_snapshot(cur, database: str) -> Optional[BackupProgressSnapshot]:
    if not (database or "").strip():
        return None
    try:
        cur.execute(_BACKUP_DMV_SQL, (database.strip(),))
        rows = list(cur.fetchall() or [])
        if not rows:
            return None
        row = _pick_best_backup_row(rows, database)
        if not row:
            return None
        return _snapshot_from_row(database, row)
    except Exception:
        return None


def monitor_backup_request_progress(
    *,
    connect_kwargs: Dict[str, Any],
    database: str,
    log: LogFn,
    stop_event: threading.Event,
    poll_sec: float = 5.0,
    progress_callback: Optional[ProgressFn] = None,
    odbc_spid: Optional[int] = None,
) -> None:
    try:
        from ..utils.database import connect_to_database
    except ImportError:
        from src.utils.database import connect_to_database

    mconn = None
    try:
        mconn = connect_to_database(**connect_kwargs)
        try:
            mconn.timeout = 0
        except Exception:
            pass
        mcur = mconn.cursor()
        last_pct = -1.0
        last_line = ""
        last_spid: Optional[int] = None
        while not stop_event.is_set():
            try:
                snap = fetch_backup_progress_snapshot(mcur, database)
                if snap is None or snap.percent_complete is None:
                    stop_event.wait(poll_sec)
                    continue
                pct = max(0.0, min(100.0, float(snap.percent_complete)))
                sid = snap.session_id
                if sid is not None and sid != last_spid:
                    log(
                        f"  [backup progress] Tracking SPID {sid} for {database}"
                        + (f" (ODBC session was SPID {odbc_spid})" if odbc_spid else "")
                    )
                    last_spid = sid
                if pct - last_pct >= 0.1 or pct >= 100.0:
                    line = snap.format_log_line()
                    if line != last_line:
                        log(line)
                        last_line = line
                    last_pct = pct
                    if progress_callback:
                        try:
                            progress_callback(pct)
                        except Exception:
                            pass
            except Exception:
                pass
            stop_event.wait(poll_sec)
    except Exception as exc:
        log(f"  (backup progress monitor unavailable: {exc})")
    finally:
        try:
            if mconn is not None:
                mconn.close()
        except Exception:
            pass


def get_spid(cur) -> Optional[int]:
    try:
        cur.execute("SELECT @@SPID")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None