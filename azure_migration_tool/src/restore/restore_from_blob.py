# Author: S@tish Ch@uhan

"""
Restore SQL Server database from Azure Blob Storage (.bak) via RESTORE DATABASE FROM URL.

Handles both single-file and striped backups. If the selected blob path matches
the striped naming convention `<db>_partNNofMM.bak`, all sibling stripes in the
same folder are auto-discovered and used in the RESTORE statement.

Uses the same credential pattern as backup (container SAS); no WITH CREDENTIAL for SAS.

Diagnostic helpers:
  * run_test_blob_sdk_read — Azure SDK get_blob_properties on **this PC** (DefaultAzureCredential).
  * run_test_blob_headeronly_via_sql_odbc — ODBC to SQL Server, CREATE CREDENTIAL, RESTORE HEADERONLY
    FROM URL (same blob access path as full RESTORE).
"""

import re
import time
import logging
import threading
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple, Callable

try:
    from ..utils.redact_secrets import redact_sensitive_text
except ImportError:
    try:
        from src.utils.redact_secrets import redact_sensitive_text
    except ImportError:
        def redact_sensitive_text(t: str) -> str:  # type: ignore[misc]
            return t

logger = logging.getLogger(__name__)


_STRIPE_RE = re.compile(r"_part(\d+)of(\d+)\.bak$", re.IGNORECASE)
_RESTORE_TARGET_DB_RE = re.compile(
    r"RESTORE\s+(?:DATABASE|LOG)\s+(?:\[([^\]]+)\]|([^\s;\[]+))",
    re.IGNORECASE,
)

# Commands that expose percent_complete in sys.dm_exec_requests.
_PROGRESS_COMMANDS = (
    "BACKUP DATABASE",
    "RESTORE DATABASE",
    "RESTORE LOG",
    "DBCC TABLE CHECK",
    "ALTER INDEX",
    "DBCC ALLOC CHECK",
    "UPDATE STATISTICS",
    "KILLED/ROLLBACK",
)


def _log_restore_urls(log, restore_urls: List[str]) -> None:
    """Avoid flooding the log with dozens of identical stripe URLs."""
    if len(restore_urls) <= 3:
        for u in restore_urls:
            log(f"Restore URL: {u}")
        return
    log(f"Restore URLs: {len(restore_urls)} stripe file(s)")
    log(f"  first: {restore_urls[0]}")
    log(f"  last:  {restore_urls[-1]}")


def _credential_exists(cur, credential_name: str, esc_sql) -> bool:
    name_lit = esc_sql(credential_name)
    cur.execute(
        "SELECT COUNT(1) FROM sys.credentials WHERE name = N'" + name_lit + "'"
    )
    row = cur.fetchone()
    return bool(row and int(row[0]) > 0)


def _ensure_sql_blob_credential(
    cur,
    *,
    credential_name: str,
    blob_auth_mode: str,
    sas_token: Optional[str],
    log,
    mi_credential_sql,
    esc_sql,
) -> None:
    """
    Create or reuse the SQL credential for RESTORE FROM URL.

    Managed Identity credentials are reused when already present (retry-safe).
    SAS credentials are dropped and recreated so the SECRET can be refreshed.
    """
    cred_bracket = credential_name.replace("]", "]]")
    exists = _credential_exists(cur, credential_name, esc_sql)
    mode = (blob_auth_mode or "").strip().lower()

    if exists and mode == "managed_identity":
        log(
            f"SQL credential already exists [{credential_name}] — reusing for Managed Identity RESTORE."
        )
        return

    if exists:
        log(f"Dropping existing credential [{credential_name}] to refresh SAS…")
        drop_sql = (
            "IF EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'"
            + esc_sql(credential_name)
            + "') DROP CREDENTIAL ["
            + cred_bracket
            + "]"
        )
        cur.execute(drop_sql)
    else:
        log("Creating SQL Server credential for blob container…")

    if mode == "managed_identity":
        create_cred_sql = mi_credential_sql(credential_name)
        log("CREATE CREDENTIAL (IDENTITY = 'Managed Identity').")
    else:
        create_cred_sql = (
            f"CREATE CREDENTIAL [{cred_bracket}] "
            f"WITH IDENTITY = N'SHARED ACCESS SIGNATURE', "
            f"SECRET = N'{esc_sql(sas_token or '')}'"
        )
        log("CREATE CREDENTIAL (SHARED ACCESS SIGNATURE).")

    try:
        cur.execute(create_cred_sql)
    except Exception as exc:
        err = str(exc)
        if exists and mode == "managed_identity" and (
            "15530" in err or "already exists" in err.lower()
        ):
            log("Credential already present — continuing with RESTORE.")
            return
        raise


def _restore_target_db_from_sql(sql_text: Optional[str]) -> Optional[str]:
    """Parse ``RESTORE DATABASE|LOG <name>`` from the batch text for a DMV session."""
    if not sql_text:
        return None
    m = _RESTORE_TARGET_DB_RE.search(str(sql_text))
    if not m:
        return None
    return (m.group(1) or m.group(2) or "").strip()


def _filter_restore_rows_for_database(
    rows: List[Tuple[Any, ...]],
    database: str,
) -> Tuple[List[Tuple[Any, ...]], bool]:
    """
    When several RESTOREs run on one instance, keep rows whose T-SQL targets ``database``.

    Returns (filtered_rows, matched_by_sql_text).
    """
    if not rows or not (database or "").strip():
        return rows, False
    db_key = database.strip().lower()
    by_sql: List[Tuple[Any, ...]] = []
    for row in rows:
        sql = row[8] if len(row) > 8 else None
        target = _restore_target_db_from_sql(sql)
        if target and target.lower() == db_key:
            by_sql.append(row)
    if by_sql:
        return by_sql, True
    by_ctx = [
        r
        for r in rows
        if len(r) > 6 and str(r[6] or "").strip().lower() == db_key
    ]
    if by_ctx:
        return by_ctx, False
    return rows, False


def _warn_if_restore_in_progress(cur, log, database: str) -> None:
    """Best-effort warning when another RESTORE may still be running on the instance."""
    try:
        rows = _fetch_restore_dmv_rows(cur)
        if not rows:
            return
        db_key = (database or "").strip().lower()
        same_db: List[str] = []
        other_db: List[str] = []
        for row in rows:
            sid, cmd, pct = row[0], row[1], row[2]
            sql = row[8] if len(row) > 8 else None
            target = _restore_target_db_from_sql(sql)
            label = target or (str(row[6]) if len(row) > 6 else "") or "?"
            entry = f"SPID {sid} {cmd} {float(pct or 0):.1f}% ({label})"
            if target and target.lower() == db_key:
                same_db.append(entry)
            elif target and target.lower() != db_key:
                other_db.append(entry)
        if same_db:
            log(
                "WARNING: RESTORE already active for this database on this instance:\n  "
                + "\n  ".join(same_db)
                + "\n  Wait for it to finish before starting another restore to the same DB."
            )
            return
        if other_db and len(rows) > 1:
            log(
                "WARNING: Other RESTORE operation(s) are running on this instance:\n  "
                + "\n  ".join(other_db)
                + f"\n  Your restore targets [{database}] — progress will be matched by RESTORE "
                "T-SQL when possible; avoid overlapping restores when you can."
            )
    except Exception:
        pass


@dataclass
class RestoreServerSnapshot:
    """Live restore state from sys.databases + sys.dm_exec_requests (SQL Server host)."""

    database: str
    state_desc: str = "UNKNOWN"
    session_id: Optional[int] = None
    command: str = ""
    percent_complete: Optional[float] = None
    request_status: str = ""
    wait_type: str = ""
    est_minutes_remaining: Optional[float] = None
    overall_phase: str = ""
    status_source: str = "dmv"
    blob_restore_session_count: int = 0

    def is_misleading_complete_reading(self) -> bool:
        """Query ``ISNULL(...,100)`` / Complete phase without a worker row — not real progress."""
        if self.session_id is not None or self.wait_type:
            return False
        if self.percent_complete is not None and 0 < float(self.percent_complete) < 100:
            return False
        phase = self.overall_phase or ""
        if "Complete and Ready" in phase and self.session_id is None:
            return True
        if (
            self.percent_complete is not None
            and float(self.percent_complete) >= 100
            and self.session_id is None
        ):
            return True
        return (self.state_desc or "").upper() == "ONLINE" and "Complete and Ready" in phase

    def indicates_active_restore(self) -> bool:
        if self.blob_restore_session_count > 0:
            return True
        state_u = (self.state_desc or "").upper()
        if state_u in ("RESTORING", "RECOVERING"):
            return True
        phase = self.overall_phase or ""
        if "Moving Data" in phase or "Spinning Up" in phase:
            return True
        if self.session_id is not None and (self.wait_type or self.request_status):
            return True
        if self.percent_complete is not None and 0 < float(self.percent_complete) < 100:
            return True
        return False

    def progress_for_bar(self) -> float:
        if self.is_misleading_complete_reading():
            return 0.0
        if self.percent_complete is not None and self.percent_complete >= 0:
            if (self.state_desc or "").upper() == "RECOVERING" and self.percent_complete >= 100:
                return 99.0
            if self.percent_complete < 100:
                return float(self.percent_complete)
        state = (self.state_desc or "").upper()
        if state == "ONLINE" and self.percent_complete is not None:
            return min(100.0, float(self.percent_complete))
        if state == "ONLINE" and self.status_source != "blob_url_worker":
            return 100.0
        if state == "RECOVERING":
            return 99.0
        if state == "RESTORING":
            return 0.0
        return 0.0

    def format_status_line(self) -> str:
        db = self.database or "database"
        parts = [f"{db}: {self.state_desc or 'UNKNOWN'}"]
        if self.overall_phase:
            parts.append(self.overall_phase)
        if self.command:
            pct = (
                f"{self.percent_complete:.1f}%"
                if self.percent_complete is not None
                else "—"
            )
            parts.append(f"{self.command} {pct}")
        if self.request_status:
            parts.append(self.request_status)
        if self.wait_type:
            parts.append(f"wait={self.wait_type}")
        if self.est_minutes_remaining is not None and self.est_minutes_remaining > 0:
            parts.append(f"ETA ~{self.est_minutes_remaining:.0f} min")
        if self.session_id is not None:
            parts.append(f"SPID {self.session_id}")
        return " · ".join(parts)


# Blob RESTORE FROM URL: parent session (0% / SLEEP_TASK) + worker (real %) share the same .bak URL.
_TARGETED_BLOB_RESTORE_STATUS_SQL = """
DECLARE @TargetDB NVARCHAR(128) = ?;

WITH RestoreRequests AS (
    SELECT
        r.session_id,
        r.percent_complete,
        r.status,
        r.wait_type,
        t.text AS executed_query,
        SUBSTRING(
            t.text,
            CHARINDEX('https://', t.text),
            CHARINDEX('.bak', t.text) - CHARINDEX('https://', t.text) + 4
        ) AS BackupURL
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
    WHERE r.command LIKE 'RESTORE%'
      AND t.text LIKE '%https://%.bak%'
),
ActiveRestore AS (
    SELECT
        @TargetDB AS DBName,
        Child.session_id AS worker_spid,
        Child.percent_complete,
        Child.status AS worker_status,
        Child.wait_type AS worker_wait
    FROM RestoreRequests Parent
    JOIN RestoreRequests Child
        ON Parent.BackupURL = Child.BackupURL
    WHERE Parent.executed_query LIKE '%' + @TargetDB + '%'
      AND Child.session_id <> Parent.session_id
)
SELECT
    @TargetDB AS Target_Database,
    ISNULL(ar.percent_complete, CASE WHEN d.name IS NOT NULL THEN 100 ELSE 0 END)
        AS Restore_Percent_Complete,
    ISNULL(d.state_desc, 'INITIALIZING') AS SQL_Database_State,
    CASE
        WHEN ar.percent_complete IS NOT NULL THEN '1 - Moving Data (Restore Active)'
        WHEN d.state_desc = 'RESTORING' THEN '2 - Spinning Up (Recovery Phase)'
        WHEN d.state_desc = 'ONLINE' THEN '3 - Complete and Ready'
        ELSE 'Not Found / Dropped'
    END AS Overall_Phase,
    ar.worker_status,
    ar.worker_wait,
    ar.worker_spid
FROM (SELECT @TargetDB AS DBName) Anchor
LEFT JOIN ActiveRestore ar ON Anchor.DBName = ar.DBName
LEFT JOIN sys.databases d ON Anchor.DBName = d.name;
"""

_RESTORE_DMV_SQL = """
            SELECT
                r.session_id,
                r.command,
                r.percent_complete,
                r.status,
                r.wait_type,
                (r.estimated_completion_time / 1000.0) / 60.0 AS est_min,
                DB_NAME(r.database_id) AS db_ctx,
                r.total_elapsed_time,
                st.text AS sql_text
            FROM sys.dm_exec_requests r
            OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
            WHERE r.command IN ('RESTORE DATABASE', 'RESTORE LOG', 'RESTORE VERIFYONLY')
"""

# Worker sessions report real %; the ODBC coordinator often sits at 0% + SLEEP_TASK.
_RESTORE_ACTIVE_WAIT_TYPES = frozenset(
    {
        "BACKUPTHREAD",
        "IO_COMPLETION",
        "ASYNC_IO_COMPLETION",
        "WRITELOG",
        "PAGEIOLATCH_SH",
        "PAGEIOLATCH_EX",
    }
)


def _narrow_restore_dmv_rows(rows: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
    """Drop idle RESTORE coordinator rows when a worker session is visible in the DMV."""
    if len(rows) <= 1:
        return rows
    with_progress = [r for r in rows if float(r[2] or 0) > 0.0]
    if with_progress:
        return with_progress
    active = [
        r
        for r in rows
        if str(r[4] or "").upper() in _RESTORE_ACTIVE_WAIT_TYPES
    ]
    if active:
        return active
    non_sleep = [r for r in rows if str(r[4] or "").upper() != "SLEEP_TASK"]
    if non_sleep:
        return non_sleep
    return rows


def _pick_best_restore_request_row(
    rows: List[Tuple[Any, ...]],
    database: str,
    preferred_spid: Optional[int] = None,
) -> Optional[Tuple[Any, ...]]:
    """
    Choose the active RESTORE row when DMV shows multiple sessions (e.g. 0% + real %).

    SQL Server often has two rows: ODBC session at 0% / SLEEP_TASK and a worker at real %
    / BACKUPTHREAD. ``preferred_spid`` (``@@SPID`` on the RESTORE connection) is **not** used
    for ranking — it is usually the idle coordinator.

    Prefer highest ``percent_complete``, then target DB context, then longest elapsed time.
    """
    if not rows:
        return None
    rows, _sql_matched = _filter_restore_rows_for_database(list(rows), database)
    rows = _narrow_restore_dmv_rows(rows)
    db_key = (database or "").strip().lower()

    def _score(row: Tuple[Any, ...]) -> Tuple[float, int, int]:
        pct = float(row[2]) if row[2] is not None else 0.0
        elapsed = int(row[7]) if len(row) > 7 and row[7] is not None else 0
        db_ctx = (str(row[6] or "") if len(row) > 6 else "").lower()
        db_match = 1 if db_key and db_ctx == db_key else 0
        return (pct, db_match, elapsed)

    return max(rows, key=_score)


def _fetch_restore_dmv_rows(cur) -> List[Tuple[Any, ...]]:
    cur.execute(_RESTORE_DMV_SQL)
    return list(cur.fetchall() or [])


def _count_blob_restore_sessions_for_database(cur, database: str) -> int:
    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM sys.dm_exec_requests r
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
            WHERE r.command LIKE 'RESTORE%'
              AND t.text LIKE '%https://%.bak%'
              AND t.text LIKE '%' + ? + '%'
            """,
            (database,),
        )
        cnt = cur.fetchone()
        return int(cnt[0]) if cnt and cnt[0] is not None else 0
    except Exception:
        return 0


def _snapshot_from_targeted_blob_status_row(
    database: str,
    row: Tuple[Any, ...],
) -> RestoreServerSnapshot:
    """Map the optimized blob-restore status query row to ``RestoreServerSnapshot``."""
    snap = RestoreServerSnapshot(database=database, status_source="blob_url_worker")
    snap.state_desc = str(row[2] or "UNKNOWN")
    snap.overall_phase = str(row[3] or "")
    snap.command = "RESTORE DATABASE"
    worker_status = row[4]
    worker_wait = row[5]
    worker_spid = row[6] if len(row) > 6 else None
    snap.request_status = str(worker_status or "")
    snap.wait_type = str(worker_wait or "")
    if worker_spid is not None:
        snap.session_id = int(worker_spid)

    state_u = snap.state_desc.upper()
    raw_pct = float(row[1]) if row[1] is not None else None
    if worker_status is not None and raw_pct is not None:
        snap.percent_complete = raw_pct
    elif state_u == "ONLINE" and worker_status is not None:
        snap.percent_complete = raw_pct if raw_pct is not None else 100.0
    elif state_u == "ONLINE":
        # DB existed ONLINE before RESTORE — query's ISNULL(...,100) is not real progress.
        snap.percent_complete = None
        if "Complete and Ready" in snap.overall_phase:
            snap.overall_phase = "Waiting for RESTORE to start…"
    elif state_u == "RECOVERING":
        snap.percent_complete = None
    elif state_u == "RESTORING" and worker_status is None:
        snap.percent_complete = None
    elif "Not Found" in snap.overall_phase:
        snap.percent_complete = None
    elif worker_status is not None and raw_pct is not None:
        snap.percent_complete = raw_pct
    else:
        snap.percent_complete = None
    return snap


def _fetch_targeted_blob_restore_snapshot(
    cur,
    database: str,
) -> Optional[RestoreServerSnapshot]:
    """Run parent/child BackupURL restore status query for ``RESTORE FROM URL``."""
    if not (database or "").strip():
        return None
    try:
        cur.execute(_TARGETED_BLOB_RESTORE_STATUS_SQL, (database.strip(),))
        row = cur.fetchone()
        if not row:
            return None
        snap = _snapshot_from_targeted_blob_status_row(database, row)
        snap.blob_restore_session_count = _count_blob_restore_sessions_for_database(
            cur, database
        )
        return snap
    except Exception:
        return None


def _fetch_restore_server_snapshot(
    cur,
    database: str,
    *,
    preferred_spid: Optional[int] = None,
) -> Tuple[RestoreServerSnapshot, int]:
    targeted = _fetch_targeted_blob_restore_snapshot(cur, database)
    if targeted is not None:
        n_blob = _count_blob_restore_sessions_for_database(cur, database)
        targeted.blob_restore_session_count = n_blob
        return targeted, n_blob

    snap = RestoreServerSnapshot(database=database, status_source="dmv")
    try:
        cur.execute("SELECT state_desc FROM sys.databases WHERE name = ?", (database,))
        row = cur.fetchone()
        if row and row[0]:
            snap.state_desc = str(row[0])
    except Exception:
        pass
    try:
        rows = _fetch_restore_dmv_rows(cur)
        row = _pick_best_restore_request_row(rows, database, preferred_spid)
        if row:
            snap.session_id = int(row[0]) if row[0] is not None else None
            snap.command = str(row[1] or "")
            snap.percent_complete = float(row[2]) if row[2] is not None else None
            snap.request_status = str(row[3] or "")
            snap.wait_type = str(row[4] or "")
            if row[5] is not None:
                snap.est_minutes_remaining = float(row[5])
        return snap, len(rows)
    except Exception:
        pass
    return snap, 0


def watch_restore_on_sql_server(
    *,
    connect_kwargs: Dict[str, Any],
    database: str,
    log: Callable[[str], None],
    stop_event: threading.Event,
    poll_sec: float = 5.0,
    on_snapshot: Optional[Callable[[RestoreServerSnapshot], None]] = None,
    get_preferred_spid: Optional[Callable[[], Optional[int]]] = None,
) -> str:
    """
    Poll SQL Server until the database is ONLINE/SUSPECT or ``stop_event`` is set.

    Use when the ODBC restore client disconnects but work may continue on the server.
    Returns the last ``state_desc`` observed.
    """
    try:
        from ..utils.database import connect_to_database
    except ImportError:
        from src.utils.database import connect_to_database

    last_line = ""
    last_state = ""
    logged_multi = False
    seen_restore_activity = False
    conn = None
    try:
        conn = connect_to_database(**connect_kwargs)
        try:
            conn.timeout = 30
        except Exception:
            pass
        cur = conn.cursor()
        while not stop_event.is_set():
            pref = get_preferred_spid() if get_preferred_spid else None
            snap, n_restore = _fetch_restore_server_snapshot(
                cur,
                database,
                preferred_spid=pref,
            )
            if not logged_multi and (
                n_restore > 1 or snap.status_source == "blob_url_worker"
            ):
                logged_multi = True
                if snap.status_source == "blob_url_worker":
                    log(
                        f"  [server status] Using blob URL worker query for [{database}] "
                        f"({snap.overall_phase or 'status'}"
                        + (
                            f", SPID {snap.session_id}"
                            if snap.session_id is not None
                            else ""
                        )
                        + ")."
                    )
                else:
                    scoped, by_sql = _filter_restore_rows_for_database(
                        _fetch_restore_dmv_rows(cur), database
                    )
                    scope_note = (
                        f"{len(scoped)} session(s) match RESTORE T-SQL for [{database}]"
                        if by_sql
                        else f"could not match RESTORE T-SQL for [{database}] — "
                        "using best-effort % among all RESTORE sessions on this instance"
                    )
                    log(
                        f"  [server status] {n_restore} RESTORE DMV row(s) on instance — {scope_note}; "
                        f"tracking SPID {snap.session_id or pref} at "
                        f"{snap.percent_complete or 0:.1f}%."
                    )
            line = snap.format_status_line()
            if pref and snap.session_id and snap.session_id != pref:
                line = f"{line} (ODBC SPID {pref})"
            state_u = (snap.state_desc or "").upper()
            if line != last_line:
                log(f"  [server status] {line}")
                last_line = line
            if state_u != last_state:
                last_state = state_u
            if snap.indicates_active_restore():
                seen_restore_activity = True
            if on_snapshot:
                try:
                    if snap.is_misleading_complete_reading():
                        waiting = RestoreServerSnapshot(
                            database=snap.database,
                            state_desc=snap.state_desc,
                            overall_phase="Waiting for RESTORE to start…",
                            status_source=snap.status_source,
                            blob_restore_session_count=snap.blob_restore_session_count,
                        )
                        on_snapshot(waiting)
                    else:
                        on_snapshot(snap)
                except Exception:
                    pass
            if state_u == "ONLINE":
                if seen_restore_activity and not snap.indicates_active_restore():
                    snap.percent_complete = 100.0
                    snap.overall_phase = "3 - Complete and Ready"
                    if on_snapshot:
                        try:
                            on_snapshot(snap)
                        except Exception:
                            pass
                    log(f"  [server status] {database} is ONLINE on SQL Server.")
                    return snap.state_desc
            if state_u in ("SUSPECT", "EMERGENCY", "OFFLINE"):
                log(f"  [server status] {database} entered {snap.state_desc} — restore may have failed.")
                return snap.state_desc
            stop_event.wait(poll_sec)
    except Exception as exc:
        log(f"  (server status monitor unavailable: {exc})")
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
    return last_state or "UNKNOWN"


def _odbc_restore_disconnect_may_be_false_failure(err: str) -> bool:
    """ODBC lost the RESTORE session though work may have finished on SQL (common on Azure SQL MI)."""
    low = (err or "").lower()
    markers = (
        "42036",
        "3013",
        "terminating abnormally",
        "operation has been cancelled",
        "restore managed database",
        "hyt00",
        "query timeout expired",
        "communication link failure",
        "connection is broken",
        "connection was killed",
    )
    return any(m in low for m in markers)


def _fetch_database_state_desc(cur, database: str) -> Optional[str]:
    try:
        cur.execute("SELECT state_desc FROM sys.databases WHERE name = ?", (database,))
        row = cur.fetchone()
        return str(row[0]) if row and row[0] else None
    except Exception:
        return None


def _verify_database_state_with_new_connection(
    connect_kwargs: Dict[str, Any],
    database: str,
) -> Optional[str]:
    try:
        from ..utils.database import connect_to_database
    except ImportError:
        from src.utils.database import connect_to_database

    conn = None
    try:
        conn = connect_to_database(**connect_kwargs)
        cur = conn.cursor()
        return _fetch_database_state_desc(cur, database)
    except Exception:
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _resolve_restore_after_odbc_error(
    connect_kwargs: Dict[str, Any],
    database: str,
    err_text: str,
    log: Callable[[str], None],
) -> Optional[str]:
    """
    If ODBC reports cancel/3013 but the database is already ONLINE, return ``success``.

    Returns ``client_lost`` when RESTORE likely still running; ``None`` if still a real failure.
    """
    if not _odbc_restore_disconnect_may_be_false_failure(err_text):
        return None
    state = (_verify_database_state_with_new_connection(connect_kwargs, database) or "").upper()
    if state == "ONLINE":
        log(
            "ODBC reported an error on the RESTORE connection, but "
            f"[{database}] is ONLINE on SQL Server — treating restore as successful."
        )
        return "success"
    if state in ("RESTORING", "RECOVERING"):
        log(
            f"ODBC connection ended ({err_text[:120]}…) but [{database}] is {state} on the server — "
            "restore may still be in progress."
        )
        return "client_lost"
    return None


def _get_spid(cur) -> Optional[int]:
    """Return the SPID (session_id) of the given connection's cursor, or None."""
    try:
        cur.execute("SELECT @@SPID")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def _monitor_request_progress(
    *,
    connect_kwargs: Dict[str, Any],
    database: str,
    odbc_spid: Optional[int],
    log,
    stop_event: "threading.Event",
    poll_sec: float = 5.0,
    progress_callback=None,
) -> None:
    """Best-effort progress monitor.

    Polls all RESTORE rows in ``sys.dm_exec_requests`` and uses the active worker session
    (highest %, BACKUPTHREAD — not the ODBC coordinator at 0% / SLEEP_TASK).
    """
    try:
        from ..utils.database import connect_to_database
    except ImportError:
        try:
            from src.utils.database import connect_to_database
        except ImportError:
            from utils.database import connect_to_database

    est_query = (
        "SELECT DATEADD(second, r.estimated_completion_time/1000, GETDATE()) "
        "FROM sys.dm_exec_requests r WHERE r.session_id = ?"
    )

    mconn = None
    try:
        mconn = connect_to_database(**connect_kwargs)
        try:
            mconn.timeout = 0
        except Exception:
            pass
        mcur = mconn.cursor()
        last_pct = -1.0
        last_wait = ""
        last_tracked_spid: Optional[int] = None
        while not stop_event.is_set():
            try:
                snap = _fetch_targeted_blob_restore_snapshot(mcur, database)
                from_targeted_snap = False
                if (
                    snap is not None
                    and snap.percent_complete is not None
                    and not snap.is_misleading_complete_reading()
                    and "Not Found" not in (snap.overall_phase or "")
                ):
                    from_targeted_snap = True
                    pct = float(snap.percent_complete)
                    sid = snap.session_id
                    cmd = snap.command or "RESTORE DATABASE"
                    req_status = snap.request_status
                    wait_type = snap.wait_type
                    phase = snap.overall_phase
                else:
                    snap = None
                    rows = _fetch_restore_dmv_rows(mcur)
                    row = _pick_best_restore_request_row(rows, database)
                    if not row or row[2] is None:
                        stop_event.wait(poll_sec)
                        continue
                    pct = float(row[2])
                    sid = int(row[0]) if row[0] is not None else None
                    cmd = str(row[1] or "RESTORE DATABASE")
                    req_status = str(row[3] or "")
                    wait_type = str(row[4] or "")
                    phase = ""
                if sid is not None and sid != last_tracked_spid:
                    note = (
                        f"  [progress] Tracking worker SPID {sid} for {database}"
                        + (f" (ODBC session was SPID {odbc_spid})" if odbc_spid else "")
                        + (" via blob URL query." if phase else ".")
                    )
                    log(note)
                    last_tracked_spid = sid
                if pct >= 100.0 and sid is None:
                    stop_event.wait(poll_sec)
                    continue
                if from_targeted_snap and snap is not None and snap.is_misleading_complete_reading():
                    stop_event.wait(poll_sec)
                    continue
                wait_changed = bool(wait_type and wait_type != last_wait)
                if pct >= 0 and (
                    pct - last_pct >= 0.1
                    or (pct >= 100.0 and sid is not None)
                    or wait_changed
                ):
                    if wait_type:
                        last_wait = wait_type
                    est_s = ""
                    if sid is not None:
                        try:
                            mcur.execute(est_query, sid)
                            est_row = mcur.fetchone()
                            est = est_row[0] if est_row else None
                            est_s = (
                                est.strftime("%Y-%m-%d %H:%M:%S")
                                if hasattr(est, "strftime")
                                else str(est)
                            )
                        except Exception:
                            est_s = "—"
                    extra = ""
                    if phase:
                        extra = f" | {phase}"
                    if req_status or wait_type:
                        extra += f" | {req_status} | wait {wait_type}"
                    log(
                        f"  [progress] SPID {sid}: {cmd}: {pct:.1f}% complete"
                        + (f" (est. finish {est_s})" if est_s else "")
                        + extra
                    )
                    last_pct = pct
                    if progress_callback:
                        try:
                            progress_callback(pct)
                        except Exception:
                            pass
            except Exception:
                # Transient DMV/read errors should not stop monitoring.
                pass
            stop_event.wait(poll_sec)
    except Exception as e:
        log(f"  (progress monitor unavailable: {e})")
    finally:
        try:
            if mconn is not None:
                mconn.close()
        except Exception:
            pass


def _log_sql_engine_context_for_mi_blob(cur, server: str, log) -> None:
    """Log SQL Server version and Windows service account — helps explain MI vs laptop identity."""
    log("--- SQL Server host (identity used for Managed Identity blob access) ---")
    log(f"  Connected instance: {server}")
    edition = ""
    try:
        cur.execute(
            """
            SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT),
                   CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(64)),
                   CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256)),
                   CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(128))
            """
        )
        row = cur.fetchone()
        if row:
            maj, ver, ed, mach = row[0], row[1], row[2], row[3]
            edition = str(ed or "")
            log(f"  ProductMajorVersion: {maj} (SQL Server 2022 = 16)")
            log(f"  ProductVersion / Edition: {ver} / {ed}")
            if mach:
                log(f"  MachineName (SERVERPROPERTY): {mach}")
            if "SQL Azure" in edition:
                log(
                    "  Azure SQL detected: RESTORE FROM URL with IDENTITY = 'Managed Identity' uses the "
                    "**Managed Instance's Azure AD identity** — not your PC user and not the VM service account."
                )
                log(
                    "  Portal → your SQL managed instance → Security → Identity → grant that identity "
                    "**Storage Blob Data Reader** on the storage account (or use Connection String / SAS auth in Step 1)."
                )
    except Exception as ex:
        log(f"  (Could not read SERVERPROPERTY: {ex})")
    try:
        cur.execute(
            """
            SELECT servicename, service_account
            FROM sys.dm_server_services
            WHERE servicename LIKE N'SQL Server (%'
            """
        )
        for r in cur.fetchall() or []:
            log(f"  Windows service: {r[0]} → runs as: {r[1]}")
    except Exception as ex:
        log(f"  (Could not read sys.dm_server_services: {ex})")
    log(
        "  For RESTORE/BACKUP … TO/FROM URL with IDENTITY = 'Managed Identity', Azure Storage "
        "RBAC must be granted to the **managed identity of this host** (Azure VM / MI), not to "
        "your workstation user."
    )
    log("--- (end SQL host context) ---")


def _q(name: str) -> str:
    """Quote SQL identifier."""
    return "[" + name.replace("]", "]]") + "]"


def _esc_sql(s: str) -> str:
    return s.replace("'", "''")


def _discover_stripe_set(
    blob_connection_string: str,
    container: str,
    blob_path: str,
    log: Optional[Any] = None,
    blob_service_client=None,
) -> List[str]:
    """
    Given any blob path that ends in .bak, return the full ordered list of
    stripe blob paths. For a single file the result is just `[blob_path]`.

    Striped naming: <prefix>_partNNofMM.bak (zero-padded). Sibling stripes
    must live in the same folder.
    """
    def _say(msg: str) -> None:
        if log:
            try:
                log(msg)
            except Exception:
                pass

    blob_path = blob_path.replace("\\", "/").lstrip("/")
    folder, fname = blob_path.rsplit("/", 1) if "/" in blob_path else ("", blob_path)

    # IMPORTANT: regex is matched against the file name (not the full path),
    # otherwise m.start() is an offset into the path and `fname[: m.start()]`
    # silently returns the entire filename (Python clamps slice indices), which
    # makes the list-by-prefix search match nothing and only one stripe ends
    # up being passed to RESTORE.
    m = _STRIPE_RE.search(fname)
    if not m:
        return [blob_path]

    total = int(m.group(2))
    prefix = fname[: m.start()]  # everything in the filename before "_partNN..."

    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        return [blob_path]

    if blob_service_client is not None:
        client = blob_service_client
    elif blob_connection_string:
        client = BlobServiceClient.from_connection_string(blob_connection_string)
    else:
        return [blob_path]
    container_client = client.get_container_client(container)
    list_prefix = (folder + "/" if folder else "") + prefix + "_part"

    found: Dict[int, str] = {}
    for b in container_client.list_blobs(name_starts_with=list_prefix):
        # Match against just the filename portion of the listed blob too.
        bname = b.name.rsplit("/", 1)[-1]
        mm = _STRIPE_RE.search(bname)
        if not mm:
            continue
        if int(mm.group(2)) != total:
            continue
        found[int(mm.group(1))] = b.name

    if len(found) != total:
        ordered = [found[k] for k in sorted(found)]
        _say(
            f"Stripe discovery found {len(found)} of {total} expected stripes "
            f"under prefix '{list_prefix}'. RESTORE will fail unless all stripes are present."
        )
        # Return what we have so RESTORE produces a clear "media family missing" error
        # rather than us silently restoring from the selected stripe alone.
        return ordered or [blob_path]

    return [found[i] for i in range(1, total + 1)]


def _import_bak_to_blob_helpers():
    """Import helpers from bak_to_blob (package-relative)."""
    try:
        from ..backup import bak_to_blob as _b
    except ImportError:
        try:
            from src.backup import bak_to_blob as _b
        except ImportError:
            from azure_migration_tool.src.backup import bak_to_blob as _b
    return _b


def _prepare_blob_restore_urls(
    *,
    blob_path: str,
    container: str,
    blob_connection_string: str,
    storage_account_url: str,
    blob_auth_mode: str,
    log,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Resolve account URL, container SAS or MI credential name, stripe paths, full HTTPS URLs.

    Returns:
        (error_dict, None) on validation / import failure — same shape as run_restore_from_blob result.
        (None, context) on success. context keys: acct_url, container, credential_name, sas_token,
        stripe_paths, restore_urls (list of full blob URLs).
    """
    fail = lambda msg: ({"status": "failed", "error": msg, "diagnostic": None}, None)

    try:
        _b = _import_bak_to_blob_helpers()
    except Exception as e:
        return fail(f"Backup module not available (needed for credential/SAS): {e}")

    _parse_storage_connection_string = _b._parse_storage_connection_string
    _container_sas_and_url = _b._container_sas_and_url
    _parse_storage_account_url = _b._parse_storage_account_url
    _get_mi_blob_service_client = _b._get_mi_blob_service_client
    normalize_storage_blob_account_url = _b.normalize_storage_blob_account_url

    blob_path = (blob_path or "").strip().replace("\\", "/").lstrip("/")
    container = (container or "").strip()
    if not blob_path or not blob_path.endswith(".bak"):
        return fail("Blob path must be set and end with .bak.")

    if blob_auth_mode == "managed_identity":
        if not storage_account_url:
            return fail(
                "Storage account URL is required for Managed Identity mode. "
                "Enter it in the format: https://myaccount.blob.core.windows.net"
            )
        _raw_u = (storage_account_url or "").strip().rstrip("/")
        acct_url, container = _parse_storage_account_url(storage_account_url, container)
        if normalize_storage_blob_account_url(_raw_u).rstrip("/") != _raw_u:
            log(
                "Adjusted storage URL to Azure Blob endpoint "
                f"(use .blob.core.windows.net): {acct_url}"
            )
        credential_name = f"{acct_url}/{container}"
        log(f"Using Managed Identity auth (credential = {credential_name})")
        sas_token = None
    else:
        if not container:
            return fail("Container name is required in Connection String mode.")
        parts = _parse_storage_connection_string(blob_connection_string)
        account_name = parts.get("accountname", "")
        account_key = parts.get("accountkey", "")
        endpoint_suffix = parts.get("endpointsuffix", "core.windows.net")
        if not account_name or not account_key:
            return fail("Connection string missing AccountName or AccountKey.")
        acct_url = f"https://{account_name}.blob.{endpoint_suffix}"
        log(f"Generating container SAS for credential (container={container})")
        sas_token, credential_name = _container_sas_and_url(
            account_name,
            account_key,
            container,
            endpoint_suffix=endpoint_suffix,
            expiry_hours=48,
        )

    if blob_auth_mode == "managed_identity":
        log(
            "Next: listing blobs / stripe detection uses **this PC’s** Azure AD credential "
            "(same chain as Browse Azure) — not SQL Server."
        )

    if blob_auth_mode == "managed_identity":
        try:
            from ..backup.local_backup_and_upload import _get_tool_blob_service_client
        except ImportError:
            from src.backup.local_backup_and_upload import _get_tool_blob_service_client
        list_client = _get_tool_blob_service_client(
            blob_auth_mode=blob_auth_mode,
            blob_connection_string="",
            blob_account_url=storage_account_url,
            container=container,
            log=log,
        )
        stripe_paths = _discover_stripe_set(
            "", container, blob_path, log=log, blob_service_client=list_client
        )
    else:
        stripe_paths = _discover_stripe_set(
            blob_connection_string, container, blob_path, log=log
        )
    if len(stripe_paths) > 1:
        log(f"Detected striped backup: {len(stripe_paths)} stripe(s)")
    else:
        log("Single-file backup (no stripes detected)")

    restore_urls = [f"{acct_url}/{container}/{p}" for p in stripe_paths]
    _log_restore_urls(log, restore_urls)

    ctx: Dict[str, Any] = {
        "acct_url": acct_url,
        "container": container,
        "credential_name": credential_name,
        "sas_token": sas_token,
        "stripe_paths": stripe_paths,
        "restore_urls": restore_urls,
    }
    return None, ctx


def run_test_blob_sdk_read(
    *,
    blob_connection_string: str = "",
    container: str = "",
    blob_path: str = "",
    log_callback: Optional[Any] = None,
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
) -> Dict[str, Any]:
    """
    Read blob properties via Azure SDK on **this machine** (same credential path as list-backups).
    Does not prove SQL Server can read the blob.
    """
    result: Dict[str, Any] = {"status": "failed", "error": None}

    def log(msg: str) -> None:
        safe = redact_sensitive_text(str(msg))
        logger.info(safe)
        if log_callback:
            try:
                log_callback(safe)
            except Exception:
                pass

    try:
        _b = _import_bak_to_blob_helpers()
        _get_mi_blob_service_client = _b._get_mi_blob_service_client
    except Exception as e:
        result["error"] = str(e)
        return result

    prep_err, ctx = _prepare_blob_restore_urls(
        blob_path=blob_path,
        container=container,
        blob_connection_string=blob_connection_string,
        storage_account_url=storage_account_url,
        blob_auth_mode=blob_auth_mode,
        log=log,
    )
    if prep_err:
        return prep_err

    assert ctx is not None
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        result["error"] = "Install azure-storage-blob (pip install azure-storage-blob)."
        return result

    first_rel = ctx["stripe_paths"][0]
    log(f"SDK test: get_blob_properties for container={ctx['container']!r} blob={first_rel!r}")
    try:
        if blob_auth_mode == "managed_identity":
            svc = _get_mi_blob_service_client(ctx["acct_url"])
        else:
            svc = BlobServiceClient.from_connection_string(blob_connection_string)
        bc = svc.get_blob_client(ctx["container"], first_rel)
        p = bc.get_blob_properties()
        log(f"SDK OK: size_bytes={p.size}, etag={p.etag!r}, last_modified={p.last_modified}")
        result["status"] = "success"
        result["size_bytes"] = p.size
        return result
    except Exception as e:
        err = str(e)
        result["error"] = redact_sensitive_text(err)
        log(f"SDK blob read failed: {err}")
        return result


def run_test_blob_headeronly_via_sql_odbc(
    server: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
    blob_connection_string: str = "",
    container: str = "",
    blob_path: str = "",
    log_callback: Optional[Any] = None,
    target_managed_instance: bool = False,
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
) -> Dict[str, Any]:
    """
    Same path as RESTORE FROM URL: ODBC to SQL Server, CREATE CREDENTIAL, then
    RESTORE HEADERONLY FROM URL (read-only). Validates the **SQL host** can open the backup device.
    """
    import pyodbc  # noqa: F401

    def log(msg: str) -> None:
        safe = redact_sensitive_text(str(msg))
        logger.info(safe)
        if log_callback:
            try:
                log_callback(safe)
            except Exception:
                pass

    result: Dict[str, Any] = {"status": "failed", "error": None}

    try:
        _b = _import_bak_to_blob_helpers()
        _diagnose_backup_error = _b._diagnose_backup_error
        _mi_credential_sql = _b._mi_credential_sql
        _check_mi_backup_supported = _b._check_mi_backup_supported
    except Exception as e:
        result["error"] = f"Backup module not available: {e}"
        return result

    prep_err, ctx = _prepare_blob_restore_urls(
        blob_path=blob_path,
        container=container,
        blob_connection_string=blob_connection_string,
        storage_account_url=storage_account_url,
        blob_auth_mode=blob_auth_mode,
        log=log,
    )
    if prep_err:
        return prep_err
    assert ctx is not None

    acct_url = ctx["acct_url"]
    container = ctx["container"]
    credential_name = ctx["credential_name"]
    sas_token = ctx.get("sas_token")
    restore_urls = ctx["restore_urls"]
    result["stripes"] = len(ctx["stripe_paths"])

    try:
        try:
            from ..utils.database import connect_to_database, pick_sql_driver
        except ImportError:
            try:
                from src.utils.database import connect_to_database, pick_sql_driver
            except ImportError:
                from utils.database import connect_to_database, pick_sql_driver

        driver = pick_sql_driver(logger)
        conn = connect_to_database(
            server=server,
            db="master",
            user=user or "",
            driver=driver,
            auth=auth or "windows",
            password=password,
            timeout=120,
            logger=logger,
        )
        conn.timeout = 0
        conn.autocommit = True
        cur = conn.cursor()

        if blob_auth_mode == "managed_identity":
            _log_sql_engine_context_for_mi_blob(cur, server, log)

        if blob_auth_mode == "managed_identity" and not target_managed_instance:
            mi_err = _check_mi_backup_supported(cur, log)
            if mi_err:
                result["error"] = redact_sensitive_text(mi_err)
                log(mi_err)
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass
                return result

        _ensure_sql_blob_credential(
            cur,
            credential_name=credential_name,
            blob_auth_mode=blob_auth_mode,
            sas_token=sas_token,
            log=log,
            mi_credential_sql=_mi_credential_sql,
            esc_sql=_esc_sql,
        )

        url_clauses = ", ".join(f"URL = N'{_esc_sql(u)}'" for u in restore_urls)
        header_sql = f"RESTORE HEADERONLY FROM {url_clauses}"
        log("Running RESTORE HEADERONLY FROM URL … (read-only; same blob access path as RESTORE DATABASE)")
        t0 = time.perf_counter()
        cur.execute(header_sql)
        cols = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall() or []
        while True:
            try:
                for _ in cur.fetchall():
                    pass
            except Exception:
                pass
            if not cur.nextset():
                break
        elapsed = time.perf_counter() - t0

        if rows and cols:
            preview = [str(x)[:120] for x in rows[0][: min(8, len(rows[0]))]]
            log(f"HEADERONLY OK in {elapsed:.1f}s. Columns (first 8): {cols[:8]}")
            log(f"First row (first 8 values, truncated): {preview}")
        else:
            log(f"HEADERONLY completed in {elapsed:.1f}s (no rows returned — unusual for a valid .bak).")

        cur.close()
        conn.close()
        result["status"] = "success"
        result["header_rows"] = len(rows)
        return result
    except Exception as e:
        err_text = str(e)
        try:
            if getattr(e, "args", None):
                log(f"(ODBC/SQL raw exception.args) {e.args!r}")
        except Exception:
            pass
        try:
            diagnostic = _diagnose_backup_error(
                err_text,
                blob_auth_mode=blob_auth_mode,
                storage_account_url=acct_url,
                container_name=container,
            )
        except Exception:
            diagnostic = ""
        result["error"] = redact_sensitive_text(err_text)
        if diagnostic:
            result["diagnostic"] = diagnostic.strip()
        log(f"HEADERONLY test failed: {err_text}")
        if diagnostic:
            log(diagnostic.strip())
        return result


_ABORT_BLOB_RESTORE_ON_STOP_SQL = """
DECLARE @DBName NVARCHAR(128) = ?;
DECLARE @BackupURL NVARCHAR(MAX);
DECLARE @KillSQL NVARCHAR(MAX) = N'';

SELECT TOP 1
    @BackupURL = SUBSTRING(
        t.text,
        CHARINDEX('https://', t.text),
        CHARINDEX('.bak', t.text) - CHARINDEX('https://', t.text) + 4
    )
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.command LIKE 'RESTORE%'
  AND t.text LIKE '%' + @DBName + '%';

IF @BackupURL IS NOT NULL
BEGIN
    SELECT @KillSQL = @KillSQL + N'KILL ' + CAST(r.session_id AS NVARCHAR(10)) + N'; '
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
    WHERE r.command LIKE 'RESTORE%'
      AND t.text LIKE '%' + @BackupURL + '%';

    IF LEN(@KillSQL) > 0
        EXEC sp_executesql @KillSQL;

    WAITFOR DELAY '00:00:05';
END

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @DBName)
BEGIN
    BEGIN TRY
        DECLARE @DropSQL NVARCHAR(MAX) = N'DROP DATABASE ' + QUOTENAME(@DBName) + N';';
        EXEC sp_executesql @DropSQL;
    END TRY
    BEGIN CATCH
        -- Rollback may have already removed the database.
    END CATCH
END
"""


def run_abort_blob_restore_on_sql_server(
    *,
    server: str,
    database: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
    log_callback: Optional[Callable[[str], None]] = None,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Stop an in-flight blob RESTORE: KILL all sessions on the same backup URL, then DROP DB.

    Matches the operational script used for Azure SQL MI blob restores (parent URL + workers).
    """
    log = log_callback or (lambda m: None)
    result: Dict[str, Any] = {"status": "error", "database": database, "killed_sessions": 0}

    db_name = (database or "").strip()
    if not db_name:
        result["error"] = "Database name is required to stop restore."
        log(result["error"])
        return result
    if re.search(r"[\];'\"\\]", db_name):
        result["error"] = "Invalid database name."
        log(result["error"])
        return result

    try:
        from ..utils.database import connect_to_database, pick_sql_driver
    except ImportError:
        from src.utils.database import connect_to_database, pick_sql_driver

    killed_before = 0
    conn = None
    try:
        driver = pick_sql_driver(logger)
        conn = connect_to_database(
            server=server,
            db="master",
            user=user or "",
            driver=driver,
            auth=auth or "windows",
            password=password,
            timeout=120,
            logger=logger,
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM sys.dm_exec_requests r
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
            WHERE r.command LIKE 'RESTORE%'
              AND t.text LIKE '%' + ? + '%'
            """,
            (db_name,),
        )
        row = cur.fetchone()
        killed_before = int(row[0]) if row and row[0] is not None else 0

        if killed_before:
            log(
                f"Stop: terminating {killed_before} RESTORE session(s) for [{db_name}] "
                "(all sessions on the same blob .bak URL)…"
            )
        else:
            log(f"Stop: no active RESTORE session found for [{db_name}] in DMV.")

        log("Stop: running KILL + 5s wait + DROP DATABASE on SQL Server…")
        cur.execute(_ABORT_BLOB_RESTORE_ON_STOP_SQL, (db_name,))

        cur.execute("SELECT DB_ID(?)", (db_name,))
        still_there = cur.fetchone()
        if still_there and still_there[0] is not None:
            log(f"Stop: [{db_name}] still exists on the instance (DROP may have failed).")
            result["status"] = "partial"
            result["error"] = f"Database {db_name} could not be dropped."
        else:
            log(f"Stop: [{db_name}] removed — restore aborted and storage cleared on SQL Server.")
            result["status"] = "success"

        result["killed_sessions"] = killed_before
        cur.close()
        conn.close()
        return result
    except Exception as exc:
        err = redact_sensitive_text(str(exc))
        result["error"] = err
        log(f"Stop/abort on SQL Server failed: {err}")
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        return result


def run_restore_from_blob(
    server: str,
    database: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
    blob_connection_string: str = "",
    container: str = "",
    blob_path: str = "",
    log_callback: Optional[Any] = None,
    target_managed_instance: bool = False,
    blob_auth_mode: str = "connection_string",
    storage_account_url: str = "",
    progress_callback: Optional[Any] = None,
    cancel_event: Optional[Any] = None,
    on_connect: Optional[Any] = None,
    on_restore_spid: Optional[Callable[[int], None]] = None,
) -> Dict[str, Any]:
    """
    Restore a SQL Server database from one or more .bak stripes in Azure Blob.

    Args:
        server: Target SQL Server instance.
        database: Target database name (will be created or replaced).
        auth: windows | sql.
        user/password: For SQL auth; None for Windows.
        blob_connection_string: Azure Storage connection string (required for
            blob_auth_mode='connection_string').
        container: Blob container name (no default; required unless URL contains it in MI mode).
        blob_path: Path within container to the .bak. May be the single file or any
            one stripe of a striped set; sibling stripes are auto-discovered.
        log_callback: Optional callable(msg) for progress.
        target_managed_instance: If True, omit REPLACE/STATS (required for Azure SQL MI).
        blob_auth_mode: 'connection_string' (default, SAS) or 'managed_identity'.
        storage_account_url: Required for blob_auth_mode='managed_identity'.
        progress_callback: Optional callable(percent: float) for driving a progress bar.

    Returns:
        dict with status, error message if failed.
    """
    import pyodbc  # noqa: F401

    def log(msg: str) -> None:
        safe = redact_sensitive_text(str(msg))
        logger.info(safe)
        if log_callback:
            try:
                log_callback(safe)
            except Exception:
                pass

    result: Dict[str, Any] = {"status": "failed", "error": None}

    try:
        try:
            _b = _import_bak_to_blob_helpers()
            _diagnose_backup_error = _b._diagnose_backup_error
            _mi_credential_sql = _b._mi_credential_sql
            _check_mi_backup_supported = _b._check_mi_backup_supported
        except Exception as e:
            result["error"] = f"Backup module not available (needed for credential/SAS): {e}"
            return result

        prep_err, ctx = _prepare_blob_restore_urls(
            blob_path=blob_path,
            container=container,
            blob_connection_string=blob_connection_string,
            storage_account_url=storage_account_url,
            blob_auth_mode=blob_auth_mode,
            log=log,
        )
        if prep_err:
            return prep_err
        assert ctx is not None
        acct_url = ctx["acct_url"]
        container = ctx["container"]
        credential_name = ctx["credential_name"]
        sas_token = ctx.get("sas_token")
        restore_urls = ctx["restore_urls"]
        result["stripes"] = len(ctx["stripe_paths"])

        try:
            from ..utils.database import connect_to_database, pick_sql_driver
        except ImportError:
            try:
                from src.utils.database import connect_to_database, pick_sql_driver
            except ImportError:
                from utils.database import connect_to_database, pick_sql_driver

        driver = pick_sql_driver(logger)
        conn = connect_to_database(
            server=server,
            db="master",
            user=user or "",
            driver=driver,
            auth=auth or "windows",
            password=password,
            timeout=120,
            logger=logger,
        )
        conn.timeout = 0
        conn.autocommit = True
        cur = conn.cursor()

        if on_connect:
            try:
                on_connect(conn)
            except Exception:
                pass
        _watcher_stop = False
        if cancel_event is not None:
            def _watch_cancel() -> None:
                while not _watcher_stop:
                    if cancel_event.wait(0.5):
                        try:
                            conn.cancel()
                            log("Cancellation requested — aborting RESTORE…")
                        except Exception:
                            pass
                        return

            threading.Thread(target=_watch_cancel, daemon=True).start()

        if blob_auth_mode == "managed_identity":
            _log_sql_engine_context_for_mi_blob(cur, server, log)
            log(
                "RESTORE … FROM URL uses the **SQL host** managed identity from the block above — "
                "grant that identity **Storage Blob Data Reader** (or Contributor) on the storage account if you see OS error 5."
            )

        if blob_auth_mode == "managed_identity" and not target_managed_instance:
            mi_err = _check_mi_backup_supported(cur, log)
            if mi_err:
                result["error"] = redact_sensitive_text(mi_err)
                log(mi_err)
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass
                return result

        _warn_if_restore_in_progress(cur, log, database)
        _ensure_sql_blob_credential(
            cur,
            credential_name=credential_name,
            blob_auth_mode=blob_auth_mode,
            sas_token=sas_token,
            log=log,
            mi_credential_sql=_mi_credential_sql,
            esc_sql=_esc_sql,
        )
        log("Running RESTORE DATABASE … FROM URL …")

        url_clauses = ", ".join(f"URL = N'{_esc_sql(u)}'" for u in restore_urls)
        if target_managed_instance:
            restore_sql = f"RESTORE DATABASE {_q(database)} FROM {url_clauses}"
        else:
            restore_sql = f"RESTORE DATABASE {_q(database)} FROM {url_clauses} WITH REPLACE, STATS = 5"

        # Start a best-effort progress monitor on a second connection (polls percent_complete).
        stop_event = threading.Event()
        monitor: Optional[threading.Thread] = None
        monitor_connect_kwargs = dict(
            server=server,
            db="master",
            user=user or "",
            driver=driver,
            auth=auth or "windows",
            password=password,
            timeout=120,
            logger=logger,
        )
        spid = _get_spid(cur)
        if spid and on_restore_spid:
            try:
                on_restore_spid(int(spid))
            except Exception:
                pass
        if spid:
            monitor = threading.Thread(
                target=_monitor_request_progress,
                kwargs=dict(
                    connect_kwargs=monitor_connect_kwargs,
                    database=database,
                    odbc_spid=spid,
                    log=log,
                    stop_event=stop_event,
                    poll_sec=5.0,
                    progress_callback=progress_callback,
                ),
                daemon=True,
            )
            log(
                f"Monitoring restore progress (ODBC SPID {spid}; "
                "status from blob URL parent/child DMV query)…"
            )
            monitor.start()

        t0 = time.perf_counter()
        try:
            cur.execute(restore_sql)
            while True:
                try:
                    for _ in cur.fetchall():
                        pass
                except Exception:
                    pass
                if not cur.nextset():
                    break
        except Exception as restore_error:
            _watcher_stop = True
            if cancel_event is not None and cancel_event.is_set():
                result["status"] = "cancelled"
                result["error"] = "Restore cancelled by user."
                log("Restore cancelled by user.")
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass
                return result
            err_text = str(restore_error)
            resolved = _resolve_restore_after_odbc_error(
                monitor_connect_kwargs, database, err_text, log
            )
            if resolved == "success":
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass
                result["status"] = "success"
                log(f"Restore completed. Database: {database}")
                return result
            if resolved == "client_lost":
                result["status"] = "client_lost"
                result["error"] = redact_sensitive_text(err_text)
                try:
                    cur.close()
                    conn.close()
                except Exception:
                    pass
                return result
            raise restore_error
        finally:
            stop_event.set()
            if monitor is not None:
                monitor.join(timeout=6)
        _watcher_stop = True
        elapsed = time.perf_counter() - t0
        log(f"RESTORE command completed in {elapsed:.1f} s")

        cur.close()
        conn.close()
        result["status"] = "success"
        log(f"Restore completed. Database: {database}")
        return result
    except Exception as e:
        if cancel_event is not None and cancel_event.is_set():
            result["status"] = "cancelled"
            result["error"] = "Restore cancelled by user."
            log("Restore cancelled by user.")
            return result
        err_text = str(e)
        try:
            if getattr(e, "args", None):
                log(f"(ODBC/SQL raw exception.args) {e.args!r}")
        except Exception:
            pass
        verify_kw = locals().get("monitor_connect_kwargs")
        if verify_kw:
            resolved = _resolve_restore_after_odbc_error(
                verify_kw, database, err_text, log
            )
            if resolved == "success":
                result["status"] = "success"
                log(f"Restore completed. Database: {database}")
                return result
            if resolved == "client_lost":
                result["status"] = "client_lost"
                result["error"] = redact_sensitive_text(err_text)
                return result
        acct_url_hint = locals().get("acct_url") or ""
        container_hint = locals().get("container") or ""
        try:
            diagnostic = _diagnose_backup_error(
                err_text,
                blob_auth_mode=blob_auth_mode,
                storage_account_url=acct_url_hint,
                container_name=container_hint,
            )
        except Exception:
            diagnostic = ""
        result["error"] = redact_sensitive_text(err_text)
        if diagnostic:
            result["diagnostic"] = diagnostic.strip()
        log(f"Restore failed: {err_text}")
        if "hyt00" in err_text.lower() or "query timeout expired" in err_text.lower():
            log(
                "ODBC reported a query timeout. A large RESTORE may still be running on the SQL "
                "instance — check Azure portal activity or sys.dm_exec_requests before retrying. "
                "Retrying immediately can fail with 'credential already exists' (15530)."
            )
        if diagnostic:
            log(diagnostic.strip())
        return result
