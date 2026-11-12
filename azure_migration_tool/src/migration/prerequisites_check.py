# Author: Satish Chauhan
"""Quick prerequisite sanity (connectivity + storage auth only — no migration)."""

from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar

T = TypeVar("T")

ROW_PREREQ_TIMEOUT_SEC = 120
_BLOB_SQL_PROBE_TIMEOUT_SEC = 90
_UNC_BACKUP_PROBE_TIMEOUT_SEC = 90

LogFn = Callable[[str], None]


@dataclass
class PrerequisiteRowResult:
    label: str
    src_ok: bool
    dest_ok: bool
    src_blob_ok: bool
    dest_blob_ok: bool
    src_detail: str = ""
    dest_detail: str = ""
    dest_db_exists: bool = False
    src_blob_detail: str = ""
    dest_blob_detail: str = ""
    unc_ok: Optional[bool] = None
    unc_detail: str = ""
    warnings: List[str] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        if not (self.src_ok and self.dest_ok and self.dest_blob_ok):
            return False
        if self.unc_ok is False:
            return False
        if not self.src_blob_ok:
            return False
        return True

    # Back-compat for older UI references
    @property
    def blob_ok(self) -> bool:
        return self.src_blob_ok and self.dest_blob_ok

    @property
    def blob_detail(self) -> str:
        return f"Src: {self.src_blob_detail} | Dest: {self.dest_blob_detail}"


def _norm_auth(auth: Optional[str]) -> str:
    return (auth or "windows").strip().lower() or "windows"


def _norm_blob_mode(mode: Optional[str]) -> str:
    m = (mode or "connection_string").strip().lower()
    if m in ("mi", "managed_identity", "managed identity"):
        return "managed_identity"
    return "connection_string"


def _blob_fields(row: Dict[str, Any]) -> Dict[str, str]:
    return {
        "blob_auth_mode": str(row.get("blob_auth_mode") or "connection_string"),
        "blob_connection_string": str(row.get("blob_connection_string") or ""),
        "blob_account_url": str(row.get("blob_account_url") or ""),
        "blob_container": str(row.get("blob_container") or ""),
    }


_QUICK_ODBC_TIMEOUT = 20


def _run_with_timeout(seconds: float, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Optional[T]:
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn, *args, **kwargs)
        try:
            return fut.result(timeout=seconds)
        except FuturesTimeoutError:
            return None


def _unc_path_candidates(raw_path: str) -> List[str]:
    """UNC path and parent folders down to \\\\server\\share."""
    norm = os.path.normpath(os.path.expanduser((raw_path or "").strip()))
    if not norm:
        return []
    seen: set[str] = set()
    out: List[str] = []
    cur = norm
    while cur and cur not in seen:
        seen.add(cur)
        out.append(cur)
        if not (cur.startswith("\\\\") or cur.startswith("//")):
            parent = os.path.dirname(cur)
            if parent == cur:
                break
            cur = parent
            continue
        parts = cur.replace("/", "\\").split("\\")
        # ['', '', 'server', 'share', ...]
        if len(parts) <= 4:
            break
        cur = "\\".join(parts[:-1])
    return out


def test_sql_connection(
    role: str,
    server: str,
    database: str,
    auth: str,
    user: str,
    password: Optional[str],
    log: LogFn,
) -> tuple[bool, str]:
    """Lightweight ODBC ping (SELECT 1) — not a migration or backup."""
    server = (server or "").strip()
    database = (database or "").strip()
    if not server or not database:
        return False, f"{role}: server or database is empty"

    auth_l = _norm_auth(auth)
    if auth_l == "entra_mfa" and not (user or "").strip():
        return False, f"{role}: UPN required for Entra MFA"
    if auth_l == "sql" and (not user or not password):
        return False, f"{role}: SQL login and password required"

    try:
        import pyodbc
        from ..utils.database import build_conn_str, connect_to_database, pick_sql_driver
    except ImportError:
        from src.utils.database import build_conn_str, connect_to_database, pick_sql_driver

    import logging

    temp_log = logging.getLogger("prereq_sql")
    driver = pick_sql_driver(temp_log)
    log(f"{role}: quick connect {server} / {database} ({auth_l})...")
    try:
        if auth_l == "entra_mfa":
            conn = connect_to_database(
                server,
                database,
                user.strip(),
                driver,
                auth_l,
                None,
                timeout=_QUICK_ODBC_TIMEOUT,
                logger=temp_log,
            )
        else:
            conn_str = build_conn_str(server, database, user or "", driver, auth_l, password)
            conn = pyodbc.connect(conn_str, timeout=_QUICK_ODBC_TIMEOUT)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.execute("SELECT DB_NAME(), SUSER_SNAME()")
        row = cur.fetchone()
        cur.close()
        conn.close()
        return True, f"OK: {row[0]}, login={row[1]}"
    except Exception as ex:
        return False, str(ex)[:400]


def test_dest_server(
    server: str,
    dest_db: str,
    auth: str,
    user: str,
    password: Optional[str],
    log: LogFn,
) -> tuple[bool, str, List[str], bool]:
    """Destination: server must be reachable (master). Target DB may not exist yet."""
    warnings: List[str] = []
    dest_exists = False
    if not (server or "").strip():
        return False, "Destination server is empty", warnings, False

    auth_l = _norm_auth(auth)
    if auth_l == "entra_mfa" and not (user or "").strip():
        return False, "UPN/email required for Entra MFA on destination", warnings, False

    try:
        import pyodbc
        from ..utils.database import build_conn_str, connect_to_database, pick_sql_driver
    except ImportError:
        from src.utils.database import build_conn_str, connect_to_database, pick_sql_driver

    import logging

    temp_log = logging.getLogger("prereq_dest")
    driver = pick_sql_driver(temp_log)
    log(f"Destination: connecting to server {server} (master)...")
    try:
        if auth_l == "entra_mfa":
            conn = connect_to_database(
                server.strip(),
                "master",
                user.strip(),
                driver,
                auth_l,
                None,
                timeout=_QUICK_ODBC_TIMEOUT,
                logger=temp_log,
            )
        else:
            conn_str = build_conn_str(server.strip(), "master", user or "", driver, auth_l, password)
            conn = pyodbc.connect(conn_str, timeout=_QUICK_ODBC_TIMEOUT)
        cur = conn.cursor()
        cur.execute("SELECT @@VERSION")
        ver = (cur.fetchone()[0] or "")[:80]
        db_name = (dest_db or "").strip()
        if db_name:
            cur.execute("SELECT database_id FROM sys.databases WHERE name = ?", db_name)
            row = cur.fetchone()
            if row:
                dest_exists = True
                msg = f"Server OK. Database '{db_name}' already exists on destination."
                warnings.append(f"Destination database '{db_name}' already exists (migration may overwrite or skip).")
                log(f"[WARN] {msg}")
            else:
                msg = f"Server OK. Database '{db_name}' not present yet (OK for new migration)."
                log(msg)
        else:
            msg = f"Server reachable on master | {ver}..."
            log(msg)
        cur.close()
        conn.close()
        return True, msg, warnings, dest_exists
    except Exception as ex:
        return False, str(ex)[:400], warnings, False


def test_blob_container_quick(row: Dict[str, Any], log: LogFn) -> tuple[bool, str]:
    """Container metadata only from this PC — no upload, restore, or SQL RESTORE."""
    bf = _blob_fields(row)
    container = (bf["blob_container"] or "").strip()
    if not container:
        return False, "Blob container not configured"
    try:
        from ..backup.local_backup_and_upload import _get_tool_blob_service_client
    except ImportError:
        from src.backup.local_backup_and_upload import _get_tool_blob_service_client

    log("Storage: quick container auth check (no data movement)...")
    try:
        client = _get_tool_blob_service_client(
            blob_auth_mode=_norm_blob_mode(bf["blob_auth_mode"]),
            blob_connection_string=bf["blob_connection_string"],
            blob_account_url=bf["blob_account_url"],
            container=container,
            log=log,
            verbose_log=False,
        )
        cc = client.get_container_client(container)
        props = cc.get_container_properties()
        lm = props.get("last_modified")
        return True, f"Container '{container}' reachable (last_modified={lm})"
    except Exception as ex:
        return False, str(ex)[:400]


def _backup_dir_from_path(raw_path: str) -> str:
    norm = os.path.normpath(os.path.expanduser((raw_path or "").strip()))
    if norm.lower().endswith(".bak"):
        return os.path.dirname(norm)
    return norm


def _bracket_db_name(name: str) -> str:
    n = (name or "").strip()
    return n.replace("]", "]]")


def _sql_path_is_backup_file(
    server: str,
    auth: str,
    user: str,
    password: Optional[str],
    file_path: str,
) -> bool:
    try:
        from ..utils.database import connect_to_database, pick_sql_driver
    except ImportError:
        from src.utils.database import connect_to_database, pick_sql_driver

    import logging

    temp_log = logging.getLogger("prereq_unc_poll")
    driver = pick_sql_driver(temp_log)
    auth_l = _norm_auth(auth)
    conn = connect_to_database(
        server.strip(),
        "master",
        (user or "").strip(),
        driver,
        auth_l,
        password,
        timeout=_QUICK_ODBC_TIMEOUT,
        logger=temp_log,
    )
    cur = conn.cursor()
    cur.execute("IF OBJECT_ID('tempdb..#f') IS NOT NULL DROP TABLE #f")
    cur.execute("CREATE TABLE #f (FileExists int, FileIsDirectory int, ParentExists int)")
    cur.execute("INSERT #f EXEC master.dbo.xp_fileexist ?", file_path)
    cur.execute("SELECT FileExists, FileIsDirectory FROM #f")
    fr = cur.fetchone()
    cur.execute("IF OBJECT_ID('tempdb..#f') IS NOT NULL DROP TABLE #f")
    cur.close()
    conn.close()
    return bool(fr and int(fr[0]) == 1 and int(fr[1]) == 0)


def test_src_sql_unc(
    server: str,
    database: str,
    auth: str,
    user: str,
    password: Optional[str],
    unc_path: str,
    log: LogFn,
) -> tuple[Optional[bool], str, List[str]]:
    """Start COPY_ONLY backup to UNC; cancel as soon as SQL sees the .bak file (write confirmed)."""
    raw = (unc_path or "").strip()
    if not raw:
        return None, "Not configured", []
    if not (server or "").strip():
        return False, "Source server empty", ["UNC set but source server missing"]
    db = (database or "").strip()
    if not db:
        return False, "Source database required", ["Set source database for UNC backup probe"]

    folder = _backup_dir_from_path(raw)
    warnings: List[str] = []
    log(f"Source SQL → UNC backup probe: {folder}")

    try:
        os.makedirs(folder, exist_ok=True)
        log("Ensured backup folder from this PC (when visible).")
    except OSError as ex:
        log(f"Note: could not create folder from PC ({ex}); SQL Server will attempt backup anyway.")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_db = re.sub(r"[^A-Za-z0-9._-]+", "_", db)[:64]
    probe_bak = os.path.join(folder, f"_amt_unc_probe_{safe_db}_{ts}.bak")

    def _backup_probe() -> tuple[bool, str]:
        cancel_ev = threading.Event()
        holder: Dict[str, Any] = {"conn": None, "err": None}

        def _poll_and_cancel() -> None:
            until = time.monotonic() + _UNC_BACKUP_PROBE_TIMEOUT_SEC
            while time.monotonic() < until and not cancel_ev.is_set():
                try:
                    if _sql_path_is_backup_file(server, auth, user, password, probe_bak):
                        log("Probe .bak visible to SQL — canceling BACKUP.")
                        cancel_ev.set()
                        conn = holder.get("conn")
                        if conn is not None:
                            try:
                                conn.cancel()
                            except Exception:
                                pass
                        return
                except Exception:
                    pass
                time.sleep(0.4)

        def _run_backup() -> None:
            try:
                try:
                    from ..utils.database import connect_to_database, pick_sql_driver
                except ImportError:
                    from src.utils.database import connect_to_database, pick_sql_driver

                import logging

                temp_log = logging.getLogger("prereq_unc_backup")
                driver = pick_sql_driver(temp_log)
                auth_l = _norm_auth(auth)
                conn = connect_to_database(
                    server.strip(),
                    db,
                    (user or "").strip(),
                    driver,
                    auth_l,
                    password,
                    timeout=_QUICK_ODBC_TIMEOUT,
                    logger=temp_log,
                )
                conn.autocommit = True
                holder["conn"] = conn
                cur = conn.cursor()
                disk_literal = probe_bak.replace("'", "''")
                db_br = _bracket_db_name(db)
                sql = (
                    f"BACKUP DATABASE [{db_br}] TO DISK = N'{disk_literal}' "
                    f"WITH INIT, COPY_ONLY, CHECKSUM, STATS = 5"
                )
                log(f"Starting probe BACKUP (cancel when file appears): {probe_bak}")
                try:
                    cur.execute(sql)
                    while cur.nextset():
                        pass
                except Exception as ex:
                    holder["err"] = ex
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception as ex:
                holder["err"] = ex

        poll_t = threading.Thread(target=_poll_and_cancel, daemon=True)
        backup_t = threading.Thread(target=_run_backup, daemon=True)
        poll_t.start()
        backup_t.start()
        backup_t.join(timeout=_UNC_BACKUP_PROBE_TIMEOUT_SEC + 5)
        cancel_ev.set()

        if _sql_path_is_backup_file(server, auth, user, password, probe_bak):
            log("UNC write confirmed (probe .bak on share).")
            return True, "Write confirmed (backup canceled)"

        err = holder.get("err")
        if err is not None:
            msg = str(err)[:400]
            if _sql_path_is_backup_file(server, auth, user, password, probe_bak):
                return True, "Write confirmed (backup canceled)"
            return False, msg
        return False, "No probe .bak on UNC before timeout"

    outcome = _run_with_timeout(_UNC_BACKUP_PROBE_TIMEOUT_SEC, _backup_probe)
    if outcome is None:
        return False, "UNC backup probe timed out", [f"Timed out after {_UNC_BACKUP_PROBE_TIMEOUT_SEC}s"]
    ok, detail = outcome
    if not ok:
        return False, detail, [detail]

    try:
        if os.path.isfile(probe_bak):
            os.remove(probe_bak)
            log("Removed probe .bak from share.")
    except OSError as ex:
        warnings.append(f"Probe backup succeeded but could not delete {probe_bak}: {ex}")
        log(f"[WARN] {warnings[-1]}")
        return True, "WARN", warnings

    return True, detail, warnings


def _find_probe_bak_blob(row: Dict[str, Any], log: LogFn) -> Optional[str]:
    bf = _blob_fields(row)
    container = (bf["blob_container"] or "").strip()
    if not container:
        return None
    try:
        from ..backup.local_backup_and_upload import _get_tool_blob_service_client
    except ImportError:
        from src.backup.local_backup_and_upload import _get_tool_blob_service_client

    try:
        client = _get_tool_blob_service_client(
            blob_auth_mode=_norm_blob_mode(bf["blob_auth_mode"]),
            blob_connection_string=bf["blob_connection_string"],
            blob_account_url=bf["blob_account_url"],
            container=container,
            log=lambda _m: None,
            verbose_log=False,
        )
        cc = client.get_container_client(container)
        prefix = (row.get("blob_probe_prefix") or "").strip()
        for blob in cc.list_blobs(name_starts_with=prefix):
            name = getattr(blob, "name", "") or ""
            if name.lower().endswith(".bak"):
                log(f"Probe blob for SQL read test: {name}")
                return name
    except Exception as ex:
        log(f"Could not list blobs: {ex}")
    return None


def test_sql_blob_access(
    role: str,
    server: str,
    auth: str,
    user: str,
    password: Optional[str],
    row: Dict[str, Any],
    blob_path: str,
    *,
    target_managed_instance: bool = False,
    log: LogFn,
) -> tuple[bool, str]:
    try:
        from ..restore.restore_from_blob import run_test_blob_headeronly_via_sql_odbc
    except ImportError:
        from src.restore.restore_from_blob import run_test_blob_headeronly_via_sql_odbc

    bf = _blob_fields(row)

    def cb(msg: str) -> None:
        log(f"  {role}: {msg}")

    result = run_test_blob_headeronly_via_sql_odbc(
        server=server,
        auth=_norm_auth(auth),
        user=user,
        password=password,
        blob_connection_string=bf["blob_connection_string"],
        container=bf["blob_container"],
        blob_path=blob_path,
        log_callback=cb,
        target_managed_instance=target_managed_instance,
        blob_auth_mode=_norm_blob_mode(bf["blob_auth_mode"]),
        storage_account_url=bf["blob_account_url"],
    )
    if (result.get("status") or "").lower() == "success":
        return True, f"{role} SQL can read blob (HEADERONLY)."
    err = (result.get("error") or result.get("message") or "SQL blob test failed").strip()
    diag = (result.get("diagnostic") or "").strip()
    if diag:
        err = f"{err} — {diag[:200]}"
    return False, err[:400]


def test_unc_to_blob(
    row: Dict[str, Any],
    backup_path: str,
    src_sql_unc: Optional[bool],
    log: LogFn,
) -> tuple[bool, str]:
    """UNC backup folder + configured blob (upload/landing path)."""
    storage_ok, storage_msg = test_blob_container_quick(row, log)
    if not storage_ok:
        return False, storage_msg
    raw = (backup_path or "").strip()
    if not raw:
        log("UNC→Blob: blob only (no UNC path on row).")
        return True, "Blob container OK"
    if src_sql_unc is not True:
        return False, "Source UNC backup probe must pass before UNC→Blob"
    log("UNC→Blob: probe .bak path + blob container.")
    return True, "SQL backup path + blob"


def test_dest_sql_blob(row: Dict[str, Any], log: LogFn) -> tuple[bool, str]:
    """Destination SQL host can read blob (HEADERONLY on a probe .bak)."""
    probe = _find_probe_bak_blob(row, log)
    if not probe:
        log("[FAIL] No .bak in container — cannot verify destination SQL→blob.")
        return False, "No .bak in container for SQL blob probe"

    def _headeronly() -> tuple[bool, str]:
        return test_sql_blob_access(
            "Destination",
            str(row.get("dest_server") or ""),
            str(row.get("dest_auth") or "entra_mfa"),
            str(row.get("dest_user") or ""),
            row.get("dest_password"),
            row,
            probe,
            target_managed_instance=bool(row.get("dest_managed_instance") or row.get("target_managed_instance")),
            log=log,
        )

    out = _run_with_timeout(_BLOB_SQL_PROBE_TIMEOUT_SEC, _headeronly)
    if out is None:
        return False, f"Timed out after {_BLOB_SQL_PROBE_TIMEOUT_SEC}s (dest SQL→blob)"
    return out


def run_row_prerequisites_bounded(
    row: Dict[str, Any],
    log: Optional[LogFn] = None,
    *,
    timeout_sec: int = ROW_PREREQ_TIMEOUT_SEC,
) -> PrerequisiteRowResult:
    """Run row checks with a hard wall-clock timeout so the UI does not hang."""
    out = _run_with_timeout(timeout_sec, run_row_prerequisites, row, log)
    if out is not None:
        return out
    label = (
        str(row.get("row_label") or row.get("label") or "").strip()
        or f"{row.get('src_db', '?')} → {row.get('dest_db', '?')}"
    )
    msg = f"Prerequisite check timed out after {timeout_sec}s"
    return PrerequisiteRowResult(
        label=label,
        src_ok=False,
        dest_ok=False,
        src_blob_ok=False,
        dest_blob_ok=False,
        src_detail=msg,
        dest_detail=msg,
        src_blob_detail=msg,
        dest_blob_detail=msg,
        unc_ok=False,
        unc_detail=msg,
        warnings=[msg],
        logs=[msg],
    )


def merge_row_with_defaults(row: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(defaults)
    for k, v in row.items():
        if v is not None and str(v).strip():
            out[k] = v
    return out


def run_row_prerequisites(row: Dict[str, Any], log: Optional[LogFn] = None) -> PrerequisiteRowResult:
    _log = log or (lambda _m: None)
    label = (
        str(row.get("row_label") or row.get("label") or "").strip()
        or f"{row.get('src_db', '?')} → {row.get('dest_db', '?')}"
    )
    lines: List[str] = []
    warnings: List[str] = []

    def inner(msg: str) -> None:
        lines.append(msg)
        _log(msg)

    inner(f"=== {label} ===")
    inner("Quick sanity only — verifies connectivity/storage; does not migrate data.")

    src_ok, src_msg = test_sql_connection(
        "Source",
        str(row.get("src_server") or ""),
        str(row.get("src_db") or ""),
        str(row.get("src_auth") or "windows"),
        str(row.get("src_user") or ""),
        row.get("src_password"),
        inner,
    )

    dest_ok, dest_msg, dest_warnings, dest_db_exists = test_dest_server(
        str(row.get("dest_server") or ""),
        str(row.get("dest_db") or ""),
        str(row.get("dest_auth") or "entra_mfa"),
        str(row.get("dest_user") or ""),
        row.get("dest_password"),
        inner,
    )
    warnings.extend(dest_warnings)

    backup_path = (
        str(row.get("backup_unc_path") or row.get("local_backup_path") or row.get("backup_path") or "").strip()
    )
    unc_ok, unc_detail, unc_warnings = test_src_sql_unc(
        str(row.get("src_server") or ""),
        str(row.get("src_db") or ""),
        str(row.get("src_auth") or "windows"),
        str(row.get("src_user") or ""),
        row.get("src_password"),
        backup_path,
        inner,
    )
    warnings.extend(unc_warnings)

    inner("--- UNC → blob ---")
    unc_blob_ok, unc_blob_msg = test_unc_to_blob(row, backup_path, unc_ok, inner)
    src_blob_ok = unc_blob_ok
    src_blob_msg = unc_blob_msg

    inner("--- Destination SQL → blob ---")
    dest_blob_ok, dest_blob_msg = test_dest_sql_blob(row, inner)

    if src_ok and dest_ok and unc_blob_ok and dest_blob_ok and (unc_ok is None or unc_ok):
        inner("Sanity OK — migration prerequisites look reachable.")

    inner(
        f"Summary: src={'OK' if src_ok else 'FAIL'} dest={'OK' if dest_ok else 'FAIL'} "
        f"src_unc={'—' if unc_ok is None else ('OK' if unc_ok else 'FAIL')} "
        f"unc_blob={'OK' if unc_blob_ok else 'FAIL'} dest_blob={'OK' if dest_blob_ok else 'FAIL'} "
        f"dest_db={'EXISTS' if dest_db_exists else 'new'} warnings={len(warnings)}"
    )
    for w in warnings:
        inner(f"[WARN] {w}")

    return PrerequisiteRowResult(
        label=label,
        src_ok=src_ok,
        dest_ok=dest_ok,
        src_blob_ok=src_blob_ok,
        dest_blob_ok=dest_blob_ok,
        src_detail=src_msg,
        dest_detail=dest_msg,
        dest_db_exists=dest_db_exists,
        src_blob_detail=src_blob_msg,
        dest_blob_detail=dest_blob_msg,
        unc_ok=unc_ok,
        unc_detail=unc_detail,
        warnings=warnings,
        logs=lines,
    )


def run_workstation_baseline(log: Optional[LogFn] = None) -> List[tuple[str, bool, str]]:
    """Client tools: ODBC, driver, pyodbc, AzCopy (non-blocking warnings)."""
    _log = log or (lambda _m: None)
    items: List[tuple[str, bool, str]] = []
    try:
        from .bcp_preflight import (
            _check_bcp,
            _check_msal_for_mfa,
            _check_odbc_driver,
            _check_pyodbc,
        )
    except ImportError:
        from src.migration.bcp_preflight import (
            _check_bcp,
            _check_msal_for_mfa,
            _check_odbc_driver,
            _check_pyodbc,
        )

    for item in (
        _check_pyodbc(_log),
        _check_odbc_driver(_log),
        _check_bcp(_log, install_if_missing=False),
        _check_msal_for_mfa({}),
    ):
        items.append((f"{item.category}/{item.name}", item.passed, item.message))
        _log(f"{'[OK]' if item.passed else '[FAIL]'} {item.category}/{item.name}: {item.message}")
    return items
