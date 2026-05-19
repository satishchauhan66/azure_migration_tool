# Author: Satish Chauhan

"""Pre-flight checks before BCP data migration."""

from __future__ import annotations

import logging
import os
import shutil
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

LogFn = Callable[[str], None]


@dataclass
class PreflightItem:
    category: str
    name: str
    passed: bool
    message: str
    blocking: bool = True  # if False, warning only


def _check_pyodbc(log: LogFn) -> PreflightItem:
    try:
        import pyodbc  # noqa: F401
        return PreflightItem("Client", "Python pyodbc", True, "pyodbc is installed", True)
    except ImportError:
        return PreflightItem(
            "Client", "Python pyodbc", False, "pip install pyodbc", True
        )


def _check_odbc_driver(log: LogFn) -> PreflightItem:
    try:
        import pyodbc

        drivers = pyodbc.drivers()
        for name in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
            if name in drivers:
                return PreflightItem("Client", "ODBC driver", True, name, True)
        return PreflightItem(
            "Client",
            "ODBC driver",
            False,
            f"No SQL ODBC 17/18 driver. Found: {', '.join(drivers) or 'none'}",
            True,
        )
    except Exception as e:
        return PreflightItem("Client", "ODBC driver", False, str(e), True)


def _check_bcp(log: LogFn, *, install_if_missing: bool) -> PreflightItem:
    try:
        from ..utils.bcp_tools import ensure_bcp_installed, find_bcp_exe

        path = find_bcp_exe()
        if path:
            return PreflightItem("Client", "BCP (bcp.exe)", True, path, True)
        if install_if_missing:
            log("BCP not found — attempting install from bundled/downloaded MSI...")
            ok, msg = ensure_bcp_installed(log, allow_download=True)
            if ok:
                return PreflightItem("Client", "BCP (bcp.exe)", True, msg, True)
            return PreflightItem("Client", "BCP (bcp.exe)", False, msg, True)
        return PreflightItem(
            "Client",
            "BCP (bcp.exe)",
            False,
            "Not found. Use Validate again with install, or run the full installer.",
            True,
        )
    except Exception as e:
        return PreflightItem("Client", "BCP (bcp.exe)", False, str(e), True)


def _check_bcp_entra_dest_on_windows(cfg: Dict[str, Any]) -> PreflightItem:
    dest_auth = (cfg.get("dest_auth") or "").strip().lower()
    if dest_auth != "entra_mfa" or not sys.platform.startswith("win"):
        return PreflightItem(
            "Auth",
            "BCP + Entra (Windows)",
            True,
            "Not applicable",
            False,
        )
    try:
        from ..utils.bcp_tools import bcp_entra_token_via_cli_supported, find_bcp_exe

        bcp18 = find_bcp_exe(prefer_odbc_18=True)
        if bcp_entra_token_via_cli_supported():
            return PreflightItem(
                "Auth",
                "BCP + Entra (Windows)",
                True,
                "Token-based BCP supported on this platform",
                False,
            )
        detail = (
            "Windows bcp cannot use cached Entra tokens; migration will create a temporary "
            "SQL login on the destination for BCP import (using your Entra connection)."
        )
        if bcp18:
            detail += f" Using {bcp18}."
        else:
            detail += " Install ODBC 18 / SQL Command Line Utilities (bcp 18) if import fails."
        return PreflightItem("Auth", "BCP + Entra (Windows)", True, detail, False)
    except Exception as e:
        return PreflightItem("Auth", "BCP + Entra (Windows)", False, str(e)[:300], False)


def _check_msal_for_mfa(cfg: Dict[str, Any]) -> PreflightItem:
    dest_auth = (cfg.get("dest_auth") or "").strip().lower()
    src_auth = (cfg.get("src_auth") or "").strip().lower()
    if dest_auth != "entra_mfa" and src_auth != "entra_mfa":
        return PreflightItem(
            "Auth", "MSAL (Entra MFA)", True, "Not required for current auth modes", False
        )
    try:
        import msal  # noqa: F401

        return PreflightItem("Auth", "MSAL (Entra MFA)", True, "msal installed", True)
    except ImportError:
        return PreflightItem(
            "Auth", "MSAL (Entra MFA)", False, "pip install msal", True
        )


# BCP native (-n) exports logical row data; compressed/heaped SQL pages are often much smaller.
_BCP_NATIVE_FACTOR_UNCOMPRESSED = 1.45
_BCP_NATIVE_FACTOR_COMPRESSED = 3.25
_BCP_SAMPLE_MIN_ROWS = 25_000
_BCP_SAMPLE_MAX_ROWS = 150_000
_BCP_SAMPLE_PCT = 0.05  # 5% of rows, within min/max


@dataclass
class TableBcpSize:
    fqn: str
    row_count: int
    sql_reserved_gb: float
    sql_used_gb: float
    compressed: bool
    bcp_estimated_gb: float
    method: str  # "bcp_sample" | "sql_heuristic"


@dataclass
class BcpDiskEstimate:
    """Disk space guidance for BCP (one .dat at a time ≈ largest table in scope)."""

    table_count: int
    scope_sql_gb: float
    scope_bcp_gb: float
    largest_table: str
    largest_table_sql_gb: float
    largest_table_bcp_gb: float
    largest_table_method: str
    recommended_free_gb: float
    work_root: str
    work_root_free_gb: float
    sufficient: bool
    summary: str
    top_tables: List[TableBcpSize]

    @property
    def largest_table_gb(self) -> float:
        """Peak .dat size (BCP estimate), not SQL reserved."""
        return self.largest_table_bcp_gb


def _connect_source_for_estimate(cfg: Dict[str, Any]):
    import pyodbc
    from ..utils.database import build_conn_str, connect_to_database, pick_sql_driver

    temp_log = logging.getLogger("bcp_disk_estimate")
    driver = pick_sql_driver(temp_log)
    auth_l = (cfg.get("src_auth") or "").strip().lower()
    server = (cfg.get("src_server") or "").strip()
    db = (cfg.get("src_db") or "").strip()
    user = (cfg.get("src_user") or "").strip()
    if auth_l == "entra_mfa":
        return connect_to_database(
            server, db, user, driver, auth_l, None, timeout=120, logger=temp_log
        )
    return pyodbc.connect(
        build_conn_str(server, db, user, driver, auth_l, cfg.get("src_password")),
        timeout=120,
    )


def _fetch_table_size_details(cfg: Dict[str, Any]) -> List[TableBcpSize]:
    """
    Per-table size from sys.dm_db_partition_stats (heap/clustered only).

    SQL reserved/used reflects on-disk pages (often compressed). BCP native export
    uses logical row width, so we apply heuristics and optional sampling.
    """
    conn = _connect_source_for_estimate(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                s.name + '.' + t.name,
                ISNULL(SUM(CAST(ps.row_count AS BIGINT)), 0),
                ISNULL(SUM(CAST(ps.reserved_page_count AS BIGINT)), 0) * 8.0
                    / 1073741824.0,
                ISNULL(SUM(CAST(ps.used_page_count AS BIGINT)), 0) * 8.0
                    / 1073741824.0,
                MAX(CASE WHEN p.data_compression_desc <> N'NONE' THEN 1 ELSE 0 END)
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.indexes i
                ON i.object_id = t.object_id AND i.index_id IN (0, 1)
            JOIN sys.partitions p
                ON p.object_id = i.object_id AND p.index_id = i.index_id
            JOIN sys.dm_db_partition_stats ps
                ON ps.object_id = p.object_id
               AND ps.index_id = p.index_id
               AND ps.partition_number = p.partition_number
            WHERE t.is_ms_shipped = 0
            GROUP BY s.name, t.name
            """
        )
        out: List[TableBcpSize] = []
        for row in cur.fetchall():
            fqn = row[0]
            rows = int(row[1] or 0)
            reserved_gb = float(row[2] or 0)
            used_gb = float(row[3] or 0)
            compressed = bool(row[4])
            base = max(reserved_gb, used_gb)
            factor = (
                _BCP_NATIVE_FACTOR_COMPRESSED
                if compressed
                else _BCP_NATIVE_FACTOR_UNCOMPRESSED
            )
            heuristic_gb = base * factor
            out.append(
                TableBcpSize(
                    fqn=fqn,
                    row_count=rows,
                    sql_reserved_gb=reserved_gb,
                    sql_used_gb=used_gb,
                    compressed=compressed,
                    bcp_estimated_gb=heuristic_gb,
                    method="sql_heuristic",
                )
            )
        return out
    finally:
        conn.close()


def _bcp_source_auth_args(cfg: Dict[str, Any]) -> List[str]:
    auth = (cfg.get("src_auth") or "").strip().lower()
    args = ["-S", (cfg.get("src_server") or "").strip(), "-d", (cfg.get("src_db") or "").strip()]
    if auth == "windows":
        args.append("-T")
    elif auth == "sql":
        args.extend(["-U", cfg.get("src_user") or "", "-P", cfg.get("src_password") or ""])
    elif auth in ("entra_mfa", "entra_password"):
        args.extend(["-G", "-U", cfg.get("src_user") or ""])
        if cfg.get("src_password"):
            args.extend(["-P", cfg["src_password"]])
    else:
        args.append("-T")
    return args


def _sample_rows_for_table(row_count: int) -> int:
    if row_count <= 0:
        return 0
    if row_count <= _BCP_SAMPLE_MIN_ROWS:
        return row_count
    pct = max(_BCP_SAMPLE_MIN_ROWS, int(row_count * _BCP_SAMPLE_PCT))
    return min(pct, _BCP_SAMPLE_MAX_ROWS, row_count)


def _sample_table_bcp_gb(
    cfg: Dict[str, Any],
    bcp_exe: str,
    table: TableBcpSize,
    work_root: str,
    log: LogFn,
) -> Optional[float]:
    """Run a small BCP queryout and extrapolate full-table .dat size."""
    import os
    import tempfile

    from ..utils.bcp_tools import bcp_query_for_export, ensure_bcp_output_file, format_bcp_data_path

    try:
        from ..utils.subprocess_utils import run_silent
    except ImportError:
        import subprocess

        run_silent = subprocess.run  # type: ignore[assignment]

    schema, tname = table.fqn.split(".", 1)
    sample_n = _sample_rows_for_table(table.row_count)
    if sample_n <= 0:
        return 0.0

    sample_dir = tempfile.mkdtemp(prefix="bcp_size_sample_", dir=work_root)
    data_file = format_bcp_data_path(os.path.join(sample_dir, f"{schema}_{tname}_sample.dat"))
    query = bcp_query_for_export(schema, tname).replace(
        "SELECT *", f"SELECT TOP ({sample_n}) *", 1
    )
    cmd = [
        bcp_exe,
        query,
        "queryout",
        data_file,
        *_bcp_source_auth_args(cfg),
        "-n",
        "-q",
    ]
    log(f"  Sampling {table.fqn}: TOP {sample_n:,} rows for BCP size calibration...")
    try:
        ensure_bcp_output_file(data_file)
        result = run_silent(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()[:300]
            log(f"  Sample failed for {table.fqn}: {err}")
            return None
        if not os.path.isfile(data_file):
            return None
        file_bytes = os.path.getsize(data_file)
        if file_bytes <= 0:
            return 0.0
        bytes_per_row = file_bytes / sample_n
        extrapolated_gb = (bytes_per_row * table.row_count) / (1024**3)
        # Small safety margin for remainder of table / wider rows
        return extrapolated_gb * 1.08
    finally:
        try:
            if os.path.isfile(data_file):
                os.remove(data_file)
            os.rmdir(sample_dir)
        except OSError:
            pass


def _calibrate_with_bcp_samples(
    cfg: Dict[str, Any],
    tables: List[TableBcpSize],
    bcp_exe: str,
    work_root: str,
    log: LogFn,
    *,
    max_tables: int = 3,
) -> None:
    """Replace heuristic with measured BCP sample for the largest tables."""
    candidates = sorted(tables, key=lambda t: t.bcp_estimated_gb, reverse=True)
    calibrated = 0
    for table in candidates:
        if calibrated >= max_tables:
            break
        if table.row_count <= 0:
            continue
        measured = _sample_table_bcp_gb(cfg, bcp_exe, table, work_root, log)
        if measured is None:
            continue
        table.bcp_estimated_gb = max(table.bcp_estimated_gb, measured)
        table.method = "bcp_sample"
        log(
            f"  {table.fqn}: SQL reserved {table.sql_reserved_gb:.2f} GB → "
            f"BCP ~{table.bcp_estimated_gb:.2f} GB (sampled)"
        )
        calibrated += 1
    if calibrated == 0:
        log("  (BCP sampling skipped or failed; using SQL heuristics only)")


def estimate_bcp_disk_space(
    cfg: Dict[str, Any],
    table_names: Optional[List[str]] = None,
    *,
    work_root: Optional[str] = None,
    bcp_exe: Optional[str] = None,
    log: Optional[LogFn] = None,
    calibrate_with_bcp: bool = True,
) -> BcpDiskEstimate:
    """
    Estimate BCP work-disk requirements before migration.

    1) Read SQL page allocation (often undercounts vs BCP when compression is used).
    2) Apply native-format heuristics, then optionally run BCP sample exports on the
       largest table(s) for a measured extrapolation (most accurate).
    """
    from ..utils.bcp_tools import find_bcp_exe, resolve_bcp_work_root

    _log = log or (lambda _m: None)
    preferred = (cfg.get("bcp_work_dir") or "").strip() or None
    root = work_root or resolve_bcp_work_root(preferred)
    free_gb = shutil.disk_usage(root).free / (1024**3)

    all_tables = _fetch_table_size_details(cfg)
    by_fqn = {t.fqn: t for t in all_tables}

    if table_names:
        scope_tables = [by_fqn[t] for t in table_names if t in by_fqn]
        missing = [t for t in table_names if t not in by_fqn]
    else:
        scope_tables = all_tables
        missing = []

    if not scope_tables:
        return BcpDiskEstimate(
            table_count=len(table_names or []),
            scope_sql_gb=0.0,
            scope_bcp_gb=0.0,
            largest_table="(none)",
            largest_table_sql_gb=0.0,
            largest_table_bcp_gb=0.0,
            largest_table_method="n/a",
            recommended_free_gb=2.0,
            work_root=root,
            work_root_free_gb=free_gb,
            sufficient=free_gb >= 2.0,
            summary="No table size data available; allow at least 2 GB free on the work folder.",
            top_tables=[],
        )

    _log("Calculating BCP disk estimate (SQL sizes + optional BCP sample)...")
    exe = bcp_exe or find_bcp_exe()
    if calibrate_with_bcp and exe and sys.platform.startswith("win"):
        _calibrate_with_bcp_samples(cfg, scope_tables, exe, root, _log, max_tables=3)
    elif calibrate_with_bcp and not exe:
        _log("  (bcp.exe not found — using SQL heuristics only; estimate may be low)")

    largest = max(scope_tables, key=lambda t: t.bcp_estimated_gb)
    scope_sql_gb = sum(t.sql_reserved_gb for t in scope_tables)
    scope_bcp_gb = sum(t.bcp_estimated_gb for t in scope_tables)
    recommended = max(2.0, largest.bcp_estimated_gb * 1.12 + 1.0)
    sufficient = free_gb >= recommended

    top = sorted(scope_tables, key=lambda t: t.bcp_estimated_gb, reverse=True)[:8]
    lines = [
        f"Tables in scope: {len(scope_tables)}",
        f"SQL reserved (on disk, may be compressed): ~{scope_sql_gb:.2f} GB total",
        f"BCP export estimate (native -n): ~{scope_bcp_gb:.2f} GB if all tables exported",
        "",
        f"Largest table: {largest.fqn}",
        f"  SQL reserved: ~{largest.sql_reserved_gb:.2f} GB"
        + (" (compression detected)" if largest.compressed else ""),
        f"  BCP .dat estimate: ~{largest.bcp_estimated_gb:.2f} GB ({largest.method})",
        "",
        f"Recommended free on work disk: ~{recommended:.2f} GB",
        "(peak = one .dat for the largest table; files deleted after each import)",
        f"Work folder: {root}",
        f"Free now: {free_gb:.2f} GB — {'OK' if sufficient else 'NOT ENOUGH'}",
        "",
        "Top tables by estimated BCP .dat size:",
    ]
    for t in top:
        flag = " compressed" if t.compressed else ""
        lines.append(
            f"  {t.fqn}: BCP ~{t.bcp_estimated_gb:.2f} GB "
            f"(SQL {t.sql_reserved_gb:.2f} GB{flag}, {t.method})"
        )
    if missing:
        lines.append(f"({len(missing)} table(s) not found on source for size lookup)")
    if not sufficient:
        lines.append("Use BCP work folder on a drive with more space (e.g. D:\\BCPWork).")

    return BcpDiskEstimate(
        table_count=len(scope_tables),
        scope_sql_gb=scope_sql_gb,
        scope_bcp_gb=scope_bcp_gb,
        largest_table=largest.fqn,
        largest_table_sql_gb=largest.sql_reserved_gb,
        largest_table_bcp_gb=largest.bcp_estimated_gb,
        largest_table_method=largest.method,
        recommended_free_gb=recommended,
        work_root=root,
        work_root_free_gb=free_gb,
        sufficient=sufficient,
        summary="\n".join(lines),
        top_tables=top,
    )


def log_bcp_disk_estimate(estimate: BcpDiskEstimate, log: LogFn) -> None:
    log("=== BCP disk space estimate ===")
    for line in estimate.summary.splitlines():
        log(line)
    log("=" * 31)


def _check_bcp_disk_estimate(cfg: Dict[str, Any], log: LogFn) -> PreflightItem:
    try:
        single = (cfg.get("tables") or "").strip()
        tables = None
        if single:
            tables = [single if "." in single else f"dbo.{single}"]
        try:
            from ..utils.bcp_tools import find_bcp_exe

            bcp_path = find_bcp_exe()
        except Exception:
            bcp_path = None
        est = estimate_bcp_disk_space(
            cfg, tables, bcp_exe=bcp_path, log=log, calibrate_with_bcp=True
        )
        log_bcp_disk_estimate(est, log)
        detail = (
            f"Largest {est.largest_table}: BCP ~{est.largest_table_bcp_gb:.1f} GB "
            f"(SQL {est.largest_table_sql_gb:.1f} GB, {est.largest_table_method}); "
            f"need ~{est.recommended_free_gb:.1f} GB free; "
            f"have {est.work_root_free_gb:.1f} GB"
        )
        return PreflightItem(
            "Client",
            "Disk space (estimate)",
            est.sufficient,
            detail,
            blocking=not est.sufficient,
        )
    except Exception as e:
        return PreflightItem(
            "Client",
            "Disk space (estimate)",
            False,
            str(e)[:400],
            False,
        )


def _check_bcp_work_dir(log: LogFn, cfg: Dict[str, Any], min_gb: float = 2.0) -> PreflightItem:
    try:
        from ..utils.bcp_tools import resolve_bcp_work_root

        preferred = (cfg.get("bcp_work_dir") or "").strip() or None
        work_root = resolve_bcp_work_root(preferred)
        usage = shutil.disk_usage(work_root)
        free_gb = usage.free / (1024**3)
        detail = f"Writable, {free_gb:.1f} GB free: {work_root}"
        if free_gb >= min_gb:
            return PreflightItem("Client", "BCP work folder", True, detail, True)
        return PreflightItem(
            "Client",
            "BCP work folder",
            False,
            f"Only {free_gb:.1f} GB free in {work_root} (need at least ~{min_gb} GB)",
            True,
        )
    except Exception as e:
        return PreflightItem("Client", "BCP work folder", False, str(e)[:400], True)


def tempfile_dir() -> str:
    import tempfile

    return tempfile.gettempdir()


def _test_connection(
    label: str,
    server: str,
    db: str,
    auth: str,
    user: str,
    password: Optional[str],
    log: LogFn,
) -> PreflightItem:
    if not (server or "").strip():
        return PreflightItem(label, "Server", False, "Server is empty", True)
    if not (db or "").strip():
        return PreflightItem(label, "Database", False, "Database is empty", True)

    auth_l = (auth or "").strip().lower()
    if auth_l == "entra_mfa" and not (user or "").strip():
        return PreflightItem(label, "Entra user (UPN)", False, "UPN/email required for MFA", True)
    if auth_l == "sql" and (not user or not password):
        return PreflightItem(label, "SQL login", False, "Username and password required", True)

    if not sys.platform.startswith("win") and auth_l == "windows":
        return PreflightItem(
            label, "Windows auth", False, "Windows auth only on Windows", True
        )

    try:
        import pyodbc
        from ..utils.database import build_conn_str, connect_to_database, pick_sql_driver

        temp_log = logging.getLogger("bcp_preflight_odbc")
        driver = pick_sql_driver(temp_log)
        log(f"{label}: connecting to {server} / {db} ({auth})...")
        if auth_l == "entra_mfa":
            conn = connect_to_database(
                server.strip(), db.strip(), user.strip(), driver, auth_l, None, timeout=30, logger=temp_log
            )
        else:
            conn_str = build_conn_str(
                server.strip(), db.strip(), user or "", driver, auth_l, password
            )
            conn = pyodbc.connect(conn_str, timeout=30)
        cur = conn.cursor()
        cur.execute("SELECT @@VERSION")
        ver = (cur.fetchone()[0] or "")[:80]
        cur.execute("SELECT DB_NAME(), SUSER_SNAME()")
        row = cur.fetchone()
        conn.close()
        detail = f"DB={row[0]}, login={row[1]} | {ver}..."
        return PreflightItem(label, "Connection", True, detail, True)
    except Exception as e:
        return PreflightItem(label, "Connection", False, str(e)[:400], True)


def _check_tables_on_destination(
    cfg: Dict[str, Any],
    log: LogFn,
) -> PreflightItem:
    """Compare source table count vs tables present on destination."""
    try:
        import pyodbc
        from ..utils.database import build_conn_str, connect_to_database, pick_sql_driver

        temp_log = logging.getLogger("bcp_preflight_tables")
        driver = pick_sql_driver(temp_log)

        def connect(server, db, auth, user, password):
            auth_l = (auth or "").strip().lower()
            if auth_l == "entra_mfa":
                return connect_to_database(server, db, user, driver, auth_l, None, timeout=60, logger=temp_log)
            return pyodbc.connect(
                build_conn_str(server, db, user or "", driver, auth_l, password)
            )

        src = connect(
            cfg["src_server"],
            cfg["src_db"],
            cfg.get("src_auth"),
            cfg.get("src_user") or "",
            cfg.get("src_password"),
        )
        dest = connect(
            cfg["dest_server"],
            cfg["dest_db"],
            cfg.get("dest_auth"),
            cfg.get("dest_user") or "",
            cfg.get("dest_password"),
        )
        sql = """
            SELECT s.name + '.' + t.name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.is_ms_shipped = 0
        """
        src_cur = src.cursor()
        src_cur.execute(sql)
        src_tables = {r[0] for r in src_cur.fetchall()}
        dest_cur = dest.cursor()
        dest_cur.execute(sql)
        dest_tables = {r[0] for r in dest_cur.fetchall()}
        src.close()
        dest.close()

        single = (cfg.get("tables") or "").strip()
        if single:
            if "." not in single:
                single = f"dbo.{single}"
            if single not in src_tables:
                return PreflightItem(
                    "Schema",
                    "Single table on source",
                    False,
                    f"{single} not found on source",
                    True,
                )
            if single not in dest_tables:
                return PreflightItem(
                    "Schema",
                    "Single table on destination",
                    False,
                    f"{single} missing on destination — run Schema migration first",
                    True,
                )
            return PreflightItem(
                "Schema",
                "Single table ready",
                True,
                f"{single} exists on source and destination",
                True,
            )

        missing = src_tables - dest_tables
        overlap = len(src_tables & dest_tables)
        if not missing:
            return PreflightItem(
                "Schema",
                "Tables on destination",
                True,
                f"All {len(src_tables)} source tables exist on destination",
                False,
            )
        sample = ", ".join(sorted(missing)[:5])
        more = f" (+{len(missing) - 5} more)" if len(missing) > 5 else ""
        return PreflightItem(
            "Schema",
            "Tables on destination",
            False,
            f"{len(missing)} of {len(src_tables)} source tables missing on dest "
            f"({overlap} ready). Examples: {sample}{more}. "
            "Run Schema migration or enable 'Skip tables not in destination'.",
            False,
        )
    except Exception as e:
        return PreflightItem("Schema", "Table comparison", False, str(e)[:400], False)


def run_bcp_preflight(
    cfg: Dict[str, Any],
    log: Optional[LogFn] = None,
    *,
    install_bcp_if_missing: bool = True,
) -> List[PreflightItem]:
    """
    Run full BCP pre-flight checklist.

    cfg keys: src_server, src_db, src_auth, src_user, src_password,
               dest_server, dest_db, dest_auth, dest_user, dest_password,
               tables (optional single table)
    """
    _log = log or (lambda _m: None)
    items: List[PreflightItem] = []

    _log("=== BCP pre-flight checklist ===")
    for fn in (
        lambda: _check_pyodbc(_log),
        lambda: _check_odbc_driver(_log),
        lambda: _check_bcp(_log, install_if_missing=install_bcp_if_missing),
        lambda: _check_msal_for_mfa(cfg),
        lambda: _check_bcp_entra_dest_on_windows(cfg),
        lambda: _check_bcp_work_dir(_log, cfg),
        lambda: _check_bcp_disk_estimate(cfg, _log),
    ):
        item = fn()
        items.append(item)
        _log(f"{'[OK]' if item.passed else '[FAIL]'} {item.category} / {item.name}: {item.message}")

    items.append(
        _test_connection(
            "Source",
            cfg.get("src_server", ""),
            cfg.get("src_db", ""),
            cfg.get("src_auth", ""),
            cfg.get("src_user", ""),
            cfg.get("src_password"),
            _log,
        )
    )
    _log(
        f"{'[OK]' if items[-1].passed else '[FAIL]'} Source / {items[-1].name}: {items[-1].message}"
    )

    items.append(
        _test_connection(
            "Destination",
            cfg.get("dest_server", ""),
            cfg.get("dest_db", ""),
            cfg.get("dest_auth", ""),
            cfg.get("dest_user", ""),
            cfg.get("dest_password"),
            _log,
        )
    )
    _log(
        f"{'[OK]' if items[-1].passed else '[FAIL]'} Destination / {items[-1].name}: {items[-1].message}"
    )

    items.append(_check_tables_on_destination(cfg, _log))
    _log(
        f"{'[OK]' if items[-1].passed else '[WARN]'} Schema / {items[-1].name}: {items[-1].message}"
    )

    blocking_fail = [i for i in items if not i.passed and i.blocking]
    _log(
        f"=== Pre-flight {'PASSED' if not blocking_fail else 'FAILED'} "
        f"({len(blocking_fail)} blocking issue(s)) ==="
    )
    return items


def preflight_passed(items: List[PreflightItem]) -> bool:
    return not any(not i.passed and i.blocking for i in items)
