"""
LegacyDataValidationService: Python-only data comparison (DB2 vs Azure SQL).
compare_row_counts, compare_column_nulls. Same __init__ and save_comparison_to_csv as schema_service.
Output format: ValidationType, Status, ObjectType, SourceObjectName, SourceSchemaName,
DestinationObjectName, DestinationSchemaName, ElementPath, ErrorCode, ErrorDescription, DetailsJson.
Single file per run: data_validate_{all|specify}_{db}_{YYYYMMDD}_{HHMMSS}_{microseconds}_{uuid6}.csv
"""

import json
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from .config import get_output_dir, load_config, normalize_config
from .connections import connect_db2, connect_azure_sql
from . import azure_catalog
from gui.utils import db2_schema

# Per-table query timeout (seconds). Prevents one slow table from blocking the whole run.
QUERY_TIMEOUT_SECONDS = 120

# Standard output columns for data validation (single consolidated CSV)
STANDARD_DATA_VALIDATION_COLUMNS = [
    "ValidationType",
    "Status",
    "ObjectType",
    "SourceObjectName",
    "SourceSchemaName",
    "DestinationObjectName",
    "DestinationSchemaName",
    "ElementPath",
    "ErrorCode",
    "ErrorDescription",
    "DetailsJson",
]


def _run_db2_count(config: Dict[str, Any], schema: str, table: str) -> Optional[int]:
    """Run COUNT(*) on DB2 in a worker thread. Opens and closes its own connection. Returns count or None on error."""
    try:
        conn = connect_db2(config)
        try:
            cur = conn.cursor()
            cur.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"' if schema and table else f"SELECT COUNT(*) FROM {schema}.{table}"
            )
            row = cur.fetchone()
            return int(row[0]) if row is not None else None
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        return None


def _run_azure_count(config: Dict[str, Any], schema: str, table: str) -> Optional[int]:
    """Run COUNT(*) on Azure SQL in a worker thread. Opens and closes its own connection. Returns count or None on error."""
    try:
        conn = connect_azure_sql(config)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            row = cur.fetchone()
            return int(row[0]) if row is not None else None
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        return None


def _run_db2_null_counts(
    config: Dict[str, Any], schema: str, table: str, col_name: str
) -> Optional[Tuple[int, int]]:
    """Run COUNT(*), COUNT(col) on DB2 in a worker. Returns (total, non_null) or None."""
    try:
        conn = connect_db2(config)
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT COUNT(*), COUNT("{col_name}") FROM "{schema}"."{table}"')
            row = cur.fetchone()
            if row is None:
                return None
            return (int(row[0]) if row[0] is not None else 0, int(row[1]) if row[1] is not None else 0)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        return None


def _run_azure_null_counts(
    config: Dict[str, Any], schema: str, table: str, col_name: str
) -> Optional[Tuple[int, int]]:
    """Run COUNT(*), COUNT(col) on Azure SQL in a worker. Returns (total, non_null) or None."""
    try:
        conn = connect_azure_sql(config)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*), COUNT([{col_name}]) FROM [{schema}].[{table}]")
            row = cur.fetchone()
            if row is None:
                return None
            return (int(row[0]) if row[0] is not None else 0, int(row[1]) if row[1] is not None else 0)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception:
        return None


def _row_counts_to_standard(df: pd.DataFrame) -> pd.DataFrame:
    """Convert row counts DataFrame to standard column format."""
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_DATA_VALIDATION_COLUMNS)
    rows = []
    for _, r in df.iterrows():
        src_s = str(r.get("SourceSchemaName", "") or "")
        src_o = str(r.get("SourceObjectName", "") or "")
        dst_s = str(r.get("DestinationSchemaName", "") or "")
        dst_o = str(r.get("DestinationObjectName", "") or "")
        match = str(r.get("RowCountMatch", "") or "").strip().upper()
        err = str(r.get("ErrorDescription", "") or "")
        status = "error" if match == "MISMATCH" or err else ""
        details = json.dumps({
            "SourceRowCount": r.get("SourceRowCount"),
            "DestinationRowCount": r.get("DestinationRowCount"),
        })
        rows.append({
            "ValidationType": "RowCount",
            "Status": status,
            "ObjectType": "TABLE",
            "SourceObjectName": src_o,
            "SourceSchemaName": src_s,
            "DestinationObjectName": dst_o,
            "DestinationSchemaName": dst_s,
            "ElementPath": f"{src_s}.{src_o}" if src_s and src_o else "",
            "ErrorCode": "ROW_COUNT_MISMATCH" if status else "",
            "ErrorDescription": err,
            "DetailsJson": details,
        })
    return pd.DataFrame(rows, columns=STANDARD_DATA_VALIDATION_COLUMNS)


def _null_values_to_standard(df: pd.DataFrame) -> pd.DataFrame:
    """Convert null values DataFrame to standard column format."""
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_DATA_VALIDATION_COLUMNS)
    rows = []
    for _, r in df.iterrows():
        src_s = str(r.get("SourceSchemaName", "") or "")
        src_o = str(r.get("SourceObjectName", "") or "")
        col = str(r.get("ColumnName", "") or "")
        dst_s = str(r.get("DestinationSchemaName", "") or "")
        dst_o = str(r.get("DestinationObjectName", "") or "")
        nm = str(r.get("NullCountMatch", "") or "").strip().upper()
        em = str(r.get("EmptyCountMatch", "") or "").strip().upper()
        status = "error" if nm == "MISMATCH" or em == "MISMATCH" else ""
        details = json.dumps({
            "SourceNullCount": r.get("SourceNullCount"),
            "DestinationNullCount": r.get("DestinationNullCount"),
            "SourceEmptyCount": r.get("SourceEmptyCount"),
            "DestinationEmptyCount": r.get("DestinationEmptyCount"),
        })
        rows.append({
            "ValidationType": "NullCount",
            "Status": status,
            "ObjectType": "TABLE",
            "SourceObjectName": src_o,
            "SourceSchemaName": src_s,
            "DestinationObjectName": dst_o,
            "DestinationSchemaName": dst_s,
            "ElementPath": f"{src_s}.{src_o}.{col}" if col else f"{src_s}.{src_o}",
            "ErrorCode": "NULL_COUNT_MISMATCH" if status else "",
            "ErrorDescription": "Null or empty count mismatch" if status else "",
            "DetailsJson": details,
        })
    return pd.DataFrame(rows, columns=STANDARD_DATA_VALIDATION_COLUMNS)


def _distinct_key_to_standard(df: pd.DataFrame) -> pd.DataFrame:
    """Convert distinct key (PK) DataFrame to standard column format."""
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_DATA_VALIDATION_COLUMNS)
    rows = []
    for _, r in df.iterrows():
        src_s = str(r.get("SourceSchemaName", "") or "")
        src_o = str(r.get("SourceObjectName", "") or "")
        dst_s = str(r.get("DestinationSchemaName", "") or "")
        dst_o = str(r.get("DestinationObjectName", "") or "")
        err = str(r.get("ErrorDescription", r.get("Status", "")) or "")
        # Ensure ErrorCode and DetailsJson exist
        rows.append({
            "ValidationType": "PrimaryKey",
            "Status": str(r.get("Status", "info") or "info"),
            "ObjectType": "TABLE",
            "SourceObjectName": src_o,
            "SourceSchemaName": src_s,
            "DestinationObjectName": dst_o,
            "DestinationSchemaName": dst_s,
            "ElementPath": f"{src_s}.{src_o}.PRIMARY KEY" if src_s and src_o else "PRIMARY KEY",
            "ErrorCode": str(r.get("ErrorCode", "") or ""),
            "ErrorDescription": err or "No primary key on either side (note)",
            "DetailsJson": json.dumps({}),
        })
    return pd.DataFrame(rows, columns=STANDARD_DATA_VALIDATION_COLUMNS)


class LegacyDataValidationService:
    def __init__(
        self,
        config_path: Optional[str] = None,
        config: Optional[dict] = None,
        output_dir: Optional[str] = None,
    ):
        if config is not None:
            self._config = normalize_config(config)
            self._output_dir = get_output_dir(output_dir_override=output_dir or os.environ.get("VALIDATION_OUTPUT_DIR", os.getcwd()))
        elif config_path:
            self._config = load_config(config_path)
            self._output_dir = get_output_dir(config_path=config_path)
        else:
            raise ValueError("Provide either config_path or config (in-memory dict)")
        self.config_path = config_path  # may be None when using config dict

    def _get_config(self):
        return self._config

    def save_comparison_to_csv(self, df: pd.DataFrame, filename_prefix: str) -> str:
        """Write df to output_dir with timestamped filename; return path (legacy per-step save)."""
        os.makedirs(self._output_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"{filename_prefix}_{ts}.csv"
        path = os.path.join(self._output_dir, name)
        df.to_csv(path, index=False)
        return path

    def build_unified_data_validation_df(
        self,
        row_counts_df: Optional[pd.DataFrame] = None,
        null_values_df: Optional[pd.DataFrame] = None,
        distinct_key_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Combine row counts, null values, and distinct key results into one DataFrame with standard columns."""
        parts = []
        if row_counts_df is not None and not row_counts_df.empty:
            parts.append(_row_counts_to_standard(row_counts_df))
        if null_values_df is not None and not null_values_df.empty:
            parts.append(_null_values_to_standard(null_values_df))
        if distinct_key_df is not None and not distinct_key_df.empty:
            parts.append(_distinct_key_to_standard(distinct_key_df))
        if not parts:
            return pd.DataFrame(columns=STANDARD_DATA_VALIDATION_COLUMNS)
        return pd.concat(parts, ignore_index=True)

    def save_data_validation_single_csv(
        self,
        unified_df: pd.DataFrame,
        scope: str = "all",
        azure_db_name: Optional[str] = None,
    ) -> str:
        """
        Save all data validation results to a single CSV.
        Filename: data_validate_{scope}_{db}_{YYYYMMDD}_{HHMMSS}_{microseconds}_{uuid6}.csv
        scope: 'all' or 'specify' (specific schema).
        """
        os.makedirs(self._output_dir, exist_ok=True)
        db = (azure_db_name or "").strip().replace(" ", "_").replace(".", "_") or "db"
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")  # includes microseconds (6 digits)
        uniq = uuid.uuid4().hex[:6]
        name = f"data_validate_{scope}_{db}_{ts}_{uniq}.csv"
        path = os.path.join(self._output_dir, name)
        for c in STANDARD_DATA_VALIDATION_COLUMNS:
            if c not in unified_df.columns:
                unified_df = unified_df.copy()
                unified_df[c] = ""
        unified_df = unified_df[STANDARD_DATA_VALIDATION_COLUMNS]
        unified_df.to_csv(path, index=False)
        return path

    def compare_row_counts(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
        progress_callback=None,
        reuse_connections: bool = True,
    ) -> pd.DataFrame:
        """Return DataFrame: ObjectType, SourceSchemaName, SourceObjectName, DestinationSchemaName, DestinationObjectName, SourceRowCount, DestinationRowCount, RowCountMatch, ErrorDescription.
        progress_callback(current_index, total, schema_name, table_name) is called for each table.
        reuse_connections=True (default): one DB2 and one Azure connection for the whole run (~1 min for 110 tables).
        reuse_connections=False: new connection per table (slower, ~20+ min) but allows per-query timeout if run in a thread."""
        if not object_types:
            object_types = ["TABLE"]
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur = conn_db2.cursor()
            tables_db2 = db2_schema.fetch_db2_tables(cur, source_schema)
            tables_db2 = [(str(t.schema_name).strip(), str(t.table_name).strip()) for t in tables_db2]
        finally:
            try:
                conn_db2.close()
            except Exception:
                pass

        az_tables = azure_catalog.get_tables(conn_azure, target_schema)
        set_az = set((str(r["schema_name"]).strip().upper(), str(r["object_name"]).strip().upper()) for _, r in az_tables.iterrows())
        pairs = []
        for s, o in tables_db2:
            if (s.upper(), o.upper()) in set_az:
                row = az_tables[(az_tables["schema_name"].astype(str).str.upper() == s.upper()) & (az_tables["object_name"].astype(str).str.upper() == o.upper())]
                if not row.empty:
                    r = row.iloc[0]
                    pairs.append((s, o, str(r["schema_name"]).strip(), str(r["object_name"]).strip()))
        try:
            conn_azure.close()
        except Exception:
            pass
        conn_azure = None

        total = len(pairs)
        out_rows = []

        if reuse_connections:
            # Fast path: one DB2 and one Azure connection for the whole run (~1 min for 110 tables).
            conn_db2_2 = connect_db2(self._get_config())
            conn_azure_2 = connect_azure_sql(self._get_config())
            try:
                cur = conn_db2_2.cursor()
                for idx, (s, o, d_schema, d_table) in enumerate(pairs):
                    if progress_callback:
                        try:
                            progress_callback(idx + 1, total, s, o)
                        except Exception:
                            pass
                    src_count = None
                    dest_count = None
                    try:
                        cur.execute(f'SELECT COUNT(*) FROM "{s}"."{o}"' if s and o else f"SELECT COUNT(*) FROM {s}.{o}")
                        row = cur.fetchone()
                        src_count = int(row[0]) if row is not None else None
                    except Exception:
                        src_count = None
                    try:
                        cursor_az = conn_azure_2.cursor()
                        cursor_az.execute(f"SELECT COUNT(*) FROM [{d_schema}].[{d_table}]")
                        row = cursor_az.fetchone()
                        dest_count = int(row[0]) if row is not None else None
                        cursor_az.close()
                    except Exception:
                        dest_count = None
                    match = "MATCH" if src_count is not None and dest_count is not None and src_count == dest_count else "MISMATCH"
                    err = ""
                    if src_count is None:
                        err = "Source count failed"
                    elif dest_count is None:
                        err = "Destination count failed"
                    elif src_count != dest_count:
                        err = f"Row count mismatch: {src_count} vs {dest_count}"
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s,
                        "SourceObjectName": o,
                        "DestinationSchemaName": d_schema,
                        "DestinationObjectName": d_table,
                        "SourceRowCount": src_count if src_count is not None else "",
                        "DestinationRowCount": dest_count if dest_count is not None else "",
                        "RowCountMatch": match,
                        "ErrorDescription": err,
                    })
            finally:
                try:
                    conn_db2_2.close()
                except Exception:
                    pass
                try:
                    conn_azure_2.close()
                except Exception:
                    pass
            return pd.DataFrame(out_rows)

        # Slow path: new connection per table (for per-query timeout when run in-process).
        config = self._get_config()
        executor = ThreadPoolExecutor(max_workers=4)
        try:
            for idx, (s, o, d_schema, d_table) in enumerate(pairs):
                if progress_callback:
                    try:
                        progress_callback(idx + 1, total, s, o)
                    except Exception:
                        pass
                src_count = None
                dest_count = None
                try:
                    f_db2 = executor.submit(_run_db2_count, config, s, o)
                    src_count = f_db2.result(timeout=QUERY_TIMEOUT_SECONDS)
                except TimeoutError:
                    src_count = None
                except Exception:
                    src_count = None
                try:
                    f_az = executor.submit(_run_azure_count, config, d_schema, d_table)
                    dest_count = f_az.result(timeout=QUERY_TIMEOUT_SECONDS)
                except TimeoutError:
                    dest_count = None
                except Exception:
                    dest_count = None
                match = "MATCH" if src_count is not None and dest_count is not None and src_count == dest_count else "MISMATCH"
                err = ""
                if src_count is None:
                    err = "Source count failed or timed out"
                elif dest_count is None:
                    err = "Destination count failed or timed out"
                elif src_count != dest_count:
                    err = f"Row count mismatch: {src_count} vs {dest_count}"
                out_rows.append({
                    "ObjectType": "TABLE",
                    "SourceSchemaName": s,
                    "SourceObjectName": o,
                    "DestinationSchemaName": d_schema,
                    "DestinationObjectName": d_table,
                    "SourceRowCount": src_count if src_count is not None else "",
                    "DestinationRowCount": dest_count if dest_count is not None else "",
                    "RowCountMatch": match,
                    "ErrorDescription": err,
                })
        finally:
            executor.shutdown(wait=False)
        return pd.DataFrame(out_rows)

    def compare_column_nulls(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
        only_when_rowcount_matches: bool = True,
        output_only_issues: bool = False,
        progress_callback=None,
        reuse_connections: bool = True,
    ) -> pd.DataFrame:
        """Return DataFrame: ObjectType, SourceSchemaName, SourceObjectName, ColumnName, SourceNullCount, DestinationNullCount, SourceEmptyCount, DestinationEmptyCount, NullCountMatch, EmptyCountMatch.
        Iterate tables; where row counts match run COUNT(*) and COUNT(column) for nullable columns.
        progress_callback(current_index, total, schema_name, table_name) is called for each table.
        reuse_connections=True (default): one DB2 and one Azure connection for the whole run (fast).
        """
        if not object_types:
            object_types = ["TABLE"]
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur = conn_db2.cursor()
            tables_db2 = [(str(t.schema_name).strip(), str(t.table_name).strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
        finally:
            try:
                conn_db2.close()
            except Exception:
                pass

        az_tables = azure_catalog.get_tables(conn_azure, target_schema)
        set_az = set((str(r["schema_name"]).strip().upper(), str(r["object_name"]).strip().upper()) for _, r in az_tables.iterrows())
        pairs = []
        for s, o in tables_db2:
            if (s.upper(), o.upper()) in set_az:
                row = az_tables[(az_tables["schema_name"].astype(str).str.upper() == s.upper()) & (az_tables["object_name"].astype(str).str.upper() == o.upper())]
                if not row.empty:
                    r = row.iloc[0]
                    pairs.append((s, o, str(r["schema_name"]).strip(), str(r["object_name"]).strip()))

        try:
            conn_azure.close()
        except Exception:
            pass
        conn_azure = None

        if only_when_rowcount_matches:
            row_counts = self.compare_row_counts(source_schema, target_schema, object_types, reuse_connections=reuse_connections)
            match_set = set()
            for _, r in row_counts.iterrows():
                if str(r.get("RowCountMatch", "")).strip().upper() == "MATCH":
                    match_set.add((str(r.get("SourceSchemaName", "")).strip().upper(), str(r.get("SourceObjectName", "")).strip().upper()))
            pairs = [p for p in pairs if (p[0].upper(), p[1].upper()) in match_set]

        total_tables = len(pairs)
        out_rows = []
        config = self._get_config()

        if reuse_connections:
            # Fast path: one DB2 and one Azure connection for the whole run.
            conn_db2_2 = connect_db2(config)
            conn_azure_2 = connect_azure_sql(config)
            try:
                cur = conn_db2_2.cursor()
                for idx, (s, o, d_schema, d_table) in enumerate(pairs):
                    if progress_callback:
                        try:
                            progress_callback(idx + 1, total_tables, s, o)
                        except Exception:
                            pass
                    cols_db2 = db2_schema.fetch_db2_columns(cur, s, o)
                    nullable_cols = [c.column_name for c in cols_db2 if c.nullable]
                    if not nullable_cols:
                        continue
                    for col_name in nullable_cols:
                        src_nulls = None
                        src_empty = 0
                        dest_nulls = None
                        dest_empty = 0
                        try:
                            cur.execute(f'SELECT COUNT(*), COUNT("{col_name}") FROM "{s}"."{o}"')
                            row = cur.fetchone()
                            if row is not None:
                                src_total = int(row[0]) if row[0] is not None else 0
                                src_non_null = int(row[1]) if row[1] is not None else 0
                                src_nulls = src_total - src_non_null
                        except Exception:
                            pass
                        try:
                            cursor_az = conn_azure_2.cursor()
                            cursor_az.execute(f"SELECT COUNT(*), COUNT([{col_name}]) FROM [{d_schema}].[{d_table}]")
                            row_az = cursor_az.fetchone()
                            cursor_az.close()
                            if row_az is not None:
                                dest_total = int(row_az[0]) if row_az[0] is not None else 0
                                dest_non_null = int(row_az[1]) if row_az[1] is not None else 0
                                dest_nulls = dest_total - dest_non_null
                        except Exception:
                            pass
                        null_match = "MATCH" if src_nulls is not None and dest_nulls is not None and src_nulls == dest_nulls else "MISMATCH"
                        empty_match = "MATCH" if src_empty is not None and dest_empty is not None and src_empty == dest_empty else "MISMATCH"
                        if output_only_issues and null_match == "MATCH" and empty_match == "MATCH":
                            continue
                        out_rows.append({
                            "ObjectType": "TABLE",
                            "SourceSchemaName": s,
                            "SourceObjectName": o,
                            "DestinationSchemaName": d_schema,
                            "DestinationObjectName": d_table,
                            "ColumnName": col_name,
                            "SourceNullCount": src_nulls if src_nulls is not None else "",
                            "DestinationNullCount": dest_nulls if dest_nulls is not None else "",
                            "SourceEmptyCount": src_empty if src_empty is not None else "",
                            "DestinationEmptyCount": dest_empty if dest_empty is not None else "",
                            "NullCountMatch": null_match,
                            "EmptyCountMatch": empty_match,
                        })
            finally:
                try:
                    conn_db2_2.close()
                except Exception:
                    pass
                try:
                    conn_azure_2.close()
                except Exception:
                    pass
            return pd.DataFrame(out_rows)

        # Slow path: new connection per column (for per-query timeout).
        conn_db2_2 = connect_db2(config)
        executor = ThreadPoolExecutor(max_workers=4)
        try:
            cur = conn_db2_2.cursor()
            for idx, (s, o, d_schema, d_table) in enumerate(pairs):
                if progress_callback:
                    try:
                        progress_callback(idx + 1, total_tables, s, o)
                    except Exception:
                        pass
                cols_db2 = db2_schema.fetch_db2_columns(cur, s, o)
                nullable_cols = [c.column_name for c in cols_db2 if c.nullable]
                if not nullable_cols:
                    continue
                for col_name in nullable_cols:
                    src_nulls = None
                    src_empty = 0
                    dest_nulls = None
                    dest_empty = 0
                    try:
                        f_db2 = executor.submit(_run_db2_null_counts, config, s, o, col_name)
                        res = f_db2.result(timeout=QUERY_TIMEOUT_SECONDS)
                        if res is not None:
                            src_total, src_non_null = res
                            src_nulls = src_total - src_non_null
                    except TimeoutError:
                        pass
                    except Exception:
                        pass
                    try:
                        f_az = executor.submit(_run_azure_null_counts, config, d_schema, d_table, col_name)
                        res = f_az.result(timeout=QUERY_TIMEOUT_SECONDS)
                        if res is not None:
                            dest_total, dest_non_null = res
                            dest_nulls = dest_total - dest_non_null
                    except TimeoutError:
                        pass
                    except Exception:
                        pass
                    null_match = "MATCH" if src_nulls is not None and dest_nulls is not None and src_nulls == dest_nulls else "MISMATCH"
                    empty_match = "MATCH" if src_empty is not None and dest_empty is not None and src_empty == dest_empty else "MISMATCH"
                    if output_only_issues and null_match == "MATCH" and empty_match == "MATCH":
                        continue
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s,
                        "SourceObjectName": o,
                        "DestinationSchemaName": d_schema,
                        "DestinationObjectName": d_table,
                        "ColumnName": col_name,
                        "SourceNullCount": src_nulls if src_nulls is not None else "",
                        "DestinationNullCount": dest_nulls if dest_nulls is not None else "",
                        "SourceEmptyCount": src_empty if src_empty is not None else "",
                        "DestinationEmptyCount": dest_empty if dest_empty is not None else "",
                        "NullCountMatch": null_match,
                        "EmptyCountMatch": empty_match,
                    })
        finally:
            try:
                conn_db2_2.close()
            except Exception:
                pass
            executor.shutdown(wait=False)
        return pd.DataFrame(out_rows)
