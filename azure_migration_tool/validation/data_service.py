# Author: Satish Ch@uhan

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
from .connections import connect_db2, connect_azure_sql, connect_source, connect_destination
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


def _run_source_sql_count(config: Dict[str, Any], schema: str, table: str) -> Optional[int]:
    """Run COUNT(*) on SQL Server source in a worker thread. Returns count or None on error."""
    try:
        conn = connect_source(config)
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


def _run_source_sql_null_counts(
    config: Dict[str, Any], schema: str, table: str, col_name: str
) -> Optional[Tuple[int, int]]:
    """Run COUNT(*), COUNT(col) on SQL Server source in a worker. Returns (total, non_null) or None."""
    try:
        conn = connect_source(config)
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
        
        # Ensure unified_df is a valid DataFrame with standard columns
        if unified_df is None or unified_df.empty:
            unified_df = pd.DataFrame(columns=STANDARD_DATA_VALIDATION_COLUMNS)
        else:
            # Ensure all standard columns exist
            for c in STANDARD_DATA_VALIDATION_COLUMNS:
                if c not in unified_df.columns:
                    unified_df = unified_df.copy()
                    unified_df[c] = ""
            unified_df = unified_df[STANDARD_DATA_VALIDATION_COLUMNS]
        
        unified_df.to_csv(path, index=False)
        return path

    def _is_source_db2(self) -> bool:
        """Check if source database is DB2."""
        return self._get_config().get("source_db_type", "db2").lower() == "db2"

    def _get_source_tables(self, conn, schema: Optional[str]) -> List[Tuple[str, str]]:
        """Get list of (schema, table) from source database."""
        if self._is_source_db2():
            cur = conn.cursor()
            tables = db2_schema.fetch_db2_tables(cur, schema)
            try:
                cur.close()
            except Exception:
                pass
            return [(str(t.schema_name).strip(), str(t.table_name).strip()) for t in tables]
        else:
            # SQL Server source - use azure_catalog functions
            try:
                tables_df = azure_catalog.get_tables(conn, schema)
                if tables_df.empty:
                    return []
                return [(str(r["schema_name"]).strip(), str(r["object_name"]).strip()) for _, r in tables_df.iterrows()]
            except Exception as e:
                # Log error but return empty list to prevent crash
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error fetching source tables: {e}")
                return []

    def _build_source_count_query(self, schema: str, table: str) -> str:
        """Build COUNT(*) query for source database."""
        if self._is_source_db2():
            return f'SELECT COUNT(*) FROM "{schema}"."{table}"'
        else:
            return f"SELECT COUNT(*) FROM [{schema}].[{table}]"

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
        reuse_connections=True (default): one source and one destination connection for the whole run (~1 min for 110 tables).
        reuse_connections=False: new connection per table (slower, ~20+ min) but allows per-query timeout if run in a thread."""
        if not object_types:
            object_types = ["TABLE"]
        
        # Use the appropriate connection based on source type
        conn_source = connect_source(self._get_config())
        conn_dest = connect_destination(self._get_config())
        
        try:
            source_tables = self._get_source_tables(conn_source, source_schema)
        finally:
            try:
                conn_source.close()
            except Exception:
                pass

        dest_tables = azure_catalog.get_tables(conn_dest, target_schema)
        set_dest = set((str(r["schema_name"]).strip().upper(), str(r["object_name"]).strip().upper()) for _, r in dest_tables.iterrows())
        pairs = []
        for s, o in source_tables:
            if (s.upper(), o.upper()) in set_dest:
                row = dest_tables[(dest_tables["schema_name"].astype(str).str.upper() == s.upper()) & (dest_tables["object_name"].astype(str).str.upper() == o.upper())]
                if not row.empty:
                    r = row.iloc[0]
                    pairs.append((s, o, str(r["schema_name"]).strip(), str(r["object_name"]).strip()))
        try:
            conn_dest.close()
        except Exception:
            pass
        conn_dest = None

        total = len(pairs)
        out_rows = []

        if reuse_connections:
            # Fast path: one source and one destination connection for the whole run.
            conn_source_2 = connect_source(self._get_config())
            conn_dest_2 = connect_destination(self._get_config())
            try:
                cur = conn_source_2.cursor()
                for idx, (s, o, d_schema, d_table) in enumerate(pairs):
                    if progress_callback:
                        try:
                            progress_callback(idx + 1, total, s, o)
                        except Exception:
                            pass
                    src_count = None
                    dest_count = None
                    try:
                        cur.execute(self._build_source_count_query(s, o))
                        row = cur.fetchone()
                        src_count = int(row[0]) if row is not None else None
                    except Exception:
                        src_count = None
                    try:
                        cursor_dest = conn_dest_2.cursor()
                        cursor_dest.execute(f"SELECT COUNT(*) FROM [{d_schema}].[{d_table}]")
                        row = cursor_dest.fetchone()
                        dest_count = int(row[0]) if row is not None else None
                        cursor_dest.close()
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
                    conn_source_2.close()
                except Exception:
                    pass
                try:
                    conn_dest_2.close()
                except Exception:
                    pass
            return pd.DataFrame(out_rows)

        # Slow path: new connection per table (for per-query timeout when run in-process).
        config = self._get_config()
        is_db2 = self._is_source_db2()
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
                    if is_db2:
                        f_src = executor.submit(_run_db2_count, config, s, o)
                    else:
                        f_src = executor.submit(_run_source_sql_count, config, s, o)
                    src_count = f_src.result(timeout=QUERY_TIMEOUT_SECONDS)
                except TimeoutError:
                    src_count = None
                except Exception:
                    src_count = None
                try:
                    f_dest = executor.submit(_run_azure_count, config, d_schema, d_table)
                    dest_count = f_dest.result(timeout=QUERY_TIMEOUT_SECONDS)
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

    def _get_source_columns(self, conn, schema: str, table: str) -> List:
        """Get list of nullable columns from source database."""
        if self._is_source_db2():
            cur = conn.cursor()
            cols = db2_schema.fetch_db2_columns(cur, schema, table)
            try:
                cur.close()
            except Exception:
                pass
            return [c for c in cols if c.nullable]
        else:
            # SQL Server source - get columns from sys.columns
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT c.name
                    FROM sys.columns c
                    JOIN sys.tables t ON c.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = ? AND t.name = ? AND c.is_nullable = 1
                """, (schema, table))
                rows = cur.fetchall()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
            # Return simple objects with column_name attribute
            class ColInfo:
                def __init__(self, name):
                    self.column_name = name
            return [ColInfo(row[0]) for row in rows]

    def _build_source_null_count_query(self, schema: str, table: str, col_name: str) -> str:
        """Build COUNT(*), COUNT(col) query for source database."""
        if self._is_source_db2():
            return f'SELECT COUNT(*), COUNT("{col_name}") FROM "{schema}"."{table}"'
        else:
            return f"SELECT COUNT(*), COUNT([{col_name}]) FROM [{schema}].[{table}]"

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
        reuse_connections=True (default): one source and one destination connection for the whole run (fast).
        """
        if not object_types:
            object_types = ["TABLE"]
        
        conn_source = connect_source(self._get_config())
        conn_dest = connect_destination(self._get_config())
        
        try:
            source_tables = self._get_source_tables(conn_source, source_schema)
        finally:
            try:
                conn_source.close()
            except Exception:
                pass

        dest_tables = azure_catalog.get_tables(conn_dest, target_schema)
        set_dest = set((str(r["schema_name"]).strip().upper(), str(r["object_name"]).strip().upper()) for _, r in dest_tables.iterrows())
        pairs = []
        for s, o in source_tables:
            if (s.upper(), o.upper()) in set_dest:
                row = dest_tables[(dest_tables["schema_name"].astype(str).str.upper() == s.upper()) & (dest_tables["object_name"].astype(str).str.upper() == o.upper())]
                if not row.empty:
                    r = row.iloc[0]
                    pairs.append((s, o, str(r["schema_name"]).strip(), str(r["object_name"]).strip()))

        try:
            conn_dest.close()
        except Exception:
            pass
        conn_dest = None

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
            # Fast path: one source and one destination connection for the whole run.
            conn_source_2 = connect_source(config)
            conn_dest_2 = connect_destination(config)
            try:
                for idx, (s, o, d_schema, d_table) in enumerate(pairs):
                    if progress_callback:
                        try:
                            progress_callback(idx + 1, total_tables, s, o)
                        except Exception:
                            pass
                    nullable_cols = self._get_source_columns(conn_source_2, s, o)
                    if not nullable_cols:
                        continue
                    for col_info in nullable_cols:
                        col_name = col_info.column_name
                        src_nulls = None
                        src_empty = 0
                        dest_nulls = None
                        dest_empty = 0
                        try:
                            cur = conn_source_2.cursor()
                            cur.execute(self._build_source_null_count_query(s, o, col_name))
                            row = cur.fetchone()
                            cur.close()
                            if row is not None:
                                src_total = int(row[0]) if row[0] is not None else 0
                                src_non_null = int(row[1]) if row[1] is not None else 0
                                src_nulls = src_total - src_non_null
                        except Exception:
                            try:
                                cur.close()
                            except Exception:
                                pass
                        try:
                            cursor_dest = conn_dest_2.cursor()
                            cursor_dest.execute(f"SELECT COUNT(*), COUNT([{col_name}]) FROM [{d_schema}].[{d_table}]")
                            row_dest = cursor_dest.fetchone()
                            cursor_dest.close()
                            if row_dest is not None:
                                dest_total = int(row_dest[0]) if row_dest[0] is not None else 0
                                dest_non_null = int(row_dest[1]) if row_dest[1] is not None else 0
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
                    conn_source_2.close()
                except Exception:
                    pass
                try:
                    conn_dest_2.close()
                except Exception:
                    pass
            return pd.DataFrame(out_rows)

        # Slow path: new connection per column (for per-query timeout).
        conn_source_2 = connect_source(config)
        is_db2 = self._is_source_db2()
        executor = ThreadPoolExecutor(max_workers=4)
        try:
            for idx, (s, o, d_schema, d_table) in enumerate(pairs):
                if progress_callback:
                    try:
                        progress_callback(idx + 1, total_tables, s, o)
                    except Exception:
                        pass
                nullable_cols = self._get_source_columns(conn_source_2, s, o)
                if not nullable_cols:
                    continue
                for col_info in nullable_cols:
                    col_name = col_info.column_name
                    src_nulls = None
                    src_empty = 0
                    dest_nulls = None
                    dest_empty = 0
                    try:
                        if is_db2:
                            f_src = executor.submit(_run_db2_null_counts, config, s, o, col_name)
                        else:
                            f_src = executor.submit(_run_source_sql_null_counts, config, s, o, col_name)
                        res = f_src.result(timeout=QUERY_TIMEOUT_SECONDS)
                        if res is not None:
                            src_total, src_non_null = res
                            src_nulls = src_total - src_non_null
                    except TimeoutError:
                        pass
                    except Exception:
                        pass
                    try:
                        f_dest = executor.submit(_run_azure_null_counts, config, d_schema, d_table, col_name)
                        res = f_dest.result(timeout=QUERY_TIMEOUT_SECONDS)
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
                conn_source_2.close()
            except Exception:
                pass
            executor.shutdown(wait=False)
        return pd.DataFrame(out_rows)
