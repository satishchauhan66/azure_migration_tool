# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
LegacySchemaValidationService: Python-only schema comparison (DB2 vs Azure SQL).
Returns pandas DataFrames with exact column names expected by Legacy schema tab.
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from .config import load_config, get_output_dir, normalize_config
from .connections import connect_db2, connect_azure_sql, connect_source, connect_destination
from . import azure_catalog
from gui.utils import db2_schema
from gui.utils.db2_type_mapping import get_expected_sql_type, validate_type_mapping, normalize_db2_type, normalize_sql_type


# Default object types for presence
DEFAULT_OBJECT_TYPES = ["TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER", "INDEX", "CONSTRAINT", "SEQUENCE"]


def _norm(s: str) -> str:
    return (s or "").strip().upper()


def _list_db2_objects(cursor, object_types: List[str], schema: Optional[str]) -> pd.DataFrame:
    """Build DataFrame with columns object_type, schema_name, object_name for DB2."""
    rows = []
    u = [t.upper() for t in object_types]
    if "TABLE" in u:
        for t in db2_schema.fetch_db2_tables(cursor, schema):
            rows.append(("TABLE", t.schema_name.strip(), t.table_name.strip()))
    if "VIEW" in u:
        for s, n in db2_schema.fetch_db2_views(cursor, schema):
            rows.append(("VIEW", s.strip(), n.strip()))
    if "PROCEDURE" in u:
        for s, n in db2_schema.fetch_db2_procedures(cursor, schema):
            rows.append(("PROCEDURE", s.strip(), n.strip()))
    if "FUNCTION" in u:
        for s, n in db2_schema.fetch_db2_functions(cursor, schema):
            rows.append(("FUNCTION", s.strip(), n.strip()))
    if "TRIGGER" in u:
        for s, n, _ in db2_schema.fetch_db2_triggers(cursor, schema):
            rows.append(("TRIGGER", s.strip(), n.strip()))
    if "INDEX" in u:
        for idx in db2_schema.fetch_db2_indexes(cursor, schema):
            obj_name = f"{idx.table_name}.{idx.index_name}"
            rows.append(("INDEX", idx.schema_name.strip(), obj_name.strip()))
    if "CONSTRAINT" in u:
        for pk in db2_schema.fetch_db2_primary_keys(cursor, schema):
            obj_name = f"{pk['table_name']}.{pk['constraint_name']}"
            rows.append(("CONSTRAINT", pk["schema_name"].strip(), obj_name.strip()))
        for chk in db2_schema.fetch_db2_check_constraints(cursor, schema):
            obj_name = f"{chk['table_name']}.{chk['constraint_name']}"
            rows.append(("CONSTRAINT", chk["schema_name"].strip(), obj_name.strip()))
    if "SEQUENCE" in u:
        for s, n in db2_schema.fetch_db2_sequences(cursor, schema):
            rows.append(("SEQUENCE", s.strip(), n.strip()))
    return pd.DataFrame(rows, columns=["object_type", "schema_name", "object_name"])


def _list_azure_objects(conn, object_types: List[str], schema: Optional[str]) -> pd.DataFrame:
    """Build DataFrame with columns object_type, schema_name, object_name for Azure.
    Note: Only supports TABLE, VIEW, PROCEDURE, FUNCTION from sys.objects.
    INDEX, CONSTRAINT, SEQUENCE, TRIGGER are not available via sys.objects and must be queried separately.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Filter to only object types that can be queried from sys.objects
    supported_types = {"TABLE", "VIEW", "PROCEDURE", "FUNCTION"}
    filtered_types = [t for t in object_types if t.upper() in supported_types]
    
    if not filtered_types:
        logger.warning(f"_list_azure_objects: No supported object types in {object_types}, returning empty")
        return pd.DataFrame(columns=["object_type", "schema_name", "object_name"])
    
    try:
        df = azure_catalog.get_objects(conn, filtered_types, schema)
        logger.info(f"_list_azure_objects: get_objects returned {len(df)} rows for types {filtered_types}")
        if df.empty:
            logger.warning(f"_list_azure_objects: get_objects returned empty DataFrame for schema={schema}, object_types={filtered_types}")
            return pd.DataFrame(columns=["object_type", "schema_name", "object_name"])
        type_map = {"U": "TABLE", "V": "VIEW", "P": "PROCEDURE", "FN": "FUNCTION", "IF": "FUNCTION", "TF": "FUNCTION"}
        df["object_type"] = df["type"].astype(str).map(lambda x: type_map.get(x, "OTHER"))
        logger.info(f"_list_azure_objects: After type mapping: {len(df)} rows, types: {df['object_type'].unique()}")
        df = df[df["object_type"].isin(filtered_types)]
        logger.info(f"_list_azure_objects: After filtering by object_types: {len(df)} rows")
        result = df[["object_type", "schema_name", "object_name"]].copy()
        logger.info(f"_list_azure_objects: Final result: {len(result)} rows")
        return result
    except Exception as e:
        logger.error(f"_list_azure_objects: Error: {e}", exc_info=True)
        return pd.DataFrame(columns=["object_type", "schema_name", "object_name"])


class LegacySchemaValidationService:
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
        self.config_path = config_path

    def _get_config(self):
        return self._config

    def _is_source_db2(self) -> bool:
        """Check if source database is DB2."""
        config = self._get_config()
        source_type = config.get("source_db_type", "").lower()
        # Fallback: if source_db_type not set, check if db2 key exists
        if not source_type:
            if "db2" in config:
                return True
            elif "source_sql" in config:
                return False
            # Default to db2 for backward compatibility
            return True
        return source_type == "db2"

    def save_comparison_to_csv(self, df: pd.DataFrame, filename_prefix: str) -> str:
        """Write df to output_dir with timestamped filename; return path."""
        os.makedirs(self._output_dir, exist_ok=True)
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"{filename_prefix}_{ts}.csv"
        path = os.path.join(self._output_dir, name)
        df.to_csv(path, index=False)
        return path

    def compare_schema_presence(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Return DataFrame: ObjectType, SourceSchemaName, SourceObjectName, DestinationSchemaName, DestinationObjectName, ChangeType, ElementPath."""
        if not object_types:
            object_types = DEFAULT_OBJECT_TYPES.copy()
        
        is_db2_source = self._is_source_db2()
        conn_source = connect_source(self._get_config())
        conn_dest = connect_destination(self._get_config())
        
        try:
            if is_db2_source:
                cur_source = conn_source.cursor()
                left = _list_db2_objects(cur_source, object_types, source_schema)
                try:
                    cur_source.close()
                except Exception:
                    pass
            else:
                # SQL Server source - use azure_catalog
                left = _list_azure_objects(conn_source, object_types, source_schema)
            right = _list_azure_objects(conn_dest, object_types, target_schema)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error fetching objects: {e}")
            # Return empty DataFrame with correct columns
            return pd.DataFrame(columns=["ObjectType", "SourceSchemaName", "SourceObjectName", 
                                        "DestinationSchemaName", "DestinationObjectName", 
                                        "ChangeType", "ElementPath"])
        finally:
            try:
                conn_source.close()
            except Exception:
                pass
            try:
                conn_dest.close()
            except Exception:
                pass

        # Handle empty DataFrames
        if left is None or left.empty:
            left = pd.DataFrame(columns=["object_type", "schema_name", "object_name"])
        if right is None or right.empty:
            right = pd.DataFrame(columns=["object_type", "schema_name", "object_name"])

        # Log what was found for debugging
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Source objects found: {len(left)}")
        logger.info(f"Destination objects found: {len(right)}")
        if not left.empty:
            logger.info(f"Source object types: {left['object_type'].unique() if 'object_type' in left.columns else 'N/A'}")
        if not right.empty:
            logger.info(f"Destination object types: {right['object_type'].unique() if 'object_type' in right.columns else 'N/A'}")

        # Create normalized columns for merging
        # Ensure both DataFrames have required columns before processing
        required_cols = ["schema_name", "object_name", "object_type"]
        for col in required_cols:
            if col not in left.columns:
                if left.empty:
                    left[col] = pd.Series(dtype=str)
                else:
                    left[col] = ""
            if col not in right.columns:
                if right.empty:
                    right[col] = pd.Series(dtype=str)
                else:
                    right[col] = ""
        
        left = left.copy()
        right = right.copy()
        
        # Create normalized columns for merge keys (works even on empty DataFrames)
        try:
            left["_sn"] = left["schema_name"].astype(str).str.strip().str.upper()
            left["_on"] = left["object_name"].astype(str).str.strip().str.upper()
            left["_t"] = left["object_type"].astype(str).str.strip().str.upper()
        except Exception as e:
            logger.warning(f"Error creating normalized columns for left DataFrame: {e}")
            # Fallback: create empty normalized columns
            left["_sn"] = pd.Series(dtype=str, index=left.index)
            left["_on"] = pd.Series(dtype=str, index=left.index)
            left["_t"] = pd.Series(dtype=str, index=left.index)
        
        try:
            right["_sn"] = right["schema_name"].astype(str).str.strip().str.upper()
            right["_on"] = right["object_name"].astype(str).str.strip().str.upper()
            right["_t"] = right["object_type"].astype(str).str.strip().str.upper()
        except Exception as e:
            logger.warning(f"Error creating normalized columns for right DataFrame: {e}")
            # Fallback: create empty normalized columns
            right["_sn"] = pd.Series(dtype=str, index=right.index)
            right["_on"] = pd.Series(dtype=str, index=right.index)
            right["_t"] = pd.Series(dtype=str, index=right.index)

        # Perform merge
        try:
            merged = left.merge(
                right,
                on=["_t", "_sn", "_on"],
                how="outer",
                indicator=True,
                suffixes=("_l", "_r"),
            )
        except Exception as e:
            logger.error(f"Error during merge: {e}")
            logger.error(f"Left columns: {list(left.columns)}")
            logger.error(f"Right columns: {list(right.columns)}")
            logger.error(f"Left empty: {left.empty}, Right empty: {right.empty}")
            return pd.DataFrame(columns=["ObjectType", "SourceSchemaName", "SourceObjectName", 
                                        "DestinationSchemaName", "DestinationObjectName", 
                                        "ChangeType", "ElementPath"])
        
        logger.info(f"After merge: {len(merged)} total rows")
        logger.info(f"Merged columns: {list(merged.columns)}")
        if "_merge" in merged.columns:
            merge_counts = merged["_merge"].value_counts()
            logger.info(f"Merge breakdown: {merge_counts.to_dict()}")
        else:
            logger.warning("_merge column not found in merged DataFrame")
        
        # Ensure merged DataFrame has expected columns (handle edge cases)
        expected_cols_l = ["object_type_l", "schema_name_l", "object_name_l"]
        expected_cols_r = ["object_type_r", "schema_name_r", "object_name_r"]
        for col in expected_cols_l + expected_cols_r:
            if col not in merged.columns:
                merged[col] = ""
        
        out_rows = []
        for idx, row in merged.iterrows():
            try:
                merge_indicator = row.get("_merge", "")
                if merge_indicator == "left_only":
                    # Use object_type_l since this row only exists in left
                    obj_type = row.get("object_type_l") or row.get("object_type", "") or "TABLE"
                    schema_l = row.get("schema_name_l", "") or row.get("schema_name", "")
                    obj_l = row.get("object_name_l", "") or row.get("object_name", "")
                    out_rows.append({
                        "ObjectType": str(obj_type) if obj_type else "TABLE",
                        "SourceSchemaName": str(schema_l) if schema_l else "",
                        "SourceObjectName": str(obj_l) if obj_l else "",
                        "DestinationSchemaName": "",
                        "DestinationObjectName": "",
                        "ChangeType": "MISSING_IN_TARGET",
                        "ElementPath": f"{schema_l}.{obj_l}" if schema_l and obj_l else (obj_l if obj_l else ""),
                    })
                elif merge_indicator == "right_only":
                    # Use object_type_r since this row only exists in right
                    obj_type = row.get("object_type_r") or row.get("object_type", "") or "TABLE"
                    schema_r = row.get("schema_name_r", "") or row.get("schema_name", "")
                    obj_r = row.get("object_name_r", "") or row.get("object_name", "")
                    out_rows.append({
                        "ObjectType": str(obj_type) if obj_type else "TABLE",
                        "SourceSchemaName": "",
                        "SourceObjectName": "",
                        "DestinationSchemaName": str(schema_r) if schema_r else "",
                        "DestinationObjectName": str(obj_r) if obj_r else "",
                        "ChangeType": "MISSING_IN_SOURCE",
                        "ElementPath": f"{schema_r}.{obj_r}" if schema_r and obj_r else (obj_r if obj_r else ""),
                    })
                # Note: "both" case means objects match - we don't add them to out_rows (only differences)
            except Exception as e:
                logger.error(f"Error processing merged row {idx}: {e}")
                logger.error(f"Row keys: {list(row.keys()) if hasattr(row, 'keys') else 'N/A'}")
                logger.error(f"Row data: {dict(row) if hasattr(row, '__dict__') else 'N/A'}")
                continue
        
        # Log results for debugging
        import logging
        logger = logging.getLogger(__name__)
        missing_in_target = sum(1 for r in out_rows if r.get("ChangeType") == "MISSING_IN_TARGET")
        missing_in_source = sum(1 for r in out_rows if r.get("ChangeType") == "MISSING_IN_SOURCE")
        logger.info(f"Presence check results: {len(out_rows)} total differences ({missing_in_target} missing in target, {missing_in_source} missing in source)")
        
        return pd.DataFrame(out_rows)

    def compare_column_datatypes_mapped(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Return DataFrame: ObjectType, SourceSchemaName, SourceObjectName, ColumnName, SourceDataType, DestinationDataType, ExpectedAzureType, Status."""
        if not object_types:
            object_types = ["TABLE"]
        
        is_db2_source = self._is_source_db2()
        conn_source = connect_source(self._get_config())
        conn_dest = connect_destination(self._get_config())
        
        try:
            if is_db2_source:
                cur = conn_source.cursor()
                source_tables = db2_schema.fetch_db2_tables(cur, source_schema)
                source_tables = [(t.schema_name.strip(), t.table_name.strip()) for t in source_tables]
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                # SQL Server source
                source_tables_df = azure_catalog.get_tables(conn_source, source_schema)
                source_tables = [(str(r["schema_name"]).strip(), str(r["object_name"]).strip()) for _, r in source_tables_df.iterrows()]
        finally:
            try:
                conn_source.close()
            except Exception:
                pass

        dest_tables = azure_catalog.get_tables(conn_dest, target_schema)
        if not dest_tables.empty:
            dest_tables_list = list(dest_tables.apply(lambda r: (str(r["schema_name"]).strip(), str(r["object_name"]).strip()), axis=1))
        else:
            dest_tables_list = []

        # Match tables by schema.object (case-insensitive)
        def norm_t(t):
            return (t[0].upper(), t[1].upper())
        set_dest = set(norm_t(t) for t in dest_tables_list)
        pairs = []
        for s, o in source_tables:
            if norm_t((s, o)) in set_dest:
                dest = next((x for x in dest_tables_list if norm_t(x) == norm_t((s, o))), None)
                if dest:
                    pairs.append((s, o, dest[0], dest[1]))

        if not pairs:
            try:
                conn_dest.close()
            except Exception:
                pass
            return pd.DataFrame(columns=[
                "ObjectType", "SourceSchemaName", "SourceObjectName", "ColumnName",
                "SourceDataType", "DestinationDataType", "ExpectedAzureType", "Status"
            ])

        s_schemas = list({p[0] for p in pairs})
        r_schemas = list({p[2] for p in pairs})
        cols_source = []
        conn_source_2 = connect_source(self._get_config())
        try:
            if is_db2_source:
                cur = conn_source_2.cursor()
                for schema, table in [(p[0], p[1]) for p in pairs]:
                    for c in db2_schema.fetch_db2_columns(cur, schema, table):
                        cols_source.append({
                            "schema_name": schema,
                            "table_name": table,
                            "column_name": c.column_name,
                            "data_type": c.data_type,
                            "length": c.length,
                            "scale": c.scale,
                        })
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                # SQL Server source - use azure_catalog
                cols_source_df = azure_catalog.get_columns_with_types(conn_source_2, s_schemas)
                for _, r in cols_source_df.iterrows():
                    cols_source.append({
                        "schema_name": str(r["schema_name"]).strip(),
                        "table_name": str(r["table_name"]).strip(),
                        "column_name": str(r["column_name"]).strip(),
                        "data_type": str(r["data_type"]).strip(),
                        "length": r.get("char_len"),
                        "scale": r.get("scale"),
                    })
        finally:
            try:
                conn_source_2.close()
            except Exception:
                pass
        
        cols_dest = azure_catalog.get_columns_with_types(conn_dest, r_schemas)
        try:
            conn_dest.close()
        except Exception:
            pass

        out_rows = []
        for schema, table, d_schema, d_table in pairs:
            source_cols = [x for x in cols_source if x["schema_name"].upper() == schema.upper() and x["table_name"].upper() == table.upper()]
            dest_cols = cols_dest[(cols_dest["schema_name"].astype(str).str.upper() == d_schema.upper()) & (cols_dest["table_name"].astype(str).str.upper() == d_table.upper())]
            dest_col_map = {str(r["column_name"]).strip().upper(): r for _, r in dest_cols.iterrows()}
            for c in source_cols:
                col_name = c["column_name"]
                cn_upper = col_name.strip().upper()
                dest_row = dest_col_map.get(cn_upper)
                src_type = (c["data_type"] or "").strip()
                # For SQL Server to SQL Server, expected type is the same as source type
                expected = get_expected_sql_type(src_type) if is_db2_source else src_type
                if dest_row is None:
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": schema,
                        "SourceObjectName": table,
                        "ColumnName": col_name,
                        "SourceDataType": src_type,
                        "DestinationDataType": "",
                        "ExpectedAzureType": expected,
                        "Status": "ERROR",
                    })
                    continue
                dest_type = str(dest_row.get("data_type", "") or "").strip()
                if is_db2_source:
                    result = validate_type_mapping(
                        db2_type=src_type,
                        db2_length=c.get("length"),
                        db2_scale=c.get("scale"),
                        sql_type=dest_type,
                        sql_length=dest_row.get("char_len"),
                        sql_scale=dest_row.get("scale"),
                        column_name=col_name,
                    )
                    status = result.get("status", "ERROR")
                else:
                    # SQL Server to SQL Server - simple type comparison
                    if src_type.upper() == dest_type.upper():
                        status = "SUCCESS"
                    else:
                        status = "MISMATCH"
                if status == "SUCCESS":
                    continue
                out_rows.append({
                    "ObjectType": "TABLE",
                    "SourceSchemaName": schema,
                    "SourceObjectName": table,
                    "ColumnName": col_name,
                    "SourceDataType": src_type,
                    "DestinationDataType": dest_type,
                    "ExpectedAzureType": expected,
                    "Status": status,
                })
        return pd.DataFrame(out_rows)

    def compare_column_default_values(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Return DataFrame: ObjectType, SourceSchemaName, SourceObjectName, ColumnName, SourceDefault, DestinationDefault, DefaultMatch, Status."""
        if not object_types:
            object_types = ["TABLE"]
        
        is_db2_source = self._is_source_db2()
        conn_source = connect_source(self._get_config())
        conn_dest = connect_destination(self._get_config())
        
        try:
            if is_db2_source:
                cur = conn_source.cursor()
                source_tables = [(t.schema_name.strip(), t.table_name.strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                source_tables_df = azure_catalog.get_tables(conn_source, source_schema)
                source_tables = [(str(r["schema_name"]).strip(), str(r["object_name"]).strip()) for _, r in source_tables_df.iterrows()]
        finally:
            try:
                conn_source.close()
            except Exception:
                pass

        dest_tables = azure_catalog.get_tables(conn_dest, target_schema)
        set_dest = set((str(r["schema_name"]).strip().upper(), str(r["object_name"]).strip().upper()) for _, r in dest_tables.iterrows())
        pairs = [(s, o, next((x for x in dest_tables.itertuples() if str(x.schema_name).strip().upper() == s.upper() and str(x.object_name).strip().upper() == o.upper()), None)) for s, o in source_tables if (s.upper(), o.upper()) in set_dest]
        pairs = [(s, o, str(p.schema_name).strip(), str(p.object_name).strip()) for s, o, p in pairs if p is not None]

        def canon_default(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return ""
            v = str(v).strip()
            v = re.sub(r"^'([^']*)'$", r"\1", v)
            v = re.sub(r"^\((.*)\)$", r"\1", v)
            return v.upper().strip()

        r_schemas = list({p[2] for p in pairs})
        defs_dest_all = azure_catalog.get_column_defaults(conn_dest, r_schemas) if pairs else pd.DataFrame()

        out_rows = []
        conn_source_2 = connect_source(self._get_config())
        try:
            if is_db2_source:
                cur = conn_source_2.cursor()
                s_schemas = list({p[0] for p in pairs})
                for schema, table, d_schema, d_table in pairs:
                    defs_dest = defs_dest_all[(defs_dest_all["schema_name"].astype(str).str.upper() == d_schema.upper()) & (defs_dest_all["table_name"].astype(str).str.upper() == d_table.upper())] if not defs_dest_all.empty else pd.DataFrame()
                    for c in db2_schema.fetch_db2_columns(cur, schema, table):
                        src_def = (c.default or "").strip()
                        src_canon = canon_default(src_def)
                        defs_col = defs_dest[(defs_dest["column_name"].astype(str).str.upper() == c.column_name.strip().upper())] if not defs_dest.empty else pd.DataFrame()
                        dest_def = ""
                        if not defs_col.empty:
                            dest_def = str(defs_col.iloc[0].get("default_str") or "").strip()
                        dest_canon = canon_default(dest_def)
                        match = "MATCH" if src_canon == dest_canon else "MISMATCH"
                        status = "SUCCESS" if match == "MATCH" else "WARNING"
                        if match == "MATCH" and not src_def and not dest_def:
                            continue
                        if match == "MATCH":
                            continue
                        out_rows.append({
                            "ObjectType": "TABLE",
                            "SourceSchemaName": schema,
                            "SourceObjectName": table,
                            "ColumnName": c.column_name,
                            "SourceDefault": src_def,
                            "DestinationDefault": dest_def,
                            "DefaultMatch": match,
                            "Status": status,
                        })
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                # SQL Server source - use azure_catalog
                s_schemas = list({p[0] for p in pairs})
                defs_source_all = azure_catalog.get_column_defaults(conn_source_2, s_schemas) if pairs else pd.DataFrame()
                for schema, table, d_schema, d_table in pairs:
                    defs_source = defs_source_all[(defs_source_all["schema_name"].astype(str).str.upper() == schema.upper()) & (defs_source_all["table_name"].astype(str).str.upper() == table.upper())] if not defs_source_all.empty else pd.DataFrame()
                    defs_dest = defs_dest_all[(defs_dest_all["schema_name"].astype(str).str.upper() == d_schema.upper()) & (defs_dest_all["table_name"].astype(str).str.upper() == d_table.upper())] if not defs_dest_all.empty else pd.DataFrame()
                    for _, src_col in defs_source.iterrows():
                        col_name = str(src_col["column_name"]).strip()
                        src_def = str(src_col.get("default_str") or "").strip()
                        src_canon = canon_default(src_def)
                        defs_col = defs_dest[(defs_dest["column_name"].astype(str).str.upper() == col_name.upper())] if not defs_dest.empty else pd.DataFrame()
                        dest_def = ""
                        if not defs_col.empty:
                            dest_def = str(defs_col.iloc[0].get("default_str") or "").strip()
                        dest_canon = canon_default(dest_def)
                        match = "MATCH" if src_canon == dest_canon else "MISMATCH"
                        status = "SUCCESS" if match == "MATCH" else "WARNING"
                        if match == "MATCH" and not src_def and not dest_def:
                            continue
                        if match == "MATCH":
                            continue
                        out_rows.append({
                            "ObjectType": "TABLE",
                            "SourceSchemaName": schema,
                            "SourceObjectName": table,
                            "ColumnName": col_name,
                            "SourceDefault": src_def,
                            "DestinationDefault": dest_def,
                            "DefaultMatch": match,
                            "Status": status,
                        })
        finally:
            try:
                conn_source_2.close()
            except Exception:
                pass
        try:
            conn_dest.close()
        except Exception:
            pass
        return pd.DataFrame(out_rows)

    def compare_index_definitions(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Return DataFrame: ObjectType, SourceSchemaName, SourceObjectName, IndexName, SourceCols, DestinationCols, Status. Also supports ValidationType/Status for distinct_key (tables without PK)."""
        if not object_types:
            object_types = ["TABLE"]
        
        is_db2_source = self._is_source_db2()
        
        if is_db2_source:
            # DB2 source - use DB2-specific functions
            conn_source = connect_db2(self._get_config())
        else:
            # SQL Server source - use connect_source
            conn_source = connect_source(self._get_config())
        
        conn_dest = connect_destination(self._get_config())
        
        try:
            if is_db2_source:
                cur = conn_source.cursor()
                source_tables = [(t.schema_name.strip(), t.table_name.strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
                source_ix = db2_schema.fetch_db2_indexes(cur, source_schema)
                source_pk_list = db2_schema.fetch_db2_primary_keys(cur, source_schema)
                source_pks = {((p["schema_name"] or "").strip(), (p["table_name"] or "").strip()) for p in source_pk_list}
                source_sigs = {}
                for idx in source_ix:
                    key = (idx.schema_name.strip(), idx.table_name.strip(), idx.index_name.strip())
                    cols = ",".join((c or "").strip().lstrip("+").lstrip("-") for c in (idx.columns or []))
                    source_sigs[key] = {"cols": cols, "unique": idx.unique}
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                # SQL Server source - use azure_catalog functions
                source_tables_df = azure_catalog.get_tables(conn_source, source_schema)
                source_tables = [(str(r["schema_name"]).strip(), str(r["object_name"]).strip()) for _, r in source_tables_df.iterrows()]
                s_schemas = list({t[0] for t in source_tables})
                source_ix = azure_catalog.get_index_columns(conn_source, s_schemas)
                source_pk_df = azure_catalog.get_primary_keys(conn_source, s_schemas)
                source_pks = set((str(r["schema_name"]).strip().upper(), str(r["table_name"]).strip().upper()) for _, r in source_pk_df.iterrows())
                # Build source index signatures
                source_sigs = {}
                for _, r in source_ix.iterrows():
                    sch, tbl, idx_name = str(r["schema_name"]).strip(), str(r["table_name"]).strip(), str(r["idx_name"]).strip()
                    key = (sch, tbl, idx_name)
                    if key not in source_sigs:
                        source_sigs[key] = {"cols_list": [], "unique": r.get("is_unique") or 0}
                    ord_col = (r.get("colseq") or 0, r.get("col_name") or "", (r.get("is_descending_key") or 0))
                    source_sigs[key]["cols_list"].append(ord_col)
                for k, v in source_sigs.items():
                    v["cols_list"].sort(key=lambda x: (x[0], x[1]))
                    v["cols"] = ",".join(x[1] for x in v["cols_list"])
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
                dest = next(iter(dest_tables[(dest_tables["schema_name"].astype(str).str.upper() == s.upper()) & (dest_tables["object_name"].astype(str).str.upper() == o.upper())].itertuples()), None)
                if dest:
                    pairs.append((s, o, str(dest.schema_name).strip(), str(dest.object_name).strip()))

        r_schemas = list({p[2] for p in pairs})
        dest_ix = azure_catalog.get_index_columns(conn_dest, r_schemas)
        dest_pks = azure_catalog.get_primary_keys(conn_dest, r_schemas)
        dest_pk_tables = set((str(r["schema_name"]).strip().upper(), str(r["table_name"]).strip().upper()) for _, r in dest_pks.iterrows())

        # Build destination index signature
        dest_sigs = {}
        for _, r in dest_ix.iterrows():
            sch, tbl, idx_name = str(r["schema_name"]).strip(), str(r["table_name"]).strip(), str(r["idx_name"]).strip()
            key = (sch, tbl, idx_name)
            if key not in dest_sigs:
                dest_sigs[key] = {"cols_list": [], "unique": r.get("is_unique") or 0}
            ord_col = (r.get("colseq") or 0, r.get("col_name") or "", (r.get("is_descending_key") or 0))
            dest_sigs[key]["cols_list"].append(ord_col)
        for k, v in dest_sigs.items():
            v["cols_list"].sort(key=lambda x: (x[0], x[1]))
            v["cols"] = ",".join(x[1] for x in v["cols_list"])

        out_rows = []
        for s, o, d_schema, d_table in pairs:
            # Tables without PK on both sides -> ValidationType PrimaryKey, Status info (for distinct_key)
            has_pk_source = (s.upper(), o.upper()) in source_pks
            has_pk_dest = (d_schema.upper(), d_table.upper()) in dest_pk_tables
            if not has_pk_source and not has_pk_dest:
                out_rows.append({
                    "ObjectType": "TABLE",
                    "SourceSchemaName": s,
                    "SourceObjectName": o,
                    "DestinationSchemaName": d_schema,
                    "DestinationObjectName": d_table,
                    "IndexName": "",
                    "SourceCols": "",
                    "DestinationCols": "",
                    "Status": "info",
                    "ValidationType": "PrimaryKey",
                })
            # Index comparison: match by (schema, table, index_name) and compare column list
            for (sch, tbl, idx_name), info in source_sigs.items():
                if sch.upper() != s.upper() or tbl.upper() != o.upper():
                    continue
                d_key = (d_schema, d_table, idx_name)
                dest_info = dest_sigs.get(d_key)
                if not dest_info:
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s,
                        "SourceObjectName": o,
                        "IndexName": idx_name,
                        "SourceCols": info["cols"],
                        "DestinationCols": "",
                        "Status": "MISSING_IN_TARGET",
                    })
                elif info["cols"] != dest_info["cols"]:
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s,
                        "SourceObjectName": o,
                        "IndexName": idx_name,
                        "SourceCols": info["cols"],
                        "DestinationCols": dest_info["cols"],
                        "Status": "MISMATCH",
                    })
        try:
            conn_dest.close()
        except Exception:
            pass
        return pd.DataFrame(out_rows)

    def compare_foreign_keys(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Return DataFrame: ObjectType, SourceSchemaName, SourceObjectName, FkName, SourceRefTable, DestinationRefTable, Status."""
        is_db2_source = self._is_source_db2()
        conn_source = connect_source(self._get_config())
        conn_dest = connect_destination(self._get_config())
        
        try:
            if is_db2_source:
                cur = conn_source.cursor()
                source_fks = db2_schema.fetch_db2_foreign_keys(cur, source_schema)
                source_tables = [(t.schema_name.strip(), t.table_name.strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                # SQL Server source
                source_tables_df = azure_catalog.get_tables(conn_source, source_schema)
                source_tables = [(str(r["schema_name"]).strip(), str(r["object_name"]).strip()) for _, r in source_tables_df.iterrows()]
                s_schemas = list({t[0] for t in source_tables})
                source_fks_df = azure_catalog.get_foreign_keys(conn_source, s_schemas)
                # Convert to DB2-like format
                source_fks = []
                for _, r in source_fks_df.iterrows():
                    source_fks.append({
                        "schema_name": str(r["schema_name"]).strip(),
                        "table_name": str(r["table_name"]).strip(),
                        "constraint_name": str(r["fk_name"]).strip(),
                        "ref_schema": str(r["ref_schema_name"]).strip(),
                        "ref_table": str(r["ref_table_name"]).strip(),
                    })
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
                dest = next(iter(dest_tables[(dest_tables["schema_name"].astype(str).str.upper() == s.upper()) & (dest_tables["object_name"].astype(str).str.upper() == o.upper())].itertuples()), None)
                if dest:
                    pairs.append((s, o, str(dest.schema_name).strip(), str(dest.object_name).strip()))
        r_schemas = list({p[2] for p in pairs})
        dest_fks = azure_catalog.get_foreign_keys(conn_dest, r_schemas)
        try:
            conn_dest.close()
        except Exception:
            pass

        source_fk_by_table = {}
        for fk in source_fks:
            sch, tbl = (fk["schema_name"] or "").strip().upper(), (fk["table_name"] or "").strip().upper()
            name = (fk["constraint_name"] or "").strip().upper()
            ref = f"{(fk.get('ref_schema') or '').strip()}.{(fk.get('ref_table') or '').strip()}"
            key = (sch, tbl)
            if key not in source_fk_by_table:
                source_fk_by_table[key] = {}
            source_fk_by_table[key][name] = ref
        dest_fk_by_table = {}
        for _, r in dest_fks.iterrows():
            sch = str(r["schema_name"]).strip().upper()
            tbl = str(r["table_name"]).strip().upper()
            name = str(r["fk_name"]).strip().upper()
            ref = f"{str(r['ref_schema_name']).strip()}.{str(r['ref_table_name']).strip()}"
            key = (sch, tbl)
            if key not in dest_fk_by_table:
                dest_fk_by_table[key] = {}
            dest_fk_by_table[key][name] = ref

        out_rows = []
        for s, o, d_schema, d_table in pairs:
            d_key = (d_schema.upper(), d_table.upper())
            s_key = (s.upper(), o.upper())
            source_fks_here = source_fk_by_table.get(s_key, {})
            dest_fks_here = dest_fk_by_table.get(d_key, {})
            all_names = set(source_fks_here.keys()) | set(dest_fks_here.keys())
            for name in all_names:
                src_ref = source_fks_here.get(name)
                dest_ref = dest_fks_here.get(name)
                if src_ref is None:
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": "",
                        "SourceObjectName": "",
                        "FkName": name,
                        "SourceRefTable": "",
                        "DestinationRefTable": dest_ref or "",
                        "Status": "EXTRA_IN_TARGET",
                    })
                elif dest_ref is None:
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s,
                        "SourceObjectName": o,
                        "FkName": name,
                        "SourceRefTable": src_ref,
                        "DestinationRefTable": "",
                        "Status": "MISSING_IN_TARGET",
                    })
                elif src_ref.upper() != dest_ref.upper():
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s,
                        "SourceObjectName": o,
                        "FkName": name,
                        "SourceRefTable": src_ref,
                        "DestinationRefTable": dest_ref,
                        "Status": "MISMATCH",
                    })
        return pd.DataFrame(out_rows)

    def compare_column_nullable_constraints(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Return DataFrame: ObjectType, SourceSchemaName, SourceObjectName, ColumnName, SourceNullable, DestinationNullable, Status."""
        if not object_types:
            object_types = ["TABLE"]
        
        is_db2_source = self._is_source_db2()
        conn_source = connect_source(self._get_config())
        conn_dest = connect_destination(self._get_config())
        
        try:
            if is_db2_source:
                cur = conn_source.cursor()
                source_tables = [(t.schema_name.strip(), t.table_name.strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                source_tables_df = azure_catalog.get_tables(conn_source, source_schema)
                source_tables = [(str(r["schema_name"]).strip(), str(r["object_name"]).strip()) for _, r in source_tables_df.iterrows()]
        finally:
            try:
                conn_source.close()
            except Exception:
                pass

        dest_tables = azure_catalog.get_tables(conn_dest, target_schema)
        set_dest = set((str(r["schema_name"]).strip().upper(), str(r["object_name"]).strip().upper()) for _, r in dest_tables.iterrows())
        pairs = [(s, o, str(r.schema_name).strip(), str(r.object_name).strip()) for s, o in source_tables if (s.upper(), o.upper()) in set_dest for r in dest_tables[(dest_tables["schema_name"].astype(str).str.upper() == s.upper()) & (dest_tables["object_name"].astype(str).str.upper() == o.upper())].itertuples()]
        pairs = list({(p[0], p[1], p[2], p[3]) for p in pairs})

        dest_null = azure_catalog.get_columns_nullable(conn_dest, list({p[2] for p in pairs}))
        dest_map = {}
        for _, r in dest_null.iterrows():
            key = (str(r["schema_name"]).strip().upper(), str(r["table_name"]).strip().upper(), str(r["column_name"]).strip().upper())
            dest_map[key] = str(r.get("is_nullable", "YES")).strip().upper()

        out_rows = []
        conn_source_2 = connect_source(self._get_config())
        try:
            if is_db2_source:
                cur = conn_source_2.cursor()
                for schema, table, d_schema, d_table in pairs:
                    for c in db2_schema.fetch_db2_columns(cur, schema, table):
                        src_null = "YES" if c.nullable else "NO"
                        key = (d_schema.upper(), d_table.upper(), c.column_name.strip().upper())
                        dest_null_val = dest_map.get(key, "")
                        status = "MATCH" if src_null == dest_null_val else "MISMATCH"
                        if status == "MATCH":
                            continue
                        out_rows.append({
                            "ObjectType": "TABLE",
                            "SourceSchemaName": schema,
                            "SourceObjectName": table,
                            "ColumnName": c.column_name,
                            "SourceNullable": src_null,
                            "DestinationNullable": dest_null_val,
                            "Status": status,
                        })
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                # SQL Server source - use azure_catalog
                s_schemas = list({p[0] for p in pairs})
                source_null = azure_catalog.get_columns_nullable(conn_source_2, s_schemas)
                source_map = {}
                for _, r in source_null.iterrows():
                    key = (str(r["schema_name"]).strip().upper(), str(r["table_name"]).strip().upper(), str(r["column_name"]).strip().upper())
                    source_map[key] = str(r.get("is_nullable", "YES")).strip().upper()
                
                for schema, table, d_schema, d_table in pairs:
                    source_cols = source_null[(source_null["schema_name"].astype(str).str.upper() == schema.upper()) & (source_null["table_name"].astype(str).str.upper() == table.upper())]
                    for _, src_col in source_cols.iterrows():
                        col_name = str(src_col["column_name"]).strip()
                        src_null = str(src_col.get("is_nullable", "YES")).strip().upper()
                        key = (d_schema.upper(), d_table.upper(), col_name.upper())
                        dest_null_val = dest_map.get(key, "")
                        status = "MATCH" if src_null == dest_null_val else "MISMATCH"
                        if status == "MATCH":
                            continue
                        out_rows.append({
                            "ObjectType": "TABLE",
                            "SourceSchemaName": schema,
                            "SourceObjectName": table,
                            "ColumnName": col_name,
                            "SourceNullable": src_null,
                            "DestinationNullable": dest_null_val,
                            "Status": status,
                        })
        finally:
            try:
                conn_source_2.close()
            except Exception:
                pass
        try:
            conn_dest.close()
        except Exception:
            pass
        return pd.DataFrame(out_rows)

    def compare_check_constraints(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Return DataFrame with same shape as other comparisons: ObjectType, SourceSchemaName, SourceObjectName, ConstraintName, Status."""
        is_db2_source = self._is_source_db2()
        conn_source = connect_source(self._get_config())
        conn_dest = connect_destination(self._get_config())
        
        try:
            if is_db2_source:
                cur = conn_source.cursor()
                source_checks = db2_schema.fetch_db2_check_constraints(cur, source_schema)
                try:
                    cur.close()
                except Exception:
                    pass
            else:
                # SQL Server source - use azure_catalog
                source_tables_df = azure_catalog.get_tables(conn_source, source_schema)
                s_schemas = list(set(str(r["schema_name"]).strip() for _, r in source_tables_df.iterrows())) if not source_tables_df.empty else []
                source_checks_df = azure_catalog.get_check_constraints(conn_source, s_schemas)
                # Convert to DB2-like format
                source_checks = []
                for _, r in source_checks_df.iterrows():
                    source_checks.append({
                        "schema_name": str(r["schema_name"]).strip(),
                        "table_name": str(r["table_name"]).strip(),
                        "constraint_name": str(r["chk_name"]).strip(),
                        "definition": str(r.get("chk_def") or "").strip(),
                    })
        finally:
            try:
                conn_source.close()
            except Exception:
                pass

        dest_tables = azure_catalog.get_tables(conn_dest, target_schema)
        r_schemas = list(set(str(r["schema_name"]).strip() for _, r in dest_tables.iterrows())) if not dest_tables.empty else []
        dest_checks = azure_catalog.get_check_constraints(conn_dest, r_schemas)
        try:
            conn_dest.close()
        except Exception:
            pass

        source_map = {(str(c["schema_name"]).strip().upper(), str(c["table_name"]).strip().upper(), str(c["constraint_name"]).strip().upper()): c.get("definition", "") for c in source_checks}
        dest_map = {}
        for _, r in dest_checks.iterrows():
            key = (str(r["schema_name"]).strip().upper(), str(r["table_name"]).strip().upper(), str(r["chk_name"]).strip().upper())
            dest_map[key] = str(r.get("chk_def") or "").strip()

        out_rows = []
        all_keys = set(source_map.keys()) | set(dest_map.keys())
        for (sch, tbl, name) in all_keys:
            src_def = source_map.get((sch, tbl, name), "")
            dest_def = dest_map.get((sch, tbl, name), "")
            if not src_def:
                out_rows.append({"ObjectType": "TABLE", "SourceSchemaName": "", "SourceObjectName": "", "ConstraintName": name, "Status": "EXTRA_IN_TARGET"})
            elif not dest_def:
                out_rows.append({"ObjectType": "TABLE", "SourceSchemaName": sch, "SourceObjectName": tbl, "ConstraintName": name, "Status": "MISSING_IN_TARGET"})
            elif src_def.upper() != dest_def.upper():
                out_rows.append({"ObjectType": "TABLE", "SourceSchemaName": sch, "SourceObjectName": tbl, "ConstraintName": name, "Status": "MISMATCH"})
        return pd.DataFrame(out_rows)
