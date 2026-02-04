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
from .connections import connect_db2, connect_azure_sql
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
    """Build DataFrame with columns object_type, schema_name, object_name for Azure."""
    df = azure_catalog.get_objects(conn, object_types, schema)
    if df.empty:
        return pd.DataFrame(columns=["object_type", "schema_name", "object_name"])
    type_map = {"U": "TABLE", "V": "VIEW", "P": "PROCEDURE", "FN": "FUNCTION", "IF": "FUNCTION", "TF": "FUNCTION"}
    df["object_type"] = df["type"].astype(str).map(lambda x: type_map.get(x, "OTHER"))
    df = df[df["object_type"].isin(object_types)]
    return df[["object_type", "schema_name", "object_name"]].copy()


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
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur_db2 = conn_db2.cursor()
            left = _list_db2_objects(cur_db2, object_types, source_schema)
            right = _list_azure_objects(conn_azure, object_types, target_schema)
        finally:
            try:
                conn_db2.close()
            except Exception:
                pass
            try:
                conn_azure.close()
            except Exception:
                pass

        left = left.copy()
        left["_sn"] = left["schema_name"].str.strip().str.upper()
        left["_on"] = left["object_name"].str.strip().str.upper()
        left["_t"] = left["object_type"].str.strip().str.upper()
        right = right.copy()
        right["_sn"] = right["schema_name"].astype(str).str.strip().str.upper()
        right["_on"] = right["object_name"].astype(str).str.strip().str.upper()
        right["_t"] = right["object_type"].astype(str).str.strip().str.upper()

        merged = left.merge(
            right,
            on=["_t", "_sn", "_on"],
            how="outer",
            indicator=True,
            suffixes=("_l", "_r"),
        )
        out_rows = []
        for _, row in merged.iterrows():
            if row["_merge"] == "left_only":
                out_rows.append({
                    "ObjectType": row["object_type"],
                    "SourceSchemaName": row["schema_name_l"],
                    "SourceObjectName": row["object_name_l"],
                    "DestinationSchemaName": "",
                    "DestinationObjectName": "",
                    "ChangeType": "MISSING_IN_TARGET",
                    "ElementPath": f"{row['schema_name_l']}.{row['object_name_l']}",
                })
            elif row["_merge"] == "right_only":
                out_rows.append({
                    "ObjectType": row["object_type"],
                    "SourceSchemaName": "",
                    "SourceObjectName": "",
                    "DestinationSchemaName": row["schema_name_r"],
                    "DestinationObjectName": row["object_name_r"],
                    "ChangeType": "MISSING_IN_SOURCE",
                    "ElementPath": f"{row['schema_name_r']}.{row['object_name_r']}",
                })
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
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur = conn_db2.cursor()
            tables_db2 = db2_schema.fetch_db2_tables(cur, source_schema)
            tables_db2 = [(t.schema_name.strip(), t.table_name.strip()) for t in tables_db2]
        finally:
            try:
                conn_db2.close()
            except Exception:
                pass

        az_tables = azure_catalog.get_tables(conn_azure, target_schema)
        if not az_tables.empty:
            az_tables = list(az_tables.apply(lambda r: (str(r["schema_name"]).strip(), str(r["object_name"]).strip()), axis=1))
        else:
            az_tables = []

        # Match tables by schema.object (case-insensitive)
        def norm_t(t):
            return (t[0].upper(), t[1].upper())
        set_az = set(norm_t(t) for t in az_tables)
        pairs = []
        for s, o in tables_db2:
            if norm_t((s, o)) in set_az:
                dest = next((x for x in az_tables if norm_t(x) == norm_t((s, o))), None)
                if dest:
                    pairs.append((s, o, dest[0], dest[1]))

        if not pairs:
            try:
                conn_azure.close()
            except Exception:
                pass
            return pd.DataFrame(columns=[
                "ObjectType", "SourceSchemaName", "SourceObjectName", "ColumnName",
                "SourceDataType", "DestinationDataType", "ExpectedAzureType", "Status"
            ])

        s_schemas = list({p[0] for p in pairs})
        r_schemas = list({p[2] for p in pairs})
        cols_db2 = []
        conn_db2_2 = connect_db2(self._get_config())
        try:
            cur = conn_db2_2.cursor()
            for schema, table in [(p[0], p[1]) for p in pairs]:
                for c in db2_schema.fetch_db2_columns(cur, schema, table):
                    cols_db2.append({
                        "schema_name": schema,
                        "table_name": table,
                        "column_name": c.column_name,
                        "data_type": c.data_type,
                        "length": c.length,
                        "scale": c.scale,
                    })
        finally:
            try:
                conn_db2_2.close()
            except Exception:
                pass
        cols_az = azure_catalog.get_columns_with_types(conn_azure, r_schemas)
        try:
            conn_azure.close()
        except Exception:
            pass

        out_rows = []
        for schema, table, d_schema, d_table in pairs:
            db2_cols = [x for x in cols_db2 if x["schema_name"] == schema and x["table_name"] == table]
            az_cols = cols_az[(cols_az["schema_name"].astype(str).str.upper() == d_schema.upper()) & (cols_az["table_name"].astype(str).str.upper() == d_table.upper())]
            az_col_map = {str(r["column_name"]).strip().upper(): r for _, r in az_cols.iterrows()}
            for c in db2_cols:
                col_name = c["column_name"]
                cn_upper = col_name.strip().upper()
                az_row = az_col_map.get(cn_upper)
                src_type = (c["data_type"] or "").strip()
                expected = get_expected_sql_type(src_type)
                if az_row is None:
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
                dest_type = str(az_row.get("data_type", "") or "").strip()
                result = validate_type_mapping(
                    db2_type=src_type,
                    db2_length=c.get("length"),
                    db2_scale=c.get("scale"),
                    sql_type=dest_type,
                    sql_length=az_row.get("char_len"),
                    sql_scale=az_row.get("scale"),
                    column_name=col_name,
                )
                status = result.get("status", "ERROR")
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
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur = conn_db2.cursor()
            tables_db2 = [(t.schema_name.strip(), t.table_name.strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
        finally:
            try:
                conn_db2.close()
            except Exception:
                pass

        az_tables = azure_catalog.get_tables(conn_azure, target_schema)
        set_az = set((str(r["schema_name"]).strip().upper(), str(r["object_name"]).strip().upper()) for _, r in az_tables.iterrows())
        pairs = [(s, o, next((x for x in az_tables.itertuples() if str(x.schema_name).strip().upper() == s.upper() and str(x.object_name).strip().upper() == o.upper()), None)) for s, o in tables_db2 if (s.upper(), o.upper()) in set_az]
        pairs = [(s, o, str(p.schema_name).strip(), str(p.object_name).strip()) for s, o, p in pairs if p is not None]

        def canon_default(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return ""
            v = str(v).strip()
            v = re.sub(r"^'([^']*)'$", r"\1", v)
            v = re.sub(r"^\((.*)\)$", r"\1", v)
            return v.upper().strip()

        r_schemas = list({p[2] for p in pairs})
        defs_az_all = azure_catalog.get_column_defaults(conn_azure, r_schemas) if pairs else pd.DataFrame()

        out_rows = []
        conn_db2_2 = connect_db2(self._get_config())
        try:
            cur = conn_db2_2.cursor()
            for schema, table, d_schema, d_table in pairs:
                defs_az = defs_az_all[(defs_az_all["schema_name"].astype(str).str.upper() == d_schema.upper()) & (defs_az_all["table_name"].astype(str).str.upper() == d_table.upper())] if not defs_az_all.empty else pd.DataFrame()
                for c in db2_schema.fetch_db2_columns(cur, schema, table):
                    src_def = (c.default or "").strip()
                    src_canon = canon_default(src_def)
                    defs_col = defs_az[(defs_az["column_name"].astype(str).str.upper() == c.column_name.strip().upper())] if not defs_az.empty else pd.DataFrame()
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
        finally:
            try:
                conn_db2_2.close()
            except Exception:
                pass
        try:
            conn_azure.close()
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
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur = conn_db2.cursor()
            tables_db2 = [(t.schema_name.strip(), t.table_name.strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
            db2_ix = db2_schema.fetch_db2_indexes(cur, source_schema)
            db2_pk_list = db2_schema.fetch_db2_primary_keys(cur, source_schema)
            db2_pks = {((p["schema_name"] or "").strip(), (p["table_name"] or "").strip()) for p in db2_pk_list}
            db2_sigs = {}
            for idx in db2_ix:
                key = (idx.schema_name.strip(), idx.table_name.strip(), idx.index_name.strip())
                cols = ",".join((c or "").strip().lstrip("+").lstrip("-") for c in (idx.columns or []))
                db2_sigs[key] = {"cols": cols, "unique": idx.unique}
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
                dest = next(iter(az_tables[(az_tables["schema_name"].astype(str).str.upper() == s.upper()) & (az_tables["object_name"].astype(str).str.upper() == o.upper())].itertuples()), None)
                if dest:
                    pairs.append((s, o, str(dest.schema_name).strip(), str(dest.object_name).strip()))

        s_schemas = list({p[0] for p in pairs})
        r_schemas = list({p[2] for p in pairs})
        az_ix = azure_catalog.get_index_columns(conn_azure, r_schemas)
        az_pks = azure_catalog.get_primary_keys(conn_azure, r_schemas)
        az_pk_tables = set((str(r["schema_name"]).strip().upper(), str(r["table_name"]).strip().upper()) for _, r in az_pks.iterrows())

        # Build Azure index signature
        az_sigs = {}
        for _, r in az_ix.iterrows():
            sch, tbl, idx_name = str(r["schema_name"]).strip(), str(r["table_name"]).strip(), str(r["idx_name"]).strip()
            key = (sch, tbl, idx_name)
            if key not in az_sigs:
                az_sigs[key] = {"cols_list": [], "unique": r.get("is_unique") or 0}
            ord_col = (r.get("colseq") or 0, r.get("col_name") or "", (r.get("is_descending_key") or 0))
            az_sigs[key]["cols_list"].append(ord_col)
        for k, v in az_sigs.items():
            v["cols_list"].sort(key=lambda x: (x[0], x[1]))
            v["cols"] = ",".join(x[1] for x in v["cols_list"])

        out_rows = []
        for s, o, d_schema, d_table in pairs:
            # Tables without PK on both sides -> ValidationType PrimaryKey, Status info (for distinct_key)
            has_pk_db2 = (s.upper(), o.upper()) in db2_pks
            has_pk_az = (d_schema.upper(), d_table.upper()) in az_pk_tables
            if not has_pk_db2 and not has_pk_az:
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
            for (sch, tbl, idx_name), info in db2_sigs.items():
                if sch != s or tbl != o:
                    continue
                d_key = (d_schema, d_table, idx_name)
                az_info = az_sigs.get(d_key)
                if not az_info:
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s,
                        "SourceObjectName": o,
                        "IndexName": idx_name,
                        "SourceCols": info["cols"],
                        "DestinationCols": "",
                        "Status": "MISSING_IN_TARGET",
                    })
                elif info["cols"] != az_info["cols"]:
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s,
                        "SourceObjectName": o,
                        "IndexName": idx_name,
                        "SourceCols": info["cols"],
                        "DestinationCols": az_info["cols"],
                        "Status": "MISMATCH",
                    })
        try:
            conn_azure.close()
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
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur = conn_db2.cursor()
            db2_fks = db2_schema.fetch_db2_foreign_keys(cur, source_schema)
            tables_db2 = [(t.schema_name.strip(), t.table_name.strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
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
                dest = next(iter(az_tables[(az_tables["schema_name"].astype(str).str.upper() == s.upper()) & (az_tables["object_name"].astype(str).str.upper() == o.upper())].itertuples()), None)
                if dest:
                    pairs.append((s, o, str(dest.schema_name).strip(), str(dest.object_name).strip()))
        r_schemas = list({p[2] for p in pairs})
        az_fks = azure_catalog.get_foreign_keys(conn_azure, r_schemas)
        try:
            conn_azure.close()
        except Exception:
            pass

        db2_fk_by_table = {}
        for fk in db2_fks:
            sch, tbl = (fk["schema_name"] or "").strip().upper(), (fk["table_name"] or "").strip().upper()
            name = (fk["constraint_name"] or "").strip().upper()
            ref = f"{(fk.get('ref_schema') or '').strip()}.{(fk.get('ref_table') or '').strip()}"
            key = (sch, tbl)
            if key not in db2_fk_by_table:
                db2_fk_by_table[key] = {}
            db2_fk_by_table[key][name] = ref
        az_fk_by_table = {}
        for _, r in az_fks.iterrows():
            sch = str(r["schema_name"]).strip().upper()
            tbl = str(r["table_name"]).strip().upper()
            name = str(r["fk_name"]).strip().upper()
            ref = f"{str(r['ref_schema_name']).strip()}.{str(r['ref_table_name']).strip()}"
            key = (sch, tbl)
            if key not in az_fk_by_table:
                az_fk_by_table[key] = {}
            az_fk_by_table[key][name] = ref

        out_rows = []
        for s, o, d_schema, d_table in pairs:
            d_key = (d_schema.upper(), d_table.upper())
            s_key = (s.upper(), o.upper())
            db2_fks_here = db2_fk_by_table.get(s_key, {})
            az_fks_here = az_fk_by_table.get(d_key, {})
            all_names = set(db2_fks_here.keys()) | set(az_fks_here.keys())
            for name in all_names:
                src_ref = db2_fks_here.get(name)
                dest_ref = az_fks_here.get(name)
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
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur = conn_db2.cursor()
            tables_db2 = [(t.schema_name.strip(), t.table_name.strip()) for t in db2_schema.fetch_db2_tables(cur, source_schema)]
        finally:
            try:
                conn_db2.close()
            except Exception:
                pass

        az_tables = azure_catalog.get_tables(conn_azure, target_schema)
        set_az = set((str(r["schema_name"]).strip().upper(), str(r["object_name"]).strip().upper()) for _, r in az_tables.iterrows())
        pairs = [(s, o, str(r.schema_name).strip(), str(r.object_name).strip()) for s, o in tables_db2 if (s.upper(), o.upper()) in set_az for r in az_tables[(az_tables["schema_name"].astype(str).str.upper() == s.upper()) & (az_tables["object_name"].astype(str).str.upper() == o.upper())].itertuples()]
        pairs = list({(p[0], p[1], p[2], p[3]) for p in pairs})

        az_null = azure_catalog.get_columns_nullable(conn_azure, list({p[2] for p in pairs}))
        az_map = {}
        for _, r in az_null.iterrows():
            key = (str(r["schema_name"]).strip().upper(), str(r["table_name"]).strip().upper(), str(r["column_name"]).strip().upper())
            az_map[key] = str(r.get("is_nullable", "YES")).strip().upper()

        out_rows = []
        conn_db2_2 = connect_db2(self._get_config())
        try:
            cur = conn_db2_2.cursor()
            for schema, table, d_schema, d_table in pairs:
                for c in db2_schema.fetch_db2_columns(cur, schema, table):
                    src_null = "YES" if c.nullable else "NO"
                    key = (d_schema.upper(), d_table.upper(), c.column_name.strip().upper())
                    dest_null = az_map.get(key, "")
                    status = "MATCH" if src_null == dest_null else "MISMATCH"
                    if status == "MATCH":
                        continue
                    out_rows.append({
                        "ObjectType": "TABLE",
                        "SourceSchemaName": schema,
                        "SourceObjectName": table,
                        "ColumnName": c.column_name,
                        "SourceNullable": src_null,
                        "DestinationNullable": dest_null,
                        "Status": status,
                    })
        finally:
            try:
                conn_db2_2.close()
            except Exception:
                pass
        try:
            conn_azure.close()
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
        conn_db2 = connect_db2(self._get_config())
        conn_azure = connect_azure_sql(self._get_config())
        try:
            cur = conn_db2.cursor()
            db2_checks = db2_schema.fetch_db2_check_constraints(cur, source_schema)
        finally:
            try:
                conn_db2.close()
            except Exception:
                pass

        r_schemas = list(set(str(r["schema_name"]).strip() for _, r in azure_catalog.get_tables(conn_azure, target_schema).iterrows())) if not azure_catalog.get_tables(conn_azure, target_schema).empty else []
        az_checks = azure_catalog.get_check_constraints(conn_azure, r_schemas)
        try:
            conn_azure.close()
        except Exception:
            pass

        db2_map = {(str(c["schema_name"]).strip().upper(), str(c["table_name"]).strip().upper(), str(c["constraint_name"]).strip().upper()): c.get("definition", "") for c in db2_checks}
        az_map = {}
        for _, r in az_checks.iterrows():
            key = (str(r["schema_name"]).strip().upper(), str(r["table_name"]).strip().upper(), str(r["chk_name"]).strip().upper())
            az_map[key] = str(r.get("chk_def") or "").strip()

        out_rows = []
        all_keys = set(db2_map.keys()) | set(az_map.keys())
        for (sch, tbl, name) in all_keys:
            src_def = db2_map.get((sch, tbl, name), "")
            dest_def = az_map.get((sch, tbl, name), "")
            if not src_def:
                out_rows.append({"ObjectType": "TABLE", "SourceSchemaName": "", "SourceObjectName": "", "ConstraintName": name, "Status": "EXTRA_IN_TARGET"})
            elif not dest_def:
                out_rows.append({"ObjectType": "TABLE", "SourceSchemaName": sch, "SourceObjectName": tbl, "ConstraintName": name, "Status": "MISSING_IN_TARGET"})
            elif src_def.upper() != dest_def.upper():
                out_rows.append({"ObjectType": "TABLE", "SourceSchemaName": sch, "SourceObjectName": tbl, "ConstraintName": name, "Status": "MISMATCH"})
        return pd.DataFrame(out_rows)
