# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Data validation service thin wrappers.

These classes decouple data-validation usage from schema/behavior while
reusing the existing implementation in PySparkSchemaComparisonService.
"""
from typing import Optional, Dict, Any, List

import os
import uuid
import json
import time
from datetime import datetime
from collections import defaultdict
from functools import reduce

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    concat,
    concat_ws,
    upper,
    trim,
    coalesce,
    lower,
)
from pyspark.sql.types import StructType, StructField, StringType

from db2_azure_validation.services.pyspark_schema_comparison import (
    PySparkSchemaComparisonService,
    PySparkAzureSchemaComparisonService,
    log_duration,
)


class PySparkDataValidationService(PySparkSchemaComparisonService):
    """Data validations for DB2 → Azure."""

    def __init__(
        self,
        *,
        config_filename: Optional[str] = None,
        side_config_map: Optional[Dict[str, str]] = None,
        side_labels: Optional[Dict[str, str]] = None,
        side_engine_map: Optional[Dict[str, str]] = None,
        app_name: Optional[str] = None,
        access_token_override: Optional[str] = None,
    ):
        super().__init__(
            config_filename=config_filename,
            side_config_map=side_config_map,
            side_labels=side_labels,
            side_engine_map=side_engine_map,
            app_name=app_name,
            access_token_override=access_token_override,
        )

    # Thin wrappers to expose data comparisons via this service
    @log_duration("compare_row_counts")
    def compare_row_counts(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        clean = self._clean_identifier
        if not object_types:
            object_types = ["TABLE", "VIEW"]
        left = self._read_tables("db2", object_types, source_schema)
        right = self._read_tables("azure", object_types, target_schema)

        if not source_schema and not target_schema:
            l_schemas = left.select("schema_norm").distinct().withColumnRenamed("schema_norm", "s")
            r_schemas = right.select("schema_norm").distinct().withColumnRenamed("schema_norm", "t")
            matched = l_schemas.join(r_schemas, col("s") == col("t"), "inner").select(col("s").alias("schema_norm"))
            left = left.join(matched, "schema_norm", "inner")
            right = right.join(matched, "schema_norm", "inner")

        l = left.withColumn("key", concat_ws(".", col("schema_norm"), col("object_norm")))
        r = right.withColumn("key", concat_ws(".", col("schema_norm"), col("object_norm")))

        pairs = (
            l.select("schema_name", "object_name", "key", col("obj_type_norm").alias("l_type"))
            .alias("l")
            .join(
                r.select(
                    col("schema_name").alias("r_schema"),
                    col("object_name").alias("r_object"),
                    col("key").alias("r_key"),
                    col("obj_type_norm").alias("r_type"),
                ),
                on=col("key") == col("r_key"),
                how="full_outer",
            )
            .select(
                coalesce(col("schema_name"), col("r_schema")).alias("schema_name"),
                coalesce(col("object_name"), col("r_object")).alias("object_name"),
                col("schema_name").alias("l_schema"),
                col("object_name").alias("l_object"),
                col("r_schema"),
                col("r_object"),
                col("l_type"),
                col("r_type"),
            )
        )

        pairs_rows = pairs.collect()

        l_items = [(clean(r["l_schema"]), clean(r["l_object"])) for r in pairs_rows if r["l_schema"] and r["l_object"]]
        r_items = [(clean(r["r_schema"]), clean(r["r_object"])) for r in pairs_rows if r["r_schema"] and r["r_object"]]

        l_est_map: dict[tuple[str, str], int] = {}
        r_est_map: dict[tuple[str, str], int] = {}
        l_schemas = sorted(list({s for s, _ in l_items})) if l_items else None
        r_schemas = sorted(list({s for s, _ in r_items})) if r_items else None
        if l_schemas is not None:
            l_est_df = self._fetch_estimated_counts_all("db2", l_schemas, object_types or ["TABLE", "VIEW"])
            for row in l_est_df.collect():
                l_key = (self._norm_identifier(row["schema_name"]), self._norm_identifier(row["object_name"]))
                l_est_map[l_key] = int(row["cnt"] or 0)
        if r_schemas is not None:
            r_est_df = self._fetch_estimated_counts_all("azure", r_schemas, object_types or ["TABLE", "VIEW"])
            for row in r_est_df.collect():
                r_key = (self._norm_identifier(row["schema_name"]), self._norm_identifier(row["object_name"]))
                r_est_map[r_key] = int(row["cnt"] or 0)

        exact_l = []
        exact_r = []
        rows = []
        for r in pairs_rows:
            s_schema_raw, s_obj_raw = r["l_schema"], r["l_object"]
            t_schema_raw, t_obj_raw = r["r_schema"], r["r_object"]
            s_schema = clean(s_schema_raw)
            s_obj = clean(s_obj_raw)
            t_schema = clean(t_schema_raw)
            t_obj = clean(t_obj_raw)
            if s_schema and not t_schema:
                exact_l.append((s_schema, s_obj))
                continue
            if t_schema and not s_schema:
                exact_r.append((t_schema, t_obj))
                continue
            if s_schema and t_schema:
                lk = (self._norm_identifier(s_schema), self._norm_identifier(s_obj))
                rk = (self._norm_identifier(t_schema), self._norm_identifier(t_obj))
                l_cnt = l_est_map.get(lk, 0)
                r_cnt = r_est_map.get(rk, 0)
                needs_exact = False
                if l_cnt is None or l_cnt == -1:
                    needs_exact = True
                if r_cnt is None or r_cnt < 0:
                    needs_exact = True
                if l_cnt != r_cnt:
                    needs_exact = True
                if needs_exact:
                    exact_l.append((s_schema, s_obj))
                    exact_r.append((t_schema, t_obj))

        def chunks(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i : i + size]

        l_exact_map = {}
        r_exact_map = {}
        l_error_details: dict[tuple[str, str], dict] = {}
        r_error_details: dict[tuple[str, str], dict] = {}
        batch_size = 40
        for ch in chunks(list({(s, t) for s, t in exact_l}), batch_size):
            for row in self._fetch_exact_counts_batch("db2", ch, error_details=l_error_details).collect():
                key = (self._norm_identifier(row["schema_name"]), self._norm_identifier(row["object_name"]))
                cnt = row["cnt"]
                if cnt is None:
                    l_exact_map[key] = None
                    if key not in l_error_details:
                        l_error_details[key] = {"reason": "unknown", "message": "Exact row count returned null"}
                else:
                    l_exact_map[key] = int(cnt)
        for ch in chunks(list({(s, t) for s, t in exact_r}), batch_size):
            for row in self._fetch_exact_counts_batch("azure", ch, error_details=r_error_details).collect():
                key = (self._norm_identifier(row["schema_name"]), self._norm_identifier(row["object_name"]))
                cnt = row["cnt"]
                if cnt is None:
                    r_exact_map[key] = None
                    if key not in r_error_details:
                        r_error_details[key] = {"reason": "unknown", "message": "Exact row count returned null"}
                else:
                    r_exact_map[key] = int(cnt)

        for r in pairs_rows:
            s_schema = clean(r["l_schema"])
            s_obj = clean(r["l_object"])
            t_schema = clean(r["r_schema"])
            t_obj = clean(r["r_object"])
            rd = r.asDict(recursive=True)
            l_type, r_type = rd.get("l_type"), rd.get("r_type")
            if l_type:
                obj_type = l_type
            elif r_type:
                obj_type = r_type
            else:
                obj_type = "TABLE"

            if s_schema and not t_schema:
                lk = (self._norm_identifier(s_schema), self._norm_identifier(s_obj))
                l_cnt = l_exact_map.get(lk, l_est_map.get(lk, None))
                rows.append(
                    (obj_type, s_schema, s_obj, "", "", l_cnt, None, False, f"{s_schema}.{s_obj}", "Destination missing")
                )
                continue
            if t_schema and not s_schema:
                rk = (self._norm_identifier(t_schema), self._norm_identifier(t_obj))
                r_cnt = r_exact_map.get(rk, r_est_map.get(rk, None))
                rows.append((obj_type, "", "", t_schema, t_obj, None, r_cnt, False, f"{t_schema}.{t_obj}", "Source missing"))
                continue

            lk = (self._norm_identifier(s_schema), self._norm_identifier(s_obj))
            rk = (self._norm_identifier(t_schema), self._norm_identifier(t_obj))

            l_error_info = l_error_details.get(lk)
            r_error_info = r_error_details.get(rk)

            l_cnt_est = l_est_map.get(lk, 0)
            r_cnt_est = r_est_map.get(rk, 0)
            l_cnt = l_exact_map.get(lk, l_cnt_est)
            r_cnt = r_exact_map.get(rk, r_cnt_est)

            row_count_match = True
            error_desc = ""

            if l_error_info or r_error_info:
                row_count_match = False
                if l_error_info and r_error_info:
                    error_desc = f"{self._format_exact_count_error('Source', l_error_info)}; {self._format_exact_count_error('Target', r_error_info)}"
                elif l_error_info:
                    error_desc = self._format_exact_count_error("Source", l_error_info)
                else:
                    error_desc = self._format_exact_count_error("Target", r_error_info)
            elif l_cnt is None or r_cnt is None:
                row_count_match = False
                error_desc = "Unable to determine row count"
            elif l_cnt != r_cnt:
                row_count_match = False
                error_desc = "Found mismatch in row-count validation"

            rows.append(
                (
                    obj_type,
                    s_schema,
                    s_obj,
                    t_schema,
                    t_obj,
                    l_cnt,
                    r_cnt,
                    row_count_match,
                    f"{s_schema}.{s_obj}",
                    error_desc,
                )
            )

        return self.spark.createDataFrame(rows, schema=self.row_count_schema)

    @log_duration("compare_column_nulls")
    def compare_column_nulls(
        self,
        source_schema: str | None,
        target_schema: str | None,
        object_types: list[str] | None,
        only_when_rowcount_matches: bool,
        output_only_issues: bool,
    ) -> DataFrame:
        timeout_warn = False
        try:
            timeout_warn = os.environ.get("DV_NULLCHECK_TIMEOUT_WARN", "1").strip().lower() in ("1", "true", "yes")
        except Exception:
            timeout_warn = True

        if not object_types:
            object_types = ["TABLE", "VIEW"]

        left = self._read_tables("db2", object_types, source_schema)
        right = self._read_tables("azure", object_types, target_schema)

        if not source_schema and not target_schema:
            l_schemas = left.select("schema_norm").distinct().withColumnRenamed("schema_norm", "s")
            r_schemas = right.select("schema_norm").distinct().withColumnRenamed("schema_norm", "t")
            matched = l_schemas.join(r_schemas, col("s") == col("t"), "inner").select(col("s").alias("schema_norm"))
            left = left.join(matched, "schema_norm", "inner")
            right = right.join(matched, "schema_norm", "inner")

        l = left.withColumn("key", concat_ws(".", col("schema_norm"), col("object_norm")))
        r = right.withColumn("key", concat_ws(".", col("schema_norm"), col("object_norm")))

        pairs = (
            l.select("schema_name", "object_name", "key", col("obj_type_norm").alias("l_type"))
            .alias("l")
            .join(
                r.select(
                    col("schema_name").alias("r_schema"),
                    col("object_name").alias("r_object"),
                    col("key").alias("r_key"),
                    col("obj_type_norm").alias("r_type"),
                ),
                on=col("key") == col("r_key"),
                how="inner",
            )
            .select(
                col("schema_name").alias("l_schema"),
                col("object_name").alias("l_object"),
                col("r_schema"),
                col("r_object"),
                col("l_type"),
                col("r_type"),
            )
        )

        try:
            total_pairs = pairs.count()
            print(f"[NullCheck] Matched objects to scan: {total_pairs}")
        except Exception:
            total_pairs = None

        _skip_rows_filter = False
        try:
            _skip_rows_filter = os.environ.get("DV_NULLCHECK_SKIP_ROWCOUNT_FILTER", "0").strip().lower() in ("1", "true", "yes")
        except Exception:
            _skip_rows_filter = False
        if only_when_rowcount_matches and not _skip_rows_filter:
            pairs_rows = pairs.collect()
            l_items = [(r["l_schema"], r["l_object"]) for r in pairs_rows]
            r_items = [(r["r_schema"], r["r_object"]) for r in pairs_rows]
            l_schemas = sorted(list({s for s, _ in l_items})) if l_items else None
            r_schemas = sorted(list({s for s, _ in r_items})) if r_items else None
            l_est_df = self._fetch_estimated_counts_all("db2", l_schemas, object_types)
            r_est_df = self._fetch_estimated_counts_all("azure", r_schemas, object_types)
            l_est_map = {
                (self._norm_identifier(row["schema_name"]), self._norm_identifier(row["object_name"])): int(row["cnt"] or 0)
                for row in l_est_df.collect()
            }
            r_est_map = {
                (self._norm_identifier(row["schema_name"]), self._norm_identifier(row["object_name"])): int(row["cnt"] or 0)
                for row in r_est_df.collect()
            }

            prelim_keep = []
            need_exact_pairs = []
            for r in pairs_rows:
                lk = (self._norm_identifier(r["l_schema"]), self._norm_identifier(r["l_object"]))
                rk = (self._norm_identifier(r["r_schema"]), self._norm_identifier(r["r_object"]))
                l_cnt = l_est_map.get(lk)
                r_cnt = r_est_map.get(rk)
                if (l_cnt is not None) and (r_cnt is not None) and (l_cnt == r_cnt):
                    prelim_keep.append(r)
                else:
                    need_exact_pairs.append((lk, rk))

            need_exact_l = list({lk for lk, _ in need_exact_pairs})
            need_exact_r = list({rk for _, rk in need_exact_pairs})
            l_exact_map = {}
            r_exact_map = {}

            def chunks(lst, size):
                for i in range(0, len(lst), size):
                    yield lst[i : i + size]

            for ch in chunks(need_exact_l, 40):
                for row in self._fetch_exact_counts_batch("db2", ch).collect():
                    l_key = (self._norm_identifier(row["schema_name"]), self._norm_identifier(row["object_name"]))
                    l_exact_map[l_key] = int(row["cnt"] or 0)
            for ch in chunks(need_exact_r, 40):
                for row in self._fetch_exact_counts_batch("azure", ch).collect():
                    r_key = (self._norm_identifier(row["schema_name"]), self._norm_identifier(row["object_name"]))
                    r_exact_map[r_key] = int(row["cnt"] or 0)

            exact_keep = []
            for r in pairs_rows:
                lk = (self._norm_identifier(r["l_schema"]), self._norm_identifier(r["l_object"]))
                rk = (self._norm_identifier(r["r_schema"]), self._norm_identifier(r["r_object"]))
                if (lk in l_exact_map) or (rk in r_exact_map):
                    l_cnt = l_exact_map.get(lk)
                    r_cnt = r_exact_map.get(rk)
                    if (l_cnt is not None) and (r_cnt is not None) and (l_cnt == r_cnt):
                        exact_keep.append(r)

            kept = prelim_keep + exact_keep
            pair_schema = pairs.schema
            if kept:
                pairs = self.spark.createDataFrame(kept, schema=pair_schema)
            else:
                pairs = self.spark.createDataFrame([], schema=pair_schema)
            dropped = max(0, len(pairs_rows) - len(kept))
            print(f"[NullCheck] After row-count filter: kept {len(prelim_keep)} by estimate, {len(exact_keep)} by exact; dropped {dropped}; total {len(kept)}")

        pairs_norm = pairs.select(
            upper(trim(col("l_schema"))).alias("s_schema_norm"),
            upper(trim(col("l_object"))).alias("s_object_norm"),
            upper(trim(col("r_schema"))).alias("r_schema_norm"),
            upper(trim(col("r_object"))).alias("r_object_norm"),
            col("l_type"),
            col("r_type"),
            col("l_schema").alias("s_schema"),
            col("l_object").alias("s_object"),
            col("r_schema").alias("r_schema"),
            col("r_object").alias("r_object"),
        ).withColumn("obj_type", when(col("l_type").isNotNull(), col("l_type")).when(col("r_type").isNotNull(), col("r_type")).otherwise(lit("TABLE")))

        try:
            _nc_verbose = os.environ.get("DV_NULLCHECK_VERBOSE", "1").strip().lower() in ("1", "true", "yes")
        except Exception:
            _nc_verbose = True
        if _nc_verbose:
            pairs_list = pairs.collect()
            for idx, rpair in enumerate(pairs_list, start=1):
                print(f"[NullCheck] [{idx}/{len(pairs_list)}] {rpair['l_schema']}.{rpair['l_object']} ↔ {rpair['r_schema']}.{rpair['r_object']}")

        s_schema_list = [r["s_schema"] for r in pairs_norm.select("s_schema").distinct().collect()]
        r_schema_list = [r["r_schema"] for r in pairs_norm.select("r_schema").distinct().collect()]
        l_cols_all = self._fetch_columns_bulk("db2", s_schema_list)
        r_cols_all = self._fetch_columns_bulk("azure", r_schema_list)
        l_cols_all = (
            l_cols_all.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )
        r_cols_all = (
            r_cols_all.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )

        s_tables = pairs_norm.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")).distinct()
        r_tables = pairs_norm.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")).distinct()
        l_cols_f = l_cols_all.join(s_tables, on=["schema_norm", "table_norm"], how="inner")
        r_cols_f = r_cols_all.join(r_tables, on=["schema_norm", "table_norm"], how="inner")

        common_cols = (
            l_cols_f.alias("l")
            .join(r_cols_f.alias("r"), on=["schema_norm", "table_norm", "col_norm"], how="inner")
            .select(
                "schema_norm",
                "table_norm",
                "col_norm",
                col("l.column_name").alias("l_col_name"),
                col("l.data_type").alias("l_data_type"),
                col("r.column_name").alias("r_col_name"),
                col("r.data_type").alias("r_data_type"),
            )
        )

        try:
            s_schema_list = [r["s_schema"] for r in pairs_norm.select("s_schema").distinct().collect()]
        except Exception:
            s_schema_list = None
        try:
            r_schema_list = [r["r_schema"] for r in pairs_norm.select("r_schema").distinct().collect()]
        except Exception:
            r_schema_list = None
        l_nulls_meta = self._fetch_columns_nullable_bulk("db2", s_schema_list)
        r_nulls_meta = self._fetch_columns_nullable_bulk("azure", r_schema_list)
        l_nulls_n = (
            l_nulls_meta.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .withColumn("l_is_nullable", upper(trim(col("is_nullable_str"))) == lit("Y"))
            .select("schema_norm", "table_norm", "col_norm", "l_is_nullable")
        )
        r_nulls_n = (
            r_nulls_meta.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .withColumn("r_is_nullable", upper(trim(col("is_nullable_str"))) == lit("YES"))
            .select("schema_norm", "table_norm", "col_norm", "r_is_nullable")
        )
        common_cols = common_cols.join(l_nulls_n, on=["schema_norm", "table_norm", "col_norm"], how="left").join(
            r_nulls_n, on=["schema_norm", "table_norm", "col_norm"], how="left"
        )

        skip_cols_env = os.environ.get("DV_NULLCHECK_SKIP_COLUMNS", "")
        skip_substrings = [s.strip().lower() for s in skip_cols_env.split(",") if s.strip()]
        if skip_substrings:
            common_cols = common_cols.where(
                reduce(
                    lambda acc, sub: acc & (~lower(col("col_norm")).contains(sub)),
                    skip_substrings[1:],
                    ~lower(col("col_norm")).contains(skip_substrings[0]),
                )
            )

        meta_mismatch = common_cols.where(coalesce(col("l_is_nullable"), lit(False)) != coalesce(col("r_is_nullable"), lit(False)))
        common_cols = common_cols.where(
            (coalesce(col("l_is_nullable"), lit(False)) | coalesce(col("r_is_nullable"), lit(False)))
            & (coalesce(col("l_is_nullable"), lit(False)) == coalesce(col("r_is_nullable"), lit(False)))
        )

        meta_out = meta_mismatch.join(
            pairs_norm.select(
                "s_schema_norm",
                "s_object_norm",
                "r_schema_norm",
                "r_object_norm",
                "obj_type",
                "s_schema",
                "s_object",
                "r_schema",
                "r_object",
            ),
            on=[meta_mismatch.schema_norm == col("s_schema_norm"), meta_mismatch.table_norm == col("s_object_norm")],
            how="inner",
        ).select(
            col("obj_type").alias("ObjectType"),
            col("s_schema").alias("SourceSchemaName"),
            col("s_object").alias("SourceObjectName"),
            col("r_schema").alias("DestinationSchemaName"),
            col("r_object").alias("DestinationObjectName"),
            col("l_col_name").alias("ColumnName"),
            lit(None).cast("long").alias("SourceNullCount"),
            lit(None).cast("long").alias("DestinationNullCount"),
            lit(None).cast("long").alias("SourceEmptyCount"),
            lit(None).cast("long").alias("DestinationEmptyCount"),
            lit(False).alias("NullCountMatch"),
            lit(True).alias("EmptyCountMatch"),
            concat(trim(col("s_schema")), lit("."), trim(col("s_object")), lit("."), trim(col("l_col_name"))).alias("ElementPath"),
            lit("Nullability mismatch (metadata only)").alias("ErrorDescription"),
        )

        try:
            _skip_data_when_meta_match = (
                os.environ.get("DV_NULLCHECK_SKIP_DATA_WHEN_METADATA_MATCH", "1").strip().lower() in ("1", "true", "yes")
            )
        except Exception:
            _skip_data_when_meta_match = True

        if _skip_data_when_meta_match:
            if meta_out.rdd.isEmpty() and common_cols.rdd.isEmpty():
                return self.spark.createDataFrame(
                    [],
                    schema="ObjectType string, SourceSchemaName string, SourceObjectName string, DestinationSchemaName string, DestinationObjectName string, ColumnName string, SourceNullCount long, DestinationNullCount long, SourceEmptyCount long, DestinationEmptyCount long, NullCountMatch boolean, EmptyCountMatch boolean, ElementPath string, ErrorDescription string",
                )
            if meta_out.rdd.isEmpty() and not common_cols.rdd.isEmpty():
                return self.spark.createDataFrame(
                    [],
                    schema="ObjectType string, SourceSchemaName string, SourceObjectName string, DestinationSchemaName string, DestinationObjectName string, ColumnName string, SourceNullCount long, DestinationNullCount long, SourceEmptyCount long, DestinationEmptyCount long, NullCountMatch boolean, EmptyCountMatch boolean, ElementPath string, ErrorDescription string",
                )
            if not meta_out.rdd.isEmpty() and common_cols.rdd.isEmpty():
                return meta_out

        common_cols = common_cols.join(
            pairs_norm.select(
                "s_schema_norm",
                "s_object_norm",
                "r_schema_norm",
                "r_object_norm",
                "s_schema",
                "s_object",
                "r_schema",
                "r_object",
                "obj_type",
            ),
            on=[common_cols.schema_norm == col("s_schema_norm"), common_cols.table_norm == col("s_object_norm")],
            how="inner",
        ).select(
            common_cols["schema_norm"],
            common_cols["table_norm"],
            common_cols["col_norm"],
            "l_col_name",
            "l_data_type",
            "r_col_name",
            "r_data_type",
            "s_schema",
            "s_object",
            "r_schema",
            "r_object",
            "obj_type",
        )

        def is_string_type(dt: str) -> bool:
            if not dt:
                return False
            d = str(dt).strip().upper()
            return ("CHAR" in d) or ("VARCHAR" in d) or ("NCHAR" in d) or ("NVARCHAR" in d)

        common_rows = common_cols.collect()
        print(f"[NullCheck] Columns to aggregate: {len(common_rows)}")
        cols_by_db2_tbl: dict[tuple, List[tuple]] = defaultdict(list)
        cols_by_az_tbl: dict[tuple, List[tuple]] = defaultdict(list)
        for row in common_rows:
            s_schema = (row["s_schema"] or "").upper()
            s_table = (row["s_object"] or "").upper()
            l_col_name = row["l_col_name"]
            t_schema = row["r_schema"]
            t_table = row["r_object"]
            r_col_name = row["r_col_name"]
            cols_by_db2_tbl[(s_schema, s_table)].append((l_col_name, is_string_type(row["l_data_type"])))
            cols_by_az_tbl[(t_schema, t_table)].append((r_col_name, is_string_type(row["r_data_type"])))

        def _cols_per_query(side: str) -> int:
            try:
                if self._side_engine(side) == "db2":
                    return int(os.environ.get("DV_COL_AGG_COLS_PER_QUERY_DB2", "40"))
                return int(os.environ.get("DV_COL_AGG_COLS_PER_QUERY_AZ", "80"))
            except Exception:
                return 40 if side == "db2" else 80

        def _build_per_table_selects_db2(schema: str, table: str, cols: List[tuple]) -> List[str]:
            selects: List[str] = []
            chunk_size = _cols_per_query("db2")
            engine_is_azure = self._side_engine("db2") == "azure"

            def q_db2(x: str) -> str:
                x = (x or "").upper().replace('"', '""')
                return f'"{x}"'

            tbl_ref = f"{q_db2(schema)}.{q_db2(table)}"
            for i in range(0, len(cols), chunk_size):
                chunk = cols[i : i + chunk_size]
                exprs = ["COUNT(*) AS total_rows"]
                for cname, _ in chunk:
                    colref = f"[{cname}]" if engine_is_azure else q_db2(cname)
                    nn_expr = f"SUM(CASE WHEN {colref} IS NOT NULL THEN 1 ELSE 0 END)" if engine_is_azure else f"COUNT({colref})"
                    exprs.append(f"{nn_expr} AS nn_{cname}")
                    exprs.append("CAST(0 AS BIGINT) AS emp_{0}".format(cname))
                table_ref = f"[{schema}].[{table}]" if engine_is_azure else tbl_ref
                base = f"(SELECT {', '.join(exprs)} FROM {table_ref}) base"
                rows = []
                for cname, _ in chunk:
                    rows.append(
                        "SELECT "
                        f"'{schema}' AS schema_name, '{table}' AS table_name, '{cname}' AS column_name, "
                        f"total_rows AS total_rows, nn_{cname} AS non_nulls, emp_{cname} AS empties "
                        f"FROM {base}"
                    )
                selects.append(" UNION ALL ".join(rows))
            return selects

        def _build_per_table_selects_az(schema: str, table: str, cols: List[tuple]) -> List[str]:
            selects: List[str] = []
            chunk_size = _cols_per_query("azure")
            for i in range(0, len(cols), chunk_size):
                chunk = cols[i : i + chunk_size]
                exprs = ["COUNT(*) AS total_rows"]
                for cname, _ in chunk:
                    colref = f"[{cname}]"
                    nn_expr = f"SUM(CASE WHEN {colref} IS NOT NULL THEN 1 ELSE 0 END)"
                    exprs.append(f"{nn_expr} AS nn_{cname}")
                    exprs.append("CAST(0 AS BIGINT) AS emp_{0}".format(cname))
                base = f"(SELECT {', '.join(exprs)} FROM [{schema}].[{table}]) base"
                rows = []
                for cname, _ in chunk:
                    rows.append(
                        "SELECT "
                        f"'{schema}' AS schema_name, '{table}' AS table_name, '{cname}' AS column_name, "
                        f"total_rows AS total_rows, nn_{cname} AS non_nulls, emp_{cname} AS empties "
                        f"FROM {base}"
                    )
                selects.append(" UNION ALL ".join(rows))
            return selects

        per_table_mode = True
        try:
            per_table_mode = os.environ.get("DV_COL_AGG_MODE", "per_table").strip().lower() == "per_table"
        except Exception:
            per_table_mode = True

        def _is_timeout_error(msg: str) -> bool:
            m = (msg or "").lower()
            return any(x in m for x in ["sqlcode=-952", "57014", "timeout", "timed out"])

        def _build_timeout_warnings(msg: str) -> DataFrame:
            rows = []
            summary = self._summarize_driver_error(msg) if hasattr(self, "_summarize_driver_error") else msg
            for r in pairs_norm.collect():
                rows.append(
                    {
                        "ObjectType": r["obj_type"],
                        "SourceSchemaName": r["s_schema"],
                        "SourceObjectName": r["s_object"],
                        "DestinationSchemaName": r["r_schema"],
                        "DestinationObjectName": r["r_object"],
                        "ColumnName": None,
                        "SourceNullCount": None,
                        "DestinationNullCount": None,
                        "SourceEmptyCount": None,
                        "DestinationEmptyCount": None,
                        "NullCountMatch": False,
                        "EmptyCountMatch": False,
                        "ElementPath": f"{(r['s_schema'] or '').strip()}.{(r['s_object'] or '').strip()}",
                        "ErrorDescription": f"Null check skipped (timeout): {summary}",
                    }
                )
            if not rows:
                return self.spark.createDataFrame(
                    [],
                    schema="ObjectType string, SourceSchemaName string, SourceObjectName string, DestinationSchemaName string, DestinationObjectName string, ColumnName string, SourceNullCount long, DestinationNullCount long, SourceEmptyCount long, DestinationEmptyCount long, NullCountMatch boolean, EmptyCountMatch boolean, ElementPath string, ErrorDescription string",
                )
            return self.spark.createDataFrame(rows)

        try:
            if per_table_mode:
                db2_selects: List[str] = []
                az_selects: List[str] = []
                for (s, t), cols in cols_by_db2_tbl.items():
                    if cols:
                        db2_selects.extend(_build_per_table_selects_db2(s, t, cols))
                for (s, t), cols in cols_by_az_tbl.items():
                    if cols:
                        az_selects.extend(_build_per_table_selects_az(s, t, cols))
                l_agg = self._execute_agg_selects("db2", db2_selects)
                r_agg = self._execute_agg_selects("azure", az_selects)
            else:
                by_db2_tbl: dict[tuple, List[str]] = defaultdict(list)
                by_az_tbl: dict[tuple, List[str]] = defaultdict(list)
                left_engine = self._side_engine("db2")
                left_is_azure = left_engine == "azure"
                for row in common_rows:
                    s_schema = (row["s_schema"] or "").upper()
                    s_table = (row["s_object"] or "").upper()
                    l_col = row["col_norm"]
                    t_schema = row["r_schema"]
                    t_table = row["r_object"]
                    r_col = row["r_col_name"]
                    if left_is_azure:
                        l_colref = f"[{row['l_col_name']}]"
                        l_table_ref = f"[{s_schema}].[{s_table}]"
                    else:
                        def q_db2(x: str) -> str:
                            x = (x or "").upper().replace('"', '""')
                            return f'"{x}"'
                        l_colref = q_db2(row["l_col_name"])
                        l_table_ref = f"{q_db2(s_schema)}.{q_db2(s_table)}"
                    empties_l = "CAST(0 AS BIGINT)"
                    nn_expr_l = f"SUM(CASE WHEN {l_colref} IS NOT NULL THEN 1 ELSE 0 END)" if left_is_azure else f"COUNT({l_col})"
                    by_db2_tbl[(s_schema, s_table)].append(
                        f"SELECT '{s_schema}' AS schema_name, '{s_table}' AS table_name, '{row['l_col_name']}' AS column_name, "
                        f"COUNT(*) AS total_rows, {nn_expr_l} AS non_nulls, {empties_l} AS empties FROM {l_table_ref}"
                    )
                    colref_r = f"[{r_col}]"
                    empties_r = "CAST(0 AS BIGINT)"
                    nn_expr_r = f"SUM(CASE WHEN {colref_r} IS NOT NULL THEN 1 ELSE 0 END)"
                    by_az_tbl[(t_schema, t_table)].append(
                        f"SELECT '{t_schema}' AS schema_name, '{t_table}' AS table_name, '{r_col}' AS column_name, "
                        f"COUNT(*) AS total_rows, {nn_expr_r} AS non_nulls, {empties_r} AS empties FROM [{t_schema}].[{t_table}]"
                    )

                def union_tables(side: str, table_to_selects: dict) -> DataFrame:
                    all_selects: List[str] = []
                    for selects in table_to_selects.values():
                        all_selects.extend(selects)
                    return self._execute_agg_selects(side, all_selects)

                l_agg = union_tables("db2", by_db2_tbl)
                r_agg = union_tables("azure", by_az_tbl)

            l_agg_n = (
                l_agg.withColumn("schema_norm", upper(trim(col("schema_name"))))
                .withColumn("table_norm", upper(trim(col("table_name"))))
                .withColumn("col_norm", upper(trim(col("column_name"))))
                .withColumn("l_nulls", col("total_rows") - col("non_nulls"))
                .withColumn("l_empties", lit(0))
            )
            r_agg_n = (
                r_agg.withColumn("schema_norm", upper(trim(col("schema_name"))))
                .withColumn("table_norm", upper(trim(col("table_name"))))
                .withColumn("col_norm", upper(trim(col("column_name"))))
                .withColumn("r_nulls", col("total_rows") - col("non_nulls"))
                .withColumn("r_empties", lit(0))
            )

            joined = (
                l_agg_n.alias("l")
                .join(r_agg_n.alias("r"), on=["schema_norm", "table_norm", "col_norm"], how="inner")
                .select(
                    "schema_norm",
                    "table_norm",
                    "col_norm",
                    col("l.schema_name").alias("s_schema_name"),
                    col("l.table_name").alias("s_table_name"),
                    col("l.column_name").alias("s_column_name"),
                    col("r.schema_name").alias("r_schema_name"),
                    col("r.table_name").alias("r_table_name"),
                    col("r.column_name").alias("r_column_name"),
                    "l_nulls",
                    "r_nulls",
                    "l_empties",
                    "r_empties",
                )
            )

            joined = joined.join(
                pairs_norm.select(
                    "s_schema_norm",
                    "s_object_norm",
                    "r_schema_norm",
                    "r_object_norm",
                    "obj_type",
                    "s_schema",
                    "s_object",
                    "r_schema",
                    "r_object",
                ),
                on=[joined.schema_norm == col("s_schema_norm"), joined.table_norm == col("s_object_norm")],
                how="inner",
            )

            res = joined.withColumn("NullCountMatch", col("l_nulls") == col("r_nulls")).withColumn("EmptyCountMatch", lit(True))
            if output_only_issues:
                res = res.where(~(col("NullCountMatch") & col("EmptyCountMatch")))

            out_data = res.select(
                col("obj_type").alias("ObjectType"),
                col("s_schema").alias("SourceSchemaName"),
                col("s_object").alias("SourceObjectName"),
                col("r_schema").alias("DestinationSchemaName"),
                col("r_object").alias("DestinationObjectName"),
                col("s_column_name").alias("ColumnName"),
                col("l_nulls").cast("long").alias("SourceNullCount"),
                col("r_nulls").cast("long").alias("DestinationNullCount"),
                lit(0).cast("long").alias("SourceEmptyCount"),
                lit(0).cast("long").alias("DestinationEmptyCount"),
                col("NullCountMatch"),
                lit(True).alias("EmptyCountMatch"),
                concat(trim(col("s_schema")), lit("."), trim(col("s_object")), lit("."), trim(col("s_column_name"))).alias("ElementPath"),
                when(~(col("NullCountMatch")), lit("Column-level null mismatch")).otherwise(lit("")).alias("ErrorDescription"),
            )

            out = out_data if meta_out.rdd.isEmpty() else out_data.unionByName(meta_out, allowMissingColumns=True)
            return out

        except Exception as ex:
            msg = str(ex)
            if timeout_warn and _is_timeout_error(msg):
                warn_df = _build_timeout_warnings(msg)
                return warn_df
            raise

    @log_duration("check_reference_integrity")
    def check_reference_integrity(
        self,
        source_schema: str | None,
        target_schema: str | None,
        sample_limit: int = 10,
        max_workers: int | None = None,
    ) -> DataFrame:
        l = self._read_tables("db2", ["TABLE"], source_schema)
        r = self._read_tables("azure", ["TABLE"], target_schema)
        if not source_schema and not target_schema:
            l_s = l.select("schema_norm").distinct().withColumnRenamed("schema_norm", "s")
            r_s = r.select("schema_norm").distinct().withColumnRenamed("schema_norm", "t")
            matched_s = l_s.join(r_s, col("s") == col("t"), "inner").select(col("s").alias("schema_norm"))
            l = l.join(matched_s, "schema_norm", "inner")
            r = r.join(matched_s, "schema_norm", "inner")
        pairs = (
            l.select("schema_name", "object_name", "schema_norm", "object_norm")
            .alias("l")
            .join(
                r.select(
                    col("schema_name").alias("r_schema"),
                    col("object_name").alias("r_object"),
                    col("schema_norm").alias("r_schema_norm"),
                    col("object_norm").alias("r_object_norm"),
                ).alias("r"),
                on=(col("l.schema_norm") == col("r.r_schema_norm")) & (col("l.object_norm") == col("r.r_object_norm")),
                how="inner",
            )
            .select(
                col("l.schema_name").alias("SourceSchemaName"),
                col("l.object_name").alias("SourceObjectName"),
                col("r_schema").alias("DestinationSchemaName"),
                col("r_object").alias("DestinationObjectName"),
                upper(trim(col("l.schema_name"))).alias("s_schema_norm"),
                upper(trim(col("l.object_name"))).alias("s_object_norm"),
                upper(trim(col("r_schema"))).alias("r_schema_norm"),
                upper(trim(col("r_object"))).alias("r_object_norm"),
            )
        )

        s_schemas = [r["SourceSchemaName"] for r in pairs.select("SourceSchemaName").distinct().collect()]
        r_schemas = [r["DestinationSchemaName"] for r in pairs.select("DestinationSchemaName").distinct().collect()]
        db2_fk = self._build_fk_metadata("db2", s_schemas)
        az_fk = self._build_fk_metadata("azure", r_schemas)

        db2_f = db2_fk.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        az_f = az_fk.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )

        def collect_fk_groups(df: DataFrame, side: str) -> list[dict]:
            rows = df.select(
                col("schema_name"),
                col("table_name"),
                col("fk_name"),
                col("ref_schema_name"),
                col("ref_table_name"),
                col("col_name"),
                col("ref_col_name"),
                col("colseq"),
            ).collect()
            groups: dict[tuple[str, str, str], dict] = {}
            for rrow in rows:
                key = (rrow["schema_name"], rrow["table_name"], rrow["fk_name"])
                if key not in groups:
                    groups[key] = {
                        "side": side,
                        "child_schema": rrow["schema_name"],
                        "child_table": rrow["table_name"],
                        "fk_name": rrow["fk_name"],
                        "parent_schema": rrow["ref_schema_name"],
                        "parent_table": rrow["ref_table_name"],
                        "pairs": [],
                    }
                groups[key]["pairs"].append((int(rrow["colseq"] or 0), rrow["col_name"], rrow["ref_col_name"]))
            out = []
            for g in groups.values():
                g["pairs"] = [p[1:] for p in sorted(g["pairs"], key=lambda x: x[0])]
                out.append(g)
            return out

        fk_groups: list[dict] = []
        fk_groups.extend(collect_fk_groups(db2_f, "db2"))
        fk_groups.extend(collect_fk_groups(az_f, "azure"))

        pairs_l = {
            (row["s_schema_norm"], row["s_object_norm"]): (
                row["SourceSchemaName"],
                row["SourceObjectName"],
                row["DestinationSchemaName"],
                row["DestinationObjectName"],
            )
            for row in pairs.collect()
        }
        pairs_r = {
            (row["r_schema_norm"], row["r_object_norm"]): (
                row["SourceSchemaName"],
                row["SourceObjectName"],
                row["DestinationSchemaName"],
                row["DestinationObjectName"],
            )
            for row in pairs.collect()
        }

        def process_fk(g: dict) -> list[dict]:
            side = g["side"]
            child_schema = g["child_schema"]
            child_table = g["child_table"]
            parent_schema = g["parent_schema"]
            parent_table = g["parent_table"]
            col_pairs = g["pairs"]
            if not col_pairs:
                return []
            try:
                if os.environ.get("DV_REFINT_DISABLE_PUSHDOWN", "0").lower() in ("1", "true", "yes"):
                    raise RuntimeError("reference_integrity pushdown disabled by env")
                creds = self._build_jdbc_urls()["azure" if side == "azure" else "db2"]
                child_cols = [c for (c, _) in col_pairs]
                parent_cols = [p for (_, p) in col_pairs]

                if self._side_engine(side) == "azure":
                    tbl_child = f"[{child_schema}].[{child_table}]"
                    tbl_parent = f"[{parent_schema}].[{parent_table}]"

                    def qcol(alias: str, c: str) -> str:
                        return f"{alias}.[{c}]"

                    def norm(alias: str, c: str) -> str:
                        return f"LOWER(LTRIM(RTRIM({qcol(alias, c)})))"

                    def concat_keys(alias: str, cols: list[str]) -> str:
                        parts: list[str] = []
                        for idx, nm in enumerate(cols):
                            if idx > 0:
                                parts.append("'|'")
                            parts.append(norm(alias, nm))
                        return f"CONCAT({', '.join(parts)})"

                    top_clause = f"TOP {int(sample_limit)}"
                    fetch_clause = ""
                else:
                    tbl_child = f"{child_schema.upper()}.{child_table.upper()}"
                    tbl_parent = f"{parent_schema.upper()}.{parent_table.upper()}"

                    def qcol(alias: str, c: str) -> str:
                        return f"{alias}.{(c or '').upper()}"

                    def norm(alias: str, c: str) -> str:
                        return f"LOWER(TRIM({qcol(alias, c)}))"

                    def concat_keys(alias: str, cols: list[str]) -> str:
                        exprs = [f"COALESCE({norm(alias, nm)}, '<NULL>')" for nm in cols]
                        return " || '|' || ".join(exprs)

                    top_clause = ""
                    fetch_clause = f" FETCH FIRST {int(sample_limit)} ROWS ONLY"

                non_null = " AND ".join([f"{qcol('c', cc)} IS NOT NULL" for cc in child_cols]) or "1=1"
                join_cond = " AND ".join([f"{norm('c', cc)} = {norm('p', pp)}" for cc, pp in zip(child_cols, parent_cols)]) or "1=1"

                parent_null_probe = parent_cols[0] if parent_cols else None
                if self._side_engine(side) == "azure":
                    count_sql = (
                        f"SELECT COUNT(*) AS broken_count FROM {tbl_child} c "
                        f"LEFT JOIN {tbl_parent} p ON {join_cond} "
                        f"WHERE ({non_null}) AND {('p.' + parent_null_probe + ' IS NULL') if parent_null_probe else '1=1'}"
                    )
                else:
                    count_sql = (
                        f"SELECT COUNT(*) AS broken_count FROM {tbl_child} c "
                        f"LEFT JOIN {tbl_parent} p ON {join_cond} "
                        f"WHERE ({non_null}) AND {((parent_null_probe or '').upper() + ' IS NULL') if parent_null_probe else '1=1'}"
                    )
                debug_sql = os.environ.get("DV_DEBUG_SQL", "").strip().lower() in ("1", "true", "yes", "on")
                if debug_sql:
                    print(
                        f"[RefInt][{side}] COUNT START fk={g['fk_name']} child={child_schema}.{child_table} parent={parent_schema}.{parent_table} sql={count_sql}",
                        flush=True,
                    )
                t0 = time.time()
                count_reader = (
                    self.spark.read.format("jdbc")
                    .option("url", creds["url"])
                    .option("dbtable", f"({count_sql}) x")
                    .option("driver", creds["driver"])
                    .option("fetchsize", os.environ.get("JDBC_FETCHSIZE", "1000"))
                )
                count_reader = self._apply_jdbc_auth(count_reader, creds)
                qt = os.environ.get("JDBC_QUERY_TIMEOUT")
                if qt:
                    count_reader = count_reader.option("queryTimeout", str(qt))
                cnt_row = count_reader.load().first()
                broken_count = int((cnt_row[0] if cnt_row is not None else 0) or 0)
                if debug_sql:
                    print(
                        f"[RefInt][{side}] COUNT END fk={g['fk_name']} child={child_schema}.{child_table} parent={parent_schema}.{parent_table} broken_count={broken_count} took={time.time()-t0:.2f}s",
                        flush=True,
                    )
                if broken_count <= 0:
                    return []

                key_expr = concat_keys("c", child_cols)
                if self._side_engine(side) == "azure":
                    sample_sql = (
                        f"SELECT {top_clause} {key_expr} AS KeySig FROM {tbl_child} c "
                        f"LEFT JOIN {tbl_parent} p ON {join_cond} "
                        f"WHERE ({non_null}) AND {('p.' + parent_null_probe + ' IS NULL') if parent_null_probe else '1=1'}"
                        f"{fetch_clause}"
                    ).strip()
                else:
                    sample_sql = (
                        f"SELECT {top_clause} {key_expr} AS KeySig FROM {tbl_child} c "
                        f"LEFT JOIN {tbl_parent} p ON {join_cond} "
                        f"WHERE ({non_null}) AND {((parent_null_probe or '').upper() + ' IS NULL') if parent_null_probe else '1=1'}"
                        f"{fetch_clause}"
                    ).strip()
                if debug_sql:
                    print(
                        f"[RefInt][{side}] SAMPLE START fk={g['fk_name']} child={child_schema}.{child_table} parent={parent_schema}.{parent_table} sql={sample_sql}",
                        flush=True,
                    )
                t1 = time.time()
                sample_reader = (
                    self.spark.read.format("jdbc")
                    .option("url", creds["url"])
                    .option("dbtable", f"({sample_sql}) x")
                    .option("driver", creds["driver"])
                    .option("fetchsize", os.environ.get("JDBC_FETCHSIZE", "1000"))
                )
                sample_reader = self._apply_jdbc_auth(sample_reader, creds)
                if qt:
                    sample_reader = sample_reader.option("queryTimeout", str(qt))
                samples = [r["KeySig"] for r in sample_reader.load().collect()]
                if debug_sql:
                    print(
                        f"[RefInt][{side}] SAMPLE END fk={g['fk_name']} child={child_schema}.{child_table} parent={parent_schema}.{parent_table} samples={len(samples)} took={time.time()-t1:.2f}s",
                        flush=True,
                    )

                if self._side_engine(side) == "azure":
                    names = pairs_r.get((child_schema.upper(), child_table.upper()))
                else:
                    names = pairs_l.get((child_schema.upper(), child_table.upper()))
                if names:
                    s_schema, s_table, d_schema, d_table = names
                else:
                    s_schema, s_table, d_schema, d_table = (child_schema, child_table, parent_schema, parent_table)

                err_code = "REF_INTEGRITY_IN_TARGET" if side == "azure" else "REF_INTEGRITY_IN_SOURCE"
                return [
                    {
                        "ValidationType": "reference_integrity",
                        "ObjectType": "TABLE",
                        "SourceSchemaName": s_schema,
                        "SourceObjectName": s_table,
                        "DestinationSchemaName": d_schema,
                        "DestinationObjectName": d_table,
                        "ElementPath": f"{(child_schema or '').strip()}.{(child_table or '').strip()}.{(g['fk_name'] or '').strip()}",
                        "ErrorCode": err_code,
                        "ErrorDescription": f"Child rows without matching parent: {broken_count}",
                        "DetailsJson": json.dumps(
                            {
                                "fk_name": g["fk_name"],
                                "child_schema": child_schema,
                                "child_table": child_table,
                                "parent_schema": parent_schema,
                                "parent_table": parent_table,
                                "fk_columns": child_cols,
                                "ref_columns": parent_cols,
                                "broken_row_count": broken_count,
                                "sample_child_keys": samples,
                            }
                        ),
                    }
                ]
            except Exception:
                return []

        from concurrent.futures import ThreadPoolExecutor, as_completed

        if max_workers is None:
            try:
                max_workers = int(os.environ.get("DV_BROKEN_FK_WORKERS", "8"))
            except Exception:
                max_workers = 8
        issues: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(process_fk, g) for g in fk_groups]
            for f in as_completed(futures):
                res = f.result()
                if res:
                    issues.extend(res)

        ref_schema = StructType(
            [
                StructField("ValidationType", StringType(), True),
                StructField("ObjectType", StringType(), True),
                StructField("SourceSchemaName", StringType(), True),
                StructField("SourceObjectName", StringType(), True),
                StructField("DestinationSchemaName", StringType(), True),
                StructField("DestinationObjectName", StringType(), True),
                StructField("ElementPath", StringType(), True),
                StructField("ChangeType", StringType(), True),
                StructField("ErrorCode", StringType(), True),
                StructField("ErrorDescription", StringType(), True),
                StructField("DetailsJson", StringType(), True),
            ]
        )
        if issues:
            result_df = self.spark.createDataFrame(issues, schema=ref_schema)
        else:
            result_df = self.spark.createDataFrame(
                [], schema=ref_schema
            )

        cols = result_df.columns
        first_cols = [
            "ValidationType",
            "ObjectType",
            "SourceObjectName",
            "SourceSchemaName",
            "DestinationObjectName",
            "DestinationSchemaName",
        ]
        last_cols = ["DetailsJson"] if "DetailsJson" in cols else []
        middle_cols = [c for c in cols if c not in first_cols and c not in last_cols]
        ordered = [c for c in first_cols if c in cols] + middle_cols + last_cols
        return result_df.select(*ordered)

    @log_duration("check_constraint_integrity")
    def check_constraint_integrity(
        self,
        source_schema: str | None,
        target_schema: str | None,
    ) -> DataFrame:
        if os.environ.get("DV_CI_DISABLE_PUSHDOWN", "0").strip().lower() in ("1", "true", "yes"):
            return self.spark.createDataFrame(
                [],
                schema="ValidationType string, ObjectType string, SourceSchemaName string, SourceObjectName string, DestinationSchemaName string, DestinationObjectName string, ElementPath string, ErrorCode string, ErrorDescription string, DetailsJson string",
            )
        l = self._read_tables("db2", ["TABLE"], source_schema)
        r = self._read_tables("azure", ["TABLE"], target_schema)
        if not source_schema and not target_schema:
            l_s = l.select("schema_norm").distinct().withColumnRenamed("schema_norm", "s")
            r_s = r.select("schema_norm").distinct().withColumnRenamed("schema_norm", "t")
            matched_s = l_s.join(r_s, col("s") == col("t"), "inner").select(col("s").alias("schema_norm"))
            l = l.join(matched_s, "schema_norm", "inner")
            r = r.join(matched_s, "schema_norm", "inner")
        pairs = (
            l.select("schema_name", "object_name", "schema_norm", "object_norm")
            .alias("l")
            .join(
                r.select(
                    col("schema_name").alias("r_schema"),
                    col("object_name").alias("r_object"),
                    col("schema_norm").alias("r_schema_norm"),
                    col("object_norm").alias("r_object_norm"),
                ).alias("r"),
                on=(col("l.schema_norm") == col("r.r_schema_norm")) & (col("l.object_norm") == col("r.r_object_norm")),
                how="inner",
            )
            .select(
                col("l.schema_name").alias("SourceSchemaName"),
                col("l.object_name").alias("SourceObjectName"),
                col("r_schema").alias("DestinationSchemaName"),
                col("r_object").alias("DestinationObjectName"),
                upper(trim(col("l.schema_name"))).alias("s_schema_norm"),
                upper(trim(col("l.object_name"))).alias("s_object_norm"),
                upper(trim(col("r_schema"))).alias("r_schema_norm"),
                upper(trim(col("r_object"))).alias("r_object_norm"),
            )
        )

        s_schemas = [r["SourceSchemaName"] for r in pairs.select("SourceSchemaName").distinct().collect()]
        r_schemas = [r["DestinationSchemaName"] for r in pairs.select("DestinationSchemaName").distinct().collect()]

        l_nulls = self._fetch_columns_nullable_bulk("db2", s_schemas)
        r_nulls = self._fetch_columns_nullable_bulk("azure", r_schemas)
        l_nulls_n = (
            l_nulls.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .withColumn("IsNullable", upper(trim(col("is_nullable_str"))) == lit("Y"))
        )
        r_nulls_n = (
            r_nulls.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .withColumn("IsNullable", upper(trim(col("is_nullable_str"))) == lit("YES"))
        )

        l_types = self._fetch_columns_types_bulk("db2", s_schemas)
        r_types = self._fetch_columns_types_bulk("azure", r_schemas)
        l_types_n = (
            l_types.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )
        r_types_n = (
            r_types.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )

        db2_chk = self._fetch_db2_check_constraints("db2", s_schemas)
        az_chk = self._fetch_sql_check_constraints("azure", r_schemas)
        db2_chk_n = (
            db2_chk.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("chk_norm", upper(trim(col("chk_name"))))
        )
        az_chk_n = (
            az_chk.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("chk_norm", upper(trim(col("chk_name"))))
        )

        l_nn_cols = l_nulls_n.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        r_nn_cols = r_nulls_n.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        l_types_f = l_types_n.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        r_types_f = r_types_n.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        db2_chk_f = db2_chk_n.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        az_chk_f = az_chk_n.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )

        issues: list[dict] = []

        def run_sql_count(side: str, sql: str) -> int:
            creds = self._build_jdbc_urls()["azure" if side == "azure" else "db2"]
            if os.environ.get("DV_DEBUG_SQL") == "1":
                print(f"[ConstraintSQL][{side}] {sql}")
            reader = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", f"({sql}) x")
                .option("driver", creds["driver"])
                .option("fetchsize", os.environ.get("JDBC_FETCHSIZE", "1000"))
            )
            reader = self._apply_jdbc_auth(reader, creds)
            qt = os.environ.get("JDBC_QUERY_TIMEOUT")
            if qt:
                reader = reader.option("queryTimeout", str(qt))
            row = reader.load().first()
            return int((row[0] if row is not None else 0) or 0)

        try:
            cfg = self.load_database_config() or {}
            overrides = cfg.get("date_format_overrides") or {}
        except Exception:
            overrides = {}

        def get_override(schema: str, table: str, column: str) -> str | None:
            key = f"{(schema or '').upper()}.{(table or '').upper()}.{(column or '').upper()}"
            return overrides.get(key)

        def db2_regex_for(fmt: str) -> str | None:
            f = (fmt or "").upper().strip()
            repl = (
                f.replace("YYYY", "[0-9]{4}")
                .replace("MM", "(0[1-9]|1[0-2])")
                .replace("DD", "([0-2][0-9]|3[0-1])")
                .replace("HH24", "([0-1][0-9]|2[0-3])")
                .replace("MI", "([0-5][0-9])")
                .replace("SS", "([0-5][0-9])")
            )
            return f"^{repl}$"

        def mssql_style_for(fmt: str) -> int | None:
            f = (fmt or "").upper().strip()
            if f == "YYYY-MM-DD":
                return 23
            if f == "YYYY-MM-DD HH24:MI:SS":
                return 120
            if f == "MM/DD/YYYY":
                return 101
            return None

        def _is_numeric(dt: str | None) -> bool:
            if not dt:
                return False
            d = str(dt).strip().upper()
            return any(x in d for x in ["DECIMAL", "NUMERIC"])

        def process_pair(pr) -> list[dict]:
            s_schema = pr["SourceSchemaName"]
            s_table = pr["SourceObjectName"]
            r_schema = pr["DestinationSchemaName"]
            r_table = pr["DestinationObjectName"]

            lt_rows = (
                l_types_f.where((col("schema_norm") == upper(lit(s_schema))) & (col("table_norm") == upper(lit(s_table))))
                .select("column_name", "data_type", "precision", "scale", upper(trim(col("column_name"))).alias("col_norm"), "char_len")
                .collect()
            )
            rt_rows = (
                r_types_f.where((col("schema_norm") == upper(lit(r_schema))) & (col("table_norm") == upper(lit(r_table))))
                .select("column_name", "data_type", "precision", "scale", upper(trim(col("column_name"))).alias("col_norm"), "char_len")
                .collect()
            )
            lmap = {row["col_norm"]: row for row in lt_rows}
            rmap = {row["col_norm"]: row for row in rt_rows}
            common_cols = sorted(set(lmap.keys()).intersection(rmap.keys()))
            local_issues: list[dict] = []

            l_notnull = (
                l_nn_cols.where((col("schema_norm") == upper(lit(s_schema))) & (col("table_norm") == upper(lit(s_table))) & (~col("IsNullable")))
                .select("column_name")
                .distinct()
                .collect()
            )
            for rowc in l_notnull:
                coln = rowc["column_name"]
                s_quoted = f'"{(s_schema or "").upper()}"'
                t_quoted = f'"{(s_table or "").upper()}"'
                c_quoted = f'"{(coln or "").upper()}"'
                sql = f"SELECT COUNT(*) FROM {s_quoted}.{t_quoted} WHERE {c_quoted} IS NULL"
                try:
                    cnt = run_sql_count("db2", sql)
                    if cnt > 0:
                        local_issues.append(
                            {
                                "ValidationType": "constraint_integrity",
                                "ObjectType": "TABLE",
                                "SourceSchemaName": s_schema,
                                "SourceObjectName": s_table,
                                "DestinationSchemaName": r_schema,
                                "DestinationObjectName": r_table,
                                "ElementPath": f"{(s_schema or '').strip()}.{(s_table or '').strip()}.{(coln or '').strip()}",
                                "ErrorCode": "NOT_NULL_VIOLATION_IN_SOURCE",
                                "ErrorDescription": f"Non-nullable column has NULLs: {cnt}",
                                "DetailsJson": json.dumps({"column_name": coln}),
                            }
                        )
                except Exception:
                    pass

            r_notnull = (
                r_nn_cols.where((col("schema_norm") == upper(lit(r_schema))) & (col("table_norm") == upper(lit(r_table))) & (~col("IsNullable")))
                .select("column_name")
                .distinct()
                .collect()
            )
            for rowc in r_notnull:
                coln = rowc["column_name"]
                sql = f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] WHERE [{coln}] IS NULL"
                try:
                    cnt = run_sql_count("azure", sql)
                    if cnt > 0:
                        local_issues.append(
                            {
                                "ValidationType": "constraint_integrity",
                                "ObjectType": "TABLE",
                                "SourceSchemaName": s_schema,
                                "SourceObjectName": s_table,
                                "DestinationSchemaName": r_schema,
                                "DestinationObjectName": r_table,
                                "ElementPath": f"{(r_schema or '').strip()}.{(r_table or '').strip()}.{(coln or '').strip()}",
                                "ErrorCode": "NOT_NULL_VIOLATION_IN_TARGET",
                                "ErrorDescription": f"Non-nullable column has NULLs: {cnt}",
                                "DetailsJson": json.dumps({"column_name": coln}),
                            }
                        )
                except Exception:
                    pass

            l_len = (
                l_types_f.where((col("schema_norm") == upper(lit(s_schema))) & (col("table_norm") == upper(lit(s_table))) & (col("char_len").isNotNull()))
                .select("column_name", "char_len")
                .collect()
            )
            for rr in l_len:
                coln = rr["column_name"]
                lim = int(rr["char_len"] or 0)
                if lim > 0:
                    s_quoted = f'"{(s_schema or "").upper()}"'
                    t_quoted = f'"{(s_table or "").upper()}"'
                    c_quoted = f'"{(coln or "").upper()}"'
                    sql = f"SELECT COUNT(*) FROM {s_quoted}.{t_quoted} WHERE LENGTH({c_quoted}) > {lim}"
                    try:
                        cnt = run_sql_count("db2", sql)
                        if cnt > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(s_schema or '').strip()}.{(s_table or '').strip()}.{(coln or '').strip()}",
                                    "ErrorCode": "LENGTH_EXCEEDED_IN_SOURCE",
                                    "ErrorDescription": f"Values exceed length {lim}: {cnt}",
                                    "DetailsJson": json.dumps({"column_name": coln, "max_length": lim}),
                                }
                            )
                    except Exception:
                        pass
            r_len = (
                r_types_f.where((col("schema_norm") == upper(lit(r_schema))) & (col("table_norm") == upper(lit(r_table))) & (col("char_len").isNotNull()))
                .select("column_name", "char_len")
                .collect()
            )
            for rr in r_len:
                coln = rr["column_name"]
                lim = int(rr["char_len"] or 0)
                if lim > 0:
                    sql = f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] WHERE LEN([{coln}]) > {lim}"
                    try:
                        cnt = run_sql_count("azure", sql)
                        if cnt > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(r_schema or '').strip()}.{(r_table or '').strip()}.{(coln or '').strip()}",
                                    "ErrorCode": "LENGTH_EXCEEDED_IN_TARGET",
                                    "ErrorDescription": f"Values exceed length {lim}: {cnt}",
                                    "DetailsJson": json.dumps({"column_name": coln, "max_length": lim}),
                                }
                            )
                    except Exception:
                        pass

            l_checks = (
                db2_chk_f.where((col("schema_norm") == upper(lit(s_schema))) & (col("table_norm") == upper(lit(s_table))))
                .select("chk_name", "chk_def")
                .collect()
            )
            for rr in l_checks:
                cname = rr["chk_name"]
                cdef = str(rr["chk_def"] or "")
                if cdef.strip():
                    expr = cdef.strip().rstrip(";")
                    sql = f"SELECT COUNT(*) FROM {s_schema.upper()}.{s_table.upper()} WHERE NOT ({expr})"
                    try:
                        cnt = run_sql_count("db2", sql)
                        if cnt > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(s_schema or '').strip()}.{(s_table or '').strip()}.{(cname or '').strip()}",
                                    "ErrorCode": "CHECK_VIOLATION_IN_SOURCE",
                                    "ErrorDescription": f"Check constraint violated: {cnt}",
                                    "DetailsJson": json.dumps({"constraint_name": cname, "expression": expr}),
                                }
                            )
                    except Exception:
                        pass
            r_checks = (
                az_chk_f.where((col("schema_norm") == upper(lit(r_schema))) & (col("table_norm") == upper(lit(r_table))))
                .select("chk_name", "chk_def")
                .collect()
            )
            for rr in r_checks:
                cname = rr["chk_name"]
                cdef = str(rr["chk_def"] or "")
                if cdef.strip():
                    expr = cdef.strip().rstrip(";")
                    sql = f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] WHERE NOT ({expr})"
                    try:
                        cnt = run_sql_count("azure", sql)
                        if cnt > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(r_schema or '').strip()}.{(r_table or '').strip()}.{(cname or '').strip()}",
                                    "ErrorCode": "CHECK_VIOLATION_IN_TARGET",
                                    "ErrorDescription": f"Check constraint violated: {cnt}",
                                    "DetailsJson": json.dumps({"constraint_name": cname, "expression": expr}),
                                }
                            )
                    except Exception:
                        pass

            for cn in common_cols:
                lmd = lmap.get(cn)
                rmd = rmap.get(cn)
                if not lmd or not rmd:
                    continue
                if (lmd["char_len"] is not None) and (
                    str(rmd["data_type"] or "").strip().upper().find("DATE") >= 0
                    or str(rmd["data_type"] or "").strip().upper().find("TIME") >= 0
                ):
                    ov = get_override(s_schema, s_table, lmd["column_name"])
                    if ov:
                        rx = db2_regex_for(ov)
                        if rx:
                            coln = lmd["column_name"]
                            s_quoted = f'"{(s_schema or "").upper()}"'
                            t_quoted = f'"{(s_table or "").upper()}"'
                            c_quoted = f'"{(coln or "").upper()}"'
                            sql = f"SELECT COUNT(*) FROM {s_quoted}.{t_quoted} WHERE {c_quoted} IS NOT NULL AND NOT REGEXP_LIKE({c_quoted}, '{rx}')"
                            try:
                                cnt = run_sql_count("db2", sql)
                                if cnt > 0:
                                    issues.append(
                                        {
                                            "ValidationType": "constraint_integrity",
                                            "ObjectType": "TABLE",
                                            "SourceSchemaName": s_schema,
                                            "SourceObjectName": s_table,
                                            "DestinationSchemaName": r_schema,
                                            "DestinationObjectName": r_table,
                                            "ElementPath": f"{(s_schema or '').strip()}.{(s_table or '').strip()}.{(coln or '').strip()}",
                                            "ErrorCode": "INVALID_DATE_FORMAT_IN_SOURCE",
                                            "ErrorDescription": f"Does not match override format {ov}: {cnt}",
                                            "DetailsJson": json.dumps({"column_name": coln, "expected_format": ov}),
                                        }
                                    )
                            except Exception:
                                pass
                if (rmd["char_len"] is not None) and (
                    str(lmd["data_type"] or "").strip().upper().find("DATE") >= 0
                    or str(lmd["data_type"] or "").strip().upper().find("TIME") >= 0
                ):
                    ov = get_override(r_schema, r_table, rmd["column_name"])
                    coln = rmd["column_name"]
                    if ov:
                        style = mssql_style_for(ov)
                        if style is not None:
                            sql = f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] WHERE [{coln}] IS NOT NULL AND TRY_CONVERT(datetime2, [{coln}], {style}) IS NULL"
                        else:
                            sql = f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] WHERE [{coln}] IS NOT NULL AND TRY_CONVERT(date, [{coln}], 23) IS NULL"
                    else:
                        sql = (
                            f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] "
                            f"WHERE [{coln}] IS NOT NULL AND TRY_CONVERT(date, [{coln}], 23) IS NULL"
                        )
                    try:
                        cnt = run_sql_count("azure", sql)
                        if cnt > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(r_schema or '').strip()}.{(r_table or '').strip()}.{(coln or '').strip()}",
                                    "ErrorCode": "INVALID_DATE_FORMAT_IN_TARGET",
                                    "ErrorDescription": f"Does not match override format {ov}: {cnt}",
                                    "DetailsJson": json.dumps({"column_name": coln, "expected_format": ov}),
                                }
                            )
                    except Exception:
                        pass

            def _is_numeric(dt: str | None) -> bool:
                if not dt:
                    return False
                d = str(dt).strip().upper()
                return any(x in d for x in ["DECIMAL", "NUMERIC"])

            for cn in common_cols:
                lmd = lmap[cn]
                rmd = rmap[cn]
                t_prec = rmd["precision"]
                t_scale = rmd["scale"]
                s_prec = lmd["precision"]
                s_scale = lmd["scale"]
                if _is_numeric(lmd["data_type"]) and (t_prec is not None) and (t_scale is not None):
                    ip_digits = int((int(t_prec or 0) - int(t_scale or 0)))
                    if ip_digits < 0:
                        ip_digits = 0
                    coln = lmd["column_name"]
                    sql_overflow = (
                        f"SELECT COUNT(*) FROM {s_schema.upper()}.{s_table.upper()} "
                        f"WHERE {coln.upper()} IS NOT NULL AND ABS({coln.upper()}) >= POWER(10, {ip_digits})"
                    )
                    sql_round = (
                        f"SELECT COUNT(*) FROM {s_schema.upper()}.{s_table.upper()} "
                        f"WHERE {coln.upper()} IS NOT NULL AND ABS({coln.upper()}) < POWER(10, {ip_digits}) AND {coln.upper()} <> ROUND({coln.upper()}, {int(t_scale)})"
                    )
                    try:
                        cnt_of = run_sql_count("db2", sql_overflow)
                        if cnt_of > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(s_schema or '').strip()}.{(s_table or '').strip()}.{(str(lmd['column_name']) or '').strip()}",
                                    "ErrorCode": "NUMERIC_OVERFLOW_IN_SOURCE",
                                    "ErrorDescription": f"Integer digits exceed target precision (p={int(t_prec)}, s={int(t_scale)}): {cnt_of}",
                                    "DetailsJson": json.dumps(
                                        {"column_name": lmd["column_name"], "target_precision": int(t_prec), "target_scale": int(t_scale)}
                                    ),
                                }
                            )
                        cnt_rd = run_sql_count("db2", sql_round)
                        if cnt_rd > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(s_schema or '').strip()}.{(s_table or '').strip()}.{(str(lmd['column_name']) or '').strip()}",
                                    "ErrorCode": "NUMERIC_SCALE_ROUNDING_IN_SOURCE",
                                    "ErrorDescription": f"Requires rounding to fit target scale (p={int(t_prec)}, s={int(t_scale)}): {cnt_rd}",
                                    "DetailsJson": json.dumps(
                                        {"column_name": lmd["column_name"], "target_precision": int(t_prec), "target_scale": int(t_scale)}
                                    ),
                                }
                            )
                    except Exception:
                        pass
                if _is_numeric(rmd["data_type"]) and (s_prec is not None) and (s_scale is not None):
                    ip_digits = int((int(s_prec or 0) - int(s_scale or 0)))
                    if ip_digits < 0:
                        ip_digits = 0
                    coln = rmd["column_name"]
                    sql_overflow = (
                        f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] "
                        f"WHERE [{coln}] IS NOT NULL AND ABS([{coln}]) >= POWER(10, {ip_digits})"
                    )
                    sql_round = (
                        f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] "
                        f"WHERE [{coln}] IS NOT NULL AND ABS([{coln}]) < POWER(10, {ip_digits}) AND [{coln}] <> ROUND([{coln}], {int(s_scale)})"
                    )
                    try:
                        cnt_of = run_sql_count("azure", sql_overflow)
                        if cnt_of > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(r_schema or '').strip()}.{(r_table or '').strip()}.{(str(rmd['column_name']) or '').strip()}",
                                    "ErrorCode": "NUMERIC_OVERFLOW_IN_TARGET",
                                    "ErrorDescription": f"Integer digits exceed source precision (p={int(s_prec)}, s={int(s_scale)}): {cnt_of}",
                                    "DetailsJson": json.dumps(
                                        {"column_name": rmd["column_name"], "source_precision": int(s_prec), "source_scale": int(s_scale)}
                                    ),
                                }
                            )
                        cnt_rd = run_sql_count("azure", sql_round)
                        if cnt_rd > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(r_schema or '').strip()}.{(r_table or '').strip()}.{(str(rmd['column_name']) or '').strip()}",
                                    "ErrorCode": "NUMERIC_SCALE_ROUNDING_IN_TARGET",
                                    "ErrorDescription": f"Requires rounding to fit source scale (p={int(s_prec)}, s={int(s_scale)}): {cnt_rd}",
                                    "DetailsJson": json.dumps(
                                        {"column_name": rmd["column_name"], "source_precision": int(s_prec), "source_scale": int(s_scale)}
                                    ),
                                }
                            )
                    except Exception:
                        pass

            for cn in common_cols:
                lmd = lmap[cn]
                rmd = rmap[cn]
                l_is_char = lmd["char_len"] is not None
                r_is_date = str(rmd["data_type"] or "").strip().upper().find("DATE") >= 0 or str(rmd["data_type"] or "").strip().upper().find("TIME") >= 0
                if l_is_char and r_is_date:
                    ov = get_override(s_schema, s_table, lmd["column_name"]) if "get_override" in locals() else None
                    if ov:
                        pass
                    else:
                        coln = lmd["column_name"]
                        sql = (
                            f"SELECT COUNT(*) FROM {s_schema.upper()}.{s_table.upper()} "
                            f"WHERE {coln.upper()} IS NOT NULL AND NOT REGEXP_LIKE({coln.upper()}, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$')"
                        )
                    try:
                        cnt = run_sql_count("db2", sql)
                        if cnt > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(s_schema or '').strip()}.{(s_table or '').strip()}.{(str(lmd['column_name']) or '').strip()}",
                                    "ErrorCode": "INVALID_DATE_FORMAT_IN_SOURCE",
                                    "ErrorDescription": "Non-ISO date format (YYYY-MM-DD): {cnt}",
                                    "DetailsJson": json.dumps({"column_name": lmd["column_name"], "expected_format": "YYYY-MM-DD"}),
                                }
                            )
                    except Exception:
                        pass
                r_is_char = rmd["char_len"] is not None
                l_is_date = str(lmd["data_type"] or "").strip().upper().find("DATE") >= 0 or str(lmd["data_type"] or "").strip().upper().find("TIME") >= 0
                if r_is_char and l_is_date:
                    ov = get_override(r_schema, r_table, rmd["column_name"]) if "get_override" in locals() else None
                    coln = rmd["column_name"]
                    if ov:
                        style = mssql_style_for(ov)
                        if style is not None:
                            sql = f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] WHERE [{coln}] IS NOT NULL AND TRY_CONVERT(datetime2, [{coln}], {style}) IS NULL"
                        else:
                            sql = f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] WHERE [{coln}] IS NOT NULL AND TRY_CONVERT(date, [{coln}], 23) IS NULL"
                    else:
                        sql = (
                            f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] "
                            f"WHERE [{coln}] IS NOT NULL AND TRY_CONVERT(date, [{coln}], 23) IS NULL"
                        )
                    try:
                        cnt = run_sql_count("azure", sql)
                        if cnt > 0:
                            local_issues.append(
                                {
                                    "ValidationType": "constraint_integrity",
                                    "ObjectType": "TABLE",
                                    "SourceSchemaName": s_schema,
                                    "SourceObjectName": s_table,
                                    "DestinationSchemaName": r_schema,
                                    "DestinationObjectName": r_table,
                                    "ElementPath": f"{(r_schema or '').strip()}.{(r_table or '').strip()}.{(str(rmd['column_name']) or '').strip()}",
                                    "ErrorCode": "INVALID_DATE_FORMAT_IN_TARGET",
                                    "ErrorDescription": f"Invalid/Non-ISO date values (expect YYYY-MM-DD): {cnt}",
                                    "DetailsJson": json.dumps({"column_name": rmd["column_name"], "expected_format": "YYYY-MM-DD"}),
                                }
                            )
                    except Exception:
                        pass

            for cn in common_cols:
                lmd = lmap.get(cn)
                rmd = rmap.get(cn)
                if not lmd or not rmd:
                    continue
                if (lmd["char_len"] is not None) and _is_numeric(rmd["data_type"]):
                    t_prec = rmd["precision"]
                    t_scale = rmd["scale"]
                    if (t_prec is not None) and (t_scale is not None):
                        ip = int((int(t_prec or 0) - int(t_scale or 0)))
                        if ip < 0:
                            ip = 0
                        coln = lmd["column_name"]
                        rx = f"^[+-]?[0-9]{{1,{ip}}}(\\.[0-9]{{0,{int(t_scale)}}})?$" if int(ip) > 0 else f"^[+-]?0?(\\.[0-9]{{0,{int(t_scale)}}})?$"
                        sql = f"SELECT COUNT(*) FROM {s_schema.upper()}.{s_table.upper()} WHERE {coln.upper()} IS NOT NULL AND NOT REGEXP_LIKE({coln.upper()}, '{rx}')"
                        try:
                            cnt = run_sql_count("db2", sql)
                            if cnt > 0:
                                local_issues.append(
                                    {
                                        "ValidationType": "constraint_integrity",
                                        "ObjectType": "TABLE",
                                        "SourceSchemaName": s_schema,
                                        "SourceObjectName": s_table,
                                        "DestinationSchemaName": r_schema,
                                        "DestinationObjectName": r_table,
                                        "ElementPath": f"{(s_schema or '').strip()}.{(s_table or '').strip()}.{(coln or '').strip()}",
                                        "ErrorCode": "NUMERIC_STRING_CONVERSION_FAILED_IN_SOURCE",
                                        "ErrorDescription": f"Not convertible to DECIMAL({int(t_prec)},{int(t_scale)}): {cnt}",
                                        "DetailsJson": json.dumps(
                                            {"column_name": coln, "target_precision": int(t_prec), "target_scale": int(t_scale)}
                                        ),
                                    }
                                )
                        except Exception:
                            pass
                if (rmd["char_len"] is not None) and _is_numeric(lmd["data_type"]):
                    s_prec = lmd["precision"]
                    s_scale = lmd["scale"]
                    if (s_prec is not None) and (s_scale is not None):
                        coln = rmd["column_name"]
                        sql = f"SELECT COUNT(*) FROM [{r_schema}].[{r_table}] WHERE [{coln}] IS NOT NULL AND TRY_CONVERT(decimal({int(s_prec)},{int(s_scale)}), [{coln}]) IS NULL"
                        try:
                            cnt = run_sql_count("azure", sql)
                            if cnt > 0:
                                local_issues.append(
                                    {
                                        "ValidationType": "constraint_integrity",
                                        "ObjectType": "TABLE",
                                        "SourceSchemaName": s_schema,
                                        "SourceObjectName": s_table,
                                        "DestinationSchemaName": r_schema,
                                        "DestinationObjectName": r_table,
                                        "ElementPath": f"{(r_schema or '').strip()}.{(r_table or '').strip()}.{(coln or '').strip()}",
                                        "ErrorCode": "NUMERIC_STRING_CONVERSION_FAILED_IN_TARGET",
                                        "ErrorDescription": f"Not convertible to DECIMAL({int(s_prec)},{int(s_scale)}): {cnt}",
                                        "DetailsJson": json.dumps(
                                            {"column_name": coln, "source_precision": int(s_prec), "source_scale": int(s_scale)}
                                        ),
                                    }
                                )
                        except Exception:
                            pass
            return local_issues

        from concurrent.futures import ThreadPoolExecutor, as_completed

        max_workers = int(os.environ.get("DV_CI_WORKERS", "8"))
        error_budget = int(os.environ.get("DV_CI_ERROR_BUDGET", "10"))
        errors = 0
        rows = pairs.collect()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(process_pair, pr) for pr in rows]
            for f in as_completed(futures):
                try:
                    issues.extend(f.result())
                except Exception:
                    errors += 1
                    if errors >= error_budget:
                        break

        result_df = (
            self.spark.createDataFrame(issues)
            if issues
            else self.spark.createDataFrame(
                [],
                schema="ValidationType string, ObjectType string, SourceSchemaName string, SourceObjectName string, DestinationSchemaName string, DestinationObjectName string, ElementPath string, ErrorCode string, ErrorDescription string, DetailsJson string",
            )
        )

        cols = result_df.columns
        first_cols = [
            "ValidationType",
            "ObjectType",
            "SourceObjectName",
            "SourceSchemaName",
            "DestinationObjectName",
            "DestinationSchemaName",
        ]
        last_cols = ["DetailsJson"] if "DetailsJson" in cols else []
        middle_cols = [c for c in cols if c not in first_cols and c not in last_cols]
        ordered = [c for c in first_cols if c in cols] + middle_cols + last_cols
        return result_df.select(*ordered)

    def generate_summary_statistics(self, comparison_df: DataFrame) -> Dict[str, int]:
        """Generate summary statistics using PySpark."""
        total_objects = comparison_df.count()
        matches = comparison_df.filter(col("status") == "MATCH").count()
        missing_in_target = comparison_df.filter(col("status") == "MISSING_IN_TARGET").count()
        missing_in_source = comparison_df.filter(col("status") == "MISSING_IN_SOURCE").count()

        differences = total_objects - matches
        return {
            "total_objects_compared": total_objects,
            "matches": matches,
            "differences": differences,
            "missing_in_target": missing_in_target,
            "missing_in_source": missing_in_source,
        }

    @log_duration("save_comparison_to_csv")
    def save_comparison_to_csv(self, comparison_df: DataFrame, filename_prefix: str) -> str:
        """Save comparison results to CSV using PySpark."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        uniq = uuid.uuid4().hex[:6]
        dbn = self.get_azure_db_name_for_filename()

        outputs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "outputs")
        os.makedirs(outputs_dir, exist_ok=True)

        filename = f"{filename_prefix}_{dbn}_{ts}_{uniq}.csv"
        final_csv = os.path.join(outputs_dir, filename)

        pandas_df = comparison_df.toPandas()
        pandas_df.to_csv(final_csv, mode="a", index=False, header=not pd.io.common.file_exists(final_csv))
        return final_csv


class PySparkDataValidationAzureService(PySparkAzureSchemaComparisonService):
    """Data validations for Azure → Azure."""

    def __init__(self, *, access_token_override: Optional[str] = None):
        super().__init__(
            config_filename="azure_database_config.json",
            side_config_map={"db2": "source_azure_sql", "azure": "target_azure_sql"},
            side_labels={"db2": "Source Azure SQL", "azure": "Target Azure SQL"},
            side_engine_map={"db2": "azure", "azure": "azure"},
            app_name="Azure_Azure_Data_Validation",
            access_token_override=access_token_override,
        )

