# Author: Sa-tish Chauhan

"""Schema validation service thin wrappers."""
from typing import Optional, Dict, Any, List

import uuid
from datetime import datetime

import os
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
    regexp_replace,
    collect_set,
    sort_array,
    array_sort,
    split,
    transform,
)
from pyspark.sql.types import StructType, StructField, StringType
from functools import reduce

from db2_azure_validation.services.pyspark_schema_comparison import (
    PySparkSchemaComparisonService,
    PySparkAzureSchemaComparisonService,
    log_duration,
)


class PySparkSchemaValidationService(PySparkSchemaComparisonService):
    """Schema validations for DB2 to Azure (structure only)."""

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

    # Thin wrappers to expose schema comparisons via this service
    @log_duration("compare_schema_presence")
    def compare_schema_presence(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        if not object_types:
            object_types = [
                "TABLE",
                "VIEW",
                "PROCEDURE",
                "FUNCTION",
                "TRIGGER",
                "INDEX",
                "CONSTRAINT",
                "SEQUENCE",
                "SYNONYM",
            ]

        def read_objects(side: str, obj_types: list[str], schema: str | None) -> DataFrame:
            frames = []
            creds = self._build_jdbc_urls()[side]
            engine = self._side_engine(side)
            schema_norm = schema.strip() if isinstance(schema, str) else None
            schema_upper = schema_norm.upper() if schema_norm else None

            def load(q: str) -> DataFrame:
                reader = (
                    self.spark.read.format("jdbc")
                    .option("url", creds["url"])
                    .option("dbtable", f"({q}) x")
                    .option("driver", creds["driver"])
                    .option("fetchsize", self._default_fetchsize())
                )
                reader = self._apply_jdbc_auth(reader, creds)
                return reader.load()

            def add(df: DataFrame, typ: str) -> None:
                base = df.withColumn("schema_name", trim(col("schema_name")))
                if schema_upper:
                    base = base.where(upper(col("schema_name")) == lit(schema_upper))
                frames.append(base.select(lit(typ).alias("object_type"), col("schema_name"), col("object_name")))

            u = [t.upper() for t in obj_types]
            if engine == "azure":
                if any(t in u for t in ["TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER"]):
                    mapping = {
                        "TABLE": "type='U'",
                        "VIEW": "type='V'",
                        "PROCEDURE": "type='P'",
                        "FUNCTION": "type in ('FN','IF','TF')",
                    }
                    for t, cond in mapping.items():
                        if t in u:
                            q = f"SELECT RTRIM(SCHEMA_NAME(schema_id)) AS schema_name, RTRIM(name) AS object_name FROM sys.objects WHERE {cond}"
                            if schema_norm:
                                q += f" AND UPPER(SCHEMA_NAME(schema_id)) = UPPER('{schema_norm}')"
                            add(load(q), t)
                if "INDEX" in u:
                    q = (
                        "SELECT RTRIM(s.name) AS schema_name, (RTRIM(t.name) + '.' + RTRIM(i.name)) AS object_name FROM sys.indexes i "
                        "JOIN sys.tables t ON i.object_id=t.object_id JOIN sys.schemas s ON t.schema_id=s.schema_id "
                        "WHERE i.is_hypothetical=0 AND i.name IS NOT NULL AND i.is_primary_key=0 AND i.is_unique=0"
                    )
                    if schema_norm:
                        q += f" AND UPPER(s.name) = UPPER('{schema_norm}')"
                    add(load(q), "INDEX")
                if "CONSTRAINT" in u:
                    q = (
                        "SELECT RTRIM(s.name) AS schema_name, (RTRIM(t.name) + '.' + RTRIM(kc.name)) AS object_name FROM sys.key_constraints kc "
                        "JOIN sys.tables t ON kc.parent_object_id=t.object_id JOIN sys.schemas s ON t.schema_id=s.schema_id"
                    )
                    if schema_norm:
                        q += f" WHERE UPPER(s.name) = UPPER('{schema_norm}') AND kc.type <> 'PK'"
                    else:
                        q += " WHERE kc.type <> 'PK'"
                    add(load(q), "CONSTRAINT")
                    q = (
                        "SELECT RTRIM(s.name) AS schema_name, (RTRIM(t.name) + '.' + RTRIM(cc.name)) AS object_name FROM sys.check_constraints cc "
                        "JOIN sys.tables t ON cc.parent_object_id=t.object_id JOIN sys.schemas s ON t.schema_id=s.schema_id"
                    )
                    if schema_norm:
                        q += f" WHERE UPPER(s.name) = UPPER('{schema_norm}')"
                    add(load(q), "CONSTRAINT")
                    q = (
                        "SELECT RTRIM(s.name) AS schema_name, (RTRIM(t.name) + '.' + RTRIM(dc.name)) AS object_name FROM sys.default_constraints dc "
                        "JOIN sys.tables t ON dc.parent_object_id=t.object_id JOIN sys.schemas s ON t.schema_id=s.schema_id"
                    )
                    if schema_norm:
                        q += f" WHERE UPPER(s.name) = UPPER('{schema_norm}')"
                    add(load(q), "CONSTRAINT")
                if "SEQUENCE" in u:
                    q = "SELECT RTRIM(SCHEMA_NAME(schema_id)) AS schema_name, RTRIM(name) AS object_name FROM sys.sequences"
                    if schema_norm:
                        q += f" WHERE UPPER(SCHEMA_NAME(schema_id)) = UPPER('{schema_norm}')"
                    add(load(q), "SEQUENCE")
                if "SYNONYM" in u:
                    q = "SELECT RTRIM(SCHEMA_NAME(schema_id)) AS schema_name, RTRIM(name) AS object_name FROM sys.synonyms"
                    if schema_norm:
                        q += f" WHERE UPPER(SCHEMA_NAME(schema_id)) = UPPER('{schema_norm}')"
                    add(load(q), "SYNONYM")
                if "TRIGGER" in u:
                    q = (
                        "SELECT RTRIM(s.name) AS schema_name, RTRIM(trg.name) AS object_name, trg.parent_id "
                        "FROM sys.triggers trg "
                        "JOIN sys.tables t ON trg.parent_id=t.object_id "
                        "JOIN sys.schemas s ON t.schema_id=s.schema_id "
                        "WHERE trg.is_ms_shipped=0 AND trg.parent_class = 1 AND trg.is_disabled = 0"
                    )
                    if schema_upper:
                        q += f" AND UPPER(RTRIM(s.name)) = '{schema_upper}'"
                    add(load(q), "TRIGGER")
            else:
                if "TABLE" in u or "VIEW" in u:
                    q = "SELECT RTRIM(CREATOR) AS schema_name, RTRIM(NAME) AS object_name, TYPE FROM SYSIBM.SYSTABLES"
                    if schema:
                        q += f" WHERE UPPER(CREATOR) = UPPER('{schema}')"
                    df = load(q).withColumn("TYPE", upper(trim(col("TYPE"))))
                    if "TABLE" in u:
                        add(df.where(col("TYPE") == "T").select(col("schema_name"), col("object_name")), "TABLE")
                    if "VIEW" in u:
                        add(df.where(col("TYPE") == "V").select(col("schema_name"), col("object_name")), "VIEW")
                if "PROCEDURE" in u or "FUNCTION" in u:
                    q = "SELECT RTRIM(ROUTINESCHEMA) AS schema_name, RTRIM(ROUTINENAME) AS object_name, ROUTINETYPE FROM SYSCAT.ROUTINES"
                    if schema:
                        q += f" WHERE UPPER(ROUTINESCHEMA) = UPPER('{schema}')"
                    df = load(q).withColumn("ROUTINETYPE", upper(trim(col("ROUTINETYPE"))))
                    if "PROCEDURE" in u:
                        add(df.where(col("ROUTINETYPE") == "P").select(col("schema_name"), col("object_name")), "PROCEDURE")
                    if "FUNCTION" in u:
                        add(df.where(col("ROUTINETYPE") == "F").select(col("schema_name"), col("object_name")), "FUNCTION")
                if "TRIGGER" in u:
                    q = "SELECT RTRIM(TRIGSCHEMA) AS schema_name, RTRIM(TRIGNAME) AS object_name FROM SYSCAT.TRIGGERS WHERE VALID = 'Y'"
                    if schema_upper:
                        q += f" AND UPPER(RTRIM(TRIGSCHEMA)) = '{schema_upper}'"
                    add(load(q), "TRIGGER")
                if "INDEX" in u:
                    q = (
                        "SELECT RTRIM(INDSCHEMA) AS schema_name, (RTRIM(TABNAME) || '.' || RTRIM(INDNAME)) AS object_name FROM SYSCAT.INDEXES WHERE UNIQUERULE NOT IN ('P','U')"
                    )
                    if schema:
                        q += f" AND UPPER(INDSCHEMA) = UPPER('{schema}')"
                    add(load(q), "INDEX")
                if "CONSTRAINT" in u:
                    q = "SELECT RTRIM(TABSCHEMA) AS schema_name, (RTRIM(TABNAME) || '.' || RTRIM(CONSTNAME)) AS object_name FROM SYSCAT.TABCONST WHERE TYPE NOT IN ('F','P')"
                    if schema:
                        q += f" AND UPPER(TABSCHEMA) = UPPER('{schema}')"
                    add(load(q), "CONSTRAINT")
                if "SEQUENCE" in u:
                    q = "SELECT RTRIM(SEQSCHEMA) AS schema_name, RTRIM(SEQNAME) AS object_name FROM SYSCAT.SEQUENCES"
                    if schema:
                        q += f" WHERE UPPER(SEQSCHEMA) = UPPER('{schema}')"
                    add(load(q), "SEQUENCE")

            if not frames:
                return self.spark.createDataFrame(
                    [],
                    StructType(
                        [
                            StructField("object_type", StringType(), True),
                            StructField("schema_name", StringType(), True),
                            StructField("object_name", StringType(), True),
                        ]
                    ),
                )
            return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

        l = read_objects("db2", object_types, source_schema)
        r = read_objects("azure", object_types, target_schema)

        lk = l.select(
            upper(trim(col("schema_name"))).alias("s_schema_norm"),
            upper(trim(col("object_name"))).alias("s_object_norm"),
            upper(trim(col("object_type"))).alias("s_type"),
            trim(col("schema_name")).alias("s_schema"),
            trim(col("object_name")).alias("s_object"),
        )
        rk = r.select(
            upper(trim(col("schema_name"))).alias("r_schema_norm"),
            upper(trim(col("object_name"))).alias("r_object_norm"),
            upper(trim(col("object_type"))).alias("r_type"),
            trim(col("schema_name")).alias("r_schema"),
            trim(col("object_name")).alias("r_object"),
        )

        left_only = lk.join(
            rk,
            on=(
                (col("s_schema_norm") == col("r_schema_norm"))
                & (col("s_object_norm") == col("r_object_norm"))
                & (col("s_type") == col("r_type"))
            ),
            how="left_anti",
        ).select(
            col("s_type").alias("ObjectType"),
            col("s_schema").alias("SourceSchemaName"),
            col("s_object").alias("SourceObjectName"),
            lit("").alias("DestinationSchemaName"),
            lit("").alias("DestinationObjectName"),
            lit("MISSING_IN_TARGET").alias("ChangeType"),
            concat(trim(col("s_schema")), lit("."), trim(col("s_object"))).alias("ElementPath"),
            lit("Object exists in source but not in target").alias("ErrorDescription"),
        )

        right_only = rk.join(
            lk,
            on=(
                (col("r_schema_norm") == col("s_schema_norm"))
                & (col("r_object_norm") == col("s_object_norm"))
                & (col("r_type") == col("s_type"))
            ),
            how="left_anti",
        ).select(
            col("r_type").alias("ObjectType"),
            lit("").alias("SourceSchemaName"),
            lit("").alias("SourceObjectName"),
            col("r_schema").alias("DestinationSchemaName"),
            col("r_object").alias("DestinationObjectName"),
            lit("MISSING_IN_SOURCE").alias("ChangeType"),
            concat(trim(col("r_schema")), lit("."), trim(col("r_object"))).alias("ElementPath"),
            lit("Object exists in Azure SQL but not in DB2").alias("ErrorDescription"),
        )

        return left_only.unionByName(right_only)

    @log_duration("table_column_counts")
    def table_column_counts(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        return super().table_column_counts(source_schema, target_schema, object_types)

    @log_duration("compare_column_datatypes_mapped")
    def compare_column_datatypes_mapped(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        if not object_types:
            object_types = ["TABLE"]
        l = self._read_tables("db2", object_types, source_schema)
        r = self._read_tables("azure", object_types, target_schema)
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
        l_types = self._fetch_columns_types_bulk("db2", s_schemas)
        r_types = self._fetch_columns_types_bulk("azure", r_schemas)
        l_n = (
            l_types.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )
        r_n = (
            r_types.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )

        l_cols = l_n.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        r_cols = r_n.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        j = (
            l_cols.alias("l")
            .join(r_cols.alias("r"), on=["schema_norm", "table_norm", "col_norm"], how="inner")
            .select(
                col("schema_norm"),
                col("table_norm"),
                col("col_norm"),
                col("l.column_name").alias("ColumnName"),
                col("l.data_type").alias("SourceType"),
                col("l.char_len").alias("SourceCharLen"),
                col("l.precision").alias("SourcePrecision"),
                col("l.scale").alias("SourceScale"),
                col("r.data_type").alias("DestinationType"),
                col("r.char_len").alias("DestinationCharLen"),
                col("r.precision").alias("DestinationPrecision"),
                col("r.scale").alias("DestinationScale"),
            )
        )

        st = upper(trim(col("SourceType")))
        dt = upper(trim(col("DestinationType")))

        source_engine = self._side_engine("db2")
        target_engine = self._side_engine("azure")

        expected_type_expr = (
            st
            if source_engine == "azure"
            else when(st.isin("INTEGER", "INT"), lit("INT"))
            .when(st == lit("SMALLINT"), lit("SMALLINT"))
            .when(st == lit("BIGINT"), lit("BIGINT"))
            .when(st == lit("VARCHAR"), lit("NVARCHAR"))
            .when(st.isin("CHAR", "CHARACTER"), lit("CHAR"))
            .when(st.isin("CLOB", "DBCLOB"), lit("NVARCHAR"))
            .when(st == lit("TIMESTAMP"), lit("DATETIME2"))
            .when(st == lit("DATE"), lit("DATE"))
            .when(st == lit("TIME"), lit("TIME"))
            .when(st.isin("DECIMAL", "NUMERIC"), lit("DECIMAL"))
            .when(st.isin("DOUBLE", "FLOAT", "DOUBLE PRECISION"), lit("FLOAT"))
            .when(st == lit("REAL"), lit("REAL"))
            .when(st == lit("BLOB"), lit("VARBINARY"))
            .when(st == lit("BINARY"), lit("BINARY"))
            .when(st == lit("VARBINARY"), lit("VARBINARY"))
            .when(st == lit("XML"), lit("XML"))
            .when(st == lit("BOOLEAN"), lit("BIT"))
            .otherwise(st)
        )

        actual_type_expr = (
            dt
            if target_engine == "azure"
            else when(dt.isin("INT", "INTEGER"), lit("INT"))
            .when(dt == lit("SMALLINT"), lit("SMALLINT"))
            .when(dt == lit("BIGINT"), lit("BIGINT"))
            .when(dt == lit("NVARCHAR"), lit("NVARCHAR"))
            .when(dt.isin("CHAR", "CHARACTER"), lit("CHAR"))
            .when(dt == lit("DATETIME2"), lit("DATETIME2"))
            .when(dt == lit("DATE"), lit("DATE"))
            .when(dt == lit("TIME"), lit("TIME"))
            .when(dt == lit("DECIMAL"), lit("DECIMAL"))
            .when(dt == lit("FLOAT"), lit("FLOAT"))
            .when(dt == lit("REAL"), lit("REAL"))
            .when(dt == lit("VARBINARY"), lit("VARBINARY"))
            .when(dt == lit("BINARY"), lit("BINARY"))
            .when(dt == lit("XML"), lit("XML"))
            .when(dt == lit("BIT"), lit("BIT"))
            .otherwise(dt)
        )

        comp = j.withColumn("ExpectedType", expected_type_expr).withColumn("ActualType", actual_type_expr)

        if source_engine == "azure":
            comp2 = (
                comp.withColumn("ExpectedCharLen", col("SourceCharLen"))
                .withColumn("ExpectedPrecision", col("SourcePrecision"))
                .withColumn("ExpectedScale", col("SourceScale"))
            )
        else:
            comp2 = (
                comp.withColumn(
                    "ExpectedCharLen",
                    when(st == lit("VARCHAR"), col("SourceCharLen"))
                    .when(st.isin("CLOB", "DBCLOB"), lit(-1))
                    .when(st.isin("CHAR", "CHARACTER", "BINARY", "VARBINARY"), col("SourceCharLen"))
                    .otherwise(lit(None).cast("long")),
                )
                .withColumn(
                    "ExpectedPrecision",
                    when(st.isin("DECIMAL", "NUMERIC"), col("SourcePrecision")).otherwise(lit(None).cast("long")),
                )
                .withColumn(
                    "ExpectedScale",
                    when(st == lit("TIMESTAMP"), lit(7))
                    .when(st.isin("DECIMAL", "NUMERIC"), col("SourceScale"))
                    .otherwise(lit(None).cast("long")),
                )
            )

        res = (
            comp2.withColumn(
                "TypeNameMismatch",
                when(upper(trim(col("SourceType"))) == upper(trim(col("DestinationType"))), lit(False)).otherwise(
                    col("ExpectedType") != col("ActualType")
                ),
            )
            .withColumn(
                "LenMismatch",
                when(
                    col("ExpectedType").isin("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "VARBINARY", "BINARY"),
                    (col("ExpectedCharLen").isNotNull()) & (col("ExpectedCharLen") != col("DestinationCharLen")),
                ).otherwise(lit(False)),
            )
            .withColumn(
                "DecMismatch",
                when(
                    col("ExpectedType") == "DECIMAL",
                    (col("ExpectedPrecision").isNotNull())
                    & (
                        (col("ExpectedPrecision") != col("DestinationPrecision"))
                        | (coalesce(col("ExpectedScale"), lit(0)) != coalesce(col("DestinationScale"), lit(0)))
                    ),
                ).otherwise(lit(False)),
            )
        )

        allow_ts_lte = os.environ.get("TYPE_LENIENCY_TS_SCALE_LTE_7", "").strip().lower() in ("1", "true", "yes", "y", "on")
        if allow_ts_lte:
            res = res.withColumn(
                "TsMismatch",
                when(
                    col("ExpectedType") == "DATETIME2",
                    (col("DestinationScale").isNull()) | (col("DestinationScale") > lit(7)),
                ).otherwise(lit(False)),
            )
        else:
            res = res.withColumn(
                "TsMismatch",
                when(
                    col("ExpectedType") == "DATETIME2",
                    (col("ExpectedScale").isNotNull()) & (col("ExpectedScale") != col("DestinationScale")),
                ).otherwise(lit(False)),
            )

        res = res.withColumn("Mismatch", col("TypeNameMismatch") | col("LenMismatch") | col("DecMismatch") | col("TsMismatch"))

        res2 = res.join(
            pairs.select(
                "s_schema_norm",
                "s_object_norm",
                "SourceSchemaName",
                "SourceObjectName",
                "DestinationSchemaName",
                "DestinationObjectName",
            ),
            on=[res.schema_norm == col("s_schema_norm"), res.table_norm == col("s_object_norm")],
            how="inner",
        )

        res2 = res2.where(col("Mismatch"))

        dtype_rules = self._parse_validation_rules("DV_DTYPE_RULES")
        warn_varchar_varbinary = any(
            (str(r.get("rule_type", "")).lower() == "varchar_to_varbinary")
            and (str(r.get("match_type", "")).lower() == "warning")
            for r in dtype_rules
        )
        res2 = res2.withColumn(
            "Status",
            when(
                (upper(trim(col("SourceType"))) == lit("VARCHAR"))
                & (upper(trim(col("DestinationType"))) == lit("VARBINARY"))
                & lit(bool(warn_varchar_varbinary)),
                lit("warning"),
            ).otherwise(lit("error")),
        )

        return res2.select(
            lit("DataType").alias("ValidationType"),
            lit("TABLE").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            col("ColumnName"),
            col("SourceType").alias("SourceDataType"),
            col("DestinationType").alias("DestinationDataType"),
            col("ExpectedType").alias("ExpectedAzureType"),
            col("ActualType").alias("ActualAzureType"),
            col("SourceCharLen"),
            col("DestinationCharLen"),
            col("SourcePrecision"),
            col("SourceScale"),
            col("DestinationPrecision"),
            col("DestinationScale"),
            col("TypeNameMismatch"),
            (col("LenMismatch") | col("DecMismatch") | col("TsMismatch")).alias("SizeMismatch"),
            col("Status"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName")), lit("."), trim(col("ColumnName"))).alias(
                "ElementPath"
            ),
            when(col("TypeNameMismatch"), lit("Data type name mismatch"))
            .when(col("SizeMismatch"), lit("Data type size mismatch"))
            .otherwise(lit(""))
            .alias("ErrorDescription"),
        )

    @log_duration("compare_column_default_values")
    def compare_column_default_values(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        if not object_types:
            object_types = ["TABLE"]
        l = self._read_tables("db2", object_types, source_schema)
        r = self._read_tables("azure", object_types, target_schema)
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
        l_defs = self._fetch_columns_defaults_bulk("db2", s_schemas)
        r_defs = self._fetch_columns_defaults_bulk("azure", r_schemas)
        l_n = (
            l_defs.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )
        r_n = (
            r_defs.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )

        l_cols = l_n.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        r_cols = r_n.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )

        l_c = (
            l_cols.withColumn("SourceDefault", col("default_str"))
            .withColumn(
                "SourceDefaultCanon",
                upper(
                    trim(
                        regexp_replace(
                            regexp_replace(col("default_str"), "^'([^']*)'$", "$1"),
                            "^\\((.*)\\)$",
                            "$1",
                        )
                    )
                ),
            )
        )
        r_c = (
            r_cols.withColumn("DestinationDefault", col("default_str"))
            .withColumn(
                "DestinationDefaultCanon",
                upper(
                    trim(
                        regexp_replace(
                            regexp_replace(col("default_str"), "^'([^']*)'$", "$1"),
                            "^\\((.*)\\)$",
                            "$1",
                        )
                    )
                ),
            )
        )

        j = (
            l_c.alias("l")
            .join(r_c.alias("r"), on=["schema_norm", "table_norm", "col_norm"], how="inner")
            .select(
                col("schema_norm"),
                col("table_norm"),
                col("col_norm"),
                col("l.column_name").alias("ColumnName"),
                col("SourceDefault"),
                col("SourceDefaultCanon"),
                col("DestinationDefault"),
                col("DestinationDefaultCanon"),
            )
        )

        default_value_rules = self._parse_validation_rules("DV_DEFAULT_VALUE_RULES")
        be_warn = any(
            (str(r.get("rule_type", "")).lower() == "bracket_equivalent")
            and (str(r.get("match_type", "")).lower() == "warning")
            for r in (default_value_rules or [])
        )
        be_ign = any(
            (str(r.get("rule_type", "")).lower() == "bracket_equivalent")
            and (str(r.get("match_type", "")).lower() == "ignore")
            for r in (default_value_rules or [])
        )
        fe_warn = any(
            (str(r.get("rule_type", "")).lower() == "function_equivalent")
            and (str(r.get("match_type", "")).lower() == "warning")
            for r in (default_value_rules or [])
        )
        fe_ign = any(
            (str(r.get("rule_type", "")).lower() == "function_equivalent")
            and (str(r.get("match_type", "")).lower() == "ignore")
            for r in (default_value_rules or [])
        )
        mv_warn = any(
            (str(r.get("rule_type", "")).lower() == "missing_vs_numeric")
            and (str(r.get("match_type", "")).lower() == "warning")
            for r in (default_value_rules or [])
        )
        mv_ign = any(
            (str(r.get("rule_type", "")).lower() == "missing_vs_numeric")
            and (str(r.get("match_type", "")).lower() == "ignore")
            for r in (default_value_rules or [])
        )

        rules_bc = self.spark.sparkContext.broadcast(default_value_rules)
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        def _apply_rule_local(source_val, dest_val):
            import re as _re

            s = ("" if source_val is None else str(source_val)).upper()
            d = ("" if dest_val is None else str(dest_val)).upper()
            if s == d:
                return "match"

            def _strip_parens(x: str) -> str:
                y = x
                for _ in range(2):
                    if len(y) >= 2 and y.startswith("(") and y.endswith(")"):
                        y = y[1:-1]
                    else:
                        break
                return y

            s2 = _strip_parens(s)
            d2 = _strip_parens(d)
            for rule in (rules_bc.value or []):
                rule_type = str(rule.get("rule_type", "")).lower()
                pattern = str(rule.get("pattern", "")).strip()
                match_type = str(rule.get("match_type", "")).lower()

                def _hit(pat: str, val: str) -> bool:
                    if pat == "*":
                        return True
                    if pat == "":
                        return False
                    try:
                        return _re.match(pat.replace("*", ".*"), val, _re.IGNORECASE) is not None
                    except Exception:
                        return val == pat.upper()

                _global_ok = (pattern == "") and (
                    rule_type in ("bracket_equivalent", "function_equivalent", "missing_vs_numeric")
                )
                if not (_global_ok or _hit(pattern, s) or _hit(pattern, d)):
                    continue
                if rule_type == "bracket_equivalent":
                    if (s2 == d2) and (s != d):
                        return "warning" if match_type == "warning" else ("ignore" if match_type == "ignore" else "error")
                if rule_type == "function_equivalent":
                    groups = [
                        {"CURRENT_TIMESTAMP", "CURRENT TIMESTAMP", "SYSDATE", "SYSDATETIME()", "GETDATE()", "GETUTCDATE()"},
                        {"CURRENT_DATE", "CURRENT_DATE()"},
                        {"CURRENT TIME", "CURRENT_TIME()"},
                    ]
                    if pattern not in ("", "*"):
                        up = pattern.upper()
                        groups = [g for g in groups if up in g] or groups
                    for g in groups:
                        if s2 in g and d2 in g:
                            return "warning" if match_type == "warning" else ("ignore" if match_type == "ignore" else "error")
                if rule_type == "missing_vs_numeric":
                    is_num = lambda x: _re.match(r"^[+\-]?\d+(\.\d+)?$", x) is not None
                    miss_vs_num = ((s2 == "") and is_num(d2)) or ((d2 == "") and is_num(s2))
                    if miss_vs_num:
                        return "warning" if match_type == "warning" else ("ignore" if match_type == "ignore" else "error")
            return "error"

        apply_rule_spark_udf = udf(_apply_rule_local, StringType())

        out = j.join(
            pairs.select(
                "s_schema_norm",
                "s_object_norm",
                "SourceSchemaName",
                "SourceObjectName",
                "DestinationSchemaName",
                "DestinationObjectName",
            ),
            on=[j.schema_norm == col("s_schema_norm"), j.table_norm == col("s_object_norm")],
            how="inner",
        )

        from pyspark.sql.functions import regexp_replace as rxr

        s0 = coalesce(col("SourceDefaultCanon"), lit(""))
        d0 = coalesce(col("DestinationDefaultCanon"), lit(""))
        paren_rx = "^\\((.*)\\)$"
        # Strip a few layers of outer parentheses to normalize before comparison
        s_un = rxr(rxr(rxr(s0, paren_rx, "$1"), paren_rx, "$1"), paren_rx, "$1")
        d_un = rxr(rxr(rxr(d0, paren_rx, "$1"), paren_rx, "$1"), paren_rx, "$1")
        num_rx = "^[+\\-]?\\d+(\\.\\d+)?$"
        func_equiv_fast = (
            (s_un.isin("CURRENT_TIMESTAMP", "CURRENT TIMESTAMP", "SYSDATE", "SYSDATETIME()", "GETDATE()", "GETUTCDATE()")
             & d_un.isin("CURRENT_TIMESTAMP", "CURRENT TIMESTAMP", "SYSDATE", "SYSDATETIME()", "GETDATE()", "GETUTCDATE()"))
            | (s_un.isin("CURRENT_DATE", "CURRENT_DATE()") & d_un.isin("CURRENT_DATE", "CURRENT_DATE()"))
            | (s_un.isin("CURRENT TIME", "CURRENT_TIME()") & d_un.isin("CURRENT TIME", "CURRENT_TIME()"))
        )
        missing_vs_numeric_fast = ((s_un == lit("")) & d_un.rlike(num_rx)) | ((d_un == lit("")) & s_un.rlike(num_rx))

        dr0 = coalesce(upper(trim(col("DestinationDefault"))), lit(""))
        dr_un = rxr(rxr(rxr(dr0, paren_rx, "$1"), paren_rx, "$1"), paren_rx, "$1")
        dr_un_noq = rxr(dr_un, "^'(.*)'$", "$1")
        dest_paren_present_raw = dr_un != dr0
        s0_raw = coalesce(upper(trim(col("SourceDefaultCanon"))), lit(""))
        paren_only_wrap_warn = (dest_paren_present_raw & (dr_un_noq == s0_raw) & (s0_raw != coalesce(upper(trim(col("DestinationDefaultCanon"))), lit(""))))
        bracket_equiv_fast = (s_un == d_un) & (s0 != d0)

        cs = coalesce(col("SourceDefaultCanon"), lit(""))
        cd = coalesce(col("DestinationDefaultCanon"), lit(""))
        out = out.withColumn(
            "ValidationAction",
            when(cs == cd, lit("match"))
            .when(paren_only_wrap_warn & lit(bool(be_ign)), lit("ignore"))
            .when(paren_only_wrap_warn & lit(bool(be_warn)), lit("warning"))
            .when(bracket_equiv_fast & lit(bool(be_ign)), lit("ignore"))
            .when(bracket_equiv_fast & lit(bool(be_warn)), lit("warning"))
            .when(func_equiv_fast & lit(bool(fe_ign)), lit("ignore"))
            .when(func_equiv_fast & lit(bool(fe_warn)), lit("warning"))
            .when(missing_vs_numeric_fast & lit(bool(mv_ign)), lit("ignore"))
            .when(missing_vs_numeric_fast & lit(bool(mv_warn)), lit("warning"))
            .otherwise(apply_rule_spark_udf(col("SourceDefaultCanon"), col("DestinationDefaultCanon"))),
        )

        out = out.where(col("ValidationAction").isin(["error", "warning"]))

        return out.select(
            lit("DefaultValue").alias("ValidationType"),
            when(col("ValidationAction") == "error", lit("error")).otherwise(lit("warning")).alias("Status"),
            lit("TABLE").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            col("ColumnName"),
            col("SourceDefault"),
            col("DestinationDefault"),
            when(col("ValidationAction") == "error", lit(False)).otherwise(lit(True)).alias("DefaultMatch"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName")), lit("."), trim(col("ColumnName"))).alias(
                "ElementPath"
            ),
            when(col("ValidationAction") == "warning", lit("Default value difference (treated as warning)")).otherwise(
                lit("Default value mismatch")
            ).alias("ErrorDescription"),
        )

    @log_duration("compare_index_definitions")
    def compare_index_definitions(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        if not object_types:
            object_types = ["TABLE"]
        l = self._read_tables("db2", object_types, source_schema)
        r = self._read_tables("azure", object_types, target_schema)
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
        db2_ix = self._build_index_metadata("db2", s_schemas)
        az_ix = self._build_index_metadata("azure", r_schemas)

        try:
            many_col_threshold = int(os.environ.get("DV_MANY_COLUMNS_THRESHOLD", "120"))
        except Exception:
            many_col_threshold = 120

        l_cols = self._fetch_columns_bulk("db2", s_schemas)
        r_cols = self._fetch_columns_bulk("azure", r_schemas)
        l_cnt = (
            l_cols.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .groupBy("schema_norm", "table_norm")
            .count()
            .withColumnRenamed("count", "SourceColumnCount")
        )
        r_cnt = (
            r_cols.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .groupBy("schema_norm", "table_norm")
            .count()
            .withColumnRenamed("count", "DestinationColumnCount")
        )

        db2_f = db2_ix.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        az_f = az_ix.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )

        def agg_sig(df: DataFrame) -> DataFrame:
            return (
                df.groupBy("schema_norm", "table_norm", "idx_norm", "Kind", "IsUnique")
                .agg(concat_ws(",", collect_set(concat(col("col_name"), lit(" "), col("ord")))).alias("ColsSig"))
            )

        db2_sig = agg_sig(db2_f.select("schema_norm", "table_norm", "idx_norm", "Kind", "IsUnique", "colseq", "col_name", col("col_order").alias("ord")))
        az_sig = agg_sig(az_f.select("schema_norm", "table_norm", "idx_norm", "Kind", "IsUnique", "colseq", "col_name", col("col_order").alias("ord")))

        joined = db2_sig.alias("l").join(
            az_sig.alias("r"),
            on=[
                col("l.schema_norm") == col("r.schema_norm"),
                col("l.table_norm") == col("r.table_norm"),
                col("l.idx_norm") == col("r.idx_norm"),
                col("l.Kind") == col("r.Kind"),
            ],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.table_norm"), col("r.table_norm")).alias("table_norm"),
            coalesce(col("l.idx_norm"), col("r.idx_norm")).alias("IndexName"),
            coalesce(col("l.Kind"), col("r.Kind")).alias("Kind"),
            col("l.IsUnique").alias("SourceUnique"),
            col("r.IsUnique").alias("DestinationUnique"),
            col("l.ColsSig").alias("SourceCols"),
            col("r.ColsSig").alias("DestinationCols"),
        )

        sig_pairs = (
            db2_sig.select("schema_norm", "table_norm", "Kind", "IsUnique", "ColsSig", col("idx_norm").alias("l_idx"))
            .alias("l")
            .join(
                az_sig.select("schema_norm", "table_norm", "Kind", "IsUnique", "ColsSig", col("idx_norm").alias("r_idx")).alias("r"),
                on=[
                    col("l.schema_norm") == col("r.schema_norm"),
                    col("l.table_norm") == col("r.table_norm"),
                    col("l.Kind") == col("r.Kind"),
                    col("l.IsUnique") == col("r.IsUnique"),
                    col("l.ColsSig") == col("r.ColsSig"),
                ],
                how="inner",
            )
            .select(col("l.schema_norm").alias("schema_norm"), col("l.table_norm").alias("table_norm"), col("l.l_idx").alias("l_idx"), col("r.r_idx").alias("r_idx"))
        )

        out = joined.join(
            pairs.select(
                "s_schema_norm",
                "s_object_norm",
                "SourceSchemaName",
                "SourceObjectName",
                "DestinationSchemaName",
                "DestinationObjectName",
            ),
            on=[joined.schema_norm == col("s_schema_norm"), joined.table_norm == col("s_object_norm")],
            how="left",
        )

        out = (
            out.withColumn("MissingInSource", col("SourceCols").isNull())
            .withColumn("MissingInTarget", col("DestinationCols").isNull())
            .withColumn("ColsMatch", (col("SourceCols") == col("DestinationCols")))
            .withColumn("UniqueMatch", (coalesce(col("SourceUnique"), lit(False)) == coalesce(col("DestinationUnique"), lit(False))))
        )

        left_mask = out.join(
            sig_pairs.alias("sp"),
            on=[out.schema_norm == col("sp.schema_norm"), out.table_norm == col("sp.table_norm"), out.IndexName == col("sp.l_idx")],
            how="left",
        ).select(out["*"], col("sp.r_idx").isNotNull().alias("HasSigRight"))
        both_mask = left_mask.join(
            sig_pairs.alias("sp2"),
            on=[left_mask.schema_norm == col("sp2.schema_norm"), left_mask.table_norm == col("sp2.table_norm"), left_mask.IndexName == col("sp2.r_idx")],
            how="left",
        ).select(left_mask["*"], col("sp2.l_idx").isNotNull().alias("HasSigLeft"))

        # Carry signature-presence flags to mask out symmetric matches
        masked = both_mask.withColumn(
            "MaskBySig",
            when(col("MissingInTarget") & col("HasSigRight"), lit(True))
            .when(col("MissingInSource") & col("HasSigLeft"), lit(True))
            .otherwise(lit(False)),
        )

        out = masked.where(
            ((col("MissingInSource")) | (col("MissingInTarget")) | (~col("ColsMatch")) | (~col("UniqueMatch"))) & (~col("MaskBySig"))
        )

        idx_rules = self._parse_validation_rules("DV_INDEX_RULES")
        order_insensitive_warn = any(
            (str(r.get("rule_type", "")).lower() == "column_order_insensitive") and (str(r.get("match_type", "")).lower() == "warning")
            for r in idx_rules
        )
        missing_sysname_warn = any(
            (str(r.get("rule_type", "")).lower() == "missing_sysname") and (str(r.get("match_type", "")).lower() == "warning")
            for r in idx_rules
        )

        s_cols_arr = array_sort(transform(split(coalesce(col("SourceCols"), lit("")), ","), lambda x: upper(trim(regexp_replace(x, r"\s+", "")))))
        d_cols_arr = array_sort(transform(split(coalesce(col("DestinationCols"), lit("")), ","), lambda x: upper(trim(regexp_replace(x, r"\s+", "")))))
        out = out.withColumn("ColsSetEqual", s_cols_arr == d_cols_arr)
        warn_order = ((~col("ColsMatch")) & col("ColsSetEqual") & lit(bool(order_insensitive_warn)))
        idx_name_uc = upper(coalesce(col("IndexName"), lit("")))
        warn_sysname = (
            ((col("MissingInTarget") | col("MissingInSource"))
             & (idx_name_uc.startswith(lit("SQL")) | idx_name_uc.startswith(lit("PK")))
             & lit(bool(missing_sysname_warn)))
        )
        out = out.withColumn("Status", when(warn_order | warn_sysname, lit("warning")).otherwise(lit("error")))

        result_idx = out.select(
            lit("Index").alias("ValidationType"),
            col("Kind").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            col("IndexName"),
            col("SourceUnique"),
            col("DestinationUnique"),
            col("SourceCols"),
            col("DestinationCols"),
            col("Status"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName")), lit("."), trim(col("IndexName"))).alias("ElementPath"),
            when(col("MissingInSource"), lit("Index missing in source"))
            .when(col("MissingInTarget"), lit("Index missing in target"))
            .when((~col("ColsMatch")) & col("ColsSetEqual"), lit("Index column order mismatch"))
            .when(~col("ColsMatch"), lit("Index columns mismatch"))
            .when(~col("UniqueMatch"), lit("Index uniqueness mismatch"))
            .otherwise(lit(""))
            .alias("ErrorDescription"),
        )

        db2_pk = db2_ix.where(col("Kind") == lit("PK")).select("schema_norm", "table_norm").distinct()
        az_pk = az_ix.where(col("Kind") == lit("PK")).select("schema_norm", "table_norm").distinct()

        pk_presence = (
            pairs.select(
                col("SourceSchemaName"),
                col("SourceObjectName"),
                col("DestinationSchemaName"),
                col("DestinationObjectName"),
                col("s_schema_norm").alias("schema_norm"),
                col("s_object_norm").alias("table_norm"),
            )
            .join(
                db2_pk.withColumnRenamed("schema_norm", "pk_schema").withColumnRenamed("table_norm", "pk_table"),
                on=[col("schema_norm") == col("pk_schema"), col("table_norm") == col("pk_table")],
                how="left",
            )
            .withColumn("HasPkSource", col("pk_schema").isNotNull())
            .drop("pk_schema", "pk_table")
            .join(
                az_pk.withColumnRenamed("schema_norm", "pk_schema").withColumnRenamed("table_norm", "pk_table"),
                on=[col("schema_norm") == col("pk_schema"), col("table_norm") == col("pk_table")],
                how="left",
            )
            .withColumn("HasPkTarget", col("pk_schema").isNotNull())
            .drop("pk_schema", "pk_table")
        )

        pk_missing = pk_presence.where(col("HasPkSource") != col("HasPkTarget")).select(
            lit("PrimaryKey").alias("ValidationType"),
            lit("PK").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            lit("PRIMARY KEY").alias("IndexName"),
            lit(True).alias("SourceUnique"),
            lit(True).alias("DestinationUnique"),
            lit("").alias("SourceCols"),
            lit("").alias("DestinationCols"),
            lit("error").alias("Status"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName")), lit("."), lit("PRIMARY KEY")).alias("ElementPath"),
            when(col("HasPkSource") & ~col("HasPkTarget"), lit("Primary key missing in target"))
            .when(~col("HasPkSource") & col("HasPkTarget"), lit("Primary key missing in source"))
            .otherwise(lit("Primary key presence mismatch"))
            .alias("ErrorDescription"),
        )

        pk_absent_both = pk_presence.where(~col("HasPkSource") & ~col("HasPkTarget")).select(
            lit("PrimaryKey").alias("ValidationType"),
            lit("PK").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            lit("PRIMARY KEY").alias("IndexName"),
            lit(None).cast("boolean").alias("SourceUnique"),
            lit(None).cast("boolean").alias("DestinationUnique"),
            lit("").alias("SourceCols"),
            lit("").alias("DestinationCols"),
            lit("info").alias("Status"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName")), lit("."), lit("PRIMARY KEY")).alias("ElementPath"),
            lit("Table has no primary key on either side (note)").alias("ErrorDescription"),
        )

        many_cols = (
            pairs.select(
                col("SourceSchemaName"),
                col("SourceObjectName"),
                col("DestinationSchemaName"),
                col("DestinationObjectName"),
                col("s_schema_norm").alias("schema_norm"),
                col("s_object_norm").alias("table_norm"),
            )
            .join(l_cnt, on=["schema_norm", "table_norm"], how="left")
            .join(r_cnt, on=["schema_norm", "table_norm"], how="left")
        )
        many_cols = (
            many_cols.withColumn("SourceColumnCount", coalesce(col("SourceColumnCount"), lit(0).cast("long")))
            .withColumn("DestinationColumnCount", coalesce(col("DestinationColumnCount"), lit(0).cast("long")))
            .withColumn(
                "MaxColumns",
                when(col("SourceColumnCount") > col("DestinationColumnCount"), col("SourceColumnCount")).otherwise(
                    col("DestinationColumnCount")
                ),
            )
            .where(col("MaxColumns") >= lit(many_col_threshold))
        )
        many_cols = many_cols.select(
            lit("TableSize").alias("ValidationType"),
            lit("TABLE").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            lit("").alias("IndexName"),
            lit(None).cast("boolean").alias("SourceUnique"),
            lit(None).cast("boolean").alias("DestinationUnique"),
            col("SourceColumnCount").cast("string").alias("SourceCols"),
            col("DestinationColumnCount").cast("string").alias("DestinationCols"),
            lit("info").alias("Status"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName"))).alias("ElementPath"),
            concat(lit("High column count (>="), lit(many_col_threshold).cast("string"), lit(")")).alias("ErrorDescription"),
        )

        return (
            result_idx.unionByName(pk_missing, allowMissingColumns=True)
            .unionByName(pk_absent_both, allowMissingColumns=True)
            .unionByName(many_cols, allowMissingColumns=True)
        )


    @log_duration("compare_foreign_keys")
    def compare_foreign_keys(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        if not object_types:
            object_types = ["TABLE"]
        l = self._read_tables("db2", object_types, source_schema)
        r = self._read_tables("azure", object_types, target_schema)
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

        _normalize_fk_actions = os.environ.get("DV_FK_ACTION_NORMALIZE", "0").strip().lower() in ("1", "true", "yes")
        if _normalize_fk_actions:
            db2_fk = db2_fk.withColumn("DeleteAction", upper(trim(regexp_replace(col("DeleteAction"), "_", " ")))) \
                           .withColumn("UpdateAction", upper(trim(regexp_replace(col("UpdateAction"), "_", " "))))
            az_fk = az_fk.withColumn("DeleteAction", upper(trim(regexp_replace(col("DeleteAction"), "_", " ")))) \
                         .withColumn("UpdateAction", upper(trim(regexp_replace(col("UpdateAction"), "_", " "))))

        def _canonical_fk_action(c):
            cleaned = upper(trim(regexp_replace(coalesce(c, lit("")), "_", " ")))
            return (
                when(cleaned.isin("A", "R", "NO ACTION"), lit("NO ACTION"))
                .when(cleaned.isin("C", "CASCADE"), lit("CASCADE"))
                .when(cleaned.isin("N", "SET NULL"), lit("SET NULL"))
                .otherwise(cleaned)
            )

        db2_fk = db2_fk.withColumn("DeleteActionNorm", _canonical_fk_action(col("DeleteAction"))) \
                       .withColumn("UpdateActionNorm", _canonical_fk_action(col("UpdateAction")))
        az_fk = az_fk.withColumn("DeleteActionNorm", _canonical_fk_action(col("DeleteAction"))) \
                     .withColumn("UpdateActionNorm", _canonical_fk_action(col("UpdateAction")))

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

        db2_sig = (
            db2_f.groupBy(
                "schema_norm",
                "table_norm",
                "fk_norm",
                "ref_schema_norm",
                "ref_table_norm",
                "DeleteAction",
                "UpdateAction",
                "DeleteActionNorm",
                "UpdateActionNorm",
            )
            .agg(sort_array(collect_set(concat(col("col_name"), lit("->"), col("ref_col_name")))).alias("ColPairsArr"))
            .withColumn("ColPairs", concat_ws(",", col("ColPairsArr")))
        )
        az_sig = (
            az_f.groupBy(
                "schema_norm",
                "table_norm",
                "fk_norm",
                "ref_schema_norm",
                "ref_table_norm",
                "DeleteAction",
                "UpdateAction",
                "DeleteActionNorm",
                "UpdateActionNorm",
            )
            .agg(sort_array(collect_set(concat(col("col_name"), lit("->"), col("ref_col_name")))).alias("ColPairsArr"))
            .withColumn("ColPairs", concat_ws(",", col("ColPairsArr")))
        )

        joined = db2_sig.alias("l").join(
            az_sig.alias("r"),
            on=[
                col("l.schema_norm") == col("r.schema_norm"),
                col("l.table_norm") == col("r.table_norm"),
                col("l.fk_norm") == col("r.fk_norm"),
            ],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.table_norm"), col("r.table_norm")).alias("table_norm"),
            coalesce(col("l.fk_norm"), col("r.fk_norm")).alias("FkName"),
            col("l.ref_schema_norm").alias("SourceRefSchema"),
            col("l.ref_table_norm").alias("SourceRefTable"),
            col("r.ref_schema_norm").alias("DestinationRefSchema"),
            col("r.ref_table_norm").alias("DestinationRefTable"),
            col("l.DeleteAction").alias("SourceDelete"),
            col("r.DeleteAction").alias("DestinationDelete"),
            col("l.DeleteActionNorm").alias("SourceDeleteNorm"),
            col("r.DeleteActionNorm").alias("DestinationDeleteNorm"),
            col("l.UpdateAction").alias("SourceUpdate"),
            col("r.UpdateAction").alias("DestinationUpdate"),
            col("l.UpdateActionNorm").alias("SourceUpdateNorm"),
            col("r.UpdateActionNorm").alias("DestinationUpdateNorm"),
            col("l.ColPairs").alias("SourcePairs"),
            col("r.ColPairs").alias("DestinationPairs"),
        )

        sig_pairs = (
            db2_sig.select(
                "schema_norm",
                "table_norm",
                "ref_schema_norm",
                "ref_table_norm",
                "DeleteAction",
                "UpdateAction",
                "DeleteActionNorm",
                "UpdateActionNorm",
                "ColPairs",
                col("fk_norm").alias("l_fk"),
            )
            .alias("l")
            .join(
                az_sig.select(
                    "schema_norm",
                    "table_norm",
                    "ref_schema_norm",
                    "ref_table_norm",
                    "DeleteAction",
                    "UpdateAction",
                    "DeleteActionNorm",
                    "UpdateActionNorm",
                    "ColPairs",
                    col("fk_norm").alias("r_fk"),
                ).alias("r"),
                on=[
                    col("l.schema_norm") == col("r.schema_norm"),
                    col("l.table_norm") == col("r.table_norm"),
                    col("l.ref_schema_norm") == col("r.ref_schema_norm"),
                    col("l.ref_table_norm") == col("r.ref_table_norm"),
                    col("l.DeleteActionNorm") == col("r.DeleteActionNorm"),
                    col("l.UpdateActionNorm") == col("r.UpdateActionNorm"),
                    col("l.ColPairs") == col("r.ColPairs"),
                ],
                how="inner",
            )
            .select(
                col("l.schema_norm").alias("schema_norm"),
                col("l.table_norm").alias("table_norm"),
                col("l.l_fk").alias("l_fk"),
                col("r.r_fk").alias("r_fk"),
            )
        )

        left_mask = joined.join(
            sig_pairs.alias("sp"),
            on=[joined.schema_norm == col("sp.schema_norm"), joined.table_norm == col("sp.table_norm"), joined.FkName == col("sp.l_fk")],
            how="left",
        ).select(joined["*"], col("sp.r_fk").isNotNull().alias("HasSigRight"))
        both_mask = left_mask.join(
            sig_pairs.alias("sp2"),
            on=[left_mask.schema_norm == col("sp2.schema_norm"), left_mask.table_norm == col("sp2.table_norm"), left_mask.FkName == col("sp2.r_fk")],
            how="left",
        ).select(left_mask["*"], col("sp2.l_fk").isNotNull().alias("HasSigLeft"))

        out = both_mask.withColumn("MissingInSource", col("SourcePairs").isNull()) \
                       .withColumn("MissingInTarget", col("DestinationPairs").isNull()) \
                       .withColumn("PairsMatch", (col("SourcePairs") == col("DestinationPairs"))) \
                       .withColumn("RefTableMatch", (coalesce(col("SourceRefSchema"), lit("")) == coalesce(col("DestinationRefSchema"), lit(""))) & (coalesce(col("SourceRefTable"), lit("")) == coalesce(col("DestinationRefTable"), lit("")))) \
                       .withColumn("DeleteMatch", (coalesce(col("SourceDelete"), lit("")) == coalesce(col("DestinationDelete"), lit("")))) \
                       .withColumn("UpdateMatch", (coalesce(col("SourceUpdate"), lit("")) == coalesce(col("DestinationUpdate"), lit("")))) \
                       .withColumn("MaskBySig", when(col("MissingInTarget") & col("HasSigRight"), lit(True)).when(col("MissingInSource") & col("HasSigLeft"), lit(True)).otherwise(lit(False)))

        out = out.where(((col("MissingInSource")) | (col("MissingInTarget")) | (~col("PairsMatch")) | (~col("RefTableMatch")) | (~col("DeleteMatch")) | (~col("UpdateMatch"))) & (~col("MaskBySig")))

        fk_rules = self._parse_validation_rules("DV_FK_RULES")
        warn_enabled = any(
            (str(r.get("rule_type", "")).lower() == "action_equivalent") and (str(r.get("match_type", "")).lower() == "warning")
            for r in fk_rules
        )
        out = out.withColumn("DeleteEquivalent", col("SourceDeleteNorm") == col("DestinationDeleteNorm")) \
                 .withColumn("UpdateEquivalent", col("SourceUpdateNorm") == col("DestinationUpdateNorm"))

        base_error = (col("MissingInSource") | col("MissingInTarget") | (~col("PairsMatch")) | (~col("RefTableMatch")))
        del_error = ((~col("DeleteMatch")) & (~(col("DeleteEquivalent") & lit(bool(warn_enabled)))))
        upd_error = ((~col("UpdateMatch")) & (~(col("UpdateEquivalent") & lit(bool(warn_enabled)))))
        any_error = (base_error | del_error | upd_error)
        del_warn_only = ((~col("DeleteMatch")) & (col("DeleteEquivalent") & lit(bool(warn_enabled))))
        upd_warn_only = ((~col("UpdateMatch")) & (col("UpdateEquivalent") & lit(bool(warn_enabled))))
        any_warn = (~any_error) & (del_warn_only | upd_warn_only)
        out = out.withColumn("Status", when(any_error, lit("error")).when(any_warn, lit("warning")).otherwise(lit("")))

        out2 = out.join(
            pairs.select("s_schema_norm", "s_object_norm", "SourceSchemaName", "SourceObjectName", "DestinationSchemaName", "DestinationObjectName"),
            on=[out.schema_norm == col("s_schema_norm"), out.table_norm == col("s_object_norm")],
            how="left",
        )

        return out2.select(
            lit("ForeignKey").alias("ValidationType"),
            lit("TABLE").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            col("FkName"),
            col("SourceRefSchema"),
            col("SourceRefTable"),
            col("DestinationRefSchema"),
            col("DestinationRefTable"),
            col("SourcePairs"),
            col("DestinationPairs"),
            col("SourceDelete"),
            col("DestinationDelete"),
            col("SourceUpdate"),
            col("DestinationUpdate"),
            col("Status"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName")), lit("."), trim(col("FkName"))).alias("ElementPath"),
            when(col("MissingInSource"), lit("FK missing in source"))
            .when(col("MissingInTarget"), lit("FK missing in target"))
            .when(~col("RefTableMatch"), lit("Referenced table mismatch"))
            .when(~col("PairsMatch"), lit("FK column pairs mismatch"))
            .when(~col("DeleteMatch"), lit("Delete action mismatch"))
            .when(~col("UpdateMatch"), lit("Update action mismatch"))
            .otherwise(lit(""))
            .alias("ErrorDescription"),
        )

    @log_duration("compare_check_constraints")
    def compare_check_constraints(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        if not object_types:
            object_types = ["TABLE"]
        l = self._read_tables("db2", object_types, source_schema)
        r = self._read_tables("azure", object_types, target_schema)
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
        db2_chk = self._build_check_metadata("db2", s_schemas)
        az_chk = self._build_check_metadata("azure", r_schemas)

        def canon_def(c):
            return regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(upper(trim(c)), r"\\s+", ""),
                        r"[\\[\\]\"']", ""
                    ),
                    r"[\\(\\)]", ""
                ),
                r";+$", ""
            )

        db2_n = db2_chk.withColumn("DefCanon", canon_def(col("chk_def")))
        az_n = az_chk.withColumn("DefCanon", canon_def(col("chk_def")))

        db2_f = db2_n.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        az_f = az_n.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )

        joined = db2_f.alias("l").join(
            az_f.alias("r"),
            on=[
                col("l.schema_norm") == col("r.schema_norm"),
                col("l.table_norm") == col("r.table_norm"),
                col("l.chk_norm") == col("r.chk_norm"),
            ],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.table_norm"), col("r.table_norm")).alias("table_norm"),
            coalesce(col("l.chk_norm"), col("r.chk_norm")).alias("ConstraintName"),
            col("l.chk_def").alias("SourceDefinition"),
            col("r.chk_def").alias("DestinationDefinition"),
            col("l.DefCanon").alias("SourceCanon"),
            col("r.DefCanon").alias("DestinationCanon"),
        )

        sig_pairs = (
            db2_f.select("schema_norm", "table_norm", "DefCanon", col("chk_norm").alias("l_chk"))
            .alias("l")
            .join(
                az_f.select("schema_norm", "table_norm", "DefCanon", col("chk_norm").alias("r_chk")).alias("r"),
                on=[
                    col("l.schema_norm") == col("r.schema_norm"),
                    col("l.table_norm") == col("r.table_norm"),
                    col("l.DefCanon") == col("r.DefCanon"),
                ],
                how="inner",
            )
            .select(col("l.schema_norm").alias("schema_norm"), col("l.table_norm").alias("table_norm"), col("l.l_chk").alias("l_chk"), col("r.r_chk").alias("r_chk"))
        )

        left_mask = joined.join(
            sig_pairs.alias("sp"),
            on=[joined.schema_norm == col("sp.schema_norm"), joined.table_norm == col("sp.table_norm"), joined.ConstraintName == col("sp.l_chk")],
            how="left",
        ).select(joined["*"], col("sp.r_chk").isNotNull().alias("HasSigRight"))
        both_mask = left_mask.join(
            sig_pairs.alias("sp2"),
            on=[left_mask.schema_norm == col("sp2.schema_norm"), left_mask.table_norm == col("sp2.table_norm"), left_mask.ConstraintName == col("sp2.r_chk")],
            how="left",
        ).select(left_mask["*"], col("sp2.l_chk").isNotNull().alias("HasSigLeft"))

        out = both_mask.withColumn("MissingInSource", col("SourceCanon").isNull()) \
                       .withColumn("MissingInTarget", col("DestinationCanon").isNull()) \
                       .withColumn("DefMatch", coalesce(col("SourceCanon"), lit("")) == coalesce(col("DestinationCanon"), lit(""))) \
                       .withColumn("MaskBySig", when(col("MissingInTarget") & col("HasSigRight"), lit(True)).when(col("MissingInSource") & col("HasSigLeft"), lit(True)).otherwise(lit(False)))

        out = out.where(((col("MissingInSource")) | (col("MissingInTarget")) | (~col("DefMatch"))) & (~col("MaskBySig")))

        out2 = out.join(
            pairs.select("s_schema_norm", "s_object_norm", "SourceSchemaName", "SourceObjectName", "DestinationSchemaName", "DestinationObjectName"),
            on=[out.schema_norm == col("s_schema_norm"), out.table_norm == col("s_object_norm")],
            how="left",
        )

        return out2.select(
            lit("TABLE").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            col("ConstraintName"),
            col("SourceDefinition"),
            col("DestinationDefinition"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName")), lit("."), trim(col("ConstraintName"))).alias("ElementPath"),
            when(col("MissingInSource"), lit("Check constraint missing in source"))
            .when(col("MissingInTarget"), lit("Check constraint missing in target"))
            .when(~col("DefMatch"), lit("Check constraint definition mismatch"))
            .otherwise(lit(""))
            .alias("ErrorDescription"),
        )

    @log_duration("compare_column_nullable_constraints")
    def compare_column_nullable_constraints(
        self, source_schema: str | None, target_schema: str | None, object_types: list[str] | None
    ) -> DataFrame:
        if not object_types:
            object_types = ["TABLE"]
        l = self._read_tables("db2", object_types, source_schema)
        r = self._read_tables("azure", object_types, target_schema)
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
        db2_cols = self._fetch_columns_nullable_bulk("db2", s_schemas)
        az_cols = self._fetch_columns_nullable_bulk("azure", r_schemas)

        nullable_source_expr = (
            when(col("is_nullable_str").isNull(), lit(None))
            .when(upper(trim(col("is_nullable_str"))).isin("NO", "N"), lit(False))
            .otherwise(lit(True))
        )
        nullable_target_expr = (
            when(col("is_nullable_str").isNull(), lit(None))
            .when(upper(trim(col("is_nullable_str"))).isin("NO", "N"), lit(False))
            .otherwise(lit(True))
        )

        db2_f = (
            db2_cols.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .withColumn("NullableSource", nullable_source_expr)
        )
        az_f = (
            az_cols.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .withColumn("NullableTarget", nullable_target_expr)
        )

        joined = db2_f.alias("l").join(
            az_f.alias("r"),
            on=[
                col("l.schema_norm") == col("r.schema_norm"),
                col("l.table_norm") == col("r.table_norm"),
                col("l.col_norm") == col("r.col_norm"),
            ],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.table_norm"), col("r.table_norm")).alias("table_norm"),
            coalesce(col("l.col_norm"), col("r.col_norm")).alias("ColumnName"),
            col("l.NullableSource").alias("NullableSource"),
            col("r.NullableTarget").alias("NullableTarget"),
        )

        out = joined.join(
            pairs.select("s_schema_norm", "s_object_norm", "SourceSchemaName", "SourceObjectName", "DestinationSchemaName", "DestinationObjectName"),
            on=[joined.schema_norm == col("s_schema_norm"), joined.table_norm == col("s_object_norm")],
            how="inner",
        )

        out = out.withColumn("MissingInSource", col("NullableSource").isNull()) \
                 .withColumn("MissingInTarget", col("NullableTarget").isNull()) \
                 .withColumn("NullableMatch", (coalesce(col("NullableSource"), lit(True)) == coalesce(col("NullableTarget"), lit(True))))

        out = out.where((col("MissingInSource")) | (col("MissingInTarget")) | (~col("NullableMatch")))

        return out.select(
            lit("Nullable").alias("ValidationType"),
            lit("TABLE").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            col("ColumnName"),
            col("NullableSource").alias("SourceNullable"),
            col("NullableTarget").alias("DestinationNullable"),
            concat(trim(col("SourceSchemaName")), lit("."), trim(col("SourceObjectName")), lit("."), trim(col("ColumnName"))).alias("ElementPath"),
            when(col("MissingInSource"), lit("Column missing in source"))
            .when(col("MissingInTarget"), lit("Column missing in target"))
            .when(~col("NullableMatch"), lit("Nullable constraint mismatch"))
            .otherwise(lit(""))
            .alias("ErrorDescription"),
        )

    @log_duration("validate_schema_consistency")
    def validate_schema_consistency(self, comparison_df: DataFrame) -> Dict[str, Any]:
        """Validate schema consistency using PySpark operations."""
        validation_results: Dict[str, Any] = {}

        naming_issues = comparison_df.filter(
            (col("source_object") != col("target_object"))
            & (col("status") == "MATCH")
        ).count()
        validation_results["naming_inconsistencies"] = naming_issues

        case_issues = comparison_df.filter(
            (col("source_object").rlike(".*[a-z].*"))
            & (col("target_object").rlike(".*[A-Z].*"))
            & (col("status") == "MATCH")
        ).count()
        validation_results["case_sensitivity_issues"] = case_issues

        critical_objects: List[str] = ["TABLE", "VIEW", "PROCEDURE", "FUNCTION"]
        missing_critical: List[str] = []
        for obj_type in critical_objects:
            count = comparison_df.filter(
                (col("object_type") == obj_type)
                & (col("status") == "MISSING_IN_TARGET")
            ).count()
            if count > 0:
                missing_critical.append(f"{obj_type}: {count} missing")

        validation_results["missing_critical_objects"] = missing_critical
        return validation_results

    @log_duration("save_comparison_to_csv")
    def save_comparison_to_csv(self, comparison_df: DataFrame, filename_prefix: str) -> str:
        """Save schema comparison results to CSV using PySpark."""
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


class PySparkSchemaValidationAzureService(PySparkSchemaValidationService):
    """Schema validations for Azure to Azure (structure only)."""

    def __init__(self, *, access_token_override: Optional[str] = None):
        super().__init__(
            config_filename="azure_database_config.json",
            side_config_map={"db2": "source_azure_sql", "azure": "target_azure_sql"},
            side_labels={"db2": "Source Azure SQL", "azure": "Target Azure SQL"},
            side_engine_map={"db2": "azure", "azure": "azure"},
            app_name="Azure_Azure_Schema_Validation",
            access_token_override=access_token_override,
        )

    @log_duration("save_comparison_to_csv")
    def save_comparison_to_csv(self, comparison_df: DataFrame, filename_prefix: str) -> str:
        """Save schema comparison results to CSV using PySpark."""
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

