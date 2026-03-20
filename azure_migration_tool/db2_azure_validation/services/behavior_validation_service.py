# Author: S@tish Chauhan

from typing import Optional, List
import os
import json
import uuid
from datetime import datetime
import pandas as pd

from pyspark.sql.functions import (
    col,
    when,
    lit,
    upper,
    trim,
    coalesce,
    concat,
    to_json,
    struct,
    regexp_replace,
    array,
    array_remove,
    explode,
)

from db2_azure_validation.services.pyspark_schema_comparison import (
    PySparkSchemaComparisonService,
    PySparkAzureSchemaComparisonService,
    log_duration,
)
from db2_azure_validation.schemas.common import ensure_all_columns_as_strings, get_unified_columns


class PySparkBehaviorValidationService(PySparkSchemaComparisonService):
    """Behavior/readiness validations for DB2 to Azure."""

    @staticmethod
    def _default_fetchsize() -> str:
        return os.environ.get("JDBC_FETCHSIZE", "50000")

    def _fetch_identity_metadata(self, side: str, schemas: Optional[List[str]] | None) -> "DataFrame":
        creds = self._build_jdbc_urls()[side]
        engine = self._side_engine(side)
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{(str(s) or '').upper()}'" for s in schemas])
            where_schema = f" WHERE UPPER(schema_name) IN ({schema_list})"
        if engine == "azure":
            q = (
                "(SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) AS table_name, RTRIM(c.name) AS column_name, "
                " ic.is_identity AS is_identity, "
                " CAST(ic.seed_value AS NVARCHAR(128)) AS seed_value, "
                " CAST(ic.increment_value AS NVARCHAR(128)) AS increment_value, "
                " CAST(ic.last_value AS NVARCHAR(128)) AS last_value "
                " FROM sys.identity_columns ic "
                " JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id "
                " JOIN sys.tables t ON t.object_id=c.object_id "
                " JOIN sys.schemas s ON s.schema_id=t.schema_id "
                " WHERE ic.is_identity=1) x"
            )
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
                .option("customSchema", "schema_name string, table_name string, column_name string, is_identity boolean, seed_value string, increment_value string, last_value string")
            )
            df = self._apply_jdbc_auth(df, creds).load()
        else:
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{(str(s) or '').upper()}'" for s in schemas])
                where_schema = f" WHERE UPPER(tabschema) IN ({schema_list})"

            def _load_query(qstr: str):
                reader = (
                    self.spark.read.format("jdbc")
                    .option("url", creds["url"])
                    .option("dbtable", qstr)
                    .option("driver", creds["driver"])
                    .option("fetchsize", self._default_fetchsize())
                )
                return self._apply_jdbc_auth(reader, creds).load()

            # Prefer COLIDENTITIES for full metadata when available; otherwise fall back to SYSCAT.COLUMNS with limited fields.
            colidentities_q = (
                "("
                " SELECT RTRIM(tabschema) AS schema_name, RTRIM(tabname) AS table_name, RTRIM(colname) AS column_name, "
                "        CAST(1 AS SMALLINT) AS is_identity, "
                "        identitystart AS seed_value, "
                "        identityincrement AS increment_value, "
                "        CAST(NULL AS BIGINT) AS last_value "
                " FROM syscat.colidentities"
                f"{where_schema}"
                ") x"
            )
            try:
                df = _load_query(colidentities_q)
            except Exception as ex:
                print(f"[_fetch_identity_metadata][{side}] COLIDENTITIES unavailable, falling back. Reason: {ex}")
                columns_q = (
                    "("
                    " SELECT RTRIM(tabschema) AS schema_name, RTRIM(tabname) AS table_name, RTRIM(colname) AS column_name, "
                    "        CASE WHEN identity = 'Y' THEN 1 ELSE 0 END AS is_identity, "
                    "        CAST(NULL AS BIGINT) AS seed_value, "
                    "        CAST(NULL AS BIGINT) AS increment_value, "
                    "        CAST(NULL AS BIGINT) AS last_value "
                    " FROM syscat.columns"
                    f"{where_schema}"
                    ") x"
                )
                df = _load_query(columns_q)
        return df.select(
            trim(col("schema_name")).alias("schema_name"),
            trim(col("table_name")).alias("table_name"),
            trim(col("column_name")).alias("column_name"),
            col("is_identity").cast("boolean").alias("is_identity"),
            col("seed_value").cast("string").alias("seed_value"),
            col("increment_value").cast("string").alias("increment_value"),
            col("last_value").cast("string").alias("last_value"),
        )

    def _fetch_sequence_metadata(self, side: str, schemas: Optional[List[str]] | None) -> "DataFrame":
        creds = self._build_jdbc_urls()[side]
        engine = self._side_engine(side)
        where_schema = ""
        if engine == "azure":
            if schemas:
                schema_list = ",".join([f"'{(str(s) or '').upper()}'" for s in schemas])
                where_schema = f" WHERE UPPER(SCHEMA_NAME(schema_id)) IN ({schema_list})"
            else:
                where_schema = " WHERE UPPER(SCHEMA_NAME(schema_id)) NOT IN ('SYS','INFORMATION_SCHEMA')"
            q = (
                "(SELECT RTRIM(SCHEMA_NAME(schema_id)) AS schema_name, RTRIM(name) AS sequence_name, "
                " CAST(start_value AS NVARCHAR(128)) AS start_value, "
                " CAST(increment AS NVARCHAR(128)) AS increment, "
                " CAST(minimum_value AS NVARCHAR(128)) AS minimum_value, "
                " CAST(maximum_value AS NVARCHAR(128)) AS maximum_value, "
                " is_cycling, "
                " CAST(cache_size AS NVARCHAR(128)) AS cache_size "
                f" FROM sys.sequences{where_schema}) x"
            )
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
                .option(
                    "customSchema",
                    "schema_name string, sequence_name string, start_value string, increment string, minimum_value string, maximum_value string, is_cycling boolean, cache_size string",
                )
            )
            df = self._apply_jdbc_auth(df, creds).load()
        else:
            if schemas:
                schema_list = ",".join([f"'{(str(s) or '').upper()}'" for s in schemas])
                where_schema = f" WHERE UPPER(seqschema) IN ({schema_list})"
            else:
                where_schema = " WHERE UPPER(seqschema) NOT IN ('SYSIBM','SYSCAT','SYSSTAT')"
            q = (
                "(SELECT RTRIM(seqschema) AS schema_name, RTRIM(seqname) AS sequence_name, "
                " CAST(start AS VARCHAR(64)) AS start_value, "
                " CAST(increment AS VARCHAR(64)) AS increment, "
                " CAST(minvalue AS VARCHAR(64)) AS minimum_value, "
                " CAST(maxvalue AS VARCHAR(64)) AS maximum_value, "
                " CASE WHEN cycle = 'Y' THEN 1 ELSE 0 END AS is_cycling, "
                " CAST(cache AS VARCHAR(64)) AS cache_size "
                f" FROM syscat.sequences{where_schema}) x"
            )
        if engine != "azure":
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
                .option(
                    "customSchema",
                    "schema_name string, sequence_name string, start_value string, increment string, minimum_value string, maximum_value string, is_cycling boolean, cache_size string",
                )
            )
            df = self._apply_jdbc_auth(df, creds).load()
        return df.select(
            trim(col("schema_name")).alias("schema_name"),
            trim(col("sequence_name")).alias("sequence_name"),
            col("start_value").cast("string").alias("start_value"),
            col("increment").cast("string").alias("increment"),
            col("minimum_value").cast("string").alias("minimum_value"),
            col("maximum_value").cast("string").alias("maximum_value"),
            col("is_cycling").cast("boolean").alias("is_cycling"),
            col("cache_size").cast("string").alias("cache_size"),
        )

    def _fetch_collation_metadata(self, side: str, schemas: Optional[List[str]] | None) -> "DataFrame":
        creds = self._build_jdbc_urls()[side]
        engine = self._side_engine(side)
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{(str(s) or '').upper()}'" for s in schemas])
            if engine == "azure":
                where_schema = f" WHERE UPPER(s.name) IN ({schema_list})"
            else:
                where_schema = f" WHERE UPPER(tabschema) IN ({schema_list})"
        if engine == "azure":
            q = (
                "(SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) AS table_name, RTRIM(c.name) AS column_name, "
                "        CAST(c.collation_name AS NVARCHAR(256)) AS collation_name "
                " FROM sys.columns c "
                " JOIN sys.tables t ON t.object_id=c.object_id "
                " JOIN sys.schemas s ON s.schema_id=t.schema_id) x"
            )
            if where_schema:
                q = q.replace(") x", f"{where_schema}) x")
        else:
            q = (
                "(SELECT RTRIM(tabschema) AS schema_name, RTRIM(tabname) AS table_name, RTRIM(colname) AS column_name, "
                "        CAST(NULL AS VARCHAR(128)) AS collation_name "
                " FROM syscat.columns) x"
            )
            if where_schema:
                q = q.replace(") x", f"{where_schema}) x")
        df = (
            self.spark.read.format("jdbc")
            .option("url", creds["url"])
            .option("dbtable", q)
            .option("driver", creds["driver"])
            .option("fetchsize", self._default_fetchsize())
            .option("customSchema", "schema_name string, table_name string, column_name string, collation_name string")
        )
        df = self._apply_jdbc_auth(df, creds).load()
        return df.select(
            trim(col("schema_name")).alias("schema_name"),
            trim(col("table_name")).alias("table_name"),
            trim(col("column_name")).alias("column_name"),
            trim(col("collation_name")).alias("collation_name"),
        )

    def _fetch_database_collation(self, side: str) -> str:
        try:
            creds = self._build_jdbc_urls()[side]
            engine = self._side_engine(side)
            if engine == "azure":
                q = "(SELECT CONVERT(varchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation')) AS collation_name) x"
            else:
                q = "(SELECT COLLATIONSCHEMA || '.' || COLLATIONNAME AS collation_name FROM SYSCAT.DATABASES FETCH FIRST 1 ROW ONLY) x"
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", "1000")
            )
            df = self._apply_jdbc_auth(df, creds).load()
            row = df.limit(1).collect()
            if row:
                return (row[0]["collation_name"] or "").strip()
        except Exception:
            return ""
        return ""

    def _fetch_table_privileges(self, side: str, schemas: Optional[List[str]] | None) -> "DataFrame":
        creds = self._build_jdbc_urls()[side]
        engine = self._side_engine(side)
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{(str(s) or '').upper()}'" for s in schemas])
            if engine == "azure":
                where_schema = f" AND UPPER(s.name) IN ({schema_list})"
            else:
                where_schema = f" WHERE UPPER(tabschema) IN ({schema_list})"
        if engine == "azure":
            q = (
                "SELECT RTRIM(s.name) AS schema_name, RTRIM(o.name) AS object_name, "
                " USER_NAME(dp.grantee_principal_id) AS grantee, dp.permission_name AS perm, dp.state_desc AS state_desc "
                " FROM sys.database_permissions dp "
                " JOIN sys.objects o ON dp.major_id=o.object_id "
                " JOIN sys.schemas s ON o.schema_id=s.schema_id "
                " WHERE o.type='U'"
            )
            if where_schema:
                q += where_schema
            q = f"({q}) x"
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
                .option(
                    "customSchema",
                    "schema_name string, object_name string, grantee string, perm string, state_desc string",
                )
            )
            df = self._apply_jdbc_auth(df, creds).load()
            df = (
                df.withColumn("perm_norm", upper(trim(col("perm"))))
                .withColumn("state_norm", upper(trim(col("state_desc"))))
                .withColumn("perm_final", concat(col("perm_norm"), lit(":"), col("state_norm")))
            )
            return df.select(
                trim(col("schema_name")).alias("schema_name"),
                trim(col("object_name")).alias("object_name"),
                trim(col("grantee")).alias("grantee"),
                col("perm_final").alias("perm"),
            )
        else:
            q = (
                "SELECT RTRIM(tabschema) AS schema_name, RTRIM(tabname) AS object_name, RTRIM(grantee) AS grantee, "
                " CONTROLAUTH, ALTERAUTH, DELETEAUTH, INDEXAUTH, INSERTAUTH, REFAUTH, SELECTAUTH, UPDATEAUTH "
                " FROM syscat.tabauth"
            )
            if where_schema:
                q += where_schema
            q = f"({q}) x"
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
                .option(
                    "customSchema",
                    "schema_name string, object_name string, grantee string, CONTROLAUTH string, ALTERAUTH string, DELETEAUTH string, INDEXAUTH string, INSERTAUTH string, REFAUTH string, SELECTAUTH string, UPDATEAUTH string",
                )
            )
            df = self._apply_jdbc_auth(df, creds).load()
            # Unpivot privilege columns via stack to avoid array-related iterator issues
            df = df.selectExpr(
                "schema_name",
                "object_name",
                "grantee",
                "stack(8, "
                " 'CONTROL', CONTROLAUTH, "
                " 'ALTER', ALTERAUTH, "
                " 'DELETE', DELETEAUTH, "
                " 'INDEX', INDEXAUTH, "
                " 'INSERT', INSERTAUTH, "
                " 'REFERENCES', REFAUTH, "
                " 'SELECT', SELECTAUTH, "
                " 'UPDATE', UPDATEAUTH"
                ") as (perm_name, perm_flag)"
            ).where(upper(trim(col("perm_flag"))) == lit("Y"))

            df = df.select(
                trim(col("schema_name")).alias("schema_name"),
                trim(col("object_name")).alias("object_name"),
                trim(col("grantee")).alias("grantee"),
                concat(upper(trim(col("perm_name"))), lit(":GRANT")).alias("perm"),
            )
            return df

    # --- Public comparisons ---

    @log_duration("compare_identity_sequences")  # type: ignore
    def compare_identity_sequences(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] | None,
    ) -> "DataFrame":
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
        pairs = l.select("schema_name", "object_name", "schema_norm", "object_norm").alias("l").join(
            r.select(
                col("schema_name").alias("r_schema"),
                col("object_name").alias("r_object"),
                col("schema_norm").alias("r_schema_norm"),
                col("object_norm").alias("r_object_norm"),
            ).alias("r"),
            on=(col("l.schema_norm") == col("r.r_schema_norm")) & (col("l.object_norm") == col("r.r_object_norm")),
            how="inner",
        ).select(
            col("l.schema_name").alias("SourceSchemaName"),
            col("l.object_name").alias("SourceObjectName"),
            col("r_schema").alias("DestinationSchemaName"),
            col("r_object").alias("DestinationObjectName"),
            upper(trim(col("l.schema_name"))).alias("s_schema_norm"),
            upper(trim(col("l.object_name"))).alias("s_object_norm"),
            upper(trim(col("r_schema"))).alias("r_schema_norm"),
            upper(trim(col("r_object"))).alias("r_object_norm"),
        )

        s_schemas = [r["SourceSchemaName"] for r in pairs.select("SourceSchemaName").distinct().collect()]
        r_schemas = [r["DestinationSchemaName"] for r in pairs.select("DestinationSchemaName").distinct().collect()]
        db2_id = self._fetch_identity_metadata("db2", s_schemas)
        az_id = self._fetch_identity_metadata("azure", r_schemas)

        db2_id_n = (
            db2_id.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .where(col("is_identity") == lit(True))
        )
        az_id_n = (
            az_id.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .where(col("is_identity") == lit(True))
        )

        db2_f = db2_id_n.join(
            pairs.select(col("s_schema_norm").alias("schema_norm"), col("s_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )
        az_f = az_id_n.join(
            pairs.select(col("r_schema_norm").alias("schema_norm"), col("r_object_norm").alias("table_norm")),
            on=["schema_norm", "table_norm"],
            how="inner",
        )

        joined = (
            db2_f.alias("l")
            .join(az_f.alias("r"), on=["schema_norm", "table_norm", "col_norm"], how="full_outer")
            .select(
                coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
                coalesce(col("l.table_norm"), col("r.table_norm")).alias("table_norm"),
                coalesce(col("l.col_norm"), col("r.col_norm")).alias("col_norm"),
                col("l.is_identity").alias("SourceIdentity"),
                col("r.is_identity").alias("DestinationIdentity"),
                col("l.seed_value").alias("SourceSeed"),
                col("r.seed_value").alias("DestinationSeed"),
                col("l.increment_value").alias("SourceIncrement"),
                col("r.increment_value").alias("DestinationIncrement"),
            )
        )

        out = (
            joined.withColumn("MissingInSource", col("SourceIdentity").isNull())
            .withColumn("MissingInTarget", col("DestinationIdentity").isNull())
            .withColumn(
                "IdentityMismatch", coalesce(col("SourceIdentity"), lit(False)) != coalesce(col("DestinationIdentity"), lit(False))
            )
            .withColumn(
                "SeedMismatch",
                when(
                    col("SourceIdentity") & col("DestinationIdentity") & col("SourceSeed").isNotNull() & col("DestinationSeed").isNotNull(),
                    coalesce(col("SourceSeed"), lit("")) != coalesce(col("DestinationSeed"), lit("")),
                ).otherwise(lit(False)),
            )
            .withColumn(
                "IncrementMismatch",
                when(
                    col("SourceIdentity")
                    & col("DestinationIdentity")
                    & col("SourceIncrement").isNotNull()
                    & col("DestinationIncrement").isNotNull(),
                    coalesce(col("SourceIncrement"), lit("")) != coalesce(col("DestinationIncrement"), lit("")),
                ).otherwise(lit(False)),
            )
        )

        out = out.where(
            col("MissingInSource")
            | col("MissingInTarget")
            | col("IdentityMismatch")
            | ((~col("IdentityMismatch")) & (col("SeedMismatch") | col("IncrementMismatch")))
        )

        out2 = out.join(
            pairs.select(
                "s_schema_norm",
                "s_object_norm",
                "SourceSchemaName",
                "SourceObjectName",
                "DestinationSchemaName",
                "DestinationObjectName",
            ),
            on=[out.schema_norm == col("s_schema_norm"), out.table_norm == col("s_object_norm")],
            how="left",
        )

        return out2.select(
            lit("IdentitySequence").alias("ValidationType"),
            lit("TABLE").alias("ObjectType"),
            col("SourceSchemaName"),
            col("SourceObjectName"),
            col("DestinationSchemaName"),
            col("DestinationObjectName"),
            coalesce(col("col_norm"), lit("")).alias("ElementPath"),
            when(col("MissingInSource"), lit("error"))
            .when(col("MissingInTarget"), lit("error"))
            .when(col("IdentityMismatch"), lit("error"))
            .when(col("SeedMismatch") | col("IncrementMismatch"), lit("warning"))
            .otherwise(lit("")).alias("Status"),
            when(col("MissingInSource"), lit("IDENTITY_MISSING_IN_SOURCE"))
            .when(col("MissingInTarget"), lit("IDENTITY_MISSING_IN_TARGET"))
            .when(col("IdentityMismatch"), lit("IDENTITY_FLAG_MISMATCH"))
            .when(col("SeedMismatch"), lit("IDENTITY_SEED_MISMATCH"))
            .when(col("IncrementMismatch"), lit("IDENTITY_INCREMENT_MISMATCH"))
            .otherwise(lit("")).alias("ErrorCode"),
            when(col("MissingInSource"), lit("Identity column missing in source"))
            .when(col("MissingInTarget"), lit("Identity column missing in target"))
            .when(col("IdentityMismatch"), lit("Identity flag mismatch"))
            .when(col("SeedMismatch"), lit("Identity seed mismatch"))
            .when(col("IncrementMismatch"), lit("Identity increment mismatch"))
            .otherwise(lit("")).alias("ErrorDescription"),
            to_json(
                struct(
                    col("SourceSeed").alias("source_seed"),
                    col("DestinationSeed").alias("destination_seed"),
                    col("SourceIncrement").alias("source_increment"),
                    col("DestinationIncrement").alias("destination_increment"),
                )
            ).alias("DetailsJson"),
        )

    @log_duration("compare_sequence_definitions")  # type: ignore
    def compare_sequence_definitions(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
    ) -> "DataFrame":
        l = self._fetch_sequence_metadata("db2", [source_schema] if source_schema else None)
        r = self._fetch_sequence_metadata("azure", [target_schema] if target_schema else None)
        l_n = l.withColumn("schema_norm", upper(trim(col("schema_name")))).withColumn("seq_norm", upper(trim(col("sequence_name"))))
        r_n = r.withColumn("schema_norm", upper(trim(col("schema_name")))).withColumn("seq_norm", upper(trim(col("sequence_name"))))

        joined = l_n.alias("l").join(
            r_n.alias("r"),
            on=[col("l.schema_norm") == col("r.schema_norm"), col("l.seq_norm") == col("r.seq_norm")],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.sequence_name"), col("r.sequence_name")).alias("SequenceName"),
            col("l.start_value").alias("SourceStart"),
            col("r.start_value").alias("DestinationStart"),
            col("l.increment").alias("SourceIncrement"),
            col("r.increment").alias("DestinationIncrement"),
            col("l.minimum_value").alias("SourceMin"),
            col("r.minimum_value").alias("DestinationMin"),
            col("l.maximum_value").alias("SourceMax"),
            col("r.maximum_value").alias("DestinationMax"),
            col("l.is_cycling").alias("SourceCycle"),
            col("r.is_cycling").alias("DestinationCycle"),
            col("l.cache_size").alias("SourceCache"),
            col("r.cache_size").alias("DestinationCache"),
        )

        out = (
            joined.withColumn("MissingInSource", col("SourceStart").isNull())
            .withColumn("MissingInTarget", col("DestinationStart").isNull())
            .withColumn("StartMismatch", coalesce(col("SourceStart"), lit("")) != coalesce(col("DestinationStart"), lit("")))
            .withColumn(
                "IncrementMismatch", coalesce(col("SourceIncrement"), lit("")) != coalesce(col("DestinationIncrement"), lit(""))
            )
            .withColumn(
                "RangeMismatch",
                (coalesce(col("SourceMin"), lit("")) != coalesce(col("DestinationMin"), lit("")))
                | (coalesce(col("SourceMax"), lit("")) != coalesce(col("DestinationMax"), lit(""))),
            )
            .withColumn("CycleMismatch", coalesce(col("SourceCycle"), lit(False)) != coalesce(col("DestinationCycle"), lit(False)))
            .withColumn("CacheMismatch", coalesce(col("SourceCache"), lit("")) != coalesce(col("DestinationCache"), lit("")))
        )

        out = out.where(
            col("MissingInSource")
            | col("MissingInTarget")
            | col("StartMismatch")
            | col("IncrementMismatch")
            | col("RangeMismatch")
            | col("CycleMismatch")
            | col("CacheMismatch")
        )

        return out.select(
            lit("Sequence").alias("ValidationType"),
            lit("SEQUENCE").alias("ObjectType"),
            lit("").alias("SourceSchemaName"),
            lit("").alias("SourceObjectName"),
            lit("").alias("DestinationSchemaName"),
            lit("").alias("DestinationObjectName"),
            concat(col("schema_norm"), lit("."), col("SequenceName")).alias("ElementPath"),
            lit("error").alias("Status"),
            when(col("MissingInSource"), lit("SEQUENCE_MISSING_IN_SOURCE"))
            .when(col("MissingInTarget"), lit("SEQUENCE_MISSING_IN_TARGET"))
            .when(col("StartMismatch"), lit("SEQUENCE_START_MISMATCH"))
            .when(col("IncrementMismatch"), lit("SEQUENCE_INCREMENT_MISMATCH"))
            .when(col("RangeMismatch"), lit("SEQUENCE_RANGE_MISMATCH"))
            .when(col("CycleMismatch"), lit("SEQUENCE_CYCLE_MISMATCH"))
            .when(col("CacheMismatch"), lit("SEQUENCE_CACHE_MISMATCH"))
            .otherwise(lit("SEQUENCE_MISMATCH")).alias("ErrorCode"),
            when(col("MissingInSource"), lit("Sequence missing in source"))
            .when(col("MissingInTarget"), lit("Sequence missing in target"))
            .when(col("StartMismatch"), lit("Sequence start value mismatch"))
            .when(col("IncrementMismatch"), lit("Sequence increment mismatch"))
            .when(col("RangeMismatch"), lit("Sequence range mismatch"))
            .when(col("CycleMismatch"), lit("Sequence cycle mismatch"))
            .when(col("CacheMismatch"), lit("Sequence cache mismatch"))
            .otherwise(lit("Sequence mismatch")).alias("ErrorDescription"),
            to_json(
                struct(
                    col("SourceStart").alias("source_start"),
                    col("DestinationStart").alias("destination_start"),
                    col("SourceIncrement").alias("source_increment"),
                    col("DestinationIncrement").alias("destination_increment"),
                    col("SourceMin").alias("source_min"),
                    col("DestinationMin").alias("destination_min"),
                    col("SourceMax").alias("source_max"),
                    col("DestinationMax").alias("destination_max"),
                    col("SourceCycle").alias("source_cycle"),
                    col("DestinationCycle").alias("destination_cycle"),
                    col("SourceCache").alias("source_cache"),
                    col("DestinationCache").alias("destination_cache"),
                )
            ).alias("DetailsJson"),
        )

    @log_duration("compare_trigger_definitions")  # type: ignore
    def compare_trigger_definitions(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
    ) -> "DataFrame":
        def canon_def(c):
            return regexp_replace(regexp_replace(upper(trim(coalesce(c, lit("")))), r"\s+", ""), r";+$", "")

        def fetch(side: str, schema: Optional[str]) -> "DataFrame":
            creds = self._build_jdbc_urls()[side]
            engine = self._side_engine(side)
            if engine == "azure":
                where_schema = ""
                if schema:
                    where_schema = f" AND UPPER(s.name) = UPPER('{schema}')"
                q = (
                    f"(SELECT RTRIM(s.name) AS schema_name, RTRIM(trg.name) AS trigger_name, OBJECT_DEFINITION(trg.object_id) AS definition "
                    f" FROM sys.triggers trg "
                    f" JOIN sys.objects o ON o.object_id = trg.object_id "
                    f" JOIN sys.schemas s ON s.schema_id = o.schema_id "
                    f" WHERE trg.is_ms_shipped = 0{where_schema}) x"
                )
            else:
                where_schema = ""
                if schema:
                    where_schema = f" WHERE UPPER(trigschema) = UPPER('{schema}')"
                q = (
                    f"(SELECT RTRIM(trigschema) AS schema_name, RTRIM(trigname) AS trigger_name, TEXT AS definition "
                    f" FROM syscat.triggers{where_schema}) x"
                )
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
                .option("customSchema", "schema_name string, trigger_name string, definition string")
            )
            return self._apply_jdbc_auth(df, creds).load()

        l = (
            fetch("db2", source_schema)
            .withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("trg_norm", upper(trim(col("trigger_name"))))
            .withColumn("DefCanon", canon_def(col("definition")))
        )
        r = (
            fetch("azure", target_schema)
            .withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("trg_norm", upper(trim(col("trigger_name"))))
            .withColumn("DefCanon", canon_def(col("definition")))
        )

        joined = l.alias("l").join(
            r.alias("r"),
            on=[col("l.schema_norm") == col("r.schema_norm"), col("l.trg_norm") == col("r.trg_norm")],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.trigger_name"), col("r.trigger_name")).alias("TriggerName"),
            col("l.DefCanon").alias("SourceCanon"),
            col("r.DefCanon").alias("DestinationCanon"),
        )

        # Normalize function differences between DB2 and Azure (timestamp/user functions) to reduce false mismatches
        def _norm(col_expr):
            expr = col_expr
            # Strip T-SQL comments (any inline starting with --)
            expr = regexp_replace(expr, "--.*", "")
            # Normalize timestamp/user functions
            expr = regexp_replace(expr, "SYSDATETIME\\(\\)", "TIMESTAMP_FN")
            expr = regexp_replace(expr, "CURRENT_TIMESTAMP", "TIMESTAMP_FN")
            expr = regexp_replace(expr, "CURRENTTIMESTAMP", "TIMESTAMP_FN")
            expr = regexp_replace(expr, "CURRENT_USER", "USER_FN")
            expr = regexp_replace(expr, "USER", "USER_FN")
            # Remove SET NOCOUNT and DB2 mode/boilerplate that don't affect logic
            expr = regexp_replace(expr, "SETNOCOUNTON;", "")
            expr = regexp_replace(expr, "MODEDB2SQL", "")
            expr = regexp_replace(expr, "NOTSECURED", "")
            # Remove explicit REFERENCING aliases and SQL Server deleted/inserted aliases
            expr = regexp_replace(expr, "REFERENCINGOLDAS[A-Z]+", "")
            expr = regexp_replace(expr, "REFERENCINGNEWAS[A-Z]+", "")
            expr = regexp_replace(expr, "FROMDELETEDAS[A-Z]+", "FROMDELETED")
            expr = regexp_replace(expr, "FROMINSERTEDAS[A-Z]+", "FROMINSERTED")
            # Strip alias prefixes like N., O., D., I. before column names
            expr = regexp_replace(expr, r"[A-Z]\\.", "")
            # Remove BEGIN/END wrappers
            expr = regexp_replace(expr, "BEGIN", "")
            expr = regexp_replace(expr, "END", "")
            # Remove SQL Server brackets
            expr = regexp_replace(expr, "\\[", "")
            expr = regexp_replace(expr, "\\]", "")
            return expr

        # Apply normalization
        joined = joined.select(
            "schema_norm",
            "TriggerName",
            _norm(col("SourceCanon")).alias("SourceCanonNorm"),
            _norm(col("DestinationCanon")).alias("DestinationCanonNorm"),
            "SourceCanon",
            "DestinationCanon",
        )

        out = (
            joined.withColumn("MissingInSource", col("SourceCanon").isNull())
            .withColumn("MissingInTarget", col("DestinationCanon").isNull())
            .withColumn("DefMatch", coalesce(col("SourceCanonNorm"), lit("")) == coalesce(col("DestinationCanonNorm"), lit("")))
        )

        # Report missing triggers or true (post-normalization) definition differences
        out = out.where(col("MissingInSource") | col("MissingInTarget") | (~col("DefMatch")))

        return out.select(
            lit("Trigger").alias("ValidationType"),
            lit("TRIGGER").alias("ObjectType"),
            when(col("SourceCanon").isNull(), lit("")).otherwise(col("schema_norm")).alias("SourceSchemaName"),
            when(col("SourceCanon").isNull(), lit("")).otherwise(col("TriggerName")).alias("SourceObjectName"),
            when(col("DestinationCanon").isNull(), lit("")).otherwise(col("schema_norm")).alias("DestinationSchemaName"),
            when(col("DestinationCanon").isNull(), lit("")).otherwise(col("TriggerName")).alias("DestinationObjectName"),
            concat(col("schema_norm"), lit("."), col("TriggerName")).alias("ElementPath"),
            when(col("MissingInSource") | col("MissingInTarget"), lit("error")).otherwise(lit("warning")).alias("Status"),
            when(col("MissingInSource"), lit("TRIGGER_MISSING_IN_SOURCE"))
            .when(col("MissingInTarget"), lit("TRIGGER_MISSING_IN_TARGET"))
            .when(~col("DefMatch"), lit("TRIGGER_DEFINITION_DIFFERENT"))
            .otherwise(lit("TRIGGER_MISMATCH")).alias("ErrorCode"),
            when(col("MissingInSource"), lit("Trigger missing in source"))
            .when(col("MissingInTarget"), lit("Trigger missing in target"))
            .when(~col("DefMatch"), lit("Trigger definitions differ after normalization"))
            .otherwise(lit("Trigger mismatch")).alias("ErrorDescription"),
            to_json(
                struct(
                    col("SourceCanon").alias("source_definition"),
                    col("DestinationCanon").alias("destination_definition"),
                )
            ).alias("DetailsJson"),
        )

    @log_duration("compare_routine_definitions")  # type: ignore
    def compare_routine_definitions(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] | None,
    ) -> "DataFrame":
        if not object_types:
            object_types = ["PROCEDURE", "FUNCTION"]

        def canon_def(c):
            return regexp_replace(regexp_replace(upper(trim(coalesce(c, lit("")))), r"\s+", ""), r";+$", "")

        def fetch(side: str, schema: Optional[str]) -> "DataFrame":
            creds = self._build_jdbc_urls()[side]
            engine = self._side_engine(side)
            if engine == "azure":
                where_schema = ""
                if schema:
                    where_schema = f" AND UPPER(s.name) = UPPER('{schema}')"
                q = (
                    f"(SELECT RTRIM(s.name) AS schema_name, RTRIM(o.name) AS routine_name, o.type AS obj_type, m.definition "
                    f" FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id "
                    f" JOIN sys.sql_modules m ON m.object_id=o.object_id "
                    f" WHERE o.type IN ('P','FN','IF','TF'){where_schema}) x"
                )
            else:
                where_schema = ""
                if schema:
                    where_schema = f" WHERE UPPER(routineschema) = UPPER('{schema}')"
                q = (
                    f"(SELECT RTRIM(routineschema) AS schema_name, RTRIM(routinename) AS routine_name, routinetype AS obj_type, TEXT AS definition "
                    f" FROM syscat.routines{where_schema}) x"
                )
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
                .option("customSchema", "schema_name string, routine_name string, obj_type string, definition string")
            )
            return self._apply_jdbc_auth(df, creds).load()

        l = (
            fetch("db2", source_schema)
            .withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("obj_norm", upper(trim(col("routine_name"))))
            .withColumn("DefCanon", canon_def(col("definition")))
        )
        r = (
            fetch("azure", target_schema)
            .withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("obj_norm", upper(trim(col("routine_name"))))
            .withColumn("DefCanon", canon_def(col("definition")))
        )

        joined = l.alias("l").join(
            r.alias("r"),
            on=[col("l.schema_norm") == col("r.schema_norm"), col("l.obj_norm") == col("r.obj_norm")],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.routine_name"), col("r.routine_name")).alias("RoutineName"),
            col("l.obj_type").alias("SourceType"),
            col("r.obj_type").alias("DestinationType"),
            col("l.DefCanon").alias("SourceCanon"),
            col("r.DefCanon").alias("DestinationCanon"),
        )

        out = (
            joined.withColumn("MissingInSource", col("SourceCanon").isNull())
            .withColumn("MissingInTarget", col("DestinationCanon").isNull())
            .withColumn("DefMatch", coalesce(col("SourceCanon"), lit("")) == coalesce(col("DestinationCanon"), lit("")))
        )

        out = out.where(col("MissingInSource") | col("MissingInTarget") | (~col("DefMatch")))

        return out.select(
            lit("Routine").alias("ValidationType"),
            lit("ROUTINE").alias("ObjectType"),
            lit("").alias("SourceSchemaName"),
            lit("").alias("SourceObjectName"),
            lit("").alias("DestinationSchemaName"),
            lit("").alias("DestinationObjectName"),
            concat(col("schema_norm"), lit("."), col("RoutineName")).alias("ElementPath"),
            lit("error").alias("Status"),
            when(col("MissingInSource"), lit("ROUTINE_MISSING_IN_SOURCE"))
            .when(col("MissingInTarget"), lit("ROUTINE_MISSING_IN_TARGET"))
            .when(~col("DefMatch"), lit("ROUTINE_DEFINITION_MISMATCH"))
            .otherwise(lit("ROUTINE_MISMATCH")).alias("ErrorCode"),
            when(col("MissingInSource"), lit("Routine missing in source"))
            .when(col("MissingInTarget"), lit("Routine missing in target"))
            .when(~col("DefMatch"), lit("Routine definition mismatch"))
            .otherwise(lit("Routine mismatch")).alias("ErrorDescription"),
            to_json(
                struct(
                    col("SourceType").alias("source_type"),
                    col("DestinationType").alias("destination_type"),
                    col("SourceCanon").alias("source_definition"),
                    col("DestinationCanon").alias("destination_definition"),
                )
            ).alias("DetailsJson"),
        )

    @log_duration("compare_extended_properties")  # type: ignore
    def compare_extended_properties(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
    ) -> "DataFrame":
        def fetch_tables(side: str, schema: Optional[str]) -> "DataFrame":
            creds = self._build_jdbc_urls()[side]
            engine = self._side_engine(side)
            if engine == "azure":
                where_schema = ""
                if schema:
                    where_schema = f" AND UPPER(s.name) = UPPER('{schema}')"
                q = (
                    f"(SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) AS table_name, CAST(ep.value AS NVARCHAR(4000)) AS description "
                    f" FROM sys.tables t "
                    f" JOIN sys.schemas s ON s.schema_id=t.schema_id "
                    f" LEFT JOIN sys.extended_properties ep ON ep.major_id=t.object_id AND ep.minor_id=0 AND ep.name = 'MS_Description' "
                    f" WHERE t.is_ms_shipped=0{where_schema}) x"
                )
            else:
                where_schema = ""
                if schema:
                    where_schema = f" WHERE UPPER(tabschema) = UPPER('{schema}')"
                q = (
                    f"(SELECT RTRIM(tabschema) AS schema_name, RTRIM(tabname) AS table_name, remarks AS description "
                    f" FROM syscat.tables{where_schema}) x"
                )
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
            )
            return self._apply_jdbc_auth(df, creds).load()

        def fetch_columns(side: str, schema: Optional[str]) -> "DataFrame":
            creds = self._build_jdbc_urls()[side]
            engine = self._side_engine(side)
            if engine == "azure":
                where_schema = ""
                if schema:
                    where_schema = f" AND UPPER(s.name) = UPPER('{schema}')"
                q = (
                    f"(SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) AS table_name, RTRIM(c.name) AS column_name, "
                    f"        CAST(ep.value AS NVARCHAR(4000)) AS description "
                    f" FROM sys.columns c "
                    f" JOIN sys.tables t ON t.object_id=c.object_id "
                    f" JOIN sys.schemas s ON s.schema_id=t.schema_id "
                    f" LEFT JOIN sys.extended_properties ep ON ep.major_id=c.object_id AND ep.minor_id=c.column_id AND ep.name = 'MS_Description' "
                    f" WHERE t.is_ms_shipped=0{where_schema}) x"
                )
            else:
                where_schema = ""
                if schema:
                    where_schema = f" WHERE UPPER(tabschema) = UPPER('{schema}')"
                q = (
                    f"(SELECT RTRIM(tabschema) AS schema_name, RTRIM(tabname) AS table_name, RTRIM(colname) AS column_name, remarks AS description "
                    f" FROM syscat.columns{where_schema}) x"
                )
            df = (
                self.spark.read.format("jdbc")
                .option("url", creds["url"])
                .option("dbtable", q)
                .option("driver", creds["driver"])
                .option("fetchsize", self._default_fetchsize())
            )
            return self._apply_jdbc_auth(df, creds).load()

        tbl_l = (
            fetch_tables("db2", source_schema)
            .withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("desc_norm", upper(trim(coalesce(col("description"), lit("")))))
        )
        tbl_r = (
            fetch_tables("azure", target_schema)
            .withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("desc_norm", upper(trim(coalesce(col("description"), lit("")))))
        )

        tbl_join = tbl_l.alias("l").join(
            tbl_r.alias("r"),
            on=[col("l.schema_norm") == col("r.schema_norm"), col("l.table_norm") == col("r.table_norm")],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.table_name"), col("r.table_name")).alias("table_name"),
            col("l.description").alias("SourceDescription"),
            col("r.description").alias("DestinationDescription"),
            col("l.desc_norm").alias("SourceDescNorm"),
            col("r.desc_norm").alias("DestinationDescNorm"),
        )

        tbl_out = (
            tbl_join.withColumn("MissingInSource", col("SourceDescription").isNull())
            .withColumn("MissingInTarget", col("DestinationDescription").isNull())
            .withColumn("DescMatch", coalesce(col("SourceDescNorm"), lit("")) == coalesce(col("DestinationDescNorm"), lit("")))
            .withColumn("BothNull", col("SourceDescription").isNull() & col("DestinationDescription").isNull())
        )

        tbl_out = tbl_out.where((col("MissingInSource") | col("MissingInTarget") | (~col("DescMatch"))) & (~col("BothNull")))

        tbl_final = tbl_out.select(
            lit("ExtendedProperty").alias("ValidationType"),
            lit("TABLE").alias("ObjectType"),
            lit("").alias("SourceSchemaName"),
            lit("").alias("SourceObjectName"),
            lit("").alias("DestinationSchemaName"),
            lit("").alias("DestinationObjectName"),
            concat(col("schema_norm"), lit("."), col("table_name")).alias("ElementPath"),
            lit("error").alias("Status"),
            when(col("MissingInSource"), lit("TABLE_DESCRIPTION_MISSING_IN_SOURCE"))
            .when(col("MissingInTarget"), lit("TABLE_DESCRIPTION_MISSING_IN_TARGET"))
            .when(~col("DescMatch"), lit("TABLE_DESCRIPTION_MISMATCH"))
            .otherwise(lit("TABLE_DESCRIPTION_MISMATCH")).alias("ErrorCode"),
            when(col("MissingInSource"), lit("Table description missing in source"))
            .when(col("MissingInTarget"), lit("Table description missing in target"))
            .when(~col("DescMatch"), lit("Table description mismatch"))
            .otherwise(lit("Table description mismatch")).alias("ErrorDescription"),
            to_json(
                struct(
                    col("SourceDescription").alias("source_description"),
                    col("DestinationDescription").alias("destination_description"),
                )
            ).alias("DetailsJson"),
        )

        col_l = (
            fetch_columns("db2", source_schema)
            .withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .withColumn("desc_norm", upper(trim(coalesce(col("description"), lit("")))))
        )
        col_r = (
            fetch_columns("azure", target_schema)
            .withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
            .withColumn("desc_norm", upper(trim(coalesce(col("description"), lit("")))))
        )

        col_join = col_l.alias("l").join(
            col_r.alias("r"),
            on=[
                col("l.schema_norm") == col("r.schema_norm"),
                col("l.table_norm") == col("r.table_norm"),
                col("l.col_norm") == col("r.col_norm"),
            ],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.table_norm"), col("r.table_norm")).alias("table_norm"),
            coalesce(col("l.column_name"), col("r.column_name")).alias("column_name"),
            col("l.description").alias("SourceDescription"),
            col("r.description").alias("DestinationDescription"),
            col("l.desc_norm").alias("SourceDescNorm"),
            col("r.desc_norm").alias("DestinationDescNorm"),
        )

        col_out = (
            col_join.withColumn("MissingInSource", col("SourceDescription").isNull())
            .withColumn("MissingInTarget", col("DestinationDescription").isNull())
            .withColumn("DescMatch", coalesce(col("SourceDescNorm"), lit("")) == coalesce(col("DestinationDescNorm"), lit("")))
            .withColumn("BothNull", col("SourceDescription").isNull() & col("DestinationDescription").isNull())
        )

        col_out = col_out.where((col("MissingInSource") | col("MissingInTarget") | (~col("DescMatch"))) & (~col("BothNull")))

        col_final = col_out.select(
            lit("ExtendedProperty").alias("ValidationType"),
            lit("COLUMN").alias("ObjectType"),
            lit("").alias("SourceSchemaName"),
            lit("").alias("SourceObjectName"),
            lit("").alias("DestinationSchemaName"),
            lit("").alias("DestinationObjectName"),
            concat(col("schema_norm"), lit("."), col("table_norm"), lit("."), col("column_name")).alias("ElementPath"),
            lit("error").alias("Status"),
            when(col("MissingInSource"), lit("COLUMN_DESCRIPTION_MISSING_IN_SOURCE"))
            .when(col("MissingInTarget"), lit("COLUMN_DESCRIPTION_MISSING_IN_TARGET"))
            .when(~col("DescMatch"), lit("COLUMN_DESCRIPTION_MISMATCH"))
            .otherwise(lit("COLUMN_DESCRIPTION_MISMATCH")).alias("ErrorCode"),
            when(col("MissingInSource"), lit("Column description missing in source"))
            .when(col("MissingInTarget"), lit("Column description missing in target"))
            .when(~col("DescMatch"), lit("Column description mismatch"))
            .otherwise(lit("Column description mismatch")).alias("ErrorDescription"),
            to_json(
                struct(
                    col("SourceDescription").alias("source_description"),
                    col("DestinationDescription").alias("destination_description"),
                )
            ).alias("DetailsJson"),
        )

        return tbl_final.unionByName(col_final, allowMissingColumns=True)

    @log_duration("compare_collation_encoding")  # type: ignore
    def compare_collation_encoding(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
    ) -> "DataFrame":
        db2_coll = self._fetch_database_collation("db2")
        az_coll = self._fetch_database_collation("azure")

        # If database collations differ, return a single database-level error and skip column-level checks
        if db2_coll or az_coll:
            if db2_coll != az_coll:
                return self.spark.createDataFrame(
                    [
                        {
                            "ValidationType": "CollationEncoding",
                            "ObjectType": "DATABASE",
                            "SourceSchemaName": "",
                            "SourceObjectName": "",
                            "DestinationSchemaName": "",
                            "DestinationObjectName": "",
                            "ElementPath": "DATABASE",
                            "Status": "error",
                            "ErrorCode": "DATABASE_COLLATION_MISMATCH",
                            "ErrorDescription": "Database collation/encoding mismatch",
                            "DetailsJson": json.dumps(
                                {"source_collation": db2_coll, "destination_collation": az_coll}
                            ),
                        }
                    ]
                )

        # Database collations match (or are both empty): check only explicit column overrides on Azure
        db2_cols = self._fetch_collation_metadata("db2", [source_schema] if source_schema else None)
        az_cols = self._fetch_collation_metadata("azure", [target_schema] if target_schema else None)

        l = (
            db2_cols.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )
        r = (
            az_cols.withColumn("schema_norm", upper(trim(col("schema_name"))))
            .withColumn("table_norm", upper(trim(col("table_name"))))
            .withColumn("col_norm", upper(trim(col("column_name"))))
        )

        joined = l.alias("l").join(
            r.alias("r"),
            on=["schema_norm", "table_norm", "col_norm"],
            how="full_outer",
        ).select(
            coalesce(col("l.schema_norm"), col("r.schema_norm")).alias("schema_norm"),
            coalesce(col("l.table_norm"), col("r.table_norm")).alias("table_norm"),
            coalesce(col("l.col_norm"), col("r.col_norm")).alias("col_norm"),
            col("l.collation_name").alias("SourceCollation"),
            col("r.collation_name").alias("DestinationCollation"),
        )

        # Only flag columns where Azure has an explicit collation that differs from the (matching) DB collation
        out = (
            joined.withColumn("HasDestCollation", col("DestinationCollation").isNotNull())
            .withColumn(
                "CollationMismatch",
                col("HasDestCollation") & (coalesce(col("DestinationCollation"), lit("")) != lit(az_coll or "")),
            )
        )

        out = out.where(col("CollationMismatch"))

        return out.select(
            lit("CollationEncoding").alias("ValidationType"),
            lit("COLUMN").alias("ObjectType"),
            lit("").alias("SourceSchemaName"),
            lit("").alias("SourceObjectName"),
            lit("").alias("DestinationSchemaName"),
            lit("").alias("DestinationObjectName"),
            concat(col("schema_norm"), lit("."), col("table_norm"), lit("."), col("col_norm")).alias("ElementPath"),
            lit("warning").alias("Status"),
            lit("COLLATION_MISMATCH").alias("ErrorCode"),
            lit("Collation mismatch").alias("ErrorDescription"),
            to_json(
                struct(
                    lit("").alias("source_collation"),
                    col("DestinationCollation").alias("destination_collation"),
                )
            ).alias("DetailsJson"),
        )

    @log_duration("validate_behavior_all")  # type: ignore
    def validate_behavior_all(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] | None,
    ) -> "DataFrame":
        unified_cols: List[str] = get_unified_columns(include_change_type=False)

        id_df = ensure_all_columns_as_strings(
            self.compare_identity_sequences(source_schema, target_schema, object_types), unified_cols
        )
        seq_df = ensure_all_columns_as_strings(self.compare_sequence_definitions(source_schema, target_schema), unified_cols)
        trg_df = ensure_all_columns_as_strings(self.compare_trigger_definitions(source_schema, target_schema), unified_cols)
        rte_df = ensure_all_columns_as_strings(
            self.compare_routine_definitions(source_schema, target_schema, object_types), unified_cols
        )
        ext_df = ensure_all_columns_as_strings(self.compare_extended_properties(source_schema, target_schema), unified_cols)
        col_df = ensure_all_columns_as_strings(self.compare_collation_encoding(source_schema, target_schema), unified_cols)

        combined = (
            id_df.unionByName(seq_df, allowMissingColumns=True)
            .unionByName(trg_df, allowMissingColumns=True)
            .unionByName(rte_df, allowMissingColumns=True)
            .unionByName(ext_df, allowMissingColumns=True)
            .unionByName(col_df, allowMissingColumns=True)
        )

        combined = combined.select(*[col(c) for c in unified_cols])
        return combined

    @log_duration("save_comparison_to_csv")
    def save_comparison_to_csv(self, comparison_df: "DataFrame", filename_prefix: str) -> str:
        """Save behavior comparison results to CSV using PySpark."""
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


class PySparkBehaviorValidationAzureService(PySparkBehaviorValidationService, PySparkAzureSchemaComparisonService):
    """Behavior validations for Azure to Azure (inherits Azure config)."""

    def __init__(self, *, access_token_override: Optional[str] = None):
        # PySparkAzureSchemaComparisonService already sets Azure-to-Azure configs in its __init__
        PySparkAzureSchemaComparisonService.__init__(self, access_token_override=access_token_override)  # type: ignore

    @log_duration("save_comparison_to_csv")
    def save_comparison_to_csv(self, comparison_df: "DataFrame", filename_prefix: str) -> str:
        """Save behavior comparison results to CSV using PySpark."""
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

