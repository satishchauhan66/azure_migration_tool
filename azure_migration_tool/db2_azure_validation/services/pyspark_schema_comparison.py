# Author: Satish Ch@uhan

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    lit,
    concat,
    upper,
    trim,
    coalesce,
    concat_ws,
    regexp_replace,
    lower,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, IntegerType
from typing import List, Dict, Any, Optional, Tuple
from db2_azure_validation.schemas.common import get_unified_columns, ensure_all_columns_as_strings
import concurrent.futures
import sys
import json
import os
import subprocess
import re
import time
from collections import defaultdict
from functools import reduce

def log_duration(label: str | None = None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            name = label or func.__name__
            try:
                return func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                print(f"[Timing] {name} took {elapsed:.2f}s")
        return wrapper
    return decorator


class PySparkSchemaComparisonService:
    DEFAULT_CONFIG_FILENAME = "database_config.json"
    DEFAULT_SIDE_CONFIG_MAP = {'db2': 'db2', 'azure': 'azure_sql'}
    DEFAULT_SIDE_LABELS = {'db2': 'DB2', 'azure': 'Azure SQL'}
    DEFAULT_SIDE_ENGINES = {'db2': 'db2', 'azure': 'azure'}

    @staticmethod
    def _default_fetchsize() -> str:
        """Return JDBC fetchsize with env override."""
        return os.environ.get("JDBC_FETCHSIZE", "50000")

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
        # Cache for loaded DB config (per service instance/request)
        self._config = None
        self._dbn = None
        self._config_filename = config_filename or self.DEFAULT_CONFIG_FILENAME
        self._side_config_map = (side_config_map or self.DEFAULT_SIDE_CONFIG_MAP).copy()
        self._side_labels = (side_labels or self.DEFAULT_SIDE_LABELS).copy()
        self._side_engine_map = (side_engine_map or self.DEFAULT_SIDE_ENGINES).copy()
        self._app_name = app_name or "DB2_Azure_Schema_Comparison"
        self._access_token_override = access_token_override
        # Ensure JAVA_HOME is set on macOS; try Java 11 then 17; fallback to common Homebrew paths
        if "JAVA_HOME" not in os.environ or not os.path.isdir(os.environ.get("JAVA_HOME", "")):
            java_home = None
            try:
                java_home = subprocess.check_output(["/usr/libexec/java_home", "-v", "11"]).decode().strip()
            except Exception:
                try:
                    java_home = subprocess.check_output(["/usr/libexec/java_home", "-v", "17"]).decode().strip()
                except Exception:
                    java_home = None
            if not java_home:
                # Homebrew/OpenJDK common locations
                candidates = [
                    "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home",
                    "/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home",
                    "/usr/lib/jvm/java-11-openjdk",
                ]
                for p in candidates:
                    if os.path.isdir(p):
                        java_home = p
                        break
            if java_home:
                os.environ["JAVA_HOME"] = java_home
                os.environ["PATH"] = f"{java_home}/bin:" + os.environ.get("PATH", "")

        # Stability hint for local Spark on macOS
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

        # Cache for managed identity token (per service instance)
        self._azure_token_cache: dict[str, Any] = {}

        try:
            # Allow overriding packages and providing local jars via env vars
            default_packages = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11,com.ibm.db2:jcc:11.5.9.0"
            packages = os.environ.get("SPARK_MAVEN_PACKAGES", default_packages)
            local_jars = []
            for env_name in ["MSSQL_JDBC_JAR", "DB2_JDBC_JAR"]:
                p = os.environ.get(env_name)
                if p and os.path.isfile(p):
                    local_jars.append(p)

            # Ensure PySpark uses the correct Python executable (override any bad VM/global values)
            py_exec = sys.executable
            os.environ["PYSPARK_PYTHON"] = py_exec
            os.environ["PYSPARK_DRIVER_PYTHON"] = py_exec

            # If an active SparkSession exists with a mismatched Python, stop it to avoid reusing bad config
            try:
                active = SparkSession.getActiveSession()
            except Exception:
                active = None
            if active is not None:
                try:
                    cur_py = active.sparkContext.getConf().get("spark.pyspark.python")
                except Exception:
                    cur_py = None
                if cur_py and os.path.normcase(os.path.abspath(cur_py)) != os.path.normcase(os.path.abspath(py_exec)):
                    try:
                        active.stop()
                    except Exception:
                        pass

            builder = (
                SparkSession.builder
                .master(os.environ.get("SPARK_MASTER", "local[*]"))
                .appName(self._app_name)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
                .config("spark.ui.showConsoleProgress", os.environ.get("SPARK_CONSOLE_PROGRESS", "false"))
                .config("spark.network.timeout", os.environ.get("SPARK_NETWORK_TIMEOUT", "600s"))
                .config("spark.executor.heartbeatInterval", os.environ.get("SPARK_EXECUTOR_HEARTBEAT", "60s"))
                .config("spark.sql.broadcastTimeout", os.environ.get("SPARK_BROADCAST_TIMEOUT", "1200"))
                .config("spark.pyspark.driver.python", py_exec)
                .config("spark.pyspark.python", py_exec)
                .config("spark.executorEnv.PYSPARK_PYTHON", py_exec)
                .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", py_exec)
                .config("spark.python.worker.reuse", "true")
                .config("spark.sql.debug.maxToStringFields", os.environ.get("SPARK_DEBUG_MAX_TOSTRING","100"))
            )

            # Prefer local jars if provided; also keep maven packages by default
            if local_jars:
                jars_csv = ",".join(local_jars)
                builder = builder.config("spark.jars", jars_csv) \
                                 .config("spark.driver.extraClassPath", jars_csv) \
                                 .config("spark.executor.extraClassPath", jars_csv)
            builder = builder.config("spark.jars.packages", packages)

            self.spark = builder.getOrCreate()
        except Exception as e:
            raise RuntimeError(
                f"PySpark initialization failed: {e}. Please ensure a compatible JDK (11 or 17) is installed and JAVA_HOME is set."
            )
        
        # Define schema for comparison results
        self.comparison_schema = StructType([
            StructField("object_type", StringType(), True),
            StructField("source_schema", StringType(), True),
            StructField("source_object", StringType(), True),
            StructField("target_schema", StringType(), True),
            StructField("target_object", StringType(), True),
            StructField("status", StringType(), True),
            StructField("added_in_destination", StringType(), True),
            StructField("removed_from_destination", StringType(), True),
            StructField("details", StringType(), True),
            StructField("column_differences", StringType(), True),
            StructField("data_type_differences", StringType(), True),
            StructField("constraint_differences", StringType(), True)
        ])

        # Schema for row count comparison results (ensures empty DF creation works)
        self.row_count_schema = StructType([
            StructField("ObjectType", StringType(), True),
            StructField("SourceSchemaName", StringType(), True),
            StructField("SourceObjectName", StringType(), True),
            StructField("DestinationSchemaName", StringType(), True),
            StructField("DestinationObjectName", StringType(), True),
            StructField("SourceRowCount", LongType(), True),
            StructField("DestinationRowCount", LongType(), True),
            StructField("RowCountMatch", BooleanType(), True),
            StructField("ElementPath", StringType(), True),
            StructField("ErrorDescription", StringType(), True)
        ])

        # Schema for column null/empty comparison
        self.null_check_schema = StructType([
            StructField("ObjectType", StringType(), True),
            StructField("SourceSchemaName", StringType(), True),
            StructField("SourceObjectName", StringType(), True),
            StructField("DestinationSchemaName", StringType(), True),
            StructField("DestinationObjectName", StringType(), True),
            StructField("ColumnName", StringType(), True),
            StructField("SourceNullCount", LongType(), True),
            StructField("DestinationNullCount", LongType(), True),
            StructField("SourceEmptyCount", LongType(), True),
            StructField("DestinationEmptyCount", LongType(), True),
            StructField("NullCountMatch", BooleanType(), True),
            StructField("EmptyCountMatch", BooleanType(), True),
            StructField("ElementPath", StringType(), True),
            StructField("ErrorDescription", StringType(), True)
        ])

    @staticmethod
    def _norm_identifier(value: Any) -> str:
        if value is None:
            return ''
        return str(value).strip().upper()

    @staticmethod
    def _clean_identifier(value: Any) -> str:
        if value is None:
            return ''
        return str(value).strip()

    def _resolve_config_path(self) -> str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        return os.path.join(project_root, self._config_filename)

    def _side_engine(self, side: str) -> str:
        return self._side_engine_map.get(side, side)

    def _side_label(self, side: str) -> str:
        return self._side_labels.get(side, side)

    def _get_db_config(self, side: str) -> Dict[str, Any]:
        cfg = self.load_database_config() or {}
        section_name = self._side_config_map.get(side)
        if not section_name:
            return {}
        section = cfg.get(section_name, {})
        if not isinstance(section, dict):
            return {}
        return section

    def load_database_config(self):
        """Load database configuration from JSON file"""
        if self._config is not None:
            return self._config

        config_path = self._resolve_config_path()

        try:
            with open(config_path, 'r') as f:
                self._config = json.load(f)
                # Print once per request
                print(f"Loaded config from: {config_path}")
                return self._config
        except FileNotFoundError:
            raise Exception(f"Database configuration file not found at: {config_path}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON in database configuration file: {e}")

    def get_azure_db_name_for_filename(self) -> str:
        """Return Azure SQL database name (sanitized) for filenames, cached per service instance."""
        if getattr(self, "_az_dbn", None):
            return self._az_dbn
        cfg = self.load_database_config() or {}
        target_section = self._side_config_map.get('azure', 'azure_sql')
        dbn = (cfg.get(target_section, {}).get("database") or "azure")
        dbn = dbn.replace("/", "_").replace("\\", "_")
        self._az_dbn = dbn
        return self._az_dbn

    def _parse_validation_rules(self, rule_env_var: str) -> List[Dict[str, Any]]:
        """Parse validation rules from environment variable.

        Format: RULE_TYPE:PATTERN:MATCH_TYPE,RULE_TYPE:PATTERN:MATCH_TYPE,...
        Example: bracket_equivalent:*:warning,exact_match:NULL:ignore
        """
        rules_str = os.environ.get(rule_env_var, '')
        if not rules_str.strip():
            return []

        rules = []
        for rule_str in rules_str.split(','):
            rule_str = rule_str.strip()
            if not rule_str:
                continue

            parts = rule_str.split(':')
            if len(parts) != 3:
                print(f"[ValidationRules] Invalid rule format: {rule_str}, expected RULE_TYPE:PATTERN:MATCH_TYPE")
                continue

            rule_type, pattern, match_type = parts
            rules.append({
                'rule_type': rule_type.strip(),
                'pattern': pattern.strip(),
                'match_type': match_type.strip()
            })

        return rules

    def _apply_validation_rule(self, source_val: str, dest_val: str, rules: List[Dict[str, Any]]) -> Tuple[bool, str]:
        """Apply validation rules to determine if values match and what action to take.

        Returns: (is_match, action) where action is 'error', 'warning', or 'ignore'
        """
        # Default behavior: strict equality
        if source_val == dest_val:
            return True, 'match'

        # Apply rules
        for rule in rules:
            rule_type = rule['rule_type']
            pattern = rule['pattern']
            match_type = rule['match_type']

            # Check if pattern matches (use * as wildcard)
            if pattern == '*' or re.match(pattern.replace('*', '.*'), source_val, re.IGNORECASE) or re.match(pattern.replace('*', '.*'), dest_val, re.IGNORECASE):
                if rule_type == 'bracket_equivalent':
                    # Check if one value is bracketed version of the other
                    if f"({source_val})" == dest_val or f"({dest_val})" == source_val:
                        if match_type == 'warning':
                            return True, 'warning'  # Treat as match but with warning
                        elif match_type == 'ignore':
                            return True, 'ignore'   # Skip this validation entirely
                # Add more rule types here as needed

        return False, 'error'

    def create_db2_dataframe(self, db2_objects: List[Dict[str, Any]]) -> 'DataFrame':
        """Create PySpark DataFrame from source objects (DB2 default labels)."""
        if not db2_objects:
            return self.spark.createDataFrame([], self.comparison_schema)
        
        # Convert source objects to DataFrame
        db2_data = []
        for obj in db2_objects:
            db2_data.append({
                "object_type": obj.get('object_type', ''),
                "source_schema": obj.get('schema_name', ''),
                "source_object": obj.get('object_name', ''),
                "target_schema": "",
                "target_object": "",
                "status": "MISSING_IN_TARGET",
                "added_in_destination": "No",
                "removed_from_destination": "Yes",
                "details": "Object exists in source but not in target",
                "column_differences": "",
                "data_type_differences": "",
                "constraint_differences": ""
            })
        
        return self.spark.createDataFrame(db2_data, self.comparison_schema)

    def create_azure_dataframe(self, azure_objects: List[Dict[str, Any]]) -> 'DataFrame':
        """Create PySpark DataFrame from Azure SQL objects"""
        if not azure_objects:
            return self.spark.createDataFrame([], self.comparison_schema)
        
        # Convert Azure objects to DataFrame
        azure_data = []
        for obj in azure_objects:
            azure_data.append({
                "object_type": obj.get('object_type', ''),
                "source_schema": "",
                "source_object": "",
                "target_schema": obj.get('schema_name', ''),
                "target_object": obj.get('object_name', ''),
                "status": "MISSING_IN_SOURCE",
                "added_in_destination": "Yes",
                "removed_from_destination": "No",
                "details": "Object exists in target but not in source",
                "column_differences": "",
                "data_type_differences": "",
                "constraint_differences": ""
            })
        
        return self.spark.createDataFrame(azure_data, self.comparison_schema)

    def test_jdbc_connection_detail(self, side: str) -> dict:
        """Test JDBC connectivity and return diagnostic details on failure."""
        try:
            creds = self._build_jdbc_urls()[side]
            engine = self._side_engine(side)
            if engine == 'azure':
                q = "(SELECT 1 AS x) t"
            else:
                q = "(SELECT 1 AS x FROM SYSIBM.SYSDUMMY1) t"
            reader = (
                self.spark.read.format('jdbc')
                .option('url', creds['url'])
                .option('dbtable', q)
                .option('driver', creds['driver'])
                .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '1000'))
            )
            reader = self._apply_jdbc_auth(reader, creds)
            reader.load().limit(1).count()
            return {"ok": True, "url": creds.get('url',''), "user": creds.get('user','')}
        except Exception as e:
            # Return trimmed message to avoid leaking stack traces in API
            return {"ok": False, "url": self._build_jdbc_urls()[side].get('url',''), "user": self._build_jdbc_urls()[side].get('user',''), "error": str(e)}



    @log_duration("table_column_counts")
    def table_column_counts(
        self,
        source_schema: str | None,
        target_schema: str | None,
        object_types: list[str] | None
    ) -> 'DataFrame':
        """For matched TABLE/VIEW pairs, return per-table column counts for DB2 and Azure and a match flag."""
        clean = self._clean_identifier
        if not object_types:
            object_types = ['TABLE']
        l = self._read_tables('db2', object_types, source_schema)
        r = self._read_tables('azure', object_types, target_schema)

        if not source_schema and not target_schema:
            l_s = l.select('schema_norm').distinct().withColumnRenamed('schema_norm','s')
            r_s = r.select('schema_norm').distinct().withColumnRenamed('schema_norm','t')
            matched_s = l_s.join(r_s, col('s') == col('t'), 'inner').select(col('s').alias('schema_norm'))
            l = l.join(matched_s, 'schema_norm', 'inner')
            r = r.join(matched_s, 'schema_norm', 'inner')

        pairs = l.select('schema_name','object_name','schema_norm','object_norm', col('obj_type_norm').alias('l_type')).alias('l').join(
            r.select(col('schema_name').alias('r_schema'), col('object_name').alias('r_object'), col('schema_norm').alias('r_schema_norm'), col('object_norm').alias('r_object_norm'), col('obj_type_norm').alias('r_type')).alias('r'),
            on=(col('l.schema_norm') == col('r.r_schema_norm')) & (col('l.object_norm') == col('r.r_object_norm')),
            how='inner'
        ).select(
            col('l.schema_norm').alias('s_schema_norm'),
            col('l.object_norm').alias('s_object_norm'),
            col('r.r_schema_norm').alias('r_schema_norm'),
            col('r.r_object_norm').alias('r_object_norm'),
            col('l.schema_name').alias('SourceSchemaName'),
            col('l.object_name').alias('SourceObjectName'),
            col('r_schema').alias('DestinationSchemaName'),
            col('r_object').alias('DestinationObjectName'),
            when(col('l_type').isNotNull(), col('l_type')).when(col('r_type').isNotNull(), col('r_type')).otherwise(lit('TABLE')).alias('ObjectType')
        )

        # Bulk columns
        s_schema_list = [clean(r['SourceSchemaName']) for r in pairs.select('SourceSchemaName').distinct().collect()]
        r_schema_list = [clean(r['DestinationSchemaName']) for r in pairs.select('DestinationSchemaName').distinct().collect()]
        l_cols = self._fetch_columns_bulk('db2', s_schema_list)
        r_cols = self._fetch_columns_bulk('azure', r_schema_list)
        l_cols_n = l_cols.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                       .withColumn('table_norm', upper(trim(col('table_name'))))
        r_cols_n = r_cols.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                       .withColumn('table_norm', upper(trim(col('table_name'))))

        l_cnt = l_cols_n.join(pairs.select(col('s_schema_norm').alias('schema_norm'), col('s_object_norm').alias('table_norm')).distinct(), on=['schema_norm','table_norm'], how='inner') \
                        .groupBy('schema_norm','table_norm').count().withColumnRenamed('count','SourceColumnCount')
        r_cnt = r_cols_n.join(pairs.select(col('r_schema_norm').alias('schema_norm'), col('r_object_norm').alias('table_norm')).distinct(), on=['schema_norm','table_norm'], how='inner') \
                        .groupBy('schema_norm','table_norm').count().withColumnRenamed('count','DestinationColumnCount')

        out = pairs.join(
            l_cnt, on=[pairs.s_schema_norm == l_cnt.schema_norm, pairs.s_object_norm == l_cnt.table_norm], how='left'
        ).drop(l_cnt.schema_norm).drop(l_cnt.table_norm).join(
            r_cnt, on=[col('r_schema_norm') == r_cnt.schema_norm, col('r_object_norm') == r_cnt.table_norm], how='left'
        ).drop(r_cnt.schema_norm).drop(r_cnt.table_norm)

        out = out.withColumn('SourceColumnCount', coalesce(col('SourceColumnCount'), lit(0).cast('long'))) \
                 .withColumn('DestinationColumnCount', coalesce(col('DestinationColumnCount'), lit(0).cast('long'))) \
                 .withColumn('ColumnCountMatch', col('SourceColumnCount') == col('DestinationColumnCount')) \
                 .withColumn('ElementPath', concat(trim(col('SourceSchemaName')), lit('.'), trim(col('SourceObjectName')))) \
                 .withColumn('ErrorDescription', when(~col('ColumnCountMatch'), lit('Column count mismatch')).otherwise(lit('')))

        out = out.where(~col('ColumnCountMatch'))

        return out.select(
            lit('ColumnCount').alias('ValidationType'),
            'ObjectType',
            'SourceSchemaName','SourceObjectName',
            'DestinationSchemaName','DestinationObjectName',
            'SourceColumnCount','DestinationColumnCount','ColumnCountMatch',
            'ElementPath','ErrorDescription'
        )

    @log_duration("compare_schemas_with_pyspark")
    def compare_schemas_with_pyspark(
        self, 
        db2_objects: List[Dict[str, Any]], 
        azure_objects: List[Dict[str, Any]]
    ) -> 'DataFrame':
        """Compare schemas using PySpark operations"""
        
        # Create DataFrames
        db2_df = self.create_db2_dataframe(db2_objects)
        azure_df = self.create_azure_dataframe(azure_objects)
        
        # Build normalized keys (ignore schema for matching; compare by object name + type)
        db2_with_key = (
            db2_df
            .withColumn("object_norm", upper(trim(col("source_object"))))
            .withColumn("type_norm", upper(trim(col("object_type"))))
            .withColumn("comparison_key", concat(col("object_norm"), lit("."), col("type_norm")))
        )

        azure_with_key = (
            azure_df
            .withColumn("object_norm", upper(trim(col("target_object"))))
            .withColumn("type_norm", upper(trim(col("object_type"))))
            .withColumn("comparison_key", concat(col("object_norm"), lit("."), col("type_norm")))
        )
        
        # Alias to avoid ambiguous columns
        d = db2_with_key.alias("d")
        a = azure_with_key.alias("a")

        # Find matches using PySpark joins
        matches = d.join(
            a,
            on=col("d.comparison_key") == col("a.comparison_key"),
            how="inner"
        ).select(
            col("d.object_type").alias("object_type"),
            col("d.source_schema").alias("source_schema"),
            col("d.source_object").alias("source_object"),
            col("a.target_schema").alias("target_schema"),
            col("a.target_object").alias("target_object"),
            lit("MATCH").alias("status"),
            lit("No").alias("added_in_destination"),
            lit("No").alias("removed_from_destination"),
            lit("Objects match between databases").alias("details"),
            lit("").alias("column_differences"),
            lit("").alias("data_type_differences"),
            lit("").alias("constraint_differences")
        )
        
        # Find objects only in DB2 (missing in target)
        db2_only = d.join(
            a,
            on=col("d.comparison_key") == col("a.comparison_key"),
            how="left_anti"
        ).select(
            col("d.object_type").alias("object_type"),
            col("d.source_schema").alias("source_schema"),
            col("d.source_object").alias("source_object"),
            lit("").alias("target_schema"),
            lit("").alias("target_object"),
            lit("MISSING_IN_TARGET").alias("status"),
            lit("No").alias("added_in_destination"),
            lit("Yes").alias("removed_from_destination"),
            lit("Object exists in DB2 but not in Azure SQL").alias("details"),
            lit("").alias("column_differences"),
            lit("").alias("data_type_differences"),
            lit("").alias("constraint_differences")
        )
        
        # Find objects only in Azure SQL (missing in source)
        azure_only = a.join(
            d,
            on=col("a.comparison_key") == col("d.comparison_key"),
            how="left_anti"
        ).select(
            col("a.object_type").alias("object_type"),
            lit("").alias("source_schema"),
            lit("").alias("source_object"),
            col("a.target_schema").alias("target_schema"),
            col("a.target_object").alias("target_object"),
            lit("MISSING_IN_SOURCE").alias("status"),
            lit("Yes").alias("added_in_destination"),
            lit("No").alias("removed_from_destination"),
            lit("Object exists in Azure SQL but not in DB2").alias("details"),
            lit("").alias("column_differences"),
            lit("").alias("data_type_differences"),
            lit("").alias("constraint_differences")
        )
        
        # Union all results
        comparison_result = matches.union(db2_only).union(azure_only)
        
        return comparison_result

    @log_duration("compare_columns_with_pyspark")
    def compare_columns_with_pyspark(
        self,
        db2_columns: List[Dict[str, Any]],
        azure_columns: List[Dict[str, Any]],
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str
    ) -> 'DataFrame':
        """Compare columns using PySpark and return a DataFrame in report schema."""
        source_schema = (source_schema or "").strip()
        source_table = (source_table or "").strip()
        target_schema = (target_schema or "").strip()
        target_table = (target_table or "").strip()
        col_schema = StructType([
            StructField("column_name", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("length", StringType(), True),
            StructField("precision", StringType(), True),
            StructField("scale", StringType(), True),
            StructField("nullable", StringType(), True),
            StructField("default_value", StringType(), True),
            StructField("ordinal", IntegerType(), True),
            StructField("identity_flag", BooleanType(), True),
            StructField("identity_seed", StringType(), True),
            StructField("identity_increment", StringType(), True),
        ])

        def _with_ordinal(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            enriched: List[Dict[str, Any]] = []
            for idx, row in enumerate(rows or []):
                r = (row or {}).copy()
                r.setdefault("ordinal", idx)
                r.setdefault("identity_flag", None)
                r.setdefault("identity_seed", None)
                r.setdefault("identity_increment", None)
                enriched.append(r)
            return enriched

        ldf_base = self.spark.createDataFrame(_with_ordinal(db2_columns), schema=col_schema) if db2_columns else self.spark.createDataFrame([], schema=col_schema)
        # Keep case and whitespace to enforce case-sensitive, space-sensitive matching.
        ldf = ldf_base \
            .withColumnRenamed("column_name", "l_name") \
            .withColumnRenamed("data_type", "l_type") \
            .withColumnRenamed("length", "l_length") \
            .withColumnRenamed("precision", "l_precision") \
            .withColumnRenamed("scale", "l_scale") \
            .withColumnRenamed("nullable", "l_nullable") \
            .withColumnRenamed("default_value", "l_default") \
            .withColumnRenamed("ordinal", "l_ordinal") \
            .withColumnRenamed("identity_flag", "l_identity_flag") \
            .withColumnRenamed("identity_seed", "l_identity_seed") \
            .withColumnRenamed("identity_increment", "l_identity_increment") \
            .withColumn("l_norm", coalesce(col("l_name"), lit(""))) \
            .withColumn("l_norm_ci", upper(trim(coalesce(col("l_name"), lit("")))))

        rdf_base = self.spark.createDataFrame(_with_ordinal(azure_columns), schema=col_schema) if azure_columns else self.spark.createDataFrame([], schema=col_schema)
        rdf = rdf_base \
            .withColumnRenamed("column_name", "r_name") \
            .withColumnRenamed("data_type", "r_type") \
            .withColumnRenamed("length", "r_length") \
            .withColumnRenamed("precision", "r_precision") \
            .withColumnRenamed("scale", "r_scale") \
            .withColumnRenamed("nullable", "r_nullable") \
            .withColumnRenamed("default_value", "r_default") \
            .withColumnRenamed("ordinal", "r_ordinal") \
            .withColumnRenamed("identity_flag", "r_identity_flag") \
            .withColumnRenamed("identity_seed", "r_identity_seed") \
            .withColumnRenamed("identity_increment", "r_identity_increment") \
            .withColumn("r_norm", coalesce(col("r_name"), lit(""))) \
            .withColumn("r_norm_ci", upper(trim(coalesce(col("r_name"), lit("")))))

        # Step 1: exact name match
        exact_join = ldf.alias("l").join(
            rdf.alias("r"),
            on=col("l.l_norm") == col("r.r_norm"),
            how="inner"
        )

        # Step 2: unmatched on each side after exact match
        l_unmatched = ldf.alias("l").join(
            rdf.alias("r"),
            on=col("l.l_norm") == col("r.r_norm"),
            how="left_anti"
        )
        r_unmatched = rdf.alias("r").join(
            ldf.alias("l"),
            on=col("l.l_norm") == col("r.r_norm"),
            how="left_anti"
        )

        # Step 3: case/space-insensitive match to surface name-only differences
        name_mismatch = l_unmatched.alias("l").join(
            r_unmatched.alias("r"),
            on=col("l.l_norm_ci") == col("r.r_norm_ci"),
            how="inner"
        )

        # Step 4: remaining unmatched are true adds/removes
        l_only = l_unmatched.alias("l").join(
            name_mismatch.select(col("l.l_norm_ci").alias("match_key")).distinct(),
            on=col("l.l_norm_ci") == col("match_key"),
            how="left_anti"
        )
        r_only = r_unmatched.alias("r").join(
            name_mismatch.select(col("r.r_norm_ci").alias("match_key")).distinct(),
            on=col("r.r_norm_ci") == col("match_key"),
            how="left_anti"
        )

        # Union all scenarios
        joined = exact_join.unionByName(name_mismatch, allowMissingColumns=True) \
                           .unionByName(l_only, allowMissingColumns=True) \
                           .unionByName(r_only, allowMissingColumns=True)

        # Build dtype signatures
        l_sig = concat(
            coalesce(col("l.l_type"), lit("")),
            lit("("),
            coalesce(col("l.l_length").cast("string"), coalesce(col("l.l_precision").cast("string"), lit(""))),
            when(col("l.l_scale").isNotNull() & (col("l.l_scale") != 0), concat(lit(","), col("l.l_scale").cast("string"))).otherwise(lit("")),
            lit(")")
        )
        r_sig = concat(
            coalesce(col("r.r_type"), lit("")),
            lit("("),
            coalesce(col("r.r_precision").cast("string"), coalesce(col("r.r_length").cast("string"), lit(""))),
            when(col("r.r_scale").isNotNull() & (col("r.r_scale") != 0), concat(lit(","), col("r.r_scale").cast("string"))).otherwise(lit("")),
            lit(")")
        )

        # Differences
        dtype_diff = when(l_sig != r_sig, concat(lit("DataType: DB2 "), l_sig, lit(" vs Azure "), r_sig))
        null_diff = when(col("l.l_nullable").isNotNull() & col("r.r_nullable").isNotNull() & (col("l.l_nullable") != col("r.r_nullable")),
                         concat(lit("Nullable: DB2 "), col("l.l_nullable").cast("string"), lit(" vs Azure "), col("r.r_nullable").cast("string")))
        # Normalize default expressions so semantically equivalent defaults do not false-positive.
        def _normalize_default(expr_col):
            # Trim, uppercase for keywords, keep quoted strings as-is.
            return upper(trim(coalesce(expr_col.cast("string"), lit(""))))

        def _default_canon(expr_col, side: str):
            norm = _normalize_default(expr_col)
            # Map common DB2 -> Azure equivalents
            mappings = {
                "CURRENT TIMESTAMP": "SYSDATETIME()",
                "CURRENT_TIMESTAMP": "SYSDATETIME()",
                "CURRENT DATE": "CAST(GETDATE() AS DATE)",
                "CURRENT_DATE": "CAST(GETDATE() AS DATE)",
                "CURRENT TIME": "CONVERT(TIME, GETDATE())",
                "CURRENT_TIME": "CONVERT(TIME, GETDATE())",
            }
            # Only apply DB2 -> Azure mapping when comparing; otherwise keep normalized.
            if side == "db2":
                return when(norm.isNull(), lit("")).otherwise(
                    when(norm.isin(list(mappings.keys())), lit("map:" + mappings[norm]))
                    .otherwise(norm)
                )
            # Azure side: leave as-is but still normalized for comparison.
            return when(norm.isNull(), lit("")).otherwise(norm)

        l_def_norm = _default_canon(col("l.l_default"), "db2")
        r_def_norm = _default_canon(col("r.r_default"), "azure")

        # Treat empty string and explicit NULL as equivalent when both allow NULL.
        def_equal = (coalesce(l_def_norm, lit("")) == coalesce(r_def_norm, lit(""))) | (
            (l_def_norm == lit("")) & (r_def_norm == lit("NULL"))
        ) | (
            (l_def_norm == lit("NULL")) & (r_def_norm == lit(""))
        )

        def_diff = when(~def_equal,
                        concat(lit("Default: DB2 "), coalesce(col("l.l_default").cast("string"), lit("")),
                              lit(" vs Azure "), coalesce(col("r.r_default").cast("string"), lit(""))))

        name_diff = when(
            col("l.l_norm_ci").isNotNull() & col("r.r_norm_ci").isNotNull() &
            (col("l.l_norm_ci") == col("r.r_norm_ci")) &
            (coalesce(col("l.l_name"), lit("")) != coalesce(col("r.r_name"), lit(""))),
            concat(lit("Name: DB2 '"), coalesce(col("l.l_name"), lit("")), lit("' vs Azure '"), coalesce(col("r.r_name"), lit("")), lit("'"))
        )

        order_diff = when(
            col("l.l_norm").isNotNull() & col("r.r_norm").isNotNull() &
            col("l.l_ordinal").isNotNull() & col("r.r_ordinal").isNotNull() &
            (col("l.l_ordinal") != col("r.r_ordinal")),
            concat(lit("Order: DB2 "), (col("l.l_ordinal") + lit(1)).cast("string"), lit(" vs Azure "), (col("r.r_ordinal") + lit(1)).cast("string"))
        )

        ident_flag_diff = when(
            coalesce(col("l.l_identity_flag"), lit(False)) != coalesce(col("r.r_identity_flag"), lit(False)),
            concat(lit("Identity: DB2 "), coalesce(col("l.l_identity_flag").cast("string"), lit("False")), lit(" vs Azure "), coalesce(col("r.r_identity_flag").cast("string"), lit("False")))
        )
        ident_seed_diff = when(
            coalesce(col("l.l_identity_flag"), lit(False)) & coalesce(col("r.r_identity_flag"), lit(False)) &
            col("l.l_identity_seed").isNotNull() & col("r.r_identity_seed").isNotNull() &
            (col("l.l_identity_seed") != col("r.r_identity_seed")),
            concat(lit("IdentitySeed: DB2 "), col("l.l_identity_seed").cast("string"), lit(" vs Azure "), col("r.r_identity_seed").cast("string"))
        )
        ident_inc_diff = when(
            coalesce(col("l.l_identity_flag"), lit(False)) & coalesce(col("r.r_identity_flag"), lit(False)) &
            col("l.l_identity_increment").isNotNull() & col("r.r_identity_increment").isNotNull() &
            (col("l.l_identity_increment") != col("r.r_identity_increment")),
            concat(lit("IdentityIncrement: DB2 "), col("l.l_identity_increment").cast("string"), lit(" vs Azure "), col("r.r_identity_increment").cast("string"))
        )

        details = concat_ws("; ",
            coalesce(name_diff, lit("")),
            coalesce(dtype_diff, lit("")),
            coalesce(null_diff, lit("")),
            coalesce(def_diff, lit("")),
            coalesce(order_diff, lit("")),
            coalesce(ident_flag_diff, lit("")),
            coalesce(ident_seed_diff, lit("")),
            coalesce(ident_inc_diff, lit(""))
        )

        change_type = when(col("r.r_norm").isNull(), lit("Removed")) \
                      .when(col("l.l_norm").isNull(), lit("Added")) \
                      .when(name_diff.isNotNull(), lit("NameMismatch")) \
                      .otherwise(
                          when(
                              (coalesce(dtype_diff, lit("")) != "") |
                              (coalesce(null_diff, lit("")) != "") |
                              (coalesce(def_diff, lit("")) != "") |
                              (coalesce(order_diff, lit("")) != "") |
                              (coalesce(ident_flag_diff, lit("")) != "") |
                              (coalesce(ident_seed_diff, lit("")) != "") |
                              (coalesce(ident_inc_diff, lit("")) != ""),
                              lit("Modified")
                          ).otherwise(lit("Unchanged"))
                      )

        element_path = when(coalesce(col("l.l_name"), lit("")).cast("string") != "",
                            concat(lit(source_schema or target_schema or ""), lit("."), lit(source_table or target_table or ""), lit("."), coalesce(col("l.l_name"), col("r.r_name")))) \
                       .otherwise(concat(lit(target_schema or source_schema or ""), lit("."), lit(target_table or source_table or ""), lit("."), coalesce(col("r.r_name"), col("l.l_name"))))

        result = joined.select(
            lit("column").alias("ObjectType"),
            lit(source_schema or "").alias("SourceSchemaName"),
            lit(source_table or "").alias("SourceObjectName"),
            lit(target_schema or "").alias("DestinationSchemaName"),
            lit(target_table or "").alias("DestinationObjectName"),
            change_type.alias("ChangeType"),
            element_path.alias("ElementPath"),
            details.alias("ErrorDescription")
        ).where(col("ChangeType") != lit("Unchanged"))

        return result

    # --- Row count comparison (PySpark JDBC) ---
    def _build_jdbc_urls(self):
        cfg = self.load_database_config()

        def _to_jdbc_bool(value: Any, default: bool) -> str:
            if isinstance(value, bool):
                return 'true' if value else 'false'
            s = str(value).strip().lower() if value is not None else ('true' if default else 'false')
            if s in ('1', 'true', 'yes', 'y', 'on'):
                return 'true'
            if s in ('0', 'false', 'no', 'n', 'off'):
                return 'false'
            return 'true' if default else 'false'

        def _normalize_driver(engine: str, raw_driver: str | None) -> str:
            candidate = (raw_driver or '').strip()
            if not candidate:
                return "com.microsoft.sqlserver.jdbc.SQLServerDriver" if engine == 'azure' else "com.ibm.db2.jcc.DB2Driver"
            lower_candidate = candidate.lower()
            if 'odbc driver' in lower_candidate:
                return "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            return candidate

        auth_mode = os.environ.get("AZURE_SQL_AUTH", "password").strip().lower()

        def _get_azure_access_token() -> str:
            # Supports managed identity and interactive MFA (AAD token) modes.
            if auth_mode not in ("managed_identity", "aad_mfa"):
                return ""
            # Cache token with a small buffer to avoid expiry mid-flight
            cached = self._azure_token_cache.get("token")
            exp = self._azure_token_cache.get("expires_on")
            now = time.time()
            if cached and exp and now < (exp - 300):
                return cached

            # Optional timeout to avoid hanging forever on interactive/device prompts
            try:
                timeout_sec = int(os.environ.get("AZURE_SQL_AUTH_TIMEOUT", "300"))
            except Exception:
                timeout_sec = 300
            timeout_sec = max(30, timeout_sec)

            def _get_token_with_timeout(cred):
                scope = "https://database.windows.net/.default"
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    fut = ex.submit(cred.get_token, scope)
                    try:
                        return fut.result(timeout=timeout_sec)
                    except concurrent.futures.TimeoutError as tex:
                        raise RuntimeError(f"AAD auth timed out after {timeout_sec}s") from tex

            if auth_mode == "managed_identity":
                try:
                    from azure.identity import ManagedIdentityCredential  # type: ignore
                except Exception as ex:
                    raise RuntimeError("Managed identity requested but azure-identity is not installed") from ex
                client_id = os.environ.get("AZURE_SQL_MI_CLIENT_ID")
                cred = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()
            else:
                # MFA / AAD interactive flow. Use device code when headless, otherwise interactive browser.
                try:
                    from azure.identity import (  # type: ignore
                        DeviceCodeCredential,
                        InteractiveBrowserCredential,
                    )
                except Exception as ex:
                    raise RuntimeError("AAD MFA requested but azure-identity is not installed") from ex
                tenant_id = os.environ.get("AZURE_SQL_TENANT_ID")
                client_id = os.environ.get("AZURE_SQL_CLIENT_ID")
                use_device = str(os.environ.get("AZURE_SQL_MFA_DEVICE_CODE", "false")).strip().lower() in (
                    "1",
                    "true",
                    "yes",
                    "y",
                )
                credential_cls = DeviceCodeCredential if use_device else InteractiveBrowserCredential
                cred = credential_cls(tenant_id=tenant_id, client_id=client_id)

            token = _get_token_with_timeout(cred)
            self._azure_token_cache["token"] = token.token
            self._azure_token_cache["expires_on"] = getattr(token, "expires_on", now + 3000)
            return token.token

        def _auth_options_for_engine(engine: str) -> dict[str, Any]:
            # If a caller supplied an explicit access token, always prefer it (azure only)
            if engine == "azure" and self._access_token_override:
                return {"access_token": self._access_token_override}
            if engine == "azure" and auth_mode in ("managed_identity", "aad_mfa"):
                tok = _get_azure_access_token()
                return {"access_token": tok}
            return {}

        urls: Dict[str, Dict[str, Any]] = {}
        for side in ('db2', 'azure'):
            section_name = self._side_config_map.get(side)
            section = cfg.get(section_name, {}) if section_name else {}
            engine = self._side_engine(side)
            auth_opts = _auth_options_for_engine(engine)
            if engine == 'azure':
                enc = _to_jdbc_bool(section.get('encrypt', 'yes'), default=True)
                tsc = _to_jdbc_bool(section.get('trust_server_certificate', 'no'), default=False)
                host = section.get('server') or ''
                port = section.get('port', 1433)
                database = section.get('database') or ''
                az_url = f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt={enc};trustServerCertificate={tsc};hostNameInCertificate=*.database.windows.net"
                urls[side] = {
                    "url": az_url,
                    "user": section.get('username',''),
                    "password": section.get('password',''),
                    "driver": _normalize_driver(engine, section.get('jdbc_driver') or section.get('driver')),
                    **auth_opts,
                }
            elif engine == 'db2':
                host = section.get('host') or ''
                port = section.get('port', 50000)
                database = section.get('database') or ''
                db2_url = f"jdbc:db2://{host}:{port}/{database}"
                urls[side] = {
                    "url": db2_url,
                    "user": section.get('username',''),
                    "password": section.get('password',''),
                    "driver": _normalize_driver(engine, section.get('jdbc_driver') or section.get('driver'))
                }
            else:
                raise ValueError(f"Unsupported engine '{engine}' for side '{side}'")
        return urls

    def _apply_jdbc_auth(self, reader, creds: Dict[str, Any]):
        """
        Apply JDBC authentication options (password or accessToken) to a Spark reader.
        """
        token = creds.get("access_token")
        if token:
            reader = reader.option("accessToken", token)
        else:
            reader = reader.option("user", creds.get("user", "")).option("password", creds.get("password", ""))
        return reader

    def get_azure_access_token_info(self) -> Dict[str, Any]:
        """
        Return the current Azure SQL access token (and expiry if available) after applying auth logic.
        """
        creds = self._build_jdbc_urls().get("azure", {})
        token = creds.get("access_token")
        if not token:
            raise RuntimeError("Azure access token not available; ensure AZURE_SQL_AUTH is configured for token-based auth.")
        return {
            "access_token": token,
            "expires_on": self._azure_token_cache.get("expires_on"),
        }

    def _read_tables(self, side: str, object_types: list[str], schema: str | None):
        creds = self._build_jdbc_urls()[side]
        engine = self._side_engine(side)
        if engine == 'azure':
            # sys.objects with schemas
            base = f"(SELECT SCHEMA_NAME(schema_id) AS schema_name, name AS object_name, type FROM sys.objects) x"
            reader = (
                self.spark.read.format('jdbc')
                .option('url', creds['url'])
                .option('dbtable', base)
                .option('driver', creds['driver'])
                .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
            )
            reader = self._apply_jdbc_auth(reader, creds)
            df = reader.load()
            # Normalize schema/object and map type to TABLE/VIEW
            df = df.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                   .withColumn('object_norm', upper(trim(col('object_name')))) \
                   .withColumn('obj_type_norm', when(col('type') == lit('U'), lit('TABLE'))
                                              .when(col('type') == lit('V'), lit('VIEW'))
                                              .otherwise(lit('OTHER')))
            if object_types:
                mapping = {
                    'TABLE': [ 'U' ],
                    'VIEW':  [ 'V' ]
                }
                allowed = []
                for t in object_types:
                    allowed += mapping.get(t.upper(), [])
                df = df.where(col('type').isin(allowed))
            if schema:
                df = df.where(col('schema_norm') == upper(lit(schema.strip())))
            return df.select('schema_name','object_name','schema_norm','object_norm','obj_type_norm').distinct()
        else:
            # DB2 SYSIBM.SYSTABLES
            base = f"(SELECT CREATOR AS schema_name, NAME AS object_name, TYPE FROM SYSIBM.SYSTABLES) x"
            reader = (
                self.spark.read.format('jdbc')
                .option('url', creds['url'])
                .option('dbtable', base)
                .option('driver', creds['driver'])
                .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
            )
            reader = self._apply_jdbc_auth(reader, creds)
            df = reader.load()
            # Normalize schema/object and map type to TABLE/VIEW
            df = df.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                   .withColumn('object_norm', upper(trim(col('object_name')))) \
                   .withColumn('obj_type_norm', when(col('TYPE') == lit('T'), lit('TABLE'))
                                              .when(col('TYPE') == lit('V'), lit('VIEW'))
                                              .otherwise(lit('OTHER')))
            if object_types:
                mapping = {
                    'TABLE': [ 'T' ],
                    'VIEW':  [ 'V' ]
                }
                allowed = []
                for t in object_types:
                    allowed += mapping.get(t.upper(), [])
                df = df.where(col('TYPE').isin(allowed))
            if schema:
                df = df.where(col('schema_norm') == upper(lit(schema.strip())))
            return df.select('schema_name','object_name','schema_norm','object_norm','obj_type_norm').distinct()

    def _estimated_count_df(self, side: str, schema: str, table: str):
        creds = self._build_jdbc_urls()[side]
        engine = self._side_engine(side)
        if engine == 'azure':
            query = f"(SELECT COALESCE(SUM(p.rows),0) AS cnt FROM sys.tables t JOIN sys.partitions p ON t.object_id=p.object_id WHERE p.index_id IN (0,1) AND SCHEMA_NAME(t.schema_id)='{schema}' AND t.name='{table}') c"
        else:
            query = f"(SELECT COALESCE(CARD,0) AS cnt FROM SYSCAT.TABLES WHERE UPPER(TABSCHEMA)=UPPER('{schema}') AND UPPER(TABNAME)=UPPER('{table}')) c"
        reader = (
            self.spark.read.format('jdbc')
            .option('url', creds['url'])
            .option('dbtable', query)
            .option('driver', creds['driver'])
            .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
        )
        reader = self._apply_jdbc_auth(reader, creds)
        return reader.load()

    def _exact_count_df(self, side: str, schema: str, table: str, timeout: int):
        creds = self._build_jdbc_urls()[side]
        engine = self._side_engine(side)
        if engine == 'azure':
            query = f"(SELECT COUNT(*) AS cnt FROM [{schema}].[{table}]) c"
        else:
            s = (schema or '').upper()
            t = (table or '').upper()
            query = f"(SELECT COUNT(*) AS cnt FROM {s}.{t}) c"
        reader = (
            self.spark.read.format('jdbc')
            .option('url', creds['url'])
            .option('dbtable', query)
            .option('driver', creds['driver'])
            .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
            .option('queryTimeout', str(timeout))
        )
        reader = self._apply_jdbc_auth(reader, creds)
        return reader.load()

    def _fetch_estimated_counts_all(self, side: str, schemas: Optional[List[str]], object_types: list[str]) -> 'DataFrame':
        """Return estimated counts for all tables (and optionally views) in one JDBC read per side.
        Columns: schema_name, object_name, cnt
        """
        creds = self._build_jdbc_urls()[side]
        engine = self._side_engine(side)
        if engine == 'azure':
            # Tables from sys.tables + partitions
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{s}'" for s in schemas])
                where_schema = f" AND SCHEMA_NAME(t.schema_id) IN ({schema_list})"
            base_tables = (
                f"(SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS object_name, COALESCE(SUM(p.rows),0) AS cnt "
                f" FROM sys.tables t JOIN sys.partitions p ON t.object_id=p.object_id AND p.index_id IN (0,1) "
                f" WHERE 1=1 {where_schema} GROUP BY SCHEMA_NAME(t.schema_id), t.name) x"
            )
            reader = (
                self.spark.read.format('jdbc')
                .option('url', creds['url'])
                .option('dbtable', base_tables)
                .option('driver', creds['driver'])
                .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
            )
            reader = self._apply_jdbc_auth(reader, creds)
            df = reader.load()
            return df.select(col('schema_name'), col('object_name'), col('cnt'))
        else:
            # DB2 from SYSCAT.TABLES (CARD)
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{s}'" for s in schemas])
                where_schema = f" AND TABSCHEMA IN ({schema_list})"
            base = (
                f"(SELECT TABSCHEMA AS schema_name, TABNAME AS object_name, COALESCE(CARD,0) AS cnt "
                f" FROM SYSCAT.TABLES WHERE 1=1 {where_schema}) x"
            )
            reader = (
                self.spark.read.format('jdbc')
                .option('url', creds['url'])
                .option('dbtable', base)
                .option('driver', creds['driver'])
                .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
            )
            reader = self._apply_jdbc_auth(reader, creds)
            df = reader.load()
            return df.select(col('schema_name'), col('object_name'), col('cnt'))

    def _fetch_exact_counts_batch(
        self,
        side: str,
        items: List[tuple],
        error_details: Optional[dict[tuple[str, str], dict]] = None,
    ) -> 'DataFrame':
        """Build a UNION ALL exact-count query for a list of (schema, table) and run in one JDBC read.
        Columns: schema_name, object_name, cnt

        Handles DB2 authorization errors gracefully by executing queries individually when batch fails.
        """
        if not items:
            # Return empty DF with expected schema
            return self.spark.createDataFrame([], StructType([
                StructField('schema_name', StringType(), True),
                StructField('object_name', StringType(), True),
                StructField('cnt', LongType(), True)
            ]))

        creds = self._build_jdbc_urls()[side]
        qt = os.environ.get('JDBC_QUERY_TIMEOUT')

        # First try batch execution
        try:
            selects = []
            engine = self._side_engine(side)
            for (s, t) in items:
                if not s or not t:
                    continue
                trimmed_schema = (s or '').strip()
                trimmed_table = (t or '').strip()
                if not trimmed_schema or not trimmed_table:
                    continue
                if engine == 'azure':
                    selects.append(
                        f"SELECT '{trimmed_schema}' AS schema_name, '{trimmed_table}' AS object_name, COUNT(*) AS cnt "
                        f"FROM [{trimmed_schema}].[{trimmed_table}]"
                    )
                else:
                    # Quote DB2 identifiers to handle special chars while normalizing to upper
                    su = trimmed_schema.upper().replace('\"', '\"\"')
                    tu = trimmed_table.upper().replace('\"', '\"\"')
                    selects.append(
                        f"SELECT '{su}' AS schema_name, '{tu}' AS object_name, COUNT(*) AS cnt FROM \"{su}\".\"{tu}\""
                    )
            union_sql = " UNION ALL ".join(selects)
            wrapped = f"({union_sql}) x"
            reader = (
                self.spark.read.format('jdbc')
                .option('url', creds['url'])
                .option('dbtable', wrapped)
                .option('driver', creds['driver'])
                .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
            )
            reader = self._apply_jdbc_auth(reader, creds)
            if qt:
                reader = reader.option('queryTimeout', str(qt))
            df = reader.load()
            return df.select(col('schema_name'), col('object_name'), col('cnt'))
        except Exception as batch_ex:
            full_msg = str(batch_ex)
            display_msg = self._summarize_driver_error(full_msg)
            if self._is_access_or_object_error(full_msg):
                print(f"[DB2Auth] Batch query failed with access error, falling back to individual queries: {display_msg}")
                return self._fetch_exact_counts_individual(side, items, error_details=error_details)
            # Re-raise non-access errors
            raise

    def _fetch_exact_counts_individual(
        self,
        side: str,
        items: List[tuple],
        error_details: Optional[dict[tuple[str, str], dict]] = None,
    ) -> 'DataFrame':
        """Execute exact count queries individually, handling authorization errors gracefully.
        Returns results for successful queries and None for failed ones.
        """
        creds = self._build_jdbc_urls()[side]
        qt = os.environ.get('JDBC_QUERY_TIMEOUT')

        results = []
        for (s, t) in items:
            if not s or not t:
                continue

            engine = self._side_engine(side)
            try:
                trimmed_schema = (s or '').strip()
                trimmed_table = (t or '').strip()
                if not trimmed_schema or not trimmed_table:
                    continue
                if engine == 'azure':
                    query = (
                        f"(SELECT '{trimmed_schema}' AS schema_name, '{trimmed_table}' AS object_name, COUNT(*) AS cnt "
                        f"FROM [{trimmed_schema}].[{trimmed_table}]) x"
                    )
                else:
                    su = trimmed_schema.upper()
                    tu = trimmed_table.upper()
                    query = (
                        f"(SELECT '{su}' AS schema_name, '{tu}' AS object_name, COUNT(*) AS cnt FROM {su}.{tu}) x"
                    )

                reader = (
                    self.spark.read.format('jdbc')
                    .option('url', creds['url'])
                    .option('dbtable', query)
                    .option('driver', creds['driver'])
                    .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
                )
                reader = self._apply_jdbc_auth(reader, creds)
                if qt:
                    reader = reader.option('queryTimeout', str(qt))

                df = reader.load()
                row = df.collect()[0]
                row_dict = row.asDict(recursive=True)
                # DB2 upper-cases aliases, so fall back to the uppercase keys if needed
                schema_val = row_dict.get('schema_name') or row_dict.get('SCHEMA_NAME')
                object_val = row_dict.get('object_name') or row_dict.get('OBJECT_NAME')
                cnt_val = row_dict.get('cnt') or row_dict.get('CNT')
                results.append((schema_val, object_val, cnt_val))

            except Exception as ex:
                full_msg = str(ex)
                reason = self._classify_exact_count_error(full_msg)
                display_msg = self._summarize_driver_error(full_msg)
                self._record_exact_count_error(error_details, s, t, reason, display_msg)
                if reason == 'object_inoperative':
                    print(f"[DB2Auth] Skipping table {s}.{t}: object is inoperative (SQLCODE -575 / SQLSTATE 51024)")
                elif reason == 'authorization':
                    print(f"[DB2Auth] Skipping table {s}.{t} due to authorization error")
                elif reason == 'object_missing':
                    print(f"[DB2Auth] Skipping table {s}.{t}: object missing or invalid")
                else:
                    print(f"[DB2Auth] Skipping table {s}.{t} due to unexpected error: {display_msg}")
                results.append((s, t, None))

        # Create DataFrame from results
        schema = StructType([
            StructField('schema_name', StringType(), True),
            StructField('object_name', StringType(), True),
            StructField('cnt', LongType(), True)
        ])

        if results:
            return self.spark.createDataFrame(results, schema)
        else:
            return self.spark.createDataFrame([], schema)

    def _summarize_driver_error(self, message: str | Exception | None) -> str:
        """Return a concise single-line summary from a verbose JDBC error string."""
        if message is None:
            return ''
        text = message if isinstance(message, str) else str(message)
        if not text:
            return ''
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            return ''
        first = lines[0]
        sql_line = next((ln for ln in lines if 'SQLCODE' in ln or 'SQLSTATE' in ln), None)
        if sql_line and sql_line != first:
            return f"{first} | {sql_line}"
        return first

    def _is_access_or_object_error(self, message: str | None) -> bool:
        """Return True if the error string indicates an authorization/inoperative/missing object issue."""
        if not message:
            return False
        msg = str(message).upper()
        tokens = [
            'SQLCODE=-551', 'SQLSTATE=42501',  # authorization failures
            'SQLCODE=-552', 'SQLSTATE=42502',
            'SQLCODE=-554', 'SQLSTATE=42512',
            'SQLCODE=-575', 'SQLSTATE=51024',  # inoperative view/MQT
            'SQLCODE=-204', 'SQLSTATE=42704',  # object not found
            'NOT AUTHORIZED', 'PERMISSION', 'OBJECT DOES NOT EXIST'
        ]
        return any(tok in msg for tok in tokens)

    def _classify_exact_count_error(self, message: str) -> str:
        """Map driver error text to a coarse error category."""
        if not message:
            return 'unknown'
        msg_upper = message.upper()
        if 'SQLCODE=-575' in msg_upper or 'SQLSTATE=51024' in msg_upper:
            return 'object_inoperative'
        if 'SQLCODE=-551' in msg_upper or 'SQLSTATE=42501' in msg_upper or 'NOT AUTHORIZED' in msg_upper or 'PERMISSION' in msg_upper:
            return 'authorization'
        if 'SQLCODE=-204' in msg_upper or 'SQLSTATE=42704' in msg_upper or 'INVALID OBJECT NAME' in msg_upper or 'OBJECT DOES NOT EXIST' in msg_upper:
            return 'object_missing'
        return 'unknown'

    def _record_exact_count_error(
        self,
        store: Optional[dict[tuple[str, str], dict]],
        schema: str,
        table: str,
        reason: str,
        message: str | None,
    ) -> None:
        if store is None:
            return
        schema_key = self._norm_identifier(schema)
        table_key = self._norm_identifier(table)
        if not schema_key or not table_key:
            return
        sanitized = (message or '').replace('\n', ' ').strip()
        store[(schema_key, table_key)] = {
            'reason': reason or 'unknown',
            'message': sanitized,
        }

    def _format_exact_count_error(self, side_label: str, error_info: Optional[dict]) -> str:
        if not error_info:
            return f"{side_label} row-count query failed"
        reason = error_info.get('reason') or 'unknown'
        message = (error_info.get('message') or '').strip()
        if reason == 'object_inoperative':
            return f"{side_label} object is inoperative (SQLCODE -575 / SQLSTATE 51024)"
        if reason == 'authorization':
            return f"Authorization error on {side_label.lower()}"
        if reason == 'object_missing':
            return f"{side_label} object missing or invalid"
        detail = message or 'row-count query failed'
        return f"{side_label} row-count query failed: {detail}"

    def _describe_agg_select_target(self, select_sql: str | None) -> str:
        """Best-effort extraction of schema.table from a generated aggregation select."""
        if not select_sql:
            return 'unknown object'
        try:
            match = re.search(
                r"SELECT\s+'([^']+)'\s+AS\s+schema_name\s*,\s*'([^']+)'\s+AS\s+table_name",
                select_sql,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if match:
                schema = match.group(1).strip()
                table = match.group(2).strip()
                if schema and table:
                    return f"{schema}.{table}"
            return 'target object'
        except Exception:
            return 'target object'

    def _fetch_columns(self, side: str, schema: str, table: str) -> 'DataFrame':
        """Return columns and data types for a table. Columns: column_name, data_type"""
        creds = self._build_jdbc_urls()[side]
        if self._side_engine(side) == 'azure':
            q = (
                f"(SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type "
                f" FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}') c"
            )
        else:
            s = (schema or '').upper()
            t = (table or '').upper()
            q = (
                f"(SELECT COLNAME AS column_name, TYPENAME AS data_type "
                f" FROM SYSCAT.COLUMNS WHERE TABSCHEMA='{s}' AND TABNAME='{t}') c"
            )
        df = (
            self.spark.read.format('jdbc')
            .option('url', creds['url'])
            .option('dbtable', q)
            .option('driver', creds['driver'])
            .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
        )
        df = self._apply_jdbc_auth(df, creds).load()
        return df.select(col('column_name'), col('data_type'))

    def _fetch_columns_bulk(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        """Return all columns for schemas. Columns: schema_name, table_name, column_name, data_type"""
        creds = self._build_jdbc_urls()[side]
        if self._side_engine(side) == 'azure':
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{s}'" for s in schemas])
                where_schema = f" WHERE TABLE_SCHEMA IN ({schema_list})"
            q = (
                f"(SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, DATA_TYPE AS data_type "
                f" FROM INFORMATION_SCHEMA.COLUMNS{where_schema}) x"
            )
        else:
            where_schema = ""
            if schemas:
                # Build properly quoted, upper-cased schema list for DB2
                schema_list = ",".join([f"'{(str(s) if s is not None else '').upper()}'" for s in schemas])
                where_schema = f" WHERE TABSCHEMA IN ({schema_list})"
            q = (
                f"(SELECT TABSCHEMA AS schema_name, TABNAME AS table_name, COLNAME AS column_name, TYPENAME AS data_type "
                f" FROM SYSCAT.COLUMNS{where_schema}) x"
            )
        df = (
            self.spark.read.format('jdbc')
            .option('url', creds['url'])
            .option('dbtable', q)
            .option('driver', creds['driver'])
            .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
        )
        df = self._apply_jdbc_auth(df, creds).load()
        return df.select(col('schema_name'), col('table_name'), col('column_name'), col('data_type'))

    def _execute_agg_selects(self, side: str, selects: List[str]) -> 'DataFrame':
        """Execute UNION ALL of simple aggregation selects with adaptive batching/splitting.
        Each select must project: schema_name, table_name, column_name, total_rows, non_nulls, empties.
        Avoids DB2 SQLCODE -101 by recursively splitting batches on failure.
        """
        agg_schema = StructType([
            StructField('schema_name', StringType(), True),
            StructField('table_name', StringType(), True),
            StructField('column_name', StringType(), True),
            StructField('total_rows', LongType(), True),
            StructField('non_nulls', LongType(), True),
            StructField('empties', LongType(), True),
        ])

        def _empty_agg_df():
            return self.spark.createDataFrame([], agg_schema)

        if not selects:
            return _empty_agg_df()

        creds = self._build_jdbc_urls()[side]
        qt = os.environ.get('JDBC_QUERY_TIMEOUT')

        # Default batch sizes (smaller on DB2; allow override via env)
        try:
            if self._side_engine(side) == 'db2':
                default_batch = 20 if (sys.platform == 'win32') else 40
                batch_size = int(os.environ.get('DV_COL_AGG_BATCH_DB2', str(default_batch)))
            else:
                batch_size = int(os.environ.get('DV_COL_AGG_BATCH_AZ', '200'))
        except Exception:
            batch_size = 20 if side == 'db2' else 200

        # Helper to load a list of selects; recursively split on SQLCODE -101
        def load_selects(sel_list: List[str]) -> 'DataFrame':
            if not sel_list:
                return _empty_agg_df()
            union_sql = " UNION ALL ".join(sel_list)
            wrapped_sql = f"({union_sql}) agg"
            try:
                self._log_sql(f"agg_union_{side}", wrapped_sql)
            except Exception:
                pass
            reader = (
                self.spark.read.format('jdbc')
                .option('url', creds['url'])
                .option('dbtable', wrapped_sql)
                .option('driver', creds['driver'])
                .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
            )
            reader = self._apply_jdbc_auth(reader, creds)
            if qt:
                reader = reader.option('queryTimeout', str(qt))
            try:
                return reader.load()
            except Exception as ex:
                full_msg = str(ex)
                display_msg = self._summarize_driver_error(full_msg)
                # Handle DB2 statement too long SQLCODE -101 or timeout -952 by splitting further
                if (('SQLCODE=-101' in full_msg or '54001' in full_msg or 'SQLCODE=-952' in full_msg or '57014' in full_msg) and len(sel_list) > 1):
                    mid = max(1, len(sel_list) // 2)
                    left_df = load_selects(sel_list[:mid])
                    right_df = load_selects(sel_list[mid:])
                    from functools import reduce
                    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [left_df, right_df])
                if self._is_access_or_object_error(full_msg):
                    if len(sel_list) > 1:
                        print(f"[_execute_agg_selects][{side}] Access error in batch, retrying per-select: {display_msg}")
                        from functools import reduce
                        part_dfs = [load_selects([single]) for single in sel_list]
                        if not part_dfs:
                            return _empty_agg_df()
                        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), part_dfs)
                    target = self._describe_agg_select_target(sel_list[0] if sel_list else '')
                    print(f"[_execute_agg_selects][{side}] Skipping {target} due to access error: {display_msg}")
                    return _empty_agg_df()
                # Optional skip on timeout when no further split is possible
                if ('SQLCODE=-952' in full_msg or '57014' in full_msg) and len(sel_list) <= 1:
                    try:
                        if os.environ.get('DV_COL_AGG_TIMEOUT_SKIP', '').strip().lower() in ('1','true','yes'):
                            print(f"[_execute_agg_selects][{side}] Skipping timed-out aggregation select due to DV_COL_AGG_TIMEOUT_SKIP=1")
                            return _empty_agg_df()
                    except Exception:
                        pass
                print(f"[_execute_agg_selects][{side}] Aggregation batch failed: {display_msg}")
                raise

        # Create top-level batches by count first
        batches: List[List[str]] = [selects[i:i+batch_size] for i in range(0, len(selects), batch_size)]

        # Optional parallel load of batches
        try:
            workers = int(os.environ.get('DV_COL_AGG_WORKERS', '1'))
        except Exception:
            workers = 1
        dfs: List['DataFrame'] = []
        if workers > 1 and len(batches) > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            error_budget = int(os.environ.get('DV_COL_AGG_ERROR_BUDGET', '5'))
            errors = 0
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(load_selects, b) for b in batches]
                for f in as_completed(futures):
                    try:
                        dfs.append(f.result())
                    except Exception as ex2:
                        errors += 1
                        err_msg = self._summarize_driver_error(str(ex2))
                        print(f"[_execute_agg_selects][{side}][ERROR] {err_msg}")
                        if errors >= error_budget:
                            print(f"[_execute_agg_selects][{side}] Error budget reached; stopping early")
                            break
        else:
            for b in batches:
                dfs.append(load_selects(b))

        if not dfs:
            return _empty_agg_df()
        from functools import reduce
        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

    def _fetch_columns_nullable_bulk(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        """Return nullable metadata for columns. Columns: schema_name, table_name, column_name, is_nullable_str"""
        creds = self._build_jdbc_urls()[side]
        if self._side_engine(side) == 'azure':
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{s}'" for s in schemas])
                where_schema = f" WHERE TABLE_SCHEMA IN ({schema_list})"
            q = (
                f"(SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, IS_NULLABLE AS is_nullable_str "
                f" FROM INFORMATION_SCHEMA.COLUMNS{where_schema}) x"
            )
        else:
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{(str(s) if s is not None else '').upper()}'" for s in schemas])
                where_schema = f" WHERE TABSCHEMA IN ({schema_list})"
            q = (
                f"(SELECT TABSCHEMA AS schema_name, TABNAME AS table_name, COLNAME AS column_name, NULLS AS is_nullable_str "
                f" FROM SYSCAT.COLUMNS{where_schema}) x"
            )
        df = (
            self.spark.read.format('jdbc')
            .option('url', creds['url'])
            .option('dbtable', q)
            .option('driver', creds['driver'])
            .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
        )
        df = self._apply_jdbc_auth(df, creds).load()
        return df.select(col('schema_name'), col('table_name'), col('column_name'), col('is_nullable_str'))

    def _fetch_columns_types_bulk(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        """Return data type and size metadata per column.
        Columns: schema_name, table_name, column_name, data_type, char_len, precision, scale
        """
        creds = self._build_jdbc_urls()[side]
        if self._side_engine(side) == 'azure':
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{s}'" for s in schemas])
                where_schema = f" WHERE TABLE_SCHEMA IN ({schema_list})"
            q = (
                f"(SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, DATA_TYPE AS data_type, "
                f" CAST(CHARACTER_MAXIMUM_LENGTH AS BIGINT) AS char_len, CAST(NUMERIC_PRECISION AS BIGINT) AS precision, CAST(NUMERIC_SCALE AS BIGINT) AS scale "
                f" FROM INFORMATION_SCHEMA.COLUMNS{where_schema}) x"
            )
        else:
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{(str(s) if s is not None else '').upper()}'" for s in schemas])
                where_schema = f" WHERE TABSCHEMA IN ({schema_list})"
            # LENGTH in DB2: for character types it's length, for DECIMAL it's precision; SCALE is scale
            q = (
                f"(SELECT TABSCHEMA AS schema_name, TABNAME AS table_name, COLNAME AS column_name, TYPENAME AS data_type, "
                f" CAST(LENGTH AS BIGINT) AS len, CAST(SCALE AS BIGINT) AS scale "
                f" FROM SYSCAT.COLUMNS{where_schema}) x"
            )
        df = (
            self.spark.read.format('jdbc')
            .option('url', creds['url'])
            .option('dbtable', q)
            .option('driver', creds['driver'])
            .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
        )
        df = self._apply_jdbc_auth(df, creds).load()
        if self._side_engine(side) == 'azure':
            return df.select('schema_name','table_name','column_name','data_type','char_len','precision','scale')
        else:
            # Derive char_len vs precision from DB2 len by type family
            return df.select(
                col('schema_name'), col('table_name'), col('column_name'), col('data_type'),
                when(upper(trim(col('data_type'))).isin('CHAR','CHARACTER','VARCHAR','GRAPHIC','VARGRAPHIC','CLOB'), col('len')).otherwise(lit(None).cast('bigint')).alias('char_len'),
                when(upper(trim(col('data_type'))).isin('DECIMAL','NUMERIC','DECFLOAT'), col('len')).otherwise(lit(None).cast('bigint')).alias('precision'),
                col('scale')
            )

    def _fetch_columns_defaults_bulk(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        """Return default expressions per column. Columns: schema_name, table_name, column_name, default_str"""
        creds = self._build_jdbc_urls()[side]
        if self._side_engine(side) == 'azure':
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{s}'" for s in schemas])
                where_schema = f" AND s.name IN ({schema_list})"
            # Prefer sys catalogs to reliably fetch default constraint definitions when INFORMATION_SCHEMA is null
            q = (
                f"("
                f" SELECT s.name AS schema_name, t.name AS table_name, c.name AS column_name, "
                f"        CAST(dc.definition AS NVARCHAR(4000)) AS default_str "
                f" FROM sys.columns c "
                f" JOIN sys.tables t ON c.object_id = t.object_id "
                f" JOIN sys.schemas s ON t.schema_id = s.schema_id "
                f" LEFT JOIN sys.default_constraints dc "
                f"   ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id "
                f" WHERE 1=1{where_schema}"
                f") x"
            )
        else:
            where_schema = ""
            if schemas:
                schema_list = ",".join([f"'{(str(s) if s is not None else '').upper()}'" for s in schemas])
                where_schema = f" WHERE TABSCHEMA IN ({schema_list})"
            q = (
                f"(SELECT TABSCHEMA AS schema_name, TABNAME AS table_name, COLNAME AS column_name, DEFAULT AS default_str "
                f" FROM SYSCAT.COLUMNS{where_schema}) x"
            )
        df = (
            self.spark.read.format('jdbc')
            .option('url', creds['url'])
            .option('dbtable', q)
            .option('driver', creds['driver'])
            .option('fetchsize', os.environ.get('JDBC_FETCHSIZE', '50000'))
        )
        df = self._apply_jdbc_auth(df, creds).load()
        return df.select(col('schema_name'), col('table_name'), col('column_name'), col('default_str'))

    def _fetch_db2_index_cols(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        if self._side_engine(side) != 'db2':
            raise ValueError(f"_fetch_db2_index_cols called for non-DB2 side '{side}'")
        creds = self._build_jdbc_urls()[side]
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{(str(s) if s is not None else '').upper()}'" for s in schemas])
            where_schema = f" AND i.tabschema IN ({schema_list})"
        q = (
            "(SELECT i.tabschema AS schema_name, i.tabname AS table_name, i.indname AS idx_name, i.uniquerule AS unique_rule, "
            " ic.colseq AS colseq, ic.colname AS col_name, ic.colorder AS colorder "
            " FROM SYSCAT.INDEXES i JOIN SYSCAT.INDEXCOLUSE ic ON i.indschema=ic.indschema AND i.indname=ic.indname "
            f" WHERE 1=1{where_schema}) x"
        )
        df = (self.spark.read.format('jdbc')
              .option('url', creds['url']).option('dbtable', q)
              .option('driver', creds['driver']).option('fetchsize', os.environ.get('JDBC_FETCHSIZE','50000')))
        df = self._apply_jdbc_auth(df, creds).load()
        return df

    def _fetch_sql_index_cols(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        if self._side_engine(side) != 'azure':
            raise ValueError(f"_fetch_sql_index_cols called for non-Azure side '{side}'")
        creds = self._build_jdbc_urls()[side]
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{s}'" for s in schemas])
            where_schema = f" AND s.name IN ({schema_list})"
        q = (
            "(SELECT s.name AS schema_name, t.name AS table_name, i.name AS idx_name, i.is_unique, i.is_primary_key, ic.key_ordinal AS colseq, c.name AS col_name, ic.is_descending_key "
            " FROM sys.indexes i JOIN sys.tables t ON i.object_id=t.object_id JOIN sys.schemas s ON t.schema_id=s.schema_id "
            " JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id "
            " JOIN sys.columns c ON c.object_id=t.object_id AND c.column_id=ic.column_id "
            f" WHERE i.is_hypothetical=0 AND i.name IS NOT NULL{where_schema}) x"
        )
        df = (self.spark.read.format('jdbc')
              .option('url', creds['url']).option('dbtable', q)
              .option('driver', creds['driver']).option('fetchsize', os.environ.get('JDBC_FETCHSIZE','50000')))
        df = self._apply_jdbc_auth(df, creds).load()
        return df

    def _build_index_metadata(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        engine = self._side_engine(side)
        if engine == 'azure':
            df = self._fetch_sql_index_cols(side, schemas)
            df = df.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                   .withColumn('table_norm', upper(trim(col('table_name')))) \
                   .withColumn('idx_norm', upper(trim(col('idx_name')))) \
                   .withColumn('Kind', when(col('is_primary_key') == lit(True), lit('PK'))
                                       .when(col('is_unique') == lit(True), lit('UNIQUE'))
                                       .otherwise(lit('INDEX'))) \
                   .withColumn('IsUnique', col('is_primary_key') | col('is_unique')) \
                   .withColumn('col_order', when(col('is_descending_key') == lit(True), lit('D')).otherwise(lit('A')))
        else:
            df = self._fetch_db2_index_cols(side, schemas)
            df = df.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                   .withColumn('table_norm', upper(trim(col('table_name')))) \
                   .withColumn('idx_norm', upper(trim(col('idx_name')))) \
                   .withColumn('Kind', when(upper(trim(col('unique_rule'))) == lit('P'), lit('PK'))
                                       .when(upper(trim(col('unique_rule'))) == lit('U'), lit('UNIQUE'))
                                       .otherwise(lit('INDEX'))) \
                   .withColumn('IsUnique', upper(trim(col('unique_rule'))).isin('P','U')) \
                   .withColumn('col_order', upper(trim(col('colorder'))))
        return df

    @log_duration("compare_index_definitions")
    def compare_index_definitions(
        self,
        source_schema: str | None,
        target_schema: str | None,
        object_types: list[str] | None
    ) -> 'DataFrame':
        """Validate PK/UNIQUE/INDEX definitions: same names, columns (order), uniqueness. Outputs only mismatches or missing."""
        if not object_types:
            object_types = ['TABLE']
        # Matched tables
        l = self._read_tables('db2', object_types, source_schema)
        r = self._read_tables('azure', object_types, target_schema)
        if not source_schema and not target_schema:
            l_s = l.select('schema_norm').distinct().withColumnRenamed('schema_norm','s')
            r_s = r.select('schema_norm').distinct().withColumnRenamed('schema_norm','t')
            matched_s = l_s.join(r_s, col('s') == col('t'), 'inner').select(col('s').alias('schema_norm'))
            l = l.join(matched_s, 'schema_norm', 'inner')
            r = r.join(matched_s, 'schema_norm', 'inner')
        pairs = l.select('schema_name','object_name','schema_norm','object_norm').alias('l').join(
            r.select(col('schema_name').alias('r_schema'), col('object_name').alias('r_object'), col('schema_norm').alias('r_schema_norm'), col('object_norm').alias('r_object_norm')).alias('r'),
            on=(col('l.schema_norm') == col('r.r_schema_norm')) & (col('l.object_norm') == col('r.r_object_norm')),
            how='inner'
        ).select(
            col('l.schema_name').alias('SourceSchemaName'), col('l.object_name').alias('SourceObjectName'),
            col('r_schema').alias('DestinationSchemaName'), col('r_object').alias('DestinationObjectName'),
            upper(trim(col('l.schema_name'))).alias('s_schema_norm'), upper(trim(col('l.object_name'))).alias('s_object_norm'),
            upper(trim(col('r_schema'))).alias('r_schema_norm'), upper(trim(col('r_object'))).alias('r_object_norm')
        )

        # Fetch index columns
        s_schemas = [r['SourceSchemaName'] for r in pairs.select('SourceSchemaName').distinct().collect()]
        r_schemas = [r['DestinationSchemaName'] for r in pairs.select('DestinationSchemaName').distinct().collect()]
        db2_ix = self._build_index_metadata('db2', s_schemas)
        az_ix = self._build_index_metadata('azure', r_schemas)

        # Column counts for "many columns" note (info only)
        try:
            many_col_threshold = int(os.environ.get('DV_MANY_COLUMNS_THRESHOLD', '120'))
        except Exception:
            many_col_threshold = 120

        l_cols = self._fetch_columns_bulk('db2', s_schemas)
        r_cols = self._fetch_columns_bulk('azure', r_schemas)
        l_cnt = l_cols.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                      .withColumn('table_norm', upper(trim(col('table_name')))) \
                      .groupBy('schema_norm','table_norm').count().withColumnRenamed('count','SourceColumnCount')
        r_cnt = r_cols.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                      .withColumn('table_norm', upper(trim(col('table_name')))) \
                      .groupBy('schema_norm','table_norm').count().withColumnRenamed('count','DestinationColumnCount')

        # Restrict to matched tables
        db2_f = db2_ix.join(pairs.select(col('s_schema_norm').alias('schema_norm'), col('s_object_norm').alias('table_norm')), on=['schema_norm','table_norm'], how='inner')
        az_f = az_ix.join(pairs.select(col('r_schema_norm').alias('schema_norm'), col('r_object_norm').alias('table_norm')), on=['schema_norm','table_norm'], how='inner')

        # Build column signature per index (ordered by colseq)
        from pyspark.sql.window import Window
        w = Window.partitionBy('schema_norm','table_norm','idx_norm').orderBy(col('colseq').asc_nulls_last())
        # Create ordered list by aggregating after sorting is not straightforward; create seq-ordered rows then aggregate
        db2_cols = db2_f.select('schema_norm','table_norm','idx_norm','Kind','IsUnique','colseq','col_name', col('col_order').alias('ord'))
        az_cols = az_f.select('schema_norm','table_norm','idx_norm','Kind','IsUnique','colseq','col_name', col('col_order').alias('ord'))

        # Aggregate to signature strings
        def agg_sig(df: 'DataFrame') -> 'DataFrame':
            return df.groupBy('schema_norm','table_norm','idx_norm','Kind','IsUnique') \
                     .agg(concat_ws(',', collect_list(concat(col('col_name'), lit(' '), col('ord')))).alias('ColsSig'))

        from pyspark.sql.functions import collect_set, sort_array, collect_list
        db2_sig = agg_sig(db2_cols)
        az_sig = agg_sig(az_cols)

        # Join by table + index name + kind
        joined = db2_sig.alias('l').join(
            az_sig.alias('r'), on=[col('l.schema_norm')==col('r.schema_norm'), col('l.table_norm')==col('r.table_norm'), col('l.idx_norm')==col('r.idx_norm'), col('l.Kind')==col('r.Kind')], how='full_outer'
        ).select(
            coalesce(col('l.schema_norm'), col('r.schema_norm')).alias('schema_norm'),
            coalesce(col('l.table_norm'), col('r.table_norm')).alias('table_norm'),
            coalesce(col('l.idx_norm'), col('r.idx_norm')).alias('IndexName'),
            coalesce(col('l.Kind'), col('r.Kind')).alias('Kind'),
            col('l.IsUnique').alias('SourceUnique'), col('r.IsUnique').alias('DestinationUnique'),
            col('l.ColsSig').alias('SourceCols'), col('r.ColsSig').alias('DestinationCols')
        )

        # Build signature-based matches ignoring index names (definition+kind+uniqueness must match)
        sig_pairs = db2_sig.select('schema_norm','table_norm','Kind','IsUnique','ColsSig', col('idx_norm').alias('l_idx')).alias('l').join(
            az_sig.select('schema_norm','table_norm','Kind','IsUnique','ColsSig', col('idx_norm').alias('r_idx')).alias('r'),
            on=[col('l.schema_norm')==col('r.schema_norm'), col('l.table_norm')==col('r.table_norm'), col('l.Kind')==col('r.Kind'), col('l.IsUnique')==col('r.IsUnique'), col('l.ColsSig')==col('r.ColsSig')],
            how='inner'
        ).select(col('l.schema_norm').alias('schema_norm'), col('l.table_norm').alias('table_norm'), col('l.l_idx').alias('l_idx'), col('r.r_idx').alias('r_idx'))

        # Attach original names
        joined2 = joined.join(
            pairs.select('s_schema_norm','s_object_norm','SourceSchemaName','SourceObjectName','DestinationSchemaName','DestinationObjectName'),
            on=[joined.schema_norm == col('s_schema_norm'), joined.table_norm == col('s_object_norm')], how='left'
        )

        # Determine mismatches/missing
        out = joined2.withColumn('MissingInSource', col('SourceCols').isNull()) \
                     .withColumn('MissingInTarget', col('DestinationCols').isNull()) \
                     .withColumn('ColsMatch', (col('SourceCols') == col('DestinationCols'))) \
                     .withColumn('UniqueMatch', (coalesce(col('SourceUnique'), lit(False)) == coalesce(col('DestinationUnique'), lit(False))))

        # Mask out name-only differences when a signature match exists on the other side
        left_mask = out.join(sig_pairs.alias('sp'), on=[out.schema_norm == col('sp.schema_norm'), out.table_norm == col('sp.table_norm'), out.IndexName == col('sp.l_idx')], how='left') \
                       .select(out['*'], col('sp.r_idx').isNotNull().alias('HasSigRight'))
        both_mask = left_mask.join(sig_pairs.alias('sp2'), on=[left_mask.schema_norm == col('sp2.schema_norm'), left_mask.table_norm == col('sp2.table_norm'), left_mask.IndexName == col('sp2.r_idx')], how='left') \
                              .select(left_mask['*'], col('sp2.l_idx').isNotNull().alias('HasSigLeft'))
        out = both_mask.withColumn('MaskBySig', when(col('MissingInTarget') & col('HasSigRight'), lit(True))
                                             .when(col('MissingInSource') & col('HasSigLeft'), lit(True))
                                             .otherwise(lit(False)))

        out = out.where(((col('MissingInSource')) | (col('MissingInTarget')) | (~col('ColsMatch')) | (~col('UniqueMatch')))
                        & (~col('MaskBySig')))

        # Dynamic rules for indexes
        idx_rules = self._parse_validation_rules('DV_INDEX_RULES')
        order_insensitive_warn = any((str(r.get('rule_type','')).lower() == 'column_order_insensitive') and (str(r.get('match_type','')).lower() == 'warning') for r in idx_rules)
        missing_sysname_warn = any((str(r.get('rule_type','')).lower() == 'missing_sysname') and (str(r.get('match_type','')).lower() == 'warning') for r in idx_rules)
        from pyspark.sql.functions import split, regexp_replace as rxr, array_sort, transform, startswith
        s_cols_arr = array_sort(transform(split(coalesce(col('SourceCols'), lit('')), ','), lambda x: upper(trim(rxr(x, r"\\s+", "")))))
        d_cols_arr = array_sort(transform(split(coalesce(col('DestinationCols'), lit('')), ','), lambda x: upper(trim(rxr(x, r"\\s+", "")))))
        out = out.withColumn('ColsSetEqual', s_cols_arr == d_cols_arr)
        # Assign Status based on rules:
        # 1) Column order-insensitive warning
        warn_order = ((~col('ColsMatch')) & col('ColsSetEqual') & lit(bool(order_insensitive_warn)))
        # 2) Missing on one side with system-generated or normalized name patterns (SQL* or PK_*)
        idx_name_uc = upper(coalesce(col('IndexName'), lit('')))
        warn_sysname = (
            ((col('MissingInTarget') | col('MissingInSource')) &
             (idx_name_uc.startswith(lit('SQL')) | idx_name_uc.startswith(lit('PK'))) &
             lit(bool(missing_sysname_warn)))
        )
        out = out.withColumn('Status', when(warn_order | warn_sysname, lit('warning')).otherwise(lit('error')))

        result_idx = out.select(
            lit('Index').alias('ValidationType'),
            col('Kind').alias('ObjectType'),
            col('SourceSchemaName'), col('SourceObjectName'),
            col('DestinationSchemaName'), col('DestinationObjectName'),
            col('IndexName'),
            col('SourceUnique'), col('DestinationUnique'),
            col('SourceCols'), col('DestinationCols'),
            col('Status'),
            concat(trim(col('SourceSchemaName')), lit('.'), trim(col('SourceObjectName')), lit('.'), trim(col('IndexName'))).alias('ElementPath'),
            when(col('MissingInSource'), lit('Index missing in source'))
             .when(col('MissingInTarget'), lit('Index missing in target'))
             .when((~col('ColsMatch')) & col('ColsSetEqual'), lit('Index column order mismatch'))
             .when(~col('ColsMatch'), lit('Index columns mismatch'))
             .when(~col('UniqueMatch'), lit('Index uniqueness mismatch'))
             .otherwise(lit('')).alias('ErrorDescription')
        )

        # --- Primary key presence check (table-level) ---
        db2_pk = db2_ix.where(col('Kind') == lit('PK')).select('schema_norm','table_norm').distinct()
        az_pk = az_ix.where(col('Kind') == lit('PK')).select('schema_norm','table_norm').distinct()

        pk_presence = pairs.select(
            col('SourceSchemaName'), col('SourceObjectName'),
            col('DestinationSchemaName'), col('DestinationObjectName'),
            col('s_schema_norm').alias('schema_norm'),
            col('s_object_norm').alias('table_norm')
        ).join(
            db2_pk.withColumnRenamed('schema_norm','pk_schema').withColumnRenamed('table_norm','pk_table'),
            on=[col('schema_norm') == col('pk_schema'), col('table_norm') == col('pk_table')],
            how='left'
        ).withColumn('HasPkSource', col('pk_schema').isNotNull()).drop('pk_schema','pk_table').join(
            az_pk.withColumnRenamed('schema_norm','pk_schema').withColumnRenamed('table_norm','pk_table'),
            on=[col('schema_norm') == col('pk_schema'), col('table_norm') == col('pk_table')],
            how='left'
        ).withColumn('HasPkTarget', col('pk_schema').isNotNull()).drop('pk_schema','pk_table')

        pk_missing = pk_presence.where(col('HasPkSource') != col('HasPkTarget')).select(
            lit('PrimaryKey').alias('ValidationType'),
            lit('PK').alias('ObjectType'),
            col('SourceSchemaName'), col('SourceObjectName'),
            col('DestinationSchemaName'), col('DestinationObjectName'),
            lit('PRIMARY KEY').alias('IndexName'),
            lit(True).alias('SourceUnique'),
            lit(True).alias('DestinationUnique'),
            lit('').alias('SourceCols'),
            lit('').alias('DestinationCols'),
            lit('error').alias('Status'),
            concat(trim(col('SourceSchemaName')), lit('.'), trim(col('SourceObjectName')), lit('.'), lit('PRIMARY KEY')).alias('ElementPath'),
            when(col('HasPkSource') & ~col('HasPkTarget'), lit('Primary key missing in target'))
             .when(~col('HasPkSource') & col('HasPkTarget'), lit('Primary key missing in source'))
             .otherwise(lit('Primary key presence mismatch')).alias('ErrorDescription')
        )

        pk_absent_both = pk_presence.where(~col('HasPkSource') & ~col('HasPkTarget')).select(
            lit('PrimaryKey').alias('ValidationType'),
            lit('PK').alias('ObjectType'),
            col('SourceSchemaName'), col('SourceObjectName'),
            col('DestinationSchemaName'), col('DestinationObjectName'),
            lit('PRIMARY KEY').alias('IndexName'),
            lit(None).cast('boolean').alias('SourceUnique'),
            lit(None).cast('boolean').alias('DestinationUnique'),
            lit('').alias('SourceCols'),
            lit('').alias('DestinationCols'),
            lit('info').alias('Status'),
            concat(trim(col('SourceSchemaName')), lit('.'), trim(col('SourceObjectName')), lit('.'), lit('PRIMARY KEY')).alias('ElementPath'),
            lit('Table has no primary key on either side (note)').alias('ErrorDescription')
        )

        p_base = pairs.select(
            col('SourceSchemaName'), col('SourceObjectName'),
            col('DestinationSchemaName'), col('DestinationObjectName'),
            col('s_schema_norm').alias('p_schema_norm'),
            col('s_object_norm').alias('p_table_norm')
        )
        lc = l_cnt.alias('lc')
        rc = r_cnt.alias('rc')
        many_cols = p_base.alias('p') \
            .join(lc, on=[col('p.p_schema_norm') == col('lc.schema_norm'), col('p.p_table_norm') == col('lc.table_norm')], how='left') \
            .drop(col('lc.schema_norm')).drop(col('lc.table_norm')) \
            .join(rc, on=[col('p.p_schema_norm') == col('rc.schema_norm'), col('p.p_table_norm') == col('rc.table_norm')], how='left') \
            .drop(col('rc.schema_norm')).drop(col('rc.table_norm')) \
            .withColumnRenamed('p_schema_norm', 'schema_norm') \
            .withColumnRenamed('p_table_norm', 'table_norm')
        many_cols = many_cols.withColumn('SourceColumnCount', coalesce(col('SourceColumnCount'), lit(0).cast('long'))) \
                             .withColumn('DestinationColumnCount', coalesce(col('DestinationColumnCount'), lit(0).cast('long'))) \
                             .withColumn('MaxColumns', when(col('SourceColumnCount') > col('DestinationColumnCount'), col('SourceColumnCount')).otherwise(col('DestinationColumnCount'))) \
                             .where(col('MaxColumns') >= lit(many_col_threshold))
        many_cols = many_cols.select(
            lit('TableSize').alias('ValidationType'),
            lit('TABLE').alias('ObjectType'),
            col('SourceSchemaName'), col('SourceObjectName'),
            col('DestinationSchemaName'), col('DestinationObjectName'),
            lit('').alias('IndexName'),
            lit(None).cast('boolean').alias('SourceUnique'),
            lit(None).cast('boolean').alias('DestinationUnique'),
            col('SourceColumnCount').cast('string').alias('SourceCols'),
            col('DestinationColumnCount').cast('string').alias('DestinationCols'),
            lit('info').alias('Status'),
            concat(trim(col('SourceSchemaName')), lit('.'), trim(col('SourceObjectName'))).alias('ElementPath'),
            concat(lit('High column count (>= '), lit(many_col_threshold).cast('string'), lit(')')).alias('ErrorDescription')
        )

    def _fetch_db2_fk_cols(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        if self._side_engine(side) == 'azure':
            return self._fetch_sql_fk_cols(side, schemas)
        creds = self._build_jdbc_urls()[side]
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{(str(s) if s is not None else '').upper()}'" for s in schemas])
            where_schema = f" AND r.tabschema IN ({schema_list})"
        q = (
            "(SELECT r.tabschema AS schema_name, r.tabname AS table_name, r.constname AS fk_name, "
            " r.reftabschema AS ref_schema_name, r.reftabname AS ref_table_name, r.deleterule, r.updaterule, "
            " kc.colseq AS colseq, kc.colname AS col_name, pkc.colname AS ref_col_name "
            " FROM SYSCAT.REFERENCES r "
            " JOIN SYSCAT.KEYCOLUSE kc ON kc.tabschema=r.tabschema AND kc.tabname=r.tabname AND kc.constname=r.constname "
            " JOIN SYSCAT.KEYCOLUSE pkc ON pkc.tabschema=r.reftabschema AND pkc.tabname=r.reftabname AND pkc.constname=r.refkeyname AND pkc.colseq=kc.colseq "
            f" WHERE 1=1{where_schema}) x"
        )
        reader = (self.spark.read.format('jdbc')
                .option('url', creds['url']).option('dbtable', q)
                .option('driver', creds['driver']).option('fetchsize', os.environ.get('JDBC_FETCHSIZE','50000')))
        return self._apply_jdbc_auth(reader, creds).load()

    def _fetch_sql_fk_cols(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        if self._side_engine(side) != 'azure':
            raise ValueError(f"_fetch_sql_fk_cols called for non-Azure side '{side}'")
        creds = self._build_jdbc_urls()[side]
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{s}'" for s in schemas])
            where_schema = f" AND s.name IN ({schema_list})"
        q = (
            "(SELECT s.name AS schema_name, t.name AS table_name, fk.name AS fk_name, "
            " rs.name AS ref_schema_name, rt.name AS ref_table_name, "
            " fk.delete_referential_action_desc AS delete_action, fk.update_referential_action_desc AS update_action, "
            " fkc.constraint_column_id AS colseq, pc.name AS col_name, rc.name AS ref_col_name "
            " FROM sys.foreign_keys fk "
            " JOIN sys.tables t ON fk.parent_object_id=t.object_id "
            " JOIN sys.schemas s ON t.schema_id=s.schema_id "
            " JOIN sys.tables rt ON fk.referenced_object_id=rt.object_id "
            " JOIN sys.schemas rs ON rt.schema_id=rs.schema_id "
            " JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id=fk.object_id "
            " JOIN sys.columns pc ON pc.object_id=t.object_id AND pc.column_id=fkc.parent_column_id "
            " JOIN sys.columns rc ON rc.object_id=rt.object_id AND rc.column_id=fkc.referenced_column_id "
            f" WHERE fk.is_ms_shipped=0{where_schema}) x"
        )
        reader = (self.spark.read.format('jdbc')
                .option('url', creds['url']).option('dbtable', q)
                .option('driver', creds['driver']).option('fetchsize', os.environ.get('JDBC_FETCHSIZE','50000')))
        return self._apply_jdbc_auth(reader, creds).load()

    def _build_fk_metadata(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        engine = self._side_engine(side)
        if engine == 'azure':
            df = self._fetch_sql_fk_cols(side, schemas)
            df = df.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                   .withColumn('table_norm', upper(trim(col('table_name')))) \
                   .withColumn('fk_norm', upper(trim(col('fk_name')))) \
                   .withColumn('ref_schema_norm', upper(trim(col('ref_schema_name')))) \
                   .withColumn('ref_table_norm', upper(trim(col('ref_table_name')))) \
                   .withColumn('DeleteAction', upper(trim(col('delete_action')))) \
                   .withColumn('UpdateAction', upper(trim(col('update_action'))))
        else:
            df = self._fetch_db2_fk_cols(side, schemas)
            df = df.withColumn('schema_norm', upper(trim(col('schema_name')))) \
                   .withColumn('table_norm', upper(trim(col('table_name')))) \
                   .withColumn('fk_norm', upper(trim(col('fk_name')))) \
                   .withColumn('ref_schema_norm', upper(trim(col('ref_schema_name')))) \
                   .withColumn('ref_table_norm', upper(trim(col('ref_table_name')))) \
                   .withColumn('DeleteAction', upper(trim(col('deleterule')))) \
                   .withColumn('UpdateAction', upper(trim(col('updaterule'))))
        return df

    def _fetch_db2_check_constraints(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        if self._side_engine(side) == 'azure':
            return self._fetch_sql_check_constraints(side, schemas)
        creds = self._build_jdbc_urls()[side]
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{(str(s) if s is not None else '').upper()}'" for s in schemas])
            where_schema = f" AND tabschema IN ({schema_list})"
        q = (
            "(SELECT tabschema AS schema_name, tabname AS table_name, constname AS chk_name, text AS chk_def "
            " FROM SYSCAT.CHECKS "
            f" WHERE 1=1{where_schema}) x"
        )
        reader = (self.spark.read.format('jdbc')
                .option('url', creds['url']).option('dbtable', q)
                .option('driver', creds['driver']).option('fetchsize', os.environ.get('JDBC_FETCHSIZE','50000')))
        return self._apply_jdbc_auth(reader, creds).load()

    def _fetch_sql_check_constraints(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        if self._side_engine(side) != 'azure':
            raise ValueError(f"_fetch_sql_check_constraints called for non-Azure side '{side}'")
        creds = self._build_jdbc_urls()[side]
        where_schema = ""
        if schemas:
            schema_list = ",".join([f"'{s}'" for s in schemas])
            where_schema = f" AND s.name IN ({schema_list})"
        q = (
            "(SELECT s.name AS schema_name, t.name AS table_name, cc.name AS chk_name, cc.definition AS chk_def "
            " FROM sys.check_constraints cc "
            " JOIN sys.objects o ON o.object_id = cc.parent_object_id "
            " JOIN sys.tables t ON t.object_id = o.object_id "
            " JOIN sys.schemas s ON s.schema_id = t.schema_id "
            f" WHERE o.type='U'{where_schema}) x"
        )
        reader = (self.spark.read.format('jdbc')
                .option('url', creds['url']).option('dbtable', q)
                .option('driver', creds['driver']).option('fetchsize', os.environ.get('JDBC_FETCHSIZE','50000')))
        return self._apply_jdbc_auth(reader, creds).load()

    def _build_check_metadata(self, side: str, schemas: Optional[List[str]] | None) -> 'DataFrame':
        engine = self._side_engine(side)
        if engine == 'azure':
            df = self._fetch_sql_check_constraints(side, schemas)
        else:
            df = self._fetch_db2_check_constraints(side, schemas)
        df = df.withColumn('schema_norm', upper(trim(col('schema_name')))) \
               .withColumn('table_norm', upper(trim(col('table_name')))) \
               .withColumn('chk_norm', upper(trim(col('chk_name'))))
        return df
    @log_duration("check_constraint_integrity")
    def check_constraint_integrity(
        self,
        source_schema: str | None,
        target_schema: str | None,
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to PySparkDataValidationService.check_constraint_integrity")

    @log_duration("compare_column_nulls")
    def compare_column_nulls(
        self,
        source_schema: str | None,
        target_schema: str | None,
        object_types: list[str] | None,
        only_when_rowcount_matches: bool,
        output_only_issues: bool
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to PySparkDataValidationService.compare_column_nulls")



    @log_duration("compare_identity_sequences")
    def compare_identity_sequences(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] | None
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to BehaviorValidationService.compare_identity_sequences")

    @log_duration("compare_sequence_definitions")
    def compare_sequence_definitions(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str]
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to BehaviorValidationService.compare_sequence_definitions")

    @log_duration("compare_trigger_definitions")
    def compare_trigger_definitions(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str]
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to BehaviorValidationService.compare_trigger_definitions")

    @log_duration("compare_routine_definitions")
    def compare_routine_definitions(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] | None
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to BehaviorValidationService.compare_routine_definitions")

    @log_duration("compare_extended_properties")
    def compare_extended_properties(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str]
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to BehaviorValidationService.compare_extended_properties")

    @log_duration("compare_collation_encoding")
    def compare_collation_encoding(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str]
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to BehaviorValidationService.compare_collation_encoding")

    @log_duration("compare_permissions_roles")
    def compare_permissions_roles(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str]
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to BehaviorValidationService.compare_permissions_roles")

    @log_duration("validate_behavior_all")
    def validate_behavior_all(
        self,
        source_schema: Optional[str],
        target_schema: Optional[str],
        object_types: Optional[List[str]] | None
    ) -> 'DataFrame':
        raise NotImplementedError("Moved to BehaviorValidationService.validate_behavior_all")


    def stop_spark(self):
        """Stop Spark session"""
        try:
            sp = getattr(self, "spark", None)
            if sp is None:
                return
            try:
                # Reduce log level to hide WARN messages during stop on Windows
                sp.sparkContext.setLogLevel("ERROR")
            except Exception:
                pass
            try:
                sp.catalog.clearCache()
            except Exception:
                pass
            try:
                sp.stop()
            except Exception as ex_stop:
                print(f"[SparkShutdown][WARN] stop() raised: {ex_stop}")
        finally:
            # Drop instance attribute to release reference
            try:
                del self.spark
            except Exception:
                pass


class PySparkAzureSchemaComparisonService(PySparkSchemaComparisonService):
    """Specialized PySpark service for Azure-to-Azure comparisons."""

    def __init__(self, *, access_token_override: Optional[str] = None, **_: Any):
        super().__init__(
            config_filename="azure_database_config.json",
            side_config_map={'db2': 'source_azure_sql', 'azure': 'target_azure_sql'},
            side_labels={'db2': 'Source Azure SQL', 'azure': 'Target Azure SQL'},
            side_engine_map={'db2': 'azure', 'azure': 'azure'},
            app_name="Azure_Azure_Schema_Comparison",
            access_token_override=access_token_override,
        )
