# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Subprocess entry point for legacy data validation.
Runs validation in a separate process so the JVM (DB2/JDBC) does not block the main process UI.
No GUI imports here; safe to run as multiprocessing.Process target.
"""

import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure package is on path when run as subprocess
_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


def run_legacy_data_validation_subprocess(
    queue: "Any",  # multiprocessing.Queue
    config: Dict[str, Any],
    output_dir: str,
    source_schema: Optional[str],
    target_schema: Optional[str],
    run_row_counts: bool,
    run_null_empty: bool,
    run_distinct_key: bool,
) -> None:
    """
    Run full legacy data validation in this process. Put progress and results on queue.
    Messages: ("progress", step, current, total, schema, table), ("result", step, df), ("done", None), ("error", str).
    """
    try:
        from azure_migration_tool.validation.data_service import LegacyDataValidationService
    except ImportError:
        from validation.data_service import LegacyDataValidationService

    service = LegacyDataValidationService(config=config, output_dir=output_dir)
    object_types = ["TABLE"]
    total_steps = sum([run_row_counts, run_null_empty, run_distinct_key])
    current_step = 0
    results = {}
    validation_results = {}

    def put_progress(step: str, current: int, total: int, schema: str, table: str) -> None:
        try:
            queue.put(("progress", step, current, total, schema, table))
        except Exception:
            pass

    try:
        if run_row_counts:
            current_step += 1
            put_progress("row_counts", 0, 0, "", "")
            def prog(c, t, s, o):
                put_progress("row_counts", c, t, s or "", o or "")
            try:
                df = service.compare_row_counts(
                    source_schema=source_schema,
                    target_schema=target_schema,
                    object_types=object_types,
                    progress_callback=prog,
                )
                if df is None:
                    import pandas as pd
                    df = pd.DataFrame()
                queue.put(("result", "row_counts", df))
                validation_results["row_counts"] = df
            except Exception as e:
                import traceback
                import pandas as pd
                error_msg = f"Error in row_counts validation: {str(e)}\n{traceback.format_exc()}"
                queue.put(("result", "row_counts", pd.DataFrame()))
                validation_results["row_counts"] = pd.DataFrame()

        if run_null_empty:
            current_step += 1
            put_progress("null_values", 0, 0, "", "")
            def prog(c, t, s, o):
                put_progress("null_values", c, t, s or "", o or "")
            try:
                df = service.compare_column_nulls(
                    source_schema=source_schema,
                    target_schema=target_schema,
                    object_types=object_types,
                    only_when_rowcount_matches=True,
                    output_only_issues=False,
                    progress_callback=prog,
                )
                if df is None:
                    import pandas as pd
                    df = pd.DataFrame()
                queue.put(("result", "null_values", df))
                validation_results["null_values"] = df
            except Exception as e:
                import traceback
                import pandas as pd
                error_msg = f"Error in null_values validation: {str(e)}\n{traceback.format_exc()}"
                queue.put(("result", "null_values", pd.DataFrame()))
                validation_results["null_values"] = pd.DataFrame()

        if run_distinct_key:
            current_step += 1
            put_progress("distinct_key", 0, 0, "", "")
            try:
                from azure_migration_tool.validation.schema_service import LegacySchemaValidationService
            except ImportError:
                from validation.schema_service import LegacySchemaValidationService
            try:
                schema_service = LegacySchemaValidationService(config=config, output_dir=output_dir)
                df = schema_service.compare_index_definitions(
                    source_schema=source_schema,
                    target_schema=target_schema,
                    object_types=object_types,
                )
                if df is None:
                    import pandas as pd
                    df = pd.DataFrame()
                if "ValidationType" in df.columns and "Status" in df.columns:
                    pk_absent_df = df[(df["ValidationType"] == "PrimaryKey") & (df["Status"] == "info")].copy()
                else:
                    pk_absent_df = df.iloc[0:0].copy() if not df.empty else df
                queue.put(("result", "distinct_key", pk_absent_df))
                validation_results["distinct_key"] = pk_absent_df
            except Exception as e:
                import traceback
                import pandas as pd
                error_msg = f"Error in distinct_key validation: {str(e)}\n{traceback.format_exc()}"
                queue.put(("result", "distinct_key", pd.DataFrame()))
                validation_results["distinct_key"] = pd.DataFrame()

        unified_df = service.build_unified_data_validation_df(
            row_counts_df=validation_results.get("row_counts"),
            null_values_df=validation_results.get("null_values"),
            distinct_key_df=validation_results.get("distinct_key"),
        )
        scope = "all" if not source_schema else "specify"
        # Get destination database name - handle both DB2 and SQL Server destinations
        dest_db_name = ""
        if config.get("destination_db_type", "sqlserver").lower() == "db2":
            dest_db_name = config.get("dest_db2", {}).get("database", "")
        else:
            dest_db_name = config.get("azure_sql", {}).get("database", "")
        single_csv_path = service.save_data_validation_single_csv(
            unified_df, scope=scope, azure_db_name=dest_db_name
        )
        queue.put(("done", single_csv_path))
    except Exception as e:
        import traceback
        queue.put(("error", str(e) + "\n" + traceback.format_exc()))
