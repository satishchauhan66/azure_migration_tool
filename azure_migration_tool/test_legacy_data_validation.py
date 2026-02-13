#!/usr/bin/env python
# Author: Sa-tish Chauhan

"""
Test Legacy Data Validation (DB2 -> Azure SQL) from the command line.

Uses the Python-only validation module (azure_migration_tool/validation); no PySpark required.

Usage:
  # With a config file (JSON with db2 and azure_sql sections; use "username" and "password"):
  python -m azure_migration_tool.test_legacy_data_validation --config path/to/database_config.json

  # E2E: full data validation flow (row counts, null values, distinct key, single CSV)
  python -m azure_migration_tool.test_legacy_data_validation --config path/to/database_config.json --e2e

  # Smoke test (no real DBs needed – checks that the service loads and fails gracefully on connect):
  python -m azure_migration_tool.test_legacy_data_validation --smoke

  # Optional: limit to one schema (same schema on DB2 and Azure)
  python -m azure_migration_tool.test_legacy_data_validation --config config.json --schema MYSCHEMA
  python -m azure_migration_tool.test_legacy_data_validation --config config.json --e2e --schema USERID
"""

import os
import sys
import json
import tempfile
from pathlib import Path

# Ensure in-repo package is used
APP_DIR = Path(__file__).resolve().parent
REPO_ROOT = APP_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


def _check_java():
    if not os.environ.get("JAVA_HOME"):
        print("[WARNING] JAVA_HOME is not set. DB2 JDBC (jaydebeapi) needs Java.")
        return False
    print(f"[OK] JAVA_HOME={os.environ['JAVA_HOME']}")
    return True


def _check_deps():
    """Check required deps (pandas); print install hint and exit if missing."""
    try:
        import pandas  # noqa: F401
    except ImportError:
        print("[ERROR] pandas is required. Install with: pip install pandas")
        print("  Or from repo: pip install -r azure_migration_tool/requirements.txt")
        sys.exit(1)


def _load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        cfg = json.load(f)
    # Normalize: some files use "user", service expects "username"
    for section in ("db2", "azure_sql"):
        if section in cfg and "user" in cfg[section] and "username" not in cfg[section]:
            cfg[section]["username"] = cfg[section]["user"]
    return cfg


def run_smoke_test():
    """Smoke test: init service and call compare_row_counts; expect connection failure if no DBs."""
    _check_deps()
    print("=" * 60)
    print("Legacy Data Validation – SMOKE TEST (no DBs required, no PySpark)")
    print("=" * 60)

    # Minimal config with placeholder hosts so service init succeeds
    config = {
        "db2": {
            "host": "localhost",
            "port": 50000,
            "database": "TESTDB",
            "username": "u",
            "password": "p",
        },
        "azure_sql": {
            "server": "localhost",
            "database": "TESTDB",
            "username": "u",
            "password": "p",
            "authentication": "SqlPassword",
            "encrypt": "no",
            "trust_server_certificate": "yes",
        },
    }
    fd, config_path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(config, f, indent=2)
        os.environ["VALIDATION_OUTPUT_DIR"] = tempfile.gettempdir()

        print("\n[1/3] Importing LegacyDataValidationService (validation module)...")
        try:
            from azure_migration_tool.validation.data_service import LegacyDataValidationService
        except ImportError:
            from validation.data_service import LegacyDataValidationService
        print("[OK] Imported.")

        print("\n[2/3] Initializing service...")
        service = LegacyDataValidationService(config_path)
        print("[OK] Service initialized.")

        print("\n[3/3] Calling compare_row_counts (will likely fail to connect without real DBs)...")
        try:
            df = service.compare_row_counts(
                source_schema=None,
                target_schema=None,
                object_types=["TABLE"],
            )
            n = len(df)
            print(f"[OK] compare_row_counts returned {n} row(s).")
            print(df.head(5).to_string() if n else "(empty)")
        except Exception as e:
            msg = str(e)
            # Guard against regressions: we must NOT get the generic jaydebeapi "DB2Driver is not found"
            if "DB2Driver" in msg and "is not found" in msg.lower():
                if "JVM was already started" not in msg and "JAR not found" not in msg and "driver JAR" not in msg:
                    print(f"[FAIL] Got generic DB2Driver-not-found error; expected connection failure or explicit driver/JAR message.")
                    raise AssertionError(
                        "Smoke test failed: driver not on classpath (generic error). "
                        "Ensure DB2 JAR is validated and JVM-already-started handling is in place."
                    ) from e
            print(f"[EXPECTED] Connection or runtime error (no real DBs): {e}")
            print("\nSmoke test PASSED: service and method ran; connection failed as expected.")
    finally:
        try:
            os.unlink(config_path)
        except Exception:
            pass
    print("\n" + "=" * 60)


def run_with_config(config_path: str, schema: str = None):
    """Run legacy data validation with a real config file."""
    _check_deps()
    print("=" * 60)
    print("Legacy Data Validation - DB2 -> Azure SQL (Python-only, no PySpark)")
    print("=" * 60)
    _check_java()

    config = _load_config(config_path)
    out_dir = os.environ.get("VALIDATION_OUTPUT_DIR", str(Path(config_path).parent))
    os.makedirs(out_dir, exist_ok=True)
    os.environ["VALIDATION_OUTPUT_DIR"] = out_dir
    print(f"\n[OK] Config: {config_path}")
    print(f"[OK] Output dir: {out_dir}")

    fd, normalized_path = tempfile.mkstemp(suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(config, f, indent=2)
        service_config_path = normalized_path
    except Exception:
        service_config_path = config_path

    print("\n[1/4] Importing LegacyDataValidationService...")
    try:
        from azure_migration_tool.validation.data_service import LegacyDataValidationService
    except ImportError:
        from validation.data_service import LegacyDataValidationService
    print("[OK] Imported.")

    print("\n[2/4] Initializing service...")
    service = LegacyDataValidationService(service_config_path)
    print("[OK] Service initialized.")

    src_schema = schema.strip() or None if schema else None
    tgt_schema = src_schema

    print("\n[3/4] Running compare_row_counts...")
    df = service.compare_row_counts(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=["TABLE"],
    )
    n = len(df)
    print(f"[OK] Got {n} row comparison(s).")

    print("\n[4/4] Saving CSV and showing sample...")
    csv_path = service.save_comparison_to_csv(df, "row_counts")
    print(f"[OK] Saved: {csv_path}")
    print(df.head(10).to_string())

    if service_config_path != config_path:
        try:
            os.unlink(service_config_path)
        except Exception:
            pass
    print("\n" + "=" * 60)
    print("Done.")


def run_e2e_with_config(config_path: str, schema: str = None):
    """Run full Legacy Data Validation E2E: row counts, null values, distinct key, single CSV."""
    _check_deps()
    print("=" * 60)
    print("Legacy Data Validation E2E - DB2 -> Azure SQL (full flow, single CSV)")
    print("=" * 60)
    _check_java()

    config = _load_config(config_path)
    out_dir = os.environ.get("VALIDATION_OUTPUT_DIR", str(Path(config_path).parent))
    os.makedirs(out_dir, exist_ok=True)
    os.environ["VALIDATION_OUTPUT_DIR"] = out_dir
    print(f"\n[OK] Config: {config_path}")
    print(f"[OK] Output dir: {out_dir}")

    src_schema = schema.strip() or None if schema else None
    tgt_schema = src_schema
    if src_schema:
        print(f"[OK] Schema filter: {src_schema} (same on source and target)")
    else:
        print("[OK] Schema filter: all")

    try:
        from azure_migration_tool.validation.data_service import LegacyDataValidationService
    except ImportError:
        from validation.data_service import LegacyDataValidationService
    try:
        from azure_migration_tool.validation.schema_service import LegacySchemaValidationService
    except ImportError:
        from validation.schema_service import LegacySchemaValidationService

    print("\n[1/5] Initializing LegacyDataValidationService...")
    service = LegacyDataValidationService(config_path=config_path)
    print("[OK] Service initialized.")

    object_types = ["TABLE"]
    row_counts_df = None
    null_values_df = None
    distinct_key_df = None

    print("\n[2/5] Running compare_row_counts...")
    row_counts_df = service.compare_row_counts(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types,
    )
    n_row = len(row_counts_df)
    print(f"[OK] Row counts: {n_row} result(s)")
    if n_row:
        print(row_counts_df.head(5).to_string())

    print("\n[3/5] Running compare_column_nulls...")
    null_values_df = service.compare_column_nulls(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types,
        only_when_rowcount_matches=True,
        output_only_issues=False,
    )
    n_null = len(null_values_df)
    print(f"[OK] Null values: {n_null} result(s)")
    if n_null:
        print(null_values_df.head(5).to_string())

    print("\n[4/5] Running distinct key (primary key) check...")
    schema_service = LegacySchemaValidationService(config_path=config_path)
    index_df = schema_service.compare_index_definitions(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types,
    )
    if "ValidationType" in index_df.columns and "Status" in index_df.columns:
        distinct_key_df = index_df[
            (index_df["ValidationType"] == "PrimaryKey") & (index_df["Status"] == "info")
        ].copy()
    else:
        distinct_key_df = index_df.iloc[0:0].copy() if not index_df.empty else index_df
    n_pk = len(distinct_key_df)
    print(f"[OK] Distinct key (tables without PK): {n_pk} result(s)")
    if n_pk:
        print(distinct_key_df.head(5).to_string())

    print("\n[5/5] Building unified CSV...")
    unified_df = service.build_unified_data_validation_df(
        row_counts_df=row_counts_df,
        null_values_df=null_values_df,
        distinct_key_df=distinct_key_df,
    )
    scope = "all" if not src_schema else "specify"
    azure_db_name = config.get("azure_sql", {}).get("database", "")
    single_csv_path = service.save_data_validation_single_csv(
        unified_df, scope=scope, azure_db_name=azure_db_name
    )
    print(f"[OK] Saved: {single_csv_path}")

    print("\n" + "=" * 60)
    print("E2E SUMMARY")
    print("=" * 60)
    print(f"  Row counts:     {n_row}")
    print(f"  Null values:   {n_null}")
    print(f"  Distinct key:  {n_pk}")
    print(f"  Single CSV:    {single_csv_path}")
    print("=" * 60)
    print("Done.")


def main():
    import argparse
    p = argparse.ArgumentParser(description="Test Legacy Data Validation (DB2 -> Azure SQL)")
    p.add_argument("--config", "-c", help="Path to database_config.json (db2 + azure_sql with username/password)")
    p.add_argument("--schema", "-s", help="Optional schema name to limit comparison (same on source and target)")
    p.add_argument("--smoke", action="store_true", help="Smoke test only (no real DBs)")
    p.add_argument("--e2e", action="store_true", help="E2E: full data validation (row counts, nulls, distinct key, single CSV)")
    args = p.parse_args()

    if args.smoke:
        run_smoke_test()
        return

    if not args.config:
        print("Either provide --config path/to/database_config.json or run --smoke for a smoke test.")
        print("For E2E with real DBs: --config path/to/database_config.json --e2e")
        print("\nExample config (database_config.json):")
        print(json.dumps({
            "db2": {
                "host": "your-db2-host",
                "port": 50000,
                "database": "YOURDB",
                "username": "user",
                "password": "***",
            },
            "azure_sql": {
                "server": "your-server.database.windows.net",
                "database": "YOURDB",
                "username": "user",
                "password": "***",
                "authentication": "SqlPassword",
                "encrypt": "yes",
                "trust_server_certificate": "yes",
            },
        }, indent=2))
        sys.exit(1)

    if not os.path.isfile(args.config):
        print(f"Config file not found: {args.config}")
        sys.exit(1)

    if args.e2e:
        run_e2e_with_config(args.config, schema=args.schema or "")
    else:
        run_with_config(args.config, schema=args.schema or "")


if __name__ == "__main__":
    main()
