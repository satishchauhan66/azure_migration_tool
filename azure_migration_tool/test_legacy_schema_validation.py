#!/usr/bin/env python
# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Test Legacy Schema Validation (DB2 -> Azure SQL) from the command line.

Uses the Python-only validation module (azure_migration_tool/validation); no PySpark required.

Usage:
  # With a config file (JSON with db2 and azure_sql sections):
  python -m azure_migration_tool.test_legacy_schema_validation --config path/to/database_config.json

  # E2E: full schema validation flow (presence, datatypes, defaults, indexes, FK, nullable, check constraints)
  python -m azure_migration_tool.test_legacy_schema_validation --config path/to/database_config.json --e2e

  # Smoke test (no real DBs needed – checks that the service loads and fails gracefully on connect):
  python -m azure_migration_tool.test_legacy_schema_validation --smoke

  # Optional: limit to one schema (same schema on DB2 and Azure)
  python -m azure_migration_tool.test_legacy_schema_validation --config config.json --schema MYSCHEMA
  python -m azure_migration_tool.test_legacy_schema_validation --config config.json --e2e --schema USERID
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
    for section in ("db2", "azure_sql"):
        if section in cfg and "user" in cfg[section] and "username" not in cfg[section]:
            cfg[section]["username"] = cfg[section]["user"]
    return cfg


def run_smoke_test():
    """Smoke test: init service and call compare_schema_presence; expect connection failure if no DBs."""
    _check_deps()
    print("=" * 60)
    print("Legacy Schema Validation – SMOKE TEST (no DBs required, no PySpark)")
    print("=" * 60)

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

        print("\n[1/3] Importing LegacySchemaValidationService...")
        try:
            from azure_migration_tool.validation.schema_service import LegacySchemaValidationService
        except ImportError:
            from validation.schema_service import LegacySchemaValidationService
        print("[OK] Imported.")

        print("\n[2/3] Initializing service...")
        service = LegacySchemaValidationService(config_path=config_path)
        print("[OK] Service initialized.")

        print("\n[3/3] Calling compare_schema_presence (will likely fail to connect without real DBs)...")
        try:
            df = service.compare_schema_presence(
                source_schema=None,
                target_schema=None,
                object_types=["TABLE"],
            )
            n = len(df)
            print(f"[OK] compare_schema_presence returned {n} row(s).")
            print(df.head(5).to_string() if n else "(empty)")
        except Exception as e:
            msg = str(e)
            if "DB2Driver" in msg and "is not found" in msg.lower():
                if "JVM was already started" not in msg and "JAR not found" not in msg and "driver JAR" not in msg:
                    print(f"[FAIL] Got generic DB2Driver-not-found error.")
                    raise AssertionError("Smoke test failed: driver not on classpath.") from e
            print(f"[EXPECTED] Connection or runtime error (no real DBs): {e}")
            print("\nSmoke test PASSED: service and method ran; connection failed as expected.")
    finally:
        try:
            os.unlink(config_path)
        except Exception:
            pass
    print("\n" + "=" * 60)


def run_e2e_with_config(config_path: str, schema: str = None):
    """Run full Legacy Schema Validation E2E: presence, datatypes, defaults, indexes, FK, nullable, check constraints."""
    _check_deps()
    print("=" * 60)
    print("Legacy Schema Validation E2E - DB2 -> Azure SQL (full flow)")
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
        from azure_migration_tool.validation.schema_service import LegacySchemaValidationService
    except ImportError:
        from validation.schema_service import LegacySchemaValidationService

    print("\n[1/8] Initializing LegacySchemaValidationService...")
    service = LegacySchemaValidationService(config_path=config_path)
    print("[OK] Service initialized.")

    object_types_table = ["TABLE"]
    object_types_presence = ["TABLE", "VIEW"]

    results = []

    print("\n[2/8] Running compare_schema_presence...")
    df = service.compare_schema_presence(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types_presence,
    )
    n = len(df)
    results.append(("Schema Presence", n, service.save_comparison_to_csv(df, "schema_presence")))
    print(f"[OK] Presence: {n} result(s)")

    print("\n[3/8] Running compare_column_datatypes_mapped...")
    df = service.compare_column_datatypes_mapped(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types_table,
    )
    n = len(df)
    results.append(("Data Types", n, service.save_comparison_to_csv(df, "datatype_mismatch")))
    print(f"[OK] Data types: {n} result(s)")

    print("\n[4/8] Running compare_column_default_values...")
    df = service.compare_column_default_values(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types_table,
    )
    n = len(df)
    results.append(("Default Values", n, service.save_comparison_to_csv(df, "default_values")))
    print(f"[OK] Default values: {n} result(s)")

    print("\n[5/8] Running compare_index_definitions...")
    df = service.compare_index_definitions(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types_table,
    )
    n = len(df)
    results.append(("Indexes", n, service.save_comparison_to_csv(df, "indexes")))
    print(f"[OK] Indexes: {n} result(s)")

    print("\n[6/8] Running compare_foreign_keys...")
    df = service.compare_foreign_keys(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types_table,
    )
    n = len(df)
    results.append(("Foreign Keys", n, service.save_comparison_to_csv(df, "foreign_keys")))
    print(f"[OK] Foreign keys: {n} result(s)")

    print("\n[7/8] Running compare_column_nullable_constraints...")
    df = service.compare_column_nullable_constraints(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types_table,
    )
    n = len(df)
    results.append(("Nullable", n, service.save_comparison_to_csv(df, "nullable")))
    print(f"[OK] Nullable: {n} result(s)")

    print("\n[8/8] Running compare_check_constraints...")
    df = service.compare_check_constraints(
        source_schema=src_schema,
        target_schema=tgt_schema,
        object_types=object_types_table,
    )
    n = len(df)
    results.append(("Check Constraints", n, service.save_comparison_to_csv(df, "check_constraints")))
    print(f"[OK] Check constraints: {n} result(s)")

    print("\n" + "=" * 60)
    print("E2E SUMMARY")
    print("=" * 60)
    for name, count, csv_path in results:
        print(f"  {name:<22} {count:>6}  {csv_path}")
    print("=" * 60)
    print("Done.")


def main():
    import argparse
    p = argparse.ArgumentParser(description="Test Legacy Schema Validation (DB2 -> Azure SQL)")
    p.add_argument("--config", "-c", help="Path to database_config.json (db2 + azure_sql)")
    p.add_argument("--schema", "-s", help="Optional schema name (same on source and target)")
    p.add_argument("--smoke", action="store_true", help="Smoke test only (no real DBs)")
    p.add_argument("--e2e", action="store_true", help="E2E: full schema validation (all steps, CSVs)")
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
        # Single-step run (presence only) when no --e2e
        _check_deps()
        _check_java()
        config = _load_config(args.config)
        out_dir = os.environ.get("VALIDATION_OUTPUT_DIR", str(Path(args.config).parent))
        os.makedirs(out_dir, exist_ok=True)
        os.environ["VALIDATION_OUTPUT_DIR"] = out_dir
        try:
            from azure_migration_tool.validation.schema_service import LegacySchemaValidationService
        except ImportError:
            from validation.schema_service import LegacySchemaValidationService
        service = LegacySchemaValidationService(config_path=args.config)
        src_schema = args.schema.strip() or None if args.schema else None
        df = service.compare_schema_presence(source_schema=src_schema, target_schema=src_schema, object_types=["TABLE"])
        csv_path = service.save_comparison_to_csv(df, "schema_presence")
        print(f"Presence: {len(df)} result(s). Saved: {csv_path}")
        print(df.head(10).to_string())


if __name__ == "__main__":
    main()
