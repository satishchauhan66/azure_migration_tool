#!/usr/bin/env python3
# Author: Sa-tish Chauhan

"""
Example script demonstrating how to use the db2_azure_validation module.

This script shows how to run various validation checks between
DB2 and Azure SQL databases.

Usage:
    python run_validation.py --schema MYSCHEMA --output ./results

Prerequisites:
    1. Create a database_config.json file with connection details
    2. Install dependencies: pip install -r requirements.txt
    3. Ensure JAVA_HOME is set to JDK 11 or 17
"""

import argparse
import os
import sys
from datetime import datetime

# Add parent directory to path for module import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from db2_azure_validation import (
    PySparkSchemaValidationService,
    PySparkDataValidationService,
    PySparkBehaviorValidationService,
)


def run_schema_validation(service, schema: str, output_dir: str) -> list:
    """Run all schema validations and save results."""
    results = []
    
    print(f"\n{'='*60}")
    print("SCHEMA VALIDATION")
    print(f"{'='*60}")
    
    # 1. Schema presence check
    print("\n[1/6] Checking schema presence...")
    try:
        df = service.compare_schema_presence(
            source_schema=schema,
            target_schema=schema,
            object_types=["TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER", "INDEX"]
        )
        count = df.count()
        print(f"    Found {count} differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "schema_presence")
            results.append(("Schema Presence", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 2. Column data types
    print("\n[2/6] Checking column data types...")
    try:
        df = service.compare_column_datatypes_mapped(
            source_schema=schema,
            target_schema=schema,
            object_types=["TABLE"]
        )
        count = df.count()
        print(f"    Found {count} type mismatches")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "datatype_mismatch")
            results.append(("Data Types", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 3. Default values
    print("\n[3/6] Checking column default values...")
    try:
        df = service.compare_column_default_values(
            source_schema=schema,
            target_schema=schema,
            object_types=["TABLE"]
        )
        count = df.count()
        print(f"    Found {count} default value differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "default_values")
            results.append(("Default Values", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 4. Index definitions
    print("\n[4/6] Checking index definitions...")
    try:
        df = service.compare_index_definitions(
            source_schema=schema,
            target_schema=schema,
            object_types=["TABLE"]
        )
        count = df.count()
        print(f"    Found {count} index differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "indexes")
            results.append(("Indexes", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 5. Foreign keys
    print("\n[5/6] Checking foreign keys...")
    try:
        df = service.compare_foreign_keys(
            source_schema=schema,
            target_schema=schema,
            object_types=["TABLE"]
        )
        count = df.count()
        print(f"    Found {count} foreign key differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "foreign_keys")
            results.append(("Foreign Keys", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 6. Nullable constraints
    print("\n[6/6] Checking nullable constraints...")
    try:
        df = service.compare_column_nullable_constraints(
            source_schema=schema,
            target_schema=schema,
            object_types=["TABLE"]
        )
        count = df.count()
        print(f"    Found {count} nullable differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "nullable")
            results.append(("Nullable", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    return results


def run_data_validation(service, schema: str, output_dir: str) -> list:
    """Run all data validations and save results."""
    results = []
    
    print(f"\n{'='*60}")
    print("DATA VALIDATION")
    print(f"{'='*60}")
    
    # 1. Row counts
    print("\n[1/2] Checking row counts...")
    try:
        df = service.compare_row_counts(
            source_schema=schema,
            target_schema=schema,
            object_types=["TABLE"]
        )
        count = df.count()
        print(f"    Found {count} row count mismatches")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "row_counts")
            results.append(("Row Counts", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 2. Null/empty values
    print("\n[2/2] Checking null/empty value counts...")
    try:
        df = service.compare_column_null_empty(
            source_schema=schema,
            target_schema=schema
        )
        count = df.count()
        print(f"    Found {count} null/empty differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "null_empty")
            results.append(("Null/Empty", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    return results


def run_behavior_validation(service, schema: str, output_dir: str) -> list:
    """Run all behavior validations and save results."""
    results = []
    
    print(f"\n{'='*60}")
    print("BEHAVIOR VALIDATION")
    print(f"{'='*60}")
    
    # 1. Identity columns
    print("\n[1/4] Checking identity columns...")
    try:
        df = service.compare_identity_sequences(
            source_schema=schema,
            target_schema=schema,
            object_types=["TABLE"]
        )
        count = df.count()
        print(f"    Found {count} identity differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "identity")
            results.append(("Identity", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 2. Sequences
    print("\n[2/4] Checking sequences...")
    try:
        df = service.compare_sequence_definitions(
            source_schema=schema,
            target_schema=schema
        )
        count = df.count()
        print(f"    Found {count} sequence differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "sequences")
            results.append(("Sequences", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 3. Triggers
    print("\n[3/4] Checking triggers...")
    try:
        df = service.compare_trigger_definitions(
            source_schema=schema,
            target_schema=schema
        )
        count = df.count()
        print(f"    Found {count} trigger differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "triggers")
            results.append(("Triggers", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    # 4. Collation
    print("\n[4/4] Checking collation/encoding...")
    try:
        df = service.compare_collation_encoding(
            source_schema=schema,
            target_schema=schema
        )
        count = df.count()
        print(f"    Found {count} collation differences")
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "collation")
            results.append(("Collation", count, csv_path))
    except Exception as e:
        print(f"    Error: {e}")
    
    return results


def print_summary(all_results: list):
    """Print a summary of all validation results."""
    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")
    
    if not all_results:
        print("\nNo differences found!")
        return
    
    total_issues = 0
    print(f"\n{'Validation Type':<25} {'Issues':<10} {'Output File'}")
    print("-" * 80)
    
    for name, count, path in all_results:
        filename = os.path.basename(path) if path else "N/A"
        print(f"{name:<25} {count:<10} {filename}")
        total_issues += count
    
    print("-" * 80)
    print(f"{'TOTAL':<25} {total_issues:<10}")


def main():
    parser = argparse.ArgumentParser(
        description="Run DB2 to Azure SQL migration validation"
    )
    parser.add_argument(
        "--schema",
        "-s",
        required=True,
        help="Schema name to validate"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="./outputs",
        help="Output directory for CSV files (default: ./outputs)"
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip schema validation"
    )
    parser.add_argument(
        "--skip-data",
        action="store_true",
        help="Skip data validation"
    )
    parser.add_argument(
        "--skip-behavior",
        action="store_true",
        help="Skip behavior validation"
    )
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    print(f"\n{'#'*60}")
    print("DB2 to Azure SQL Migration Validation")
    print(f"{'#'*60}")
    print(f"\nSchema: {args.schema}")
    print(f"Output: {args.output}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    all_results = []
    
    try:
        # Run schema validation
        if not args.skip_schema:
            schema_service = PySparkSchemaValidationService()
            results = run_schema_validation(schema_service, args.schema, args.output)
            all_results.extend(results)
        
        # Run data validation
        if not args.skip_data:
            data_service = PySparkDataValidationService()
            results = run_data_validation(data_service, args.schema, args.output)
            all_results.extend(results)
        
        # Run behavior validation
        if not args.skip_behavior:
            behavior_service = PySparkBehaviorValidationService()
            results = run_behavior_validation(behavior_service, args.schema, args.output)
            all_results.extend(results)
        
        # Print summary
        print_summary(all_results)
        
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
    
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
