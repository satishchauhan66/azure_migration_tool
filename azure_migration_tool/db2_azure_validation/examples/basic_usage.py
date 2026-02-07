#!/usr/bin/env python3
# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Basic usage example for the db2_azure_validation module.

This script demonstrates the simplest way to use the module
for validating a database migration.
"""

import os
import sys

# Add parent directory to path for module import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# Import the validation services
from db2_azure_validation import (
    PySparkSchemaValidationService,
    PySparkDataValidationService,
    PySparkBehaviorValidationService,
)


def main():
    """Run basic validation example."""
    
    # Configuration
    SCHEMA = "MYSCHEMA"  # Change this to your schema name
    
    print("DB2 to Azure Validation - Basic Example")
    print("=" * 50)
    
    # -------------------------------------------------------
    # SCHEMA VALIDATION
    # -------------------------------------------------------
    print("\n1. Schema Validation")
    print("-" * 30)
    
    # Initialize the schema validation service
    # This will load database_config.json from the current directory
    schema_service = PySparkSchemaValidationService()
    
    # Find objects that exist in one database but not the other
    print("   Checking for missing objects...")
    df = schema_service.compare_schema_presence(
        source_schema=SCHEMA,
        target_schema=SCHEMA,
        object_types=["TABLE", "VIEW"]
    )
    
    # Show results
    missing_count = df.count()
    print(f"   Found {missing_count} missing objects")
    
    if missing_count > 0:
        # Display first 5 results
        print("\n   Sample results:")
        df.show(5, truncate=False)
        
        # Save to CSV
        csv_path = schema_service.save_comparison_to_csv(df, "missing_objects")
        print(f"   Results saved to: {csv_path}")
    
    # -------------------------------------------------------
    # DATA VALIDATION
    # -------------------------------------------------------
    print("\n2. Data Validation")
    print("-" * 30)
    
    # Initialize the data validation service
    data_service = PySparkDataValidationService()
    
    # Compare row counts between source and target
    print("   Comparing row counts...")
    df = data_service.compare_row_counts(
        source_schema=SCHEMA,
        target_schema=SCHEMA,
        object_types=["TABLE"]
    )
    
    mismatch_count = df.count()
    print(f"   Found {mismatch_count} row count mismatches")
    
    if mismatch_count > 0:
        csv_path = data_service.save_comparison_to_csv(df, "row_count_mismatch")
        print(f"   Results saved to: {csv_path}")
    
    # -------------------------------------------------------
    # BEHAVIOR VALIDATION
    # -------------------------------------------------------
    print("\n3. Behavior Validation")
    print("-" * 30)
    
    # Initialize the behavior validation service
    behavior_service = PySparkBehaviorValidationService()
    
    # Check identity columns
    print("   Checking identity columns...")
    df = behavior_service.compare_identity_sequences(
        source_schema=SCHEMA,
        target_schema=SCHEMA,
        object_types=["TABLE"]
    )
    
    identity_issues = df.count()
    print(f"   Found {identity_issues} identity column issues")
    
    # -------------------------------------------------------
    # COMPLETE VALIDATION (All at once)
    # -------------------------------------------------------
    print("\n4. Complete Validation (All at Once)")
    print("-" * 30)
    
    # Run all behavior validations in one call
    print("   Running complete behavior validation...")
    df = behavior_service.validate_behavior_all(
        source_schema=SCHEMA,
        target_schema=SCHEMA,
        object_types=["TABLE"]
    )
    
    total_issues = df.count()
    print(f"   Total behavior issues: {total_issues}")
    
    if total_issues > 0:
        csv_path = behavior_service.save_comparison_to_csv(df, "behavior_all")
        print(f"   Results saved to: {csv_path}")
    
    print("\n" + "=" * 50)
    print("Validation complete!")


if __name__ == "__main__":
    main()
