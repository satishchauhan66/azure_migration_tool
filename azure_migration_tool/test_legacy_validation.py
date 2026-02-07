#!/usr/bin/env python
# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Test script for Legacy Data Validation with real DB2 and Azure SQL.
"""

import os
import sys
import json
import getpass
from pathlib import Path
from datetime import datetime

# Add paths
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

def main():
    print("=" * 60)
    print("Legacy Data Validation Test - DB2 to Azure SQL")
    print("=" * 60)
    
    # Check Java
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        print("\n[WARNING] JAVA_HOME is not set!")
        print("PySpark requires Java 11 or 17.")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return
    else:
        print(f"\n[OK] JAVA_HOME: {java_home}")
    
    # Connection details from the image
    print("\n--- Source Database (DB2) ---")
    src_host = input("Host [ss-sld-db22q]: ").strip() or "ss-sld-db22q"
    src_port = input("Port [50000]: ").strip() or "50000"
    src_database = input("Database [D_MPAYER]: ").strip() or "D_MPAYER"
    src_schema = input("Schema [USERID]: ").strip() or "USERID"
    src_user = input("User [chauhs]: ").strip() or "chauhs"
    src_password = getpass.getpass("Password: ")
    
    print("\n--- Destination Database (Azure SQL) ---")
    dest_server = input("Server [route66-qa-eus2-mi.313a5ac3664e.database.windows.net]: ").strip() or "route66-qa-eus2-mi.313a5ac3664e.database.windows.net"
    dest_database = input("Database [D_MPAYER_ss_sld_db22q]: ").strip() or "D_MPAYER_ss_sld_db22q"
    dest_user = input("User [svc_azdm_q]: ").strip() or "svc_azdm_q"
    dest_password = getpass.getpass("Password: ")
    
    # Create output directory
    output_dir = Path.home() / "Desktop" / "validation_test_output"
    output_dir.mkdir(exist_ok=True)
    print(f"\n[OK] Output directory: {output_dir}")
    
    # Create database config
    config = {
        "db2": {
            "host": src_host,
            "port": int(src_port),
            "database": src_database,
            "user": src_user,
            "password": src_password
        },
        "azure_sql": {
            "server": dest_server,
            "database": dest_database,
            "user": dest_user,
            "password": dest_password,
            "authentication": "SqlPassword"
        }
    }
    
    config_path = output_dir / "database_config.json"
    with open(config_path, 'w') as f:
        # Don't write passwords to file for security
        safe_config = {
            "db2": {**config["db2"], "password": "***"},
            "azure_sql": {**config["azure_sql"], "password": "***"}
        }
        json.dump(safe_config, f, indent=2)
    print(f"[OK] Config saved (without passwords): {config_path}")
    
    # Write actual config to temp location
    import tempfile
    temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(config, temp_config)
    temp_config.close()
    
    os.environ["VALIDATION_OUTPUT_DIR"] = str(output_dir)
    
    print("\n" + "=" * 60)
    print("Starting Validation...")
    print("=" * 60)
    
    try:
        # Import validation service
        print("\n[1/4] Importing db2_azure_validation module...")
        from db2_azure_validation.services.data_validation_service import PySparkDataValidationService
        print("[OK] Module imported successfully")
        
        # Initialize service
        print("\n[2/4] Initializing PySpark service...")
        print("      (This may take a minute to start Spark...)")
        service = PySparkDataValidationService(config_filename=temp_config.name)
        print("[OK] Service initialized")
        
        # Run row count comparison
        print("\n[3/4] Running row count comparison...")
        print(f"      Source schema: {src_schema}")
        print(f"      Target schema: {src_schema}")
        
        df = service.compare_row_counts(
            source_schema=src_schema,
            target_schema=src_schema,
            object_types=["TABLE"]
        )
        
        count = df.count()
        print(f"[OK] Found {count} table comparisons")
        
        # Save results
        if count > 0:
            csv_path = service.save_comparison_to_csv(df, "row_counts")
            print(f"[OK] Results saved to: {csv_path}")
            
            # Show first few results
            print("\n--- Sample Results ---")
            pdf = df.limit(10).toPandas()
            print(pdf.to_string())
        
        # Stop Spark
        print("\n[4/4] Stopping Spark...")
        service.spark.stop()
        print("[OK] Spark stopped")
        
        print("\n" + "=" * 60)
        print("VALIDATION COMPLETE!")
        print("=" * 60)
        print(f"\nResults saved to: {output_dir}")
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup temp config
        try:
            os.unlink(temp_config.name)
        except:
            pass

if __name__ == "__main__":
    main()
