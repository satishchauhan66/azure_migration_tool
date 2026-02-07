# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Validation services for DB2 to Azure Migration.

This module provides PySpark-based services for:
- Schema validation (compare database structures)
- Data validation (compare row counts, checksums, null values)
- Behavior validation (identity, sequences, triggers, routines)

Services support both DB2→Azure and Azure→Azure comparisons.

Note: PySpark must be installed separately: pip install pyspark
"""

# Lazy imports - PySpark services are only loaded when accessed
# This allows the app to start even if PySpark is not installed

def __getattr__(name):
    """Lazy import for PySpark-dependent classes."""
    
    if name in ('PySparkSchemaComparisonService', 'PySparkAzureSchemaComparisonService', 'log_duration'):
        from db2_azure_validation.services import pyspark_schema_comparison
        return getattr(pyspark_schema_comparison, name)
    
    if name in ('PySparkSchemaValidationService', 'PySparkSchemaValidationAzureService'):
        from db2_azure_validation.services import schema_validation_service
        return getattr(schema_validation_service, name)
    
    if name in ('PySparkDataValidationService', 'PySparkDataValidationAzureService'):
        from db2_azure_validation.services import data_validation_service
        return getattr(data_validation_service, name)
    
    if name in ('PySparkBehaviorValidationService', 'PySparkBehaviorValidationAzureService'):
        from db2_azure_validation.services import behavior_validation_service
        return getattr(behavior_validation_service, name)
    
    raise AttributeError(f"module 'db2_azure_validation.services' has no attribute '{name}'")


__all__ = [
    # Base services
    "PySparkSchemaComparisonService",
    "PySparkAzureSchemaComparisonService",
    "log_duration",
    # Schema validation
    "PySparkSchemaValidationService",
    "PySparkSchemaValidationAzureService",
    # Data validation
    "PySparkDataValidationService",
    "PySparkDataValidationAzureService",
    # Behavior validation
    "PySparkBehaviorValidationService",
    "PySparkBehaviorValidationAzureService",
]
