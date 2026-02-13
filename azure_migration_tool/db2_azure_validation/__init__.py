# Author: Sa-tish Chauhan

"""
DB2 to Azure Migration Validation Module

A standalone Python module for validating database migrations from DB2 to Azure SQL.
This module provides schema, data, and behavior validation capabilities.

Note: PySpark is required for validation services. Install with: pip install pyspark

Usage:
    from db2_azure_validation.services.schema_validation_service import (
        PySparkSchemaValidationService
    )

    # Initialize service
    service = PySparkSchemaValidationService()

    # Run validation
    df = service.compare_schema_presence(
        source_schema="MY_SCHEMA",
        target_schema="MY_SCHEMA",
        object_types=["TABLE", "VIEW"]
    )

    # Save results
    csv_path = service.save_comparison_to_csv(df, "schema_validation")
"""

__version__ = "1.0.0"
__author__ = "Satish Chauhan"

# Lazy imports - don't import PySpark services at module load time
# This allows the app to start even if PySpark is not installed

def __getattr__(name):
    """Lazy import for PySpark-dependent classes."""
    pyspark_services = {
        'PySparkSchemaValidationService': 'db2_azure_validation.services.schema_validation_service',
        'PySparkSchemaValidationAzureService': 'db2_azure_validation.services.schema_validation_service',
        'PySparkDataValidationService': 'db2_azure_validation.services.data_validation_service',
        'PySparkDataValidationAzureService': 'db2_azure_validation.services.data_validation_service',
        'PySparkBehaviorValidationService': 'db2_azure_validation.services.behavior_validation_service',
        'PySparkBehaviorValidationAzureService': 'db2_azure_validation.services.behavior_validation_service',
        'PySparkSchemaComparisonService': 'db2_azure_validation.services.pyspark_schema_comparison',
        'PySparkAzureSchemaComparisonService': 'db2_azure_validation.services.pyspark_schema_comparison',
    }
    
    if name in pyspark_services:
        import importlib
        module = importlib.import_module(pyspark_services[name])
        return getattr(module, name)
    
    # For other attributes, try schemas and config
    schema_attrs = [
        'Message', 'Pagination',
        'ValidationCountBase', 'ValidationCountCreate', 'ValidationCountUpdate', 'ValidationCountRead',
        'DB2SchemaComparisonBase', 'DB2SchemaComparisonCreate', 'DB2SchemaComparisonUpdate', 'DB2SchemaComparison',
        'SchemaComparisonRequest', 'SchemaComparisonResponse',
        'ObjectInventoryBase', 'ObjectInventoryCreate', 'ObjectInventoryUpdate', 'ObjectInventoryRead',
        'SchemaDiffBase', 'SchemaDiffCreate', 'SchemaDiffUpdate', 'SchemaDiffRead',
    ]
    
    if name in schema_attrs:
        from db2_azure_validation import schemas
        return getattr(schemas, name)
    
    if name == 'Settings' or name == 'settings':
        from db2_azure_validation import config
        return getattr(config, name)
    
    raise AttributeError(f"module 'db2_azure_validation' has no attribute '{name}'")


__all__ = [
    # Version
    "__version__",
    # Services - DB2 to Azure (lazy loaded)
    "PySparkSchemaComparisonService",
    "PySparkSchemaValidationService",
    "PySparkDataValidationService",
    "PySparkBehaviorValidationService",
    # Services - Azure to Azure (lazy loaded)
    "PySparkAzureSchemaComparisonService",
    "PySparkSchemaValidationAzureService",
    "PySparkDataValidationAzureService",
    "PySparkBehaviorValidationAzureService",
    # Schemas
    "Message",
    "Pagination",
    "ValidationCountBase",
    "ValidationCountCreate",
    "ValidationCountUpdate",
    "ValidationCountRead",
    "DB2SchemaComparisonBase",
    "DB2SchemaComparisonCreate",
    "DB2SchemaComparisonUpdate",
    "DB2SchemaComparison",
    "SchemaComparisonRequest",
    "SchemaComparisonResponse",
    "ObjectInventoryBase",
    "ObjectInventoryCreate",
    "ObjectInventoryUpdate",
    "ObjectInventoryRead",
    "SchemaDiffBase",
    "SchemaDiffCreate",
    "SchemaDiffUpdate",
    "SchemaDiffRead",
    # Config
    "Settings",
    "settings",
]
