"""
Pydantic schemas for DB2 to Azure Migration Validation.

These schemas define the data models used for validation requests,
responses, and internal data structures.
"""

from db2_azure_validation.schemas.common import (
    Message,
    Pagination,
    get_unified_columns,
    ensure_all_columns_as_strings,
)

from db2_azure_validation.schemas.validation_counts import (
    ValidationCountBase,
    ValidationCountCreate,
    ValidationCountUpdate,
    ValidationCountRead,
)

from db2_azure_validation.schemas.db2_schema_comparison import (
    DB2SchemaComparisonBase,
    DB2SchemaComparisonCreate,
    DB2SchemaComparisonUpdate,
    DB2SchemaComparison,
    SchemaComparisonRequest,
    SchemaComparisonResponse,
    RowCountComparisonRequest,
    RowCountComparisonResponse,
    ColumnNullCheckRequest,
    ColumnNullCheckResponse,
)

from db2_azure_validation.schemas.object_inventory import (
    ObjectInventoryBase,
    ObjectInventoryCreate,
    ObjectInventoryUpdate,
    ObjectInventoryRead,
)

from db2_azure_validation.schemas.schema_diffs import (
    SchemaDiffBase,
    SchemaDiffCreate,
    SchemaDiffUpdate,
    SchemaDiffRead,
)

__all__ = [
    # Common
    "Message",
    "Pagination",
    "get_unified_columns",
    "ensure_all_columns_as_strings",
    # Validation Counts
    "ValidationCountBase",
    "ValidationCountCreate",
    "ValidationCountUpdate",
    "ValidationCountRead",
    # Schema Comparison
    "DB2SchemaComparisonBase",
    "DB2SchemaComparisonCreate",
    "DB2SchemaComparisonUpdate",
    "DB2SchemaComparison",
    "SchemaComparisonRequest",
    "SchemaComparisonResponse",
    "RowCountComparisonRequest",
    "RowCountComparisonResponse",
    "ColumnNullCheckRequest",
    "ColumnNullCheckResponse",
    # Object Inventory
    "ObjectInventoryBase",
    "ObjectInventoryCreate",
    "ObjectInventoryUpdate",
    "ObjectInventoryRead",
    # Schema Diffs
    "SchemaDiffBase",
    "SchemaDiffCreate",
    "SchemaDiffUpdate",
    "SchemaDiffRead",
]
