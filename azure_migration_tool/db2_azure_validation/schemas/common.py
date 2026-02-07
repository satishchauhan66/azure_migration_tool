# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Common schemas and utilities for DB2 to Azure Migration Validation.
"""

from pydantic import BaseModel
from typing import Optional, List

# Import DataFrame only when available (for type hints)
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import lit, col
    PYSPARK_AVAILABLE = True
except ImportError:
    DataFrame = None
    PYSPARK_AVAILABLE = False


class Message(BaseModel):
    """Simple message response model."""
    message: str


class Pagination(BaseModel):
    """Pagination parameters for list endpoints."""
    limit: Optional[int] = 50
    offset: Optional[int] = 0


def get_unified_columns(include_change_type: bool = True) -> List[str]:
    """
    Return the canonical unified CSV columns (order enforced across all APIs).
    
    Args:
        include_change_type: When True, include ChangeType as a standardized column.
                           (Currently excluded from unified columns)
    
    Returns:
        List of column names in the standard order.
    """
    base = [
        "ValidationType",
        "Status",
        "ObjectType",
    ]
    # ChangeType intentionally excluded from unified columns
    base += [
        "SourceObjectName",
        "SourceSchemaName",
        "DestinationObjectName",
        "DestinationSchemaName",
        "ElementPath",
        "ErrorCode",
        "ErrorDescription",
        "DetailsJson",
    ]
    return base


def ensure_all_columns_as_strings(df: "DataFrame", all_cols: List[str]) -> "DataFrame":
    """
    Add any missing columns as nulls and cast all to string for safe union/save.
    
    The input DataFrame may already contain a subset of the columns.
    This ensures consistent column structure across different validation results.
    
    Args:
        df: Input PySpark DataFrame
        all_cols: List of all required column names
    
    Returns:
        DataFrame with all columns present and cast to string type.
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError("PySpark is required for this function")
    
    cur = set(df.columns)
    out = df
    for c in all_cols:
        if c not in cur:
            out = out.withColumn(c, lit(None))
    return out.select([col(c).cast("string").alias(c) for c in all_cols])
