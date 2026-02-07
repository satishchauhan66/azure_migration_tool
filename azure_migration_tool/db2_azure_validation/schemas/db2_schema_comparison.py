# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
DB2 to Azure schema comparison schemas.
"""

from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class DB2SchemaComparisonBase(BaseModel):
    """Base schema for DB2 schema comparison records."""
    run_id: str
    source_schema: str
    source_object_name: str
    source_object_type: str
    source_definition: Optional[str] = None
    target_schema: str
    target_object_name: str
    target_object_type: str
    target_definition: Optional[str] = None
    comparison_status: str
    difference_details: Optional[str] = None


class DB2SchemaComparisonCreate(DB2SchemaComparisonBase):
    """Schema for creating a new comparison record."""
    pass


class DB2SchemaComparisonUpdate(BaseModel):
    """Schema for updating an existing comparison record."""
    target_definition: Optional[str] = None
    comparison_status: Optional[str] = None
    difference_details: Optional[str] = None


class DB2SchemaComparison(DB2SchemaComparisonBase):
    """Full schema comparison record with database fields."""
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class SchemaComparisonRequest(BaseModel):
    """Request schema for schema comparison operations."""
    source_schema: Optional[str] = None
    target_schema: Optional[str] = None
    object_types: Optional[List[str]] = None  # ['TABLE', 'VIEW', 'PROCEDURE', 'FUNCTION', etc.]
    include_definitions: bool = True


class SchemaComparisonResponse(BaseModel):
    """Response schema for schema comparison operations."""
    run_id: str
    total_objects_compared: int
    matches: int
    differences: int
    missing_in_target: int
    missing_in_source: int
    comparison_details: List[DB2SchemaComparisonCreate]


class RowCountComparisonRequest(BaseModel):
    """Request schema for row count comparison."""
    source_schema: Optional[str] = None
    target_schema: Optional[str] = None
    object_types: Optional[List[str]] = None  # ["TABLE","VIEW"]


class RowCountComparisonResponse(BaseModel):
    """Response schema for row count comparison."""
    csv_file: str


class ColumnNullCheckRequest(BaseModel):
    """Request schema for column null check comparison."""
    # Optional schema scoping; when omitted, compare across all matched schemas
    source_schema: Optional[str] = None
    target_schema: Optional[str] = None


class ColumnNullCheckResponse(BaseModel):
    """Response schema for column null check comparison."""
    csv_file: str
