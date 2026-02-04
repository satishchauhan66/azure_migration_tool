"""
Schema diff schemas for tracking differences between source and target.
"""

from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class SchemaDiffBase(BaseModel):
    """Base schema for schema diff records."""
    run_id: str
    source_object_type: Optional[str] = None
    source_schema_name: Optional[str] = None
    source_object_name: Optional[str] = None
    destination_schema_name: Optional[str] = None
    destination_object_name: Optional[str] = None
    change_type: Optional[str] = None
    element_path: Optional[str] = None
    error_description: Optional[str] = None


class SchemaDiffCreate(SchemaDiffBase):
    """Schema for creating a new schema diff record."""
    pass


class SchemaDiffUpdate(BaseModel):
    """Schema for updating an existing schema diff record."""
    source_object_type: Optional[str] = None
    source_schema_name: Optional[str] = None
    source_object_name: Optional[str] = None
    destination_schema_name: Optional[str] = None
    destination_object_name: Optional[str] = None
    change_type: Optional[str] = None
    element_path: Optional[str] = None
    error_description: Optional[str] = None


class SchemaDiffRead(SchemaDiffBase):
    """Schema for reading a schema diff record with database fields."""
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
