# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Validation count schemas for tracking migration validation results.
"""

from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional


class ValidationCountBase(BaseModel):
    """Base schema for validation counts."""
    run_id: str
    server_name: str
    database_name: str
    table_count: Optional[int] = None
    procedure_count: Optional[int] = None
    index_count: Optional[int] = None
    function_count: Optional[int] = None
    trigger_count: Optional[int] = None
    synonyms_count: Optional[int] = None
    user_count: Optional[int] = None


class ValidationCountCreate(ValidationCountBase):
    """Schema for creating a new validation count record."""
    pass


class ValidationCountUpdate(BaseModel):
    """Schema for updating an existing validation count record."""
    table_count: Optional[int] = None
    procedure_count: Optional[int] = None
    index_count: Optional[int] = None
    function_count: Optional[int] = None
    trigger_count: Optional[int] = None
    synonyms_count: Optional[int] = None
    user_count: Optional[int] = None


class ValidationCountRead(ValidationCountBase):
    """Schema for reading a validation count record with database fields."""
    id: int
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)
