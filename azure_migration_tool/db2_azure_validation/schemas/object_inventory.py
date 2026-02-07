# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Object inventory schemas for tracking database objects.
"""

from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional


class ObjectInventoryBase(BaseModel):
    """Base schema for object inventory records."""
    run_id: str
    server_name: str
    database_name: str
    object_name: str
    object_type: str


class ObjectInventoryCreate(ObjectInventoryBase):
    """Schema for creating a new object inventory record."""
    pass


class ObjectInventoryUpdate(BaseModel):
    """Schema for updating an existing object inventory record."""
    object_type: Optional[str] = None


class ObjectInventoryRead(ObjectInventoryBase):
    """Schema for reading an object inventory record with database fields."""
    id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)
