"""
Python-only Legacy validation module (DB2 vs Azure SQL).
No PySpark; uses jaydebeapi, pyodbc, and pandas.
"""

from .config import load_config, get_output_dir
from .schema_service import LegacySchemaValidationService
from .data_service import LegacyDataValidationService

__all__ = [
    "load_config",
    "get_output_dir",
    "LegacySchemaValidationService",
    "LegacyDataValidationService",
]
