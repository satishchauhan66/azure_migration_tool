"""Tab components for the main window."""

from gui.tabs.project_tab import ProjectTab
from gui.tabs.full_migration_tab import FullMigrationTab
from gui.tabs.schema_tab import SchemaTab
from gui.tabs.data_migration_tab import DataMigrationTab
from gui.tabs.data_validation_tab import DataValidationTab
from gui.tabs.schema_validation_tab import SchemaValidationTab
from gui.tabs.legacy_data_validation_tab import LegacyDataValidationTab
from gui.tabs.legacy_schema_validation_tab import LegacySchemaValidationTab

__all__ = [
    "ProjectTab",
    "FullMigrationTab",
    "SchemaTab",
    "DataMigrationTab",
    "DataValidationTab",
    "SchemaValidationTab",
    "LegacyDataValidationTab",
    "LegacySchemaValidationTab",
]