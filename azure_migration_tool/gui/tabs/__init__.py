# Author: Satish Ch@uhan

"""Tab components for the main window."""

from gui.tabs.project_tab import ProjectTab
from gui.tabs.backup_restore_tab import BackupRestoreTab
from gui.tabs.full_migration_tab import FullMigrationTab
from gui.tabs.schema_tab import SchemaTab
from gui.tabs.data_migration_tab import DataMigrationTab
from gui.tabs.data_validation_tab import DataValidationTab
from gui.tabs.schema_validation_tab import SchemaValidationTab
from gui.tabs.adf_trigger_tab import ADFTriggerTab
from gui.tabs.mi_pitr_restore_tab import MiPitrRestoreTab
from gui.tabs.legacy_data_validation_tab import LegacyDataValidationTab
from gui.tabs.legacy_schema_validation_tab import LegacySchemaValidationTab

__all__ = [
    "ProjectTab",
    "BackupRestoreTab",
    "FullMigrationTab",
    "SchemaTab",
    "DataMigrationTab",
    "DataValidationTab",
    "SchemaValidationTab",
    "ADFTriggerTab",
    "MiPitrRestoreTab",
    "LegacyDataValidationTab",
    "LegacySchemaValidationTab",
]