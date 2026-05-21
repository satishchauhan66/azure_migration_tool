# Author: Sa-tish Chauhan

"""Schema compare and repair (SQL Compare–style) for live source vs target databases."""

from .schema_compare import compare_schema_catalogs, fetch_live_catalog, load_backup_catalog
from .repair_generator import (
    generate_repair_items,
    generate_repair_script,
    repair_items_to_sql,
    write_repair_script,
)
from .apply_repair import (
    apply_redgate_deployment_script,
    apply_repair_item,
    apply_repair_items,
    apply_schema_repair,
)
from .normalize import definition_hash, normalize_module_text_for_compare

__all__ = [
    "compare_schema_catalogs",
    "fetch_live_catalog",
    "load_backup_catalog",
    "generate_repair_items",
    "generate_repair_script",
    "repair_items_to_sql",
    "write_repair_script",
    "apply_schema_repair",
    "apply_repair_item",
    "apply_repair_items",
    "apply_redgate_deployment_script",
    "definition_hash",
    "normalize_module_text_for_compare",
]
