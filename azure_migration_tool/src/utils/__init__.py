# Author: Sa-tish Chauhan

"""Utility modules for database operations."""

from .azure_compat import (
    filter_azure_incompatible_batches,
    is_azure_compatible,
    should_skip_azure_error,
    should_skip_default_constraint_error,
    should_skip_index_error,
)
from .config import (
    get_config_value,
    get_input,
    get_yes_no,
    load_config_file,
    merge_configs,
    save_config_file,
)
from .database import (
    build_conn_str,
    pick_sql_driver,
    resolve_password,
)
from .logging import setup_logger
from .paths import (
    qident,
    safe_name,
    short_slug,
    utc_iso,
    utc_ts_compact,
)
from .sql import (
    split_sql_on_go,
    sql_header,
    type_sql,
)

__all__ = [
    # Azure Compatibility
    "filter_azure_incompatible_batches",
    "is_azure_compatible",
    "should_skip_azure_error",
    "should_skip_default_constraint_error",
    "should_skip_index_error",
    # Config
    "get_config_value",
    "get_input",
    "get_yes_no",
    "load_config_file",
    "merge_configs",
    "save_config_file",
    # Database
    "build_conn_str",
    "pick_sql_driver",
    "resolve_password",
    # Logging
    "setup_logger",
    # Paths
    "qident",
    "safe_name",
    "short_slug",
    "utc_iso",
    "utc_ts_compact",
    # SQL
    "split_sql_on_go",
    "sql_header",
    "type_sql",
]

