# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Open DB2 (jaydebeapi) and Azure SQL (pyodbc) connections from config.
Config can be a file path (str) or an in-memory dict (no file stored).
"""

import sys
from pathlib import Path
from typing import Any, Dict, Optional, Union

# Ensure gui.utils is importable from azure_migration_tool
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from .config import load_config, normalize_config


def _get_config(config_path_or_dict: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Return normalized config from path or dict."""
    if isinstance(config_path_or_dict, dict):
        return normalize_config(config_path_or_dict)
    return load_config(config_path_or_dict)


def _map_azure_auth(authentication: str) -> str:
    """Map config authentication to database_utils auth string."""
    a = (authentication or "").strip()
    mapping = {
        "SqlPassword": "sql",
        "ActiveDirectoryPassword": "entra_password",
        "ActiveDirectoryInteractive": "entra_mfa",
        "IntegratedSecurity": "windows",
    }
    return mapping.get(a, "sql")


def connect_db2(config_path_or_dict: Union[str, Dict[str, Any]], logger=None):
    """
    Open DB2 connection via gui.utils.database_utils._connect_to_db2_jdbc_internal.
    config_path_or_dict: path to JSON file or in-memory config dict (no file stored).
    """
    from gui.utils.database_utils import _connect_to_db2_jdbc_internal

    config = _get_config(config_path_or_dict)
    db2 = config.get("db2", {})
    host = db2.get("host", "").strip()
    port = int(db2.get("port", 50000))
    database = db2.get("database", "").strip()
    user = db2.get("username", db2.get("user", "")).strip()
    password = db2.get("password", "")
    return _connect_to_db2_jdbc_internal(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        logger=logger,
    )


def connect_azure_sql(config_path_or_dict: Union[str, Dict[str, Any]], logger=None):
    """
    Open Azure SQL connection via gui.utils.database_utils.connect_to_any_database.
    config_path_or_dict: path to JSON file or in-memory config dict (no file stored).
    """
    from gui.utils.database_utils import connect_to_any_database

    config = _get_config(config_path_or_dict)
    az = config.get("azure_sql", {})
    server = az.get("server", "").strip()
    database = az.get("database", "").strip()
    user = az.get("username", az.get("user", "")).strip()
    password = az.get("password", "")
    auth_config = az.get("authentication", "SqlPassword")
    auth = _map_azure_auth(auth_config)
    return connect_to_any_database(
        server=server,
        database=database,
        auth=auth,
        user=user,
        password=password,
        db_type="sqlserver",
    )


def connect_source_sql(config_path_or_dict: Union[str, Dict[str, Any]], logger=None):
    """
    Open SQL Server source connection via gui.utils.database_utils.connect_to_any_database.
    config_path_or_dict: path to JSON file or in-memory config dict (no file stored).
    Used when source database is SQL Server instead of DB2.
    """
    from gui.utils.database_utils import connect_to_any_database

    config = _get_config(config_path_or_dict)
    sql = config.get("source_sql", {})
    server = sql.get("server", "").strip()
    database = sql.get("database", "").strip()
    user = sql.get("username", sql.get("user", "")).strip()
    password = sql.get("password", "")
    auth_config = sql.get("authentication", "SqlPassword")
    auth = _map_azure_auth(auth_config)
    return connect_to_any_database(
        server=server,
        database=database,
        auth=auth,
        user=user,
        password=password,
        db_type="sqlserver",
    )


def connect_source(config_path_or_dict: Union[str, Dict[str, Any]], logger=None):
    """
    Connect to the source database based on source_db_type in config.
    Supports 'db2' and 'sqlserver' source types.
    """
    config = _get_config(config_path_or_dict)
    source_type = config.get("source_db_type", "").strip().lower()
    
    # If source_db_type is not set, try to infer from available config sections
    if not source_type:
        if "db2" in config and config["db2"]:
            source_type = "db2"
        elif "source_sql" in config and config["source_sql"]:
            source_type = "sqlserver"
        else:
            # Default to db2 for backward compatibility, but warn
            if logger:
                logger.warning("source_db_type not set in config, defaulting to 'db2'. Set source_db_type explicitly to avoid this warning.")
            source_type = "db2"
    
    if source_type == "db2":
        return connect_db2(config_path_or_dict, logger)
    else:
        # SQL Server source
        return connect_source_sql(config_path_or_dict, logger)


def connect_destination(config_path_or_dict: Union[str, Dict[str, Any]], logger=None):
    """
    Connect to the destination database based on destination_db_type in config.
    Currently supports 'sqlserver' / 'azure_sql'.
    """
    config = _get_config(config_path_or_dict)
    dest_type = config.get("destination_db_type", "sqlserver").lower()
    
    # Currently only SQL Server / Azure SQL destination is supported
    return connect_azure_sql(config_path_or_dict, logger)
