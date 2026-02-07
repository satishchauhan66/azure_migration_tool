# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Server configuration manager for saving and loading server connection details.
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Configuration file location (in user's app data directory)
CONFIG_DIR = Path.home() / ".azure_migration_tool"
CONFIG_FILE = CONFIG_DIR / "saved_servers.json"


def ensure_config_dir():
    """Ensure the configuration directory exists."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def load_saved_servers() -> List[Dict]:
    """
    Load saved server configurations from JSON file.
    
    Returns:
        List of server configuration dictionaries
    """
    ensure_config_dir()
    
    if not CONFIG_FILE.exists():
        return []
    
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            servers = json.load(f)
            # Ensure it's a list
            if not isinstance(servers, list):
                return []
            return servers
    except Exception as e:
        logger.error(f"Error loading saved servers: {e}")
        return []


def save_server_config(
    server: str,
    auth: str,
    user: str,
    password: str,
    display_name: Optional[str] = None,
    db_type: Optional[str] = None,
    port: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> bool:
    """
    Save a server configuration.
    
    Args:
        server: Server name/address
        auth: Authentication type (entra_mfa, entra_password, sql, windows)
        user: Username
        password: Password (will be stored, consider encryption for production)
        display_name: Optional display name for the server
        db_type: Database type (sqlserver or db2)
        port: Port number (especially for DB2)
        database: Database name
        schema: Schema name (especially for DB2)
    
    Returns:
        True if saved successfully, False otherwise
    """
    ensure_config_dir()
    
    try:
        servers = load_saved_servers()
        
        # Check if server already exists (match by server, auth, user, and db_type)
        existing_index = None
        for idx, srv in enumerate(servers):
            if (srv.get('server') == server and 
                srv.get('auth') == auth and 
                srv.get('user') == user and
                srv.get('db_type', 'sqlserver') == (db_type or 'sqlserver')):
                existing_index = idx
                break
        
        # Create server config
        server_config = {
            'server': server,
            'auth': auth,
            'user': user,
            'password': password,  # Note: In production, consider encrypting this
            'display_name': display_name or f"{server} ({auth})",
            'db_type': db_type or 'sqlserver',
            'port': port or '',
            'database': database or '',
            'schema': schema or ''
        }
        
        if existing_index is not None:
            # Update existing
            servers[existing_index] = server_config
        else:
            # Add new
            servers.append(server_config)
        
        # Save to file
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(servers, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved server configuration: {server} ({auth}) - db_type: {db_type}, database: {database}, schema: {schema}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving server configuration: {e}")
        return False


def delete_server_config(server: str, auth: str, user: str) -> bool:
    """
    Delete a server configuration.
    
    Args:
        server: Server name/address
        auth: Authentication type
        user: Username
    
    Returns:
        True if deleted successfully, False otherwise
    """
    try:
        servers = load_saved_servers()
        
        # Find and remove the server
        original_count = len(servers)
        servers = [
            srv for srv in servers
            if not (srv.get('server') == server and 
                   srv.get('auth') == auth and 
                   srv.get('user') == user)
        ]
        
        if len(servers) < original_count:
            # Save updated list
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(servers, f, indent=2, ensure_ascii=False)
            logger.info(f"Deleted server configuration: {server} ({auth})")
            return True
        else:
            logger.warning(f"Server configuration not found: {server} ({auth})")
            return False
            
    except Exception as e:
        logger.error(f"Error deleting server configuration: {e}")
        return False


def get_server_display_names() -> List[str]:
    """
    Get list of display names for saved servers.
    
    Returns:
        List of display names
    """
    servers = load_saved_servers()
    return [srv.get('display_name', f"{srv.get('server')} ({srv.get('auth')})") for srv in servers]


def get_server_config(display_name: str) -> Optional[Dict]:
    """
    Get server configuration by display name.
    
    Args:
        display_name: Display name of the server
    
    Returns:
        Server configuration dictionary or None if not found
    """
    servers = load_saved_servers()
    for srv in servers:
        if srv.get('display_name') == display_name:
            return srv
    return None
