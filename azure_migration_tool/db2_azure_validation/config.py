# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Configuration settings for DB2 to Azure Migration Validation Module.

This module provides environment-based configuration using Pydantic Settings.
Configuration can be set via environment variables or a .env file.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    """
    Configuration settings for database connections and validation.
    
    All settings can be overridden via environment variables.
    """
    
    # Azure SQL Settings
    azure_sql_driver: str = Field(
        default="ODBC Driver 18 for SQL Server",
        env="AZURE_SQL_DRIVER",
        description="ODBC driver for Azure SQL connections"
    )
    azure_sql_encrypt: str = Field(
        default="yes",
        env="AZURE_SQL_ENCRYPT",
        description="Enable encryption for Azure SQL connections"
    )
    azure_sql_trust_server_certificate: str = Field(
        default="no",
        env="AZURE_SQL_TRUST_SERVER_CERTIFICATE",
        description="Trust server certificate without validation"
    )
    azure_sql_port: int = Field(
        default=1433,
        env="AZURE_SQL_PORT",
        description="Port for Azure SQL connections"
    )
    azure_sql_force_tcp: str = Field(
        default="auto",
        env="AZURE_SQL_FORCE_TCP",
        description="Force TCP protocol for connections"
    )
    azure_sql_tenant_id: str = Field(
        default="",
        env="AZURE_SQL_TENANT_ID",
        description="Azure AD tenant ID for authentication"
    )
    azure_sql_login_hint: str = Field(
        default="",
        env="AZURE_SQL_LOGIN_HINT",
        description="Login hint for Azure AD authentication"
    )
    azure_sql_auth_mode: str = Field(
        default="default",
        env="AZURE_SQL_AUTH_MODE",
        description="Authentication mode: 'default', 'managed_identity', or 'password'"
    )
    azure_sql_connection_timeout_seconds: int = Field(
        default=30,
        env="AZURE_SQL_CONNECTION_TIMEOUT_SECONDS",
        description="Connection timeout in seconds"
    )
    
    # Spark Settings
    spark_master: str = Field(
        default="local[*]",
        env="SPARK_MASTER",
        description="Spark master URL"
    )
    spark_shuffle_partitions: str = Field(
        default="16",
        env="SPARK_SHUFFLE_PARTITIONS",
        description="Number of shuffle partitions"
    )
    jdbc_fetchsize: str = Field(
        default="50000",
        env="JDBC_FETCHSIZE",
        description="JDBC fetch size for database queries"
    )
    
    # Validation Rule Settings
    dv_dtype_rules: str = Field(
        default="",
        env="DV_DTYPE_RULES",
        description="Data type validation rules"
    )
    dv_default_value_rules: str = Field(
        default="",
        env="DV_DEFAULT_VALUE_RULES",
        description="Default value validation rules"
    )
    dv_index_rules: str = Field(
        default="",
        env="DV_INDEX_RULES",
        description="Index validation rules"
    )
    dv_fk_rules: str = Field(
        default="",
        env="DV_FK_RULES",
        description="Foreign key validation rules"
    )
    
    # Output Settings
    output_dir: str = Field(
        default="outputs",
        env="VALIDATION_OUTPUT_DIR",
        description="Directory for output files"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra fields that are not defined in the model


# Global settings instance
settings = Settings()


def get_output_dir() -> str:
    """Get the output directory path, creating it if it doesn't exist."""
    output_dir = settings.output_dir
    if not os.path.isabs(output_dir):
        # Make relative path absolute from current working directory
        output_dir = os.path.join(os.getcwd(), output_dir)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def get_config_path(filename: str = "database_config.json") -> str:
    """
    Get the path to a configuration file.
    
    Looks in the following locations (in order):
    1. Current working directory
    2. Module directory
    3. User's home directory
    """
    # Check current working directory
    cwd_path = os.path.join(os.getcwd(), filename)
    if os.path.isfile(cwd_path):
        return cwd_path
    
    # Check module directory
    module_dir = os.path.dirname(os.path.abspath(__file__))
    module_path = os.path.join(module_dir, filename)
    if os.path.isfile(module_path):
        return module_path
    
    # Check parent of module directory (package root)
    parent_path = os.path.join(os.path.dirname(module_dir), filename)
    if os.path.isfile(parent_path):
        return parent_path
    
    # Default to current working directory (will raise error if not found)
    return cwd_path
