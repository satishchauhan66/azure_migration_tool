"""
Database utility functions for GUI.
"""

import pyodbc
import logging
from typing import List, Optional, Union
import sys
from pathlib import Path
import struct
import os

# Add parent directories to path
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

try:
    from src.utils.database import pick_sql_driver, connect_to_database, connect_to_db2_jdbc
except ImportError:
    try:
        from utils.database import pick_sql_driver, connect_to_database
        connect_to_db2_jdbc = None
    except ImportError:
        pick_sql_driver = None
        connect_to_database = None
        connect_to_db2_jdbc = None


def _connect_to_db2_jdbc_internal(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    timeout: int = 30,
    logger: Optional[logging.Logger] = None
):
    """
    Connect to DB2 database using JDBC (via jaydebeapi).
    
    This function is used as a fallback when the external DB2 connection module is not available.
    
    Args:
        host: DB2 server hostname
        port: DB2 port (default 50000)
        database: Database name
        user: Username
        password: Password
        timeout: Connection timeout in seconds
        logger: Optional logger
    
    Returns:
        JDBC connection object
    
    Raises:
        ImportError: If jaydebeapi or jpype1 is not installed
        Exception: If connection fails
    """
    try:
        import jaydebeapi
        import jpype
    except ImportError as e:
        raise ImportError(
            "DB2 JDBC support not available. Run: pip install jaydebeapi jpype1"
        ) from e
    
    if logger:
        logger.info(f"Connecting to DB2 at {host}:{port}/{database} via JDBC...")
    
    # Find the DB2 JDBC driver JAR file
    # Get the azure_migration_tool directory (parent of gui/utils)
    azure_tool_dir = Path(__file__).parent.parent.parent
    
    # Common locations to check
    jar_paths = [
        # Environment variable
        os.environ.get("DB2_JDBC_DRIVER_PATH", ""),
        # Relative to azure_migration_tool folder (where user placed it)
        os.path.join(str(azure_tool_dir), "drivers", "db2jcc4.jar"),
        os.path.join(str(azure_tool_dir), "drivers", "db2jcc.jar"),
        # Relative to project root
        os.path.join(str(parent_dir), "drivers", "db2jcc4.jar"),
        os.path.join(str(parent_dir), "drivers", "db2jcc.jar"),
        os.path.join(str(parent_dir), "lib", "db2jcc4.jar"),
        os.path.join(str(parent_dir), "lib", "db2jcc.jar"),
        # Common Windows locations
        r"C:\Program Files\IBM\SQLLIB\java\db2jcc4.jar",
        r"C:\Program Files\IBM\SQLLIB\java\db2jcc.jar",
        r"C:\IBM\SQLLIB\java\db2jcc4.jar",
        r"C:\IBM\SQLLIB\java\db2jcc.jar",
    ]
    
    driver_jar = None
    for path in jar_paths:
        if not path or not os.path.exists(path):
            continue
        if not os.path.isfile(path) or not str(path).lower().endswith(".jar"):
            continue
        driver_jar = path
        break

    if not driver_jar:
        raise FileNotFoundError(
            "DB2 JDBC driver JAR not found or invalid (must be a .jar file). Please either:\n"
            "1. Set the DB2_JDBC_DRIVER_PATH environment variable to the full path of db2jcc4.jar\n"
            "2. Place db2jcc4.jar in the 'drivers' folder under azure_migration_tool (or 'lib' in project root)\n"
            "3. Install IBM Data Server Driver Package\n\n"
            "You can download the driver from:\n"
            "https://www.ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads"
        )

    driver_jar = os.path.abspath(driver_jar)
    if logger:
        logger.info(f"Using DB2 JDBC driver: {driver_jar}")

    # Classpath safe for JVM on Windows: absolute path, forward slashes, quoted if path has spaces
    path_for_cp = driver_jar.replace("\\", "/")
    if " " in path_for_cp:
        classpath_arg = f'-Djava.class.path="{path_for_cp}"'
    else:
        classpath_arg = f"-Djava.class.path={path_for_cp}"

    # Start JVM if not already started
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), classpath_arg)

    # If JVM was already started, verify DB2 driver is on classpath; if not, raise clear error
    if jpype.isJVMStarted():
        try:
            jpype.JClass("com.ibm.db2.jcc.DB2Driver")
        except (TypeError, Exception):
            raise RuntimeError(
                "The JVM was already started without the DB2 driver. The DB2 JDBC driver (db2jcc4.jar) "
                "must be on the classpath when the JVM starts.\n\n"
                "Please restart the application and run Legacy Data (or Schema) validation again so that "
                "this tool can start the JVM with the DB2 JAR. Ensure no other program or plugin starts "
                "Java before you use DB2 comparison.\n\n"
                "You can also set DB2_JDBC_DRIVER_PATH to the full path of db2jcc4.jar before starting the app."
            ) from None

    # JDBC connection URL
    jdbc_url = f"jdbc:db2://{host}:{port}/{database}"
    jdbc_driver = "com.ibm.db2.jcc.DB2Driver"

    try:
        conn = jaydebeapi.connect(
            jdbc_driver,
            jdbc_url,
            [user, password],
            driver_jar,
        )
        
        if logger:
            logger.info(f"Successfully connected to DB2 database: {database}")
        
        return conn
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to connect to DB2: {e}")
        raise

logger = logging.getLogger(__name__)


def connect_to_any_database(
    server: str,
    database: str,
    auth: str,
    user: str,
    password: Optional[str],
    db_type: str = "sqlserver",
    port: int = 50000,
    driver: Optional[str] = None,
    timeout: int = 30
) -> Union[pyodbc.Connection, object]:
    """
    Connect to any database type (SQL Server or DB2).
    
    This is the unified connection function that should be used throughout the GUI.
    
    Args:
        server: Server name/hostname
        database: Database name
        auth: Authentication type (for SQL Server: entra_mfa, entra_password, sql, windows)
        user: Username
        password: Password
        db_type: Database type - "sqlserver" or "db2"
        port: Port number (used for DB2, default 50000)
        driver: ODBC driver name (auto-detected if not provided)
        timeout: Connection timeout in seconds
    
    Returns:
        Connection object (pyodbc.Connection for SQL Server, jaydebeapi connection for DB2)
    
    Raises:
        Exception: If connection fails
    """
    if db_type == "db2":
        # Use DB2 JDBC connection
        if connect_to_db2_jdbc is not None:
            # Use external implementation if available
            return connect_to_db2_jdbc(
                host=server,
                port=port,
                database=database,
                user=user,
                password=password,
                timeout=timeout,
                logger=logger
            )
        else:
            # Use internal implementation
            return _connect_to_db2_jdbc_internal(
                host=server,
                port=port,
                database=database,
                user=user,
                password=password,
                timeout=timeout,
                logger=logger
            )
    else:
        # SQL Server connection
        return connect_with_msal_cache(
            server=server,
            database=database,
            auth=auth,
            user=user,
            password=password,
            driver=driver,
            timeout=timeout
        )


def connect_with_msal_cache(
    server: str,
    database: str,
    auth: str,
    user: str,
    password: Optional[str],
    driver: Optional[str] = None,
    timeout: int = 30
) -> pyodbc.Connection:
    """
    Connect to SQL Server with MSAL token cache support.
    
    For entra_mfa authentication:
    1. Tries to get cached MSAL token first (no MFA prompt if cached)
    2. If token available: Uses SQL_COPT_SS_ACCESS_TOKEN connection attribute
    3. If no token: Falls back to ActiveDirectoryInteractive (will prompt once, then cache)
    
    Args:
        server: Server name/address
        database: Database name
        auth: Authentication type (entra_mfa, entra_password, sql, windows)
        user: Username (required for entra_mfa, entra_password, sql)
        password: Password (required for entra_password, sql)
        driver: ODBC driver name (auto-detected if not provided)
        timeout: Connection timeout in seconds
    
    Returns:
        pyodbc.Connection object
    
    Raises:
        Exception: If connection fails
    """
    # Validate server name
    if not server or not server.strip():
        raise ValueError("Server name cannot be empty")
    
    server = server.strip()
    auth = (auth or "").strip().lower()
    
    # Validate server doesn't look like an email domain
    if "@" in server or (server.endswith(".com") and len(server.split(".")) < 3):
        raise ValueError(f"Invalid server name: '{server}'. Server name appears to be an email domain, not a SQL Server address.")
    
    # Pick driver if not provided
    if not driver:
        if pick_sql_driver:
            temp_logger = logging.getLogger(__name__)
            temp_logger.setLevel(logging.WARNING)
            driver = pick_sql_driver(temp_logger)
        else:
            drivers = pyodbc.drivers()
            preferred = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "ODBC Driver 13 for SQL Server",
            ]
            driver = None
            for name in preferred:
                if name in drivers:
                    driver = name
                    break
            if not driver:
                for d in drivers:
                    if "SQL Server" in d:
                        driver = d
                        break
            if not driver:
                raise RuntimeError("No SQL Server ODBC driver found")
    
    # Try to use connect_to_database if available (it already has MSAL token support)
    if connect_to_database:
        try:
            temp_logger = logging.getLogger(__name__)
            temp_logger.setLevel(logging.WARNING)
            return connect_to_database(
                server=server,
                db=database,
                user=user,
                driver=driver,
                auth=auth,
                password=password,
                timeout=timeout,
                logger=temp_logger
            )
        except Exception:
            # If connect_to_database fails, fall through to manual connection
            pass
    
    # Fallback: manual connection with MSAL token support
    driver_str = "{" + driver + "}"
    base = f"Driver={driver_str};Server={server};Database={database};Encrypt=yes;TrustServerCertificate=no;"
    
    if auth == "entra_mfa":
        if not user:
            raise ValueError("For entra_mfa auth, user (UPN/email) is required.")
        
        # Try to use MSAL token cache first (no MFA prompt if token is cached)
        try:
            # Import azure_token_cache from root level
            root_dir = Path(__file__).parent.parent.parent.parent
            root_str = str(root_dir)
            if root_str not in sys.path:
                sys.path.insert(0, root_str)
            
            from azure_token_cache import get_cached_token
            
            access_token = get_cached_token(user, refresh_if_expiring_soon=True)
            if access_token:
                # Use SQL_COPT_SS_ACCESS_TOKEN (1256) for token-based connection
                token_bytes = access_token.encode('utf-16-le')
                token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
                # Connection string should NOT have UID/PWD/Authentication when using token
                return pyodbc.connect(base, timeout=timeout, attrs_before={1256: token_struct})
        except (ImportError, Exception):
            # MSAL not available or token fetch failed, fall back to interactive
            pass
        
        # Fallback to interactive authentication (will prompt once, then cache)
        conn_str = base + f"UID={user};Authentication=ActiveDirectoryInteractive;"
        return pyodbc.connect(conn_str, timeout=timeout)
    
    elif auth == "entra_password":
        if not user or not password:
            raise ValueError("For entra_password auth, user and password are required.")
        conn_str = base + f"UID={user};PWD={password};Authentication=ActiveDirectoryPassword;"
        return pyodbc.connect(conn_str, timeout=timeout)
    
    elif auth == "sql":
        if not user or not password:
            raise ValueError("For sql auth, user and password are required.")
        conn_str = base + f"UID={user};PWD={password};"
        return pyodbc.connect(conn_str, timeout=timeout)
    
    elif auth == "windows":
        conn_str = base + "Trusted_Connection=yes;"
        return pyodbc.connect(conn_str, timeout=timeout)
    
    else:
        raise ValueError(f"Unknown auth type: {auth}")


def list_databases(
    server: str,
    auth: str,
    user: str,
    password: Optional[str],
    timeout: int = 10
) -> List[str]:
    """
    List all databases on a SQL Server.
    
    Args:
        server: Server name/address
        auth: Authentication type (entra_mfa, entra_password, sql, windows)
        user: Username (required for entra_mfa, entra_password, sql)
        password: Password (required for entra_password, sql)
        timeout: Connection timeout in seconds
    
    Returns:
        List of database names
    
    Raises:
        Exception: If connection fails
    """
    # Connect to master database to list all databases
    try:
        # Try to use the utility function
        if connect_to_database:
            # Get logger
            import logging
            temp_logger = logging.getLogger(__name__)
            temp_logger.setLevel(logging.WARNING)  # Suppress info messages
            
            # Pick driver
            if pick_sql_driver:
                driver = pick_sql_driver(temp_logger)
            elif pick_driver_legacy:
                driver = pick_driver_legacy(temp_logger)
            else:
                # Fallback: try to find driver manually
                drivers = pyodbc.drivers()
                preferred = [
                    "ODBC Driver 18 for SQL Server",
                    "ODBC Driver 17 for SQL Server",
                    "ODBC Driver 13 for SQL Server",
                ]
                driver = None
                for name in preferred:
                    if name in drivers:
                        driver = name
                        break
                if not driver:
                    for d in drivers:
                        if "SQL Server" in d:
                            driver = d
                            break
                if not driver:
                    raise RuntimeError("No SQL Server ODBC driver found")
            
            # Validate server name before connecting
            if not server or not server.strip():
                raise ValueError("Server name cannot be empty")
            
            # Validate server doesn't look like an email domain
            server_clean = server.strip()
            if "@" in server_clean or (server_clean.endswith(".com") and len(server_clean.split(".")) < 3):
                raise ValueError(f"Invalid server name: '{server_clean}'. Server name appears to be an email domain, not a SQL Server address.")
            
            # Connect to master database
            conn = connect_to_database(
                server=server_clean,
                db="master",  # Connect to master to list all databases
                user=user,
                driver=driver,
                auth=auth,
                password=password,
                timeout=timeout,
                logger=temp_logger
            )
        else:
            # Fallback: use pyodbc directly with MSAL token support
            # Pick driver
            drivers = pyodbc.drivers()
            preferred = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "ODBC Driver 13 for SQL Server",
            ]
            driver = None
            for name in preferred:
                if name in drivers:
                    driver = name
                    break
            if not driver:
                for d in drivers:
                    if "SQL Server" in d:
                        driver = d
                        break
            if not driver:
                raise RuntimeError("No SQL Server ODBC driver found")
            
            # Build connection string
            driver_str = "{" + driver + "}"
            base = f"Driver={driver_str};Server={server};Database=master;Encrypt=yes;TrustServerCertificate=no;"
            
            if auth == "entra_mfa":
                # Try to use MSAL token cache first (no MFA prompt if token is cached)
                try:
                    # Import azure_token_cache from root level
                    import sys
                    from pathlib import Path
                    root_dir = Path(__file__).parent.parent.parent.parent
                    root_str = str(root_dir)
                    if root_str not in sys.path:
                        sys.path.insert(0, root_str)
                    
                    from azure_token_cache import get_cached_token
                    import struct
                    
                    access_token = get_cached_token(user, refresh_if_expiring_soon=True)
                    if access_token:
                        # Use SQL_COPT_SS_ACCESS_TOKEN (1256) for token-based connection
                        token_bytes = access_token.encode('utf-16-le')
                        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
                        # Connection string should NOT have UID/PWD/Authentication when using token
                        conn = pyodbc.connect(base, timeout=timeout, attrs_before={1256: token_struct})
                    else:
                        # No cached token, fall back to interactive
                        conn_str = base + f"UID={user};Authentication=ActiveDirectoryInteractive;"
                        conn = pyodbc.connect(conn_str, timeout=timeout)
                except (ImportError, Exception) as e:
                    # MSAL not available or token fetch failed, fall back to interactive
                    conn_str = base + f"UID={user};Authentication=ActiveDirectoryInteractive;"
                    conn = pyodbc.connect(conn_str, timeout=timeout)
            elif auth == "entra_password":
                conn_str = base + f"UID={user};PWD={password};Authentication=ActiveDirectoryPassword;"
                conn = pyodbc.connect(conn_str, timeout=timeout)
            elif auth == "sql":
                conn_str = base + f"UID={user};PWD={password};"
                conn = pyodbc.connect(conn_str, timeout=timeout)
            elif auth == "windows":
                conn_str = base + "Trusted_Connection=yes;"
                conn = pyodbc.connect(conn_str, timeout=timeout)
            else:
                raise ValueError(f"Unknown auth type: {auth}")
        
        try:
            cursor = conn.cursor()
            # Query to list all databases (excluding system databases)
            cursor.execute("""
                SELECT name 
                FROM sys.databases 
                WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
                ORDER BY name
            """)
            
            databases = [row[0] for row in cursor.fetchall()]
            return databases
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error listing databases from {server}: {e}")
        raise
