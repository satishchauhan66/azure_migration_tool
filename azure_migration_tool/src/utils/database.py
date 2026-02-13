# Author: Satish Ch@uhan

"""Database connection utilities."""

import os
import sys
import logging
import struct
from typing import Optional, Tuple, Dict, Any

import pyodbc


def resolve_password(cli_value: Optional[str], default_value: Optional[str], env_key: str) -> Optional[str]:
    """Resolve password from CLI, default, or environment variable (in that order)"""
    if cli_value:
        return cli_value
    if default_value:
        return default_value
    return os.environ.get(env_key)


def get_driver_install_instructions() -> str:
    """Get instructions for installing ODBC driver."""
    is_64bit = sys.maxsize > 2**32
    arch = "64-bit" if is_64bit else "32-bit"
    
    return f"""
=== ODBC Driver Installation Required ===

Your Python is {arch}. Install the matching ODBC driver:

Option 1 - Download from Microsoft:
  https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
  Download "ODBC Driver 18 for SQL Server" ({arch})

Option 2 - Using winget (Windows 10/11):
  winget install Microsoft.msodbcsql18

Option 3 - Using the GUI:
  Run the Azure Migration Tool GUI and go to:
  Tools > Install ODBC Driver

After installation, restart your application.
"""


def check_odbc_driver() -> Tuple[bool, Optional[str]]:
    """
    Check if a SQL Server ODBC driver is available.
    
    Returns:
        Tuple of (is_available, driver_name or None)
    """
    try:
        drivers = pyodbc.drivers()
    except Exception:
        return False, None
    
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server",
    ]
    
    for name in preferred:
        if name in drivers:
            return True, name
    
    for d in drivers:
        if "SQL Server" in d:
            return True, d
    
    return False, None


def check_db2_driver() -> Tuple[bool, Optional[str]]:
    """
    Check if a DB2 ODBC driver is available.
    
    Returns:
        Tuple of (is_available, driver_name or None)
    """
    try:
        drivers = pyodbc.drivers()
    except Exception:
        return False, None
    
    # Common DB2 driver names
    preferred = [
        "IBM DB2 ODBC DRIVER",
        "IBM DB2 ODBC DRIVER - DB2COPY1",
        "IBM DB2 ODBC DRIVER - DB2COPY2",
        "DB2",
    ]
    
    for name in preferred:
        if name in drivers:
            return True, name
    
    # Check for any driver containing "DB2"
    for d in drivers:
        if "DB2" in d.upper():
            return True, d
    
    return False, None


def pick_sql_driver(logger) -> str:
    """
    Select best available SQL Server ODBC driver.
    Returns driver name or raises RuntimeError if none found.
    """
    drivers = pyodbc.drivers()
    logger.info(
        "ODBC drivers visible to pyodbc (%d): %s",
        len(drivers),
        ", ".join(drivers) if drivers else "NONE",
    )
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server",
    ]
    for name in preferred:
        if name in drivers:
            logger.info("Selected ODBC driver: %s", name)
            return name
    for d in reversed(drivers):
        if "SQL Server" in d:
            logger.info("Selected ODBC driver (fallback): %s", d)
            return d
    
    # No driver found - provide helpful error message
    error_msg = "No SQL Server ODBC driver found.\n" + get_driver_install_instructions()
    logger.error(error_msg)
    raise RuntimeError(error_msg)


def build_conn_str(
    server: str,
    db: str,
    user: str,
    driver: str,
    auth: str,
    password: Optional[str],
) -> str:
    """
    Build ODBC connection string for SQL Server.
    
    Args:
        server: Server name/address
        db: Database name
        user: Username
        driver: ODBC driver name
        auth: Authentication type (entra_mfa | entra_password | sql | windows)
        password: Password (required for entra_password and sql auth)
    
    Returns:
        Connection string
    
    Raises:
        ValueError: If auth type is invalid or required params missing
    """
    auth = (auth or "").strip().lower()
    # Use TrustServerCertificate=yes for Windows auth (on-prem SQL Server often has untrusted certs; matches SSMS behavior)
    trust_cert = "yes" if auth == "windows" else "no"
    base = f"Driver={{{driver}}};Server={server};Database={db};Encrypt=yes;TrustServerCertificate={trust_cert};"

    if auth == "entra_mfa":
        if not user:
            raise ValueError("For entra_mfa auth, user (UPN/email) is required.")
        
        # Try to get cached MSAL token first (persists across app restarts)
        # This provides better persistence than relying solely on ODBC's Windows Credential Manager
        try:
            # Try importing from root level (azure_token_cache.py)
            # Add root directory to path if not already there
            import sys
            from pathlib import Path
            root_dir = Path(__file__).parent.parent.parent
            root_str = str(root_dir)
            if root_str not in sys.path:
                sys.path.insert(0, root_str)
            
            from azure_token_cache import get_cached_token
            
            access_token = get_cached_token(user, refresh_if_expiring_soon=True)
            if access_token:
                # NOTE: ODBC Driver 18 doesn't support AccessToken= in connection strings
                # This function should not be used directly for entra_mfa - use connect_to_database() instead
                # For backward compatibility, we still return it but it may not work
                # The caller should use connect_to_database() which uses SQL_COPT_SS_ACCESS_TOKEN
                logger = logging.getLogger(__name__)
                logger.warning(
                    "build_conn_str() with entra_mfa returns AccessToken= which doesn't work with ODBC Driver 18. "
                    "Use connect_to_database() instead which uses SQL_COPT_SS_ACCESS_TOKEN."
                )
                # Still return it for backward compatibility, but it will likely fail
                return base + f"UID={user};AccessToken={access_token};"
        except ImportError:
            # MSAL library not available, fall back to interactive authentication
            # ODBC driver will cache in Windows Credential Manager (less reliable across restarts)
            pass
        except Exception as e:
            # Token fetch failed (e.g., cache file corrupted, network issue)
            # Log at debug level and fall back to interactive authentication
            logger = logging.getLogger(__name__)
            logger.debug(f"Could not get cached MSAL token for {user}: {e}, falling back to interactive auth")
        
        # Fallback to interactive authentication (ODBC will cache in Windows Credential Manager)
        # Note: This cache may not persist reliably across app restarts
        return base + f"UID={user};Authentication=ActiveDirectoryInteractive;"

    if auth == "entra_password":
        if not user or not password:
            raise ValueError("For entra_password auth, user and password are required.")
        return base + f"UID={user};PWD={password};Authentication=ActiveDirectoryPassword;"

    if auth == "sql":
        if not user or not password:
            raise ValueError("For sql auth, user and password are required (or set env variable).")
        return base + f"UID={user};PWD={password};"

    if auth == "windows":
        return base + "Trusted_Connection=yes;"

    raise ValueError(f"Unknown auth type '{auth}'. Use: entra_mfa | entra_password | sql | windows")


def connect_to_database(
    server: str,
    db: str,
    user: str,
    driver: str,
    auth: str,
    password: Optional[str],
    timeout: int = 30,
    logger: Optional[logging.Logger] = None,
) -> pyodbc.Connection:
    """
    Connect to SQL Server database with automatic token handling.
    
    For entra_mfa authentication, this function:
    1. Tries to get cached MSAL token
    2. If token available: Uses SQL_COPT_SS_ACCESS_TOKEN connection attribute
    3. If no token: Falls back to ActiveDirectoryInteractive
    
    Args:
        server: Server name/address
        db: Database name
        user: Username
        driver: ODBC driver name
        auth: Authentication type (entra_mfa | entra_password | sql | windows)
        password: Password (required for entra_password and sql auth)
        timeout: Connection timeout in seconds
        logger: Optional logger for debug messages
    
    Returns:
        pyodbc.Connection object
    """
    # Validate inputs
    if not server or not server.strip():
        raise ValueError(f"Server name cannot be empty. Received: '{server}'")
    
    server = server.strip()
    auth = (auth or "").strip().lower()
    
    # Validate server doesn't look like an email domain
    if "@" in server or (server.endswith(".com") and len(server.split(".")) < 3):
        raise ValueError(f"Invalid server name: '{server}'. Server name appears to be an email domain, not a SQL Server address.")
    
    # Use TrustServerCertificate=yes for Windows auth (on-prem SQL Server often has untrusted certs; matches SSMS behavior)
    trust_cert = "yes" if auth == "windows" else "no"
    base = f"Driver={{{driver}}};Server={server};Database={db};Encrypt=yes;TrustServerCertificate={trust_cert};"
    
    if auth == "entra_mfa":
        if not user:
            raise ValueError("For entra_mfa auth, user (UPN/email) is required.")
        
        # Try to get cached MSAL token first (persists across app restarts)
        try:
            # Try importing from root level (azure_token_cache.py)
            import sys
            from pathlib import Path
            root_dir = Path(__file__).parent.parent.parent
            root_str = str(root_dir)
            if root_str not in sys.path:
                sys.path.insert(0, root_str)
            
            from azure_token_cache import get_cached_token
            
            access_token = get_cached_token(user, refresh_if_expiring_soon=True)
            if access_token:
                # ODBC Driver 18 doesn't support AccessToken= in connection string
                # Use SQL_COPT_SS_ACCESS_TOKEN connection attribute instead (1256)
                if logger:
                    logger.debug("Using cached MSAL token via SQL_COPT_SS_ACCESS_TOKEN")
                
                # Build connection string WITHOUT UID, PWD, or Authentication when using token attribute
                # CRITICAL: ODBC Driver 18 requires connection string to have NO auth parameters when using SQL_COPT_SS_ACCESS_TOKEN
                conn_str = base
                
                if logger:
                    logger.debug(f"Connection string for token auth (no UID/PWD/Auth): {conn_str}")
                
                # SQL_COPT_SS_ACCESS_TOKEN = 1256
                # Token must be encoded as UTF-16-LE bytes and packed in struct: <I (4-byte length) + bytes
                token_bytes = access_token.encode('utf-16-le')
                token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
                
                if logger:
                    logger.debug(f"Token encoded: {len(token_bytes)} UTF-16-LE bytes, struct: {len(token_struct)} bytes")
                
                # Connect using attrs_before to pass token
                # This is the ONLY supported way to pass tokens with ODBC Driver 18
                try:
                    conn = pyodbc.connect(conn_str, timeout=timeout, attrs_before={1256: token_struct})
                    if logger:
                        logger.info("Successfully connected using MSAL token via SQL_COPT_SS_ACCESS_TOKEN")
                    return conn
                except Exception as conn_err:
                    # Token connection failed - retry once with refreshed token, then fail with clear message (do not trigger interactive in background)
                    if logger:
                        logger.warning(f"Token-based connection failed: {type(conn_err).__name__}: {conn_err}; retrying with refreshed token")
                    access_token_2 = get_cached_token(user, refresh_if_expiring_soon=True)
                    if access_token_2 and access_token_2 != access_token:
                        token_bytes_2 = access_token_2.encode('utf-16-le')
                        token_struct_2 = struct.pack(f"<I{len(token_bytes_2)}s", len(token_bytes_2), token_bytes_2)
                        try:
                            conn = pyodbc.connect(conn_str, timeout=timeout, attrs_before={1256: token_struct_2})
                            if logger:
                                logger.info("Connected using refreshed MSAL token")
                            return conn
                        except Exception:
                            pass
                    raise RuntimeError(
                        "Cached token was rejected or expired. Use 'Test Connection' (or sign in from another tab) to sign in again, then retry. Do not cancel the sign-in when it appears."
                    ) from conn_err
        except ImportError:
            # MSAL library not available, fall back to interactive authentication
            if logger:
                logger.debug("MSAL not available, using ActiveDirectoryInteractive")
        except RuntimeError:
            raise
        except Exception as e:
            # Token fetch failed (e.g. no cache yet)
            if logger:
                logger.warning(f"Could not get cached MSAL token for {user}: {e}, falling back to interactive auth")
                logger.debug(f"Token fetch exception details: {type(e).__name__}: {e}", exc_info=True)
        
        # Fallback to interactive authentication only when we had no token (first-time sign-in)
        if logger:
            logger.info(f"Using ActiveDirectoryInteractive authentication for {user}")
        conn_str = base + f"UID={user};Authentication=ActiveDirectoryInteractive;"
        if logger:
            logger.debug(f"Connection string (masked): {conn_str.split('UID=')[0]}UID=***;Authentication=ActiveDirectoryInteractive;")
        return pyodbc.connect(conn_str, timeout=timeout)
    
    # For other auth types, use normal connection string
    conn_str = build_conn_str(server, db, user, driver, auth, password)
    return pyodbc.connect(conn_str, timeout=timeout)


def connect_with_token(
    server: str,
    db: str,
    driver: str,
    access_token: str,
    timeout: int = 30,
) -> pyodbc.Connection:
    """
    Connect to SQL Server using MSAL access token via SQL_COPT_SS_ACCESS_TOKEN.
    
    ODBC Driver 18 doesn't support AccessToken= in connection strings.
    Instead, we use the SQL_COPT_SS_ACCESS_TOKEN connection attribute (1256).
    
    Args:
        server: Server name/address
        db: Database name
        driver: ODBC driver name
        access_token: Azure AD access token (from MSAL)
        timeout: Connection timeout in seconds
    
    Returns:
        pyodbc.Connection object
    """
    # Build connection string WITHOUT UID, PWD, or Authentication when using token attribute
    conn_str = f"Driver={{{driver}}};Server={server};Database={db};Encrypt=yes;TrustServerCertificate=no;"
    
    # SQL_COPT_SS_ACCESS_TOKEN = 1256
    # Token must be encoded as UTF-16-LE bytes and packed in struct: <I (4-byte length) + bytes
    token_bytes = access_token.encode('utf-16-le')
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    
    # Connect using attrs_before to pass token
    return pyodbc.connect(conn_str, timeout=timeout, attrs_before={1256: token_struct})


def build_conn_str_and_token(
    server: str,
    db: str,
    user: str,
    driver: str,
    auth: str,
    password: Optional[str],
) -> Tuple[str, Optional[str], Optional[Dict[int, bytes]]]:
    """
    Build connection string and optionally prepare token for SQL_COPT_SS_ACCESS_TOKEN.
    
    Returns:
        Tuple of (connection_string, access_token, attrs_before_dict)
        - If token is available: (conn_str_without_auth, token_string, attrs_before_dict)
        - If no token: (conn_str_with_auth, None, None)
    """
    auth = (auth or "").strip().lower()
    base = f"Driver={{{driver}}};Server={server};Database={db};Encrypt=yes;TrustServerCertificate=no;"
    
    if auth == "entra_mfa":
        if not user:
            raise ValueError("For entra_mfa auth, user (UPN/email) is required.")
        
        # Try to get cached MSAL token first (persists across app restarts)
        try:
            # Try importing from root level (azure_token_cache.py)
            import sys
            from pathlib import Path
            root_dir = Path(__file__).parent.parent.parent
            root_str = str(root_dir)
            if root_str not in sys.path:
                sys.path.insert(0, root_str)
            
            from azure_token_cache import get_cached_token
            
            access_token = get_cached_token(user, refresh_if_expiring_soon=True)
            if access_token:
                # Return connection string WITHOUT auth params, token, and attrs_before dict
                # Caller should use connect_with_token() or pyodbc.connect(..., attrs_before={1256: token_struct})
                token_bytes = access_token.encode('utf-16-le')
                token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
                return base, access_token, {1256: token_struct}
        except ImportError:
            # MSAL library not available, fall back to interactive authentication
            pass
        except Exception as e:
            # Token fetch failed
            logger = logging.getLogger(__name__)
            logger.debug(f"Could not get cached MSAL token for {user}: {e}, falling back to interactive auth")
        
        # Fallback to interactive authentication
        return base + f"UID={user};Authentication=ActiveDirectoryInteractive;", None, None
    
    # For other auth types, return normal connection string
    if auth == "entra_password":
        if not user or not password:
            raise ValueError("For entra_password auth, user and password are required.")
        return base + f"UID={user};PWD={password};Authentication=ActiveDirectoryPassword;", None, None
    
    if auth == "sql":
        if not user or not password:
            raise ValueError("For sql auth, user and password are required (or set env variable).")
        return base + f"UID={user};PWD={password};", None, None
    
    if auth == "windows":
        return base + "Trusted_Connection=yes;", None, None
    
    raise ValueError(f"Unknown auth type '{auth}'. Use: entra_mfa | entra_password | sql | windows")


def ensure_db2_jdbc_driver(logger: Optional[logging.Logger] = None) -> Optional[str]:
    """
    Ensure DB2 JDBC driver is available, downloading if necessary.
    
    Returns:
        Path to the JDBC jar file, or None if not available
    """
    from pathlib import Path
    
    # Look for JDBC driver in common locations
    jdbc_jars = ["db2jcc4.jar", "db2jcc.jar", "jcc.jar"]
    search_paths = [
        Path(__file__).parent.parent.parent / "jdbc_drivers",  # Project jdbc_drivers folder
        Path.cwd() / "jdbc_drivers",
        Path.home() / ".db2" / "java",
        Path(r"C:\Program Files\IBM\SQLLIB\java"),
        Path(r"C:\IBM\SQLLIB\java"),
    ]
    
    # Check existing paths
    for search_path in search_paths:
        for jar in jdbc_jars:
            candidate = search_path / jar
            if candidate.exists():
                if logger:
                    logger.debug(f"Found JDBC driver: {candidate}")
                return str(candidate)
    
    # Not found - try to download
    jdbc_dir = search_paths[0]  # Use project jdbc_drivers folder
    jdbc_dir.mkdir(parents=True, exist_ok=True)
    jar_path = jdbc_dir / "db2jcc4.jar"
    
    if logger:
        logger.info(f"DB2 JDBC driver not found. Downloading to {jar_path}...")
    else:
        print(f"DB2 JDBC driver not found. Downloading to {jar_path}...")
    
    # Download from Maven Central
    url = "https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar"
    
    try:
        import ssl
        import urllib.request
        
        # Create context that doesn't verify SSL (some corporate networks have issues)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        if logger:
            logger.debug(f"Downloading from {url}...")
        
        with urllib.request.urlopen(url, context=ssl_context, timeout=60) as response:
            data = response.read()
            with open(jar_path, 'wb') as f:
                f.write(data)
        
        if jar_path.exists():
            if logger:
                logger.info(f"Successfully downloaded DB2 JDBC driver: {jar_path}")
            else:
                print(f"Successfully downloaded DB2 JDBC driver: {jar_path}")
            return str(jar_path)
    except Exception as e:
        if logger:
            logger.error(f"Failed to download DB2 JDBC driver: {e}")
        else:
            print(f"Failed to download DB2 JDBC driver: {e}")
    
    return None


def connect_to_db2_jdbc(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    timeout: int = 30,
    logger: Optional[logging.Logger] = None,
):
    """
    Connect to DB2 database using JDBC (JayDeBeApi).
    
    This is an alternative to ODBC that works better when ODBC driver is not available.
    Requires: jaydebeapi, jpype1 packages and Java JRE installed.
    Auto-downloads the JDBC driver if not found.
    
    Args:
        host: DB2 server hostname
        port: DB2 server port (typically 50000)
        database: Database name
        user: Username
        password: Password
        timeout: Connection timeout in seconds
        logger: Optional logger for debug messages
    
    Returns:
        JayDeBeApi connection object (DB-API 2.0 compatible)
    
    Raises:
        ImportError: If jaydebeapi or jpype1 not installed
        RuntimeError: If JDBC driver not found or connection fails
    """
    try:
        import jaydebeapi
        import jpype
    except ImportError as e:
        error_msg = f"JDBC packages not installed: {e}. Run: pip install jaydebeapi jpype1"
        if logger:
            logger.error(error_msg)
        raise ImportError(error_msg)
    
    # Ensure JDBC driver is available (auto-download if needed)
    jar_path = ensure_db2_jdbc_driver(logger)
    
    if not jar_path:
        error_msg = "DB2 JDBC driver not found and could not be downloaded."
        if logger:
            logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    if logger:
        logger.debug(f"Using JDBC driver: {jar_path}")
    
    # JDBC connection URL
    jdbc_url = f"jdbc:db2://{host}:{port}/{database}"
    
    if logger:
        logger.debug(f"JDBC URL: {jdbc_url}")
        logger.debug(f"Connecting as user: {user}")
    
    try:
        # Start JVM if not already started
        if not jpype.isJVMStarted():
            if logger:
                logger.debug("Starting JVM...")
            jpype.startJVM(classpath=[jar_path])
        
        # Connect
        if logger:
            logger.debug("Attempting JDBC connection...")
        conn = jaydebeapi.connect(
            "com.ibm.db2.jcc.DB2Driver",
            jdbc_url,
            [user, password],
            jar_path
        )
        
        if logger:
            logger.info(f"Successfully connected to DB2: {host}:{port}/{database}")
        
        return conn
        
    except Exception as e:
        error_details = str(e)
        if logger:
            logger.error(f"DB2 JDBC connection failed: {error_details}")
        raise RuntimeError(f"DB2 JDBC connection failed: {error_details}")


def check_db2_driver() -> Tuple[bool, Optional[str]]:
    """
    Check if a DB2 ODBC driver is available.
    
    Returns:
        Tuple of (is_available, driver_name or None)
    """
    try:
        drivers = pyodbc.drivers()
    except Exception:
        return False, None
    
    # Common DB2 driver names
    preferred = [
        "IBM DB2 ODBC DRIVER",
        "IBM DB2 ODBC DRIVER - DB2COPY1",
        "IBM DB2 ODBC DRIVER - DB2COPY2",
        "DB2",
    ]
    
    for name in preferred:
        if name in drivers:
            return True, name
    
    # Check for any driver containing "DB2"
    for d in drivers:
        if "DB2" in d.upper():
            return True, d
    
    return False, None


def build_db2_conn_str(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    driver: str,
) -> str:
    """
    Build ODBC connection string for DB2.
    
    Args:
        host: DB2 server hostname
        port: DB2 server port (typically 50000)
        database: Database name (can be empty)
        user: Username
        password: Password
        driver: ODBC driver name
    
    Returns:
        Connection string
    """
    # DB2 connection string format
    # Driver={IBM DB2 ODBC DRIVER};Database=database;Hostname=host;Port=port;UID=user;PWD=password;
    # Note: Some DB2 drivers may use different parameter names
    if database:
        return f"Driver={{{driver}}};Database={database};Hostname={host};Port={port};UID={user};PWD={password};"
    else:
        # Without database name
        return f"Driver={{{driver}}};Hostname={host};Port={port};UID={user};PWD={password};"


def connect_to_db2(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    driver: Optional[str] = None,
    timeout: int = 30,
    logger: Optional[logging.Logger] = None,
) -> pyodbc.Connection:
    """
    Connect to DB2 database.
    
    Args:
        host: DB2 server hostname
        port: DB2 server port (typically 50000)
        database: Database name (can be empty for some DB2 setups)
        user: Username
        password: Password
        driver: ODBC driver name (auto-detected if not provided)
        timeout: Connection timeout in seconds
        logger: Optional logger for debug messages
    
    Returns:
        pyodbc.Connection object
    
    Raises:
        RuntimeError: If no DB2 driver is found
        pyodbc.Error: If connection fails
    """
    # Validate inputs
    if not host or not host.strip():
        raise ValueError(f"Host cannot be empty. Received: '{host}'")
    if not port or port <= 0:
        raise ValueError(f"Port must be a positive integer. Received: {port}")
    if not user or not user.strip():
        raise ValueError(f"Username cannot be empty. Received: '{user}'")
    if not password:
        raise ValueError("Password cannot be empty")
    
    host = host.strip()
    database = database.strip() if database else ""
    user = user.strip()
    
    # Auto-detect driver if not provided
    if not driver:
        is_available, detected_driver = check_db2_driver()
        if not is_available:
            error_msg = (
                "No DB2 ODBC driver found.\n\n"
                "Please install IBM DB2 ODBC Driver:\n"
                "https://www.ibm.com/products/db2/drivers"
            )
            if logger:
                logger.error(error_msg)
            raise RuntimeError(error_msg)
        driver = detected_driver
        if logger:
            logger.info(f"Using DB2 ODBC driver: {driver}")
    
    # Build connection string
    # Note: Some DB2 setups allow empty database name
    if database:
        conn_str = build_db2_conn_str(host, port, database, user, password, driver)
    else:
        # Try without database name (some DB2 setups allow this)
        conn_str = f"Driver={{{driver}}};Hostname={host};Port={port};UID={user};PWD={password};"
    
    if logger:
        logger.debug(f"Connecting to DB2: {host}:{port}/{database if database else '(no database)'} as {user}")
        logger.debug(f"Connection string (masked): Driver={{{driver}}};Hostname={host};Port={port};UID=***;PWD=***;")
        if database:
            logger.debug(f"Database: {database}")
    
    # Connect to DB2
    try:
        conn = pyodbc.connect(conn_str, timeout=timeout)
        if logger:
            logger.info(f"Successfully connected to DB2: {host}:{port} as {user}")
        return conn
    except pyodbc.Error as e:
        error_details = str(e)
        if logger:
            logger.error(f"Failed to connect to DB2: {error_details}")
            logger.error(f"Connection string used: Driver={{{driver}}};Hostname={host};Port={port};UID={user};PWD=***;")
        # Re-raise with more context
        raise pyodbc.Error(f"DB2 connection failed: {error_details}")
    except Exception as e:
        error_details = str(e)
        if logger:
            logger.error(f"Unexpected error connecting to DB2: {error_details}")
        raise RuntimeError(f"DB2 connection failed: {error_details}")

