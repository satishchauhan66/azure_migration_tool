# Author: Sa-tish Chauhan

"""Azure SQL compatibility utilities for filtering unsupported features."""

import re
from typing import Any, Dict, List, Optional, Tuple

# Human-readable reasons for objects/batches we intentionally do not restore on Azure.
EXPECTED_SKIP_REASONS = {
    "credential_secret_placeholder": (
        "Credential SECRET uses a placeholder — set the real secret on the target before use"
    ),
    "encrypted_module_inventory": (
        "Encrypted module definitions are inventory-only (password/key not exportable)"
    ),
    "clr_assembly_azure": "CLR assemblies and assembly-bound modules are not supported on Azure SQL",
    "clr_framework_policy_azure": (
        ".NET Framework CLR assemblies blocked by Azure SQL strict security "
        "(policy / untested framework / missing dependency chain on MI)"
    ),
    "server_login_password": (
        "Server login PASSWORD is a placeholder — create or alter logins in master separately"
    ),
    "syslogins_master_azure": (
        "Server-level syslogins / USE master / cross-database batches are not supported on Azure SQL"
    ),
    "azure_unsupported_feature": "Feature not supported on Azure SQL (expected platform gap)",
    "windows_principal_azure": (
        "Windows logins/users and principal-named schemas are not portable to Azure SQL MI "
        "(map Entra ID groups/users on the target)"
    ),
    "object_already_exists": "Object already exists (idempotent re-run)",
}

# T-SQL schema identifiers (excludes Windows-style names like USPG\\user used as default schemas).
_VALID_TSQL_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# File types treated as inventory/reference during restore (not executed as DDL).
EXPECTED_SKIP_INVENTORY_FILE_TYPES = frozenset(
    {
        "ENCRYPTED_MODULES",
        "SERVER_LOGINS",
    }
)

# Runtime error / batch text markers mapped to EXPECTED_SKIP_REASONS keys.
_EXPECTED_SKIP_ERROR_MARKERS: Tuple[Tuple[str, str], ...] = (
    ("REPLACE_SECRET", "credential_secret_placeholder"),
    ("***REPLACE_SECRET***", "credential_secret_placeholder"),
    ("WITH PASSWORD = N'***'", "server_login_password"),
    ("PASSWORD = N'***'", "server_login_password"),
    ("WITH ENCRYPTION", "encrypted_module_inventory"),
    ("ENCRYPTED MODULE", "encrypted_module_inventory"),
    ("CREATE ASSEMBLY", "clr_assembly_azure"),
    ("EXTERNAL NAME", "clr_assembly_azure"),
    ("6586", "clr_framework_policy_azure"),
    ("6503", "clr_framework_policy_azure"),
    ("10308", "clr_framework_policy_azure"),
    ("COULD NOT BE INSTALLED BECAUSE EXISTING POLICY", "clr_framework_policy_azure"),
    ("NOT FULLY TESTED IN THE SQL SERVER HOSTED ENVIRONMENT", "clr_framework_policy_azure"),
    ("WAS NOT FOUND IN THE SQL CATALOG", "clr_framework_policy_azure"),
    ("KEEP IT FROM BEING USED", "clr_framework_policy_azure"),
    ("syslogins", "syslogins_master_azure"),
    ("USE MASTER", "syslogins_master_azure"),
    ("ALTER DATABASE STATEMENT IS NOT SUPPORTED", "azure_unsupported_feature"),
    ("NOT SUPPORTED IN THIS VERSION", "azure_unsupported_feature"),
    ("40515", "syslogins_master_azure"),
    ("5008", "azure_unsupported_feature"),
    ("FROM WINDOWS", "windows_principal_azure"),
    ("CREATE LOGIN", "windows_principal_azure"),
    ("41906", "windows_principal_azure"),
    ("15007", "windows_principal_azure"),
    ("15151", "windows_principal_azure"),
    ("LOGIN DOES NOT EXIST", "windows_principal_azure"),
    ("CANNOT FIND THE USER", "windows_principal_azure"),
)

# EngineEdition: 5 = Azure SQL Database, 8 = Azure SQL Managed Instance
AZURE_SQL_DATABASE_EDITION = 5
AZURE_MANAGED_INSTANCE_EDITION = 8
_AZURE_ENGINE_EDITIONS = frozenset({AZURE_SQL_DATABASE_EDITION, AZURE_MANAGED_INSTANCE_EDITION})

# Restored on MI (EngineEdition 8); still filtered on Azure SQL Database.
MI_CLR_SUPPORTED_FILE_TYPES = frozenset(
    {
        "ASSEMBLIES",
        "CLR_TYPES",
        "CLR_PROCEDURES",
    }
)

# File types that must be filtered on Azure targets even when mirror_source=True
AZURE_ALWAYS_FILTER_FILE_TYPES = frozenset(
    {
        "ASSEMBLIES",
        "CLR_TYPES",
        "CLR_PROCEDURES",
        "CRYPTOGRAPHIC_OBJECTS",
        "DATABASE_OPTIONS",
        "SERVICE_BROKER",
        "REPLICATION",
        "LEGACY_RULES_DEFAULTS",
    }
)

# ALTER DATABASE options not supported on Azure SQL Database / Managed Instance
_AZURE_UNSUPPORTED_ALTER_DATABASE = re.compile(
    r"\bALTER\s+DATABASE\s+(?:CURRENT|\[[^\]]+\]|\w+)\s+SET\s+"
    r"(?:COMPATIBILITY_LEVEL|RECOVERY|PAGE_VERIFY|AUTO_CLOSE|AUTO_SHRINK|"
    r"AUTO_CREATE_STATISTICS|AUTO_UPDATE_STATISTICS|BULK_LOGGED|"
    r"CHANGE_TRACKING|DB_CHAINING|HONOR_BROKER_PRIORITY|MULTI_USER|"
    r"RESTRICTED_USER|SINGLE_USER|TRUSTWORTHY)\b",
    re.IGNORECASE,
)

# Programmable type_desc values that cannot exist on Azure SQL (CLR / assembly-bound).
AZURE_EXPECTED_SKIP_PROGRAMMABLE_PREFIXES = frozenset(
    {
        "CLR_STORED_PROCEDURE",
        "CLR_SCALAR_FUNCTION",
        "CLR_TABLE_VALUED_FUNCTION",
        "CLR_AGGREGATE_FUNCTION",
    }
)

# .NET Framework assemblies commonly referenced by ASPstate but blocked on Azure SQL MI CLR policy.
_MI_BLOCKED_FRAMEWORK_ASSEMBLY_MARKERS: Tuple[str, ...] = (
    "accessibility",
    "system.messaging",
    "system.configuration.install",
    "system.directoryservices",
    "system.drawing",
    "system.runtime.serialization.formatters.soap",
    "system.windows.forms",
    # Redgate exports System.Messaging as assembly [Messaging] on ASPstate
    "create assembly [messaging]",
)

# Module names that cannot be made identical on Azure MI (syslogins / CLR).
AZURE_EXPECTED_SKIP_MODULE_NAMES = frozenset(
    {
        "addwindowsuserandlogin",
    }
)

_AZURE_SERVER_HOST_MARKERS = (
    ".database.windows.net",
    ".database.usgovcloudapi.net",
    ".database.chinacloudapi.cn",
    ".database.cloudapi.de",
    ".privatelink.database.windows.net",
)


# Azure SQL unsupported features patterns
AZURE_UNSUPPORTED_PATTERNS = [
    # System tables/views not supported in Azure SQL
    (r"master\.(?:dbo\.)?syslogins", "syslogins (not supported in Azure SQL)"),
    (r"sys\.syslogins", "syslogins (not supported in Azure SQL)"),
    (r"master\.(?:dbo\.)?sysprocesses", "sysprocesses (not supported in Azure SQL)"),
    (r"sys\.sysprocesses", "sysprocesses (not supported in Azure SQL)"),
    (r"xp_cmdshell", "xp_cmdshell (not supported in Azure SQL)"),
    (r"xp_regread", "xp_regread (not supported in Azure SQL)"),
    (r"xp_regwrite", "xp_regwrite (not supported in Azure SQL)"),
    (r"sp_configure", "sp_configure (limited in Azure SQL)"),
    (r"DBCC\s+CHECKDB", "DBCC CHECKDB (not supported in Azure SQL)"),
    (r"DBCC\s+DBREINDEX", "DBCC DBREINDEX (not supported in Azure SQL)"),
    (r"DBCC\s+INDEXDEFRAG", "DBCC INDEXDEFRAG (not supported in Azure SQL)"),
    (r"BACKUP\s+DATABASE", "BACKUP DATABASE (not supported in Azure SQL)"),
    (r"RESTORE\s+DATABASE", "RESTORE DATABASE (not supported in Azure SQL)"),
    (r"USE\s+master", "USE master (cross-database queries limited in Azure SQL)"),
    (r"USE\s+msdb", "USE msdb (cross-database queries limited in Azure SQL)"),
    (r"\.\.master\.", "Cross-database reference to master (not supported)"),
    (r"\.\.msdb\.", "Cross-database reference to msdb (not supported)"),
    (r"OPENROWSET\s*\(", "OPENROWSET (requires special configuration in Azure SQL)"),
    (r"OPENDATASOURCE\s*\(", "OPENDATASOURCE (requires special configuration in Azure SQL)"),
    (r"BULK\s+INSERT", "BULK INSERT (limited in Azure SQL)"),
    (r"CREATE\s+ASSEMBLY", "CREATE ASSEMBLY (CLR not supported in Azure SQL)"),
    (r"PERMISSION_SET\s*=\s*UNSAFE_ACCESS", "UNSAFE_ACCESS is invalid (use UNSAFE)"),
    (r"PERMISSION_SET\s*=\s*SAFE_ACCESS", "SAFE_ACCESS is invalid (use SAFE)"),
    (r"CREATE\s+RULE\b", "CREATE RULE (legacy, not supported in Azure SQL)"),
    (r"CREATE\s+DEFAULT\b", "CREATE DEFAULT (legacy, not supported in Azure SQL)"),
    (r"sp_bindrule\b", "sp_bindrule (legacy, not supported in Azure SQL)"),
    (r"sp_binddefault\b", "sp_binddefault (legacy, not supported in Azure SQL)"),
    (r"EXEC\s+master\.", "EXEC master (cross-database execution not supported)"),
    (r"EXEC\s+msdb\.", "EXEC msdb (cross-database execution not supported)"),
]


def is_valid_tsql_schema_name(name: str) -> bool:
    """False for Windows principal default schemas (USPG\\user) and other non-identifier names."""
    if not name or "\\" in name or "/" in name:
        return False
    return bool(_VALID_TSQL_SCHEMA_NAME.match(name))


def is_windows_principal_name(name: str) -> bool:
    """Heuristic: domain\\user or machine\\user style SQL login/principal name."""
    if not name:
        return False
    return "\\" in name or name.upper().startswith("NT AUTHORITY\\")


def is_exportable_schema_authorization(schema_name: str, owner_name: Optional[str] = None) -> bool:
    """Skip ALTER AUTHORIZATION for principal-named schemas mistaken as real schemas."""
    if not is_valid_tsql_schema_name(schema_name):
        return False
    if owner_name and is_windows_principal_name(owner_name):
        return False
    return True


def is_windows_principal_batch(batch: str) -> bool:
    """Batches that require on-prem Windows logins or syslogins (not valid on Azure SQL MI)."""
    if not batch or not batch.strip():
        return False
    upper = batch.upper()
    if "FROM WINDOWS" in upper or re.search(r"\bCREATE\s+LOGIN\b", batch, re.IGNORECASE):
        return True
    if "FOR LOGIN" in upper and re.search(r"\[[^\]]*\\[^\]]*\]", batch):
        return True
    if re.search(r"\bALTER\s+AUTHORIZATION\s+ON\s+SCHEMA\b", batch, re.IGNORECASE):
        m = re.search(
            r"ALTER\s+AUTHORIZATION\s+ON\s+SCHEMA::\s*\[([^\]]+)\]",
            batch,
            re.IGNORECASE,
        )
        if m and not is_valid_tsql_schema_name(m.group(1)):
            return True
    return False


def should_skip_windows_principal_error(error_msg: str) -> bool:
    """Missing Windows login / user on Azure SQL MI (expected when source used AD logins)."""
    error_upper = (error_msg or "").upper()
    patterns = (
        "41906",
        "15007",
        "15151",
        "FROM WINDOWS",
        "LOGIN DOES NOT EXIST",
        "CANNOT FIND THE USER",
        "WINDOWS NT",
        "NOT A VALID LOGIN",
        "PRINCIPAL 'NT AUTHORITY",
    )
    return any(p in error_upper for p in patterns)


def is_azure_sql_server(server: str) -> bool:
    """Heuristic: host name looks like Azure SQL Database or Managed Instance."""
    host = (server or "").split(",")[0].strip().lower()
    if not host:
        return False
    if any(marker in host for marker in _AZURE_SERVER_HOST_MARKERS):
        return True
    if "-mi-" in host or host.endswith("-mi") or ".mi." in host:
        return True
    return False


def detect_azure_engine_edition(cur, server: str = "") -> Optional[int]:
    """Return SERVERPROPERTY('EngineEdition') when connected to Azure SQL, else None."""
    try:
        cur.execute("SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT);")
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    if is_azure_sql_server(server):
        return AZURE_MANAGED_INSTANCE_EDITION if "-mi-" in (server or "").lower() else None
    return None


def detect_azure_sql_target(cur, server: str = "") -> bool:
    """Detect Azure SQL (DB or MI) from SERVERPROPERTY when connected."""
    edition = detect_azure_engine_edition(cur, server)
    if edition in _AZURE_ENGINE_EDITIONS:
        return True
    return is_azure_sql_server(server)


def is_azure_managed_instance_edition(engine_edition: Optional[int]) -> bool:
    return engine_edition == AZURE_MANAGED_INSTANCE_EDITION


def should_apply_azure_batch_filter(
    mirror_source: bool,
    azure_target: bool,
    file_type: str,
    engine_edition: Optional[int] = None,
) -> bool:
    """Whether to run filter_azure_incompatible_batches for this file."""
    ft = (file_type or "").upper()
    if azure_target and ft in AZURE_ALWAYS_FILTER_FILE_TYPES:
        if is_azure_managed_instance_edition(engine_edition) and ft in MI_CLR_SUPPORTED_FILE_TYPES:
            return not mirror_source
        return True
    return not mirror_source


def is_azure_compatible(sql_text: str, engine_edition: Optional[int] = None) -> Tuple[bool, List[str]]:
    """
    Check if SQL text is compatible with Azure SQL.
    
    Args:
        sql_text: SQL text to check
    
    Returns:
        Tuple of (is_compatible, list_of_issues)
    """
    issues = []
    sql_upper = sql_text.upper()
    
    mi_clr_ok = is_azure_managed_instance_edition(engine_edition)
    for pattern, description in AZURE_UNSUPPORTED_PATTERNS:
        if mi_clr_ok and "CREATE ASSEMBLY" in description:
            continue
        if re.search(pattern, sql_text, re.IGNORECASE):
            issues.append(description)

    return len(issues) == 0, issues


def is_azure_supported_database_option_batch(batch: str) -> bool:
    """Return False when batch contains ALTER DATABASE options Azure SQL cannot apply."""
    if not batch or not batch.strip():
        return False
    if _AZURE_UNSUPPORTED_ALTER_DATABASE.search(batch):
        return False
    return True


def is_clr_module_batch(batch: str) -> bool:
    """CLR/assembly-bound module (EXTERNAL NAME / assembly reference)."""
    upper = batch.upper()
    return "EXTERNAL NAME" in upper or "CREATE ASSEMBLY" in upper


def is_mi_blocked_framework_assembly_name(assembly_name: str) -> bool:
    """True for .NET Framework assemblies blocked by Azure SQL MI CLR policy."""
    if not assembly_name:
        return False
    lower = assembly_name.strip().lower()
    blocked_names = {
        "accessibility",
        "system.directoryservices",
        "system.drawing",
        "system.runtime.serialization.formatters.soap",
        "system.windows.forms",
        "system.configuration.install",
        "messaging",  # System.Messaging alias in ASPstate backups
    }
    if lower in blocked_names:
        return True
    return any(marker in f"[{lower}]" for marker in _MI_BLOCKED_FRAMEWORK_ASSEMBLY_MARKERS if "\\" not in marker)


def is_mi_blocked_framework_assembly_batch(batch: str) -> bool:
    """ASPstate-style .NET Framework assemblies that MI CLR strict security blocks."""
    if not batch or "CREATE ASSEMBLY" not in batch.upper():
        return False
    lower = batch.lower()
    if any(marker in lower for marker in _MI_BLOCKED_FRAMEWORK_ASSEMBLY_MARKERS):
        return True
    m = re.search(r"create\s+assembly\s+\[([^\]]+)\]", batch, re.IGNORECASE)
    return bool(m and is_mi_blocked_framework_assembly_name(m.group(1)))


def is_azure_nonportable_module(entry: Dict[str, Any]) -> Optional[str]:
    """
    Return EXPECTED_SKIP_REASONS key when a module cannot match on Azure MI.

    entry: catalog procedure/view dict with name, definition, type_desc.
    """
    name = (entry.get("name") or "").strip().lower()
    defn = (entry.get("definition") or "").upper()
    type_desc = (entry.get("type_desc") or "").upper()

    if name in AZURE_EXPECTED_SKIP_MODULE_NAMES:
        if name == "addwindowsuserandlogin":
            return "syslogins_master_azure"
        return "clr_assembly_azure"

    if type_desc.startswith("CLR_") or "EXTERNAL NAME" in defn:
        m = re.search(r"EXTERNAL\s+NAME\s+\[([^\]]+)\]", defn, re.IGNORECASE)
        if m and not is_mi_blocked_framework_assembly_name(m.group(1)):
            return None
        return "clr_assembly_azure"

    if "MASTER.DBO.SYSLOGINS" in defn or "SYS.SYSLOGINS" in defn:
        return "syslogins_master_azure"

    return None


def expected_azure_gap_note(object_key: str, gap_kind: str, dest_is_azure: bool) -> Optional[str]:
    """
    Return a short note when a source/dest schema gap is expected on Azure SQL targets.

    object_key examples:
      - SQL_STORED_PROCEDURE.dbo.MyClrProc (programmable — CLR prefix)
      - dbo.sysdiagrams.UK_principal_name (constraint)
    """
    if not dest_is_azure:
        return None

    key = (object_key or "").strip()
    if gap_kind == "programmable":
        prefix = key.split(".", 1)[0] if key else ""
        if prefix in AZURE_EXPECTED_SKIP_PROGRAMMABLE_PREFIXES:
            return "CLR/assembly modules are not supported on Azure SQL (expected difference)"
        return None

    if gap_kind == "constraint":
        parts = key.split(".")
        if len(parts) == 3 and parts[1].lower() == "sysdiagrams" and parts[2].upper() == "UK_PRINCIPAL_NAME":
            return (
                "sysdiagrams unique constraint is optional on Azure; "
                "re-backup after tool update to export via database_diagrams.sql"
            )
        return None

    return None


def filter_azure_incompatible_batches(
    batches: List[str],
    logger,
    file_type: Optional[str] = None,
    engine_edition: Optional[int] = None,
) -> List[Tuple[int, str, List[str]]]:
    """
    Filter out batches that are incompatible with Azure SQL.
    
    Args:
        batches: List of SQL batches
        logger: Logger instance
    
    Returns:
        List of tuples: (original_index, batch_text, issues_found)
        Batches with issues are excluded from the list
    """
    compatible_batches = []

    if file_type in ("PROCEDURES", "PROCEDURE"):
        from .sql import expand_module_session_set_batches, split_procedure_batches

        expanded: List[str] = []
        for batch in batches:
            batch = (batch or "").strip()
            if not batch:
                continue
            expanded.extend(
                expand_module_session_set_batches(split_procedure_batches(batch))
            )
        batches = expanded if expanded else batches
    elif file_type in ("FUNCTIONS", "CLR_PROCEDURES"):
        from .sql import split_batches_on_create_module

        expanded = []
        for batch in batches:
            batch = (batch or "").strip()
            if not batch:
                continue
            expanded.extend(split_batches_on_create_module([batch]))
        batches = expanded
    
    for i, batch in enumerate(batches, start=1):
        batch = batch.strip()
        if not batch:
            continue

        if (
            file_type in ("ASSEMBLIES", "SCHEMA_REPAIR")
            and is_azure_managed_instance_edition(engine_edition)
            and is_mi_blocked_framework_assembly_batch(batch)
        ):
            logger.info(
                "Skipping batch %d (blocked .NET Framework CLR assembly on Azure SQL MI)",
                i,
            )
            continue

        if file_type == "DATABASE_OPTIONS" and not is_azure_supported_database_option_batch(batch):
            logger.info(
                "Skipping batch %d (ALTER DATABASE option not supported on Azure SQL)",
                i,
            )
            continue

        if (
            file_type in ("PROCEDURES", "PROCEDURE", "CLR_PROCEDURES")
            and is_clr_module_batch(batch)
            and not is_azure_managed_instance_edition(engine_edition)
        ):
            logger.info(
                "Skipping batch %d (CLR module — not supported on Azure SQL Database)",
                i,
            )
            continue

        if file_type in (
            "DATABASE_PRINCIPALS",
            "SCHEMA_AUTHORIZATION",
            "ROLE_MEMBERSHIPS",
            "PERMISSIONS",
            "COLUMN_PERMISSIONS",
            "SCHEMA_REPAIR",
        ) and is_windows_principal_batch(batch):
            logger.info(
                "Skipping batch %d (Windows principal / non-portable schema auth on Azure SQL)",
                i,
            )
            continue

        is_compat, issues = is_azure_compatible(batch, engine_edition=engine_edition)
        
        if not is_compat:
            logger.warning(
                "Skipping batch %d (Azure SQL incompatible): %s",
                i,
                "; ".join(issues)
            )
            continue
        
        compatible_batches.append((i, batch, issues))
    
    return compatible_batches


def should_skip_default_constraint_error(error_msg: str) -> bool:
    """Check if default constraint error should be ignored (already exists)"""
    error_upper = error_msg.upper()
    return (
        "already has a default" in error_upper or
        "1781" in error_msg  # SQL error code for "Column already has a DEFAULT bound"
    )


def should_skip_index_error(error_msg: str) -> bool:
    """Check if index error should be ignored (conflict with existing index)"""
    error_upper = error_msg.upper()
    return (
        "cannot create more than one clustered index" in error_upper or
        "already exists" in error_upper or
        "1902" in error_msg or  # Cannot create more than one clustered index
        "1913" in error_msg     # Index already exists
    )


def should_skip_azure_error(error_msg: str) -> bool:
    """Check if error is due to Azure SQL incompatibility and should be skipped"""
    error_upper = error_msg.upper()
    azure_error_patterns = [
        "not supported in this version",
        "alter database statement is not supported",
        "5008",  # ALTER DATABASE not supported on Azure SQL
        "5069",  # ALTER DATABASE statement failed
        "40515",  # Reference to database/server name not supported
        "41906",  # Windows login not supported (Azure SQL Database)
        "15007",  # Login does not exist
        "15151",  # User does not exist or no permission
        "syslogins",
        "sysprocesses",
        "xp_cmdshell",
        "from windows",
        "login does not exist",
        "cannot find the user",
    ]
    return any(pattern in error_upper for pattern in azure_error_patterns) or should_skip_windows_principal_error(
        error_msg
    )


def is_expected_skip_inventory_file(file_type: str) -> bool:
    """True when the whole SQL file is reference-only (encrypted modules, server logins)."""
    return (file_type or "").upper() in EXPECTED_SKIP_INVENTORY_FILE_TYPES


def classify_expected_skip(
    error_msg: str = "",
    file_type: str = "",
    skip_reason: Optional[str] = None,
    batch_text: str = "",
) -> Optional[str]:
    """
    Return an EXPECTED_SKIP_REASONS key when a skip is uncontrollable on Azure restore.

    Returns None for real failures (syntax errors, missing referenced objects, etc.).
    """
    ft = (file_type or "").upper()
    if ft in EXPECTED_SKIP_INVENTORY_FILE_TYPES:
        return "encrypted_module_inventory" if ft == "ENCRYPTED_MODULES" else "server_login_password"

    if ft == "ASSEMBLIES" and is_mi_blocked_framework_assembly_batch(batch_text):
        return "clr_framework_policy_azure"

    combined = f"{error_msg or ''} {skip_reason or ''} {batch_text or ''}".upper()
    for marker, reason_key in _EXPECTED_SKIP_ERROR_MARKERS:
        if marker.upper() in combined:
            return reason_key

    if skip_reason and "azure sql incompatible" in skip_reason.lower():
        return "azure_unsupported_feature"
    if skip_reason and "already exists" in skip_reason.lower():
        return "object_already_exists"

    if should_skip_azure_error(error_msg or ""):
        return "azure_unsupported_feature"

    return None


def should_skip_already_exists_error(error_msg: str) -> bool:
    """
    Check if error is due to object already existing and should be skipped.
    Handles: tables, views, procedures, functions, constraints, indexes, schemas.
    """
    error_upper = error_msg.upper()
    already_exists_patterns = [
        "already exists",
        "already has",
        "already been created",
        "duplicate key",
        "duplicate object",
        "name is already used",
        "2714",  # Object already exists
        "1750",  # Cannot create constraint
        "1779",  # Table already has a primary key
        "1781",  # Column already has a DEFAULT bound
        "1782",  # Column already has an IDENTITY property
        "1902",  # Cannot create more than one clustered index
        "1913",  # Index already exists
        "1505",  # CREATE UNIQUE INDEX terminated because a duplicate key was found
        "1507",  # CREATE UNIQUE INDEX terminated because duplicate keys were found
        "3728",  # Cannot drop the constraint because it does not exist
        "3729",  # Cannot drop the object because it does not exist or you do not have permission
    ]
    return any(pattern in error_upper for pattern in already_exists_patterns)
