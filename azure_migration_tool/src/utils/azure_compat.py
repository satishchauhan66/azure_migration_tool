# Author: Sa-tish Chauhan

"""Azure SQL compatibility utilities for filtering unsupported features."""

import re
from typing import List, Tuple


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
    (r"EXEC\s+master\.", "EXEC master (cross-database execution not supported)"),
    (r"EXEC\s+msdb\.", "EXEC msdb (cross-database execution not supported)"),
]


def is_azure_compatible(sql_text: str) -> Tuple[bool, List[str]]:
    """
    Check if SQL text is compatible with Azure SQL.
    
    Args:
        sql_text: SQL text to check
    
    Returns:
        Tuple of (is_compatible, list_of_issues)
    """
    issues = []
    sql_upper = sql_text.upper()
    
    for pattern, description in AZURE_UNSUPPORTED_PATTERNS:
        if re.search(pattern, sql_text, re.IGNORECASE):
            issues.append(description)
    
    return len(issues) == 0, issues


def filter_azure_incompatible_batches(batches: List[str], logger) -> List[Tuple[int, str, List[str]]]:
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
    
    for i, batch in enumerate(batches, start=1):
        batch = batch.strip()
        if not batch:
            continue
        
        is_compat, issues = is_azure_compatible(batch)
        
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
        "40515",  # Reference to database/server name not supported
        "syslogins",
        "sysprocesses",
        "xp_cmdshell",
    ]
    return any(pattern in error_upper for pattern in azure_error_patterns)


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
