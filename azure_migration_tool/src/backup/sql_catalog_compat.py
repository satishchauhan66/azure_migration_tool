# Author: S@tish Chauhan

"""SQL Server catalog predicates safe across versions/editions (no optional DMV columns)."""

# sys.sql_modules.is_encrypted exists only on newer builds; OBJECTPROPERTY works broadly.
ENCRYPTED_MODULE_WHERE = (
    "OBJECTPROPERTY(object_id, 'IsEncrypted') = 1 "
    "AND object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped = 0)"
)

ENCRYPTED_MODULE_INVENTORY_SQL = """
SELECT s.name AS schema_name, o.name AS object_name, o.type_desc
FROM sys.sql_modules m
JOIN sys.objects o ON o.object_id = m.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE OBJECTPROPERTY(m.object_id, 'IsEncrypted') = 1 AND o.is_ms_shipped = 0
ORDER BY o.type_desc, s.name, o.name;
"""
