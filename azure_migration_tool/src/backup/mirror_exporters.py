# Author: S@tish Chauhan

"""Additional schema exporters for mirror-style backup (SSMS object tree parity)."""

import re
from typing import List, Optional, Tuple

from ..utils.azure_compat import is_valid_tsql_schema_name
from ..utils.paths import qident
from .exporters import (
    object_definition,
    object_module_session_options,
    type_sql,
    wrap_create_or_alter,
)

# Schemas created by SQL Server (skip on export)
_SYSTEM_SCHEMAS = frozenset(
    {
        "sys",
        "INFORMATION_SCHEMA",
        "guest",
        "db_owner",
        "db_accessadmin",
        "db_securityadmin",
        "db_ddladmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_denydatareader",
        "db_denydatawriter",
    }
)


def _go_block(lines: List[str]) -> str:
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def _sql_literal(name: str) -> str:
    return (name or "").replace("'", "''")


def _is_exportable_schema(schema_name: str, owner_name: Optional[str], owner_type: Optional[str]) -> bool:
    if not schema_name or schema_name in _SYSTEM_SCHEMAS:
        return False
    if not is_valid_tsql_schema_name(schema_name):
        return False
    if schema_name.lower().startswith("db_"):
        return False
    if owner_type == "R":
        return False
    if owner_name and owner_name in _SYSTEM_SCHEMAS:
        return False
    return True


def export_schemas(cur) -> str:
    """Export user-defined schemas (CREATE SCHEMA)."""
    cur.execute(
        """
        SELECT s.name AS schema_name, dp.name AS owner_name, dp.type AS owner_type
        FROM sys.schemas s
        LEFT JOIN sys.database_principals dp ON dp.principal_id = s.principal_id
        WHERE s.schema_id > 4
          AND s.name NOT IN (N'dbo')
          AND s.name NOT LIKE N'db[_]%'
          AND NOT EXISTS (
              SELECT 1 FROM sys.database_principals r
              WHERE r.name = s.name AND r.type = N'R'
          )
        ORDER BY s.name;
        """
    )
    rows = cur.fetchall()
    out: List[str] = []
    for row in rows:
        name = row.schema_name
        owner = row.owner_name or "dbo"
        owner_type = getattr(row, "owner_type", None)
        if not _is_exportable_schema(name, owner, owner_type):
            continue
        if owner_type == "R" or owner in _SYSTEM_SCHEMAS:
            owner = "dbo"
        name_lit = _sql_literal(name)
        out.append(f"-- Schema [{name}]")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{name_lit}')")
        out.append("BEGIN")
        out.append(f"    CREATE SCHEMA {qident(name)} AUTHORIZATION {qident(owner)};")
        out.append("END")
        out.append("GO")
        out.append("")
    return _go_block(out)


def export_user_defined_types(cur, logger=None) -> Tuple[str, List[str]]:
    """Export alias types and table types (CREATE TYPE)."""
    warnings: List[str] = []
    out: List[str] = []

    # Alias types (FROM base_type)
    cur.execute(
        """
        SELECT
            s.name AS schema_name,
            t.name AS type_name,
            bt.name AS base_type,
            t.max_length,
            t.precision,
            t.scale,
            CASE WHEN t.is_nullable = 1 THEN 1 ELSE 0 END AS is_nullable
        FROM sys.types t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.types bt ON t.system_type_id = bt.user_type_id
        WHERE t.is_user_defined = 1
          AND t.is_table_type = 0
          AND t.is_assembly_type = 0
        ORDER BY s.name, t.name;
        """
    )
    for row in cur.fetchall():
        full = f"{qident(row.schema_name)}.{qident(row.type_name)}"
        base = type_sql(row.base_type, row.max_length, row.precision, row.scale)
        null_clause = " NULL" if row.is_nullable else " NOT NULL"
        out.append(f"-- Alias type {full}")
        out.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.types t JOIN sys.schemas s ON s.schema_id = t.schema_id "
            f"WHERE s.name = N'{row.schema_name}' AND t.name = N'{row.type_name}' AND t.is_user_defined = 1)"
        )
        out.append("BEGIN")
        out.append(f"    CREATE TYPE {full} FROM {base}{null_clause};")
        out.append("END")
        out.append("GO")
        out.append("")

    # Table types — OBJECT_DEFINITION when available
    cur.execute(
        """
        SELECT s.name AS schema_name, tt.name AS type_name, tt.type_table_object_id AS object_id
        FROM sys.table_types tt
        JOIN sys.schemas s ON s.schema_id = tt.schema_id
        ORDER BY s.name, tt.name;
        """
    )
    for row in cur.fetchall():
        defn = object_definition(cur, row.object_id)
        full = f"{row.schema_name}.{row.type_name}"
        if defn:
            out.append(f"-- Table type {full}")
            text = defn.strip()
            if not text.upper().startswith("CREATE TYPE"):
                out.append(f"-- {full} (TABLE TYPE)")
            out.append(text)
            out.append("GO")
            out.append("")
        else:
            msg = f"Table type {full}: definition not available (skipped)"
            warnings.append(msg)
            if logger:
                logger.warning(msg)

    if logger:
        logger.info("User-defined types export: %d alias + table type blocks", len([x for x in out if x.startswith("--")]))
    return _go_block(out), warnings


def export_ddl_triggers(cur, logger=None) -> Tuple[str, List[str]]:
    """Export database-level (DDL) triggers."""
    cur.execute(
        """
        SELECT t.object_id, s.name AS schema_name, t.name AS trigger_name, t.is_disabled
        FROM sys.triggers t
        JOIN sys.objects o ON o.object_id = t.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE t.parent_class = 0
          AND t.is_ms_shipped = 0
        ORDER BY s.name, t.name;
        """
    )
    rows = cur.fetchall()
    out: List[str] = []
    warnings: List[str] = []
    for row in rows:
        defn = object_definition(cur, row.object_id)
        if not defn:
            msg = f"DDL trigger {row.schema_name}.{row.trigger_name}: definition not available"
            warnings.append(msg)
            continue
        session_opts = object_module_session_options(cur, row.object_id)
        out.append(
            wrap_create_or_alter(
                row.schema_name,
                row.trigger_name,
                defn,
                "DDL TRIGGER",
                session_options=session_opts,
            )
        )
        if row.is_disabled:
            out.append(
                f"DISABLE TRIGGER {qident(row.trigger_name)} ON DATABASE;\nGO\n"
            )
    return _go_block(out), warnings


def export_external_resources(cur) -> str:
    """Export external data sources and file formats."""
    out: List[str] = []

    cur.execute(
        """
        SELECT
            eds.name,
            eds.location,
            eds.type_desc,
            eds.database_name,
            eds.resource_manager_location,
            eds.credential_id,
            c.name AS credential_name
        FROM sys.external_data_sources eds
        LEFT JOIN sys.database_credentials c ON c.credential_id = eds.credential_id
        ORDER BY eds.name;
        """
    )
    for row in cur.fetchall():
        name = row.name.replace("'", "''")
        loc = (row.location or "").replace("'", "''")
        cred = row.credential_name
        out.append(f"-- External data source [{row.name}]")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = N'{name}')")
        out.append("BEGIN")
        parts = [f"TYPE = {row.type_desc}"]
        if loc:
            parts.append(f"LOCATION = N'{loc}'")
        if row.database_name:
            parts.append(f"DATABASE_NAME = N'{(row.database_name or '').replace(chr(39), chr(39)+chr(39))}'")
        if row.resource_manager_location:
            parts.append(
                f"RESOURCE_MANAGER_LOCATION = N'{(row.resource_manager_location or '').replace(chr(39), chr(39)+chr(39))}'"
            )
        if cred:
            parts.append(f"CREDENTIAL = {qident(cred)}")
        out.append(f"    CREATE EXTERNAL DATA SOURCE {qident(row.name)} WITH ({', '.join(parts)});")
        out.append("END")
        out.append("GO")
        out.append("")

    cur.execute(
        """
        SELECT name, format_type, field_terminator, string_delimiter,
               date_format, use_type_default, data_compression
        FROM sys.external_file_formats
        ORDER BY name;
        """
    )
    for row in cur.fetchall():
        name = row.name.replace("'", "''")
        out.append(f"-- External file format [{row.name}]")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = N'{name}')")
        out.append("BEGIN")
        opts = [f"FORMAT_TYPE = {row.format_type}"]
        if row.field_terminator:
            opts.append(f"FIELD_TERMINATOR = N'{(row.field_terminator or '').replace(chr(39), chr(39)+chr(39))}'")
        if row.string_delimiter:
            opts.append(f"STRING_DELIMITER = N'{(row.string_delimiter or '').replace(chr(39), chr(39)+chr(39))}'")
        if row.date_format:
            opts.append(f"DATE_FORMAT = N'{(row.date_format or '').replace(chr(39), chr(39)+chr(39))}'")
        if row.use_type_default is not None:
            opts.append(f"USE_TYPE_DEFAULT = {'TRUE' if row.use_type_default else 'FALSE'}")
        if row.data_compression:
            opts.append(f"DATA_COMPRESSION = N'{(row.data_compression or '').replace(chr(39), chr(39)+chr(39))}'")
        out.append(f"    CREATE EXTERNAL FILE FORMAT {qident(row.name)} WITH ({', '.join(opts)});")
        out.append("END")
        out.append("GO")
        out.append("")

    return _go_block(out)


def export_partitioning(cur, logger=None) -> str:
    """Export partition functions and partition schemes."""
    out: List[str] = []

    cur.execute(
        """
        SELECT pf.name,
               t.name AS type_name,
               pp.max_length,
               pp.precision,
               pp.scale,
               pf.boundary_value_on_right,
               pf.function_id
        FROM sys.partition_functions pf
        JOIN sys.partition_parameters pp ON pp.function_id = pf.function_id
        JOIN sys.types t
            ON pp.system_type_id = t.system_type_id
           AND t.user_type_id = t.system_type_id
        ORDER BY pf.name;
        """
    )
    pfs = cur.fetchall()
    for pf in pfs:
        type_decl = type_sql(pf.type_name, pf.max_length, pf.precision, pf.scale)
        cur.execute(
            """
            SELECT CONVERT(NVARCHAR(4000), prv.value) AS boundary_value
            FROM sys.partition_range_values prv
            WHERE prv.function_id = ?
            ORDER BY prv.boundary_id;
            """,
            pf.function_id,
        )
        boundaries = [r.boundary_value for r in cur.fetchall() if r.boundary_value is not None]
        side = "RIGHT" if pf.boundary_value_on_right else "LEFT"
        vals = ", ".join(boundaries) if boundaries else ""
        out.append(f"-- Partition function [{pf.name}]")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'{pf.name}')")
        out.append("BEGIN")
        if vals:
            out.append(
                f"    CREATE PARTITION FUNCTION {qident(pf.name)} ({type_decl}) "
                f"AS RANGE {side} FOR VALUES ({vals});"
            )
        else:
            out.append(
                f"    CREATE PARTITION FUNCTION {qident(pf.name)} ({type_decl}) AS RANGE {side} FOR VALUES ();"
            )
        out.append("END")
        out.append("GO")
        out.append("")

    cur.execute(
        """
        SELECT ps.name AS scheme_name, pf.name AS function_name, ps.data_space_id
        FROM sys.partition_schemes ps
        JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
        ORDER BY ps.name;
        """
    )
    for ps in cur.fetchall():
        cur.execute(
            """
            SELECT fg.name
            FROM sys.destination_data_spaces dds
            JOIN sys.filegroups fg ON fg.data_space_id = dds.data_space_id
            WHERE dds.partition_scheme_id = (
                SELECT partition_scheme_id FROM sys.partition_schemes WHERE name = ?
            )
            ORDER BY dds.destination_id;
            """,
            ps.scheme_name,
        )
        fgs = [qident(r.name) for r in cur.fetchall()]
        if not fgs:
            if logger:
                logger.warning("Partition scheme %s: no filegroups found, skipped", ps.scheme_name)
            continue
        fg_list = ", ".join(fgs)
        out.append(f"-- Partition scheme [{ps.scheme_name}]")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'{ps.scheme_name}')")
        out.append("BEGIN")
        out.append(
            f"    CREATE PARTITION SCHEME {qident(ps.scheme_name)} AS PARTITION {qident(ps.function_name)} "
            f"TO ({fg_list});"
        )
        out.append("END")
        out.append("GO")
        out.append("")

    return _go_block(out)


def export_security_policies(cur, logger=None) -> Tuple[str, List[str]]:
    """Export row-level security policies (CREATE SECURITY POLICY)."""
    cur.execute(
        """
        SELECT sp.object_id, s.name AS schema_name, sp.name AS policy_name, sp.is_enabled
        FROM sys.security_policies sp
        JOIN sys.schemas s ON s.schema_id = sp.schema_id
        ORDER BY s.name, sp.name;
        """
    )
    out: List[str] = []
    warnings: List[str] = []
    for row in cur.fetchall():
        defn = object_definition(cur, row.object_id)
        if not defn:
            warnings.append(f"Security policy {row.schema_name}.{row.policy_name}: definition not available")
            continue
        out.append(f"-- Security policy {row.schema_name}.{row.policy_name}")
        out.append(defn.strip())
        out.append("GO")
        out.append("")
        if not row.is_enabled:
            out.append(
                f"ALTER SECURITY POLICY {qident(row.schema_name)}.{qident(row.policy_name)} WITH (STATE = OFF);\nGO\n"
            )
    return _go_block(out), warnings


def export_database_principals(cur) -> str:
    """
    Export database users and roles (CREATE USER / CREATE ROLE).
    Server logins must exist separately; we emit comments for EXTERNAL/Windows users.
    """
    cur.execute(
        """
        SELECT
            dp.name,
            dp.type_desc,
            dp.default_schema_name,
            sp.name AS login_name
        FROM sys.database_principals dp
        LEFT JOIN sys.server_principals sp ON sp.sid = dp.sid
        WHERE dp.type IN ('S', 'U', 'G', 'E', 'X', 'R', 'A')
          AND dp.principal_id > 4
          AND dp.name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys')
          AND dp.is_fixed_role = 0
        ORDER BY dp.type_desc, dp.name;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        name = row.name.replace("'", "''")
        out.append(f"-- Principal [{row.name}] ({row.type_desc})")
        if row.type_desc == "APPLICATION_ROLE":
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{name}' AND type = 'A')")
            out.append("BEGIN")
            out.append(
                f"    CREATE APPLICATION ROLE {qident(row.name)} "
                f"WITH DEFAULT_SCHEMA = {qident(row.default_schema_name or 'dbo')};"
            )
            out.append(
                f"-- Set password: ALTER APPLICATION ROLE {qident(row.name)} WITH PASSWORD = N'***';"
            )
            out.append("END")
        elif row.type_desc == "DATABASE_ROLE":
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{name}' AND type = 'R')")
            out.append("BEGIN")
            out.append(f"    CREATE ROLE {qident(row.name)};")
            out.append("END")
        elif row.type_desc in ("WINDOWS_USER", "WINDOWS_GROUP", "EXTERNAL_USER", "EXTERNAL_GROUPS"):
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{name}')")
            out.append("BEGIN")
            if row.login_name:
                out.append(
                    f"    CREATE USER {qident(row.name)} FOR LOGIN {qident(row.login_name)} "
                    f"WITH DEFAULT_SCHEMA = {qident(row.default_schema_name or 'dbo')};"
                )
            else:
                out.append(
                    f"    CREATE USER {qident(row.name)} FROM EXTERNAL PROVIDER "
                    f"WITH DEFAULT_SCHEMA = {qident(row.default_schema_name or 'dbo')};"
                )
            out.append("END")
        elif row.type_desc == "SQL_USER" and row.login_name:
            login = row.login_name.replace("'", "''")
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{name}')")
            out.append("BEGIN")
            out.append(
                f"    CREATE USER {qident(row.name)} FOR LOGIN {qident(row.login_name)} "
                f"WITH DEFAULT_SCHEMA = {qident(row.default_schema_name or 'dbo')};"
            )
            out.append("END")
        elif row.type_desc == "SQL_USER":
            out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{name}')")
            out.append("BEGIN")
            out.append(
                f"    CREATE USER {qident(row.name)} WITHOUT LOGIN "
                f"WITH DEFAULT_SCHEMA = {qident(row.default_schema_name or 'dbo')};"
            )
            out.append("END")
        else:
            out.append(
                f"-- Skipped automatic CREATE for {row.type_desc}; "
                f"create manually (login: {row.login_name or 'N/A'})"
            )
        out.append("GO")
        out.append("")
    return _go_block(out)


def export_database_permissions(cur) -> str:
    """Export GRANT/DENY/REVOKE statements for database permissions."""
    cur.execute(
        """
        SELECT
            dp.state_desc,
            dp.permission_name,
            dp.class_desc,
            dp.major_id,
            dp.minor_id,
            grantee.name AS grantee_name,
            grantor.name AS grantor_name,
            COALESCE(SCHEMA_NAME(o.schema_id), SCHEMA_NAME(typ.schema_id)) AS object_schema,
            COALESCE(o.name, typ.name) AS object_name,
            o.type AS object_type
        FROM sys.database_permissions dp
        JOIN sys.database_principals grantee ON grantee.principal_id = dp.grantee_principal_id
        JOIN sys.database_principals grantor ON grantor.principal_id = dp.grantor_principal_id
        LEFT JOIN sys.objects o
            ON o.object_id = dp.major_id
           AND dp.class_desc IN (N'OBJECT_OR_COLUMN', N'OBJECT')
        LEFT JOIN sys.types typ
            ON typ.user_type_id = dp.major_id
           AND dp.class_desc = N'TYPE'
        WHERE dp.class <> 0
          AND grantee.principal_id > 4
          AND dp.permission_name <> 'CONNECT'
        ORDER BY grantee.name, dp.class_desc, dp.permission_name;
        """
    )
    out: List[str] = []
    for row in cur.fetchall():
        action = row.state_desc.replace("_", " ")  # GRANT / DENY / GRANT_WITH_GRANT_OPTION
        if "GRANT_OPTION" in action:
            action = "GRANT"
            with_grant = " WITH GRANT OPTION"
        else:
            with_grant = ""

        perm = row.permission_name
        grantee = qident(row.grantee_name)
        target = _permission_target(row)
        if not target:
            continue
        out.append(f"-- {perm} on {target} to {row.grantee_name}")
        out.append(f"{action} {perm} ON {target} TO {grantee}{with_grant};")
        out.append("GO")
        out.append("")
    return _go_block(out)


def _permission_target(row) -> Optional[str]:
    """Build ON clause target for a permission row."""
    class_desc = (row.class_desc or "").upper()
    if class_desc == "DATABASE":
        return "DATABASE"
    if class_desc == "SCHEMA":
        return "SCHEMA::" + qident(row.object_schema or row.object_name or "")
    if class_desc in ("OBJECT_OR_COLUMN", "OBJECT"):
        if row.object_schema and row.object_name:
            return f"{qident(row.object_schema)}.{qident(row.object_name)}"
    if class_desc == "TYPE":
        if row.object_schema and row.object_name:
            return f"TYPE::{qident(row.object_schema)}.{qident(row.object_name)}"
    return None


def _is_valid_legacy_rule_or_default(defn: str, label: str) -> bool:
    """Legacy rules/defaults must be complete CREATE statements (not binding fragments)."""
    text = (defn or "").strip()
    if not text:
        return False
    upper = text.upper()
    if label == "RULE":
        if not upper.startswith("CREATE RULE"):
            return False
    elif label == "DEFAULT":
        if not upper.startswith("CREATE DEFAULT"):
            return False
    # OBJECT_DEFINITION sometimes returns expression-only fragments on newer builds
    if re.match(r"^(GETUTCDATE|GETDATE|\d+|\(.*\))\s*;?\s*$", upper):
        return False
    if upper.startswith("AS "):
        return False
    return True


def export_legacy_rules_and_defaults(cur) -> str:
    """Export legacy standalone rules and defaults (rare)."""
    out: List[str] = []
    for obj_type, label in (("R", "RULE"), ("D", "DEFAULT")):
        cur.execute(
            """
            SELECT s.name AS schema_name, o.name AS object_name, o.object_id
            FROM sys.objects o
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE o.type = ? AND o.is_ms_shipped = 0
            ORDER BY s.name, o.name;
            """,
            obj_type,
        )
        for row in cur.fetchall():
            defn = object_definition(cur, row.object_id)
            if defn and _is_valid_legacy_rule_or_default(defn, label):
                out.append(f"-- {label} {row.schema_name}.{row.object_name}")
                out.append(defn.strip())
                out.append("GO")
                out.append("")
            else:
                out.append(
                    f"-- Skipped {label} {row.schema_name}.{row.object_name} "
                    f"(not a standalone CREATE {label} on this server version)"
                )
                out.append("")
    return _go_block(out)


def export_plan_guides(cur, logger=None) -> str:
    """Export plan guides when present."""
    try:
        cur.execute(
            """
            SELECT name, is_disabled, scope_type_desc, scope_type, scope_id,
                   CAST(query_text AS NVARCHAR(MAX)) AS query_text,
                   CAST(params AS NVARCHAR(MAX)) AS params,
                   CAST(hints AS NVARCHAR(MAX)) AS hints
            FROM sys.plan_guides
            WHERE is_ms_shipped = 0
            ORDER BY name;
            """
        )
    except Exception:
        if logger:
            logger.info("Plan guides catalog not available on this server edition.")
        return ""

    out: List[str] = []
    for row in cur.fetchall():
        qt = (row.query_text or "").replace("'", "''")
        params = (row.params or "").replace("'", "''") if row.params else None
        hints = (row.hints or "").replace("'", "''")
        out.append(f"-- Plan guide [{row.name}]")
        out.append(f"IF NOT EXISTS (SELECT 1 FROM sys.plan_guides WHERE name = N'{row.name}')")
        out.append("BEGIN")
        stmt = (
            f"    EXEC sp_create_plan_guide @name = N'{row.name}', "
            f"@stmt = N'{qt}', @type = N'SQL', @params = NULL, @hints = N'{hints}';"
        )
        if params:
            stmt = (
                f"    EXEC sp_create_plan_guide @name = N'{row.name}', "
                f"@stmt = N'{qt}', @type = N'SQL', @params = N'{params}', @hints = N'{hints}';"
            )
        out.append(stmt)
        out.append("END")
        if row.is_disabled:
            out.append(f"DISABLE PLAN GUIDE [{row.name}];")
        out.append("GO")
        out.append("")
    return _go_block(out)
