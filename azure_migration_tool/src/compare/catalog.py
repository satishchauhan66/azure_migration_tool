# Author: Sa-tish Chauhan

"""Load object catalogs from live SQL Server or mirror backup folders."""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..backup.exporters import (
    CATALOG_COMPARE_PROCEDURE_NAMES,
    SUPPLEMENTAL_PROCEDURE_NAMES,
    SUPPLEMENTAL_PROCEDURE_NAME_LIKE,
    fetch_objects,
    fetch_tables,
    object_definition,
    object_module_session_options,
)
from ..utils.azure_compat import is_valid_tsql_schema_name, is_windows_principal_name
from .normalize import definition_hash, module_compare_fingerprint, object_key

_ASSEMBLY_NAME = re.compile(
    r"CREATE\s+ASSEMBLY\s+\[([^\]]+)\]",
    re.IGNORECASE,
)
_MODULE_HEADER = re.compile(
    r"^--\s+(\w+)\.(\w+)\s+\((\w+)\)",
    re.IGNORECASE,
)


def _fetch_assembly_names(cur) -> List[str]:
    cur.execute(
        """
        SELECT a.name
        FROM sys.assemblies a
        WHERE a.is_user_defined = 1
        ORDER BY a.name;
        """
    )
    return [r[0] for r in cur.fetchall()]


def _fetch_database_users(cur) -> List[Tuple[str, str]]:
    """Return (user_name, authentication_type_desc)."""
    cur.execute(
        """
        SELECT dp.name, dp.type_desc
        FROM sys.database_principals dp
        WHERE dp.type IN ('S', 'U', 'G')
          AND dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')
          AND dp.name NOT LIKE '##%'
        ORDER BY dp.name;
        """
    )
    return [(r[0], r[1] or "") for r in cur.fetchall()]


def _fetch_module_definitions(
    cur,
    object_type_letter: str,
    kind: str,
    *,
    supplemental_names: Optional[frozenset] = None,
    supplemental_like: Optional[tuple] = None,
) -> Dict[str, Dict[str, Any]]:
    """object_type_letter: P/PC=procedure, V=view, FN/TF/IF=function."""
    rows = fetch_objects(
        cur,
        object_type_letter,
        include_object_names=supplemental_names,
        include_name_like=supplemental_like,
    )
    out: Dict[str, Dict[str, Any]] = {}
    for schema_name, name, obj_type, oid in rows:
        defn = object_definition(cur, oid)
        ansi, qi = object_module_session_options(cur, oid)
        key = object_key(schema_name, name, kind)
        entry = {
            "schema": schema_name,
            "name": name,
            "type": kind,
            "type_desc": obj_type or "",
            "definition": defn,
            "uses_ansi_nulls": ansi,
            "uses_quoted_identifier": qi,
            "hash": definition_hash(defn, kind),
        }
        entry["fingerprint"] = module_compare_fingerprint(entry, kind)
        out[key] = entry
    return out


def _fetch_principal_schemas(cur) -> Dict[str, Dict[str, Any]]:
    """Windows-principal default schemas (CREATE SCHEMA in Redgate; expected_skip on MI)."""
    cur.execute(
        """
        SELECT s.name AS schema_name, dp.name AS owner_name
        FROM sys.schemas s
        JOIN sys.database_principals dp ON dp.principal_id = s.principal_id
        WHERE s.schema_id > 4
          AND s.name <> N'dbo'
        ORDER BY s.name;
        """
    )
    out: Dict[str, Dict[str, Any]] = {}
    for row in cur.fetchall():
        name = row.schema_name
        if is_valid_tsql_schema_name(name):
            continue
        key = object_key("dbo", name, "SCHEMA")
        out[key] = {
            "name": name,
            "owner": row.owner_name or "",
            "windows_principal": is_windows_principal_name(name)
            or is_windows_principal_name(row.owner_name or ""),
        }
    return out


def _split_sql_go_batches(sql_text: str) -> List[str]:
    """Return non-empty executable batches separated by GO lines."""
    batches: List[str] = []
    current: List[str] = []
    for line in (sql_text or "").splitlines():
        stripped = line.strip()
        if stripped.upper() == "GO":
            text = "\n".join(current).strip()
            if text and not text.startswith("--"):
                batches.append(text)
            current = []
            continue
        if stripped.startswith("--") and not current:
            continue
        current.append(line)
    text = "\n".join(current).strip()
    if text and not text.startswith("--"):
        batches.append(text)
    return batches


def fetch_live_permission_batches(cur) -> List[str]:
    """GRANT/DENY/REVOKE batches from sys.database_permissions (live source)."""
    from ..backup.mirror_exporters import export_database_permissions

    return _split_sql_go_batches(export_database_permissions(cur))


def fetch_live_assembly_batches(cur) -> Dict[str, str]:
    """CREATE ASSEMBLY batches keyed by assembly name from live source."""
    from ..backup.exporters import normalize_assembly_batch_for_deploy
    from ..backup.remaining_exporters import export_assemblies_full

    text, _warnings = export_assemblies_full(cur)
    raw = _parse_assembly_batches_from_sql(text)
    out: Dict[str, str] = {}
    for name, batch in raw.items():
        deploy = normalize_assembly_batch_for_deploy(batch)
        out[name] = deploy or batch
    return out


def _parse_assembly_batches_from_sql(sql_text: str) -> Dict[str, str]:
    batches: Dict[str, str] = {}
    current_name: Optional[str] = None
    current_lines: List[str] = []

    def flush():
        nonlocal current_name, current_lines
        if current_name and current_lines:
            batches[current_name] = "\n".join(current_lines).strip()
        current_name = None
        current_lines = []

    for line in (sql_text or "").splitlines():
        m = _ASSEMBLY_NAME.search(line)
        if m:
            flush()
            current_name = m.group(1)
            current_lines = [line]
        elif current_name:
            if line.strip().upper() == "GO" and current_lines:
                flush()
            else:
                current_lines.append(line)
    flush()
    return batches


def fetch_live_catalog(
    cur,
    *,
    include_definitions: bool = True,
    include_permissions: bool = False,
    include_assembly_batches: bool = False,
    for_schema_compare: bool = False,
    include_diagram_supplemental: bool = False,
) -> Dict[str, Any]:
    """Build compare catalog from an open pyodbc cursor (current database)."""
    tables = fetch_tables(cur)
    table_keys = {object_key(t.schema_name, t.table_name, "TABLE") for t in tables}

    if for_schema_compare:
        proc_supplemental = (
            SUPPLEMENTAL_PROCEDURE_NAMES
            if include_diagram_supplemental
            else CATALOG_COMPARE_PROCEDURE_NAMES
        )
        proc_like = SUPPLEMENTAL_PROCEDURE_NAME_LIKE
    else:
        proc_supplemental = None
        proc_like = None

    catalog: Dict[str, Any] = {
        "tables": sorted(table_keys),
        "procedures": _fetch_module_definitions(
            cur,
            "P,PC",
            "PROC",
            supplemental_names=proc_supplemental,
            supplemental_like=proc_like,
        ),
        "views": _fetch_module_definitions(cur, "V", "VIEW"),
        "functions": _fetch_module_definitions(cur, "FN,TF,IF", "FUNCTION"),
        "assemblies": sorted(_fetch_assembly_names(cur)),
        "principal_schemas": _fetch_principal_schemas(cur),
        "users": {},
    }

    for user_name, type_desc in _fetch_database_users(cur):
        key = object_key("dbo", user_name, "USER")
        catalog["users"][key] = {
            "name": user_name,
            "type_desc": type_desc,
            "windows": is_windows_principal_name(user_name) or "WINDOWS" in (type_desc or "").upper(),
        }

    if not include_definitions:
        for bucket in ("procedures", "views", "functions"):
            for entry in catalog[bucket].values():
                entry.pop("definition", None)

    if include_permissions:
        catalog["permission_batches"] = fetch_live_permission_batches(cur)
    if include_assembly_batches:
        catalog["assembly_batches"] = fetch_live_assembly_batches(cur)

    return catalog


def _resolve_backup_roots(backup_path: Path) -> Dict[str, Path]:
    """Return known subpaths whether backup is flat or under schema/."""
    base = backup_path
    schema = base / "schema"
    if schema.is_dir():
        base = schema
    return {
        "procedures_dir": base / "02_programmables" / "procedures",
        "assemblies_file": base / "00_foundation" / "assemblies.sql",
        "tables_file": base / "01_tables_all.sql",
        "permissions_file": base / "04_security" / "permissions.sql",
    }


def _load_procedure_files(proc_dir: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not proc_dir.is_dir():
        return out
    for path in sorted(proc_dir.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        schema_name, proc_name = "dbo", path.stem
        m = _MODULE_HEADER.match(text.lstrip())
        if m:
            schema_name, proc_name = m.group(1), m.group(2)
        else:
            parts = path.stem.split(".", 1)
            if len(parts) == 2:
                schema_name, proc_name = parts[0], parts[1]
        key = object_key(schema_name, proc_name, "PROC")
        out[key] = {
            "schema": schema_name,
            "name": proc_name,
            "type": "PROC",
            "definition": text,
            "hash": definition_hash(text, "PROC"),
            "source_file": str(path),
        }
    return out


def _parse_assembly_names(sql_text: str) -> List[str]:
    return sorted({m.group(1) for m in _ASSEMBLY_NAME.finditer(sql_text or "")})


def _load_tables_from_backup(tables_file: Path) -> List[str]:
    if not tables_file.is_file():
        return []
    text = tables_file.read_text(encoding="utf-8")
    keys = []
    for m in re.finditer(
        r"CREATE\s+TABLE\s+(\w+)\.(\w+)|CREATE\s+TABLE\s+\[(\w+)\]\.\[(\w+)\]",
        text,
        re.IGNORECASE,
    ):
        if m.group(1):
            keys.append(object_key(m.group(1), m.group(2), "TABLE"))
        else:
            keys.append(object_key(m.group(3), m.group(4), "TABLE"))
    return sorted(set(keys))


def load_backup_catalog(backup_path: Path) -> Dict[str, Any]:
    """Load catalog from a mirror schema backup folder."""
    paths = _resolve_backup_roots(Path(backup_path))
    assemblies: List[str] = []
    if paths["assemblies_file"].is_file():
        assemblies = _parse_assembly_names(paths["assemblies_file"].read_text(encoding="utf-8"))

    return {
        "tables": _load_tables_from_backup(paths["tables_file"]),
        "procedures": _load_procedure_files(paths["procedures_dir"]),
        "views": {},
        "functions": {},
        "assemblies": assemblies,
        "users": {},
        "backup_paths": {k: str(v) for k, v in paths.items()},
    }


def read_assembly_batches_from_backup(backup_path: Path) -> Dict[str, str]:
    """Parse assemblies.sql into {assembly_name: full CREATE ASSEMBLY batch text}."""
    paths = _resolve_backup_roots(Path(backup_path))
    asm_file = paths["assemblies_file"]
    if not asm_file.is_file():
        return {}

    text = asm_file.read_text(encoding="utf-8")
    batches: Dict[str, str] = {}
    current_name = None
    current_lines: List[str] = []

    def flush():
        nonlocal current_name, current_lines
        if current_name and current_lines:
            batches[current_name] = "\n".join(current_lines).strip()
        current_name = None
        current_lines = []

    for line in text.splitlines():
        m = _ASSEMBLY_NAME.search(line)
        if m:
            flush()
            current_name = m.group(1)
            current_lines = [line]
        elif current_name:
            if line.strip().upper() == "GO" and current_lines:
                flush()
            else:
                current_lines.append(line)
    flush()
    return batches


def read_permission_batches_from_backup(backup_path: Path) -> List[str]:
    """Return executable GRANT/DENY/REVOKE batches from backup permissions.sql."""
    paths = _resolve_backup_roots(Path(backup_path))
    perm_file = paths["permissions_file"]
    if not perm_file.is_file():
        return []

    batches: List[str] = []
    current: List[str] = []
    for line in perm_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.upper() == "GO":
            text = "\n".join(current).strip()
            if text and not text.startswith("--"):
                batches.append(text)
            current = []
            continue
        if stripped.startswith("--") and not current:
            continue
        current.append(line)
    text = "\n".join(current).strip()
    if text and not text.startswith("--"):
        batches.append(text)
    return batches
