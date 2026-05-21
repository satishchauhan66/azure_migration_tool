#!/usr/bin/env python
# Author: S@tish Chauhan

"""
Smoke tests for Azure Migration Tool (no GUI, no DB).
Run from repo root: python -m azure_migration_tool.run_smoke_tests
Or: cd azure_migration_tool && python run_smoke_tests.py
"""

import re
import sys
import os
from pathlib import Path

# Ensure package and parent are on path
repo_root = Path(__file__).resolve().parent.parent
app_dir = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

def test_imports():
    """Test that main modules can be imported."""
    errors = []
    # Main (no GUI yet)
    try:
        import azure_migration_tool.main as main_mod
        assert hasattr(main_mod, "main")
    except Exception as e:
        errors.append(("azure_migration_tool.main", e))
    # GUI modules (tk may not be available in headless)
    try:
        import tkinter
    except ImportError:
        print("SKIP: tkinter not available (no GUI tests)")
        return errors
    try:
        from azure_migration_tool.gui.main_window import MainWindow
    except Exception as e:
        errors.append(("MainWindow", e))
    try:
        from azure_migration_tool.gui.widgets.connection_widget import ConnectionWidget, DB_TYPE_DISPLAY, AUTH_DISPLAY
        assert "SQL Server" in str(DB_TYPE_DISPLAY.values())
        assert "Microsoft" in str(AUTH_DISPLAY.values())
    except Exception as e:
        errors.append(("ConnectionWidget", e))
    try:
        from azure_migration_tool.gui.tabs.legacy_schema_validation_tab import LegacySchemaValidationTab
    except Exception as e:
        errors.append(("LegacySchemaValidationTab", e))
    try:
        from azure_migration_tool.gui.utils.tooltip import add_tooltip
    except Exception as e:
        errors.append(("tooltip", e))
    return errors


def test_pyspark_optional():
    """Test PySpark import (optional)."""
    try:
        import pyspark
        print(f"  PySpark {pyspark.__version__} found.")
        return None
    except ImportError as e:
        print(f"  PySpark not installed (optional): {e}")
        return None


def test_wrap_create_or_alter_alter_proc():
    """ALTER PROCEDURE from OBJECT_DEFINITION must become CREATE OR ALTER."""
    from azure_migration_tool.src.backup.exporters import wrap_create_or_alter

    defn = "ALTER PROCEDURE [dbo].[TempGetVersion]\nAS\nSELECT 1;\n"
    out = wrap_create_or_alter("dbo", "TempGetVersion", defn, "PROC")
    assert "CREATE OR ALTER PROCEDURE" in out.upper(), out
    assert not re.search(r"^\s*ALTER\s+PROCEDURE\b", out, re.IGNORECASE | re.MULTILINE), out


def test_temp_get_version_quoted_identifier_export():
    """Double-quoted literals need QUOTED_IDENTIFIER OFF preamble and/or single-quote normalize."""
    from azure_migration_tool.src.backup.exporters import wrap_create_or_alter
    from azure_migration_tool.src.utils.sql import prepare_sql_batches

    defn = (
        "CREATE PROCEDURE [dbo].[TempGetVersion]\n"
        "    @ver CHAR(10) OUTPUT\n"
        "AS\n"
        '    SELECT @ver = "2"\n'
        "    RETURN 0\n"
    )
    out = wrap_create_or_alter(
        "dbo",
        "TempGetVersion",
        defn,
        "PROC",
        session_options=(True, False),
    )
    upper = out.upper()
    assert "SET QUOTED_IDENTIFIER OFF" in upper, out
    assert "SET ANSI_NULLS ON" in upper, out
    assert "SELECT @ver = '2'" in out, out
    assert 'SELECT @ver = "2"' not in out, out

    batches = prepare_sql_batches(out, file_type="PROCEDURES")
    assert len(batches) == 3, batches
    assert batches[0].strip().upper() == "SET ANSI_NULLS ON"
    assert batches[1].strip().upper() == "SET QUOTED_IDENTIFIER OFF"
    proc_batch = batches[2]
    assert "CREATE OR ALTER PROCEDURE" in proc_batch.upper()
    assert not re.match(r"^\s*SET\s+", proc_batch, re.IGNORECASE)
    assert "SELECT @ver = '2'" in proc_batch
    assert "Invalid column name" not in proc_batch


def test_prepare_sql_batches_normalizes_legacy_double_quoted_literal():
    """Old backups without SET preamble still restore via literal normalization."""
    from azure_migration_tool.src.utils.sql import prepare_sql_batches

    legacy = """
CREATE OR ALTER PROCEDURE dbo.TempGetVersion
    @ver CHAR(10) OUTPUT
AS
    SELECT @ver = "2"
    RETURN 0
GO
"""
    batches = prepare_sql_batches(legacy, file_type="PROCEDURES")
    assert len(batches) == 1
    assert "SELECT @ver = '2'" in batches[0]
    assert 'SELECT @ver = "2"' not in batches[0]


def test_procedures_sql_contains_required():
    from azure_migration_tool.src.backup.exporters import procedures_sql_contains_required

    sample = """
    CREATE OR ALTER PROCEDURE dbo.GetMajorVersion AS SELECT 1;
    GO
    CREATE OR ALTER PROCEDURE dbo.sp_creatediagram AS SELECT 1;
    GO
    """
    result = procedures_sql_contains_required(sample)
    assert not result["ok"]
    assert "TempGetVersion" in result["missing"]
    assert "AddWindowsUserAndLogin" in result["missing"]
    assert "sp_creatediagram" not in result["missing"]


def test_split_batches_preserves_temp_get_version():
    """Dynamic SQL must not false-split; TempGetVersion body must stay one batch."""
    from azure_migration_tool.src.utils.sql import prepare_sql_batches

    merged = """
CREATE OR ALTER PROCEDURE dbo.TempGetStateItem2
AS
SELECT 1;
CREATE OR ALTER PROCEDURE dbo.TempGetVersion
    @ver char(10) OUTPUT
AS
    SELECT @ver = '2'
    RETURN 0
"""
    batches = prepare_sql_batches(merged, file_type="PROCEDURES")
    assert len(batches) == 2, batches
    version_batch = batches[1]
    assert "TempGetVersion" in version_batch
    assert "SELECT @ver = '2'" in version_batch


def test_split_batches_ignores_create_inside_string():
    from azure_migration_tool.src.utils.sql import prepare_sql_batches

    merged = """
CREATE OR ALTER PROCEDURE dbo.AddWindowsUserAndLogin
AS
DECLARE @sql nvarchar(max) = N'
CREATE PROCEDURE #inner
AS
SELECT 1';
EXEC sp_executesql @sql;
GO
CREATE OR ALTER PROCEDURE dbo.TempGetVersion
AS
    SELECT @ver = '2'
"""
    batches = prepare_sql_batches(merged, file_type="PROCEDURES")
    assert len(batches) == 2, batches
    assert "AddWindowsUserAndLogin" in batches[0]
    assert "TempGetVersion" in batches[1]


def test_prepare_sql_batches_splits_set_preamble():
    """SET ANSI_NULLS / QUOTED_IDENTIFIER must be separate ODBC batches before CREATE PROCEDURE."""
    from azure_migration_tool.src.utils.sql import prepare_sql_batches

    sql = """
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO
CREATE OR ALTER PROCEDURE dbo.TempGetVersion
    @ver CHAR(10) OUTPUT
AS
    SELECT @ver = '2'
"""
    batches = prepare_sql_batches(sql, file_type="PROCEDURES")
    assert len(batches) == 3, batches
    assert batches[0].strip().upper() == "SET ANSI_NULLS ON"
    assert batches[1].strip().upper() == "SET QUOTED_IDENTIFIER OFF"
    assert re.match(r"^\s*CREATE\s+(?:OR\s+ALTER\s+)?PROC", batches[2], re.IGNORECASE)
    assert "SET ANSI_NULLS" not in batches[2].splitlines()[0].upper()


def test_split_procedure_batches_three_procs():
    """Legacy procedures.sql with few GO lines must still yield one batch per procedure."""
    from azure_migration_tool.src.utils.sql import split_procedure_batches

    sample = """
SET NOCOUNT ON;
GO
-- header batch
CREATE OR ALTER PROCEDURE dbo.GetHashCode
AS
SELECT 1;
CREATE OR ALTER PROCEDURE dbo.TempGetVersion
    @ver char(10) OUTPUT
AS
    SELECT @ver = '2';
CREATE OR ALTER PROCEDURE dbo.sp_creatediagram
AS
SELECT 1;
"""
    batches = split_procedure_batches(sample)
    assert len(batches) == 3, [b.splitlines()[0][:40] for b in batches]
    assert all(
        re.match(r"^\s*CREATE\s+(?:OR\s+ALTER\s+)?PROC(?:EDURE)?\s+", b, re.IGNORECASE)
        for b in batches
    )
    assert "GetHashCode" in batches[0]
    assert "TempGetVersion" in batches[1]
    assert "sp_creatediagram" in batches[2]


def test_expected_skip_classification():
    from azure_migration_tool.src.utils.azure_compat import (
        EXPECTED_SKIP_REASONS,
        classify_expected_skip,
        is_expected_skip_inventory_file,
        is_exportable_schema_authorization,
        is_valid_tsql_schema_name,
        is_windows_principal_batch,
        should_skip_windows_principal_error,
    )

    assert "credential_secret_placeholder" in EXPECTED_SKIP_REASONS
    assert "windows_principal_azure" in EXPECTED_SKIP_REASONS
    assert is_expected_skip_inventory_file("ENCRYPTED_MODULES")
    assert classify_expected_skip(file_type="SERVER_LOGINS") == "server_login_password"
    assert classify_expected_skip(batch_text="SECRET = N'***REPLACE_SECRET***'") == "credential_secret_placeholder"
    assert classify_expected_skip(error_msg="CREATE ASSEMBLY failed", file_type="ASSEMBLIES") == "clr_assembly_azure"
    assert (
        classify_expected_skip(
            error_msg="Msg 6586, Level 16, State 1, Assembly 'Accessibility' could not be installed "
            "because existing policy would keep it from being used",
            file_type="ASSEMBLIES",
        )
        == "clr_framework_policy_azure"
    )
    assert (
        classify_expected_skip(
            error_msg="Msg 10308, Level 16, The assembly 'System.Messaging' is not fully tested "
            "in the SQL Server hosted environment",
            file_type="ASSEMBLIES",
        )
        == "clr_framework_policy_azure"
    )
    assert classify_expected_skip(error_msg="Msg 15007, Level 16") == "windows_principal_azure"
    assert classify_expected_skip(error_msg="Msg 15151, Level 16") == "windows_principal_azure"
    assert classify_expected_skip(error_msg="Msg 41906") == "windows_principal_azure"
    assert not is_valid_tsql_schema_name(r"USPG\eSurveyDev")
    assert is_valid_tsql_schema_name("dbo")
    assert not is_exportable_schema_authorization(r"USPG\eSurveyDev", r"USPG\eSurveyDev")
    assert is_windows_principal_batch("CREATE LOGIN [USPG\\user] FROM WINDOWS")
    assert should_skip_windows_principal_error("Login 'USPG\\user' does not exist. (15007)")


def test_mi_clr_assemblies_not_always_filtered():
    from azure_migration_tool.src.utils.azure_compat import (
        AZURE_MANAGED_INSTANCE_EDITION,
        filter_azure_incompatible_batches,
        should_apply_azure_batch_filter,
    )

    class _Log:
        def info(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

    assert not should_apply_azure_batch_filter(
        True, True, "ASSEMBLIES", engine_edition=AZURE_MANAGED_INSTANCE_EDITION
    )
    assert should_apply_azure_batch_filter(
        True, True, "ASSEMBLIES", engine_edition=5
    )
    batches = ["CREATE ASSEMBLY [MQSessionExpiration] AUTHORIZATION dbo FROM 0x00 WITH PERMISSION_SET = UNSAFE;"]
    kept = filter_azure_incompatible_batches(
        batches, _Log(), file_type="ASSEMBLIES", engine_edition=AZURE_MANAGED_INSTANCE_EDITION
    )
    assert len(kept) == 1
    kept_db = filter_azure_incompatible_batches(batches, _Log(), file_type="ASSEMBLIES", engine_edition=5)
    assert kept_db == []


def test_filter_windows_principal_batches():
    from azure_migration_tool.src.utils.azure_compat import filter_azure_incompatible_batches

    class _Log:
        def info(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

    batches = [
        "CREATE USER [USPG\\IT_ESURVEY_DB] FOR LOGIN [USPG\\IT_ESURVEY_DB];",
        "ALTER AUTHORIZATION ON SCHEMA::[dbo] TO [dbo];",
        "ALTER AUTHORIZATION ON SCHEMA::[USPG\\eSurveyDev] TO [USPG\\eSurveyDev];",
    ]
    kept = filter_azure_incompatible_batches(batches, _Log(), file_type="DATABASE_PRINCIPALS")
    assert len(kept) == 1
    assert "dbo" in kept[0][1]


def test_build_restore_order_builtin():
    import tempfile
    from azure_migration_tool.src.restore.schema_restore import BUILTIN_RESTORE_ORDER, build_restore_order

    class _Log:
        def info(self, *args, **kwargs):
            pass

    with tempfile.TemporaryDirectory() as tmp:
        backup_path = Path(tmp)
        proc_dir = backup_path / "procedures"
        proc_dir.mkdir()
        schemas = backup_path / "schemas.sql"
        schemas.write_text("CREATE SCHEMA x;", encoding="utf-8")
        proc_a = proc_dir / "a.sql"
        proc_a.write_text("CREATE OR ALTER PROCEDURE dbo.a AS SELECT 1;", encoding="utf-8")
        paths = {
            "schemas_file": schemas,
            "procedures_dir": proc_dir,
            "procedure_files": [proc_a],
        }
        cfg = {
            "full_mirror": True,
            "restore_tables": True,
            "restore_programmables": True,
            "restore_constraints": True,
            "restore_indexes": True,
            "restore_security": True,
        }
        order = build_restore_order(backup_path, paths, cfg, _Log())
        keys = [k for k, _, _ in order]
        assert "schemas_file" in keys
        assert any(k.startswith("procedure:") for k in keys)
        assert len(BUILTIN_RESTORE_ORDER) >= 40


def test_azure_database_options_filter():
    from azure_migration_tool.src.utils.azure_compat import (
        filter_azure_incompatible_batches,
        is_azure_supported_database_option_batch,
    )

    class _Log:
        def info(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

    sql = """
ALTER DATABASE CURRENT SET COMPATIBILITY_LEVEL = 150;
GO
ALTER DATABASE CURRENT SET RECOVERY FULL;
GO
"""
    batches = [b.strip() for b in sql.split("GO") if b.strip()]
    assert not is_azure_supported_database_option_batch(batches[0])
    assert not is_azure_supported_database_option_batch(batches[1])
    kept = filter_azure_incompatible_batches(batches, _Log(), file_type="DATABASE_OPTIONS")
    assert kept == []


def test_normalize_procedure_text_for_compare():
    from azure_migration_tool.src.compare.normalize import (
        definition_hash,
        module_compare_fingerprint,
        normalize_module_text_for_compare,
    )

    a = "CREATE PROCEDURE dbo.X AS SELECT 1;"
    b = "CREATE OR ALTER PROCEDURE dbo.X AS SELECT 1;"
    c = "ALTER PROCEDURE dbo.X AS SELECT 1;"
    assert normalize_module_text_for_compare(a, "PROC") == normalize_module_text_for_compare(b, "PROC")
    assert normalize_module_text_for_compare(b, "PROC") == normalize_module_text_for_compare(c, "PROC")
    assert definition_hash(a, "PROC") == definition_hash(c, "PROC")

    same_body = {
        "definition": "CREATE PROCEDURE dbo.TempGetVersion AS SELECT @ver = '2';",
        "hash": definition_hash("CREATE PROCEDURE dbo.TempGetVersion AS SELECT @ver = '2';", "PROC"),
        "uses_ansi_nulls": True,
        "uses_quoted_identifier": True,
    }
    qi_off = dict(same_body, uses_quoted_identifier=False)
    assert module_compare_fingerprint(same_body, "PROC") != module_compare_fingerprint(
        qi_off, "PROC"
    )


def test_repair_script_missing_proc_drop_and_create():
    from azure_migration_tool.src.compare.repair_generator import generate_repair_script
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {
            "proc.dbo.tempgetversion": {
                "schema": "dbo",
                "name": "TempGetVersion",
                "type": "PROC",
                "definition": 'ALTER PROCEDURE dbo.TempGetVersion AS SELECT @ver = "2";',
                "uses_ansi_nulls": True,
                "uses_quoted_identifier": False,
                "hash": "abc",
            }
        },
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
    }
    target = {
        "tables": [],
        "procedures": {},
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
    }
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    sql = generate_repair_script(
        report,
        source,
        source_label="source_db",
        target_label="target_db",
        dest_is_azure=True,
    )
    upper = sql.upper()
    assert "DROP PROCEDURE" in upper
    assert "CREATE OR ALTER PROCEDURE" in upper
    assert "TEMPGETVERSION" in upper
    assert "SET QUOTED_IDENTIFIER OFF" in upper
    assert "SELECT @VER = '2'" in upper


def test_mi_blocked_framework_assembly_names():
    from azure_migration_tool.src.utils.azure_compat import (
        is_mi_blocked_framework_assembly_batch,
        is_mi_blocked_framework_assembly_name,
    )

    assert is_mi_blocked_framework_assembly_name("Accessibility")
    assert is_mi_blocked_framework_assembly_name("Messaging")
    assert not is_mi_blocked_framework_assembly_name("MQSessionExpiration")
    batch = "CREATE ASSEMBLY [System.Drawing] AUTHORIZATION [dbo] FROM 0x00 WITH PERMISSION_SET = UNSAFE"
    assert is_mi_blocked_framework_assembly_batch(batch)


def test_assemblies_expected_skip_in_compare_report():
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {},
        "views": {},
        "functions": {},
        "assemblies": [
            "Accessibility",
            "MQSessionExpiration",
            "System.Drawing",
        ],
        "users": {},
    }
    target = {
        "tables": [],
        "procedures": {},
        "views": {},
        "functions": {},
        "assemblies": ["MQSessionExpiration"],
        "users": {},
    }
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    asm = report["assemblies"]
    assert asm["expected_skip_count"] == 2
    skipped = {e["name"] for e in asm["expected_skip"]}
    assert skipped == {"Accessibility", "System.Drawing"}


def test_clr_custom_proc_not_expected_skip_on_mi():
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {
            "proc.dbo.uspmsmqsend": {
                "schema": "dbo",
                "name": "uspMSMQSend",
                "type": "PROC",
                "type_desc": "CLR_STORED_PROCEDURE",
                "definition": (
                    "CREATE PROCEDURE dbo.uspMSMQSend AS EXTERNAL NAME "
                    "[MQSessionExpiration].[PressGaney.ElectronicSurvey."
                    "SQLMQSessionExpiration.SQLMQSessionExpiration].[SendSessionData]"
                ),
                "hash": "x",
                "fingerprint": "x",
            }
        },
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
        "principal_schemas": {},
    }
    target = dict(source)
    target["procedures"] = {}
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    assert report["procedures"]["missing_count"] == 1
    assert report["procedures"]["expected_skip_count"] == 0


def test_nonportable_proc_expected_skip_in_compare():
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {
            "proc.dbo.addwindowsuserandlogin": {
                "schema": "dbo",
                "name": "AddWindowsUserAndLogin",
                "type": "PROC",
                "definition": "CREATE PROCEDURE dbo.AddWindowsUserAndLogin AS SELECT * FROM master.dbo.syslogins",
                "hash": "x",
            }
        },
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
    }
    target = {"tables": [], "procedures": {}, "views": {}, "functions": {}, "assemblies": [], "users": {}}
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    assert report["procedures"]["expected_skip_count"] == 1
    assert report["procedures"]["expected_skip"][0]["reason"] == "syslogins_master_azure"


def test_schema_compare_output_dir():
    from azure_migration_tool.src.utils.paths import schema_compare_output_dir

    base = Path(os.environ.get("TEMP", "."))
    out = schema_compare_output_dir(
        base, "src-srv", "MyDb", "tgt-srv", "OtherDb"
    )
    assert out.is_dir()
    assert "schema_compare_output" in str(out)
    assert "src-srv_MyDb_to_tgt-srv_OtherDb" in str(out)


def test_normalize_module_create_after_comment():
    from azure_migration_tool.src.backup.exporters import _normalize_module_definition

    defn = "/* header */\nCREATE PROCEDURE [dbo].[GetHashCode] AS SELECT 1"
    out = _normalize_module_definition(defn, "PROC", session_options=None)
    assert "CREATE OR ALTER PROCEDURE" in out.upper()
    assert "CREATE PROCEDURE" not in re.sub(
        r"CREATE\s+OR\s+ALTER\s+PROCEDURE", "", out, flags=re.IGNORECASE
    )


def test_normalize_assembly_batch_strips_wrapper():
    from azure_migration_tool.src.backup.exporters import normalize_assembly_batch_for_deploy

    raw = (
        "IF NOT EXISTS (SELECT 1 FROM sys.assemblies WHERE name = N'MyAsm')\n"
        "BEGIN\n"
        "    CREATE ASSEMBLY [MyAsm] AUTHORIZATION dbo FROM 0x00 WITH PERMISSION_SET = UNSAFE;\n"
        "END"
    )
    out = normalize_assembly_batch_for_deploy(raw)
    assert "BEGIN" not in out.upper().split()
    assert out.upper().startswith("CREATE ASSEMBLY")
    assert "WITH PERMISSION_SET" in out.upper()


def test_repair_gethashcode_section_batches():
    """Module repair must end proc batch before next PRINT (GO between items)."""
    from azure_migration_tool.src.compare.repair_generator import (
        generate_repair_items,
        repair_items_to_sql,
    )
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {
            "proc.dbo.gethashcode": {
                "schema": "dbo",
                "name": "GetHashCode",
                "type": "PROC",
                "definition": (
                    "/* comment */\n"
                    "CREATE PROCEDURE [dbo].[GetHashCode] AS\n"
                    "    SELECT 1\n"
                    "    RETURN 0\n"
                ),
                "uses_ansi_nulls": True,
                "uses_quoted_identifier": False,
                "hash": "a",
            },
            "proc.dbo.tempgetappid": {
                "schema": "dbo",
                "name": "TempGetAppID",
                "type": "PROC",
                "definition": "CREATE PROCEDURE [dbo].[TempGetAppID] AS SELECT 2",
                "uses_ansi_nulls": True,
                "uses_quoted_identifier": False,
                "hash": "b",
            },
        },
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
    }
    target = {
        "tables": [],
        "procedures": {},
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
    }
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    items = generate_repair_items(report, source, dest_is_azure=True)
    gh = next(i for i in items if i.get("object_name") == "dbo.GetHashCode")
    batches = gh["sql_batches"]
    assert batches[0].startswith("PRINT")
    create_batches = [b for b in batches if re.search(r"CREATE\s+OR\s+ALTER\s+PROC", b, re.I)]
    assert len(create_batches) == 1
    assert create_batches[0].strip().endswith("RETURN 0")
    assert "SET ANSI_NULLS ON" in batches
    assert "SET QUOTED_IDENTIFIER OFF" in batches
    script = repair_items_to_sql(items)
    norm = script.replace("\r\n", "\n")
    assert "RETURN 0\nGO\nPRINT N'Fixing [dbo].[TempGetAppID]'" in norm


def test_repair_clr_proc_batches_include_external_name():
    from azure_migration_tool.src.compare.repair_generator import generate_repair_items
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {
            "proc.dbo.uspmsmqsend": {
                "schema": "dbo",
                "name": "uspMSMQSend",
                "type": "PROC",
                "type_desc": "CLR_STORED_PROCEDURE",
                "definition": (
                    "CREATE PROCEDURE [dbo].[uspMSMQSend] "
                    "AS EXTERNAL NAME [MQSessionExpiration].[PressGaney.ElectronicSurvey."
                    "SQLMQSessionExpiration.SQLMQSessionExpiration].[SendSessionData]"
                ),
                "uses_ansi_nulls": True,
                "uses_quoted_identifier": True,
                "hash": "x",
                "fingerprint": "x",
            }
        },
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
    }
    target = dict(source)
    target["procedures"] = {}
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    items = generate_repair_items(report, source, dest_is_azure=True)
    proc = next(i for i in items if i.get("object_name") == "dbo.uspMSMQSend")
    joined = "\n".join(proc["sql_batches"]).upper()
    assert "EXTERNAL NAME" in joined
    assert "CREATE OR ALTER PROCEDURE" in joined
    assert "DEFINITION NOT AVAILABLE" not in joined


def test_repair_items_from_mock_diff():
    from azure_migration_tool.src.compare.repair_generator import generate_repair_items
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {
            "proc.dbo.tempgetversion": {
                "schema": "dbo",
                "name": "TempGetVersion",
                "type": "PROC",
                "definition": 'ALTER PROCEDURE dbo.TempGetVersion AS SELECT @ver = "2";',
                "uses_ansi_nulls": True,
                "uses_quoted_identifier": False,
                "hash": "abc",
            }
        },
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
    }
    target = {
        "tables": [],
        "procedures": {},
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
    }
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    items = generate_repair_items(report, source, dest_is_azure=True)
    actionable = [i for i in items if not i["expected_skip"]]
    assert len(actionable) == 1
    proc = actionable[0]
    assert proc["category"] == "PROCEDURE"
    assert proc["object_name"] == "dbo.TempGetVersion"
    assert proc["action"] == "CREATE"
    assert proc["sql_batches"]
    joined = "\n".join(proc["sql_batches"]).upper()
    assert "DROP PROCEDURE" in joined
    assert "CREATE OR ALTER PROCEDURE" in joined


def test_repair_script_permissions_from_live_catalog():
    from azure_migration_tool.src.compare.repair_generator import generate_repair_script
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {},
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
        "principal_schemas": {},
        "permission_batches": [
            "GRANT EXECUTE ON [dbo].[SomeProc] TO [app_user];",
            "GRANT REFERENCES ON TYPE:: [dbo].[tAppName] TO [public];",
        ],
    }
    target = dict(source)
    target["permission_batches"] = [
        "GRANT EXECUTE ON [dbo].[SomeProc] TO [app_user];",
    ]
    report = compare_schema_catalogs(source, target, dest_is_azure=False)
    sql = generate_repair_script(report, source, dest_is_azure=False)
    assert "GRANT REFERENCES ON TYPE::" in sql
    assert "TAPPNAME" in sql.upper()
    assert "TO [PUBLIC]" in sql.upper()
    assert "permissions from source" in sql.lower()
    assert report["permissions"]["missing_count"] == 1


def test_diagram_procs_extra_on_target_compare():
    import copy

    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {"proc.dbo.tempgetversion": {"schema": "dbo", "name": "TempGetVersion", "hash": "a", "fingerprint": "a"}},
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
        "principal_schemas": {},
    }
    target = copy.deepcopy(source)
    target["procedures"]["proc.dbo.sp_creatediagram"] = {
        "schema": "dbo",
        "name": "sp_creatediagram",
        "hash": "b",
        "fingerprint": "b",
    }
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    assert "proc.dbo.sp_creatediagram" in (report["procedures"].get("extra") or [])


def test_type_permission_export_sql():
    from azure_migration_tool.src.backup.mirror_exporters import _permission_target

    class Row:
        class_desc = "TYPE"
        object_schema = "dbo"
        object_name = "tAppName"

    assert _permission_target(Row()) == "TYPE::[dbo].[tAppName]"


def test_repair_items_missing_type_grants_to_public():
    """TYPE REFERENCES grants to [public] must diff and emit deployable repair batches."""
    from azure_migration_tool.src.compare.repair_generator import generate_repair_items
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    type_grants = [
        "GRANT REFERENCES ON TYPE::[dbo].[tSessionItemShort] TO [public];",
        "GRANT REFERENCES ON TYPE::[dbo].[tSessionId] TO [public];",
    ]
    source = {
        "tables": [],
        "procedures": {},
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {},
        "permission_batches": type_grants + [
            "GRANT EXECUTE ON [dbo].[SomeProc] TO [app_user];",
        ],
    }
    target = dict(source)
    target["permission_batches"] = [
        "GRANT EXECUTE ON [dbo].[SomeProc] TO [app_user];",
    ]
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    assert report["permissions"]["missing_count"] == 2
    missing = report["permissions"]["missing"]
    for type_name in ("TSESSIONITEMSHORT", "TSESSIONID"):
        assert any(type_name in b.upper() for b in missing)
    assert all("TO [PUBLIC]" in b.upper() for b in missing)

    items = generate_repair_items(report, source, dest_is_azure=True)
    perm_items = [
        i
        for i in items
        if i.get("category") == "PERMISSION"
        and i.get("id") != "permission.header"
        and not i.get("expected_skip")
    ]
    assert len(perm_items) == 2
    for item in perm_items:
        batch = item["sql_batches"][0]
        assert batch.upper().startswith("GRANT REFERENCES ON TYPE::[")
        assert "TYPE:: [" not in batch
        assert "to [public]" in batch.lower()


def test_expected_skip_windows_user_in_compare_report():
    from azure_migration_tool.src.compare.schema_compare import compare_schema_catalogs

    source = {
        "tables": [],
        "procedures": {},
        "views": {},
        "functions": {},
        "assemblies": [],
        "users": {
            "user.dbo.uspg\\it_esurvey_db": {
                "name": "USPG\\IT_ESURVEY_DB",
                "windows": True,
            }
        },
    }
    target = {"tables": [], "procedures": {}, "views": {}, "functions": {}, "assemblies": [], "users": {}}
    report = compare_schema_catalogs(source, target, dest_is_azure=True)
    assert report["users"]["expected_skip_count"] >= 1
    assert report["users"]["expected_skip"][0]["reason"] == "windows_principal_azure"


def test_procedures_sql_from_backup_if_present():
    """When SCHEMA_BACKUP_PATH points at a run folder, verify procedures.sql markers."""
    backup_root = os.environ.get("SCHEMA_BACKUP_PATH", "").strip()
    if not backup_root:
        return None
    from azure_migration_tool.src.backup.exporters import procedures_sql_contains_required

    proc_file = Path(backup_root) / "02_programmables" / "procedures.sql"
    if not proc_file.is_file():
        proc_file = Path(backup_root) / "schema" / "02_programmables" / "procedures.sql"
    if not proc_file.is_file():
        print(f"  SKIP: no procedures.sql under {backup_root}")
        return None
    result = procedures_sql_contains_required(proc_file.read_text(encoding="utf-8"))
    if not result["ok"]:
        return [("procedures.sql", f"Missing markers: {', '.join(result['missing'])}")]
    print(f"  OK procedures.sql contains required procs ({proc_file})")
    return None


def test_db2_azure_validation_optional():
    """Test db2_azure_validation import (optional)."""
    try:
        from db2_azure_validation.services.schema_validation_service import PySparkSchemaValidationService
        print("  db2_azure_validation found.")
        return None
    except ImportError as e:
        print(f"  db2_azure_validation not found (optional): {e}")
        return None


def main():
    print("=" * 60)
    print("Azure Migration Tool – smoke tests")
    print("=" * 60)
    print("\n1. Core imports")
    import_errors = test_imports()
    if import_errors:
        for name, err in import_errors:
            print(f"  FAIL {name}: {err}")
        print("\nResult: FAIL (import errors)")
        return 1
    print("  OK")
    print("\n2. Procedure export helpers")
    try:
        test_wrap_create_or_alter_alter_proc()
        test_temp_get_version_quoted_identifier_export()
        test_prepare_sql_batches_splits_set_preamble()
        test_prepare_sql_batches_normalizes_legacy_double_quoted_literal()
        test_procedures_sql_contains_required()
        test_split_batches_preserves_temp_get_version()
        test_split_batches_ignores_create_inside_string()
        test_split_procedure_batches_three_procs()
        test_expected_skip_classification()
        test_mi_clr_assemblies_not_always_filtered()
        test_filter_windows_principal_batches()
        test_build_restore_order_builtin()
        test_azure_database_options_filter()
        test_normalize_procedure_text_for_compare()
        test_repair_script_missing_proc_drop_and_create()
        test_normalize_module_create_after_comment()
        test_normalize_assembly_batch_strips_wrapper()
        test_repair_gethashcode_section_batches()
        test_repair_clr_proc_batches_include_external_name()
        test_repair_items_from_mock_diff()
        test_schema_compare_output_dir()
        test_repair_script_permissions_from_live_catalog()
        test_diagram_procs_extra_on_target_compare()
        test_type_permission_export_sql()
        test_repair_items_missing_type_grants_to_public()
        test_mi_blocked_framework_assembly_names()
        test_assemblies_expected_skip_in_compare_report()
        test_clr_custom_proc_not_expected_skip_on_mi()
        test_nonportable_proc_expected_skip_in_compare()
        test_expected_skip_windows_user_in_compare_report()
        print("  OK")
    except Exception as e:
        print(f"  FAIL: {e}")
        return 1
    print("\n3. Optional: procedures.sql in SCHEMA_BACKUP_PATH")
    backup_errors = test_procedures_sql_from_backup_if_present()
    if backup_errors:
        for name, err in backup_errors:
            print(f"  FAIL {name}: {err}")
        return 1
    print("\n4. Optional: PySpark")
    test_pyspark_optional()
    print("\n5. Optional: db2_azure_validation")
    test_db2_azure_validation_optional()
    print("\n" + "=" * 60)
    print("Result: PASS")
    print("=" * 60)
    print("For E2E with real DBs: python -m azure_migration_tool.test_legacy_data_validation --config path/to/database_config.json --e2e")
    print("For schema E2E: python -m azure_migration_tool.test_legacy_schema_validation --config path/to/database_config.json --e2e")
    return 0


if __name__ == "__main__":
    sys.exit(main())
