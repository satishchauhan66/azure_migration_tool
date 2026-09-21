#!/usr/bin/env python
# Author: Satish Chauhan
"""Tests for AzCopy / Azure CLI helper utilities."""

import sys
import unittest
from pathlib import Path
from unittest import mock

_PKG = Path(__file__).resolve().parent
sys.path.insert(0, str(_PKG))
sys.path.insert(0, str(_PKG.parent))

import importlib.util
import types

sys.modules.setdefault("src", types.ModuleType("src"))
sys.modules.setdefault("src.utils", types.ModuleType("src.utils"))

_sub_spec = importlib.util.spec_from_file_location(
    "subprocess_utils",
    _PKG / "src" / "utils" / "subprocess_utils.py",
)
_sub_mod = importlib.util.module_from_spec(_sub_spec)
assert _sub_spec.loader is not None
_sub_spec.loader.exec_module(_sub_mod)
sys.modules["src.utils.subprocess_utils"] = _sub_mod

_spec = importlib.util.spec_from_file_location(
    "azcopy_utils",
    _PKG / "src" / "utils" / "azcopy_utils.py",
)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)

build_blob_destination_url = _mod.build_blob_destination_url
ensure_azcopy_ready_for_upload = _mod.ensure_azcopy_ready_for_upload
format_azure_tools_status = _mod.format_azure_tools_status
install_instructions = _mod.install_instructions
resolve_parallel_upload_workers = _mod.resolve_parallel_upload_workers
azcopy_concurrency_for_parallel_jobs = _mod.azcopy_concurrency_for_parallel_jobs
apply_azcopy_performance_env = _mod.apply_azcopy_performance_env
apply_azcopy_entra_auth_env = _mod.apply_azcopy_entra_auth_env


class TestParallelUploadTuning(unittest.TestCase):
    def test_max_workers_all_stripes_capped(self):
        self.assertEqual(resolve_parallel_upload_workers(4, None), 4)
        self.assertEqual(resolve_parallel_upload_workers(80, None), 64)

    def test_explicit_worker_limit(self):
        self.assertEqual(resolve_parallel_upload_workers(8, 4), 4)

    def test_concurrency_scales_down_with_jobs(self):
        self.assertLess(
            azcopy_concurrency_for_parallel_jobs(8),
            azcopy_concurrency_for_parallel_jobs(1),
        )

    def test_apply_performance_env(self):
        env = apply_azcopy_performance_env({}, parallel_file_jobs=4)
        self.assertIn("AZCOPY_CONCURRENCY_VALUE", env)

    def test_apply_entra_auth_env_uses_azcli(self):
        env = apply_azcopy_entra_auth_env({}, tenant_id="11111111-2222-3333-4444-555555555555")
        self.assertEqual(env["AZCOPY_AUTO_LOGIN_TYPE"], "AZCLI")
        self.assertEqual(env["AZCOPY_TENANT_ID"], "11111111-2222-3333-4444-555555555555")


class TestUploadAzcopyCommand(unittest.TestCase):
    @mock.patch.object(_mod, "popen_silent")
    @mock.patch.object(_mod, "get_azure_cli_account")
    @mock.patch.object(_mod, "prepare_azcopy_entra_auth", return_value=None)
    @mock.patch.object(_mod, "find_azcopy_executable", return_value="C:\\AzCopy\\azcopy.exe")
    def test_copy_omits_auth_mode_flag(self, _find, _prep, mock_cli, mock_popen):
        _mod._azcopy_entra_session_ready = True
        mock_cli.return_value = {
            "user": "user@contoso.com",
            "tenant": "tid",
        }
        proc = mock.Mock()
        proc.stdout = iter([])
        proc.wait = mock.Mock(return_value=0)
        proc.returncode = 0
        mock_popen.return_value = proc

        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(suffix=".bak", delete=False) as tmp:
            path = Path(tmp.name)
        try:
            _mod.upload_file_with_azcopy(
                path,
                blob_auth_mode="managed_identity",
                blob_connection_string="",
                blob_account_url="https://acct.blob.core.windows.net",
                container="c",
                blob_path="x/y.bak",
                log=lambda _m: None,
            )
        finally:
            path.unlink(missing_ok=True)

        cmd = mock_popen.call_args[0][0]
        self.assertNotIn("--auth-mode", cmd)
        env = mock_popen.call_args[1]["env"]
        self.assertNotIn("AZCOPY_AUTO_LOGIN_TYPE", env)

    def test_cap_parallel_workers_for_entra(self):
        self.assertEqual(_mod.cap_parallel_workers_for_entra("managed_identity", 64), 4)
        self.assertEqual(_mod.cap_parallel_workers_for_entra("connection_string", 64), 64)


class TestBuildBlobDestinationUrl(unittest.TestCase):
    def test_basic_path(self):
        url = build_blob_destination_url(
            account_url="https://myaccount.blob.core.windows.net",
            container="sqlbackups",
            blob_path="MyDb/20260810_031718/MyDb.bak",
        )
        self.assertEqual(
            url,
            "https://myaccount.blob.core.windows.net/sqlbackups/MyDb/20260810_031718/MyDb.bak",
        )


class TestEnsureAzcopyReady(unittest.TestCase):
    @mock.patch.object(_mod, "run_azcopy_smoke_check", return_value=None)
    @mock.patch.object(_mod, "find_azcopy_executable", return_value=None)
    def test_missing_azcopy(self, _mock_find, _smoke):
        msg = ensure_azcopy_ready_for_upload("connection_string")
        self.assertIsNotNone(msg)
        self.assertIn("AzCopy", msg)

    @mock.patch.object(_mod, "run_azcopy_smoke_check", return_value=None)
    @mock.patch.object(_mod, "get_azure_cli_account")
    @mock.patch.object(_mod, "find_azcopy_executable", return_value="C:\\AzCopy\\azcopy.exe")
    def test_mi_requires_login(self, _mock_az, mock_cli, _smoke):
        mock_cli.return_value = {
            "installed": True,
            "logged_in": False,
            "user": "",
        }
        msg = ensure_azcopy_ready_for_upload("managed_identity")
        self.assertIsNotNone(msg)
        self.assertIn("signed in", msg.lower())

    @mock.patch.object(_mod, "prepare_azcopy_entra_auth", return_value=None)
    @mock.patch.object(_mod, "run_azcopy_smoke_check", return_value=None)
    @mock.patch.object(_mod, "get_azure_cli_account")
    @mock.patch.object(_mod, "find_azcopy_executable", return_value="C:\\AzCopy\\azcopy.exe")
    def test_mi_ready(self, _mock_az, mock_cli, _smoke, _prep):
        mock_cli.return_value = {
            "installed": True,
            "logged_in": True,
            "user": "user@contoso.com",
        }
        msg = ensure_azcopy_ready_for_upload("managed_identity")
        self.assertIsNone(msg)


class TestFindBundledAzcopy(unittest.TestCase):
    @mock.patch.object(_mod, "_app_install_dirs")
    def test_prefers_bundled_next_to_exe(self, mock_dirs):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            exe = base / "tools" / "azcopy" / "azcopy.exe"
            exe.parent.mkdir(parents=True)
            exe.write_bytes(b"stub")
            mock_dirs.return_value = [base]
            with mock.patch.object(_mod, "shutil") as mock_shutil:
                mock_shutil.which.return_value = None
                found = _mod.find_azcopy_executable()
            self.assertEqual(found, str(exe.resolve()))


class TestFormatStatus(unittest.TestCase):
    @mock.patch.object(_mod, "get_azure_cli_account")
    @mock.patch.object(_mod, "find_azcopy_executable", return_value="azcopy")
    @mock.patch.object(_mod, "get_azcopy_version", return_value="azcopy version 10.24.0")
    def test_format(self, _ver, _find, mock_cli):
        mock_cli.return_value = {
            "installed": True,
            "logged_in": True,
            "user": "dba@contoso.com",
        }
        text = format_azure_tools_status()
        self.assertIn("AzCopy", text)
        self.assertIn("dba@contoso.com", text)


class TestInstallInstructions(unittest.TestCase):
    def test_contains_winget(self):
        self.assertIn("winget", install_instructions())


if __name__ == "__main__":
    unittest.main()
