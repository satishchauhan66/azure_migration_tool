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
    @mock.patch.object(_mod, "find_azcopy_executable", return_value=None)
    def test_missing_azcopy(self, _mock_find):
        msg = ensure_azcopy_ready_for_upload("connection_string")
        self.assertIsNotNone(msg)
        self.assertIn("AzCopy", msg)

    @mock.patch.object(_mod, "get_azure_cli_account")
    @mock.patch.object(_mod, "find_azcopy_executable", return_value="C:\\AzCopy\\azcopy.exe")
    def test_mi_requires_login(self, _mock_az, mock_cli):
        mock_cli.return_value = {
            "installed": True,
            "logged_in": False,
            "user": "",
        }
        msg = ensure_azcopy_ready_for_upload("managed_identity")
        self.assertIsNotNone(msg)
        self.assertIn("signed in", msg.lower())

    @mock.patch.object(_mod, "get_azure_cli_account")
    @mock.patch.object(_mod, "find_azcopy_executable", return_value="C:\\AzCopy\\azcopy.exe")
    def test_mi_ready(self, _mock_az, mock_cli):
        mock_cli.return_value = {
            "installed": True,
            "logged_in": True,
            "user": "user@contoso.com",
        }
        msg = ensure_azcopy_ready_for_upload("managed_identity")
        self.assertIsNone(msg)


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
