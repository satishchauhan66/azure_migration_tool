#!/usr/bin/env python
# Author: Satish Chauhan
"""Tests for local .bak upload blob auth (container URL parsing + mocked upload)."""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_PKG = Path(__file__).resolve().parent
sys.path.insert(0, str(_PKG))
sys.path.insert(0, str(_PKG.parent))

try:
    from src.backup.local_backup_and_upload import (
        _get_tool_blob_service_client,
        _parse_backup_filename,
        _parse_storage_account_url_for_upload,
        build_blob_upload_path,
        _upload_file_to_blob,
        upload_existing_bak_to_blob,
    )
    from gui.tabs.backup_restore_tab import (
        _normalize_blob_account_url_for_gui,
        _resolve_container_for_gui,
    )
except ImportError:
    from azure_migration_tool.src.backup.local_backup_and_upload import (
        _get_tool_blob_service_client,
        _parse_backup_filename,
        _parse_storage_account_url_for_upload,
        build_blob_upload_path,
        _upload_file_to_blob,
        upload_existing_bak_to_blob,
    )
    from azure_migration_tool.gui.tabs.backup_restore_tab import (
        _normalize_blob_account_url_for_gui,
        _resolve_container_for_gui,
    )


ACCOUNT = "https://myaccount.blob.core.windows.net"
ACCOUNT_WITH_CONTAINER = "https://myaccount.blob.core.windows.net/sqlbackups"


class TestBuildBlobUploadPath(unittest.TestCase):
    def test_structured_layout_matches_bak_to_blob(self):
        path = build_blob_upload_path(
            local_filename="NICU_ss_sld_db22u_20260810_031718.bak",
            database="NICU_ss_sld_db22u",
            structured_layout=True,
        )
        self.assertEqual(path, "NICU_ss_sld_db22u/20260810_031718/NICU_ss_sld_db22u_20260810_031718.bak")

    def test_structured_striped_file_same_run_folder(self):
        path = build_blob_upload_path(
            local_filename="NICU_ss_sld_db22u_20260810_031718_part01of04.bak",
            database="NICU_ss_sld_db22u",
            structured_layout=True,
        )
        self.assertEqual(
            path,
            "NICU_ss_sld_db22u/20260810_031718/NICU_ss_sld_db22u_20260810_031718_part01of04.bak",
        )

    def test_structured_with_optional_root_prefix(self):
        path = build_blob_upload_path(
            local_filename="MyDb_20260810_120000.bak",
            blob_folder="prod/backups",
            database="MyDb",
            structured_layout=True,
        )
        self.assertEqual(path, "prod/backups/MyDb/20260810_120000/MyDb_20260810_120000.bak")

    def test_flat_layout_legacy(self):
        path = build_blob_upload_path(
            local_filename="MyDb_20260810_120000.bak",
            blob_folder="backups",
            structured_layout=False,
        )
        self.assertEqual(path, "backups/MyDb_20260810_120000.bak")

    def test_parse_backup_filename(self):
        db, run_id = _parse_backup_filename("NICU_ss_sld_db22u_20260810_031718_part02of04.bak")
        self.assertEqual(db, "NICU_ss_sld_db22u")
        self.assertEqual(run_id, "20260810_031718")


class TestParseStorageAccountUrlForUpload(unittest.TestCase):
    def test_account_url_plus_separate_container_field(self):
        """Regression: MI upload must accept container from Step 3 field, not URL path."""
        root = _parse_storage_account_url_for_upload(ACCOUNT, "sqlbackups")
        self.assertEqual(root, ACCOUNT)

    def test_container_in_url_when_field_empty(self):
        root = _parse_storage_account_url_for_upload(ACCOUNT_WITH_CONTAINER, "")
        self.assertEqual(root, ACCOUNT)

    def test_empty_container_and_bare_account_url_raises(self):
        with self.assertRaises(ValueError) as ctx:
            _parse_storage_account_url_for_upload(ACCOUNT, "")
        self.assertIn("Container is required", str(ctx.exception))

    def test_gui_normalize_strips_container_then_resolve(self):
        """Same sequence as Local Backup upload handler."""
        storage_url = _normalize_blob_account_url_for_gui(ACCOUNT_WITH_CONTAINER)
        self.assertEqual(storage_url, ACCOUNT)
        container = _resolve_container_for_gui("managed_identity", "sqlbackups", storage_url)
        self.assertEqual(container, "sqlbackups")
        root = _parse_storage_account_url_for_upload(storage_url, container)
        self.assertEqual(root, ACCOUNT)


class TestGetToolBlobServiceClient(unittest.TestCase):
    @mock.patch("azure.storage.blob.BlobServiceClient")
    @mock.patch("utils.azure_shared_credential.get_shared_azure_credential")
    def test_managed_identity_does_not_require_container_in_url(
        self, mock_cred, mock_bsc
    ):
        mock_cred.return_value = object()
        mock_bsc.return_value = mock.Mock()

        _get_tool_blob_service_client(
            blob_auth_mode="managed_identity",
            blob_connection_string="",
            blob_account_url=ACCOUNT,
            container="sqlbackups",
            log=lambda _: None,
        )

        mock_bsc.assert_called_once()
        call_kw = mock_bsc.call_args.kwargs
        self.assertEqual(call_kw["account_url"], ACCOUNT)
        self.assertIs(call_kw["credential"], mock_cred.return_value)

    @mock.patch("azure.storage.blob.BlobServiceClient")
    @mock.patch("utils.azure_shared_credential.get_shared_azure_credential")
    def test_managed_identity_empty_container_still_raises(self, mock_cred, mock_bsc):
        with self.assertRaises(ValueError) as ctx:
            _get_tool_blob_service_client(
                blob_auth_mode="managed_identity",
                blob_connection_string="",
                blob_account_url=ACCOUNT,
                container="",
                log=lambda _: None,
            )
        self.assertIn("Container is required", str(ctx.exception))
        mock_bsc.assert_not_called()
        mock_cred.assert_not_called()


class TestUploadFileToBlob(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="amt_blob_up_")
        self.bak = Path(self._tmp) / "NICU_test.bak"
        self.bak.write_bytes(b"x" * 1024)

    def tearDown(self):
        import shutil

        shutil.rmtree(self._tmp, ignore_errors=True)

    @mock.patch("src.backup.local_backup_and_upload._get_tool_blob_service_client")
    def test_upload_uses_container_and_blob_path(self, mock_get_client):
        mock_blob = mock.Mock()
        mock_service = mock.Mock()
        mock_service.get_blob_client.return_value = mock_blob
        mock_service.url = ACCOUNT
        mock_get_client.return_value = mock_service

        url = _upload_file_to_blob(
            local_file=self.bak,
            blob_auth_mode="managed_identity",
            blob_connection_string="",
            blob_account_url=ACCOUNT,
            container="sqlbackups",
            blob_path="NICU_test/20260810_031718/NICU_test.bak",
            log=lambda _: None,
        )

        mock_get_client.assert_called_once()
        self.assertEqual(mock_get_client.call_args.kwargs["container"], "sqlbackups")
        mock_service.get_blob_client.assert_called_once_with(
            container="sqlbackups",
            blob="NICU_test/20260810_031718/NICU_test.bak",
        )
        mock_blob.upload_blob.assert_called_once()
        self.assertIn("sqlbackups", url)


class TestUploadExistingBakToBlob(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="amt_blob_up2_")
        self.bak = Path(self._tmp) / "NICU_ss_sld_db22u_20260810_031718.bak"
        self.bak.write_bytes(b"backup")

    def tearDown(self):
        import shutil

        shutil.rmtree(self._tmp, ignore_errors=True)

    @mock.patch("src.backup.local_backup_and_upload._upload_file_to_blob")
    def test_end_to_end_mi_with_separate_container(self, mock_upload):
        mock_upload.return_value = (
            f"{ACCOUNT}/sqlbackups/NICU_ss_sld_db22u/20260810_031718/db.bak"
        )
        logs = []

        result = upload_existing_bak_to_blob(
            local_file_path=str(self.bak),
            blob_auth_mode="managed_identity",
            blob_account_url=ACCOUNT,
            blob_container="sqlbackups",
            database="NICU_ss_sld_db22u",
            structured_blob_paths=True,
            log=logs.append,
        )

        self.assertTrue(result["success"], msg=result.get("message"))
        mock_upload.assert_called_once()
        kw = mock_upload.call_args.kwargs
        self.assertEqual(kw["container"], "sqlbackups")
        self.assertEqual(
            kw["blob_path"],
            "NICU_ss_sld_db22u/20260810_031718/NICU_ss_sld_db22u_20260810_031718.bak",
        )


if __name__ == "__main__":
    unittest.main()
