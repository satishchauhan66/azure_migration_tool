#!/usr/bin/env python
"""Tests for blob backup catalog discovery (structured upload paths)."""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

_PKG = Path(__file__).resolve().parent
sys.path.insert(0, str(_PKG))
sys.path.insert(0, str(_PKG.parent))

try:
    from src.backup.blob_backup_catalog import (
        backup_blobs_for_database,
        build_backup_list_display,
        database_folder_from_blob_path,
        discover_database_names,
    )
except ImportError:
    from azure_migration_tool.src.backup.blob_backup_catalog import (
        backup_blobs_for_database,
        build_backup_list_display,
        database_folder_from_blob_path,
        discover_database_names,
    )


def _blob(name: str, size: int = 1024):
    return SimpleNamespace(name=name, size=size)


class TestBlobBackupCatalog(unittest.TestCase):
    def test_structured_upload_path(self):
        path = "OUT2K_ss_sld_db22u/20260810_054126/OUT2K_ss_sld_db22u_20260810_054126.bak"
        self.assertEqual(database_folder_from_blob_path(path), "OUT2K_ss_sld_db22u")

    def test_structured_with_root_prefix(self):
        path = "archive/OUT2K_ss_sld_db22u/20260810_054126/OUT2K_ss_sld_db22u_20260810_054126.bak"
        self.assertEqual(database_folder_from_blob_path(path), "OUT2K_ss_sld_db22u")

    def test_discover_and_list_structured_backup(self):
        blobs = [
            _blob("OUT2K_ss_sld_db22u/20260810_054126/OUT2K_ss_sld_db22u_20260810_054126.bak", 50 * 1024 * 1024),
            _blob("other/readme.txt"),
        ]
        names = discover_database_names(blobs)
        self.assertEqual(names, ["OUT2K_ss_sld_db22u"])

        matched = backup_blobs_for_database(blobs, "OUT2K_ss_sld_db22u")
        self.assertEqual(len(matched), 1)
        labels, path_map = build_backup_list_display(matched)
        self.assertEqual(len(labels), 1)
        self.assertIn("OUT2K_ss_sld_db22u/20260810_054126/", labels[0])
        self.assertTrue(path_map[labels[0]].endswith(".bak"))


if __name__ == "__main__":
    unittest.main()
