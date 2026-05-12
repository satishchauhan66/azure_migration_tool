#!/usr/bin/env python
# Author: Satish Chauhan
"""Unit tests for local backup path normalization and folder prepare (no SQL / no real UNC required)."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

# Package root (directory containing this file)
_PKG = Path(__file__).resolve().parent
sys.path.insert(0, str(_PKG))
sys.path.insert(0, str(_PKG.parent))

try:
    from src.backup.local_backup_and_upload import (
        normalize_backup_input_path,
        ensure_dir_and_probe_write,
    )
except ImportError:
    from azure_migration_tool.src.backup.local_backup_and_upload import (
        normalize_backup_input_path,
        ensure_dir_and_probe_write,
    )


class TestNormalizeBackupInputPath(unittest.TestCase):
    def test_strips_quotes_and_whitespace(self):
        self.assertEqual(
            normalize_backup_input_path('  "D:/backups/db"  '),
            "D:\\backups\\db",
        )

    def test_forward_slash_to_backslash(self):
        self.assertEqual(
            normalize_backup_input_path("//server/share/folder"),
            "\\\\server\\share\\folder",
        )


class TestEnsureDirAndProbeWrite(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="amt_local_bak_")

    def tearDown(self):
        import shutil

        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_creates_nested_folder_and_probe(self):
        nested = os.path.join(self._tmp, "a", "b", "c")
        r = ensure_dir_and_probe_write(nested, apply_icacls_everyone=False, log=lambda _: None)
        self.assertTrue(r["success"], msg=r.get("message"))
        self.assertTrue(os.path.isdir(nested))
        self.assertEqual(
            os.path.normpath(r["resolved_directory"]),
            os.path.normpath(nested),
        )

    def test_explicit_bak_uses_parent(self):
        bak_path = os.path.join(self._tmp, "deep", "sub", "MyDb.bak").replace("\\", "/")
        r = ensure_dir_and_probe_write(bak_path, apply_icacls_everyone=False, log=lambda _: None)
        self.assertTrue(r["success"], msg=r.get("message"))
        parent = os.path.join(self._tmp, "deep", "sub")
        self.assertTrue(os.path.isdir(parent))
        self.assertTrue(r["explicit_bak_file"])


if __name__ == "__main__":
    unittest.main()
