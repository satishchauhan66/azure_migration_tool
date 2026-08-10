#!/usr/bin/env python
"""Tests for blob download path mapping and restore multi-disk SQL."""

import sys
import tempfile
import unittest
from pathlib import Path

_PKG = Path(__file__).resolve().parent
sys.path.insert(0, str(_PKG))
sys.path.insert(0, str(_PKG.parent))

try:
    from src.restore.download_from_blob import _resolve_local_download_paths
except ImportError:
    from azure_migration_tool.src.restore.download_from_blob import _resolve_local_download_paths


class TestResolveLocalDownloadPaths(unittest.TestCase):
    def test_folder_destination_keeps_blob_filenames(self):
        folder = tempfile.mkdtemp(prefix="amt_dl_")
        paths = _resolve_local_download_paths(
            folder,
            [
                "MyDb/20260810_031718/MyDb_20260810_031718_part01of02.bak",
                "MyDb/20260810_031718/MyDb_20260810_031718_part02of02.bak",
            ],
        )
        self.assertEqual(len(paths), 2)
        self.assertTrue(paths[0].endswith("_part01of02.bak"))
        self.assertTrue(paths[1].endswith("_part02of02.bak"))
        self.assertEqual(Path(paths[0]).parent, Path(paths[1]).parent)

    def test_single_file_explicit_bak_path(self):
        target = str(Path(tempfile.mkdtemp()) / "restore_me.bak")
        paths = _resolve_local_download_paths(
            target,
            ["MyDb/20260810_031718/MyDb_20260810_031718.bak"],
        )
        self.assertEqual(paths, [target])


if __name__ == "__main__":
    unittest.main()
