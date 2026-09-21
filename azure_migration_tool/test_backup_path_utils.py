#!/usr/bin/env python
# Author: Satish Chauhan
"""Tests for backup path helpers (stripes, DB name, UNC normalization)."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

_PKG = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "backup_path_utils",
    _PKG / "src" / "backup" / "backup_path_utils.py",
)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)

normalize_backup_path = _mod.normalize_backup_path
discover_disk_stripe_set = _mod.discover_disk_stripe_set
infer_database_name_from_backup_path = _mod.infer_database_name_from_backup_path
build_structured_local_backup_dir = _mod.build_structured_local_backup_dir
format_backup_paths_for_ui = _mod.format_backup_paths_for_ui
format_run_folder_for_ui = _mod.format_run_folder_for_ui
discover_bak_files_in_run_folder = _mod.discover_bak_files_in_run_folder
resolve_upload_paths_from_state = _mod.resolve_upload_paths_from_state


class TestNormalizeBackupPath(unittest.TestCase):
    def test_double_slash_unc(self):
        raw = "//gpitd-shir01.us.pressganey.com/sqlbackups/MyDb_20260903_031135.bak"
        self.assertEqual(
            normalize_backup_path(raw),
            r"\\gpitd-shir01.us.pressganey.com\sqlbackups\MyDb_20260903_031135.bak",
        )


class TestInferDatabaseName(unittest.TestCase):
    def test_flat_filename(self):
        path = r"\\server\share\URG2K_ps_x_db22q_20260903_031135.bak"
        self.assertEqual(
            infer_database_name_from_backup_path(path),
            "URG2K_ps_x_db22q",
        )

    def test_structured_folder(self):
        path = r"D:\backups\MyDatabase\20260903_031135\MyDatabase_20260903_031135.bak"
        self.assertEqual(infer_database_name_from_backup_path(path), "MyDatabase")

    def test_striped_filename(self):
        path = r"\\server\share\MyDb_20260903_031135_part02of04.bak"
        self.assertEqual(infer_database_name_from_backup_path(path), "MyDb")


class TestDiscoverDiskStripeSet(unittest.TestCase):
    def test_single_file(self):
        paths = discover_disk_stripe_set(r"C:\backups\single.bak")
        self.assertEqual(paths, [r"C:\backups\single.bak"])

    def test_unc_stripes_without_local_access(self):
        primary = (
            r"//gpitd-shir01/sqlbackups/"
            r"URG2K_ps_x_db22q_20260903_031135_part01of03.bak"
        )
        paths = discover_disk_stripe_set(primary)
        self.assertEqual(len(paths), 3)
        self.assertTrue(all("part" in p and "of03.bak" in p for p in paths))
        self.assertIn("part01of03", paths[0])
        self.assertIn("part03of03", paths[2])

    def test_stripes_on_disk(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for i in range(1, 4):
                name = f"MyDb_20260903_031135_part{i:02d}of03.bak"
                (root / name).write_bytes(b"x")
            primary = str(root / "MyDb_20260903_031135_part02of03.bak")
            paths = discover_disk_stripe_set(primary)
            self.assertEqual(len(paths), 3)
            self.assertEqual(Path(paths[0]).name, "MyDb_20260903_031135_part01of03.bak")


class TestStructuredPaths(unittest.TestCase):
    def test_build_dir(self):
        out = build_structured_local_backup_dir(
            r"\\fileserver\sqlbackups", "My Db", "20260903_031135"
        )
        self.assertIn("My_Db", str(out))
        self.assertIn("20260903_031135", str(out))

    def test_format_for_ui(self):
        joined = format_backup_paths_for_ui([r"a\b1.bak", r"a\b2.bak"])
        self.assertEqual(joined, r"a\b1.bak; a\b2.bak")

    def test_format_run_folder(self):
        paths = [
            r"\\server\share\Db\20260918_011647\Db_20260918_011647_part01of02.bak",
            r"\\server\share\Db\20260918_011647\Db_20260918_011647_part02of02.bak",
        ]
        self.assertEqual(
            format_run_folder_for_ui(paths),
            r"\\server\share\Db\20260918_011647",
        )


class TestRunFolderDiscovery(unittest.TestCase):
    def test_discover_stripes_in_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "FUSION" / "20260918_011647"
            root.mkdir(parents=True)
            for i in range(1, 4):
                (root / f"FUSION_20260918_011647_part{i:02d}of03.bak").write_bytes(b"x")
            found = discover_bak_files_in_run_folder(str(root))
            self.assertEqual(len(found), 3)

    def test_resolve_from_folder_entry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "run"
            root.mkdir()
            (root / "single.bak").write_bytes(b"x")
            paths = resolve_upload_paths_from_state(entry_text=str(root))
            self.assertEqual(len(paths), 1)


if __name__ == "__main__":
    unittest.main()
