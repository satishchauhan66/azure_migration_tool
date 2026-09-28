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


try:
    from src.restore.restore_from_blob import (
        _filter_restore_rows_for_database,
        _pick_best_restore_request_row,
        _snapshot_from_targeted_blob_status_row,
    )
except ImportError:
    from azure_migration_tool.src.restore.restore_from_blob import (
        _filter_restore_rows_for_database,
        _pick_best_restore_request_row,
        _snapshot_from_targeted_blob_status_row,
    )


def _dmv_row(
    session_id: int,
    percent: float,
    *,
    db_ctx: str = "",
    elapsed_ms: int = 0,
    wait_type: str = "",
    sql_text: str = "",
):
    return (
        session_id,
        "RESTORE DATABASE",
        percent,
        "suspended",
        wait_type,
        0.0,
        db_ctx,
        elapsed_ms,
        sql_text,
    )


class TestTargetedBlobStatusRow(unittest.TestCase):
    def test_stale_online_before_restore_is_not_one_hundred_percent(self):
        row = (
            "testdb11",
            100.0,
            "ONLINE",
            "3 - Complete and Ready",
            None,
            None,
            None,
        )
        snap = _snapshot_from_targeted_blob_status_row("testdb11", row)
        self.assertIsNone(snap.percent_complete)
        self.assertEqual(snap.progress_for_bar(), 0.0)
        self.assertTrue(snap.is_misleading_complete_reading())

    def test_online_with_active_dmv_sessions_is_misleading(self):
        row = (
            "testdb11",
            100.0,
            "ONLINE",
            "3 - Complete and Ready",
            None,
            None,
            None,
        )
        snap = _snapshot_from_targeted_blob_status_row("testdb11", row)
        snap.blob_restore_session_count = 2
        self.assertTrue(snap.is_misleading_complete_reading())

    def test_maps_worker_percent_and_phase(self):
        row = (
            "testsbdelete1",
            3.267771,
            "RESTORING",
            "1 - Moving Data (Restore Active)",
            "suspended",
            "BACKUPTHREAD",
            132,
        )
        snap = _snapshot_from_targeted_blob_status_row("testsbdelete1", row)
        self.assertEqual(snap.status_source, "blob_url_worker")
        self.assertAlmostEqual(snap.percent_complete or 0, 3.267771)
        self.assertEqual(snap.session_id, 132)
        self.assertIn("Moving Data", snap.overall_phase)


class TestPickBestRestoreRequestRow(unittest.TestCase):
    def test_prefers_highest_percent_over_stale_zero(self):
        rows = [
            _dmv_row(117, 0.0, elapsed_ms=100),
            _dmv_row(88, 42.5, elapsed_ms=50),
        ]
        best = _pick_best_restore_request_row(rows, "MyDb", preferred_spid=117)
        self.assertIsNotNone(best)
        self.assertEqual(int(best[0]), 88)
        self.assertEqual(float(best[2]), 42.5)

    def test_odbc_spid_does_not_win_over_worker_percent(self):
        rows = [
            _dmv_row(124, 0.0, wait_type="SLEEP_TASK"),
            _dmv_row(132, 3.267771, wait_type="BACKUPTHREAD"),
        ]
        best = _pick_best_restore_request_row(rows, "MyDb", preferred_spid=124)
        self.assertEqual(int(best[0]), 132)
        self.assertAlmostEqual(float(best[2]), 3.267771)

    def test_prefers_longer_elapsed_on_percent_tie(self):
        rows = [
            _dmv_row(117, 10.0, elapsed_ms=100),
            _dmv_row(88, 10.0, elapsed_ms=500),
        ]
        best = _pick_best_restore_request_row(rows, "MyDb", preferred_spid=117)
        self.assertEqual(int(best[0]), 88)

    def test_concurrent_restores_match_by_restore_sql(self):
        rows = [
            _dmv_row(
                124,
                0.0,
                wait_type="SLEEP_TASK",
                sql_text="RESTORE DATABASE [DbA] FROM URL = N'https://x/a.bak'",
            ),
            _dmv_row(
                132,
                55.0,
                wait_type="BACKUPTHREAD",
                sql_text="RESTORE DATABASE [DbB] FROM URL = N'https://x/b.bak'",
            ),
            _dmv_row(
                140,
                2.0,
                wait_type="BACKUPTHREAD",
                sql_text="RESTORE DATABASE DbA FROM URL = N'https://x/a2.bak'",
            ),
        ]
        scoped, matched = _filter_restore_rows_for_database(rows, "DbA")
        self.assertTrue(matched)
        self.assertEqual({int(r[0]) for r in scoped}, {124, 140})
        best = _pick_best_restore_request_row(rows, "DbA")
        self.assertEqual(int(best[0]), 140)
        self.assertEqual(float(best[2]), 2.0)


if __name__ == "__main__":
    unittest.main()
