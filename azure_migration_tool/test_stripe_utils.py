#!/usr/bin/env python
# Author: Satish Chauhan
"""Tests for backup stripe auto-sizing."""

import importlib.util
import sys
import unittest
from pathlib import Path

_PKG = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "stripe_utils",
    _PKG / "src" / "backup" / "stripe_utils.py",
)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)

recommend_backup_stripes = _mod.recommend_backup_stripes
format_stripe_hint = _mod.format_stripe_hint


class TestRecommendBackupStripes(unittest.TestCase):
    def test_small_db_one_stripe(self):
        self.assertEqual(recommend_backup_stripes(3 * 1024), 1)

    def test_medium_db(self):
        # 400 GB / 10 GB -> 40 -> next power of 2 = 64
        self.assertEqual(recommend_backup_stripes(400 * 1024), 64)

    def test_100gb_db(self):
        # 100 GB / 10 GB -> 10 -> next power of 2 = 16
        self.assertEqual(recommend_backup_stripes(100 * 1024), 16)

    def test_50gb_db(self):
        # 50 GB / 10 GB -> 5 -> next power of 2 = 8
        self.assertEqual(recommend_backup_stripes(50 * 1024), 8)

    def test_7tb_db(self):
        size_mb = 7 * 1024 * 1024
        self.assertEqual(recommend_backup_stripes(size_mb), 64)

    def test_unknown_size(self):
        self.assertEqual(recommend_backup_stripes(None), 1)

    def test_hint_tb(self):
        hint = format_stripe_hint(7 * 1024 * 1024, 64)
        self.assertIn("TB", hint)
        self.assertIn("64", hint)
        self.assertIn("10", hint)


if __name__ == "__main__":
    unittest.main()
