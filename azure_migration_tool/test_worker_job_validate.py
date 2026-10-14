# Author: Satish Chauhan

"""Smoke tests for headless worker job validation (no database)."""

import json
import tempfile
import unittest
from pathlib import Path


class TestWorkerJobValidate(unittest.TestCase):
    def test_validate_example_job_structure(self) -> None:
        from azure_migration_tool.worker.__main__ import _load_job, _validate_job

        example = (
            Path(__file__).resolve().parent
            / "examples"
            / "jobs"
            / "bulk_local_blob_restore.example.json"
        )
        job = _load_job(example)
        issues = _validate_job(job)
        self.assertEqual(issues, [])

    def test_validate_missing_server(self) -> None:
        from azure_migration_tool.worker.__main__ import _validate_job

        job = {
            "steps": {"backup": True, "upload": False, "restore": False},
            "source": {"databases": ["x"]},
            "local_backup": {"path": "C:\\bak"},
        }
        issues = _validate_job(job)
        self.assertTrue(any("source.server" in i for i in issues))

    def test_upload_only_requires_backup_runs(self) -> None:
        from azure_migration_tool.worker.__main__ import _validate_job

        job = {
            "steps": {"backup": False, "upload": True, "restore": False},
            "blob": {"container": "c"},
            "backup_runs": [{"database": "db", "files": ["a.bak"], "run_id": "1"}],
        }
        issues = _validate_job(job)
        self.assertEqual(issues, [])


if __name__ == "__main__":
    unittest.main()
