# Author: Satish Chauhan

"""Tests for CSV/Excel worker job import."""

import tempfile
import unittest
from pathlib import Path


class TestJobSpreadsheet(unittest.TestCase):
    def test_csv_single_database(self) -> None:
        from azure_migration_tool.src.orchestration.job_spreadsheet import read_job_spreadsheet

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "job.csv"
            p.write_text(
                "job_id,source_server,database,restore_user\n"
                "j1,SRV\\INST,MyDb,user@example.com\n",
                encoding="utf-8",
            )
            jobs = read_job_spreadsheet(p)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["job_id"], "j1")
            self.assertEqual(jobs[0]["source"]["databases"], ["MyDb"])
            self.assertEqual(jobs[0]["restore_target"]["user"], "user@example.com")

    def test_group_rows_by_job_id(self) -> None:
        from azure_migration_tool.src.orchestration.job_spreadsheet import read_job_spreadsheet

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "job.csv"
            p.write_text(
                "job_id,source_server,database\n"
                "same,SRV\\INST,Db1\n"
                "same,SRV\\INST,Db2\n",
                encoding="utf-8",
            )
            jobs = read_job_spreadsheet(p)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(sorted(jobs[0]["source"]["databases"]), ["Db1", "Db2"])


if __name__ == "__main__":
    unittest.main()
