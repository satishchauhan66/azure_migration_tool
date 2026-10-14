# Author: Satish Chauhan

import unittest
from pathlib import Path

from azure_migration_tool.src.orchestration.playbooks import (
    _substitute,
    bundled_playbooks_dir,
    load_playbook,
    run_playbook,
)


class TestPlaybooks(unittest.TestCase):
    def test_qa_pilot_dry_run(self) -> None:
        path = bundled_playbooks_dir() / "qa-pilot-pipeline.json"
        pb = load_playbook(path)
        out = run_playbook(pb, dry_run=True)
        self.assertTrue(out["dry_run"])
        self.assertEqual(out["trace"][0]["status"], "ok")
        self.assertIn("env", out["steps"])

    def test_substitute_vars(self) -> None:
        ctx = {"vars": {"database": "MyDb"}, "steps": {"env": {"environment": "qa"}}}
        job = _substitute(
            {
                "environment": "{{steps.env.environment}}",
                "source": {"databases": ["{{vars.database}}"]},
            },
            ctx,
        )
        self.assertEqual(job["environment"], "qa")
        self.assertEqual(job["source"]["databases"], ["MyDb"])


if __name__ == "__main__":
    unittest.main()
