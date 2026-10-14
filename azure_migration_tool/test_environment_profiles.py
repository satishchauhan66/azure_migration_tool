# Author: Satish Chauhan

import unittest

from azure_migration_tool.gui.utils.environment_profiles import (
    detect_environment_from_hints,
    resolve_database_catalog,
    _text_matches_patterns,
)


class TestEnvironmentDetection(unittest.TestCase):
    def test_hostname_uat(self) -> None:
        r = detect_environment_from_hints(
            restore_server="myapp-uat-eus2-sqlmi-01.abc123def456.database.windows.net"
        )
        self.assertEqual(r["environment"], "uat")

    def test_db22q_qa(self) -> None:
        r = detect_environment_from_hints(database="SampleApp_ss_sld_db22q")
        self.assertEqual(r["environment"], "qa")

    def test_explicit_prod(self) -> None:
        r = detect_environment_from_hints(environment="prod", database="anything")
        self.assertEqual(r["environment"], "prod")
        self.assertEqual(r["confidence"], "explicit")

    def test_glob_pattern(self) -> None:
        self.assertTrue(_text_matches_patterns("SampleApp_ss_sld_db22q", ["*db22q*"]))

    def test_catalog_structure(self) -> None:
        c = resolve_database_catalog("SampleApp_ss_sld_db22q")
        self.assertEqual(c["environment"], "qa")
        self.assertIn("semantic", c)
        self.assertIn("restore_target", c)


if __name__ == "__main__":
    unittest.main()
