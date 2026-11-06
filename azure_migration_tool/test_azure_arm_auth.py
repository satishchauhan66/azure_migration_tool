# Author: Satish Chauhan

from unittest import TestCase

from azure_migration_tool.gui.utils.azure_arm_auth import (
    azure_auth_error_needs_refresh,
    format_user_azure_error,
)


class TestAzureArmAuth(TestCase):
    def test_detects_mfa_required(self) -> None:
        msg = "AADSTS50076 multi-factor authentication required invalid_grant"
        self.assertTrue(azure_auth_error_needs_refresh(msg))

    def test_user_message_short_for_refresh(self) -> None:
        text = format_user_azure_error("AADSTS50076")
        self.assertIn("Refresh sign-in", text)
