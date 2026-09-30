"""Tests for GitHub release version parsing."""

from src.utils.app_update import (
    UpdateInfo,
    normalize_release_version,
    should_prompt_for_update,
    version_tuple,
)


def test_version_tuple_ordering() -> None:
    assert version_tuple("1.0.0") < version_tuple("1.0.1")
    assert version_tuple("1.5.0") < version_tuple("1.5.99")
    assert version_tuple("build/2.0.10") > version_tuple("1.9.9")
    assert version_tuple("v2.1.0 (main)") == version_tuple("2.1.0")


def test_normalize_release_version() -> None:
    assert normalize_release_version("build/1.2.456") == "1.2.456"
    assert normalize_release_version("v1.0.0") == "1.0.0"


def test_should_prompt_respects_dismissed(tmp_path, monkeypatch) -> None:
    dismissed = tmp_path / "dismissed_update_version.txt"
    monkeypatch.setattr(
        "src.utils.app_update.dismissed_update_version_path",
        lambda: str(dismissed),
    )
    info = UpdateInfo(
        current_version="1.0.0",
        latest_version="2.0.0",
        release_page_url="https://example.com",
        portable_download_url=None,
        setup_download_url=None,
        release_notes="",
    )
    assert should_prompt_for_update(info) is True
    dismissed.write_text("2.0.0", encoding="utf-8")
    assert should_prompt_for_update(info) is False
