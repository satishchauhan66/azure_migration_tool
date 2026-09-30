"""Tests for SQL Server host sanitize/validate helpers."""

from src.utils.sql_server_name import sanitize_sql_server_name, validate_sql_server_name


def test_sanitize_trims_and_strips_port() -> None:
    assert sanitize_sql_server_name("  myhost,1433;  ") == "myhost"
    assert sanitize_sql_server_name("tcp:sql01.contoso.com") == "sql01.contoso.com"


def test_validate_allows_fqdn_com() -> None:
    assert (
        validate_sql_server_name("gpitd-shir01.us.pressganey.com")
        == "gpitd-shir01.us.pressganey.com"
    )


def test_validate_rejects_email_domain() -> None:
    try:
        validate_sql_server_name("contoso.com")
        assert False, "expected ValueError"
    except ValueError:
        pass
