# Author: Satish Chauhan

"""Azure ARM sign-in refresh helpers (expired CLI token / MFA required)."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

ARM_SCOPE = "https://management.azure.com/.default"


def azure_auth_error_needs_refresh(message: str) -> bool:
    """True when the user should run az logout/login or in-app Refresh sign-in."""
    upper = (message or "").upper()
    markers = (
        "AADSTS50076",
        "INVALID_GRANT",
        "INTERACTIONREQUIRED",
        "INTERACTION_REQUIRED",
        "MULTI-FACTOR AUTHENTICATION",
        "MULTI FACTOR",
        "3399614476",
        "BASIC_ACTION",
        "TOKEN HAS EXPIRED",
        "REFRESH TOKEN",
        "CHAINEDTOKENCREDENTIAL FAILED",
        "STATUS_INTERACTIONREQUIRED",
        " 401 ",
        "UNAUTHORIZED",
        "ARM LIST GET",
    )
    return any(m in upper for m in markers)


def format_user_azure_error(raw: str, *, max_len: int = 320) -> str:
    if azure_auth_error_needs_refresh(raw):
        return (
            "Azure sign-in expired or MFA is required.\n\n"
            "Right-click the button → Load subscriptions or Refresh sign-in. Use Sign out when finished."
        )
    text = (raw or "Unknown Azure error.").strip()
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return text


def clear_all_azure_auth_caches() -> None:
    try:
        from azure_migration_tool.utils.azure_shared_credential import clear_shared_azure_credential
    except ImportError:
        try:
            from utils.azure_shared_credential import clear_shared_azure_credential
        except ImportError:
            clear_shared_azure_credential = None  # type: ignore[assignment,misc]
    if clear_shared_azure_credential is not None:
        clear_shared_azure_credential()
    try:
        from azure_migration_tool.src.utils.azcopy_utils import clear_azcopy_entra_session
    except ImportError:
        try:
            from src.utils.azcopy_utils import clear_azcopy_entra_session
        except ImportError:
            clear_azcopy_entra_session = None  # type: ignore[assignment,misc]
    if clear_azcopy_entra_session is not None:
        clear_azcopy_entra_session()


def refresh_azure_sign_in(log: Optional[Callable[[str], None]] = None) -> bool:
    """Sign out of Azure CLI and run interactive az login; clears in-app token caches."""
    clear_all_azure_auth_caches()
    try:
        from azure_migration_tool.src.utils.azcopy_utils import run_az_relogin
    except ImportError:
        from src.utils.azcopy_utils import run_az_relogin
    return bool(run_az_relogin(log))


def azure_cli_logged_in() -> bool:
    try:
        from azure_migration_tool.src.utils.azcopy_utils import azure_cli_logged_in as _logged_in
    except ImportError:
        from src.utils.azcopy_utils import azure_cli_logged_in as _logged_in
    try:
        return bool(_logged_in())
    except Exception:
        return False


def azure_auth_primary_button_label() -> str:
    return "Refresh sign-in" if azure_cli_logged_in() else "Sign in to Azure"


def azure_subscriptions_button_label() -> str:
    if azure_cli_logged_in():
        return "Sign out"
    return "Sign in to Azure"


def _get_shared_credential(log: Optional[Callable[[str], None]] = None) -> Any:
    try:
        from azure_migration_tool.utils.azure_shared_credential import get_shared_azure_credential
    except ImportError:
        from utils.azure_shared_credential import get_shared_azure_credential
    return get_shared_azure_credential(log)


def probe_arm_token(credential: Any) -> Optional[str]:
    """Return an error string if ARM token cannot be acquired; None if OK."""
    try:
        credential.get_token(ARM_SCOPE)
        return None
    except Exception as ex:
        return str(ex)


def ensure_arm_credential(
    log: Optional[Callable[[str], None]] = None,
    *,
    allow_refresh: bool = True,
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Return (credential, error). Signs in or refreshes when CLI session is missing or ARM token is stale.
    """
    _log = log or (lambda _m: None)

    if not azure_cli_logged_in():
        _log("Azure CLI is not signed in — opening sign-in…")
        if not sign_in_azure(_log, force=True):
            return None, "Azure sign-in did not complete."

    clear_all_azure_auth_caches()
    cred = _get_shared_credential(_log)
    err = probe_arm_token(cred)
    if err and allow_refresh and azure_auth_error_needs_refresh(err):
        _log("Refreshing Azure sign-in (complete MFA in the browser if prompted)…")
        if refresh_azure_sign_in(_log):
            clear_all_azure_auth_caches()
            cred = _get_shared_credential(_log)
            err = probe_arm_token(cred)
    if err:
        return None, err
    return cred, None


def fetch_arm_subscriptions(
    log: Optional[Callable[[str], None]] = None,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """Sign in / refresh as needed, then list subscriptions via ARM."""
    cred, err = ensure_arm_credential(log)
    if err or cred is None:
        return [], err or "Could not acquire Azure credentials."
    try:
        from azure_migration_tool.src.azure_mgmt.mi_pitr_restore import list_subscriptions
    except ImportError:
        from src.azure_mgmt.mi_pitr_restore import list_subscriptions
    rows, list_err = list_subscriptions(cred)
    if list_err and azure_auth_error_needs_refresh(list_err):
        if log:
            log("Subscription list failed — refreshing sign-in and retrying…")
        if refresh_azure_sign_in(log):
            clear_all_azure_auth_caches()
            cred, err = ensure_arm_credential(log, allow_refresh=False)
            if err or cred is None:
                return [], list_err
            rows, list_err = list_subscriptions(cred)
    return rows, list_err


def sign_out_azure(log: Optional[Callable[[str], None]] = None) -> bool:
    clear_all_azure_auth_caches()
    try:
        from azure_migration_tool.src.utils.azcopy_utils import run_az_logout
    except ImportError:
        from src.utils.azcopy_utils import run_az_logout
    return bool(run_az_logout(log))


def sign_in_azure(log: Optional[Callable[[str], None]] = None, *, force: bool = True) -> bool:
    clear_all_azure_auth_caches()
    try:
        from azure_migration_tool.src.utils.azcopy_utils import run_az_login
    except ImportError:
        from src.utils.azcopy_utils import run_az_login
    return bool(run_az_login(log, force=force))
