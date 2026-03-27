# Author: Satish Chauhan

"""One Azure Identity credential for ARM + Key Vault; avoids repeat browser prompts."""

from __future__ import annotations

import threading
from typing import Any, Callable, Optional

_lock = threading.Lock()
_credential: Optional[Any] = None


class _LockedCredential:
    """Serialize get_token so concurrent ARM calls do not race MSAL / interactive login."""

    __slots__ = ("_inner", "_token_lock")

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._token_lock = threading.Lock()

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        with self._token_lock:
            return self._inner.get_token(*scopes, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _build_interactive_browser_credential() -> Any:
    from azure.identity import InteractiveBrowserCredential

    try:
        from azure.identity import TokenCachePersistenceOptions

        opts = TokenCachePersistenceOptions(name="azure_migration_tool")
        try:
            return InteractiveBrowserCredential(
                cache_persistence_options=opts,
                additionally_allowed_tenants=["*"],
            )
        except TypeError:
            return InteractiveBrowserCredential(cache_persistence_options=opts)
    except (ImportError, TypeError):
        try:
            return InteractiveBrowserCredential(additionally_allowed_tenants=["*"])
        except TypeError:
            return InteractiveBrowserCredential()


def _build_management_chain() -> Any:
    """Prefer non-interactive sources (CLI / PowerShell), then browser with persistent cache."""
    from azure.identity import AzureCliCredential, ChainedTokenCredential

    links: list = [AzureCliCredential()]
    try:
        from azure.identity import AzurePowerShellCredential

        links.append(AzurePowerShellCredential())
    except ImportError:
        pass
    links.append(_build_interactive_browser_credential())
    return ChainedTokenCredential(*links)


def get_shared_azure_credential(
    logger: Optional[Callable[[str], None]] = None,
) -> Any:
    """Return a single process-wide credential for management plane + Key Vault.

    Uses ``ChainedTokenCredential``: ``AzureCliCredential`` (``az login``) and
    ``AzurePowerShellCredential`` when available, then ``InteractiveBrowserCredential``
    with a persistent token cache. That way a second ADF trigger in the same session
    reuses tokens instead of opening the browser again, and developers who use
    ``az login`` avoid in-app browser entirely.

    ``get_token`` is serialized with a lock to prevent overlapping interactive flows.
    """
    global _credential
    log = logger or (lambda _m: None)
    with _lock:
        if _credential is not None:
            return _credential
        try:
            inner = _build_management_chain()
            _credential = _LockedCredential(inner)
            log(
                "Azure sign-in: shared chain (Azure CLI / PowerShell if available, "
                "else browser with token cache) for Key Vault + Data Factory."
            )
        except Exception as exc:  # pragma: no cover - defensive
            from azure.identity import DefaultAzureCredential

            _credential = _LockedCredential(DefaultAzureCredential())
            log(f"Azure sign-in: DefaultAzureCredential ({exc})")
        return _credential
