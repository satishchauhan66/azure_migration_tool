# Author: Satish Chauhan

"""One Azure Identity credential for ARM + Key Vault; avoids repeat browser prompts."""

from __future__ import annotations

import sys
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

_lock = threading.Lock()
_credential: Optional[Any] = None
_silent_subprocess_patched = False


def _apply_silent_windows_identity_subprocess() -> None:
    """Avoid visible console flashes when Azure CLI / PowerShell credentials spawn subprocesses (Windows)."""
    global _silent_subprocess_patched
    if _silent_subprocess_patched:
        return
    _silent_subprocess_patched = True
    if not sys.platform.startswith("win"):
        return
    import subprocess

    flag = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    if not flag:
        return
    try:
        from azure.identity._credentials import azure_cli as _ac

        _orig_co = _ac.subprocess.check_output

        def _check_output_silent(*args: Any, **kwargs: Any) -> Any:
            if kwargs.get("creationflags") is None:
                kwargs = dict(kwargs)
                kwargs["creationflags"] = flag
            return _orig_co(*args, **kwargs)

        _ac.subprocess.check_output = _check_output_silent  # type: ignore[method-assign]
    except Exception:
        pass
    try:
        from azure.identity._credentials import azure_powershell as _ap
        from azure.identity._credentials.azure_cli import get_safe_working_dir

        def _start_process_silent(args: Any) -> Any:
            import subprocess as sp

            proc = sp.Popen(
                args,
                cwd=get_safe_working_dir(),
                stdout=sp.PIPE,
                stderr=sp.PIPE,
                stdin=sp.DEVNULL,
                universal_newlines=True,
                creationflags=flag,
            )
            return proc

        _ap.start_process = _start_process_silent  # type: ignore[method-assign]
    except Exception:
        pass


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


class _CachingTokenCredential:
    """Reuse access tokens in-process so CLI / PowerShell subprocesses are not spawned on every ARM call."""

    __slots__ = ("_inner", "_lock", "_entries")

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._lock = threading.Lock()
        self._entries: Dict[Tuple[Tuple[str, ...], Optional[str]], Tuple[float, Any]] = {}

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        tenant_id = kwargs.get("tenant_id")
        key = (tuple(scopes), tenant_id if isinstance(tenant_id, str) else None)
        now = time.time()
        with self._lock:
            hit = self._entries.get(key)
            if hit is not None:
                expires_on, tok = hit
                if now < expires_on - 300:
                    return tok
            tok = self._inner.get_token(*scopes, **kwargs)
            exp = float(tok.expires_on)
            self._entries[key] = (exp, tok)
            return tok

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
    _apply_silent_windows_identity_subprocess()
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

    Tokens are also cached in-process (``_CachingTokenCredential``) so repeated ARM
    calls do not spawn ``az`` / PowerShell for every request. On Windows, subprocess
    creation uses ``CREATE_NO_WINDOW`` where supported so console windows do not flash.

    ``get_token`` is serialized with a lock to prevent overlapping interactive flows.
    """
    global _credential
    log = logger or (lambda _m: None)
    with _lock:
        if _credential is not None:
            return _credential
        try:
            inner = _CachingTokenCredential(_build_management_chain())
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
