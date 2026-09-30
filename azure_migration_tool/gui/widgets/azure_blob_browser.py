# Author: Satish Chauhan

"""
Azure Blob Browser dialog.

Lets the user pick Subscription -> Storage Account -> Container interactively
(similar to the SSMS "Connect to a Microsoft Subscription" dialog) and applies
the selection back to the Backup & Restore tab via a callback.

Type in each picker to filter — no separate Search boxes.

Two output modes:
  * managed_identity: fills storage URL (with container) and container name.
  * connection_string: fetches the storage account key and builds a full
    connection string.

Required packages (install in the same interpreter that runs the app):
  pip install azure-identity azure-mgmt-subscription azure-mgmt-storage
"""

from __future__ import annotations

import sys
import threading
import tkinter as tk
from collections.abc import Mapping
from tkinter import messagebox, ttk
from typing import Any, Callable, List, Optional, Tuple

try:
    from gui.widgets.searchable_picker import SearchablePicker
except ImportError:
    from azure_migration_tool.gui.widgets.searchable_picker import SearchablePicker


def _first_account_key(list_keys_result: Any) -> str:
    """Return the first storage account key value from a StorageAccounts.list_keys() result."""
    if list_keys_result is None:
        return ""
    if isinstance(list_keys_result, Mapping):
        key_items = list_keys_result.get("keys")
    else:
        key_items = getattr(list_keys_result, "keys", None)
    if not key_items:
        return ""
    first = key_items[0]
    if isinstance(first, Mapping):
        return first.get("value") or ""
    return getattr(first, "value", "") or ""


def azure_browse_dependency_error() -> Optional[str]:
    """
    Return a user-facing error message if Browse Azure dependencies are missing.
    """
    missing: List[str] = []
    try:
        import azure.identity  # noqa: F401
    except ImportError:
        missing.append("azure-identity")
    try:
        import azure.mgmt.subscription  # noqa: F401
    except ImportError:
        missing.append("azure-mgmt-subscription")
    try:
        import azure.mgmt.storage  # noqa: F401
    except ImportError:
        missing.append("azure-mgmt-storage")
    if not missing:
        return None
    exe = sys.executable or "python"
    pkgs = " ".join(missing)
    return (
        "Browse Azure needs packages that are not installed in this app's Python environment:\n\n"
        f"  {', '.join(missing)}\n\n"
        "Install them with:\n"
        f'  "{exe}" -m pip install {pkgs}\n\n'
        "Or install all app dependencies from the repository:\n"
        "  pip install -r azure_migration_tool/requirements.txt"
    )


_CACHED_CREDENTIAL: Optional[Any] = None
_CACHED_USER_INFO: Optional[str] = None


def _get_or_create_credential() -> Tuple[Any, str]:
    """Get cached credential or create a new one."""
    global _CACHED_CREDENTIAL, _CACHED_USER_INFO

    if _CACHED_CREDENTIAL is not None and _CACHED_USER_INFO is not None:
        return (_CACHED_CREDENTIAL, _CACHED_USER_INFO)

    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential(
        exclude_managed_identity_credential=True,
        exclude_interactive_browser_credential=False,
    )

    user_info = _extract_user_info(credential)
    _CACHED_CREDENTIAL = credential
    _CACHED_USER_INFO = user_info

    return (credential, user_info)


def _extract_user_info(credential: Any) -> str:
    """Extract user display name from credential token."""
    try:
        token = credential.get_token("https://management.azure.com/.default")
        payload = token.token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        import base64
        import json

        data = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
        return data.get("upn") or data.get("preferred_username") or data.get("unique_name") or "(signed in)"
    except Exception:
        return "(signed in)"


def clear_azure_credential_cache() -> None:
    """Clear cached Azure credential to force re-authentication next time."""
    global _CACHED_CREDENTIAL, _CACHED_USER_INFO
    _CACHED_CREDENTIAL = None
    _CACHED_USER_INFO = None


class AzureBlobBrowser(tk.Toplevel):
    """Modal dialog: browse Azure Subscriptions / Storage Accounts / Containers."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        on_apply: Callable[..., None],
        default_mode: str = "managed_identity",
        mi_allowed: bool = True,
    ) -> None:
        super().__init__(parent)
        self.title("Browse Azure Storage")
        self.transient(parent)
        self.grab_set()
        self.resizable(True, False)
        self.geometry("640x420")

        self._on_apply = on_apply
        self._mi_allowed = mi_allowed
        self._credential: Any = None
        self._subs: List[Tuple[str, str]] = []
        self._accounts: List[Tuple[str, str, str, str, str]] = []
        self._containers: List[str] = []
        self._selected_account: Optional[Tuple[str, str, str, str, str]] = None
        self._sub_choices: Tuple[str, ...] = ()
        self._acct_choices: Tuple[str, ...] = ()
        self._cont_choices: Tuple[str, ...] = ()

        self._build_ui(default_mode)
        self.after(100, self._kick_off_login)

    def _build_ui(self, default_mode: str) -> None:
        self.signed_in_var = tk.StringVar(value="Connecting to Azure...")
        auth_row = ttk.Frame(self)
        auth_row.pack(fill=tk.X, padx=12, pady=(12, 6))
        tk.Label(auth_row, textvariable=self.signed_in_var, anchor=tk.W).pack(
            side=tk.LEFT, fill=tk.X, expand=True
        )
        self._signin_btn = ttk.Button(
            auth_row, text="Sign in again", width=12, command=self._on_sign_in_clicked
        )
        self._signin_btn.pack(side=tk.RIGHT, padx=(4, 0))
        self._signout_btn = ttk.Button(
            auth_row, text="Sign out", width=10, command=self._on_sign_out_clicked
        )
        self._signout_btn.pack(side=tk.RIGHT, padx=(4, 0))
        self._auth_busy = False

        row = ttk.Frame(self)
        row.pack(fill=tk.X, padx=12, pady=4)
        tk.Label(row, text="Subscription:", width=18, anchor=tk.W).pack(side=tk.LEFT)
        self.sub_picker = SearchablePicker(
            row,
            width_chars=52,
            get_choices=lambda: self._sub_choices,
        )
        self.sub_picker.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.sub_picker.bind("<<ComboboxSelected>>", self._on_sub_change)

        row = ttk.Frame(self)
        row.pack(fill=tk.X, padx=12, pady=4)
        tk.Label(row, text="Storage Account:", width=18, anchor=tk.W).pack(side=tk.LEFT)
        self.acct_picker = SearchablePicker(
            row,
            width_chars=52,
            get_choices=lambda: self._acct_choices,
        )
        self.acct_picker.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.acct_picker.bind("<<ComboboxSelected>>", self._on_acct_change)

        row = ttk.Frame(self)
        row.pack(fill=tk.X, padx=12, pady=4)
        tk.Label(row, text="Blob Container:", width=18, anchor=tk.W).pack(side=tk.LEFT)
        self.cont_picker = SearchablePicker(
            row,
            width_chars=52,
            get_choices=lambda: self._cont_choices,
        )
        self.cont_picker.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.cont_picker.bind("<<ComboboxSelected>>", lambda *_: self._on_cont_change())

        row = ttk.Frame(self)
        row.pack(fill=tk.X, padx=12, pady=(10, 2))
        tk.Label(row, text="Apply as:", width=18, anchor=tk.W).pack(side=tk.LEFT)
        self.mode_var = tk.StringVar(value=default_mode)
        self.rb_mi = ttk.Radiobutton(
            row,
            text="Managed Identity (recommended for SQL 2022+)",
            variable=self.mode_var,
            value="managed_identity",
        )
        self.rb_mi.pack(side=tk.LEFT, padx=(0, 12))
        self.rb_conn = ttk.Radiobutton(
            row,
            text="Connection String (account key)",
            variable=self.mode_var,
            value="connection_string",
        )
        self.rb_conn.pack(side=tk.LEFT)

        if not self._mi_allowed:
            self.rb_mi.configure(state="disabled")
            self.mode_var.set("connection_string")
            hint_row = ttk.Frame(self)
            hint_row.pack(fill=tk.X, padx=12, pady=(0, 4))
            tk.Label(hint_row, text="", width=18).pack(side=tk.LEFT)
            tk.Label(
                hint_row,
                text="(Managed Identity disabled: SQL Server version < 2022)",
                fg="gray",
                font=("Segoe UI", 8),
            ).pack(side=tk.LEFT)

        self.status_var = tk.StringVar(value="")
        tk.Label(
            self,
            textvariable=self.status_var,
            fg="gray",
            anchor=tk.W,
            wraplength=600,
            justify=tk.LEFT,
        ).pack(fill=tk.X, padx=12, pady=(8, 4))

        btns = ttk.Frame(self)
        btns.pack(fill=tk.X, padx=12, pady=10, side=tk.BOTTOM)
        self.ok_btn = ttk.Button(btns, text="OK", command=self._apply, width=10)
        self.ok_btn.pack(side=tk.RIGHT, padx=4)
        ttk.Button(btns, text="Cancel", command=self.destroy, width=10).pack(side=tk.RIGHT)
        self.ok_btn.config(state=tk.DISABLED)

    def _set_status(self, msg: str) -> None:
        self.status_var.set(msg or "")

    def _set_auth_buttons_enabled(self, enabled: bool) -> None:
        state = tk.NORMAL if enabled else tk.DISABLED
        try:
            self._signin_btn.configure(state=state)
            self._signout_btn.configure(state=state)
        except tk.TclError:
            pass

    def _reset_pickers(self) -> None:
        self._subs = []
        self._accounts = []
        self._containers = []
        self._selected_account = None
        self._sub_choices = ()
        self._acct_choices = ()
        self._cont_choices = ()
        self.sub_picker.set("")
        self.acct_picker.set("")
        self.cont_picker.set("")
        self.sub_picker.refresh_suggestions()
        self.acct_picker.refresh_suggestions()
        self.cont_picker.refresh_suggestions()
        self.ok_btn.config(state=tk.DISABLED)

    def _on_sign_out_clicked(self) -> None:
        if self._auth_busy:
            return
        self._auth_busy = True
        self._set_auth_buttons_enabled(False)
        self.signed_in_var.set("Signing out...")
        self._set_status("")

        def run() -> None:
            try:
                try:
                    from src.utils.azcopy_utils import run_az_logout
                except ImportError:
                    from azure_migration_tool.src.utils.azcopy_utils import run_az_logout
                clear_azure_credential_cache()
                self._credential = None
                ok = run_az_logout()
            except Exception as exc:
                ok = False
                err = str(exc)
            else:
                err = ""

            def finish() -> None:
                self._auth_busy = False
                self._set_auth_buttons_enabled(True)
                self._reset_pickers()
                if ok:
                    self.signed_in_var.set("Signed out")
                    self._set_status("Sign in to load subscriptions.")
                else:
                    self.signed_in_var.set("Sign-out may have failed")
                    if err:
                        self._set_status(err[:400])

            self.after(0, finish)

        threading.Thread(target=run, daemon=True).start()

    def _on_sign_in_clicked(self) -> None:
        if self._auth_busy:
            return
        self._auth_busy = True
        self._set_auth_buttons_enabled(False)
        self.signed_in_var.set("Signing in...")
        self._set_status("Complete sign-in in the browser window if one opens.")

        def run() -> None:
            try:
                try:
                    from src.utils.azcopy_utils import run_az_relogin
                except ImportError:
                    from azure_migration_tool.src.utils.azcopy_utils import run_az_relogin
                clear_azure_credential_cache()
                self._credential = None
                ok = run_az_relogin()
            except Exception as exc:
                ok = False
                err = str(exc)
            else:
                err = ""

            def after_login() -> None:
                self._auth_busy = False
                self._set_auth_buttons_enabled(True)
                if not ok:
                    self.signed_in_var.set("Sign-in failed")
                    self._set_status(err[:400] if err else "Try Sign out, then Sign in again.")
                    return
                self._kick_off_login()

            self.after(0, after_login)

        threading.Thread(target=run, daemon=True).start()

    def _kick_off_login(self) -> None:
        self._set_auth_buttons_enabled(False)
        threading.Thread(target=self._login_and_load_subs, daemon=True).start()

    def _login_and_load_subs(self) -> None:
        err = azure_browse_dependency_error()
        if err:
            self.after(0, lambda m=err: self._fatal_missing_deps(m))
            return
        try:
            self._credential, user = _get_or_create_credential()
            from azure.mgmt.subscription import SubscriptionClient
        except ImportError as e:
            self.after(
                0,
                lambda msg=str(e): self._fatal_missing_deps(
                    f"Unexpected import error after dependency check: {msg}\n\n"
                    "Reinstall: pip install azure-identity azure-mgmt-subscription azure-mgmt-storage"
                ),
            )
            return
        try:
            client = SubscriptionClient(self._credential)
            subs = list(client.subscriptions.list())
            self._subs = [
                (f"{s.display_name} ({s.subscription_id})", s.subscription_id) for s in subs
            ]
            self.after(0, lambda: self._populate_subs(user))
        except Exception as e:
            self.after(0, lambda msg=str(e): self._on_list_subscriptions_failed(msg))

    def _fatal_missing_deps(self, msg: str) -> None:
        self.signed_in_var.set("Missing dependencies")
        self._set_auth_buttons_enabled(True)
        messagebox.showerror("Browse Azure", msg)
        self.destroy()

    def _on_list_subscriptions_failed(self, msg: str) -> None:
        clear_azure_credential_cache()
        self._credential = None
        self.signed_in_var.set("Could not load subscriptions")
        self._set_status("Use Sign out, then Sign in (MFA) and retry.")
        self._set_auth_buttons_enabled(True)
        short = (msg or "").strip()
        if len(short) > 600:
            short = short[:600] + "..."
        messagebox.showerror(
            "Browse Azure",
            f"Could not list subscriptions:\n\n{short}\n\n"
            "Click Sign out, then Sign in to refresh your Azure session.",
        )

    def _populate_subs(self, user: str) -> None:
        self._set_auth_buttons_enabled(True)
        self.signed_in_var.set(f"Signed in as: {user}")
        if not self._subs:
            self._set_status("No subscriptions visible to this user.")
            return
        self._sub_choices = tuple(d[0] for d in self._subs)
        self.sub_picker.set("")
        self.sub_picker.refresh_suggestions()
        self._set_status(f"{len(self._sub_choices)} subscription(s).")

    def _on_sub_change(self, *_: Any) -> None:
        selected_display = (self.sub_picker.get() or "").strip()
        if not selected_display:
            return
        sub_id = None
        for sub in self._subs:
            if sub[0] == selected_display:
                sub_id = sub[1]
                break
        if sub_id is None:
            return
        self._acct_choices = ()
        self._cont_choices = ()
        self.acct_picker.set("")
        self.cont_picker.set("")
        self.acct_picker.refresh_suggestions()
        self.cont_picker.refresh_suggestions()
        self.ok_btn.config(state=tk.DISABLED)
        self._set_status("Loading storage accounts...")
        threading.Thread(target=self._load_accounts, args=(sub_id,), daemon=True).start()

    def _load_accounts(self, sub_id: str) -> None:
        try:
            from azure.mgmt.storage import StorageManagementClient

            sm = StorageManagementClient(self._credential, sub_id)
            accounts = list(sm.storage_accounts.list())
            items: List[Tuple[str, str, str, str, str]] = []
            for a in accounts:
                rg = ""
                try:
                    parts = (a.id or "").split("/")
                    if "resourceGroups" in parts:
                        rg = parts[parts.index("resourceGroups") + 1]
                except Exception:
                    pass
                ep = ""
                try:
                    ep = (a.primary_endpoints.blob or "").rstrip("/")
                except Exception:
                    pass
                items.append((f"{a.name} ({rg})", a.name, rg, sub_id, ep))
            items.sort(key=lambda t: t[1].lower())
            self._accounts = items
            self.after(0, self._populate_accounts)
        except Exception as e:
            self.after(0, lambda msg=str(e): self._set_status(f"List accounts failed: {msg}"))

    def _populate_accounts(self) -> None:
        if not self._accounts:
            self._set_status("No storage accounts visible to this user.")
            return
        self._acct_choices = tuple(a[0] for a in self._accounts)
        self.acct_picker.set("")
        self.acct_picker.refresh_suggestions()
        self._set_status(
            f"{len(self._acct_choices)} storage account(s) loaded — type or open the list to pick one."
        )

    def _on_acct_change(self, *_: Any) -> None:
        selected_display = (self.acct_picker.get() or "").strip()
        if not selected_display:
            return
        self._selected_account = None
        for acct in self._accounts:
            if acct[0] == selected_display:
                self._selected_account = acct
                break
        if self._selected_account is None:
            return
        self._cont_choices = ()
        self.cont_picker.set("")
        self.cont_picker.refresh_suggestions()
        self.ok_btn.config(state=tk.DISABLED)
        self._set_status("Loading containers...")
        threading.Thread(target=self._load_containers, daemon=True).start()

    def _load_containers(self) -> None:
        try:
            assert self._selected_account is not None
            _, name, rg, sub_id, _ep = self._selected_account
            from azure.mgmt.storage import StorageManagementClient

            sm = StorageManagementClient(self._credential, sub_id)
            containers = list(sm.blob_containers.list(rg, name))
            self._containers = sorted([c.name for c in containers if c.name])
            self.after(0, self._populate_containers)
        except Exception as e:
            self.after(0, lambda msg=str(e): self._set_status(f"List containers failed: {msg}"))

    def _populate_containers(self) -> None:
        if not self._containers:
            self._set_status("No containers found in this storage account.")
            return
        self._cont_choices = tuple(self._containers)
        self.cont_picker.set("")
        self.cont_picker.refresh_suggestions()
        self.ok_btn.config(state=tk.DISABLED)
        self._set_status(
            f"{len(self._cont_choices)} container(s) loaded — type or open the list to pick one."
        )

    def _on_cont_change(self) -> None:
        cont = (self.cont_picker.get() or "").strip()
        if cont and self._selected_account is not None:
            self._set_status("")
            self.ok_btn.config(state=tk.NORMAL)
        else:
            self.ok_btn.config(state=tk.DISABLED)

    def _apply(self) -> None:
        if self._selected_account is None:
            messagebox.showerror("Pick storage", "Select a storage account.")
            return
        cont = (self.cont_picker.get() or "").strip()
        if not cont:
            messagebox.showerror("Pick container", "Select a container.")
            return
        if cont not in self._containers:
            # Allow typed exact match even if filter list was narrowed
            if cont.lower() not in {c.lower() for c in self._containers}:
                messagebox.showerror("Pick container", "Select a container from the list.")
                return

        _, name, rg, sub_id, ep = self._selected_account
        account_url = ep or f"https://{name}.blob.core.windows.net"
        url_with_container = f"{account_url}/{cont}"

        mode = self.mode_var.get()
        conn_str = ""
        if mode == "connection_string":
            try:
                from azure.mgmt.storage import StorageManagementClient

                sm = StorageManagementClient(self._credential, sub_id)
                keys = sm.storage_accounts.list_keys(rg, name)
                key = _first_account_key(keys)
                if not key:
                    raise RuntimeError("No account keys returned.")
                conn_str = (
                    f"DefaultEndpointsProtocol=https;AccountName={name};"
                    f"AccountKey={key};EndpointSuffix=core.windows.net"
                )
            except Exception as e:
                messagebox.showerror(
                    "Get keys failed",
                    f"Could not retrieve account key: {e}\n\n"
                    "You may not have 'Storage Account Key Operator' role. "
                    "Use Managed Identity mode instead.",
                )
                return

        try:
            self._on_apply(
                mode=mode,
                connection_string=conn_str,
                container=cont,
                account_url=url_with_container,
            )
        finally:
            self.destroy()
