# Author: Satish Chauhan

"""
Azure Blob Browser dialog.

Lets the user pick Subscription -> Storage Account -> Container interactively
(similar to the SSMS "Connect to a Microsoft Subscription" dialog) and applies
the selection back to the Backup & Restore tab via a callback.

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
from tkinter import messagebox, ttk
from typing import Any, Callable, List, Optional, Tuple


def azure_browse_dependency_error() -> Optional[str]:
    """
    Return a user-facing error message if Browse Azure dependencies are missing.
    Each import is checked separately so the message lists only what is actually absent.
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
    """
    Get cached credential or create a new one.
    Returns (credential, user_display_name).
    Caches at module level so user doesn't re-authenticate every time.
    """
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
        import base64, json
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
        self.resizable(False, False)
        self.geometry("620x520")

        self._on_apply = on_apply
        self._mi_allowed = mi_allowed
        self._credential: Any = None
        self._subs: List[Tuple[str, str]] = []
        self._accounts: List[Tuple[str, str, str, str, str]] = []
        self._containers: List[str] = []
        self._selected_account: Optional[Tuple[str, str, str, str, str]] = None
        
        self._sub_search_var = tk.StringVar()
        self._acct_search_var = tk.StringVar()
        self._cont_search_var = tk.StringVar()

        self._build_ui(default_mode)
        self.after(100, self._kick_off_login)

    def _build_ui(self, default_mode: str) -> None:
        self.signed_in_var = tk.StringVar(value="Connecting to Azure...")
        tk.Label(
            self, textvariable=self.signed_in_var, anchor=tk.W
        ).pack(fill=tk.X, padx=12, pady=(12, 4))

        row = ttk.Frame(self)
        row.pack(fill=tk.X, padx=12, pady=4)
        tk.Label(row, text="Subscription:", width=18, anchor=tk.W).pack(side=tk.LEFT)
        search_frame = ttk.Frame(row)
        search_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        tk.Label(search_frame, text="Search:", fg="gray", font=("Segoe UI", 8)).pack(side=tk.LEFT, padx=(0, 4))
        self.sub_search_entry = ttk.Entry(search_frame, textvariable=self._sub_search_var, width=15)
        self.sub_search_entry.pack(side=tk.LEFT)
        self._sub_search_var.trace_add("write", lambda *_: self._filter_subscriptions())
        
        row2 = ttk.Frame(self)
        row2.pack(fill=tk.X, padx=12, pady=(0, 4))
        tk.Label(row2, text="", width=18).pack(side=tk.LEFT)
        self.sub_var = tk.StringVar()
        self.sub_combo = ttk.Combobox(
            row2, textvariable=self.sub_var, state="readonly"
        )
        self.sub_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.sub_combo.bind("<<ComboboxSelected>>", self._on_sub_change)

        row = ttk.Frame(self)
        row.pack(fill=tk.X, padx=12, pady=4)
        tk.Label(row, text="Storage Account:", width=18, anchor=tk.W).pack(side=tk.LEFT)
        search_frame = ttk.Frame(row)
        search_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        tk.Label(search_frame, text="Search:", fg="gray", font=("Segoe UI", 8)).pack(side=tk.LEFT, padx=(0, 4))
        self.acct_search_entry = ttk.Entry(search_frame, textvariable=self._acct_search_var, width=15)
        self.acct_search_entry.pack(side=tk.LEFT)
        self._acct_search_var.trace_add("write", lambda *_: self._filter_accounts())
        
        row2 = ttk.Frame(self)
        row2.pack(fill=tk.X, padx=12, pady=(0, 4))
        tk.Label(row2, text="", width=18).pack(side=tk.LEFT)
        self.acct_var = tk.StringVar()
        self.acct_combo = ttk.Combobox(
            row2, textvariable=self.acct_var, state="readonly"
        )
        self.acct_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.acct_combo.bind("<<ComboboxSelected>>", self._on_acct_change)

        row = ttk.Frame(self)
        row.pack(fill=tk.X, padx=12, pady=4)
        tk.Label(row, text="Blob Container:", width=18, anchor=tk.W).pack(side=tk.LEFT)
        search_frame = ttk.Frame(row)
        search_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        tk.Label(search_frame, text="Search:", fg="gray", font=("Segoe UI", 8)).pack(side=tk.LEFT, padx=(0, 4))
        self.cont_search_entry = ttk.Entry(search_frame, textvariable=self._cont_search_var, width=15)
        self.cont_search_entry.pack(side=tk.LEFT)
        self._cont_search_var.trace_add("write", lambda *_: self._filter_containers())
        
        row2 = ttk.Frame(self)
        row2.pack(fill=tk.X, padx=12, pady=(0, 4))
        tk.Label(row2, text="", width=18).pack(side=tk.LEFT)
        self.cont_var = tk.StringVar()
        self.cont_combo = ttk.Combobox(
            row2, textvariable=self.cont_var, state="readonly"
        )
        self.cont_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)

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
            wraplength=580,
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
    
    def _filter_items(self, full_list: List[str], search_text: str) -> List[str]:
        """Case-insensitive substring filter."""
        q = search_text.strip().lower()
        if not q:
            return full_list
        return [item for item in full_list if q in item.lower()]
    
    def _filter_subscriptions(self) -> None:
        """Filter subscriptions based on search text."""
        search = self._sub_search_var.get()
        all_subs = [d[0] for d in self._subs]
        filtered = self._filter_items(all_subs, search)
        self.sub_combo["values"] = filtered
        if filtered and self.sub_var.get() not in filtered:
            self.sub_combo.set("")
    
    def _filter_accounts(self) -> None:
        """Filter storage accounts based on search text."""
        search = self._acct_search_var.get()
        all_accts = [a[0] for a in self._accounts]
        filtered = self._filter_items(all_accts, search)
        self.acct_combo["values"] = filtered
        if filtered and self.acct_var.get() not in filtered:
            self.acct_combo.set("")
    
    def _filter_containers(self) -> None:
        """Filter containers based on search text."""
        search = self._cont_search_var.get()
        filtered = self._filter_items(self._containers, search)
        self.cont_combo["values"] = filtered
        if filtered and self.cont_var.get() not in filtered:
            self.cont_combo.set("")

    def _kick_off_login(self) -> None:
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
            self.after(0, lambda msg=str(e): self._fatal_missing_deps(
                f"Could not list subscriptions: {msg}\n\n"
                "Run `az login` (or sign in via Visual Studio / VS Code) and retry."
            ))

    def _fatal_missing_deps(self, msg: str) -> None:
        self.signed_in_var.set("Sign-in failed")
        messagebox.showerror("Browse Azure", msg)
        self.destroy()

    def _populate_subs(self, user: str) -> None:
        self.signed_in_var.set(f"Signed in as: {user} (cached)")
        if not self._subs:
            self._set_status("No subscriptions visible to this user.")
            return
        self._filter_subscriptions()
        if self.sub_combo["values"]:
            self.sub_combo.current(0)
            self._on_sub_change()

    def _on_sub_change(self, *_: Any) -> None:
        selected_display = self.sub_var.get()
        if not selected_display:
            return
        sub_id = None
        for sub in self._subs:
            if sub[0] == selected_display:
                sub_id = sub[1]
                break
        if sub_id is None:
            return
        self.acct_combo["values"] = []
        self.cont_combo["values"] = []
        self.acct_var.set("")
        self.cont_var.set("")
        self._acct_search_var.set("")
        self._cont_search_var.set("")
        self.ok_btn.config(state=tk.DISABLED)
        self._set_status("Loading storage accounts...")
        threading.Thread(
            target=self._load_accounts, args=(sub_id,), daemon=True
        ).start()

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
        self._filter_accounts()
        if self.acct_combo["values"]:
            self.acct_combo.current(0)
            self._on_acct_change()

    def _on_acct_change(self, *_: Any) -> None:
        selected_display = self.acct_var.get()
        if not selected_display:
            return
        self._selected_account = None
        for acct in self._accounts:
            if acct[0] == selected_display:
                self._selected_account = acct
                break
        if self._selected_account is None:
            return
        self.cont_combo["values"] = []
        self.cont_var.set("")
        self._cont_search_var.set("")
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
        self._filter_containers()
        if self.cont_combo["values"]:
            self.cont_combo.current(0)
            self._set_status("")
            self.ok_btn.config(state=tk.NORMAL)

    def _apply(self) -> None:
        if self._selected_account is None or self.acct_combo.current() < 0:
            messagebox.showerror("Pick storage", "Select a storage account.")
            return
        cont = (self.cont_var.get() or "").strip()
        if not cont:
            messagebox.showerror("Pick container", "Select a container.")
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
                key = keys.keys[0].value if keys.keys else ""
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
