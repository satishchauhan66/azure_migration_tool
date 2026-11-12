# Author: Satish Chauhan
"""Add/Edit prerequisite row with saved servers, profiles, and blob presets."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, simpledialog, ttk
from typing import Any, Callable, Dict, Optional

try:
    from gui.widgets.connection_widget import ConnectionWidget
except ImportError:
    from azure_migration_tool.gui.widgets.connection_widget import ConnectionWidget

try:
    from gui.utils.prerequisites_profiles import (
        CUSTOM_BLOB_PRESET_ID,
        ORG_BLOB_PRESET_ID,
        blob_preset_display,
        delete_blob_preset,
        delete_profile,
        get_blob_preset,
        get_profile,
        list_blob_preset_names,
        list_profile_names,
        resolve_blob_preset_id,
        save_blob_preset,
        save_profile,
    )
except ImportError:
    from azure_migration_tool.gui.utils.prerequisites_profiles import (
        CUSTOM_BLOB_PRESET_ID,
        ORG_BLOB_PRESET_ID,
        blob_preset_display,
        delete_blob_preset,
        delete_profile,
        get_blob_preset,
        get_profile,
        list_blob_preset_names,
        list_profile_names,
        resolve_blob_preset_id,
        save_blob_preset,
        save_profile,
    )

try:
    from gui.utils.blob_config import load_blob_settings, save_blob_settings
except ImportError:
    from azure_migration_tool.gui.utils.blob_config import load_blob_settings, save_blob_settings

try:
    from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll
except ImportError:
    from azure_migration_tool.gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll


def open_prerequisites_row_dialog(
    parent: tk.Misc,
    main_window: Any,
    *,
    title: str = "Prerequisite row",
    initial: Optional[Dict[str, Any]] = None,
    get_blob_defaults: Optional[Callable[[], Dict[str, str]]] = None,
) -> Optional[Dict[str, str]]:
    initial = dict(initial or {})
    blob_defaults_fn = get_blob_defaults or (lambda: {})

    dlg = tk.Toplevel(parent)
    dlg.title(title)
    dlg.transient(parent.winfo_toplevel())
    dlg.grab_set()
    dlg.minsize(620, 480)
    dlg.geometry("680x560")

    shell = ttk.Frame(dlg)
    shell.pack(fill=tk.BOTH, expand=True)
    canvas = tk.Canvas(shell, highlightthickness=0)
    vsb = ttk.Scrollbar(shell, orient=tk.VERTICAL, command=canvas.yview)
    canvas.configure(yscrollcommand=vsb.set)
    vsb.pack(side=tk.RIGHT, fill=tk.Y)
    canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    body = ttk.Frame(canvas, padding=10)
    win_id = canvas.create_window((0, 0), window=body, anchor="nw")

    def _on_body_configure(_event: tk.Event) -> None:
        canvas.configure(scrollregion=canvas.bbox("all"))

    def _on_canvas_configure(event: tk.Event) -> None:
        canvas.itemconfig(win_id, width=event.width)

    body.bind("<Configure>", _on_body_configure)
    canvas.bind("<Configure>", _on_canvas_configure)
    bind_canvas_vertical_scroll(canvas, body)

    # --- Saved profile ---
    prof_row = ttk.Frame(body)
    prof_row.pack(fill=tk.X, pady=(0, 8))
    ttk.Label(prof_row, text="Saved profile:").pack(side=tk.LEFT)
    profile_var = tk.StringVar()
    profile_combo = ttk.Combobox(prof_row, textvariable=profile_var, width=36, state="readonly")
    profile_combo.pack(side=tk.LEFT, padx=6)

    label_var = tk.StringVar(value=str(initial.get("row_label") or ""))
    ttk.Label(body, text="Row label:").pack(anchor=tk.W)
    ttk.Entry(body, textvariable=label_var, width=60).pack(fill=tk.X, pady=(0, 8))

    src_server = tk.StringVar(value=str(initial.get("src_server") or ""))
    src_db = tk.StringVar(value=str(initial.get("src_db") or ""))
    src_auth = tk.StringVar(value=str(initial.get("src_auth") or "windows"))
    src_user = tk.StringVar(value=str(initial.get("src_user") or ""))
    src_password = tk.StringVar(value=str(initial.get("src_password") or ""))

    dest_server = tk.StringVar(value=str(initial.get("dest_server") or ""))
    dest_db = tk.StringVar(value=str(initial.get("dest_db") or ""))
    dest_auth = tk.StringVar(value=str(initial.get("dest_auth") or "entra_mfa"))
    dest_user = tk.StringVar(value=str(initial.get("dest_user") or ""))
    dest_password = tk.StringVar(value=str(initial.get("dest_password") or ""))

    src_frame = ttk.LabelFrame(body, text="Source", padding=8)
    src_frame.pack(fill=tk.X, pady=4)
    ConnectionWidget(
        src_frame,
        src_server,
        src_db,
        src_auth,
        src_user,
        src_password,
        label_text="",
        row_start=0,
    )

    dest_frame = ttk.LabelFrame(body, text="Destination", padding=8)
    dest_frame.pack(fill=tk.X, pady=4)
    dest_widget = ConnectionWidget(
        dest_frame,
        dest_server,
        dest_db,
        dest_auth,
        dest_user,
        dest_password,
        label_text="",
        row_start=0,
    )
    dest_mi_var = tk.BooleanVar(value=bool(initial.get("dest_managed_instance")))
    ttk.Checkbutton(
        dest_frame,
        text="Destination is Azure SQL Managed Instance (blob restore)",
        variable=dest_mi_var,
    ).grid(row=dest_widget.grid_last_row + 1, column=0, columnspan=2, sticky=tk.W, pady=(6, 0))
    tk.Label(
        dest_frame,
        text="Target database name is optional — server reachability is checked on master; "
        "existing DB is a warning only.",
        fg="gray",
        wraplength=560,
        justify=tk.LEFT,
    ).grid(row=dest_widget.grid_last_row + 2, column=0, columnspan=2, sticky=tk.W, pady=(4, 0))

    opt_frame = ttk.LabelFrame(body, text="Optional (local backup path)", padding=8)
    opt_frame.pack(fill=tk.X, pady=4)
    backup_path_var = tk.StringVar(
        value=str(
            initial.get("backup_unc_path")
            or initial.get("local_backup_path")
            or initial.get("backup_path")
            or ""
        )
    )
    ttk.Label(opt_frame, text="Backup UNC / folder (source SQL):").pack(anchor=tk.W)
    ttk.Entry(opt_frame, textvariable=backup_path_var, width=70).pack(fill=tk.X, pady=(2, 0))
    tk.Label(
        opt_frame,
        text="Leave blank to skip. If set, checks path when visible (UNC also probed via source SQL).",
        fg="gray",
        wraplength=560,
        justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(4, 0))

    blob_frame = ttk.LabelFrame(body, text="Blob storage", padding=8)
    blob_frame.pack(fill=tk.X, pady=4)

    preset_names = list_blob_preset_names()
    preset_displays = [blob_preset_display(n) for n in preset_names]
    blob_preset_var = tk.StringVar(value=blob_preset_display(ORG_BLOB_PRESET_ID))

    blob_auth_var = tk.StringVar(value=str(initial.get("blob_auth_mode") or "connection_string"))
    blob_conn_var = tk.StringVar(value=str(initial.get("blob_connection_string") or ""))
    blob_url_var = tk.StringVar(value=str(initial.get("blob_account_url") or ""))
    blob_cont_var = tk.StringVar(value=str(initial.get("blob_container") or ""))

    def _using_org_blob() -> bool:
        return resolve_blob_preset_id(blob_preset_var.get()) == ORG_BLOB_PRESET_ID

    def _apply_blob_preset(_event=None) -> None:
        pid = resolve_blob_preset_id(blob_preset_var.get())
        if pid == CUSTOM_BLOB_PRESET_ID:
            return
        if pid == ORG_BLOB_PRESET_ID:
            d = blob_defaults_fn() or {}
            if not d:
                conn, cont, mode, url = load_blob_settings()
                d = {
                    "blob_auth_mode": mode,
                    "blob_connection_string": conn,
                    "blob_account_url": url,
                    "blob_container": cont,
                }
            blob_auth_var.set(d.get("blob_auth_mode") or "connection_string")
            blob_conn_var.set(d.get("blob_connection_string") or "")
            blob_url_var.set(d.get("blob_account_url") or "")
            blob_cont_var.set(d.get("blob_container") or "")
        else:
            fields = get_blob_preset(pid)
            if fields:
                blob_auth_var.set(fields.get("blob_auth_mode") or "connection_string")
                blob_conn_var.set(fields.get("blob_connection_string") or "")
                blob_url_var.set(fields.get("blob_account_url") or "")
                blob_cont_var.set(fields.get("blob_container") or "")

    pr = ttk.Frame(blob_frame)
    pr.pack(fill=tk.X)
    ttk.Label(pr, text="Preset:").pack(side=tk.LEFT)
    blob_preset_combo = ttk.Combobox(
        pr, textvariable=blob_preset_var, values=preset_displays, state="readonly", width=32
    )
    blob_preset_combo.pack(side=tk.LEFT, padx=4)
    blob_preset_combo.bind("<<ComboboxSelected>>", _apply_blob_preset)

    def _refresh_blob_presets(select: Optional[str] = None) -> None:
        names = list_blob_preset_names()
        blob_preset_combo["values"] = [blob_preset_display(n) for n in names]
        if select:
            blob_preset_var.set(blob_preset_display(select))
        _apply_blob_preset()

    ttk.Button(pr, text="Save preset…", command=lambda: _save_blob_preset_click()).pack(side=tk.LEFT, padx=4)
    ttk.Button(pr, text="Del preset", command=lambda: _delete_blob_preset_click()).pack(side=tk.LEFT, padx=2)
    ttk.Button(
        pr,
        text="Save as org default",
        command=lambda: _save_org_blob(),
    ).pack(side=tk.LEFT, padx=8)

    def _save_org_blob() -> None:
        save_blob_settings(
            blob_conn_var.get(),
            blob_cont_var.get(),
            blob_auth_var.get(),
            blob_url_var.get(),
        )
        mw = main_window
        if hasattr(mw, "shared_blob_auth_mode"):
            mw.shared_blob_auth_mode.set(blob_auth_var.get())
        if hasattr(mw, "shared_blob_connection_string"):
            mw.shared_blob_connection_string.set(blob_conn_var.get())
        if hasattr(mw, "shared_blob_account_url"):
            mw.shared_blob_account_url.set(blob_url_var.get())
        if hasattr(mw, "shared_blob_container"):
            mw.shared_blob_container.set(blob_cont_var.get())
        messagebox.showinfo("Blob", "Organization blob settings saved.", parent=dlg)

    def _save_blob_preset_click() -> None:
        name = simpledialog.askstring("Blob preset", "Preset name:", parent=dlg)
        if not name:
            return
        fields = {
            "blob_auth_mode": blob_auth_var.get(),
            "blob_connection_string": blob_conn_var.get(),
            "blob_account_url": blob_url_var.get(),
            "blob_container": blob_cont_var.get(),
        }
        if save_blob_preset(name.strip(), fields):
            _refresh_blob_presets(name.strip())
            messagebox.showinfo("Blob", "Preset saved.", parent=dlg)
        else:
            messagebox.showerror("Blob", "Could not save preset.", parent=dlg)

    def _delete_blob_preset_click() -> None:
        pid = resolve_blob_preset_id(blob_preset_var.get())
        if pid in (ORG_BLOB_PRESET_ID, CUSTOM_BLOB_PRESET_ID):
            messagebox.showwarning("Blob", "Cannot delete this preset.", parent=dlg)
            return
        if delete_blob_preset(pid):
            blob_preset_var.set(blob_preset_display(ORG_BLOB_PRESET_ID))
            _refresh_blob_presets()
            messagebox.showinfo("Blob", "Preset deleted.", parent=dlg)

    row_b = ttk.Frame(blob_frame)
    row_b.pack(fill=tk.X, pady=(6, 0))
    ttk.Label(row_b, text="Auth:").grid(row=0, column=0, sticky=tk.W)
    ttk.Combobox(
        row_b,
        textvariable=blob_auth_var,
        values=("connection_string", "managed_identity"),
        state="readonly",
        width=22,
    ).grid(row=0, column=1, sticky=tk.W, padx=4)
    ttk.Label(row_b, text="Container:").grid(row=0, column=2, sticky=tk.W, padx=(12, 0))
    ttk.Entry(row_b, textvariable=blob_cont_var, width=28).grid(row=0, column=3, sticky=tk.EW, padx=4)
    row_b.columnconfigure(3, weight=1)

    ttk.Label(blob_frame, text="Connection string:").pack(anchor=tk.W, pady=(6, 0))
    ttk.Entry(blob_frame, textvariable=blob_conn_var, show="*").pack(fill=tk.X)
    ttk.Label(blob_frame, text="Account URL (MI):").pack(anchor=tk.W, pady=(6, 0))
    ttk.Entry(blob_frame, textvariable=blob_url_var).pack(fill=tk.X)

    # Guess blob preset from initial row vs org
    init_blob = {
        "blob_auth_mode": initial.get("blob_auth_mode"),
        "blob_connection_string": initial.get("blob_connection_string"),
        "blob_container": initial.get("blob_container"),
        "blob_account_url": initial.get("blob_account_url"),
    }
    if any(str(v or "").strip() for v in init_blob.values()):
        blob_preset_var.set("(Custom — edit fields)")
        blob_auth_var.set(str(initial.get("blob_auth_mode") or "connection_string"))
        blob_conn_var.set(str(initial.get("blob_connection_string") or ""))
        blob_url_var.set(str(initial.get("blob_account_url") or ""))
        blob_cont_var.set(str(initial.get("blob_container") or ""))
    else:
        _refresh_blob_presets()

    def _refresh_profiles(select: Optional[str] = None) -> None:
        profile_combo["values"] = list_profile_names()
        if select:
            profile_var.set(select)

    def _load_profile(_event=None) -> None:
        name = profile_var.get().strip()
        row = get_profile(name)
        if not row:
            return
        label_var.set(row.get("row_label") or name)
        src_server.set(row.get("src_server") or "")
        src_db.set(row.get("src_db") or "")
        src_auth.set(row.get("src_auth") or "windows")
        src_user.set(row.get("src_user") or "")
        src_password.set(row.get("src_password") or "")
        dest_server.set(row.get("dest_server") or "")
        dest_db.set(row.get("dest_db") or "")
        dest_auth.set(row.get("dest_auth") or "entra_mfa")
        dest_user.set(row.get("dest_user") or "")
        dest_password.set(row.get("dest_password") or "")
        dest_mi_var.set(bool(row.get("dest_managed_instance")))
        backup_path_var.set(
            str(row.get("backup_unc_path") or row.get("local_backup_path") or row.get("backup_path") or "")
        )
        blob_auth_var.set(row.get("blob_auth_mode") or "connection_string")
        blob_conn_var.set(row.get("blob_connection_string") or "")
        blob_url_var.set(row.get("blob_account_url") or "")
        blob_cont_var.set(row.get("blob_container") or "")
        blob_preset_var.set("(Custom — edit fields)")

    profile_combo.bind("<<ComboboxSelected>>", _load_profile)
    _refresh_profiles()

    def _save_profile_click() -> None:
        data = _collect()
        if not data:
            return
        default_name = data.get("row_label") or f"{data['src_db']} → {data['dest_db']}"
        name = simpledialog.askstring("Save profile", "Profile name:", initialvalue=default_name, parent=dlg)
        if not name:
            return
        if save_profile(name.strip(), data):
            _refresh_profiles(name.strip())
            messagebox.showinfo("Profile", "Profile saved.", parent=dlg)
        else:
            messagebox.showerror("Profile", "Could not save profile.", parent=dlg)

    def _delete_profile_click() -> None:
        name = profile_var.get().strip()
        if not name:
            messagebox.showwarning("Profile", "Select a profile to delete.", parent=dlg)
            return
        if delete_profile(name):
            profile_var.set("")
            _refresh_profiles()
            messagebox.showinfo("Profile", "Profile deleted.", parent=dlg)

    ttk.Button(prof_row, text="Load", width=6, command=_load_profile).pack(side=tk.LEFT, padx=2)
    ttk.Button(prof_row, text="Save…", width=6, command=_save_profile_click).pack(side=tk.LEFT, padx=2)
    ttk.Button(prof_row, text="Del", width=4, command=_delete_profile_click).pack(side=tk.LEFT, padx=2)

    out: Dict[str, Optional[Dict[str, str]]] = {"value": None}

    def _collect() -> Optional[Dict[str, str]]:
        data = {
            "row_label": label_var.get().strip(),
            "src_server": src_server.get().strip(),
            "src_db": src_db.get().strip(),
            "src_auth": src_auth.get().strip(),
            "src_user": src_user.get().strip(),
            "src_password": src_password.get(),
            "dest_server": dest_server.get().strip(),
            "dest_db": dest_db.get().strip(),
            "dest_auth": dest_auth.get().strip(),
            "dest_user": dest_user.get().strip(),
            "dest_password": dest_password.get(),
        }
        if not data["src_server"] or not data["src_db"] or not data["dest_server"]:
            messagebox.showwarning(
                "Row",
                "Source server/database and destination server are required.",
                parent=dlg,
            )
            return None
        data["dest_managed_instance"] = dest_mi_var.get()
        bp = backup_path_var.get().strip()
        if bp:
            data["backup_unc_path"] = bp
        if resolve_blob_preset_id(blob_preset_var.get()) == ORG_BLOB_PRESET_ID:
            # Leave blob keys empty so merge uses org defaults at runtime
            return data
        data.update(
            {
                "blob_auth_mode": blob_auth_var.get().strip(),
                "blob_connection_string": blob_conn_var.get().strip(),
                "blob_account_url": blob_url_var.get().strip(),
                "blob_container": blob_cont_var.get().strip(),
            }
        )
        return data

    def _ok() -> None:
        data = _collect()
        if data:
            out["value"] = data
            dlg.destroy()

    def _cancel() -> None:
        dlg.destroy()

    btns = ttk.Frame(dlg, padding=(10, 0, 10, 10))
    btns.pack(fill=tk.X)
    ttk.Button(btns, text="OK", command=_ok).pack(side=tk.RIGHT, padx=4)
    ttk.Button(btns, text="Cancel", command=_cancel).pack(side=tk.RIGHT)

    dlg.wait_window()
    return out["value"]
