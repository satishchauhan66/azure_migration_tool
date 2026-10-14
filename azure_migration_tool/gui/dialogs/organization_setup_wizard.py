# Author: Satish Chauhan

"""Multi-step one-time setup: Blob → backup UNC → project defaults."""

from __future__ import annotations

import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Dict, List

try:
    from gui.tabs.backup_restore_tab import _compact_dialog_error
except ImportError:
    from azure_migration_tool.gui.tabs.backup_restore_tab import _compact_dialog_error

try:
    from gui.dialogs.blob_settings_dialog import blob_settings_summary
except ImportError:
    from azure_migration_tool.gui.dialogs.blob_settings_dialog import blob_settings_summary


def open_organization_setup_wizard(main_window: Any, *, parent: tk.Misc | None = None) -> None:
    root = parent or main_window.root
    dlg = tk.Toplevel(root)
    dlg.title("Organization setup")
    dlg.transient(root)
    dlg.grab_set()
    dlg.resizable(True, True)
    dlg.minsize(560, 400)

    try:
        from gui.utils.org_settings import load_org_settings
    except ImportError:
        from azure_migration_tool.gui.utils.org_settings import load_org_settings

    org = load_org_settings()
    state = {"step": 0}

    header = ttk.Frame(dlg, padding=(14, 12, 14, 0))
    header.pack(fill=tk.X)
    title_var = tk.StringVar()
    step_var = tk.StringVar()
    tk.Label(header, textvariable=title_var, font=("Arial", 12, "bold")).pack(anchor=tk.W)
    tk.Label(header, textvariable=step_var, fg="gray").pack(anchor=tk.W, pady=(4, 0))

    body = ttk.Frame(dlg, padding=14)
    body.pack(fill=tk.BOTH, expand=True)

    pages: List[ttk.Frame] = []
    for _ in range(4):
        f = ttk.Frame(body)
        pages.append(f)

    # --- Step 1: Blob ---
    p1 = pages[0]
    tk.Label(
        p1,
        text="Step 1 — Azure Blob storage (upload & restore)",
        font=("Arial", 10, "bold"),
    ).pack(anchor=tk.W)
    tk.Label(
        p1,
        text="Save connection string and container once. Worker and MCP jobs use this automatically.",
        wraplength=500,
        justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(6, 10))

    auth_var = main_window.shared_blob_auth_mode
    conn_var = main_window.shared_blob_connection_string
    cont_var = main_window.shared_blob_container
    url_var = main_window.shared_blob_account_url
    show_plain = tk.BooleanVar(value=False)

    mode_row = ttk.Frame(p1)
    mode_row.pack(fill=tk.X)
    conn_frame = ttk.Frame(p1)
    mi_frame = ttk.Frame(p1)
    conn_ent = tk.Entry(conn_frame, textvariable=conn_var, width=70, show="*")
    conn_ent.pack(fill=tk.X, pady=4)

    def _sync_show() -> None:
        conn_ent.config(show="" if show_plain.get() else "*")

    ttk.Checkbutton(
        conn_frame, text="Show connection string", variable=show_plain, command=_sync_show
    ).pack(anchor=tk.W)
    ttk.Entry(mi_frame, textvariable=url_var, width=70).pack(fill=tk.X, pady=4)
    ttk.Label(p1, text="Container:").pack(anchor=tk.W, pady=(8, 0))
    ttk.Entry(p1, textvariable=cont_var, width=40).pack(anchor=tk.W)

    blob_status = tk.StringVar(value=blob_settings_summary())
    tk.Label(p1, textvariable=blob_status, fg="#1a5276", wraplength=500, justify=tk.LEFT).pack(
        anchor=tk.W, pady=(10, 0)
    )

    def _blob_visibility() -> None:
        if (auth_var.get() or "connection_string") == "managed_identity":
            conn_frame.pack_forget()
            mi_frame.pack(fill=tk.X, pady=(8, 0))
        else:
            mi_frame.pack_forget()
            conn_frame.pack(fill=tk.X, pady=(8, 0))

    ttk.Radiobutton(
        mode_row, text="Connection string", variable=auth_var, value="connection_string", command=_blob_visibility
    ).pack(side=tk.LEFT, padx=(0, 8))
    ttk.Radiobutton(
        mode_row, text="Managed Identity", variable=auth_var, value="managed_identity", command=_blob_visibility
    ).pack(side=tk.LEFT)

    def _browse_azure() -> None:
        try:
            from gui.widgets.azure_blob_browser import AzureBlobBrowser, azure_browse_dependency_error
        except ImportError:
            from azure_migration_tool.gui.widgets.azure_blob_browser import (
                AzureBlobBrowser,
                azure_browse_dependency_error,
            )
        dep_err = azure_browse_dependency_error()
        if dep_err:
            messagebox.showerror("Browse Azure", dep_err, parent=dlg)
            return

        def on_apply(*, mode: str, connection_string: str, container: str, account_url: str) -> None:
            auth_var.set(mode)
            cont_var.set(container)
            if mode == "managed_identity":
                url_var.set(account_url)
            else:
                conn_var.set(connection_string)
                url_var.set(account_url)
            _blob_visibility()

        AzureBlobBrowser(dlg, on_apply=on_apply, default_mode=auth_var.get() or "connection_string")

    ttk.Button(mode_row, text="Browse Azure…", command=_browse_azure).pack(side=tk.RIGHT)
    _blob_visibility()

    def _save_blob_step() -> bool:
        try:
            from gui.utils.blob_config import blob_settings_configured, save_blob_settings
        except ImportError:
            from azure_migration_tool.gui.utils.blob_config import blob_settings_configured, save_blob_settings
        save_blob_settings(
            conn_var.get(),
            cont_var.get(),
            blob_auth_mode=auth_var.get(),
            storage_account_url=url_var.get(),
        )
        blob_status.set(blob_settings_summary())
        if not blob_settings_configured():
            messagebox.showwarning(
                "Incomplete",
                "Enter blob connection string + container (or MI URL).",
                parent=dlg,
            )
            return False
        try:
            from gui.utils.environment_profiles import load_environment_profiles, save_environment_profiles
        except ImportError:
            from azure_migration_tool.gui.utils.environment_profiles import (
                load_environment_profiles,
                save_environment_profiles,
            )
        cfg = load_environment_profiles()
        uat = cfg.setdefault("profiles", {}).setdefault("uat", {})
        if isinstance(uat, dict):
            if not (uat.get("storage_account_url") or "").strip():
                uat["storage_account_url"] = (url_var.get() or "").strip()
            if not (uat.get("container") or "").strip():
                uat["container"] = (cont_var.get() or "").strip()
            uat["blob_auth_mode"] = (auth_var.get() or "managed_identity").strip()
            if (conn_var.get() or "").strip():
                uat["connection_string"] = (conn_var.get() or "").strip()
        save_environment_profiles(cfg)
        return True

    # --- Step 2: Backup UNC ---
    p2 = pages[1]
    tk.Label(p2, text="Step 2 — SQL Server backup folder (UNC)", font=("Arial", 10, "bold")).pack(anchor=tk.W)
    tk.Label(
        p2,
        text="Path on the SQL Server host (share both SQL and this PC can use).",
        wraplength=500,
        justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(6, 10))
    unc_var = tk.StringVar(value=(org.get("default_backup_unc_path") or "").strip())
    ttk.Label(p2, text="Backup UNC folder:").pack(anchor=tk.W)
    ttk.Entry(p2, textvariable=unc_var, width=72).pack(fill=tk.X, pady=4)

    def _use_sql_default_unc() -> None:
        server = (main_window.shared_src_server.get() or "").strip()
        if not server:
            messagebox.showwarning("Server", "Set source server on main window first.", parent=dlg)
            return

        def work() -> None:
            sql_dir = None
            try:
                try:
                    from src.utils.database import connect_to_database
                except ImportError:
                    from azure_migration_tool.src.utils.database import connect_to_database
                import logging

                conn = connect_to_database(
                    server=server,
                    db="master",
                    user=main_window.shared_src_user.get() or "",
                    driver="ODBC Driver 18 for SQL Server",
                    auth=main_window.shared_src_auth.get() or "windows",
                    password=main_window.shared_src_password.get() or None,
                    timeout=30,
                    logger=logging.getLogger(__name__),
                )
                cur = conn.cursor()
                cur.execute(
                    """
                    DECLARE @BackupDirectory NVARCHAR(512)
                    EXEC master.dbo.xp_instance_regread
                        N'HKEY_LOCAL_MACHINE',
                        N'Software\\Microsoft\\MSSQLServer\\MSSQLServer',
                        N'BackupDirectory',
                        @BackupDirectory OUTPUT
                    SELECT @BackupDirectory AS BackupDirectory
                    """
                )
                row = cur.fetchone()
                sql_dir = row[0] if row and row[0] else None
                cur.close()
                conn.close()
            except Exception:
                pass
            try:
                from src.backup.backup_path_utils import resolve_sql_default_backup_path
            except ImportError:
                from azure_migration_tool.src.backup.backup_path_utils import resolve_sql_default_backup_path
            path, _reason = resolve_sql_default_backup_path(sql_dir)
            dlg.after(0, lambda: unc_var.set(path))

        import threading

        threading.Thread(target=work, daemon=True).start()

    ttk.Button(p2, text="Use SQL Server default", command=_use_sql_default_unc).pack(anchor=tk.W, pady=(8, 0))

    def _save_unc_step() -> bool:
        path = (unc_var.get() or "").strip()
        if not path:
            messagebox.showwarning("Required", "Enter the backup UNC path.", parent=dlg)
            return False
        try:
            from gui.utils.org_settings import save_org_settings
        except ImportError:
            from azure_migration_tool.gui.utils.org_settings import save_org_settings
        save_org_settings(default_backup_unc_path=path)
        return True

    # --- Step 3: Project & blob prefix ---
    p3 = pages[2]
    tk.Label(p3, text="Step 3 — Project & blob paths", font=("Arial", 10, "bold")).pack(anchor=tk.W)
    tk.Label(
        p3,
        text="Default project folder for job results; optional blob root prefix inside the container.",
        wraplength=500,
        justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(6, 10))
    proj_var = tk.StringVar(value=(org.get("default_project_path") or "").strip())
    prefix_var = tk.StringVar(value=(org.get("default_blob_folder_prefix") or "").strip())
    ttk.Label(p3, text="Default project folder:").pack(anchor=tk.W)
    proj_row = ttk.Frame(p3)
    proj_row.pack(fill=tk.X, pady=4)
    ttk.Entry(proj_row, textvariable=proj_var, width=55).pack(side=tk.LEFT, fill=tk.X, expand=True)

    def _browse_proj() -> None:
        d = filedialog.askdirectory(parent=dlg, title="Default project folder")
        if d:
            proj_var.set(d)

    ttk.Button(proj_row, text="Browse…", command=_browse_proj).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Label(p3, text="Optional blob folder prefix:").pack(anchor=tk.W, pady=(10, 0))
    ttk.Entry(p3, textvariable=prefix_var, width=40).pack(anchor=tk.W)

    def _save_final_step() -> bool:
        try:
            from gui.utils.org_settings import save_org_settings
        except ImportError:
            from azure_migration_tool.gui.utils.org_settings import save_org_settings
        save_org_settings(
            default_backup_unc_path=(unc_var.get() or "").strip(),
            default_blob_folder_prefix=(prefix_var.get() or "").strip(),
            default_project_path=(proj_var.get() or "").strip(),
        )
        if hasattr(main_window, "_project_path") and (proj_var.get() or "").strip():
            main_window._project_path = (proj_var.get() or "").strip()
        return True

    # --- Step 4: Environment profiles ---
    p4 = pages[3]
    tk.Label(
        p4,
        text="Step 4 — Environments (dev / qa / uat / prod)",
        font=("Arial", 10, "bold"),
    ).pack(anchor=tk.W)
    tk.Label(
        p4,
        text="MI hostname and storage per environment. MCP resolves db22q→qa, route66-uat→uat, etc.",
        wraplength=520,
        justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(6, 10))
    try:
        from gui.utils.environment_profiles import load_environment_profiles
    except ImportError:
        from azure_migration_tool.gui.utils.environment_profiles import load_environment_profiles
    env_cfg = load_environment_profiles()
    default_env_var = tk.StringVar(value=(env_cfg.get("default_environment") or "uat").strip())
    ttk.Label(p4, text="Default when unknown:").pack(anchor=tk.W)
    ttk.Combobox(
        p4,
        textvariable=default_env_var,
        values=("dev", "qa", "uat", "prod"),
        width=12,
        state="readonly",
    ).pack(anchor=tk.W, pady=(4, 10))
    env_fields: Dict[str, Dict[str, tk.StringVar]] = {}
    for env_name in ("dev", "qa", "uat", "prod"):
        prof = (env_cfg.get("profiles") or {}).get(env_name) or {}
        frame = ttk.LabelFrame(p4, text=env_name.upper(), padding=6)
        frame.pack(fill=tk.X, pady=4)
        mi_v = tk.StringVar(value=(prof.get("mi_server") or "").strip())
        url_v = tk.StringVar(value=(prof.get("storage_account_url") or "").strip())
        cont_v = tk.StringVar(value=(prof.get("container") or "sqlbackups").strip())
        owner_v = tk.StringVar(value=(prof.get("owner_team") or "").strip())
        app_v = tk.StringVar(value=(prof.get("application") or "").strip())
        dbpat_v = tk.StringVar(
            value=",".join(
                p
                for p in (
                    prof.get("database_name_patterns")
                    if isinstance(prof.get("database_name_patterns"), list)
                    else str(prof.get("database_name_patterns") or "").split(",")
                )
                if str(p).strip()
            )
        )
        hostpat_v = tk.StringVar(
            value=",".join(
                p
                for p in (
                    prof.get("hostname_patterns")
                    if isinstance(prof.get("hostname_patterns"), list)
                    else str(prof.get("hostname_patterns") or "").split(",")
                )
                if str(p).strip()
            )
        )
        env_fields[env_name] = {
            "mi_server": mi_v,
            "storage_account_url": url_v,
            "container": cont_v,
            "owner_team": owner_v,
            "application": app_v,
            "database_name_patterns": dbpat_v,
            "hostname_patterns": hostpat_v,
        }
        ttk.Label(frame, text="MI server:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(frame, textvariable=mi_v, width=58).grid(row=0, column=1, sticky=tk.EW, padx=(6, 0))
        ttk.Label(frame, text="Storage URL:").grid(row=1, column=0, sticky=tk.W, pady=(4, 0))
        ttk.Entry(frame, textvariable=url_v, width=58).grid(
            row=1, column=1, sticky=tk.EW, padx=(6, 0), pady=(4, 0)
        )
        ttk.Label(frame, text="Container:").grid(row=2, column=0, sticky=tk.W, pady=(4, 0))
        ttk.Entry(frame, textvariable=cont_v, width=24).grid(
            row=2, column=1, sticky=tk.W, padx=(6, 0), pady=(4, 0)
        )
        ttk.Label(frame, text="Owner team:").grid(row=3, column=0, sticky=tk.W, pady=(4, 0))
        ttk.Entry(frame, textvariable=owner_v, width=36).grid(
            row=3, column=1, sticky=tk.W, padx=(6, 0), pady=(4, 0)
        )
        ttk.Label(frame, text="Application:").grid(row=4, column=0, sticky=tk.W, pady=(4, 0))
        ttk.Entry(frame, textvariable=app_v, width=36).grid(
            row=4, column=1, sticky=tk.W, padx=(6, 0), pady=(4, 0)
        )
        ttk.Label(frame, text="DB name patterns:").grid(row=5, column=0, sticky=tk.W, pady=(4, 0))
        ttk.Entry(frame, textvariable=dbpat_v, width=58).grid(
            row=5, column=1, sticky=tk.EW, padx=(6, 0), pady=(4, 0)
        )
        tk.Label(frame, text="e.g. *db22q*, *_qa_*", fg="gray").grid(
            row=6, column=1, sticky=tk.W, padx=(6, 0)
        )
        ttk.Label(frame, text="Hostname patterns:").grid(row=7, column=0, sticky=tk.W, pady=(4, 0))
        ttk.Entry(frame, textvariable=hostpat_v, width=58).grid(
            row=7, column=1, sticky=tk.EW, padx=(6, 0), pady=(4, 0)
        )
        frame.columnconfigure(1, weight=1)

    def _save_env_profiles_step() -> bool:
        try:
            from gui.utils.environment_profiles import load_environment_profiles, save_environment_profiles
        except ImportError:
            from azure_migration_tool.gui.utils.environment_profiles import (
                load_environment_profiles,
                save_environment_profiles,
            )
        cfg = load_environment_profiles()
        cfg["default_environment"] = (default_env_var.get() or "uat").strip()
        profiles = cfg.setdefault("profiles", {})
        for env_name, fields in env_fields.items():
            p = profiles.setdefault(env_name, {})
            if isinstance(p, dict):
                p["mi_server"] = (fields["mi_server"].get() or "").strip()
                p["storage_account_url"] = (fields["storage_account_url"].get() or "").strip()
                p["container"] = (fields["container"].get() or "sqlbackups").strip()
                p["owner_team"] = (fields["owner_team"].get() or "").strip()
                p["application"] = (fields["application"].get() or "").strip()
                p["database_name_patterns"] = [
                    x.strip() for x in (fields["database_name_patterns"].get() or "").split(",") if x.strip()
                ]
                p["hostname_patterns"] = [
                    x.strip() for x in (fields["hostname_patterns"].get() or "").split(",") if x.strip()
                ]
                p.setdefault("blob_auth_mode", "managed_identity")
                p.setdefault("target_managed_instance", True)
        save_environment_profiles(cfg)
        return True

    step_titles = [
        "Azure Blob",
        "Backup UNC path",
        "Project & paths",
        "Environment profiles",
    ]

    def _show_step(idx: int) -> None:
        state["step"] = idx
        for i, page in enumerate(pages):
            page.pack_forget()
        pages[idx].pack(fill=tk.BOTH, expand=True)
        title_var.set(f"Organization setup — {step_titles[idx]}")
        step_var.set(f"Step {idx + 1} of {len(pages)}")
        back_btn.config(state=tk.NORMAL if idx > 0 else tk.DISABLED)
        if idx < len(pages) - 1:
            next_btn.config(text="Save & Next →")
        else:
            next_btn.config(text="Finish")

    foot = ttk.Frame(dlg, padding=(14, 0, 14, 14))
    foot.pack(fill=tk.X)
    back_btn = ttk.Button(foot, text="← Back", width=10)
    back_btn.pack(side=tk.LEFT)
    next_btn = ttk.Button(foot, text="Save & Next →", width=14)
    next_btn.pack(side=tk.RIGHT)
    ttk.Button(foot, text="Cancel", command=dlg.destroy, width=10).pack(side=tk.RIGHT, padx=(0, 8))

    savers: List[Callable[[], bool]] = [
        _save_blob_step,
        _save_unc_step,
        _save_final_step,
        _save_env_profiles_step,
    ]

    def _on_back() -> None:
        i = state["step"]
        if i > 0:
            _show_step(i - 1)

    def _on_next() -> None:
        i = state["step"]
        if not savers[i]():
            return
        if i < len(pages) - 1:
            _show_step(i + 1)
        else:
            messagebox.showinfo(
                "Setup complete",
                "Organization defaults saved.\n\nAgent Workflow and jobs will use these automatically.",
                parent=dlg,
            )
            dlg.destroy()

    back_btn.config(command=_on_back)
    next_btn.config(command=_on_next)

    _show_step(0)
    dlg.update_idletasks()
    try:
        px = root.winfo_rootx() + max(0, (root.winfo_width() - dlg.winfo_width()) // 2)
        py = root.winfo_rooty() + max(0, (root.winfo_height() - dlg.winfo_height()) // 3)
        dlg.geometry(f"+{px}+{py}")
    except Exception:
        pass
