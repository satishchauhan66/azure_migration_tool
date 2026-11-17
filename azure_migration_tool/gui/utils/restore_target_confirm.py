# Author: Satish Chauhan

"""Check target SQL databases and confirm drop-and-restore before overwrite."""

from __future__ import annotations

import logging
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


def _compact_err(msg: str, max_len: int = 1200) -> str:
    try:
        from gui.tabs.backup_restore_tab import _compact_dialog_error
    except ImportError:
        try:
            from azure_migration_tool.gui.tabs.backup_restore_tab import _compact_dialog_error
        except ImportError:
            return (msg or "")[:max_len]
    return _compact_dialog_error(msg, max_len=max_len)


def database_state_if_exists(
    *,
    server: str,
    database: str,
    auth: str,
    user: Optional[str],
    password: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """(state_desc, None) if exists; (None, None) if missing; (None, err) on failure."""
    db = (database or "").strip()
    if not db or not (server or "").strip():
        return None, "Server and database name are required."
    try:
        from src.utils.database import connect_to_database, pick_sql_driver
    except ImportError:
        from azure_migration_tool.src.utils.database import connect_to_database, pick_sql_driver

    conn = None
    try:
        driver = pick_sql_driver(logger)
        conn = connect_to_database(
            server=(server or "").strip(),
            db="master",
            user=(user or "") if auth not in ("windows",) else "",
            driver=driver,
            auth=auth or "windows",
            password=password,
            timeout=60,
            logger=logger,
        )
        cur = conn.cursor()
        cur.execute("SELECT state_desc FROM sys.databases WHERE name = ?", (db,))
        row = cur.fetchone()
        cur.close()
        if row and row[0] is not None:
            return str(row[0]).strip(), None
        return None, None
    except Exception as e:
        return None, _compact_err(str(e))
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def prompt_typed_replace_existing_db(
    parent: tk.Misc,
    *,
    server: str,
    rows: List[Tuple[str, str]],
    title_suffix: str = "",
) -> bool:
    if not rows:
        return True
    root = parent.winfo_toplevel()
    dlg = tk.Toplevel(root)
    dlg.title("Destination database already exists")
    dlg.transient(root)
    dlg.grab_set()
    dlg.resizable(True, True)

    names = [n for n, _ in rows]
    lines = "\n".join(f"  • [{n}] — state: {st or 'UNKNOWN'}" for n, st in rows)
    extra = f"\n{title_suffix.strip()}\n" if title_suffix.strip() else ""
    intro = (
        "These destination database(s) already exist on the target server.\n"
        "To migrate fresh data, each will be **dropped** (SINGLE_USER), then restored from blob.\n\n"
        f"Server: {server}\n\n"
        f"{lines}\n\n"
        "Other databases on this server are not affected. "
        "Active sessions on the listed databases will be disconnected.\n"
        f"{extra}\n"
        "Cancel to skip starting migration (you can remove rows or change dest_db).\n"
    )
    tk.Label(dlg, text=intro, justify=tk.LEFT, wraplength=520).pack(anchor=tk.W, padx=14, pady=(12, 8))

    confirm_var = tk.StringVar()
    if len(rows) == 1:
        db0 = names[0]
        tk.Label(
            dlg,
            text=f'Type this line exactly to confirm drop and restore:\n"{db0}"',
            font=("Segoe UI", 9, "bold"),
        ).pack(anchor=tk.W, padx=14, pady=(0, 4))
        ent = ttk.Entry(dlg, textvariable=confirm_var, width=56)
        ent.pack(fill=tk.X, padx=14, pady=(0, 8))
        ent.focus_set()
    else:
        tk.Label(
            dlg,
            text="Type each line above on its own line (exact text, any order):",
            font=("Segoe UI", 9, "bold"),
        ).pack(anchor=tk.W, padx=14, pady=(0, 4))
        txt = scrolledtext.ScrolledText(dlg, height=8, width=64, wrap=tk.WORD)
        txt.pack(fill=tk.BOTH, expand=True, padx=14, pady=(0, 8))
        txt.focus_set()

    btn_row = ttk.Frame(dlg, padding=(14, 0, 14, 12))
    btn_row.pack(fill=tk.X)
    result = {"ok": False}

    def _cancel() -> None:
        try:
            dlg.grab_release()
        except Exception:
            pass
        dlg.destroy()

    def _confirm() -> None:
        if len(rows) == 1:
            typed = (confirm_var.get() or "").strip()
            if typed != names[0]:
                messagebox.showerror(
                    "Confirmation failed",
                    f'Name does not match "{names[0]}". Migration not started.',
                    parent=dlg,
                )
                return
        else:
            raw = txt.get("1.0", tk.END)
            got = {line.strip() for line in raw.splitlines() if line.strip()}
            want = set(names)
            if got != want:
                messagebox.showerror(
                    "Confirmation failed",
                    "Database names did not match. Migration not started.",
                    parent=dlg,
                )
                return
        result["ok"] = True
        _cancel()

    ttk.Button(btn_row, text="Cancel", command=_cancel, width=12).pack(side=tk.RIGHT, padx=(6, 0))
    ttk.Button(btn_row, text="Drop and migrate", command=_confirm, width=18).pack(side=tk.RIGHT)
    dlg.protocol("WM_DELETE_WINDOW", _cancel)
    dlg.update_idletasks()
    if len(rows) == 1:
        ent.bind("<Return>", lambda _e: _confirm())
    else:
        txt.bind("<Control-Return>", lambda _e: _confirm())
    dlg.wait_window()
    return bool(result["ok"])


def confirm_existing_targets_for_jobs(
    parent: tk.Misc,
    jobs: List[Dict[str, Any]],
) -> Tuple[bool, Dict[str, Set[str]]]:
    """
  For each job, check restore target DB on restore_target.server.

  Returns:
    (False, {}) — user cancelled or check failed.
    (True, {}) — no existing DBs.
    (True, {job_id: {db names}}) — user confirmed drop for those names per job.
    """
    entries: List[Tuple[str, str, str, str, Optional[str], Optional[str]]] = []
    for job in jobs:
        jid = (job.get("job_id") or "").strip()
        tgt = job.get("restore_target") or {}
        server = (tgt.get("server") or "").strip()
        auth = (tgt.get("auth") or "windows").strip()
        user = tgt.get("user")
        password = tgt.get("password")
        dest = (job.get("dest_db") or tgt.get("database") or "").strip()
        if not server:
            continue
        src = job.get("source") or {}
        dbs = list(src.get("databases") or [])
        if not dest and dbs:
            dest = (dbs[0] or "").strip()
        if not dest:
            continue
        try:
            from src.orchestration.bulk_local_blob_pipeline import resolve_restore_database_name
        except ImportError:
            from azure_migration_tool.src.orchestration.bulk_local_blob_pipeline import (
                resolve_restore_database_name,
            )
        src_db = dest
        if dbs:
            src_db = (dbs[0] or dest).strip()
        restore_as = resolve_restore_database_name(src_db, job=job, target=tgt)
        entries.append((jid, server, restore_as, auth, user, password))

    existing_by_server: Dict[str, List[Tuple[str, str, str]]] = {}
    for jid, server, db, auth, user, password in entries:
        st, err = database_state_if_exists(
            server=server,
            database=db,
            auth=auth,
            user=user,
            password=password,
        )
        if err:
            messagebox.showerror(
                "Cannot verify destination",
                f"Could not check [{db}] on {server}:\n\n{err}",
                parent=parent,
            )
            return False, {}
        if st is not None:
            existing_by_server.setdefault(server, []).append((jid, db, st))

    if not existing_by_server:
        return True, {}

    all_hits: List[Tuple[str, str, str]] = []
    for hits in existing_by_server.values():
        all_hits.extend(hits)

    jid_server: Dict[str, str] = {j: srv for j, srv, _d, _a, _u, _p in entries}
    display_rows: List[Tuple[str, str]] = []
    seen_key: Set[Tuple[str, str]] = set()
    for jid, db, st in all_hits:
        server = jid_server.get(jid, "")
        key = (server.casefold(), db.casefold())
        if key in seen_key:
            continue
        seen_key.add(key)
        label = f"[{db}] on {server}" if server else f"[{db}]"
        display_rows.append((label, st))

    n_jobs = len({jid for jid, _db, _st in all_hits})
    suffix = (
        f"You are about to start {n_jobs} migration job(s). "
        f"Every database listed below must be confirmed before any job runs."
    )
    servers = sorted({srv for _j, srv, _d, _a, _u, _p in entries})
    server_hint = servers[0] if len(servers) == 1 else ", ".join(servers[:4])
    if not prompt_typed_replace_existing_db(
        parent,
        server=server_hint,
        rows=display_rows,
        title_suffix=suffix,
    ):
        return False, {}

    confirmed: Dict[str, Set[str]] = {}
    for jid, db, _st in all_hits:
        confirmed.setdefault(jid, set()).add(db)
    return True, confirmed