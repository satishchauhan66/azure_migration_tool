# Author: Satish Chauhan
"""Tools > Prerequisites — multi-DB connection and blob checks."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd

try:
    from gui.utils.excel_utils import read_bulk_config_spreadsheet
except ImportError:
    from azure_migration_tool.gui.utils.excel_utils import read_bulk_config_spreadsheet

_SECRET_EXPORT_KEYS = frozenset(
    {"src_password", "dest_password", "blob_connection_string", "password"}
)

try:
    from gui.dialogs.prerequisites_row_dialog import open_prerequisites_row_dialog
except ImportError:
    from azure_migration_tool.gui.dialogs.prerequisites_row_dialog import open_prerequisites_row_dialog

try:
    from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll
except ImportError:
    from azure_migration_tool.gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll

try:
    from src.migration.prerequisites_check import (
        ROW_PREREQ_TIMEOUT_SEC,
        merge_row_with_defaults,
        run_row_prerequisites_bounded,
        run_workstation_baseline,
    )
except ImportError:
    from azure_migration_tool.src.migration.prerequisites_check import (
        ROW_PREREQ_TIMEOUT_SEC,
        merge_row_with_defaults,
        run_row_prerequisites_bounded,
        run_workstation_baseline,
    )

_REQUIRED_IMPORT = ["src_server", "src_db", "dest_server"]


class PrerequisitesToolTab:
    def __init__(self, parent: tk.Widget, main_window: Any) -> None:
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self._rows: Dict[str, Dict[str, Any]] = {}
        self._results: Dict[str, Any] = {}
        self._run_thread: Optional[threading.Thread] = None
        self._create_widgets()

    def set_project_path(self, _project_path: Optional[str]) -> None:
        pass

    def _blob_defaults(self) -> Dict[str, str]:
        mw = self.main_window
        return {
            "blob_auth_mode": (mw.shared_blob_auth_mode.get() or "connection_string").strip(),
            "blob_connection_string": (mw.shared_blob_connection_string.get() or "").strip(),
            "blob_account_url": (mw.shared_blob_account_url.get() or "").strip(),
            "blob_container": (mw.shared_blob_container.get() or "").strip(),
        }

    def _create_widgets(self) -> None:
        shell = ttk.Frame(self.frame)
        shell.pack(fill=tk.BOTH, expand=True)

        hdr = ttk.Frame(shell)
        hdr.pack(fill=tk.X, padx=8, pady=(8, 6))
        tk.Label(hdr, text="Migration prerequisites", font=("Arial", 12, "bold")).pack(side=tk.LEFT)
        self.status_var = tk.StringVar(
            value="Run quick check = all rows (sanity). Start migration = selected → Full Migration."
        )
        tk.Label(hdr, textvariable=self.status_var, fg="gray").pack(side=tk.RIGHT)

        btn_row = ttk.Frame(shell)
        btn_row.pack(fill=tk.X, padx=8, pady=(0, 6))
        ttk.Button(btn_row, text="Import CSV/Excel…", command=self._import_spreadsheet, width=18).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(btn_row, text="Export rows…", command=self._export_rows, width=14).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(btn_row, text="Add row…", command=self._add_row_dialog, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_row, text="Edit row…", command=self._edit_row_dialog, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_row, text="Remove selected", command=self._remove_selected, width=14).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(btn_row, text="Run workstation checks", command=self._run_baseline, width=20).pack(
            side=tk.LEFT, padx=8
        )
        self.run_btn = ttk.Button(btn_row, text="Run quick check (all)", command=self._run_all, width=18)
        self.run_btn.pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_row, text="Start migration (selected)", command=self._start_migrations, width=22).pack(
            side=tk.LEFT, padx=2
        )

        scroll_host = ttk.Frame(shell)
        scroll_host.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
        canvas = tk.Canvas(scroll_host, highlightthickness=0)
        vsb = ttk.Scrollbar(scroll_host, orient=tk.VERTICAL, command=canvas.yview)
        hsb = ttk.Scrollbar(scroll_host, orient=tk.HORIZONTAL, command=canvas.xview)
        canvas.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        outer = ttk.Frame(canvas)
        win_id = canvas.create_window((0, 0), window=outer, anchor="nw")

        def _on_outer_configure(_event: tk.Event) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _on_canvas_configure(event: tk.Event) -> None:
            canvas.itemconfig(win_id, width=max(event.width, outer.winfo_reqwidth()))

        outer.bind("<Configure>", _on_outer_configure)
        canvas.bind("<Configure>", _on_canvas_configure)
        bind_canvas_vertical_scroll(canvas, outer)

        table_frame = ttk.Frame(outer)
        table_frame.pack(fill=tk.BOTH, expand=True)
        cols = (
            "label",
            "src",
            "dest",
            "container",
            "src_st",
            "dest_st",
            "src_blob",
            "dest_blob",
            "unc",
            "overall",
        )
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=12)
        headings = {
            "label": "Label",
            "src": "Source",
            "dest": "Destination",
            "container": "Blob container",
            "src_st": "Src",
            "dest_st": "Dest",
            "src_blob": "UNC→Blob",
            "dest_blob": "Dest blob",
            "unc": "Src UNC",
            "overall": "Overall",
        }
        widths = {
            "label": 100,
            "src": 120,
            "dest": 120,
            "container": 80,
            "src_st": 40,
            "dest_st": 40,
            "src_blob": 52,
            "dest_blob": 56,
            "unc": 48,
            "overall": 52,
        }
        for c in cols:
            self.tree.heading(c, text=headings[c])
            self.tree.column(c, width=widths.get(c, 80), stretch=c in ("src", "dest"))
        tree_vsb = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        tree_hsb = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=tree_vsb.set, xscrollcommand=tree_hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        tree_vsb.grid(row=0, column=1, sticky="ns")
        tree_hsb.grid(row=1, column=0, sticky="ew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        tk.Label(outer, text="Log (selected row):", fg="gray").pack(anchor=tk.W, pady=(6, 0))
        self.log_text = scrolledtext.ScrolledText(outer, height=12, wrap=tk.WORD, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=(0, 4))

    def _append_log(self, text: str) -> None:
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)

    def _clear_log(self) -> None:
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.config(state=tk.DISABLED)

    def _refresh_tree_row(self, iid: str) -> None:
        row = self._rows.get(iid, {})
        res = self._results.get(iid)
        label = row.get("row_label") or f"{row.get('src_db', '')} → {row.get('dest_db', '')}"
        src = f"{row.get('src_server', '')}/{row.get('src_db', '')}"
        dest = f"{row.get('dest_server', '')}/{row.get('dest_db', '')}"
        cont = row.get("blob_container") or self._blob_defaults().get("blob_container", "")
        if res:
            src_st = "OK" if res.src_ok else "FAIL"
            if not res.dest_ok:
                dest_st = "FAIL"
            elif getattr(res, "dest_db_exists", False):
                dest_st = "WARN"
            else:
                dest_st = "OK"
            sb = "OK" if res.src_blob_ok else "FAIL"
            db = "OK" if res.dest_blob_ok else "FAIL"
            if res.unc_ok is None:
                unc_st = "—"
            elif res.unc_ok and str(res.unc_detail or "").upper() == "WARN":
                unc_st = "WARN"
            elif res.unc_ok:
                unc_st = "OK"
            else:
                unc_st = "FAIL"
            overall = "PASS" if res.passed else "FAIL"
            if res.warnings:
                overall = overall + "*"
        else:
            src_st = dest_st = sb = db = unc_st = overall = "—"
        if self.tree.exists(iid):
            self.tree.item(
                iid,
                values=(label, src, dest, cont, src_st, dest_st, sb, db, unc_st, overall),
            )
        else:
            self.tree.insert(
                "",
                tk.END,
                iid=iid,
                values=(label, src, dest, cont, src_st, dest_st, sb, db, unc_st, overall),
            )

    def _add_row_from_dict(self, row: Dict[str, Any]) -> str:
        merged = merge_row_with_defaults(row, self._blob_defaults())
        iid = str(uuid4())
        self._rows[iid] = merged
        self._refresh_tree_row(iid)
        return iid

    def _import_spreadsheet(self) -> None:
        path = filedialog.askopenfilename(
            title="Import prerequisites",
            filetypes=[
                ("Spreadsheets", "*.xlsx *.csv"),
                ("Excel", "*.xlsx"),
                ("CSV", "*.csv"),
                ("All", "*.*"),
            ],
        )
        if not path:
            return
        try:
            rows = read_bulk_config_spreadsheet(path, _REQUIRED_IMPORT)
        except Exception as ex:
            messagebox.showerror("Import", str(ex))
            return
        for row in rows:
            self._add_row_from_dict(row)
        self.status_var.set(f"Imported {len(rows)} row(s).")

    def _export_rows(self) -> None:
        if not self._rows:
            messagebox.showinfo("Export", "No rows to export.")
            return
        path = filedialog.asksaveasfilename(
            title="Export prerequisite rows",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx"), ("CSV", "*.csv"), ("All", "*.*")],
        )
        if not path:
            return
        records: List[Dict[str, Any]] = []
        for iid, row in self._rows.items():
            rec = dict(row)
            for key in _SECRET_EXPORT_KEYS:
                if key in rec and rec[key]:
                    rec[key] = "***"
            res = self._results.get(iid)
            if res:
                rec["check_src"] = "OK" if res.src_ok else "FAIL"
                rec["check_dest"] = (
                    "WARN"
                    if res.dest_ok and getattr(res, "dest_db_exists", False)
                    else ("OK" if res.dest_ok else "FAIL")
                )
                rec["check_src_blob"] = "OK" if res.src_blob_ok else "FAIL"
                rec["check_dest_blob"] = "OK" if res.dest_blob_ok else "FAIL"
                rec["check_unc"] = (
                    "—"
                    if res.unc_ok is None
                    else ("OK" if res.unc_ok else "WARN")
                )
                rec["check_overall"] = "PASS" if res.passed else "FAIL"
                if res.warnings:
                    rec["check_warnings"] = " | ".join(res.warnings[:5])
            records.append(rec)
        try:
            df = pd.DataFrame(records)
            if Path(path).suffix.lower() == ".csv":
                df.to_csv(path, index=False)
            else:
                df.to_excel(path, index=False)
            messagebox.showinfo("Export", f"Exported {len(records)} row(s):\n{path}")
        except Exception as ex:
            messagebox.showerror("Export", str(ex))

    def _remove_selected(self) -> None:
        for iid in self.tree.selection():
            self._rows.pop(iid, None)
            self._results.pop(iid, None)
            self.tree.delete(iid)

    def _open_row_editor(self, title: str, initial: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, str]]:
        return open_prerequisites_row_dialog(
            self.frame,
            self.main_window,
            title=title,
            initial=initial,
            get_blob_defaults=self._blob_defaults,
        )

    def _add_row_dialog(self) -> None:
        defaults = self._blob_defaults()
        defaults.update(
            {
                "src_auth": "windows",
                "dest_auth": "entra_mfa",
                "src_server": self.main_window.shared_src_server.get(),
                "src_db": self.main_window.shared_src_db.get(),
                "dest_server": self.main_window.shared_dest_server.get(),
                "dest_db": self.main_window.shared_dest_db.get(),
                "dest_user": self.main_window.shared_dest_user.get(),
                "src_user": self.main_window.shared_src_user.get(),
            }
        )
        data = self._open_row_editor("Add prerequisite row", defaults)
        if data:
            self._add_row_from_dict(data)

    def _edit_row_dialog(self) -> None:
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Edit", "Select a row first.")
            return
        iid = sel[0]
        data = self._open_row_editor("Edit prerequisite row", dict(self._rows.get(iid) or {}))
        if data:
            merged = merge_row_with_defaults(data, self._blob_defaults())
            self._rows[iid] = merged
            self._results.pop(iid, None)
            self._refresh_tree_row(iid)

    def _on_select(self, _event=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        iid = sel[0]
        res = self._results.get(iid)
        self._clear_log()
        if res:
            for line in res.logs:
                self._append_log(line)
        else:
            row = self._rows.get(iid, {})
            self._append_log(f"Row: {row.get('row_label') or iid}")
            self._append_log("Not checked yet — use Run quick check (all).")

    def _run_baseline(self) -> None:
        self._clear_log()
        self.status_var.set("Workstation checks…")

        def work() -> None:
            lines: List[str] = []

            def log(m: str) -> None:
                lines.append(m)

            run_workstation_baseline(log)
            text = "\n".join(lines)

            def ui() -> None:
                self._clear_log()
                self._append_log(text)
                self.status_var.set("Workstation checks completed.")

            self.frame.after(0, ui)

        threading.Thread(target=work, daemon=True).start()

    def _run_ids(self, iids: List[str]) -> None:
        if self._run_thread and self._run_thread.is_alive():
            messagebox.showwarning("Busy", "A run is already in progress.")
            return
        if not iids:
            messagebox.showinfo("Prerequisites", "No rows to check.")
            return
        self.run_btn.config(state=tk.DISABLED)
        total = len(iids)
        workers = min(8, max(1, total))
        self.status_var.set(f"Running {total} row(s) in parallel ({workers} workers)…")

        def _check_one(row_id: str) -> Optional[tuple[str, Any]]:
            row = self._rows.get(row_id)
            if not row:
                return None
            res = run_row_prerequisites_bounded(dict(row), None, timeout_sec=ROW_PREREQ_TIMEOUT_SEC)
            return row_id, res

        def work() -> None:
            done_count = 0
            result_lock = threading.Lock()
            completed: List[str] = []

            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_to_rid = {pool.submit(_check_one, rid): rid for rid in iids}
                for fut in as_completed(future_to_rid):
                    rid_key = future_to_rid[fut]
                    try:
                        pair = fut.result()
                    except Exception as ex:
                        pair = None

                        def _ui_err(msg: str = str(ex)[:200], rk: str = rid_key) -> None:
                            self.status_var.set(f"Error on {rk[:8]}: {msg}")

                        self.frame.after(0, _ui_err)
                    if not pair:
                        continue
                    row_id, res = pair
                    with result_lock:
                        self._results[row_id] = res
                        completed.append(row_id)
                        done_count += 1
                        n = done_count

                    def ui_row(rid: str = row_id, count: int = n) -> None:
                        self._refresh_tree_row(rid)
                        self.status_var.set(f"Checked {count}/{total} (parallel)…")

                    self.frame.after(0, ui_row)

            done_ids = list(completed)

            def ui_done() -> None:
                for rid in done_ids:
                    self._refresh_tree_row(rid)
                self.run_btn.config(state=tk.NORMAL)
                fails = sum(1 for i in done_ids if i in self._results and not self._results[i].passed)
                self.status_var.set(
                    f"Completed {len(done_ids)} row(s); {fails} failed."
                    if fails
                    else f"All {len(done_ids)} row(s) passed."
                )
                if done_ids:
                    self.tree.selection_set(done_ids[0])
                    self._on_select()

            self.frame.after(0, ui_done)

        self._run_thread = threading.Thread(target=work, daemon=True)
        self._run_thread.start()

    def _run_all(self) -> None:
        self._run_ids(list(self._rows.keys()))

    def _start_migrations(self) -> None:
        sel = list(self.tree.selection())
        if not sel:
            messagebox.showwarning("Migration", "Select one or more rows to migrate.")
            return
        configs: List[Dict[str, Any]] = []
        not_checked: List[str] = []
        failed: List[str] = []
        for iid in sel:
            row = dict(self._rows.get(iid) or {})
            if not row:
                continue
            label = row.get("row_label") or f"{row.get('src_db')} → {row.get('dest_db')}"
            res = self._results.get(iid)
            if not res:
                not_checked.append(str(label))
            elif not res.passed:
                failed.append(str(label))
            configs.append(row)
        if not configs:
            return
        if not_checked:
            if not messagebox.askyesno(
                "Migration",
                f"{len(not_checked)} row(s) were not quick-checked.\nStart migration anyway?",
            ):
                return
        elif failed:
            if not messagebox.askyesno(
                "Migration",
                f"{len(failed)} row(s) did not pass quick check.\nStart migration anyway?",
            ):
                return
        mw = self.main_window
        try:
            mw.root.lift()
            mw._ensure_tab_created(2)
            tab = mw._tab_instances.get(2)
        except Exception as ex:
            messagebox.showerror("Migration", f"Could not open Full Migration tab: {ex}")
            return
        if tab is None or not hasattr(tab, "load_bulk_configs"):
            messagebox.showerror("Migration", "Full Migration tab is not available.")
            return
        tab.load_bulk_configs(configs, source_label="Prerequisites")
        mw.notebook.select(2)
        if messagebox.askyesno(
            "Full Migration",
            f"Loaded {len(configs)} database(s) on the Full Migration tab.\n"
            "Start bulk migration now?",
        ):
            tab._start_bulk_migration()
