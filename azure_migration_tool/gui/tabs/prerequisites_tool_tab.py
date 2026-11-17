# Author: Satish Chauhan
"""Tools > Prerequisites — multi-DB connection and blob checks."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
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
        ROW_PREREQ_FAST_TIMEOUT_SEC,
        ROW_PREREQ_TIMEOUT_SEC,
        merge_row_with_defaults,
        run_row_prerequisites_bounded,
        run_workstation_baseline,
    )
except ImportError:
    from azure_migration_tool.src.migration.prerequisites_check import (
        ROW_PREREQ_FAST_TIMEOUT_SEC,
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
        self._check_executor: Optional[ThreadPoolExecutor] = None
        self._in_flight: set[str] = set()
        self._check_lock = threading.Lock()
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
            value="Quick check: selected rows, or all if none selected."
        )
        tk.Label(hdr, textvariable=self.status_var, fg="gray").pack(side=tk.RIGHT)

        row_btn = ttk.Frame(shell)
        row_btn.pack(fill=tk.X, padx=8, pady=(0, 4))
        ttk.Button(row_btn, text="Import…", command=self._import_spreadsheet, width=10).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(row_btn, text="Export…", command=self._export_rows, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(row_btn, text="Add…", command=self._add_row_dialog, width=8).pack(side=tk.LEFT, padx=2)
        ttk.Button(row_btn, text="Edit…", command=self._edit_row_dialog, width=8).pack(side=tk.LEFT, padx=2)
        ttk.Button(row_btn, text="Remove", command=self._remove_selected, width=10).pack(side=tk.LEFT, padx=2)

        run_btn = ttk.Frame(shell)
        run_btn.pack(fill=tk.X, padx=8, pady=(0, 6))
        ttk.Button(run_btn, text="Quick check", command=self._run_quick_check, width=12).pack(
            side=tk.LEFT, padx=2
        )
        self.cancel_btn = ttk.Button(
            run_btn, text="Cancel", command=self._cancel_checks, width=8, state=tk.DISABLED
        )
        self.cancel_btn.pack(side=tk.LEFT, padx=2)
        ttk.Button(run_btn, text="Start migration", command=self._start_migrations, width=16).pack(
            side=tk.LEFT, padx=8
        )
        more = tk.Menubutton(run_btn, text="More", relief=tk.RAISED, width=8)
        more_menu = tk.Menu(more, tearoff=0)
        more_menu.add_command(label="Workstation checks…", command=self._run_baseline)
        more.config(menu=more_menu)
        more.pack(side=tk.LEFT, padx=2)
        opts = ttk.Frame(shell)
        opts.pack(fill=tk.X, padx=8, pady=(0, 4))
        self.skip_unc_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            opts,
            text="Fast check (skip UNC backup write probe)",
            variable=self.skip_unc_var,
        ).pack(side=tk.LEFT)
        self.auto_check_new_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            opts,
            text="Auto-check new rows",
            variable=self.auto_check_new_var,
        ).pack(side=tk.LEFT, padx=(12, 0))

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
        if self.auto_check_new_var.get():
            self.frame.after(100, lambda: self._submit_checks([iid]))
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

    def _ensure_check_executor(self) -> ThreadPoolExecutor:
        if self._check_executor is None:
            self._check_executor = ThreadPoolExecutor(max_workers=8)
        return self._check_executor

    def _check_timeout_sec(self) -> int:
        return ROW_PREREQ_FAST_TIMEOUT_SEC if self.skip_unc_var.get() else ROW_PREREQ_TIMEOUT_SEC

    def _submit_checks(self, iids: List[str]) -> None:
        if not iids:
            messagebox.showinfo("Prerequisites", "No rows to check.")
            return
        skip_unc = bool(self.skip_unc_var.get())
        timeout = self._check_timeout_sec()
        pool = self._ensure_check_executor()
        queued: List[str] = []
        with self._check_lock:
            for rid in iids:
                if rid not in self._rows or rid in self._in_flight:
                    continue
                self._in_flight.add(rid)
                queued.append(rid)
        if not queued:
            self.status_var.set("Selected row(s) already checking — wait or pick others.")
            return
        for rid in queued:
            fut = pool.submit(self._check_one_row, rid, skip_unc, timeout)
            fut.add_done_callback(lambda f, r=rid: self.frame.after(0, lambda: self._finish_check_row(f, r)))
        self.cancel_btn.config(state=tk.NORMAL)
        mode = "fast" if skip_unc else "full"
        self.status_var.set(
            f"Checking {len(queued)} row(s) ({mode}, {timeout}s max each); "
            f"{len(self._in_flight)} in flight…"
        )

    def _check_one_row(
        self, row_id: str, skip_unc: bool, timeout_sec: int
    ) -> Optional[tuple[str, Any]]:
        row = dict(self._rows.get(row_id) or {})
        if not row:
            return None
        if skip_unc:
            row["_skip_unc_probe"] = True
        return row_id, run_row_prerequisites_bounded(row, None, timeout_sec=timeout_sec)

    def _finish_check_row(self, fut: Any, row_id: str) -> None:
        with self._check_lock:
            self._in_flight.discard(row_id)
            inflight = len(self._in_flight)
        try:
            pair = fut.result()
        except Exception as ex:
            self.status_var.set(f"Check error: {str(ex)[:120]}")
            pair = None
        if pair:
            rid, res = pair
            if res is not None:
                self._results[rid] = res
                self._refresh_tree_row(rid)
        if inflight == 0:
            self.cancel_btn.config(state=tk.DISABLED)
            fails = sum(1 for r in self._results.values() if not getattr(r, "passed", False))
            self.status_var.set(
                f"Idle — {len(self._results)} result(s); {fails} failed overall."
                if fails
                else f"Idle — {len(self._results)} result(s)."
            )
        else:
            self.status_var.set(f"{inflight} row check(s) still running…")

    def _cancel_checks(self) -> None:
        with self._check_lock:
            pending = len(self._in_flight)
        if pending == 0:
            return
        if self._check_executor is not None:
            self._check_executor.shutdown(wait=False, cancel_futures=True)
            self._check_executor = None
        with self._check_lock:
            self._in_flight.clear()
        self.cancel_btn.config(state=tk.DISABLED)
        self.status_var.set(
            "Cancelled queued checks. In-flight ODBC calls may run until their timeout."
        )

    def _run_quick_check(self) -> None:
        sel = list(self.tree.selection())
        if sel:
            self._submit_checks(sel)
        else:
            self._submit_checks(list(self._rows.keys()))

    def _build_mcp_job_for_row(
        self, row: Dict[str, Any], project_path: Optional[str]
    ) -> Dict[str, Any]:
        try:
            from azure_migration_tool.src.orchestration.job_spreadsheet import (
                prerequisite_row_to_job,
            )
        except ImportError:
            from src.orchestration.job_spreadsheet import prerequisite_row_to_job

        import re
        from uuid import uuid4

        label = str(row.get("row_label") or row.get("src_db") or "db").strip()
        safe = re.sub(r"[^\w\-]+", "-", label).strip("-")[:32] or "db"
        jid = f"preq-{uuid4().hex[:8]}-{safe}"
        job = prerequisite_row_to_job(dict(row), job_id=jid)
        if project_path:
            job["project_path"] = project_path
        job["_run_origin"] = "mcp"
        return job

    def _run_prepared_mcp_job(self, job: Dict[str, Any], row_label: str = "") -> None:
        try:
            try:
                from gui.utils.run_history import (
                    record_pipeline_run_failed,
                    record_pipeline_run_started,
                )
            except ImportError:
                from azure_migration_tool.gui.utils.run_history import (
                    record_pipeline_run_failed,
                    record_pipeline_run_started,
                )

            record_pipeline_run_started(job, origin="mcp")
            try:
                from azure_migration_tool.mcp_server import _run_job_dict
            except ImportError:
                from mcp_server import _run_job_dict  # type: ignore[no-redef]
            _run_job_dict(job)
        except Exception as exc:
            try:
                record_pipeline_run_failed(job, str(exc), origin="mcp")
            except Exception:
                pass
            who = row_label or job.get("src_db") or job.get("job_id") or "?"
            err = f"{who}: {exc}"
            self.frame.after(0, lambda m=err: self.status_var.set(f"MCP job failed — {m[:120]}"))

    def _run_mcp_job_for_row(self, row: Dict[str, Any], project_path: Optional[str]) -> None:
        job = self._build_mcp_job_for_row(row, project_path)
        label = str(row.get("row_label") or row.get("src_db") or "")
        self._run_prepared_mcp_job(job, row_label=label)

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
        try:
            from azure_migration_tool.worker.__main__ import _validate_job
        except ImportError:
            from worker.__main__ import _validate_job  # type: ignore[no-redef]
        try:
            from azure_migration_tool.src.orchestration.job_defaults import (
                prepare_job_with_defaults,
            )
        except ImportError:
            from src.orchestration.job_defaults import prepare_job_with_defaults

        mw = self.main_window
        pp = getattr(mw, "_project_path", None) or None
        bundles: List[tuple[Dict[str, Any], Dict[str, Any]]] = []
        blockers: List[str] = []
        for row in configs:
            job = self._build_mcp_job_for_row(dict(row), pp)
            prepared = prepare_job_with_defaults(dict(job))
            for issue in _validate_job(prepared):
                label = row.get("row_label") or row.get("src_db")
                blockers.append(f"{label}: {issue}")
            bundles.append((dict(row), prepared))
        if blockers:
            messagebox.showerror(
                "Cannot start",
                "Fix these before migration:\n\n" + "\n".join(blockers[:12]),
            )
            return
        try:
            try:
                from gui.utils.restore_target_confirm import confirm_existing_targets_for_jobs
            except ImportError:
                from azure_migration_tool.gui.utils.restore_target_confirm import (
                    confirm_existing_targets_for_jobs,
                )
            ok_targets, drop_confirmed = confirm_existing_targets_for_jobs(
                mw.root,
                [job for _row, job in bundles],
            )
            if not ok_targets:
                self.status_var.set("Migration cancelled — destination not confirmed.")
                return
            for _row, job in bundles:
                jid = (job.get("job_id") or "").strip()
                drops = drop_confirmed.get(jid) or set()
                if drops:
                    job.setdefault("restore_options", {})
                    job["restore_options"]["drop_existing_confirmed_databases"] = sorted(drops)
            mw.root.lift()
            if hasattr(mw, "_open_agent_ops_dashboard"):
                mw._open_agent_ops_dashboard(auto_refresh=True)
            def _start_job(delay_ms: int, j: Dict[str, Any], lbl: str) -> None:
                if delay_ms > 0:
                    import time

                    time.sleep(delay_ms / 1000.0)
                self._run_prepared_mcp_job(j, row_label=lbl)

            for idx, (row, job) in enumerate(bundles):
                label = str(row.get("row_label") or row.get("src_db") or "")
                threading.Thread(
                    target=_start_job,
                    args=(idx * 150, dict(job), label),
                    daemon=True,
                ).start()
            self.status_var.set(f"Started {len(bundles)} MCP job(s). Track in AgentOps.")
        except Exception as ex:
            messagebox.showerror("Migration", str(ex))
