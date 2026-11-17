# Author: Satish Chauhan

"""AgentOps — run history and pipeline dashboard."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk
import tkinter as tk
from typing import Any, Dict, List, Optional

try:
    from gui.tabs.backup_restore_tab import _compact_dialog_error
    from gui.utils.agent_ops_progress import enrich_step_state, format_step_chips
    from gui.utils.run_history import clear_run_history, load_run_history, summarize_history
except ImportError:
    from azure_migration_tool.gui.tabs.backup_restore_tab import _compact_dialog_error
    from azure_migration_tool.gui.utils.agent_ops_progress import enrich_step_state, format_step_chips
    from azure_migration_tool.gui.utils.run_history import (
        clear_run_history,
        load_run_history,
        summarize_history,
    )


_AUTO_REFRESH_MS = 30_000
_LIVE_POLL_MS = 1_500


class AgentOpsDashboardTab:
    def __init__(self, parent: tk.Widget, main_window: Any) -> None:
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self._rows: List[Dict[str, Any]] = []
        self._auto_refresh_id: Optional[str] = None
        self._live_poll_id: Optional[str] = None
        self._auto_refresh_ms = _AUTO_REFRESH_MS
        self._create_widgets()
        self.refresh()
        if getattr(self.main_window, "_pending_agent_ops_auto_refresh", False):
            self.main_window._pending_agent_ops_auto_refresh = False
            self.auto_refresh_var.set(True)
            top = self.frame.winfo_toplevel()
            top.bind("<Destroy>", self._stop_auto_refresh, add="+")
            self._schedule_auto_refresh(self._auto_refresh_ms)

    def _create_widgets(self) -> None:
        header = ttk.Frame(self.frame)
        header.pack(fill=tk.X, padx=8, pady=(8, 4))
        tk.Label(
            header,
            text="AgentOps — pipeline run history",
            font=("Arial", 13, "bold"),
        ).pack(side=tk.LEFT)
        self.auto_refresh_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            header,
            text="Auto-refresh (30s)",
            variable=self.auto_refresh_var,
            command=self._on_auto_refresh_toggle,
        ).pack(side=tk.RIGHT, padx=4)
        ttk.Button(header, text="Refresh", command=lambda: self.refresh(reload_detail=True), width=10).pack(
            side=tk.RIGHT, padx=4
        )
        ttk.Button(header, text="Playbooks…", command=self._show_playbooks, width=12).pack(
            side=tk.RIGHT, padx=4
        )
        ttk.Button(header, text="Open Agent Workflow", command=self._open_workflow, width=18).pack(
            side=tk.RIGHT, padx=4
        )

        stats = ttk.LabelFrame(self.frame, text="Summary", padding=8)
        stats.pack(fill=tk.X, padx=8, pady=4)
        self.stat_total = tk.StringVar(value="0")
        self.stat_success = tk.StringVar(value="0")
        self.stat_failed = tk.StringVar(value="0")
        self.stat_partial = tk.StringVar(value="0")
        for label, var, col in (
            ("Runs", self.stat_total, 0),
            ("Success", self.stat_success, 1),
            ("Failed", self.stat_failed, 2),
            ("Partial / warnings", self.stat_partial, 3),
        ):
            cell = ttk.Frame(stats)
            cell.grid(row=0, column=col, padx=16, sticky=tk.W)
            ttk.Label(cell, text=label, font=("Arial", 9)).pack(anchor=tk.W)
            ttk.Label(cell, textvariable=var, font=("Arial", 14, "bold")).pack(anchor=tk.W)

        filt = ttk.Frame(self.frame)
        filt.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(filt, text="Filter status:").pack(side=tk.LEFT)
        self.filter_var = tk.StringVar(value="all")
        ttk.Combobox(
            filt,
            textvariable=self.filter_var,
            values=("all", "running", "success", "failed", "completed_with_errors"),
            width=22,
            state="readonly",
        ).pack(side=tk.LEFT, padx=6)
        self.filter_var.trace_add("write", lambda *_: self._apply_filter())

        paned = ttk.PanedWindow(self.frame, orient=tk.VERTICAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        tree_frame = ttk.LabelFrame(paned, text="Recent runs (newest first)", padding=4)
        paned.add(tree_frame, weight=3)
        cols = (
            "started",
            "status",
            "step_backup",
            "step_upload",
            "step_restore",
            "src_db",
            "dest_db",
            "dest_server",
            "source",
            "duration",
            "origin",
        )
        tree_row = ttk.Frame(tree_frame)
        tree_row.pack(fill=tk.BOTH, expand=True)
        tree_row.grid_rowconfigure(0, weight=1)
        tree_row.grid_columnconfigure(0, weight=1)
        vsb = ttk.Scrollbar(tree_row, orient=tk.VERTICAL)
        hsb = ttk.Scrollbar(tree_row, orient=tk.HORIZONTAL)
        self.tree = ttk.Treeview(
            tree_row,
            columns=cols,
            show="headings",
            height=14,
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set,
        )
        vsb.config(command=self.tree.yview)
        hsb.config(command=self.tree.xview)
        headings = {
            "started": ("Started (UTC)", 130),
            "status": ("Status", 88),
            "step_backup": ("Backup", 62),
            "step_upload": ("Upload", 62),
            "step_restore": ("Restore", 62),
            "src_db": ("Src DB", 100),
            "dest_db": ("Dest DB", 100),
            "dest_server": ("Dest server", 140),
            "source": ("Src server", 120),
            "duration": ("Duration", 64),
            "origin": ("Origin", 48),
        }
        center_cols = ("duration", "status", "step_backup", "step_upload", "step_restore")
        for c, (title, w) in headings.items():
            self.tree.heading(c, text=title)
            self.tree.column(
                c,
                width=w,
                anchor=tk.CENTER if c in center_cols else tk.W,
            )
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        detail_frame = ttk.LabelFrame(paned, text="Run detail", padding=4)
        paned.add(detail_frame, weight=2)
        steps = ttk.LabelFrame(detail_frame, text="Steps", padding=6)
        steps.pack(fill=tk.X, pady=(0, 6))
        self._step_bars: Dict[str, ttk.Progressbar] = {}
        self._step_labels: Dict[str, tk.StringVar] = {}
        for key, title in (("backup", "Backup"), ("upload", "Upload"), ("restore", "Restore")):
            row = ttk.Frame(steps)
            row.pack(fill=tk.X, pady=2)
            ttk.Label(row, text=title, width=8).pack(side=tk.LEFT)
            bar = ttk.Progressbar(row, length=220, mode="determinate", maximum=100)
            bar.pack(side=tk.LEFT, padx=(4, 8), fill=tk.X, expand=True)
            st = tk.StringVar(value="—")
            ttk.Label(row, textvariable=st, width=10).pack(side=tk.LEFT)
            self._step_bars[key] = bar
            self._step_labels[key] = st
        self.error_preview_var = tk.StringVar(value="")
        tk.Label(
            detail_frame,
            textvariable=self.error_preview_var,
            fg="#a93226",
            wraplength=880,
            justify=tk.LEFT,
        ).pack(fill=tk.X, pady=(0, 4))
        ttk.Label(detail_frame, text="Live log (newest at bottom)").pack(anchor=tk.W)
        self.log_text = scrolledtext.ScrolledText(
            detail_frame, height=8, font=("Consolas", 9), state=tk.DISABLED
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=(2, 6))
        btn_row = ttk.Frame(detail_frame)
        btn_row.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(btn_row, text="Open result JSON", command=self._open_result, width=16).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(btn_row, text="Open project folder", command=self._open_project, width=18).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Button(btn_row, text="Export migration brief…", command=self._export_brief, width=20).pack(
            side=tk.LEFT, padx=6
        )
        self.retry_btn = ttk.Button(
            btn_row, text="Retry failed step…", command=self._retry_failed_step, width=18
        )
        self.retry_btn.pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_row, text="Clear history", command=self._clear_history, width=14).pack(
            side=tk.RIGHT
        )
        ttk.Label(detail_frame, text="Summary / JSON").pack(anchor=tk.W)
        self.detail_text = scrolledtext.ScrolledText(detail_frame, height=6, font=("Consolas", 9))
        self.detail_text.pack(fill=tk.BOTH, expand=True)

        tk.Label(
            self.frame,
            text="Every pipeline run (GUI, worker CLI, MCP) is recorded here when a result file is written.",
            fg="gray",
            wraplength=900,
            justify=tk.LEFT,
        ).pack(fill=tk.X, padx=8, pady=(0, 8))

    def _show_playbooks(self) -> None:
        try:
            from src.orchestration.playbooks import bundled_playbooks_dir, list_playbook_paths, load_playbook
        except ImportError:
            from azure_migration_tool.src.orchestration.playbooks import (
                bundled_playbooks_dir,
                list_playbook_paths,
                load_playbook,
            )
        lines = [f"Bundled: {bundled_playbooks_dir()}", ""]
        for path in list_playbook_paths():
            try:
                pb = load_playbook(path)
                lines.append(f"• {pb.get('name') or path.name}")
                lines.append(f"  {path}")
                if pb.get("description"):
                    lines.append(f"  {pb['description']}")
            except (OSError, json.JSONDecodeError, ValueError):
                lines.append(f"• (invalid) {path}")
        lines.append("")
        lines.append("MCP: amt_list_playbooks, amt_run_playbook(path, variables_json, dry_run)")
        messagebox.showinfo("Playbooks", "\n".join(lines))

    def _open_workflow(self) -> None:
        if hasattr(self.main_window, "_open_agent_workflow"):
            self.main_window._open_agent_workflow()

    def _on_auto_refresh_toggle(self) -> None:
        if self.auto_refresh_var.get():
            if self._auto_refresh_id is None:
                self._schedule_auto_refresh(self._auto_refresh_ms)
        else:
            self._stop_auto_refresh()

    def _schedule_auto_refresh(self, interval_ms: int) -> None:
        self._auto_refresh_ms = interval_ms
        self._stop_auto_refresh()

        def tick() -> None:
            if not self.frame.winfo_exists():
                return
            if not self.auto_refresh_var.get():
                self._auto_refresh_id = None
                return
            self.refresh(reload_detail=False)
            self._auto_refresh_id = self.frame.after(interval_ms, tick)

        self._auto_refresh_id = self.frame.after(interval_ms, tick)

    def _stop_auto_refresh(self, _event: Optional[tk.Event] = None) -> None:
        aid = self._auto_refresh_id
        self._auto_refresh_id = None
        if aid and self.frame.winfo_exists():
            try:
                self.frame.after_cancel(aid)
            except tk.TclError:
                pass

    def _stop_live_poll(self) -> None:
        pid = self._live_poll_id
        self._live_poll_id = None
        if pid and self.frame.winfo_exists():
            try:
                self.frame.after_cancel(pid)
            except tk.TclError:
                pass

    def _row_is_live(self, row: Dict[str, Any]) -> bool:
        return (row.get("status") or "").lower() in ("running", "started", "queued")

    def _schedule_live_poll(self) -> None:
        self._stop_live_poll()
        if not self.frame.winfo_exists():
            return
        row = self._selected_row()
        if not row or not self._row_is_live(row):
            return

        def tick() -> None:
            if not self.frame.winfo_exists():
                return
            self._poll_live_detail()
            sel = self._selected_row()
            if sel and self._row_is_live(sel):
                self._live_poll_id = self.frame.after(_LIVE_POLL_MS, tick)
            else:
                self._live_poll_id = None

        self._live_poll_id = self.frame.after(_LIVE_POLL_MS, tick)

    def _poll_live_detail(self) -> None:
        sel = list(self.tree.selection())
        sel_id = sel[0] if sel else None
        fresh = load_run_history(200)
        row = None
        if sel_id:
            for r in fresh:
                if r.get("id") == sel_id:
                    row = r
                    break
        if row is None:
            return
        for i, cached in enumerate(self._rows):
            if cached.get("id") == sel_id:
                self._rows[i] = row
                break
        iid = row.get("id") or sel_id
        if iid and self.tree.exists(iid):
            self.tree.set(iid, "step_backup", self._step_cell(row, "backup"))
            self.tree.set(iid, "step_upload", self._step_cell(row, "upload"))
            self.tree.set(iid, "step_restore", self._step_cell(row, "restore"))
            self.tree.set(iid, "status", row.get("status") or "")
        self._update_step_bars(row)
        self._update_log_tail(row, scroll=False)

    def _effective_step_state(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return enrich_step_state(row.get("step_state"), list(row.get("log_lines") or []))

    def refresh(self, *, reload_detail: bool = True) -> None:
        sel = list(self.tree.selection())
        sel_id = sel[0] if sel else None
        self._rows = load_run_history(200)
        sums = summarize_history(self._rows)
        self.stat_total.set(str(sums["total"]))
        self.stat_success.set(str(sums["success"]))
        self.stat_failed.set(str(sums["failed"]))
        self.stat_partial.set(str(sums["partial"]))
        self._apply_filter()
        if sel_id and self.tree.exists(sel_id):
            self.tree.selection_set(sel_id)
            try:
                self.tree.see(sel_id)
            except tk.TclError:
                pass
            if reload_detail:
                self._on_select()
            else:
                row = self._selected_row()
                if row:
                    self._update_step_bars(row)
                    self._update_log_tail(row, scroll=False)
                    self._update_retry_button(row)
                    if self._row_is_live(row):
                        self._schedule_live_poll()

    def _progress_display(self, row: Dict[str, Any]) -> str:
        state = self._effective_step_state(row)
        if state:
            chips = format_step_chips(state)
            if chips:
                return chips
        progress = (row.get("progress") or "").strip()
        if progress:
            return progress
        if (row.get("status") or "").lower() == "running":
            step = (row.get("current_step") or "").strip()
            return f"Running{(': ' + step) if step else '…'}"
        return "—"

    def _step_cell(self, row: Dict[str, Any], step: str) -> str:
        state = self._effective_step_state(row)
        slot = state.get(step) if isinstance(state.get(step), dict) else {}
        st = (slot.get("status") or "pending").lower()
        pct = slot.get("percent")
        if st == "success":
            return "OK"
        if st == "failed":
            return "FAIL"
        if st == "skipped":
            return "skip"
        if st == "running":
            if pct is not None:
                try:
                    return f"{int(round(float(pct)))}%"
                except (TypeError, ValueError):
                    pass
            return "…"
        if (row.get("status") or "").lower() in ("success", "failed", "completed_with_errors"):
            prog = (row.get("progress") or "").lower()
            if f"{step}:" in prog or f"{step} " in prog:
                if "success" in prog or ":ok" in prog.replace(" ", ""):
                    return "OK"
        return "—"

    def _update_step_bars(self, row: Dict[str, Any]) -> None:
        state = self._effective_step_state(row)
        for key, bar in self._step_bars.items():
            if str(bar.cget("mode")) != "determinate":
                bar.configure(mode="determinate")
            slot = state.get(key) if isinstance(state.get(key), dict) else {}
            st = (slot.get("status") or "pending").lower()
            pct = slot.get("percent")
            label = self._step_labels[key]
            if st == "success":
                bar["value"] = 100
                label.set("OK")
            elif st == "failed":
                bar["value"] = 0
                label.set("FAIL")
            elif st == "skipped":
                bar["value"] = 0
                label.set("skip")
            elif st == "running":
                if pct is not None:
                    try:
                        val = max(0.0, min(100.0, float(pct)))
                    except (TypeError, ValueError):
                        val = 0.0
                    bar["value"] = val
                    label.set(f"{int(val)}%")
                else:
                    bar["value"] = 0
                    label.set("…" if key != "restore" else "0%")
            else:
                bar["value"] = 0
                label.set("—")

    def _update_log_tail(self, row: Dict[str, Any], *, scroll: bool = True) -> None:
        lines = list(row.get("log_lines") or [])
        err = (row.get("error_preview") or "").strip()
        self.error_preview_var.set(f"Last error: {err}" if err else "")
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        if lines:
            self.log_text.insert(tk.END, "\n".join(lines))
        else:
            self.log_text.insert(tk.END, "(no live log yet — job output appears here while running)\n")
        if scroll:
            self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)

    def _apply_filter(self) -> None:
        flt = (self.filter_var.get() or "all").strip().lower()
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self._rows:
            st = (row.get("status") or "").lower()
            if flt != "all" and st != flt:
                continue
            started = (row.get("started_utc") or row.get("recorded_utc") or "")[:19]
            dur = row.get("duration_sec")
            dur_s = f"{int(dur)}s" if dur is not None else "—"
            src_db = (row.get("source_database") or "").strip()
            dest_db = (row.get("dest_database") or "").strip()
            if not src_db:
                preview = (row.get("databases_preview") or "").strip()
                src_db = preview.split(",")[0].strip() if preview else "—"
            if not dest_db:
                dest_db = src_db if src_db != "—" else "—"
            self.tree.insert(
                "",
                tk.END,
                iid=row.get("id") or started,
                values=(
                    started,
                    row.get("status") or "",
                    self._step_cell(row, "backup"),
                    self._step_cell(row, "upload"),
                    self._step_cell(row, "restore"),
                    src_db or "—",
                    dest_db or "—",
                    row.get("dest_server") or "—",
                    row.get("source_server") or "",
                    dur_s,
                    row.get("origin") or "",
                ),
            )

    def _selected_row(self) -> Optional[Dict[str, Any]]:
        sel = self.tree.selection()
        if not sel:
            return None
        iid = sel[0]
        for row in self._rows:
            if row.get("id") == iid:
                return row
        return None

    def _on_select(self, _event: Optional[tk.Event] = None) -> None:
        self._stop_live_poll()
        row = self._selected_row()
        self.detail_text.delete("1.0", tk.END)
        if not row:
            return
        self._update_step_bars(row)
        self._update_log_tail(row)
        if self._row_is_live(row):
            self._schedule_live_poll()
        lines = [
            f"Job ID: {row.get('job_id') or '—'}",
            f"Status: {row.get('status') or '—'}",
            f"Steps: {self._progress_display(row)}",
            f"Source: {row.get('source_server') or '—'} / {row.get('source_database') or row.get('databases_preview') or '—'}",
            f"Target: {row.get('dest_server') or '—'} / {row.get('dest_database') or '—'}",
        ]
        path = (row.get("result_path") or "").strip()
        if path and Path(path).is_file():
            try:
                full = json.loads(Path(path).read_text(encoding="utf-8"))
                pipe = (full.get("steps") or {}).get("pipeline") or {}
                dbs = pipe.get("databases") or []
                if dbs:
                    lines.append("\n--- Per-database pipeline ---\n")
                    lines.append(json.dumps(dbs, indent=2))
                if full.get("errors"):
                    lines.append("\n--- Errors ---\n")
                    lines.append("\n".join(str(e) for e in full["errors"][:15]))
            except (json.JSONDecodeError, OSError) as exc:
                lines.append(f"\n(could not read result file: {exc})")
        self.detail_text.insert(tk.END, "\n".join(lines))
        self._update_retry_button(row)

    def _update_retry_button(self, row: Optional[Dict[str, Any]] = None) -> None:
        btn = getattr(self, "retry_btn", None)
        if btn is None:
            return
        row = row if row is not None else self._selected_row()
        if not row:
            btn.state(["disabled"])
            return
        st = (row.get("status") or "").lower()
        if st in ("running", "started", "queued"):
            btn.state(["disabled"])
            return
        try:
            from gui.utils.agent_ops_retry import failed_steps_from_run, load_run_artifacts
        except ImportError:
            from azure_migration_tool.gui.utils.agent_ops_retry import (
                failed_steps_from_run,
                load_run_artifacts,
            )
        job, result, err = load_run_artifacts(row)
        failed = failed_steps_from_run(result, step_state=row.get("step_state"))
        if failed and not err and job:
            btn.state(["!disabled"])
        else:
            btn.state(["disabled"])

    def _retry_failed_step(self) -> None:
        row = self._selected_row()
        if not row:
            messagebox.showinfo("Retry", "Select a run first.")
            return
        try:
            from gui.utils.agent_ops_retry import (
                build_retry_job,
                failed_steps_from_run,
                load_run_artifacts,
                run_retry_job_async,
            )
        except ImportError:
            from azure_migration_tool.gui.utils.agent_ops_retry import (
                build_retry_job,
                failed_steps_from_run,
                load_run_artifacts,
                run_retry_job_async,
            )
        try:
            from src.orchestration.job_defaults import prepare_job_with_defaults
        except ImportError:
            from azure_migration_tool.src.orchestration.job_defaults import prepare_job_with_defaults

        job, result, err = load_run_artifacts(row)
        if err:
            messagebox.showerror("Retry", err)
            return
        failed = failed_steps_from_run(result, step_state=row.get("step_state"))
        if not failed:
            messagebox.showinfo("Retry", "No failed step found for this run.")
            return

        labels = {"backup": "Backup", "upload": "Upload", "restore": "Restore"}
        if len(failed) == 1:
            step = failed[0]
            if not messagebox.askyesno(
                "Retry failed step",
                f"Retry only the {labels[step]} step for job\n{row.get('job_id') or '—'}?\n\n"
                "Earlier successful steps will be skipped.",
            ):
                return
            chosen = [step]
        else:
            dlg = tk.Toplevel(self.frame.winfo_toplevel())
            dlg.title("Retry failed step(s)")
            dlg.transient(self.frame.winfo_toplevel())
            dlg.grab_set()
            tk.Label(
                dlg,
                text="Select which failed step(s) to retry:",
                wraplength=400,
            ).pack(anchor=tk.W, padx=12, pady=(12, 6))
            vars_map: Dict[str, tk.BooleanVar] = {}
            for step in failed:
                v = tk.BooleanVar(value=True)
                vars_map[step] = v
                ttk.Checkbutton(dlg, text=labels[step], variable=v).pack(anchor=tk.W, padx=20)
            picked: Dict[str, Any] = {"ok": False}

            def _go() -> None:
                picked["ok"] = True
                dlg.destroy()

            def _no() -> None:
                dlg.destroy()

            br = ttk.Frame(dlg, padding=12)
            br.pack(fill=tk.X)
            ttk.Button(br, text="Cancel", command=_no).pack(side=tk.RIGHT, padx=4)
            ttk.Button(br, text="Retry", command=_go).pack(side=tk.RIGHT)
            dlg.wait_window()
            if not picked["ok"]:
                return
            chosen = [s for s in failed if vars_map[s].get()]
            if not chosen:
                return

        retry_job, build_err = build_retry_job(
            job, result, chosen, step_state=row.get("step_state")
        )
        if build_err or not retry_job:
            messagebox.showerror("Retry", build_err or "Could not build retry job.")
            return
        prepared = prepare_job_with_defaults(retry_job)
        if "restore" in chosen:
            try:
                from gui.utils.restore_target_confirm import confirm_existing_targets_for_jobs
            except ImportError:
                from azure_migration_tool.gui.utils.restore_target_confirm import (
                    confirm_existing_targets_for_jobs,
                )
            ok_targets, drop_confirmed = confirm_existing_targets_for_jobs(
                self.main_window.root,
                [prepared],
            )
            if not ok_targets:
                return
            jid = (prepared.get("job_id") or "").strip()
            drops = drop_confirmed.get(jid) or set()
            if drops:
                prepared.setdefault("restore_options", {})
                prepared["restore_options"]["drop_existing_confirmed_databases"] = sorted(drops)

        def _done() -> None:
            if self.frame.winfo_exists():
                self.frame.after(0, lambda: self.refresh(reload_detail=True))

        run_retry_job_async(prepared, on_done=_done)
        self.auto_refresh_var.set(True)
        self._schedule_auto_refresh(self._auto_refresh_ms)
        if hasattr(self.main_window, "_open_agent_ops_dashboard"):
            try:
                self.refresh(reload_detail=True)
            except Exception:
                pass
        messagebox.showinfo(
            "Retry started",
            f"Retrying: {', '.join(labels[s] for s in chosen)}\n"
            f"New job id: {prepared.get('job_id')}",
        )

    def _open_result(self) -> None:
        row = self._selected_row()
        if not row:
            messagebox.showinfo("Run history", "Select a run first.")
            return
        path = (row.get("result_path") or "").strip()
        if not path or not Path(path).is_file():
            messagebox.showerror("Run history", "Result file not found.")
            return
        try:
            os.startfile(path)  # type: ignore[attr-defined]
        except AttributeError:
            subprocess.Popen(["open", path])
        except OSError as exc:
            messagebox.showerror("Run history", _compact_dialog_error(str(exc)))

    def _open_project(self) -> None:
        row = self._selected_row()
        if not row:
            messagebox.showinfo("Run history", "Select a run first.")
            return
        path = (row.get("project_path") or "").strip()
        if not path:
            rp = (row.get("result_path") or "").strip()
            if rp:
                path = str(Path(rp).parent)
        if not path or not Path(path).exists():
            messagebox.showerror("Run history", "Project folder not found.")
            return
        try:
            subprocess.Popen(["explorer", path])
        except OSError as exc:
            messagebox.showerror("Run history", _compact_dialog_error(str(exc)))

    def _export_brief(self) -> None:
        row = self._selected_row()
        if not row:
            messagebox.showinfo("Migration brief", "Select a run first.")
            return
        job_id = (row.get("job_id") or "migration").strip()
        default_name = f"migration_brief_{job_id}.md"
        path = filedialog.asksaveasfilename(
            title="Export migration brief",
            defaultextension=".md",
            initialfile=default_name,
            filetypes=[("Markdown", "*.md"), ("JSON", "*.json")],
        )
        if not path:
            return
        try:
            try:
                from src.orchestration.migration_brief import export_migration_brief
            except ImportError:
                from azure_migration_tool.src.orchestration.migration_brief import (
                    export_migration_brief,
                )
            fmt = "json" if path.lower().endswith(".json") else "markdown"
            out = export_migration_brief(
                path,
                result_path=row.get("result_path"),
                history_entry=row,
                format=fmt,
            )
            messagebox.showinfo("Migration brief", f"Saved:\n{out}")
        except Exception as exc:
            messagebox.showerror("Migration brief", _compact_dialog_error(str(exc)))

    def _clear_history(self) -> None:
        if not messagebox.askyesno("Clear history", "Remove all stored run history entries?"):
            return
        clear_run_history()
        self.refresh()
        self.detail_text.delete("1.0", tk.END)
