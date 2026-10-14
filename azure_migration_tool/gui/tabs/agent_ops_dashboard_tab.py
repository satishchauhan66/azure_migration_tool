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
    from gui.utils.run_history import clear_run_history, load_run_history, summarize_history
except ImportError:
    from azure_migration_tool.gui.tabs.backup_restore_tab import _compact_dialog_error
    from azure_migration_tool.gui.utils.run_history import (
        clear_run_history,
        load_run_history,
        summarize_history,
    )


class AgentOpsDashboardTab:
    def __init__(self, parent: tk.Widget, main_window: Any) -> None:
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self._rows: List[Dict[str, Any]] = []
        self._create_widgets()
        self.refresh()

    def _create_widgets(self) -> None:
        header = ttk.Frame(self.frame)
        header.pack(fill=tk.X, padx=8, pady=(8, 4))
        tk.Label(
            header,
            text="AgentOps — pipeline run history",
            font=("Arial", 13, "bold"),
        ).pack(side=tk.LEFT)
        ttk.Button(header, text="Refresh", command=self.refresh, width=10).pack(side=tk.RIGHT, padx=4)
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
            values=("all", "success", "failed", "completed_with_errors"),
            width=22,
            state="readonly",
        ).pack(side=tk.LEFT, padx=6)
        self.filter_var.trace_add("write", lambda *_: self._apply_filter())

        paned = ttk.PanedWindow(self.frame, orient=tk.VERTICAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        tree_frame = ttk.LabelFrame(paned, text="Recent runs (newest first)", padding=4)
        paned.add(tree_frame, weight=3)
        cols = ("started", "job_id", "env", "status", "dbs", "source", "duration", "origin")
        tree_row = ttk.Frame(tree_frame)
        tree_row.pack(fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(tree_row)
        self.tree = ttk.Treeview(
            tree_row,
            columns=cols,
            show="headings",
            height=14,
            yscrollcommand=sb.set,
        )
        sb.config(command=self.tree.yview)
        headings = {
            "started": ("Started (UTC)", 150),
            "job_id": ("Job ID", 140),
            "env": ("Env", 48),
            "status": ("Status", 110),
            "dbs": ("DBs", 40),
            "source": ("Source server", 160),
            "duration": ("Duration", 72),
            "origin": ("Origin", 56),
        }
        for c, (title, w) in headings.items():
            self.tree.heading(c, text=title)
            self.tree.column(c, width=w, anchor=tk.W if c not in ("dbs", "duration") else tk.CENTER)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        detail_frame = ttk.LabelFrame(paned, text="Run detail", padding=4)
        paned.add(detail_frame, weight=2)
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
        ttk.Button(btn_row, text="Clear history", command=self._clear_history, width=14).pack(
            side=tk.RIGHT
        )
        self.detail_text = scrolledtext.ScrolledText(detail_frame, height=10, font=("Consolas", 9))
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

    def refresh(self) -> None:
        self._rows = load_run_history(200)
        sums = summarize_history(self._rows)
        self.stat_total.set(str(sums["total"]))
        self.stat_success.set(str(sums["success"]))
        self.stat_failed.set(str(sums["failed"]))
        self.stat_partial.set(str(sums["partial"]))
        self._apply_filter()

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
            self.tree.insert(
                "",
                tk.END,
                iid=row.get("id") or started,
                values=(
                    started,
                    row.get("job_id") or "",
                    row.get("environment") or "—",
                    row.get("status") or "",
                    row.get("database_count") or 0,
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
        row = self._selected_row()
        self.detail_text.delete("1.0", tk.END)
        if not row:
            return
        lines = [json.dumps(row, indent=2)]
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
