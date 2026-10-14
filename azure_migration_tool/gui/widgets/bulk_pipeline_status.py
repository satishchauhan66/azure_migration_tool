# Author: Satish Chauhan

"""Per-database backup → upload → restore status grid for bulk Tools UI."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Callable, Dict, List, Optional

_STEP_LABELS = ("backup", "upload", "restore")
_STATUS_DISPLAY = {
    "pending": "\u2014",
    "running": "\u2026",
    "success": "OK",
    "failed": "ERR",
    "skipped": "n/a",
}


class BulkPipelineStatusPanel:
    def __init__(self, parent: tk.Widget) -> None:
        self.frame = ttk.LabelFrame(
            parent,
            text="Pipeline progress (each database moves through steps independently)",
            padding=8,
        )
        self.summary_var = tk.StringVar(value="Idle")
        ttk.Label(self.frame, textvariable=self.summary_var, anchor=tk.W).pack(fill=tk.X, pady=(0, 6))

        tree_row = ttk.Frame(self.frame)
        tree_row.pack(fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(tree_row)
        cols = ("database", "backup", "upload", "restore", "note")
        self.tree = ttk.Treeview(
            tree_row,
            columns=cols,
            show="headings",
            height=8,
            yscrollcommand=sb.set,
        )
        self.tree.heading("database", text="Database")
        self.tree.heading("backup", text="1 Backup")
        self.tree.heading("upload", text="2 Upload")
        self.tree.heading("restore", text="3 Restore")
        self.tree.heading("note", text="Last update")
        self.tree.column("database", width=220, anchor=tk.W)
        for c in ("backup", "upload", "restore"):
            self.tree.column(c, width=72, anchor=tk.CENTER)
        self.tree.column("note", width=280, anchor=tk.W)
        sb.config(command=self.tree.yview)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        self._item_by_db: Dict[str, str] = {}
        self._state: Dict[str, Dict[str, str]] = {}

    def pack(self, **kwargs) -> None:
        self.frame.pack(**kwargs)

    def reset(
        self,
        databases: List[str],
        *,
        do_backup: bool = True,
        do_upload: bool = True,
        do_restore: bool = True,
    ) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._item_by_db.clear()
        self._state.clear()
        for db in databases:
            row = {
                "backup": "pending" if do_backup else "skipped",
                "upload": "pending" if do_upload else "skipped",
                "restore": "pending" if do_restore else "skipped",
                "note": "",
            }
            self._state[db] = row
            iid = self.tree.insert(
                "",
                tk.END,
                values=(
                    db,
                    _STATUS_DISPLAY[row["backup"]],
                    _STATUS_DISPLAY[row["upload"]],
                    _STATUS_DISPLAY[row["restore"]],
                    "",
                ),
            )
            self._item_by_db[db] = iid
        self._refresh_summary()

    def set_step(self, database: str, step: str, status: str, detail: str = "") -> None:
        if step not in _STEP_LABELS or database not in self._item_by_db:
            return
        self._state[database][step] = status
        if detail:
            self._state[database]["note"] = detail[:200]
        row = self._state[database]
        self.tree.item(
            self._item_by_db[database],
            values=(
                database,
                _STATUS_DISPLAY.get(row["backup"], row["backup"]),
                _STATUS_DISPLAY.get(row["upload"], row["upload"]),
                _STATUS_DISPLAY.get(row["restore"], row["restore"]),
                row.get("note") or "",
            ),
        )
        self._refresh_summary()

    def apply_pipeline_rows(self, rows: List[Dict]) -> None:
        for row in rows or []:
            db = (row.get("database") or "").strip()
            if not db:
                continue
            for step in _STEP_LABELS:
                st = row.get(step)
                if st:
                    self.set_step(db, step, st, "; ".join(row.get("errors") or [])[:200])

    def _refresh_summary(self) -> None:
        if not self._state:
            self.summary_var.set("Idle")
            return
        done = 0
        failed = 0
        active = 0
        total = len(self._state)
        for row in self._state.values():
            steps = [row.get(s, "pending") for s in _STEP_LABELS if row.get(s) != "skipped"]
            if any(s == "running" for s in steps):
                active += 1
            if any(s == "failed" for s in steps):
                failed += 1
            if steps and all(s in ("success", "skipped") for s in steps):
                done += 1
        self.summary_var.set(
            f"{total} database(s) — {done} finished, {active} in progress, {failed} with errors"
        )


def make_ui_progress_callback(
    panel: BulkPipelineStatusPanel,
    schedule: Callable[[Callable[[], None]], None],
) -> Callable[[str, str, str, str], None]:
    """Thread-safe: schedule panel updates on the UI thread."""

    def progress(database: str, step: str, status: str, detail: str = "") -> None:
        schedule(lambda: panel.set_step(database, step, status, detail))

    return progress
