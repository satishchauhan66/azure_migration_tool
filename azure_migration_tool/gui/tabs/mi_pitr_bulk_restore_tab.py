# -*- coding: utf-8 -*-
"""
Azure SQL Managed Instance — bulk point-in-time restore (cross-instance ARM).

Select multiple databases on one source MI and restore them sequentially to one target MI.
"""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from pathlib import Path
import sys
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll

try:
    from gui.tabs.mi_pitr_restore_tab import (
        _ArmPicker,
        _combo_values_from_subscriptions,
        _mi_instance_label,
    )
except ImportError:
    from azure_migration_tool.gui.tabs.mi_pitr_restore_tab import (
        _ArmPicker,
        _combo_values_from_subscriptions,
        _mi_instance_label,
    )

try:
    from src.azure_mgmt.mi_pitr_restore import (
        delete_managed_database,
        get_managed_instance_location,
        list_managed_databases,
        list_managed_instances_in_resource_group,
        list_resource_groups,
        list_subscriptions,
        managed_database_id,
        normalize_restore_point_in_time,
        poll_async_operation,
        resolve_restore_point_in_time,
        start_point_in_time_restore,
        AUTO_RESTORE_POINT_KEYWORDS,
    )
except ImportError:
    from azure_migration_tool.src.azure_mgmt.mi_pitr_restore import (
        delete_managed_database,
        get_managed_instance_location,
        list_managed_databases,
        list_managed_instances_in_resource_group,
        list_resource_groups,
        list_subscriptions,
        managed_database_id,
        normalize_restore_point_in_time,
        poll_async_operation,
        resolve_restore_point_in_time,
        start_point_in_time_restore,
        AUTO_RESTORE_POINT_KEYWORDS,
    )

try:
    from utils.azure_shared_credential import get_shared_azure_credential
except ImportError:
    try:
        from azure_migration_tool.utils.azure_shared_credential import get_shared_azure_credential
    except ImportError:
        get_shared_azure_credential = None  # type: ignore


class MiPitrBulkRestoreTab:
    """Tab: bulk PITR restore of multiple managed databases to another managed instance."""

    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None
        self._busy = False
        self._subscriptions: List[Dict[str, str]] = []
        self._src_rgs: List[str] = []
        self._src_mis: List[Dict[str, str]] = []
        self._src_dbs: List[str] = []
        self._tgt_rgs: List[str] = []
        self._tgt_mis: List[Dict[str, str]] = []
        self._create_widgets()

    def set_project_path(self, project_path):
        self.project_path = project_path

    def _log(self, msg: str) -> None:
        self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)

    def _set_busy(self, busy: bool) -> None:
        self._busy = busy
        btn_state = tk.DISABLED if busy else tk.NORMAL
        cb_state = "disabled" if busy else "normal"
        for w in (
            self.btn_load_subs,
            self.btn_test,
            self.btn_start,
            self.btn_copy_sel,
            self.btn_select_all,
            self.btn_clear_sel,
            self.btn_apply_naming,
        ):
            w.config(state=btn_state)
        for cb in (
            self.src_sub_cb,
            self.src_rg_cb,
            self.src_mi_cb,
            self.tgt_sub_cb,
            self.tgt_rg_cb,
            self.tgt_mi_cb,
        ):
            cb.set_picker_state(cb_state)
        if busy:
            self._cancel_edit()
            self.db_tree.state(["disabled"])
        else:
            self.db_tree.state(["!disabled"])

    def _create_widgets(self) -> None:
        canvas = tk.Canvas(self.frame)
        scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=canvas.yview)
        scrollable = ttk.Frame(canvas)
        scrollable.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        cw = canvas.create_window((0, 0), window=scrollable, anchor="nw")

        def _on_canvas_configure(event):
            canvas.itemconfig(cw, width=event.width)

        canvas.bind("<Configure>", _on_canvas_configure)
        canvas.configure(yscrollcommand=scrollbar.set)
        bind_canvas_vertical_scroll(canvas, scrollable)

        tk.Label(
            scrollable,
            text="MI database — bulk point-in-time restore (cross-instance)",
            font=("Arial", 16, "bold"),
        ).pack(pady=(10, 4))
        tk.Label(
            scrollable,
            text=(
                "Pick source and target managed instances, tick the databases to restore, and edit each "
                "target name in place (double-click the Target name cell). All restores run one after "
                "another in a single Azure session, so you sign in only once. Each restore uses the same "
                "restore point."
            ),
            fg="gray",
            wraplength=780,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, padx=10)

        top_btn = ttk.Frame(scrollable)
        top_btn.pack(fill=tk.X, padx=10, pady=(8, 4))
        self.btn_load_subs = ttk.Button(
            top_btn,
            text="Load subscriptions from Azure",
            command=self._on_load_subscriptions,
        )
        self.btn_load_subs.pack(side=tk.LEFT)

        src = ttk.LabelFrame(scrollable, text="Source (managed instance)", padding=10)
        src.pack(fill=tk.X, padx=10, pady=8)
        self.src_sub_cb = _ArmPicker(
            src,
            width_chars=78,
            get_choices=lambda: _combo_values_from_subscriptions(self._subscriptions),
        )
        self.src_rg_cb = _ArmPicker(src, width_chars=78, get_choices=lambda: tuple(self._src_rgs))
        self.src_mi_cb = _ArmPicker(
            src,
            width_chars=78,
            get_choices=lambda: tuple(_mi_instance_label(m) for m in self._src_mis),
        )
        self._grid_combo_row(src, 0, "Subscription", self.src_sub_cb)
        self._grid_combo_row(src, 1, "Resource group", self.src_rg_cb)
        self._grid_combo_row(src, 2, "Managed instance", self.src_mi_cb)
        src.columnconfigure(1, weight=1)
        self.src_sub_cb.bind("<<ComboboxSelected>>", lambda e: self._on_src_sub_changed())
        self.src_rg_cb.bind("<<ComboboxSelected>>", lambda e: self._on_src_rg_changed())
        self.src_mi_cb.bind("<<ComboboxSelected>>", lambda e: self._on_src_mi_changed())

        db_frame = ttk.LabelFrame(
            scrollable,
            text="Databases to restore (tick rows, double-click Target name to edit)",
            padding=10,
        )
        db_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)
        db_inner = ttk.Frame(db_frame)
        db_inner.pack(fill=tk.BOTH, expand=True)
        db_sb = ttk.Scrollbar(db_inner)
        self.db_tree = ttk.Treeview(
            db_inner,
            columns=("check", "source", "target"),
            show="headings",
            selectmode="browse",
            height=10,
            yscrollcommand=db_sb.set,
        )
        self.db_tree.heading("check", text="\u2713")
        self.db_tree.heading("source", text="Source database")
        self.db_tree.heading("target", text="Target name (double-click to edit)")
        self.db_tree.column("check", width=36, anchor=tk.CENTER, stretch=False)
        self.db_tree.column("source", width=320, anchor=tk.W)
        self.db_tree.column("target", width=320, anchor=tk.W)
        db_sb.config(command=self.db_tree.yview)
        self.db_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        db_sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.db_tree.bind("<Button-1>", self._on_tree_click)
        self.db_tree.bind("<Double-1>", self._on_tree_double_click)
        # Inline editor state for the Target name column.
        self._edit_entry: Optional[ttk.Entry] = None
        self._edit_item: Optional[str] = None
        db_btn_row = ttk.Frame(db_frame)
        db_btn_row.pack(fill=tk.X, pady=(6, 0))
        self.btn_select_all = ttk.Button(db_btn_row, text="Select all", command=self._select_all_dbs)
        self.btn_select_all.pack(side=tk.LEFT, padx=(0, 6))
        self.btn_clear_sel = ttk.Button(db_btn_row, text="Clear selection", command=self._clear_db_selection)
        self.btn_clear_sel.pack(side=tk.LEFT)

        tgt = ttk.LabelFrame(scrollable, text="Target (destination managed instance)", padding=10)
        tgt.pack(fill=tk.X, padx=10, pady=8)
        self.tgt_sub_cb = _ArmPicker(
            tgt,
            width_chars=78,
            get_choices=lambda: _combo_values_from_subscriptions(self._subscriptions),
        )
        self.tgt_rg_cb = _ArmPicker(tgt, width_chars=78, get_choices=lambda: tuple(self._tgt_rgs))
        self.tgt_mi_cb = _ArmPicker(
            tgt,
            width_chars=78,
            get_choices=lambda: tuple(_mi_instance_label(m) for m in self._tgt_mis),
        )
        self._grid_combo_row(tgt, 0, "Subscription", self.tgt_sub_cb)
        self._grid_combo_row(tgt, 1, "Resource group", self.tgt_rg_cb)
        self._grid_combo_row(tgt, 2, "Managed instance", self.tgt_mi_cb)
        tgt.columnconfigure(1, weight=1)
        self.tgt_sub_cb.bind("<<ComboboxSelected>>", lambda e: self._on_tgt_sub_changed())
        self.tgt_rg_cb.bind("<<ComboboxSelected>>", lambda e: self._on_tgt_rg_changed())
        self.tgt_mi_cb.bind("<<ComboboxSelected>>", lambda e: self._on_tgt_mi_changed())

        naming = ttk.LabelFrame(
            scrollable,
            text="Target name helper (fills the Target name column; you can still edit each row)",
            padding=10,
        )
        naming.pack(fill=tk.X, padx=10, pady=8)
        self.naming_mode_var = tk.StringVar(value="same")
        ttk.Radiobutton(
            naming,
            text="Same name as source",
            variable=self.naming_mode_var,
            value="same",
        ).grid(row=0, column=0, sticky=tk.W, padx=4, pady=2)
        ttk.Radiobutton(
            naming,
            text="Prefix:",
            variable=self.naming_mode_var,
            value="prefix",
        ).grid(row=1, column=0, sticky=tk.W, padx=4, pady=2)
        self.name_prefix_var = tk.StringVar()
        ttk.Entry(naming, textvariable=self.name_prefix_var, width=40).grid(
            row=1, column=1, sticky=tk.W, padx=4, pady=2
        )
        ttk.Radiobutton(
            naming,
            text="Suffix:",
            variable=self.naming_mode_var,
            value="suffix",
        ).grid(row=2, column=0, sticky=tk.W, padx=4, pady=2)
        self.name_suffix_var = tk.StringVar()
        ttk.Entry(naming, textvariable=self.name_suffix_var, width=40).grid(
            row=2, column=1, sticky=tk.W, padx=4, pady=2
        )
        self.btn_apply_naming = ttk.Button(
            naming, text="Apply to all rows", command=self._apply_naming_to_all
        )
        self.btn_apply_naming.grid(row=3, column=0, sticky=tk.W, padx=4, pady=(6, 2))

        opts = ttk.LabelFrame(scrollable, text="Restore options", padding=10)
        opts.pack(fill=tk.X, padx=10, pady=8)
        self.restore_time_var = tk.StringVar(value="latest")
        self.poll_sec_var = tk.StringVar(value="15")
        self.timeout_sec_var = tk.StringVar(value="7200")
        self.continue_on_error_var = tk.BooleanVar(value=True)
        self._grid_entry_row(
            opts,
            0,
            "Restore point ('latest', or UTC if no offset)",
            self.restore_time_var,
        )
        self._grid_entry_row(opts, 1, "Poll interval (seconds)", self.poll_sec_var)
        self._grid_entry_row(opts, 2, "Max wait per database (seconds)", self.timeout_sec_var)
        ttk.Checkbutton(
            opts,
            text="Continue on error (skip failed DB and proceed to next)",
            variable=self.continue_on_error_var,
        ).grid(row=3, column=0, columnspan=2, sticky=tk.W, padx=4, pady=4)
        opts.columnconfigure(1, weight=1)

        btn_row = ttk.Frame(scrollable)
        btn_row.pack(fill=tk.X, padx=10, pady=8)
        self.btn_test = ttk.Button(btn_row, text="Test: resolve target MI location", command=self._on_test_click)
        self.btn_test.pack(side=tk.LEFT, padx=(0, 8))
        self.btn_start = ttk.Button(
            btn_row,
            text="Start bulk restore + poll each until done",
            command=self._on_start_bulk_click,
        )
        self.btn_start.pack(side=tk.LEFT, padx=(0, 8))
        self.btn_copy_sel = ttk.Button(
            btn_row, text="Copy source selection → target", command=self._copy_source_to_target
        )
        self.btn_copy_sel.pack(side=tk.LEFT)

        log_frame = ttk.LabelFrame(scrollable, text="Log", padding=6)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)
        self.log = scrolledtext.ScrolledText(log_frame, height=14, wrap=tk.WORD, font=("Consolas", 9))
        self.log.pack(fill=tk.BOTH, expand=True)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _grid_combo_row(self, parent, row: int, label: str, combo: ttk.Widget) -> None:
        ttk.Label(parent, text=label + ":").grid(row=row, column=0, sticky=tk.W, padx=4, pady=3)
        combo.grid(row=row, column=1, sticky=tk.EW, padx=4, pady=3)

    def _grid_entry_row(self, parent, row: int, label: str, var: tk.StringVar) -> None:
        ttk.Label(parent, text=label + ":").grid(row=row, column=0, sticky=tk.W, padx=4, pady=3)
        ttk.Entry(parent, textvariable=var, width=76).grid(row=row, column=1, sticky=tk.EW, padx=4, pady=3)

    def _subscription_index_from_display(self, raw: str) -> int:
        raw = (raw or "").strip()
        if not raw or not self._subscriptions:
            return -1
        vals = _combo_values_from_subscriptions(self._subscriptions)
        try:
            return vals.index(raw)
        except ValueError:
            return -1

    def _src_mi_index_from_label(self, raw: str) -> int:
        raw = (raw or "").strip()
        for i, m in enumerate(self._src_mis):
            if _mi_instance_label(m) == raw:
                return i
        return -1

    def _tgt_mi_index_from_label(self, raw: str) -> int:
        raw = (raw or "").strip()
        for i, m in enumerate(self._tgt_mis):
            if _mi_instance_label(m) == raw:
                return i
        return -1

    def _clear_src_downstream(self, from_rg: bool = False) -> None:
        if not from_rg:
            self._src_rgs = []
            self.src_rg_cb.set("")
            self.src_rg_cb.refresh_suggestions()
        self._src_mis = []
        self.src_mi_cb.set("")
        self.src_mi_cb.refresh_suggestions()
        self._src_dbs = []
        self._refresh_db_listbox()

    def _clear_tgt_downstream(self, from_rg: bool = False) -> None:
        if not from_rg:
            self._tgt_rgs = []
            self.tgt_rg_cb.set("")
            self.tgt_rg_cb.refresh_suggestions()
        self._tgt_mis = []
        self.tgt_mi_cb.set("")
        self.tgt_mi_cb.refresh_suggestions()

    # Glyphs for the "check" column (Treeview has no native checkbox).
    _CHECKED = "\u2611"  # ☑
    _UNCHECKED = "\u2610"  # ☐

    def _refresh_db_listbox(self) -> None:
        """Rebuild the database tree from self._src_dbs (all rows ticked, target = naming default)."""
        self._cancel_edit()
        for item in self.db_tree.get_children():
            self.db_tree.delete(item)
        for name in self._src_dbs:
            self.db_tree.insert(
                "",
                tk.END,
                values=(self._CHECKED, name, self._target_name_for(name)),
            )

    def _select_all_dbs(self) -> None:
        for item in self.db_tree.get_children():
            self.db_tree.set(item, "check", self._CHECKED)

    def _clear_db_selection(self) -> None:
        for item in self.db_tree.get_children():
            self.db_tree.set(item, "check", self._UNCHECKED)

    def _apply_naming_to_all(self) -> None:
        """Recompute every Target name cell from the current naming-helper mode."""
        self._cancel_edit()
        for item in self.db_tree.get_children():
            src = self.db_tree.set(item, "source")
            self.db_tree.set(item, "target", self._target_name_for(src))

    def _get_checked_pairs(self) -> List[Tuple[str, str]]:
        """Return (source_db, target_name) for every ticked row."""
        pairs: List[Tuple[str, str]] = []
        for item in self.db_tree.get_children():
            if self.db_tree.set(item, "check") == self._CHECKED:
                src = self.db_tree.set(item, "source")
                tgt = (self.db_tree.set(item, "target") or "").strip()
                pairs.append((src, tgt))
        return pairs

    def _on_tree_click(self, event: tk.Event) -> None:
        """Toggle the tick when the check column is clicked."""
        if "disabled" in self.db_tree.state():
            return
        if self.db_tree.identify_region(event.x, event.y) != "cell":
            return
        if self.db_tree.identify_column(event.x) != "#1":  # "check" is the first column
            return
        item = self.db_tree.identify_row(event.y)
        if not item:
            return
        current = self.db_tree.set(item, "check")
        self.db_tree.set(item, "check", self._UNCHECKED if current == self._CHECKED else self._CHECKED)

    def _on_tree_double_click(self, event: tk.Event) -> Optional[str]:
        """Open an inline editor over the Target name cell."""
        if "disabled" in self.db_tree.state():
            return None
        if self.db_tree.identify_region(event.x, event.y) != "cell":
            return None
        if self.db_tree.identify_column(event.x) != "#3":  # "target" is the third column
            return None
        item = self.db_tree.identify_row(event.y)
        if not item:
            return None
        self._begin_edit_target(item)
        return "break"

    def _begin_edit_target(self, item: str) -> None:
        self._cancel_edit()
        bbox = self.db_tree.bbox(item, "target")
        if not bbox:
            return
        x, y, w, h = bbox
        entry = ttk.Entry(self.db_tree)
        entry.insert(0, self.db_tree.set(item, "target"))
        entry.select_range(0, tk.END)
        entry.focus_set()
        entry.place(x=x, y=y, width=w, height=h)
        entry.bind("<Return>", lambda e: self._commit_edit())
        entry.bind("<Escape>", lambda e: self._cancel_edit())
        entry.bind("<FocusOut>", lambda e: self._commit_edit())
        self._edit_entry = entry
        self._edit_item = item

    def _commit_edit(self) -> None:
        if self._edit_entry is None or self._edit_item is None:
            return
        try:
            new_value = self._edit_entry.get()
            if self.db_tree.exists(self._edit_item):
                self.db_tree.set(self._edit_item, "target", new_value)
        finally:
            self._destroy_editor()

    def _cancel_edit(self) -> None:
        self._destroy_editor()

    def _destroy_editor(self) -> None:
        entry = self._edit_entry
        # Clear state first so the <FocusOut> fired by destroy() is a no-op (no re-entrancy).
        self._edit_entry = None
        self._edit_item = None
        if entry is not None:
            try:
                entry.destroy()
            except tk.TclError:
                pass

    def _subscription_id_at_combo_index(self, idx: int) -> Optional[str]:
        if idx < 0 or idx >= len(self._subscriptions):
            return None
        return self._subscriptions[idx]["subscription_id"]

    def _target_name_for(self, src_db: str) -> str:
        mode = self.naming_mode_var.get()
        if mode == "prefix":
            return (self.name_prefix_var.get() or "") + src_db
        if mode == "suffix":
            return src_db + (self.name_suffix_var.get() or "")
        return src_db

    def _resolve_src_mi(self) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Returns (sub_id, rg, mi_name, error)."""
        sidx = self._subscription_index_from_display(self.src_sub_cb.get())
        sub = self._subscription_id_at_combo_index(sidx)
        rg = (self.src_rg_cb.get() or "").strip()
        mi_i = self._src_mi_index_from_label(self.src_mi_cb.get())
        if not self._subscriptions:
            return None, None, None, "Load subscriptions first."
        if not sub:
            return None, None, None, "Select a source subscription."
        if not rg:
            return None, None, None, "Select a source resource group."
        if mi_i < 0:
            return None, None, None, "Select a source managed instance."
        mi = self._src_mis[mi_i]["name"]
        return sub, rg, mi, None

    def _resolve_tgt_ids(self) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        sidx = self._subscription_index_from_display(self.tgt_sub_cb.get())
        sub = self._subscription_id_at_combo_index(sidx)
        rg = (self.tgt_rg_cb.get() or "").strip()
        mi_i = self._tgt_mi_index_from_label(self.tgt_mi_cb.get())
        if not sub:
            return None, None, None, "Select a target subscription."
        if not rg:
            return None, None, None, "Select a target resource group."
        if mi_i < 0:
            return None, None, None, "Select a target managed instance."
        mi = self._tgt_mis[mi_i]["name"]
        return sub, rg, mi, None

    def _on_load_subscriptions(self) -> None:
        if self._busy:
            return
        if get_shared_azure_credential is None:
            messagebox.showerror("Error", "azure-identity / utils.azure_shared_credential not available.")
            return

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log("Loading subscriptions…"))
            try:
                cred = get_shared_azure_credential(lambda m: self.frame.after(0, lambda p=m: self._log(p)))
                rows, err = list_subscriptions(cred)
                if err:
                    self.frame.after(0, lambda: self._log(f"[X] {err}"))
                    self.frame.after(0, lambda: messagebox.showerror("Azure", err))
                    return
                if not rows:
                    self.frame.after(0, lambda: self._log("[X] No subscriptions returned."))
                    self.frame.after(0, lambda: messagebox.showwarning("Azure", "No subscriptions found."))
                    return

                def apply():
                    self._subscriptions = rows
                    self.src_sub_cb.refresh_suggestions()
                    self.tgt_sub_cb.refresh_suggestions()
                    self._clear_src_downstream()
                    self._clear_tgt_downstream()
                    self.src_sub_cb.set("")
                    self.tgt_sub_cb.set("")
                    self._log(f"[OK] Loaded {len(rows)} subscription(s).")

                self.frame.after(0, apply)
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
                self.frame.after(0, lambda: messagebox.showerror("Error", str(ex)))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _on_src_sub_changed(self) -> None:
        self.src_sub_cb.restore_full_suggestions()
        idx = self._subscription_index_from_display(self.src_sub_cb.get())
        if idx < 0:
            self._clear_src_downstream()
            return
        sub_id = self._subscription_id_at_combo_index(idx)
        if not sub_id:
            self._clear_src_downstream()
            return

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log("Loading resource groups (source)…"))
            try:
                cred = get_shared_azure_credential(lambda m: self.frame.after(0, lambda p=m: self._log(p)))
                rgs, err = list_resource_groups(cred, sub_id)
                if err:
                    self.frame.after(0, lambda: self._log(f"[X] {err}"))
                    return

                def apply():
                    self._src_rgs = rgs
                    self.src_rg_cb.refresh_suggestions()
                    self._clear_src_downstream(from_rg=True)
                    self.src_rg_cb.set(rgs[0] if len(rgs) == 1 else "")
                    self._log(f"[OK] {len(rgs)} resource group(s) (source).")

                self.frame.after(0, apply)
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _on_src_rg_changed(self) -> None:
        self.src_rg_cb.restore_full_suggestions()
        sidx = self._subscription_index_from_display(self.src_sub_cb.get())
        if sidx < 0:
            self._clear_src_downstream()
            return
        sub_id = self._subscription_id_at_combo_index(sidx)
        rg = (self.src_rg_cb.get() or "").strip()
        if not sub_id or not rg:
            self._clear_src_downstream(from_rg=True)
            return

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log(f"Loading managed instances in {rg}…"))
            try:
                cred = get_shared_azure_credential(lambda m: self.frame.after(0, lambda p=m: self._log(p)))
                mis, err = list_managed_instances_in_resource_group(cred, sub_id, rg)
                if err:
                    self.frame.after(0, lambda: self._log(f"[X] {err}"))
                    return

                def apply():
                    self._src_mis = mis
                    self.src_mi_cb.refresh_suggestions()
                    self._src_dbs = []
                    self._refresh_db_listbox()
                    labels = tuple(_mi_instance_label(m) for m in mis)
                    self.src_mi_cb.set(labels[0] if len(labels) == 1 else "")
                    self._log(f"[OK] {len(mis)} managed instance(s) in {rg}.")

                self.frame.after(0, apply)
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _on_src_mi_changed(self) -> None:
        self.src_mi_cb.restore_full_suggestions()
        sidx = self._subscription_index_from_display(self.src_sub_cb.get())
        if sidx < 0:
            self._clear_src_downstream()
            return
        sub_id = self._subscription_id_at_combo_index(sidx)
        rg = (self.src_rg_cb.get() or "").strip()
        mi_idx = self._src_mi_index_from_label(self.src_mi_cb.get())
        if not sub_id or not rg or mi_idx < 0:
            self._src_dbs = []
            self._refresh_db_listbox()
            return
        mi_name = self._src_mis[mi_idx]["name"]

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log(f"Loading databases on {mi_name}…"))
            try:
                cred = get_shared_azure_credential(lambda m: self.frame.after(0, lambda p=m: self._log(p)))
                dbs, err = list_managed_databases(cred, sub_id, rg, mi_name)
                if err:
                    self.frame.after(0, lambda: self._log(f"[X] {err}"))
                    return

                def apply():
                    self._src_dbs = dbs
                    self._refresh_db_listbox()
                    self._log(f"[OK] {len(dbs)} database(s) on {mi_name} (all ticked).")

                self.frame.after(0, apply)
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _on_tgt_sub_changed(self) -> None:
        self.tgt_sub_cb.restore_full_suggestions()
        idx = self._subscription_index_from_display(self.tgt_sub_cb.get())
        if idx < 0:
            self._clear_tgt_downstream()
            return
        sub_id = self._subscription_id_at_combo_index(idx)
        if not sub_id:
            self._clear_tgt_downstream()
            return

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log("Loading resource groups (target)…"))
            try:
                cred = get_shared_azure_credential(lambda m: self.frame.after(0, lambda p=m: self._log(p)))
                rgs, err = list_resource_groups(cred, sub_id)
                if err:
                    self.frame.after(0, lambda: self._log(f"[X] {err}"))
                    return

                def apply():
                    self._tgt_rgs = rgs
                    self.tgt_rg_cb.refresh_suggestions()
                    self._clear_tgt_downstream(from_rg=True)
                    self.tgt_rg_cb.set(rgs[0] if len(rgs) == 1 else "")
                    self._log(f"[OK] {len(rgs)} resource group(s) (target).")

                self.frame.after(0, apply)
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _on_tgt_rg_changed(self) -> None:
        self.tgt_rg_cb.restore_full_suggestions()
        sidx = self._subscription_index_from_display(self.tgt_sub_cb.get())
        if sidx < 0:
            self._clear_tgt_downstream()
            return
        sub_id = self._subscription_id_at_combo_index(sidx)
        rg = (self.tgt_rg_cb.get() or "").strip()
        if not sub_id or not rg:
            self._clear_tgt_downstream(from_rg=True)
            return

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log(f"Loading managed instances (target) in {rg}…"))
            try:
                cred = get_shared_azure_credential(lambda m: self.frame.after(0, lambda p=m: self._log(p)))
                mis, err = list_managed_instances_in_resource_group(cred, sub_id, rg)
                if err:
                    self.frame.after(0, lambda: self._log(f"[X] {err}"))
                    return

                def apply():
                    self._tgt_mis = mis
                    self.tgt_mi_cb.refresh_suggestions()
                    labels = tuple(_mi_instance_label(m) for m in mis)
                    self.tgt_mi_cb.set(labels[0] if len(labels) == 1 else "")
                    self._log(f"[OK] {len(mis)} managed instance(s) (target).")

                self.frame.after(0, apply)
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _on_tgt_mi_changed(self) -> None:
        """Target managed instance is the last field in the chain: nothing downstream to load."""
        self.tgt_mi_cb.restore_full_suggestions()
        mi_i = self._tgt_mi_index_from_label(self.tgt_mi_cb.get())
        if mi_i < 0 or mi_i >= len(self._tgt_mis):
            return
        mi = self._tgt_mis[mi_i]
        loc = (mi.get("location") or "").strip()
        self._log(f"Target managed instance: {mi.get('name', '')}" + (f" ({loc})" if loc else ""))

    def _copy_source_to_target(self) -> None:
        if self._busy:
            return
        if get_shared_azure_credential is None:
            messagebox.showerror("Error", "azure-identity / utils.azure_shared_credential not available.")
            return
        sidx = self._subscription_index_from_display(self.src_sub_cb.get())
        if sidx < 0 or sidx >= len(self._subscriptions):
            messagebox.showwarning("Copy", "Select a source subscription first.")
            return
        src_rg = (self.src_rg_cb.get() or "").strip()
        mi_i = self._src_mi_index_from_label(self.src_mi_cb.get())
        if not src_rg or mi_i < 0:
            messagebox.showwarning("Copy", "Select source resource group and managed instance first.")
            return
        src_mi_name = self._src_mis[mi_i]["name"]

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log("Copying source → target…"))
            try:
                cred = get_shared_azure_credential(lambda m: self.frame.after(0, lambda p=m: self._log(p)))
                sub_id = self._subscriptions[sidx]["subscription_id"]
                rgs, err = list_resource_groups(cred, sub_id)
                if err:
                    self.frame.after(0, lambda: messagebox.showerror("Copy failed", err))
                    return
                mis, err2 = list_managed_instances_in_resource_group(cred, sub_id, src_rg)
                if err2:
                    self.frame.after(0, lambda: messagebox.showerror("Copy failed", err2))
                    return

                def apply():
                    vals = _combo_values_from_subscriptions(self._subscriptions)
                    self.tgt_sub_cb.refresh_suggestions()
                    if vals and 0 <= sidx < len(vals):
                        self.tgt_sub_cb.set(vals[sidx])
                    self._tgt_rgs = rgs
                    self.tgt_rg_cb.refresh_suggestions()
                    self.tgt_rg_cb.set(src_rg if src_rg in rgs else (rgs[0] if rgs else ""))
                    self._tgt_mis = mis
                    self.tgt_mi_cb.refresh_suggestions()
                    labels = tuple(_mi_instance_label(m) for m in mis)
                    if labels:
                        pick = 0
                        for j, m in enumerate(mis):
                            if m.get("name") == src_mi_name:
                                pick = j
                                break
                        self.tgt_mi_cb.set(labels[pick])
                    self._log("[OK] Target dropdowns updated from source selection.")

                self.frame.after(0, apply)
            except Exception as ex:
                self.frame.after(0, lambda: messagebox.showerror("Copy failed", str(ex)))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _cred_log(self, msg: str) -> None:
        self.frame.after(0, lambda: self._log(msg))

    def _on_test_click(self) -> None:
        if self._busy:
            return
        if get_shared_azure_credential is None:
            messagebox.showerror("Error", "azure-identity / utils.azure_shared_credential not available.")
            return
        tgt_sub, tgt_rg, tgt_mi, err = self._resolve_tgt_ids()
        if err:
            messagebox.showwarning("Missing fields", err)
            return

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            try:
                cred = get_shared_azure_credential(self._cred_log)
                loc, e2 = get_managed_instance_location(cred, tgt_sub, tgt_rg, tgt_mi)
                if e2:
                    self.frame.after(0, lambda: messagebox.showerror("Test failed", e2))
                else:
                    self.frame.after(0, lambda: self._log(f"[OK] Target MI location: {loc}"))
                    self.frame.after(
                        0,
                        lambda: messagebox.showinfo("Test OK", f"Target managed instance location:\n{loc}"),
                    )
            except Exception as ex:
                self.frame.after(0, lambda: messagebox.showerror("Error", str(ex)))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _delete_target_db_if_needed(
        self,
        cred: Any,
        *,
        tgt_sub: str,
        tgt_rg: str,
        tgt_mi: str,
        drop_name: str,
        poll_sec: float,
        timeout_sec: float,
        log: Callable[[str], None],
    ) -> bool:
        log(f"Deleting existing target database '{drop_name}' (ARM DELETE)…")
        dres = delete_managed_database(
            cred,
            subscription_id=tgt_sub,
            resource_group=tgt_rg,
            managed_instance=tgt_mi,
            database_name=drop_name,
        )
        if not dres.ok:
            log(f"[X] Delete failed: {dres.error}")
            return False
        if dres.async_operation_url:
            ok_del, msg_del, _ = poll_async_operation(
                cred,
                dres.async_operation_url,
                poll_interval_sec=max(5.0, poll_sec),
                timeout_sec=max(60.0, timeout_sec),
                log=log,
            )
            if not ok_del:
                log(f"[X] Delete did not finish: {msg_del}")
                return False
        log(f"[OK] Removed existing database '{drop_name}'.")
        return True

    def _restore_one_database(
        self,
        cred: Any,
        *,
        src_sub: str,
        src_rg: str,
        src_mi: str,
        src_db: str,
        tgt_sub: str,
        tgt_rg: str,
        tgt_mi: str,
        tgt_db: str,
        rp: str,
        loc: str,
        poll_sec: float,
        timeout_sec: float,
        drop_existing: bool,
        log: Callable[[str], None],
    ) -> bool:
        if drop_existing:
            if not self._delete_target_db_if_needed(
                cred,
                tgt_sub=tgt_sub,
                tgt_rg=tgt_rg,
                tgt_mi=tgt_mi,
                drop_name=tgt_db,
                poll_sec=poll_sec,
                timeout_sec=timeout_sec,
                log=log,
            ):
                return False

        source_arm = managed_database_id(src_sub, src_rg, src_mi, src_db)
        log(f"Source ARM ID: {source_arm}")
        log("Submitting PUT (PointInTimeRestore)…")

        start = start_point_in_time_restore(
            cred,
            target_subscription_id=tgt_sub,
            target_resource_group=tgt_rg,
            target_managed_instance=tgt_mi,
            new_database_name=tgt_db,
            source_database_arm_id=source_arm,
            restore_point_in_time_utc=rp,
            location=loc,
        )
        if not start.ok:
            log(f"[X] Start failed: {start.error}")
            return False
        if start.async_operation_url:
            ok, msg, _ = poll_async_operation(
                cred,
                start.async_operation_url,
                poll_interval_sec=max(5.0, poll_sec),
                timeout_sec=max(60.0, timeout_sec),
                log=log,
            )
            if ok:
                log(f"[OK] Restore completed: {msg}")
                return True
            log(f"[X] Restore failed or timed out: {msg}")
            return False
        log(f"[OK] Request finished without async URL (HTTP {start.http_status}).")
        return True

    def _on_start_bulk_click(self) -> None:
        if self._busy:
            return
        if get_shared_azure_credential is None:
            messagebox.showerror("Error", "azure-identity / utils.azure_shared_credential not available.")
            return

        self._commit_edit()
        src_sub, src_rg, src_mi, e1 = self._resolve_src_mi()
        if e1:
            messagebox.showwarning("Source", e1)
            return
        pairs = self._get_checked_pairs()
        if not pairs:
            messagebox.showwarning("Source", "Tick at least one database to restore.")
            return
        tgt_sub, tgt_rg, tgt_mi, e2 = self._resolve_tgt_ids()
        if e2:
            messagebox.showwarning("Target", e2)
            return

        empty_targets = [s for s, t in pairs if not (t or "").strip()]
        if empty_targets:
            messagebox.showwarning(
                "Target naming",
                "One or more target database names are empty:\n  " + "\n  ".join(empty_targets),
            )
            return
        tgt_names_lower = [t.casefold() for _, t in pairs]
        if len(tgt_names_lower) != len(set(tgt_names_lower)):
            messagebox.showwarning(
                "Target naming",
                "Two or more selected databases would map to the same target name. Adjust prefix/suffix or selection.",
            )
            return

        rp_raw = self.restore_time_var.get().strip()
        try:
            poll_sec = float(self.poll_sec_var.get().strip() or "15")
            timeout_sec = float(self.timeout_sec_var.get().strip() or "7200")
        except ValueError:
            messagebox.showerror("Invalid number", "Poll interval and max wait must be numbers.")
            return

        # An explicit timestamp is validated now so typos surface before any Azure work;
        # the point actually used is resolved per database once the restore window is known.
        rp_is_auto = rp_raw.lower() in AUTO_RESTORE_POINT_KEYWORDS
        if not rp_is_auto:
            _, rp_err = normalize_restore_point_in_time(rp_raw)
            if rp_err:
                messagebox.showerror("Restore time", rp_err)
                return

        continue_on_error = self.continue_on_error_var.get()

        try:
            cred_chk = get_shared_azure_credential(self._cred_log)
        except Exception as ex:
            messagebox.showerror("Azure", f"Could not get credentials: {ex}")
            return

        dbs_t, err_lst = list_managed_databases(cred_chk, tgt_sub, tgt_rg, tgt_mi)
        if err_lst:
            messagebox.showerror(
                "Target database check",
                f"Could not list databases on the target managed instance.\n\n{err_lst}",
            )
            return

        tgt_existing = {d.casefold(): d for d in dbs_t}
        collisions: List[Tuple[str, str, str]] = []
        for src_db, tgt_db in pairs:
            hit = tgt_existing.get(tgt_db.casefold())
            if hit is not None:
                collisions.append((src_db, tgt_db, hit))

        drop_names: Set[str] = set()
        if collisions:
            lines = "\n".join(f"  {src} → {tgt} (exists as '{exist}')" for src, tgt, exist in collisions)
            if not messagebox.askyesno(
                "Database name conflicts",
                (
                    f"The following target database name(s) already exist on '{tgt_mi}':\n\n{lines}\n\n"
                    "Point-in-time restore needs those names to be free. Delete the existing database(s) "
                    "via ARM and continue?\n\n"
                    "This does not create a backup. Export or back up first if you need to keep the data."
                ),
            ):
                return
            drop_names = {exist for _, _, exist in collisions}

        def run():
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log("=" * 60))
            self.frame.after(0, lambda: self._log(f"Starting bulk MI PITR restore for {len(pairs)} database(s)…"))
            self.frame.after(
                0,
                lambda: self._log(
                    "Restore point: latest available (resolved per database)"
                    if rp_is_auto
                    else f"Restore point requested (UTC): {rp_raw}"
                ),
            )

            success_count = 0
            fail_count = 0

            def plog(m: str) -> None:
                self.frame.after(0, lambda p=m: self._log(p))

            try:
                cred = get_shared_azure_credential(self._cred_log)
                loc, err = get_managed_instance_location(cred, tgt_sub, tgt_rg, tgt_mi)
                if err or not loc:
                    self.frame.after(0, lambda: self._log(f"[X] Could not read target MI: {err}"))
                    self.frame.after(0, lambda: messagebox.showerror("Error", err or "No location"))
                    return

                self.frame.after(0, lambda: self._log(f"Target MI location: {loc}"))

                for idx, (src_db, tgt_db) in enumerate(pairs, 1):
                    self.frame.after(0, lambda i=idx, s=src_db, t=tgt_db: self._log(f"\n[{i}/{len(pairs)}] {s} → {t}"))
                    self.frame.after(0, lambda: self._log("-" * 40))

                    rp, rp_resolve_err = resolve_restore_point_in_time(
                        cred,
                        source_database_arm_id=managed_database_id(src_sub, src_rg, src_mi, src_db),
                        requested=rp_raw,
                        log=plog,
                    )
                    if rp_resolve_err or not rp:
                        plog(f"[X] {rp_resolve_err or 'Could not determine a restore point.'}")
                        fail_count += 1
                        if not continue_on_error:
                            self.frame.after(0, lambda: self._log("[X] Stopping bulk restore (continue on error is off)."))
                            break
                        continue

                    ok = self._restore_one_database(
                        cred,
                        src_sub=src_sub,
                        src_rg=src_rg,
                        src_mi=src_mi,
                        src_db=src_db,
                        tgt_sub=tgt_sub,
                        tgt_rg=tgt_rg,
                        tgt_mi=tgt_mi,
                        tgt_db=tgt_db,
                        rp=rp,
                        loc=loc,
                        poll_sec=poll_sec,
                        timeout_sec=timeout_sec,
                        drop_existing=tgt_db.casefold() in {n.casefold() for n in drop_names},
                        log=plog,
                    )
                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                        if not continue_on_error:
                            self.frame.after(0, lambda: self._log("[X] Stopping bulk restore (continue on error is off)."))
                            break

                summary = f"Bulk restore complete: {success_count} succeeded, {fail_count} failed."
                self.frame.after(0, lambda: self._log(f"\n{'=' * 60}\n{summary}"))
                self.frame.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Bulk restore complete",
                        summary,
                    ),
                )
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
                self.frame.after(0, lambda: messagebox.showerror("Error", str(ex)))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()
