# -*- coding: utf-8 -*-
"""
Azure SQL Managed Instance — point-in-time restore to another MI (ARM), with async polling.

Subscription / resource group / MI / database are picked from Azure ARM list APIs (searchable entry + suggestion list).
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from pathlib import Path
import sys
from typing import Callable, Dict, List, Optional, Tuple

parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll

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
        start_point_in_time_restore,
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
        start_point_in_time_restore,
    )

try:
    from utils.azure_shared_credential import get_shared_azure_credential
except ImportError:
    try:
        from azure_migration_tool.utils.azure_shared_credential import get_shared_azure_credential
    except ImportError:
        get_shared_azure_credential = None  # type: ignore


def _combo_values_from_subscriptions(rows: List[Dict[str, str]]) -> Tuple[str, ...]:
    """Display strings for combobox (same order as rows)."""
    return tuple(f"{r['display_name']}  |  {r['subscription_id']}" for r in rows)


def _mi_instance_label(m: Dict[str, str]) -> str:
    return f"{m['name']}  ({m.get('location') or '?'})"


def _filter_combo_values(full: Tuple[str, ...], needle: str) -> Tuple[str, ...]:
    """Case-insensitive substring filter for type-ahead combobox lists."""
    q = (needle or "").strip().lower()
    if not q:
        return full
    return tuple(v for v in full if q in v.lower())


# Keys where we should not re-filter the suggestion list (navigation / modifiers).
_TYPEAHEAD_SKIP_KEYSYMS = frozenset(
    {
        "Down",
        "Up",
        "Next",
        "Prior",
        "Return",
        "Tab",
        "Escape",
        "Shift_L",
        "Shift_R",
        "Control_L",
        "Control_R",
        "Alt_L",
        "Alt_R",
        "Left",
        "Right",
        "Home",
        "End",
        "Caps_Lock",
        "Win_L",
        "Super_L",
        "Super_R",
    }
)


class _ArmPicker(ttk.Frame):
    """Entry + dropdown button + floating suggestion list (no native combobox focus bugs on Windows)."""

    def __init__(
        self,
        parent,
        *,
        width_chars: int = 78,
        get_choices: Callable[[], Tuple[str, ...]],
        **kwargs,
    ) -> None:
        super().__init__(parent, **kwargs)
        self._get_choices = get_choices
        self._hide_after_id: Optional[str] = None

        self._row = ttk.Frame(self)
        self._row.pack(fill=tk.X, expand=True)
        self.entry = ttk.Entry(self._row, width=width_chars)
        self._btn_drop = ttk.Button(
            self._row,
            text="\u25be",
            width=2,
            command=self._on_dropdown_click,
            takefocus=False,
        )
        self.entry.grid(row=0, column=0, sticky="ew", padx=(0, 2))
        self._btn_drop.grid(row=0, column=1, sticky="ns")
        self._row.columnconfigure(0, weight=1)
        self._btn_drop.bind("<ButtonPress-1>", lambda e: self._cancel_hide_popup())

        top = self.winfo_toplevel()
        self._popup = tk.Toplevel(top)
        self._popup.withdraw()
        self._popup.wm_overrideredirect(True)
        try:
            self._popup.attributes("-topmost", True)
        except tk.TclError:
            pass

        inner = ttk.Frame(self._popup)
        sb = ttk.Scrollbar(inner)
        self._lb = tk.Listbox(inner, height=10, exportselection=False, activestyle="dotbox")
        self._lb.configure(yscrollcommand=sb.set, font=("Segoe UI", 9))
        sb.configure(command=self._lb.yview)
        self._lb.grid(row=0, column=0, sticky="nsew")
        sb.grid(row=0, column=1, sticky="ns")
        inner.grid_rowconfigure(0, weight=1)
        inner.grid_columnconfigure(0, weight=1)
        inner.pack(fill=tk.BOTH, expand=True)

        self.entry.bind("<KeyRelease>", self._on_entry_keyrelease)
        self.entry.bind("<Return>", self._on_entry_return)
        self.entry.bind("<Escape>", lambda e: self.hide_popup())
        self.entry.bind("<FocusOut>", self._on_entry_focusout)
        self._lb.bind("<Enter>", self._cancel_hide_popup)
        # Button-1 + nearest(y): one click selects (ButtonRelease + curselection() often needs two clicks on Windows).
        self._lb.bind("<Button-1>", self._on_lb_button1)
        self._lb.bind("<Return>", self._on_lb_return_key)
        self._lb.bind("<Escape>", lambda e: self.hide_popup())
        self.bind("<Destroy>", self._on_destroy)

    def get(self) -> str:
        return self.entry.get()

    def set(self, text: str) -> None:
        self.entry.delete(0, tk.END)
        self.entry.insert(0, text)

    def set_picker_state(self, state: str) -> None:
        self.entry.configure(state=state)
        self._btn_drop.configure(state=state)

    def hide_popup(self) -> None:
        self._popup.withdraw()

    def refresh_suggestions(self) -> None:
        """Sync listbox from ARM data after async load; do not pop the list (avoids reopening after a pick)."""
        self._apply_filter_and_popup(show_popup=False)

    def restore_full_suggestions(self) -> None:
        """Re-sync filtered list for current entry text without opening the popup (used after <<ComboboxSelected>>)."""
        self._apply_filter_and_popup(show_popup=False)

    def _on_destroy(self, event: tk.Event) -> None:
        self.hide_popup()
        try:
            self._popup.destroy()
        except tk.TclError:
            pass

    def _cancel_hide_popup(self, event: Optional[tk.Event] = None) -> None:
        hid = self._hide_after_id
        if hid is not None:
            try:
                self.after_cancel(hid)
            except (ValueError, tk.TclError):
                pass
        self._hide_after_id = None

    def _on_entry_focusout(self, event: tk.Event) -> None:
        self._cancel_hide_popup()
        self._hide_after_id = self.after(150, self._maybe_hide_popup)

    def _maybe_hide_popup(self) -> None:
        self._hide_after_id = None
        fw = self.focus_get()
        if fw in (self.entry, self._lb, self._btn_drop):
            return
        if fw is not None:
            try:
                if str(fw).startswith(str(self._popup)):
                    return
            except Exception:
                pass
        self.hide_popup()

    def _filtered_choices(self) -> Tuple[str, ...]:
        try:
            full = self._get_choices()
        except Exception:
            full = ()
        return _filter_combo_values(full, self.entry.get())

    def _apply_filter_and_popup(self, *, show_popup: bool = True) -> None:
        if str(self.entry.cget("state")) == "disabled":
            self.hide_popup()
            return
        filt = self._filtered_choices()
        self._lb.delete(0, tk.END)
        for v in filt:
            self._lb.insert(tk.END, v)
        if filt and show_popup:
            self._show_popup()
        else:
            self.hide_popup()

    def _on_dropdown_click(self) -> None:
        self._cancel_hide_popup()
        if str(self.entry.cget("state")) == "disabled":
            return
        try:
            if self._popup.winfo_viewable():
                self.hide_popup()
                return
        except tk.TclError:
            pass
        self._apply_filter_and_popup(show_popup=True)
        try:
            self.entry.focus_set()
        except tk.TclError:
            pass

    def _show_popup(self) -> None:
        self.update_idletasks()
        x = self._row.winfo_rootx()
        y = self._row.winfo_rooty() + self._row.winfo_height()
        w = max(self._row.winfo_width(), 360)
        n = self._lb.size()
        row_h = 18
        h = min(240, max(72, n * row_h + 16))
        self._popup.geometry(f"{int(w)}x{int(h)}+{int(x)}+{int(y)}")
        self._popup.deiconify()
        self._popup.lift()

    def _on_entry_keyrelease(self, event: tk.Event) -> None:
        if event.keysym in _TYPEAHEAD_SKIP_KEYSYMS:
            return
        self._apply_filter_and_popup(show_popup=True)

    def _on_entry_return(self, event: tk.Event) -> Optional[str]:
        had = bool(self._lb.curselection())
        if not had and self._lb.size() == 1:
            self._lb.selection_set(0)
            had = True
        if had:
            self._commit_list_selection()
            self.update_idletasks()
            self.event_generate("<<ComboboxSelected>>", when="tail")
        return "break"

    def _on_lb_button1(self, event: tk.Event) -> None:
        self._cancel_hide_popup()
        idx = self._lb.nearest(event.y)
        if 0 <= idx < self._lb.size():
            self._apply_pick_index(idx, fire_event=True)

    def _on_lb_return_key(self, event: Optional[tk.Event] = None) -> Optional[str]:
        self._cancel_hide_popup()
        sel = self._lb.curselection()
        if sel:
            self._apply_pick_index(int(sel[0]), fire_event=True)
        elif self._lb.size() == 1:
            self._apply_pick_index(0, fire_event=True)
        return "break"

    def _commit_list_selection(self) -> None:
        sel = self._lb.curselection()
        if not sel:
            return
        self._apply_pick_index(int(sel[0]), fire_event=False)

    def _apply_pick_index(self, idx: int, *, fire_event: bool) -> None:
        if idx < 0 or idx >= self._lb.size():
            return
        self.set(self._lb.get(idx))
        self.hide_popup()
        try:
            self.entry.focus_set()
        except tk.TclError:
            pass
        if fire_event:
            # Defer so FocusOut / listbox default bindings finish before handlers run (Windows one-click select).
            self.update_idletasks()
            self.event_generate("<<ComboboxSelected>>", when="tail")


class MiPitrRestoreTab:
    """Tab: PITR restore managed database to another managed instance via Azure portal–equivalent ARM."""

    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path = None
        self._busy = False
        # Shared subscription list (same for source & target pickers)
        self._subscriptions: List[Dict[str, str]] = []
        self._src_rgs: List[str] = []
        self._src_mis: List[Dict[str, str]] = []
        self._src_dbs: List[str] = []
        self._tgt_rgs: List[str] = []
        self._tgt_mis: List[Dict[str, str]] = []
        self._tgt_dbs: List[str] = []
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
        for w in (self.btn_load_subs, self.btn_test, self.btn_start, self.btn_copy_sel):
            w.config(state=btn_state)
        for cb in (
            self.src_sub_cb,
            self.src_rg_cb,
            self.src_mi_cb,
            self.src_db_cb,
            self.tgt_sub_cb,
            self.tgt_rg_cb,
            self.tgt_mi_cb,
        ):
            cb.set_picker_state(cb_state)

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
            text="MI database — point-in-time restore (cross-instance)",
            font=("Arial", 16, "bold"),
        ).pack(pady=(10, 4))
        tk.Label(
            scrollable,
            text=(
                "Pick subscription → resource group → managed instance → database from Azure (ARM). "
                "Suggestions appear under the field as you type, or open them with the ▾ button. "
                "Choose a row (click or Enter) to confirm. "
                "Use az login first when possible; otherwise a browser sign-in may appear. "
                "New database name on the target is still typed in (it does not exist yet)."
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

        src = ttk.LabelFrame(scrollable, text="Source (database to copy from)", padding=10)
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
        self.src_db_cb = _ArmPicker(src, width_chars=78, get_choices=lambda: tuple(self._src_dbs))
        self._grid_combo_row(src, 0, "Subscription", self.src_sub_cb)
        self._grid_combo_row(src, 1, "Resource group", self.src_rg_cb)
        self._grid_combo_row(src, 2, "Managed instance", self.src_mi_cb)
        self._grid_combo_row(src, 3, "Database", self.src_db_cb)
        src.columnconfigure(1, weight=1)
        self.src_sub_cb.bind("<<ComboboxSelected>>", lambda e: self._on_src_sub_changed())
        self.src_rg_cb.bind("<<ComboboxSelected>>", lambda e: self._on_src_rg_changed())
        self.src_mi_cb.bind("<<ComboboxSelected>>", lambda e: self._on_src_mi_changed())

        tgt = ttk.LabelFrame(scrollable, text="Target (new database on destination MI)", padding=10)
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
        self.tgt_new_db_var = tk.StringVar()
        self._grid_combo_row(tgt, 0, "Subscription", self.tgt_sub_cb)
        self._grid_combo_row(tgt, 1, "Resource group", self.tgt_rg_cb)
        self._grid_combo_row(tgt, 2, "Managed instance", self.tgt_mi_cb)
        ttk.Label(tgt, text="New database name:").grid(row=3, column=0, sticky=tk.W, padx=4, pady=3)
        ttk.Entry(tgt, textvariable=self.tgt_new_db_var, width=76).grid(
            row=3, column=1, sticky=tk.EW, padx=4, pady=3
        )
        tgt.columnconfigure(1, weight=1)
        self.tgt_sub_cb.bind("<<ComboboxSelected>>", lambda e: self._on_tgt_sub_changed())
        self.tgt_rg_cb.bind("<<ComboboxSelected>>", lambda e: self._on_tgt_rg_changed())
        self.tgt_mi_cb.bind("<<ComboboxSelected>>", lambda e: self._on_tgt_mi_changed())

        opts = ttk.LabelFrame(scrollable, text="Restore options", padding=10)
        opts.pack(fill=tk.X, padx=10, pady=8)
        self.restore_time_var = tk.StringVar(
            value=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00")
        )
        self.poll_sec_var = tk.StringVar(value="15")
        self.timeout_sec_var = tk.StringVar(value="7200")
        self._grid_entry_row(opts, 0, "Restore point (UTC if no offset)", self.restore_time_var)
        self._grid_entry_row(opts, 1, "Poll interval (seconds)", self.poll_sec_var)
        self._grid_entry_row(opts, 2, "Max wait (seconds)", self.timeout_sec_var)
        opts.columnconfigure(1, weight=1)

        btn_row = ttk.Frame(scrollable)
        btn_row.pack(fill=tk.X, padx=10, pady=8)
        self.btn_test = ttk.Button(btn_row, text="Test: resolve target MI location", command=self._on_test_click)
        self.btn_test.pack(side=tk.LEFT, padx=(0, 8))
        self.btn_start = ttk.Button(btn_row, text="Start restore + poll until done", command=self._on_start_click)
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
        self.src_db_cb.set("")
        self.src_db_cb.refresh_suggestions()

    def _clear_tgt_downstream(self, from_rg: bool = False) -> None:
        if not from_rg:
            self._tgt_rgs = []
            self.tgt_rg_cb.set("")
            self.tgt_rg_cb.refresh_suggestions()
        self._tgt_mis = []
        self.tgt_mi_cb.set("")
        self.tgt_mi_cb.refresh_suggestions()

    def _subscription_id_at_combo_index(self, idx: int) -> Optional[str]:
        if idx < 0 or idx >= len(self._subscriptions):
            return None
        return self._subscriptions[idx]["subscription_id"]

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
                    self.frame.after(0, lambda: messagebox.showwarning("Azure", "No subscriptions found for this account."))
                    return

                def apply():
                    self._subscriptions = rows
                    self.src_sub_cb.refresh_suggestions()
                    self.tgt_sub_cb.refresh_suggestions()
                    self._clear_src_downstream()
                    self._clear_tgt_downstream()
                    self.src_sub_cb.set("")
                    self.tgt_sub_cb.set("")
                    self._log(f"[OK] Loaded {len(rows)} subscription(s). Pick subscription, then resource group.")

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
            self.frame.after(0, lambda: self._log(f"Loading resource groups (source)…"))
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
                    self.src_db_cb.set("")
                    self.src_db_cb.refresh_suggestions()
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
            self.src_db_cb.set("")
            self.src_db_cb.refresh_suggestions()
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
                    self.src_db_cb.refresh_suggestions()
                    self.src_db_cb.set(dbs[0] if len(dbs) == 1 else "")
                    self._log(f"[OK] {len(dbs)} database(s) on {mi_name}.")

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
            self.frame.after(0, lambda: self._log(f"Loading resource groups (target)…"))
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
        # Target MI only needs name for restore PUT; no need to list DBs on target.
        pass

    def _resolve_src_ids(self) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Returns (sub_id, rg, mi_name, db_name, error)."""
        sidx = self._subscription_index_from_display(self.src_sub_cb.get())
        sub = self._subscription_id_at_combo_index(sidx)
        rg = (self.src_rg_cb.get() or "").strip()
        mi_i = self._src_mi_index_from_label(self.src_mi_cb.get())
        db = (self.src_db_cb.get() or "").strip()
        if not self._subscriptions:
            return None, None, None, None, "Load subscriptions first."
        if not sub:
            return None, None, None, None, "Select a source subscription."
        if not rg:
            return None, None, None, None, "Select a source resource group."
        if mi_i < 0:
            return None, None, None, None, "Select a source managed instance."
        mi = self._src_mis[mi_i]["name"]
        if not db:
            return None, None, None, None, "Select a source database."
        if db not in self._src_dbs:
            return None, None, None, None, "Select a source database from the list (or reload MI)."
        return sub, rg, mi, db, None

    def _resolve_tgt_ids(self) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Returns (sub_id, rg, mi_name, error)."""
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

    def _copy_source_to_target(self) -> None:
        """Copy source subscription / RG / MI to target (same names on target side after ARM reload)."""
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
            self.frame.after(0, lambda: self._log("Copying source → target (loading target ARM lists)…"))
            try:
                cred = get_shared_azure_credential(lambda m: self.frame.after(0, lambda p=m: self._log(p)))
                sub_id = self._subscriptions[sidx]["subscription_id"]
                rgs, err = list_resource_groups(cred, sub_id)
                if err:
                    self.frame.after(0, lambda: self._log(f"[X] {err}"))
                    self.frame.after(0, lambda: messagebox.showerror("Copy failed", err))
                    return
                mis, err2 = list_managed_instances_in_resource_group(cred, sub_id, src_rg)
                if err2:
                    self.frame.after(0, lambda: self._log(f"[X] {err2}"))
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
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
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
            self.frame.after(0, lambda: self._log("Getting Azure credential…"))
            try:
                cred = get_shared_azure_credential(self._cred_log)
                loc, e2 = get_managed_instance_location(cred, tgt_sub, tgt_rg, tgt_mi)
                if e2:
                    self.frame.after(0, lambda: self._log(f"[X] {e2}"))
                    self.frame.after(0, lambda: messagebox.showerror("Test failed", e2))
                else:
                    self.frame.after(0, lambda: self._log(f"[OK] Target MI location: {loc}"))
                    self.frame.after(
                        0,
                        lambda: messagebox.showinfo("Test OK", f"Target managed instance location:\n{loc}"),
                    )
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
                self.frame.after(0, lambda: messagebox.showerror("Error", str(ex)))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()

    def _on_start_click(self) -> None:
        if self._busy:
            return
        if get_shared_azure_credential is None:
            messagebox.showerror("Error", "azure-identity / utils.azure_shared_credential not available.")
            return

        src_sub, src_rg, src_mi, src_db, e1 = self._resolve_src_ids()
        if e1:
            messagebox.showwarning("Source", e1)
            return
        tgt_sub, tgt_rg, tgt_mi, e2 = self._resolve_tgt_ids()
        if e2:
            messagebox.showwarning("Target", e2)
            return
        new_db = (self.tgt_new_db_var.get() or "").strip()
        if not new_db:
            messagebox.showwarning("Target", "Enter the new database name on the target MI.")
            return
        rp_raw = self.restore_time_var.get().strip()

        try:
            poll_sec = float(self.poll_sec_var.get().strip() or "15")
            timeout_sec = float(self.timeout_sec_var.get().strip() or "7200")
        except ValueError:
            messagebox.showerror("Invalid number", "Poll interval and max wait must be numbers.")
            return

        rp, rp_err = normalize_restore_point_in_time(rp_raw)
        if rp_err or not rp:
            messagebox.showerror("Restore time", rp_err or "Invalid restore time.")
            return

        source_arm = managed_database_id(src_sub, src_rg, src_mi, src_db)

        drop_existing_name: Optional[str] = None
        try:
            cred_chk = get_shared_azure_credential(self._cred_log)
        except Exception as ex:
            messagebox.showerror("Azure", f"Could not get credentials to check the target database: {ex}")
            return
        dbs_t, err_lst = list_managed_databases(cred_chk, tgt_sub, tgt_rg, tgt_mi)
        if err_lst:
            messagebox.showerror(
                "Target database check",
                (
                    f"Could not list databases on the target managed instance, so '{new_db}' was not checked "
                    f"for a name conflict.\n\n{err_lst}"
                ),
            )
            return
        collision = next((d for d in dbs_t if d.casefold() == new_db.casefold()), None)
        if collision is not None:
            if not messagebox.askyesno(
                "Database already exists",
                (
                    f"A managed database named '{collision}' already exists on '{tgt_mi}'.\n\n"
                    "Point-in-time restore needs that name to be free. You can delete the existing database "
                    "here (Azure ARM), then this tool will run the restore.\n\n"
                    "This does not create a backup for you. Take a BACPAC export, native backup, or use your own "
                    "retention copy before you continue if you need to keep the data.\n\n"
                    "Delete the existing database and continue with restore?"
                ),
            ):
                return
            drop_existing_name = collision

        def run(drop_name: Optional[str] = drop_existing_name):
            self.frame.after(0, lambda: self._set_busy(True))
            self.frame.after(0, lambda: self._log("=" * 60))
            self.frame.after(0, lambda: self._log("Starting MI PITR restore (ARM)…"))
            self.frame.after(0, lambda: self._log(f"Source database ARM ID:\n{source_arm}"))
            self.frame.after(0, lambda: self._log(f"Restore point (UTC): {rp}"))

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

                if drop_name:
                    self.frame.after(
                        0,
                        lambda n=drop_name: self._log(f"Deleting existing target database '{n}' (ARM DELETE)…"),
                    )
                    dres = delete_managed_database(
                        cred,
                        subscription_id=tgt_sub,
                        resource_group=tgt_rg,
                        managed_instance=tgt_mi,
                        database_name=drop_name,
                    )
                    if not dres.ok:
                        self.frame.after(0, lambda: self._log(f"[X] Delete failed: {dres.error}"))
                        self.frame.after(
                            0,
                            lambda: messagebox.showerror("Delete failed", dres.error or "Unknown error"),
                        )
                        return
                    if dres.async_operation_url:
                        self.frame.after(
                            0,
                            lambda u=dres.async_operation_url: self._log(f"Delete async URL:\n{u}"),
                        )
                        ok_del, msg_del, _ = poll_async_operation(
                            cred,
                            dres.async_operation_url,
                            poll_interval_sec=max(5.0, poll_sec),
                            timeout_sec=max(60.0, timeout_sec),
                            log=plog,
                        )
                        if not ok_del:
                            self.frame.after(0, lambda: self._log(f"[X] Delete did not finish: {msg_del}"))
                            self.frame.after(
                                0,
                                lambda: messagebox.showerror("Delete failed or timed out", str(msg_del)[:1500]),
                            )
                            return
                    self.frame.after(0, lambda: self._log("[OK] Existing target database removed; starting restore…"))

                self.frame.after(0, lambda: self._log("Submitting PUT (PointInTimeRestore)…"))

                start = start_point_in_time_restore(
                    cred,
                    target_subscription_id=tgt_sub,
                    target_resource_group=tgt_rg,
                    target_managed_instance=tgt_mi,
                    new_database_name=new_db,
                    source_database_arm_id=source_arm,
                    restore_point_in_time_utc=rp,
                    location=loc,
                )

                if not start.ok:
                    self.frame.after(0, lambda: self._log(f"[X] Start failed: {start.error}"))
                    self.frame.after(0, lambda: messagebox.showerror("Start failed", start.error or "Unknown error"))
                    return

                if start.async_operation_url:
                    self.frame.after(0, lambda: self._log(f"Async operation URL:\n{start.async_operation_url}"))

                    ok, msg, body = poll_async_operation(
                        cred,
                        start.async_operation_url,
                        poll_interval_sec=max(5.0, poll_sec),
                        timeout_sec=max(60.0, timeout_sec),
                        log=plog,
                    )
                    if ok:
                        self.frame.after(0, lambda: self._log(f"[OK] Restore completed: {msg}"))
                        self.frame.after(
                            0,
                            lambda: messagebox.showinfo(
                                "Restore complete",
                                f"Database '{new_db}' on '{tgt_mi}' should be available.\n\n{msg}",
                            ),
                        )
                    else:
                        self.frame.after(0, lambda: self._log(f"[X] Restore failed or timed out: {msg}"))
                        self.frame.after(0, lambda: messagebox.showerror("Restore failed", msg[:1500]))
                else:
                    self.frame.after(
                        0,
                        lambda: self._log(f"[OK] Request finished without async URL (HTTP {start.http_status})."),
                    )
                    self.frame.after(
                        0,
                        lambda: messagebox.showinfo("Done", "Operation returned without async polling URL."),
                    )
            except Exception as ex:
                self.frame.after(0, lambda: self._log(f"[X] {ex}"))
                self.frame.after(0, lambda: messagebox.showerror("Error", str(ex)))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=run, daemon=True).start()
