# Author: Satish Chauhan
"""
Type-ahead searchable picker (Entry + dropdown + floating suggestion list).

Same UX as MI PITR subscription/MI/database pickers: type to filter the list
in-place — no separate Search box required.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Callable, Optional, Sequence, Tuple

# Keys where we should not re-filter the suggestion list (navigation / modifiers).
TYPEAHEAD_SKIP_KEYSYMS = frozenset(
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


def filter_choices(full: Sequence[str], needle: str) -> Tuple[str, ...]:
    """Case-insensitive substring filter for type-ahead lists."""
    values = tuple(full or ())
    q = (needle or "").strip().lower()
    if not q:
        return values
    return tuple(v for v in values if q in str(v).lower())


def choices_for_typeahead(full: Sequence[str], needle: str) -> Tuple[str, ...]:
    """
    Filter while typing, but keep the full list when the box is empty or already
    holds an exact listed value (so listing/selecting one item does not hide the rest).
    """
    values = tuple(full or ())
    q = (needle or "").strip()
    if not q:
        return values
    q_l = q.lower()
    if any(str(v).lower() == q_l for v in values):
        return values
    return filter_choices(values, q)


def enable_combobox_typeahead(
    combo: ttk.Combobox,
    *,
    get_full_values: Optional[Callable[[], Sequence[str]]] = None,
) -> Callable[[], None]:
    """
    Make a normal ttk.Combobox filter its dropdown as the user types.

    Returns a ``remember()`` callable — invoke it after you assign a new full
    ``combo['values']`` list so typing filters against the latest data.

    If ``get_full_values`` is provided, that source is used instead of a snapshot.
    """
    state: dict = {"full": tuple(combo.cget("values") or ())}

    def remember() -> None:
        if get_full_values is None:
            state["full"] = tuple(combo.cget("values") or ())

    def _full() -> Tuple[str, ...]:
        if get_full_values is not None:
            try:
                return tuple(get_full_values() or ())
            except Exception:
                return state["full"]
        return state["full"]

    def on_keyrelease(event: tk.Event) -> None:
        if event.keysym in TYPEAHEAD_SKIP_KEYSYMS:
            return
        full = _full()
        if get_full_values is None and not full:
            remember()
            full = state["full"]
        combo.configure(values=choices_for_typeahead(full, combo.get()))

    combo.bind("<KeyRelease>", on_keyrelease, add="+")
    combo.bind(
        "<<ComboboxSelected>>",
        lambda _e=None: combo.configure(values=_full()),
        add="+",
    )
    existing_post = combo.cget("postcommand")

    def on_dropdown_open() -> None:
        if existing_post:
            try:
                combo.tk.call(existing_post)
            except tk.TclError:
                pass
        combo.configure(values=_full())

    combo.configure(postcommand=on_dropdown_open)
    remember()
    # Stash for callers that want to refresh the snapshot later.
    setattr(combo, "_typeahead_remember", remember)
    return remember


class SearchablePicker(ttk.Frame):
    """Entry + dropdown button + floating suggestion list (type-to-filter)."""

    def __init__(
        self,
        parent,
        *,
        width_chars: int = 40,
        get_choices: Callable[[], Tuple[str, ...]],
        textvariable: Optional[tk.StringVar] = None,
        **kwargs,
    ) -> None:
        super().__init__(parent, **kwargs)
        self._get_choices = get_choices
        self._hide_after_id: Optional[str] = None
        self._textvariable = textvariable

        self._row = ttk.Frame(self)
        self._row.pack(fill=tk.X, expand=True)
        if textvariable is not None:
            self.entry = ttk.Entry(self._row, width=width_chars, textvariable=textvariable)
        else:
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
        self._lb.bind("<Button-1>", self._on_lb_button1)
        self._lb.bind("<Return>", self._on_lb_return_key)
        self._lb.bind("<Escape>", lambda e: self.hide_popup())
        self.bind("<Destroy>", self._on_destroy)

    def get(self) -> str:
        return self.entry.get()

    def set(self, text: str) -> None:
        if self._textvariable is not None:
            self._textvariable.set(text)
            return
        self.entry.delete(0, tk.END)
        self.entry.insert(0, text)

    def set_picker_state(self, state: str) -> None:
        self.entry.configure(state=state)
        self._btn_drop.configure(state=state)

    def hide_popup(self) -> None:
        self._popup.withdraw()

    def refresh_suggestions(self) -> None:
        """Sync listbox from data after async load; do not open the popup."""
        self._apply_filter_and_popup(show_popup=False)

    def restore_full_suggestions(self) -> None:
        """Re-sync filtered list for current entry text without opening the popup."""
        self._apply_filter_and_popup(show_popup=False)

    def _on_destroy(self, event: tk.Event) -> None:
        if event.widget is not self:
            return
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

    def _all_choices(self) -> Tuple[str, ...]:
        try:
            return tuple(self._get_choices() or ())
        except Exception:
            return ()

    def _filtered_choices(self, *, force_all: bool = False) -> Tuple[str, ...]:
        full = self._all_choices()
        if force_all:
            return full
        return choices_for_typeahead(full, self.entry.get())

    def _apply_filter_and_popup(self, *, show_popup: bool = True, force_all: bool = False) -> None:
        if str(self.entry.cget("state")) == "disabled":
            self.hide_popup()
            return
        filt = self._filtered_choices(force_all=force_all)
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
        # Arrow always shows the full list; typing filters, an auto-selected value does not.
        self._apply_filter_and_popup(show_popup=True, force_all=True)
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
        if event.keysym in TYPEAHEAD_SKIP_KEYSYMS:
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
            self.update_idletasks()
            self.event_generate("<<ComboboxSelected>>", when="tail")


# Backward-compatible alias used by MI PITR tabs.
_ArmPicker = SearchablePicker
