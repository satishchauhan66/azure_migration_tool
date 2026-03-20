# Author: Satish Chauhan
"""
Route mouse wheel to the correct scrollable canvas without stealing events from
Text, Listbox, Treeview, combobox popups, etc.

Tabs register (canvas, scrollable_frame) pairs; a single bind_all dispatches so
the last-opened tab does not overwrite others.
"""

from __future__ import annotations

import tkinter as tk
from typing import Any, List, Optional, Tuple

# (canvas, scrollable_inner_frame)
_registrations: List[Tuple[tk.Canvas, tk.Misc]] = []


def _widget_under_pointer(event: Any) -> Optional[tk.Misc]:
    w = event.widget
    try:
        top = w.winfo_toplevel()
        at = top.winfo_containing(event.x_root, event.y_root)
        if at is not None:
            return at
    except tk.TclError:
        pass
    return w


def _is_descendant(widget: tk.Misc, ancestor: tk.Misc) -> bool:
    w: Optional[tk.Misc] = widget
    while w is not None:
        if w == ancestor:
            return True
        w = getattr(w, "master", None)
    return False


def _wheel_should_scroll_outer_canvas(widget: tk.Misc) -> bool:
    """False = let the widget handle wheel (or default) instead of outer canvas."""
    try:
        cls = widget.winfo_class()
    except tk.TclError:
        return False

    if cls == "Text":
        return False
    if cls == "Listbox":
        return False
    if cls == "Treeview":
        return False
    if cls == "Scale":
        return False
    # Combobox dropdown is usually Listbox; closed combobox: allow page scroll
    return True


def _find_canvas_for_widget(widget: tk.Misc) -> Optional[tk.Canvas]:
    """Pick innermost matching canvas (last registered wins for nesting)."""
    for canvas, scrollable_frame in reversed(_registrations):
        try:
            if not canvas.winfo_exists():
                continue
            if not scrollable_frame.winfo_exists():
                continue
        except tk.TclError:
            continue
        if widget == canvas or _is_descendant(widget, scrollable_frame):
            return canvas
    return None


def _dispatch_windows(event: tk.Event) -> Optional[str]:
    w = _widget_under_pointer(event)
    if w is None:
        return None
    if not _wheel_should_scroll_outer_canvas(w):
        return None
    canvas = _find_canvas_for_widget(w)
    if canvas is None:
        return None
    try:
        delta = getattr(event, "delta", 0) or 0
        canvas.yview_scroll(int(-1 * (delta / 120)), "units")
    except tk.TclError:
        pass
    return "break"


def _dispatch_linux(event: Any, direction: int) -> Optional[str]:
    w = _widget_under_pointer(event)
    if w is None:
        return None
    if not _wheel_should_scroll_outer_canvas(w):
        return None
    canvas = _find_canvas_for_widget(w)
    if canvas is None:
        return None
    try:
        canvas.yview_scroll(direction, "units")
    except tk.TclError:
        pass
    return "break"


def _install_on_root(root: tk.Misc) -> None:
    if getattr(root, "_amt_canvas_wheel_dispatch", False):
        return
    root._amt_canvas_wheel_dispatch = True

    root.bind_all("<MouseWheel>", _dispatch_windows)
    root.bind_all("<Button-4>", lambda e: _dispatch_linux(e, -1))
    root.bind_all("<Button-5>", lambda e: _dispatch_linux(e, 1))


def bind_canvas_vertical_scroll(canvas: tk.Canvas, scrollable_frame: tk.Misc) -> None:
    """
    Register this canvas so wheel events scroll it when the pointer is over
    the canvas or any descendant of scrollable_frame, unless the target is a
    self-scrolling widget (Text, Listbox, Treeview, ...).
    """
    pair = (canvas, scrollable_frame)
    if pair not in _registrations:
        _registrations.append(pair)
    try:
        root = canvas.winfo_toplevel()
    except tk.TclError:
        return
    _install_on_root(root)


def unregister_canvas_scroll(canvas: tk.Canvas, scrollable_frame: tk.Misc) -> None:
    """Remove registration (e.g. when destroying a tab)."""
    pair = (canvas, scrollable_frame)
    try:
        _registrations.remove(pair)
    except ValueError:
        pass
