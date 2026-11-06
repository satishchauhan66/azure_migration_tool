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
_root: Optional[tk.Misc] = None


def _resolution_base() -> Optional[tk.Misc]:
    if _root is not None:
        try:
            if _root.winfo_exists():
                return _root
        except tk.TclError:
            pass
    for canvas, _frame in reversed(_registrations):
        try:
            if canvas.winfo_exists():
                return canvas
        except tk.TclError:
            continue
    return None


def _as_widget(target: Any) -> Optional[tk.Misc]:
    """
    Tk reports a bare widget path string for windows with no Python counterpart
    (ttk combobox popdowns and other Tcl-created windows). Resolve those to a
    widget, or None when the window is unknown to this interpreter.
    """
    if isinstance(target, tk.Misc):
        return target
    if not isinstance(target, str) or not target:
        return None
    base = _resolution_base()
    if base is None:
        return None
    try:
        return base.nametowidget(target)
    except (KeyError, tk.TclError):
        return None


def _widget_under_pointer(event: Any) -> Optional[tk.Misc]:
    w = _as_widget(getattr(event, "widget", None))
    if w is None:
        return None
    try:
        top = w.winfo_toplevel()
        at = _as_widget(top.winfo_containing(event.x_root, event.y_root))
        if at is not None:
            return at
    except (KeyError, tk.TclError):
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
    except (AttributeError, tk.TclError):
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


def _scroll_canvas_units(canvas: tk.Canvas, direction: int) -> None:
    """Scroll `canvas` by `direction` yview units (positive = down)."""
    try:
        canvas.yview_scroll(direction, "units")
    except tk.TclError:
        pass


def _on_risky_widget_mousewheel_windows(event: tk.Event) -> Optional[str]:
    """Combobox/Spinbox: scroll registered canvas instead of cycling the control (class binding runs before bind_all)."""
    w = _as_widget(getattr(event, "widget", None))
    if w is None:
        return None
    canvas = _find_canvas_for_widget(w)
    if canvas is None:
        return None
    _scroll_canvas_units(canvas, int(-1 * ((getattr(event, "delta", 0) or 0) / 120)))
    return "break"


def _on_risky_widget_mousewheel_linux(event: tk.Event, direction: int) -> Optional[str]:
    w = _as_widget(getattr(event, "widget", None))
    if w is None:
        return None
    canvas = _find_canvas_for_widget(w)
    if canvas is None:
        return None
    _scroll_canvas_units(canvas, direction)
    return "break"


def _install_wheel_redirect_on_widget(w: tk.Misc) -> None:
    """
    Ttk Combobox and Spinbox (and classic Spinbox) bind MouseWheel to change values.
    That runs in the widget/class binding phase *before* our bind_all canvas dispatcher,
    so scrolling the page accidentally changes credentials / picks. Redirect wheel to
    the registered outer canvas instead.
    """
    if getattr(w, "_amt_wheel_redirect", False):
        return
    try:
        cls = w.winfo_class()
    except tk.TclError:
        return
    if cls not in ("TCombobox", "TSpinbox", "Spinbox"):
        return
    w._amt_wheel_redirect = True
    w.bind("<MouseWheel>", _on_risky_widget_mousewheel_windows, add=True)
    w.bind("<Button-4>", lambda e: _on_risky_widget_mousewheel_linux(e, -1), add=True)
    w.bind("<Button-5>", lambda e: _on_risky_widget_mousewheel_linux(e, 1), add=True)


def _walk_install_wheel_redirect(frame: tk.Misc) -> None:
    """Depth-first walk; bind combobox/spinbox descendants."""
    stack: List[tk.Misc] = [frame]
    while stack:
        w = stack.pop()
        try:
            children = w.winfo_children()
        except tk.TclError:
            continue
        for ch in children:
            stack.append(ch)
        _install_wheel_redirect_on_widget(w)


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
    # After first paint, patch ttk combobox/spinbox so page scroll wins over value-cycling
    try:

        def _defer() -> None:
            if not canvas.winfo_exists() or not scrollable_frame.winfo_exists():
                return
            _walk_install_wheel_redirect(scrollable_frame)

        canvas.after_idle(_defer)
    except tk.TclError:
        pass


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
        _scroll_canvas_units(canvas, int(-1 * (delta / 120)))
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
        _scroll_canvas_units(canvas, direction)
    except tk.TclError:
        pass
    return "break"


def _install_on_root(root: tk.Misc) -> None:
    global _root

    if _root is None:
        _root = root
    if getattr(root, "_amt_canvas_wheel_dispatch", False):
        return
    root._amt_canvas_wheel_dispatch = True

    root.bind_all("<MouseWheel>", _dispatch_windows)
    root.bind_all("<Button-4>", lambda e: _dispatch_linux(e, -1))
    root.bind_all("<Button-5>", lambda e: _dispatch_linux(e, 1))


def unregister_canvas_scroll(canvas: tk.Canvas, scrollable_frame: tk.Misc) -> None:
    """Remove registration (e.g. when destroying a tab)."""
    pair = (canvas, scrollable_frame)
    try:
        _registrations.remove(pair)
    except ValueError:
        pass
