# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Simple hover tooltip for tkinter widgets.
"""

import tkinter as tk


def add_tooltip(widget, text, delay_ms=500):
    """
    Add a tooltip that shows after delay_ms when the mouse hovers over the widget.
    
    Args:
        widget: tk/ttk widget to attach tooltip to
        text: tooltip text
        delay_ms: delay before showing (milliseconds)
    """
    tip = [None]  # use list so inner func can rebind
    after_id = [None]

    def on_enter(event):
        def show():
            if tip[0] is not None:
                return
            tw = tk.Toplevel(widget)
            tw.wm_overrideredirect(True)
            tw.wm_geometry(f"+{event.x_root + 12}+{event.y_root + 12}")
            lbl = tk.Label(tw, text=text, justify=tk.LEFT, background="#ffffe0",
                           relief=tk.SOLID, borderwidth=1, font=("Segoe UI", 9))
            lbl.pack()
            tip[0] = tw

        after_id[0] = widget.after(delay_ms, show)

    def on_leave(event):
        if after_id[0] is not None:
            widget.after_cancel(after_id[0])
            after_id[0] = None
        if tip[0] is not None:
            tip[0].destroy()
            tip[0] = None

    widget.bind("<Enter>", on_enter)
    widget.bind("<Leave>", on_leave)
