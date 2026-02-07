# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""
Scrollable frame utility for tabs that have too much content.
"""

import tkinter as tk
from tkinter import ttk


class ScrollableFrame(ttk.Frame):
    """A scrollable frame widget."""
    
    def __init__(self, parent, *args, **kwargs):
        # Create canvas and scrollbar
        self.canvas = tk.Canvas(parent, *args, **kwargs)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)
        
        # Pack canvas and scrollbar
        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Enable mouse wheel scrolling
        def _on_mousewheel(event):
            self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        self.canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        # Also bind to canvas focus
        self.canvas.bind("<Enter>", lambda e: self.canvas.focus_set())
        self.canvas.bind("<Leave>", lambda e: self.canvas.master.focus_set())
        
        # Update scroll region on configure
        def update_scroll_region(event=None):
            self.canvas.update_idletasks()
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        self.scrollable_frame.bind("<Configure>", update_scroll_region)
        
        # Make scrollable_frame the main widget
        super().__init__(self.scrollable_frame)


