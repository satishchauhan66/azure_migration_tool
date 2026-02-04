"""
Schema diff viewer dialog for side-by-side comparison.
"""

import tkinter as tk
from tkinter import ttk, scrolledtext


class SchemaDiffViewer(tk.Toplevel):
    """Dialog for viewing schema object differences side-by-side."""
    
    def __init__(self, parent, obj_name: str, source_code: str, dest_code: str):
        """
        Initialize diff viewer dialog.
        
        Args:
            parent: Parent window
            obj_name: Name of the object being compared
            source_code: Source code (left side)
            dest_code: Destination code (right side)
        """
        super().__init__(parent)
        self.obj_name = obj_name
        self.source_code = source_code or ""
        self.dest_code = dest_code or ""
        
        self.title(f"Schema Comparison: {obj_name}")
        self.geometry("1200x700")
        
        # Main frame
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Labels frame
        labels_frame = ttk.Frame(main_frame)
        labels_frame.pack(fill=tk.X, pady=(0, 5))
        
        tk.Label(labels_frame, text="Source (Left)", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Label(labels_frame, text="Destination (Right)", font=("Arial", 10, "bold")).pack(side=tk.RIGHT, padx=5)
        
        # Text widgets frame
        text_frame = ttk.Frame(main_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        # Source text widget (left)
        source_frame = ttk.Frame(text_frame)
        source_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        tk.Label(source_frame, text="Source Code", font=("Arial", 9)).pack(anchor=tk.W)
        source_scroll = ttk.Scrollbar(source_frame)
        source_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.source_text = tk.Text(source_frame, wrap=tk.NONE, yscrollcommand=source_scroll.set,
                                  font=("Consolas", 10), bg="white", fg="black", state=tk.DISABLED)
        self.source_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        source_scroll.config(command=self.source_text.yview)
        
        # Destination text widget (right)
        dest_frame = ttk.Frame(text_frame)
        dest_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))
        
        tk.Label(dest_frame, text="Destination Code", font=("Arial", 9)).pack(anchor=tk.W)
        dest_scroll = ttk.Scrollbar(dest_frame)
        dest_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.dest_text = tk.Text(dest_frame, wrap=tk.NONE, yscrollcommand=dest_scroll.set,
                                font=("Consolas", 10), bg="white", fg="black", state=tk.DISABLED)
        self.dest_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        dest_scroll.config(command=self.dest_text.yview)
        
        # Sync scrolling
        self.source_text.bind('<KeyRelease>', self._sync_scroll)
        self.source_text.bind('<Button-1>', self._sync_scroll)
        self.source_text.bind('<MouseWheel>', self._sync_scroll)
        self.dest_text.bind('<KeyRelease>', self._sync_scroll)
        self.dest_text.bind('<Button-1>', self._sync_scroll)
        self.dest_text.bind('<MouseWheel>', self._sync_scroll)
        
        # Load content
        self.source_text.config(state=tk.NORMAL)
        self.source_text.insert("1.0", self.source_code)
        self.source_text.config(state=tk.DISABLED)
        
        self.dest_text.config(state=tk.NORMAL)
        self.dest_text.insert("1.0", self.dest_code)
        self.dest_text.config(state=tk.DISABLED)
        
        # Highlight differences
        self._highlight_differences()
        
        # Buttons frame
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(btn_frame, text="Close", command=self.destroy).pack(side=tk.RIGHT, padx=5)
    
    def _sync_scroll(self, event):
        """Sync scrolling between source and destination text widgets."""
        try:
            # Get scroll position from source
            source_pos = self.source_text.yview()[0]
            # Apply to destination
            self.dest_text.yview_moveto(source_pos)
        except:
            pass
    
    def _highlight_differences(self):
        """Highlight differences between source and destination code."""
        self.source_text.config(state=tk.NORMAL)
        self.dest_text.config(state=tk.NORMAL)
        
        source_lines = self.source_code.split('\n')
        dest_lines = self.dest_code.split('\n')
        
        # Simple line-by-line comparison
        max_lines = max(len(source_lines), len(dest_lines))
        
        for i in range(max_lines):
            source_line = source_lines[i] if i < len(source_lines) else ""
            dest_line = dest_lines[i] if i < len(dest_lines) else ""
            
            if source_line != dest_line:
                # Highlight source line
                start_pos = f"{i+1}.0"
                end_pos = f"{i+1}.end"
                self.source_text.tag_add("diff", start_pos, end_pos)
                self.dest_text.tag_add("diff", start_pos, end_pos)
        
        # Configure diff tag
        self.source_text.tag_config("diff", background="#ffcccc", foreground="black")
        self.dest_text.tag_config("diff", background="#ffcccc", foreground="black")
        
        self.source_text.config(state=tk.DISABLED)
        self.dest_text.config(state=tk.DISABLED)
