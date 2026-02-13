# Author: Sa-tish Chauhan

"""
Preview dialog for SQL restore operations.
Shows SQL query before deployment and allows user to approve or skip.
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox


class RestorePreviewDialog:
    """Dialog to preview SQL before deployment."""
    
    def __init__(self, parent, file_type: str, batch_number: int, total_batches: int, sql_batch: str):
        self.parent = parent
        self.file_type = file_type
        self.batch_number = batch_number
        self.total_batches = total_batches
        self.sql_batch = sql_batch
        self.approved = False
        self.skip_all = False  # Initialize skip_all flag
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title(f"Preview SQL - {file_type} (Batch {batch_number}/{total_batches})")
        self.dialog.geometry("800x600")
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Center the dialog
        self.dialog.update_idletasks()
        x = (self.dialog.winfo_screenwidth() // 2) - (self.dialog.winfo_width() // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (self.dialog.winfo_height() // 2)
        self.dialog.geometry(f"+{x}+{y}")
        
        self._create_widgets()
        
        # Wait for user response
        self.dialog.wait_window()
    
    def _create_widgets(self):
        """Create dialog widgets."""
        # Header frame
        header_frame = ttk.Frame(self.dialog, padding=10)
        header_frame.pack(fill=tk.X)
        
        ttk.Label(
            header_frame,
            text=f"Review SQL before deployment",
            font=("Arial", 12, "bold")
        ).pack(anchor=tk.W)
        
        info_text = f"Type: {self.file_type} | Batch: {self.batch_number} of {self.total_batches}"
        ttk.Label(header_frame, text=info_text, foreground="gray").pack(anchor=tk.W, pady=(5, 0))
        
        # SQL preview frame
        sql_frame = ttk.LabelFrame(self.dialog, text="SQL Query", padding=10)
        sql_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.sql_text = scrolledtext.ScrolledText(
            sql_frame,
            wrap=tk.NONE,
            font=("Consolas", 10),
            width=80,
            height=20
        )
        self.sql_text.pack(fill=tk.BOTH, expand=True)
        self.sql_text.insert("1.0", self.sql_batch)
        self.sql_text.config(state=tk.DISABLED)  # Read-only
        
        # Buttons frame
        button_frame = ttk.Frame(self.dialog, padding=10)
        button_frame.pack(fill=tk.X)
        
        ttk.Button(
            button_frame,
            text="Deploy",
            command=self._on_deploy,
            width=15
        ).pack(side=tk.RIGHT, padx=5)
        
        ttk.Button(
            button_frame,
            text="Skip",
            command=self._on_skip,
            width=15
        ).pack(side=tk.RIGHT, padx=5)
        
        ttk.Button(
            button_frame,
            text="Skip All Remaining",
            command=self._on_skip_all,
            width=20
        ).pack(side=tk.LEFT, padx=5)
    
    def _on_deploy(self):
        """User approved deployment."""
        self.approved = True
        self.dialog.destroy()
    
    def _on_skip(self):
        """User skipped this batch."""
        self.approved = False
        self.dialog.destroy()
    
    def _on_skip_all(self):
        """User wants to skip all remaining batches."""
        if messagebox.askyesno(
            "Skip All Remaining",
            "Are you sure you want to skip all remaining batches in this file type?"
        ):
            self.approved = False
            self.skip_all = True
            self.dialog.destroy()
        else:
            self.skip_all = False


def create_preview_callback(parent_window):
    """
    Create a preview callback function for restore operations.
    This callback must be called from the main Tkinter thread.
    
    Args:
        parent_window: Tkinter root window or frame
        
    Returns:
        Callback function that shows preview dialog and returns True/False
    """
    import threading
    skip_all_flag = {"value": False}
    result_queue = {}  # Store results by batch number
    
    def preview_callback(file_type: str, batch_number: int, total_batches: int, sql_batch: str, batch_index: int = None):
        """
        Preview callback function.
        Must be called from main Tkinter thread (use after() if in background thread).
        
        Args:
            file_type: Type of object being restored (FOREIGN_KEYS, INDEXES, etc.)
            batch_number: Current batch number
            total_batches: Total number of batches
            sql_batch: SQL query to preview
            batch_index: Optional batch index
            
        Returns:
            True if user approved, False if skipped
        """
        # Check if user previously chose to skip all
        if skip_all_flag["value"]:
            return False
        
        # Check if we're on the main thread
        if threading.current_thread() != threading.main_thread():
            # We're in a background thread - need to schedule on main thread
            import queue
            result_queue_obj = queue.Queue()
            result_queue[batch_number] = result_queue_obj
            
            def show_dialog():
                try:
                    dialog = RestorePreviewDialog(
                        parent=parent_window,
                        file_type=file_type,
                        batch_number=batch_number,
                        total_batches=total_batches,
                        sql_batch=sql_batch
                    )
                    
                    # Check if user chose to skip all
                    if hasattr(dialog, 'skip_all') and dialog.skip_all:
                        skip_all_flag["value"] = True
                    
                    result_queue_obj.put(dialog.approved)
                except Exception as e:
                    result_queue_obj.put(False)  # Default to skip on error
            
            # Schedule on main thread
            parent_window.after(0, show_dialog)
            
            # Wait for result (with timeout)
            try:
                result = result_queue_obj.get(timeout=300)  # 5 minute timeout
                return result
            except queue.Empty:
                return False  # Timeout - skip
        else:
            # We're on main thread - show dialog directly
            dialog = RestorePreviewDialog(
                parent=parent_window,
                file_type=file_type,
                batch_number=batch_number,
                total_batches=total_batches,
                sql_batch=sql_batch
            )
            
            # Check if user chose to skip all
            if hasattr(dialog, 'skip_all') and dialog.skip_all:
                skip_all_flag["value"] = True
            
            return dialog.approved
    
    return preview_callback
