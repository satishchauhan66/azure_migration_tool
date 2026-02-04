"""
Streaming Log Console Window
Real-time log viewer for debugging and monitoring application activity.
"""

import tkinter as tk
from tkinter import ttk, scrolledtext
import logging
import sys
import io
import threading
import queue
from datetime import datetime
from typing import Optional


class QueueHandler(logging.Handler):
    """Custom logging handler that puts log records into a queue."""
    
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue
        
    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_queue.put((record.levelno, msg))
        except Exception:
            self.handleError(record)


class StdoutRedirector(io.StringIO):
    """Redirects stdout to a queue for display in the console."""
    
    def __init__(self, log_queue, original_stdout, level=logging.INFO):
        super().__init__()
        self.log_queue = log_queue
        self.original_stdout = original_stdout
        self.level = level
        
    def write(self, text):
        if text and text.strip():
            self.log_queue.put((self.level, text.rstrip()))
        # Also write to original stdout
        if self.original_stdout:
            self.original_stdout.write(text)
            
    def flush(self):
        if self.original_stdout:
            self.original_stdout.flush()


class LogConsoleWindow:
    """Floating log console window for streaming application logs."""
    
    _instance = None
    _log_queue = queue.Queue()
    _handler = None
    _stdout_redirector = None
    _stderr_redirector = None
    _original_stdout = None
    _original_stderr = None
    
    def __new__(cls, parent=None):
        """Singleton pattern - only one console window."""
        if cls._instance is None or not cls._instance._is_alive():
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, parent=None):
        if self._initialized:
            # Just bring existing window to front
            self._bring_to_front()
            return
            
        self._initialized = True
        self.parent = parent
        self.window = None
        self.text_widget = None
        self.auto_scroll = True
        self.stay_on_top = False
        self.paused = False
        self.filter_level = logging.DEBUG
        
        self._create_window()
        self._setup_logging()
        self._start_queue_processor()
        
    def _is_alive(self):
        """Check if window still exists."""
        try:
            return self.window is not None and self.window.winfo_exists()
        except:
            return False
            
    def _bring_to_front(self):
        """Bring existing window to front."""
        if self._is_alive():
            self.window.lift()
            self.window.focus_force()
            
    def _create_window(self):
        """Create the console window."""
        self.window = tk.Toplevel(self.parent)
        self.window.title("📋 Log Console - Real-time Streaming")
        self.window.geometry("900x500")
        self.window.minsize(600, 300)
        
        # Configure window
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)
        
        # Create toolbar frame
        toolbar = ttk.Frame(self.window)
        toolbar.pack(fill=tk.X, padx=5, pady=5)
        
        # Clear button
        ttk.Button(toolbar, text="🗑️ Clear", command=self._clear_log).pack(side=tk.LEFT, padx=2)
        
        # Pause/Resume button
        self.pause_btn = ttk.Button(toolbar, text="⏸️ Pause", command=self._toggle_pause)
        self.pause_btn.pack(side=tk.LEFT, padx=2)
        
        # Separator
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=10)
        
        # Filter by level
        ttk.Label(toolbar, text="Filter:").pack(side=tk.LEFT, padx=2)
        self.level_var = tk.StringVar(value="DEBUG")
        level_combo = ttk.Combobox(toolbar, textvariable=self.level_var, 
                                   values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                                   state="readonly", width=10)
        level_combo.pack(side=tk.LEFT, padx=2)
        level_combo.bind("<<ComboboxSelected>>", self._on_level_change)
        
        # Search
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=10)
        ttk.Label(toolbar, text="Search:").pack(side=tk.LEFT, padx=2)
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(toolbar, textvariable=self.search_var, width=20)
        search_entry.pack(side=tk.LEFT, padx=2)
        search_entry.bind("<Return>", lambda e: self._search_next())
        ttk.Button(toolbar, text="Find", command=self._search_next).pack(side=tk.LEFT, padx=2)
        
        # Auto-scroll checkbox
        self.auto_scroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(toolbar, text="Auto-scroll", variable=self.auto_scroll_var,
                       command=self._on_auto_scroll_change).pack(side=tk.RIGHT, padx=5)
        
        # Stay on top checkbox
        self.stay_on_top_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(toolbar, text="Stay on top", variable=self.stay_on_top_var,
                       command=self._on_stay_on_top_change).pack(side=tk.RIGHT, padx=5)
        
        # Create text widget with scrollbar
        text_frame = ttk.Frame(self.window)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))
        
        # Create text widget
        self.text_widget = tk.Text(text_frame, wrap=tk.WORD, font=("Consolas", 9),
                                   bg="#1e1e1e", fg="#d4d4d4", insertbackground="white")
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.text_widget.yview)
        self.text_widget.configure(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Configure text tags for different log levels
        self.text_widget.tag_configure("DEBUG", foreground="#808080")
        self.text_widget.tag_configure("INFO", foreground="#d4d4d4")
        self.text_widget.tag_configure("WARNING", foreground="#dcdcaa")
        self.text_widget.tag_configure("ERROR", foreground="#f14c4c")
        self.text_widget.tag_configure("CRITICAL", foreground="#ff0000", font=("Consolas", 9, "bold"))
        self.text_widget.tag_configure("TIMESTAMP", foreground="#569cd6")
        self.text_widget.tag_configure("HIGHLIGHT", background="#264f78")
        
        # Status bar
        self.status_var = tk.StringVar(value="Console ready - streaming logs...")
        status_bar = ttk.Label(self.window, textvariable=self.status_var, 
                              relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)
        
        # Make text widget read-only (but allow selection)
        self.text_widget.bind("<Key>", lambda e: "break" if e.keysym not in ["c", "C"] or not (e.state & 4) else None)
        
        # Add welcome message
        self._log_message(logging.INFO, "=" * 60)
        self._log_message(logging.INFO, "  Log Console Started - Streaming application logs")
        self._log_message(logging.INFO, f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._log_message(logging.INFO, "=" * 60)
        self._log_message(logging.INFO, "")
        
        self.line_count = 0
        self.max_lines = 10000  # Keep last 10000 lines to prevent memory issues
        
    def _setup_logging(self):
        """Setup logging handler to capture all logs."""
        # Create and add queue handler to root logger
        if LogConsoleWindow._handler is None:
            LogConsoleWindow._handler = QueueHandler(LogConsoleWindow._log_queue)
            LogConsoleWindow._handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(name)s - %(message)s')
            LogConsoleWindow._handler.setFormatter(formatter)
            
            # Add to root logger
            root_logger = logging.getLogger()
            root_logger.addHandler(LogConsoleWindow._handler)
            root_logger.setLevel(logging.DEBUG)
            
        # Redirect stdout/stderr
        if LogConsoleWindow._original_stdout is None:
            LogConsoleWindow._original_stdout = sys.stdout
            LogConsoleWindow._original_stderr = sys.stderr
            
            LogConsoleWindow._stdout_redirector = StdoutRedirector(
                LogConsoleWindow._log_queue, 
                LogConsoleWindow._original_stdout,
                logging.INFO
            )
            LogConsoleWindow._stderr_redirector = StdoutRedirector(
                LogConsoleWindow._log_queue, 
                LogConsoleWindow._original_stderr,
                logging.ERROR
            )
            
            sys.stdout = LogConsoleWindow._stdout_redirector
            sys.stderr = LogConsoleWindow._stderr_redirector
            
    def _start_queue_processor(self):
        """Start processing log queue."""
        self._process_queue()
        
    def _process_queue(self):
        """Process messages from the queue."""
        if not self._is_alive():
            return
            
        try:
            while True:
                try:
                    level, msg = LogConsoleWindow._log_queue.get_nowait()
                    if not self.paused and level >= self.filter_level:
                        self._log_message(level, msg)
                except queue.Empty:
                    break
        except Exception as e:
            pass
            
        # Schedule next check
        if self._is_alive():
            self.window.after(100, self._process_queue)
            
    def _log_message(self, level: int, message: str):
        """Add a log message to the console."""
        if not self._is_alive():
            return
            
        try:
            # Determine tag based on level
            if level >= logging.CRITICAL:
                level_tag = "CRITICAL"
                level_str = "CRIT"
            elif level >= logging.ERROR:
                level_tag = "ERROR"
                level_str = "ERR "
            elif level >= logging.WARNING:
                level_tag = "WARNING"
                level_str = "WARN"
            elif level >= logging.INFO:
                level_tag = "INFO"
                level_str = "INFO"
            else:
                level_tag = "DEBUG"
                level_str = "DBG "
                
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            
            # Insert timestamp
            self.text_widget.insert(tk.END, f"[{timestamp}] ", "TIMESTAMP")
            self.text_widget.insert(tk.END, f"[{level_str}] ", level_tag)
            self.text_widget.insert(tk.END, f"{message}\n", level_tag)
            
            self.line_count += 1
            
            # Trim if too many lines
            if self.line_count > self.max_lines:
                self.text_widget.delete("1.0", "1000.0")
                self.line_count -= 1000
                
            # Auto-scroll
            if self.auto_scroll_var.get():
                self.text_widget.see(tk.END)
                
            # Update status
            self.status_var.set(f"Lines: {self.line_count} | Filter: {self.level_var.get()}")
            
        except Exception as e:
            pass
            
    def _clear_log(self):
        """Clear the log console."""
        if self.text_widget:
            self.text_widget.delete("1.0", tk.END)
            self.line_count = 0
            self._log_message(logging.INFO, "Console cleared")
            
    def _toggle_pause(self):
        """Toggle pause/resume."""
        self.paused = not self.paused
        if self.paused:
            self.pause_btn.configure(text="▶️ Resume")
            self.status_var.set("PAUSED - Click Resume to continue")
        else:
            self.pause_btn.configure(text="⏸️ Pause")
            self.status_var.set("Streaming resumed...")
            
    def _on_level_change(self, event=None):
        """Handle log level filter change."""
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        self.filter_level = level_map.get(self.level_var.get(), logging.DEBUG)
        self._log_message(logging.INFO, f"Filter changed to: {self.level_var.get()}")
        
    def _on_auto_scroll_change(self):
        """Handle auto-scroll toggle."""
        self.auto_scroll = self.auto_scroll_var.get()
        
    def _on_stay_on_top_change(self):
        """Handle stay on top toggle."""
        self.stay_on_top = self.stay_on_top_var.get()
        self.window.attributes('-topmost', self.stay_on_top)
        
    def _search_next(self):
        """Find next occurrence of search term."""
        search_term = self.search_var.get()
        if not search_term:
            return
            
        # Remove previous highlights
        self.text_widget.tag_remove("HIGHLIGHT", "1.0", tk.END)
        
        # Search from current position or start
        start_pos = self.text_widget.index(tk.INSERT)
        pos = self.text_widget.search(search_term, start_pos, stopindex=tk.END, nocase=True)
        
        if not pos:
            # Wrap around
            pos = self.text_widget.search(search_term, "1.0", stopindex=start_pos, nocase=True)
            
        if pos:
            end_pos = f"{pos}+{len(search_term)}c"
            self.text_widget.tag_add("HIGHLIGHT", pos, end_pos)
            self.text_widget.mark_set(tk.INSERT, end_pos)
            self.text_widget.see(pos)
            self.status_var.set(f"Found at {pos}")
        else:
            self.status_var.set(f"'{search_term}' not found")
            
    def _on_close(self):
        """Handle window close."""
        self._initialized = False
        LogConsoleWindow._instance = None
        
        # Restore stdout/stderr
        if LogConsoleWindow._original_stdout:
            sys.stdout = LogConsoleWindow._original_stdout
            sys.stderr = LogConsoleWindow._original_stderr
            LogConsoleWindow._original_stdout = None
            LogConsoleWindow._original_stderr = None
            LogConsoleWindow._stdout_redirector = None
            LogConsoleWindow._stderr_redirector = None
            
        # Remove handler
        if LogConsoleWindow._handler:
            root_logger = logging.getLogger()
            root_logger.removeHandler(LogConsoleWindow._handler)
            LogConsoleWindow._handler = None
            
        if self.window:
            self.window.destroy()
            self.window = None
            
    def log(self, message: str, level: int = logging.INFO):
        """Manually log a message."""
        LogConsoleWindow._log_queue.put((level, message))


def show_log_console(parent=None):
    """Show or create the log console window."""
    return LogConsoleWindow(parent)


def log_to_console(message: str, level: int = logging.INFO):
    """Log a message to the console if it exists."""
    LogConsoleWindow._log_queue.put((level, message))
