# Author: Satish Chauhan
"""Dedicated BCP Migration tab — list/select tables, validate, migrate (V1+V2)."""

from __future__ import annotations

import csv
import json
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Any, Dict, List, Optional

from gui.utils.canvas_mousewheel import bind_canvas_vertical_scroll
from gui.widgets.connection_widget import ConnectionWidget

try:
    from src.migration.bcp_migration import (
        default_work_dir,
        enrich_with_dest,
        list_tables,
        run_bcp_migration,
    )
    from src.migration.bcp_preflight import preflight_passed, run_bcp_preflight
    from src.migration.csv_bcp_import import (
        auto_map_columns,
        list_dest_columns,
        peek_csv_headers,
        run_csv_bcp_import,
    )
    from src.migration.db2_bcp_migration import (
        list_db2_tables,
        run_db2_bcp_migration,
        run_db2_preflight,
    )
    from src.utils.bcp_tools import find_bcp_exe
except ImportError:
    from azure_migration_tool.src.migration.bcp_migration import (
        default_work_dir,
        enrich_with_dest,
        list_tables,
        run_bcp_migration,
    )
    from azure_migration_tool.src.migration.bcp_preflight import preflight_passed, run_bcp_preflight
    from azure_migration_tool.src.migration.csv_bcp_import import (
        auto_map_columns,
        list_dest_columns,
        peek_csv_headers,
        run_csv_bcp_import,
    )
    from azure_migration_tool.src.migration.db2_bcp_migration import (
        list_db2_tables,
        run_db2_bcp_migration,
        run_db2_preflight,
    )
    from azure_migration_tool.src.utils.bcp_tools import find_bcp_exe


class BcpTab:
    """Advanced BCP migration UI for one or many SQL Server tables."""

    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.project_path: Optional[Path] = None
        self._tables: List[Any] = []
        self._busy = False
        self._cancel = threading.Event()
        self._preflight_ok = False
        self._csv_files: List[str] = []
        self._csv_headers: List[str] = []
        self._csv_map_vars: Dict[str, tk.StringVar] = {}
        self._create_widgets()

    def set_project_path(self, project_path):
        self.project_path = Path(project_path) if project_path else None

    def _create_widgets(self):
        canvas = tk.Canvas(self.frame)
        scrollbar = ttk.Scrollbar(self.frame, orient="vertical", command=canvas.yview)
        scrollable = ttk.Frame(canvas)
        scrollable.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        win = canvas.create_window((0, 0), window=scrollable, anchor="nw")

        def _on_cfg(event):
            canvas.itemconfig(win, width=event.width)

        canvas.bind("<Configure>", _on_cfg)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        bind_canvas_vertical_scroll(canvas, scrollable)

        tk.Label(scrollable, text="BCP Migration", font=("Arial", 14, "bold")).pack(pady=(8, 2))
        self.subtitle_var = tk.StringVar(
            value="SQL Server → SQL Server · pick tables · Validate · Start"
        )
        tk.Label(scrollable, textvariable=self.subtitle_var, fg="gray").pack(
            anchor=tk.W, padx=10, pady=(0, 6)
        )

        mode_row = ttk.Frame(scrollable)
        mode_row.pack(fill=tk.X, padx=10, pady=(0, 4))
        tk.Label(mode_row, text="Source mode").pack(side=tk.LEFT)
        self.source_mode_var = tk.StringVar(value="sql")
        ttk.Radiobutton(
            mode_row,
            text="Database",
            value="sql",
            variable=self.source_mode_var,
            command=self._on_source_mode_changed,
        ).pack(side=tk.LEFT, padx=(8, 4))
        ttk.Radiobutton(
            mode_row,
            text="CSV files",
            value="csv",
            variable=self.source_mode_var,
            command=self._on_source_mode_changed,
        ).pack(side=tk.LEFT, padx=4)

        # --- Connections ---
        self._conn_frame = ttk.Frame(scrollable)
        self._conn_frame.pack(fill=tk.X, padx=10, pady=2)
        self._src_frame = ttk.LabelFrame(self._conn_frame, text="Source", padding=8)
        self._src_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        self._dest_frame = ttk.LabelFrame(self._conn_frame, text="Destination", padding=8)
        self._dest_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))
        left = self._src_frame
        right = self._dest_frame

        self.src_server_var = tk.StringVar()
        self.src_db_var = tk.StringVar()
        self.src_auth_var = tk.StringVar(value="windows")
        self.src_user_var = tk.StringVar()
        self.src_password_var = tk.StringVar()
        self.src_db_type_var = tk.StringVar(value="sqlserver")
        self.src_db_type_var.trace_add("write", lambda *_: self._update_subtitle())
        self.src_widget = ConnectionWidget(
            parent=left,
            server_var=self.src_server_var,
            db_var=self.src_db_var,
            auth_var=self.src_auth_var,
            user_var=self.src_user_var,
            password_var=self.src_password_var,
            label_text="",
            row_start=0,
            db_type_var=self.src_db_type_var,
        )

        self.dest_server_var = tk.StringVar()
        self.dest_db_var = tk.StringVar()
        self.dest_auth_var = tk.StringVar(value="windows")
        self.dest_user_var = tk.StringVar()
        self.dest_password_var = tk.StringVar()
        self.dest_db_type_var = tk.StringVar(value="sqlserver")
        self.dest_widget = ConnectionWidget(
            parent=right,
            server_var=self.dest_server_var,
            db_var=self.dest_db_var,
            auth_var=self.dest_auth_var,
            user_var=self.dest_user_var,
            password_var=self.dest_password_var,
            label_text="",
            row_start=0,
            db_type_var=self.dest_db_type_var,
        )

        # --- Staging (disk / network) ---
        self._staging_frame = ttk.LabelFrame(scrollable, text="Staging folder (BCP data files)", padding=8)
        self._staging_frame.pack(fill=tk.X, padx=10, pady=6)
        self.work_dir_var = tk.StringVar(value=str(default_work_dir()))
        st_row = ttk.Frame(self._staging_frame)
        st_row.pack(fill=tk.X)
        ttk.Entry(st_row, textvariable=self.work_dir_var).pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 6)
        )
        ttk.Button(st_row, text="Browse…", command=self._browse_work, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(st_row, text="Test", command=self._test_staging, width=8).pack(side=tk.LEFT, padx=2)
        tk.Label(
            self._staging_frame,
            text=(
                "Low local disk? Use a network UNC or Azure Files SMB path "
                r"(e.g. \\fileserver\share\bcp or \\acct.file.core.windows.net\share\bcp). "
                "Raw https:// blob URLs are not supported by bcp.exe."
            ),
            fg="gray",
            wraplength=880,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(4, 0))

        # --- CSV source (shown only in CSV mode) ---
        self._csv_frame = ttk.LabelFrame(scrollable, text="CSV source → destination table", padding=8)
        # packed by _on_source_mode_changed

        csv_files_row = ttk.Frame(self._csv_frame)
        csv_files_row.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(csv_files_row, text="Add CSV…", command=self._csv_add_files, width=12).pack(
            side=tk.LEFT
        )
        ttk.Button(csv_files_row, text="Clear", command=self._csv_clear_files, width=8).pack(
            side=tk.LEFT, padx=4
        )
        self.csv_files_var = tk.StringVar(value="No files selected")
        tk.Label(csv_files_row, textvariable=self.csv_files_var, fg="gray").pack(
            side=tk.LEFT, padx=8
        )

        csv_opts = ttk.Frame(self._csv_frame)
        csv_opts.pack(fill=tk.X, pady=2)
        tk.Label(csv_opts, text="Dest table").pack(side=tk.LEFT)
        self.csv_table_var = tk.StringVar(value="")
        ttk.Entry(csv_opts, textvariable=self.csv_table_var, width=28).pack(side=tk.LEFT, padx=4)
        tk.Label(csv_opts, text="(schema.name)", fg="gray").pack(side=tk.LEFT, padx=(0, 6))
        tk.Label(csv_opts, text="Delimiter").pack(side=tk.LEFT, padx=(10, 0))
        self.csv_delim_var = tk.StringVar(value=",")
        ttk.Entry(csv_opts, textvariable=self.csv_delim_var, width=4).pack(side=tk.LEFT, padx=4)
        tk.Label(csv_opts, text="Encoding").pack(side=tk.LEFT, padx=(10, 0))
        self.csv_encoding_var = tk.StringVar(value="utf-8")
        ttk.Entry(csv_opts, textvariable=self.csv_encoding_var, width=10).pack(side=tk.LEFT, padx=4)
        self.csv_has_header_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(csv_opts, text="Has header", variable=self.csv_has_header_var).pack(
            side=tk.LEFT, padx=8
        )

        csv_wide = ttk.Frame(self._csv_frame)
        csv_wide.pack(fill=tk.X, pady=2)
        tk.Label(csv_wide, text="Wide text column (unquoted commas)").pack(side=tk.LEFT)
        self.csv_wide_col_var = tk.StringVar(value="")
        self.csv_wide_combo = ttk.Combobox(
            csv_wide, textvariable=self.csv_wide_col_var, width=28, state="readonly"
        )
        self.csv_wide_combo.pack(side=tk.LEFT, padx=4)
        ttk.Button(csv_wide, text="Load mapping", command=self._csv_load_mapping, width=14).pack(
            side=tk.LEFT, padx=8
        )
        ttk.Button(csv_wide, text="Auto-map", command=self._csv_auto_map, width=10).pack(
            side=tk.LEFT
        )

        map_frame = ttk.Frame(self._csv_frame)
        map_frame.pack(fill=tk.BOTH, expand=True, pady=(6, 0))
        self.csv_map_canvas = tk.Canvas(map_frame, height=160, highlightthickness=0)
        map_scroll = ttk.Scrollbar(map_frame, orient=tk.VERTICAL, command=self.csv_map_canvas.yview)
        self.csv_map_inner = ttk.Frame(self.csv_map_canvas)
        self.csv_map_inner.bind(
            "<Configure>",
            lambda e: self.csv_map_canvas.configure(scrollregion=self.csv_map_canvas.bbox("all")),
        )
        self.csv_map_canvas.create_window((0, 0), window=self.csv_map_inner, anchor="nw")
        self.csv_map_canvas.configure(yscrollcommand=map_scroll.set)
        self.csv_map_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        map_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        tk.Label(
            self._csv_frame,
            text="Map each destination column to a CSV header (blank = skip). Create missing uses NVARCHAR(MAX).",
            fg="gray",
            wraplength=880,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(4, 0))

        # --- Tables (SQL mode) ---
        self._picker_frame = ttk.LabelFrame(scrollable, text="Tables", padding=8)
        self._picker_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=4)
        picker = self._picker_frame

        filt = ttk.Frame(picker)
        filt.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(filt, text="Refresh", command=self._refresh_tables, width=10).pack(side=tk.LEFT)
        tk.Label(filt, text="Search").pack(side=tk.LEFT, padx=(10, 2))
        self.filter_var = tk.StringVar()
        self.filter_var.trace_add("write", lambda *_: self._apply_filter())
        ttk.Entry(filt, textvariable=self.filter_var, width=28).pack(side=tk.LEFT, padx=2)

        sel_menu = ttk.Menubutton(filt, text="Selection ▾")
        sel_menu.pack(side=tk.LEFT, padx=8)
        menu = tk.Menu(sel_menu, tearoff=0)
        menu.add_command(label="Select all visible", command=self._select_all)
        menu.add_command(label="Clear selection", command=self._clear_sel)
        menu.add_command(label="Invert selection", command=self._invert_sel)
        menu.add_separator()
        menu.add_command(label="Load list from CSV…", command=self._load_selection_file)
        menu.add_command(label="Copy source → dest connection", command=self._copy_src_to_dest)
        sel_menu["menu"] = menu

        self.filter_missing_var = tk.BooleanVar(value=False)
        self.filter_mismatch_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            filt, text="Missing on dest", variable=self.filter_missing_var, command=self._apply_filter
        ).pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(
            filt, text="Row mismatch", variable=self.filter_mismatch_var, command=self._apply_filter
        ).pack(side=tk.LEFT, padx=4)

        tree_frame = ttk.Frame(picker)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        cols = ("check", "schema", "table", "src_rows", "dest_exists", "dest_rows", "status")
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=11, selectmode="browse")
        self.tree.heading("check", text="✓")
        self.tree.heading("schema", text="Schema")
        self.tree.heading("table", text="Table")
        self.tree.heading("src_rows", text="Src rows")
        self.tree.heading("dest_exists", text="On dest?")
        self.tree.heading("dest_rows", text="Dest rows")
        self.tree.heading("status", text="Status")
        self.tree.column("check", width=36, anchor=tk.CENTER, stretch=False)
        self.tree.column("schema", width=100)
        self.tree.column("table", width=220)
        self.tree.column("src_rows", width=90, anchor=tk.E)
        self.tree.column("dest_exists", width=70, anchor=tk.CENTER)
        self.tree.column("dest_rows", width=90, anchor=tk.E)
        self.tree.column("status", width=140)
        ysb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=ysb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.bind("<Button-1>", self._on_tree_click)
        self._checked: Dict[str, bool] = {}

        # Defaults for options (advanced panel may hide most of these)
        self.create_missing_var = tk.BooleanVar(value=True)
        self.truncate_var = tk.BooleanVar(value=False)
        self.keep_identity_var = tk.BooleanVar(value=True)
        self.native_var = tk.BooleanVar(value=True)
        self.verify_var = tk.BooleanVar(value=True)
        self.continue_on_error_var = tk.BooleanVar(value=True)
        self.skip_equal_var = tk.BooleanVar(value=False)
        self.resume_var = tk.BooleanVar(value=False)
        self.keep_files_var = tk.BooleanVar(value=False)
        self.dry_run_var = tk.BooleanVar(value=False)
        self.batch_var = tk.StringVar(value="50000")
        self.parallel_var = tk.StringVar(value="4")
        self.retries_var = tk.StringVar(value="2")
        self.exclude_var = tk.StringVar(value="tmp_*,temp_*,backup_*")

        # --- Simple options always visible ---
        self._simple_frame = ttk.Frame(scrollable)
        self._simple_frame.pack(fill=tk.X, padx=10, pady=4)
        ttk.Checkbutton(
            self._simple_frame, text="Create missing tables", variable=self.create_missing_var
        ).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Checkbutton(
            self._simple_frame, text="Truncate before load", variable=self.truncate_var
        ).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Checkbutton(
            self._simple_frame, text="Verify row counts", variable=self.verify_var
        ).pack(side=tk.LEFT)

        # --- Advanced (collapsed) ---
        self._advanced_visible = tk.BooleanVar(value=False)
        adv_toggle = ttk.Frame(scrollable)
        adv_toggle.pack(fill=tk.X, padx=10, pady=(2, 0))
        self._adv_btn = ttk.Button(
            adv_toggle, text="Show advanced ▸", command=self._toggle_advanced, width=18
        )
        self._adv_btn.pack(side=tk.LEFT)

        self._adv_frame = ttk.LabelFrame(scrollable, text="Advanced", padding=8)
        # packed only when toggled on
        row1 = ttk.Frame(self._adv_frame)
        row1.pack(fill=tk.X)
        ttk.Checkbutton(row1, text="Keep identity (-E)", variable=self.keep_identity_var).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Checkbutton(row1, text="Native format (-n)", variable=self.native_var).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Checkbutton(row1, text="Skip if src==dest", variable=self.skip_equal_var).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Checkbutton(row1, text="Resume", variable=self.resume_var).pack(side=tk.LEFT, padx=4)
        ttk.Checkbutton(row1, text="Keep .bcp files", variable=self.keep_files_var).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Checkbutton(row1, text="Dry-run", variable=self.dry_run_var).pack(side=tk.LEFT, padx=4)

        row2 = ttk.Frame(self._adv_frame)
        row2.pack(fill=tk.X, pady=(6, 0))
        tk.Label(row2, text="Batch").pack(side=tk.LEFT)
        ttk.Entry(row2, textvariable=self.batch_var, width=8).pack(side=tk.LEFT, padx=4)
        tk.Label(row2, text="Parallel").pack(side=tk.LEFT, padx=(10, 0))
        ttk.Entry(row2, textvariable=self.parallel_var, width=5).pack(side=tk.LEFT, padx=4)
        tk.Label(row2, text="Retries").pack(side=tk.LEFT, padx=(10, 0))
        ttk.Entry(row2, textvariable=self.retries_var, width=5).pack(side=tk.LEFT, padx=4)
        tk.Label(row2, text="Exclude").pack(side=tk.LEFT, padx=(10, 0))
        ttk.Entry(row2, textvariable=self.exclude_var, width=28).pack(side=tk.LEFT, padx=4)

        # --- Primary actions only ---
        actions = ttk.Frame(scrollable)
        actions.pack(fill=tk.X, padx=10, pady=8)
        self.btn_validate = ttk.Button(actions, text="Validate", command=self._validate, width=12)
        self.btn_validate.pack(side=tk.LEFT, padx=(0, 6))
        self.btn_start = ttk.Button(actions, text="Start BCP", command=self._start, width=12)
        self.btn_start.pack(side=tk.LEFT, padx=(0, 6))
        self.btn_stop = ttk.Button(actions, text="Stop", command=self._stop, width=8, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=(0, 6))
        self.status_var = tk.StringVar(value="Ready")
        tk.Label(actions, textvariable=self.status_var, fg="gray").pack(side=tk.LEFT, padx=10)
        ttk.Button(actions, text="Export…", command=self._export_report, width=10).pack(side=tk.RIGHT)

        self.progress = ttk.Progressbar(scrollable, mode="determinate")
        self.progress.pack(fill=tk.X, padx=10, pady=(0, 2))
        self.progress_label = tk.StringVar(value="")
        tk.Label(scrollable, textvariable=self.progress_label, anchor=tk.W).pack(fill=tk.X, padx=10)

        # Combined activity: results + log in notebook to reduce clutter
        activity = ttk.Notebook(scrollable)
        activity.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)
        log_tab = ttk.Frame(activity)
        res_tab = ttk.Frame(activity)
        activity.add(log_tab, text="Log")
        activity.add(res_tab, text="Results")
        self.log = scrolledtext.ScrolledText(log_tab, height=10, wrap=tk.WORD)
        self.log.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        rcols = ("table", "status", "src", "dest", "sec", "message")
        self.results = ttk.Treeview(res_tab, columns=rcols, show="headings", height=8)
        for c, w, t in (
            ("table", 220, "Table"),
            ("status", 70, "Status"),
            ("src", 80, "Src rows"),
            ("dest", 80, "Dest rows"),
            ("sec", 70, "Sec"),
            ("message", 360, "Message"),
        ):
            self.results.heading(c, text=t)
            self.results.column(c, width=w)
        self.results.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        self._last_report: Optional[Dict[str, Any]] = None
        self._on_source_mode_changed()

    def _is_csv_mode(self) -> bool:
        return (self.source_mode_var.get() or "sql").strip().lower() == "csv"

    def _is_db2_source(self) -> bool:
        return (self.src_db_type_var.get() or "").strip().lower() == "db2"

    def _update_subtitle(self) -> None:
        if self._is_csv_mode():
            self.subtitle_var.set("CSV files → SQL Server · map columns · Validate · Start")
            return
        if self._is_db2_source():
            self.subtitle_var.set(
                "DB2 → SQL · auto EXPORT (large) / JDBC (small) + BCP · Validate · Start"
            )
        else:
            self.subtitle_var.set("SQL Server → SQL Server · pick tables · Validate · Start")

    def _on_source_mode_changed(self) -> None:
        csv_mode = self._is_csv_mode()
        self._src_frame.pack_forget()
        self._dest_frame.pack_forget()
        self._csv_frame.pack_forget()
        self._picker_frame.pack_forget()

        if csv_mode:
            self._dest_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            self._csv_frame.pack(
                fill=tk.BOTH, expand=True, padx=10, pady=4, before=self._simple_frame
            )
            self.btn_start.config(text="Start CSV import")
            self._preflight_ok = False
        else:
            self._src_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
            self._dest_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))
            self._picker_frame.pack(
                fill=tk.BOTH, expand=True, padx=10, pady=4, before=self._simple_frame
            )
            self.btn_start.config(text="Start BCP")
        self._update_subtitle()

    def _csv_add_files(self) -> None:
        paths = filedialog.askopenfilenames(
            title="Select CSV file(s)",
            filetypes=[("CSV", "*.csv"), ("Text", "*.txt"), ("All", "*.*")],
        )
        if not paths:
            return
        for p in paths:
            if p not in self._csv_files:
                self._csv_files.append(p)
        self._csv_refresh_files_label()
        # Auto-suggest dbo.<filename> when dest table is blank / incomplete
        cur = (self.csv_table_var.get() or "").strip()
        if self._csv_files and (not cur or cur.endswith(".") or "." not in cur):
            stem = Path(self._csv_files[0]).stem
            safe = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in stem).strip("_") or "CSV_IMPORT"
            self.csv_table_var.set(f"dbo.{safe}")
        if self._csv_files:
            self._csv_peek_headers()

    def _csv_clear_files(self) -> None:
        self._csv_files = []
        self._csv_headers = []
        self._csv_refresh_files_label()
        self.csv_wide_combo["values"] = [""]
        self.csv_wide_col_var.set("")

    def _csv_refresh_files_label(self) -> None:
        n = len(self._csv_files)
        if n == 0:
            self.csv_files_var.set("No files selected")
        elif n == 1:
            self.csv_files_var.set(Path(self._csv_files[0]).name)
        else:
            self.csv_files_var.set(f"{n} files · {Path(self._csv_files[0]).name} …")

    def _csv_peek_headers(self) -> None:
        if not self._csv_files:
            return
        try:
            headers = peek_csv_headers(
                Path(self._csv_files[0]),
                delimiter=self.csv_delim_var.get() or ",",
                encoding=self.csv_encoding_var.get() or "utf-8",
                has_header=self.csv_has_header_var.get(),
            )
        except Exception as ex:
            messagebox.showerror("CSV", f"Could not read headers:\n{ex}")
            return
        self._csv_headers = headers
        vals = [""] + list(headers)
        self.csv_wide_combo["values"] = vals
        # Heuristic: COMMENT or similar for wide text
        if not self.csv_wide_col_var.get():
            for h in headers:
                if h.lower() in ("comment", "comments", "text", "body", "message"):
                    self.csv_wide_col_var.set(h)
                    break
        self._append_log(f"[OK] CSV headers ({len(headers)}): {', '.join(headers[:12])}" + ("…" if len(headers) > 12 else ""))

    def _csv_load_mapping(self) -> None:
        if self._busy:
            return
        if not self._csv_files:
            messagebox.showwarning("CSV", "Add at least one CSV file first.")
            return
        dest = self._role("dest")
        table = (self.csv_table_var.get() or "").strip()
        if not dest["server"] or not dest["db"] or not table:
            messagebox.showwarning("CSV", "Destination server, database, and table are required.")
            return
        self._csv_peek_headers()
        self._set_busy(True)
        self.status_var.set("Loading column map…")

        def worker():
            try:
                cols = list_dest_columns(dest, table)
                if not cols and self.create_missing_var.get():
                    # Preview map from CSV headers only (table will be created on import)
                    cols = [
                        {
                            "name": h,
                            "is_identity": False,
                            "type_name": "nvarchar",
                            "column_id": i + 1,
                        }
                        for i, h in enumerate(self._csv_headers)
                    ]
                mapping = auto_map_columns(self._csv_headers, cols)

                def done():
                    self._csv_rebuild_map_ui(cols, mapping)
                    self.status_var.set("Ready")
                    self._append_log(
                        f"[OK] Mapping: {len(mapping)} auto-matched / {len(cols)} dest column(s)."
                    )

                self.frame.after(0, done)
            except Exception as ex:
                self.frame.after(0, lambda: messagebox.showerror("CSV mapping", str(ex)))
                self._ui_log(f"[FAIL] {ex}")
                self.frame.after(0, lambda: self.status_var.set("Ready"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _csv_rebuild_map_ui(self, dest_cols: List[Dict[str, Any]], mapping: Dict[str, str]) -> None:
        for child in self.csv_map_inner.winfo_children():
            child.destroy()
        self._csv_map_vars = {}
        choices = [""] + list(self._csv_headers)
        hdr = ttk.Frame(self.csv_map_inner)
        hdr.pack(fill=tk.X)
        tk.Label(hdr, text="Destination column", width=28, anchor=tk.W).pack(side=tk.LEFT)
        tk.Label(hdr, text="← CSV header", width=28, anchor=tk.W).pack(side=tk.LEFT, padx=8)
        for col in dest_cols:
            name = col["name"]
            if col.get("is_identity"):
                row = ttk.Frame(self.csv_map_inner)
                row.pack(fill=tk.X, pady=1)
                tk.Label(row, text=f"{name} (identity)", width=28, anchor=tk.W, fg="gray").pack(
                    side=tk.LEFT
                )
                tk.Label(row, text="— skipped —", fg="gray").pack(side=tk.LEFT, padx=8)
                continue
            row = ttk.Frame(self.csv_map_inner)
            row.pack(fill=tk.X, pady=1)
            typ = col.get("type_name") or ""
            tk.Label(row, text=f"{name}  [{typ}]", width=28, anchor=tk.W).pack(side=tk.LEFT)
            var = tk.StringVar(value=mapping.get(name, ""))
            self._csv_map_vars[name] = var
            ttk.Combobox(row, textvariable=var, values=choices, width=28, state="readonly").pack(
                side=tk.LEFT, padx=8
            )

    def _csv_auto_map(self) -> None:
        if not self._csv_headers:
            self._csv_peek_headers()
        if not self._csv_map_vars:
            self._csv_load_mapping()
            return
        by_lower = {h.lower(): h for h in self._csv_headers}
        for dest_name, var in self._csv_map_vars.items():
            hit = by_lower.get(dest_name.lower())
            if hit:
                var.set(hit)

    def _csv_current_mapping(self) -> Dict[str, str]:
        return {k: (v.get() or "").strip() for k, v in self._csv_map_vars.items() if (v.get() or "").strip()}

    def _build_csv_cfg(self) -> Dict[str, Any]:
        dest = self._role("dest")
        return {
            "dest_server": dest["server"],
            "dest_db": dest["db"],
            "dest_auth": dest["auth"],
            "dest_user": dest["user"],
            "dest_password": dest["password"],
            "table": (self.csv_table_var.get() or "").strip(),
            "csv_files": list(self._csv_files),
            "delimiter": self.csv_delim_var.get() or ",",
            "encoding": self.csv_encoding_var.get() or "utf-8",
            "has_header": self.csv_has_header_var.get(),
            "mapping": self._csv_current_mapping(),
            "wide_text_csv_column": (self.csv_wide_col_var.get() or "").strip() or None,
            "create_missing": self.create_missing_var.get(),
            "truncate_dest": self.truncate_var.get(),
            "batch_size": int(self.batch_var.get() or "10000"),
            "verify_after_copy": self.verify_var.get(),
            "dry_run": self.dry_run_var.get(),
            "work_dir": self.work_dir_var.get().strip() or str(default_work_dir()),
        }

    def _toggle_advanced(self) -> None:
        if self._advanced_visible.get():
            self._adv_frame.pack_forget()
            self._advanced_visible.set(False)
            self._adv_btn.config(text="Show advanced ▸")
        else:
            self._adv_frame.pack(fill=tk.X, padx=10, pady=4)
            self._advanced_visible.set(True)
            self._adv_btn.config(text="Hide advanced ▾")

    def _test_staging(self) -> None:
        path = (self.work_dir_var.get() or "").strip()
        if not path:
            messagebox.showwarning("Staging", "Enter a local path or UNC network path.")
            return
        try:
            from src.utils.bcp_tools import disk_free_gb, verify_bcp_work_dir
        except ImportError:
            from azure_migration_tool.src.utils.bcp_tools import disk_free_gb, verify_bcp_work_dir
        ok, msg = verify_bcp_work_dir(Path(path))
        if not ok:
            messagebox.showerror("Staging", f"Not writable:\n{msg}")
            self._append_log(f"[FAIL] Staging: {msg}")
            return
        free = disk_free_gb(msg)
        free_txt = f"{free:.1f} GB free" if free >= 0 else "free space unknown"
        self.work_dir_var.set(msg)
        messagebox.showinfo("Staging", f"Writable.\n{msg}\n{free_txt}")
        self._append_log(f"[OK] Staging writable: {msg} ({free_txt})")

    # ---------- helpers ----------
    def _append_log(self, msg: str) -> None:
        self.log.insert(tk.END, msg + ("\n" if not msg.endswith("\n") else ""))
        self.log.see(tk.END)

    def _ui_log(self, msg: str) -> None:
        self.frame.after(0, lambda m=msg: self._append_log(m))

    def _set_busy(self, busy: bool) -> None:
        self._busy = busy
        state = tk.DISABLED if busy else tk.NORMAL
        self.btn_validate.config(state=state)
        self.btn_start.config(state=state)
        self.btn_stop.config(state=tk.NORMAL if busy else tk.DISABLED)

    def _role(self, which: str) -> Dict[str, Any]:
        if which == "src":
            port = "50000"
            schema = ""
            try:
                port = (self.src_widget.port_var.get() or "50000").strip()
                schema = (self.src_widget.schema_var.get() or "").strip()
            except Exception:
                pass
            if schema.lower() == "loading schemas...":
                schema = ""
            return {
                "server": self.src_server_var.get().strip(),
                "db": self.src_db_var.get().strip(),
                "auth": (self.src_auth_var.get() or "windows").strip().lower(),
                "user": self.src_user_var.get(),
                "password": self.src_password_var.get() or None,
                "db_type": (self.src_db_type_var.get() or "sqlserver").strip().lower(),
                "port": int(port or 50000),
                "schema": schema,
            }
        return {
            "server": self.dest_server_var.get().strip(),
            "db": self.dest_db_var.get().strip(),
            "auth": (self.dest_auth_var.get() or "windows").strip().lower(),
            "user": self.dest_user_var.get(),
            "password": self.dest_password_var.get() or None,
            "db_type": (self.dest_db_type_var.get() or "sqlserver").strip().lower(),
        }

    def _copy_src_to_dest(self) -> None:
        self.dest_server_var.set(self.src_server_var.get())
        self.dest_db_var.set(self.src_db_var.get())
        self.dest_auth_var.set(self.src_auth_var.get())
        self.dest_user_var.set(self.src_user_var.get())
        self.dest_password_var.set(self.src_password_var.get())
        # Dest for BCP load must stay SQL Server even when source is DB2
        self.dest_db_type_var.set("sqlserver")
        self._append_log(
            "[Note] Copied source host into destination — set dest DB to SQL Server target."
        )

    def _browse_work(self) -> None:
        # askdirectory does not browse UNC well on all Windows builds — still allow paste.
        folder = filedialog.askdirectory(title="BCP staging folder (local). For UNC, paste path in the box.")
        if folder:
            self.work_dir_var.set(folder)

    def _ensure_sql_dest(self) -> bool:
        if (self.dest_db_type_var.get() or "").strip().lower() == "db2":
            messagebox.showerror(
                "BCP",
                "Destination must be SQL Server / Azure SQL for BCP load.",
            )
            return False
        return True

    # ---------- table list ----------
    def _refresh_tables(self) -> None:
        if self._busy:
            return
        if self._is_csv_mode():
            return
        if not self._ensure_sql_dest():
            return
        src = self._role("src")
        dest = self._role("dest")
        if not src["server"] or not src["db"]:
            messagebox.showwarning("BCP", "Source server and database are required.")
            return
        if self._is_db2_source() and not (src.get("user") or "").strip():
            messagebox.showwarning("BCP", "DB2 user (and password) are required.")
            return

        self._set_busy(True)
        self.status_var.set("Loading tables…")
        kind = "DB2" if self._is_db2_source() else "SQL"
        self._append_log(f"Listing {kind} tables from {src['server']} / {src['db']}…")

        def worker():
            try:
                if self._is_db2_source():
                    tables = list_db2_tables(src, schema=src.get("schema") or None)
                else:
                    tables = list_tables(src)
                if dest["server"] and dest["db"]:
                    tables = enrich_with_dest(tables, dest)
                self.frame.after(0, lambda: self._populate_tables(tables))
            except Exception as ex:
                self.frame.after(0, lambda: messagebox.showerror("Refresh tables", str(ex)))
                self.frame.after(0, lambda: self._append_log(f"[FAIL] {ex}"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))
                self.frame.after(0, lambda: self.status_var.set("Ready"))

        threading.Thread(target=worker, daemon=True).start()

    def _populate_tables(self, tables: List[Any]) -> None:
        self._tables = tables
        self._checked = {t.fqn: False for t in tables}
        self._apply_filter()
        self._append_log(f"[OK] Loaded {len(tables)} table(s).")

    def _apply_filter(self) -> None:
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        q = (self.filter_var.get() or "").strip().lower()
        only_missing = self.filter_missing_var.get()
        only_mismatch = self.filter_mismatch_var.get()
        for t in self._tables:
            fqn = t.fqn
            if q and q not in fqn.lower():
                continue
            if only_missing and t.dest_exists:
                continue
            if only_mismatch and not (t.dest_exists and t.src_rows != t.dest_rows):
                continue
            mark = "☑" if self._checked.get(fqn) else "☐"
            self.tree.insert(
                "",
                tk.END,
                iid=fqn,
                values=(
                    mark,
                    t.schema,
                    t.name,
                    f"{t.src_rows:,}",
                    "Yes" if t.dest_exists else "No",
                    f"{t.dest_rows:,}" if t.dest_exists else "—",
                    "",
                ),
            )

    def _on_tree_click(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        col = self.tree.identify_column(event.x)
        row = self.tree.identify_row(event.y)
        if not row:
            return
        if col == "#1":
            self._checked[row] = not self._checked.get(row, False)
            vals = list(self.tree.item(row, "values"))
            vals[0] = "☑" if self._checked[row] else "☐"
            self.tree.item(row, values=vals)

    def _select_all(self) -> None:
        for iid in self.tree.get_children():
            self._checked[iid] = True
        self._apply_filter()

    def _clear_sel(self) -> None:
        for iid in self.tree.get_children():
            self._checked[iid] = False
        self._apply_filter()

    def _invert_sel(self) -> None:
        for iid in self.tree.get_children():
            self._checked[iid] = not self._checked.get(iid, False)
        self._apply_filter()

    def _selected_tables(self) -> List[str]:
        return [fqn for fqn, on in self._checked.items() if on]

    def _load_selection_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Load table list",
            filetypes=[("CSV / Excel-ish", "*.csv;*.txt"), ("All", "*.*")],
        )
        if not path:
            return
        names: List[str] = []
        with open(path, newline="", encoding="utf-8-sig") as f:
            sample = f.read(2048)
            f.seek(0)
            if "," in sample or "\t" in sample:
                dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
                reader = csv.reader(f, dialect)
                for row in reader:
                    if not row:
                        continue
                    cell = (row[0] or "").strip()
                    if cell and cell.lower() not in ("table", "name", "fqn"):
                        names.append(cell if "." in cell else f"dbo.{cell}")
            else:
                for line in f:
                    cell = line.strip()
                    if cell:
                        names.append(cell if "." in cell else f"dbo.{cell}")
        if not self._tables:
            messagebox.showinfo("Load selection", "Refresh tables first, then load the list again.")
            return
        known = {t.fqn.lower(): t.fqn for t in self._tables}
        hit = 0
        for n in names:
            key = n.lower()
            if key in known:
                self._checked[known[key]] = True
                hit += 1
        self._apply_filter()
        self._append_log(f"[OK] Selected {hit} table(s) from {Path(path).name}.")

    # ---------- validate / migrate ----------
    def _build_cfg(self, tables: List[str]) -> Dict[str, Any]:
        src = self._role("src")
        dest = self._role("dest")
        return {
            "src_server": src["server"],
            "src_db": src["db"],
            "src_auth": src["auth"],
            "src_user": src["user"],
            "src_password": src["password"],
            "src_port": src.get("port") or 50000,
            "src_schema": src.get("schema") or "",
            "src_db_type": src.get("db_type") or "sqlserver",
            "dest_server": dest["server"],
            "dest_db": dest["db"],
            "dest_auth": dest["auth"],
            "dest_user": dest["user"],
            "dest_password": dest["password"],
            "tables": tables,
            "exclude": self.exclude_var.get(),
            "create_missing": self.create_missing_var.get(),
            "truncate_dest": self.truncate_var.get(),
            "keep_identity": self.keep_identity_var.get(),
            "native_format": self.native_var.get(),
            "batch_size": int(self.batch_var.get() or "5000"),
            "parallel_tables": int(self.parallel_var.get() or "1"),
            "max_retries": int(self.retries_var.get() or "0"),
            "skip_if_equal": self.skip_equal_var.get(),
            "resume_enabled": self.resume_var.get(),
            "keep_bcp_files": self.keep_files_var.get(),
            "verify_after_copy": self.verify_var.get(),
            "dry_run": self.dry_run_var.get(),
            "continue_on_error": self.continue_on_error_var.get(),
            "work_dir": self.work_dir_var.get().strip() or str(default_work_dir()),
            "jdbc_max_rows": 2_000_000,
            "jdbc_fetch_size": 50_000,
            "table_row_hints": {
                t.fqn: int(getattr(t, "src_rows", 0) or 0) for t in self._tables
            },
        }

    def _validate(self) -> None:
        if self._busy:
            return
        if self._is_csv_mode():
            self._validate_csv()
            return
        if not self._ensure_sql_dest():
            return
        if self._is_db2_source():
            self._validate_db2()
            return
        tables = self._selected_tables()
        cfg = self._build_cfg(tables)
        if not cfg["src_server"] or not cfg["src_db"] or not cfg["dest_server"] or not cfg["dest_db"]:
            messagebox.showwarning("Validate", "Source and destination server/database are required.")
            return
        # preflight expects comma tables string optionally
        cfg["tables"] = ",".join(tables) if tables else ""

        self._set_busy(True)
        self.status_var.set("Validating…")
        self._append_log("--- BCP pre-flight ---")

        def worker():
            try:
                items = run_bcp_preflight(cfg, self._ui_log, install_bcp_if_missing=True)
                ok = preflight_passed(items)
                self._preflight_ok = ok

                def done():
                    self.status_var.set("Pre-flight PASSED" if ok else "Pre-flight FAILED")
                    if ok:
                        messagebox.showinfo("Pre-flight", "All blocking checks passed.")
                    else:
                        messagebox.showerror("Pre-flight", "Blocking checks failed — see log.")

                self.frame.after(0, done)
            except Exception as ex:
                self._preflight_ok = False
                self.frame.after(0, lambda: messagebox.showerror("Pre-flight", str(ex)))
                self._ui_log(f"[FAIL] {ex}")
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _validate_db2(self) -> None:
        tables = self._selected_tables()
        cfg = self._build_cfg(tables)
        if not cfg["src_server"] or not cfg["src_db"] or not cfg["dest_server"] or not cfg["dest_db"]:
            messagebox.showwarning("Validate", "Source and destination server/database are required.")
            return
        if not (cfg.get("src_user") or "").strip():
            messagebox.showwarning("Validate", "DB2 user and password are required.")
            return

        self._set_busy(True)
        self.status_var.set("Validating DB2 path…")
        self._append_log("--- DB2 -> SQL pre-flight (JDBC extract + BCP) ---")

        def worker():
            try:
                ok, _msgs = run_db2_preflight(cfg, self._ui_log)
                self._preflight_ok = ok

                def done():
                    self.status_var.set("DB2 pre-flight PASSED" if ok else "DB2 pre-flight FAILED")
                    if ok:
                        messagebox.showinfo(
                            "Pre-flight",
                            "DB2 path ready.\n"
                            "Large tables: CLP EXPORT if installed, else client extract + BCP.\n"
                            "Small tables: JDBC extract + BCP.",
                        )
                    else:
                        messagebox.showerror("Pre-flight", "Blocking checks failed — see log.")

                self.frame.after(0, done)
            except Exception as ex:
                self._preflight_ok = False
                self.frame.after(0, lambda: messagebox.showerror("Pre-flight", str(ex)))
                self._ui_log(f"[FAIL] {ex}")
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _validate_csv(self) -> None:
        cfg = self._build_csv_cfg()
        errs: List[str] = []
        if not cfg["dest_server"] or not cfg["dest_db"]:
            errs.append("Destination server and database are required.")
        table = cfg["table"]
        if not table or table.endswith(".") or "." not in table:
            errs.append(
                f"Destination table must be schema.name (e.g. dbo.COMMENT_DATA).\n"
                f"Current value: {table or '(empty)'}"
            )
        if not cfg["csv_files"]:
            errs.append("Add at least one CSV file.")
        if errs:
            messagebox.showwarning("Validate CSV", "\n".join(errs))
            return

        self._set_busy(True)
        self.status_var.set("Validating CSV…")
        self._append_log("--- CSV import pre-flight ---")

        def worker():
            ok = True
            try:
                bcp = find_bcp_exe()
                if not bcp:
                    ok = False
                    self._ui_log("[FAIL] bcp.exe not found")
                else:
                    self._ui_log(f"[OK] bcp.exe: {bcp}")

                headers = peek_csv_headers(
                    Path(cfg["csv_files"][0]),
                    delimiter=cfg["delimiter"],
                    encoding=cfg["encoding"],
                    has_header=cfg["has_header"],
                )
                if not headers:
                    ok = False
                    self._ui_log("[FAIL] Could not read CSV headers")
                else:
                    self._csv_headers = headers
                    self._ui_log(f"[OK] CSV columns: {len(headers)}")

                dest = self._role("dest")
                cols = list_dest_columns(dest, table)
                if not cols:
                    if cfg["create_missing"]:
                        self._ui_log(f"[OK] Table missing — will create from CSV headers on import")
                        cols = [
                            {"name": h, "is_identity": False, "type_name": "nvarchar", "column_id": i + 1}
                            for i, h in enumerate(headers)
                        ]
                    else:
                        ok = False
                        self._ui_log(f"[FAIL] Destination table not found: {table}")
                else:
                    self._ui_log(f"[OK] Destination columns: {len(cols)}")

                mapping = cfg["mapping"] or auto_map_columns(headers, cols)
                if not mapping:
                    ok = False
                    self._ui_log("[FAIL] No column mapping — use Load mapping / Auto-map")
                else:
                    self._ui_log(f"[OK] Mapped columns: {len(mapping)}")

                dry = dict(cfg)
                dry["mapping"] = mapping
                dry["dry_run"] = True
                report = run_csv_bcp_import(dry, self._ui_log)
                ok = ok and report.ok

                def done():
                    self._preflight_ok = ok
                    if mapping and not self._csv_map_vars:
                        self._csv_rebuild_map_ui(cols, mapping)
                    elif mapping:
                        for k, v in mapping.items():
                            if k in self._csv_map_vars:
                                self._csv_map_vars[k].set(v)
                    self.status_var.set("CSV validate PASSED" if ok else "CSV validate FAILED")
                    if ok:
                        messagebox.showinfo("Validate CSV", "Checks passed. Ready to import.")
                    else:
                        messagebox.showerror("Validate CSV", "Checks failed — see log.")

                self.frame.after(0, done)
            except Exception as ex:
                self._preflight_ok = False
                self._ui_log(f"[FAIL] {ex}")
                self.frame.after(0, lambda: messagebox.showerror("Validate CSV", str(ex)))
                self.frame.after(0, lambda: self.status_var.set("CSV validate FAILED"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _stop(self) -> None:
        self._cancel.set()
        self._append_log("[Note] Stop requested — will halt as soon as possible.")

    def _start(self) -> None:
        if self._busy:
            return
        if self._is_csv_mode():
            self._start_csv()
            return
        if not self._ensure_sql_dest():
            return
        tables = self._selected_tables()
        if not tables:
            messagebox.showwarning("Start BCP", "Select at least one table.")
            return
        if not self._preflight_ok:
            if not messagebox.askyesno(
                "Pre-flight",
                "Pre-flight has not passed yet. Run migration anyway?",
            ):
                return

        cfg = self._build_cfg(tables)
        use_db2 = self._is_db2_source()
        self._cancel.clear()
        self._set_busy(True)
        self.status_var.set("Migrating DB2…" if use_db2 else "Migrating…")
        self.progress["value"] = 0
        self.progress["maximum"] = len(tables)
        for iid in self.results.get_children():
            self.results.delete(iid)
        if use_db2:
            self._append_log(
                f"=== Start DB2 -> SQL (auto EXPORT/JDBC + BCP) for {len(tables)} table(s) ==="
            )
        else:
            self._append_log(f"=== Start BCP for {len(tables)} table(s) ===")

        def on_progress(done: int, total: int, table: str) -> None:
            def upd():
                self.progress["value"] = done
                self.progress_label.set(f"{done} / {total}: {table}")
                if table in self.tree.get_children():
                    vals = list(self.tree.item(table, "values"))
                    if len(vals) >= 7:
                        vals[6] = "running/done"
                        self.tree.item(table, values=vals)

            self.frame.after(0, upd)

        def worker():
            try:
                if use_db2:
                    report = run_db2_bcp_migration(
                        cfg,
                        self._ui_log,
                        cancel_event=self._cancel,
                        progress_callback=on_progress,
                    )
                else:
                    report = run_bcp_migration(
                        cfg,
                        self._ui_log,
                        cancel_event=self._cancel,
                        progress_callback=on_progress,
                    )
                self._last_report = report.to_dict()

                def finish():
                    for tr in report.tables:
                        self.results.insert(
                            "",
                            tk.END,
                            values=(
                                tr.table,
                                tr.status,
                                tr.src_rows,
                                tr.dest_rows,
                                f"{tr.duration_sec:.1f}",
                                tr.message or tr.error or "",
                            ),
                        )
                        if tr.table in self.tree.get_children():
                            vals = list(self.tree.item(tr.table, "values"))
                            if len(vals) >= 7:
                                vals[6] = tr.status
                                self.tree.item(tr.table, values=vals)
                    self.status_var.set("SUCCEEDED" if report.ok else "FAILED")
                    title = "DB2 -> SQL complete" if use_db2 else "BCP complete"
                    if report.ok:
                        messagebox.showinfo(
                            title,
                            f"Succeeded.\nWork dir:\n{report.work_dir}",
                        )
                    else:
                        fails = [
                            t
                            for t in report.tables
                            if getattr(t, "status", "") == "fail"
                        ]
                        detail_lines = []
                        for t in fails[:8]:
                            detail_lines.append(
                                f"{t.table}: {t.message or t.error or 'failed'}"
                            )
                        if len(fails) > 8:
                            detail_lines.append(f"... and {len(fails) - 8} more")
                        detail = "\n".join(detail_lines) if detail_lines else "See Results and Log."
                        messagebox.showwarning(
                            "Finished with errors",
                            f"{len(fails)} table(s) failed:\n\n{detail}",
                        )

                self.frame.after(0, finish)
            except Exception as ex:
                self._ui_log(f"[FAIL] {ex}")
                self.frame.after(0, lambda: messagebox.showerror("BCP", str(ex)))
                self.frame.after(0, lambda: self.status_var.set("FAILED"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _start_csv(self) -> None:
        cfg = self._build_csv_cfg()
        if not cfg["dest_server"] or not cfg["dest_db"]:
            messagebox.showwarning("CSV import", "Destination server and database are required.")
            return
        table = cfg["table"]
        if not table or table.endswith(".") or "." not in table:
            messagebox.showwarning(
                "CSV import",
                "Enter destination table as schema.name\n"
                f"(e.g. dbo.COMMENT_DATA).\n\nCurrent: {table or '(empty)'}",
            )
            return
        if not cfg["csv_files"]:
            messagebox.showwarning("CSV import", "Add at least one CSV file.")
            return
        if not cfg["mapping"]:
            # Prefer UI mapping; otherwise auto-map (or let engine map after create).
            try:
                if not self._csv_headers:
                    self._csv_peek_headers()
                cols = list_dest_columns(self._role("dest"), table)
                if cols:
                    cfg["mapping"] = auto_map_columns(self._csv_headers, cols)
            except Exception:
                cfg["mapping"] = cfg.get("mapping") or {}
        if not cfg["mapping"] and not self.create_missing_var.get():
            messagebox.showwarning("CSV import", "Load mapping (or enable Create missing tables).")
            return
        if not self._preflight_ok:
            if not messagebox.askyesno(
                "Validate",
                "CSV validate has not passed yet. Import anyway?",
            ):
                return

        self._cancel.clear()
        self._set_busy(True)
        self.status_var.set("Importing CSV…")
        self.progress["value"] = 0
        self.progress["maximum"] = 1
        for iid in self.results.get_children():
            self.results.delete(iid)
        self._append_log(f"=== Start CSV import → {table} ({len(cfg['csv_files'])} file(s)) ===")

        def worker():
            try:
                report = run_csv_bcp_import(cfg, self._ui_log, cancel_event=self._cancel)
                self._last_report = report.to_dict()

                def finish():
                    self.progress["value"] = 1
                    for tr in report.tables:
                        self.results.insert(
                            "",
                            tk.END,
                            values=(
                                tr.table,
                                tr.status,
                                tr.src_rows,
                                tr.dest_rows,
                                f"{tr.duration_sec:.1f}",
                                tr.message or tr.error or "",
                            ),
                        )
                    self.progress_label.set(report.tables[0].message if report.tables else "")
                    self.status_var.set("SUCCEEDED" if report.ok else "FAILED")
                    if report.ok:
                        messagebox.showinfo(
                            "CSV import complete",
                            f"Succeeded.\nWork dir:\n{report.work_dir}",
                        )
                    else:
                        messagebox.showwarning("CSV import finished with errors", "See Results and Log.")

                self.frame.after(0, finish)
            except Exception as ex:
                self._ui_log(f"[FAIL] {ex}")
                self.frame.after(0, lambda: messagebox.showerror("CSV import", str(ex)))
                self.frame.after(0, lambda: self.status_var.set("FAILED"))
            finally:
                self.frame.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _export_report(self) -> None:
        if not self._last_report:
            messagebox.showinfo("Export", "No report yet — run a migration first.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("CSV", "*.csv")],
            title="Export BCP report",
        )
        if not path:
            return
        p = Path(path)
        if p.suffix.lower() == ".csv":
            with p.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=["table", "status", "src_rows", "dest_rows", "duration_sec", "error", "message"],
                )
                w.writeheader()
                for row in self._last_report.get("tables") or []:
                    w.writerow(row)
        else:
            p.write_text(json.dumps(self._last_report, indent=2), encoding="utf-8")
        self._append_log(f"[OK] Report saved: {p}")
