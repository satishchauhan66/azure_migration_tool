# Author: Sa-tish Chauhan

"""
Reusable connection widget for server and database selection.
"""

import tkinter as tk
from tkinter import ttk, messagebox
import threading
from typing import Optional, Callable
import sys
from pathlib import Path

# Add parent directories to path
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from gui.utils.server_config import (
    load_saved_servers, save_server_config, delete_server_config,
    get_server_display_names, get_server_config
)
from gui.utils.database_utils import list_databases
try:
    from gui.utils import input_history as input_history_mod
except ImportError:
    try:
        from azure_migration_tool.gui.utils import input_history as input_history_mod
    except ImportError:
        input_history_mod = None
try:
    from gui.utils.tooltip import add_tooltip
except ImportError:
    def add_tooltip(widget, text, delay_ms=500):
        pass

# User-friendly display labels (internal value -> display text)
DB_TYPE_DISPLAY = {"sqlserver": "SQL Server / Azure SQL", "db2": "IBM DB2"}
DB_TYPE_TO_INTERNAL = {v: k for k, v in DB_TYPE_DISPLAY.items()}
AUTH_DISPLAY = {
    "entra_mfa": "Microsoft account (with MFA)",
    "entra_password": "Microsoft account (password)",
    "sql": "SQL Server login",
    "windows": "Windows login",
}
AUTH_TO_INTERNAL = {v: k for k, v in AUTH_DISPLAY.items()}


class ConnectionWidget:
    """Reusable widget for server and database connection configuration."""
    
    def __init__(
        self,
        parent,
        server_var: tk.StringVar,
        db_var: tk.StringVar,
        auth_var: tk.StringVar,
        user_var: tk.StringVar,
        password_var: tk.StringVar,
        label_text: str = "Database Connection",
        row_start: int = 0,
        db_type_var: Optional[tk.StringVar] = None,
        port_var: Optional[tk.StringVar] = None,
        schema_var: Optional[tk.StringVar] = None
    ):
        """
        Initialize connection widget.
        
        Args:
            parent: Parent widget
            server_var: StringVar for server/host
            db_var: StringVar for database
            auth_var: StringVar for authentication type
            user_var: StringVar for username
            password_var: StringVar for password
            label_text: Label text for the frame
            row_start: Starting row for grid layout
            db_type_var: Optional StringVar for database type (sqlserver/db2)
            port_var: Optional StringVar for port (for DB2)
            schema_var: Optional StringVar for schema (for DB2)
        """
        self.parent = parent
        self.server_var = server_var
        self.db_var = db_var
        self.auth_var = auth_var
        self.user_var = user_var
        self.password_var = password_var
        
        # Create database type var if not provided
        if db_type_var is None:
            self.db_type_var = tk.StringVar(value="sqlserver")
        else:
            self.db_type_var = db_type_var
        
        # Create port var if not provided
        if port_var is None:
            self.port_var = tk.StringVar(value="50000")
        else:
            self.port_var = port_var
        
        # Create schema var if not provided (for DB2)
        if schema_var is None:
            self.schema_var = tk.StringVar(value="")
        else:
            self.schema_var = schema_var
        
        # Use parent directly (parent should already be a frame)
        self.frame = parent
        
        # Display vars for user-friendly combo labels (internal values stay in db_type_var / auth_var)
        self._db_type_display_var = tk.StringVar(value=DB_TYPE_DISPLAY.get(self.db_type_var.get(), self.db_type_var.get()))
        self._auth_display_var = tk.StringVar(value=AUTH_DISPLAY.get(self.auth_var.get(), self.auth_var.get()))
        
        # Database type row (for selecting SQL Server vs DB2)
        tk.Label(self.frame, text="Database Type:").grid(row=row_start, column=0, sticky=tk.W, pady=5)
        self.db_type_combo = ttk.Combobox(self.frame, textvariable=self._db_type_display_var,
                                         values=list(DB_TYPE_DISPLAY.values()),
                                         state="readonly", width=37)
        self.db_type_combo.grid(row=row_start, column=1, pady=5, padx=5, sticky=tk.EW)
        self.db_type_combo.bind("<<ComboboxSelected>>", self._on_db_type_combo_selected)
        self.db_type_var.trace_add("write", self._sync_db_type_display)
        
        # Server/Host row
        self.server_label = tk.Label(self.frame, text="Server:")
        self.server_label.grid(row=row_start+1, column=0, sticky=tk.W, pady=5)
        server_frame = ttk.Frame(self.frame)
        server_frame.grid(row=row_start+1, column=1, pady=5, padx=5, sticky=tk.EW)
        
        self.server_combo = ttk.Combobox(server_frame, textvariable=self.server_var, width=35, state="normal")
        self.server_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.server_combo.bind("<<ComboboxSelected>>", self._on_server_selected)
        self.server_combo.bind("<FocusOut>", self._on_server_focus_out)
        self.server_combo.configure(postcommand=self._refresh_server_list)
        
        # Save/Delete buttons for server
        btn_frame = ttk.Frame(server_frame)
        btn_frame.pack(side=tk.LEFT, padx=(5, 0))
        save_btn = ttk.Button(btn_frame, text="Save", width=5, command=self._save_server)
        save_btn.pack(side=tk.LEFT, padx=1)
        add_tooltip(save_btn, "Save this connection for later")
        del_btn = ttk.Button(btn_frame, text="Del", width=4, command=self._delete_server)
        del_btn.pack(side=tk.LEFT, padx=1)
        add_tooltip(del_btn, "Remove saved connection")
        
        # Port row (for DB2)
        self.port_label = tk.Label(self.frame, text="Port:")
        self.port_label.grid(row=row_start+2, column=0, sticky=tk.W, pady=5)
        self.port_entry = ttk.Entry(self.frame, textvariable=self.port_var, width=40)
        self.port_entry.grid(row=row_start+2, column=1, pady=5, padx=5, sticky=tk.EW)
        self._update_port_visibility()
        
        # Database row
        tk.Label(self.frame, text="Database:").grid(row=row_start+3, column=0, sticky=tk.W, pady=5)
        db_frame = ttk.Frame(self.frame)
        db_frame.grid(row=row_start+3, column=1, pady=5, padx=5, sticky=tk.EW)
        
        # Create combobox - allow manual entry without triggering validation
        self.db_combo = ttk.Combobox(
            db_frame, 
            textvariable=self.db_var, 
            width=35, 
            state="normal",
            postcommand=self._on_database_dropdown_open  # Load when dropdown arrow is clicked
        )
        self.db_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        # Only bind to dropdown open, not to click/focus - let user type freely
        # Remove aggressive validation on click/focus
        
        # Refresh button for database - only way to trigger database listing
        ref_db_btn = ttk.Button(db_frame, text="Refresh", width=8, command=self._refresh_databases)
        ref_db_btn.pack(side=tk.LEFT, padx=(5, 0))
        add_tooltip(ref_db_btn, "Refresh list of databases from server")
        
        # Schema row (for DB2 - to select source schema like USERID)
        self.schema_label = tk.Label(self.frame, text="Schema (e.g. user ID):")
        self.schema_label.grid(row=row_start+4, column=0, sticky=tk.W, pady=5)
        schema_frame = ttk.Frame(self.frame)
        schema_frame.grid(row=row_start+4, column=1, pady=5, padx=5, sticky=tk.EW)
        
        self.schema_combo = ttk.Combobox(
            schema_frame, 
            textvariable=self.schema_var, 
            width=35, 
            state="normal",
            postcommand=self._on_schema_dropdown_open
        )
        self.schema_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ref_schema_btn = ttk.Button(schema_frame, text="Refresh", width=8, command=self._refresh_schemas)
        ref_schema_btn.pack(side=tk.LEFT, padx=(5, 0))
        add_tooltip(ref_schema_btn, "Refresh list of schemas")
        self._update_schema_visibility()
        
        # Auth row (only for SQL Server)
        self.auth_label = tk.Label(self.frame, text="Authentication:")
        self.auth_label.grid(row=row_start+5, column=0, sticky=tk.W, pady=5)
        self.auth_combo = ttk.Combobox(self.frame, textvariable=self._auth_display_var,
                                       values=list(AUTH_DISPLAY.values()),
                                       state="readonly", width=37)
        self.auth_combo.grid(row=row_start+5, column=1, pady=5, padx=5, sticky=tk.EW)
        self.auth_combo.bind("<<ComboboxSelected>>", self._on_auth_combo_selected)
        self.auth_var.trace_add("write", self._sync_auth_display)
        self._auth_row_start = row_start
        
        # User row (hidden when Windows auth is selected) — Combobox for recent usernames
        self.user_label = tk.Label(self.frame, text="User:")
        self.user_label.grid(row=row_start+6, column=0, sticky=tk.W, pady=5)
        self.user_combo = ttk.Combobox(
            self.frame,
            textvariable=self.user_var,
            width=40,
            state="normal",
            postcommand=self._on_user_combo_postcommand,
        )
        self.user_combo.grid(row=row_start+6, column=1, pady=5, padx=5, sticky=tk.EW)
        
        # Password row (hidden when Windows auth is selected)
        self.password_label = tk.Label(self.frame, text="Password:")
        self.password_label.grid(row=row_start+7, column=0, sticky=tk.W, pady=5)
        self.password_entry = ttk.Entry(self.frame, textvariable=self.password_var, width=40, show="*")
        self.password_entry.grid(row=row_start+7, column=1, pady=5, padx=5, sticky=tk.EW)
        
        # Hint when Windows auth: "Using your Windows account" (shown instead of User/Password)
        self.windows_auth_hint = tk.Label(self.frame, text="Using your Windows account", fg="gray")
        self.windows_auth_hint.grid(row=row_start+6, column=0, columnspan=2, sticky=tk.W, pady=5, padx=5)
        self.windows_auth_hint.grid_remove()
        
        self._update_auth_visibility()
        
        # Configure column weights
        self.frame.columnconfigure(1, weight=1)
        
        # Load saved servers + recent hosts / users
        self._refresh_server_list()
        self._on_user_combo_postcommand()
    
    def _on_db_type_combo_selected(self, event=None):
        """When user selects a database type from the combo, store internal value and update UI."""
        display = self._db_type_display_var.get()
        internal = DB_TYPE_TO_INTERNAL.get(display)
        if internal is not None:
            self.db_type_var.set(internal)
        self._on_db_type_changed()
    
    def _sync_db_type_display(self, *args):
        """Keep combo display in sync when db_type_var is set from outside."""
        internal = self.db_type_var.get()
        self._db_type_display_var.set(DB_TYPE_DISPLAY.get(internal, internal))
    
    def _on_auth_combo_selected(self, event=None):
        """When user selects auth from combo, store internal value."""
        display = self._auth_display_var.get()
        internal = AUTH_TO_INTERNAL.get(display)
        if internal is not None:
            self.auth_var.set(internal)
        self._update_auth_visibility()
    
    def _sync_auth_display(self, *args):
        """Keep auth combo display in sync when auth_var is set from outside."""
        internal = self.auth_var.get()
        self._auth_display_var.set(AUTH_DISPLAY.get(internal, internal))
        self._update_auth_visibility()
    
    def _on_db_type_changed(self, event=None):
        """Handle database type change (SQL Server vs DB2)."""
        db_type = self.db_type_var.get()
        self._update_port_visibility()
        self._update_schema_visibility()
        self._update_auth_visibility()
        # Update server label and clear values for fresh start
        if db_type == "db2":
            self.server_label.config(text="Host:")
            # Clear and set placeholder for DB2
            self.db_var.set("")
            self.db_combo['values'] = []
            # Set auth to basic (DB2 doesn't use Azure auth)
            self.auth_var.set("sql")
            self._auth_display_var.set(AUTH_DISPLAY.get("sql", "sql"))
        else:
            self.server_label.config(text="Server:")
            self.auth_var.set("entra_mfa")
            self._auth_display_var.set(AUTH_DISPLAY.get("entra_mfa", "entra_mfa"))
    
    def _update_port_visibility(self):
        """Show/hide port field based on database type."""
        db_type = self.db_type_var.get()
        if db_type == "db2":
            self.port_label.grid()
            self.port_entry.grid()
        else:
            self.port_label.grid_remove()
            self.port_entry.grid_remove()
    
    def _update_schema_visibility(self):
        """Show/hide schema field based on database type."""
        db_type = self.db_type_var.get()
        if db_type == "db2":
            self.schema_label.grid()
            self.schema_combo.master.grid()  # schema_frame
        else:
            self.schema_label.grid_remove()
            self.schema_combo.master.grid_remove()  # schema_frame
    
    def _on_schema_dropdown_open(self):
        """Called when schema dropdown is opened."""
        # Only refresh if we have a valid connection
        if self.db_type_var.get() == "db2" and self.db_var.get():
            self._refresh_schemas()
    
    def _refresh_schemas(self):
        """Load schemas from DB2 database."""
        db_type = self.db_type_var.get()
        if db_type != "db2":
            return
        
        server = (self.server_var.get() or "").strip()
        port = (self.port_var.get() or "50000").strip()
        database = (self.db_var.get() or "").strip()
        user = (self.user_var.get() or "").strip()
        password = (self.password_var.get() or "").strip()
        
        if not all([server, database, user, password]):
            messagebox.showwarning("Warning", "Please fill in Host, Database, User, and Password first.")
            return
        
        # Store current value
        current_schema = self.schema_var.get()
        self.schema_var.set("Loading schemas...")
        
        def load_schemas():
            try:
                from src.utils.database import connect_to_db2_jdbc
                
                conn = connect_to_db2_jdbc(
                    host=server,
                    port=int(port),
                    database=database,
                    user=user,
                    password=password,
                    timeout=30
                )
                
                cursor = conn.cursor()
                # Query to list all schemas in DB2
                cursor.execute("""
                    SELECT DISTINCT TABSCHEMA 
                    FROM SYSCAT.TABLES 
                    WHERE TABSCHEMA NOT LIKE 'SYS%'
                    ORDER BY TABSCHEMA
                """)
                
                schemas = [row[0].strip() for row in cursor.fetchall()]
                cursor.close()
                conn.close()
                
                def update_ui():
                    self._record_input_history_success()
                    self.schema_combo['values'] = schemas
                    # Restore previous value or set to user's ID
                    if current_schema and current_schema in schemas:
                        self.schema_var.set(current_schema)
                    elif user.upper() in schemas:
                        self.schema_var.set(user.upper())
                    elif schemas:
                        self.schema_var.set(schemas[0])
                    else:
                        self.schema_var.set("")
                
                self.frame.after(0, update_ui)
                
            except Exception as e:
                def show_error():
                    self.schema_var.set(current_schema if current_schema else "")
                    messagebox.showerror("Error", f"Failed to load schemas: {str(e)}")
                self.frame.after(0, show_error)
        
        threading.Thread(target=load_schemas, daemon=True).start()
    
    def _update_auth_visibility(self):
        """Show/hide auth and user/password fields based on database type and auth."""
        # Check if widgets exist (they might not be created yet during initialization)
        if not hasattr(self, 'user_label') or not hasattr(self, 'windows_auth_hint'):
            return
        
        db_type = self.db_type_var.get()
        auth = (self.auth_var.get() or "").strip()
        if db_type == "db2":
            # DB2 doesn't use auth types like SQL Server; always show User/Password
            self.auth_label.grid_remove()
            self.auth_combo.grid_remove()
            self.user_label.grid()
            self.user_combo.grid()
            self.password_label.grid()
            self.password_entry.grid()
            self.windows_auth_hint.grid_remove()
        else:
            self.auth_label.grid()
            self.auth_combo.grid()
            if auth == "windows":
                # Windows auth uses current Windows identity; no username/password
                self.user_label.grid_remove()
                self.user_combo.grid_remove()
                self.password_label.grid_remove()
                self.password_entry.grid_remove()
                self.windows_auth_hint.grid()
            elif auth == "entra_mfa":
                # Interactive / MFA — user principal only, no password field
                self.windows_auth_hint.grid_remove()
                self.user_label.grid()
                self.user_combo.grid()
                self.password_label.grid_remove()
                self.password_entry.grid_remove()
            else:
                self.windows_auth_hint.grid_remove()
                self.user_label.grid()
                self.user_combo.grid()
                self.password_label.grid()
                self.password_entry.grid()
    
    def _refresh_server_list(self):
        """Refresh server dropdown: saved profiles + app-wide recent hostnames."""
        saved_servers = get_server_display_names()
        current_value = (self.server_var.get() or "").strip()
        hist: list = []
        if input_history_mod:
            try:
                hist = input_history_mod.get_servers()
            except Exception:
                hist = []
        seen = set()
        merged = []
        for x in list(saved_servers) + list(hist):
            t = (x or "").strip()
            if not t:
                continue
            tl = t.lower()
            if tl in seen:
                continue
            seen.add(tl)
            merged.append(t)
        self.server_combo["values"] = tuple(merged) if merged else ()
        if current_value and current_value in merged:
            self.server_var.set(current_value)

    def _on_user_combo_postcommand(self):
        if input_history_mod:
            try:
                self.user_combo["values"] = tuple(input_history_mod.get_usernames())
            except Exception:
                pass

    def _record_input_history_success(self):
        """After a successful connection, remember server and user (no passwords)."""
        if not input_history_mod:
            return
        try:
            s = (self.server_var.get() or "").strip()
            if s:
                input_history_mod.record_server(s)
            auth = (self.auth_var.get() or "").strip()
            if auth != "windows":
                u = (self.user_var.get() or "").strip()
                if u:
                    input_history_mod.record_username(u)
        except Exception:
            pass
    
    def _on_server_selected(self, event=None):
        """Handle server selection from dropdown."""
        display_name = self.server_combo.get()
        if not display_name:
            return
        
        config = get_server_config(display_name)
        if config:
            # Auto-fill connection details - ensure all values are set correctly
            server = config.get('server', '').strip()
            auth = config.get('auth', 'entra_mfa').strip()
            user = config.get('user', '').strip()
            password = config.get('password', '').strip()
            db_type = config.get('db_type', 'sqlserver').strip()
            port = config.get('port', '50000').strip()
            database = config.get('database', '').strip()
            schema = config.get('schema', '').strip()
            
            # Validate server name
            if not server:
                messagebox.showerror("Error", "Saved server configuration has empty server name!")
                return
            
            # Set database type first (this triggers UI updates)
            self.db_type_var.set(db_type)
            self._on_db_type_changed()  # Update UI visibility
            
            # Set all values
            self.server_var.set(server)
            self.auth_var.set(auth)
            self.user_var.set(user)
            self.password_var.set(password)
            
            # Set DB2-specific fields
            if db_type == "db2":
                self.port_var.set(port if port else "50000")
                self.db_var.set(database)
                self.schema_var.set(schema)
                # Add database to dropdown values if it exists
                if database:
                    self.db_combo['values'] = [database]
                # Add schema to dropdown values if it exists
                if schema:
                    self.schema_combo['values'] = [schema]
            else:
                # Clear database list for SQL Server (will be populated when clicked)
                self.db_combo['values'] = []
                self.db_var.set("")
            self._record_input_history_success()
            print(f"DEBUG: Loaded saved server config - Server: '{server}', Auth: '{auth}', User: '{user}', DB Type: '{db_type}', Database: '{database}', Schema: '{schema}'")
    
    def _on_server_focus_out(self, event=None):
        """Handle server field focus out - allow manual entry."""
        # Allow user to type in server name manually
        pass
    
    def _on_database_dropdown_open(self):
        """Called when dropdown arrow is clicked - load databases if needed."""
        # Only refresh if we have a valid connection and no values yet
        # Don't validate - just try to load if we can
        db_type = self.db_type_var.get()
        server = (self.server_var.get() or "").strip()
        
        # For DB2, show helpful message - databases need to be entered manually
        if db_type == "db2":
            messagebox.showinfo(
                "DB2 Database Entry",
                "For DB2, please enter the database name manually.\n\n"
                "DB2 requires you to know the database name beforehand.\n"
                "Common database names:\n"
                "  - Your assigned database name\n\n"
                "After entering the database name, click the refresh button to test the connection."
            )
            return
        
        # For SQL Server, only try if we have server and it's not already loading
        if server and not self.db_combo['values']:
            # Check if we have minimum required fields for SQL Server
            auth = (self.auth_var.get() or "entra_mfa").strip()
            user = (self.user_var.get() or "").strip()
            
            # Only auto-load if we have server and (windows auth or user for other auths)
            if auth == "windows" or (auth in ["entra_mfa", "entra_password", "sql"] and user):
                # Don't show errors - just try silently
                try:
                    self._refresh_databases()
                except:
                    pass  # Fail silently - user can use refresh button
    
    def _refresh_databases(self):
        """Connect to server and populate database list."""
        # Get values and ensure they're not empty
        db_type = self.db_type_var.get()
        server = (self.server_var.get() or "").strip()
        auth = (self.auth_var.get() or "entra_mfa").strip()
        user = (self.user_var.get() or "").strip()
        password = (self.password_var.get() or "").strip()
        port = (self.port_var.get() or "50000").strip()
        
        print(f"DEBUG: Refreshing databases - Type: {db_type}, Server: '{server}', Auth: '{auth}', User: '{user}'")
        
        if not server:
            messagebox.showwarning("Warning", "Please enter a server/host name first.")
            return
        
        # For DB2, validate port and credentials
        if db_type == "db2":
            try:
                port_int = int(port)
                if port_int <= 0:
                    raise ValueError("Port must be positive")
            except ValueError:
                messagebox.showerror("Error", f"Invalid port number: '{port}'. Please enter a valid port (e.g., 50000).")
                return
            
            # DB2 requires user and password for connection test
            if not user:
                messagebox.showwarning("Warning", "User is required for DB2 connection.")
                return
            if not password:
                messagebox.showwarning("Warning", "Password is required for DB2 connection.")
                return
        else:
            # SQL Server validation
            # Validate server name doesn't look like an email domain
            if "@" in server or server.endswith(".com") and "." not in server.split(".")[0]:
                messagebox.showerror("Error", f"Invalid server name: '{server}'. Please enter a valid SQL Server address.")
                return
            
            # Validate auth requirements
            if auth in ["entra_mfa", "entra_password", "sql"] and not user:
                messagebox.showwarning("Warning", f"User is required for {auth} authentication.")
                return
            
            if auth in ["entra_password", "sql"] and not password:
                messagebox.showwarning("Warning", f"Password is required for {auth} authentication.")
                return
        
        # Store current value before showing loading
        current_db = self.db_var.get()
        if not current_db or current_db == "Loading databases...":
            current_db = ""
        
        # Show loading message
        loading_text = "Loading databases..."
        self.db_var.set(loading_text)
        
        def load_databases():
            try:
                if db_type == "db2":
                    # For DB2, test connection using JDBC (more reliable than ODBC)
                    print(f"DEBUG: DB2 connection test - Host: {server}, Port: {port}, User: {user}")
                    import traceback
                    
                    # Get the database name - user must enter it manually
                    test_db = current_db if current_db and current_db != "Loading databases..." else ""
                    
                    if not test_db:
                        # No database entered - show message to enter it
                        def show_db_prompt():
                            self.db_var.set("")
                            messagebox.showinfo(
                                "Enter Database Name",
                                "Please enter the DB2 database name in the Database field.\n\n"
                                "DB2 requires you to know the database name beforehand.\n\n"
                                "After entering the database name, click refresh again to test the connection."
                            )
                        self.frame.after(0, show_db_prompt)
                        return
                    
                    conn = None
                    error_msg = None
                    connection_method = None
                    
                    # Try JDBC connection first
                    try:
                        print("DEBUG: Attempting DB2 JDBC connection...")
                        from src.utils.database import connect_to_db2_jdbc
                        conn = connect_to_db2_jdbc(
                            host=server,
                            port=int(port),
                            database=test_db,
                            user=user,
                            password=password,
                            timeout=30
                        )
                        connection_method = "JDBC"
                        print("DEBUG: DB2 JDBC connection successful!")
                    except ImportError as ie:
                        print(f"DEBUG: JDBC not available: {ie}")
                        error_msg = f"JDBC not available: {ie}"
                    except Exception as e:
                        print(f"DEBUG: JDBC connection failed: {e}")
                        error_msg = str(e)
                    
                    # If JDBC failed, try ODBC
                    if not conn:
                        try:
                            print("DEBUG: Attempting DB2 ODBC connection...")
                            from src.utils.database import connect_to_db2, check_db2_driver
                            is_available, driver_name = check_db2_driver()
                            if is_available:
                                print(f"DEBUG: Using DB2 ODBC driver: {driver_name}")
                                conn = connect_to_db2(
                                    host=server,
                                    port=int(port),
                                    database=test_db,
                                    user=user,
                                    password=password,
                                    timeout=30
                                )
                                connection_method = "ODBC"
                                print("DEBUG: DB2 ODBC connection successful!")
                            else:
                                print("DEBUG: No DB2 ODBC driver found")
                                if error_msg:
                                    error_msg += "\n\nODBC driver also not available."
                        except Exception as e2:
                            print(f"DEBUG: ODBC connection failed: {e2}")
                            if error_msg:
                                error_msg += f"\n\nODBC also failed: {e2}"
                            else:
                                error_msg = str(e2)
                    
                    # Handle result
                    if conn:
                        conn.close()
                        # Connection successful - keep the database name
                        def show_success():
                            self._record_input_history_success()
                            self.db_var.set(test_db)
                            self.db_combo['values'] = [test_db]  # Add to dropdown for convenience
                            messagebox.showinfo(
                                "Connection Successful",
                                f"DB2 connection successful ({connection_method})!\n\n"
                                f"Database: {test_db}\n"
                                f"Host: {server}:{port}\n\n"
                                "You can now select a schema and proceed."
                            )
                        self.frame.after(0, show_success)
                    else:
                        # Connection failed
                        full_error = f"DB2 connection failed:\n\n{error_msg}\n\nPlease check the database name, host, port, and credentials."
                        self.frame.after(0, lambda: self._show_database_error(full_error, current_db))
                else:
                    # SQL Server
                    print(f"DEBUG: Attempting to list databases from {server}...")
                    databases = list_databases(
                        server=server,
                        auth=auth,
                        user=user,
                        password=password if password else None,
                        timeout=10
                    )
                    
                    print(f"DEBUG: Found {len(databases)} databases: {databases}")
                    
                    # Update UI in main thread
                    self.frame.after(0, lambda: self._update_database_list(databases, current_db))
                
            except Exception as e:
                error_msg = str(e)
                import traceback
                print(f"ERROR listing databases: {error_msg}")
                print(traceback.format_exc())
                self.frame.after(0, lambda: self._show_database_error(error_msg, current_db))
        
        # Run in background thread
        threading.Thread(target=load_databases, daemon=True).start()
    
    def _update_database_list(self, databases, previous_value=None):
        """Update database dropdown with list of databases."""
        try:
            self._record_input_history_success()
            print(f"DEBUG: Updating database list with {len(databases)} databases")
            
            # Update the values list
            if databases:
                # IMPORTANT: Set values using configure method
                # Clear any existing values first to force refresh
                self.db_combo.configure(values=[])
                self.frame.update_idletasks()
                
                # Now set the new values
                self.db_combo.configure(values=databases)
                print(f"DEBUG: Database combobox values set: {self.db_combo['values']}")
                
                # Ensure state is normal (editable) so user can type or select
                self.db_combo.config(state="normal")
                
                # Clear loading message
                current_val = self.db_var.get()
                if current_val == "Loading databases...":
                    # Restore previous value if it exists and is valid
                    if previous_value and previous_value and previous_value in databases:
                        self.db_var.set(previous_value)
                        print(f"DEBUG: Restored previous value: {previous_value}")
                    else:
                        # Clear to empty string - this allows the dropdown to be visible
                        self.db_var.set("")
                        print("DEBUG: Cleared database value - dropdown should now be visible")
                
                # Force UI update
                self.db_combo.update_idletasks()
                self.frame.update_idletasks()
                
                # Verify the values are actually set
                actual_values = self.db_combo['values']
                print(f"DEBUG: Verified combobox has {len(actual_values)} values after update")
            else:
                self.db_combo.configure(values=[])
                self.db_combo.config(state="normal")
                print("DEBUG: No databases found, cleared values")
                # Clear loading message
                if self.db_var.get() == "Loading databases...":
                    if previous_value:
                        self.db_var.set(previous_value)
                    else:
                        self.db_var.set("")
                messagebox.showinfo("Info", "No databases found on this server.\n\nYou can type the database name manually.")
            
            # Force update and refresh the combobox display
            self.db_combo.update_idletasks()
            # Try to trigger a refresh of the combobox
            try:
                # Force the combobox to refresh its display
                self.frame.update_idletasks()
            except:
                pass
            
            print(f"DEBUG: Database combobox state: {self.db_combo.cget('state')}, values count: {len(self.db_combo['values'])}, current value: '{self.db_var.get()}'")
            
            # Test: Try to see if we can programmatically show the dropdown
            # (This is just for debugging - we won't keep this)
            if databases:
                print(f"DEBUG: To select a database, click the dropdown arrow or start typing. Available: {', '.join(databases[:3])}{'...' if len(databases) > 3 else ''}")
                
        except Exception as e:
            print(f"ERROR updating database list: {e}")
            import traceback
            traceback.print_exc()
            # Make sure it's enabled even on error
            self.db_combo.config(state="normal")
            if self.db_var.get() == "Loading databases...":
                self.db_var.set(previous_value or "")
    
    def _show_database_error(self, error_msg, previous_value=None):
        """Show error message when database listing fails."""
        try:
            # Set state back to normal so user can still type manually
            self.db_combo.config(state="normal")
            # Clear loading message and restore previous value if any
            if self.db_var.get() == "Loading databases...":
                self.db_var.set(previous_value or "")
            # Keep existing values if any
            if not self.db_combo['values']:
                self.db_combo['values'] = []
            messagebox.showerror("Connection Error", 
                                f"Failed to connect to server:\n\n{error_msg}\n\n"
                                "You can still type the database name manually.")
        except Exception as e:
            print(f"Error showing database error: {e}")
            # Make sure it's enabled
            self.db_combo.config(state="normal")
    
    def _save_server(self):
        """Save current server configuration."""
        server = self.server_var.get()
        auth = self.auth_var.get()
        user = self.user_var.get()
        password = self.password_var.get()
        db_type = self.db_type_var.get()
        port = self.port_var.get()
        database = self.db_var.get()
        schema = self.schema_var.get()
        
        if not server:
            messagebox.showwarning("Warning", "Please enter a server name.")
            return
        
        # Validation for DB2
        if db_type == "db2":
            if not user:
                messagebox.showwarning("Warning", "User is required for DB2 connection.")
                return
            if not password:
                messagebox.showwarning("Warning", "Password is required for DB2 connection.")
                return
        else:
            # SQL Server validation
            if auth in ["entra_mfa", "entra_password", "sql"] and not user:
                messagebox.showwarning("Warning", f"User is required for {auth} authentication.")
                return
            
            if auth in ["entra_password", "sql"] and not password:
                messagebox.showwarning("Warning", f"Password is required for {auth} authentication.")
                return
        
        # Ask for display name with more descriptive default
        from tkinter import simpledialog
        if db_type == "db2":
            default_name = f"{server}:{port} - {database}" if database else f"{server}:{port} (DB2)"
        else:
            default_name = f"{server} ({auth})"
        
        display_name = simpledialog.askstring(
            "Save Server",
            "Enter a name for this server configuration:",
            initialvalue=default_name
        )
        
        if display_name:
            success = save_server_config(
                server=server,
                auth=auth,
                user=user,
                password=password,
                display_name=display_name,
                db_type=db_type,
                port=port,
                database=database,
                schema=schema
            )
            
            if success:
                self._record_input_history_success()
                messagebox.showinfo("Success", f"Server configuration saved!\n\nDatabase: {database}\nSchema: {schema}" if db_type == "db2" else "Server configuration saved!")
                self._refresh_server_list()
            else:
                messagebox.showerror("Error", "Failed to save server configuration.")
    
    def _delete_server(self):
        """Delete selected server configuration."""
        # Check if current server matches any saved server
        current_server = self.server_var.get()
        current_auth = self.auth_var.get()
        current_user = self.user_var.get()
        
        if not current_server:
            messagebox.showwarning("Warning", "Please select a saved server to delete.")
            return
        
        # Find matching saved server
        saved_servers = load_saved_servers()
        matching_config = None
        for srv in saved_servers:
            if (srv.get('server') == current_server and 
                srv.get('auth') == current_auth and 
                srv.get('user') == current_user):
                matching_config = srv
                break
        
        if not matching_config:
            messagebox.showwarning("Warning", "Current server is not a saved configuration.")
            return
        
        # Confirm deletion
        display_name = matching_config.get('display_name', f"{matching_config['server']} ({matching_config['auth']})")
        confirm = messagebox.askyesno(
            "Delete Server",
            f"Delete server configuration '{display_name}'?\n\n"
            f"Server: {matching_config['server']}\n"
            f"Auth: {matching_config['auth']}\n"
            f"User: {matching_config['user']}"
        )
        
        if confirm:
            success = delete_server_config(
                server=matching_config['server'],
                auth=matching_config['auth'],
                user=matching_config['user']
            )
            
            if success:
                messagebox.showinfo("Success", "Server configuration deleted!")
                # Clear fields
                self.server_var.set("")
                self.auth_var.set("entra_mfa")
                self.user_var.set("")
                self.password_var.set("")
                self.db_var.set("")
                self._refresh_server_list()
            else:
                messagebox.showerror("Error", "Failed to delete server configuration.")
