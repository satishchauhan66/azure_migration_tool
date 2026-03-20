# Author: S@tish Chauhan

"""
Schema comparison tree widget with checkboxes for object selection.
"""

import tkinter as tk
from tkinter import ttk
from typing import Dict, List, Tuple, Optional, Callable, Any


class SchemaTree(ttk.Frame):
    """Treeview widget with checkboxes for schema object selection."""
    
    def __init__(self, parent, title: str = "Schema Objects", **kwargs):
        super().__init__(parent, **kwargs)
        self.title = title
        self.selected_objects = set()  # Set of (object_type, schema, name) tuples
        self.on_selection_change = None  # Callback when selection changes
        
        # Create title label
        title_label = tk.Label(self, text=title, font=("Arial", 10, "bold"))
        title_label.pack(anchor=tk.W, pady=(0, 5))
        
        # Create treeview with scrollbar
        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        # Treeview columns
        columns = ("status", "schema", "name")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="tree headings", height=15)
        self.tree.heading("#0", text="Type")
        self.tree.heading("status", text="Status")
        self.tree.heading("schema", text="Schema")
        self.tree.heading("name", text="Name")
        
        # Configure column widths
        self.tree.column("#0", width=120)
        self.tree.column("status", width=80)
        self.tree.column("schema", width=100)
        self.tree.column("name", width=200)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Bind checkbox click
        self.tree.bind("<Button-1>", self._on_click)
        
        # Store category nodes
        self.category_nodes = {}
        
        # Filter and search frame
        filter_frame = ttk.Frame(self)
        filter_frame.pack(fill=tk.X, pady=(5, 0))
        
        tk.Label(filter_frame, text="Filter:").pack(side=tk.LEFT, padx=5)
        self.filter_var = tk.StringVar()
        self.filter_var.trace("w", self._on_filter_change)
        filter_entry = ttk.Entry(filter_frame, textvariable=self.filter_var, width=20)
        filter_entry.pack(side=tk.LEFT, padx=5)
        
        # Selection summary
        self.summary_label = tk.Label(self, text="Selected: 0 objects", font=("Arial", 9), fg="gray")
        self.summary_label.pack(anchor=tk.W, pady=(5, 0))
    
    def load_comparison(self, comparison: Dict[str, Any]):
        """
        Load comparison results into the tree.
        
        Args:
            comparison: Dictionary from schema_comparison.compare_schemas()
        """
        # Clear existing items
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.category_nodes = {}
        self.selected_objects.clear()
        
        # Add objects by category
        for obj_type, obj_data in comparison.items():
            if isinstance(obj_data, dict):
                # Check if it has 'object_type' (SQL-based) or 'missing'/'extra' (list-based)
                if 'object_type' in obj_data or 'missing' in obj_data or 'extra' in obj_data or 'status' in obj_data:
                    self._add_category(obj_type, obj_data)
        
        self._update_summary()
    
    def _add_category(self, obj_type: str, obj_data: Dict[str, Any]):
        """Add a category node with its objects."""
        category_name = obj_type.replace('_', ' ').title()
        category_node = self.tree.insert("", tk.END, text=category_name, 
                                        tags=("category",))
        self.category_nodes[obj_type] = category_node
        
        # Check if this is a SQL-based comparison (has 'status' key) or list-based (has 'missing'/'extra' keys)
        is_sql_based = 'status' in obj_data
        
        if is_sql_based:
            # SQL-based comparison (foreign_keys, indexes, constraints, etc.)
            status = obj_data.get('status', 'unknown')
            if status == 'missing':
                self.tree.set(category_node, "status", "Missing in destination")
                self.tree.item(category_node, tags=("category", "has_missing"))
                # Add checkbox for the entire category
                item = self.tree.insert(category_node, tk.END,
                                       text="[ ]", values=("Missing", "", obj_type),
                                       tags=("missing", "checkbox"))
            elif status == 'extra':
                self.tree.set(category_node, "status", "Extra in destination")
                self.tree.item(category_node, tags=("category", "has_extra"))
                item = self.tree.insert(category_node, tk.END,
                                       text="[ ]", values=("Extra", "", obj_type),
                                       tags=("extra", "checkbox"))
            elif status == 'different':
                self.tree.set(category_node, "status", "Different")
                self.tree.item(category_node, tags=("category", "has_different"))
                item = self.tree.insert(category_node, tk.END,
                                       text="[ ]", values=("Different", "", obj_type),
                                       tags=("different", "checkbox"))
            elif status == 'match':
                self.tree.set(category_node, "status", "Match")
                self.tree.item(category_node, tags=("category", "all_match"))
            else:
                self.tree.set(category_node, "status", "Unknown")
        else:
            # List-based comparison (tables, views, procedures, etc.)
            # Color code category based on status
            if 'missing' in obj_data and len(obj_data.get('missing', [])) > 0:
                self.tree.set(category_node, "status", f"{len(obj_data['missing'])} missing")
                self.tree.item(category_node, tags=("category", "has_missing"))
            elif 'extra' in obj_data and len(obj_data.get('extra', [])) > 0:
                self.tree.set(category_node, "status", f"{len(obj_data['extra'])} extra")
                self.tree.item(category_node, tags=("category", "has_extra"))
            elif 'matching' in obj_data and len(obj_data.get('matching', [])) > 0:
                self.tree.set(category_node, "status", f"{len(obj_data['matching'])} matching")
                self.tree.item(category_node, tags=("category", "all_match"))
            else:
                self.tree.set(category_node, "status", "0 objects")
            
            # Add missing objects (red)
            if 'missing' in obj_data:
                for obj in obj_data['missing']:
                    if isinstance(obj, tuple):
                        if len(obj) == 2:  # (schema, name)
                            schema, name = obj
                            obj_key = (obj_type, schema, name)
                            item = self.tree.insert(category_node, tk.END, 
                                                   text="[ ]", values=("Missing", schema, name),
                                                   tags=("missing", "checkbox"))
                            self.tree.item(item, tags=("missing", "checkbox"))
                        elif len(obj) == 3:  # (schema, name, table) for triggers
                            schema, name, table = obj
                            obj_key = (obj_type, schema, name)
                            item = self.tree.insert(category_node, tk.END,
                                                   text="[ ]", values=("Missing", schema, f"{name} (on {table})"),
                                                   tags=("missing", "checkbox"))
                            self.tree.item(item, tags=("missing", "checkbox"))
            
            # Add extra objects (blue)
            if 'extra' in obj_data:
                for obj in obj_data['extra']:
                    if isinstance(obj, tuple):
                        if len(obj) == 2:
                            schema, name = obj
                            obj_key = (obj_type, schema, name)
                            item = self.tree.insert(category_node, tk.END,
                                                   text="[ ]", values=("Extra", schema, name),
                                                   tags=("extra", "checkbox"))
                        elif len(obj) == 3:
                            schema, name, table = obj
                            obj_key = (obj_type, schema, name)
                            item = self.tree.insert(category_node, tk.END,
                                                   text="[ ]", values=("Extra", schema, f"{name} (on {table})"),
                                                   tags=("extra", "checkbox"))
        
        # Configure tag colors (only once, at the end)
        self.tree.tag_configure("category", font=("Arial", 9, "bold"))
        self.tree.tag_configure("missing", foreground="red")
        self.tree.tag_configure("extra", foreground="blue")
        self.tree.tag_configure("different", foreground="orange")
        self.tree.tag_configure("has_missing", background="#ffeeee")
        self.tree.tag_configure("has_extra", background="#eeeeff")
        self.tree.tag_configure("has_different", background="#fff4e6")
        self.tree.tag_configure("all_match", background="#eeffee")
    
    def _on_click(self, event):
        """Handle checkbox click."""
        region = self.tree.identify_region(event.x, event.y)
        if region == "cell":
            item = self.tree.identify_row(event.y)
            if item:
                tags = self.tree.item(item, "tags")
                if "checkbox" in tags:
                    # Toggle checkbox
                    current_text = self.tree.item(item, "text")
                    if current_text == "[ ]":
                        self.tree.item(item, text="[x]")
                        # Get object info
                        values = self.tree.item(item, "values")
                        if values:
                            schema = values[1] if len(values) > 1 else ""
                            name = values[2] if len(values) > 2 else ""
                            # For SQL-based objects (constraints, indexes), name might be empty and obj_type is in name
                            # For triggers, name might be in format "name (on table)", extract just the name
                            if " (on " in name:
                                name = name.split(" (on ")[0]
                            # Find parent category
                            parent = self.tree.parent(item)
                            obj_type = None
                            for cat_type, cat_node in self.category_nodes.items():
                                if cat_node == parent:
                                    obj_type = cat_type
                                    break
                            if obj_type:
                                # For SQL-based objects (constraints, indexes), use obj_type as name if name is empty
                                if not name and obj_type in ['foreign_keys', 'check_constraints', 'default_constraints', 'indexes', 'primary_keys']:
                                    name = obj_type  # Use category name as identifier
                                if name:  # Only add if we have a name
                                    self.selected_objects.add((obj_type, schema, name))
                    else:
                        self.tree.item(item, text="[ ]")
                        # Remove from selection
                        values = self.tree.item(item, "values")
                        if values:
                            schema = values[1] if len(values) > 1 else ""
                            name = values[2] if len(values) > 2 else ""
                            # For triggers, name might be in format "name (on table)", extract just the name
                            if " (on " in name:
                                name = name.split(" (on ")[0]
                            parent = self.tree.parent(item)
                            obj_type = None
                            for cat_type, cat_node in self.category_nodes.items():
                                if cat_node == parent:
                                    obj_type = cat_type
                                    break
                            if obj_type:
                                # For SQL-based objects (constraints, indexes), use obj_type as name if name is empty
                                if not name and obj_type in ['foreign_keys', 'check_constraints', 'default_constraints', 'indexes', 'primary_keys']:
                                    name = obj_type  # Use category name as identifier
                                if name:  # Only remove if we have a name
                                    self.selected_objects.discard((obj_type, schema, name))
                    
                    self._update_summary()
                    if self.on_selection_change:
                        self.on_selection_change()
    
    def _on_filter_change(self, *args):
        """Handle filter text change."""
        filter_text = self.filter_var.get().lower()
        if not filter_text:
            # Show all items
            for item in self.tree.get_children():
                self._show_item_and_children(item)
        else:
            # Filter items
            for item in self.tree.get_children():
                self._filter_item(item, filter_text)
    
    def _show_item_and_children(self, item):
        """Show an item and all its children."""
        self.tree.item(item, open=True)
        for child in self.tree.get_children(item):
            self._show_item_and_children(child)
    
    def _filter_item(self, item, filter_text):
        """Filter an item based on filter text."""
        text = self.tree.item(item, "text").lower()
        values = self.tree.item(item, "values")
        values_text = " ".join(str(v).lower() for v in values) if values else ""
        
        matches = filter_text in text or filter_text in values_text
        
        # Check children
        children = self.tree.get_children(item)
        child_matches = False
        for child in children:
            if self._filter_item(child, filter_text):
                child_matches = True
        
        if matches or child_matches:
            self.tree.item(item, open=True)
            return True
        else:
            # Hide item (we can't actually hide, so we'll just collapse)
            return False
    
    def _update_summary(self):
        """Update selection summary label."""
        count = len(self.selected_objects)
        self.summary_label.config(text=f"Selected: {count} object(s)")
    
    def get_selected_objects(self) -> List[Tuple[str, str, str]]:
        """Get list of selected objects as (object_type, schema, name) tuples."""
        return list(self.selected_objects)
    
    def select_all(self):
        """Select all objects."""
        for item in self.tree.get_children():
            self._select_all_in_category(item)
        self._update_summary()
        if self.on_selection_change:
            self.on_selection_change()
    
    def _select_all_in_category(self, category_item):
        """Select all items in a category."""
        for item in self.tree.get_children(category_item):
            tags = self.tree.item(item, "tags")
            if "checkbox" in tags:
                self.tree.item(item, text="[x]")
                values = self.tree.item(item, "values")
                if values:
                    schema = values[1] if len(values) > 1 else ""
                    name = values[2] if len(values) > 2 else ""
                    # For triggers, name might be in format "name (on table)", extract just the name
                    if " (on " in name:
                        name = name.split(" (on ")[0]
                    parent = self.tree.parent(item)
                    obj_type = None
                    for cat_type, cat_node in self.category_nodes.items():
                        if cat_node == parent:
                            obj_type = cat_type
                            break
                    if obj_type and name:
                        self.selected_objects.add((obj_type, schema, name))
            # Recursively select children
            for child in self.tree.get_children(item):
                self._select_all_in_category(child)
    
    def deselect_all(self):
        """Deselect all objects."""
        for item in self.tree.get_children():
            self._deselect_all_in_category(item)
        self.selected_objects.clear()
        self._update_summary()
        if self.on_selection_change:
            self.on_selection_change()
    
    def _deselect_all_in_category(self, category_item):
        """Deselect all items in a category."""
        for item in self.tree.get_children(category_item):
            tags = self.tree.item(item, "tags")
            if "checkbox" in tags:
                self.tree.item(item, text="[ ]")
            for child in self.tree.get_children(item):
                self._deselect_all_in_category(child)
    
    def select_missing(self):
        """Select only missing objects."""
        self.deselect_all()
        for item in self.tree.get_children():
            self._select_missing_in_category(item)
        self._update_summary()
        if self.on_selection_change:
            self.on_selection_change()
    
    def _select_missing_in_category(self, category_item):
        """Select missing items in a category."""
        for item in self.tree.get_children(category_item):
            tags = self.tree.item(item, "tags")
            if "checkbox" in tags and "missing" in tags:
                self.tree.item(item, text="[x]")
                values = self.tree.item(item, "values")
                if values:
                    schema = values[1] if len(values) > 1 else ""
                    name = values[2] if len(values) > 2 else ""
                    # For triggers, name might be in format "name (on table)", extract just the name
                    if " (on " in name:
                        name = name.split(" (on ")[0]
                    parent = self.tree.parent(item)
                    obj_type = None
                    for cat_type, cat_node in self.category_nodes.items():
                        if cat_node == parent:
                            obj_type = cat_type
                            break
                    if obj_type and name:
                        self.selected_objects.add((obj_type, schema, name))
