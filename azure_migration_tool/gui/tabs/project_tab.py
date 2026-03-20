# Author: Sa-tish Chauhan

"""
Project Management Tab
Allows users to create and manage migration projects.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path
import json
import os
from datetime import datetime


class ProjectTab:
    """Project management tab."""
    
    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self.current_project = None
        self.project_path = None
        
        self._create_widgets()
        
    def _create_widgets(self):
        """Create UI widgets."""
        # Title
        title_label = tk.Label(
            self.frame,
            text="Project Management",
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=10)
        
        # Getting started (user-friendly guidance)
        help_frame = ttk.LabelFrame(self.frame, text="Getting started", padding=10)
        help_frame.pack(fill=tk.X, padx=20, pady=5)
        help_text = (
            "A project is a folder where this tool saves backups and migration files. "
            "To migrate a database: 1) Create or open a project below. "
            "2) Open the \"Full Migration\" tab. 3) Enter your source and destination databases, then run."
        )
        help_label = tk.Label(help_frame, text=help_text, font=("Arial", 9), fg="gray", wraplength=700, justify=tk.LEFT)
        help_label.pack(anchor=tk.W)
        
        # Project info frame
        info_frame = ttk.LabelFrame(self.frame, text="Current Project", padding=10)
        info_frame.pack(fill=tk.X, padx=20, pady=10)
        
        self.project_info_label = tk.Label(
            info_frame,
            text="No project selected",
            font=("Arial", 10)
        )
        self.project_info_label.pack(anchor=tk.W)
        
        # Buttons frame
        buttons_frame = ttk.Frame(self.frame)
        buttons_frame.pack(fill=tk.X, padx=20, pady=10)
        
        # Create new project button
        create_btn = ttk.Button(
            buttons_frame,
            text="Create New Project",
            command=self.create_new_project,
            width=20
        )
        create_btn.pack(side=tk.LEFT, padx=5)
        
        # Browse project button
        browse_btn = ttk.Button(
            buttons_frame,
            text="Browse Project",
            command=self.browse_project,
            width=20
        )
        browse_btn.pack(side=tk.LEFT, padx=5)
        
        # Project details frame
        details_frame = ttk.LabelFrame(self.frame, text="Project Details", padding=10)
        details_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # Project name
        tk.Label(details_frame, text="Project Name:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.project_name_var = tk.StringVar()
        project_name_entry = ttk.Entry(details_frame, textvariable=self.project_name_var, width=40)
        project_name_entry.grid(row=0, column=1, sticky=tk.W, pady=5, padx=5)
        
        # Project folder
        tk.Label(details_frame, text="Project Folder:").grid(row=1, column=0, sticky=tk.W, pady=5)
        folder_frame = ttk.Frame(details_frame)
        folder_frame.grid(row=1, column=1, sticky=tk.W, pady=5, padx=5)
        
        self.project_folder_var = tk.StringVar()
        folder_entry = ttk.Entry(folder_frame, textvariable=self.project_folder_var, width=35)
        folder_entry.pack(side=tk.LEFT)
        
        browse_folder_btn = ttk.Button(folder_frame, text="Browse...", command=self._browse_folder)
        browse_folder_btn.pack(side=tk.LEFT, padx=5)
        
        # Description
        tk.Label(details_frame, text="Description:").grid(row=2, column=0, sticky=tk.NW, pady=5)
        self.description_text = tk.Text(details_frame, width=40, height=5)
        self.description_text.grid(row=2, column=1, sticky=tk.W, pady=5, padx=5)
        
        # Save project button
        save_btn = ttk.Button(
            details_frame,
            text="Save Project",
            command=self._save_project
        )
        save_btn.grid(row=3, column=1, sticky=tk.W, pady=10, padx=5)
        
    def create_new_project(self):
        """Create a new project."""
        dialog = tk.Toplevel(self.frame)
        dialog.title("Create New Project")
        dialog.geometry("500x300")
        dialog.transient(self.frame)
        dialog.grab_set()
        
        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (500 // 2)
        y = (dialog.winfo_screenheight() // 2) - (300 // 2)
        dialog.geometry(f'500x300+{x}+{y}')
        
        # Project name
        tk.Label(dialog, text="Project Name:").pack(pady=5)
        name_var = tk.StringVar()
        name_entry = ttk.Entry(dialog, textvariable=name_var, width=40)
        name_entry.pack(pady=5)
        
        # Project folder
        tk.Label(dialog, text="Project Folder:").pack(pady=5)
        folder_frame = ttk.Frame(dialog)
        folder_frame.pack(pady=5)
        
        folder_var = tk.StringVar()
        folder_entry = ttk.Entry(folder_frame, textvariable=folder_var, width=35)
        folder_entry.pack(side=tk.LEFT, padx=5)
        
        def browse():
            folder = filedialog.askdirectory(title="Select Project Folder")
            if folder:
                folder_var.set(folder)
        
        ttk.Button(folder_frame, text="Browse...", command=browse).pack(side=tk.LEFT, padx=5)
        
        # Description
        tk.Label(dialog, text="Description:").pack(pady=5)
        desc_text = tk.Text(dialog, width=40, height=5)
        desc_text.pack(pady=5)
        
        def create():
            name = name_var.get().strip()
            folder = folder_var.get().strip()
            description = desc_text.get("1.0", tk.END).strip()
            
            if not name:
                messagebox.showerror("Error", "Project name is required!")
                return
            
            if not folder:
                messagebox.showerror("Error", "Project folder is required!")
                return
            
            # Create project folder
            project_path = Path(folder) / name
            try:
                project_path.mkdir(parents=True, exist_ok=True)
                
                # Create subdirectories
                (project_path / "backups").mkdir(exist_ok=True)
                (project_path / "migrations").mkdir(exist_ok=True)
                (project_path / "restores").mkdir(exist_ok=True)
                (project_path / "validation").mkdir(exist_ok=True)
                (project_path / "logs").mkdir(exist_ok=True)
                
                # Create project config
                config = {
                    "name": name,
                    "folder": str(project_path),
                    "description": description,
                    "created": datetime.now().isoformat()
                }
                
                config_file = project_path / "project.json"
                with open(config_file, 'w') as f:
                    json.dump(config, f, indent=2)
                
                # Load project
                self._load_project(project_path)
                
                messagebox.showinfo("Success", f"Project '{name}' created successfully!")
                dialog.destroy()
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to create project: {str(e)}")
        
        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=10)
        
        ttk.Button(btn_frame, text="Create", command=create).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
        
    def browse_project(self):
        """Browse for an existing project."""
        folder = filedialog.askdirectory(title="Select Project Folder")
        if folder:
            project_path = Path(folder)
            config_file = project_path / "project.json"
            
            if config_file.exists():
                self._load_project(project_path)
                messagebox.showinfo("Success", "Project loaded successfully!")
            else:
                messagebox.showerror("Not a project", 
                    "This folder doesn't look like a project created by this tool. "
                    "Please choose a folder that contains a project, or create a new project first.")
                
    def _browse_folder(self):
        """Browse for project folder."""
        folder = filedialog.askdirectory(title="Select Project Folder")
        if folder:
            self.project_folder_var.set(folder)
            
    def _load_project(self, project_path):
        """Load a project."""
        self.project_path = project_path
        config_file = project_path / "project.json"
        
        if config_file.exists():
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            self.current_project = config
            self.project_name_var.set(config.get("name", ""))
            self.project_folder_var.set(config.get("folder", ""))
            self.description_text.delete("1.0", tk.END)
            self.description_text.insert("1.0", config.get("description", ""))
            
            self.project_info_label.config(
                text=f"Project: {config.get('name', 'Unknown')} | Folder: {project_path}"
            )
            
            # Update main window tabs with project info
            self.main_window._project_path = project_path
            if hasattr(self.main_window, 'full_migration_tab'):
                self.main_window.full_migration_tab.set_project_path(project_path)
            if hasattr(self.main_window, 'schema_tab'):
                self.main_window.schema_tab.set_project_path(project_path)
            if hasattr(self.main_window, 'data_migration_tab'):
                self.main_window.data_migration_tab.set_project_path(project_path)
            if hasattr(self.main_window, 'data_validation_tab'):
                self.main_window.data_validation_tab.set_project_path(project_path)
            if hasattr(self.main_window, 'schema_validation_tab'):
                self.main_window.schema_validation_tab.set_project_path(project_path)
            if hasattr(self.main_window, 'notify_poc_experiment_tabs_project_path'):
                self.main_window.notify_poc_experiment_tabs_project_path(project_path)

    def _save_project(self):
        """Save project configuration."""
        if not self.project_path:
            messagebox.showerror("Error", "No project loaded!")
            return
            
        config = {
            "name": self.project_name_var.get(),
            "folder": self.project_folder_var.get(),
            "description": self.description_text.get("1.0", tk.END).strip()
        }
        
        config_file = self.project_path / "project.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
            
        messagebox.showinfo("Success", "Project saved successfully!")

