# Author: Satish Chauhan

"""
ADF Pipeline Trigger tab.
Trigger Azure Data Factory pipelines (e.g. sample_trigger).
More features (parameters, run status, multiple pipelines) can be added later.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from pathlib import Path
import threading
import sys

parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

# Default pipeline from ADF URL (pgs-mi-to-sql-sbx-eus2-adf / sample_trigger)
DEFAULT_SUBSCRIPTION_ID = "0b7f6570-6f27-48da-8ad4-7ae96052f5d2"
DEFAULT_RESOURCE_GROUP = "pgsurveys-core-sbx-eus2-rg"
DEFAULT_FACTORY_NAME = "pgs-mi-to-sql-sbx-eus2-adf"
DEFAULT_PIPELINE_NAME = "sample_trigger"


class ADFTriggerTab:
    """Tab for triggering ADF pipelines."""

    def __init__(self, parent, main_window):
        self.main_window = main_window
        self.frame = ttk.Frame(parent)
        self._create_widgets()

    def _log(self, msg: str):
        """Append message to log area and ensure visible."""
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)

    def _create_widgets(self):
        """Create UI: pipeline config and trigger button."""
        title_label = tk.Label(
            self.frame,
            text="Azure Data Factory — Pipeline Trigger",
            font=("Arial", 16, "bold"),
        )
        title_label.pack(pady=10)

        help_frame = ttk.LabelFrame(self.frame, text="About", padding=10)
        help_frame.pack(fill=tk.X, padx=20, pady=5)
        help_text = (
            "Trigger an Azure Data Factory pipeline. Configure the factory and pipeline below, then click Trigger. "
            "More options (parameters, run status, multiple pipelines) can be added later."
        )
        tk.Label(help_frame, text=help_text, font=("Arial", 9), fg="gray", wraplength=700, justify=tk.LEFT).pack(
            anchor=tk.W
        )

        config_frame = ttk.LabelFrame(self.frame, text="Pipeline configuration", padding=10)
        config_frame.pack(fill=tk.X, padx=20, pady=10)

        row = 0
        tk.Label(config_frame, text="Subscription ID:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.subscription_var = tk.StringVar(value=DEFAULT_SUBSCRIPTION_ID)
        ttk.Entry(config_frame, textvariable=self.subscription_var, width=50).grid(
            row=row, column=1, sticky=tk.W, pady=3, padx=5
        )
        row += 1
        tk.Label(config_frame, text="Resource group:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.resource_group_var = tk.StringVar(value=DEFAULT_RESOURCE_GROUP)
        ttk.Entry(config_frame, textvariable=self.resource_group_var, width=50).grid(
            row=row, column=1, sticky=tk.W, pady=3, padx=5
        )
        row += 1
        tk.Label(config_frame, text="Factory name:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.factory_var = tk.StringVar(value=DEFAULT_FACTORY_NAME)
        ttk.Entry(config_frame, textvariable=self.factory_var, width=50).grid(
            row=row, column=1, sticky=tk.W, pady=3, padx=5
        )
        row += 1
        tk.Label(config_frame, text="Pipeline name:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.pipeline_var = tk.StringVar(value=DEFAULT_PIPELINE_NAME)
        ttk.Entry(config_frame, textvariable=self.pipeline_var, width=50).grid(
            row=row, column=1, sticky=tk.W, pady=3, padx=5
        )

        btn_frame = ttk.Frame(self.frame)
        btn_frame.pack(fill=tk.X, padx=20, pady=10)
        self.trigger_btn = ttk.Button(btn_frame, text="Trigger pipeline", command=self._on_trigger)
        self.trigger_btn.pack(side=tk.LEFT, padx=5)

        log_frame = ttk.LabelFrame(self.frame, text="Output", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=12, font=("Consolas", 9), wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self._log("Ready. Configure the pipeline above and click 'Trigger pipeline'.")

    def _on_trigger(self):
        """Start trigger in a background thread."""
        sub = (self.subscription_var.get() or "").strip()
        rg = (self.resource_group_var.get() or "").strip()
        factory = (self.factory_var.get() or "").strip()
        pipeline = (self.pipeline_var.get() or "").strip()
        if not all([sub, rg, factory, pipeline]):
            messagebox.showwarning("Missing config", "Please fill Subscription ID, Resource group, Factory name, and Pipeline name.")
            return
        self.trigger_btn.config(state=tk.DISABLED)
        self._log(f"Triggering pipeline: {pipeline} in factory {factory}...")
        threading.Thread(target=self._do_trigger, args=(sub, rg, factory, pipeline), daemon=True).start()

    def _do_trigger(self, subscription_id: str, resource_group: str, factory_name: str, pipeline_name: str):
        """Run ADF trigger in thread; update UI via after()."""
        def log_safe(msg: str):
            self.frame.after(0, lambda: self._log(msg))

        try:
            from azure_migration_tool.utils.adf_client import ADFClient
        except ImportError:
            try:
                from utils.adf_client import ADFClient
            except ImportError:
                log_safe("Error: Could not import ADFClient. Install: pip install azure-identity azure-mgmt-datafactory")
                self.frame.after(0, lambda: self.trigger_btn.config(state=tk.NORMAL))
                return

        try:
            client = ADFClient(
                factory_name=factory_name,
                resource_group=resource_group,
                subscription_id=subscription_id,
                logger=log_safe,
            )
            run_id = client.trigger_pipeline(pipeline_name, parameters={})
            log_safe(f"Pipeline triggered successfully. Run ID: {run_id}")
        except Exception as e:
            log_safe(f"Error: {e}")
        finally:
            self.frame.after(0, lambda: self.trigger_btn.config(state=tk.NORMAL))
