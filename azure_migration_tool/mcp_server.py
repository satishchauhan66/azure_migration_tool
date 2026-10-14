# Author: Satish Chauhan

"""
MCP server so Cursor (and other MCP clients) can trigger headless worker jobs.

Install: pip install -r azure_migration_tool/requirements-mcp.txt

Run (stdio — add to Cursor MCP settings):
  python -m azure_migration_tool.mcp_server

When the GUI is open, streamable HTTP MCP starts on http://127.0.0.1:7823/mcp (if mcp is installed).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MCP_INSTRUCTIONS = (
    "Azure Migration Tool worker: bulk backup → blob → restore. "
    "Resolves dev/qa/uat/prod from DB names (db22q→qa), hostnames (myapp-uat), "
    "or job.environment. Tools: amt_resolve_environment, amt_list_environment_profiles, "
    "amt_prepare_job_payload, amt_import_job_spreadsheet, amt_run_bulk_job_json, "
    "amt_list_playbooks, amt_run_playbook."
)

DEFAULT_MCP_HTTP_PORT = 7823
DEFAULT_MCP_HTTP_PATH = "/mcp"


def _run_job_dict(job: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from azure_migration_tool.src.orchestration.bulk_local_blob_pipeline import (
            run_bulk_local_backup_blob_restore_job,
        )
    except ImportError:
        from src.orchestration.bulk_local_blob_pipeline import (
            run_bulk_local_backup_blob_restore_job,
        )

    lines: list[str] = []

    def log(msg: str) -> None:
        lines.append(msg)

    summary = run_bulk_local_backup_blob_restore_job(job, log=log)
    summary["_log_tail"] = lines[-50:]
    return summary


def _register_tools(mcp: Any) -> None:
    @mcp.tool()
    def amt_validate_job(job_path: str) -> str:
        """Validate a worker job JSON file without executing it."""
        from azure_migration_tool.worker.__main__ import _load_job, _prepare_job, _validate_job

        path = Path(job_path).expanduser().resolve()
        job = _prepare_job(_load_job(path))
        issues = _validate_job(job)
        payload = {"job_path": str(path), "valid": not issues, "issues": issues}
        return json.dumps(payload, indent=2)

    @mcp.tool()
    def amt_run_bulk_local_backup_blob_restore(
        job_path: str,
        result_path: str = "",
    ) -> str:
        """
        Run bulk local backup → upload to blob → restore from blob (multi-database).

        job_path: path to JSON job file.
        result_path: optional path for result JSON; otherwise uses job output.project_path.
        """
        from azure_migration_tool.worker.__main__ import _load_job

        path = Path(job_path).expanduser().resolve()
        from azure_migration_tool.worker.__main__ import _prepare_job

        job = _prepare_job(_load_job(path))
        job["_run_origin"] = "mcp"
        if result_path:
            out = job.setdefault("output", {})
            if isinstance(out, dict):
                out["result_path"] = str(Path(result_path).expanduser().resolve())
        summary = _run_job_dict(job)
        return json.dumps(summary, indent=2)

    @mcp.tool()
    def amt_run_bulk_job_json(job_json: str, result_path: str = "") -> str:
        """
        Same as amt_run_bulk_local_backup_blob_restore but accepts inline JSON
        (for agents that build the job in memory).
        """
        from azure_migration_tool.worker.__main__ import _prepare_job

        job = _prepare_job(json.loads(job_json))
        if not isinstance(job, dict):
            raise ValueError("job_json must be a JSON object.")
        job["_run_origin"] = "mcp"
        if result_path:
            out = job.setdefault("output", {})
            if isinstance(out, dict):
                out["result_path"] = str(Path(result_path).expanduser().resolve())
        summary = _run_job_dict(job)
        return json.dumps(summary, indent=2)

    @mcp.tool()
    def amt_list_environment_profiles() -> str:
        """List dev/qa/uat/prod profiles (MI host + storage) from saved organization config."""
        try:
            from azure_migration_tool.gui.utils.environment_profiles import (
                load_environment_profiles,
            )
        except ImportError:
            from gui.utils.environment_profiles import load_environment_profiles
        return json.dumps(load_environment_profiles(), indent=2)

    @mcp.tool()
    def amt_resolve_environment(
        database: str = "",
        restore_server: str = "",
        storage_url: str = "",
        environment: str = "",
    ) -> str:
        """
        Infer dev/qa/uat/prod and return matching MI + blob profile for 3rd-party payloads.
        Pass environment to force; otherwise uses DB name (db22q), hostname tokens (e.g. myapp-uat), URL tokens.
        """
        try:
            from azure_migration_tool.gui.utils.environment_profiles import (
                detect_environment_from_hints,
                get_profile,
            )
        except ImportError:
            from gui.utils.environment_profiles import detect_environment_from_hints, get_profile

        detection = detect_environment_from_hints(
            environment=environment,
            database=database,
            restore_server=restore_server,
            storage_url=storage_url,
        )
        env = detection.get("environment") or "uat"
        prof = get_profile(env) or {}
        try:
            from azure_migration_tool.gui.utils.environment_profiles import resolve_database_catalog
        except ImportError:
            from gui.utils.environment_profiles import resolve_database_catalog
        catalog = resolve_database_catalog(
            database,
            restore_server=restore_server,
            storage_url=storage_url,
        ) if database else None
        payload = {
            "environment": env,
            "detection": detection,
            "catalog": catalog,
            "restore_target": {
                "server": (prof.get("mi_server") or "").strip(),
                "target_managed_instance": bool(prof.get("target_managed_instance", True)),
            },
            "blob": {
                "profile": env,
                "use_saved_settings": False,
                "auth_mode": (prof.get("blob_auth_mode") or "managed_identity").strip(),
                "storage_account_url": (prof.get("storage_account_url") or "").strip(),
                "container": (prof.get("container") or "sqlbackups").strip(),
                "folder": (prof.get("blob_folder_prefix") or "").strip(),
            },
        }
        return json.dumps(payload, indent=2)

    @mcp.tool()
    def amt_import_job_spreadsheet(file_path: str, prepare_defaults: bool = True) -> str:
        """
        Read a CSV or Excel worker job template (see examples/jobs/worker_job_template.xlsx).
        Returns jobs list; merges org/env defaults when prepare_defaults is true.
        """
        from azure_migration_tool.src.orchestration.job_spreadsheet import read_job_spreadsheet

        jobs = read_job_spreadsheet(file_path)
        if prepare_defaults:
            from azure_migration_tool.worker.__main__ import _prepare_job

            jobs = [_prepare_job(dict(j)) for j in jobs]
        return json.dumps({"job_count": len(jobs), "jobs": jobs}, indent=2)

    @mcp.tool()
    def amt_export_migration_brief(
        output_path: str,
        result_path: str = "",
        job_path: str = "",
    ) -> str:
        """
        Write a redacted migration brief (markdown or .json) for agents/PMs:
        summary, per-DB pipeline, errors, org/env context, run result.
        """
        from azure_migration_tool.src.orchestration.migration_brief import export_migration_brief

        fmt = "json" if output_path.lower().endswith(".json") else "markdown"
        out = export_migration_brief(
            output_path,
            result_path=result_path or None,
            job_path=job_path or None,
            format=fmt,
        )
        return json.dumps({"brief_path": str(out)}, indent=2)

    @mcp.tool()
    def amt_list_run_history(limit: int = 50) -> str:
        """AgentOps: recent pipeline runs (GUI, worker, MCP)."""
        try:
            from azure_migration_tool.gui.utils.run_history import load_run_history, summarize_history
        except ImportError:
            from gui.utils.run_history import load_run_history, summarize_history
        rows = load_run_history(max(1, min(200, int(limit))))
        return json.dumps({"summary": summarize_history(rows), "runs": rows}, indent=2)

    @mcp.tool()
    def amt_resolve_database_catalog(
        database: str,
        restore_server: str = "",
        storage_url: str = "",
    ) -> str:
        """Semantic catalog: owner, application, MI/blob mapping from DB name + profile patterns."""
        try:
            from azure_migration_tool.gui.utils.environment_profiles import resolve_database_catalog
        except ImportError:
            from gui.utils.environment_profiles import resolve_database_catalog
        return json.dumps(
            resolve_database_catalog(
                database,
                restore_server=restore_server,
                storage_url=storage_url,
            ),
            indent=2,
        )

    @mcp.tool()
    def amt_list_playbooks() -> str:
        """List bundled + user playbooks (declarative multi-step agent flows)."""
        from azure_migration_tool.src.orchestration.playbooks import list_playbook_paths, load_playbook

        items = []
        for path in list_playbook_paths():
            try:
                pb = load_playbook(path)
                items.append(
                    {
                        "path": str(path),
                        "playbook_id": pb.get("playbook_id") or pb.get("id"),
                        "name": pb.get("name"),
                        "description": pb.get("description"),
                        "step_count": len(pb.get("steps") or []),
                    }
                )
            except (OSError, json.JSONDecodeError, ValueError):
                continue
        return json.dumps({"playbooks": items}, indent=2)

    @mcp.tool()
    def amt_get_playbook(playbook_path: str) -> str:
        """Load full playbook JSON by file path (see amt_list_playbooks)."""
        from azure_migration_tool.src.orchestration.playbooks import load_playbook

        return json.dumps(load_playbook(playbook_path), indent=2)

    @mcp.tool()
    def amt_run_playbook(
        playbook_path: str,
        variables_json: str = "",
        dry_run: bool = False,
    ) -> str:
        """
        Run a declarative playbook: steps call resolve_environment, prepare_job_payload,
        run_bulk_job_json, validate_job, export_migration_brief, etc.
        variables_json overrides playbook.variables. dry_run skips execute steps.
        """
        from azure_migration_tool.src.orchestration.playbooks import load_playbook, run_playbook

        pb = load_playbook(playbook_path)
        vars_override: Dict[str, Any] = {}
        if variables_json and variables_json.strip():
            vars_override = json.loads(variables_json)
            if not isinstance(vars_override, dict):
                raise ValueError("variables_json must be a JSON object.")
        return json.dumps(run_playbook(pb, variables=vars_override, dry_run=dry_run), indent=2)

    @mcp.tool()
    def amt_prepare_job_payload(job_json: str) -> str:
        """
        Merge organization + environment defaults into a job payload without running it.
        Returns effective_job (all fields filled), what came from payload vs defaults.
        """
        from azure_migration_tool.worker.__main__ import _prepare_job

        raw = json.loads(job_json)
        if not isinstance(raw, dict):
            raise ValueError("job_json must be a JSON object.")
        effective = _prepare_job(raw)
        return json.dumps(
            {
                "effective_job": effective,
                "hint": "Send minimal payload; omitted fields use Organization setup + environment profiles.",
            },
            indent=2,
        )


def create_mcp_server() -> Optional[Any]:
    """Return a configured MCP server, or None if the mcp package is not installed."""
    try:
        from mcp.server.mcpserver import MCPServer

        server = MCPServer("azure-migration-tool", instructions=_MCP_INSTRUCTIONS)
    except ImportError:
        try:
            from mcp.server.fastmcp import FastMCP

            server = FastMCP("azure-migration-tool", instructions=_MCP_INSTRUCTIONS)
        except ImportError:
            return None
    _register_tools(server)
    return server


def mcp_http_url(host: str = "127.0.0.1", port: int = DEFAULT_MCP_HTTP_PORT) -> str:
    return f"http://{host}:{port}{DEFAULT_MCP_HTTP_PATH}"


def main() -> None:
    server = create_mcp_server()
    if server is None:
        raise SystemExit(
            "MCP package not installed.\n"
            "Run: pip install -r azure_migration_tool/requirements-mcp.txt"
        )
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
