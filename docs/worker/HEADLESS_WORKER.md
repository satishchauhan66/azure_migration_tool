# Headless worker (CLI + MCP)

Run the **Local Backup (multiple databases)** flow without the GUI:

1. **Backup** selected databases to disk  
2. **Upload** all `.bak` files to Azure Blob (structured paths per DB)  
3. **Restore** each database on the target Azure SQL / MI instance from blob  

## CLI

From the repository root (after `pip install -r azure_migration_tool/requirements.txt`):

```powershell
python -m azure_migration_tool.worker validate --job C:\path\to\job.json
python -m azure_migration_tool.worker run --job C:\path\to\job.json --json-stdout
```

Exit codes: `0` success, `1` completed with errors, `2` failed / invalid job.

Copy and edit `azure_migration_tool/examples/jobs/bulk_local_blob_restore.example.json`.

**Payload + defaults:** see [JOB_PAYLOAD.md](JOB_PAYLOAD.md). Use MCP `amt_prepare_job_payload` to preview the merged job before run.

**CSV / Excel:** `examples/jobs/worker_job_template.csv` (and `.xlsx` via **Agent Workflow → Sample template…**). MCP: `amt_import_job_spreadsheet`.

`local_backup.path` must be a folder (or `.bak` file) on the **SQL Server instance**—use a **UNC share** both SQL Server and this PC can access (same as Tools → Local Backup), not your laptop’s `D:\` unless SQL runs locally.

### Blob settings (one-time in UI)

**Tools → Organization setup** — wizard: (1) Azure Blob, (2) backup UNC, (3) project folder & blob prefix. Jobs can use empty `"local_backup": {"path": ""}` and `"blob": {}`; worker fills from saved settings on that PC.

### Skip steps

Upload/restore only (backups already done):

```json
{
  "steps": { "backup": false, "upload": true, "restore": true },
  "backup_runs": [
    {
      "database": "MyDb",
      "run_id": "20260412_120000",
      "files": ["D:\\SqlBackups\\...\\MyDb_part01of01.bak"]
    }
  ],
  "blob": { ... },
  "restore_target": { ... }
}
```

## MCP (Cursor and other clients)

```powershell
pip install -r azure_migration_tool/requirements-mcp.txt
```

Uses MCP Python SDK **2.x** (`MCPServer`). SDK 1.x (`FastMCP`) still works if already installed.

**Cursor** → Settings → MCP → add server:

```json
{
  "mcpServers": {
    "azure-migration-tool": {
      "command": "python",
      "args": [
        "-m",
        "azure_migration_tool.mcp_server"
      ],
      "cwd": "C:\\Users\\YOU\\Desktop\\program\\github\\azure_migration_tool"
    }
  }
}
```

Tools exposed:

| Tool | Purpose |
|------|---------|
| `amt_validate_job` | Check job JSON |
| `amt_run_bulk_local_backup_blob_restore` | Run from job file path |
| `amt_run_bulk_job_json` | Run from inline JSON |

The worker must run on **Windows** on a machine that can reach source SQL, blob storage, and the target server (ODBC, azcopy, Entra sign-in as needed).

### Environment profiles (dev / qa / uat / prod)

Saved in `%APPDATA%\\AzureMigrationTool\\environment_profiles.json` (wizard step 4). Jobs and MCP infer environment from:

- `job.environment` (explicit)
- MI hostname (`myapp-uat-…` → uat)
- DB name suffix letter (`db22q` → qa, `db22u` → uat, `db22p` → prod, `db22d` → dev)

**MCP (HTTP port 7823 when GUI is open):**

- `amt_resolve_environment(database, restore_server, storage_url, environment)` — build `restore_target` + `blob` for payloads
- `amt_list_environment_profiles()` — read saved profiles

### GUI auto-start

When the desktop app opens (and `mcp` is installed), it starts **streamable HTTP** MCP on `http://127.0.0.1:7823/mcp`. Endpoint is also written to `%APPDATA%\\AzureMigrationTool\\mcp_endpoint.json`. Disable with `AZURE_MIGRATION_TOOL_DISABLE_MCP=1`.

## Agent flow

1. External agent (Cursor, script, pipeline) writes `job.json`.  
2. Calls CLI or MCP `amt_run_bulk_local_backup_blob_restore`.  
3. Reads `result_path` / stdout JSON (`status`, `steps`, `errors`, `backup_runs`).  
4. Decides next action (retry, ticket update, validation job).

### GUI tester

**Tools → Agent Workflow (backup → blob → restore)** opens the same job document in a Toplevel window: build or load JSON, validate, run, and save under `worker_jobs/` for MCP/CLI.
