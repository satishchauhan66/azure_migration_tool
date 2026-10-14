# Worker job payload

Defaults come from **Organization setup** and **environment profiles** (`dev` / `qa` / `uat` / `prod`).  
**Any non-empty field in the payload overrides defaults** (nothing is silently replaced).

## Minimal payload (MCP / 3rd party)

```json
{
  "source": { "server": "SOURCE-SQL\\INSTANCE", "databases": ["SampleApp_ss_sld_db22q"] },
  "restore_target": { "auth": "entra_mfa", "user": "you@example.com" },
  "steps": { "backup": true, "upload": true, "restore": true }
}
```

Environment is inferred from the database name (`db22q` → `qa`) unless you set `"environment": "uat"`.

## Full payload

See `azure_migration_tool/examples/jobs/job_payload.full.example.json`.

## CSV / Excel

Templates (with example row and column descriptions):

- `azure_migration_tool/examples/jobs/worker_job_template.csv`
- `azure_migration_tool/examples/jobs/worker_job_template.xlsx`

In the app: **Tools → Agent Workflow → Sample template…** (save a copy) or **Import CSV/Excel…**.

| Column | Meaning |
|--------|---------|
| `job_id` | Correlation id; rows with the same `job_id` + `source_server` merge into one job |
| `database` | One database per row (or use `databases` with comma/semicolon list on a single row) |
| `environment` | `dev` / `qa` / `uat` / `prod` (optional; can be inferred from DB name) |
| `source_server`, `restore_user`, … | Same as JSON job fields (empty = use saved defaults) |

## MCP

| Tool | Purpose |
|------|---------|
| `amt_import_job_spreadsheet(file_path)` | Parse CSV/XLSX → `jobs` (with defaults merged) |
| `amt_prepare_job_payload(job_json)` | Returns `effective_job` + `_effective` (what was default vs payload) **without running** |
| `amt_resolve_environment(...)` | Suggested `blob` + `restore_target` for an environment |
| `amt_run_bulk_job_json(job_json)` | Merge defaults, then run |

## Merge order

1. Payload (explicit values)  
2. Environment profile (`environment` or inferred from DB / hostname)  
3. Organization settings (UNC path, project folder, blob prefix)  
4. Global blob settings (`blob.use_saved_settings: true` only when blob fields are empty)
