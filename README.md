# Azure Migration Tool

A desktop app to migrate and compare SQL Server / Azure SQL databases. It helps you move your database (schema and data) from a source server to a destination, and to validate that everything matches.

## Getting started (non-technical users)

1. **Create or open a project**  
   On the **Projects** tab: click "Create New Project" and choose a name and folder. A project is just a folder where the tool saves backups and migration files.

2. **Run a full migration**  
   Open the **Full Migration** tab.  
   - **Source database**: where your data is now (server, database name, and how you sign in).  
   - **Destination database**: where you want the data to go.  
   Then click Run.

3. **If something is missing**  
   The tool may ask you to install a "database driver" (from Microsoft). Use **Tools > Install database driver** or follow the on-screen steps. For normal SQL Server migration, that’s usually all you need.

4. **Need help?**  
   Use **Help > Getting started** in the app, or **Tools > Check what’s installed** to see what’s required.

## What each tab does

| Tab | Use it for |
|-----|------------|
| **Projects** | Create or open a project folder. Start here. |
| **Full Migration** | Move your whole database in one go (backup → copy data → restore). Best for most users. |
| **Schema Backup/Migration** | Backup or copy only the database structure (tables, views, etc.) without data, or compare two databases. |
| **Data Migration** | Copy data only (when you’ve already set up the schema). |
| **Data Validation** | Check that row counts and data match between source and destination. |
| **Schema Validation** | Compare database structure (tables, columns, types) between two databases. |
| **Legacy Data** / **Compare DB2 (Schema)** | Compare IBM DB2 to Azure SQL (data or schema). Requires Java; use the main Data/Schema Validation tabs if you don’t use DB2. |

## Requirements

- **Windows**
- For SQL Server / Azure SQL: the app will prompt to install the Microsoft ODBC driver if needed.
- For DB2 comparison: Java 11+ and PySpark (see in-app messages).

## Running from source

```bash
pip install -r azure_migration_tool/requirements.txt
python -m azure_migration_tool.main
```

Or run `main.py` from the `azure_migration_tool` folder.

## Testing Legacy Data Validation (DB2 → Azure SQL)

From the repo root (with Java 11+ and PySpark installed):

```bash
# Smoke test (no real DBs – checks that the service loads and runs):
python -m azure_migration_tool.test_legacy_data_validation --smoke

# With a config file (use any DB2 + Azure SQL you have):
python -m azure_migration_tool.test_legacy_data_validation --config path/to/database_config.json

# Optional: limit to one schema
python -m azure_migration_tool.test_legacy_data_validation --config path/to/database_config.json --schema MYSCHEMA
```

Config file format: JSON with `db2` and `azure_sql` sections. Use `username` and `password` (or `user` – the script normalizes it). Example: `azure_migration_tool/db2_azure_validation/database_config.example.json`.

---

For a detailed list of UX issues and fixes (what a new non-technical user sees and how it was improved), see **UX_AUDIT_AND_FIXES.md**.
