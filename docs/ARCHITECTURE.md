# Azure Migration Tool – Architecture (Low-Level)

This document describes the application architecture, components, and data flows. All diagrams use Mermaid and render in Markdown viewers that support it (e.g. GitHub, VS Code).

---

## 1. High-Level Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           AZURE MIGRATION TOOL                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ENTRY (main.py)                                                                 │
│    → Tk root, DependencyChecker (setup/auto_setup), MainWindow (gui/main_window) │
├──────────────┬──────────────┬──────────────┬──────────────┬──────────────────────┤
│  GUI LAYER   │  ORCHESTRATION │  SERVICES   │  VALIDATION  │  SETUP / UTILS       │
│  main_window │  full_migration│  backup/    │  schema_     │  auto_setup,         │
│  tabs/*      │                │  restore/   │  data_service│  driver_utils,       │
│  widgets/    │                │  migration  │  run_subproc │  adf_client          │
│  utils/      │                │             │  connections │  azure_token_cache   │
│  dialogs/    │                │             │              │                      │
├──────────────┴──────────────┴──────────────┴──────────────┴──────────────────────┤
│  DATA / AUTH: src/utils/database, gui/utils/database_utils, MSAL (azure_token_cache) │
│  CONFIG: config.json, project.json, database_config.json, env vars                │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Application Entry & Startup Flow

```mermaid
flowchart TB
    subgraph Entry["Entry (main.py)"]
        A[main]
        A --> B{getattr(sys, 'frozen')?}
        B -->|Exe| C[Set app_dir = _MEIPASS / exe.parent]
        B -->|Script| D[Set app_dir = __file__.parent]
        C --> E[Insert gui, setup, utils, src, backup into sys.path]
        D --> E
        E --> F[tk.Tk]
        F --> G[Root.withdraw]
        G --> H[DependencyChecker.quick_check]
        H --> I{All OK?}
        I -->|No| J[MessageBox: install Java/ODBC/JDBC]
        I -->|Yes| K[Root.deiconify]
        J --> K
        K --> L[MainWindow(root)]
        L --> M[root.mainloop]
    end
```

---

## 3. GUI Layer – Main Window & Tabs

```mermaid
flowchart TB
    subgraph MainWindow["gui/main_window.py - MainWindow"]
        MW[MainWindow.__init__]
        MW --> SHARED[Shared connection vars: src/dest server, db, auth, user, password]
        MW --> MENU[_create_menu]
        MW --> NB[ttk.Notebook - 7 placeholder frames]
        MW --> LAZY[_on_tab_changed: lazy-load tab content on first select]
        MW --> STATUS[Status bar]
        MW --> CLOSE[_on_closing: cleanup]
    end

    subgraph Tabs["gui/tabs/* (lazy-loaded)"]
        T0[project_tab - Project path, backups/migrations/restores]
        T1[backup_restore_tab - .bak to Blob, Restore from Blob]
        T2[full_migration_tab - run_full_migration]
        T3[schema_tab - Schema Backup/Migration, script gen]
        T4[data_migration_tab - run_migration, ADF optional]
        T5[schema_validation_tab - Schema compare]
        T6[data_validation_tab - Row comparison]
    end

    subgraph Tools["Menu: Tools"]
        LEGACY_DATA[Legacy Data Validation window]
        COMPARE_DB2[Compare DB2 Schema window]
        EXP_MENU[Experiments POC: adf_trigger_tab, identity_cdc_tab — Toplevel windows]
    end

    NB --> T0 & T1 & T2 & T3 & T4 & T5 & T6
    MENU --> Tools
```

---

## 4. GUI Sub-Components (Widgets, Utils, Dialogs)

```mermaid
flowchart LR
    subgraph Widgets["gui/widgets/"]
        CW[connection_widget - DB connection fields]
        ST[schema_tree - TreeView schema browser]
        SDV[schema_diff_viewer - Diff display]
    end

    subgraph Utils["gui/utils/"]
        DU[database_utils - connect SQL/DB2, MSAL]
        SC[schema_comparison]
        SSG[schema_script_generator]
        SM[schema_matching]
        DBS[db2_schema, db2_type_mapping]
        LC[log_console]
        EU[excel_utils]
        SCF[server_config, scrollable_frame, tooltip]
    end

    subgraph Dialogs["gui/dialogs/"]
        RPD[restore_preview_dialog]
    end

    Tabs["Tabs"] --> Widgets & Utils & Dialogs
```

---

## 5. Core Services – Backup, Restore, Migration

```mermaid
flowchart TB
    subgraph Backup["src/backup/"]
        SB[schema_backup.run_backup - schema + meta to project_path/backups]
        BTE[exporters - used by schema_backup / schema_tab]
        BTB[bak_to_blob.run_bak_backup_to_blob - .bak → Azure Blob]
    end

    subgraph Restore["src/restore/"]
        SR[schema_restore.get_backup_paths, run_restore - tables from backup]
        NF[nullability_fix]
        RFB[restore_from_blob.run_restore_from_blob - RESTORE FROM URL]
    end

    subgraph Migration["src/migration/"]
        DM[data_migration.run_migration - pyodbc/BCP or ADF]
    end

    subgraph Utils["src/utils/"]
        DB[database.py - pyodbc helpers]
        CFG[config.py]
        LOG[logging.py]
        PATHS[paths.py]
        SQL[sql.py]
        AZ[azure_compat.py]
    end

    Backup --> Utils
    Restore --> Utils
    Migration --> Utils
```

---

## 6. Full Migration Orchestration Flow

```mermaid
flowchart TB
    FM[full_migration.run_full_migration]
    FM --> S1[1. run_backup - schema backup to backup_root]
    S1 --> S2[2. run_restore - restore tables only]
    S2 --> S3[3. run_migration - data migration]
    S3 --> S4[4. run_restore - final restore]
    S4 --> SUM[Summary + log_callback to GUI]

    S1 -.-> FS1[(project_path/backups/...)]
    S2 -.-> FS1
    S3 -.-> DB[(SQL Server / Azure SQL)]
    S4 -.-> FS1
```

```mermaid
sequenceDiagram
    participant Tab as Full Migration Tab
    participant FM as full_migration
    participant Backup as schema_backup
    participant Restore as schema_restore
    participant Migrate as data_migration
    participant FS as File System
    participant DB as SQL Server / Azure SQL

    Tab->>FM: run_full_migration(cfg) [background thread]
    FM->>Backup: run_backup()
    Backup->>DB: pyodbc schema + paths
    Backup->>FS: backup_root/.../schema, meta, logs
    FM->>Restore: run_restore() [tables]
    Restore->>FS: get_backup_paths, read backup
    Restore->>DB: pyodbc CREATE TABLE etc.
    FM->>Migrate: run_migration()
    Migrate->>DB: pyodbc/BCP or ADF
    FM->>Restore: run_restore() [final]
    FM->>Tab: summary via log_callback
```

---

## 7. Validation Layer (Legacy – DB2 ↔ Azure SQL)

```mermaid
flowchart TB
    subgraph Validation["validation/"]
        VCFG[config.py - load_config, output_dir]
        VCONN[connections.py - connect_db2, connect_azure_sql]
        VS[schema_service.LegacySchemaValidationService]
        VD[data_service.LegacyDataValidationService]
        RSP[run_subprocess.run_legacy_data_validation_subprocess]
        AC[azure_catalog]
    end

    subgraph GUI_Val["Tabs / Tools"]
        LSV[legacy_schema_validation_tab]
        LDV[legacy_data_validation_tab]
    end

    LSV --> VS
    LDV --> RSP
    RSP --> VD
    VS --> VCONN
    VD --> VCONN
    VCONN --> VCFG
```

---

## 8. Legacy Data Validation – Subprocess & Queue

```mermaid
sequenceDiagram
    participant UI as legacy_data_validation_tab
    participant Q as multiprocessing.Queue
    participant Proc as multiprocessing.Process
    participant Sub as run_legacy_data_validation_subprocess
    participant Svc as LegacyDataValidationService

    UI->>Proc: start(target=Sub, args=(queue, config, ...))
    Proc->>Sub: run in separate process
    Sub->>Svc: compare_row_counts / null_values / distinct_key
    Svc->>Q: put("progress", step, ...)
    Svc->>Q: put("result", step, df)
    Sub->>Q: put("done" / "error")
    UI->>Q: get() in loop, update GUI (after(0, ...))
```

---

## 9. Data & Auth Flow

```mermaid
flowchart LR
    subgraph SQL["SQL Server / Azure SQL"]
        PYODBC[pyodbc - ODBC Driver 18/17]
        AUTH[Windows / SQL / Entra]
    end

    subgraph Entra["Entra (MSAL)"]
        ATC[azure_token_cache - token cache]
        MSAL[msal - interactive / device code]
    end

    subgraph DB2["DB2"]
        JAY[jaydebeapi + db2jcc4.jar]
    end

    GUI[gui/utils/database_utils] --> PYODBC
    GUI --> ATC
    ATC --> MSAL
    src[src/utils/database] --> PYODBC
    validation[validation/connections] --> PYODBC
    validation --> JAY
```

---

## 10. File System Layout (Project & Backups)

```mermaid
flowchart TB
    ROOT[project_path]
    ROOT --> backups[backups/]
    ROOT --> migrations[migrations/]
    ROOT --> restores[restores/]
    ROOT --> validation[validation/]
    ROOT --> logs[logs/]
    ROOT --> project_json[project.json]

    backups --> B1[server_slug/db_slug/runs/run_id/]
    B1 --> schema[schema/]
    B1 --> meta[meta/]
    B1 --> logs_b[logs/]

    FM[migration_runs/run_id/]
    FM --> logs_m[logs/]
    FM --> meta_m[meta/]
```

---

## 11. Azure Integration

```mermaid
flowchart TB
    subgraph Azure["Azure"]
        BLOB[Blob Storage - .bak upload/download]
        ADF[Data Factory - optional pipeline]
        ENTRA[Entra ID - Azure SQL auth]
    end

    bak_to_blob[src/backup/bak_to_blob] --> BLOB
    restore_from_blob[src/restore/restore_from_blob] --> BLOB
    backup_restore_tab[backup_restore_tab] --> bak_to_blob
    backup_restore_tab --> restore_from_blob

    data_migration_tab[data_migration_tab] --> data_migration[src/migration/data_migration]
    data_migration --> ADF
    adf_client[utils/adf_client] --> ADF

    database_utils[gui/utils/database_utils] --> azure_token_cache[azure_token_cache]
    azure_token_cache --> ENTRA
```

---

## 12. Build Pipeline

```mermaid
flowchart LR
    A[build_exe.py] --> B[PyInstaller]
    B --> C[dist/AzureMigrationTool_<version>.exe]
    C --> D[installer/build_installer.ps1]
    D --> E[NSIS]
    E --> F[dist/AzureMigrationTool_Setup_<version>.exe]
```

**Branding:** Put `resources/logo.png` in the app package. `build_exe.py` uses **Pillow** (auto-installed if missing) to generate `resources/app.ico`, embeds it in the PyInstaller **exe**, and the NSIS script uses the same `.ico` for the installer wizard when `app.ico` exists (`!if /FileExists`). See `resources/README.md` and `requirements-build.txt`.

The NSIS script (`installer/AzureMigrationTool.nsi`) uses the **MultiUser** plugin: installers can target **current user** (default under `%LOCALAPPDATA%\Programs\`, HKCU, per-user Start Menu) or **all users** (`Program Files`, HKLM, common Start Menu; UAC when needed). Bundled **ODBC 18 MSI** runs only for **all-users** installs; per-user installs skip it (install ODBC separately). Silent flags: `/CurrentUser` or `/AllUsers` with `/S`.

```mermaid
flowchart TB
    subgraph Bundled["Bundled in exe (build_exe.py)"]
        ENTRY[main.py]
        GUI_PKG[gui/, setup/, backup/, src/]
        VAL_PKG[validation/]
        DRIVERS[drivers/ e.g. db2jcc4.jar]
    end

    subgraph Excluded["Excluded"]
        PYS[pyspark, py4j]
        DB2_VAL[db2_azure_validation - optional, separate]
    end

    PyInstaller --> Bundled
```

---

## 13. CI/CD (GitHub Actions)

```mermaid
flowchart LR
    TRIGGER[Push main/master, tag v*, release, workflow_dispatch]
    TRIGGER --> CHECKOUT[Checkout]
    CHECKOUT --> PY[Python 3.11, install deps + PyInstaller]
    PY --> BUILD[python azure_migration_tool/build_exe.py]
    BUILD --> EXE[dist-versioned/AzureMigrationTool-<ver>.exe]
    EXE --> NSIS[Install NSIS, build_installer.ps1]
    NSIS --> SETUP[dist-versioned/AzureMigrationTool_Setup_<ver>.exe]
    SETUP --> RELEASE[Create/update GitHub Release]
```

---

## 14. Dependency Overview

| Layer        | Component / path | Role |
|-------------|-------------------|------|
| Entry       | `main.py`         | Tk root, DependencyChecker, MainWindow |
| UI          | `gui/main_window.py` | Tabs, menu, shared connection vars |
| UI          | `gui/tabs/*.py`   | Project, Backup & Restore, Full Migration, Schema, Data Migration, Schema/Data Validation |
| UI          | `gui/widgets/`, `gui/utils/`, `gui/dialogs/` | Connection, schema tree, diff, DB utils, `schema_remap` / `compare_keys` (validation keys), script gen, Excel, restore preview |
| Orchestration | `src/orchestration/full_migration.py` | run_full_migration: backup → restore tables → migrate → restore |
| Services    | `src/backup/schema_backup.py` | run_backup |
| Services    | `src/backup/bak_to_blob.py` | run_bak_backup_to_blob |
| Services    | `src/restore/schema_restore.py` | get_backup_paths, run_restore |
| Services    | `src/restore/restore_from_blob.py` | run_restore_from_blob |
| Services    | `src/migration/data_migration.py` | run_migration |
| Validation  | `validation/schema_service.py`, `validation/data_service.py` | Legacy DB2 ↔ Azure SQL |
| Subprocess  | `validation/run_subprocess.py` | Legacy data validation in separate process + Queue |
| Setup       | `setup/auto_setup.py`, `utils/driver_utils.py` | ODBC/Java/PySpark/DB2 check and install |
| Optional    | `utils/adf_client.py` | Azure Data Factory |
| Config      | `config.json`, `project.json`, `database_config.json`, env vars | |

---

## 15. Threading Model

- **Main thread**: Tk event loop (GUI).
- **Background threads**: Long operations (full migration, schema backup/restore, data migration, backup/restore to/from blob, schema/data validation) run in `threading.Thread(..., daemon=True)`; results and log messages are pushed to the UI via `frame.after(0, ...)`.
- **Legacy data validation**: Runs in a separate **process** (`multiprocessing.Process`) with a **multiprocessing.Queue** so the JVM (DB2/JDBC) does not block the main process; the UI thread reads from the queue and updates the GUI.

---

*Generated for Azure Migration Tool. Diagrams use Mermaid; render in a Markdown viewer that supports Mermaid (e.g. GitHub, VS Code with Mermaid extension).*
