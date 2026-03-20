# Azure Migration Tool – NSIS Installer

Developed by Satish Chauhan.

This folder contains a **separate installer flow** that does not change the existing PyInstaller build. It produces a Windows setup executable (e.g. `AzureMigrationTool_Setup.exe`) for easy deployment.

## Prerequisites

- **NSIS 3.x** installed and on `PATH`.  
  Download: https://nsis.sourceforge.io/  
  Or with Chocolatey: `choco install nsis`

- **Built exe** from the main build: run `python build_exe.py` from the `azure_migration_tool` folder first.  
   This creates `azure_migration_tool/dist/AzureMigrationTool.exe` and also copies it to `AzureMigrationTool_<version>.exe` (e.g. `AzureMigrationTool_1.1.6.exe`) so each build produces a new file.

## Build steps

1. **Build the application exe** (from repo root or `azure_migration_tool`):

   ```bash
   cd azure_migration_tool
   python build_exe.py
   ```

   This produces `azure_migration_tool/dist/AzureMigrationTool.exe`.

2. **Build the installer** (from the same `azure_migration_tool` folder):

   ```bash
   makensis installer\AzureMigrationTool.nsi
   ```

   When you use `installer\build_installer.bat` or `build_installer.ps1`, the script passes the app version so the output is **versioned**: `dist/AzureMigrationTool_Setup_1.1.6.exe` (or whatever `__version__` is). Each build creates a **new file** instead of overwriting the previous one. If you run `makensis` directly without `/DVERSION`, the output is `AzureMigrationTool_Setup.exe`.

3. **Distribute** the setup exe (e.g. `AzureMigrationTool_Setup_1.1.6.exe`).  
   End users run the setup to install the app (Start Menu shortcut, Add/Remove Programs, uninstaller). The installer can include **ODBC Driver 18** and **Java 17** so the destination PC has everything for SQL Server, Azure SQL, and DB2/JDBC.

## What the installer can include (all-in-one)

| Component | How to include | Purpose |
|-----------|----------------|---------|
| **App exe** | Build with `python build_exe.py` (includes `drivers/db2jcc4.jar` if present) | Main app + DB2 JDBC driver |
| **ODBC Driver 18** | Place `msodbcsql18_x64.msi` in `installer/odbc/` or let `build_installer.ps1` download it | SQL Server / Azure SQL |
| **Java 17** | Run `.\installer\download_java.ps1` then build, or use `.\installer\build_installer.ps1 -IncludeJava` | DB2 Compare (Schema/Data) tabs |

**ODBC:** The folder `installer/odbc/` can contain `msodbcsql18_x64.msi`. If present, the installer bundles it and runs it during setup. Download from https://go.microsoft.com/fwlink/?linkid=2249006 or let `build_installer.ps1` download it.

**Java:** Run `.\installer\download_java.ps1` to download Eclipse Temurin 17 and extract to `installer/java/`. If `installer/java/bin/java.exe` exists at build time, the installer bundles it so the app finds it next to the exe (no separate Java install on the target PC). Or use `.\installer\build_installer.ps1 -IncludeJava` to download Java and then build.

## Optional: run the helper script

From `azure_migration_tool` you can run:

```batch
installer\build_installer.bat
```

This checks that the exe exists and then runs `makensis` for you.

**PowerShell (recommended)** – downloads ODBC (and optionally Java) if missing; by default uses existing exe and builds the installer:

```powershell
.\installer\build_installer.ps1                  # use existing dist\AzureMigrationTool.exe, create versioned setup (default)
.\installer\build_installer.ps1 -BuildExe        # build exe from code first, then create installer
.\installer\build_installer.ps1 -IncludeJava     # download Java 17 if needed, then build installer (uses existing exe)
.\installer\build_installer.ps1 -BuildExe -IncludeJava  # build exe, add Java, then build installer
```

You must have **NSIS** installed (e.g. from https://nsis.sourceforge.io/ or `choco install nsis -y` as Administrator) to produce `AzureMigrationTool_Setup.exe`.

**DB2 JDBC:** Place `db2jcc4.jar` in `azure_migration_tool/drivers/` before running `python build_exe.py`. The exe will then contain the JDBC driver; no separate step in the installer is needed.

## Notes

- The NSIS script does **not** modify `build_exe.py` or the PyInstaller output; it only packages the existing `dist/AzureMigrationTool.exe`.
- For an **all-in-one** setup: use ODBC MSI in `installer/odbc/`, Java in `installer/java/` (via `download_java.ps1` or `-IncludeJava`), and build the exe with `drivers/db2jcc4.jar` present. The resulting installer will deploy the app, ODBC Driver 18, and Java 17 so the target PC needs nothing else.
