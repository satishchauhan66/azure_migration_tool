# Bundled tools (BCP)

Place **SqlCmdLnUtils.msi** here (Microsoft SQL Server Command Line Utilities).

The installer build script downloads it automatically:

```powershell
cd azure_migration_tool
.\installer\build_installer.ps1
```

That MSI is:

- Bundled inside the PyInstaller exe (`tools/` in `_MEIPASS`)
- Installed silently by the NSIS setup (`AzureMigrationTool_Setup_*.exe`)
- Used at runtime by **Data Migration > BCP** pre-flight to install `bcp.exe` if missing

Manual download: [SQL Server Command Line Utilities](https://learn.microsoft.com/en-us/sql/tools/sqlcmd-utility)
