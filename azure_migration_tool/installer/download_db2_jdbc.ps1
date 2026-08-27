# Download DB2 JDBC driver into azure_migration_tool/drivers for exe embedding.
# Run once before build_exe.py / build_installer.ps1 -BuildExe.
# End users never download this — it is packaged inside the exe/setup.

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -LiteralPath $MyInvocation.MyCommand.Path
$AppDir = Split-Path -LiteralPath $ScriptDir
$DriversDir = Join-Path $AppDir "drivers"
$JarPath = Join-Path $DriversDir "db2jcc4.jar"
$Url = "https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar"

New-Item -ItemType Directory -Force -Path $DriversDir | Out-Null

if ((Test-Path $JarPath) -and ((Get-Item $JarPath).Length -gt 1000000)) {
    Write-Host "DB2 JDBC already present: $JarPath ($([math]::Round((Get-Item $JarPath).Length/1MB,1)) MB)"
    exit 0
}

Write-Host "Downloading DB2 JDBC (build-time embed only)..."
Write-Host "  $Url"
Invoke-WebRequest -Uri $Url -OutFile $JarPath -UseBasicParsing -TimeoutSec 180
if (-not (Test-Path $JarPath) -or ((Get-Item $JarPath).Length -lt 1000000)) {
    Write-Host "ERROR: download failed or file too small" -ForegroundColor Red
    exit 1
}
Write-Host "Saved: $JarPath ($([math]::Round((Get-Item $JarPath).Length/1MB,1)) MB)"
Write-Host "Next: python build_exe.py  (jar is bundled into AzureMigrationTool.exe)"
