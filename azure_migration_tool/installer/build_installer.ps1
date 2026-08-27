# Build Azure Migration Tool installer (exe + ODBC + Java + NSIS).
# Run from azure_migration_tool: .\installer\build_installer.ps1
# By default: uses existing dist\AzureMigrationTool.exe and creates versioned setup (no params).
# Optional: -BuildExe to build the exe from code first, then create the installer.
# Optional: -IncludeJava to download and bundle Java 17 for DB2/JDBC (adds ~60MB to installer).
#
# The setup UI offers "all users" vs "current user" (NSIS MultiUser). Silent mode examples:
#   AzureMigrationTool_Setup_x.y.z.exe /S /CurrentUser
#   AzureMigrationTool_Setup_x.y.z.exe /S /AllUsers

param([switch]$BuildExe, [switch]$IncludeJava)

# Keep window open on error when run by double-click or from Explorer
function Pause-IfError {
    if ($Host.Name -eq "ConsoleHost") { Write-Host ""; Read-Host "Press Enter to close" }
}
trap {
    Write-Host "ERROR: $_" -ForegroundColor Red
    Pause-IfError
    exit 1
}

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -LiteralPath $MyInvocation.MyCommand.Path
if (-not $ScriptDir) { $ScriptDir = Split-Path -LiteralPath $PSCommandPath }
$AppDir = Split-Path -LiteralPath $ScriptDir
# Version from __init__.py (exe is built as AzureMigrationTool_<version>.exe)
$version = $null
$initPath = Join-Path $AppDir "__init__.py"
if (Test-Path $initPath) {
    try {
        $content = Get-Content -LiteralPath $initPath -Raw -Encoding UTF8 -ErrorAction Stop
        if ($content -and ($content -match '__version__\s*=\s*"([^"]+)"')) { $version = $Matches[1].Trim() }
    } catch { }
}
$DistExe = if ($version) { Join-Path $AppDir "dist\AzureMigrationTool_$version.exe" } else { Join-Path $AppDir "dist\AzureMigrationTool.exe" }
$OdbcMsi = Join-Path $ScriptDir "odbc\msodbcsql18_x64.msi"
$OdbcUrl = "https://go.microsoft.com/fwlink/?linkid=2249006"
$BcpMsi = Join-Path $ScriptDir "tools\SqlCmdLnUtils.msi"
$BcpUrls = @(
    "https://go.microsoft.com/fwlink/?linkid=2230791",
    "https://go.microsoft.com/fwlink/?linkid=2142258"
)

# 1. Ensure ODBC MSI is present
if (-not (Test-Path $OdbcMsi)) {
    Write-Host "Downloading ODBC Driver 18 x64 MSI..."
    $odbcDir = Join-Path $ScriptDir "odbc"
    New-Item -ItemType Directory -Force -Path $odbcDir | Out-Null
    Invoke-WebRequest -Uri $OdbcUrl -OutFile $OdbcMsi -UseBasicParsing
    Write-Host "  Saved: $OdbcMsi"
} else {
    Write-Host "ODBC MSI already present: $OdbcMsi"
}

# 1b. Ensure SQL Command Line Utilities MSI (bcp.exe) is present for bundling
if (-not (Test-Path $BcpMsi)) {
    Write-Host "Downloading SQL Server Command Line Utilities MSI (BCP)..."
    $toolsDir = Join-Path $ScriptDir "tools"
    New-Item -ItemType Directory -Force -Path $toolsDir | Out-Null
    $downloaded = $false
    foreach ($url in $BcpUrls) {
        try {
            Invoke-WebRequest -Uri $url -OutFile $BcpMsi -UseBasicParsing -TimeoutSec 300
            if ((Get-Item $BcpMsi).Length -gt 500000) { $downloaded = $true; break }
        } catch {
            Write-Host "  Failed: $url"
        }
    }
    if ($downloaded) {
        Write-Host "  Saved: $BcpMsi"
        # Also copy for PyInstaller bundle when building exe
        $appTools = Join-Path $AppDir "tools"
        New-Item -ItemType Directory -Force -Path $appTools | Out-Null
        Copy-Item -LiteralPath $BcpMsi -Destination (Join-Path $appTools "SqlCmdLnUtils.msi") -Force
    } else {
        Write-Host "Warning: BCP MSI download failed; installer will skip BCP install." -ForegroundColor Yellow
    }
} else {
    Write-Host "BCP MSI already present: $BcpMsi"
}

# 1c. Ensure DB2 JDBC jar is present for exe embed (no runtime download on target PCs)
$Db2Jar = Join-Path $AppDir "drivers\db2jcc4.jar"
if (-not (Test-Path $Db2Jar) -or ((Get-Item $Db2Jar).Length -lt 1000000)) {
    Write-Host "Fetching DB2 JDBC driver for embed (build-time)..."
    & (Join-Path $ScriptDir "download_db2_jdbc.ps1")
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: download_db2_jdbc.ps1 failed" -ForegroundColor Red
        Pause-IfError; exit 1
    }
}
if (-not (Test-Path $Db2Jar) -or ((Get-Item $Db2Jar).Length -lt 1000000)) {
    Write-Host "ERROR: drivers\db2jcc4.jar is required for build (DB2 JDBC). Place the jar or fix network access." -ForegroundColor Red
    Pause-IfError; exit 1
}
Write-Host "DB2 JDBC present for embed: $Db2Jar"

# 2. Optional: ensure Java is bundled for DB2/JDBC
$JavaExe = Join-Path $ScriptDir "java\bin\java.exe"
if ($IncludeJava -and -not (Test-Path $JavaExe)) {
    Write-Host "Downloading Java 17 for bundling (DB2/JDBC)..."
    & (Join-Path $ScriptDir "download_java.ps1")
}
if (Test-Path $JavaExe) {
    Write-Host "Java bundle present: $ScriptDir\java"
} elseif ($IncludeJava) {
    Write-Host "Warning: Java download failed; installer will not include Java." -ForegroundColor Yellow
}

# 3. By default use existing exe; use -BuildExe to build from code first.
if ($BuildExe) {
    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if (-not $pythonCmd) { $pythonCmd = Get-Command py -ErrorAction SilentlyContinue }
    if (-not $pythonCmd) {
        Write-Host "ERROR: Python not found. Install Python to use -BuildExe." -ForegroundColor Red
        Pause-IfError; exit 1
    }
    Write-Host "Building exe from code..."
    Push-Location $AppDir
    try {
        $buildResult = & $pythonCmd build_exe.py 2>&1
        if ($LASTEXITCODE -ne 0) { Write-Host $buildResult }
        if (-not (Test-Path $DistExe)) {
            Write-Host "ERROR: build_exe.py did not produce $DistExe (version from __init__.py)" -ForegroundColor Red
            Pop-Location
            Pause-IfError; exit 1
        }
    } catch {
        Write-Host "ERROR building exe: $_" -ForegroundColor Red
        Pop-Location
        Pause-IfError; exit 1
    }
    Pop-Location | Out-Null
} else {
    if (-not (Test-Path $DistExe)) {
        Write-Host "ERROR: $DistExe not found. Build it first or run with -BuildExe." -ForegroundColor Red
        Pause-IfError; exit 1
    }
    Write-Host "Using existing exe: $DistExe"
}

# 4. Find makensis
$makensis = $null
foreach ($candidate in @("makensis", "C:\Program Files (x86)\NSIS\makensis.exe", "C:\Program Files\NSIS\makensis.exe")) {
    if ($candidate -eq "makensis") {
        $exe = Get-Command makensis -ErrorAction SilentlyContinue
        if ($exe) { $makensis = $exe.Source; break }
    } else {
        if (Test-Path $candidate) { $makensis = $candidate; break }
    }
}
if (-not $makensis) {
    Write-Host ""
    Write-Host "NSIS (makensis) not found. Skipping installer creation." -ForegroundColor Yellow
    Write-Host "The standalone exe is still available at: $DistExe" -ForegroundColor Green
    Write-Host "To also build the Setup installer, install NSIS:" -ForegroundColor Yellow
    Write-Host "  https://nsis.sourceforge.io/Download  or  choco install nsis -y" -ForegroundColor Yellow
    exit 0
}

# 5. Version already read above; if missing, setup output will be unversioned
if (-not $version) {
    Write-Host "Warning: Could not read version from __init__.py; setup will be AzureMigrationTool_Setup.exe" -ForegroundColor Yellow
}

# 6. Run makensis (pass /DVERSION, /DHAVE_ODBC, /DHAVE_JAVA when applicable)
$makensisArgs = @("installer\AzureMigrationTool.nsi")
if ($version) { $makensisArgs = @("/DVERSION=$version") + $makensisArgs }
if (Test-Path (Join-Path $ScriptDir "odbc\msodbcsql18_x64.msi")) { $makensisArgs = @("/DHAVE_ODBC") + $makensisArgs }
if (Test-Path (Join-Path $ScriptDir "java\bin\java.exe")) { $makensisArgs = @("/DHAVE_JAVA") + $makensisArgs }
if (Test-Path $BcpMsi) { $makensisArgs = @("/DHAVE_BCP") + $makensisArgs }
Write-Host "Building installer with NSIS..."
Push-Location $AppDir
try {
    & $makensis $makensisArgs
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        Write-Host "ERROR: makensis exited with code $exitCode" -ForegroundColor Red
        Pop-Location
        Pause-IfError; exit 1
    }
    $setup = if ($version) { Join-Path $AppDir "dist\AzureMigrationTool_Setup_$version.exe" } else { Join-Path $AppDir "dist\AzureMigrationTool_Setup.exe" }
    Write-Host ""
    Write-Host "Installer created: $setup" -ForegroundColor Green
    if (Test-Path $setup) {
        $size = (Get-Item -LiteralPath $setup).Length
        Write-Host "Size: $([math]::Round($size/1MB, 2)) MB"
    }
} catch {
    Write-Host "ERROR: $_" -ForegroundColor Red
    Pop-Location
    Pause-IfError; exit 1
}
Pop-Location
