# Build Azure Migration Tool installer (exe + ODBC + Java + NSIS).
# Run from azure_migration_tool: .\installer\build_installer.ps1
# By default: uses existing dist\AzureMigrationTool.exe and creates versioned setup (no params).
# Optional: -BuildExe to build the exe from code first, then create the installer.
# Optional: -IncludeJava to download and bundle Java 17 for DB2/JDBC (adds ~60MB to installer).

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
$DistExe = Join-Path $AppDir "dist\AzureMigrationTool.exe"
$OdbcMsi = Join-Path $ScriptDir "odbc\msodbcsql18_x64.msi"
$OdbcUrl = "https://go.microsoft.com/fwlink/?linkid=2249006"

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
            Write-Host "ERROR: build_exe.py did not produce $DistExe" -ForegroundColor Red
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
        Write-Host "ERROR: dist\AzureMigrationTool.exe not found. Build it first or run with -BuildExe." -ForegroundColor Red
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
    Write-Host "NSIS (makensis) not found. The installer file cannot be built without it." -ForegroundColor Yellow
    Write-Host "Install NSIS from: https://nsis.sourceforge.io/Download" -ForegroundColor Yellow
    Write-Host "Or run as Administrator: choco install nsis -y" -ForegroundColor Yellow
    Write-Host "Then run this script again to create AzureMigrationTool_Setup.exe" -ForegroundColor Yellow
    Pause-IfError; exit 1
}

# 5. Get version so each build creates a new setup file (AzureMigrationTool_Setup_1.1.6.exe)
$version = $null
$initPath = Join-Path $AppDir "__init__.py"
if (Test-Path $initPath) {
    try {
        $content = Get-Content -LiteralPath $initPath -Raw -Encoding UTF8 -ErrorAction Stop
        if ($content -and ($content -match '__version__\s*=\s*"([^"]+)"')) {
            $version = $Matches[1].Trim()
        }
    } catch {
        Write-Host "Warning: Could not read version from __init__.py; setup will be AzureMigrationTool_Setup.exe" -ForegroundColor Yellow
    }
}
if (-not $version) {
    Write-Host "Warning: Could not read version from __init__.py; setup will be AzureMigrationTool_Setup.exe" -ForegroundColor Yellow
}

# 6. Run makensis (pass /DVERSION, /DHAVE_ODBC, /DHAVE_JAVA when applicable)
$makensisArgs = @("installer\AzureMigrationTool.nsi")
if ($version) { $makensisArgs = @("/DVERSION=$version") + $makensisArgs }
if (Test-Path (Join-Path $ScriptDir "odbc\msodbcsql18_x64.msi")) { $makensisArgs = @("/DHAVE_ODBC") + $makensisArgs }
if (Test-Path (Join-Path $ScriptDir "java\bin\java.exe")) { $makensisArgs = @("/DHAVE_JAVA") + $makensisArgs }
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
