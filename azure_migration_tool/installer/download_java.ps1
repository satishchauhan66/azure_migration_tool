# Download Eclipse Temurin 17 (Java) and extract to installer\java for bundling.
# Run from azure_migration_tool: .\installer\download_java.ps1
# The app expects java\bin\java.exe next to the exe (installer puts it in $INSTDIR\java).

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -LiteralPath $MyInvocation.MyCommand.Path
$JavaDir = Join-Path $ScriptDir "java"
$JavaExe = Join-Path $JavaDir "bin\java.exe"

if (Test-Path $JavaExe) {
    Write-Host "Java already present: $JavaDir"
    & "$JavaExe" -version 2>&1
    exit 0
}

$arch = "x64"
$ApiUrl = "https://api.adoptium.net/v3/binary/latest/17/ga/windows/$arch/jdk/hotspot/normal/eclipse"
$TempZip = Join-Path $ScriptDir "java_temp.zip"

Write-Host "Downloading Eclipse Temurin 17 (Windows x64)..."
try {
    $ProgressPreference = "SilentlyContinue"
    Invoke-WebRequest -Uri $ApiUrl -OutFile $TempZip -UseBasicParsing
} catch {
    Write-Error "Download failed: $_"
    exit 1
}

if (-not (Test-Path $TempZip) -or (Get-Item $TempZip).Length -lt 1MB) {
    Write-Error "Downloaded file missing or too small."
    exit 1
}

Write-Host "Extracting to installer\java..."
if (Test-Path $JavaDir) {
    Remove-Item -Recurse -Force $JavaDir
}
Expand-Archive -LiteralPath $TempZip -DestinationPath $ScriptDir -Force
Remove-Item $TempZip -Force

$extracted = Get-ChildItem -Path $ScriptDir -Directory | Where-Object { (Test-Path (Join-Path $_.FullName "bin\java.exe")) } | Select-Object -First 1
if (-not $extracted) {
    $extracted = Get-ChildItem -Path $ScriptDir -Directory | Where-Object { $_.Name -match "jdk" } | Select-Object -First 1
}
if (-not $extracted) {
    Write-Error "Could not find extracted JDK folder (expected bin\java.exe inside)."
    exit 1
}
if ($extracted.Name -ne "java") {
    if (Test-Path $JavaDir) { Remove-Item -Recurse -Force $JavaDir }
    Copy-Item -LiteralPath $extracted.FullName -Destination $JavaDir -Recurse -Force
    Remove-Item -LiteralPath $extracted.FullName -Recurse -Force
}

$JavaDir = Join-Path $ScriptDir "java"
$JavaExe = Join-Path $JavaDir "bin\java.exe"
if (-not (Test-Path $JavaExe)) {
    Write-Error "java.exe not found at $JavaExe"
    exit 1
}
Write-Host "Java installed to: $JavaDir"
& "$JavaExe" -version 2>&1
exit 0
