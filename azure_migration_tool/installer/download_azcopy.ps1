# Download Microsoft AzCopy v10 (Windows amd64) into installer\azcopy for bundling.
# Run from azure_migration_tool: .\installer\download_azcopy.ps1
# The app looks for tools\azcopy\azcopy.exe next to the installed exe (or in PyInstaller bundle).

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -LiteralPath $MyInvocation.MyCommand.Path
$AzCopyDir = Join-Path $ScriptDir "azcopy"
$AzCopyExe = Join-Path $AzCopyDir "azcopy.exe"

if (Test-Path $AzCopyExe) {
    Write-Host "AzCopy already present: $AzCopyExe"
    & $AzCopyExe --version 2>&1 | Select-Object -First 1
    exit 0
}

$Url = "https://aka.ms/downloadazcopy-v10-windows"
$TempZip = Join-Path $ScriptDir "azcopy_temp.zip"

Write-Host "Downloading AzCopy v10 (Windows)..."
try {
    $ProgressPreference = "SilentlyContinue"
    Invoke-WebRequest -Uri $Url -OutFile $TempZip -UseBasicParsing -TimeoutSec 300
} catch {
    Write-Error "Download failed: $_"
    exit 1
}

if (-not (Test-Path $TempZip) -or (Get-Item $TempZip).Length -lt 500000) {
    Write-Error "Downloaded file missing or too small."
    exit 1
}

$ExtractRoot = Join-Path $ScriptDir "azcopy_extract"
if (Test-Path $ExtractRoot) {
    Remove-Item -Recurse -Force $ExtractRoot
}
New-Item -ItemType Directory -Force -Path $ExtractRoot | Out-Null

Write-Host "Extracting..."
Expand-Archive -LiteralPath $TempZip -DestinationPath $ExtractRoot -Force
Remove-Item $TempZip -Force

$found = Get-ChildItem -Path $ExtractRoot -Recurse -Filter "azcopy.exe" -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $found) {
    Write-Error "azcopy.exe not found inside the downloaded archive."
    exit 1
}

if (Test-Path $AzCopyDir) {
    Remove-Item -Recurse -Force $AzCopyDir
}
New-Item -ItemType Directory -Force -Path $AzCopyDir | Out-Null
Copy-Item -LiteralPath $found.FullName -Destination $AzCopyExe -Force
Remove-Item -Recurse -Force $ExtractRoot

if (-not (Test-Path $AzCopyExe)) {
    Write-Error "Failed to install azcopy.exe to $AzCopyExe"
    exit 1
}

# Copy for PyInstaller embed (build_exe.py bundles tools\azcopy)
$AppTools = Join-Path (Split-Path $ScriptDir -Parent) "tools\azcopy"
New-Item -ItemType Directory -Force -Path $AppTools | Out-Null
Copy-Item -LiteralPath $AzCopyExe -Destination (Join-Path $AppTools "azcopy.exe") -Force

Write-Host "AzCopy installed to: $AzCopyDir"
& $AzCopyExe --version 2>&1 | Select-Object -First 1
exit 0
