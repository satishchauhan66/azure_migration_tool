# Download Microsoft Azure CLI MSI (Windows x64) into installer\azurecli for bundling.
# Run from azure_migration_tool: .\installer\download_azure_cli.ps1
# Setup runs this MSI during install so az login works for AzCopy Entra uploads.

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -LiteralPath $MyInvocation.MyCommand.Path
$OutDir = Join-Path $ScriptDir "azurecli"
$OutMsi = Join-Path $OutDir "AzureCLI.msi"

if ((Test-Path $OutMsi) -and ((Get-Item $OutMsi).Length -gt 1000000)) {
    Write-Host "Azure CLI MSI already present: $OutMsi"
    Write-Host ("  Size: {0:N1} MB" -f ((Get-Item $OutMsi).Length / 1MB))
    exit 0
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Official redirect to the current Windows x64 MSI
$Url = "https://aka.ms/installazurecliwindowsx64"
$TempMsi = Join-Path $ScriptDir "AzureCLI_download.msi"

Write-Host "Downloading Azure CLI (Windows x64 MSI)..."
try {
    $ProgressPreference = "SilentlyContinue"
    Invoke-WebRequest -Uri $Url -OutFile $TempMsi -UseBasicParsing -TimeoutSec 600
} catch {
    Write-Error "Download failed: $_"
    exit 1
}

if (-not (Test-Path $TempMsi) -or ((Get-Item $TempMsi).Length -lt 1000000)) {
    Write-Error "Downloaded Azure CLI MSI missing or too small."
    exit 1
}

Move-Item -LiteralPath $TempMsi -Destination $OutMsi -Force
Write-Host "Azure CLI MSI saved: $OutMsi"
Write-Host ("  Size: {0:N1} MB" -f ((Get-Item $OutMsi).Length / 1MB))
exit 0
