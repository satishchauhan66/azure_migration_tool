# Check whether IBM DB2 CLP (db2.exe) is installed for native EXPORT.
# The thin "IBM Data Server Driver" / JDBC jar alone is NOT enough — you need
# IBM Data Server Client (SQLLIB) which includes db2.exe + db2cmd.exe.
#
# Usage (from repo):
#   .\azure_migration_tool\installer\check_db2_clp.ps1
# Optional: set DB2_HOME or DB2_CLP to your SQLLIB folder before running the app.

$ErrorActionPreference = "Continue"
$candidates = @(
    $env:DB2_CLP,
    $(if ($env:DB2_HOME) { Join-Path $env:DB2_HOME "bin\db2.exe" } else { $null }),
    "C:\Program Files\IBM\SQLLIB\bin\db2.exe",
    "C:\Program Files (x86)\IBM\SQLLIB\bin\db2.exe",
    "C:\IBM\SQLLIB\bin\db2.exe",
    "D:\IBM\SQLLIB\bin\db2.exe"
)

Write-Host "DB2_HOME=$env:DB2_HOME"
Write-Host "DB2_CLP=$env:DB2_CLP"
Write-Host "DB2INSTANCE=$env:DB2INSTANCE"
Write-Host ""

$found = $null
foreach ($p in $candidates) {
    if ($p -and (Test-Path -LiteralPath $p)) {
        $found = (Resolve-Path -LiteralPath $p).Path
        break
    }
}
if (-not $found) {
    $cmd = Get-Command db2.exe -ErrorAction SilentlyContinue
    if ($cmd) { $found = $cmd.Source }
}

if ($found) {
    Write-Host "[OK] db2.exe: $found" -ForegroundColor Green
    $cmdExe = Join-Path (Split-Path $found -Parent) "db2cmd.exe"
    if (Test-Path -LiteralPath $cmdExe) {
        Write-Host "[OK] db2cmd.exe: $cmdExe" -ForegroundColor Green
    } else {
        Write-Host "[WARN] db2cmd.exe missing beside db2.exe — CLP may fail with DB21018E" -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host "App will use native CLP EXPORT for large tables after Validate/Start."
    exit 0
}

Write-Host "[MISSING] db2.exe not found on this jump box." -ForegroundColor Red
Write-Host ""
Write-Host "Install: IBM Data Server Client (includes SQLLIB\bin\db2.exe)"
Write-Host "  - Do NOT install only 'IBM Data Server Driver Package' (no CLP)"
Write-Host "  - Do NOT rely on drivers\db2jcc4.jar alone (JDBC only)"
Write-Host ""
Write-Host "After install:"
Write-Host "  1. Confirm: Test-Path 'C:\Program Files\IBM\SQLLIB\bin\db2.exe'"
Write-Host "  2. Optional: set user env DB2_HOME=C:\Program Files\IBM\SQLLIB"
Write-Host "  3. Restart Azure Migration Tool -> BCP tab -> Validate"
Write-Host ""
Write-Host "Until CLP is installed, large tables use slower client-side JDBC extract."
exit 1
