@echo off
setlocal EnableDelayedExpansion
REM Build NSIS installer for Azure Migration Tool.
REM Run from azure_migration_tool folder: installer\build_installer.bat
REM Prerequisites: 1) python build_exe.py  2) NSIS on PATH (makensis)

set SCRIPT_DIR=%~dp0
set APP_DIR=%SCRIPT_DIR%..
set DIST_EXE=%APP_DIR%\dist\AzureMigrationTool.exe

if not exist "%DIST_EXE%" (
    echo ERROR: Built exe not found: %DIST_EXE%
    echo Run first: python build_exe.py
    exit /b 1
)

where makensis >nul 2>nul
if errorlevel 1 (
    echo ERROR: makensis not found. Install NSIS and add it to PATH.
    echo See: https://nsis.sourceforge.io/
    exit /b 1
)

cd /d "%APP_DIR%"
set "MAKENSIS_OPTS="
if exist "installer\odbc\msodbcsql18_x64.msi" set "MAKENSIS_OPTS=/DHAVE_ODBC %MAKENSIS_OPTS%"
if exist "installer\java\bin\java.exe" set "MAKENSIS_OPTS=/DHAVE_JAVA %MAKENSIS_OPTS%"
REM Get version so setup output is versioned (new file per build)
for /f "delims=" %%v in ('python -c "import sys; sys.path.insert(0, '.'); from azure_migration_tool import __version__; print(__version__)" 2^>nul') do set "VER=%%v"
if defined VER set "MAKENSIS_OPTS=/DVERSION=!VER! !MAKENSIS_OPTS!"
makensis %MAKENSIS_OPTS% "installer\AzureMigrationTool.nsi"
if errorlevel 1 (
    echo Installer build failed.
    exit /b 1
)

echo.
if defined VER (
    echo Installer created: %APP_DIR%\dist\AzureMigrationTool_Setup_!VER!.exe
) else (
    echo Installer created: %APP_DIR%\dist\AzureMigrationTool_Setup.exe
)
exit /b 0
