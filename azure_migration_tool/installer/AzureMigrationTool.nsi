; Azure Migration Tool - NSIS Installer
; Developed by 66Degrees
; Separate flow: run AFTER building the exe with build_exe.py
; Requires: NSIS 3.x (https://nsis.sourceforge.io/)
; Build: from azure_migration_tool dir run: makensis installer\AzureMigrationTool.nsi
;
; Per-user vs all-users (similar to VS Code): MultiUser page + /CurrentUser /AllUsers on command line.

; ---------------------------------------------------------------------------
; Product (needed before MultiUser registry defines)
; ---------------------------------------------------------------------------
!define PRODUCT_NAME       "Azure Migration Tool"
!define PRODUCT_PUBLISHER  "Satish Chauhan"

; ---------------------------------------------------------------------------
; Multi-user (must be before !include MultiUser.nsh; MULTIUSER_MUI pulls in MUI2)
; ---------------------------------------------------------------------------
!define MULTIUSER_EXECUTIONLEVEL Highest
!define MULTIUSER_MUI
!define MULTIUSER_INSTALLMODE_COMMANDLINE
; Default selection when the account can install for everyone (admin): current user (change if you prefer).
!define MULTIUSER_INSTALLMODE_DEFAULT_CURRENTUSER
!define MULTIUSER_USE_PROGRAMFILES64
!define MULTIUSER_INSTALLMODE_INSTDIR "${PRODUCT_NAME}"

; Remember install folder for upgrades (hive matches mode at end of install)
!define MULTIUSER_INSTALLMODE_INSTDIR_REGISTRY_KEY "Software\${PRODUCT_NAME}"
!define MULTIUSER_INSTALLMODE_INSTDIR_REGISTRY_VALUENAME "InstallPath"

!include "MultiUser.nsh"

; ---------------------------------------------------------------------------
; Paths (relative to this script's directory)
; ---------------------------------------------------------------------------
!define PRODUCT_EXE        "AzureMigrationTool.exe"
; Source: versioned exe when /DVERSION passed (build_installer.ps1), else unversioned
!ifdef VERSION
!define SOURCE_EXE         "..\dist\AzureMigrationTool_${VERSION}.exe"
!else
!define SOURCE_EXE         "..\dist\AzureMigrationTool.exe"
!endif
; Bundled ODBC Driver 18 x64 MSI (optional - install runs it during setup)
!define ODBC_MSI           "odbc\msodbcsql18_x64.msi"
; Bundled Java (Eclipse Temurin 17) for DB2/JDBC - run installer\download_java.ps1 to populate
!define JAVA_DIR           "java"

; ---------------------------------------------------------------------------
; Installer attributes
; ---------------------------------------------------------------------------
Name "${PRODUCT_NAME}"
; VERSION passed by build_installer.bat / build_installer.ps1 (e.g. /DVERSION=1.2.0)
!ifdef VERSION
OutFile "..\dist\AzureMigrationTool_Setup_${VERSION}.exe"
!else
OutFile "..\dist\AzureMigrationTool_Setup.exe"
!endif
; Placeholder; MultiUser + .onInit set real $INSTDIR
InstallDir "$PROGRAMFILES64\${PRODUCT_NAME}"
Unicode True

; ---------------------------------------------------------------------------
; UI
; ---------------------------------------------------------------------------
!define MUI_ABORTWARNING
!define MUI_BRANDINGTEXT "Developed by 66Degrees"
!define MUI_WELCOMEPAGE_TITLE "Welcome to ${PRODUCT_NAME} Setup"
!define MUI_WELCOMEPAGE_TEXT "This will install ${PRODUCT_NAME} and optional components.$\r$\n$\r$\nYou can install for the current user only, or for all users (requires administrator).$\r$\n$\r$\nIncluded: application; ODBC Driver 18 (all-users installs only); Java 17 for DB2/JDBC if bundled.$\r$\nThe app exe already contains the DB2 JDBC driver (db2jcc4.jar).$\r$\n$\r$\nClick Next to continue."
!define MUI_FINISHPAGE_TITLE "Completing ${PRODUCT_NAME} Setup"
!define MUI_FINISHPAGE_TEXT "${PRODUCT_NAME} has been installed.$\r$\n$\r$\nPer-user installs do not run the ODBC MSI automatically; install Microsoft ODBC Driver 18 for SQL Server separately if needed.$\r$\n$\r$\nDeveloped by 66Degrees."
!define MUI_FINISHPAGE_RUN "$INSTDIR\${PRODUCT_EXE}"
!define MUI_FINISHPAGE_RUN_TEXT "Run ${PRODUCT_NAME} now"

Function .onInit
  !insertmacro MULTIUSER_INIT
FunctionEnd

Function un.onInit
  !insertmacro MULTIUSER_INIT
FunctionEnd

!insertmacro MUI_PAGE_WELCOME
!insertmacro MULTIUSER_PAGE_INSTALLMODE
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

!insertmacro MUI_LANGUAGE "English"

; ---------------------------------------------------------------------------
; Installer sections
; ---------------------------------------------------------------------------
Section "MainSection" SEC01
  SetOutPath "$INSTDIR"

  ; Main exe (versioned in dist; install as AzureMigrationTool.exe for shortcuts)
  File /oname=${PRODUCT_EXE} "${SOURCE_EXE}"

  ; ODBC: machine-wide MSI only for all-users install (needs admin context)
  !ifdef HAVE_ODBC
  ${if} $MultiUser.InstallMode == "AllUsers"
    SetOutPath "$INSTDIR\odbc"
    File "${ODBC_MSI}"
    DetailPrint "Installing ODBC Driver 18 for SQL Server (all users)..."
    ExecWait '"$SYSDIR\msiexec.exe" /i "$INSTDIR\odbc\msodbcsql18_x64.msi" /quiet IACCEPTMSODBCSQLLICENSETERMS=YES'
    SetOutPath "$INSTDIR"
  ${else}
    DetailPrint "Skipping bundled ODBC MSI (per-user install). Install ODBC Driver 18 separately if required."
  ${endif}
  !endif

  !ifdef HAVE_JAVA
  DetailPrint "Installing bundled Java (for DB2/JDBC)..."
  SetOutPath "$INSTDIR"
  File /r "${JAVA_DIR}"
  !endif

  ; Registry + uninstall (hive follows install mode)
  ${if} $MultiUser.InstallMode == "AllUsers"
    WriteRegStr HKLM "Software\${PRODUCT_NAME}" "InstallPath" "$INSTDIR"
    WriteUninstaller "$INSTDIR\Uninstall.exe"
    WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "DisplayName" "${PRODUCT_NAME}"
    WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "UninstallString" "$INSTDIR\Uninstall.exe"
    WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "Publisher" "${PRODUCT_PUBLISHER}"
  ${else}
    WriteRegStr HKCU "Software\${PRODUCT_NAME}" "InstallPath" "$INSTDIR"
    WriteUninstaller "$INSTDIR\Uninstall.exe"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "DisplayName" "${PRODUCT_NAME}"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "UninstallString" "$INSTDIR\Uninstall.exe"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "Publisher" "${PRODUCT_PUBLISHER}"
  ${endif}

  ; Start Menu: SetShellVarContext was set by MultiUser (all vs current)
  CreateDirectory "$SMPROGRAMS\${PRODUCT_NAME}"
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\${PRODUCT_NAME}.lnk" "$INSTDIR\${PRODUCT_EXE}" "" "$INSTDIR\${PRODUCT_EXE}" 0
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\Uninstall.lnk" "$INSTDIR\Uninstall.exe" "" "$INSTDIR\Uninstall.exe" 0
SectionEnd

; ---------------------------------------------------------------------------
; Uninstaller
; ---------------------------------------------------------------------------
Section "Uninstall"
  Delete "$INSTDIR\${PRODUCT_EXE}"
  Delete "$INSTDIR\Uninstall.exe"
  Delete "$INSTDIR\odbc\msodbcsql18_x64.msi"
  RMDir "$INSTDIR\odbc"
  RMDir /r "$INSTDIR\java"
  RMDir "$INSTDIR"

  RMDir /r "$SMPROGRAMS\${PRODUCT_NAME}"

  ${if} $MultiUser.InstallMode == "AllUsers"
    DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
    DeleteRegKey HKLM "Software\${PRODUCT_NAME}"
  ${else}
    DeleteRegKey HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
    DeleteRegKey HKCU "Software\${PRODUCT_NAME}"
  ${endif}
SectionEnd
