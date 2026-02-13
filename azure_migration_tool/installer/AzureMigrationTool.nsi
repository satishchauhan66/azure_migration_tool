; Azure Migration Tool - NSIS Installer
; Separate flow: run AFTER building the exe with build_exe.py
; Requires: NSIS 3.x (https://nsis.sourceforge.io/)
; Build: from azure_migration_tool dir run: makensis installer\AzureMigrationTool.nsi

!include "MUI2.nsh"

; ---------------------------------------------------------------------------
; Product and paths (relative to this script's directory)
; ---------------------------------------------------------------------------
!define PRODUCT_NAME       "Azure Migration Tool"
!define PRODUCT_PUBLISHER  "Satish Chauhan"
!define PRODUCT_EXE        "AzureMigrationTool.exe"
; Source: parent of installer = azure_migration_tool, so exe is ..\dist\AzureMigrationTool.exe
!define SOURCE_EXE         "..\dist\${PRODUCT_EXE}"
; Bundled ODBC Driver 18 x64 MSI (optional - install runs it during setup)
!define ODBC_MSI           "odbc\msodbcsql18_x64.msi"
; Bundled Java (Eclipse Temurin 17) for DB2/JDBC - run installer\download_java.ps1 to populate
!define JAVA_DIR           "java"

; ---------------------------------------------------------------------------
; Installer attributes
; ---------------------------------------------------------------------------
Name "${PRODUCT_NAME}"
; VERSION passed by build_installer.bat / build_installer.ps1 (e.g. /DVERSION=1.1.6)
; so each build creates a new file: AzureMigrationTool_Setup_1.1.6.exe
!ifdef VERSION
OutFile "..\dist\AzureMigrationTool_Setup_${VERSION}.exe"
!else
OutFile "..\dist\AzureMigrationTool_Setup.exe"
!endif
InstallDir "$PROGRAMFILES64\${PRODUCT_NAME}"
InstallDirRegKey HKLM "Software\${PRODUCT_NAME}" "InstallPath"
RequestExecutionLevel admin
Unicode True

; ---------------------------------------------------------------------------
; UI
; ---------------------------------------------------------------------------
!define MUI_ABORTWARNING
; !define MUI_ICON "path\to\icon.ico"   ; optional
; !define MUI_UNICON "path\to\unicon.ico"
; !define MUI_HEADERIMAGE  ; needs MUI_HEADERIMAGE_BITMAP
!define MUI_WELCOMEPAGE_TITLE "Welcome to ${PRODUCT_NAME} Setup"
!define MUI_WELCOMEPAGE_TEXT "This will install ${PRODUCT_NAME} and required components.$\r$\nDeveloped by Satish Chauhan.$\r$\n$\r$\nIncluded: application, ODBC Driver 18 for SQL Server, and Java 17 for DB2/JDBC (if bundled).$\r$\n$\r$\nClick Next to continue."
!define MUI_FINISHPAGE_TITLE "Completing ${PRODUCT_NAME} Setup"
!define MUI_FINISHPAGE_TEXT "${PRODUCT_NAME} has been installed.$\r$\nDeveloped by Satish Chauhan.$\r$\n$\r$\nIncluded: ODBC Driver 18 for SQL Server, and Java 17 for DB2/JDBC (if bundled).$\r$\nThe app exe already contains the DB2 JDBC driver (db2jcc4.jar)."
!define MUI_FINISHPAGE_RUN "$INSTDIR\${PRODUCT_EXE}"
!define MUI_FINISHPAGE_RUN_TEXT "Run ${PRODUCT_NAME} now"

!insertmacro MUI_PAGE_WELCOME
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
  
  ; Main exe (embedded at build time; if missing, makensis would have failed)
  File "${SOURCE_EXE}"
  
  ; ODBC and Java: include only when built with /DHAVE_ODBC and /DHAVE_JAVA (build_installer.ps1 passes these when files exist)
  !ifdef HAVE_ODBC
  SetOutPath "$INSTDIR\odbc"
  File "${ODBC_MSI}"
  DetailPrint "Installing ODBC Driver 18 for SQL Server..."
  ExecWait '"$SYSDIR\msiexec.exe" /i "$INSTDIR\odbc\msodbcsql18_x64.msi" /quiet IACCEPTMSODBCSQLLICENSETERMS=YES'
  SetOutPath "$INSTDIR"
  !endif
  
  !ifdef HAVE_JAVA
  DetailPrint "Installing bundled Java (for DB2/JDBC)..."
  SetOutPath "$INSTDIR"
  File /r "${JAVA_DIR}"
  !endif
  
  ; Store install path for uninstall and add/remove programs
  WriteRegStr HKLM "Software\${PRODUCT_NAME}" "InstallPath" "$INSTDIR"
  WriteUninstaller "$INSTDIR\Uninstall.exe"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "DisplayName" "${PRODUCT_NAME}"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "UninstallString" "$INSTDIR\Uninstall.exe"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}" "Publisher" "${PRODUCT_PUBLISHER}"
  
  ; Start Menu shortcut
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
  
  ; Start Menu
  RMDir /r "$SMPROGRAMS\${PRODUCT_NAME}"
  
  ; Registry
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
  DeleteRegKey HKLM "Software\${PRODUCT_NAME}"
SectionEnd
