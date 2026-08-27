DB2 JDBC driver (bundled with exe / setup)
========================================

Place or build-fetch:
  db2jcc4.jar

Build (one of):
  python build_exe.py
    -> fetches jar at build time if missing; build FAILS if jar cannot be embedded
  .\installer\download_db2_jdbc.ps1
    -> fetches jar into this folder before packaging
  .\installer\build_installer.ps1 -BuildExe
    -> downloads jar + ODBC/BCP MSIs, then builds exe

Git: db2jcc4.jar is not committed (.gitignore). The build scripts always fetch or
verify the jar before compiling — a missing driver will stop the build, not ship a broken exe.

Runtime behavior:
  The application does NOT download this driver on the target PC.
  It only loads the jar already embedded in the exe (or next to it under drivers/).

Optional override:
  DB2_JDBC_DRIVER_PATH = full path to db2jcc4.jar
