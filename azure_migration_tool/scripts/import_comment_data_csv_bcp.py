#!/usr/bin/env python
"""Convert broken COMMENT_DATA CSVs to BCP-friendly pipe files and import to testdb_bcp."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

_APP = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_APP))

from src.utils.database import build_conn_str, pick_sql_driver
from src.utils.bcp_tools import find_bcp_exe

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("csv_bcp")

SERVER = r"gpitd-shir01.us.pressganey.com\i2022"
DEST_DB = "testdb_bcp"
TABLE = "dbo.COMMENT_DATA"
CSV_DIR = Path(r"C:\Users\chauhs\Downloads\COMMENT_DATA\COMMENT_DATA")
WORK = _APP / "bcp_work" / "comment_data_import"
_CREATE_NO_WINDOW = 0x08000000 if sys.platform.startswith("win") else 0

# Fixed header for this export
HEADER = [
    "SURV_ID",
    "COMMENT_ID",
    "CSS_ID",
    "SERVICE",
    "RECDATE",
    "DISDATE",
    "UNIT_ID",
    "SPEC_ID",
    "COMMENT",
    "REACT_CODE",
]


def connect(db: str):
    import pyodbc

    driver = pick_sql_driver(log)
    cs = build_conn_str(SERVER, db, "", driver, "windows", None)
    return pyodbc.connect(cs, timeout=120, autocommit=True)


def ensure_table() -> None:
    with connect(DEST_DB) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.COMMENT_DATA', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.COMMENT_DATA (
                    SURV_ID      BIGINT NULL,
                    COMMENT_ID   BIGINT NULL,
                    CSS_ID       INT NULL,
                    SERVICE      INT NULL,
                    RECDATE      DATE NULL,
                    DISDATE      DATE NULL,
                    UNIT_ID      NVARCHAR(50) NULL,
                    SPEC_ID      INT NULL,
                    COMMENT      NVARCHAR(MAX) NULL,
                    REACT_CODE   INT NULL
                );
            END
            ELSE
            BEGIN
                TRUNCATE TABLE dbo.COMMENT_DATA;
            END
            """
        )
    log.info("[OK] Destination table %s.%s ready (truncated)", DEST_DB, TABLE)


def parse_broken_csv_line(line: str) -> list[str] | None:
    """Split line where COMMENT may contain commas and is unquoted.

    Layout: 8 leading fields, COMMENT (may contain commas), REACT_CODE (last).
    """
    line = line.rstrip("\r\n")
    if not line:
        return None
    parts = line.split(",")
    if len(parts) < 10:
        return None
    if len(parts) == 10:
        return parts
    # COMMENT ate extra commas
    head = parts[:8]
    react = parts[-1]
    comment = ",".join(parts[8:-1])
    return head + [comment, react]


def convert_csv_to_pipe(src: Path, dest: Path) -> int:
    """Write pipe-delimited UTF-16LE-friendly text for bcp -c -t| ; use UTF-8 and -C RAW later.

    We use ASCII/UTF-8 with | terminator; replace | and newlines inside COMMENT.
    """
    count = 0
    with src.open("r", encoding="utf-8", errors="replace", newline="") as fin, dest.open(
        "w", encoding="utf-8", newline="\n"
    ) as fout:
        first = True
        for line in fin:
            if first:
                first = False
                # skip header if present
                if line.upper().startswith("SURV_ID"):
                    continue
            row = parse_broken_csv_line(line)
            if not row:
                continue
            cleaned = []
            for i, val in enumerate(row):
                v = (val or "").replace("\r", " ").replace("\n", " ").replace("|", "/")
                cleaned.append(v)
            fout.write("|".join(cleaned) + "\n")
            count += 1
            if count % 200000 == 0:
                log.info("  … converted %s rows from %s", f"{count:,}", src.name)
    return count


def run_bcp(args: list[str]) -> None:
    log.info("BCP: %s", " ".join(args))
    proc = subprocess.run(args, capture_output=True, text=True, creationflags=_CREATE_NO_WINDOW)
    out = (proc.stdout or "") + (proc.stderr or "")
    for line in out.splitlines():
        if line.strip():
            log.info("  %s", line)
    if proc.returncode != 0:
        raise RuntimeError(f"bcp exit {proc.returncode}")


def count_rows() -> int:
    with connect(DEST_DB) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT_BIG(*) FROM dbo.COMMENT_DATA")
        return int(cur.fetchone()[0])


def main() -> int:
    bcp = find_bcp_exe()
    if not bcp:
        raise SystemExit("bcp.exe not found")
    log.info("bcp=%s", bcp)

    csvs = sorted(CSV_DIR.glob("COMMENT_DATA_*.csv"))
    if len(csvs) < 1:
        raise SystemExit(f"No CSV files in {CSV_DIR}")
    log.info("Found %s CSV file(s)", len(csvs))

    WORK.mkdir(parents=True, exist_ok=True)
    ensure_table()

    pipe_files: list[Path] = []
    total_src = 0
    for csv_path in csvs:
        pipe = WORK / (csv_path.stem + ".bcp.txt")
        log.info("Converting %s → %s", csv_path.name, pipe.name)
        n = convert_csv_to_pipe(csv_path, pipe)
        log.info("[OK] %s rows written", f"{n:,}")
        total_src += n
        pipe_files.append(pipe)

    for pipe in pipe_files:
        err = WORK / (pipe.stem + ".err")
        run_bcp(
            [
                bcp,
                f"{DEST_DB}.{TABLE}",
                "in",
                str(pipe),
                "-S",
                SERVER,
                "-T",
                "-c",
                "-t",
                "|",
                "-r",
                "\n",
                "-b",
                "10000",
                "-e",
                str(err),
                "-F",
                "1",
            ]
        )

    dest_n = count_rows()
    log.info("Source rows (converted): %s", f"{total_src:,}")
    log.info("Destination rows: %s", f"{dest_n:,}")
    if dest_n != total_src:
        log.error("[FAIL] Row mismatch")
        return 2
    log.info(
        "[SUCCESS] Loaded COMMENT_DATA into %s / %s.%s via BCP (pipe-delimited)",
        SERVER,
        DEST_DB,
        TABLE,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as ex:
        log.error("[FAIL] %s", ex)
        raise
