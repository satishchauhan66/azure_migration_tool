# Author: Satish Chauhan
# Proprietary - 66degrees. All rights reserved.
"""Data migration functionality."""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pyodbc

from ..utils.database import build_conn_str, pick_sql_driver, connect_to_database, resolve_password
from ..utils.logging import setup_logger
from ..utils.paths import qident, short_slug, utc_ts_compact


def parse_csv_list(s: Optional[str]) -> List[str]:
    """Parse comma-separated list"""
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


# ----------------------------
# METADATA
# ----------------------------
def fetch_tables(cur) -> List[Tuple[str, str]]:
    """Fetch all user tables"""
    cur.execute(
        """
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name;
        """
    )
    return [(r.schema_name, r.table_name) for r in cur.fetchall()]


def fetch_columns_ordered(cur, schema: str, table: str) -> List[Tuple[str, bool]]:
    """Fetch column names and identity flag"""
    cur.execute(
        """
        SELECT c.name AS column_name,
               c.is_identity
        FROM sys.columns c
        WHERE c.object_id = OBJECT_ID(?, 'U')
        ORDER BY c.column_id;
        """,
        f"{schema}.{table}",
    )
    return [(r.column_name, bool(r.is_identity)) for r in cur.fetchall()]


def count_rows(cur, schema: str, table: str) -> int:
    """Count rows in table"""
    cur.execute(f"SELECT COUNT_BIG(1) FROM {qident(schema)}.{qident(table)};")
    return int(cur.fetchone()[0])


# ----------------------------
# DATA COPY
# ----------------------------
def set_identity_insert(cur, schema: str, table: str, enabled: bool):
    """Enable/disable IDENTITY_INSERT"""
    onoff = "ON" if enabled else "OFF"
    cur.execute(f"SET IDENTITY_INSERT {qident(schema)}.{qident(table)} {onoff};")


def truncate_table(cur, schema: str, table: str, conn=None, logger=None):
    """
    Truncate table. Falls back to DELETE if TRUNCATE fails due to FK constraints.
    TRUNCATE is blocked by FKs even with NOCHECK - only DROP FK allows TRUNCATE.
    """
    try:
        cur.execute(f"TRUNCATE TABLE {qident(schema)}.{qident(table)};")
    except Exception as e:
        error_str = str(e)
        # Error 4712 = Cannot truncate table because it is being referenced by a FOREIGN KEY constraint
        if "4712" in error_str or "FOREIGN KEY" in error_str:
            if logger:
                logger.warning(f"TRUNCATE blocked by FK constraint on {schema}.{table}, using DELETE instead")
            # Rollback the failed truncate attempt
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            # Use DELETE as fallback
            cur.execute(f"DELETE FROM {qident(schema)}.{qident(table)};")
        else:
            raise


def delete_table(cur, schema: str, table: str):
    """Delete all rows from table"""
    cur.execute(f"DELETE FROM {qident(schema)}.{qident(table)};")


def build_insert_statement(schema: str, table: str, col_names: List[str]) -> str:
    """Build INSERT statement with placeholders"""
    cols = ", ".join(qident(c) for c in col_names)
    placeholders = ", ".join(["?"] * len(col_names))
    return f"INSERT INTO {qident(schema)}.{qident(table)} ({cols}) VALUES ({placeholders});"


def fetch_source_rows_in_batches(src_cur, schema: str, table: str, col_names: List[str], batch_size: int, logger: Optional[logging.Logger] = None):
    """Generator: fetch rows in batches with memory-efficient streaming"""
    cols = ", ".join(qident(c) for c in col_names)
    sql = f"SELECT {cols} FROM {qident(schema)}.{qident(table)};"
    
    # Use arraysize to control memory usage
    src_cur.arraysize = min(batch_size, 1000)  # Limit arraysize for memory efficiency
    src_cur.execute(sql)

    while True:
        try:
            rows = src_cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows
        except MemoryError:
            # If MemoryError occurs, try with smaller batch
            if batch_size > 100:
                if logger:
                    logger.warning("MemoryError during fetch, reducing batch size from %d to %d", batch_size, batch_size // 2)
                batch_size = batch_size // 2
                src_cur.arraysize = min(batch_size, 1000)
                continue
            else:
                raise


def migrate_one_table(
    logger: logging.Logger,
    src_conn,
    dest_conn,
    schema: str,
    table: str,
    batch_size: int,
    truncate_dest: bool,
    delete_dest: bool,
    continue_on_error: bool,
    dry_run: bool,
) -> dict:
    """Migrate a single table"""
    t0 = time.time()
    table_fqn = f"{schema}.{table}"
    result = {
        "table": table_fqn,
        "status": "started",
        "src_count": None,
        "dest_count_before": None,
        "dest_count_after": None,
        "inserted": 0,
        "batches": 0,
        "batches_retried": 0,
        "identity_insert": False,
        "duration_seconds": None,
        "error": None,
    }

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    try:
        cols = fetch_columns_ordered(src_cur, schema, table)
        col_names = [c for (c, _) in cols]
        has_identity = any(is_id for (_, is_id) in cols)
        result["identity_insert"] = has_identity

        result["src_count"] = count_rows(src_cur, schema, table)
        result["dest_count_before"] = count_rows(dest_cur, schema, table)

        logger.info(
            "Table: %s | src=%s | dest(before)=%s | cols=%d | identity=%s",
            table_fqn,
            result["src_count"],
            result["dest_count_before"],
            len(col_names),
            has_identity,
        )

        if dry_run:
            logger.info("DRY RUN: skipping data move for %s", table_fqn)
            result["status"] = "dry_run"
            result["duration_seconds"] = round(time.time() - t0, 3)
            return result

        # Prep dest
        if truncate_dest and result["dest_count_before"] > 0:
            logger.info("Truncating destination table: %s", table_fqn)
            truncate_table(dest_cur, schema, table, conn=dest_conn, logger=logger)
            dest_conn.commit()
        elif delete_dest and result["dest_count_before"] > 0:
            logger.info("Deleting destination rows: %s", table_fqn)
            delete_table(dest_cur, schema, table)
            dest_conn.commit()

        if has_identity:
            logger.info("IDENTITY_INSERT ON: %s", table_fqn)
            set_identity_insert(dest_cur, schema, table, True)
            dest_conn.commit()

        insert_sql = build_insert_statement(schema, table, col_names)
        
        # Try fast_executemany first, but disable if MemoryError occurs
        use_fast_executemany = True
        dest_cur.fast_executemany = use_fast_executemany

        inserted_total = 0
        batch_no = 0
        current_batch_size = batch_size

        for rows in fetch_source_rows_in_batches(src_cur, schema, table, col_names, current_batch_size, logger):
            batch_no += 1
            retry_count = 0
            max_retries = 3
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    dest_cur.executemany(insert_sql, rows)
                    dest_conn.commit()
                    inserted_total += len(rows)
                    success = True

                    result["batches"] = batch_no
                    result["inserted"] = inserted_total

                    if batch_no % 25 == 0:
                        logger.info("Progress %s: batches=%d inserted=%d", table_fqn, batch_no, inserted_total)

                except MemoryError as mem_err:
                    retry_count += 1
                    result["batches_retried"] += 1
                    try:
                        dest_conn.rollback()
                    except Exception:
                        pass
                    
                    if retry_count < max_retries:
                        # Reduce batch size and disable fast_executemany for memory efficiency
                        if use_fast_executemany:
                            logger.warning(
                                "MemoryError in batch %d for %s (attempt %d/%d): Disabling fast_executemany",
                                batch_no, table_fqn, retry_count, max_retries
                            )
                            use_fast_executemany = False
                            dest_cur.fast_executemany = False
                        
                        # Try with smaller row set if batch is large
                        if len(rows) > 100:
                            logger.warning(
                                "MemoryError: Splitting batch %d (%d rows) into smaller chunks (attempt %d/%d)",
                                batch_no, len(rows), retry_count, max_retries
                            )
                            # Split rows into smaller chunks
                            chunk_size = max(len(rows) // 2, 50)  # At least 50 rows per chunk
                            chunk_inserted = 0
                            for chunk_start in range(0, len(rows), chunk_size):
                                chunk = rows[chunk_start:chunk_start + chunk_size]
                                try:
                                    dest_cur.executemany(insert_sql, chunk)
                                    dest_conn.commit()
                                    chunk_inserted += len(chunk)
                                except Exception as chunk_err:
                                    logger.error("Chunk insert failed: %s", chunk_err)
                                    if not continue_on_error:
                                        raise
                            inserted_total += chunk_inserted
                            result["inserted"] = inserted_total
                            success = True
                            break
                        else:
                            # Wait a bit before retry with exponential backoff
                            wait_time = 0.5 * (2 ** (retry_count - 1))
                            logger.warning(
                                "MemoryError in batch %d for %s (attempt %d/%d): Retrying in %.2fs...",
                                batch_no, table_fqn, retry_count, max_retries, wait_time
                            )
                            time.sleep(wait_time)
                    else:
                        msg = f"Batch {batch_no} failed for {table_fqn} after {max_retries} retries: MemoryError: {mem_err}"
                        logger.error(msg)
                        if not continue_on_error:
                            raise
                        result["error"] = msg
                        break
                        
                except Exception as ex_batch:
                    retry_count += 1
                    result["batches_retried"] += 1
                    try:
                        dest_conn.rollback()
                    except Exception:
                        pass
                    
                    if retry_count < max_retries:
                        # Wait before retry with exponential backoff
                        wait_time = 0.5 * (2 ** (retry_count - 1))
                        logger.warning(
                            "Batch %d failed for %s (attempt %d/%d): %s. Retrying in %.2fs...",
                            batch_no, table_fqn, retry_count, max_retries, type(ex_batch).__name__, wait_time
                        )
                        time.sleep(wait_time)
                    else:
                        msg = f"Batch {batch_no} failed for {table_fqn} after {max_retries} retries: {type(ex_batch).__name__}: {ex_batch}"
                        logger.error(msg)
                        if not continue_on_error:
                            raise
                        result["error"] = msg
                        break
            
            if not success and not continue_on_error:
                break

        if has_identity:
            logger.info("IDENTITY_INSERT OFF: %s", table_fqn)
            set_identity_insert(dest_cur, schema, table, False)
            dest_conn.commit()

        result["dest_count_after"] = count_rows(dest_cur, schema, table)

        logger.info(
            "Done %s | inserted=%d | dest(after)=%s | duration=%.3fs",
            table_fqn,
            result["inserted"],
            result["dest_count_after"],
            time.time() - t0,
        )

        result["status"] = "success"
        result["duration_seconds"] = round(time.time() - t0, 3)
        result["rows_copied"] = result["inserted"]  # Alias for compatibility
        return result

    except Exception as ex:
        try:
            if result["identity_insert"]:
                set_identity_insert(dest_cur, schema, table, False)
                dest_conn.commit()
        except Exception:
            pass

        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Table failed: %s | %s", table_fqn, msg)
        result["status"] = "failed"
        result["error"] = msg
        result["duration_seconds"] = round(time.time() - t0, 3)
        return result


def run_migration(cfg: dict):
    """Run data migration with provided configuration"""
    if cfg["truncate_dest"] and cfg["delete_dest"]:
        raise ValueError("Choose only one: truncate_dest OR delete_dest")

    run_id = utc_ts_compact()

    # SHORT paths to avoid Windows MAX_PATH issues
    src_tag = short_slug(f"{cfg['src_server']}__{cfg['src_db']}")
    dest_tag = short_slug(f"{cfg['dest_server']}__{cfg['dest_db']}")
    root = Path("migrations") / run_id / f"{src_tag}__to__{dest_tag}"

    logs_dir = root / "logs"
    meta_dir = root / "meta"
    logs_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / f"migrate_{run_id}.log"
    logger = setup_logger(log_file, "data_migration")

    logger.info("Starting data migration run: %s", run_id)
    logger.info("Source: %s | %s | auth=%s | user=%s", cfg["src_server"], cfg["src_db"], cfg["src_auth"], cfg["src_user"])
    logger.info("Dest:   %s | %s | auth=%s | user=%s", cfg["dest_server"], cfg["dest_db"], cfg["dest_auth"], cfg["dest_user"])
    logger.info("Batch size: %d", cfg["batch_size"])
    logger.info("truncate_dest=%s delete_dest=%s continue_on_error=%s dry_run=%s",
                cfg["truncate_dest"], cfg["delete_dest"], cfg["continue_on_error"], cfg["dry_run"])
    logger.info("Python exe: %s", sys.executable)
    logger.info("Run folder: %s", str(root.resolve()))
    logger.info("Log file: %s", str(log_file.resolve()))

    include_tables = set(parse_csv_list(cfg["tables"]))
    exclude_tables = set(parse_csv_list(cfg["exclude"]))

    report = {
        "run_id": run_id,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "ended_utc": None,
        "duration_seconds": None,
        "source": {"server": cfg["src_server"], "db": cfg["src_db"], "auth": cfg["src_auth"], "user": cfg["src_user"]},
        "destination": {"server": cfg["dest_server"], "db": cfg["dest_db"], "auth": cfg["dest_auth"], "user": cfg["dest_user"]},
        "batch_size": cfg["batch_size"],
        "truncate_dest": cfg["truncate_dest"],
        "delete_dest": cfg["delete_dest"],
        "continue_on_error": cfg["continue_on_error"],
        "dry_run": cfg["dry_run"],
        "tables_requested": sorted(list(include_tables)) if include_tables else None,
        "tables_excluded": sorted(list(exclude_tables)) if exclude_tables else None,
        "status": "started",
        "tables": [],
        "errors": [],
        "log_file": str(log_file),
        "effective_config": {k: ("***" if "password" in k and cfg[k] else cfg[k]) for k in cfg},
    }

    started = time.time()

    try:
        driver = pick_sql_driver(logger)

        logger.info("Connecting to SOURCE (auth=%s)...", cfg["src_auth"])
        # Use connect_to_database which handles MSAL token caching automatically
        with connect_to_database(
            server=cfg["src_server"],
            db=cfg["src_db"],
            user=cfg["src_user"],
            driver=driver,
            auth=cfg["src_auth"],
            password=cfg["src_password"],
            timeout=30,
            logger=logger,
        ) as src_conn:
            src_conn.timeout = 0

            logger.info("Connecting to DESTINATION (auth=%s)...", cfg["dest_auth"])
            # Use connect_to_database which handles MSAL token caching automatically
            with connect_to_database(
                server=cfg["dest_server"],
                db=cfg["dest_db"],
                user=cfg["dest_user"],
                driver=driver,
                auth=cfg["dest_auth"],
                password=cfg["dest_password"],
                timeout=30,
                logger=logger,
            ) as dest_conn:
                dest_conn.timeout = 0

                s_cur = src_conn.cursor()
                d_cur = dest_conn.cursor()
                s_cur.execute("SELECT DB_NAME(), SUSER_SNAME(), GETDATE();")
                d_cur.execute("SELECT DB_NAME(), SUSER_SNAME(), GETDATE();")
                logger.info("Source connected: DB=%s Login=%s Time=%s", *s_cur.fetchone())
                logger.info("Dest connected:   DB=%s Login=%s Time=%s", *d_cur.fetchone())

                all_tables = fetch_tables(s_cur)
                logger.info("Source tables found: %d", len(all_tables))

                final_tables = []
                for schema, table in all_tables:
                    fqn = f"{schema}.{table}"
                    if include_tables and fqn not in include_tables:
                        continue
                    if fqn in exclude_tables:
                        continue
                    final_tables.append((schema, table))

                logger.info("Tables selected for migration: %d", len(final_tables))

                for i, (schema, table) in enumerate(final_tables, start=1):
                    table_fqn = f"{schema}.{table}"
                    logger.info("=== [%d/%d] Migrating %s ===", i, len(final_tables), table_fqn)

                    res = migrate_one_table(
                        logger=logger,
                        src_conn=src_conn,
                        dest_conn=dest_conn,
                        schema=schema,
                        table=table,
                        batch_size=cfg["batch_size"],
                        truncate_dest=cfg["truncate_dest"],
                        delete_dest=cfg["delete_dest"],
                        continue_on_error=cfg["continue_on_error"],
                        dry_run=cfg["dry_run"],
                    )
                    report["tables"].append(res)

                    if res["status"] == "failed":
                        report["errors"].append(f"{table_fqn}: {res.get('error')}")
                        if not cfg["continue_on_error"]:
                            raise RuntimeError(f"Stopping due to failure in {table_fqn}")

        report["status"] = "success" if not report["errors"] else "completed_with_errors"

    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Migration failed: %s", msg)
        report["status"] = "failed"
        report["errors"].append(msg)

    finally:
        report["ended_utc"] = datetime.now(timezone.utc).isoformat()
        report["duration_seconds"] = round(time.time() - started, 3)

        report_file = meta_dir / "migration_report.json"
        report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")

        logger.info("Migration status: %s", report["status"])
        logger.info("Duration: %s seconds", report["duration_seconds"])
        logger.info("Report JSON: %s", str(report_file.resolve()))
        logger.info("Log file: %s", str(log_file.resolve()))

        ok = sum(1 for t in report["tables"] if t.get("status") in ("success", "dry_run"))
        failed = sum(1 for t in report["tables"] if t.get("status") == "failed")
        logger.info("Summary: tables_ok=%d tables_failed=%d total_tables=%d errors=%d",
                    ok, failed, len(report["tables"]), len(report["errors"]))

    return report

