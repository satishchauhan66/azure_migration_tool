# Author: S@tish Chauhan

"""Data migration functionality."""

import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyodbc

from ..utils.database import build_conn_str, pick_sql_driver, connect_to_database, resolve_password
from ..utils.logging import setup_logger
from ..utils.paths import app_data_dir, qident, short_slug, utc_ts_compact


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


def fetch_sequence_column(cur, schema: str, table: str) -> Optional[str]:
    """Return a column suitable for ordering/slicing: first identity, else first PK column. None if none."""
    obj_id = f"{schema}.{table}"
    # First try identity column
    cur.execute(
        """
        SELECT c.name
        FROM sys.columns c
        WHERE c.object_id = OBJECT_ID(?, 'U') AND c.is_identity = 1
        ORDER BY c.column_id;
        """,
        obj_id,
    )
    row = cur.fetchone()
    if row:
        while cur.fetchone() is not None:
            pass  # Drain rest so connection is not busy for next command
        return row[0]
    # Drain identity result set before next execute
    while cur.fetchone() is not None:
        pass
    # Else first column of primary key (query returns one row per PK column)
    cur.execute(
        """
        SELECT c.name
        FROM sys.index_columns ic
        JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE i.object_id = OBJECT_ID(?, 'U') AND i.is_primary_key = 1
        ORDER BY ic.key_ordinal;
        """,
        obj_id,
    )
    row = cur.fetchone()
    while cur.fetchone() is not None:
        pass  # Drain rest (composite PK returns multiple rows)
    return row[0] if row else None


def build_slice_boundaries(
    cur,
    schema: str,
    table: str,
    sequence_col: str,
    num_slices: int,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """Use NTILE to split table into equal-sized slices; return list of {slice_id, min_val, max_val, row_count}."""
    qual = f"{qident(schema)}.{qident(table)}"
    seq_q = qident(sequence_col)
    # Single pass: NTILE then aggregate to get min/max per slice
    sql = f"""
    WITH t AS (
        SELECT {seq_q} AS seq_val, NTILE(?) OVER (ORDER BY {seq_q}) AS slice_id
        FROM {qual}
    )
    SELECT slice_id, MIN(seq_val) AS min_val, MAX(seq_val) AS max_val, COUNT_BIG(1) AS row_count
    FROM t
    GROUP BY slice_id
    ORDER BY slice_id;
    """
    cur.execute(sql, (num_slices,))
    rows = cur.fetchall()
    boundaries = [
        {"slice_id": r.slice_id, "min_val": r.min_val, "max_val": r.max_val, "row_count": int(r.row_count)}
        for r in rows
    ]
    # Avoid duplicate key: slice i (i>=2) uses exclusive lower bound = previous slice's max, so no row is in two slices
    for i in range(1, len(boundaries)):
        boundaries[i]["exclusive_min"] = boundaries[i - 1]["max_val"]
    if boundaries:
        boundaries[0]["exclusive_min"] = None
    return boundaries


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


# ----------------------------
# CONSTRAINT / INDEX / TRIGGER HELPERS (for BCP path)
# ----------------------------
def disable_all_foreign_keys(cur, logger: Optional[logging.Logger] = None):
    """Disable all foreign key constraints on user tables."""
    cur.execute("""
        SELECT OBJECT_SCHEMA_NAME(fk.parent_object_id) AS sch, OBJECT_NAME(fk.parent_object_id) AS tbl, fk.name AS fk_name
        FROM sys.foreign_keys fk
        WHERE fk.is_disabled = 0
        ORDER BY OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id);
    """)
    for r in cur.fetchall():
        sql = f"ALTER TABLE {qident(r.sch)}.{qident(r.tbl)} NOCHECK CONSTRAINT {qident(r.fk_name)};"
        try:
            cur.execute(sql)
            if logger:
                logger.info("Disabled FK: %s.%s.%s", r.sch, r.tbl, r.fk_name)
        except Exception as e:
            if logger:
                logger.warning("Could not disable FK %s: %s", r.fk_name, e)


def enable_all_foreign_keys(cur, logger: Optional[logging.Logger] = None):
    """Re-enable all foreign key constraints."""
    cur.execute("""
        SELECT OBJECT_SCHEMA_NAME(fk.parent_object_id) AS sch, OBJECT_NAME(fk.parent_object_id) AS tbl, fk.name AS fk_name
        FROM sys.foreign_keys fk
        WHERE fk.is_disabled = 1
        ORDER BY OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id);
    """)
    for r in cur.fetchall():
        sql = f"ALTER TABLE {qident(r.sch)}.{qident(r.tbl)} WITH CHECK CHECK CONSTRAINT {qident(r.fk_name)};"
        try:
            cur.execute(sql)
            if logger:
                logger.info("Enabled FK: %s.%s.%s", r.sch, r.tbl, r.fk_name)
        except Exception as e:
            if logger:
                logger.warning("Could not enable FK %s: %s", r.fk_name, e)


def disable_nonclustered_indexes(cur, logger: Optional[logging.Logger] = None):
    """Disable all non-clustered indexes on user tables."""
    cur.execute("""
        SELECT s.name AS sch, t.name AS tbl, i.name AS idx_name
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE i.type_desc = 'NONCLUSTERED' AND i.is_disabled = 0 AND i.name IS NOT NULL AND t.is_ms_shipped = 0;
    """)
    for r in cur.fetchall():
        sql = f"ALTER INDEX {qident(r.idx_name)} ON {qident(r.sch)}.{qident(r.tbl)} DISABLE;"
        try:
            cur.execute(sql)
            if logger:
                logger.info("Disabled index: %s.%s.%s", r.sch, r.tbl, r.idx_name)
        except Exception as e:
            if logger:
                logger.warning("Could not disable index %s: %s", r.idx_name, e)


def rebuild_indexes(cur, logger: Optional[logging.Logger] = None):
    """Rebuild all disabled non-clustered indexes."""
    cur.execute("""
        SELECT s.name AS sch, t.name AS tbl, i.name AS idx_name
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE i.type_desc = 'NONCLUSTERED' AND i.is_disabled = 1 AND i.name IS NOT NULL AND t.is_ms_shipped = 0;
    """)
    for r in cur.fetchall():
        sql = f"ALTER INDEX {qident(r.idx_name)} ON {qident(r.sch)}.{qident(r.tbl)} REBUILD;"
        try:
            cur.execute(sql)
            if logger:
                logger.info("Rebuilt index: %s.%s.%s", r.sch, r.tbl, r.idx_name)
        except Exception as e:
            if logger:
                logger.warning("Could not rebuild index %s: %s", r.idx_name, e)


def disable_nonclustered_indexes_for_table(
    cur, schema: str, table: str, logger: Optional[logging.Logger] = None
) -> None:
    """Disable non-clustered indexes on a single table (faster bulk insert)."""
    cur.execute(
        """
        SELECT i.name AS idx_name
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND i.type_desc = 'NONCLUSTERED' AND i.is_disabled = 0 AND i.name IS NOT NULL;
        """,
        (schema, table),
    )
    for r in cur.fetchall():
        sql = f"ALTER INDEX {qident(r.idx_name)} ON {qident(schema)}.{qident(table)} DISABLE;"
        try:
            cur.execute(sql)
            if logger:
                logger.info("Disabled index: %s.%s.%s", schema, table, r.idx_name)
        except Exception as e:
            if logger:
                logger.warning("Could not disable index %s: %s", r.idx_name, e)


def rebuild_indexes_for_table(
    cur, schema: str, table: str, logger: Optional[logging.Logger] = None
) -> None:
    """Rebuild disabled non-clustered indexes on a single table."""
    cur.execute(
        """
        SELECT i.name AS idx_name
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND i.type_desc = 'NONCLUSTERED' AND i.is_disabled = 1 AND i.name IS NOT NULL;
        """,
        (schema, table),
    )
    for r in cur.fetchall():
        sql = f"ALTER INDEX {qident(r.idx_name)} ON {qident(schema)}.{qident(table)} REBUILD;"
        try:
            cur.execute(sql)
            if logger:
                logger.info("Rebuilt index: %s.%s.%s", schema, table, r.idx_name)
        except Exception as e:
            if logger:
                logger.warning("Could not rebuild index %s: %s", r.idx_name, e)


def disable_all_triggers(cur, logger: Optional[logging.Logger] = None):
    """Disable all triggers on user tables."""
    cur.execute("""
        SELECT OBJECT_SCHEMA_NAME(tr.parent_id) AS sch, OBJECT_NAME(tr.parent_id) AS tbl
        FROM sys.triggers tr
        WHERE tr.parent_id > 0 AND tr.is_disabled = 0;
    """)
    seen = set()
    for r in cur.fetchall():
        key = (r.sch, r.tbl)
        if key in seen:
            continue
        seen.add(key)
        sql = f"ALTER TABLE {qident(r.sch)}.{qident(r.tbl)} DISABLE TRIGGER ALL;"
        try:
            cur.execute(sql)
            if logger:
                logger.info("Disabled triggers: %s.%s", r.sch, r.tbl)
        except Exception as e:
            if logger:
                logger.warning("Could not disable triggers on %s.%s: %s", r.sch, r.tbl, e)


def enable_all_triggers(cur, logger: Optional[logging.Logger] = None):
    """Re-enable all triggers on user tables."""
    cur.execute("""
        SELECT OBJECT_SCHEMA_NAME(tr.parent_id) AS sch, OBJECT_NAME(tr.parent_id) AS tbl
        FROM sys.triggers tr
        WHERE tr.parent_id > 0 AND tr.is_disabled = 1;
    """)
    seen = set()
    for r in cur.fetchall():
        key = (r.sch, r.tbl)
        if key in seen:
            continue
        seen.add(key)
        sql = f"ALTER TABLE {qident(r.sch)}.{qident(r.tbl)} ENABLE TRIGGER ALL;"
        try:
            cur.execute(sql)
            if logger:
                logger.info("Enabled triggers: %s.%s", r.sch, r.tbl)
        except Exception as e:
            if logger:
                logger.warning("Could not enable triggers on %s.%s: %s", r.sch, r.tbl, e)


def build_insert_statement(schema: str, table: str, col_names: List[str]) -> str:
    """Build INSERT statement with placeholders"""
    cols = ", ".join(qident(c) for c in col_names)
    placeholders = ", ".join(["?"] * len(col_names))
    return f"INSERT INTO {qident(schema)}.{qident(table)} ({cols}) VALUES ({placeholders});"


def delete_slice_on_dest(
    dest_cur,
    schema: str,
    table: str,
    sequence_col: str,
    min_val: Any,
    max_val: Any,
    exclusive_min: Any = None,
) -> None:
    """Delete rows on destination in the given sequence range (for retry). Use exclusive_min to match non-overlapping slice."""
    seq_q = qident(sequence_col)
    if exclusive_min is None:
        sql = f"DELETE FROM {qident(schema)}.{qident(table)} WHERE {seq_q} >= ? AND {seq_q} <= ?;"
        dest_cur.execute(sql, (min_val, max_val))
    else:
        sql = f"DELETE FROM {qident(schema)}.{qident(table)} WHERE {seq_q} > ? AND {seq_q} <= ?;"
        dest_cur.execute(sql, (exclusive_min, max_val))


def fetch_slice_batches(
    src_cur,
    schema: str,
    table: str,
    col_names: List[str],
    sequence_col: str,
    min_val: Any,
    max_val: Any,
    batch_size: int,
    exclusive_min: Any = None,
):
    """Execute SELECT for slice range once, then yield batches. Use exclusive_min to avoid boundary overlap (no duplicate key)."""
    cols = ", ".join(qident(c) for c in col_names)
    seq_q = qident(sequence_col)
    qual = f"{qident(schema)}.{qident(table)}"
    if exclusive_min is None:
        sql = f"SELECT {cols} FROM {qual} WHERE {seq_q} >= ? AND {seq_q} <= ? ORDER BY {seq_q};"
        params = (min_val, max_val)
    else:
        sql = f"SELECT {cols} FROM {qual} WHERE {seq_q} > ? AND {seq_q} <= ? ORDER BY {seq_q};"
        params = (exclusive_min, max_val)
    src_cur.arraysize = min(batch_size, 10000)  # Larger fetch for faster chunked reads
    src_cur.execute(sql, params)
    while True:
        rows = src_cur.fetchmany(batch_size)
        if not rows:
            break
        yield rows


def copy_one_slice(
    logger: logging.Logger,
    src_conn,
    dest_conn,
    schema: str,
    table: str,
    col_names: List[str],
    has_identity: bool,
    insert_sql: str,
    slice_info: Dict[str, Any],
    sequence_col: str,
    batch_size: int,
    max_retries: int = 3,
    continue_on_error: bool = False,
) -> Tuple[int, Optional[str]]:
    """Copy one slice (min_val..max_val) from source to dest. On failure, DELETE that range on dest and retry. Returns (inserted_count, error_msg)."""
    slice_id = slice_info["slice_id"]
    min_val = slice_info["min_val"]
    max_val = slice_info["max_val"]
    exclusive_min = slice_info.get("exclusive_min")  # Avoid boundary overlap (no duplicate key)
    table_fqn = f"{schema}.{table}"
    total_inserted = 0
    last_error: Optional[str] = None

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()
    dest_cur.fast_executemany = True

    # Each worker has its own connection; IDENTITY_INSERT is session-scoped, so set it ON on this connection
    if has_identity:
        set_identity_insert(dest_cur, schema, table, True)
        dest_conn.commit()

    try:
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    # Previous attempt failed: delete this slice's range on dest and retry
                    logger.warning("Slice %s %s: retry %d/%d – deleting slice range on dest and retrying", table_fqn, slice_id, attempt + 1, max_retries)
                    delete_slice_on_dest(dest_cur, schema, table, sequence_col, min_val, max_val, exclusive_min)
                    dest_conn.commit()

                inserted = 0
                for rows in fetch_slice_batches(
                    src_cur, schema, table, col_names, sequence_col, min_val, max_val, batch_size, exclusive_min
                ):
                    dest_cur.executemany(insert_sql, rows)
                    dest_conn.commit()
                    inserted += len(rows)
                total_inserted = inserted
                last_error = None
                break
            except Exception as ex:
                last_error = f"{type(ex).__name__}: {ex}"
                try:
                    dest_conn.rollback()
                except Exception:
                    pass
                logger.warning("Slice %s %s attempt %d/%d failed: %s", table_fqn, slice_id, attempt + 1, max_retries, last_error)
                if not continue_on_error and attempt == max_retries - 1:
                    return (total_inserted, last_error)

        return (total_inserted, last_error)
    finally:
        # Turn IDENTITY_INSERT OFF on this worker's connection
        if has_identity:
            try:
                set_identity_insert(dest_cur, schema, table, False)
                dest_conn.commit()
            except Exception:
                pass


def migrate_one_table_chunked(
    logger: logging.Logger,
    cfg: dict,
    src_conn,
    dest_conn,
    schema: str,
    table: str,
    batch_size: int,
    truncate_dest: bool,
    delete_dest: bool,
    continue_on_error: bool,
    dry_run: bool,
    num_chunks: int,
    chunk_workers: int,
    disable_indexes: bool = False,
) -> dict:
    """Migrate a single table by splitting into slices (control table), copying in parallel (up to 32 workers), with per-slice DELETE and retry on failure."""
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
        "chunked": True,
        "slices_ok": 0,
        "slices_failed": 0,
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

        sequence_col = fetch_sequence_column(src_cur, schema, table)
        if not sequence_col:
            result["status"] = "failed"
            result["error"] = "Chunked migration requires an identity or primary key column; none found"
            result["duration_seconds"] = round(time.time() - t0, 3)
            return result

        num_slices = min(32, max(1, num_chunks))
        workers = min(32, max(1, chunk_workers), num_slices)
        logger.info(
            "Chunked migration %s: sequence_col=%s slices=%d workers=%d",
            table_fqn, sequence_col, num_slices, workers,
        )

        if dry_run:
            logger.info("DRY RUN: skipping chunked data move for %s", table_fqn)
            result["status"] = "dry_run"
            result["duration_seconds"] = round(time.time() - t0, 3)
            return result

        # Build slice boundaries (control table in memory)
        boundaries = build_slice_boundaries(src_cur, schema, table, sequence_col, num_slices, logger)
        if not boundaries:
            result["status"] = "failed"
            result["error"] = "Failed to build slice boundaries"
            result["duration_seconds"] = round(time.time() - t0, 3)
            return result

        # Prep destination: truncate/delete when requested, or when row count mismatch (clean reload)
        dest_count = result["dest_count_before"]
        src_count = result["src_count"]
        if truncate_dest and dest_count > 0:
            logger.info("Truncating destination table: %s", table_fqn)
            truncate_table(dest_cur, schema, table, conn=dest_conn, logger=logger)
            dest_conn.commit()
        elif delete_dest and dest_count > 0:
            logger.info("Deleting destination rows: %s", table_fqn)
            delete_table(dest_cur, schema, table)
            dest_conn.commit()
        elif dest_count > 0 and dest_count != src_count:
            logger.info("Row count mismatch (src=%s dest=%s): truncating destination for clean reload: %s", src_count, dest_count, table_fqn)
            truncate_table(dest_cur, schema, table, conn=dest_conn, logger=logger)
            dest_conn.commit()

        if disable_indexes:
            logger.info("Disabling non-clustered indexes on %s (faster insert)", table_fqn)
            disable_nonclustered_indexes_for_table(dest_cur, schema, table, logger)
            dest_conn.commit()

        if has_identity:
            logger.info("IDENTITY_INSERT ON: %s", table_fqn)
            set_identity_insert(dest_cur, schema, table, True)
            dest_conn.commit()

        insert_sql = build_insert_statement(schema, table, col_names)
        driver = pick_sql_driver(logger)

        def run_slice(slice_info: Dict[str, Any]) -> Tuple[Dict[str, Any], int, Optional[str]]:
            with connect_to_database(
                server=cfg["src_server"], db=cfg["src_db"], user=cfg["src_user"], driver=driver,
                auth=cfg["src_auth"], password=cfg["src_password"], timeout=30, logger=logger,
            ) as w_src:
                w_src.timeout = 0
                with connect_to_database(
                    server=cfg["dest_server"], db=cfg["dest_db"], user=cfg["dest_user"], driver=driver,
                    auth=cfg["dest_auth"], password=cfg["dest_password"], timeout=30, logger=logger,
                ) as w_dest:
                    w_dest.timeout = 0
                    inserted, err = copy_one_slice(
                        logger, w_src, w_dest, schema, table, col_names, has_identity,
                        insert_sql, slice_info, sequence_col, batch_size, max_retries=3,
                        continue_on_error=continue_on_error,
                    )
            return (slice_info, inserted, err)

        inserted_total = 0
        slices_ok = 0
        slices_failed = 0
        errors: List[str] = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(run_slice, s): s for s in boundaries}
            for future in as_completed(futures):
                slice_info = futures[future]
                try:
                    _, inserted, err = future.result()
                    inserted_total += inserted
                    if err:
                        slices_failed += 1
                        errors.append(f"Slice {slice_info['slice_id']}: {err}")
                    else:
                        slices_ok += 1
                except Exception as ex:
                    slices_failed += 1
                    errors.append(f"Slice {slice_info['slice_id']}: {ex}")

        if has_identity:
            logger.info("IDENTITY_INSERT OFF: %s", table_fqn)
            set_identity_insert(dest_cur, schema, table, False)
            dest_conn.commit()

        if disable_indexes:
            logger.info("Rebuilding non-clustered indexes on %s", table_fqn)
            rebuild_indexes_for_table(dest_cur, schema, table, logger)
            dest_conn.commit()

        result["inserted"] = inserted_total
        result["slices_ok"] = slices_ok
        result["slices_failed"] = slices_failed
        result["dest_count_after"] = count_rows(dest_cur, schema, table)
        result["batches"] = num_slices

        if slices_failed:
            result["status"] = "completed_with_errors" if slices_ok else "failed"
            result["error"] = "; ".join(errors[:5])
            if len(errors) > 5:
                result["error"] += f" ... ({len(errors)} total)"
        else:
            result["status"] = "success"

        result["duration_seconds"] = round(time.time() - t0, 3)
        logger.info(
            "Done %s (chunked) | inserted=%d | slices_ok=%d slices_failed=%d | duration=%.3fs",
            table_fqn, result["inserted"], slices_ok, slices_failed, result["duration_seconds"],
        )
        return result

    except Exception as ex:
        try:
            if result.get("identity_insert"):
                set_identity_insert(dest_cur, schema, table, False)
                dest_conn.commit()
            if disable_indexes:
                rebuild_indexes_for_table(dest_cur, schema, table, logger)
                dest_conn.commit()
        except Exception:
            pass
        result["status"] = "failed"
        result["error"] = f"{type(ex).__name__}: {ex}"
        result["duration_seconds"] = round(time.time() - t0, 3)
        logger.exception("Chunked migration failed: %s", table_fqn)
        return result


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
    enable_chunking: bool = False,
    chunk_threshold: int = 500_000,
    num_chunks: int = 10,
    chunk_workers: int = 4,
    disable_indexes: bool = False,
    skip_completed: bool = False,
    cfg: Optional[dict] = None,
) -> dict:
    """Migrate a single table. If enable_chunking and row count >= chunk_threshold and table has identity/PK, use parallel slice copy with retry. If skip_completed and src_count == dest_count, skip."""
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

        # Skip if already completed (row counts match)
        if skip_completed and result["src_count"] == result["dest_count_before"]:
            logger.info("Skipping %s (row counts match: %d)", table_fqn, result["src_count"])
            result["status"] = "skipped"
            result["dest_count_after"] = result["dest_count_before"]
            result["duration_seconds"] = round(time.time() - t0, 3)
            return result

        # Use chunked path when enabled, table is large enough, and we have cfg + sequence column
        if (
            enable_chunking
            and result["src_count"] >= chunk_threshold
            and cfg is not None
        ):
            sequence_col = fetch_sequence_column(src_cur, schema, table)
            if sequence_col:
                return migrate_one_table_chunked(
                    logger=logger,
                    cfg=cfg,
                    src_conn=src_conn,
                    dest_conn=dest_conn,
                    schema=schema,
                    table=table,
                    batch_size=batch_size,
                    truncate_dest=truncate_dest,
                    delete_dest=delete_dest,
                    continue_on_error=continue_on_error,
                    dry_run=dry_run,
                    num_chunks=num_chunks,
                    chunk_workers=chunk_workers,
                    disable_indexes=disable_indexes,
                )
            else:
                logger.info("Chunking enabled but no identity/PK for %s; using full-table copy", table_fqn)

        if dry_run:
            logger.info("DRY RUN: skipping data move for %s", table_fqn)
            result["status"] = "dry_run"
            result["duration_seconds"] = round(time.time() - t0, 3)
            return result

        # Prep dest: truncate/delete when requested, or when row count mismatch (clean reload)
        dest_count = result["dest_count_before"]
        src_count = result["src_count"]
        if truncate_dest and dest_count > 0:
            logger.info("Truncating destination table: %s", table_fqn)
            truncate_table(dest_cur, schema, table, conn=dest_conn, logger=logger)
            dest_conn.commit()
        elif delete_dest and dest_count > 0:
            logger.info("Deleting destination rows: %s", table_fqn)
            delete_table(dest_cur, schema, table)
            dest_conn.commit()
        elif dest_count > 0 and dest_count != src_count:
            logger.info("Row count mismatch (src=%s dest=%s): truncating destination for clean reload: %s", src_count, dest_count, table_fqn)
            truncate_table(dest_cur, schema, table, conn=dest_conn, logger=logger)
            dest_conn.commit()

        if disable_indexes:
            logger.info("Disabling non-clustered indexes on %s (faster insert)", table_fqn)
            disable_nonclustered_indexes_for_table(dest_cur, schema, table, logger)
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

        if disable_indexes:
            logger.info("Rebuilding non-clustered indexes on %s", table_fqn)
            rebuild_indexes_for_table(dest_cur, schema, table, logger)
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
            if disable_indexes:
                rebuild_indexes_for_table(dest_cur, schema, table, logger)
                dest_conn.commit()
        except Exception:
            pass

        msg = f"{type(ex).__name__}: {ex}"
        logger.exception("Table failed: %s | %s", table_fqn, msg)
        result["status"] = "failed"
        result["error"] = msg
        result["duration_seconds"] = round(time.time() - t0, 3)
        return result


def run_migration(cfg: dict, log_callback=None):
    """Run data migration with provided configuration.
    log_callback: optional callable(msg: str) to stream log messages (e.g. to GUI).
    """
    if cfg["truncate_dest"] and cfg["delete_dest"]:
        raise ValueError("Choose only one: truncate_dest OR delete_dest")

    run_id = utc_ts_compact()

    # Use project path if provided, otherwise user-writable app data dir
    data_root = Path(cfg["project_path"]) if cfg.get("project_path") else app_data_dir()
    src_tag = short_slug(f"{cfg['src_server']}__{cfg['src_db']}")
    dest_tag = short_slug(f"{cfg['dest_server']}__{cfg['dest_db']}")
    root = data_root / "migrations" / run_id / f"{src_tag}__to__{dest_tag}"

    logs_dir = root / "logs"
    meta_dir = root / "meta"
    logs_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / f"migrate_{run_id}.log"
    logger = setup_logger(log_file, "data_migration")
    if log_callback:
        class CallbackHandler(logging.Handler):
            def emit(self, record):
                try:
                    log_callback(self.format(record))
                except Exception:
                    pass
        cb_handler = CallbackHandler()
        cb_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(cb_handler)
        logger.setLevel(logging.INFO)

    logger.info("Starting data migration run: %s", run_id)
    logger.info("Source: %s | %s | auth=%s | user=%s", cfg["src_server"], cfg["src_db"], cfg["src_auth"], cfg["src_user"])
    logger.info("Dest:   %s | %s | auth=%s | user=%s", cfg["dest_server"], cfg["dest_db"], cfg["dest_auth"], cfg["dest_user"])
    logger.info("Batch size: %d", cfg["batch_size"])
    logger.info("truncate_dest=%s delete_dest=%s continue_on_error=%s dry_run=%s",
                cfg["truncate_dest"], cfg["delete_dest"], cfg["continue_on_error"], cfg["dry_run"])
    if cfg.get("enable_chunking") or cfg.get("migrate_enable_chunking"):
        logger.info("Chunked migration: threshold=%s num_chunks=%s chunk_workers=%s (max 32)",
                    cfg.get("chunk_threshold") or cfg.get("migrate_chunk_threshold"),
                    cfg.get("num_chunks") or cfg.get("migrate_num_chunks"),
                    cfg.get("chunk_workers") or cfg.get("migrate_chunk_workers"))
    if cfg.get("skip_completed") or cfg.get("migrate_skip_completed"):
        logger.info("Skip completed tables: ON (tables with matching src/dest row counts will be skipped)")
    parallel_tables = max(1, min(32, cfg.get("parallel_tables", cfg.get("migrate_parallel_tables", 1))))
    if parallel_tables > 1:
        logger.info("Parallel tables: %d (migrating up to %d tables at a time)", parallel_tables, parallel_tables)
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

                num_tables = len(final_tables)
                parallel_tables_eff = min(parallel_tables, num_tables) if parallel_tables > 1 else 1

                def _migrate_one_table_worker(item: Tuple[int, str, str]) -> Tuple[int, Dict[str, Any]]:
                    """Run migrate_one_table with dedicated connections (for parallel table migration)."""
                    idx, schema, table = item
                    table_fqn = f"{schema}.{table}"
                    logger.info("=== [%d/%d] Migrating %s ===", idx + 1, num_tables, table_fqn)
                    with connect_to_database(
                        server=cfg["src_server"],
                        db=cfg["src_db"],
                        user=cfg["src_user"],
                        driver=driver,
                        auth=cfg["src_auth"],
                        password=cfg["src_password"],
                        timeout=30,
                        logger=logger,
                    ) as my_src:
                        my_src.timeout = 0
                        with connect_to_database(
                            server=cfg["dest_server"],
                            db=cfg["dest_db"],
                            user=cfg["dest_user"],
                            driver=driver,
                            auth=cfg["dest_auth"],
                            password=cfg["dest_password"],
                            timeout=30,
                            logger=logger,
                        ) as my_dest:
                            my_dest.timeout = 0
                            res = migrate_one_table(
                                logger=logger,
                                src_conn=my_src,
                                dest_conn=my_dest,
                                schema=schema,
                                table=table,
                                batch_size=cfg["batch_size"],
                                truncate_dest=cfg["truncate_dest"],
                                delete_dest=cfg["delete_dest"],
                                continue_on_error=cfg["continue_on_error"],
                                dry_run=cfg["dry_run"],
                                enable_chunking=cfg.get("enable_chunking", cfg.get("migrate_enable_chunking", False)),
                                chunk_threshold=cfg.get("chunk_threshold", cfg.get("migrate_chunk_threshold", 500_000)),
                                num_chunks=cfg.get("num_chunks", cfg.get("migrate_num_chunks", 10)),
                                chunk_workers=cfg.get("chunk_workers", cfg.get("migrate_chunk_workers", 4)),
                                disable_indexes=cfg.get("disable_indexes", cfg.get("migrate_disable_indexes", False)),
                                skip_completed=cfg.get("skip_completed", cfg.get("migrate_skip_completed", False)),
                                cfg=cfg,
                            )
                    return (idx, res)

                if parallel_tables_eff <= 1:
                    # Sequential: use the existing shared connections
                    for i, (schema, table) in enumerate(final_tables, start=1):
                        table_fqn = f"{schema}.{table}"
                        logger.info("=== [%d/%d] Migrating %s ===", i, num_tables, table_fqn)
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
                            enable_chunking=cfg.get("enable_chunking", cfg.get("migrate_enable_chunking", False)),
                            chunk_threshold=cfg.get("chunk_threshold", cfg.get("migrate_chunk_threshold", 500_000)),
                            num_chunks=cfg.get("num_chunks", cfg.get("migrate_num_chunks", 10)),
                            chunk_workers=cfg.get("chunk_workers", cfg.get("migrate_chunk_workers", 4)),
                            disable_indexes=cfg.get("disable_indexes", cfg.get("migrate_disable_indexes", False)),
                            skip_completed=cfg.get("skip_completed", cfg.get("migrate_skip_completed", False)),
                            cfg=cfg,
                        )
                        report["tables"].append(res)
                        if res["status"] in ("failed", "completed_with_errors"):
                            report["errors"].append(f"{table_fqn}: {res.get('error')}")
                            if not cfg["continue_on_error"]:
                                raise RuntimeError(f"Stopping due to failure in {table_fqn}")
                else:
                    # Parallel: each table gets its own connections via worker
                    results_by_idx = [None] * num_tables  # type: List[Optional[Dict[str, Any]]]
                    with ThreadPoolExecutor(max_workers=parallel_tables_eff) as executor:
                        futures = {
                            executor.submit(_migrate_one_table_worker, (i, schema, table)): (i, schema, table)
                            for i, (schema, table) in enumerate(final_tables)
                        }
                        for future in as_completed(futures):
                            idx, res = future.result()
                            results_by_idx[idx] = res
                    for idx in range(num_tables):
                        res = results_by_idx[idx]
                        schema, table = final_tables[idx]
                        table_fqn = f"{schema}.{table}"
                        report["tables"].append(res)
                        if res["status"] in ("failed", "completed_with_errors"):
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

        ok = sum(1 for t in report["tables"] if t.get("status") in ("success", "dry_run", "skipped"))
        failed = sum(1 for t in report["tables"] if t.get("status") in ("failed", "completed_with_errors"))
        report["tables_ok"] = ok
        report["tables_failed"] = failed
        logger.info("Summary: tables_ok=%d tables_failed=%d total_tables=%d errors=%d",
                    ok, failed, len(report["tables"]), len(report["errors"]))

    return report

