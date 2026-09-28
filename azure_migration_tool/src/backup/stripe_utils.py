# Author: Satish Chauhan
"""Shared backup stripe sizing for local disk and BACKUP TO URL."""

from __future__ import annotations

import math
from typing import Any, Optional

# Target ~10 GB per stripe for aggressive parallel I/O (faster BACKUP / upload / restore).
# Still well under block-blob limits; capped at MAX_BACKUP_STRIPES.
STRIPE_TARGET_GB = 10
# Below this size, a single .bak is fine (striping overhead not worth it).
STRIPE_MIN_SIZE_GB = 5
MAX_BACKUP_STRIPES = 64


def get_database_size_mb(cur: Any, database: str) -> Optional[float]:
    """Return total data+log size of the database in MB, or None if unreadable."""
    try:
        cur.execute(
            """
            SELECT CAST(SUM(CAST(size AS BIGINT)) * 8.0 / 1024.0 AS FLOAT) AS size_mb
            FROM sys.master_files
            WHERE database_id = DB_ID(?)
            """,
            (database,),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return None


def recommend_backup_stripes(
    size_mb: Optional[float],
    *,
    max_stripes: int = MAX_BACKUP_STRIPES,
) -> int:
    """
    Pick stripe count from database size.

    Rules:
      - < STRIPE_MIN_SIZE_GB -> 1 stripe
      - else ~STRIPE_TARGET_GB per stripe, rounded up to next power of 2 (max 64)
      - multi-TB databases -> up to 64 stripes
    """
    cap = max(1, min(int(max_stripes or MAX_BACKUP_STRIPES), MAX_BACKUP_STRIPES))
    if size_mb is None or size_mb <= 0:
        return 1
    size_gb = size_mb / 1024.0
    if size_gb < STRIPE_MIN_SIZE_GB:
        return 1
    needed = max(1, math.ceil(size_gb / STRIPE_TARGET_GB))
    power = 1
    while power < needed and power < cap:
        power *= 2
    return min(power, cap)


def format_stripe_hint(size_mb: Optional[float], stripes: int) -> str:
    """Short UI hint for recommended stripes."""
    if size_mb is None or size_mb <= 0:
        return f"Database size unknown — Auto uses {stripes} stripe(s)."
    size_gb = size_mb / 1024.0
    if size_gb >= 1024:
        size_txt = f"{size_gb / 1024.0:.2f} TB"
    else:
        size_txt = f"{size_gb:.1f} GB"
    return (
        f"Database ~{size_txt} → Auto recommends {stripes} stripe(s) "
        f"(~{STRIPE_TARGET_GB} GB per file, max {MAX_BACKUP_STRIPES})."
    )
