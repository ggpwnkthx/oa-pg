"""Helpers for efficiently ``COPY``ing binary-encoded rows into PostgreSQL."""

from __future__ import annotations

import struct
from time import perf_counter
from typing import List, Tuple
import logging

import psycopg
from psycopg import sql

from oa_etl.telemetry.metrics import Metrics

logger = logging.getLogger(__name__)


async def copy_batch_binary(
    conn: psycopg.AsyncConnection,
    table: str,
    staging_cols: List[str],
    batch_rows: List[bytes],
    metrics: Metrics,
    job_id: str,
    *,
    freeze: bool = False,  # use COPY ... FREEZE on PG15+ only when needed
) -> Tuple[int, float]:
    """
    COPY a batch of pre-encoded binary rows into `table`.

    NOTE: FREEZE only works on PostgreSQL 15+ and only for tables
    (not views / queries). If you run on PG < 15, set freeze=False.
    """
    if not batch_rows:
        return 0, 0.0

    # Standard binary copy header + trailer
    header = b"PGCOPY\n\xff\r\n\0" + \
        struct.pack("<I", 0) + struct.pack("<I", 0)
    trailer = struct.pack("!h", -1)

    # Build the COPY statement, adding FREEZE only when requested
    copy_opts = "FORMAT binary"
    if freeze:
        copy_opts += ", FREEZE"
    copy_q = sql.SQL(
        "COPY {tbl} ({cols}) FROM STDIN WITH ({opts})"
    ).format(
        tbl=sql.Identifier(table),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in staging_cols),
        opts=sql.SQL(copy_opts),
    )

    start = perf_counter()
    async with conn.cursor() as cur, cur.copy(copy_q) as copier:
        # Merge header + all rows + trailer into one payload for a single write
        payload = header + b"".join(batch_rows) + trailer
        await copier.write(payload)
    duration = perf_counter() - start

    rps = len(batch_rows) / duration if duration > 0 else 0.0
    metrics.observe_stage("copy", duration)
    metrics.observe_copy_rps(rps)
    logger.info(
        "Job %s: batch COPY processed %d features in %.3f s (%.2f rows/s)",
        job_id,
        len(batch_rows),
        duration,
        rps,
    )
    return len(batch_rows), duration


