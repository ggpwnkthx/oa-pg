"""
Single batch-writer that accumulates rows from *row_q* and flushes them to
PostgreSQL once ≥100000 rows are buffered.
"""
from __future__ import annotations

import asyncio
import logging
from time import perf_counter
from typing import Any, List

import psycopg_pool
from psycopg import sql

from oa_etl.core.copying import copy_batch_binary
from oa_etl.core.staging import create_temp_staging_table
from oa_etl.addressing import ADDRESS_HEADER
from oa_etl.core.parsing import METADATA_HEADER
from oa_etl.telemetry.metrics import Metrics
from oa_etl.constants import STOP_BATCH
from oa_etl.config import Config

logger = logging.getLogger(__name__)

BATCH_ROWS_TARGET = 100_000


async def db_batch_writer(
    row_q: asyncio.Queue[Any],
    pool: psycopg_pool.AsyncConnectionPool,
    metrics: Metrics,
    config: Config,
) -> None:
    """Flush accumulated rows from ``row_q`` into PostgreSQL in batches.

    Parameters
    ----------
    row_q:
        Queue containing encoded rows ready to be copied into the database.
    pool:
        Connection pool used to obtain PostgreSQL connections.
    metrics:
        Metrics object used to record COPY performance.
    config:
        Global configuration for deduplication and COPY behavior.
    """
    staging_cols = ADDRESS_HEADER + \
        METADATA_HEADER + ["geometry", "address_hash"]
    buffer: List[bytes] = []
    done = False

    async def _flush(rows: List[bytes]) -> float:
        """COPY ``rows`` into PostgreSQL and perform deduplication inserts."""

        if not rows:
            return 0.0

        logger.info("Flushing %d rows to database", len(rows))
        loop = asyncio.get_running_loop()
        conn_wait_start = loop.time()
        async with pool.connection() as conn:
            conn_wait = loop.time() - conn_wait_start
            metrics.observe_stage("db_conn_acquire", conn_wait)
            metrics.observe_db_wait(conn_wait)
            if conn_wait > 0.5:
                logger.warning(
                    "DB writer waited %.3fs for a DB connection", conn_wait
                )
            async with conn.transaction():
                start = perf_counter()

                staging_tbl = "tmp_buffer_ingest"
                await create_temp_staging_table(conn, staging_tbl)
                await copy_batch_binary(
                    conn,
                    staging_tbl,
                    staging_cols,
                    rows,
                    metrics,
                    job_id="bulk_batch",
                    freeze=False,
                )

                addr_cols_sql = sql.SQL(", ").join(sql.Identifier(c)
                                               for c in ADDRESS_HEADER)
                addr_cols_with_hash_sql = sql.SQL(", ").join(
                    [*(sql.Identifier(c) for c in ADDRESS_HEADER),
                     sql.Identifier("unique_hash")]
                )

                if config.dedup_strategy == "group_by":
                    dedup_sql = sql.SQL(
                        """
                        CREATE TEMP TABLE tmp_dedup ON COMMIT DROP AS
                        SELECT *
                          FROM {tmp}
                         GROUP BY address_hash, {all_cols}
                        """
                    ).format(
                        tmp=sql.Identifier(staging_tbl),
                        all_cols=sql.SQL(", ").join(
                            sql.Identifier(c) for c in staging_cols
                        ),
                    )
                else:
                    dedup_sql = sql.SQL(
                        """
                        CREATE TEMP TABLE tmp_dedup ON COMMIT DROP AS
                        SELECT DISTINCT ON (address_hash) *
                          FROM {tmp}
                         ORDER BY address_hash;
                        """
                    ).format(tmp=sql.Identifier(staging_tbl))

                async with conn.cursor() as cur:
                    await cur.execute(dedup_sql)
                    await cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO addresses ({cols_hash})
                            SELECT {cols}, t.address_hash
                              FROM tmp_dedup AS t
                            ON CONFLICT (unique_hash) DO NOTHING
                            """
                        ).format(
                            cols_hash=addr_cols_with_hash_sql,
                            cols=addr_cols_sql,
                        )
                    )
                    await cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO openaddresses (
                                address_uuid, job_id, source_name,
                                hash, canonical, geometry
                            )
                            SELECT a.uuid, t.job_id, t.source_name,
                                   t.hash, t.canonical, t.geometry
                              FROM tmp_dedup AS t
                              JOIN addresses AS a
                                ON a.unique_hash = t.address_hash
                            ON CONFLICT (job_id, hash, geometry) DO NOTHING
                            """
                        )
                    )

                flush_dur = perf_counter() - start

        metrics.observe_stage("db_writer_flush", flush_dur)
        logger.info("Flush completed in %.3f s", flush_dur)
        return flush_dur

    while not done:
        try:
            item = await row_q.get()
            if item is STOP_BATCH:
                done = True
            elif isinstance(item, list):   # type-safety guard
                buffer.extend(item)
            row_q.task_done()

            if len(buffer) >= BATCH_ROWS_TARGET or (done and buffer):
                logger.debug(
                    "Triggering flush of %d rows (done=%s)", len(buffer), done
                )
                start = perf_counter()
                duration = await _flush(buffer)
                total = perf_counter() - start
                metrics.observe_stage("batch_flush", total)
                buffer.clear()
        except Exception:
            logger.exception("DB batch-writer crashed; continuing")

    logger.info("DB batch-writer exiting gracefully")
