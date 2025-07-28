"""Top level orchestration of asynchronous ETL pipeline stages."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Tuple, Set

import psycopg_pool

from oa_etl.telemetry.metrics import Metrics
from oa_etl.clients.openaddresses import OAAsyncClient, Job
from oa_etl.autoscaling import AdjustableLimiter
from oa_etl.backpressure import EnqueueGate
from oa_etl.config import Config, initialize_environment
from oa_etl.constants import STOP_DOWNLOAD, STOP_PARSE, STOP_BATCH
from oa_etl.workers.download import download_worker
from oa_etl.workers.parse import parse_worker
from oa_etl.workers.db_writer import db_batch_writer
from oa_etl.workers.arrow_writer import arrow_batch_writer


async def _fetch_existing_job_ids(pool: psycopg_pool.AsyncConnectionPool) -> Set[str]:
    """Return the set of job ids already present in the database."""
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT DISTINCT job_id FROM openaddresses")
        rows = await cur.fetchall()
        return {str(r[0]) for r in rows if r[0] is not None}

logger = logging.getLogger(__name__)


async def run_pipeline(config: Config | None = None) -> Tuple[List[Any], Metrics]:
    """Execute the full ETL pipeline and return results and metrics.

    Parameters
    ----------
    config:
        Optional :class:`Config` instance. If ``None``, environment variables
        are loaded via :func:`initialize_environment`.
    """
    if config is None:
        config = await initialize_environment()

    config.dest_dir.mkdir(parents=True, exist_ok=True)
    metrics = Metrics()

    # ─── queues ──────────────────────────────────────────────────────────
    jobs_q:  asyncio.Queue[Any] = asyncio.Queue()
    parse_q: asyncio.Queue[Any] = asyncio.Queue(
        maxsize=config.process_queue_maxsize)
    row_q:   asyncio.Queue[Any] = asyncio.Queue(
        maxsize=config.process_queue_maxsize)  # ← Any

    dl_limiter = AdjustableLimiter(config.autoscale.dl_initial)
    enqueue_gate = EnqueueGate(initially_open=True)
    results: List[Any] = []

    pool: psycopg_pool.AsyncConnectionPool | None = None
    if not config.write_arrow:
        pool = psycopg_pool.AsyncConnectionPool(
            config.dsn,
            min_size=config.db_pool_min,
            max_size=max(2, config.db_pool_min + 1),
            timeout=config.db_conn_timeout,
        )

    if pool is not None:
        pool_cm = pool
    else:
        from contextlib import nullcontext
        pool_cm = nullcontext()

    async with OAAsyncClient(
        max_connections=config.http_max,
        login_timeout=config.login_timeout,
        request_timeout=config.req_timeout,
    ) as client, pool_cm:

        existing_ids: Set[str] = set()
        if pool is not None:
            existing_ids = await _fetch_existing_job_ids(pool)
        all_jobs: List[Job] = await client.fetch_jobs(config.source, config.layer)
        jobs = [j for j in all_jobs if j.id not in existing_ids]
        logger.info(
            "Fetched %d jobs (%d already completed)",
            len(all_jobs),
            len(all_jobs) - len(jobs),
        )

        for j in jobs:
            await jobs_q.put(j)
        for _ in range(config.dl_workers_max):
            await jobs_q.put(STOP_DOWNLOAD)

        dl_tasks = [
            asyncio.create_task(
                download_worker(
                    i, jobs_q, parse_q, client,
                    config.dest_dir, config.dl_timeout, config.chunk_size,
                    config.max_retries, config.backoff_factor,
                    metrics, dl_limiter, enqueue_gate,
                ),
                name=f"downloader-{i}",
            )
            for i in range(config.dl_workers_max)
        ]

        parse_tasks = [
            asyncio.create_task(
                parse_worker(i, parse_q, row_q, metrics),
                name=f"parser-{i}",
            )
            for i in range(max(4, config.db_workers_max))
        ]

        if config.write_arrow:
            writer_task = asyncio.create_task(
                arrow_batch_writer(row_q, config.arrow_path, metrics),
                name="arrow-batch-writer",
            )
        else:
            writer_task = asyncio.create_task(
                db_batch_writer(row_q, pool, metrics, config),
                name="db-batch-writer",
            )

        # ─── wait on pipeline stages ─────────────────────────
        await jobs_q.join()
        logger.info("Downloads finished; signalling parsers")
        for _ in parse_tasks:
            await parse_q.put(STOP_PARSE)

        await parse_q.join()
        logger.info("Parsing finished; signalling DB writer")
        await row_q.put(STOP_BATCH)

        await row_q.join()
        await writer_task
        await asyncio.gather(*dl_tasks,   return_exceptions=True)
        await asyncio.gather(*parse_tasks, return_exceptions=True)

    logger.info("Completed. Success: %d  Failures: %d",
                metrics.jobs_succeeded, metrics.jobs_failed)
    txt, _ = metrics.summary()
    logger.info("\n%s", txt)
    return results, metrics
