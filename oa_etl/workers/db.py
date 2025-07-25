"""Worker responsible for database interactions."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import psycopg_pool

from oa_etl.core.process import process_job
from oa_etl.core.retry import retry_logic
from oa_etl.telemetry.metrics import Metrics
from oa_etl.clients.openaddresses import Job, DownloadResult

from oa_etl.autoscaling import AdjustableLimiter
from oa_etl.constants import STOP_PROCESS
from oa_etl.models import Result
from oa_etl.config import Config

logger = logging.getLogger(__name__)


async def db_worker(
    wid: int,
    proc_q: asyncio.Queue[Any],
    pool: psycopg_pool.AsyncConnectionPool,
    db_conn_timeout: float,
    max_retries: int,
    backoff_factor: float,
    metrics: Metrics,
    results: list[Result],
    results_lock: asyncio.Lock,
    limiter: AdjustableLimiter,
    config: Config,
) -> None:
    """Consume parsed jobs from ``proc_q`` and write them to PostgreSQL."""
    loop = asyncio.get_running_loop()

    while True:
        item = await proc_q.get()
        if item is STOP_PROCESS:
            proc_q.task_done()
            logger.debug("DB worker %d received STOP", wid)
            break

        await limiter.acquire()

        job, dl_res = item
        job: Job
        dl_res: DownloadResult

        metrics.inc("jobs_total", 1)

        async def _process_job() -> None:
            """Process a single job within a transaction.

            Executes :func:`process_job` while capturing connection wait time
            metrics and running inside ``pool.connection()``.
            """
            conn_wait_start = loop.time()
            async with pool.connection(timeout=db_conn_timeout) as conn:
                conn_wait = loop.time() - conn_wait_start
                metrics.observe_stage("db_conn_acquire", conn_wait)
                metrics.observe_db_wait(conn_wait)
                if conn_wait > 0.5:
                    logger.warning(
                        "Job %s: waited %.3fs for a DB connection", job.id, conn_wait
                    )

                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute("SET LOCAL synchronous_commit = OFF")
                    await process_job(job, dl_res.path, conn, metrics, config)

        try:
            await retry_logic(
                _process_job,
                max_retries,
                backoff_factor,
            )

            metrics.inc("jobs_succeeded", 1)
            async with results_lock:
                results.append(Result(job_id=job.id, ok=True))

        except Exception as exc:
            metrics.inc("jobs_failed", 1)
            metrics.record_error(exc)
            logger.exception(
                "DB worker %d: job %s failed after retries: %s", wid, job.id, exc
            )
            async with results_lock:
                results.append(Result(job_id=job.id, ok=False, error=str(exc)))

        finally:
            await limiter.release()
            proc_q.task_done()
