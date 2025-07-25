"""Worker that downloads job archives from OpenAddresses."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Tuple

from oa_etl.clients.openaddresses import OAAsyncClient, Job, DownloadResult
from oa_etl.core.retry import retry_logic
from oa_etl.telemetry.metrics import Metrics
from oa_etl.autoscaling import AdjustableLimiter
from oa_etl.backpressure import EnqueueGate
from oa_etl.constants import STOP_DOWNLOAD
logger = logging.getLogger(__name__)


async def download_worker(
    wid: int,
    jobs_q: asyncio.Queue[Any],
    parse_q: asyncio.Queue[Tuple[Job, DownloadResult]],
    client: OAAsyncClient,
    dest_dir: Path,
    dl_timeout: float,
    chunk_size: int,
    max_retries: int,
    backoff_factor: float,
    metrics: Metrics,
    limiter: AdjustableLimiter,
    enqueue_gate: EnqueueGate,
) -> None:
    """
    Downloads OA job archives to disk.  When a download finishes successfully it
    enqueues **(job, DownloadResult)** into *parse_q* (instead of the former
    DB-processing queue).  The downloader now has **no knowledge of the DB
    layer** - it only feeds the parsing tier.
    """
    while True:
        item = await jobs_q.get()
        if item is STOP_DOWNLOAD:
            jobs_q.task_done()
            logger.debug("Downloader %d received STOP", wid)
            break

        await limiter.acquire()
        job: Job = item
        try:

            async def _download() -> DownloadResult:
                return await client.download_job(
                    job_id=job.id,
                    dest=dest_dir / f"{job.id}.geojson.gz",
                    timeout=dl_timeout,
                    chunk_size=chunk_size,
                )

            dl_res = await retry_logic(
                _download,
                max_retries,
                backoff_factor,
            )

            metrics.add_bytes(dl_res.bytes)
            metrics.observe_stage("download", dl_res.duration)

            # Wait for back-pressure release, then enqueue for parsing
            await enqueue_gate.wait_open()
            await parse_q.put((job, dl_res))

        except Exception as exc:
            metrics.inc("jobs_total", 1)
            metrics.inc("jobs_failed", 1)
            metrics.record_error(exc)
            logger.exception(
                "Downloader %d: job %s failed to download: %s", wid, job.id, exc
            )
        finally:
            await limiter.release()
            jobs_q.task_done()
