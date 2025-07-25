"""
Parses each downloaded *.geojson.gz* archive into **binary‑encoded COPY rows**
and feeds them into the shared *row_q* that the DB batch‑writer consumes.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, List, Tuple

from oa_etl.clients.openaddresses import Job, DownloadResult
from oa_etl.core.io import chunked_lines
from oa_etl.core.parsing import (
    parse_lines_to_binary_rows,
    BATCH_LINES,
)
from oa_etl.telemetry.metrics import Metrics
from oa_etl.constants import STOP_PARSE

logger = logging.getLogger(__name__)


async def parse_worker(
    wid: int,
    parse_q: asyncio.Queue[Any],
    row_q: asyncio.Queue[Any],
    metrics: Metrics,
) -> None:
    while True:
        item = await parse_q.get()
        if item is STOP_PARSE:
            parse_q.task_done()
            logger.debug("Parse‑worker %d received STOP", wid)
            break

        job, dl_res = item
        file_path: Path = dl_res.path

        try:
            total_features_job = 0
            for lines in chunked_lines(file_path, BATCH_LINES):
                rows, features = parse_lines_to_binary_rows(lines, job)
                if not rows:
                    continue
                total_features_job += features
                await row_q.put(rows)

            metrics.add_features(total_features_job)

        except Exception as exc:
            metrics.inc("jobs_total", 1)
            metrics.inc("jobs_failed", 1)
            metrics.record_error(exc)
            logger.exception(
                "Parser %d: job %s failed during parsing: %s", wid, job.id, exc
            )
        finally:
            try:
                file_path.unlink(missing_ok=True)
            except Exception:
                pass
            parse_q.task_done()
