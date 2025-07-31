import asyncio
import contextlib
from pathlib import Path
from time import perf_counter
from typing import Union

from .sources.base import AddressSource
from .writers import CSVGzipWriter
from .models import InputAddress, NormalizedRecord
from .utils import normalize_one
from .logging_config import logger


class NormalizationPipeline:
    def __init__(
        self,
        source: AddressSource,
        out_path: Path,
        concurrency: int = 16,        # unused now, but kept for API compatibility
        status_interval: float = 5.0,
    ) -> None:
        self.source = source
        self.out_path = out_path
        self.status_interval = max(0.5, status_interval)
        logger.debug(
            f"NormalizationPipeline initialized: "
            f"out_path={out_path}, status_interval={self.status_interval}"
        )

    async def run(self) -> None:
        start = perf_counter()
        processed = 0

        logger.info(
            f"Pipeline run starting: status_interval={self.status_interval}"
        )

        # Monitor task for periodic logging
        monitor_task = None
        if self.status_interval > 0:
            async def monitor():
                last_p = 0
                last_t = perf_counter()
                try:
                    while True:
                        await asyncio.sleep(self.status_interval)
                        now = perf_counter()
                        delta_p = processed - last_p
                        delta_t = now - last_t
                        rate = delta_p / delta_t if delta_t > 0 else 0.0
                        logger.info(
                            f"Pipeline status: processed={processed:,} rate={rate:.1f} r/s"
                        )
                        last_p, last_t = processed, now
                except asyncio.CancelledError:
                    pass
            monitor_task = asyncio.create_task(monitor())

        # Single‐pass read → normalize → write
        with CSVGzipWriter(self.out_path) as writer:
            async for item in self.source.records():
                # item may already be normalized (NormalizedRecord),
                # or raw (InputAddress)
                if isinstance(item, NormalizedRecord):
                    norm_rec = item
                else:
                    # must be InputAddress
                    norm_rec = normalize_one(item)  # now safe
                writer.write_record(norm_rec)
                processed += 1

        # Tear down monitor
        if monitor_task:
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task

        duration = perf_counter() - start
        avg_rate = processed / duration if duration > 0 else 0.0
        logger.info(
            f"Done. Wrote {processed:,} rows to {self.out_path} "
            f"in {duration:.2f}s (avg {avg_rate:.1f} r/s)"
        )
