# normalize_addresses/pipeline.py
import asyncio
import contextlib
import concurrent.futures
import os
from pathlib import Path
from time import perf_counter
from typing import List

from .sources.base import AddressSource
from .writers import CSVGzipWriter
from .models import InputAddress, NormalizedRecord
from .utils import normalize_one
from .logging_config import logger


class NormalizationPipeline:
    """
    Single-pass pipeline:

        source.records()  →  normalize_one()  →  CSVGzipWriter
    """

    def __init__(
        self,
        source: AddressSource,
        out_path: Path,
        concurrency: int = 16,       # now *used* to size the CPU pool
        status_interval: float = 5.0,
        batch_size: int = 10_000,    # rows per CSV batch
    ) -> None:
        self.source = source
        self.out_path = out_path
        self.status_interval = max(0.5, status_interval)
        self.batch_size = max(1_000, batch_size)

        # Dedicated pool for CPU-bound tasks (libpostal + gzip+csv)
        self._cpu_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, concurrency if concurrency > 0 else (os.cpu_count() or 4))
        )

        logger.debug(
            "NormalizationPipeline(out_path=%s, status_interval=%.1f, "
            "concurrency=%d, batch_size=%d)",
            out_path,
            self.status_interval,
            self._cpu_executor._max_workers,   # type: ignore[attr-defined]
            self.batch_size,
        )

    async def _async_normalize(self, addr: InputAddress) -> NormalizedRecord:
        """
        Off-load libpostal parsing to the thread pool.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._cpu_executor, normalize_one, addr)

    async def _async_write_batch(self, writer: CSVGzipWriter, rows: List[List[str]]) -> None:
        """
        Off-load heavy CSV+gzip batch write to the thread pool.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._cpu_executor, writer.write_batch, rows)

    async def run(self) -> None:
        start = perf_counter()
        processed = 0

        logger.info("Pipeline run starting (status_interval=%.1fs)", self.status_interval)

        # ------------------------------------------------------------------
        # Periodic monitor task
        # ------------------------------------------------------------------
        monitor_task = None

        if self.status_interval > 0:

            async def monitor() -> None:
                nonlocal processed
                last_p = 0
                last_t = perf_counter()
                try:
                    while True:
                        await asyncio.sleep(self.status_interval)
                        now = perf_counter()
                        delta_p = processed - last_p
                        delta_t = now - last_t
                        rate = delta_p / delta_t if delta_t > 0 else 0.0
                        logger.info("Pipeline status: processed=%s  rate=%.1f r/s",
                                    f"{processed:,}", rate)
                        last_p, last_t = processed, now
                except asyncio.CancelledError:
                    pass

            monitor_task = asyncio.create_task(monitor())

        # ------------------------------------------------------------------
        # Main loop: stream → normalise → batch-write
        # ------------------------------------------------------------------
        batch: List[List[str]] = []

        with CSVGzipWriter(self.out_path) as writer:
            async for item in self.source.records():
                if isinstance(item, NormalizedRecord):
                    norm_rec = item
                else:
                    # InputAddress → NormalizedRecord (CPU-bound)
                    norm_rec = await self._async_normalize(item)  # type: ignore[arg-type]

                batch.append(norm_rec.to_csv_row())
                processed += 1

                if len(batch) >= self.batch_size:
                    await self._async_write_batch(writer, batch)
                    batch.clear()

            # flush trailing rows
            if batch:
                await self._async_write_batch(writer, batch)
                batch.clear()

        # ------------------------------------------------------------------
        # Tear-down
        # ------------------------------------------------------------------
        if monitor_task:
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task

        self._cpu_executor.shutdown(wait=True)

        duration = perf_counter() - start
        rate = processed / duration if duration > 0 else 0.0
        logger.info(
            "Done - wrote %s rows to %s in %.2fs  (avg %.1f r/s)",
            f"{processed:,}", self.out_path, duration, rate
        )
