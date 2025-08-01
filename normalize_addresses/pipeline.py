from __future__ import annotations

import asyncio
import contextlib
import concurrent.futures
import os
from time import perf_counter
from typing import List, Set, Dict, Optional

from .sources.base import AddressSource
from .writers.base import BatchWriter
from .models import InputAddress, NormalizedRecord
from .utils import normalize_one, build_canonical_string
from .logging_config import logger


class NormalizationPipeline:
    """
    Single-pass pipeline with overlapped stages:

        source.records()  →  normalize_one()  →  (enqueue batch)  →  [writer task] Writer

    Writer is any BatchWriter (CSVGzipWriter, PostgresBulkWriter, ...).
    """

    def __init__(
        self,
        *,
        source: AddressSource,
        writer: BatchWriter,
        concurrency: int = 16,       # CPU pool size for normalization/libpostal
        status_interval: float = 5.0,
        batch_size: int = 10_000,    # rows per batch
        batch_dedupe: bool = True,   # enable per-batch dedupe
        io_workers: Optional[int] = None,       # separate pool for sink IO
        writer_queue_max_batches: int = 8,      # bounded buffer for batches
    ) -> None:
        self.source = source
        self.writer = writer
        self.status_interval = max(0.5, status_interval)
        self.batch_size = max(1_000, batch_size)
        self.batch_dedupe = batch_dedupe

        # Dedicated pools
        cpu_workers = max(1, concurrency if concurrency >
                          0 else (os.cpu_count() or 4))
        self._cpu_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=cpu_workers)

        if io_workers is None:
            io_workers = max(2, (os.cpu_count() or 4) // 4)
        self._io_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, io_workers))

        # Async batch queue for writer
        self._write_q: "asyncio.Queue[Optional[List[List[str]]]]" = asyncio.Queue(
            maxsize=max(1, writer_queue_max_batches)
        )

        logger.debug(
            "NormalizationPipeline(writer=%s, status_interval=%.1f, "
            "cpu_workers=%d, io_workers=%d, batch_size=%d, batch_dedupe=%s, qmax=%d)",
            self.writer,
            self.status_interval,
            cpu_workers,
            max(1, io_workers),
            self.batch_size,
            self.batch_dedupe,
            self._write_q.maxsize,
        )

    async def _async_normalize(self, addr: InputAddress) -> NormalizedRecord:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._cpu_executor, normalize_one, addr)

    @staticmethod
    def _dedupe_key(rec: NormalizedRecord) -> str:
        components: Dict[str, Optional[str]] = {
            "premise": rec.premise,
            "thoroughfare": rec.thoroughfare,
            "dependent_locality": rec.dependent_locality,
            "locality": rec.locality,
            "admin_area_2": rec.admin_area_2,
            "admin_area_1": rec.admin_area_1,
            "postal_code": rec.postal_code,
        }
        return build_canonical_string(components, rec.country_code)

    async def run(self) -> None:
        start = perf_counter()
        processed = 0
        written = 0
        deduped = 0
        invalids = 0

        logger.info(
            "Pipeline run starting (status_interval=%.1fs, per-batch dedupe=%s, writer=%s)",
            self.status_interval,
            self.batch_dedupe,
            self.writer,
        )

        async def writer_task() -> None:
            nonlocal written
            loop = asyncio.get_running_loop()
            with self.writer as writer:
                while True:
                    batch = await self._write_q.get()
                    if batch is None:
                        break
                    await loop.run_in_executor(self._io_executor, writer.write_batch, batch)
                    written += len(batch)

        writer = asyncio.create_task(writer_task(), name="writer")

        monitor_task = None
        if self.status_interval > 0:

            async def monitor() -> None:
                nonlocal processed, written, deduped, invalids
                last_p = 0
                last_w = 0
                last_t = perf_counter()
                try:
                    while True:
                        await asyncio.sleep(self.status_interval)
                        now = perf_counter()
                        delta_t = now - last_t
                        rate_in = (processed - last_p) / \
                            delta_t if delta_t > 0 else 0.0
                        rate_out = (written - last_w) / \
                            delta_t if delta_t > 0 else 0.0
                        logger.info(
                            "Pipeline status: processed=%s written=%s deduped=%s invalids=%s  "
                            "rates: in=%.1f/s out=%.1f/s",
                            f"{processed:,}",
                            f"{written:,}",
                            f"{deduped:,}",
                            f"{invalids:,}",
                            rate_in,
                            rate_out,
                        )
                        last_p, last_w, last_t = processed, written, now
                except asyncio.CancelledError:
                    pass

            monitor_task = asyncio.create_task(monitor())

        batch_rows: List[List[str]] = []
        batch_keys: Set[str] = set()  # bounded to batch_size

        async def enqueue_current_batch() -> None:
            nonlocal batch_rows, batch_keys
            if not batch_rows:
                return
            rows = batch_rows
            batch_rows = []
            batch_keys.clear()
            await self._write_q.put(rows)

        try:
            async for item in self.source.records():
                norm_rec = item if isinstance(item, NormalizedRecord) else await self._async_normalize(item)
                processed += 1

                if not norm_rec.is_plausible():
                    invalids += 1
                    continue

                if self.batch_dedupe:
                    key = self._dedupe_key(norm_rec)
                    if key in batch_keys:
                        deduped += 1
                        continue
                    batch_keys.add(key)

                batch_rows.append(norm_rec.to_csv_row())

                if len(batch_rows) >= self.batch_size:
                    await enqueue_current_batch()

            await enqueue_current_batch()

        finally:
            await self._write_q.put(None)
            await writer

            if monitor_task:
                monitor_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await monitor_task

            self._cpu_executor.shutdown(wait=True)
            self._io_executor.shutdown(wait=True)

        duration = perf_counter() - start
        rate_in = processed / duration if duration > 0 else 0.0
        rate_out = written / duration if duration > 0 else 0.0
        logger.info(
            "Done - wrote %s rows to %s in %.2fs  (inputs=%s, invalids=%s, deduped=%s, rates: in=%.1f/s out=%.1f/s)",
            f"{written:,}",
            self.writer,
            duration,
            f"{processed:,}",
            f"{invalids:,}",
            f"{deduped:,}",
            rate_in,
            rate_out,
        )
