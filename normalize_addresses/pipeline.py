import asyncio
import contextlib
import concurrent.futures
import os
from pathlib import Path
from time import perf_counter
from typing import List, Set, Dict, Optional

from .sources.base import AddressSource
from .writers import CSVGzipWriter
from .models import InputAddress, NormalizedRecord
from .utils import normalize_one, build_canonical_string
from .logging_config import logger


class NormalizationPipeline:
    """
    Single-pass pipeline with overlapped stages:

        source.records()  →  normalize_one()  →  (enqueue batch)  →  [writer task] CSVGzipWriter

    Changes:
      * Writer runs in its own asyncio task, fed by an async queue of batches.
      * CPU-bound normalization uses its own ThreadPoolExecutor.
      * Gzip+CSV writing uses a separate IO executor to prevent starvation.
      * Optional per-batch dedupe (bounded memory) still happens in the main loop.
      * Invalid rows (per is_plausible) are dropped before dedupe/enqueue.
    """

    def __init__(
        self,
        source: AddressSource,
        out_path: Path,
        *,
        concurrency: int = 16,       # CPU pool size for normalization/libpostal
        status_interval: float = 5.0,
        batch_size: int = 10_000,    # rows per CSV batch
        batch_dedupe: bool = True,   # enable per-batch dedupe
        io_workers: Optional[int] = None,       # separate pool for gzip/csv
        writer_queue_max_batches: int = 8,      # bounded buffer for batches
    ) -> None:
        self.source = source
        self.out_path = out_path
        self.status_interval = max(0.5, status_interval)
        self.batch_size = max(1_000, batch_size)
        self.batch_dedupe = batch_dedupe

        # Dedicated pools
        cpu_workers = max(1, concurrency if concurrency >
                          0 else (os.cpu_count() or 4))
        self._cpu_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=cpu_workers)

        # Separate IO (gzip/csv) pool; small is fine because gzip level=1 is fast-ish and I/O bound
        if io_workers is None:
            # heuristic: 1..max(2, cpu//4)
            io_workers = max(2, (os.cpu_count() or 4) // 4)
        self._io_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, io_workers))

        # Async batch queue for writer
        self._write_q: "asyncio.Queue[Optional[List[List[str]]]]" = asyncio.Queue(
            maxsize=max(1, writer_queue_max_batches)
        )

        logger.debug(
            "NormalizationPipeline(out_path=%s, status_interval=%.1f, "
            "cpu_workers=%d, io_workers=%d, batch_size=%d, batch_dedupe=%s, qmax=%d)",
            out_path,
            self.status_interval,
            cpu_workers,
            max(1, io_workers),
            self.batch_size,
            self.batch_dedupe,
            self._write_q.maxsize,
        )

    async def _async_normalize(self, addr: InputAddress) -> NormalizedRecord:
        """
        Off-load libpostal parsing to the CPU thread pool.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._cpu_executor, normalize_one, addr)

    @staticmethod
    def _dedupe_key(rec: NormalizedRecord) -> str:
        """
        Per-batch deduplication key computed from normalized components.
        Mirrors the canonical string used previously but avoids persisting it.
        """
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
        processed = 0   # number of input records seen
        written = 0     # number of rows actually written
        deduped = 0     # number of input records skipped due to per-batch dedupe
        invalids = 0    # number of records skipped by is_plausible()

        logger.info(
            "Pipeline run starting (status_interval=%.1fs, per-batch dedupe=%s)",
            self.status_interval,
            self.batch_dedupe,
        )

        # ----------------------------------------------
        # Writer task: drains batches from async queue
        # ----------------------------------------------
        async def writer_task() -> None:
            nonlocal written
            loop = asyncio.get_running_loop()
            with CSVGzipWriter(self.out_path) as writer:
                while True:
                    batch = await self._write_q.get()
                    if batch is None:
                        # sentinel: close writer & exit
                        break
                    # Dispatch gzip/csv to IO pool
                    await loop.run_in_executor(self._io_executor, writer.write_batch, batch)
                    written += len(batch)

        writer = asyncio.create_task(writer_task(), name="csv_writer")

        # ----------------------------------------------
        # Periodic monitor task
        # ----------------------------------------------
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
                        dp = processed - last_p
                        dw = written - last_w
                        rate_in = dp / delta_t if delta_t > 0 else 0.0
                        rate_out = dw / delta_t if delta_t > 0 else 0.0
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

        # ----------------------------------------------
        # Main loop: stream → normalise → plausibility → per-batch dedupe → enqueue
        # ----------------------------------------------
        batch_rows: List[List[str]] = []
        batch_keys: Set[str] = set()  # bounded to batch_size

        async def enqueue_current_batch() -> None:
            nonlocal batch_rows, batch_keys
            if not batch_rows:
                return
            # hand off the current list to the writer and start a new one
            rows = batch_rows
            batch_rows = []
            batch_keys.clear()
            await self._write_q.put(rows)

        try:
            async for item in self.source.records():
                # Normalize (if needed)
                if isinstance(item, NormalizedRecord):
                    norm_rec = item
                else:
                    # InputAddress → NormalizedRecord (CPU-bound)
                    # type: ignore[arg-type]
                    norm_rec = await self._async_normalize(item)

                processed += 1

                # Fast plausibility check; skip invalids
                if not norm_rec.is_plausible():
                    invalids += 1
                    continue

                # Per-batch dedupe on canonical normalized string
                if self.batch_dedupe:
                    key = self._dedupe_key(norm_rec)
                    if key in batch_keys:
                        deduped += 1
                        continue
                    batch_keys.add(key)

                # Convert to row and add to batch
                batch_rows.append(norm_rec.to_csv_row())

                # Enqueue when we hit batch_size (non-blocking when queue has space)
                if len(batch_rows) >= self.batch_size:
                    await enqueue_current_batch()

            # flush trailing rows
            await enqueue_current_batch()

        finally:
            # Signal writer to finish and wait for it
            await self._write_q.put(None)
            await writer

            # Tear down monitor
            if monitor_task:
                monitor_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await monitor_task

            # Shutdown executors
            self._cpu_executor.shutdown(wait=True)
            self._io_executor.shutdown(wait=True)

        # ----------------------------------------------
        # Final stats
        # ----------------------------------------------
        duration = perf_counter() - start
        rate_in = processed / duration if duration > 0 else 0.0
        rate_out = written / duration if duration > 0 else 0.0
        logger.info(
            "Done - wrote %s rows to %s in %.2fs  (inputs=%s, invalids=%s, deduped=%s, rates: in=%.1f/s out=%.1f/s)",
            f"{written:,}",
            self.out_path,
            duration,
            f"{processed:,}",
            f"{invalids:,}",
            f"{deduped:,}",
            rate_in,
            rate_out,
        )
