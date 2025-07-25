"""Runtime autoscaling utilities for managing worker concurrency."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from oa_etl.telemetry.metrics import Metrics
from oa_etl.backpressure import EnqueueGate

logger = logging.getLogger(__name__)


class AdjustableLimiter:
    """
    A concurrency limiter whose limit can be raised/lowered at runtime without the
    semaphore bookkeeping bug the previous version had.
    """

    def __init__(self, initial: int) -> None:
        """Initialize the limiter.

        Parameters
        ----------
        initial:
            Starting concurrency value. Must be non-negative.
        """
        if initial < 0:
            raise ValueError("initial must be >= 0")
        self._limit = initial
        self._active = 0
        self._cond = asyncio.Condition()

    async def set_limit(self, new_limit: int) -> None:
        """Set a new concurrency limit.

        Parameters
        ----------
        new_limit:
            The desired maximum number of concurrent tasks. Values below
            zero are treated as zero.
        """
        if new_limit < 0:
            new_limit = 0
        async with self._cond:
            self._limit = new_limit
            self._cond.notify_all()

    async def acquire(self) -> None:
        """Acquire a slot when capacity is available.

        This coroutine waits until fewer than ``limit`` tasks are active and
        then increments the active count.
        """
        async with self._cond:
            while self._active >= self._limit:
                await self._cond.wait()
            self._active += 1

    async def release(self) -> None:
        """Release a previously acquired slot and wake one waiter."""
        async with self._cond:
            self._active -= 1
            if self._active < 0:
                self._active = 0
            self._cond.notify()

    @property
    def limit(self) -> int:
        """Return the currently enforced concurrency limit."""
        return self._limit

    @property
    def active(self) -> int:
        """Return the number of tasks that have acquired the limiter."""
        return self._active


@dataclass
class AutoscaleConfig:
    """Parameters controlling runtime autoscaling.

    Attributes
    ----------
    dl_min, dl_max:
        Lower and upper bounds on concurrent download workers.
    db_min, db_max:
        Lower and upper bounds on concurrent DB workers.
    dl_initial, db_initial:
        Starting concurrency values.
    backlog_high, backlog_low:
        Thresholds on the processing queue size used to detect "hot" or
        "cold" conditions.
    db_wait_p95_high, db_wait_p95_low:
        High/low water marks for 95th percentile DB wait time.
    dl_inc, dl_dec, db_inc, db_dec:
        Step sizes applied when scaling up or down.
    interval_sec:
        Period between control loop iterations.
    hot_hysteresis, cold_hysteresis:
        Number of consecutive hot/cold intervals before acting.
    backpressure_dbwait_p95_high, backpressure_dbwait_p95_low:
        DB wait thresholds for pausing/resuming enqueueing.
    backpressure_require_full_pool:
        Whether backpressure requires the DB pool to be full before
        engaging.
    """
    dl_min: int
    dl_max: int
    db_min: int
    db_max: int
    dl_initial: int
    db_initial: int
    backlog_high: int
    backlog_low: int
    db_wait_p95_high: float
    db_wait_p95_low: float
    dl_inc: int
    dl_dec: int
    db_inc: int
    db_dec: int
    interval_sec: float
    # hysteresis: number of consecutive intervals before acting
    hot_hysteresis: int
    cold_hysteresis: int
    # backpressure thresholds
    backpressure_dbwait_p95_high: float
    backpressure_dbwait_p95_low: float
    backpressure_require_full_pool: bool


class Autoscaler:
    """
    Autoscaler with:
      * Hysteresis (N consecutive "hot" / "cold" intervals).
      * Hard clamp: DB concurrency never exceeds (pool_size - 1) we pass in.
      * Step size bounded by inc/dec knobs.
      * Backpressure: pause enqueueing to DB when hot.
    """

    def __init__(
        self,
        cfg: AutoscaleConfig,
        proc_q: asyncio.Queue[Any],
        dl_limiter: AdjustableLimiter,
        db_limiter: AdjustableLimiter,
        metrics: Metrics,
        stop_event: asyncio.Event,
        db_pool_process_max: int,
        enqueue_gate: EnqueueGate,
    ):
        """Create a new :class:`Autoscaler` instance.

        Parameters
        ----------
        cfg:
            Runtime tuning parameters.
        proc_q:
            Queue from which the backlog size is observed.
        dl_limiter:
            Concurrency limiter controlling downloader workers.
        db_limiter:
            Concurrency limiter controlling DB workers.
        metrics:
            Metrics object providing access to DB wait statistics.
        stop_event:
            When set, signals the autoscaler to terminate.
        db_pool_process_max:
            Maximum DB concurrency allowed by the process pool.
        enqueue_gate:
            Gate used to pause or resume enqueueing when backpressure is
            triggered.
        """
        self.cfg = cfg
        self.proc_q = proc_q
        self.dl_limiter = dl_limiter
        self.db_limiter = db_limiter
        self.metrics = metrics
        self.stop_event = stop_event
        self.db_pool_process_max = db_pool_process_max
        self.enqueue_gate = enqueue_gate

        self._hot_ticks = 0
        self._cold_ticks = 0

    async def run(self) -> None:
        """Run the autoscaling control loop.

        Periodically adjusts the concurrency limits for downloader and database
        workers using metrics from ``proc_q`` and ``metrics``. The loop
        continues until ``stop_event`` is set.
        """
        await self.dl_limiter.set_limit(max(self.cfg.dl_min, self.cfg.dl_initial))
        # clamp initial DB concurrency
        await self.db_limiter.set_limit(
            min(max(self.cfg.db_min, self.cfg.db_initial), self.db_pool_process_max)
        )

        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(self.cfg.interval_sec)

                backlog = self.proc_q.qsize()
                db_wait_p95 = self.metrics.stage_percentile(
                    "db_conn_acquire", 95.0)

                # conditions
                hot_db = backlog > self.cfg.backlog_high or (
                    not (db_wait_p95 != db_wait_p95)  # NaN check
                    and db_wait_p95 > self.cfg.db_wait_p95_high
                )
                cold_db = backlog < self.cfg.backlog_low and (
                    (db_wait_p95 != db_wait_p95) or db_wait_p95 < self.cfg.db_wait_p95_low
                )

                if hot_db:
                    self._hot_ticks += 1
                    self._cold_ticks = 0
                elif cold_db:
                    self._cold_ticks += 1
                    self._hot_ticks = 0
                else:
                    self._hot_ticks = 0
                    self._cold_ticks = 0

                new_dl = self.dl_limiter.limit
                new_db = self.db_limiter.limit

                if self._hot_ticks >= self.cfg.hot_hysteresis:
                    # move toward more DB concurrency and fewer downloaders
                    new_dl = max(self.cfg.dl_min, new_dl - self.cfg.dl_dec)
                    new_db = min(self.cfg.db_max, new_db + self.cfg.db_inc)
                    self._hot_ticks = 0

                if self._cold_ticks >= self.cfg.cold_hysteresis:
                    # open up downloaders, ratchet DB concurrency down
                    new_dl = min(self.cfg.dl_max, new_dl + self.cfg.dl_inc)
                    new_db = max(self.cfg.db_min, new_db - self.cfg.db_dec)
                    self._cold_ticks = 0

                # HARD CLAMP
                new_db = min(new_db, self.db_pool_process_max)

                # Apply limits if changed
                if new_dl != self.dl_limiter.limit:
                    await self.dl_limiter.set_limit(new_dl)
                    logger.info(
                        "Autoscaler: set download concurrency -> %d "
                        "(backlog=%d, db_p95=%.3fs, active=%d)",
                        new_dl, backlog, db_wait_p95, self.dl_limiter.active
                    )

                if new_db != self.db_limiter.limit:
                    await self.db_limiter.set_limit(new_db)
                    logger.info(
                        "Autoscaler: set DB concurrency -> %d "
                        "(backlog=%d, db_p95=%.3fs, active=%d, process_pool_max=%d)",
                        new_db, backlog, db_wait_p95, self.db_limiter.active, self.db_pool_process_max
                    )

                # Backpressure decision
                pool_full = self.db_limiter.active >= self.db_limiter.limit
                if (
                    not (db_wait_p95 != db_wait_p95)
                    and db_wait_p95 >= self.cfg.backpressure_dbwait_p95_high
                    and (not self.cfg.backpressure_require_full_pool or pool_full)
                ):
                    self.enqueue_gate.close()
                elif (
                    (db_wait_p95 != db_wait_p95)
                    or db_wait_p95 <= self.cfg.backpressure_dbwait_p95_low
                ):
                    # "cold enough": re-open
                    self.enqueue_gate.open()

            except Exception:
                logger.exception("Autoscaler crashed; continuing")

        logger.info("Autoscaler exiting")
