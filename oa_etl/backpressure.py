from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class EnqueueGate:
    """
    A tiny wrapper around an asyncio.Event to make the intent explicit.
    When closed, download workers will *not* enqueue into the DB processing queue.
    """

    def __init__(self, initially_open: bool = True) -> None:
        self._event = asyncio.Event()
        if initially_open:
            self._event.set()

    def is_open(self) -> bool:
        return self._event.is_set()

    async def wait_open(self) -> None:
        await self._event.wait()

    def open(self) -> None:
        if not self._event.is_set():
            logger.info("Backpressure: releasing enqueue gate")
            self._event.set()

    def close(self) -> None:
        if self._event.is_set():
            logger.warning("Backpressure: engaging enqueue gate (pause enqueueing to DB)")
            self._event.clear()
