"""Gate mechanism used to apply backpressure on downloaders."""

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
        """Create a new gate.

        Parameters
        ----------
        initially_open:
            If ``True`` (default), the gate starts open and workers may
            enqueue immediately.
        """
        self._event = asyncio.Event()
        if initially_open:
            self._event.set()

    def is_open(self) -> bool:
        """Return ``True`` if enqueueing is currently allowed."""
        return self._event.is_set()

    async def wait_open(self) -> None:
        """Wait asynchronously until the gate is opened."""
        await self._event.wait()

    def open(self) -> None:
        """Open the gate so downloaders can enqueue rows again."""
        if not self._event.is_set():
            logger.info("Backpressure: releasing enqueue gate")
            self._event.set()

    def close(self) -> None:
        """Close the gate to pause enqueueing and apply backpressure."""
        if self._event.is_set():
            logger.warning("Backpressure: engaging enqueue gate (pause enqueueing to DB)")
            self._event.clear()
