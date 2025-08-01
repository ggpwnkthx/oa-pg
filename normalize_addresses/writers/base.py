from __future__ import annotations

from types import TracebackType
from typing import List, Optional, Protocol, Type, runtime_checkable


@runtime_checkable
class BatchWriter(Protocol):
    """
    Minimal protocol for a high-throughput batch writer.

    Implementations must be context managers and support write_batch().
    """

    def __enter__(self) -> "BatchWriter": ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...
    def write_batch(self, rows: List[List[str]]) -> None: ...
