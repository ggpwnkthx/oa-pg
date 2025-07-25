"""File I/O helpers used by worker tasks."""

from __future__ import annotations

import gzip
from pathlib import Path
from typing import Iterator, List


def chunked_lines(file_path: Path, chunk_size: int) -> Iterator[List[str]]:
    """Yield ``chunk_size`` lines at a time from a gzipped file."""
    with gzip.open(file_path, "rt") as infile:
        buf: List[str] = []
        for line in infile:
            buf.append(line)
            if len(buf) >= chunk_size:
                yield buf
                buf = []
        if buf:
            yield buf
