"""Batch writer that collects rows from ``row_q`` and writes them to a
local Apache Arrow file once processing is complete."""

from __future__ import annotations

import asyncio
import logging
import struct
from pathlib import Path
from time import perf_counter
from typing import Any, List

import pyarrow as pa
import pyarrow.ipc as ipc

from oa_etl.addressing import ADDRESS_HEADER
from oa_etl.core.parsing import METADATA_HEADER
from oa_etl.telemetry.metrics import Metrics
from oa_etl.constants import STOP_BATCH

logger = logging.getLogger(__name__)


def _decode_row(row: bytes) -> List[bytes | None]:
    """Decode the binary COPY row into a list of bytes/None."""
    buf = memoryview(row)
    offset = 0
    num_cols = struct.unpack_from("!h", buf, offset)[0]
    offset += 2
    values: List[bytes | None] = []
    for _ in range(num_cols):
        length = struct.unpack_from("!i", buf, offset)[0]
        offset += 4
        if length == -1:
            values.append(None)
        else:
            values.append(bytes(buf[offset:offset + length]))
            offset += length
    return values


async def arrow_batch_writer(
    row_q: asyncio.Queue[Any],
    arrow_path: Path,
    metrics: Metrics,
) -> None:
    """Write accumulated rows from ``row_q`` to ``arrow_path``."""
    cols = ADDRESS_HEADER + METADATA_HEADER + ["geometry", "address_hash"]
    columns: dict[str, List[Any]] = {c: [] for c in cols}

    done = False

    while not done:
        item = await row_q.get()
        if item is STOP_BATCH:
            done = True
        elif isinstance(item, list):
            for row in item:
                vals = _decode_row(row)
                for c, v in zip(cols, vals):
                    columns[c].append(v)
        row_q.task_done()

    start = perf_counter()
    arrays = [pa.array(columns[c]) for c in cols]
    batch = pa.record_batch(arrays, cols)
    with arrow_path.open("wb") as f:
        with ipc.new_file(f, batch.schema) as writer:
            writer.write(batch)
    dur = perf_counter() - start
    metrics.observe_stage("arrow_writer_flush", dur)
    logger.info("Wrote %d rows to %s in %.3f s", batch.num_rows, arrow_path, dur)
