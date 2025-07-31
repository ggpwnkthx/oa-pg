# normalize_addresses/writers.py

import gzip
import io
import csv
from pathlib import Path
from .models import NormalizedRecord, CSV_HEADER

class CSVGzipWriter:
    """
    Gzip-compressed CSV writer with batching and lowered compresslevel for speed.
    """
    def __init__(
        self,
        out_path: Path,
        newline: str = "",
        encoding: str = "utf-8",
        compresslevel: int = 1,    # fastest gzip
        buffer_size: int = 1000,   # flush every N rows
    ):
        self.out_path = out_path
        self._encoding = encoding
        self._newline = newline
        self.compresslevel = compresslevel
        self.buffer_size = buffer_size

        self._buffer: list[list[str]] = []
        self._raw_fp = None
        self._gzip_fp = None
        self._text_wrapper = None
        self._writer = None

    def __enter__(self) -> "CSVGzipWriter":
        # Ensure output directory exists
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self._raw_fp = open(self.out_path, "wb")
        # Lower compresslevel for much faster writes
        self._gzip_fp = gzip.GzipFile(
            fileobj=self._raw_fp,
            mode="wb",
            mtime=0,
            compresslevel=self.compresslevel
        )
        self._text_wrapper = io.TextIOWrapper(
            self._gzip_fp,
            encoding=self._encoding,
            newline=self._newline
        )
        self._writer = csv.writer(self._text_wrapper)
        # Write header immediately
        self._writer.writerow(CSV_HEADER)
        return self

    def write_record(self, rec: NormalizedRecord) -> None:
        assert self._writer is not None, "Writer not initialized"
        self._buffer.append(rec.to_csv_row())
        if len(self._buffer) >= self.buffer_size:
            # write in one big batch
            self._writer.writerows(self._buffer)
            self._buffer.clear()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # Flush remaining rows
        if self._buffer:
            if self._writer:
                self._writer.writerows(self._buffer)
            self._buffer.clear()

        # Close in reverse order to ensure all buffers flush
        try:
            if self._text_wrapper:
                self._text_wrapper.close()
        finally:
            if self._gzip_fp:
                self._gzip_fp.close()
            if self._raw_fp:
                self._raw_fp.close()
