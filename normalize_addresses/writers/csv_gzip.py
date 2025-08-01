"""
CSVGzipWriter - fast gzip-compressed CSV output using isal (igzip).
"""
from __future__ import annotations

import csv
import io
from pathlib import Path

from isal.igzip import GzipFile  # type: ignore[import]

from ..models import NormalizedRecord, CSV_HEADER


class CSVGzipWriter:
    """
    Fast gzip-compressed CSV writer.

    * Uses isal (IGZIP) when available for ~2-5x faster compression.
    * compresslevel defaults to 1 (fast). isal typically supports 0-3.
    * write_batch(rows) emits thousands of rows at once, minimising GIL churn
    """

    def __init__(
        self,
        out_path: Path,
        *,
        newline: str = "",
        encoding: str = "utf-8",
        compresslevel: int = 1,
    ) -> None:
        self.out_path = out_path
        self._encoding = encoding
        self._newline = newline
        self.compresslevel = compresslevel

        self._raw_fp: io.BufferedWriter | None = None
        self._gzip_fp: GzipFile | None = None
        self._text_wrapper: io.TextIOWrapper | None = None
        self._writer: csv._writer | None = None  # type: ignore[attr-defined]

    # ------------------------------------------------------------------ #
    # Context-manager helpers                                            #
    # ------------------------------------------------------------------ #
    def __enter__(self) -> "CSVGzipWriter":
        self.out_path.parent.mkdir(parents=True, exist_ok=True)

        # Raw binary file
        self._raw_fp = open(self.out_path, "wb")

        # Gzip wrapper (isal when available) - keep mtime=0 for deterministic output
        self._gzip_fp = GzipFile(
            fileobj=self._raw_fp,
            mode="wb",
            mtime=0,  # type: ignore[arg-type]
            compresslevel=self.compresslevel,  # type: ignore[call-arg]
        )

        # Text wrapper and CSV writer
        self._text_wrapper = io.TextIOWrapper(
            self._gzip_fp,
            encoding=self._encoding,
            newline=self._newline,
        )
        self._writer = csv.writer(self._text_wrapper)
        if self._writer:
            self._writer.writerow(CSV_HEADER)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if self._text_wrapper:
                self._text_wrapper.flush()
                self._text_wrapper.close()
        finally:
            if self._gzip_fp:
                self._gzip_fp.close()
            if self._raw_fp:
                self._raw_fp.close()

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def write_record(self, rec: NormalizedRecord) -> None:
        """Compatibility shim - writes a single record immediately."""
        if not self._writer:
            raise RuntimeError("CSVGzipWriter not initialised")
        self._writer.writerow(rec.to_csv_row())

    def write_batch(self, rows: list[list[str]]) -> None:
        """
        High-throughput path - write many *already-converted* rows.
        Designed to be called from a thread pool.
        """
        if not self._writer:
            raise RuntimeError("CSVGzipWriter not initialised")
        self._writer.writerows(rows)

    # ------------------------------------------------------------------ #
    # Misc                                                                #
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:
        return f"<CSVGzipWriter path='{self.out_path}'>"
