import csv
import gzip
import io
from pathlib import Path
from typing import List, Iterable, Sequence, Protocol, runtime_checkable

from .models import NormalizedRecord, CSV_HEADER


@runtime_checkable
class _CSVWriter(Protocol):
    """
    Minimal structural type for csv writer objects.
    Avoids annotating with csv.writer (a function), which caused a runtime
    type error when used in a Union.
    """

    def writerow(self, row: Sequence[str]) -> None: ...
    def writerows(self, rows: Iterable[Sequence[Sequence[str]]]) -> None: ...


class CSVGzipWriter:
    """
    Fast gzip-compressed CSV writer.

    * compresslevel=1 gives ~4× speed-up vs default
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
        self._gzip_fp: gzip.GzipFile | None = None
        self._text_wrapper: io.TextIOWrapper | None = None
        self._writer: _CSVWriter | None = None

    # ------------------------------------------------------------------ #
    # Context-manager helpers                                            #
    # ------------------------------------------------------------------ #
    def __enter__(self) -> "CSVGzipWriter":
        self.out_path.parent.mkdir(parents=True, exist_ok=True)

        self._raw_fp = open(self.out_path, "wb")
        self._gzip_fp = gzip.GzipFile(
            fileobj=self._raw_fp,
            mode="wb",
            mtime=0,
            compresslevel=self.compresslevel,
        )
        self._text_wrapper = io.TextIOWrapper(
            self._gzip_fp,
            encoding=self._encoding,
            newline=self._newline,
        )
        self._writer = csv.writer(self._text_wrapper)
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

    def write_batch(self, rows: List[List[str]]) -> None:
        """
        High-throughput path - write many *already-converted* rows.
        Designed to be called from a thread pool.
        """
        if not self._writer:
            raise RuntimeError("CSVGzipWriter not initialised")
        # The writer accepts any iterable of sequences of strings.
        self._writer.writerows(rows)
