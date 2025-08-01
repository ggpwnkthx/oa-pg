"""
PostgresBulkWriter - ultra-fast COPY-based loader to PostgreSQL, now persisting
only a single `address_norm` column.
"""
from __future__ import annotations

import csv
import io
from typing import List

import psycopg  # type: ignore
from psycopg import sql  # type: ignore

# --------------------------------------------------------------------------- #
# SQL templates                                                               #
# --------------------------------------------------------------------------- #
_COPY_SQL = sql.SQL(
    """
    COPY public.addresses_staging (address_norm)
    FROM STDIN WITH (FORMAT CSV)
    """
)

_MERGE_SQL = sql.SQL(
    """
    INSERT INTO public.addresses (address_norm)
    SELECT address_norm
    FROM public.addresses_staging
    ON CONFLICT DO NOTHING
    """
)

_TRUNC_SQL = sql.SQL("TRUNCATE public.addresses_staging")


class PostgresBulkWriter:
    """
    Ultra-fast bulk loader using `COPY … FROM STDIN WITH (FORMAT CSV)`.

    Pipeline now streams *one* column per row - the canonical string produced
    by `NormalizedRecord.address_norm`.
    """

    def __init__(self, dsn: str, *, commit_every: int = 50) -> None:
        self.dsn = dsn
        self.commit_every = max(1, commit_every)

        self._conn: psycopg.Connection | None = None
        self._cur: psycopg.Cursor | None = None
        self._batches_since_commit = 0

        # In-memory CSV buffer reused across batches
        self._buffer = io.StringIO()
        self._csv_writer = csv.writer(
            self._buffer,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        )

    # ---------------------------------------------- #
    # Context management                              #
    # ---------------------------------------------- #
    def __enter__(self) -> "PostgresBulkWriter":
        # autocommit=False for explicit transaction control
        self._conn = psycopg.connect(self.dsn)
        self._cur = self._conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if self._batches_since_commit and self._conn:
                self._conn.commit()
            self._merge_into_base()
        finally:
            if self._cur:
                self._cur.close()
            if self._conn:
                self._conn.close()

    # ---------------------------------------------- #
    # Public API - conforms to BatchWriter            #
    # ---------------------------------------------- #
    def write_batch(self, rows: List[List[str]]) -> None:
        """
        Each `rows` item is `[address_norm]`. No additional transformation is
        required - the canonical string has already been generated upstream.
        """
        if not (self._conn and self._cur):
            raise RuntimeError("PostgresBulkWriter not initialised")

        # Reset and reuse the in-memory buffer
        self._buffer.seek(0)
        self._buffer.truncate(0)

        # Stream rows into the buffer and then into COPY
        self._csv_writer.writerows(rows)
        self._buffer.seek(0)

        with self._cur.copy(_COPY_SQL) as copy:
            copy.write(self._buffer.read())

        self._batches_since_commit += 1
        if self._batches_since_commit >= self.commit_every:
            self._conn.commit()
            self._batches_since_commit = 0

    # ---------------------------------------------- #
    # Final merge & cleanup                           #
    # ---------------------------------------------- #
    def _merge_into_base(self) -> None:
        assert self._cur and self._conn
        self._cur.execute(_MERGE_SQL)
        self._cur.execute(_TRUNC_SQL)
        self._conn.commit()

    # ---------------------------------------------- #
    # Debug / repr                                    #
    # ---------------------------------------------- #
    def __repr__(self) -> str:
        dsn_masked = self.dsn.replace("@", "@…")  # basic credential masking
        return f"<PostgresBulkWriter dsn='{dsn_masked}'>"
