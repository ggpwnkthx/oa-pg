"""Functions for creating per-job staging tables."""

from __future__ import annotations

import hashlib
import logging
from typing import List

import psycopg
from psycopg import sql

from oa_etl.addressing import ADDRESS_HEADER

logger = logging.getLogger(__name__)

# Only the four metadata fields we COPY
METADATA_HEADER = ["job_id", "source_name", "hash", "canonical"]


def temp_staging_table_name(job_id: str) -> str:
    """
    Generate a short, safe temp table name for a given job id.
    We hash the job id to keep table names short & valid.
    """
    h = hashlib.sha1(job_id.encode("utf-8")).hexdigest()[:16]
    return f"staging_job_{h}"


async def create_temp_staging_table(
    conn: psycopg.AsyncConnection,
    table: str,
) -> None:
    """
    Create a TEMP staging table (session-local) for this job.
    It will automatically disappear ON COMMIT DROP.
    """
    staging_cols: List[str] = ADDRESS_HEADER + \
        METADATA_HEADER + ["geometry", "address_hash"]

    column_defs = []
    for col in ADDRESS_HEADER:
        column_defs.append(sql.SQL("{} TEXT").format(sql.Identifier(col)))
    for col in METADATA_HEADER:
        column_defs.append(sql.SQL("{} TEXT").format(sql.Identifier(col)))
    # Geometry can be any valid type, so avoid constraining to only ``POINT``
    # in the staging table. Keep the SRID of 4326 which matches the source
    # data used throughout the pipeline.
    column_defs.append(sql.SQL("geometry GEOMETRY(Geometry, 4326)"))
    column_defs.append(sql.SQL("address_hash BYTEA"))

    create_table_q = sql.SQL(
        "CREATE TEMP TABLE {tbl} ({defs}) ON COMMIT DROP;"
    ).format(
        tbl=sql.Identifier(table),
        defs=sql.SQL(", ").join(column_defs),
    )

    async with conn.cursor() as cur:
        await cur.execute(create_table_q)

    logger.debug(
        "Created TEMP staging table %s with columns: %s",
        table,
        ", ".join(staging_cols),
    )
