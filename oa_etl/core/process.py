"""High level job processing routine used by DB workers."""

from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

import psycopg
from psycopg import sql

from oa_etl.addressing import ADDRESS_HEADER
from oa_etl.telemetry.metrics import Metrics
from oa_etl.clients.openaddresses import Job

from oa_etl.config import Config
from .copying import copy_batch_binary
from .io import chunked_lines
from .parsing import (
    parse_lines_to_binary_rows,
    BATCH_LINES,
    METADATA_HEADER,
)
from .staging import temp_staging_table_name, create_temp_staging_table

logger = logging.getLogger(__name__)


async def process_job(
    job: Job,
    file_path: Path,
    conn: psycopg.AsyncConnection,
    metrics: Metrics,
    config: Config,
) -> None:
    """
    Lazily create a TEMP TABLE per job (ON COMMIT DROP) **only if** the first
    parsed chunk has at least one valid row. This avoids paying DDL costs for
    the many 0-feature jobs we see in OA.

    Improvements here:
      - Fewer COPY calls by batching rows more efficiently.
      - Only FREEZE on the first batch to reduce per-batch overhead.
      - Optional CREATE TEMP HASH INDEX on (address_hash) before CTAS/INSERTs.
      - Configurable dedup strategy:
          * DISTINCT ON (address_hash)  (deterministic order)
          * GROUP BY address_hash       (faster hash-agg, but you lose ordering semantics)
      - Optional psycopg pipeline() to shave RTTs on CTAS/INSERTs; if libpq is too old,
        we automatically fallback to the non-pipeline path.
    """
    overall_start = perf_counter()
    logger.info("Job %s: starting process_job (lazy temp table)", job.id)

    # ---- 0) Read first chunk & see if we have anything worth inserting ----
    chunks_iter = chunked_lines(file_path, BATCH_LINES)

    try:
        first_chunk = next(chunks_iter)
    except StopIteration:
        metrics.add_features(0)
        try:
            file_path.unlink(missing_ok=True)
            logger.info("Job %s: deleted (empty) input file %s",
                        job.id, file_path)
        except Exception as e:
            logger.warning("Job %s: failed to delete %s: %s",
                           job.id, file_path, e)
        total = perf_counter() - overall_start
        metrics.observe_stage("job_total", total)
        metrics.inc_lazy_skipped(total)
        logger.info(
            "Job %s: completed in %.3f total seconds (0 features)", job.id, total)
        return

    first_rows, first_features = parse_lines_to_binary_rows(first_chunk, job)

    if first_features == 0:
        metrics.add_features(0)
        logger.info(
            "Job %s: first chunk had 0 valid addresses; skipping entire job",
            job.id,
        )
        try:
            file_path.unlink(missing_ok=True)
            logger.info("Job %s: deleted input file %s", job.id, file_path)
        except Exception as e:
            logger.warning("Job %s: failed to delete %s: %s",
                           job.id, file_path, e)

        total = perf_counter() - overall_start
        metrics.observe_stage("job_total", total)
        metrics.inc_lazy_skipped(total)
        logger.info(
            "Job %s: completed in %.3f total seconds (0 features)", job.id, total)
        return

    # ---- 1) We do have rows; now create the per-job temp table ----
    staging_table = temp_staging_table_name(job.id)
    staging_cols = ADDRESS_HEADER + \
        METADATA_HEADER + ["geometry", "address_hash"]

    step_start = perf_counter()
    await create_temp_staging_table(conn, staging_table)
    staging_prep_dur = perf_counter() - step_start
    metrics.observe_stage("temp_table", staging_prep_dur)
    logger.info(
        "Job %s: created TEMP staging table %s in %.3f s",
        job.id,
        staging_table,
        staging_prep_dur,
    )

    # ---- 2) COPY ... FREEZE on first batch, then normal COPY for the rest ----
    total_features = 0

    # First batch with FREEZE
    copied, _ = await copy_batch_binary(
        conn,
        staging_table,
        staging_cols,
        first_rows,
        metrics,
        job.id,
        freeze=True,
    )
    total_features += copied

    # Subsequent batches without FREEZE
    for lines in chunks_iter:
        rows, n = parse_lines_to_binary_rows(lines, job)
        if n == 0:
            continue
        copied, _ = await copy_batch_binary(
            conn,
            staging_table,
            staging_cols,
            rows,
            metrics,
            job.id,
            freeze=False,
        )
        total_features += copied

    metrics.add_features(total_features)

    # ---- 3) Optional temp hash index to speed dedup CTAS ----
    if config.create_temp_hash_index:
        logger.info(
            "Job %s: creating TEMP INDEX ON %s(address_hash)",
            job.id,
            staging_table,
        )
        step_start = perf_counter()
        async with conn.cursor() as cur:
            await cur.execute(
                sql.SQL(
                    "CREATE INDEX ON {tbl} USING hash(address_hash);"
                ).format(tbl=sql.Identifier(staging_table))
            )
        idx_dur = perf_counter() - step_start
        metrics.observe_stage("temp_hash_index", idx_dur)
        logger.info(
            "Job %s: temp hash index built in %.3f s", job.id, idx_dur
        )

    # ---- 4) Deduplicate & insert ----
    addr_cols_sql = sql.SQL(", ").join(sql.Identifier(c)
                                       for c in ADDRESS_HEADER)
    addr_cols_with_hash_sql = sql.SQL(", ").join(
        [*(sql.Identifier(c)
           for c in ADDRESS_HEADER), sql.Identifier("unique_hash")]
    )

    if config.dedup_strategy == "group_by":
        dedup_sql = sql.SQL(
            """
            CREATE TEMP TABLE tmp_dedup ON COMMIT DROP AS
            SELECT *
              FROM {tmp}
             GROUP BY address_hash, {all_cols}
            """
        ).format(
            tmp=sql.Identifier(staging_table),
            all_cols=sql.SQL(", ").join(
                [*(sql.Identifier(c) for c in staging_cols)]
            ),
        )
    else:
        dedup_sql = sql.SQL(
            """
            CREATE TEMP TABLE tmp_dedup ON COMMIT DROP AS
            SELECT DISTINCT ON (address_hash) *
              FROM {tmp}
             ORDER BY address_hash;
            """
        ).format(tmp=sql.Identifier(staging_table))

    step_start = perf_counter()

    async def _ddl_and_inserts(cur: psycopg.AsyncCursor) -> None:
        await cur.execute(dedup_sql)
        await cur.execute(
            sql.SQL(
                """
                INSERT INTO addresses ({addr_cols_with_hash})
                SELECT {addr_cols}, t.address_hash
                  FROM tmp_dedup AS t
                ON CONFLICT (unique_hash) DO NOTHING
                """
            ).format(
                addr_cols_with_hash=addr_cols_with_hash_sql,
                addr_cols=addr_cols_sql,
            )
        )
        await cur.execute(
            sql.SQL(
                """
                INSERT INTO openaddresses (
                    address_uuid, job_id, source_name,
                    hash, canonical, geometry
                )
                SELECT a.uuid, t.job_id, t.source_name,
                       t.hash, t.canonical, t.geometry
                  FROM tmp_dedup AS t
                  JOIN addresses AS a
                    ON a.unique_hash = t.address_hash
                ON CONFLICT (job_id, hash, geometry) DO NOTHING
                """
            )
        )

    used_pipeline = False
    if config.pipeline_mode:
        try:
            logger.info(
                "Job %s: running CTAS + INSERTs in psycopg pipeline mode", job.id
            )
            async with conn.pipeline():
                async with conn.cursor() as cur:
                    await _ddl_and_inserts(cur)
            used_pipeline = True
        except psycopg.NotSupportedError:
            logger.warning(
                "Job %s: libpq too old for pipeline(); falling back to non-pipeline mode",
                job.id,
            )

    if not used_pipeline:
        async with conn.cursor() as cur:
            await _ddl_and_inserts(cur)

    inserts_duration = perf_counter() - step_start
    metrics.observe_stage("db_inserts", inserts_duration)
    logger.info("Job %s: DB inserts took %.3f s", job.id, inserts_duration)

    # ---- 5) Cleanup local file; temp tables drop automatically at COMMIT ----
    step_start = perf_counter()
    try:
        file_path.unlink(missing_ok=True)
        logger.info("Job %s: deleted input file %s", job.id, file_path)
    except Exception as e:
        logger.warning(
            "Job %s: failed to delete %s: %s", job.id, file_path, e
        )
    cleanup_duration = perf_counter() - step_start
    metrics.observe_stage("cleanup", cleanup_duration)
    logger.info("Job %s: cleanup took %.3f s", job.id, cleanup_duration)

    # ---- Final summary ----
    total = perf_counter() - overall_start
    metrics.observe_stage("job_total", total)
    logger.info(
        "Job %s: completed in %.3f total seconds (%d features before SQL dedup)",
        job.id,
        total,
        total_features,
    )
