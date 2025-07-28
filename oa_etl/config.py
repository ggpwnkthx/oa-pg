"""Environment-based configuration loading for the pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

from oa_etl.autoscaling import AutoscaleConfig


DedupStrategy = Literal["distinct_on", "group_by"]


@dataclass
class Config:
    """Configuration values derived from environment variables."""
    # OA / HTTP
    source: str
    layer: str
    dest_dir: Path
    http_max: int
    login_timeout: float
    req_timeout: float

    # Download stage
    dl_workers_max: int
    dl_initial: int
    dl_timeout: float
    chunk_size: int
    max_retries: int
    backoff_factor: float

    # DB stage
    db_workers_max: int
    db_initial: int
    dsn: str
    db_conn_timeout: float
    db_pool_total: int
    db_pool_min: int
    db_pool_process_max: int  # <= total - 1

    # Queues
    process_queue_maxsize: int

    # Autoscaling
    autoscale: AutoscaleConfig

    # Dedup
    dedup_strategy: DedupStrategy
    create_temp_hash_index: bool
    geom_hash: bool

    # Psycopg pipeline
    pipeline_mode: bool

    # Arrow output
    write_arrow: bool
    arrow_path: Path


def _libpq_supports_pipeline() -> bool:
    """
    psycopg pipeline() requires libpq >= 14.0.
    Detect it and return False if too old (so we auto-disable).
    """
    try:
        from psycopg import pq  # type: ignore
        ver = pq.version()  # e.g. 140002
        return ver >= 140000
    except Exception:
        return False


async def initialize_environment() -> Config:
    """Load environment variables and build a :class:`Config` instance.

    Environment variables control concurrency settings, database connection
    information, autoscaler thresholds and other runtime behavior. This helper
    reads them all and returns a populated :class:`Config` object.
    """
    load_dotenv()

    # Base concurrency knobs (max workers == number of tasks created)
    dl_workers_max = int(os.getenv("OA_DL_WORKERS_MAX", "32"))
    db_workers_max = int(os.getenv("OA_DB_WORKERS_MAX", "16"))

    # Initial limits (<= max)
    dl_initial = int(os.getenv("OA_DL_INITIAL", "16"))
    db_initial = int(os.getenv("OA_DB_INITIAL", "4"))

    # Pools
    http_max = int(os.getenv("OA_HTTP_MAX", str(dl_workers_max * 2)))
    db_pool_total = int(os.getenv("OA_DB_POOL_SIZE", str(db_workers_max)))
    db_pool_min = int(os.getenv("OA_DB_POOL_MIN", "1"))

    # We keep 1 connection for admin/VACUUM/etc.
    db_pool_process_max = max(1, db_pool_total - 1)

    # Autoscale thresholds
    process_queue_max = int(os.getenv("OA_PROCESS_QUEUE_MAX", "200"))
    backlog_high = int(os.getenv("AUTOSCALE_BACKLOG_HIGH",
                       str(int(process_queue_max * 0.8))))
    backlog_low = int(os.getenv("AUTOSCALE_BACKLOG_LOW",
                      str(int(process_queue_max * 0.2))))
    db_wait_p95_high = float(os.getenv("AUTOSCALE_DBWAIT_P95_HIGH", "0.50"))
    db_wait_p95_low = float(os.getenv("AUTOSCALE_DBWAIT_P95_LOW", "0.10"))
    interval_sec = float(os.getenv("AUTOSCALE_INTERVAL_SEC", "2.0"))

    # Floors/ceilings (plus enforce dl_min >= 4)
    dl_min = max(4, int(os.getenv("AUTOSCALE_DL_MIN", "4")))
    dl_max = int(os.getenv("AUTOSCALE_DL_MAX", str(dl_workers_max)))
    db_min = int(os.getenv("AUTOSCALE_DB_MIN", "1"))
    db_max_env = int(os.getenv("AUTOSCALE_DB_MAX", str(db_workers_max)))
    db_max = min(db_max_env, db_pool_process_max)  # enforce pool - 1

    dl_inc = int(os.getenv("AUTOSCALE_DL_INC", "2"))
    dl_dec = int(os.getenv("AUTOSCALE_DL_DEC", "2"))
    db_inc = int(os.getenv("AUTOSCALE_DB_INC", "1"))
    db_dec = int(os.getenv("AUTOSCALE_DB_DEC", "1"))

    # hysteresis
    hot_hysteresis = int(os.getenv("AUTOSCALE_HOT_HYSTERESIS", "3"))
    cold_hysteresis = int(os.getenv("AUTOSCALE_COLD_HYSTERESIS", "3"))

    # backpressure thresholds
    bp_high = float(os.getenv("BACKPRESSURE_DBWAIT_P95_HIGH", "0.75"))
    bp_low = float(os.getenv("BACKPRESSURE_DBWAIT_P95_LOW", "0.25"))
    bp_need_full = bool(int(os.getenv("BACKPRESSURE_REQUIRE_FULL_POOL", "1")))

    autoscale = AutoscaleConfig(
        dl_min=dl_min,
        dl_max=dl_max,
        db_min=db_min,
        db_max=db_max,
        dl_initial=dl_initial,
        db_initial=db_initial,
        backlog_high=backlog_high,
        backlog_low=backlog_low,
        db_wait_p95_high=db_wait_p95_high,
        db_wait_p95_low=db_wait_p95_low,
        dl_inc=dl_inc,
        dl_dec=dl_dec,
        db_inc=db_inc,
        db_dec=db_dec,
        interval_sec=interval_sec,
        hot_hysteresis=hot_hysteresis,
        cold_hysteresis=cold_hysteresis,
        backpressure_dbwait_p95_high=bp_high,
        backpressure_dbwait_p95_low=bp_low,
        backpressure_require_full_pool=bp_need_full,
    )

    dedup_strategy: DedupStrategy = os.getenv(
        "DEDUP_STRATEGY", "distinct_on").lower()  # type: ignore
    if dedup_strategy not in ("distinct_on", "group_by"):
        dedup_strategy = "distinct_on"

    create_temp_hash_index = bool(
        int(os.getenv("CREATE_TEMP_HASH_INDEX", "0")))
    geom_hash = bool(int(os.getenv("GEOM_HASH", "0")))

    pipeline_mode_env = bool(int(os.getenv("DB_PIPELINE_MODE", "0")))
    pipeline_mode = pipeline_mode_env and _libpq_supports_pipeline()

    dest_dir = Path(os.getenv("OA_DEST_DIR", "/tmp/oa_jobs"))
    write_arrow = bool(int(os.getenv("OA_WRITE_ARROW", "0")))
    arrow_path = Path(os.getenv("OA_ARROW_PATH", str(dest_dir / "addresses.arrow")))

    return Config(
        source=os.getenv("OA_SOURCE", "us/"),
        layer=os.getenv("OA_LAYER", "addresses"),
        dest_dir=dest_dir,
        http_max=http_max,
        login_timeout=float(os.getenv("OA_LOGIN_TIMEOUT", "30")),
        req_timeout=float(os.getenv("OA_REQ_TIMEOUT", "30")),

        dl_workers_max=dl_workers_max,
        dl_initial=dl_initial,
        dl_timeout=float(os.getenv("OA_DL_TIMEOUT", "60")),
        chunk_size=int(os.getenv("OA_CHUNK_SIZE", "8192")),
        max_retries=int(os.getenv("OA_MAX_RETRIES", "3")),
        backoff_factor=float(os.getenv("OA_BACKOFF_FACTOR", "2.0")),

        db_workers_max=db_workers_max,
        db_initial=db_initial,
        dsn=os.getenv("OA_DSN", "postgresql://user:password@localhost/db"),
        db_conn_timeout=float(os.getenv("OA_DB_CONN_TIMEOUT", "60")),
        db_pool_total=db_pool_total,
        db_pool_min=db_pool_min,
        db_pool_process_max=db_pool_process_max,

        process_queue_maxsize=process_queue_max,

        autoscale=autoscale,

        dedup_strategy=dedup_strategy,
        create_temp_hash_index=create_temp_hash_index,
        geom_hash=geom_hash,
        pipeline_mode=pipeline_mode,
        write_arrow=write_arrow,
        arrow_path=arrow_path,
    )
