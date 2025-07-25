from __future__ import annotations

import argparse

from oa_etl.logging_setup import configure_logging
from oa_etl.pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="OpenAddresses async ETL pipeline")
    p.add_argument(
        "--log-level",
        default=None,
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Defaults to $LOG_LEVEL or INFO.",
    )
    return p


async def main() -> None:
    args = build_parser().parse_args()
    configure_logging(args.log_level)

    # run the pipeline (env-backed config)
    await run_pipeline()
