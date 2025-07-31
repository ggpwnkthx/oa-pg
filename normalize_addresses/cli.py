import argparse
import asyncio
import logging
from pathlib import Path

from .sources.openaddresses import OpenAddressesSource
from .pipeline import NormalizationPipeline
from .logging_config import logger


def build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Normalize addresses with libpostal.")
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    p.add_argument(
        "--status-interval",
        type=float,
        default=5.0,
        help="Seconds between pipeline status logs (default: 5.0; set 0 to disable)",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    oa = sub.add_parser(
        "openaddresses", help="Process from OpenAddresses batch API")
    oa.add_argument("--source", required=True,
                    help="OA source id, e.g. 'us/ca'")
    oa.add_argument("--layer", default="addresses",
                    help="OA layer (default: addresses)")
    oa.add_argument("--out", required=True, type=Path,
                    help="Output CSV.gz path")
    oa.add_argument("--max-connections", type=int,
                    default=16, help="HTTP concurrency")
    oa.add_argument("--jobs-limit", type=int,
                    default=None, help="Limit OA jobs")
    oa.add_argument("--token", default=None, help="OA bearer token")
    oa.add_argument("--concurrency", type=int, default=16,
                    help="Normalization threads")

    return p


async def main_async(args: argparse.Namespace) -> None:
    # Configure logging level early
    root_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.getLogger().setLevel(root_level)
    logger.setLevel(root_level)
    logging.getLogger("httpx").setLevel(
        logging.INFO if root_level > logging.DEBUG else logging.DEBUG)

    if args.cmd == "openaddresses":
        src = OpenAddressesSource(
            source=args.source,
            layer=args.layer,
            max_connections=args.max_connections,
            jobs_limit=args.jobs_limit,
            token=args.token,
        )
    else:
        raise SystemExit(f"Unknown command: {args.cmd}")

    pipeline = NormalizationPipeline(
        source=src,
        out_path=args.out,
        concurrency=args.concurrency,
        status_interval=args.status_interval,
    )
    await pipeline.run()


def main() -> None:
    parser = build_cli()
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
