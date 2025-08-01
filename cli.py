#!/usr/bin/env python3
"""
Unified CLI for Address Normalization and TIGER geocoding
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Normalize Addresses CLI
from normalize_addresses.cli import main as normalize_main

# TIGER Geocoder configuration and commands
from tiger_geocoder.config import DSN, OUTPUT_SCRIPT, OS_OVERRIDE, EXTRA_PSQL_ARGS
from tiger_geocoder.preflight import preflight_local_tools, preflight_server
from tiger_geocoder.loader import (
    generate_nation_loader_script,
    execute_loader_script,
    post_load_maintenance,
)


def build_root_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ESQ CLI for Address Normalization and TIGER geocoding"
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO").upper(),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # Normalize subcommand forwards to normalize_addresses.cli
    sub.add_parser(
        "normalize", help="Normalize addresses with libpostal"
    )

    # TIGER geocoder subcommand
    tg = sub.add_parser(
        "tiger", help="Run TIGER geocoder nation loader"
    )
    tg.add_argument(
        "--dsn", default=DSN,
        help="PostgreSQL DSN (env OA_DSN)"
    )
    tg.add_argument(
        "--output-script",
        type=Path,
        default=Path(OUTPUT_SCRIPT),
        help="Output loader script path",
    )
    tg.add_argument(
        "--os-override",
        choices=["sh", "windows"],
        default=OS_OVERRIDE,
        help="Override OS for loader script (sh or windows)",
    )
    tg.add_argument(
        "--no-analyze",
        action="store_false",
        dest="analyze",
        help="Skip ANALYZE after load",
    )
    return parser


def main() -> None:
    parser = build_root_parser()
    args, rest = parser.parse_known_args()

    # Configure logging
    level = getattr(logging, args.log_level, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger().setLevel(level)

    if args.cmd == "normalize":
        # Forward remaining args to the normalize_addresses CLI
        sys.argv = [sys.argv[0]] + rest
        try:
            normalize_main()
        except KeyboardInterrupt:
            import normalize_addresses.logging_config as _lc
            _lc.logger.warning("Interrupted.")

    elif args.cmd == "tiger":
        conn_str = args.dsn
        preflight_local_tools()
        preflight_server(conn_str, EXTRA_PSQL_ARGS)
        generate_nation_loader_script(
            conn_str,
            args.output_script,
            args.os_override,
            EXTRA_PSQL_ARGS,
        )
        execute_loader_script(args.output_script)
        post_load_maintenance(
            conn_str,
            EXTRA_PSQL_ARGS,
            analyze=args.analyze,
        )
        print("== Done ==")

    else:
        parser.error(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
