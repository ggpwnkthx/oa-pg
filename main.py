"""Entry point for invoking the OpenAddresses ETL pipeline via the CLI."""

from __future__ import annotations

import asyncio

from oa_etl.cli import main as cli_main

if __name__ == "__main__":
    asyncio.run(cli_main())
