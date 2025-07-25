from __future__ import annotations

import logging
import os


def configure_logging(level: str | None = None) -> None:
    """
    Configure root logging once. Level can be given explicitly or taken from
    LOG_LEVEL env var (default INFO).
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d | %(message)s",
    )
