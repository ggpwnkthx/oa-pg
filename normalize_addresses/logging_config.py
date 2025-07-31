import logging
import os

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(
    logging, LEVEL, logging.INFO), format=LOG_FORMAT)
logger = logging.getLogger("addr-normalize")
