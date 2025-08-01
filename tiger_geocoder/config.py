# tiger_geocoder/config.py

import os
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()

# Single-source DSN—same env var as normalize_addresses uses
DSN = os.getenv("PG_DSN")
if not DSN:
    DSN = 'postgresql://root:password@localhost:5432/postgres'

# Output script path, OS override, post-load analyze flag
OUTPUT_SCRIPT = os.getenv("TIGER_NATION_SCRIPT", "/tmp/load_nation.sh")
OS_OVERRIDE = os.getenv("TIGER_LOADER_OS")    # 'sh' or 'windows'
ANALYZE_AFTER_LOAD = os.getenv(
    "ANALYZE_AFTER_LOAD", "true").lower() in ("true", "1", "yes")

# Extra psql args (e.g. ["--set", "SEARCH_PATH=public,tiger"])
EXTRA_PSQL_ARGS = None
