import sys
from pathlib import Path
from dotenv import load_dotenv

from .preflight import preflight_local_tools, preflight_server
from .loader import generate_nation_loader_script, execute_loader_script, post_load_maintenance
import config

# Load .env
load_dotenv()


def setup_tiger_geocoder_nation() -> None:
    preflight_local_tools()
    preflight_server(config.DSN, config.EXTRA_PSQL_ARGS)
    generate_nation_loader_script(
        config.DSN,
        Path(config.OUTPUT_SCRIPT),
        config.OS_OVERRIDE,
        config.EXTRA_PSQL_ARGS,
    )
    execute_loader_script(Path(config.OUTPUT_SCRIPT))
    post_load_maintenance(
        config.DSN,
        config.EXTRA_PSQL_ARGS,
        analyze=config.ANALYZE_AFTER_LOAD,
    )
    print("== Done ==")


if __name__ == "__main__":
    try:
        setup_tiger_geocoder_nation()
    except Exception as e:
        print("\nFAILED:", e, file=sys.stderr)
        sys.exit(1)
