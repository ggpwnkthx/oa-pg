from .utils import assert_tool, psql_exec_sql


def preflight_local_tools() -> None:
    print("== Preflight: local tools ==")
    for tool in ("psql", "shp2pgsql", "wget", "unzip"):
        assert_tool(tool)


def preflight_server(
    conn_str: str,
) -> None:
    print("== Preflight: server extensions ==")

    # Connectivity test
    connectivity_sql = """
DO $$
BEGIN
  PERFORM 1;
END$$;
"""
    psql_exec_sql(connectivity_sql, conn_str)

    # Core extensions
    psql_exec_sql("CREATE EXTENSION IF NOT EXISTS postgis;", conn_str)
    print("✓ postgis extension ready")

    psql_exec_sql("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;", conn_str)
    print("✓ fuzzystrmatch extension ready")

    # Optional address standardizer
    try:
        psql_exec_sql(
            "CREATE EXTENSION IF NOT EXISTS address_standardizer;", conn_str)
        psql_exec_sql(
            "CREATE EXTENSION IF NOT EXISTS address_standardizer_data_us;", conn_str)
        print("✓ address_standardizer (+_data_us) ready")
    except Exception as e:
        print("! address_standardizer not installed; continuing (optional). Details:\n", e)

    # Tiger geocoder
    psql_exec_sql(
        "CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;", conn_str)
    print("✓ postgis_tiger_geocoder extension ready")

    # Show PostGIS version
    ver = psql_exec_sql("SELECT PostGIS_Full_Version();", conn_str)
    print(ver.strip())
