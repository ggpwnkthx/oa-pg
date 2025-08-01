import io
import zipfile
from pathlib import Path
from typing import List, Optional, cast

import httpx
import psycopg
from psycopg import sql
import shapefile
from shapely.geometry import shape
from typing_extensions import LiteralString


# === CONFIGURATION ===
TMPDIR = Path("/tmp/gis")
BASEDIR = TMPDIR / "data"
STATE_URL = "https://www2.census.gov/geo/tiger/TIGER2021/STATE/tl_2021_us_state.zip"
COUNTY_URL = "https://www2.census.gov/geo/tiger/TIGER2021/COUNTY/tl_2021_us_county.zip"

# === SQL HELPER ===


def Q(s: str) -> sql.SQL:
    """
    Cast a Python str into a LiteralString so that
    psycopg.sql.SQL(...) accepts it without Pylance errors.
    """
    return sql.SQL(cast(LiteralString, s))


def execute_sql(conn: psycopg.Connection, sql_text: str) -> None:
    """
    Run a SQL command (or multiple statements) by wrapping the string
    in Q(...) so that execute() sees a Composed/Query, not a raw str.
    """
    with conn.cursor() as cur:
        print(f"[SQL] {sql_text.strip()[:60]}...")
        cur.execute(Q(sql_text))
    conn.commit()


# === OTHER HELPERS ===

def download_and_extract(url: str, dest: Path) -> None:
    """Download a ZIP from `url` and extract all files into `dest`."""
    print(f"[+] Downloading {url}")
    resp = httpx.get(url)
    resp.raise_for_status()
    dest.mkdir(parents=True, exist_ok=True)

    print(f"[+] Extracting to {dest}")
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(path=dest)


def clear_tmp(tmpdir: Path) -> None:
    """Remove all files in TMPDIR."""
    tmpdir.mkdir(parents=True, exist_ok=True)
    for f in tmpdir.iterdir():
        if f.is_file():
            f.unlink()


def infer_dbf_fields(shp_path: Path) -> list[str]:
    """
    Read the .dbf of a shapefile and return a list of
    "<name> TEXT" strings for CREATE TABLE.
    """
    reader = shapefile.Reader(str(shp_path))
    fields = reader.fields[1:]  # skip deletion flag
    return [f"{name} TEXT" for name, _, _, _ in fields]


def load_shapefile_to_staging(
    conn: psycopg.Connection,
    shp_path: Path,
    schema: str,
    table: str,
) -> None:
    """
    Read every record from shp_path and insert into
    tiger_staging.<table> (<dbf fields>, the_geom).
    """
    print(f"[+] Loading {shp_path.name} into {schema}.{table}")
    reader = shapefile.Reader(str(shp_path))
    field_names = [f[0] for f in reader.fields[1:]]
    columns = ", ".join(field_names + ["the_geom"])
    placeholder = ", ".join(["%s"] * (len(field_names) + 1))
    insert_sql = (
        f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholder});"
    )

    with conn.cursor() as cur:
        for sr in reader.shapeRecords():
            rec = (
                sr.record.as_dict()
                if hasattr(sr.record, "as_dict")
                else dict(zip(field_names, sr.record))
            )
            geom = shape(sr.shape.__geo_interface__)
            wkb = geom.wkb
            vals = [rec[name] for name in field_names] + [psycopg.Binary(wkb)]
            cur.execute(Q(insert_sql), vals)
    conn.commit()


# === PROCESSORS ===

def process_states(conn: psycopg.Connection) -> None:
    clear_tmp(TMPDIR)
    execute_sql(conn, "DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    execute_sql(conn, "CREATE SCHEMA tiger_staging;")

    state_dir = BASEDIR / "www2.census.gov" / \
        "geo" / "tiger" / "TIGER2021" / "STATE"
    download_and_extract(STATE_URL, state_dir)

    clear_tmp(TMPDIR)
    for f in state_dir.glob("tl_*state.*"):
        f.rename(TMPDIR / f.name)

    execute_sql(
        conn,
        """
        CREATE TABLE IF NOT EXISTS tiger_data.state_all (
            CONSTRAINT pk_state_all PRIMARY KEY (statefp),
            CONSTRAINT uidx_state_all_stusps UNIQUE (stusps),
            CONSTRAINT uidx_state_all_gid UNIQUE (gid)
        ) INHERITS(tiger.state);
        """,
    )

    shp = TMPDIR / "tl_2021_us_state.shp"
    field_defs = infer_dbf_fields(shp)
    execute_sql(conn, "DROP TABLE IF EXISTS tiger_staging.state;")
    execute_sql(
        conn,
        "CREATE TABLE tiger_staging.state ("
        + ", ".join(field_defs)
        + ", the_geom geometry(Geometry,4269));",
    )

    load_shapefile_to_staging(conn, shp, "tiger_staging", "state")

    execute_sql(conn, "SELECT loader_load_staged_data('state','state_all');")
    execute_sql(
        conn,
        "CREATE INDEX IF NOT EXISTS tiger_data_state_all_the_geom_gist "
        "ON tiger_data.state_all USING gist(the_geom);",
    )
    execute_sql(conn, "VACUUM ANALYZE tiger_data.state_all;")


def process_counties(conn: psycopg.Connection) -> None:
    clear_tmp(TMPDIR)
    execute_sql(conn, "DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
    execute_sql(conn, "CREATE SCHEMA tiger_staging;")

    county_dir = BASEDIR / "www2.census.gov" / \
        "geo" / "tiger" / "TIGER2021" / "COUNTY"
    download_and_extract(COUNTY_URL, county_dir)

    clear_tmp(TMPDIR)
    for f in county_dir.glob("tl_*county.*"):
        f.rename(TMPDIR / f.name)

    execute_sql(
        conn,
        """
        CREATE TABLE IF NOT EXISTS tiger_data.county_all (
            CONSTRAINT pk_tiger_data_county_all PRIMARY KEY (cntyidfp),
            CONSTRAINT uidx_tiger_data_county_all_gid UNIQUE (gid)
        ) INHERITS(tiger.county);
        """,
    )

    shp = TMPDIR / "tl_2021_us_county.shp"
    field_defs = infer_dbf_fields(shp)
    execute_sql(conn, "DROP TABLE IF EXISTS tiger_staging.county;")
    execute_sql(
        conn,
        "CREATE TABLE tiger_staging.county ("
        + ", ".join(field_defs)
        + ", the_geom geometry(Geometry,4269));",
    )

    load_shapefile_to_staging(conn, shp, "tiger_staging", "county")

    execute_sql(
        conn, "ALTER TABLE tiger_staging.county RENAME COLUMN geoid TO cntyidfp;")
    execute_sql(conn, "SELECT loader_load_staged_data('county','county_all');")
    execute_sql(
        conn,
        "CREATE INDEX IF NOT EXISTS tiger_data_county_the_geom_gist "
        "ON tiger_data.county_all USING gist(the_geom);",
    )
    execute_sql(
        conn,
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "uidx_tiger_data_county_all_statefp_countyfp "
        "ON tiger_data.county_all(statefp,countyfp);",
    )

    execute_sql(
        conn,
        """
        CREATE TABLE IF NOT EXISTS tiger_data.county_all_lookup (
            CONSTRAINT pk_county_all_lookup PRIMARY KEY (st_code, co_code)
        ) INHERITS(tiger.county_lookup);
        """,
    )
    execute_sql(conn, "VACUUM ANALYZE tiger_data.county_all;")
    execute_sql(
        conn,
        """
        INSERT INTO tiger_data.county_all_lookup(st_code, state, co_code, name)
        SELECT CAST(s.statefp AS integer), s.abbrev, CAST(c.countyfp AS integer), c.name
          FROM tiger_data.county_all AS c
          JOIN state_lookup AS s ON s.statefp = c.statefp;
        """,
    )
    execute_sql(conn, "VACUUM ANALYZE tiger_data.county_all_lookup;")


def post_load_maintenance(conn: psycopg.Connection) -> None:
    print("== Post-load maintenance ==")
    execute_sql(conn, "SELECT install_missing_indexes();")
    print("✓ install_missing_indexes() completed")
    execute_sql(conn, "ANALYZE;")
    print("✓ ANALYZE completed")
