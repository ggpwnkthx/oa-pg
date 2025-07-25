"""Parsing helpers that convert OA features into binary COPY rows."""

from __future__ import annotations

import io
import logging
import struct
from typing import Any, List, Tuple

import orjson
from shapely.geometry import shape

from oa_etl.clients.openaddresses import Job
from oa_etl.addressing import is_valid_address
from .hashing import compute_raw_hash_bytes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

METADATA_HEADER = ["job_id", "source_name", "hash", "canonical"]
BATCH_LINES = 25_000


def parse_lines_to_binary_rows(
    lines: List[str],
    job: Job,
) -> Tuple[List[bytes], int]:
    """Return binary COPY rows and count from a list of GeoJSON lines."""

    rows: List[bytes] = []
    features_processed = 0
    first_hash_hex_logged = False

    for line in lines:
        try:
            _, region, place = job.source_name.split("/")
            feat = orjson.loads(line)
            geom = feat.get("geometry")
            props = feat.get("properties", {})
            if not props["region"]:
                props["region"] = region.upper()
            if not props["city"]:
                props["city"] = place.replace("city_of_", "").replace("_", " ").upper()

            parsed = is_valid_address(props)
            if not parsed:
                continue

            addr_vals = [
                parsed.get("AddressNumberPrefix"),
                parsed.get("AddressNumber"),
                parsed.get("AddressNumberSuffix"),
                parsed.get("StreetNamePreDirectional"),
                parsed.get("StreetNamePreModifier"),
                parsed.get("StreetNamePreType"),
                parsed.get("StreetName"),
                parsed.get("StreetNamePostType"),
                parsed.get("StreetNamePostModifier"),
                parsed.get("StreetNamePostDirectional"),
                parsed.get("BuildingName"),
                parsed.get("CornerOf"),
                parsed.get("IntersectionSeparator"),
                parsed.get("LandmarkName"),
                parsed.get("PlaceName"),
                parsed.get("SubaddressType"),
                parsed.get("SubaddressIdentifier"),
                parsed.get("OccupancyType"),
                parsed.get("OccupancyIdentifier"),
                parsed.get("USPSBoxGroupID"),
                parsed.get("USPSBoxGroupType"),
                parsed.get("USPSBoxID"),
                parsed.get("USPSBoxType"),
                parsed.get("StateName"),
                parsed.get("ZipCode"),
            ]

            canonical = (
                f"{props.get('number', '')} {props.get('street', '')}"
                f" {props.get('unit', '')} {props.get('city', '')}"
                f" {props.get('region', '')} {props.get('postcode', '')}"
            ).strip()
            meta_vals = [job.id, job.source_name, props.get("hash"), canonical]

            geom_val = None
            if geom:
                geom_val = shape(geom).wkb

            raw_hash_bytes = compute_raw_hash_bytes(addr_vals)
            if not first_hash_hex_logged:
                logger.debug(("first_hash", raw_hash_bytes.hex(), addr_vals))
                first_hash_hex_logged = True

            row_vals: List[Any] = [*addr_vals, *
                                   meta_vals, geom_val, raw_hash_bytes]

            buf = io.BytesIO()
            buf.write(struct.pack("!h", len(row_vals)))
            for v in row_vals:
                if v is None:
                    buf.write(struct.pack("!i", -1))
                else:
                    if isinstance(v, int):
                        data_bytes = struct.pack("<i", v)
                    elif isinstance(v, bytes):
                        data_bytes = v
                    else:
                        data_bytes = str(v).encode()
                    buf.write(struct.pack("!i", len(data_bytes)))
                    buf.write(data_bytes)

            rows.append(buf.getvalue())
            features_processed += 1

        except Exception as e:
            logger.error("Job %s: error processing feature: %s", job.id, e)

    return rows, features_processed
