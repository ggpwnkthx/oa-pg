from __future__ import annotations

import json
import re
import logging
from typing import Any

import usaddress
from scourgify import normalize_address_record

logger = logging.getLogger(__name__)

US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR"
}
STATE_NAME_TO_CODE = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA",
    "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC", "Puerto Rico": "PR"
}
STATE_CODE_TO_NAME = {v: k for k, v in STATE_NAME_TO_CODE.items()}
ZIP_REGEX = re.compile(r"^\d{5}(-\d{4})?$")

ADDRESS_HEADER = [
    "address_number_prefix",
    "address_number",
    "address_number_suffix",
    "street_name_pre_directional",
    "street_name_pre_modifier",
    "street_name_pre_type",
    "street_name",
    "street_name_post_type",
    "street_name_post_modifier",
    "street_name_post_directional",
    "building_name",
    "corner_of",
    "intersection_separator",
    "landmark_name",
    "place_name",
    "subaddress_type",
    "subaddress_identifier",
    "occupancy_type",
    "occupancy_identifier",
    "usps_box_group_id",
    "usps_box_group_type",
    "usps_box_id",
    "usps_box_type",
    "state_name",
    "zipcode",
]

ADDRESS_METADATA_HEADER = [
    "job_id",
    "source_name",
    "hash",
    "canonical",
    "geometry",
]

ALL_COLUMNS = ADDRESS_HEADER + ADDRESS_METADATA_HEADER


def is_valid_address(props: dict[str, Any]) -> dict[str, str] | None:
    """Returns parsed address dict if valid, else None."""
    for f in ("number", "street", "city", "region", "postcode"):
        if not props.get(f) or not str(props[f]).strip():
            return None
    if not str(props["number"]).isdigit():
        return None

    region = str(props["region"]).strip()
    if not region or region.isnumeric():
        return None
    if (
        region.upper() not in US_STATE_CODES
        and STATE_NAME_TO_CODE.get(region.title()) not in US_STATE_CODES
    ):
        return None

    if not ZIP_REGEX.match(str(props["postcode"])):
        return None

    try:
        norm = normalize_address_record(
            f"{props['number']} {props['street']} {props.get('unit', '')} "
            f"{props['city']} {props['region']} {props['postcode']}".upper(),
            long_hand=True,
        )
        parts = [norm["address_line_1"]]
        if norm.get("address_line_2"):
            parts.append(norm["address_line_2"])
        parts.append(f"{norm['city']}, {norm['state']} {norm['postal_code']}")
        cleaned = ", ".join(parts)

        parsed, _ = usaddress.tag(cleaned)
        if "AddressNumber" not in parsed or "StreetName" not in parsed:
            return None
        return parsed

    except Exception:
        return None


def to_csv_cell(value: Any) -> str | None:
    """Efficiently serialize any JSON-serializable Python value to a CSV-compatible string."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        esc = value.replace('"', '""')
        if any(c in esc for c in [",", "\n", '"']):
            return f'"{esc}"'
        return esc
    if isinstance(value, (dict, list)):
        return f'"{json.dumps(value, separators=(",", ":"))}"'
    return str(value)
