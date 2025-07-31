import re
import unicodedata
from typing import Optional, Sequence, Tuple, List, Dict, Any
from postal.parser import parse_address

from normalize_addresses.models import InputAddress, NormalizedRecord

# Whitespace cleanup
SPACE_RE = re.compile(r"\s+")

# USPS state abbreviations for the 50 U.S. states + DC
STATE_ABBREVIATIONS: Dict[str, str] = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
}


def _ascii_fold(s: str) -> str:
    """Strip accents using NFKD Unicode normalization."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _clean_ws(s: str) -> str:
    """Collapse all whitespace to single spaces and trim."""
    return SPACE_RE.sub(" ", s).strip()


def normalize_region(region: Optional[str], country_code: Optional[str]) -> str:
    """
    Given a raw region or state name/abbreviation, return a two-letter
    USPS state code when country_code == "US". Otherwise, return the
    input (trimmed and upper-cased).
    """
    if not region:
        return ""
    r = region.strip()
    if country_code and country_code.upper() == "US":
        title = r.title()
        if title in STATE_ABBREVIATIONS:
            return STATE_ABBREVIATIONS[title]
        upper = r.upper()
        if upper in STATE_ABBREVIATIONS.values():
            return upper
        for full, abbr in STATE_ABBREVIATIONS.items():
            if full.upper() == upper:
                return abbr
        return upper[:2]
    return r.upper()


def _pick(props: Dict[str, str], keys: Sequence[str]) -> Optional[str]:
    """Return first non-empty value for keys in props."""
    for k in keys:
        v = props.get(k)
        if v:
            return v
    return None


def _map_libpostal_components(
    pairs: List[Tuple[str, str]]
) -> Dict[str, Optional[str]]:
    """
    Map libpostal output labels to our component names.
    Returns a dict whose values may be None.
    """
    d: Dict[str, str] = {}
    for label, value in pairs:
        d[label] = _clean_ws(value.lower())

    out: Dict[str, Optional[str]] = {}

    # Thoroughfare (street)
    out["thoroughfare"] = _pick(
        d,
        [
            "road",
            "pedestrian",
            "footway",
            "path",
            "residential",
            "cycleway",
            "construction",
        ],
    )

    # Premise (house number + building name)
    house_number = d.get("house_number")
    house = _pick(d, ["house", "building"])
    if house_number and house:
        out["premise"] = f"{house_number} {house}"
    else:
        out["premise"] = house_number or house

    # Sub-premise (unit / level)
    unit_bits = [x for x in (d.get("level"), d.get("unit")) if x]
    out["sub_premise"] = " ".join(unit_bits) if unit_bits else None

    # Locality hierarchy
    out["dependent_locality"] = _pick(
        d,
        [
            "suburb",
            "neighbourhood",
            "neighborhood",
            "hamlet",
            "quarter",
            "borough",
        ],
    )
    out["locality"] = _pick(
        d,
        [
            "city",
            "town",
            "village",
            "municipality",
            "city_district",
            "district",
        ],
    )

    # Admin areas
    out["admin_area_2"] = _pick(
        d, ["county", "state_district", "province_district"])
    out["admin_area_1"] = _pick(d, ["state", "province", "region", "island"])

    # Postal code & country
    out["postal_code"] = d.get("postcode")
    out["country"] = d.get("country")

    return out


def build_canonical_string(
    components: Dict[str, Optional[str]], country_code: Optional[str]
) -> str:
    """
    Build a deterministic normalized address string using stable ordering.
    """
    parts: List[str] = []
    pt = " ".join(
        p for p in [components.get("premise"), components.get("thoroughfare")] if p
    )
    if pt:
        parts.append(pt)

    for key in (
        "dependent_locality",
        "locality",
        "admin_area_2",
        "admin_area_1",
        "postal_code",
    ):
        v = components.get(key)
        if v:
            parts.append(v)

    if country_code:
        parts.append(country_code.lower())

    return _clean_ws(_ascii_fold(", ".join(parts)).lower())


def normalize_one(addr: InputAddress) -> NormalizedRecord:
    """
    Parse with libpostal, map to schema, back-fill region/locality, then build canonical string.
    """
    # avoid top-level import cycle
    from .models import NormalizedRecord  # noqa: F811

    # 1) run libpostal
    parsed_pairs = parse_address(addr.address_raw)
    mapped = _map_libpostal_components(parsed_pairs)

    # 2) back-fill admin_area_1
    if not mapped.get("admin_area_1"):
        raw_reg = addr.extras.get("region") or addr.extras.get("state")
        reg_norm = normalize_region(str(raw_reg), addr.country_code)
        if reg_norm:
            mapped["admin_area_1"] = reg_norm

    # 3) back-fill locality
    if not mapped.get("locality"):
        raw_city = addr.extras.get("city") or addr.extras.get("locality")
        if raw_city:
            mapped["locality"] = _clean_ws(str(raw_city)).title()

    # 4) build canonical
    cc = (addr.country_code or "").strip().upper()
    canonical = build_canonical_string(mapped, cc or None)

    return NormalizedRecord(
        country_code=cc,
        postal_code=mapped.get("postal_code"),
        admin_area_1=mapped.get("admin_area_1"),
        admin_area_2=mapped.get("admin_area_2"),
        locality=mapped.get("locality"),
        dependent_locality=mapped.get("dependent_locality"),
        thoroughfare=mapped.get("thoroughfare"),
        premise=mapped.get("premise"),
        sub_premise=mapped.get("sub_premise"),
        address_raw=_clean_ws(addr.address_raw),
        address_norm=canonical,
    )
