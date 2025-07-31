from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


@dataclass(slots=True)
class InputAddress:
    """Raw input address for normalization."""
    address_raw: str
    country_code: Optional[str] = None
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NormalizedRecord:
    """Normalized record aligned to the Postgres schema."""
    country_code: str
    postal_code: Optional[str]
    admin_area_1: Optional[str]
    admin_area_2: Optional[str]
    locality: Optional[str]
    dependent_locality: Optional[str]
    thoroughfare: Optional[str]
    premise: Optional[str]
    sub_premise: Optional[str]
    address_raw: str
    address_norm: str

    @classmethod
    def from_input(cls, addr: InputAddress) -> 'NormalizedRecord':
        # avoid circular import by importing here
        from .utils import normalize_one  # noqa: F811
        return normalize_one(addr)

    def to_csv_row(self) -> List[str]:
        def nz(x: Optional[str]) -> str:
            return x or ""
        return [
            nz(self.country_code),
            nz(self.postal_code),
            nz(self.admin_area_1),
            nz(self.admin_area_2),
            nz(self.locality),
            nz(self.dependent_locality),
            nz(self.thoroughfare),
            nz(self.premise),
            nz(self.sub_premise),
            self.address_raw,
            self.address_norm,
        ]


# Header order for CSV output
CSV_HEADER: List[str] = [
    "country_code",
    "postal_code",
    "admin_area_1",
    "admin_area_2",
    "locality",
    "dependent_locality",
    "thoroughfare",
    "premise",
    "sub_premise",
    "address_raw",
    "address_norm",
]
