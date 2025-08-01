from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# -----------------------------
# Fast validity helpers
# -----------------------------

# Postal code patterns (cheap, shape-only). Uppercased before matching.
_POSTAL_RE: Dict[str, re.Pattern[str]] = {
    "US": re.compile(r"^\d{5}(?:-\d{4})?$"),
    "CA": re.compile(r"^[A-Z]\d[A-Z][ -]?\d[A-Z]\d$"),
    "GB": re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$"),
    "AU": re.compile(r"^\d{4}$"),
    "NZ": re.compile(r"^\d{4}$"),
    "FR": re.compile(r"^\d{5}$"),
    "DE": re.compile(r"^\d{5}$"),
    "NL": re.compile(r"^\d{4}\s?[A-Z]{2}$"),
    "ES": re.compile(r"^\d{5}$"),
    "IT": re.compile(r"^\d{5}$"),
}

# Very fast sanity checks
_STATE2_RE = re.compile(r"^[A-Z]{2}$")  # e.g., US state code shape
# disallow ASCII controls
_CTRL_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
_HAS_WORD_RE = re.compile(r"[A-Za-z0-9]")  # some substance in strings
_MAX_FIELD_LEN = 256  # conservative upper bound per component


def _ok_len(s: Optional[str]) -> bool:
    return s is None or len(s) <= _MAX_FIELD_LEN


def _no_ctrl(s: Optional[str]) -> bool:
    return s is None or not _CTRL_RE.search(s)


def _has_content(s: Optional[str]) -> bool:
    return s is not None and bool(_HAS_WORD_RE.search(s))


@dataclass(slots=True)
class InputAddress:
    """Raw input address for normalization."""
    address_raw: str
    language_code: Optional[str] = "en"
    country_code: Optional[str] = "us"
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NormalizedRecord:
    """
    Normalized record aligned to the (simplified) persistence schema.

    • Component fields exist for validation/dedupe.
    • Writers persist ONLY `address_norm`, a deterministic, fully-normalized
      canonical string.
    """
    country_code: str
    postal_code: Optional[str]
    admin_area_1: Optional[str]
    admin_area_2: Optional[str]
    locality: Optional[str]
    dependent_locality: Optional[str]
    thoroughfare: Optional[str]
    premise: Optional[str]
    sub_premise: Optional[str]

    # private slot-backed cache (works with slots=True)
    _address_norm_cache: Optional[str] = field(
        default=None, init=False, repr=False)

    # ------------------------------------------------------------------ #
    # Construction helpers                                               #
    # ------------------------------------------------------------------ #
    @classmethod
    def from_input(cls, addr: InputAddress) -> "NormalizedRecord":
        # avoid circular import by importing here
        from .utils import normalize_one  # noqa: F401
        return normalize_one(addr)

    # ------------------------------------------------------------------ #
    # Canonical string (cached manually)                                 #
    # ------------------------------------------------------------------ #
    @property
    def address_norm(self) -> str:
        """Canonical, fully-normalized single-line address (lower-cased)."""
        if self._address_norm_cache is None:
            from .utils import build_canonical_string  # lazy import
            components: Dict[str, Optional[str]] = {
                "premise": self.premise,
                "thoroughfare": self.thoroughfare,
                "dependent_locality": self.dependent_locality,
                "locality": self.locality,
                "admin_area_2": self.admin_area_2,
                "admin_area_1": self.admin_area_1,
                "postal_code": self.postal_code,
            }
            self._address_norm_cache = build_canonical_string(
                components, self.country_code)
        return self._address_norm_cache

    # ------------------------------------------------------------------ #
    # CSV / writer interface                                             #
    # ------------------------------------------------------------------ #
    def to_csv_row(self) -> List[str]:
        """Writers expect exactly one column - the canonical string."""
        return [self.address_norm]

    # -----------------------------
    # Fast validity check
    # -----------------------------
    def fast_validate(self) -> Tuple[bool, Optional[str]]:
        """
        O(1) structural plausibility check. Does not hit libpostal or I/O.

        Returns:
            (ok, reason) where reason is None when ok is True.
        """
        cc = (self.country_code or "").strip().upper()

        # Country code must be 2 letters (ISO-like)
        if len(cc) != 2 or not cc.isalpha():
            return False, "invalid country_code"

        # At least one delivery-line hint
        if not (self.thoroughfare or self.premise):
            return False, "missing thoroughfare_or_premise"

        # Some locality/admin/postal signal
        if not (self.locality or self.admin_area_1 or self.admin_area_2 or self.postal_code):
            return False, "missing locality_admin_or_postal"

        # Quick field hygiene: length and control chars
        for name, val in (
            ("postal_code", self.postal_code),
            ("admin_area_1", self.admin_area_1),
            ("admin_area_2", self.admin_area_2),
            ("locality", self.locality),
            ("dependent_locality", self.dependent_locality),
            ("thoroughfare", self.thoroughfare),
            ("premise", self.premise),
            ("sub_premise", self.sub_premise),
        ):
            if not _ok_len(val):
                return False, f"{name}_too_long"
            if not _no_ctrl(val):
                return False, f"{name}_has_control_chars"

        # If postal_code is present, validate country-shaped pattern (when known)
        if self.postal_code:
            pc = self.postal_code.strip().upper()
            patt = _POSTAL_RE.get(cc)
            if patt and not patt.match(pc):
                return False, "postal_code_shape_mismatch"

        # US-specific shape for state (when present)
        if cc == "US" and self.admin_area_1:
            st = self.admin_area_1.strip().upper()
            if not _STATE2_RE.match(st):
                return False, "admin_area_1_not_two_letter_state"

        # Require some alnum content in at least one key field—not just punctuation/spaces
        if not (_has_content(self.thoroughfare) or _has_content(self.premise)):
            return False, "empty_delivery_line_content"

        return True, None

    def is_plausible(self) -> bool:
        """Convenience wrapper returning only a boolean."""
        ok, _ = self.fast_validate()
        return ok


# Header order for CSV output (single column)
CSV_HEADER: List[str] = ["address_norm"]
