from __future__ import annotations

import hashlib
from typing import List, Optional


def compute_raw_hash_bytes(addr_vals: List[Optional[str]]) -> bytes:
    field_strings = [str(v) if v is not None else "" for v in addr_vals]
    concat_str = "|".join(field_strings)
    return hashlib.sha256(concat_str.encode("utf-8")).digest()
