from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Result:
    job_id: str
    ok: bool
    error: str | None = None
