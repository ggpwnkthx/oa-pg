from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# Sentinel objects to terminate workers
# ──────────────────────────────────────────────────────────────────────────────
STOP_DOWNLOAD: object = object()      # download workers
STOP_PARSE: object    = object()      # parse workers
STOP_BATCH: object    = object()      # DB batch‑writer
STOP_PROCESS: object  = object()
