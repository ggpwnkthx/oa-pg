from __future__ import annotations

import threading
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Iterable, Tuple


def _percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return float("nan")
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return d0 + d1


def pct_summary(values: Iterable[float]) -> Dict[str, float]:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return {"count": 0, "min": float("nan"), "p50": float("nan"),
                "p95": float("nan"), "p99": float("nan"), "max": float("nan")}
    return {
        "count": len(vals),
        "min": vals[0],
        "p50": _percentile(vals, 50),
        "p95": _percentile(vals, 95),
        "p99": _percentile(vals, 99),
        "max": vals[-1],
    }


@dataclass
class Metrics:
    lock: threading.Lock = field(default_factory=threading.Lock)

    jobs_total: int = 0
    jobs_succeeded: int = 0
    jobs_failed: int = 0
    jobs_zero_features: int = 0

    features_total: int = 0
    bytes_downloaded: int = 0

    # stage -> list of durations
    stage_durations: Dict[str, List[float]] = field(
        default_factory=lambda: defaultdict(list))
    # rows/s for big COPYs (optional)
    copy_rows_per_sec: List[float] = field(default_factory=list)

    # per-job DB wait
    db_wait_seconds: List[float] = field(default_factory=list)

    # lazy skip stats
    lazy_skipped_jobs_total: int = 0
    lazy_skipped_jobs_seconds_sum: float = 0.0

    # error classification
    errors_by_type: Counter[str] = field(default_factory=Counter)

    def inc(self, attr: str, value: int = 1) -> None:
        with self.lock:
            setattr(self, attr, getattr(self, attr) + value)

    def add_bytes(self, n: int) -> None:
        with self.lock:
            self.bytes_downloaded += n

    def add_features(self, n: int) -> None:
        with self.lock:
            self.features_total += n
            if n == 0:
                self.jobs_zero_features += 1

    def observe_stage(self, stage: str, duration: float) -> None:
        with self.lock:
            self.stage_durations[stage].append(duration)

    def observe_copy_rps(self, rps: float) -> None:
        with self.lock:
            self.copy_rows_per_sec.append(rps)

    def observe_db_wait(self, wait: float) -> None:
        with self.lock:
            self.db_wait_seconds.append(wait)

    def inc_lazy_skipped(self, seconds: float) -> None:
        with self.lock:
            self.lazy_skipped_jobs_total += 1
            self.lazy_skipped_jobs_seconds_sum += seconds

    def record_error(self, exc: BaseException) -> None:
        with self.lock:
            self.errors_by_type[type(exc).__name__] += 1

    def summary(self) -> Tuple[str, Dict]:
        with self.lock:
            stage_stats = {
                stage: pct_summary(durations)
                for stage, durations in self.stage_durations.items()
            }
            copy_rps_stats = pct_summary(self.copy_rows_per_sec)
            db_wait_stats = pct_summary(self.db_wait_seconds)
            res = {
                "jobs_total": self.jobs_total,
                "jobs_succeeded": self.jobs_succeeded,
                "jobs_failed": self.jobs_failed,
                "jobs_zero_features": self.jobs_zero_features,
                "features_total": self.features_total,
                "bytes_downloaded": self.bytes_downloaded,
                "stage_stats": stage_stats,
                "copy_rows_per_sec": copy_rps_stats,
                "db_wait_seconds": db_wait_stats,
                "lazy_skipped_jobs_total": self.lazy_skipped_jobs_total,
                "lazy_skipped_jobs_seconds_sum": self.lazy_skipped_jobs_seconds_sum,
                "errors_by_type": dict(self.errors_by_type),
            }

        lines = []
        lines.append("===== METRICS SUMMARY =====")
        lines.append(f"Jobs       : total={res['jobs_total']}  ok={res['jobs_succeeded']}  "
                     f"fail={res['jobs_failed']}  zero_features={res['jobs_zero_features']}")
        lines.append(f"Features   : total={res['features_total']:,}")
        lines.append(
            f"Downloaded : {res['bytes_downloaded'] / (1024*1024):.2f} MiB")
        lines.append("")
        lines.append("Per-stage timings (seconds):")
        for stage, stats in stage_stats.items():
            lines.append(
                f"  {stage:20s} "
                f"count={stats['count']:6d}  "
                f"min={stats['min']:.4f}  p50={stats['p50']:.4f}  "
                f"p95={stats['p95']:.4f}  p99={stats['p99']:.4f}  max={stats['max']:.4f}"
            )
        lines.append("")
        if copy_rps_stats["count"]:
            lines.append("COPY rows/sec:")
            lines.append(
                f"  p50={copy_rps_stats['p50']:.2f}  "
                f"p95={copy_rps_stats['p95']:.2f}  "
                f"p99={copy_rps_stats['p99']:.2f}  "
                f"max={copy_rps_stats['max']:.2f}"
            )
        if db_wait_stats["count"]:
            lines.append("")
            lines.append("DB wait seconds:")
            lines.append(
                f"  p50={db_wait_stats['p50']:.3f}  "
                f"p95={db_wait_stats['p95']:.3f}  "
                f"p99={db_wait_stats['p99']:.3f}  "
                f"max={db_wait_stats['max']:.3f}"
            )
        if res["lazy_skipped_jobs_total"]:
            lines.append("")
            lines.append("Lazy skipped jobs:")
            lines.append(
                f"  total={res['lazy_skipped_jobs_total']}  "
                f"seconds_sum={res['lazy_skipped_jobs_seconds_sum']:.3f}"
            )
        if res["errors_by_type"]:
            lines.append("")
            lines.append("Errors by type:")
            for k, v in sorted(res["errors_by_type"].items(), key=lambda kv: kv[1], reverse=True):
                lines.append(f"  {k}: {v}")

        return "\n".join(lines), res

    def stage_percentile(self, stage: str, p: float) -> float:
        with self.lock:
            vals = self.stage_durations.get(stage, [])
            if not vals:
                return float("nan")
            sorted_vals = sorted(vals)
            return _percentile(sorted_vals, p)
