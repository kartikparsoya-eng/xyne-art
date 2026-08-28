#!/usr/bin/env python3
"""
metric_series_diff.py — Gate C: metric-series differential.

The wedge/metrics checks assert a metric NAME is *emitted* (present at all).
They never assert the LABEL cardinality or the family-set against TS — so a
whole label series can go missing silently. The 2026-08-28 flush-stats bug was
exactly that: rust merged `recordSyncFlushStats`/`#recordAsyncFlushStats` into
one function and left the async recorder unwired (`metrics_callback = None`), so
the `flush.type="async"` series never appeared, while TS emits it. The count
gate was green because `...cvr_flush_time..._count` (the sync series) existed.

This gate scrapes `/metrics` from BOTH the rust candidate and the TS reference
after the release load and asserts:
  1. FAMILY parity — every metric family TS exposes also exists on rust (rust is
     not missing a whole metric);
  2. LABEL parity on differential-sensitive families — e.g. the set of
     `flush.type` values on `zero_sync_cvr_flush_time_seconds_count` must match
     (catches the missing `async` series).

Absolute counter VALUES legitimately differ (candidate vs mirror see different
traffic), so this gate diffs the PRESENCE structure, not the magnitudes.

Runs against the live pair (ab_common containers); SKIPs if either /metrics is
unavailable.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import Report, RUST_CONTAINER, TS_CONTAINER  # noqa: E402
from gate_introspect import scrape_metrics, label_values  # noqa: E402

TAG = os.environ.get("ART_TAG", time.strftime("%Y%m%d-%H%M%S"))

# Families whose LABEL cardinality must match TS (not just be present). The
# `flush.type` split is the flush-stats regression surface.
LABEL_SENSITIVE = [
    ("zero_sync_cvr_flush_time_seconds_count", "flush.type"),
    ("zero_sync_cvr_flush_time_seconds_bucket", "flush.type"),
]

# Rust-only or infra families TS never emits (don't hold rust to TS's absence).
IGNORE_PREFIXES = (
    "process_", "go_", "nodejs_", "http_", "promhttp_", "target_info",
)


def family(series_key: str) -> str:
    return series_key.split("{", 1)[0]


def families(metrics: dict) -> set[str]:
    return {family(k) for k in metrics
            if not any(family(k).startswith(p) for p in IGNORE_PREFIXES)}


def zero_sync_families(fams: set[str]) -> set[str]:
    return {f for f in fams if f.startswith("zero_sync") or f.startswith("zero_")}


def run() -> int:
    rep = Report(os.path.join("reports", f"metric-series-{TAG}.json"))
    rust = scrape_metrics(RUST_CONTAINER)
    ts = scrape_metrics(TS_CONTAINER)
    if not rust or not ts:
        rep.add("C/metric-series", "SKIP",
                f"/metrics unavailable (rust={len(rust)} series, ts={len(ts)} series)")
        return rep.finish()

    # --- 1. Family parity: rust must expose every zero_* family TS does. -----
    rf, tf = families(rust), families(ts)
    ts_zero = zero_sync_families(tf)
    rust_zero = zero_sync_families(rf)
    missing = sorted(ts_zero - rust_zero)
    if missing:
        rep.add("C/metric-series:family", "FAIL",
                f"rust is MISSING {len(missing)} metric families TS emits: {missing[:10]}")
    else:
        rep.add("C/metric-series:family", "PASS",
                f"rust exposes all {len(ts_zero)} zero_* families TS emits")

    # --- 2. Label parity on differential-sensitive families. -----------------
    any_label_checked = False
    for fam, label in LABEL_SENSITIVE:
        rv = label_values(rust, fam, label)
        tv = label_values(ts, fam, label)
        if not tv and not rv:
            continue  # neither side exercised this family; nothing to assert
        any_label_checked = True
        if tv - rv:
            rep.add(f"C/metric-series:label:{fam.split('_')[-2]}", "FAIL",
                    f"{fam} `{label}` values: rust={sorted(rv)} MISSING {sorted(tv - rv)} "
                    f"(TS={sorted(tv)}) — a whole label series is absent on rust")
        elif rv - tv:
            rep.add(f"C/metric-series:label:{fam.split('_')[-2]}", "WATCH",
                    f"{fam} `{label}`: rust has EXTRA {sorted(rv - tv)} vs TS {sorted(tv)}")
        else:
            rep.add(f"C/metric-series:label:{fam.split('_')[-2]}", "PASS",
                    f"{fam} `{label}` values match TS: {sorted(rv)}")
    if not any_label_checked:
        rep.add("C/metric-series:label", "WATCH",
                "no label-sensitive family was exercised (no flush activity captured) — "
                "re-run after a mutation-bearing load to assert flush.type parity")

    return rep.finish()


if __name__ == "__main__":
    sys.exit(run())
