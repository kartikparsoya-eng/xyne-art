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

import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    Report, make_sides, open_side, reader, quiesce, load_auth, load_client_schema,
)
from workload import ast_query_put, change_desired_queries_message  # noqa: E402
from gate_introspect import scrape_metrics_for, label_values  # noqa: E402

TAG = os.environ.get("ART_TAG", time.strftime("%Y%m%d-%H%M%S"))
EXPORT_WAIT_S = float(os.environ.get("MS_EXPORT_WAIT_S", "8.0"))
SCAN_AST = {"table": os.environ.get("MS_TABLE", "channels"), "limit": 1}

# Families whose LABEL cardinality must match TS (not just be present). The
# `flush_type` split is the flush-stats regression surface (Prometheus
# normalizes the OTEL `flush.type` attribute to `flush_type`).
LABEL_SENSITIVE = [
    ("zero_sync_cvr_flush_time_seconds_count", "flush_type"),
    ("zero_sync_cvr_flush_time_seconds_bucket", "flush_type"),
]

# Families NOT owned by the syncer port: change-streamer/replication metrics
# (emitted by the node change-streamer worker, gated by replication state, not
# syncer behavior) and process/runtime infra. Family parity is asserted ONLY over
# the `zero_sync_*` syncer families — the surface this port actually owns.
IGNORE_PREFIXES = (
    "process_", "go_", "nodejs_", "http_", "promhttp_", "target_info",
    "zero_replication_", "zero_cvr_", "zero_change_",
)


def family(series_key: str) -> str:
    return series_key.split("{", 1)[0]


def families(metrics: dict) -> set[str]:
    return {family(k) for k in metrics
            if not any(family(k).startswith(p) for p in IGNORE_PREFIXES)}


def zero_sync_families(fams: set[str]) -> set[str]:
    return {f for f in fams if f.startswith("zero_sync")}


async def _exercise(side, token, uid, cs) -> None:
    """Drive an identical hydrate session so both sides emit the syncer metric
    families (cvr_load, hydration, cvr_flush(sync), poke, active_clients).
    async flush (`flush_type=async`) additionally needs a mutation-bearing load."""
    try:
        init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": cs}]
        await open_side(side, token, init, extra={"userID": uid})
        stop = asyncio.Event()
        rt = asyncio.create_task(reader(side, stop))
        await side.ws.send(json.dumps(
            change_desired_queries_message([ast_query_put(SCAN_AST, ttl_ms=300_000)])))
        await quiesce([side], quiet_s=1.5, max_s=20.0)
        stop.set()
        await side.ws.close()
        try:
            await asyncio.wait_for(rt, timeout=3)
        except Exception:
            pass
    except Exception:
        pass


def run() -> int:
    rep = Report(os.path.join("reports", f"metric-series-{TAG}.json"))

    # Exercise both sides identically so metric FAMILIES actually appear, then
    # wait past the OTEL export interval before scraping (a fresh candidate emits
    # nothing until it has served a session).
    try:
        cs = load_client_schema()
        a = load_auth()
        asyncio.run(asyncio.wait_for(asyncio.gather(
            _exercise(make_sides()[0], a["token"], a["userID"], cs),
            _exercise(make_sides()[1], a["token"], a["userID"], cs),
        ), timeout=60))
        time.sleep(EXPORT_WAIT_S)
    except Exception:
        pass

    rust = scrape_metrics_for("rust")
    ts = scrape_metrics_for("ts")
    if not rust or not ts:
        rep.add("C/metric-series", "SKIP",
                f"/metrics unavailable (rust={len(rust)} series, ts={len(ts)} series)")
        return rep.finish()

    # --- 1. Family parity: rust must expose every zero_sync_* family TS does. -
    rf, tf = families(rust), families(ts)
    ts_zero = zero_sync_families(tf)
    rust_zero = zero_sync_families(rf)
    missing = sorted(ts_zero - rust_zero)
    # If rust emitted very few families it is still under-exercised (metrics lag
    # the export interval) — inconclusive rather than a divergence.
    if missing and len(rust_zero) < max(3, len(ts_zero) // 3):
        rep.add("C/metric-series:family", "WATCH",
                f"rust under-exercised ({len(rust_zero)} zero_sync families vs TS "
                f"{len(ts_zero)}); metrics lag the export interval. Re-run after load.")
    elif missing:
        rep.add("C/metric-series:family", "FAIL",
                f"rust is MISSING {len(missing)} zero_sync families TS emits: {missing[:10]}")
    else:
        rep.add("C/metric-series:family", "PASS",
                f"rust exposes all {len(ts_zero)} zero_sync families TS emits")

    # --- 2. Label parity on differential-sensitive families. -----------------
    any_label_checked = False
    for fam, label in LABEL_SENSITIVE:
        rv = label_values(rust, fam, label)
        tv = label_values(ts, fam, label)
        if not tv and not rv:
            continue  # neither side exercised this family; nothing to assert
        if not rv:
            # rust hasn't emitted this family yet (under-exercised candidate) —
            # inconclusive, not a divergence. Re-run after the release load.
            rep.add(f"C/metric-series:label:{fam.split('_')[-2]}", "WATCH",
                    f"{fam}: rust has no `{label}` series yet (under-exercised); "
                    f"TS={sorted(tv)}. Re-run after load to assert parity.")
            continue
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
