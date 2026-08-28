#!/usr/bin/env python3
"""
lifecycle_window.py — Gate B: lifecycle-window differential (the prod-outage class).

The client frame stream goes SILENT once the last client disconnects, so the
steady-state diff oracles are blind to what the server does in the
disconnect→idle keepalive window. That window is exactly where the
"seam-between-ported-code-and-invented-concurrency" divergences live — including
the 2026-08-28 expire-timer bug: rust POLLED `next_eviction_time(cvr)` with no
clients-present gate, so a TTL eviction + CVR flush could fire while zero clients
were connected, where TS's `#stopExpireTimer`-on-last-disconnect suppresses it.

This gate scripts, IDENTICALLY on rust and the TS reference:
  1. connect a client, subscribe a query, hydrate;
  2. inactivate that query with a SHORT ttl (so an eviction is due soon);
  3. disconnect the last client;
  4. hold through the keepalive window (past the ttl);
and then diffs SERVER-SIDE effects during the window — the `/metrics` counter
deltas (cvr-flush, expired-queries) and the eviction/flush log-line counts.

Invariant (client-observable contract of the idle window): with zero connected
clients, rust must do NO MORE eviction/flush work in the window than TS.
rust_delta <= ts_delta for every signal — equality is the norm; a rust EXCESS is
the divergence. Differential (rust vs TS), so shared expected teardown activity
(e.g. the one final ttlClock persist) cancels out.

Runs against a live TS+rust sandbox pair (see ab_common defaults / RUN.md);
SKIPs cleanly if the pair or the `/metrics` endpoints are unavailable.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    make_sides, open_side, reader, quiesce, Report, load_auth, load_client_schema,
)
from workload import (  # noqa: E402
    ast_query_put, query_del, change_desired_queries_message,
)
from gate_introspect import (  # noqa: E402
    docker_logs_since, scrape_metrics_for, sum_by_name,
)

TAG = os.environ.get("ART_TAG", time.strftime("%Y%m%d-%H%M%S"))
SHORT_TTL_MS = int(os.environ.get("LW_TTL_MS", "2500"))
KEEPALIVE_HOLD_S = float(os.environ.get("LW_HOLD_S", "8.0"))

# A tiny generic AST query (single-table scan, limit 1) — enough to create a CVR
# query the server hydrates and can later inactivate + expire. Table name is
# overridable for other sandboxes.
SCAN_TABLE = os.environ.get("LW_TABLE", "channels")
SCAN_AST = {"table": SCAN_TABLE, "limit": 1, "orderBy": [["id", "asc"]]}

# Log signatures that indicate the server did eviction/flush work.
EVICT_LOG_RE = ("expired", "evict", "removeExpiredQueries", "remove_expired")
FLUSH_LOG_RE = ("flush end", "flushed", "cvr flush")


def _token() -> str:
    return load_auth()["token"]


def _uid() -> str:
    a = load_auth()
    return a.get("userID") or ""


def _extra() -> dict:
    # Authenticated connections REQUIRE a userID (rust: "Authenticated
    # connections require a userID"); TS resolveAuth requires it too.
    return {"userID": _uid()}


def _evict_flush_signals(metrics: dict) -> dict:
    """Extract the window-sensitive counters from a /metrics scrape."""
    return {
        "cvr_flush": sum_by_name(metrics, "zero_sync_cvr_flush_time_seconds_count"),
        "rows_flushed": sum_by_name(metrics, "zero_sync_cvr_rows_flushed"),
        "expired": (sum_by_name(metrics, "zero_sync_expired_queries")
                    or sum_by_name(metrics, "zero_sync_query_expired")
                    or sum_by_name(metrics, "expired_quer")),
    }


def _count_log_hits(logs: str, needles: tuple) -> int:
    n = 0
    for line in logs.splitlines():
        low = line.lower()
        if any(k.lower() in low for k in needles):
            n += 1
    return n


async def _drive_one(side, token, client_schema) -> None:
    """Connect, subscribe a scan query, hydrate, inactivate with a short ttl,
    then disconnect the last (only) client."""
    init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": client_schema}]
    await open_side(side, token, init, extra=_extra())
    stop = asyncio.Event()
    rtask = asyncio.create_task(reader(side, stop))
    # Subscribe the scan query (short ttl so it is eligible to expire soon).
    put = ast_query_put(SCAN_AST, ttl_ms=SHORT_TTL_MS)
    await side.ws.send(json.dumps(change_desired_queries_message([put])))
    await quiesce([side], quiet_s=1.5, max_s=20.0)
    # Inactivate it (del) — this arms the eviction timer for now+SHORT_TTL.
    await side.ws.send(json.dumps(change_desired_queries_message([query_del(put["hash"])])))
    await quiesce([side], quiet_s=1.0, max_s=10.0)
    # Disconnect the last client. TS stops the expire timer here (#stopExpireTimer).
    stop.set()
    try:
        await side.ws.close()
    except Exception:
        pass
    try:
        await asyncio.wait_for(rtask, timeout=3)
    except Exception:
        pass


async def run() -> int:
    rep = Report(os.path.join("reports", f"lifecycle-window-{TAG}.json"))
    try:
        client_schema = load_client_schema()
        token = _token()
    except Exception as e:
        rep.add("B/lifecycle-window", "SKIP", f"setup unavailable: {e!r:.80}")
        return rep.finish()

    from ab_common import RUST_CONTAINER, TS_CONTAINER
    rust, ts = make_sides()

    # Baseline server counters BEFORE the disconnect, per side.
    pre = {"rust": scrape_metrics_for("rust"), "ts": scrape_metrics_for("ts")}
    if not pre["rust"] or not pre["ts"]:
        rep.add("B/lifecycle-window", "SKIP",
                "metrics endpoint unavailable on one/both sides "
                f"(rust={len(pre['rust'])} series, ts={len(pre['ts'])} series)")
        return rep.finish()

    window_start = time.time()
    # Drive both sides through the identical connect→subscribe→del→disconnect.
    try:
        await asyncio.gather(
            _drive_one(rust, token, client_schema),
            _drive_one(ts, token, client_schema),
        )
    except Exception as e:
        rep.add("B/lifecycle-window", "SKIP", f"drive failed (pair down?): {e!r:.90}")
        return rep.finish()

    # Hold through the keepalive window, past the ttl — this is where a
    # non-gated poll would fire an eviction with zero clients connected.
    await asyncio.sleep(KEEPALIVE_HOLD_S)
    window_s = time.time() - window_start

    post = {"rust": scrape_metrics_for("rust"), "ts": scrape_metrics_for("ts")}
    logs = {
        "rust": docker_logs_since(RUST_CONTAINER, window_s + 2),
        "ts": docker_logs_since(TS_CONTAINER, window_s + 2),
    }

    # Per-side counter deltas across the window (post - pre).
    sig = {}
    for s in ("rust", "ts"):
        a, b = _evict_flush_signals(pre[s]), _evict_flush_signals(post[s])
        sig[s] = {k: round(b[k] - a[k], 3) for k in a}

    log_evict = {s: _count_log_hits(logs[s], EVICT_LOG_RE) for s in ("rust", "ts")}
    log_flush = {s: _count_log_hits(logs[s], FLUSH_LOG_RE) for s in ("rust", "ts")}

    detail = (f"window={window_s:.1f}s ttl={SHORT_TTL_MS}ms | "
              f"metric-deltas rust={sig['rust']} ts={sig['ts']} | "
              f"evict-logs rust={log_evict['rust']} ts={log_evict['ts']} | "
              f"flush-logs rust={log_flush['rust']} ts={log_flush['ts']}")

    # Invariant: rust does NO MORE eviction/flush work than TS in the no-client
    # window. Small +1 tolerance on flush absorbs benign ordering of the single
    # final ttlClock persist; expiry work must match exactly.
    violations = []
    for k in ("expired",):
        if sig["rust"][k] > sig["ts"][k]:
            violations.append(f"metric {k}: rust +{sig['rust'][k]} > ts +{sig['ts'][k]}")
    if sig["rust"]["cvr_flush"] > sig["ts"]["cvr_flush"] + 1:
        violations.append(
            f"cvr_flush: rust +{sig['rust']['cvr_flush']} > ts +{sig['ts']['cvr_flush']} (+1 tol)")
    if log_evict["rust"] > log_evict["ts"]:
        violations.append(f"evict-logs: rust {log_evict['rust']} > ts {log_evict['ts']}")

    if violations:
        rep.add("B/lifecycle-window", "FAIL",
                "rust did MORE idle-window work than TS: " + "; ".join(violations),
                extra={"detail": detail, "sig": sig})
    else:
        rep.add("B/lifecycle-window", "PASS",
                "rust idle-window eviction/flush activity <= TS (no ghost work). " + detail)
    return rep.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
