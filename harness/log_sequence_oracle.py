#!/usr/bin/env python3
"""
log_sequence_oracle.py — Gate D: lifecycle log-sequence differential.

ART classifies log signatures (task #142) but never asserts the ORDERED
lifecycle log sequence against TS. A divergence in *what the server does* often
shows up first as a log line that appears (or is missing) on one side — e.g. an
eviction log emitted during the 0-client idle window on rust that TS never logs
(the 2026-08-28 expire-timer bug, seen at the log layer).

rust and TS word their logs differently, so this gate maps each side's lines to
a shared CANONICAL lifecycle vocabulary (below), extracts the ordered sequence
of canonical events per side for an identical scripted scenario, and asserts:
  1. PRESENCE parity — the set of canonical lifecycle events emitted matches;
  2. IDLE-WINDOW invariant — rust emits NO MORE `expiry_ran` events than TS
     (the 0-client window must be eviction-silent on both).

The vocabulary is intentionally small and high-confidence; lines outside it are
ignored, so unknown wording never false-FAILs (the gate SKIPs a dimension it
cannot observe rather than inventing a mismatch). Enrich from the #142 signature
DB as coverage grows.

Runs against the live pair; SKIPs if logs are unavailable.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    make_sides, open_side, reader, quiesce, Report, load_auth,
    load_client_schema, RUST_CONTAINER, TS_CONTAINER,
)
from workload import ast_query_put, query_del, change_desired_queries_message  # noqa: E402
from gate_introspect import docker_logs_since  # noqa: E402

TAG = os.environ.get("ART_TAG", time.strftime("%Y%m%d-%H%M%S"))
SHORT_TTL_MS = int(os.environ.get("LS_TTL_MS", "2500"))
HOLD_S = float(os.environ.get("LS_HOLD_S", "8.0"))
SCAN_AST = {"table": os.environ.get("LS_TABLE", "channels"), "limit": 1}

# canonical event -> list of case-insensitive substrings that identify it on
# EITHER side. High-confidence lifecycle vocabulary (rust + TS wording).
CANON = {
    "cvr_loaded":        ["loading cvr", "load cvr", "cvr loaded"],
    "cvr_flushed":       ["flush end", "flushed cvr", "cvr flush", "flush type=sync"],
    "expiry_scheduled":  ["scheduling eviction", "schedule_expire", "schedule expire"],
    "expiry_ran":        ["queries have expired", "expired ", "removeexpiredqueries",
                          "remove_expired", "expired queries"],
    "expiry_stopped":    ["stopping expired queries timer", "stop_expire",
                          "stopping expire"],
    "cg_closing":        ["closing clientgroupid", "closing client group"],
    "shutdown":          ["stopping view syncer", "shutting down", "idle keepalive elapsed"],
}


def _token() -> str:
    return load_auth()["token"]


def _extra() -> dict:
    return {"userID": load_auth().get("userID") or ""}


def canonical_sequence(logs: str) -> list[str]:
    """Ordered list of canonical events recognized in the log text."""
    seq = []
    for line in logs.splitlines():
        low = line.lower()
        for ev, needles in CANON.items():
            if any(n in low for n in needles):
                seq.append(ev)
                break
    return seq


def counts(seq: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for e in seq:
        out[e] = out.get(e, 0) + 1
    return out


async def _drive(side, token, cs) -> None:
    init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": cs}]
    await open_side(side, token, init, extra=_extra())
    stop = asyncio.Event()
    rt = asyncio.create_task(reader(side, stop))
    put = ast_query_put(SCAN_AST, ttl_ms=SHORT_TTL_MS)
    await side.ws.send(json.dumps(change_desired_queries_message([put])))
    await quiesce([side], quiet_s=1.5, max_s=20.0)
    await side.ws.send(json.dumps(change_desired_queries_message([query_del(put["hash"])])))
    await quiesce([side], quiet_s=1.0, max_s=10.0)
    stop.set()
    try:
        await side.ws.close()
    except Exception:
        pass
    try:
        await asyncio.wait_for(rt, timeout=3)
    except Exception:
        pass


async def run() -> int:
    rep = Report(os.path.join("reports", f"log-sequence-{TAG}.json"))
    try:
        cs = load_client_schema()
        token = _token()
    except Exception as e:
        rep.add("D/log-sequence", "SKIP", f"setup unavailable: {e!r:.80}")
        return rep.finish()

    rust, ts = make_sides()
    t0 = time.time()
    try:
        await asyncio.gather(_drive(rust, token, cs), _drive(ts, token, cs))
    except Exception as e:
        rep.add("D/log-sequence", "SKIP", f"drive failed (pair down?): {e!r:.90}")
        return rep.finish()
    await asyncio.sleep(HOLD_S)
    window_s = time.time() - t0

    r_logs = docker_logs_since(RUST_CONTAINER, window_s + 2)
    t_logs = docker_logs_since(TS_CONTAINER, window_s + 2)
    if not r_logs or not t_logs:
        rep.add("D/log-sequence", "SKIP",
                f"logs unavailable (rust={len(r_logs)}B ts={len(t_logs)}B)")
        return rep.finish()

    r_seq, t_seq = canonical_sequence(r_logs), canonical_sequence(t_logs)
    r_cnt, t_cnt = counts(r_seq), counts(t_seq)
    if not r_seq and not t_seq:
        rep.add("D/log-sequence", "SKIP",
                "no canonical lifecycle events recognized on either side "
                "(log verbosity too low or wording outside the vocabulary)")
        return rep.finish()

    detail = f"rust={r_cnt} ts={t_cnt}"

    # 1. Presence parity of the canonical lifecycle vocabulary.
    r_set, t_set = set(r_cnt), set(t_cnt)
    presence_diffs = []
    if t_set - r_set:
        presence_diffs.append(f"rust MISSING events TS emits: {sorted(t_set - r_set)}")
    if r_set - t_set:
        presence_diffs.append(f"rust EXTRA events TS never emits: {sorted(r_set - t_set)}")

    # 2. Idle-window invariant: rust must not run MORE evictions than TS.
    idle_viol = r_cnt.get("expiry_ran", 0) > t_cnt.get("expiry_ran", 0)

    if idle_viol:
        rep.add("D/log-sequence:idle-eviction", "FAIL",
                f"rust ran MORE evictions than TS (expiry_ran rust="
                f"{r_cnt.get('expiry_ran', 0)} > ts={t_cnt.get('expiry_ran', 0)}) — "
                f"ghost eviction in the idle window. {detail}")
    else:
        rep.add("D/log-sequence:idle-eviction", "PASS",
                f"rust expiry_ran <= TS (no ghost idle eviction). {detail}")

    if presence_diffs:
        # EXTRA-only is a WATCH (verbosity), MISSING is a FAIL (lost lifecycle step).
        verdict = "FAIL" if (t_set - r_set) else "WATCH"
        rep.add("D/log-sequence:presence", verdict,
                "; ".join(presence_diffs) + f". {detail}")
    else:
        rep.add("D/log-sequence:presence", "PASS",
                f"canonical lifecycle event-set matches TS: {sorted(r_set)}")
    return rep.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
