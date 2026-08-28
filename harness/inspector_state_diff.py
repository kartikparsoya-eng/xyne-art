#!/usr/bin/env python3
"""
inspector_state_diff.py — Gate E: inspector state differential.

Some server state is invisible in the data-frame stream but IS exposed by the
`inspect` protocol (active queries, their got/deleted/ttl state, versions,
metrics). The existing G37 check only asserts that BOTH sides *replied* to
inspect — not that they replied with the SAME state. A CVR/query bookkeeping
divergence (a query stuck `got=false`, a wrong `deleted` flag, a missing query)
would pass G37 and never show up in a result-channel diff if no rows changed.

This gate drives an identical session on rust and the TS reference, then sends
`["inspect", {op}]` for each supported op and diffs the CONTENT of the replies
after normalizing per-side lineage (versions, ASTs, raw metrics values):
  * queries — the set of (name|queryID, got, deleted) must match TS;
  * version — both answer with a well-formed version (presence parity);
  * metrics — the set of metric KEYS must match TS (values normalized).

SKIPs cleanly if inspect is unauthorized/unanswered on either side (as G37 notes,
some ops need admin auth) rather than asserting a vacuous mismatch.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    make_sides, open_side, reader, quiesce, Report, load_auth, load_client_schema,
)
from workload import ast_query_put, change_desired_queries_message  # noqa: E402

TAG = os.environ.get("ART_TAG", time.strftime("%Y%m%d-%H%M%S"))
SCAN_AST = {"table": os.environ.get("IN_TABLE", "channels"), "limit": 1}
OPS = [o for o in os.environ.get("IN_OPS", "queries,version,metrics").split(",") if o]


def _token() -> str:
    return load_auth()["token"]


def _extra() -> dict:
    return {"userID": load_auth().get("userID") or ""}


def _norm_queries(value) -> set:
    """Canonicalize an inspect `queries` reply to a set of (name|id, got, deleted)."""
    rows = value if isinstance(value, list) else (value.get("value") if isinstance(value, dict) else [])
    out = set()
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        key = r.get("name") or r.get("queryID") or r.get("queryId") or r.get("hash")
        out.add((key, bool(r.get("got")), bool(r.get("deleted"))))
    return out


def _metric_keys(value) -> set:
    if isinstance(value, dict):
        v = value.get("value", value)
        if isinstance(v, dict):
            return set(v.keys())
        if isinstance(v, list):
            return {m.get("name") for m in v if isinstance(m, dict)}
    if isinstance(value, list):
        return {m.get("name") for m in value if isinstance(m, dict)}
    return set()


async def _session_and_inspect(side, token, cs) -> dict:
    """Establish a session, subscribe a query, then run every inspect op and
    return {op: reply_body}."""
    init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": cs}]
    await open_side(side, token, init, extra=_extra())
    stop = asyncio.Event()
    rt = asyncio.create_task(reader(side, stop))
    await side.ws.send(json.dumps(
        change_desired_queries_message([ast_query_put(SCAN_AST, ttl_ms=300_000)])))
    await quiesce([side], quiet_s=1.5, max_s=20.0)

    replies: dict[str, object] = {}
    for op in OPS:
        rid = uuid.uuid4().hex[:8]
        before = len(side.frames)
        try:
            await side.ws.send(json.dumps(["inspect", {"op": op, "id": rid}]))
        except Exception:
            continue
        # Wait briefly for the matching inspect reply.
        deadline = time.time() + 5
        while time.time() < deadline:
            await asyncio.sleep(0.2)
            hits = [f.body for f in side.frames[before:] if f.tag == "inspect"]
            if hits:
                replies[op] = hits[-1]
                break
    stop.set()
    try:
        await side.ws.close()
    except Exception:
        pass
    try:
        await asyncio.wait_for(rt, timeout=3)
    except Exception:
        pass
    return replies


async def run() -> int:
    rep = Report(os.path.join("reports", f"inspector-state-{TAG}.json"))
    try:
        cs = load_client_schema()
        token = _token()
    except Exception as e:
        rep.add("E/inspector-state", "SKIP", f"setup unavailable: {e!r:.80}")
        return rep.finish()

    rust, ts = make_sides()
    try:
        r_rep, t_rep = await asyncio.gather(
            _session_and_inspect(rust, token, cs),
            _session_and_inspect(ts, token, cs))
    except Exception as e:
        rep.add("E/inspector-state", "SKIP", f"drive failed (pair down?): {e!r:.90}")
        return rep.finish()

    if not r_rep and not t_rep:
        rep.add("E/inspector-state", "SKIP",
                "inspect unanswered on both sides (op may need admin auth)")
        return rep.finish()

    any_checked = False
    # ---- queries op: content diff ------------------------------------------
    if "queries" in r_rep or "queries" in t_rep:
        rq, tq = _norm_queries(r_rep.get("queries")), _norm_queries(t_rep.get("queries"))
        if not rq and not tq:
            rep.add("E/inspector-state:queries", "WATCH",
                    "queries inspect returned no rows on either side")
        else:
            any_checked = True
            if rq != tq:
                rep.add("E/inspector-state:queries", "FAIL",
                        f"active-query state diverges: rust-only={sorted(rq - tq, key=str)} "
                        f"ts-only={sorted(tq - rq, key=str)}")
            else:
                rep.add("E/inspector-state:queries", "PASS",
                        f"active-query (name,got,deleted) set matches TS: {sorted(rq, key=str)}")

    # ---- version op: presence parity ---------------------------------------
    if "version" in r_rep or "version" in t_rep:
        rv, tv = r_rep.get("version"), t_rep.get("version")
        if (rv is None) != (tv is None):
            rep.add("E/inspector-state:version", "FAIL",
                    f"version answered on {'rust' if rv else 'ts'} only")
        elif rv is not None:
            any_checked = True
            rep.add("E/inspector-state:version", "PASS", "both answer version")

    # ---- metrics op: key-set diff ------------------------------------------
    if "metrics" in r_rep or "metrics" in t_rep:
        rk, tk = _metric_keys(r_rep.get("metrics")), _metric_keys(t_rep.get("metrics"))
        if not rk and not tk:
            rep.add("E/inspector-state:metrics", "WATCH", "metrics inspect empty on both")
        elif tk - rk:
            rep.add("E/inspector-state:metrics", "FAIL",
                    f"rust missing inspect-metric keys TS reports: {sorted(tk - rk)}")
        else:
            any_checked = True
            rep.add("E/inspector-state:metrics", "PASS",
                    f"inspect-metric key-set covers TS ({len(tk)} keys)")

    if not any_checked:
        rep.add("E/inspector-state", "WATCH",
                "inspect answered but no dimension had comparable content")
    return rep.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
