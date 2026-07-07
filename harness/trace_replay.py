#!/usr/bin/env python3
"""
trace_replay.py — TRACE-FAITHFUL replay: play real prod sessions (mined by
tools/mine_traces.py) against a zero-cache, preserving exactly what the
statistical mode (replay.py) approximates:

  * per-session event SEQUENCES (which query after which, real pagination
    bursts, real revisit patterns)
  * inter-event TIMING (ms-precision dt, optionally --time-compress'd)
  * session INTERLEAVING (sessions start at their real offsets — real
    concurrency, real bursts)
  * entity REUSE topology (prod user hammering one channel 40x stays one
    sandbox channel hammered 40x) via frequency-ranked id mapping
  * client-group continuity (sessions sharing a prod cgid share a mapped
    cgid + cookie jar -> REAL resume-from-cookie behavior; concurrent
    sessions on one cgid = real multi-tab)

What it deliberately does NOT preserve (documented, by design):
  * data content — prod ids are remapped onto sandbox entities rank-by-rank
    (hottest prod channel -> hottest sandbox channel; the pool is already
    hotness-ranked by gen_id_pool_db.py). Unmappable keys pass through
    verbatim and hydrate 0 rows (honest fallback, counted in the report).
  * epoch-ms values (pagination cursors, lastUpdatedAt) are REBASED by
    (replay_now - trace_end) so "everything since 5 minutes ago" stays
    "since 5 minutes ago" instead of an 8-day-stale cursor.
  * mutation args — prod doesn't log them; with --enable-mutations the two
    hand-built arg builders fire (same 78%-of-volume coverage as replay.py),
    the rest are counted as skipped-by-type.
  * query removals — prod emits no removal event; TTL (--ttl-ms) expires
    desired queries exactly as an idle real client's would.

Emits a run summary in the SAME schema as replay.py (reports/run-<tag>.json)
so tools/local_gate.py gates it unchanged; the G5 shape key carries
trace=<basename> so trace runs never compare against statistical baselines.

    .venv/bin/python harness/trace_replay.py \
        --trace raw/traces/trace-last10m.ndjson \
        --target ws://rust-test.localhost/zero \
        --auth-pool harness/auth-pool.json \
        [--time-compress 5] [--max-sessions 100] [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from workload import (  # noqa: E402
    MUTATION_ARG_BUILDERS, ArgResolver, change_desired_queries_message,
    custom_mutation, init_connection_message, push_message, query_put,
)
from replay import (  # noqa: E402
    DEFAULT_PROTOCOL_VERSION, encode_sec_protocols,
)

INITIAL_WINDOW_S = 3.0     # puts within this of connect = "initial" latency


def dist(vals: list) -> dict:
    """Same shape as replay.write_summary's dist (that one is a closure)."""
    if not vals:
        return {"samples": 0}
    v = sorted(vals)
    q = lambda p: round(v[min(len(v) - 1, int(p * len(v)))], 1)  # noqa: E731
    return {"samples": len(v), "p50": q(.5), "p90": q(.9), "p95": q(.95),
            "p99": q(.99), "max": round(v[-1], 1),
            "mean": round(sum(v) / len(v), 1)}


# --------------------------------------------------------------------------- #
# ID mapping: prod entity ids -> sandbox pool ids, reuse-topology preserving
# --------------------------------------------------------------------------- #
# leaf arg key -> id-pool key. Keys absent here (or with an empty pool) pass
# through verbatim — they hydrate 0 rows, which is the honest fallback and is
# counted per-key in the mapping report.
KEY_ALIAS = {
    "channelId": "channelId", "scopeId": "channelId",
    "conversationId": "conversationId", "parentConversationId": "conversationId",
    "ticketId": "ticketId", "mappedTicketId": "ticketId",
    "subTicketId": "ticketId", "xyneId": "ticketId",
    "messageId": "messageId", "initialMessageId": "messageId",
    "projectId": "projectId", "boardId": "boardId",
    "userId": "userId", "assignee": "userId", "createdBy": "userId",
    "memberId": "userId", "targetUserId": "userId",
    "workspaceId": "workspaceId", "orgId": "orgId",
    "entityId": "entityId", "contextId": "contextId",
    # seeded by tools/seed_aux_tables.py (bulk-seed leaves these tables empty;
    # they were the top-2 unmapped keys: 937 userGroupId / 196 canvasId)
    "userGroupId": "userGroupId", "groupId": "userGroupId",
    "canvasId": "canvasId", "folderId": "folderId",
}


class TraceMapper:
    """Frequency-ranked bijection per pool key: prod id ranked #i (by
    occurrence count across the whole trace) -> pool id #i. The pool is
    hotness-ranked, so hot maps to hot. Overflow wraps modulo pool size
    (bijection degrades to surjection for the cold tail — counted)."""

    def __init__(self, pool_ids: dict[str, list], rebase_delta_ms: int):
        self.pool = {k: v for k, v in pool_ids.items() if v}
        self.delta = rebase_delta_ms
        self.rank: dict[str, dict[str, int]] = {}     # poolkey -> prodid -> rank
        self.stats = Counter()
        self.unmapped_keys = Counter()

    def build(self, sessions: list[dict]) -> None:
        counts: dict[str, Counter] = {}
        for s in sessions:
            for e in s["events"]:
                self._walk_count(e.get("args"), counts)
        for pk, c in counts.items():
            self.rank[pk] = {pid: i for i, (pid, _) in
                             enumerate(c.most_common())}

    def _walk_count(self, obj: Any, counts: dict, key: str = "") -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                self._walk_count(v, counts, k)
        elif isinstance(obj, list):
            for v in obj:
                self._walk_count(v, counts, key)
        elif isinstance(obj, str):
            pk = KEY_ALIAS.get(key)
            if pk and pk in self.pool:
                counts.setdefault(pk, Counter())[obj] += 1

    def map_value(self, key: str, v: Any) -> Any:
        # epoch-ms rebase: 13-digit ints/floats -> shift into replay time
        if isinstance(v, (int, float)) and not isinstance(v, bool) \
                and 1.5e12 < v < 2.1e12:
            self.stats["rebased_ts"] += 1
            return int(v) + self.delta
        if not isinstance(v, str):
            return v
        pk = KEY_ALIAS.get(key)
        if pk is None or pk not in self.pool:
            if key.endswith("Id") and len(v) > 15:
                self.unmapped_keys[key] += 1
            return v
        r = self.rank.get(pk, {}).get(v)
        if r is None:
            return v
        ids = self.pool[pk]
        if r >= len(ids):
            self.stats[f"wrapped:{pk}"] += 1
        self.stats[f"mapped:{pk}"] += 1
        return ids[r % len(ids)]

    def map_args(self, obj: Any, key: str = "") -> Any:
        if isinstance(obj, dict):
            return {k: self.map_args(v, k) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.map_args(v, key) for v in obj]
        return self.map_value(key, obj)

    def report(self) -> dict:
        return {"mapped": {k.split(":", 1)[1]: n for k, n in self.stats.items()
                           if k.startswith("mapped:")},
                "wrapped": {k.split(":", 1)[1]: n for k, n in self.stats.items()
                            if k.startswith("wrapped:")},
                "rebased_timestamps": self.stats.get("rebased_ts", 0),
                "unmapped_id_keys": dict(self.unmapped_keys.most_common(10))}


# --------------------------------------------------------------------------- #
@dataclass
class TStats:
    opened: int = 0
    failed_open: int = 0
    sessions_played: int = 0
    events_sent: int = 0
    dedup_puts: int = 0
    pokes: int = 0
    errors: int = 0
    mutations_sent: int = 0
    mutation_ok: int = 0
    muts_skipped: Counter = field(default_factory=Counter)
    per_query: Counter = field(default_factory=Counter)
    per_error: Counter = field(default_factory=Counter)
    lat_steady: list = field(default_factory=list)
    lat_initial: list = field(default_factory=list)
    lat_by_query: dict = field(default_factory=dict)
    hydrated: set = field(default_factory=set)      # names with >=1 got-ack
    behind_ms: list = field(default_factory=list)   # scheduling lag per event


async def play_session(sess: dict, a, mapper: TraceMapper, identity: dict,
                       stats: TStats, cookie_jar: dict, t_start: float,
                       stop: asyncio.Event) -> None:
    import urllib.parse
    import websockets

    compress = a.time_compress
    # wait for this session's (compressed) start offset
    delay = sess["offset_ms"] / 1000.0 / compress - (time.perf_counter() - t_start)
    if delay > 0:
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            return                                    # stopped while waiting
        except asyncio.TimeoutError:
            pass

    cg_key = sess.get("cgid") or sess["sid"]
    cgid = "art-tr" + hashlib.sha1(cg_key.encode()).hexdigest()[:12]
    cid = "art-tr" + hashlib.sha1(sess["sid"].encode()).hexdigest()[:12]
    base_cookie = cookie_jar.get(cgid, "")            # resume if cgid seen before
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": base_cookie,
              "ts": str(time.time() * 1000), "lmid": "0",
              "wsid": hashlib.sha1(os.urandom(8)).hexdigest()[:12],
              "userID": identity["userID"]}
    url = (a.target.rstrip("/") + f"/sync/v{a.protocol_version}/connect?"
           + urllib.parse.urlencode(params))
    try:
        ws = await websockets.connect(
            url, subprotocols=[encode_sec_protocols(None, identity["token"])],
            open_timeout=20, max_size=None, ping_interval=None)
        await ws.send(json.dumps(init_connection_message(
            [], client_schema=a.cschema)))
    except Exception as e:
        stats.failed_open += 1
        stats.per_error[f"connect: {str(e)[:160]}"] += 1
        return
    stats.opened += 1

    pending: dict[str, tuple[float, bool, str]] = {}
    desired: set[str] = set()
    connected_at = time.perf_counter()
    sess_done = asyncio.Event()

    async def reader() -> None:
        while not sess_done.is_set() and not stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                return
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if not isinstance(msg, list) or not msg:
                continue
            tag = msg[0]
            body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
            if tag == "pokeEnd":
                stats.pokes += 1
                ck = body.get("cookie")
                if isinstance(ck, str) and ck and not body.get("cancel"):
                    cookie_jar[cgid] = ck             # feeds future resumes
            elif tag == "pokePart":
                for got in body.get("gotQueriesPatch", []) or []:
                    if isinstance(got, dict) and got.get("op") == "put":
                        ent = pending.pop(got.get("hash"), None)
                        if ent:
                            t0, initial, qname = ent
                            dt = (time.perf_counter() - t0) * 1000.0
                            stats.hydrated.add(qname)
                            if initial:
                                stats.lat_initial.append(dt)
                            else:
                                # steady-only per-query attribution — same
                                # rule as replay.py, so G5b diffs like-for-like
                                stats.lat_steady.append(dt)
                                stats.lat_by_query.setdefault(
                                    qname, []).append(dt)
            elif tag == "error":
                kind = body.get("kind", "?")
                stats.per_error[f"{kind}: {str(body.get('message', ''))[:60]}"] += 1
                if kind != "Rehome":
                    stats.errors += 1

    rtask = asyncio.create_task(reader())
    mid = 0
    try:
        for e in sess["events"]:
            if stop.is_set():
                break
            # schedule at the event's compressed dt; never sleep negative
            # (if we're behind — slow pod — catch up, preserving order)
            target = connected_at + e["dt"] / 1000.0 / compress
            lag = time.perf_counter() - target
            if lag < 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=-lag)
                    break
                except asyncio.TimeoutError:
                    pass
            else:
                stats.behind_ms.append(lag * 1000.0)
            kind = e["kind"]
            if kind in ("query", "run"):
                args = mapper.map_args(e.get("args") or {})
                put = query_put(e["name"], args, ttl_ms=a.ttl_ms)
                if put["hash"] in desired:
                    stats.dedup_puts += 1             # real client dedupes too
                    continue
                desired.add(put["hash"])
                initial = (time.perf_counter() - connected_at) < INITIAL_WINDOW_S
                pending[put["hash"]] = (time.perf_counter(), initial, e["name"])
                await ws.send(json.dumps(change_desired_queries_message([put])))
                stats.events_sent += 1
                stats.per_query[e["name"]] += 1
            elif kind == "mutation":
                if not a.enable_mutations:
                    stats.muts_skipped["mutations-off"] += 1
                    continue
                builder = MUTATION_ARG_BUILDERS.get(e["name"])
                if builder is None:
                    stats.muts_skipped[e["name"]] += 1
                    continue
                margs = builder(a.resolver, int(time.time() * 1000))
                if margs is None:
                    stats.muts_skipped[e["name"] + ":unresolvable"] += 1
                    continue
                mid += 1
                await ws.send(json.dumps(push_message(
                    cgid, [custom_mutation(mid, cid, e["name"], margs,
                                           int(time.time() * 1000))],
                    request_id=f"{cid}-{mid}", now_ms=int(time.time() * 1000))))
                stats.mutations_sent += 1
            elif kind == "disconnect":
                break                                  # real end mid-trace
        # linger so late hydrations resolve (compressed tail)
        try:
            await asyncio.wait_for(stop.wait(), timeout=min(5.0, 15.0 / compress))
        except asyncio.TimeoutError:
            pass
    except Exception as exc:
        # 160 not 60: a ws close frame carries up to 123 bytes of reason
        # (the empty-reason ws-1002s would hide a reason at 60)
        stats.per_error[f"session: {str(exc)[:160]}"] += 1
    finally:
        sess_done.set()
        rtask.cancel()
        abrupt = any(e["kind"] == "disconnect"
                     and "unable" in str((e.get("meta") or {}).get("reason", ""))
                     for e in sess["events"])
        try:
            if abrupt:
                # no close frame — the server sees a dead TCP peer
                ws.transport.abort() if hasattr(ws, "transport") else await ws.close()
            else:
                await ws.close()
        except Exception:
            pass
        stats.sessions_played += 1


async def amain(a) -> int:
    with open(a.trace) as f:
        header = json.loads(f.readline())
        sessions = [json.loads(ln) for ln in f]
    if not header.get("_trace_header"):
        print("ERROR: not a trace file (missing header line)", file=sys.stderr)
        return 2
    sessions.sort(key=lambda s: s["offset_ms"])
    if a.max_sessions:
        sessions = sessions[:a.max_sessions]

    pool = json.load(open(a.id_pool))
    # rebase: the trace's end becomes the replay's start
    trace_end_ms = 0
    for s in sessions:
        trace_end_ms = max(trace_end_ms, s["offset_ms"] + s["events"][-1]["dt"])
    # offsets are trace-relative; reconstruct absolute from mined_at is fuzzy —
    # use the header's mined_at as the trace-now anchor
    mined_ms = int(time.mktime(time.strptime(
        header["mined_at"], "%Y-%m-%dT%H:%M:%SZ")) - time.timezone) * 1000
    delta = int(time.time() * 1000) - mined_ms
    mapper = TraceMapper(pool["ids"], delta)
    mapper.build(sessions)

    # identity per prod USER (rank by activity -> auth-pool round robin):
    # a user's sessions all share one identity, like prod
    users = Counter(s["user"] for s in sessions)
    auth = json.load(open(a.auth_pool))
    idents = auth if isinstance(auth, list) else auth.get("identities", [])
    user_ident = {u: idents[i % len(idents)]
                  for i, (u, _) in enumerate(users.most_common())}

    n_events = sum(len(s["events"]) for s in sessions)
    span_min = trace_end_ms / 60000.0
    print(f"trace: {os.path.basename(a.trace)} — {len(sessions)} sessions, "
          f"{n_events} events, {len(users)} users, span {span_min:.1f}min "
          f"-> compressed {span_min / a.time_compress:.1f}min")
    print(f"identities: {len(idents)} for {len(users)} prod users")
    if a.dry_run:
        # exercise the mapper over every event so the report is populated
        for s in sessions:
            for e in s["events"]:
                if e.get("args"):
                    mapper.map_args(e["args"])
        print(json.dumps(mapper.report(), indent=1))
        kinds = Counter(e["kind"] for s in sessions for e in s["events"])
        print("event kinds:", dict(kinds))
        return 0

    resolver = ArgResolver.from_pool_file(a.id_pool, random.Random(7), zipf_s=0.0)
    a.resolver = resolver
    a.cschema = json.load(open(a.client_schema))
    stats = TStats()
    cookie_jar: dict[str, str] = {}
    stop = asyncio.Event()
    t_start = time.perf_counter()
    start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    tasks = [asyncio.create_task(
        play_session(s, a, mapper, user_ident[s["user"]], stats, cookie_jar,
                     t_start, stop)) for s in sessions]
    wall_budget = trace_end_ms / 1000.0 / a.time_compress + 60.0
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True),
                               timeout=wall_budget)
    except asyncio.TimeoutError:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
    end_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    summary = {
        "mode": "trace-replay",
        "window": {"start": start_iso, "end": end_iso},
        "config": {
            "target": a.target, "trace": os.path.basename(a.trace),
            "time_compress": a.time_compress,
            "connections": len(sessions),          # G5 shape component
            "sessions": len(sessions), "profile": f"trace:{os.path.basename(a.trace)}",
            "zipf_s": 0.0, "lifecycle": True,
            "mutations_per_min": None if not a.enable_mutations else -1,
        },
        "counters": {
            "opened": stats.opened, "failed_open": stats.failed_open,
            "sessions_played": stats.sessions_played,
            "puts_sent": stats.events_sent, "dedup_puts": stats.dedup_puts,
            "pokes": stats.pokes, "errors": stats.errors,
            "mutations_sent": stats.mutations_sent,
            "mutation_ok": stats.mutation_ok,
            "rehomes": 0, "reconnects": 0, "invariant_violations": 0,
        },
        "client_latency_steady_ms": dist(stats.lat_steady),
        "client_latency_initial_ms": dist(stats.lat_initial),
        "client_latency_ms": dist(stats.lat_steady + stats.lat_initial),
        "scheduling_lag_ms": dist(stats.behind_ms),
        # steady-phase latency dist per query name (>=5 samples) — same shape
        # and filter as replay.py so local_gate.py's G5b works on trace runs
        "latency_by_query": {
            name: dist(v)
            for name, v in sorted(stats.lat_by_query.items(),
                                  key=lambda kv: -sorted(kv[1])[len(kv[1]) // 2])
            if len(v) >= 5
        },
        "coverage": {
            "queries_driven": len(stats.per_query),
            "queries_hydrated": len(stats.hydrated),
            "never_hydrated": sorted(set(stats.per_query) - stats.hydrated),
        },
        "per_error": dict(stats.per_error.most_common(20)),
        "top_queries_driven": stats.per_query.most_common(15),
        "mutations_skipped_by_type": dict(stats.muts_skipped.most_common(15)),
        "id_mapping": mapper.report(),
        "trace_header": {k: header[k] for k in
                         ("window", "sessions", "events", "distinct_users")
                         if k in header},
        "note": "trace-faithful replay: real prod session sequences/timing/"
                "interleaving/reuse-topology, ids mapped rank-to-rank onto "
                "sandbox entities, epoch-ms cursors rebased. scheduling_lag_ms"
                " tracks how far event sends fell behind the trace schedule "
                "(pod slowness pushes it up — it is itself a signal).",
    }
    tag = a.run_tag or time.strftime("%Y%m%d-%H%M%S")
    out = os.path.join(a.out_dir, f"run-{tag}.json")
    os.makedirs(a.out_dir, exist_ok=True)
    with open(out, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nrun summary: {out}")
    c = summary["counters"]
    print(f"opened={c['opened']}/{len(sessions)} puts={c['puts_sent']} "
          f"(dedup {c['dedup_puts']}) pokes={c['pokes']} errors={c['errors']} "
          f"muts={c['mutations_sent']}")
    for k in ("client_latency_steady_ms", "client_latency_initial_ms",
              "scheduling_lag_ms"):
        d = summary[k]
        if d.get("samples"):
            print(f"{k}: p50={d['p50']} p95={d['p95']} n={d['samples']}")
    return 0 if stats.errors == 0 and stats.failed_open == 0 else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace", required=True)
    ap.add_argument("--target", default="ws://rust-test.localhost/zero")
    ap.add_argument("--auth-pool", default=os.path.join(
        os.path.dirname(__file__), "auth-pool.json"))
    ap.add_argument("--id-pool", default=None,
                    help="defaults to id-pool.trace.json (unscoped harvest, "
                         "canvas/user-group seeded) when present, else "
                         "id-pool.sandbox.json")
    ap.add_argument("--client-schema", default=os.path.join(
        os.path.dirname(__file__), "client-schema.json"))
    ap.add_argument("--time-compress", type=float, default=1.0)
    ap.add_argument("--max-sessions", type=int, default=0)
    ap.add_argument("--ttl-ms", type=int, default=300_000)
    ap.add_argument("--protocol-version", type=int,
                    default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--enable-mutations", action="store_true")
    ap.add_argument("--i-know-this-writes", action="store_true")
    ap.add_argument("--out-dir", default=os.path.join(
        os.path.dirname(__file__), "..", "reports"))
    ap.add_argument("--run-tag", default=None,
                    help="override the output tag (default: end-time "
                         "timestamp). Dual/simultaneous runs MUST pass "
                         "distinct tags — two sides finishing in the same "
                         "second would clobber one run-<tag>.json")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if a.id_pool is None:
        here = os.path.dirname(os.path.abspath(__file__))
        pref = os.path.join(here, "id-pool.trace.json")
        a.id_pool = pref if os.path.exists(pref) \
            else os.path.join(here, "id-pool.sandbox.json")
        print(f"id-pool: {os.path.basename(a.id_pool)}")
    if a.enable_mutations and not a.i_know_this_writes:
        print("REFUSING: --enable-mutations needs --i-know-this-writes",
              file=sys.stderr)
        return 2
    return asyncio.run(amain(a))


if __name__ == "__main__":
    raise SystemExit(main())
