#!/usr/bin/env python3
"""
replay.py — ART Mode-A load driver.

Opens N concurrent WebSocket connections to a target zero-cache and drives the
production-derived workload from art-baseline.json: each virtual client keeps a
working set of NAMED desired queries (weighted by prod frequency, args from the
id-pool) and periodically churns them, which exercises the same engine paths the
server SLOs measure (query-transform -> hydration -> advance -> poke -> cvr).

It does NOT need the Xyne schema or query definitions: named custom queries go
on the wire as {op:put,hash,name,args} and zero-cache transforms them via the
app's query endpoint (verified: packages/zero-protocol/src/queries-patch.ts).

Then run tools/evaluate_gates.py over the run window to PASS/FAIL the change.

    # inspect the plan without connecting (no deps needed):
    python3 harness/replay.py --dry-run --id-pool harness/id-pool.json

    # live (needs: pip install websockets, a reachable zero-cache, valid auth):
    python3 harness/replay.py \
        --target wss://zero-canary.example/zero \
        --id-pool harness/id-pool.json \
        --connections 200 --working-set 15 --churn-ms 500 --duration 600 \
        --auth-token "$JWT"           # or --cookie "user_session_id=..."

Mutations are OFF by default (they write real data). Enable read-tracking
writes (~78% of prod write volume: channel.markChannelAsViewed +
activities.markThreadActivitiesAsReadV2) with BOTH:
    --enable-mutations --i-know-this-writes   [--mutations-per-min 4]
They write as the authenticated user (channel_user_status.lastViewedAt,
activities.isRead, one deterministic empty draft row per touched entity).
Only point this at disposable/staging data.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import random
import sys
import time
import urllib.parse
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from workload import (  # noqa: E402
    ArgResolver, WeightedSampler, MutationSampler, ImpactIndex, load_baseline,
    query_put, query_del, change_desired_queries_message, init_connection_message,
    custom_mutation, push_message, MUTATION_ARG_BUILDERS,
)

# Current mono protocol version (packages/zero-protocol/src/protocol-version.ts).
DEFAULT_PROTOCOL_VERSION = 49


def encode_sec_protocols(init_connection_message: Optional[list],
                         auth_token: Optional[str]) -> str:
    """Port of packages/zero-protocol/src/connect.ts::encodeSecProtocols.
    base64(utf8(JSON)) then percent-encode. Kept byte-for-byte compatible so
    zero-cache's decodeSecProtocols round-trips. If that file changes, update here."""
    protocols = {"initConnectionMessage": init_connection_message, "authToken": auth_token}
    raw = json.dumps(protocols, separators=(",", ":")).encode("utf-8")
    return urllib.parse.quote(base64.b64encode(raw).decode("ascii"))


# --------------------------------------------------------------------------- #
@dataclass
class Config:
    target: str
    path_prefix: str
    protocol_version: int
    connections: int
    working_set: int
    churn_ms: int
    duration_s: int
    ttl_ms: int
    auth_token: Optional[str]
    cookie: Optional[str]
    extra_params: list[tuple[str, str]]
    post_handshake: bool
    user_query_url: Optional[str]
    seed: int
    enable_mutations: bool = False
    mutations_per_min: float = 4.0
    client_schema: Optional[dict] = None
    # --- lifecycle realism (all off unless --lifecycle) ---
    lifecycle: bool = False
    session_mean_s: float = 45.0   # mean client session lifetime (expovariate)
    zombie_pct: float = 0.10       # clients that connect, then vanish forever
    abrupt_pct: float = 0.50       # session ends without a close frame
    resume_pct: float = 0.70       # reconnects that resume from last poke cookie
    retire_pct: float = 0.15       # chance per session-end to retire the clientID
                                   # (new cid + deleteClients for the old — drives
                                   # the server's client-GC path, gate G7)
    # --- multi-user auth ---
    auth_pool: Optional[list] = None  # [{token, userID}] assigned round-robin
    zipf_s: float = 0.0               # recorded in the run config for G5 shape matching
    profile: Optional[str] = None     # behavior profile name (G5 shape component)
    # --- impact-aware mutation targeting (G14; see workload.ImpactIndex) ---
    impact_path: Optional[str] = None
    impact_bias: float = 0.75         # P(aim at a subscribed query's entity)


@dataclass
class Stats:
    opened: int = 0
    failed_open: int = 0
    puts_sent: int = 0
    dels_sent: int = 0
    pokes: int = 0
    messages: int = 0
    errors: int = 0
    rehomes: int = 0
    reconnects: int = 0
    mutations_sent: int = 0
    mutation_ok: int = 0
    mutation_err: int = 0
    sessions: int = 0
    zombies: int = 0
    aborts: int = 0
    resumes: int = 0
    invariants: int = 0
    retired: int = 0        # clientIDs retired via activeClients/deleteClients
    delete_acks: int = 0    # clientIDs the server acked as deleted
    latencies_ms: list[float] = field(default_factory=list)
    # Session-opening puts hydrate BEHIND resume catch-up (server replays the
    # whole accumulated client-group delta first) — in a 1h lifecycle soak that
    # dominates the tail (p95 was 72s while steady p50 was ~100ms). Track them
    # separately so steady-state latency stays a clean regression signal.
    latencies_initial_ms: list[float] = field(default_factory=list)
    per_query: dict[str, int] = field(default_factory=dict)
    # steady-state hydration latency per query NAME — the attribution that
    # turns "p50 regressed 24x" into "THESE queries regressed" (2026-07-06:
    # Go 837ms vs TS 34ms steady p50 on identical 50c mixes needed exactly
    # this to be diagnosable). Initial-phase samples are excluded, same as
    # client_latency_steady_ms.
    lat_by_query: dict[str, list] = field(default_factory=dict)
    hydrated_queries: set[str] = field(default_factory=set)  # names that got a gotQueriesPatch put
    per_mutation: dict[str, int] = field(default_factory=dict)
    per_error: dict[str, int] = field(default_factory=dict)
    per_tag: dict[str, int] = field(default_factory=dict)
    # impact-edge coverage (G14): (query, mutator) edges where the mutator
    # fired while the query was subscribed on the same client.
    impact_edges: set = field(default_factory=set)
    impact_targeted: int = 0
    impact_fallback: int = 0

    def merge(self, o: "Stats") -> None:
        self.opened += o.opened
        self.failed_open += o.failed_open
        self.puts_sent += o.puts_sent
        self.dels_sent += o.dels_sent
        self.pokes += o.pokes
        self.messages += o.messages
        self.errors += o.errors
        self.rehomes += o.rehomes
        self.reconnects += o.reconnects
        self.mutations_sent += o.mutations_sent
        self.mutation_ok += o.mutation_ok
        self.mutation_err += o.mutation_err
        self.sessions += o.sessions
        self.zombies += o.zombies
        self.aborts += o.aborts
        self.resumes += o.resumes
        self.invariants += o.invariants
        self.retired += o.retired
        self.delete_acks += o.delete_acks
        self.latencies_ms.extend(o.latencies_ms)
        self.latencies_initial_ms.extend(o.latencies_initial_ms)
        self.hydrated_queries |= o.hydrated_queries
        self.impact_edges |= o.impact_edges
        self.impact_targeted += o.impact_targeted
        self.impact_fallback += o.impact_fallback
        for k, v in o.per_query.items():
            self.per_query[k] = self.per_query.get(k, 0) + v
        for k, v in o.lat_by_query.items():
            self.lat_by_query.setdefault(k, []).extend(v)
        for k, v in o.per_mutation.items():
            self.per_mutation[k] = self.per_mutation.get(k, 0) + v
        for k, v in o.per_error.items():
            self.per_error[k] = self.per_error.get(k, 0) + v
        for k, v in o.per_tag.items():
            self.per_tag[k] = self.per_tag.get(k, 0) + v


def build_connect_url(cfg: Config, cgid: str, cid: str, lmid: int = 0,
                      base_cookie: str = "", user_id: Optional[str] = None) -> str:
    # lmid = last mutation ID this client has had confirmed (0 for a fresh one).
    # base_cookie = last completed poke cookie -> server sends catch-up delta
    # instead of a full rehydrate (the resume path real clients exercise).
    # wsid must be unique per connection attempt (real zero-client sends a
    # fresh nanoid each time). It is the server's discriminator for stale
    # queued initConnection tasks after a reconnect; omitting it defeats the
    # wsID guard and trips "newClient must match existing client" under churn.
    params = {
        "clientGroupID": cgid, "clientID": cid, "baseCookie": base_cookie,
        "ts": str(time.time() * 1000), "lmid": str(lmid),
        "wsid": uuid.uuid4().hex[:12],
    }
    for k, v in cfg.extra_params:
        params[k] = v
    if user_id is not None:
        params["userID"] = user_id
    base = cfg.target.rstrip("/") + cfg.path_prefix + f"/sync/v{cfg.protocol_version}/connect"
    return base + "?" + urllib.parse.urlencode(params)


def new_put(cfg: Config, sampler: WeightedSampler, resolver: ArgResolver):
    # Resample when args can't be fully resolved (id missing from the pool,
    # e.g. empty table in a sandbox DB): real clients never send null IDs, so
    # replaying them only measures the server's validation error path. The
    # dry-run mix report still surfaces unresolved keys via resolver.unresolved.
    for _ in range(10):
        op = sampler.sample()
        args, ok = resolver.resolve(op)
        if ok:
            break
    put = query_put(op.name, args, ttl_ms=cfg.ttl_ms)
    return op.name, put


# --------------------------------------------------------------------------- #
async def run_client(cfg: Config, sampler: WeightedSampler, resolver: ArgResolver,
                     stats: Stats, deadline: float, rng: random.Random,
                     msampler: Optional[MutationSampler] = None,
                     client_index: int = 0) -> None:
    import websockets  # lazy: only needed for live runs

    # Client identity must be UNIQUE per run (never seeded): zero-cache tracks
    # lastMutationID per clientID, so reusing IDs across runs makes it reject
    # our pushes as "already processed". Sampling stays deterministic via rng.
    idrng = random.SystemRandom()
    cgid = "art-" + "".join(idrng.choice("abcdefghijklmnop0123456789") for _ in range(10))
    cid = "art-" + "".join(idrng.choice("abcdefghijklmnop0123456789") for _ in range(10))

    # Multi-user mode: each client is pinned to one identity from the pool.
    auth_token, user_id = cfg.auth_token, None
    if cfg.auth_pool:
        entry = cfg.auth_pool[client_index % len(cfg.auth_pool)]
        auth_token, user_id = entry["token"], entry.get("userID")

    headers = {}
    if cfg.cookie:
        headers["Cookie"] = cfg.cookie

    # Persist across reconnects: mutation counter, last acked id, poke cookie.
    mid = 0
    acked = 0
    base_cookie = ""      # last completed poke cookie (resume point)
    desired: set[str] = set()  # every hash ever desired — CVR keeps them per
                               # client group across sessions, so this must too
    hash_name: dict[str, str] = {}  # hash -> query name (for coverage tracking)
    hash_args: dict[str, dict] = {}  # hash -> args obj (impact-targeted mutations)
    retired_pending: list[str] = []  # old clientIDs to announce in deleted.clientIDs
    first_session = True
    is_zombie = cfg.lifecycle and rng.random() < cfg.zombie_pct

    def invariant(name: str) -> None:
        stats.invariants += 1
        key = f"INVARIANT: {name}"
        stats.per_error[key] = stats.per_error.get(key, 0) + 1

    # zero-cache rehomes client groups across syncer workers and expects the
    # client to reconnect (real clients do). Loop sessions until the deadline.
    while time.perf_counter() < deadline:
        # Lifecycle mode: sessions have realistic finite lifetimes; otherwise
        # one session runs to the wall-clock deadline (unless rehomed).
        if cfg.lifecycle:
            life = rng.expovariate(1.0 / cfg.session_mean_s)
            session_deadline = min(deadline, time.perf_counter() + max(3.0, life))
            abrupt = rng.random() < cfg.abrupt_pct
            resume = (not first_session and base_cookie
                      and rng.random() < cfg.resume_pct)
        else:
            session_deadline = deadline
            abrupt = False
            resume = bool(base_cookie) and not first_session

        # Mutations sent but never acked (e.g. lost to an abrupt end) are gone
        # server-side; the server expects acked+1 next. Rewind like a real
        # client that discarded its unsynced queue, else -> oooMutation.
        mid = acked

        # Fresh working set each session; new client groups need clientSchema.
        initial_puts, names = [], []
        for _ in range(cfg.working_set):
            name, put = new_put(cfg, sampler, resolver)
            initial_puts.append(put)
            names.append(name)
        init_msg = init_connection_message(
            initial_puts, user_query_url=cfg.user_query_url,
            client_schema=cfg.client_schema,
            # Real clients (protocol v19+) advertise the live clientIDs of the
            # group; the server GCs CVR state of clients not in the set. Only
            # meaningful under lifecycle churn, so gate it on that.
            active_clients=[cid] if cfg.lifecycle else None,
            deleted_client_ids=list(retired_pending) or None)
        sec = encode_sec_protocols(None if cfg.post_handshake else init_msg,
                                   auth_token)
        url = build_connect_url(cfg, cgid, cid, lmid=acked,
                                base_cookie=base_cookie if resume else "",
                                user_id=user_id)
        if resume:
            stats.resumes += 1

        # websockets renamed extra_headers -> additional_headers around v12.
        connect_kwargs: dict[str, Any] = {"subprotocols": [sec], "open_timeout": 20,
                                          "max_size": None, "ping_interval": None}
        try:
            connect_kwargs["additional_headers"] = headers or None
            conn = websockets.connect(url, **connect_kwargs)
        except TypeError:
            connect_kwargs.pop("additional_headers", None)
            connect_kwargs["extra_headers"] = headers or None
            conn = websockets.connect(url, **connect_kwargs)

        pending: dict[str, tuple[float, bool]] = {}  # hash -> (t_sent, is_initial)
        active: list[str] = []           # hashes currently in working set
        stop = asyncio.Event()           # session teardown signal (rehome etc.)
        poke_open: Optional[str] = None  # pokeID of the currently open poke frame

        def register(put: dict, name: str, initial: bool = False) -> None:
            pending[put["hash"]] = (time.perf_counter(), initial)
            active.append(put["hash"])
            desired.add(put["hash"])
            hash_name[put["hash"]] = name
            hash_args[put["hash"]] = (put.get("args") or [{}])[0]
            stats.puts_sent += 1
            stats.per_query[name] = stats.per_query.get(name, 0) + 1

        try:
            async with conn as ws:
                stats.sessions += 1
                if first_session:
                    stats.opened += 1
                    first_session = False
                else:
                    stats.reconnects += 1
                retired_pending.clear()  # announced in this session's initConnection
                if cfg.post_handshake:
                    await ws.send(json.dumps(init_msg))
                for p, n in zip(initial_puts, names):
                    register(p, n, initial=True)

                async def sleep_or_stop(seconds: float) -> bool:
                    remaining = session_deadline - time.perf_counter()
                    if remaining <= 0 or stop.is_set():
                        return False
                    try:
                        await asyncio.wait_for(
                            stop.wait(), timeout=min(seconds, remaining))
                        return False
                    except asyncio.TimeoutError:
                        return time.perf_counter() < session_deadline and not stop.is_set()

                async def churn() -> None:
                    while time.perf_counter() < session_deadline and not stop.is_set():
                        if not await sleep_or_stop(cfg.churn_ms / 1000.0):
                            return
                        patch = []
                        if active and len(active) >= cfg.working_set:
                            old = active.pop(0)
                            patch.append(query_del(old))
                            stats.dels_sent += 1
                        name, put = new_put(cfg, sampler, resolver)
                        patch.append(put)
                        try:
                            await ws.send(json.dumps(change_desired_queries_message(patch)))
                            register(put, name)
                        except Exception:
                            return

                async def mutator() -> None:
                    """Read-tracking custom mutations at --mutations-per-min per
                    client; ids increase monotonically across reconnects."""
                    nonlocal mid
                    if msampler is None or cfg.mutations_per_min <= 0:
                        return
                    interval = 60.0 / cfg.mutations_per_min

                    # de-sync clients so pushes don't arrive in lockstep
                    if not await sleep_or_stop(rng.uniform(0, interval)):
                        return
                    while time.perf_counter() < session_deadline and not stop.is_set():
                        now_ms = int(time.time() * 1000)
                        if msampler.impact is not None:
                            sub = [(hash_name[h], hash_args.get(h))
                                   for h in active if h in hash_name]
                            built, edges, targeted = msampler.build_targeted(
                                sub, resolver, now_ms, rng, cfg.impact_bias)
                        else:
                            built, edges, targeted = msampler.build(resolver, now_ms), set(), False
                        if built is not None:
                            if msampler.impact is not None:
                                if targeted:
                                    stats.impact_targeted += 1
                                else:
                                    stats.impact_fallback += 1
                                stats.impact_edges |= edges
                            name, args = built
                            mid += 1
                            msg = push_message(
                                cgid, [custom_mutation(mid, cid, name, args, now_ms)],
                                request_id=f"{cid}-{mid}", now_ms=now_ms)
                            try:
                                await ws.send(json.dumps(msg))
                            except Exception:
                                return
                            stats.mutations_sent += 1
                            stats.per_mutation[name] = stats.per_mutation.get(name, 0) + 1
                        if not await sleep_or_stop(interval):
                            return

                async def reader() -> None:
                    nonlocal acked, base_cookie, poke_open
                    while time.perf_counter() < session_deadline and not stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue
                        except Exception:
                            return
                        stats.messages += 1
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msg, list) or not msg:
                            continue
                        tag = msg[0]
                        if isinstance(tag, str):
                            stats.per_tag[tag] = stats.per_tag.get(tag, 0) + 1
                        if tag == "error":
                            body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
                            kind = body.get("kind", "unknown")
                            key = f"{kind}: {str(body.get('message', ''))[:80]}"
                            stats.per_error[key] = stats.per_error.get(key, 0) + 1
                            if kind == "Rehome":
                                # Operational reshuffle, not a failure: reconnect.
                                stats.rehomes += 1
                                stop.set()
                                return
                            stats.errors += 1
                            # Only connection-level kinds are session-fatal.
                            # Query-scoped ones (e.g. TransformFailed) leave the
                            # socket usable; if the server closes it anyway the
                            # recv raises and we reconnect via the outer loop.
                            if kind in ("InvalidConnectionRequest", "Unauthorized",
                                        "AuthInvalidated", "ClientNotFound",
                                        "InvalidMessage", "VersionNotSupported",
                                        "SchemaVersionNotSupported", "Internal"):
                                stop.set()
                                return
                        elif tag == "transformError":
                            # Per-query transform failures (socket stays usable).
                            details = msg[1] if len(msg) > 1 else None
                            items = details if isinstance(details, list) else [details]
                            for it in items:
                                d = it if isinstance(it, dict) else {"raw": str(it)[:80]}
                                key = (f"transformError: {d.get('name', '?')}: "
                                       f"{str(d.get('message', d.get('details', '')))[:70]}")
                                stats.per_error[key] = stats.per_error.get(key, 0) + 1
                        elif tag == "deleteClients":
                            # Server ack: it deleted these clients' CVR state
                            # (client-handler.ts sendDeleteClients).
                            body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
                            stats.delete_acks += len(body.get("clientIDs") or [])
                        elif tag == "pushResponse":
                            body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
                            for m in body.get("mutations", []) or []:
                                res = m.get("result", {}) if isinstance(m, dict) else {}
                                if isinstance(res, dict) and "error" in res:
                                    stats.mutation_err += 1
                                else:
                                    stats.mutation_ok += 1
                                mid_obj = m.get("id", {}) if isinstance(m, dict) else {}
                                if isinstance(mid_obj, dict):
                                    acked = max(acked, int(mid_obj.get("id", 0)))
                        elif tag == "pokeStart":
                            body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
                            if poke_open is not None:
                                invariant("pokeStart while a poke frame is open")
                            poke_open = body.get("pokeID")
                        elif tag == "pokeEnd":
                            body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
                            pid = body.get("pokeID")
                            if poke_open is None:
                                invariant("pokeEnd without pokeStart")
                            elif pid is not None and pid != poke_open:
                                invariant("pokeEnd pokeID != open pokeStart pokeID")
                            poke_open = None
                            # The cookie of a completed poke is the resume point
                            # for reconnects (baseCookie catch-up path).
                            ck = body.get("cookie")
                            if isinstance(ck, str) and ck and not body.get("cancel"):
                                base_cookie = ck
                            stats.pokes += 1
                        elif tag == "pokePart":
                            body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
                            pid = body.get("pokeID")
                            if poke_open is None:
                                invariant("pokePart outside a poke frame")
                            elif pid is not None and pid != poke_open:
                                invariant("pokePart pokeID != open pokeStart pokeID")
                            lmids = body.get("lastMutationIDChanges") or {}
                            if cid in lmids:
                                new_acked = int(lmids[cid])
                                if new_acked < acked:
                                    invariant("lastMutationID went backwards")
                                elif new_acked > acked:
                                    # This Go build sends no pushResponse; the
                                    # lmid advancing IS the mutation ack.
                                    stats.mutation_ok += new_acked - acked
                                    acked = new_acked
                            # Query hydration ack arrives as gotQueriesPatch
                            # {op:"put", hash} — that's the latency endpoint.
                            for got in body.get("gotQueriesPatch", []) or []:
                                if got.get("op") != "put":
                                    continue
                                h = got.get("hash")
                                if h is not None and h not in desired:
                                    invariant("gotQueriesPatch for a hash we never desired")
                                if h in hash_name:
                                    stats.hydrated_queries.add(hash_name[h])
                                ent = pending.pop(h, None)
                                if ent is not None:
                                    t0, was_initial = ent
                                    dt = (time.perf_counter() - t0) * 1000.0
                                    if was_initial:
                                        stats.latencies_initial_ms.append(dt)
                                    else:
                                        stats.latencies_ms.append(dt)
                                        if h in hash_name:
                                            stats.lat_by_query.setdefault(
                                                hash_name[h], []).append(dt)

                await asyncio.gather(churn(), reader(), mutator())

                # Lifecycle: end this session the way real clients do.
                if cfg.lifecycle and time.perf_counter() < deadline and not stop.is_set():
                    if abrupt or is_zombie:
                        # Vanish without a close frame (laptop sleep, network
                        # drop): the server must detect + clean up on its own.
                        stats.aborts += 1
                        transport = getattr(ws, "transport", None)
                        if transport is not None:
                            transport.abort()
        except Exception:
            if first_session:
                stats.failed_open += 1
                return
            # connection dropped mid-run: back off briefly and reconnect

        if is_zombie:
            # Zombie clients never come back; their client group + CVR rows
            # are the server's problem now (GC gate checks it gets cleaned).
            stats.zombies += 1
            return
        if (cfg.lifecycle and time.perf_counter() < deadline
                and rng.random() < cfg.retire_pct):
            # Retire this clientID like a closed tab: the next session connects
            # with a NEW cid, advertises it via activeClients, and lists the
            # old one in deleted.clientIDs. The server must delete the old
            # client's CVR rows (ack: downstream deleteClients) — this drives
            # the client-GC path that zombie-only churn never triggers.
            retired_pending.append(cid)
            stats.retired += 1
            cid = "art-" + "".join(
                idrng.choice("abcdefghijklmnop0123456789") for _ in range(10))
            acked = 0  # server tracks lastMutationID per clientID; fresh cid = 0
        if time.perf_counter() < deadline:
            await asyncio.sleep(0.25 + rng.random() * 0.5)


async def run_live(cfg: Config, sampler, resolver,
                   msampler: Optional[MutationSampler] = None) -> Stats:
    stats = Stats()
    deadline = time.perf_counter() + cfg.duration_s
    sem = asyncio.Semaphore(cfg.connections)

    async def one(i: int):
        async with sem:
            await run_client(cfg, sampler, resolver, stats,
                             deadline, random.Random(cfg.seed + i + 1), msampler,
                             client_index=i)

    # Stagger connection starts across the first ~5s to avoid a thundering herd.
    tasks = []
    for i in range(cfg.connections):
        tasks.append(asyncio.create_task(one(i)))
        if cfg.connections > 20:
            await asyncio.sleep(5.0 / cfg.connections)

    async def ticker():
        while time.perf_counter() < deadline:
            await asyncio.sleep(2.0)
            sys.stdout.write(f"\r  open={stats.opened} puts={stats.puts_sent} "
                             f"pokes={stats.pokes} muts={stats.mutations_sent} "
                             f"rehome={stats.rehomes} msgs={stats.messages} "
                             f"errs={stats.errors}")
            sys.stdout.flush()
    t = asyncio.create_task(ticker())
    await asyncio.gather(*tasks)
    t.cancel()
    sys.stdout.write("\n")
    return stats


# --------------------------------------------------------------------------- #
def dry_run(cfg: Config, sampler, resolver, n: int,
            msampler: Optional[MutationSampler] = None) -> None:
    rng = random.Random(cfg.seed)
    print("=== DRY RUN (no network) ===")
    print(f"target        : {cfg.target}{cfg.path_prefix}/sync/v{cfg.protocol_version}/connect")
    print(f"auth          : {'cookie' if cfg.cookie else ('token' if cfg.auth_token else 'NONE')}"
          f" | post_handshake={cfg.post_handshake}")
    print(f"load model    : {cfg.connections} conns x {cfg.working_set} queries, "
          f"churn every {cfg.churn_ms}ms for {cfg.duration_s}s")
    approx = cfg.connections * (1000.0 / cfg.churn_ms)
    print(f"approx churn  : ~{approx:.0f} query puts/sec at steady state")
    if msampler is not None:
        mrate = cfg.connections * cfg.mutations_per_min / 60.0
        print(f"mutations     : ON — ~{mrate:.1f}/sec across "
              f"{len(msampler.supported)} supported types "
              f"({', '.join(m.name for m in msampler.supported)})")
    else:
        print("mutations     : OFF (default)")
    print(f"example connect URL:\n  {build_connect_url(cfg, 'art-CGID', 'art-CID')}")
    print(f"\n--- sample initConnection (working set of {min(cfg.working_set,3)} shown) ---")
    puts = [new_put(cfg, sampler, resolver)[1] for _ in range(min(cfg.working_set, 3))]
    msg = init_connection_message(puts, user_query_url=cfg.user_query_url,
                                  client_schema=cfg.client_schema)
    if cfg.client_schema:
        msg[1]["clientSchema"] = f"<{len(cfg.client_schema.get('tables', {}))} tables omitted>"
    print(json.dumps(msg, indent=2)[:1400])
    print("\n--- sample churn message ---")
    _n, put = new_put(cfg, sampler, resolver)
    print(json.dumps(change_desired_queries_message([query_del("OLDHASH"), put])))
    if msampler is not None:
        print("\n--- sample push (mutation) message ---")
        now_ms = int(time.time() * 1000)
        built = msampler.build(resolver, now_ms)
        if built:
            name, args = built
            print(json.dumps(push_message(
                "art-CGID", [custom_mutation(1, "art-CID", name, args, now_ms)],
                request_id="art-CID-1", now_ms=now_ms), indent=2))
        else:
            print("(!) could not build mutation args — id-pool missing "
                  "channelId/conversationId")
        # projected mutation mix
        mcounts: dict[str, int] = {}
        for _ in range(n):
            mop = msampler.sample()
            mcounts[mop.name] = mcounts.get(mop.name, 0) + 1
        print(f"projected mutation mix over {n} draws: "
              + ", ".join(f"{k}={100*v//n}%" for k, v in
                          sorted(mcounts.items(), key=lambda kv: -kv[1])))
    # coverage
    counts, fully = {}, 0
    for _ in range(n):
        op = sampler.sample()
        counts[op.name] = counts.get(op.name, 0) + 1
        _a, ok = resolver.resolve(op)
        fully += ok
    print(f"\nprojected mix over {n} draws: {len(counts)} distinct queries, "
          f"{100*fully//n}% fully-resolved args")
    if resolver.unresolved:
        print("unresolved keys:", sorted(resolver.unresolved.items(), key=lambda kv: -kv[1])[:8])
    print("\nOK: plan is valid. Add --target + auth and drop --dry-run to drive load.")


def write_summary(cfg: Config, stats: Stats, start_iso: str, end_iso: str, out_dir: str,
                  impact: Optional[ImpactIndex] = None) -> str:
    def dist(vals: list[float]) -> dict:
        s = sorted(vals)
        def pct(p):
            if not s:
                return None
            return round(s[min(len(s) - 1, int(p * len(s)))], 1)
        return {"samples": len(s), "p50": pct(0.50), "p95": pct(0.95),
                "p99": pct(0.99)}
    summary = {
        "mode": "A-replay", "target": cfg.target,
        "window": {"start": start_iso, "end": end_iso, "duration_s": cfg.duration_s},
        # lifecycle/zipf_s/profile belong in the shape: G5 compares latency
        # only across runs with identical load geometry (lifecycle resume
        # catch-up, hot-key skew, and behavior-profile cadence all change the
        # distribution wholesale).
        "config": {"connections": cfg.connections, "working_set": cfg.working_set,
                   "churn_ms": cfg.churn_ms, "lifecycle": cfg.lifecycle,
                   "zipf_s": cfg.zipf_s, "profile": cfg.profile},
        "counters": {"opened": stats.opened, "failed_open": stats.failed_open,
                     "puts_sent": stats.puts_sent, "dels_sent": stats.dels_sent,
                     "pokes": stats.pokes, "messages": stats.messages, "errors": stats.errors,
                     "rehomes": stats.rehomes, "reconnects": stats.reconnects,
                     "sessions": stats.sessions, "zombies": stats.zombies,
                     "aborts": stats.aborts, "resumes": stats.resumes,
                     "invariant_violations": stats.invariants,
                     "clients_retired": stats.retired,
                     "delete_client_acks": stats.delete_acks,
                     "mutations_sent": stats.mutations_sent,
                     "mutation_ok": stats.mutation_ok, "mutation_err": stats.mutation_err},
        "per_error": dict(sorted(stats.per_error.items(), key=lambda kv: -kv[1])),
        "per_tag": dict(sorted(stats.per_tag.items(), key=lambda kv: -kv[1])),
        # client_latency_ms keeps the legacy meaning (ALL samples) so existing
        # blessed baselines stay comparable; _steady (churn puts only) is the
        # preferred G5 signal, _initial isolates session-open/resume catch-up.
        "client_latency_ms": dist(stats.latencies_ms + stats.latencies_initial_ms),
        "client_latency_steady_ms": dist(stats.latencies_ms),
        "client_latency_initial_ms": dist(stats.latencies_initial_ms),
        "top_queries_driven": sorted(stats.per_query.items(), key=lambda kv: -kv[1])[:15],
        # steady-phase latency dist per query name (>=5 samples) — diff two
        # runs' maps to attribute an aggregate regression to specific queries.
        "latency_by_query": {
            name: dist(v)
            for name, v in sorted(stats.lat_by_query.items(),
                                  key=lambda kv: -sorted(kv[1])[len(kv[1]) // 2])
            if len(v) >= 5
        },
        "coverage": {
            # driven = distinct query names we desired; hydrated = names that
            # got at least one gotQueriesPatch ack. never_hydrated is the
            # blind-spot list: desired but never delivered (build drift or bug).
            "queries_driven": len(stats.per_query),
            "queries_hydrated": len(stats.hydrated_queries),
            "never_hydrated": sorted(set(stats.per_query) - stats.hydrated_queries),
        },
        "mutations_driven": sorted(stats.per_mutation.items(), key=lambda kv: -kv[1]),
        "note": "client_latency is best-effort (rowsPatch queryHash attribution); "
                "_steady excludes session-open puts whose hydration waits behind "
                "resume catch-up. PRIMARY regression signal is server histograms "
                "via evaluate_gates.py.",
    }
    # G14 impact-edge coverage: reachable = impact edges whose query AND
    # mutator were both driven this run; exercised = fired co-active on one
    # client (MutationSampler.build_targeted). uncovered_sample names concrete
    # (query <- mutator) pairs to chase — actionable, not just a percentage.
    if impact is not None:
        reachable = {(q, m) for (q, m) in impact.edges
                     if q in stats.per_query and m in stats.per_mutation}
        exercised = stats.impact_edges & reachable
        uncovered = sorted(reachable - exercised)
        summary["impact"] = {
            "map": os.path.basename(impact.path),
            "edges_in_map": len(impact.edges),
            "edges_reachable": len(reachable),
            "edges_exercised": len(exercised),
            "targeted_picks": stats.impact_targeted,
            "fallback_picks": stats.impact_fallback,
            "uncovered_sample": uncovered[:8],
        }
    os.makedirs(os.path.abspath(out_dir), exist_ok=True)
    path = os.path.join(os.path.abspath(out_dir),
                        "run-" + time.strftime("%Y%m%d-%H%M%S") + ".json")
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="ART Mode-A replay load driver.")
    ap.add_argument("--baseline", default=os.path.join(os.path.dirname(__file__), "..", "art-baseline.json"))
    ap.add_argument("--id-pool", default=None, help="harness/id-pool.json from gen_id_pool.py")
    ap.add_argument("--zipf-s", type=float, default=0.0,
                    help="Zipf exponent for id-pool sampling (0=uniform; ~1.1 approximates "
                         "prod hot-key skew; pool must be hotness-ranked, see gen_id_pool_db.py)")
    ap.add_argument("--target", default="wss://REPLACE-ME/zero", help="zero-cache ws/wss base URL")
    ap.add_argument("--path-prefix", default="", help="e.g. /zero for xyne (auto-set if --target ends in /zero)")
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--connections", type=int, default=50)
    ap.add_argument("--profile", default=None,
                    help="JSON behavior profile (e.g. profiles/prod-7d.json from "
                         "derive_prod_profile.py). Profile knobs become the "
                         "defaults; explicit CLI flags still win. The default "
                         "(no profile) is the HOT torture shape (~250x prod churn).")
    ap.add_argument("--working-set", type=int, default=None, help="desired queries kept per client (default 12)")
    ap.add_argument("--churn-ms", type=int, default=None, help="ms between query swaps per client (default 750)")
    ap.add_argument("--duration", type=int, default=300, help="run seconds")
    ap.add_argument("--ttl-ms", type=int, default=300000)
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--cookie", default=None)
    ap.add_argument("--extra-param", action="append", default=[], help="key=value (repeatable), e.g. userID=... profileID=...")
    ap.add_argument("--user-query-url", default=None, help="override app query endpoint in initConnection")
    ap.add_argument("--client-schema", default=None,
                    help="JSON file with the app clientSchema — REQUIRED for new client groups "
                         "(harvest from a CVR instances row or the app schema)")
    ap.add_argument("--no-post-handshake", action="store_true", help="send initConnection in the header (small only)")
    ap.add_argument("--enable-mutations", action="store_true",
                    help="drive read-tracking custom mutations too (WRITES data; "
                         "requires --i-know-this-writes)")
    ap.add_argument("--i-know-this-writes", action="store_true",
                    help="confirm you understand --enable-mutations writes to the target DB")
    ap.add_argument("--impact", default=None,
                    help="query-mutator impact matrix json (tools/gen_impact_matrix.sh): "
                         "impact-aware mutation targeting + G14 edge coverage")
    ap.add_argument("--impact-bias", type=float, default=0.75,
                    help="P(aim a mutation at a subscribed query's own entity) (default 0.75)")
    ap.add_argument("--mutations-per-min", type=float, default=None,
                    help="mutations per client per minute when enabled (default 4)")
    ap.add_argument("--lifecycle", action="store_true", default=None,
                    help="realistic client lifecycles: finite sessions, abrupt "
                         "disconnects, resume-from-cookie reconnects, zombie clients")
    ap.add_argument("--session-mean-s", type=float, default=None,
                    help="mean session lifetime in lifecycle mode (default 45s)")
    ap.add_argument("--zombie-pct", type=float, default=None,
                    help="fraction of clients that vanish forever (default 0.10)")
    ap.add_argument("--abrupt-pct", type=float, default=None,
                    help="fraction of session ends without a close frame (default 0.50)")
    ap.add_argument("--resume-pct", type=float, default=None,
                    help="fraction of reconnects resuming from last poke cookie (default 0.70)")
    ap.add_argument("--retire-pct", type=float, default=None,
                    help="per-session-end chance to retire the clientID and announce "
                         "it via deleteClients/activeClients (lifecycle mode, default 0.15)")
    ap.add_argument("--auth-pool", default=None,
                    help="JSON file: [{token, userID}, ...] — clients round-robin "
                         "identities (multi-user write contention)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "..", "reports"))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    # ---- resolve behavior knobs: explicit CLI > profile > hard default ----
    # (profiled args parse with default=None so "not passed" is detectable)
    HARD = {"working_set": 12, "churn_ms": 750, "mutations_per_min": 4.0,
            "lifecycle": False, "session_mean_s": 45.0, "zombie_pct": 0.10,
            "abrupt_pct": 0.50, "resume_pct": 0.70, "retire_pct": 0.15}
    profile_name = None
    profile_knobs: dict = {}
    if a.profile:
        with open(a.profile) as f:
            prof = json.load(f)
        profile_knobs = prof.get("knobs", {})
        profile_name = prof.get("name", os.path.basename(a.profile))
    for knob, hard in HARD.items():
        cli = getattr(a, knob)
        if cli is not None:
            continue                       # explicit flag wins
        setattr(a, knob, profile_knobs.get(knob, hard))
    if profile_name:
        print(f"PROFILE: {profile_name} — churn one/{a.churn_ms/1000:.0f}s, "
              f"{a.mutations_per_min}/min mutations, "
              f"session mean {a.session_mean_s:.0f}s, "
              f"abrupt {a.abrupt_pct:.0%}, lifecycle={a.lifecycle}")

    extra = []
    for p in a.extra_param:
        if "=" not in p:
            print(f"--extra-param expects key=value, got {p!r}", file=sys.stderr)
            return 2
        k, v = p.split("=", 1)
        extra.append((k, v))

    path_prefix = a.path_prefix
    if not path_prefix and a.target.rstrip("/").endswith("/zero"):
        # target already includes /zero as the base; keep prefix empty.
        path_prefix = ""

    client_schema = None
    if a.client_schema:
        with open(a.client_schema) as f:
            client_schema = json.load(f)

    auth_pool = None
    if a.auth_pool:
        with open(a.auth_pool) as f:
            auth_pool = json.load(f)
        if not (isinstance(auth_pool, list) and auth_pool
                and all("token" in e for e in auth_pool)):
            print("ERROR: --auth-pool must be a non-empty JSON array of "
                  "{token, userID} objects", file=sys.stderr)
            return 2

    cfg = Config(
        target=a.target, path_prefix=path_prefix, protocol_version=a.protocol_version,
        connections=a.connections, working_set=a.working_set, churn_ms=a.churn_ms,
        duration_s=a.duration, ttl_ms=a.ttl_ms, auth_token=a.auth_token, cookie=a.cookie,
        extra_params=extra, post_handshake=not a.no_post_handshake,
        user_query_url=a.user_query_url, seed=a.seed,
        enable_mutations=a.enable_mutations, mutations_per_min=a.mutations_per_min,
        client_schema=client_schema,
        lifecycle=a.lifecycle, session_mean_s=a.session_mean_s,
        zombie_pct=a.zombie_pct, abrupt_pct=a.abrupt_pct, resume_pct=a.resume_pct,
        retire_pct=a.retire_pct,
        auth_pool=auth_pool,
        zipf_s=a.zipf_s,
        profile=profile_name,
        impact_path=a.impact,
        impact_bias=a.impact_bias,
    )

    rng = random.Random(a.seed)
    bl = load_baseline(a.baseline)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng, zipf_s=a.zipf_s)
    sampler = WeightedSampler(bl.queries, rng)
    print(f"baseline v{bl.version}: {len(bl.queries)} queries | "
          f"id-pool {'none' if not a.id_pool else a.id_pool} "
          f"({sum(len(v) for v in resolver.ids.values())} ids)")

    if a.enable_mutations:
        if not a.i_know_this_writes:
            print("ERROR: --enable-mutations WRITES to the target DB "
                  "(read-tracking: channel_user_status, activities.isRead, empty draft rows\n"
                  "       for the authenticated user). Re-run with --i-know-this-writes "
                  "to confirm.", file=sys.stderr)
            return 2
        msampler = MutationSampler(bl.mutations, rng)
        supported_pct = sum(m.weight for m in msampler.supported)
        print(f"MUTATIONS ON: {len(msampler.supported)} types "
              f"({supported_pct:.0f}% of prod write volume), "
              f"{a.mutations_per_min}/min per client — writes as the authenticated user")
        if a.impact:
            idx = ImpactIndex.from_file(a.impact, set(MUTATION_ARG_BUILDERS))
            if idx is None:
                print(f"WARN: --impact {a.impact} unreadable — targeting off, "
                      "G14 will SKIP", file=sys.stderr)
                cfg.impact_path = None
            else:
                msampler.impact = idx
                print(f"IMPACT TARGETING ON: {len(idx.edges)} query-mutator edges "
                      f"for the {len(msampler.supported)} buildable mutators "
                      f"(bias {a.impact_bias:.0%})")
    else:
        msampler = None
        if a.impact:
            print("NOTE: --impact has no effect without --enable-mutations", file=sys.stderr)
            cfg.impact_path = None

    if cfg.lifecycle:
        print(f"LIFECYCLE ON: mean session {cfg.session_mean_s:.0f}s, "
              f"{cfg.zombie_pct:.0%} zombies, {cfg.abrupt_pct:.0%} abrupt ends, "
              f"{cfg.resume_pct:.0%} cookie resumes")
    if cfg.auth_pool:
        print(f"AUTH POOL: {len(cfg.auth_pool)} identities round-robin")

    if a.dry_run:
        dry_run(cfg, sampler, resolver, 20000, msampler)
        return 0

    if "REPLACE-ME" in a.target:
        print("ERROR: set --target to a real zero-cache URL (or use --dry-run).", file=sys.stderr)
        return 2

    start = time.time()
    start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start))
    print(f"driving load for {a.duration}s against {a.target} ...")
    try:
        stats = asyncio.run(run_live(cfg, sampler, resolver, msampler))
    except ModuleNotFoundError:
        print("ERROR: live mode needs websockets -> pip install websockets", file=sys.stderr)
        return 2
    end_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time()))

    path = write_summary(cfg, stats, start_iso, end_iso, a.out_dir,
                         impact=msampler.impact if msampler else None)
    window_min = max(1, (a.duration + 120) // 60)
    print(f"\nrun summary: {path}")
    print(f"opened={stats.opened}/{cfg.connections} puts={stats.puts_sent} "
          f"pokes={stats.pokes} muts={stats.mutations_sent} "
          f"(ok={stats.mutation_ok} err={stats.mutation_err}) "
          f"rehomes={stats.rehomes} reconnects={stats.reconnects} "
          f"errors={stats.errors} latency_samples={len(stats.latencies_ms)}"
          f"+{len(stats.latencies_initial_ms)}init")
    if cfg.lifecycle:
        print(f"lifecycle: sessions={stats.sessions} resumes={stats.resumes} "
              f"aborts={stats.aborts} zombies={stats.zombies} "
              f"invariant_violations={stats.invariants}")
    elif stats.invariants:
        print(f"INVARIANT VIOLATIONS: {stats.invariants}")
    if msampler is not None and msampler.impact is not None:
        print(f"impact: edges exercised={len(stats.impact_edges)} "
              f"targeted={stats.impact_targeted} fallback={stats.impact_fallback}")
    if stats.per_error:
        print("error kinds:")
        for k, v in sorted(stats.per_error.items(), key=lambda kv: -kv[1])[:8]:
            print(f"  {v:6d}  {k}")
    print(f"\nNOW GATE THE RUN:\n  python3 tools/evaluate_gates.py --window {window_min}m")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
