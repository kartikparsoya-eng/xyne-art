#!/usr/bin/env python3
"""
diff_concurrent.py — G43: W-mode CONCURRENT-workload TS ⇄ Rust differential.

Why: G32-G39 are L-mode lockstep — one scripted client, quiesce between
steps. Lockstep cannot see race-dependent divergences: several clients in
ONE client group interleaving query changes, upstream commits landing mid-
hydration, a client reconnecting mid-poke. That is exactly the class where
the Rust async architecture most plausibly diverges from single-threaded TS.

Design: per side, N clients share one client group. Both sides run the SAME
seeded op schedule (client index → put/del/reconnect at jittered offsets),
while a background writer touches hot subscribed rows in the SHARED upstream
PG (both sides replicate the same commits). Ops race freely — no per-op
barrier. At the end both sides quiesce, and the CONVERGED end-state must
match:

  G43a  CVR canonical state (canon_cvr + provenance watermark; desires
        compared per (client-INDEX, queryHash) since cids are random)
  G43b  rowSetSignature equality on shared transformationHashes
  G43c  per-client final got-query sets from the wire frames

Interleaving-dependent internals (poke slicing, patchVersions) are already
normalized away by canon_cvr — W-mode gates END state, not the race itself.

Usage: python3 harness/diff_concurrent.py [--clients 4] [--ops 24]
       [--writes 12] [--seed 7]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    Side, make_sides, open_side, reader, quiesce, Report, load_auth,
    load_client_schema, dump_cvr, canon_cvr, diff_canon, diff_signatures,
)
from workload import (  # noqa: E402
    init_connection_message, change_desired_queries_message, query_del,
)
from diff_surface import pick_queries  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))

PG_CONTAINER = os.environ.get("AB_PG_CONTAINER", "xyne-sandbox-postgres")
PG_USER = os.environ.get("AB_PG_USER", "xyne")
PG_DB = os.environ.get("AB_PG_DB", "sandbox_rust_test_db")


def upstream_touch() -> bool:
    """One replication event both sides must apply: bump lastActivityAt on a
    hot subscribed row (same tables bg_writer.py targets; column names
    verified there against the sandbox schema)."""
    sql = ('UPDATE public.conversations SET "lastActivityAt" = now() '
           'WHERE "conversationId" IN (SELECT "conversationId" FROM '
           'public.conversations ORDER BY "lastActivityAt" DESC LIMIT 1);')
    out = subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", PG_USER, "-d", PG_DB,
         "-Atc", sql], capture_output=True, text=True, timeout=30)
    return out.returncode == 0


def make_schedule(n_clients: int, n_ops: int, puts: list, seed: int) -> list:
    """Seeded op schedule, identical for both sides. Each entry:
    (delay_s, client_idx, kind, payload). Keeps every query desired by at
    least one PUT before any DEL of it so end-state isn't vacuously empty."""
    rng = random.Random(seed)
    hashes = [p["hash"] for p in puts]
    by_hash = {p["hash"]: p for p in puts}
    desired: set[str] = set()
    ops = []
    t = 0.0
    for i in range(n_ops):
        t += rng.uniform(0.05, 0.35)
        idx = rng.randrange(n_clients)
        if desired and rng.random() < 0.3:
            h = rng.choice(sorted(desired))
            desired.discard(h)
            ops.append((t, idx, "del", h))
        else:
            h = rng.choice(hashes)
            desired.add(h)
            ops.append((t, idx, "put", by_hash[h]))
        # One mid-session reconnect of client 1, roughly mid-schedule.
        if i == n_ops // 2:
            t += rng.uniform(0.05, 0.2)
            ops.append((t, 1 % n_clients, "reconnect", None))
    return ops


async def run_side(tpl: Side, auth: dict, schema: dict, puts: list,
                   schedule: list, n_clients: int) -> tuple[str, list, dict]:
    """Run the schedule on one side. Returns (cgid, sides, got_by_idx)."""
    import websockets  # noqa: F401  (transitively required)
    cgid = f"abconc-{random.Random().randrange(16**12):012x}"
    sides: list[Side] = []
    stops: list[asyncio.Event] = []
    tasks: list[asyncio.Task] = []
    # Client 0 carries the initial queries (init hydration racing later ops);
    # the rest connect with empty inits, staggered — no barrier.
    for idx in range(n_clients):
        s = Side(tpl.name, tpl.target, tpl.cvr_schema, tpl.container)
        s.fresh_ids()
        s.cgid = cgid
        init = init_connection_message(puts if idx == 0 else [],
                                       client_schema=schema)
        await open_side(s, auth["token"], init,
                        extra={"userID": auth["userID"]})
        stop = asyncio.Event()
        tasks.append(asyncio.create_task(reader(s, stop)))
        sides.append(s)
        stops.append(stop)
        await asyncio.sleep(0.1)

    t0 = time.perf_counter()
    for delay, idx, kind, payload in schedule:
        now = time.perf_counter() - t0
        if delay > now:
            await asyncio.sleep(delay - now)
        s = sides[idx]
        try:
            if kind == "put":
                await s.ws.send(json.dumps(
                    change_desired_queries_message([payload])))
            elif kind == "del":
                await s.ws.send(json.dumps(
                    change_desired_queries_message([query_del(payload)])))
            elif kind == "reconnect":
                cookie = s.last_cookie
                stops[idx].set()
                try:
                    await s.ws.close()
                except Exception:
                    pass
                tasks[idx].cancel()
                s2 = Side(s.name, s.target, s.cvr_schema, s.container,
                          cgid=s.cgid, cid=s.cid)
                await open_side(s2, auth["token"], None, base_cookie=cookie,
                                extra={"userID": auth["userID"]})
                stops[idx] = asyncio.Event()
                tasks[idx] = asyncio.create_task(reader(s2, stops[idx]))
                sides[idx] = s2
        except Exception as e:
            print(f"  [{tpl.name}] op {kind}@{idx} failed: {e}", flush=True)

    await quiesce(sides, quiet_s=2.0, max_s=45)

    got_by_idx: dict[int, set] = {}
    for idx, s in enumerate(sides):
        got: set[str] = set()
        for f in s.frames:
            if f.tag == "pokePart":
                for g in f.body.get("gotQueriesPatch") or []:
                    if isinstance(g, dict):
                        if g.get("op") == "put":
                            got.add(g.get("hash"))
                        elif g.get("op") == "del":
                            got.discard(g.get("hash"))
        got_by_idx[idx] = got

    for idx, s in enumerate(sides):
        stops[idx].set()
        try:
            await s.ws.close()
        except Exception:
            pass
        tasks[idx].cancel()
    await asyncio.sleep(1.5)  # final CVR flush
    return cgid, sides, got_by_idx


def desires_by_index(dump: dict, cid_to_idx: dict[str, int]) -> dict:
    """Per-(client index, queryHash) desire end-state — canon_cvr's by-hash
    keying assumes a single client and would collide here."""
    out = {}
    for r in dump["desires"]:
        idx = cid_to_idx.get(r["clientID"])
        out[(idx, r["queryHash"])] = {
            "deleted": r["deleted"],
            "ttlMs": r["ttlMs"],
            "inactive": r["inactivatedAtMs"] is not None,
        }
    return out


async def amain(a) -> int:
    rep = Report(a.report)
    auth, schema = load_auth(), load_client_schema()
    rust_tpl, ts_tpl = make_sides()
    puts = pick_queries(a.queries)
    schedule = make_schedule(a.clients, a.ops, puts, a.seed)
    print(f"G43 concurrent: {a.clients} clients/side, {len(schedule)} ops, "
          f"{a.writes} upstream touches, seed={a.seed}", flush=True)

    async def writer_task():
        for _ in range(a.writes):
            ok = await asyncio.to_thread(upstream_touch)
            if not ok:
                print("  upstream touch failed (writer stopped)", flush=True)
                return
            await asyncio.sleep(0.4)

    wt = asyncio.create_task(writer_task())
    # Both sides run CONCURRENTLY against the same upstream commits.
    (r_cgid, r_sides, r_got), (t_cgid, t_sides, t_got) = await asyncio.gather(
        run_side(rust_tpl, auth, schema, puts, schedule, a.clients),
        run_side(ts_tpl, auth, schema, puts, schedule, a.clients),
    )
    await wt

    dr, dt = dump_cvr(rust_tpl.cvr_schema, [r_cgid]), \
        dump_cvr(ts_tpl.cvr_schema, [t_cgid])
    rvs = [i.get("replicaVersion")
           for d in (dr, dt) for i in d["instances"] if i.get("replicaVersion")]
    watermark = max(rvs) if rvs else None
    cr, ct = canon_cvr(dr, watermark), canon_cvr(dt, watermark)
    # Desires: re-key per client INDEX (cids are random per side).
    cid_idx_r = {s.cid: i for i, s in enumerate(r_sides)}
    cid_idx_t = {s.cid: i for i, s in enumerate(t_sides)}
    cr["desires"] = {str(k): v
                     for k, v in desires_by_index(dr, cid_idx_r).items()}
    ct["desires"] = {str(k): v
                     for k, v in desires_by_index(dt, cid_idx_t).items()}

    diffs = diff_canon(cr, ct)
    if diffs:
        rep.add("G43a concurrent-cvr", "FAIL",
                f"{len(diffs)} differences; first: {diffs[0]}", diffs[:50])
    else:
        rep.add("G43a concurrent-cvr", "PASS",
                f"{a.clients}-client racing end-state identical "
                f"(queries={len(cr['queries'])} rows={len(cr['rows'])})")

    sig_diffs, compared = diff_signatures(cr, ct)
    if sig_diffs:
        rep.add("G43b concurrent-signature", "FAIL",
                f"{len(sig_diffs)}/{compared} mismatches; first: {sig_diffs[0]}",
                sig_diffs[:20])
    elif compared == 0:
        rep.add("G43b concurrent-signature", "WATCH",
                "no comparable signatures under the concurrent schedule")
    else:
        rep.add("G43b concurrent-signature", "PASS",
                f"{compared} signatures identical")

    got_diffs = [f"client{i}: rust={sorted(r_got.get(i, set()))} "
                 f"ts={sorted(t_got.get(i, set()))}"
                 for i in range(a.clients)
                 if r_got.get(i, set()) != t_got.get(i, set())]
    if got_diffs:
        rep.add("G43c concurrent-got-sets", "FAIL",
                f"{len(got_diffs)} clients diverge; first: {got_diffs[0]}",
                got_diffs)
    else:
        rep.add("G43c concurrent-got-sets", "PASS",
                f"final got-query sets identical across {a.clients} clients")
    return rep.finish()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=4)
    ap.add_argument("--ops", type=int, default=24)
    ap.add_argument("--queries", type=int, default=8)
    ap.add_argument("--writes", type=int, default=12)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--report", default=os.path.join(
        os.path.dirname(HERE), "reports",
        f"diff-concurrent-{time.strftime('%Y%m%d-%H%M%S')}.json"))
    return asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
