#!/usr/bin/env python3
"""
latency_ab.py — G42: paired TS ⇄ Rust latency A/B over the weighted workload.

Unlike G5/G25 (absolute SLOs vs the prod baseline), this measures the SAME
op classes on BOTH images, same box, same PG, alternating order per trial:

  connect     socket open → ['connected']
  cold        initConnection(K queries) → all gots + poke-quiet   (fresh cgid)
  incremental changeDesiredQueries(+1)  → next pokeEnd
  catchup     reconnect with own cookie → poke-quiet

Gate: rust p50 ≤ --p50-factor × ts p50 AND rust p95 ≤ --p95-factor × ts p95
per class (defaults 1.5 / 2.0). Always prints the full comparison table.

KNOWN ASYMMETRY: in the default sandbox the rust target is reached through
Traefik (ws://rust-test.localhost/zero) while TS is a direct host port —
the `connect` class carries proxy overhead on the rust side. Publish rust's
:4848 directly (AB_RUST_WS) for a fair connect comparison.

Usage: python3 harness/latency_ab.py [--trials 6] [--queries 5]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    Side, make_sides, open_side, reader, quiesce, Report, load_auth,
    load_client_schema,
)
from workload import (  # noqa: E402
    init_connection_message, change_desired_queries_message,
    load_baseline, ArgResolver, MutationSampler, custom_mutation,
    push_message,
)
from diff_surface import pick_queries  # noqa: E402
from diff_concurrent import upstream_touch  # noqa: E402
import random  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def make_push_builder():
    """Best-effort push-mutation builder from the prod baseline; None when the
    baseline carries no supported mutators (classes then report insufficient)."""
    try:
        rng = random.Random(1234)
        baseline = load_baseline(os.path.join(os.path.dirname(HERE),
                                              "art-baseline.json"))
        resolver = ArgResolver.from_pool_file(
            os.path.join(HERE, "id-pool.sandbox.json"), rng)
        sampler = MutationSampler(baseline.mutations, rng)
    except Exception:
        return None

    def build(cgid: str, cid: str, mid: int):
        now_ms = int(time.time() * 1000)
        built = sampler.build(resolver, now_ms)
        if built is None:
            return None
        name, args = built
        return push_message(cgid, [custom_mutation(mid, cid, name, args,
                                                   now_ms)],
                            request_id=f"{cid}-{mid}", now_ms=now_ms)
    return build


async def one_trial(tpl: Side, auth: dict, schema: dict, puts: list,
                    inc_put: dict, quiet_s: float,
                    push_builder=None) -> dict:
    """One full measurement cycle on one side. Fresh clientGroupID = cold."""
    s = Side(tpl.name, tpl.target, tpl.cvr_schema, tpl.container)
    s.fresh_ids()
    out: dict[str, float] = {}
    stop = asyncio.Event()

    init = init_connection_message(puts, client_schema=schema)
    t0 = time.perf_counter()
    await open_side(s, auth["token"], init, extra={"userID": auth["userID"]})
    out["connect"] = s.connected_ms
    rt = asyncio.create_task(reader(s, stop))
    want = {p["hash"] for p in puts}
    deadline = time.perf_counter() + 60
    while time.perf_counter() < deadline:
        got = set()
        for f in s.frames:
            if f.tag == "pokePart":
                for g in f.body.get("gotQueriesPatch") or []:
                    if isinstance(g, dict) and g.get("op") == "put":
                        got.add(g.get("hash"))
        if want <= got and (time.perf_counter() - s.last_activity) >= quiet_s:
            break
        await asyncio.sleep(0.15)
    out["cold"] = (time.perf_counter() - t0) * 1000.0

    n_ends = sum(1 for f in s.frames if f.tag == "pokeEnd")
    t1 = time.perf_counter()
    await s.ws.send(json.dumps(change_desired_queries_message([inc_put])))
    deadline = time.perf_counter() + 30
    while time.perf_counter() < deadline:
        if sum(1 for f in s.frames if f.tag == "pokeEnd"
               and not f.body.get("cancel")) > n_ends:
            break
        await asyncio.sleep(0.05)
    out["incremental"] = (time.perf_counter() - t1) * 1000.0

    # serving-lag: upstream commit → next non-cancel pokeEnd on this
    # (subscribed) client. Skipped when the touch fails or nothing pokes
    # (query set not covering the touched table) — class then reports
    # insufficient samples rather than failing.
    n_ends = sum(1 for f in s.frames if f.tag == "pokeEnd"
                 and not f.body.get("cancel"))
    t2 = time.perf_counter()
    if await asyncio.to_thread(upstream_touch):
        deadline = time.perf_counter() + 15
        while time.perf_counter() < deadline:
            if sum(1 for f in s.frames if f.tag == "pokeEnd"
                   and not f.body.get("cancel")) > n_ends:
                out["serving_lag"] = (time.perf_counter() - t2) * 1000.0
                break
            await asyncio.sleep(0.05)

    # push: custom mutation → lastMutationIDChanges ack for this client in a
    # pokePart (write round-trip through the relay + CVR poke).
    if push_builder is not None:
        msg = push_builder(s.cgid, s.cid, 1)
        if msg is not None:
            t3 = time.perf_counter()
            try:
                await s.ws.send(json.dumps(msg))
                deadline = time.perf_counter() + 20
                while time.perf_counter() < deadline:
                    acked = any(
                        f.tag == "pokePart"
                        and (f.body.get("lastMutationIDChanges") or {})
                        .get(s.cid, 0) >= 1
                        for f in s.frames)
                    if acked:
                        out["push"] = (time.perf_counter() - t3) * 1000.0
                        break
                    await asyncio.sleep(0.05)
            except Exception:
                pass

    cookie = s.last_cookie
    stop.set()
    try:
        await s.ws.close()
    except Exception:
        pass
    rt.cancel()

    if cookie:
        s2 = Side(s.name, s.target, s.cvr_schema, s.container,
                  cgid=s.cgid, cid=s.cid)
        stop2 = asyncio.Event()
        t2 = time.perf_counter()
        await open_side(s2, auth["token"], None, base_cookie=cookie,
                        extra={"userID": auth["userID"]})
        rt2 = asyncio.create_task(reader(s2, stop2))
        await quiesce([s2], quiet_s=1.2, max_s=20)
        out["catchup"] = (time.perf_counter() - t2) * 1000.0
        stop2.set()
        try:
            await s2.ws.close()
        except Exception:
            pass
        rt2.cancel()
    return out


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


async def amain(a) -> int:
    rep = Report(a.report)
    auth, schema = load_auth(), load_client_schema()
    rust_tpl, ts_tpl = make_sides()
    puts = pick_queries(a.queries)
    inc_put = pick_queries(a.queries + 1)[a.queries]
    push_builder = make_push_builder()
    if push_builder is None:
        print("  (push class disabled: no supported baseline mutations)")
    samples: dict[str, dict[str, list[float]]] = {}

    for trial in range(a.trials):
        order = [(rust_tpl, "rust"), (ts_tpl, "ts")]
        if trial % 2:                    # alternate to kill warmup/order bias
            order.reverse()
        for tpl, name in order:
            try:
                r = await one_trial(tpl, auth, schema, puts, inc_put,
                                    a.quiet_s, push_builder)
            except Exception as e:
                print(f"  trial {trial} {name}: ERROR {e}", flush=True)
                continue
            for k, v in r.items():
                samples.setdefault(k, {}).setdefault(name, []).append(v)
        print(f"  trial {trial + 1}/{a.trials} done", flush=True)

    print(f"\n{'class':12s} {'n':>3s}  {'rust p50':>9s} {'ts p50':>9s} "
          f"{'ratio':>6s}  {'rust p95':>9s} {'ts p95':>9s} {'ratio':>6s}")
    fails = []
    table = {}
    for cls in ("connect", "cold", "incremental", "serving_lag", "push",
                "catchup"):
        cs = samples.get(cls, {})
        xr, xt = cs.get("rust", []), cs.get("ts", [])
        if len(xr) < a.min_trials or len(xt) < a.min_trials:
            print(f"{cls:12s} insufficient samples rust={len(xr)} ts={len(xt)}")
            continue
        r50, t50 = pct(xr, 50), pct(xt, 50)
        r95, t95 = pct(xr, 95), pct(xt, 95)
        ratio50 = r50 / t50 if t50 else float("inf")
        ratio95 = r95 / t95 if t95 else float("inf")
        table[cls] = {"rust_p50": r50, "ts_p50": t50, "rust_p95": r95,
                      "ts_p95": t95, "n": len(xr)}
        print(f"{cls:12s} {len(xr):>3d}  {r50:>8.0f}ms {t50:>8.0f}ms "
              f"{ratio50:>5.2f}x  {r95:>8.0f}ms {t95:>8.0f}ms {ratio95:>5.2f}x")
        if ratio50 > a.p50_factor or ratio95 > a.p95_factor:
            fails.append(f"{cls}: p50 {ratio50:.2f}x (cap {a.p50_factor}) "
                         f"p95 {ratio95:.2f}x (cap {a.p95_factor})")
    print()
    if fails:
        rep.add("G42 latency-ab", "FAIL",
                f"rust regresses vs ts: {'; '.join(fails)}", table)
    elif not table:
        rep.add("G42 latency-ab", "FAIL", "no measurable classes", samples)
    else:
        worst = max((v["rust_p50"] / v["ts_p50"]) for v in table.values()
                    if v["ts_p50"])
        rep.add("G42 latency-ab", "PASS",
                f"{len(table)} classes within caps (worst p50 ratio "
                f"{worst:.2f}x)", table)
    return rep.finish()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=6)
    ap.add_argument("--min-trials", type=int, default=4)
    ap.add_argument("--queries", type=int, default=5)
    ap.add_argument("--quiet-s", type=float, default=1.2)
    ap.add_argument("--p50-factor", type=float, default=1.5)
    ap.add_argument("--p95-factor", type=float, default=2.0)
    ap.add_argument("--report", default=os.path.join(
        os.path.dirname(HERE), "reports",
        f"latency-ab-{time.strftime('%Y%m%d-%H%M%S')}.json"))
    return asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
