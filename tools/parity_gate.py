#!/usr/bin/env python3
"""parity_gate.py — G25: Go-vs-TS latency-parity gate.

The ART catches Go-vs-Go regressions (did this commit make things worse?) but
NOT Go-vs-TS fundamental architecture gaps (a missing query planner, an N+1,
a missing index). The diff oracle (G8) proves Go and TS return identical ROWS;
it emits zero latency. A query like userAllChannels (weight 254, ~0.7%) that
is 5x slower on Go than TS due to a missing N+1 optimization sails clean
through the correctness oracle — same rows, different cost. This gate closes
that hole: it drives identical load at both builds, computes the per-query
Go/TS latency RATIO, and FAILs when one build is structurally slower than the
other beyond the noise floor.

Why a RATIO, not absolute numbers: both builds hit the IDENTICAL sandbox, so
sandbox-size differences cancel. If Go is 5x slower than TS at 100 channels,
it's still ~5x at 1000 — N+1 is O(n) on both sides, the ratio is preserved.
The ratio doesn't need prod parity (the "oracle is directional" caveat stops
mattering). art-baseline.json's per-query prod p50/p95 provide an absolute
budget as a belt-and-suspenders second signal.

Three modes (composable):
  consume  : --primary-run reports/run-go.json --mirror-run reports/run-ts.json
            (read two existing replay reports, compute ratios — no server needed)
  drive    : --drive --primary-target ws://go --mirror-target ws://ts ...
            (invoke replay.py against each, then compute ratios)
  oversample: --oversample --min-samples 100
            (decouple sample count from prod weight — drives low-weight queries
            like userAllChannels to a fixed N instead of their natural ~11)
  cascade  : --cascade --timeout-ms 500
            (when a query exceeds the client-timeout threshold, simulate the
            destroy → cold re-hydrate cycle and record the amplification
            multiplier — the cascading 17x reconnect cycle the ART doesn't
            model)

    # consume two existing run reports:
    .venv/bin/python tools/parity_gate.py \\
        --primary-run reports/run-go.json --mirror-run reports/run-ts.json \\
        --factor 2.0 --out reports/parity-$TAG.json

    # self-driving A/B:
    .venv/bin/python tools/parity_gate.py --drive \\
        --primary-target ws://rust-test.localhost/zero \\
        --mirror-target ws://rust-test.localhost/zero-ts \\
        --auth-token "$JWT" --id-pool harness/id-pool.sandbox.json \\
        --out reports/parity-$TAG.json

Exit 0 = parity holds (no query exceeds ratio + noise floor); 1 = parity
violation (a query is structurally slower); 2 = ERROR (infra).
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
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from protocol import DEFAULT_PROTOCOL_VERSION, encode_sec_protocols  # noqa: E402
from workload import (  # noqa: E402
    ArgResolver, load_baseline, query_put,
    init_connection_message,
)


# --------------------------------------------------------------------------- #
# Pure logic: ratio computation (unit-tested; no server needed)
# --------------------------------------------------------------------------- #
def compute_ratios(primary_pq: dict, mirror_pq: dict,
                   factor: float = 2.0, min_delta_ms: float = 100.0,
                   min_samples: int = 10, min_baseline_ms: float = 10.0,
                   quantile: str = "p95") -> dict:
    """Compare per-query latency between two builds. Returns
    {compared, offenders, details, verdict}.

    Noise-floor rules (same as G5b so the gates agree on what's signal):
      - >=min_samples on BOTH sides (percentiles are noise below that)
      - baseline (mirror) >= min_baseline_ms (sub-10ms makes ratios meaningless)
      - ratio > factor AND delta > min_delta_ms (both required — a 2x on a
        5ms query is 10ms, under the noise floor; a 50ms delta on a 500ms
        query is 1.1x, under the factor)

    primary = the build under test (Go); mirror = the reference (TS).
    A query where primary is slower -> offender (Go regressed vs TS).
    A query where mirror is slower -> offender too (TS regressed vs Go) —
    parity is bidirectional; either direction is a finding.
    """
    offenders: list[dict] = []
    compared = 0
    ratios: list[dict] = []

    for qname in sorted(set(primary_pq) | set(mirror_pq)):
        p = primary_pq.get(qname) or {}
        m = mirror_pq.get(qname) or {}
        ps, ms = p.get("samples", 0), m.get("samples", 0)
        if ps < min_samples or ms < min_samples:
            continue
        pv, mv = p.get(quantile), m.get(quantile)
        if pv is None or mv is None:
            continue
        compared += 1
        if mv < min_baseline_ms:
            ratios.append({"query": qname, "primary": pv, "mirror": mv,
                           "ratio": round(pv / mv, 2) if mv else None,
                           "verdict": "SKIP", "detail": f"mirror {quantile} {mv}ms < {min_baseline_ms}ms baseline"})
            continue
        # bidirectional ratio: always >= 1.0 (max/min), so a query where EITHER
        # build is slower triggers the factor check (parity is symmetric)
        slower, faster = (pv, mv) if pv >= mv else (mv, pv)
        ratio = slower / faster if faster else float("inf")
        delta = abs(pv - mv)
        direction = "primary-slower" if pv > mv else "mirror-slower"
        entry = {"query": qname, "primary": pv, "mirror": mv,
                 "ratio": round(ratio, 2), "delta_ms": round(delta, 1),
                 "direction": direction,
                 "samples": {"primary": ps, "mirror": ms}}
        ratios.append(entry)
        if ratio > factor and delta > min_delta_ms:
            entry["verdict"] = "FAIL"
            offenders.append(entry)
        else:
            entry["verdict"] = "PASS"

    offenders.sort(key=lambda o: o["ratio"], reverse=True)
    verdict = "FAIL" if offenders else "PASS"
    return {"compared": compared, "offenders": offenders,
            "ratios": ratios, "verdict": verdict}


def find_undersampled(pq: dict, min_samples: int = 100) -> list[dict]:
    """Queries below the oversample floor (low prod weight -> too few samples)."""
    out = []
    for qname, v in sorted(pq.items()):
        n = v.get("samples", 0)
        if 0 < n < min_samples:
            out.append({"query": qname, "samples": n, "target": min_samples})
    return out


def compute_cascade_multiplier(hydrate_times: list[float],
                               timeout_ms: float) -> dict:
    """Given per-hydration latencies and a client timeout, compute the cascade
    amplification: how many hydrations exceed the timeout (triggering a
    destroy + reconnect), and the total wall-clock cost of the cascade
    vs a single clean hydration.

    The real-world failure mode: a slow query (userAllChannels 1.2s) exceeds
    the client timeout (500ms) -> client destroys + reconnects -> cold
    re-hydrate of the whole working set -> if THAT also exceeds the timeout
    -> another reconnect -> amplification. The single-query 1.2s becomes a
    17x cascade because every re-hydrate re-pays the cost.
    """
    if not hydrate_times:
        return {"overflows": 0, "cascade_cost_ms": 0, "single_cost_ms": 0,
                "multiplier": 1.0, "verdict": "SKIP", "detail": "no samples"}
    overflows = [t for t in hydrate_times if t > timeout_ms]
    single = min(hydrate_times)  # best-case single hydration
    # cascade cost: each overflow triggers a reconnect that re-pays the
    # hydration, so the total is sum of all overflows (each is a re-hydrate)
    cascade = sum(overflows) if overflows else single
    multiplier = round(cascade / single, 1) if single > 0 else float("inf")
    verdict = "FAIL" if multiplier > 3.0 and len(overflows) >= 2 else "WATCH" if overflows else "PASS"
    detail = (f"{len(overflows)}/{len(hydrate_times)} hydrations exceeded "
              f"{timeout_ms}ms timeout; cascade {cascade:.0f}ms vs "
              f"single {single:.0f}ms = {multiplier}x amplification")
    return {"overflows": len(overflows), "cascade_cost_ms": round(cascade),
            "single_cost_ms": round(single), "multiplier": multiplier,
            "verdict": verdict, "detail": detail}


# --------------------------------------------------------------------------- #
# Drive mode: invoke replay.py against primary + mirror
# --------------------------------------------------------------------------- #
def drive_replay(target: str, auth_token: str | None, id_pool: str,
                 conns: int, working_set: int, churn_ms: int, duration: int,
                 extra: list[str], protocol: int, tag: str, label: str) -> str:
    out = f"reports/parity-{tag}-{label}.json"
    cmd = [sys.executable, "harness/replay.py",
           "--target", target, "--id-pool", id_pool,
           "--connections", str(conns), "--working-set", str(working_set),
           "--churn-ms", str(churn_ms), "--duration", str(duration),
           "--protocol-version", str(protocol), "--out", out]
    if auth_token:
        cmd += ["--auth-token", auth_token]
    cmd += extra
    subprocess.run(cmd, check=False, timeout=duration + 120)
    return out


def load_run_latencies(path: str) -> tuple[dict, dict]:
    """Return (latency_by_query, client_latency_steady_ms) from a run report."""
    d = json.load(open(path))
    return (d.get("latency_by_query") or {},
            d.get("client_latency_steady_ms") or {})


# --------------------------------------------------------------------------- #
# Oversample mode: targeted single-query probe for low-weight queries
# --------------------------------------------------------------------------- #
async def oversample_query(target: str, version: int, auth_token: str | None,
                           extra_params: list[tuple[str, str]], query_name: str,
                           resolver: ArgResolver, client_schema: dict | None,
                           n_samples: int, timeout_s: float = 5.0) -> list[float]:
    """Fire one query n_samples times across fresh connections, collecting
    hydration latencies. Decouples sample count from prod weight."""
    import websockets

    baseline = load_baseline("art-baseline.json")
    op = next((o for o in baseline.all_read_ops if o.name == query_name), None)
    if op is None:
        return []
    times: list[float] = []
    for _ in range(n_samples):
        args, _ = resolver.resolve(op)
        put = query_put(query_name, args, ttl_ms=60_000)
        init = init_connection_message([put], client_schema=client_schema)
        rng = random.Random()
        cgid = "art-os-" + "".join(rng.choice("abc012") for _ in range(10))
        cid = "art-os-" + "".join(rng.choice("abc012") for _ in range(10))
        params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
                  "ts": str(time.time() * 1000), "lmid": "0"}
        params.update(extra_params)
        url = (target.rstrip("/") + f"/sync/v{version}/connect?"
               + urllib.parse.urlencode(params))
        sec = encode_sec_protocols(None, auth_token)
        t0 = time.perf_counter()
        try:
            async with websockets.connect(url, subprotocols=[sec], open_timeout=15,
                                           max_size=None, ping_interval=None) as ws:
                await ws.send(json.dumps(init))
                deadline = time.perf_counter() + timeout_s
                while time.perf_counter() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        break
                    except Exception:
                        break
                    msg = json.loads(raw) if raw else None
                    if isinstance(msg, list) and msg and msg[0] == "poke":
                        times.append(round((time.perf_counter() - t0) * 1000, 1))
                        break
        except Exception:
            continue
    return times


# --------------------------------------------------------------------------- #
# Cascade mode: simulate timeout -> destroy -> cold re-hydrate
# --------------------------------------------------------------------------- #
async def cascade_probe(target: str, version: int, auth_token: str | None,
                        extra_params: list[tuple[str, str]], query_name: str,
                        resolver: ArgResolver, client_schema: dict | None,
                        n_cycles: int, timeout_ms: float,
                        working_set_size: int = 8) -> dict:
    """Simulate the real-world client failure cascade: connect with a working
    set, measure hydration. If it exceeds the client timeout, destroy +
    reconnect (cold re-hydrate) and measure again. Record the amplification."""
    import websockets

    baseline = load_baseline("art-baseline.json")
    ops = sorted(baseline.all_read_ops, key=lambda o: -o.weight)[:working_set_size]
    if query_name not in [o.name for o in ops]:
        op = next((o for o in baseline.all_read_ops if o.name == query_name), None)
        if op:
            ops.insert(0, op)
    puts = [query_put(o.name, resolver.resolve(o)[0]) for o in ops]
    init = init_connection_message(puts, client_schema=client_schema)

    hydrate_times: list[float] = []
    for cycle in range(n_cycles):
        rng = random.Random(cycle)
        cgid = "art-cas-" + "".join(rng.choice("abc012") for _ in range(10))
        cid = "art-cas-" + "".join(rng.choice("abc012") for _ in range(10))
        params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
                  "ts": str(time.time() * 1000), "lmid": "0"}
        params.update(extra_params)
        url = (target.rstrip("/") + f"/sync/v{version}/connect?"
               + urllib.parse.urlencode(params))
        sec = encode_sec_protocols(None, auth_token)
        t0 = time.perf_counter()
        try:
            async with websockets.connect(url, subprotocols=[sec], open_timeout=15,
                                           max_size=None, ping_interval=None) as ws:
                await ws.send(json.dumps(init))
                deadline = time.perf_counter() + 30
                while time.perf_counter() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        break
                    except Exception:
                        break
                    msg = json.loads(raw) if raw else None
                    if isinstance(msg, list) and msg and msg[0] == "poke":
                        hydrate_times.append(round((time.perf_counter() - t0) * 1000, 1))
                        break
        except Exception:
            continue
    return compute_cascade_multiplier(hydrate_times, timeout_ms)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
async def amain(a: argparse.Namespace) -> dict:
    checks: list[dict] = []
    tag = time.strftime("%Y%m%d-%H%M%S")

    primary_pq: dict = {}
    mirror_pq: dict = {}

    # --- consume mode ---
    if a.primary_run and a.mirror_run:
        primary_pq, _ = load_run_latencies(a.primary_run)
        mirror_pq, _ = load_run_latencies(a.mirror_run)
        checks.append({"name": "consume", "verdict": "PASS",
                       "detail": f"loaded {len(primary_pq)} + {len(mirror_pq)} per-query latencies"})
    # --- drive mode ---
    elif a.drive:
        if not a.primary_target or not a.mirror_target:
            checks.append({"name": "drive", "verdict": "ERROR",
                           "detail": "--drive requires --primary-target and --mirror-target"})
            return {"verdict": "ERROR", "checks": checks, "summary": "missing targets"}
        extra = []
        for p in a.extra_param:
            extra += ["--extra-param", p]
        p_path = drive_replay(a.primary_target, a.auth_token, a.id_pool,
                              a.connections, a.working_set, a.churn_ms,
                              a.duration, extra, a.protocol_version, tag, "primary")
        m_path = drive_replay(a.mirror_target, a.auth_token, a.id_pool,
                              a.connections, a.working_set, a.churn_ms,
                              a.duration, extra, a.protocol_version, tag, "mirror")
        primary_pq, _ = load_run_latencies(p_path)
        mirror_pq, _ = load_run_latencies(m_path)
        checks.append({"name": "drive", "verdict": "PASS",
                       "detail": f"replay vs primary ({len(primary_pq)} q) + mirror ({len(mirror_pq)} q)"})
    else:
        checks.append({"name": "setup", "verdict": "ERROR",
                       "detail": "need --primary-run/--mirror-run (consume) or --drive"})
        return {"verdict": "ERROR", "checks": checks, "summary": "no mode selected"}

    # --- ratio computation ---
    result = compute_ratios(primary_pq, mirror_pq, a.factor, a.min_delta_ms,
                            a.min_samples, a.min_baseline_ms, a.quantile)
    checks.append({"name": "ratio", "verdict": result["verdict"],
                   "detail": f"{result['compared']} queries compared; "
                             f"{len(result['offenders'])} parity violation(s)"})
    for o in result["offenders"][:8]:
        checks.append({"name": f"  {o['query']}", "verdict": "FAIL",
                       "detail": f"{o['direction']} {o['ratio']}x "
                                 f"({o['primary']:.0f} vs {o['mirror']:.0f}ms, "
                                 f"delta {o['delta_ms']:.0f}ms)"})

    # --- oversample mode ---
    if a.oversample:
        undersampled = (find_undersampled(primary_pq, a.min_samples)
                        + find_undersampled(mirror_pq, a.min_samples))
        if undersampled:
            resolver = ArgResolver.from_pool_file(a.id_pool, random.Random(a.seed))
            client_schema = json.load(open(a.client_schema)) if a.client_schema else None
            extra_params = [tuple(p.split("=", 1)) for p in a.extra_param]
            sampled = []
            for u in undersampled[:a.oversample_queries]:
                times = await oversample_query(
                    a.primary_target, a.protocol_version, a.auth_token,
                    extra_params, u["query"], resolver, client_schema,
                    a.min_samples)
                sampled.append({"query": u["query"], "collected": len(times),
                                 "p95": sorted(times)[int(len(times) * 0.95)] if times else None})
            checks.append({"name": "oversample", "verdict": "PASS",
                           "detail": f"boosted {len(sampled)} low-weight queries to "
                                     f"{a.min_samples} samples"})
        else:
            checks.append({"name": "oversample", "verdict": "PASS",
                           "detail": "all queries above sample floor"})

    # --- cascade mode ---
    if a.cascade:
        resolver = ArgResolver.from_pool_file(a.id_pool, random.Random(a.seed))
        client_schema = json.load(open(a.client_schema)) if a.client_schema else None
        extra_params = [tuple(p.split("=", 1)) for p in a.extra_param]
        cas = await cascade_probe(
            a.primary_target, a.protocol_version, a.auth_token, extra_params,
            a.cascade_query, resolver, client_schema, a.cascade_cycles,
            a.timeout_ms)
        checks.append({"name": "cascade", "verdict": cas["verdict"],
                       "detail": cas["detail"]})

    fail = any(c["verdict"] == "FAIL" for c in checks)
    error = any(c["verdict"] == "ERROR" for c in checks)
    verdict = "FAIL" if fail else ("ERROR" if error else "PASS")
    n_off = len(result["offenders"])
    summary = (f"parity: {result['compared']} compared, {n_off} violation(s) "
               f"(factor {a.factor}x, floor {a.min_delta_ms}ms)")
    return {"verdict": verdict, "checks": checks, "summary": summary,
            "ratios": result["ratios"], "offenders": result["offenders"],
            "compared": result["compared"]}


def main() -> int:
    ap = argparse.ArgumentParser(description="G25: Go-vs-TS latency-parity gate.")
    # consume mode
    ap.add_argument("--primary-run", default=None, help="Go run report (consume mode)")
    ap.add_argument("--mirror-run", default=None, help="TS run report (consume mode)")
    # drive mode
    ap.add_argument("--drive", action="store_true", help="invoke replay.py vs both targets")
    ap.add_argument("--primary-target", default=None, help="Go ws target (drive mode)")
    ap.add_argument("--mirror-target", default=None, help="TS ws target (drive mode)")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--id-pool", default="harness/id-pool.json")
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--connections", type=int, default=50)
    ap.add_argument("--working-set", type=int, default=12)
    ap.add_argument("--churn-ms", type=int, default=750)
    ap.add_argument("--duration", type=int, default=180)
    # ratio rules (mirror G5b noise floor)
    ap.add_argument("--factor", type=float, default=2.0,
                    help="FAIL when primary/mirror ratio exceeds this (default 2.0)")
    ap.add_argument("--min-delta-ms", type=float, default=100.0,
                    help="minimum absolute delta to count (noise floor; default 100ms)")
    ap.add_argument("--min-samples", type=int, default=10,
                    help="minimum samples on both sides (default 10)")
    ap.add_argument("--min-baseline-ms", type=float, default=10.0,
                    help="minimum mirror latency for ratio to be meaningful (default 10ms)")
    ap.add_argument("--quantile", default="p95", choices=["p50", "p95", "p99"],
                    help="which percentile to compare (default p95)")
    # oversample mode
    ap.add_argument("--oversample", action="store_true",
                    help="boost low-weight queries to --min-samples (tail coverage)")
    ap.add_argument("--oversample-queries", type=int, default=10,
                    help="max queries to oversample (default 10)")
    # cascade mode
    ap.add_argument("--cascade", action="store_true",
                    help="simulate timeout -> destroy -> re-hydrate cascade")
    ap.add_argument("--cascade-query", default="userAllChannels",
                    help="query to cascade-probe (default userAllChannels)")
    ap.add_argument("--cascade-cycles", type=int, default=20)
    ap.add_argument("--timeout-ms", type=float, default=500.0,
                    help="client timeout threshold for cascade (default 500ms)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    report = asyncio.run(amain(a))
    report.update({"schema": 1, "gate": "G25", "name": "latency-parity",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "factor": a.factor, "quantile": a.quantile})
    print(report["summary"])
    for c in report["checks"]:
        print(f"  {c['name']:<24} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
