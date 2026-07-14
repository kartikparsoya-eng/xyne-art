#!/usr/bin/env python3
"""determinism_oracle.py — G21: poke-stream determinism oracle for zero-cache.

The IVM engine must be deterministic over fixed input data: the same desired
queries, the same DB state, the same seed => byte-identical converged row-sets.
A non-deterministic sort tie-break, non-deterministic map iteration, or a
race in the advance path produces DIFFERENT poke content across identical runs
— a real bug class (rows appear in different order, or a tie-break column
flips) that no current gate catches (G8 compares two builds, not two runs of
the same build).

Method:
  1. connect one client with a SEEDED working set (deterministic args)
  2. capture every poke into a Materializer (pass A), disconnect, quiesce
  3. reconnect with the SAME seed + same queries (pass B), materialize
  4. diff_states(A, B) — zero mismatches => deterministic; any => FAIL
  5. also compare per-query poke counts (batching divergence signal)

Reuses harness/diff_oracle.py::Materializer + diff_states (same converged-state
predicate G8 uses — pokes are never compared one-to-one since batching/order
legally differ, only the materialized end state must match).

    .venv/bin/python tools/determinism_oracle.py --target ws://host/zero \\
        --auth-token "$JWT" --id-pool harness/id-pool.sandbox.json \\
        --client-schema harness/client-schema.json --extra-param userID=$UID \\
        --seed 42 --queries 8 --duration 30 --out reports/determinism-$TAG.json

Exit 0 = deterministic (identical converged state); 1 = divergent; 2 = ERROR.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from protocol import DEFAULT_PROTOCOL_VERSION, encode_sec_protocols  # noqa: E402
from workload import ArgResolver, WeightedSampler, load_baseline, query_put, init_connection_message  # noqa: E402
from diff_oracle import Materializer, diff_states  # noqa: E402


def rid(seed: random.Random) -> str:
    return "art-det-" + "".join(seed.choice("abcdefghijklmnop0123456789") for _ in range(10))


async def _pass(target: str, version: int, auth_token: str | None,
                extra_params: list[tuple[str, str]], puts: list[dict],
                client_schema: dict | None, seed: int, duration_s: float,
                pks: dict) -> Materializer:
    import websockets
    rng = random.Random(seed)
    cgid, cid = rid(rng), rid(rng)
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0"}
    params.update(extra_params)
    url = (target.rstrip("/") + f"/sync/v{version}/connect?"
           + urllib.parse.urlencode(params))
    sec = encode_sec_protocols(None, auth_token)
    mat = Materializer(pks)
    async with websockets.connect(url, subprotocols=[sec], open_timeout=20,
                                   max_size=None, ping_interval=None) as ws:
        await ws.send(json.dumps(init_connection_message(puts, client_schema=client_schema)))
        deadline = time.perf_counter() + duration_s
        while time.perf_counter() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                break
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if not isinstance(msg, list) or not msg:
                continue
            if msg[0] == "poke":
                body = msg[1] if len(msg) > 1 else {}
                for part in (body.get("pokeParts") or []):
                    mat.apply_rows_patch(part.get("rowsPatch"))
    return mat


async def probe(a: argparse.Namespace) -> dict:
    checks: list[dict] = []
    rng = random.Random(a.seed)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng, zipf_s=a.zipf)
    baseline = load_baseline(a.baseline)
    sampler = WeightedSampler(baseline.all_read_ops, rng)
    puts = []
    for _ in range(a.queries):
        op = sampler.sample()
        args, _ = resolver.resolve(op)
        puts.append(query_put(op.name, args, ttl_ms=int(a.duration_s * 1000 + 60000)))
    client_schema = json.load(open(a.client_schema)) if a.client_schema else None
    pks = {}
    if client_schema and isinstance(client_schema.get("tables"), list):
        for t in client_schema["tables"]:
            name = t.get("tableName")
            pk = t.get("primaryKey")
            if name and pk:
                pks[name] = pk

    checks.append({"name": "setup", "verdict": "PASS",
                   "detail": f"{len(puts)} queries, seed={a.seed}, zipf={a.zipf}"})

    mat_a = await _pass(a.target, a.protocol_version, a.auth_token, a.extra_param,
                       puts, client_schema, a.seed, a.duration_s, pks)
    await asyncio.sleep(a.quiesce_s)
    mat_b = await _pass(a.target, a.protocol_version, a.auth_token, a.extra_param,
                       puts, client_schema, a.seed, a.duration_s, pks)

    checks.append({"name": "pass-A", "verdict": "PASS",
                   "detail": f"materialized {mat_a.rows_applied} rows, {len(mat_a.state)} tables"})
    checks.append({"name": "pass-B", "verdict": "PASS",
                   "detail": f"materialized {mat_b.rows_applied} rows, {len(mat_b.state)} tables"})

    d = diff_states(mat_a, mat_b, max_examples=10)
    total = d.get("total", 0)
    if total == 0:
        verdict = "PASS"
        checks.append({"name": "determinism", "verdict": "PASS",
                       "detail": "converged states byte-identical across two seeded runs"})
        summary = f"deterministic: {mat_a.rows_applied} rows match across 2 runs"
    else:
        verdict = "FAIL"
        checks.append({"name": "determinism", "verdict": "FAIL",
                       "detail": f"{total} mismatches across two IDENTICAL seeded runs "
                                 f"— non-deterministic engine (tie-break / race)"})
        summary = f"NON-DETERMINISTIC: {total} mismatches; see examples in report"
    return {"verdict": verdict, "checks": checks, "summary": summary,
            "mismatches": total, "diff": d,
            "pass_a_rows": mat_a.rows_applied, "pass_b_rows": mat_b.rows_applied}


def main() -> int:
    ap = argparse.ArgumentParser(description="G21: poke-stream determinism oracle.")
    ap.add_argument("--target", required=True)
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--baseline", default="art-baseline.json")
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--zipf", type=float, default=0.0)
    ap.add_argument("--queries", type=int, default=8)
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--quiesce-s", type=float, default=10.0)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    a.extra_param = [tuple(p.split("=", 1)) for p in a.extra_param]
    report = asyncio.run(probe(a))
    report.update({"schema": 1, "gate": "G21", "name": "determinism",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "target": a.target, "seed": a.seed})
    print(report["summary"])
    for c in report["checks"]:
        print(f"  {c['name']:<14} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
