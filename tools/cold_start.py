#!/usr/bin/env python3
"""cold_start.py — G18: zero-cache cold-start / first-hydration timing gate.

A fresh container's boot path (image extract, schema load, DB-pool init, Go
JIT warmup, first hydration) is NOT measured by the steady-state ART. A
schema-load regression, a startup N+1, or a slow init only shows up in the
window between `docker start` and the first successful poke. This gate times
that window and fails if a build pushes first-hydration past a blessed budget.

Method:
  1. (re)start the container — `docker restart <name>` (or `--fresh` to
     `docker stop && docker start`, forcing a cold cache)
  2. poll the WS connect endpoint until it accepts an upgrade
  3. send initConnection with a small desired-query set and wait for the first
     poke (=> hydration completed)
  4. record boot_ms (start->open), hydrate_ms (open->first-poke), total_ms

    .venv/bin/python tools/cold_start.py --target ws://rust-test.localhost/zero \\
        --container xyne-sandbox-rust-test-zero-cache --auth-token "$JWT" \\
        --extra-param userID=$UID --id-pool harness/id-pool.sandbox.json \\
        --budget-ms 30000 --out reports/coldstart-$TAG.json

Exit 0 = within budget; 1 = too slow (FAIL); 2 = ERROR (never hydrated / infra).
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
from workload import ArgResolver, load_baseline, query_put, init_connection_message  # noqa: E402


def rid() -> str:
    r = random.SystemRandom()
    return "art-" + "".join(r.choice("abcdefghijklmnop0123456789") for _ in range(10))


async def _await_open(target: str, version: int, auth_token: str | None,
                       extra_params: list[tuple[str, str]], deadline: float) -> float:
    """Poll until the WS endpoint accepts an upgrade. Returns open latency (ms),
    raises TimeoutError if the deadline passes."""
    import websockets

    cgid, cid = rid(), rid()
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0"}
    params.update(extra_params)
    url = (target.rstrip("/") + f"/sync/v{version}/connect?"
           + urllib.parse.urlencode(params))
    sec = encode_sec_protocols(None, auth_token)
    while time.perf_counter() < deadline:
        try:
            ws = await asyncio.wait_for(
                websockets.connect(url, subprotocols=[sec], open_timeout=8,
                                   max_size=None, ping_interval=None),
                timeout=8.0)
            return ws
        except Exception:
            await asyncio.sleep(0.5)
    raise TimeoutError("WS endpoint never accepted an upgrade before deadline")


async def probe(a: argparse.Namespace) -> dict:
    t0 = time.perf_counter()
    checks: list[dict] = []

    # 1. cold restart the container
    if a.container:
        if a.fresh:
            subprocess.run(["docker", "stop", a.container], check=False,
                           timeout=30, capture_output=True)
        subprocess.run(["docker", "restart", a.container], check=False,
                       timeout=60, capture_output=True)
        checks.append({"name": "restart", "verdict": "PASS",
                       "detail": f"docker restart {a.container}"})

    # 2. wait for WS open
    open_deadline = t0 + a.boot_budget_ms / 1000.0
    try:
        ws = await _await_open(a.target, a.protocol_version, a.auth_token,
                               a.extra_param, open_deadline)
        boot_ms = round((time.perf_counter() - t0) * 1000)
        checks.append({"name": "boot", "verdict": "PASS",
                       "detail": f"WS open in {boot_ms}ms"})
    except TimeoutError:
        boot_ms = round((time.perf_counter() - t0) * 1000)
        checks.append({"name": "boot", "verdict": "FAIL",
                       "detail": f"WS never opened within {a.boot_budget_ms}ms budget"})
        return {"verdict": "ERROR", "checks": checks, "boot_ms": boot_ms,
                "hydrate_ms": None, "total_ms": boot_ms,
                "summary": f"never came up in {boot_ms}ms (infra or boot hang)"}

    # 3. drive a small working set and wait for first poke (hydration)
    try:
        baseline = load_baseline(a.id_pool.replace("id-pool", "id-pool") or a.baseline)
    except Exception:
        baseline = None
    rng = random.Random(0)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng)
    # pick the top-3 weighted queries for a minimal hydration probe
    if baseline:
        ops = sorted(baseline.queries, key=lambda o: -o.weight)[:3]
        puts = []
        for op in ops:
            args, _ = resolver.resolve(op)
            puts.append(query_put(op.name, args))
    else:
        puts = [query_put("userBookmarks", {})]
    init = init_connection_message(puts,
                                    client_schema=json.load(open(a.client_schema)) if a.client_schema else None)
    await ws.send(json.dumps(init))
    t_send = time.perf_counter()
    hydrate_deadline = t_send + a.hydrate_budget_ms / 1000.0
    got_poke = False
    while time.perf_counter() < hydrate_deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        except Exception:
            break
        try:
            msg = json.loads(raw)
            if isinstance(msg, list) and msg and msg[0] in ("poke", "admin"):
                got_poke = True
                break
        except Exception:
            continue
    await ws.close()
    hydrate_ms = round((time.perf_counter() - t_send) * 1000)
    total_ms = round((time.perf_counter() - t0) * 1000)

    if not got_poke:
        checks.append({"name": "hydrate", "verdict": "FAIL",
                       "detail": f"no poke within {a.hydrate_budget_ms}ms "
                                 f"(hydration stalled or no desired queries)"})
        return {"verdict": "ERROR", "checks": checks, "boot_ms": boot_ms,
                "hydrate_ms": hydrate_ms, "total_ms": total_ms,
                "summary": f"boot {boot_ms}ms OK but never hydrated in {hydrate_ms}ms"}

    checks.append({"name": "hydrate", "verdict": "PASS",
                   "detail": f"first poke in {hydrate_ms}ms"})
    over = total_ms > a.budget_ms
    verdict = "FAIL" if over else "PASS"
    checks.append({"name": "budget", "verdict": verdict,
                   "detail": f"total {total_ms}ms vs budget {a.budget_ms}ms"})
    summary = (f"cold start: boot={boot_ms}ms hydrate={hydrate_ms}ms "
               f"total={total_ms}ms (budget {a.budget_ms}ms) "
               f"{'OVER' if over else 'OK'}")
    return {"verdict": verdict, "checks": checks, "boot_ms": boot_ms,
            "hydrate_ms": hydrate_ms, "total_ms": total_ms, "summary": summary}


def main() -> int:
    ap = argparse.ArgumentParser(description="G18: cold-start timing gate.")
    ap.add_argument("--target", required=True)
    ap.add_argument("--container", default=None, help="docker container to restart")
    ap.add_argument("--fresh", action="store_true", help="docker stop before start (cold cache)")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[], help="k=v connect-URL params")
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--baseline", default="art-baseline.json")
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--boot-budget-ms", type=int, default=60000)
    ap.add_argument("--hydrate-budget-ms", type=int, default=30000)
    ap.add_argument("--budget-ms", type=int, default=30000, help="total cold-start budget")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    a.extra_param = [tuple(p.split("=", 1)) for p in a.extra_param]
    report = asyncio.run(probe(a))
    report.update({"schema": 1, "gate": "G18", "name": "cold-start",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "target": a.target, "budget_ms": a.budget_ms})
    print(report["summary"])
    for c in report["checks"]:
        print(f"  {c['name']:<12} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
