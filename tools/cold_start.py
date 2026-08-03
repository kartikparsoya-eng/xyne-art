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
from workload import (  # noqa: E402
    ArgResolver, SchemaSynthesizer, WeightedSampler, init_connection_message,
    load_baseline, query_put,
)


def rid() -> str:
    r = random.SystemRandom()
    return "art-" + "".join(r.choice("abcdefghijklmnop0123456789") for _ in range(10))


def hydration_frame(msg: object) -> tuple[int, bool, str | None]:
    """Return (row patch count, completed poke, protocol error)."""
    if not isinstance(msg, list) or not msg:
        return 0, False, None
    tag = msg[0]
    body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
    if tag in ("error", "transformError"):
        return 0, False, str(body.get("kind") or body.get("queryName") or tag)
    if tag == "pokePart":
        return len(body.get("rowsPatch") or []), False, None
    if tag == "pokeEnd":
        return 0, not body.get("cancel"), None
    if tag == "poke":
        rows = sum(len(part.get("rowsPatch") or [])
                   for part in body.get("pokeParts") or [] if isinstance(part, dict))
        return rows, True, None
    return 0, False, None


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
    baseline = load_baseline(a.baseline)
    rng = random.Random(42)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng)
    schema_doc = None
    if a.current_query_schema and os.path.exists(a.current_query_schema):
        with open(a.current_query_schema) as f:
            schema_doc = json.load(f)
        current = schema_doc.get("queries") or {}
        baseline.queries = [op for op in baseline.queries if op.name in current]
        baseline.oneshots = [op for op in baseline.oneshots if op.name in current]
    query_synth = None
    if schema_doc is not None:
        minimal_queries = {}
        for name, entry in (schema_doc.get("queries") or {}).items():
            args_schema = entry.get("args")
            minimal_queries[name] = {
                **entry,
                "args": {} if args_schema is None else {
                    key: schema for key, schema in args_schema.items()
                    if not schema.get("optional") and not schema.get("hasDefault")
                },
            }
        query_synth = SchemaSynthesizer(
            {"mutators": minimal_queries,
             "enums": schema_doc.get("enums") or {}},
            resolver.ids, resolver.scalars, {}, rng,
        )
    sampler = WeightedSampler(baseline.all_read_ops, rng)
    puts = []
    query_names = []
    seen_hashes = set()
    attempts = 0
    while len(puts) < a.queries and attempts < a.queries * 100:
        attempts += 1
        op = sampler.sample()
        if query_synth is not None:
            args, _ = query_synth.synth(op.name, int(time.time() * 1000))
            ok = args is not None
        else:
            args, ok = resolver.resolve(op)
        if not ok:
            continue
        put = query_put(op.name, args)
        if put["hash"] in seen_hashes:
            continue
        seen_hashes.add(put["hash"])
        puts.append(put)
        query_names.append(op.name)
    if len(puts) != a.queries:
        await ws.close()
        checks.append({"name": "hydrate-setup", "verdict": "ERROR",
                       "detail": f"resolved only {len(puts)}/{a.queries} queries"})
        total_ms = round((time.perf_counter() - t0) * 1000)
        return {"verdict": "ERROR", "checks": checks, "boot_ms": boot_ms,
                "hydrate_ms": None, "total_ms": total_ms,
                "summary": "could not build an evidentiary hydration query set"}
    init = init_connection_message(puts,
                                    client_schema=json.load(open(a.client_schema)) if a.client_schema else None)
    await ws.send(json.dumps(init))
    t_send = time.perf_counter()
    hydrate_deadline = t_send + a.hydrate_budget_ms / 1000.0
    got_poke = False
    rows_seen = 0
    protocol_error = None
    while time.perf_counter() < hydrate_deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        except Exception:
            break
        try:
            msg = json.loads(raw)
            row_ops, completed, frame_error = hydration_frame(msg)
            rows_seen += row_ops
            if frame_error:
                protocol_error = frame_error
                break
            if completed and rows_seen:
                got_poke = True
                break
        except Exception:
            continue
    await ws.close()
    hydrate_ms = round((time.perf_counter() - t_send) * 1000)
    total_ms = round((time.perf_counter() - t0) * 1000)

    if not got_poke:
        checks.append({"name": "hydrate", "verdict": "FAIL",
                       "detail": (f"protocol error: {protocol_error}" if protocol_error
                                  else f"no nonempty completed poke within "
                                       f"{a.hydrate_budget_ms}ms")})
        return {"verdict": "ERROR", "checks": checks, "boot_ms": boot_ms,
                "hydrate_ms": hydrate_ms, "total_ms": total_ms,
                "summary": f"boot {boot_ms}ms OK but never hydrated in {hydrate_ms}ms"}

    checks.append({"name": "hydrate", "verdict": "PASS",
                   "detail": f"first completed hydration in {hydrate_ms}ms "
                             f"({rows_seen} row patches)"})
    over = total_ms > a.budget_ms
    verdict = "FAIL" if over else "PASS"
    checks.append({"name": "budget", "verdict": verdict,
                   "detail": f"total {total_ms}ms vs budget {a.budget_ms}ms"})
    summary = (f"cold start: boot={boot_ms}ms hydrate={hydrate_ms}ms "
               f"total={total_ms}ms (budget {a.budget_ms}ms) "
               f"{'OVER' if over else 'OK'}")
    return {"verdict": verdict, "checks": checks, "boot_ms": boot_ms,
            "hydrate_ms": hydrate_ms, "total_ms": total_ms, "summary": summary,
            "rows_seen": rows_seen, "queries": query_names}


def main() -> int:
    ap = argparse.ArgumentParser(description="G18: cold-start timing gate.")
    ap.add_argument("--target", required=True)
    ap.add_argument("--container", default=None, help="docker container to restart")
    ap.add_argument("--fresh", action="store_true", help="docker stop before start (cold cache)")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[], help="k=v connect-URL params")
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--current-query-schema", default="raw/arg-schemas.source.json")
    ap.add_argument("--queries", type=int, default=8)
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
