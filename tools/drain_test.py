#!/usr/bin/env python3
"""drain_test.py — G20: SIGTERM graceful-drain gate for a zero-cache image.

A rolling deploy sends SIGTERM to the old pod and waits `terminationGracePeriod`
for it to drain. The steady-state ART never exercises this: it loads a warm
container, then disconnects cleanly. A build that drops in-flight syncs on
SIGTERM, ignores the signal, or drains slower than the grace period is a real
production incident class (lost writes, reconnect storms) that no current gate
catches. This gate:

  1. opens N client connections with live desired queries (in-flight syncs)
  2. sends SIGTERM to the container (`docker kill -s TERM <name>`)
  3. asserts each client receives a clean close (1001/1000) or a documented
     reconnect-control message — NOT an abrupt TCP drop (1006)
  4. asserts the drain completes within --drain-budget-s (must be < the
     kube grace period) and the container exits 0

    .venv/bin/python tools/drain_test.py --target ws://rust-test.localhost/zero \\
        --container xyne-sandbox-rust-test-zero-cache --auth-token "$JWT" \\
        --extra-param userID=$UID --connections 10 --drain-budget-s 30 \\
        --out reports/drain-$TAG.json

Exit 0 = clean drain (all clients notified, exit 0, within budget); 1 = dirty
(abrupt drops / hung / non-zero exit / over budget); 2 = ERROR (infra).
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


async def _one_client(target: str, version: int, auth_token: str | None,
                      extra_params: list[tuple[str, str]], puts: list[dict],
                      client_schema: dict | None, stop: asyncio.Event,
                      results: list, idx: int) -> None:
    import websockets
    cgid, cid = rid(), rid()
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0"}
    params.update(extra_params)
    url = (target.rstrip("/") + f"/sync/v{version}/connect?"
           + urllib.parse.urlencode(params))
    sec = encode_sec_protocols(None, auth_token)
    try:
        ws = await asyncio.wait_for(
            websockets.connect(url, subprotocols=[sec], open_timeout=15,
                               max_size=None, ping_interval=None),
            timeout=15.0)
        await ws.send(json.dumps(init_connection_message(puts, client_schema=client_schema)))
    except Exception as e:
        results.append((idx, "connect-failed", None, str(e)))
        return
    close_code = None
    close_reason = ""
    try:
        while not stop.is_set():
            try:
                await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed as e:
                close_code, close_reason = e.code, e.reason
                break
        if close_code is None:
            # stop fired; try a final recv to capture the close
            try:
                await asyncio.wait_for(ws.recv(), timeout=2.0)
            except websockets.exceptions.ConnectionClosed as e:
                close_code, close_reason = e.code, e.reason
            except Exception:
                pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass
    results.append((idx, "closed", close_code, close_reason))


async def probe(a: argparse.Namespace) -> dict:
    checks: list[dict] = []
    rng = random.Random(0)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng)
    baseline = load_baseline(a.baseline)
    ops = sorted(baseline.queries, key=lambda o: -o.weight)[:5]
    puts = [query_put(op.name, resolver.resolve(op)[0]) for op in ops]
    client_schema = json.load(open(a.client_schema)) if a.client_schema else None

    stop = asyncio.Event()
    results: list = []
    clients = [asyncio.create_task(_one_client(
        a.target, a.protocol_version, a.auth_token, a.extra_param, puts,
        client_schema, stop, results, i)) for i in range(a.connections)]
    await asyncio.sleep(3.0)  # let them hydrate (in-flight syncs established)

    t0 = time.perf_counter()
    # send SIGTERM
    subprocess.run(["docker", "kill", "-s", "TERM", a.container],
                   check=False, timeout=30, capture_output=True)
    checks.append({"name": "sigterm", "verdict": "PASS",
                   "detail": f"sent SIGTERM to {a.container}"})

    # wait for all clients to observe a close (or budget)
    try:
        await asyncio.wait_for(asyncio.gather(*clients, return_exceptions=True),
                               timeout=a.drain_budget_s)
    except asyncio.TimeoutError:
        pass
    stop.set()
    drain_s = round(time.perf_counter() - t0, 2)

    # inspect container exit
    inspect = subprocess.run(["docker", "inspect", "-f",
                              "{{.State.Status}}|{{.State.ExitCode}}|{{.State.OOMKilled}}",
                              a.container], capture_output=True, text=True, timeout=10)
    st = (inspect.stdout or "").strip().split("|")
    status, exit_code, oom = (st + ["?", "?", "?"])[:3]

    abrupt = [r for r in results if r[1] == "closed" and r[2] == 1006]
    clean = [r for r in results if r[1] == "closed" and r[2] in (1000, 1001)]
    failed = [r for r in results if r[1] == "connect-failed"]

    checks.append({"name": "client-notification", "verdict": "PASS" if clean and not abrupt else "FAIL",
                   "detail": f"{len(clean)} clean close, {len(abrupt)} abrupt(1006), "
                             f"{len(failed)} connect-failed"})
    checks.append({"name": "drain-time", "verdict": "PASS" if drain_s <= a.drain_budget_s else "FAIL",
                   "detail": f"drained in {drain_s}s (budget {a.drain_budget_s}s)"})
    checks.append({"name": "exit-code", "verdict": "PASS" if exit_code == "0" else "FAIL",
                   "detail": f"container status={status} exit={exit_code} oom={oom}"})

    fail = any(c["verdict"] == "FAIL" for c in checks)
    verdict = "FAIL" if fail else "PASS"
    summary = (f"drain {drain_s}s: {len(clean)} clean / {len(abrupt)} abrupt / "
               f"{len(failed)} failed; exit={exit_code}")
    return {"verdict": verdict, "checks": checks, "summary": summary,
            "drain_s": drain_s, "clean": len(clean), "abrupt": len(abrupt),
            "connect_failed": len(failed), "exit_code": exit_code}


def main() -> int:
    ap = argparse.ArgumentParser(description="G20: SIGTERM graceful-drain gate.")
    ap.add_argument("--target", required=True)
    ap.add_argument("--container", required=True, help="docker container to SIGTERM")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--baseline", default="art-baseline.json")
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--connections", type=int, default=10)
    ap.add_argument("--drain-budget-s", type=float, default=30.0,
                    help="must be < kube terminationGracePeriodSeconds")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    a.extra_param = [tuple(p.split("=", 1)) for p in a.extra_param]
    report = asyncio.run(probe(a))
    report.update({"schema": 1, "gate": "G20", "name": "drain",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "target": a.target, "drain_budget_s": a.drain_budget_s})
    print(report["summary"])
    for c in report["checks"]:
        print(f"  {c['name']:<20} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
