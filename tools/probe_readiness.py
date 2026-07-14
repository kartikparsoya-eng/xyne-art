#!/usr/bin/env python3
"""probe_readiness.py — G19: readiness/liveness probe-contract test.

A Kubernetes rolling deploy trusts the image's readiness endpoint: the new pod
only receives traffic when /readyz flips true. A probe that flips ready too
early (before the query endpoint is live) or that stays ready while the
syncer is wedged is a silent regression the behavioral gates cannot see —
clients hit a "ready" pod that 503s or hangs. This gate asserts the
documented contract:

  1. liveness (/healthz) returns 200 once the process is up
  2. readiness (/readyz) returns 200 only AFTER the sync endpoint accepts
     connections (not before) — the readiness contract
  3. readiness STAYS 200 while a small load passes through (no flapping)

Probes plain HTTP (no WS). Paths are configurable; zero-cache defaults:
  /healthz  (liveness — process up)
  /readyz   (readiness — serving)

    .venv/bin/python tools/probe_readiness.py --http http://rust-test.localhost:8080 \\
        --ws-target ws://rust-test.localhost/zero --auth-token "$JWT" \\
        --out reports/readiness-$TAG.json

Exit 0 = contract holds; 1 = violated; 2 = ERROR (infra — endpoints absent).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import urllib.parse
import urllib.request


def http_status(url: str, timeout: float = 5.0) -> int | None:
    """Return HTTP status code, or None on connection failure."""
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return None


async def ws_accepts(url_base: str, version: int, auth_token: str | None,
                     timeout: float = 8.0) -> bool:
    import websockets
    params = {"clientGroupID": "art-ready", "clientID": "art-ready",
              "baseCookie": "", "ts": str(time.time() * 1000), "lmid": "0"}
    url = (url_base.rstrip("/") + f"/sync/v{version}/connect?"
           + urllib.parse.urlencode(params))
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
    from protocol import encode_sec_protocols  # noqa: E402
    sec = encode_sec_protocols(None, auth_token)
    try:
        async with websockets.connect(url, subprotocols=[sec], open_timeout=timeout,
                                       max_size=None, ping_interval=None):
            return True
    except Exception:
        return False


async def probe(a: argparse.Namespace) -> dict:
    checks: list[dict] = []

    # 1. liveness
    hz = http_status(a.http.rstrip("/") + a.healthz_path)
    if hz is None:
        checks.append({"name": "liveness", "verdict": "ERROR",
                       "detail": f"{a.healthz_path} unreachable — endpoint absent?"})
        return {"verdict": "ERROR", "checks": checks, "summary": "liveness endpoint absent"}
    liveness_ok = 200 <= hz < 300
    checks.append({"name": "liveness", "verdict": "PASS" if liveness_ok else "FAIL",
                   "detail": f"{a.healthz_path} -> {hz}"})

    # 2. readiness
    rz = http_status(a.http.rstrip("/") + a.readyz_path)
    if rz is None:
        checks.append({"name": "readiness", "verdict": "ERROR",
                       "detail": f"{a.readyz_path} unreachable — endpoint absent?"})
        return {"verdict": "ERROR", "checks": checks, "summary": "readiness endpoint absent"}
    ready_ok = 200 <= rz < 300
    checks.append({"name": "readiness", "verdict": "PASS" if ready_ok else "FAIL",
                   "detail": f"{a.readyz_path} -> {rz}"})

    # 3. readiness contract: ready IMPLIES ws accepts (no false-ready)
    if ready_ok and a.ws_target:
        ws_ok = await ws_accepts(a.ws_target, a.protocol_version, a.auth_token)
        if ws_ok:
            checks.append({"name": "ready-implies-serving", "verdict": "PASS",
                           "detail": "readyz=200 and WS upgrade accepted (contract holds)"})
        else:
            checks.append({"name": "ready-implies-serving", "verdict": "FAIL",
                           "detail": "readyz=200 but WS upgrade REJECTED — FALSE READY (clients would 503)"})

    # 4. stability: readiness stays 200 over a short poll window (no flapping)
    flips = 0
    last = rz
    t_end = time.perf_counter() + a.stability_s
    samples = 0
    while time.perf_counter() < t_end:
        await asyncio.sleep(a.poll_interval)
        cur = http_status(a.http.rstrip("/") + a.readyz_path)
        samples += 1
        if cur != last:
            flips += 1
            last = cur
    if flips == 0 and 200 <= (last or 0) < 300:
        checks.append({"name": "stability", "verdict": "PASS",
                       "detail": f"readyz held 200 over {a.stability_s}s ({samples} samples)"})
    else:
        checks.append({"name": "stability", "verdict": "FAIL",
                       "detail": f"readyz flipped {flips}x over {a.stability_s}s — flapping"})

    fail = any(c["verdict"] == "FAIL" for c in checks)
    error = any(c["verdict"] == "ERROR" for c in checks)
    verdict = "FAIL" if fail else ("ERROR" if error else "PASS")
    summary = f"liveness={hz} readiness={rz} flips={flips} over {a.stability_s}s"
    return {"verdict": verdict, "checks": checks, "summary": summary,
            "liveness": hz, "readiness": rz, "flips": flips}


def main() -> int:
    ap = argparse.ArgumentParser(description="G19: readiness/liveness probe contract.")
    ap.add_argument("--http", required=True, help="base HTTP URL of zero-cache (http://host:port)")
    ap.add_argument("--ws-target", default=None, help="ws base to verify ready-implies-serving")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--healthz-path", default="/healthz")
    ap.add_argument("--readyz-path", default="/readyz")
    ap.add_argument("--protocol-version", type=int, default=49)
    ap.add_argument("--stability-s", type=float, default=10.0, help="window to poll readiness for flapping")
    ap.add_argument("--poll-interval", type=float, default=1.0)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    report = asyncio.run(probe(a))
    report.update({"schema": 1, "gate": "G19", "name": "readiness-contract",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "http": a.http})
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
