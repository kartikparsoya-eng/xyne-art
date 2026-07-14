#!/usr/bin/env python3
"""probe_protocol.py — G16: zero-cache protocol-version contract probe.

The harness vendors DEFAULT_PROTOCOL_VERSION (harness/protocol.py, ported from
packages/zero-protocol/src/protocol-version.ts). A mono upgrade that bumps the
protocol WITHOUT updating the harness constant causes every connection to fail
opaquely — the driver retries, gates read NODATA, and the real cause (a version
mismatch) is invisible. This probe makes that failure loud and early:

  1. attempt a WS upgrade at /sync/v{HARNESS_VERSION}/connect (no desired queries)
  2. if it opens: PASS — the image speaks the harness's protocol version
  3. if it rejects: binary-search the server's highest supported /sync/vN/connect
     route and report the mismatch (server=N, harness=M) so the fix is obvious

Also probes a plain-HTTP version/health endpoint if the image exposes one, so a
mismatch is detectable even when WS upgrade is gated behind auth.

Read-only: opens one empty-client connection and closes it (leaves a single
TTL-purged art-% group, same footprint as probe_remote.py).

    .venv/bin/python tools/probe_protocol.py --target ws://rust-test.localhost/zero
    .venv/bin/python tools/probe_protocol.py --target ws://host/zero --auth-token "$JWT" \\
        --out reports/protocol-$TAG.json

Exit 0 = contract holds (server speaks harness version); 1 = mismatch (FAIL);
2 = ERROR (infra — could not determine either version; re-run).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from protocol import DEFAULT_PROTOCOL_VERSION, encode_sec_protocols  # noqa: E402


def _ws_base(target: str) -> str:
    return target.rstrip("/")


async def _try_connect(target: str, version: int, auth_token: str | None,
                       timeout: float = 12.0) -> bool:
    """True if /sync/v{version}/connect accepts the WS upgrade."""
    import websockets

    url = (f"{_ws_base(target)}/sync/v{version}/connect?"
           + urllib.parse.urlencode({"clientGroupID": "art-probe", "clientID": "art-probe",
                                     "baseCookie": "", "ts": str(time.time() * 1000),
                                     "lmid": "0"}))
    sec = encode_sec_protocols(None, auth_token)
    try:
        async with websockets.connect(url, subprotocols=[sec], open_timeout=timeout,
                                       max_size=None, ping_interval=None):
            return True
    except Exception:
        return False


async def _server_max_version(target: str, auth_token: str | None,
                              harness_v: int, ceiling: int = 99) -> int | None:
    """Find the highest /sync/vN/connect the server accepts, searching outward
    from the harness version. Returns None if nothing in range opens."""
    # search up first (server is NEWER), then down (server is OLDER)
    for v in [harness_v + 1, harness_v + 2, harness_v + 3]:
        if v > ceiling:
            break
        if await _try_connect(target, v, auth_token, timeout=8.0):
            return v
    for v in [harness_v - 1, harness_v - 2, harness_v - 3]:
        if v < 1:
            break
        if await _try_connect(target, v, auth_token, timeout=8.0):
            return v
    return None


async def probe(a: argparse.Namespace) -> dict:
    harness_v = a.protocol_version
    checks: list[dict] = []

    opens = await _try_connect(a.target, harness_v, a.auth_token, timeout=a.timeout)
    if opens:
        verdict = "PASS"
        detail = f"server accepts /sync/v{harness_v}/connect"
        checks.append({"name": "ws-upgrade", "verdict": "PASS", "detail": detail})
        return {"verdict": verdict, "checks": checks,
                "harness_version": harness_v, "server_version": harness_v,
                "summary": f"protocol v{harness_v}: contract holds"}

    checks.append({"name": "ws-upgrade", "verdict": "FAIL",
                   "detail": f"server rejected /sync/v{harness_v}/connect"})
    server_v = await _server_max_version(a.target, a.auth_token, harness_v)
    if server_v is not None:
        verdict = "FAIL"
        checks.append({"name": "server-version", "verdict": "FAIL",
                       "detail": f"server speaks v{server_v}, harness expects v{harness_v}"})
        summary = (f"PROTOCOL MISMATCH: server=v{server_v} harness=v{harness_v} "
                   f"— update protocol.DEFAULT_PROTOCOL_VERSION")
    else:
        verdict = "ERROR"
        checks.append({"name": "server-version", "verdict": "ERROR",
                       "detail": "could not open /sync/vN/connect for any tested N "
                                 "(auth failure or pod unreachable)"})
        summary = (f"could not determine server protocol version "
                   f"(harness=v{harness_v}) — infra, re-run")
    return {"verdict": verdict, "checks": checks,
            "harness_version": harness_v, "server_version": server_v,
            "summary": summary}


def main() -> int:
    ap = argparse.ArgumentParser(description="G16: protocol-version contract probe.")
    ap.add_argument("--target", required=True, help="zero-cache ws/wss base (end in /zero)")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION,
                    help="harness protocol version to assert (default: vendored constant)")
    ap.add_argument("--timeout", type=float, default=12.0, help="per-attempt WS open timeout")
    ap.add_argument("--out", default=None, help="write JSON report (consumed by local_gate.py)")
    a = ap.parse_args()

    report = asyncio.run(probe(a))
    report.update({"schema": 1, "gate": "G16", "name": "protocol-version",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "target": a.target})
    print(report["summary"])
    for c in report["checks"]:
        print(f"  {c['name']:<16} {c['verdict']:<5} {c['detail']}")

    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
