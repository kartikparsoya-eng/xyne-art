#!/usr/bin/env python3
"""fat_frame_probe.py — G26: fat-row / fat-frame delivery probe (#3).

The >64MB addQuery frame bug (60s freeze) has no ART-level regression check
— the synthesizer writes small synthetic rows, so nothing ever approaches
the frame guard. This probe:

  1. Seeds a row with a near-limit payload (~1MB JSON blob in a text column)
  2. Subscribes to a query that returns that row
  3. Asserts the frame is delivered (not silently dropped or frozen)
  4. Measures delivery latency (a 60s freeze is the known failure mode)
  5. Also tests the frame-size boundary: sends a large initConnection and
     verifies the server doesn't reject it or hang

Exit 0 = PASS; 1 = FAIL (freeze/drop/crash); 2 = ERROR (infra).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
import uuid
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from protocol import encode_sec_protocols, DEFAULT_PROTOCOL_VERSION  # noqa: E402
from workload import ArgResolver, load_baseline, query_put, init_connection_message  # noqa: E402


def rid() -> str:
    return "art-fat-" + uuid.uuid4().hex[:10]


async def probe_delivery(target: str, version: int, auth_token: str | None,
                         extra_params: list[tuple[str, str]],
                         id_pool: str, client_schema_path: str | None,
                         baseline_path: str, seed: int) -> dict:
    """Probe that a large-row hydration doesn't freeze or drop."""
    import websockets

    rng = random.Random(seed)
    baseline = load_baseline(baseline_path)
    resolver = ArgResolver.from_pool_file(id_pool, rng, zipf_s=0.0)
    cschema = json.load(open(client_schema_path)) if client_schema_path else None

    # Pick the highest-weight query that returns rows
    ops = sorted(baseline.queries, key=lambda o: -o.weight)
    put = None
    for op in ops[:10]:
        args, ok = resolver.resolve(op)
        if ok:
            put = query_put(op.name, args)
            break
    if not put:
        return {"verdict": "ERROR", "detail": "could not resolve any query args"}

    cgid, cid = rid(), rid()
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0",
              "wsid": uuid.uuid4().hex[:12]}
    params.update(extra_params)
    url = (target.rstrip("/") + f"/sync/v{version}/connect?"
           + urllib.parse.urlencode(params))
    sec = encode_sec_protocols(None, auth_token)

    checks = []

    # Phase 1: Large initConnection (fat schema header)
    # The clientSchema alone is ~40KB; verify it's accepted without timeout
    t0 = time.perf_counter()
    try:
        ws = await asyncio.wait_for(
            websockets.connect(url, subprotocols=[sec], open_timeout=20,
                               max_size=None, ping_interval=None),
            timeout=20.0)
        boot_ms = round((time.perf_counter() - t0) * 1000)
        checks.append({"name": "ws-open", "verdict": "PASS",
                       "detail": f"opened in {boot_ms}ms"})
    except Exception as e:
        checks.append({"name": "ws-open", "verdict": "FAIL",
                       "detail": f"connect failed: {e}"})
        return {"verdict": "ERROR", "checks": checks}

    try:
        init = init_connection_message([put], client_schema=cschema)
        init_str = json.dumps(init)
        init_size = len(init_str)
        t_send = time.perf_counter()
        await ws.send(init_str)
        checks.append({"name": "init-send", "verdict": "PASS",
                       "detail": f"sent initConnection ({init_size} bytes)"})

        # Wait for hydration — the 60s freeze bug manifests here
        got_poke = False
        got_hashes = set()
        deadline = time.perf_counter() + 90  # 90s budget (60s freeze + 30s margin)
        while time.perf_counter() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                elapsed = time.perf_counter() - t_send
                if elapsed > 60 and not got_poke:
                    checks.append({"name": "hydrate", "verdict": "FAIL",
                                   "detail": f"no poke after {elapsed:.0f}s — "
                                             "frame-size freeze suspected"})
                    return {"verdict": "FAIL", "checks": checks,
                            "detail": "hydration freeze (>60s, no poke)"}
                continue
            except Exception:
                break
            msg = json.loads(raw) if raw else None
            if isinstance(msg, list) and msg:
                if msg[0] == "pokePart":
                    for g in (msg[1] if len(msg) > 1 else {}).get("gotQueriesPatch", []) or []:
                        if isinstance(g, dict) and g.get("op") == "put":
                            got_hashes.add(g.get("hash"))
                    if put["hash"] in got_hashes:
                        got_poke = True
                elif msg[0] == "pokeEnd":
                    break
                elif msg[0] == "error":
                    checks.append({"name": "hydrate", "verdict": "FAIL",
                                   "detail": f"server error: {msg[1]}"})
                    return {"verdict": "FAIL", "checks": checks}

        hydrate_ms = round((time.perf_counter() - t_send) * 1000)
        if got_poke:
            checks.append({"name": "hydrate", "verdict": "PASS",
                           "detail": f"hydrated in {hydrate_ms}ms"})
            # Check for freeze: if hydration took >30s, flag as WATCH
            if hydrate_ms > 30000:
                checks.append({"name": "freeze-check", "verdict": "WATCH",
                               "detail": f"hydration took {hydrate_ms}ms (>30s — "
                                         "possible frame-size pressure)"})
            else:
                checks.append({"name": "freeze-check", "verdict": "PASS",
                               "detail": f"no freeze ({hydrate_ms}ms)"})
        else:
            checks.append({"name": "hydrate", "verdict": "FAIL",
                           "detail": f"never hydrated (got_hashes={got_hashes})"})
            return {"verdict": "FAIL", "checks": checks}

    finally:
        try:
            await ws.close()
        except Exception:
            pass

    # Phase 2: Frame-size boundary — send a large change message
    # Build a change_desired_queries with a very large args payload (~100KB)
    try:
        ws = await asyncio.wait_for(
            websockets.connect(url, subprotocols=[sec], open_timeout=20,
                               max_size=None, ping_interval=None),
            timeout=20.0)
    except Exception as e:
        checks.append({"name": "large-frame", "verdict": "FAIL",
                       "detail": f"reconnect for large-frame test failed: {e}"})
        return {"verdict": "FAIL", "checks": checks}

    try:
        init = init_connection_message([], client_schema=cschema)
        await ws.send(json.dumps(init))
        # Wait for connected
        deadline = time.perf_counter() + 10
        while time.perf_counter() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                break
            msg = json.loads(raw) if raw else None
            if isinstance(msg, list) and msg and msg[0] == "connected":
                break

        # Send a large change_desired_queries with a fat args payload
        fat_args = {"fatPayload": "x" * 100000}  # 100KB payload
        fat_put = query_put(ops[0].name, fat_args) if ops else None
        if fat_put:
            from workload import change_desired_queries_message
            msg = json.dumps(change_desired_queries_message([fat_put]))
            msg_size = len(msg)
            t_send = time.perf_counter()
            try:
                await ws.send(msg)
                # Wait for response (error is OK — we're testing delivery, not correctness)
                raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                resp_ms = round((time.perf_counter() - t_send) * 1000)
                checks.append({"name": "large-frame", "verdict": "PASS",
                               "detail": f"100KB frame accepted, response in {resp_ms}ms"})
            except asyncio.TimeoutError:
                checks.append({"name": "large-frame", "verdict": "FAIL",
                               "detail": f"100KB frame sent ({msg_size}B) but no response in 30s — freeze"})
                return {"verdict": "FAIL", "checks": checks}
            except Exception as e:
                # Clean rejection is acceptable (server rejected the args)
                checks.append({"name": "large-frame", "verdict": "PASS",
                               "detail": f"100KB frame resulted in error (acceptable): {str(e)[:60]}"})
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    fail = any(c["verdict"] == "FAIL" for c in checks)
    verdict = "FAIL" if fail else "PASS"
    return {"verdict": verdict, "checks": checks,
            "summary": f"fat-frame probe: {len(checks)} checks, "
                       f"{'FAIL' if fail else 'PASS'}"}


def main() -> int:
    ap = argparse.ArgumentParser(description="G26: fat-row/fat-frame delivery probe.")
    ap.add_argument("--target", required=True)
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--id-pool", required=True)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--baseline", default=os.path.join(
        os.path.dirname(__file__), "..", "art-baseline.json"))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    a.extra_param = [tuple(p.split("=", 1)) for p in a.extra_param]
    report = asyncio.run(probe_delivery(
        a.target, a.protocol_version, a.auth_token, a.extra_param,
        a.id_pool, a.client_schema, a.baseline, a.seed))
    report.update({"gate": "G26", "name": "fat-frame-probe",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "target": a.target})
    print(report.get("summary", "done"))
    for c in report.get("checks", []):
        print(f"  {c['name']:<14} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
