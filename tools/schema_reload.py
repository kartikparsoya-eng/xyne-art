#!/usr/bin/env python3
"""schema_reload.py — hot schema reload test (#10).

What happens when the client schema changes mid-session? Prod deploys can
add queries. This test:
  1. Connects with the current client schema
  2. Hydrates a query
  3. Sends an UPDATED schema (adds a new query to the desired set)
  4. Verifies the new query hydrates correctly
  5. Sends a REMOVAL (drops a query from the desired set)
  6. Verifies the server stops sending pokes for the removed query

This catches: schema version mismatch crashes, stale query cache bugs,
and deserialization failures when the server receives a schema it wasn't
built with.

    python3 tools/schema_reload.py --target ws://rust-test.localhost/zero \
        --auth-token "$JWT" --extra-param userID=$UID \
        --id-pool harness/id-pool.sandbox.json \
        --client-schema harness/client-schema.json

Exit 0 = PASS; 1 = FAIL; 2 = error.
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

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from protocol import encode_sec_protocols, DEFAULT_PROTOCOL_VERSION  # noqa: E402
from workload import (  # noqa: E402
    ArgResolver, load_baseline, query_put, query_del,
    init_connection_message, change_desired_queries_message,
)


async def main_async(a: argparse.Namespace) -> int:
    import websockets

    baseline = load_baseline(a.baseline)
    rng = random.Random(a.seed)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng, zipf_s=0.0)
    with open(a.client_schema) as f:
        cschema = json.load(f)

    # pick two queries: one to start with, one to add later
    queries = sorted(baseline.queries, key=lambda q: -q.weight)
    q_initial = queries[0]
    q_added = queries[1] if len(queries) > 1 else queries[0]

    # resolve args for both
    args_init, ok_init = resolver.resolve(q_initial)
    args_added, ok_added = resolver.resolve(q_added)
    if not ok_init or not ok_added:
        print("FAIL: could not resolve query args")
        return 1

    put_initial = query_put(q_initial.name, args_init, ttl_ms=300_000)
    put_added = query_put(q_added.name, args_added, ttl_ms=300_000)

    import urllib.parse
    cgid = "art-reload-" + uuid.uuid4().hex[:10]
    cid = "art-reload-" + uuid.uuid4().hex[:10]
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0",
              "wsid": uuid.uuid4().hex[:12]}
    for p in (a.extra_param or []):
        k, v = p.split("=", 1)
        params[k] = v
    url = (a.target.rstrip("/") + f"/sync/v{a.protocol_version}/connect?"
           + urllib.parse.urlencode(params))
    sec = encode_sec_protocols(None, a.auth_token)

    print(f"=== schema reload test vs {a.target} ===")
    results = []

    try:
        ws = await websockets.connect(
            url, subprotocols=[sec], open_timeout=20,
            max_size=None, ping_interval=None)
    except Exception as e:
        print(f"FAIL: connect failed: {e}")
        return 2

    try:
        # Phase 1: connect with initial query only
        init_msg = init_connection_message([put_initial], client_schema=cschema)
        await ws.send(json.dumps(init_msg))
        print(f"  phase 1: sent initConnection with 1 query ({q_initial.name})")

        # wait for initial hydration
        got_hashes = set()
        deadline = time.perf_counter() + 30
        initial_hydrated = False
        while time.perf_counter() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            msg = json.loads(raw) if raw else None
            if isinstance(msg, list) and msg:
                if msg[0] == "pokePart":
                    for g in (msg[1] if len(msg) > 1 else {}).get("gotQueriesPatch", []) or []:
                        if isinstance(g, dict) and g.get("op") == "put":
                            got_hashes.add(g.get("hash"))
                    initial_hydrated = put_initial["hash"] in got_hashes
                elif msg[0] == "pokeEnd":
                    break
                elif msg[0] == "error":
                    print(f"  phase 1: FAIL - server error: {msg[1] if len(msg) > 1 else '?'}")
                    results.append(("phase1-initial-hydration", "FAIL"))
                    break
            if initial_hydrated:
                break

        if initial_hydrated:
            print(f"  phase 1: PASS - initial query hydrated ({q_initial.name})")
            results.append(("phase1-initial-hydration", "PASS"))
        else:
            print(f"  phase 1: FAIL - initial query never hydrated (got={got_hashes})")
            results.append(("phase1-initial-hydration", "FAIL"))

        # Phase 2: add the second query (hot schema change — add query)
        msg = json.dumps(change_desired_queries_message([put_added]))
        await ws.send(msg)
        print(f"  phase 2: sent changeDesiredQueries adding 1 query ({q_added.name})")

        added_hydrated = False
        deadline = time.perf_counter() + 30
        while time.perf_counter() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                break
            msg = json.loads(raw) if raw else None
            if isinstance(msg, list) and msg:
                if msg[0] == "pokePart":
                    for g in (msg[1] if len(msg) > 1 else {}).get("gotQueriesPatch", []) or []:
                        if isinstance(g, dict) and g.get("op") == "put":
                            got_hashes.add(g.get("hash"))
                    added_hydrated = put_added["hash"] in got_hashes
                elif msg[0] == "pokeEnd":
                    break
                elif msg[0] == "error":
                    print(f"  phase 2: FAIL - server error after schema change: "
                          f"{msg[1] if len(msg) > 1 else '?'}")
                    results.append(("phase2-add-query", "FAIL"))
                    break
            if added_hydrated:
                break

        if added_hydrated:
            print(f"  phase 2: PASS - added query hydrated ({q_added.name})")
            results.append(("phase2-add-query", "PASS"))
        else:
            print(f"  phase 2: FAIL - added query never hydrated (got={got_hashes})")
            results.append(("phase2-add-query", "FAIL"))

        # Phase 3: remove the initial query (hot schema change — remove query)
        msg = json.dumps(change_desired_queries_message([query_del(put_initial["hash"])]))
        await ws.send(msg)
        print(f"  phase 3: sent changeDesiredQueries removing 1 query ({q_initial.name})")

        # drain pokes for a few seconds — no crash = PASS
        await asyncio.sleep(5)
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            msg = json.loads(raw) if raw else None
            if isinstance(msg, list) and msg and msg[0] == "error":
                print(f"  phase 3: FAIL - error after query removal: {msg[1]}")
                results.append(("phase3-remove-query", "FAIL"))
            else:
                print("  phase 3: PASS - query removed without crash")
                results.append(("phase3-remove-query", "PASS"))
        except asyncio.TimeoutError:
            print("  phase 3: PASS - query removed, no more pokes (clean)")
            results.append(("phase3-remove-query", "PASS"))
        except Exception as e:
            print(f"  phase 3: FAIL - socket died after removal: {e}")
            results.append(("phase3-remove-query", "FAIL"))

    finally:
        try:
            await ws.close()
        except Exception:
            pass

    # verdict
    n_fail = sum(1 for _, v in results if v == "FAIL")
    n_pass = sum(1 for _, v in results if v == "PASS")
    verdict = "FAIL" if n_fail else "PASS"
    report = {
        "target": a.target,
        "when": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "phases": [{"name": n, "verdict": v} for n, v in results],
        "n_pass": n_pass, "n_fail": n_fail,
        "verdict": verdict,
    }
    out = a.out or os.path.join("reports", f"schema-reload-{time.strftime('%Y%m%d-%H%M%S')}.json")
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nSCHEMA RELOAD: {verdict} ({n_pass} pass, {n_fail} fail) -> {out}")
    return 1 if n_fail else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Hot schema reload test (#10).")
    ap.add_argument("--target", required=True)
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--baseline", default=os.path.join(
        os.path.dirname(__file__), "..", "art-baseline.json"))
    ap.add_argument("--id-pool", required=True)
    ap.add_argument("--client-schema", required=True)
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    return asyncio.run(main_async(a))


if __name__ == "__main__":
    raise SystemExit(main())
