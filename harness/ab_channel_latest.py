#!/usr/bin/env python3
"""
A/B benchmark for channelLatestMultipleConversationsV3 `after` filter.

Measures cold-hydrate latency: time from sending the desired-query put
to receiving the first poke that delivers rows (the hydration completion).

Variant A (before): args = {channelId, isMember, limit}  — no `after`
Variant B (after):  args = {channelId, isMember, limit, after}  — after = now - cutoff

Each trial uses a fresh clientGroupID (cold cache) and alternates A/B.
Reports p50/p95/p99 for each variant and the delta.
"""
from __future__ import annotations

import asyncio
import json
import os
import statistics
import sys
import time
import urllib.parse
import uuid

# Vendored protocol bits from the ART harness
sys.path.insert(0, os.path.dirname(__file__))
from protocol import encode_sec_protocols, DEFAULT_PROTOCOL_VERSION  # noqa: E402

import websockets

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TARGET = os.environ.get("AB_TARGET", "ws://rust-test.localhost/zero-ts")
CHANNEL_ID = os.environ.get("AB_CHANNEL", "artbench-heavy-chan")
USER_ID = os.environ.get("AB_USER", "cms5vksku005e11z9mqhq1y2u")
AUTH_TOKEN = os.environ.get("AB_TOKEN", "")
CLIENT_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "client-schema.json")
AUTH_POOL_PATH = os.path.join(os.path.dirname(__file__), "auth-pool.json")
QUERY_NAME = "channelLatestMultipleConversationsV3"
LIMIT = 25
TRIALS = int(os.environ.get("AB_TRIALS", "30"))           # per variant
CUTOFF_MIN = int(os.environ.get("AB_CUTOFF_MIN", "30"))   # after = now - 30 min
WARMUP = int(os.environ.get("AB_WARMUP", "3"))            # warmup trials excluded
QUIESCE_S = float(os.environ.get("AB_QUIESCE", "1.0"))    # gap between trials


def load_auth():
    if AUTH_TOKEN:
        return AUTH_TOKEN, USER_ID
    if os.path.exists(AUTH_POOL_PATH):
        pool = json.load(open(AUTH_POOL_PATH))
        entry = pool[0]
        return entry["token"], entry["userID"]
    raise RuntimeError("set AB_TOKEN or run with auth-pool.json present")


def load_client_schema():
    with open(CLIENT_SCHEMA_PATH) as f:
        return json.load(f)


def build_connect_url(cgid: str, cid: str) -> str:
    params = {
        "clientGroupID": cgid,
        "clientID": cid,
        "baseCookie": "",
        "ts": str(time.time() * 1000),
        "lmid": "0",
        "wsid": uuid.uuid4().hex[:12],
        "userID": USER_ID,
    }
    base = TARGET.rstrip("/") + f"/sync/v{DEFAULT_PROTOCOL_VERSION}/connect"
    return base + "?" + urllib.parse.urlencode(params)


def init_connection_message(desired_puts, client_schema=None):
    """Build ['initConnection', {...}] per zero-protocol/src/connect.ts."""
    body = {"desiredQueriesPatch": desired_puts}
    if client_schema:
        body["clientSchema"] = client_schema
    return ["initConnection", body]


def build_put(query_name: str, args: dict, ttl_ms: int = 600000):
    """Build a named custom-query put message."""
    import hashlib
    raw = json.dumps([query_name, args], sort_keys=True, separators=(",", ":"))
    h = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return {"op": "put", "hash": h, "name": query_name, "args": [args], "ttl": ttl_ms}


async def measure_one(args: dict, tag: str) -> float:
    """
    Open a fresh WS connection, send initConnection with one desired query,
    measure time from send to first poke with row data (hydration complete).
    Returns latency in ms, or -1 on error.
    """
    cgid = f"ab-{tag}-{uuid.uuid4().hex[:8]}"
    cid = f"ab-{tag}-c-{uuid.uuid4().hex[:8]}"

    token, uid = load_auth()
    cs = load_client_schema()
    init_msg = init_connection_message(
        [build_put(QUERY_NAME, args)],
        client_schema=cs,
    )
    # post_handshake: initConnection sent as a WS message after connect,
    # so the Sec-WebSocket-Protocol header carries only the auth token.
    sec_protocol = encode_sec_protocols(None, token)

    url = build_connect_url(cgid, cid)
    try:
        async with websockets.connect(
            url,
            subprotocols=[sec_protocol],
            open_timeout=15,
            close_timeout=5,
            max_size=64 * 1024 * 1024,
            compression=None,
        ) as ws:
            # Send initConnection as a post-handshake message
            await ws.send(json.dumps(init_msg))
            t_send = time.monotonic()
            # Wait for the pokePart that carries rowsPatch (actual hydration data)
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    return -1.0

                t_recv = time.monotonic()
                latency_ms = (t_recv - t_send) * 1000

                if isinstance(msg, bytes):
                    return latency_ms

                try:
                    data = json.loads(msg)
                except (json.JSONDecodeError, TypeError):
                    return latency_ms

                # Wire format: ["type", {body}]
                if isinstance(data, list) and len(data) >= 2:
                    msg_type = data[0] if isinstance(data[0], str) else None
                    body = data[1] if isinstance(data[1], dict) else {}
                    if msg_type == "pokePart" and "rowsPatch" in body:
                        return latency_ms
    except Exception as e:
        print(f"  [{tag}] ERROR: {type(e).__name__}: {str(e)[:80]}", file=sys.stderr)
        return -1.0

    return -1.0


async def run_variant(label: str, args: dict, trials: int) -> list[float]:
    """Run N trials for one variant, return list of latencies (ms)."""
    latencies = []
    for i in range(trials):
        lat = await measure_one(args, f"{label}-{i}")
        if lat > 0:
            latencies.append(lat)
            print(f"  {label} trial {i+1:3d}/{trials}: {lat:8.1f} ms", file=sys.stderr)
        else:
            print(f"  {label} trial {i+1:3d}/{trials}: FAILED", file=sys.stderr)
        await asyncio.sleep(QUIESCE_S)
    return latencies


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = int(len(s) * p / 100)
    k = min(k, len(s) - 1)
    return s[k]


def report(label: str, data: list[float]):
    if not data:
        print(f"  {label}: NO VALID SAMPLES")
        return
    p50 = percentile(data, 50)
    p95 = percentile(data, 95)
    p99 = percentile(data, 99)
    mean = statistics.mean(data)
    print(f"  {label:10s}: n={len(data):3d}  p50={p50:8.1f}ms  p95={p95:8.1f}ms  "
          f"p99={p99:8.1f}ms  mean={mean:8.1f}ms")


async def main():
    # Resolve auth
    token, uid = load_auth()
    if not AUTH_TOKEN:
        os.environ["AB_TOKEN"] = token
        os.environ["AB_USER"] = uid
        global USER_ID
        USER_ID = uid

    # Compute `after` cutoff: now - CUTOFF_MIN minutes (in epoch ms)
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - (CUTOFF_MIN * 60 * 1000)

    args_before = {"channelId": CHANNEL_ID, "isMember": True, "limit": LIMIT}
    args_after = {"channelId": CHANNEL_ID, "isMember": True, "limit": LIMIT, "after": cutoff_ms}

    print(f"A/B Benchmark: {QUERY_NAME}")
    print(f"  target: {TARGET}")
    print(f"  channel: {CHANNEL_ID} (2000 conversations)")
    print(f"  limit: {LIMIT}")
    print(f"  after cutoff: now - {CUTOFF_MIN}min (epoch ms {cutoff_ms})")
    print(f"  trials per variant: {TRIALS} ({WARMUP} warmup excluded)")
    print(f"  user: {USER_ID}")
    print()

    # Warmup (excluded from results)
    print("== warmup ==", file=sys.stderr)
    await run_variant("warmup-A", args_before, WARMUP)
    await run_variant("warmup-B", args_after, WARMUP)

    # A/B/A/B alternating to control for temporal drift
    print("== measured trials ==", file=sys.stderr)
    a_all = []
    b_all = []
    half = (TRIALS - WARMUP) // 2 if TRIALS > WARMUP else 1
    for i in range(half):
        # A (before)
        lat_a = await measure_one(args_before, f"A-{i}")
        if lat_a > 0:
            a_all.append(lat_a)
            print(f"  A trial {len(a_all):3d}: {lat_a:8.1f} ms", file=sys.stderr)
        await asyncio.sleep(QUIESCE_S)

        # B (after)
        lat_b = await measure_one(args_after, f"B-{i}")
        if lat_b > 0:
            b_all.append(lat_b)
            print(f"  B trial {len(b_all):3d}: {lat_b:8.1f} ms", file=sys.stderr)
        await asyncio.sleep(QUIESCE_S)

    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    report("A (before)", a_all)
    report("B (after)",  b_all)
    print()

    if a_all and b_all:
        a_p50 = percentile(a_all, 50)
        b_p50 = percentile(b_all, 50)
        a_p95 = percentile(a_all, 95)
        b_p95 = percentile(b_all, 95)
        delta_p50 = a_p50 - b_p50
        delta_p95 = a_p95 - b_p95
        speedup_p50 = a_p50 / b_p50 if b_p50 > 0 else 0
        speedup_p95 = a_p95 / b_p95 if b_p95 > 0 else 0
        print(f"  Δ p50: {delta_p50:+.1f} ms  ({speedup_p50:.2f}x {'faster' if delta_p50 > 0 else 'slower'})")
        print(f"  Δ p95: {delta_p95:+.1f} ms  ({speedup_p95:.2f}x {'faster' if delta_p95 > 0 else 'slower'})")
    print()

    # JSON output for machine consumption
    result = {
        "query": QUERY_NAME,
        "channel": CHANNEL_ID,
        "limit": LIMIT,
        "after_cutoff_min": CUTOFF_MIN,
        "target": TARGET,
        "trials": len(a_all) + len(b_all),
        "before": {"n": len(a_all), "p50": percentile(a_all, 50),
                   "p95": percentile(a_all, 95), "p99": percentile(a_all, 99),
                   "mean": statistics.mean(a_all) if a_all else 0},
        "after": {"n": len(b_all), "p50": percentile(b_all, 50),
                  "p95": percentile(b_all, 95), "p99": percentile(b_all, 99),
                  "mean": statistics.mean(b_all) if b_all else 0},
    }
    if a_all and b_all:
        result["delta_p50_ms"] = percentile(a_all, 50) - percentile(b_all, 50)
        result["delta_p95_ms"] = percentile(a_all, 95) - percentile(b_all, 95)
        result["speedup_p50"] = percentile(a_all, 50) / percentile(b_all, 50) if percentile(b_all, 50) > 0 else 0
        result["speedup_p95"] = percentile(a_all, 95) / percentile(b_all, 95) if percentile(b_all, 95) > 0 else 0

    out_path = os.path.join(os.path.dirname(__file__), "..", "reports",
                            f"ab-channel-latest-{int(time.time())}.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  report: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
