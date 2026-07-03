#!/usr/bin/env python3
"""probe_remote.py — one tiny read-only WS probe against a remote zero-cache.

Connects ONE client, sends initConnection (zero desired queries), prints the
first server frames (or close reason), disconnects. Leaves behind a single
empty art-% client group (TTL-purged like any abandoned tab).

    .venv/bin/python tools/probe_remote.py --target wss://host/zero \
        --auth-token "$JWT" --user-id <uid> [--client-schema harness/client-schema.json]
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
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from replay import encode_sec_protocols  # noqa: E402
from workload import init_connection_message  # noqa: E402


def rid() -> str:
    r = random.SystemRandom()
    return "art-" + "".join(r.choice("abcdefghijklmnop0123456789") for _ in range(10))


async def probe(a: argparse.Namespace) -> int:
    import websockets

    cgid, cid = rid(), rid()
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0",
              "wsid": uuid.uuid4().hex[:12]}
    if a.user_id:
        params["userID"] = a.user_id
    url = (a.target.rstrip("/") + f"/sync/v{a.protocol_version}/connect?"
           + urllib.parse.urlencode(params))

    client_schema = None
    if a.client_schema:
        with open(a.client_schema) as f:
            client_schema = json.load(f)
    init = init_connection_message([], client_schema=client_schema)
    sec = encode_sec_protocols(None, a.auth_token)  # post-handshake init

    print(f"target : {a.target}")
    print(f"cgid   : {cgid}  cid: {cid}")
    print(f"token  : {a.auth_token[:16]}...(redacted)")
    t0 = time.perf_counter()
    try:
        async with websockets.connect(
                url, subprotocols=[sec], open_timeout=20,
                max_size=None, ping_interval=None) as ws:
            print(f"WS OPEN in {(time.perf_counter() - t0) * 1000:.0f}ms")
            await ws.send(json.dumps(init))
            deadline = time.perf_counter() + a.listen_s
            n = 0
            while time.perf_counter() < deadline and n < a.max_frames:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=max(0.1, deadline - time.perf_counter()))
                except asyncio.TimeoutError:
                    break
                n += 1
                try:
                    msg = json.loads(raw)
                    tag = msg[0] if isinstance(msg, list) else "?"
                    body = json.dumps(msg[1])[:300] if (
                        isinstance(msg, list) and len(msg) > 1) else ""
                    print(f"  frame {n}: [{tag}] {body}")
                except Exception:
                    print(f"  frame {n}: (raw) {raw[:200]!r}")
            if n == 0:
                print("  (no frames within listen window)")
            print(f"close  : clean (we closed) after {n} frame(s)")
            return 0
    except websockets.exceptions.InvalidStatus as e:
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", "?")
        print(f"HTTP REJECT: status={code}")
        body = getattr(resp, "body", b"")
        if body:
            print(f"  body: {body[:300]!r}")
        return 1
    except websockets.exceptions.ConnectionClosed as e:
        print(f"WS CLOSED by server: code={e.code} reason={e.reason!r}")
        return 1
    except Exception as e:
        print(f"CONNECT ERROR: {type(e).__name__}: {e}")
        return 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--auth-token", required=True)
    ap.add_argument("--user-id", default=None)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--protocol-version", type=int, default=49)
    ap.add_argument("--listen-s", type=float, default=10.0)
    ap.add_argument("--max-frames", type=int, default=8)
    return asyncio.run(probe(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
