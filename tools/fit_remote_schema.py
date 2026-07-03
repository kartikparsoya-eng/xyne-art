#!/usr/bin/env python3
"""fit_remote_schema.py — converge a clientSchema to a REMOTE zero-cache.

Our client-schema.json is harvested from the local sandbox CVR; a remote
deployment (pre-prod) runs a different app build, so tables/columns drift and
initConnection fails with SchemaVersionNotSupported. Conveniently the server's
error names the offending table AND its full replicated column list — so we
iterate: connect, parse the error, drop the non-replicated columns, retry.
Read-only; each attempt leaves one empty art-% client group (TTL-purged).

    .venv/bin/python tools/fit_remote_schema.py \
        --target wss://spaces.sandbox.xyne.juspay.net/zero \
        --auth-token "$JWT" --user-id <sub> \
        --base harness/client-schema.json \
        --out harness/client-schema.remote.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
import urllib.parse
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from replay import encode_sec_protocols  # noqa: E402
from workload import init_connection_message  # noqa: E402

COL_ERR = re.compile(
    r'The "([^"]+)"\."([^"]+)" column does not exist or is not one of the '
    r'replicated columns: (.*)$')
TBL_ERR = re.compile(r'The "([^"]+)" table does not exist')


def rid() -> str:
    r = random.SystemRandom()
    return "art-" + "".join(r.choice("abcdefghijklmnop0123456789") for _ in range(10))


async def attempt(a, schema: dict) -> tuple[str, str]:
    """One connect+init. Returns (status, detail): ok | schema_err | other."""
    import websockets
    params = {"clientGroupID": rid(), "clientID": rid(), "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0",
              "wsid": uuid.uuid4().hex[:12], "userID": a.user_id}
    url = (a.target.rstrip("/") + f"/sync/v{a.protocol_version}/connect?"
           + urllib.parse.urlencode(params))
    init = init_connection_message([], client_schema=schema)
    sec = encode_sec_protocols(None, a.auth_token)
    got_poke = False
    try:
        async with websockets.connect(url, subprotocols=[sec], open_timeout=20,
                                      max_size=None, ping_interval=None) as ws:
            await ws.send(json.dumps(init))
            deadline = time.perf_counter() + a.listen_s
            while time.perf_counter() < deadline:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=max(0.1, deadline - time.perf_counter()))
                except (asyncio.TimeoutError, Exception):
                    break
                msg = json.loads(raw)
                tag = msg[0] if isinstance(msg, list) else "?"
                if tag == "error":
                    body = msg[1] if len(msg) > 1 else {}
                    kind = body.get("kind", "?")
                    if kind in ("SchemaVersionNotSupported",):
                        return "schema_err", body.get("message", "")
                    return "other", f"{kind}: {body.get('message', '')[:120]}"
                if tag == "pokeEnd":
                    # NB: schema validation is async — the error frame arrives
                    # AFTER the initial config poke. Keep listening to the end
                    # of the window; pokeEnd alone is not acceptance.
                    got_poke = True
    except Exception as e:
        return "other", f"connect failure: {type(e).__name__}: {e}"
    return ("ok", "poke completed, no error for full window") if got_poke \
        else ("other", "no verdict within listen window")


async def fit(a) -> int:
    with open(a.base) as f:
        schema = json.load(f)
    tables = schema["tables"]
    changes: list[str] = []
    for i in range(a.max_iters):
        status, detail = await attempt(a, schema)
        if status == "ok":
            print(f"iter {i}: ACCEPTED — {detail}")
            with open(a.out, "w") as f:
                json.dump(schema, f, separators=(",", ":"))
            print(f"\nfitted schema -> {a.out}")
            for c in changes:
                print(f"  {c}")
            if not changes:
                print("  (no changes needed — schemas already match)")
            return 0
        if status == "schema_err":
            m = COL_ERR.search(detail)
            if m:
                tbl, col, tail = m.group(1), m.group(2), m.group(3)
                valid = set(re.findall(r'"([^"]+)"', tail))
                if tbl not in tables:
                    print(f"iter {i}: server names table {tbl!r} we don't declare — bail")
                    return 2
                before = set(tables[tbl]["columns"])
                drop = before - valid
                for c in drop:
                    del tables[tbl]["columns"][c]
                changes.append(f"{tbl}: dropped {sorted(drop)} (remote lacks them)")
                print(f"iter {i}: {tbl} — dropped {sorted(drop)}")
                continue
            m = TBL_ERR.search(detail)
            if m and m.group(1) in tables:
                del tables[m.group(1)]
                changes.append(f"dropped table {m.group(1)} (not replicated remotely)")
                print(f"iter {i}: dropped table {m.group(1)}")
                continue
            print(f"iter {i}: unparseable schema error — {detail[:300]}")
            return 2
        print(f"iter {i}: {status} — {detail}")
        return 2
    print(f"gave up after {a.max_iters} iterations")
    return 2


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--auth-token", required=True)
    ap.add_argument("--user-id", required=True)
    ap.add_argument("--base", default=os.path.join(
        os.path.dirname(__file__), "..", "harness", "client-schema.json"))
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(__file__), "..", "harness", "client-schema.remote.json"))
    ap.add_argument("--protocol-version", type=int, default=49)
    ap.add_argument("--listen-s", type=float, default=6.0)
    ap.add_argument("--max-iters", type=int, default=40)
    return asyncio.run(fit(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
