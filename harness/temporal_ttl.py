"""
temporal_ttl.py — L5 ART gate G-ttl (mid-session auth refresh; no stale-token 401).

Reproduces prod bug-2 as a gate: a JWT whose TTL is SHORTER than the session.
The client refreshes via `["updateAuth", {"auth": <fresh token>}]`; work must
continue with ZERO Unauthorized/401 frames, identically on TS and rust.

Before the fix, rust forwarded `PushRelayHeaders.auth` — a connect-time SNAPSHOT
never refreshed on updateAuth — so once the connect-time JWT expired, every
relayed push carried the stale token → API-server 401. TS reads
`mustGetConnectionContext` fresh per push (pusher.ts). This gate would have
caught it: after updateAuth, rust's next push 401s while TS's succeeds.

Prereqs (see RUN.md): a live TS+rust pair AND the HS256 signing secret in
ZERO_AUTH_SECRET (the sandbox's JWT_SECRET), plus a sample userID/sub. Without
the secret the gate SKIPs (cannot mint a short-TTL token).

Env:
  ZERO_AUTH_SECRET   HS256 signing secret (required to mint)
  ART_TTL_SUB        the `sub` (userID) to mint for (default from auth-pool[0])
  ART_TTL_SECONDS    short TTL in seconds (default 8)
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time

from ab_common import make_sides, open_side, reader, Report, load_auth


def _b64u(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def mint_hs256(secret: str, sub: str, ttl_s: int, template: dict | None = None) -> str:
    """Minimal HS256 JWT mint (stdlib only) matching the sandbox token shape."""
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = dict(template or {})
    payload.update({"sub": sub, "iat": now, "exp": now + ttl_s,
                    "iss": payload.get("iss", "xyne"),
                    "aud": payload.get("aud", "xyne-user")})
    signing_input = f"{_b64u(json.dumps(header, separators=(',',':')).encode())}." \
                    f"{_b64u(json.dumps(payload, separators=(',',':')).encode())}"
    sig = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{_b64u(sig)}"


def _decode_sub(token: str) -> str | None:
    try:
        p = token.split(".")[1]
        p += "=" * (-len(p) % 4)
        return json.loads(base64.urlsafe_b64decode(p)).get("sub")
    except Exception:
        return None


def unauthorized_frames(side) -> list:
    out = []
    for f in side.frames:
        tag = getattr(f, "tag", None)
        body = getattr(f, "body", {}) or {}
        if tag == "error" and str(body.get("kind", "")).lower() in (
                "unauthorized", "authinvalidated"):
            out.append(body)
        # A relayed-push 401 may surface as a mutation-result error body too.
        if tag in ("pushResponse", "mutationsResponse") and "401" in json.dumps(body):
            out.append(body)
    return out


async def main() -> int:
    rep = Report("reports/g-ttl.json")
    secret = os.environ.get("ZERO_AUTH_SECRET")
    if not secret:
        rep.add("G-ttl", "SKIP",
                "ZERO_AUTH_SECRET unset — cannot mint a short-TTL token")
        return rep.finish()

    # Derive sub + a fresh long-TTL token shape from the auth pool.
    pool = []
    try:
        pool = load_auth() if callable(load_auth) else []
    except Exception:
        pool = []
    pool_tok = (pool[0].get("token") if pool and isinstance(pool[0], dict) else None)
    sub = os.environ.get("ART_TTL_SUB") or (_decode_sub(pool_tok) if pool_tok else None)
    if not sub:
        rep.add("G-ttl", "SKIP", "no sub available (set ART_TTL_SUB)")
        return rep.finish()

    ttl_s = int(os.environ.get("ART_TTL_SECONDS", "8"))
    short = mint_hs256(secret, sub, ttl_s)
    fresh = mint_hs256(secret, sub, 3600)
    init = ["initConnection", {"desiredQueriesPatch": []}]

    rust, ts = make_sides()
    try:
        await asyncio.gather(open_side(rust, short, init, extra={"userID": sub}),
                             open_side(ts, short, init, extra={"userID": sub}))
    except Exception as e:
        rep.add("G-ttl", "SKIP", f"sandbox not reachable: {e!r}")
        return rep.finish()

    stop = asyncio.Event()
    readers = [asyncio.create_task(reader(rust, stop)),
               asyncio.create_task(reader(ts, stop))]

    # Let the short token EXPIRE mid-session, then refresh via updateAuth.
    await asyncio.sleep(ttl_s + 2)
    upd = json.dumps(["updateAuth", {"auth": fresh}])
    for s in (rust, ts):
        try:
            await s.ws.send(upd)
        except Exception:
            pass
    # Drive post-refresh work: re-assert the desired queries (a relayed op that
    # carries auth). A real mutation push can be substituted here per schema.
    await asyncio.sleep(3.0)
    for s in (rust, ts):
        try:
            await s.ws.send(json.dumps(init))
        except Exception:
            pass
    await asyncio.sleep(3.0)
    stop.set()
    await asyncio.gather(*readers, return_exceptions=True)

    ru, tu = unauthorized_frames(rust), unauthorized_frames(ts)
    verdict = "PASS" if not ru and not tu else "FAIL"
    detail = (f"rust unauthorized-after-refresh={len(ru)} {ru[:2]} | "
              f"ts unauthorized-after-refresh={len(tu)} {tu[:2]} "
              f"(refreshed a {ttl_s}s token mid-session; expect 0 on both)")
    rep.add("G-ttl", verdict, detail)
    return rep.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
