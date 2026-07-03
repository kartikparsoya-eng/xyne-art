#!/usr/bin/env python3
"""
mint_local_jwt.py — mint an HS256 JWT accepted by a LOCAL Xyne sandbox.

The sandbox backend's authenticateZero middleware verifies the
`google_access_token` cookie as an HS256 JWT (iss=xyne, aud=xyne-user) signed
with JWT_SECRET — and the sandbox sets ZERO_AUTH_SECRET to the same value, so
one token serves as both the WS authToken and the forwarded cookie.

LOCAL SANDBOX ONLY: prod signs with a secret we don't (and shouldn't) have.

    SECRET=$(docker inspect <backend-container> --format '{{json .Config.Env}}' \
             | python3 -c "import sys,json;print(next(e.split('=',1)[1] for e in json.load(sys.stdin) if e.startswith('JWT_SECRET=')))")
    python3 tools/mint_local_jwt.py --secret "$SECRET" \
        --sub cmr0y90br000713zuqz84ghc6 --email sandbox@xyne.ai --name Sandbox
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import time


def b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def mint(secret: str, sub: str, email: str, name: str, ttl_s: int,
         member_id: str | None = None, workspace_id: str | None = None) -> str:
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": sub, "email": email, "name": name,
        "iat": now, "exp": now + ttl_s,
        "iss": "xyne", "aud": "xyne-user",
    }
    # Newer builds' extractAuthDataFromJWT (src/zero/server.ts) also look up
    # org_members by memberId and carry workspaceId in the claims.
    if member_id:
        payload["memberId"] = member_id
    if workspace_id:
        payload["workspaceId"] = workspace_id
    signing = (b64url(json.dumps(header, separators=(",", ":")).encode()) + "." +
               b64url(json.dumps(payload, separators=(",", ":")).encode()))
    sig = hmac.new(secret.encode(), signing.encode(), hashlib.sha256).digest()
    return signing + "." + b64url(sig)


def main() -> int:
    ap = argparse.ArgumentParser(description="Mint a local-sandbox Xyne JWT (HS256).")
    ap.add_argument("--secret", required=True, help="the sandbox JWT_SECRET")
    ap.add_argument("--sub", required=True, help="user id (users.id in the sandbox DB)")
    ap.add_argument("--email", required=True)
    ap.add_argument("--name", default="ART Harness")
    ap.add_argument("--member-id", default=None,
                    help="org_members.memberId for the user (required by newer builds)")
    ap.add_argument("--workspace-id", default=None, help="workspaces.id")
    ap.add_argument("--ttl", type=int, default=86400, help="validity seconds (default 24h)")
    a = ap.parse_args()
    print(mint(a.secret, a.sub, a.email, a.name, a.ttl, a.member_id, a.workspace_id))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
