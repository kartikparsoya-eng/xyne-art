#!/usr/bin/env python3
"""
build_auth_pool.py — mint a full auth-pool (harness/auth-pool.json) covering
every bulk-seeded sandbox user, so trace replay maps prod users onto REAL
distinct identities with distinct channel memberships.

Why: the first trace A/B collapsed 278 prod users onto 2 sandbox identities —
every session saw the same visibility set, which understates CVR diversity and
overstates per-CG fan-in. With 100 identities the mapped load spreads the way
prod load does.

The JWT secret is read from the backend container's env INSIDE this process
(docker inspect) — it is never echoed, never written to any file. Only the
minted tokens land in auth-pool.json, which is gitignored.

    .venv/bin/python tools/build_auth_pool.py            # defaults fit sandbox
    .venv/bin/python tools/build_auth_pool.py --ttl 172800

Output shape matches what trace_replay.py / replay.py consume:
    [{"token": "...", "userID": "...", "email": "...", "name": "..."}, ...]

Identities are ordered: extra emails first (the original admin-ish sandbox
user keeps index 0 = hottest prod user), then bulk users by email.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mint_local_jwt import mint  # noqa: E402


def backend_env(container: str, key: str) -> str | None:
    """Pull one env var out of a running container without echoing it."""
    try:
        out = subprocess.run(
            ["docker", "inspect", container, "--format", "{{json .Config.Env}}"],
            capture_output=True, text=True, timeout=15)
    except Exception as e:
        print(f"ERROR: docker inspect failed: {e}", file=sys.stderr)
        return None
    if out.returncode != 0:
        print(f"ERROR: docker inspect: {out.stderr.strip()[:200]}", file=sys.stderr)
        return None
    for e in json.loads(out.stdout):
        if e.startswith(key + "="):
            return e.split("=", 1)[1]
    return None


def psql(a, sql: str) -> list[list[str]]:
    if a.dsn:
        cmd = ["psql", a.dsn]
    else:
        cmd = ["docker", "exec", a.pg_container, "psql", "-U", a.pg_user,
               "-d", a.db]
    out = subprocess.run(cmd + ["-Atc", sql], capture_output=True, text=True,
                         timeout=60)
    if out.returncode != 0:
        print(f"ERROR: psql: {out.stderr.strip()[:300]}", file=sys.stderr)
        return []
    return [ln.split("|") for ln in out.stdout.splitlines() if ln.strip()]


# One row per user; workspace chosen by where the user actually has channel
# memberships (DISTINCT ON + membership-count ranking) so hydration sees the
# bulk-seeded data, not an empty sibling workspace. org_members joins by
# EMAIL — its userId column is literally the string 'deprecated'. The final
# memb column ranks duplicate users rows sharing one email: only one of the
# three sandbox@xyne.ai users.id rows has channel_user_status rows (1,977) —
# picking another yields a zero-visibility identity.
USERS_SQL = """
SELECT DISTINCT ON (u.id) u.id, u.email, u.name, om."memberId", w.id,
    (SELECT count(*) FROM public.channel_user_status cus2
     WHERE cus2."userId" = u.id) AS memb
FROM public.users u
JOIN public.org_members om ON om.email = u.email
JOIN public.workspaces w ON w."orgId" = om."orgId"
WHERE {where}
ORDER BY u.id, (
    SELECT count(*) FROM public.channels c
    JOIN public.channel_user_status cus ON cus."channelId" = c.id
    WHERE c."workspaceId" = w.id AND cus."userId" = u.id) DESC
"""


def main() -> int:
    ap = argparse.ArgumentParser(description="Mint auth-pool.json for sandbox users.")
    ap.add_argument("--backend-container", default="xyne-sandbox-rust-test-backend")
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--pg-user", default="xyne")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--dsn", default=None,
                    help="postgresql:// DSN (local psql instead of docker exec)")
    ap.add_argument("--email-like", default="bulk-user-%",
                    help="SQL LIKE for the identity sweep (default bulk users)")
    ap.add_argument("--extra-emails", default="sandbox@xyne.ai",
                    help="comma-separated exact emails prepended to the pool "
                         "(index 0 = hottest prod user in trace replay)")
    ap.add_argument("--ttl", type=int, default=86400,
                    help="token validity seconds (default 24h)")
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(__file__), "..", "harness", "auth-pool.json"))
    a = ap.parse_args()

    secret = backend_env(a.backend_container, "JWT_SECRET") \
        or backend_env(a.backend_container, "ZERO_AUTH_SECRET")
    if not secret:
        print("ERROR: no JWT_SECRET/ZERO_AUTH_SECRET in backend container env",
              file=sys.stderr)
        return 1

    like = a.email_like.replace("'", "")
    rows: list[list[str]] = []
    for em in [e.strip().replace("'", "") for e in a.extra_emails.split(",") if e.strip()]:
        got = psql(a, USERS_SQL.format(where=f"u.email = '{em}'"))
        got.sort(key=lambda r: -int(r[5] or 0))      # most-visible users.id first
        rows += got
    bulk = psql(a, USERS_SQL.format(where=f"u.email LIKE '{like}'"))
    bulk.sort(key=lambda r: (r[1], -int(r[5] or 0)))  # bulk-user-000..NNN
    rows += bulk

    pool, seen = [], set()
    for r in rows:
        # dedupe by email too: duplicate users rows sharing an email resolve
        # to the SAME org_members row (email join) — keep the users.id that
        # actually holds the channel memberships (memb rank above)
        if len(r) != 6 or r[0] in seen or r[1] in seen:
            continue
        uid, email, name, member_id, ws_id, _memb = r
        seen.update((uid, email))
        pool.append({
            "token": mint(secret, uid, email, name or "ART User", a.ttl,
                          member_id or None, ws_id or None),
            "userID": uid, "email": email, "name": name,
        })

    if not pool:
        print("ERROR: no users matched — check --email-like / DB", file=sys.stderr)
        return 1
    out = os.path.abspath(a.out)
    with open(out, "w") as f:
        json.dump(pool, f, indent=1)
    print(f"wrote {out}: {len(pool)} identities "
          f"({len(rows) - len(pool)} dupes dropped), ttl={a.ttl}s")
    print("NOTE: tokens expire — re-run before long soaks; file is gitignored.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
