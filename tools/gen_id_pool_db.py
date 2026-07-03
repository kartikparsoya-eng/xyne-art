#!/usr/bin/env python3
"""
gen_id_pool_db.py — seed harness/id-pool.json with REAL entity IDs pulled
straight from a target environment's Postgres (local sandbox / staging).

Counterpart to gen_id_pool.py (which harvests from PROD telemetry): prod IDs
don't resolve against a sandbox DB, so for local runs we pull the IDs from the
same DB the candidate zero-cache serves.

    # local docker sandbox (default container/db match xyne-sandbox):
    python3 tools/gen_id_pool_db.py \
        --container xyne-sandbox-postgres --db sandbox_rust_test_db

    # or any reachable postgres:
    python3 tools/gen_id_pool_db.py --dsn postgresql://user:pw@host:5433/db

Requires either `docker exec <container> psql` or a local `psql` binary.
Stdlib only — talks to psql over subprocess with CSV output.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

# id-pool key -> (table, column [, WHERE clause]) in the app's public schema.
# Keys mirror the arg keys of the 151-query catalogue in art-baseline.json.
# Missing tables/columns are skipped gracefully (sandboxes vary).
MAPPING: dict[str, tuple] = {
    "channelId":        ("channels", "id"),
    "conversationId":   ("conversations", '"conversationId"'),
    "ticketId":         ("tickets", "id"),
    "mappedTicketId":   ("tickets", "id"),          # mapped refs are ticket ids
    "xyneId":           ("tickets", '"xyneId"'),
    "canvasId":         ("canvases", "id"),
    "boardId":          ("boards", "id"),
    "messageId":        ("messages", '"messageId"'),
    "initialMessageId": ("conversations", '"initialMessageId"'),
    "userId":           ("users", "id"),
    "projectId":        ("projects", "id"),
    "workspaceId":      ("workspaces", "id"),
    "orgId":            ("organizations", '"orgId"'),
    "folderId":         ("canvas_folders", "id"),
    "dashboardId":      ("dashboards", "id"),
    "formId":           ("forms", "id"),
    "userGroupId":      ("user_groups", "id"),
    "collectionId":     ("collections", "id"),
    "rootCollectionId": ("collections", "id", '"parentId" IS NULL'),
    "entityId":         ("tickets", "id"),
    "scopeId":          ("channels", "id"),
    "contextId":        ("conversations", '"conversationId"'),
    "releaseId":        ("release_events", "id"),
    "impactId":         ("impacts", "id"),
    "rcaId":            ("rcas", "id"),
    "id":               ("channels", "id"),
}

# Behavioural scalars the DB can't supply — same defaults the prod harvester
# writes, so replay.py's ArgResolver finds them in one place.
SCALARS: dict[str, list] = {
    # start/dir/direction: every pagination zod schema is a nullable object
    # cursor + forward/backward literals (see queries.ts) — null = first page.
    "limit": [25, 50], "start": [None], "direction": ["forward", "backward"],
    "isMember": [True], "isRead": [False, True], "showOverdueOnly": [False],
    "viewMode": ["kanban"], "columnType": ["stage"], "groupBy": ["stage"],
    "classification": [[]], "types": [[]], "lastUpdatedAt": [0],
    "updatedAt": [0], "recapDate": [0], "contextType": ["BOARD", "STAGE"],
    "entityType": ["TICKET"], "searchQuery": ["test", "status", "release"],
    "scope": ["channel"], "scopeType": ["channel"], "dir": ["forward", "backward"],
    "type": ["TICKET_TYPE"], "boardType": ["kanban"],
}


def psql_cmd(a) -> list[str]:
    if a.dsn:
        return ["psql", a.dsn]
    return ["docker", "exec", a.container, "psql", "-U", a.user, "-d", a.db]


def fetch(a, table: str, column: str, where: str | None, per_key: int) -> list[str]:
    w = f"WHERE {column} IS NOT NULL" + (f" AND {where}" if where else "")
    sql = (f"SELECT DISTINCT {column} FROM public.{table} {w} "
           f"ORDER BY {column} DESC LIMIT {per_key}")
    return run_sql(a, sql)


def fetch_ranked_channels(a, where: str | None, per_key: int) -> list[str]:
    """channelIds ordered by participant count DESC — index 0 = hottest channel.
    Enables rank-based Zipf sampling in the driver (replay.py --zipf-s)."""
    w = 'WHERE "channelId" IS NOT NULL' + (f" AND {where}" if where else "")
    sql = (f'SELECT "channelId" FROM public.channel_user_status {w} '
           f'GROUP BY "channelId" ORDER BY count(*) DESC LIMIT {per_key}')
    return run_sql(a, sql)


def run_sql(a, sql: str) -> list[str]:
    try:
        out = subprocess.run(psql_cmd(a) + ["-Atc", sql], capture_output=True,
                             text=True, timeout=30)
    except Exception as e:
        print(f"  ERROR running psql: {e}", file=sys.stderr)
        return []
    if out.returncode != 0:
        return []           # table/column doesn't exist in this env — skip
    return [line for line in out.stdout.splitlines() if line.strip()]


def main() -> int:
    ap = argparse.ArgumentParser(description="Harvest an id-pool from a target Postgres.")
    ap.add_argument("--container", default="xyne-sandbox-postgres",
                    help="docker container running postgres (ignored with --dsn)")
    ap.add_argument("--user", default="xyne")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--dsn", default=None, help="postgresql:// DSN (uses local psql instead of docker)")
    ap.add_argument("--user-id", default=None,
                    help="scope channelId/conversationId to this user's memberships "
                         "(needed for mutations: mutators enforce participation). "
                         "Comma-separate multiple ids to INTERSECT memberships "
                         "(multi-user runs need channels shared by all identities)")
    ap.add_argument("--per-key", type=int, default=300)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..",
                                                  "harness", "id-pool.json"))
    a = ap.parse_args()

    mapping = dict(MAPPING)
    if a.user_id:
        uids = [u.strip().replace("'", "") for u in a.user_id.split(",") if u.strip()]
        # channels every listed user participates in
        chan_where = (f'"channelId" IN (SELECT "channelId" FROM public.channel_user_status '
                      f"WHERE \"userId\" IN ({', '.join(chr(39)+u+chr(39) for u in uids)}) "
                      f'GROUP BY "channelId" HAVING count(DISTINCT "userId") = {len(uids)})')
        mapping["channelId"] = ("channel_user_status", '"channelId"', chan_where)
        mapping["conversationId"] = ("conversations", '"conversationId"', chan_where)
        mapping["contextId"] = mapping["conversationId"]
        mapping["scopeId"] = mapping["channelId"]

    ids: dict[str, list] = {}
    chan_where_opt = chan_where if a.user_id else None
    for key, spec in mapping.items():
        table, column = spec[0], spec[1]
        where = spec[2] if len(spec) > 2 else None
        if key in ("channelId", "scopeId"):
            # hotness-ranked (participant count DESC) for Zipf sampling
            vals = fetch_ranked_channels(a, chan_where_opt, a.per_key)
            table, column = "channel_user_status", '"channelId" (ranked)'
        else:
            vals = fetch(a, table, column, where, a.per_key)
        if vals:
            ids[key] = vals
            print(f"  {key:20} <- {table}.{column:24} {len(vals)} ids")
        else:
            print(f"  {key:20} <- {table}.{column:24} (skipped: empty/missing)")

    pool = {
        "_meta": {
            "source": a.dsn or f"docker:{a.container}/{a.db}",
            "note": "DB-harvested id-pool (gen_id_pool_db.py) — env-specific, do not commit",
        },
        "ids": ids,
        "scalars": SCALARS,
    }
    out = os.path.abspath(a.out)
    with open(out, "w") as f:
        json.dump(pool, f, indent=1)
    total = sum(len(v) for v in ids.values())
    print(f"\nwrote {out}: {total} ids across {len(ids)} keys")
    return 0 if total else 1


if __name__ == "__main__":
    raise SystemExit(main())
