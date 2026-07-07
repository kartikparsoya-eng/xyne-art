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
    # viewMode: ticketsQueryV2 z.enum(['project','board','my-tickets',
    # 'user-tickets','group-tickets']); boardType: z.nativeEnum(BoardType)
    # {DEFAULT RELEASE NON_LINEAR}. The old "kanban" values predate both
    # enums and were masked while the running backend 404'd the queries.
    "viewMode": ["project", "board", "my-tickets"],
    "columnType": ["stage"], "groupBy": ["stage"],
    "classification": [[]], "types": [[]], "lastUpdatedAt": [0],
    "updatedAt": [0], "recapDate": [0], "contextType": ["BOARD", "STAGE"],
    "entityType": ["TICKET"], "searchQuery": ["test", "status", "release"],
    "scope": ["channel"], "scopeType": ["channel"], "dir": ["forward", "backward"],
    "type": ["TICKET_TYPE"], "boardType": ["DEFAULT", "RELEASE"],
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


# Signal-ranked pools for keys where a uniform draw mostly hits rows with no
# dependent data (G9 arg-luck blind spots: getResourceAccessForUser /
# getUserProfilesByIds returned 0 rows because a random user of 142 usually
# has no resource_access/user_profiles row; ticketsByProject drew the one
# project with 0 tickets). Ranking puts high-signal ids at index 0 so Zipf
# runs favour them, and hydration coverage stops depending on draw luck.
RANKED_SQL: dict[str, str] = {
    "userId": (
        'SELECT u.id FROM public.users u '
        'LEFT JOIN public.channel_user_status cus ON cus."userId" = u.id '
        'LEFT JOIN public.resource_access ra ON ra."userId" = u.id '
        'LEFT JOIN public.user_profiles up ON up."userId" = u.id '
        'GROUP BY u.id '
        'ORDER BY (count(DISTINCT ra.id) + count(DISTINCT up.id)) DESC, '
        'count(cus.id) DESC LIMIT {per_key}'
    ),
    "projectId": (
        'SELECT p.id FROM public.projects p '
        'LEFT JOIN public.tickets t ON t."projectId" = p.id AND t."isArchived" = false '
        'GROUP BY p.id ORDER BY count(t.id) DESC LIMIT {per_key}'
    ),
}


def fetch_ranked(a, key: str, per_key: int) -> list[str]:
    return run_sql(a, RANKED_SQL[key].format(per_key=per_key))


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


def derive_scalars_from_source(a, path: str) -> tuple[dict, list]:
    """Enum scalar values DERIVED from the deployed backend's own Zod schemas
    (raw/arg-schemas.source.json, tools/gen_arg_schemas.sh) — adopted from
    staging-regression's Zod-driven fixture synthesis. Hand-maintained enum
    defaults rot silently: viewMode:["kanban"] predated the V2 enum and kept
    poisoning ticketsQueryV2/stagesByBoards through THREE backend upgrades
    because nothing tied the value back to source. Derivation makes that
    class impossible: enum drift now fixes itself at pool refresh.

    Only enum-like kinds are derived (z.enum, z.nativeEnum, unions of string
    literals). Booleans/numbers stay hand-curated — their values encode
    intent (isMember=True picks rows the identity can actually see), not
    just validity. nativeEnums missing from the extractor's runtime
    resolution fall back to pg_enum (authoritative for the target DB).
    Keys whose enum definitions DISAGREE across queries are left alone
    (a union would poison half the catalog with the other half's values)."""
    try:
        with open(path) as f:
            doc = json.load(f)
    except Exception as e:
        print(f"  arg-schemas unreadable ({e}) — hand scalars only", file=sys.stderr)
        return {}, []
    enums = doc.get("enums") or {}
    pg_enum_cache: dict[str, list] = {}

    def enum_values(s) -> tuple | None:
        """Returns (shape, values) — shape is 'scalar' or 'array'. The shape
        MUST travel with the values: classification is z.array(z.nativeEnum(..))
        in queries; flattening it to a bare string would replace one stale-value
        bug with a wrong-shape bug (string where the zod schema wants string[])."""
        t = s.get("type")
        if t == "array":
            inner = enum_values(s.get("element") or {})
            return ("array", inner[1]) if inner else None
        if t == "enum":
            vals = s.get("values") or None
            return ("scalar", vals) if vals else None
        if t == "nativeEnum":
            name = s.get("enum")
            if not name:
                return None
            if name in enums:
                return ("scalar", enums[name])
            if name not in pg_enum_cache:
                pg_enum_cache[name] = run_sql(
                    a, "SELECT enumlabel FROM pg_enum e JOIN pg_type t "
                       "ON t.oid = e.enumtypid WHERE t.typname = "
                       f"'{name.replace(chr(39), '')}' ORDER BY e.enumsortorder")
            vals = pg_enum_cache[name] or None
            return ("scalar", vals) if vals else None
        if t == "union":
            lits = [v.get("value") for v in s.get("variants", [])
                    if v.get("type") == "literal" and isinstance(v.get("value"), str)]
            return ("scalar", lits) if lits else None
        return None

    per_key: dict[str, set] = {}
    for section in ("queries", "mutators"):
        for entry in (doc.get(section) or {}).values():
            for key, s in (entry.get("args") or {}).items():
                sv = enum_values(s)
                if sv:
                    per_key.setdefault(key, set()).add((sv[0], tuple(sv[1])))
    derived, conflicts = {}, []
    for key, variants in per_key.items():
        if len(variants) == 1:
            shape, vals = next(iter(variants))
            if shape == "array":
                # each pool entry is a complete arg VALUE: exercise the empty
                # filter and a one-element filter
                derived[key] = [[], [vals[0]]]
            else:
                derived[key] = list(vals)
        else:
            conflicts.append((key, len(variants)))
    return derived, conflicts


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
    ap.add_argument("--arg-schemas", default=os.path.join(
        os.path.dirname(__file__), "..", "raw", "arg-schemas.source.json"),
        help="source-extracted Zod arg schemas (tools/gen_arg_schemas.sh); "
             "enum scalars are derived from it when present")
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

    # scalars: hand defaults, overlaid by source-derived enum values (see
    # derive_scalars_from_source — the anti-stale-scalar mechanism)
    scalars = {k: list(v) for k, v in SCALARS.items()}
    if a.arg_schemas and os.path.exists(a.arg_schemas):
        derived, conflicts = derive_scalars_from_source(a, a.arg_schemas)
        drift_fixed = [k for k in derived
                       if k in scalars and not set(map(str, scalars[k]))
                       <= set(map(str, derived[k]))]
        scalars.update(derived)
        print(f"  scalars: {len(derived)} enum keys derived from source"
              + (f" — drift-fixed: {', '.join(drift_fixed)}" if drift_fixed else ""))
        for key, n in conflicts:
            print(f"  scalar CONFLICT: '{key}' has {n} distinct enum sets "
                  f"across queries — kept hand value")
    else:
        print("  scalars: hand defaults only (no raw/arg-schemas.source.json — "
              "run tools/gen_arg_schemas.sh)")

    for key, spec in mapping.items():
        table, column = spec[0], spec[1]
        where = spec[2] if len(spec) > 2 else None
        if key in ("channelId", "scopeId"):
            # hotness-ranked (participant count DESC) for Zipf sampling
            vals = fetch_ranked_channels(a, chan_where_opt, a.per_key)
            table, column = "channel_user_status", '"channelId" (ranked)'
        elif key in RANKED_SQL:
            # signal-ranked (see RANKED_SQL) — falls back to the plain fetch
            # if the ranking query fails (e.g. table missing in this env)
            vals = fetch_ranked(a, key, a.per_key) or fetch(a, table, column, where, a.per_key)
            column = f"{column} (ranked)"
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
        "scalars": scalars,
    }
    out = os.path.abspath(a.out)
    with open(out, "w") as f:
        json.dump(pool, f, indent=1)
    total = sum(len(v) for v in ids.values())
    print(f"\nwrote {out}: {total} ids across {len(ids)} keys")
    return 0 if total else 1


if __name__ == "__main__":
    raise SystemExit(main())
