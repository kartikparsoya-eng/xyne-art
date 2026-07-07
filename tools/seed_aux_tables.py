#!/usr/bin/env python3
"""
seed_aux_tables.py — seed the sandbox tables that bulk-seed.ts leaves empty
but prod traces reference heavily: user_groups (+ user_group_mappings) and
canvases (+ canvas_participants).

Why: trace replay left 937 userGroupId and 196 canvasId occurrences unmapped
(pass-through -> guaranteed 0-row hydration) because these tables had 0 rows,
so gen_id_pool_db.py had nothing to harvest. Per the design decision "data
doesn't matter — query shapes and interaction topology matter", synthetic
rows here restore the mapping targets.

Idempotent: every id is prefixed 'artseed-' and inserted ON CONFLICT DO
NOTHING. --wipe deletes exactly the artseed-% rows. Writes flow down the
replication stream to BOTH zero-caches — same advancement path G12 uses.

    .venv/bin/python tools/seed_aux_tables.py            # seed
    .venv/bin/python tools/seed_aux_tables.py --wipe     # remove
    # then refresh the pool:
    .venv/bin/python tools/gen_id_pool_db.py --out harness/id-pool.sandbox.json ...
"""
from __future__ import annotations

import argparse
import subprocess
import sys

WS = "cmr1unwn2002s6p43dmi2ygla"          # bulk-seeded workspace (2,077 channels)


def psql(a, sql: str) -> str:
    if a.dsn:
        cmd = ["psql", a.dsn]
    else:
        cmd = ["docker", "exec", a.pg_container, "psql", "-U", a.pg_user, "-d", a.db]
    out = subprocess.run(cmd + ["-Atc", sql], capture_output=True, text=True, timeout=120)
    if out.returncode != 0:
        print(f"ERROR: {out.stderr.strip()[:400]}", file=sys.stderr)
        raise SystemExit(1)
    return out.stdout.strip()


SEED_SQL = f"""
BEGIN;
-- {{n_groups}} user groups in the bulk workspace
INSERT INTO public.user_groups (id, "workspaceId", name, alias, description, "updatedAt")
SELECT 'artseed-ug-' || lpad(i::text, 3, '0'), '{WS}',
       'ART Group ' || i, 'art-group-' || i,
       'synthetic group for ART trace replay', now()
FROM generate_series(0, {{n_groups}} - 1) i
ON CONFLICT (id) DO NOTHING;

-- map every bulk user into ~3 groups (deterministic spread by row_number)
INSERT INTO public.user_group_mappings (id, "userId", "userGroupId", "createdAt", "updatedAt")
SELECT 'artseed-ugm-' || u.rn || '-' || g,
       u.id, 'artseed-ug-' || lpad(((u.rn + g) % {{n_groups}})::text, 3, '0'),
       now(), now()
FROM (SELECT id, row_number() OVER (ORDER BY email) AS rn
      FROM public.users WHERE email LIKE 'bulk-user-%') u,
     generate_series(0, 2) g
ON CONFLICT (id) DO NOTHING;

-- {{n_canvases}} canvases: PUBLIC, owned by bulk users round-robin, attached
-- to the hottest channels (rank via channel_user_status participant count)
INSERT INTO public.canvases (id, title, content, "channelId", "createdBy",
                             visibility, "updatedAt")
SELECT 'artseed-cv-' || lpad(i::text, 3, '0'),
       'ART Canvas ' || i,
       jsonb_build_object('type', 'doc', 'content', jsonb_build_array(
           jsonb_build_object('type', 'paragraph', 'content', jsonb_build_array(
               jsonb_build_object('type', 'text', 'text', 'synthetic ART canvas ' || i))))),
       ch."channelId", u.id, 'PUBLIC', now()
FROM generate_series(0, {{n_canvases}} - 1) i
JOIN LATERAL (SELECT id FROM public.users WHERE email LIKE 'bulk-user-%'
              ORDER BY email OFFSET (i % 100) LIMIT 1) u ON true
JOIN LATERAL (SELECT "channelId" FROM public.channel_user_status
              GROUP BY "channelId" ORDER BY count(*) DESC
              OFFSET (i % 50) LIMIT 1) ch ON true
ON CONFLICT (id) DO NOTHING;

-- owner + a viewer per canvas
INSERT INTO public.canvas_participants (id, "canvasId", "userId", role, "joinedAt", "updatedAt")
SELECT 'artseed-cvp-' || c.id || '-o', c.id, c."createdBy", 'OWNER', now(), now()
FROM public.canvases c WHERE c.id LIKE 'artseed-cv-%'
ON CONFLICT (id) DO NOTHING;
COMMIT;
"""

WIPE_SQL = """
BEGIN;
DELETE FROM public.canvas_participants WHERE id LIKE 'artseed-%';
DELETE FROM public.canvases WHERE id LIKE 'artseed-%';
DELETE FROM public.user_group_mappings WHERE id LIKE 'artseed-%';
DELETE FROM public.user_groups WHERE id LIKE 'artseed-%';
COMMIT;
"""

COUNT_SQL = """
SELECT 'user_groups: ' || count(*) FROM public.user_groups WHERE id LIKE 'artseed-%'
UNION ALL SELECT 'user_group_mappings: ' || count(*) FROM public.user_group_mappings WHERE id LIKE 'artseed-%'
UNION ALL SELECT 'canvases: ' || count(*) FROM public.canvases WHERE id LIKE 'artseed-%'
UNION ALL SELECT 'canvas_participants: ' || count(*) FROM public.canvas_participants WHERE id LIKE 'artseed-%'
"""


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed user_groups/canvases for ART.")
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--pg-user", default="xyne")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--dsn", default=None)
    ap.add_argument("--groups", type=int, default=30)
    ap.add_argument("--canvases", type=int, default=60)
    ap.add_argument("--wipe", action="store_true")
    a = ap.parse_args()
    if a.wipe:
        psql(a, WIPE_SQL)
        print("wiped artseed-% rows")
    else:
        psql(a, SEED_SQL.format(n_groups=a.groups, n_canvases=a.canvases))
    print(psql(a, COUNT_SQL))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
