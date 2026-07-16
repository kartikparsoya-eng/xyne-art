#!/usr/bin/env python3
"""
seed_prod_scale.py — scale the sandbox DB to production-like row counts
on the tables that drive EXISTS N+1 and heavy hydration paths.

The sandbox bulk-seed.ts gives us 2K channels, 500K messages, 18K
conversations — but the EXISTS-heavy tables are tiny (101 canvases, 85
tickets, 248 user_groups). This seeder scales them up so the ART replay
actually exercises the N+1 patterns at production scale.

Target row counts (based on prod 7-day call volumes):
  canvases:             101 → 5000
  canvas_participants:  249 → 25000  (5 per canvas)
  tickets:               85 → 3000
  sub_tickets:            5 → 1500   (subTicketsByMappedTicketId = 13.72% of prod calls)
  ticket_activities:    269 → 15000  (5 per ticket)
  activities:           533 → 20000
  bookmarks:           1037 → 10000  (userBookmarks = 8.70% of prod calls)
  channel_sections:     853 → 5000
  channel_participants: 6394 → 25000 (additional members beyond bulk-seed)
  user_groups:          248 → 2000   (getAllUserGroups = 2.63%)
  user_group_mappings:  326 → 15000  (varied users × groups)
  conversation_participants: 10403 → 50000 (threadConversation = 6.85%)
  users:                232 → 500    (different data visibility per user)

Idempotent: all IDs prefixed 'artscale-'. --wipe removes exactly those rows.

    .venv/bin/python tools/seed_prod_scale.py                     # seed all
    .venv/bin/python tools/seed_prod_scale.py --wipe               # remove
    .venv/bin/python tools/seed_prod_scale.py --canvases 10000     # custom scale
"""
from __future__ import annotations

import argparse
import subprocess
import sys

WS = "cmr1unwn2002s6p43dmi2ygla"  # bulk-seeded workspace


def psql(a, sql: str) -> str:
    if a.dsn:
        cmd = ["psql", a.dsn]
    else:
        cmd = ["docker", "exec", a.pg_container, "psql", "-U", a.pg_user, "-d", a.db]
    out = subprocess.run(cmd + ["-Atc", sql], capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        print(f"ERROR: {out.stderr.strip()[:500]}", file=sys.stderr)
        raise SystemExit(1)
    return out.stdout.strip()


def run_sql(a, label: str, sql: str):
    print(f"  seeding {label}...", flush=True, end=" ")
    r = psql(a, sql)
    print(f"done ({r})")


def seed(a):
    n = a.scale  # base scale factor (1.0 = default targets)

    # Collect existing IDs we'll reference
    bulk_users = psql(a, f"SELECT id FROM public.users WHERE email LIKE 'bulk-user-%' ORDER BY email LIMIT {int(200 * n)}")
    if not bulk_users:
        print("ERROR: no bulk-user-% users found. Run bulk-seed.ts first.", file=sys.stderr)
        return 1
    user_ids = bulk_users.strip().split("\n")

    channels = psql(a, f'SELECT id FROM public.channels ORDER BY id LIMIT {int(2000 * n)}')
    channel_ids = channels.strip().split("\n")

    conversations = psql(a, f'SELECT "conversationId" FROM public.conversations ORDER BY "conversationId" LIMIT {int(5000 * n)}')
    conv_ids = conversations.strip().split("\n")

    boards = psql(a, f"SELECT id FROM public.boards ORDER BY id LIMIT 10")
    board_ids = boards.strip().split("\n") if boards.strip() else ["artscale-board-0"]
    projects = psql(a, f"SELECT id FROM public.projects ORDER BY id LIMIT 10")
    project_ids = projects.strip().split("\n") if projects.strip() else ["artscale-proj-0"]

    print(f"  existing: {len(user_ids)} users, {len(channel_ids)} channels, {len(conv_ids)} conversations")

    # 1. Users (scale to 500)
    n_users = int(300 * n)
    run_sql(a, f"users ({n_users})", f"""
INSERT INTO public.users (id, name, email, "authProvider", "providerUserId",
  status, "userType", "workspaceId", role, "orgMemberId", "createdAt", "updatedAt")
SELECT 'artscale-user-' || lpad(i::text, 4, '0'),
       'Scale User ' || i,
       'scale-user-' || i || '@xyne.test',
       'GOOGLE', 'scale-provider-' || i,
       'ACTIVE', 'USER', '{WS}', 'MEMBER',
       'artscale-member-' || lpad(i::text, 4, '0'),
       now(), now()
FROM generate_series(0, {n_users} - 1) i
ON CONFLICT DO NOTHING;
""")
    # Also create org_members for these users
    run_sql(a, f"org_members ({n_users})", f"""
INSERT INTO public.org_members ("memberId", email, "orgId", "userId", role, "joinedAt")
SELECT 'artscale-member-' || lpad(i::text, 4, '0'),
       'scale-user-' || i || '@xyne.test',
       (SELECT "orgId" FROM public.organizations LIMIT 1),
       'artscale-user-' || lpad(i::text, 4, '0'),
       'MEMBER', now()
FROM generate_series(0, {n_users} - 1) i
ON CONFLICT ("memberId") DO NOTHING;
""")

    # 2. User groups (scale to 2000)
    n_groups = int(2000 * n)
    run_sql(a, f"user_groups ({n_groups})", f"""
INSERT INTO public.user_groups (id, "workspaceId", name, alias, description,
  "autoRotationEnabled", "isActive", "createdAt", "updatedAt")
SELECT 'artscale-ug-' || lpad(i::text, 4, '0'), '{WS}',
       'Scale Group ' || i, 'scale-group-' || i,
       'prod-scale synthetic group', false, true, now(), now()
FROM generate_series(0, {n_groups} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 3. User group mappings (scale to 15000, each user in ~5 groups)
    # Generate unique (userId, userGroupId) pairs: user = i % n_pool, group = (i / n_pool) % n_groups
    # This guarantees no duplicate pairs since each i maps to a unique (user, group) combo
    n_ugm = int(15000 * n)
    all_users = psql(a, f"SELECT id FROM public.users WHERE email LIKE 'bulk-user-%' OR email LIKE 'scale-user-%' ORDER BY email LIMIT 500")
    all_user_ids = all_users.strip().split("\n") if all_users.strip() else user_ids
    n_pool = min(len(all_user_ids), 200)
    run_sql(a, f"user_group_mappings ({n_ugm})", f"""
INSERT INTO public.user_group_mappings (id, "userId", "userGroupId", "createdAt", "updatedAt")
SELECT 'artscale-ugm-' || lpad(i::text, 5, '0'),
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {n_pool}) + 1],
       'artscale-ug-' || lpad(((i / {n_pool}) % {n_groups})::text, 4, '0'),
       now(), now()
FROM generate_series(0, {n_ugm} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 4. Canvases (scale to 5000)
    n_canvases = int(5000 * n)
    run_sql(a, f"canvases ({n_canvases})", f"""
INSERT INTO public.canvases (id, title, content, "channelId", "createdBy",
  visibility, "isTemplate", "docType", "isCollaborative", "createdAt", "updatedAt")
SELECT 'artscale-cv-' || lpad(i::text, 4, '0'),
       'Scale Canvas ' || i,
       jsonb_build_object('type', 'doc', 'content', jsonb_build_array(
         jsonb_build_object('type', 'paragraph', 'content', jsonb_build_array(
           jsonb_build_object('type', 'text', 'text', 'synthetic ' || i))))),
       (ARRAY[{','.join(f"'{cid}'" for cid in channel_ids[:200])}])[(i % {min(len(channel_ids), 200)}) + 1],
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       CASE i % 2 WHEN 0 THEN 'PUBLIC'::"CanvasVisibility" ELSE 'PRIVATE'::"CanvasVisibility" END,
       false, 'Canvas'::"DocType", true, now(), now()
FROM generate_series(0, {n_canvases} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 5. Canvas participants (scale to 25000, 5 per canvas)
    n_cvp = int(25000 * n)
    run_sql(a, f"canvas_participants ({n_cvp})", f"""
INSERT INTO public.canvas_participants (id, "canvasId", "userId", role, "joinedAt", "updatedAt")
SELECT 'artscale-cvp-' || lpad(i::text, 5, '0'),
       'artscale-cv-' || lpad((i / 5 % {n_canvases})::text, 4, '0'),
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       CASE i % 3 WHEN 0 THEN 'OWNER'::"CanvasRole" WHEN 1 THEN 'EDITOR'::"CanvasRole" ELSE 'VIEWER'::"CanvasRole" END,
       now(), now()
FROM generate_series(0, {n_cvp} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 6. Tickets (scale to 3000)
    n_tickets = int(3000 * n)
    run_sql(a, f"tickets ({n_tickets})", f"""
INSERT INTO public.tickets (id, title, description, status, "statusV2",
  "createdBy", "updatedBy", "conversationId", "channelId",
  priority, "xyneId", "projectId", "workspaceId", "boardId", "stageName",
  "isArchived", "emailReplyEnabled", "lastEmailAt", "statusUpdatedAt",
  "createdAt", "updatedAt")
SELECT 'artscale-tk-' || lpad(i::text, 4, '0'),
       'Scale Ticket ' || i, 'synthetic ticket for prod-scale ART',
       'NEW'::"TicketStatus", 'TODO'::"TicketStatusV2",
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       (ARRAY[{','.join(f"'{cid}'" for cid in conv_ids[:500])}])[(i % {min(len(conv_ids), 500)}) + 1],
       (ARRAY[{','.join(f"'{cid}'" for cid in channel_ids[:200])}])[(i % {min(len(channel_ids), 200)}) + 1],
       CASE i % 4 WHEN 0 THEN 'LOW'::"TicketPriority" WHEN 1 THEN 'MEDIUM'::"TicketPriority"
                  WHEN 2 THEN 'HIGH'::"TicketPriority" ELSE 'CRITICAL'::"TicketPriority" END,
       'scale-xyne-' || lpad(i::text, 4, '0'),
       (ARRAY[{','.join(f"'{pid}'" for pid in project_ids)}])[(i % {len(project_ids)}) + 1],
       '{WS}',
       (ARRAY[{','.join(f"'{bid}'" for bid in board_ids)}])[(i % {len(board_ids)}) + 1],
       'Todo', false, false, now(), now(), now(), now()
FROM generate_series(0, {n_tickets} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 7. Sub-tickets (scale to 1500, subTicketsByMappedTicketId = 13.72% of prod calls)
    n_sub = int(1500 * n)
    run_sql(a, f"sub_tickets ({n_sub})", f"""
INSERT INTO public.sub_tickets (id, title, description, "workspaceId",
  "mappedTicketId", "createdBy", "updatedBy", "createdAt", "updatedAt")
SELECT 'artscale-sub-' || lpad(i::text, 4, '0'),
       'Scale Sub ' || i, 'synthetic sub-ticket',
       '{WS}',
       'artscale-tk-' || lpad((i % {n_tickets})::text, 4, '0'),
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       now(), now()
FROM generate_series(0, {n_sub} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 8. Ticket activities (scale to 15000, 5 per ticket)
    n_ta = int(15000 * n)
    run_sql(a, f"ticket_activities ({n_ta})", f"""
INSERT INTO public.ticket_activities (id, "ticketId", "updatedBy",
  timestamp, "activityType", value)
SELECT 'artscale-ta-' || lpad(i::text, 5, '0'),
       'artscale-tk-' || lpad((i / 5 % {n_tickets})::text, 4, '0'),
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       now(),
       (ARRAY['TITLE'::"ActivityType",'DESCRIPTION'::"ActivityType",'STATUS'::"ActivityType",'PRIORITY'::"ActivityType",'ASSIGNED_TO'::"ActivityType"])[(i % 5) + 1],
       jsonb_build_object('old', 'old-val-' || i, 'new', 'new-val-' || i)
FROM generate_series(0, {n_ta} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 9. Activities (scale to 20000)
    n_act = int(20000 * n)
    run_sql(a, f"activities ({n_act})", f"""
INSERT INTO public.activities (id, "userId", "actorAction", "actionSource",
  "actionSourceId", "actorId", classification, "isRead",
  "createdAt", "updatedAt", "conversationId", "channelId", "messageId", "ticketId")
SELECT 'artscale-act-' || lpad(i::text, 5, '0'),
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       'mentioned you', 'mention', 'src-' || i,
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       CASE i % 4 WHEN 0 THEN 'ACTIONABLE'::"ActivityClassification" WHEN 1 THEN 'FYI'::"ActivityClassification"
                  WHEN 2 THEN 'SKIP'::"ActivityClassification" ELSE 'PENDING'::"ActivityClassification" END,
       (i % 3 = 0), now(), now(),
       (ARRAY[{','.join(f"'{cid}'" for cid in conv_ids[:500])}])[(i % {min(len(conv_ids), 500)}) + 1],
       (ARRAY[{','.join(f"'{cid}'" for cid in channel_ids[:200])}])[(i % {min(len(channel_ids), 200)}) + 1],
       'artscale-msg-' || i,
       CASE WHEN i % 5 = 0 THEN 'artscale-tk-' || lpad((i % {n_tickets})::text, 4, '0') ELSE NULL END
FROM generate_series(0, {n_act} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 10. Bookmarks (scale to 10000, userBookmarks = 8.70% of prod calls)
    n_bm = int(10000 * n)
    run_sql(a, f"bookmarks ({n_bm})", f"""
INSERT INTO public.bookmarks (id, "userId", "entityId", "entityType",
  "isDeleted", "isCompleted", "createdAt", "updatedAt")
SELECT 'artscale-bm-' || lpad(i::text, 5, '0'),
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       CASE i % 4 WHEN 0 THEN (ARRAY[{','.join(f"'{cid}'" for cid in conv_ids[:500])}])[(i % {min(len(conv_ids), 500)}) + 1]
                    WHEN 1 THEN 'artscale-tk-' || lpad((i % {n_tickets})::text, 4, '0')
                    WHEN 2 THEN 'artscale-cv-' || lpad((i % {n_canvases})::text, 4, '0')
                    ELSE 'msg-' || i END,
       CASE i % 4 WHEN 0 THEN 'CONVERSATION'::"BookmarkEntityType" WHEN 1 THEN 'TICKET'::"BookmarkEntityType"
                    WHEN 2 THEN 'CANVAS'::"BookmarkEntityType" ELSE 'MESSAGE'::"BookmarkEntityType" END,
       false, (i % 5 = 0), now(), now()
FROM generate_series(0, {n_bm} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 11. Channel sections (scale to 5000)
    n_cs = int(5000 * n)
    run_sql(a, f"channel_sections ({n_cs})", f"""
INSERT INTO public.channel_sections (id, "userId", "workspaceId", name,
  "isCollapsed", "isDeleted", "createdAt", "updatedAt", position)
SELECT 'artscale-cs-' || lpad(i::text, 4, '0'),
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       '{WS}', 'Section ' || (i % 20),
       false, false, now(), now(), 'ALPHABETICAL'::"ChannelSortOrder"
FROM generate_series(0, {n_cs} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 12. Additional channel participants (scale to 25000 total)
    n_cp = int(20000 * n)  # bulk-seed already added ~6K
    run_sql(a, f"channel_participants ({n_cp})", f"""
INSERT INTO public.channel_participants (id, "channelId", "userId", "joinedAt",
  role, "lastViewedAt", "isStarred", "isClosed")
SELECT 'artscale-cp-' || lpad(i::text, 5, '0'),
       (ARRAY[{','.join(f"'{cid}'" for cid in channel_ids[:500])}])[(i % {min(len(channel_ids), 500)}) + 1],
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       now(),
       CASE i % 5 WHEN 0 THEN 'ADMIN'::"ChannelRole" ELSE 'MEMBER'::"ChannelRole" END,
       now(), (i % 10 = 0), (i % 7 = 0)
FROM generate_series(0, {n_cp} - 1) i
ON CONFLICT DO NOTHING;
""")

    # 13. Conversation participants (scale to 50000)
    n_conv_p = int(40000 * n)  # bulk-seed already added ~10K
    run_sql(a, f"conversation_participants ({n_conv_p})", f"""
INSERT INTO public.conversation_participants (id, "conversationId", "userId",
  "isSubscribed", "joinedAt", "lastReadAt", "lastReplyAt")
SELECT 'artscale-cvp-' || lpad(i::text, 5, '0'),
       (ARRAY[{','.join(f"'{cid}'" for cid in conv_ids[:1000])}])[(i % {min(len(conv_ids), 1000)}) + 1],
       (ARRAY[{','.join(f"'{uid}'" for uid in all_user_ids[:200])}])[(i % {min(len(all_user_ids), 200)}) + 1],
       true, now(), now(), now()
FROM generate_series(0, {n_conv_p} - 1) i
ON CONFLICT DO NOTHING;
""")

    # Final counts
    print("\n=== final counts ===")
    counts = psql(a, """
SELECT 'users: ' || count(*) FROM public.users WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'user_groups: ' || count(*) FROM public.user_groups WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'user_group_mappings: ' || count(*) FROM public.user_group_mappings WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'canvases: ' || count(*) FROM public.canvases WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'canvas_participants: ' || count(*) FROM public.canvas_participants WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'tickets: ' || count(*) FROM public.tickets WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'sub_tickets: ' || count(*) FROM public.sub_tickets WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'ticket_activities: ' || count(*) FROM public.ticket_activities WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'activities: ' || count(*) FROM public.activities WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'bookmarks: ' || count(*) FROM public.bookmarks WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'channel_sections: ' || count(*) FROM public.channel_sections WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'channel_participants: ' || count(*) FROM public.channel_participants WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'conversation_participants: ' || count(*) FROM public.conversation_participants WHERE id LIKE 'artscale-%'
UNION ALL SELECT 'users: ' || count(*) FROM public.users WHERE id LIKE 'artscale-%';
""")
    print(counts)

    # Total table counts (including bulk-seed)
    print("\n=== total table counts (bulk + artscale) ===")
    totals = psql(a, """
SELECT 'users: ' || count(*) FROM public.users
UNION ALL SELECT 'user_groups: ' || count(*) FROM public.user_groups
UNION ALL SELECT 'user_group_mappings: ' || count(*) FROM public.user_group_mappings
UNION ALL SELECT 'canvases: ' || count(*) FROM public.canvases
UNION ALL SELECT 'canvas_participants: ' || count(*) FROM public.canvas_participants
UNION ALL SELECT 'tickets: ' || count(*) FROM public.tickets
UNION ALL SELECT 'sub_tickets: ' || count(*) FROM public.sub_tickets
UNION ALL SELECT 'ticket_activities: ' || count(*) FROM public.ticket_activities
UNION ALL SELECT 'activities: ' || count(*) FROM public.activities
UNION ALL SELECT 'bookmarks: ' || count(*) FROM public.bookmarks
UNION ALL SELECT 'channel_sections: ' || count(*) FROM public.channel_sections
UNION ALL SELECT 'channel_participants: ' || count(*) FROM public.channel_participants
UNION ALL SELECT 'conversation_participants: ' || count(*) FROM public.conversation_participants;
""")
    print(totals)
    return 0


def wipe(a):
    print("wiping artscale-% rows (child-first)...")
    tables = [
        "conversation_participants", "channel_participants", "channel_sections",
        "bookmarks", "activities", "ticket_activities", "sub_tickets",
        "tickets", "canvas_participants", "canvases",
        "user_group_mappings", "user_groups", "users",
    ]
    for t in tables:
        col = "\"memberId\"" if t == "org_members" else "id"
        r = psql(a, f"DELETE FROM public.{t} WHERE {col} LIKE 'artscale-%';")
        print(f"  {t}: deleted {r}")
    print("done")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed prod-scale data for ART.")
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--pg-user", default="xyne")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--dsn", default=None)
    ap.add_argument("--scale", type=float, default=1.0,
                    help="scale factor (1.0 = default prod-scale targets)")
    ap.add_argument("--wipe", action="store_true")
    a = ap.parse_args()
    if a.wipe:
        return wipe(a)
    return seed(a)


if __name__ == "__main__":
    raise SystemExit(main())
