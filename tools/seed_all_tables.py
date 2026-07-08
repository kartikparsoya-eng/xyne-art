#!/usr/bin/env python3
"""
seed_all_tables.py — generic FK-aware seeder: put a few rows into EVERY empty
public table so no ART layer is ever blocked by missing data.

Why: the G15 audit traced most of its coverage ceiling to empty tables, not
code — 34 mutator-type skips were unresolvable id-pool keys (callId, formId,
nudgeId, stageId, ...), ~40 app-rejections were backend "X not found" checks
against empty tables, and 52 G12 dark tables were covered-but-zero-rows. Per
the design decision "data doesn't matter — query shapes and interaction
topology matter", synthetic rows are the fix, and hand-writing 70 INSERTs
goes stale on every schema migration. This walks information_schema instead:

  for each empty table (multi-pass until fixpoint, FK deps resolve in order):
    pk text        -> 'artseed-<table>-<n>'   (int pk: max+n; has default: omit)
    fk             -> existing row's value from the referenced table (varied
                      by OFFSET) — works whether the target was pre-existing
                      or seeded in an earlier pass
    NOT NULL       -> type-derived value (enum: first pg_enum label, jsonb {},
                      array '{}', bool false, num 0, ts now(), text distinct)
    nullable non-FK-> NULL (cheapest valid instance; also dodges soft-delete
                      traps like deletedAt)
    nullable FK    -> filled when target has rows; row RETRIED with those
                      NULLed if the first attempt fails (XOR check
                      constraints like canvas_participants user/group)

Idempotent-ish: text ids are 'artseed-%'-prefixed; --wipe deletes them
child-first by re-walking the FK graph. Non-text-PK rows are wiped by their
seeded FK/text columns where possible (best effort, reported).

    .venv/bin/python tools/seed_all_tables.py            # seed all empty
    .venv/bin/python tools/seed_all_tables.py --rows 5
    .venv/bin/python tools/seed_all_tables.py --wipe
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict

SKIP_TABLES = {"_prisma_migrations"}

# Curated destructive-target list (G15 audit): these tables hold ORGANIC rows
# only, so the empty-table sweep never touches them — and the destructive
# phase (which may only consume artseed-% rows, never organic/bulk data) had
# no targets: 18 mutator types (board.delete, project.delete, channel.archive/
# leave, collection.deleteItem, users.remove, workspaceOrg.remove, ...) were
# permanently skipped. Ensure >= --rows artseed rows exist in each regardless
# of emptiness; FK resolution prefers artseed parents so delete cascades stay
# inside the artseed family. Order matters only for readability — the
# fixpoint loop resolves dependencies (projects -> boards -> tickets).
FORCE_SEED = ["projects", "boards", "channels", "collections",
              "workspaces", "users", "tickets",
              # second wave (G15 destructive audit round 2): emailDraft.delete
              # needs artseed conversations, messages.delete artseed messages,
              # workspaceOrg.remove an artseed org to pair with the artseed
              # workspace. NB: conversations/messages carry NO FK constraints
              # in this schema (Prisma relationMode) — name-affinity below
              # supplies identity/artseed values for the ref columns.
              "organizations", "conversations", "messages",
              # third wave (write-path push): apps.update needs an app the
              # identity created; channel.moveToSection a section the identity
              # owns (sections are PER-USER; the 808 organic rows all belong
              # to bulk users); repos for repo.addBranch chains.
              "apps", "channel_sections", "repos"]

# Ref columns that exist WITHOUT an FK constraint (relationMode) or whose
# semantic owner matters for backend permission checks. name -> how to fill.
USERISH_COLS = {"createdBy", "senderId", "updatedBy", "lastEditedBy",
                "invitedBy", "uploadedBy", "addedBy", "ownerId", "userId"}
# Booleans where the backend gates on the ACTIVE state: value_for's blanket
# `false` made every artseed role unusable ("Cannot add members to an
# inactive role") — G15 write-path audit.
TRUEISH_COLS = {"isActive", "active", "isEnabled", "enabled"}


def psql(a, sql: str, quiet: bool = False) -> tuple[int, str]:
    if a.dsn:
        cmd = ["psql", a.dsn]
    else:
        cmd = ["docker", "exec", "-i", a.pg_container, "psql", "-U", a.pg_user,
               "-d", a.db]
    out = subprocess.run(cmd + ["-v", "ON_ERROR_STOP=1", "-Atc", sql],
                         capture_output=True, text=True, timeout=180)
    if out.returncode != 0 and not quiet:
        print(f"  psql ERR: {out.stderr.strip().splitlines()[-1][:160]}",
              file=sys.stderr)
    return out.returncode, out.stdout.strip()


def rows(a, sql: str) -> list[list[str]]:
    rc, out = psql(a, sql)
    if rc != 0:
        raise SystemExit(1)
    return [ln.split("|") for ln in out.splitlines() if ln]


def load_meta(a):
    """columns, pks, fks, enums for all public tables."""
    cols = defaultdict(list)
    for t, c, dt, udt, nullable, default in rows(a, """
        SELECT table_name, column_name, data_type, udt_name, is_nullable,
               coalesce(column_default,'')
        FROM information_schema.columns WHERE table_schema='public'
        ORDER BY table_name, ordinal_position"""):
        cols[t].append({"name": c, "dt": dt, "udt": udt,
                        "nullable": nullable == "YES", "default": default})
    pks = defaultdict(list)
    for t, c in rows(a, """
        SELECT tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tc.constraint_name
         AND kcu.table_schema = tc.table_schema
        WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema='public'"""):
        pks[t].append(c)
    fks = defaultdict(dict)   # table -> col -> (ftable, fcol)
    for t, c, ft, fc in rows(a, """
        SELECT tc.table_name, kcu.column_name, ccu.table_name, ccu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = tc.constraint_name
         AND kcu.table_schema = tc.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema='public'"""):
        fks[t][c] = (ft, fc)
    enums = defaultdict(list)
    for typ, lab in rows(a, """
        SELECT t.typname, e.enumlabel FROM pg_enum e
        JOIN pg_type t ON t.oid=e.enumtypid ORDER BY t.typname, e.enumsortorder"""):
        enums[typ].append(lab)
    return cols, pks, fks, enums


def sq(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def value_for(col: dict, table: str, n: int, enums: dict) -> str | None:
    """SQL literal for a NOT-NULL non-FK non-PK column; None = can't."""
    dt, udt, name = col["dt"].lower(), col["udt"], col["name"]
    if dt in ("text", "character varying", "character", "citext"):
        return sq(f"artseed {table} {name} {n}")
    if dt == "user-defined" and enums.get(udt):
        return sq(enums[udt][n % len(enums[udt])])
    if dt in ("integer", "smallint", "bigint", "numeric",
              "double precision", "real"):
        return str(n)
    if dt == "boolean":
        return "false"
    if dt.startswith("timestamp") or dt == "date":
        return "now()"
    if dt in ("json", "jsonb"):
        return "'{}'::jsonb" if dt == "jsonb" else "'{}'::json"
    if dt == "array":
        return f"'{{}}'::{udt.lstrip('_')}[]"
    if dt == "uuid":
        return "gen_random_uuid()"
    if dt == "bytea":
        return "'\\x'"
    return None


def seed(a) -> int:
    cols, pks, fks, enums = load_meta(a)
    # Identity affinity: user-FK columns get the RUN IDENTITY's user id and
    # channel-FKs get that user's own channels. Without this, artseed rows
    # belong to arbitrary users — G15 then hits "X not found" on every
    # user-scoped lookup (delayed messages, nudges, signatures, calls-via-
    # channel-workspace) even though the rows exist.
    uid = a.identity_user
    if not uid:
        rc, uid = psql(a, "SELECT \"userId\" FROM public.channel_user_status "
                          "GROUP BY 1 ORDER BY count(*) DESC LIMIT 1", quiet=True)
        uid = uid.strip()
    # multi-identity: first uid owns seeded rows; ALL uids get membership
    # links (multi-user runs intersect channel memberships — artseed channels
    # must be visible to every identity or the scoped pool drops them)
    all_uids = [u.strip() for u in (uid or "").split(",") if u.strip()]
    uid = all_uids[0] if all_uids else ""
    ident_channels: list[str] = []
    ident_ws = ""
    if uid:
        rc, out = psql(a, f"SELECT \"channelId\" FROM public.channel_user_status "
                          f"WHERE \"userId\" = '{uid}' LIMIT {a.rows}", quiet=True)
        ident_channels = out.splitlines() if rc == 0 else []
        # The identity's JWT workspace. workspaceId columns are NOT FKs
        # (relationMode), so value_for used to fill them with literal garbage
        # ("artseed tickets workspaceId 0") — every workspace-scoped backend
        # lookup ("X not found in this workspace", "workspace ID mismatch")
        # then failed: ~50 G15 mutator types rejected for this one reason.
        rc, out = psql(a, f"SELECT \"workspaceId\" FROM public.users "
                          f"WHERE id = '{uid}'", quiet=True)
        ident_ws = out.strip() if rc == 0 else ""
        print(f"identity affinity: user={uid} ws={ident_ws or '?'} "
              f"({len(ident_channels)} channels, {len(all_uids)} linked identities)")

    empty = []
    for t in cols:
        if t in SKIP_TABLES:
            continue
        pk = (pks.get(t) or ["id"])[0]
        pk_dt = next((x["dt"] for x in cols[t] if x["name"] == pk), "")
        if pk_dt == "text":
            # converge on ">= rows artseed rows present", NOT fill-if-empty:
            # after one matrix run, tables accumulate applied-create leftovers
            # (cleanup gaps) — count(*)>0 then blocked re-seeding and the
            # destructive phase silently lost its artseed targets (G15 round-4
            # audit: 10 skips, pool keys with artseed=0)
            rc, cnt = psql(a, f'SELECT count(*) FROM public."{t}" '
                              f'WHERE "{pk}" LIKE \'artseed%\'', quiet=True)
            if rc == 0 and int(cnt or 0) < a.rows:
                empty.append(t)
        else:
            _, cnt = psql(a, f'SELECT count(*) FROM public."{t}"')
            if cnt == "0":
                empty.append(t)
    print(f"tables needing artseed rows: {len(empty)}")
    # FORCE_SEED is now subsumed by the artseed-count criterion above; the
    # list remains as documentation of why those tables matter
    forced: list[str] = []
    if forced:
        print(f"force-seed (destructive targets): {', '.join(forced)}")

    seeded, failed = {}, {}
    remaining = set(empty) | set(forced)
    for pass_no in range(1, 6):                      # fixpoint over FK deps
        progressed = False
        for t in sorted(remaining):
            tf = fks.get(t, {})
            pk = pks.get(t, [])
            # FK targets must have rows (either pre-existing or already seeded)
            fk_vals: dict[str, list[str]] = {}
            blocked = None
            for c, (ft, fc) in tf.items():
                nullable = next(x["nullable"] for x in cols[t] if x["name"] == c)
                if ft == "users" and fc == "id" and uid:
                    fk_vals[c] = [uid]                # identity-owned rows
                    continue
                # entities must live in the IDENTITY's workspace (JWT ws):
                # chaining to artseed-workspaces made every workspace-scoped
                # lookup fail. Exception: the workspaces table itself (its own
                # orgId etc. resolve normally).
                if ft == "workspaces" and fc == "id" and ident_ws and t != "workspaces":
                    fk_vals[c] = [ident_ws]
                    continue
                if ft == "channels" and fc == "id":
                    # artseed channels first (identity is linked to them — see
                    # LINK step), else the identity's organic channels
                    rc, out = psql(a, f'SELECT "{fc}" FROM public."{ft}" WHERE '
                                      f'"{fc}" LIKE \'artseed-%\' LIMIT {a.rows}',
                                   quiet=True)
                    art = out.splitlines() if rc == 0 else []
                    if art or ident_channels:
                        fk_vals[c] = art or ident_channels
                        continue
                # prefer artseed parents: keeps destructive cascades inside
                # the artseed family (deleting an artseed board never touches
                # an organic project)
                rc, out = psql(a, f'SELECT "{fc}" FROM public."{ft}" WHERE '
                                  f'"{fc}"::text LIKE \'artseed%\' LIMIT {a.rows}',
                               quiet=True)
                vals = out.splitlines() if rc == 0 else []
                if not vals:
                    rc, out = psql(a, f'SELECT "{fc}" FROM public."{ft}" LIMIT {a.rows}',
                                   quiet=True)
                    vals = out.splitlines() if rc == 0 else []
                if not vals:
                    if c in pk or not nullable:
                        blocked = f"fk {c}->{ft} empty"
                        break
                    continue                          # nullable fk: leave NULL
                fk_vals[c] = vals
            if blocked:
                failed[t] = blocked
                continue

            ok_rows = 0
            # name-affinity for constraint-less ref columns (conversations/
            # messages have no FKs): identity for user-ish, artseed for
            # channel/conversation refs — keeps ownership checks passable and
            # cascades inside the artseed family
            rc, out = psql(a, "SELECT id FROM public.channels WHERE id LIKE "
                              f"'artseed-%' LIMIT {a.rows}", quiet=True)
            art_chans = out.splitlines() if rc == 0 else []
            rc, out = psql(a, 'SELECT "conversationId" FROM public.conversations '
                              f"WHERE \"conversationId\" LIKE 'artseed-%' LIMIT {a.rows}",
                           quiet=True)
            art_convs = out.splitlines() if rc == 0 else []
            for n in range(a.rows):
                for attempt in ("full", "min"):       # min: nullable FKs NULLed
                    names, vals = [], []
                    give_up = False
                    for col in cols[t]:
                        c = col["name"]
                        if col["default"] and c not in tf and c not in pk:
                            continue                  # let defaults fire
                        if c in fk_vals and (attempt == "full" or not col["nullable"]
                                             or c in pk):
                            names.append(f'"{c}"')
                            vals.append(sq(fk_vals[c][n % len(fk_vals[c])]))
                            continue
                        if c in tf:                   # unfilled fk
                            if col["nullable"] and c not in pk:
                                continue
                            give_up = True
                            break
                        if c in pk:
                            if col["dt"] == "text":
                                names.append(f'"{c}"')
                                vals.append(sq(f"artseed-{t}-{n}"))
                            elif col["default"]:
                                pass                  # serial etc.
                            elif col["dt"] in ("integer", "bigint", "smallint"):
                                names.append(f'"{c}"')
                                vals.append(str(900000 + n))
                            else:
                                give_up = True
                                break
                            continue
                        if col["nullable"]:
                            continue                  # cheapest valid: NULL
                        if c in USERISH_COLS and uid and col["dt"] == "text":
                            names.append(f'"{c}"')
                            vals.append(sq(uid))
                            continue
                        if c == "workspaceId" and ident_ws and col["dt"] == "text":
                            names.append(f'"{c}"')
                            vals.append(sq(ident_ws))
                            continue
                        if c in TRUEISH_COLS and col["dt"] == "boolean":
                            names.append(f'"{c}"')
                            vals.append("true")
                            continue
                        if c == "channelId" and art_chans:
                            names.append(f'"{c}"')
                            vals.append(sq(art_chans[n % len(art_chans)]))
                            continue
                        if c == "conversationId" and art_convs:
                            names.append(f'"{c}"')
                            vals.append(sq(art_convs[n % len(art_convs)]))
                            continue
                        v = value_for(col, t, n, enums)
                        if v is None:
                            give_up = True
                            break
                        names.append(f'"{c}"')
                        vals.append(v)
                    if give_up:
                        failed[t] = "unfillable column"
                        break
                    sql = (f'INSERT INTO public."{t}" ({", ".join(names)}) '
                           f'VALUES ({", ".join(vals)}) ON CONFLICT DO NOTHING')
                    rc, _ = psql(a, sql, quiet=True)
                    if rc == 0:
                        ok_rows += 1
                        break                          # next row
                    if attempt == "min":
                        rc2, err = psql(a, sql)        # loud once, for report
                        failed[t] = "insert failed"
            if ok_rows:
                seeded[t] = ok_rows
                remaining.discard(t)
                failed.pop(t, None)
                progressed = True
        if not remaining or not progressed:
            break

    # ---- LINK steps: attach identities to the artseed entities -------------
    # Destructive mutators check MEMBERSHIP/ownership, not just existence:
    # channel.leaveChannel needs the caller IN the channel, bookmark.remove
    # needs the caller's bookmark row, workspaceOrg.remove needs the link row,
    # users.remove needs the target user to be an org member. All idempotent
    # (deterministic artseed ids + ON CONFLICT DO NOTHING).
    links = 0
    if uid:
        role_of = lambda t, d: (enums.get(t) or [d])[0]  # noqa: E731
        uid_rows = ", ".join(sq(u) for u in all_uids)
        # a SECOND known member for target-user mutators (channel.remove-
        # Participant / updateParticipantRole reject when the target isn't in
        # the channel — a random pool userId almost never is)
        rc, bulk0 = psql(a, "SELECT id FROM public.users WHERE email = "
                            "'bulk-user-000@xyne.test' LIMIT 1", quiet=True)
        bulk0 = bulk0.strip() if rc == 0 else ""
        member_ids = [u for u in all_uids + ([bulk0] if bulk0 else []) if u]
        member_rows = ", ".join(sq(u) for u in member_ids)
        cp_role = next((x["udt"] for x in cols.get("channel_participants", [])
                        if x["name"] == "role"), "ChannelRole")
        cv_role = next((x["udt"] for x in cols.get("canvas_participants", [])
                        if x["name"] == "role"), "CanvasParticipantRole")
        link_sql = [
            # Fixture-state RESTORE first: applied mutations from EARLIER
            # matrix rounds mutate the fixture channels (archiveChannel
            # archived -0, type/visibility updates retyped it) and the
            # row-existence criterion never repairs STATE — round-6 audit:
            # every chain head pinned to artseed-channels-0 died with
            # "cannot create conversations in archived channel". Reset ALL
            # artseed channels to canonical, then apply the specific fixtures.
            """UPDATE public.channels SET "isArchived"=false, type='DEFAULT',
                   visibility='PUBLIC'
               WHERE id LIKE 'artseed-channels-%';""",
            """UPDATE public.channels SET type='EMAIL'
               WHERE id = 'artseed-channels-1';""",
            """UPDATE public.channels SET "isArchived"=true
               WHERE id = 'artseed-channels-3';""",
            # tuple-unique leftovers from earlier rounds' applied grants/
            # upserts (their pks were pool-drawn or cleanup-gapped — the
            # artmx pre-sweep can't see them; the TUPLE is what collides)
            f"""DELETE FROM public.resource_access
               WHERE "userId" = {sq(uid)} AND "resourceId" LIKE 'artseed%';""",
            """DELETE FROM public.forms_context_mapping
               WHERE "contextId" LIKE 'artseed%';""",
            # canvas.create join-affinity: folder-0 must belong to project-0
            # (folderId+projectId are drawn independently by the synthesizer;
            # VALUE_OVERRIDES pins both to this deterministic pair)
            """UPDATE public.canvas_folders SET "projectId"='artseed-projects-0'
               WHERE id = 'artseed-canvas_folders-0';""",
            # every identity + bulk0 joins every artseed channel EXCEPT -4
            # (the joinChannel target must not already be a member) — BOTH
            # membership tables: channel_user_status is the sync-state row,
            # channel_participants is what the backend's participant checks
            # actually read (leaveChannel said "Not a channel participant"
            # with the cus row present).
            f"""INSERT INTO public.channel_user_status (id, "channelId", "userId")
                SELECT 'artseed-cus-' || c.id || '-' || u.id, c.id, u.id
                FROM public.channels c, public.users u
                WHERE c.id LIKE 'artseed-%' AND c.id <> 'artseed-channels-4'
                  AND u.id IN ({member_rows})
                ON CONFLICT DO NOTHING;""",
            f"""INSERT INTO public.channel_participants (id, "channelId", "userId", role)
                SELECT 'artseed-cp-' || c.id || '-' || u.id, c.id, u.id,
                       {sq(role_of(cp_role, 'MEMBER'))}
                FROM public.channels c, public.users u
                WHERE c.id LIKE 'artseed-%' AND c.id <> 'artseed-channels-4'
                  AND u.id IN ({member_rows})
                ON CONFLICT DO NOTHING;""",
            # identity is OWNER of every artseed canvas (canvas.* mutators
            # gate on "Only canvas owners or editors")
            f"""INSERT INTO public.canvas_participants (id, "canvasId", "userId", role, "joinedAt", "updatedAt")
                SELECT 'artseed-cvp-' || c.id || '-me', c.id, {sq(uid)},
                       {sq(role_of(cv_role, 'OWNER'))}, now(), now()
                FROM public.canvases c WHERE c.id LIKE 'artseed%'
                ON CONFLICT DO NOTHING;""",
            # identity participates in every artseed call, invitedBy NULL
            # (creator-ish: calls.approveLobbyRequest checks "call creator")
            f"""INSERT INTO public.call_participants (id, "callId", "userId",
                       "invitedBy", response, "displayName", "isExternal")
                SELECT 'artseed-clp-' || c.id || '-me', c.id, {sq(uid)},
                       {sq(uid)}, {sq(role_of('InvitationResponse', 'ACCEPTED'))},
                       'ART Identity', false
                FROM public.calls c WHERE c.id LIKE 'artseed-%'
                ON CONFLICT DO NOTHING;""",
            # identity holds every artseed role (role.removeMembers needs an
            # existing mapping row)
            f"""INSERT INTO public.user_role_mappings (id, "userId", "roleId", "createdAt", "updatedAt")
                SELECT 'artseed-urm-' || r.id || '-me', {sq(uid)}, r.id, now(), now()
                FROM public.roles r WHERE r.id LIKE 'artseed-%'
                ON CONFLICT DO NOTHING;""",
            # artseed users become org members (email join; userId col is the
            # literal string 'deprecated' in real rows). Organic org on
            # purpose: membership must be in the org the identity operates in.
            f"""INSERT INTO public.org_members ("memberId", "orgId", "userId", email, role)
                SELECT 'artseed-om-' || u.id,
                       (SELECT "orgId" FROM public.organizations
                        WHERE "orgId" NOT LIKE 'artseed%' LIMIT 1),
                       'deprecated', u.email, {sq(role_of('OrgRole', 'MEMBER'))}
                FROM public.users u WHERE u.id LIKE 'artseed-%'
                ON CONFLICT DO NOTHING;""",
            # artseed workspaces get workspace<->org link rows that
            # workspaceOrg.remove deletes: one to the workspace's own org and
            # one per artseed org (the destructive draw pairs artseed ids)
            f"""INSERT INTO public.workspace_organizations (id, "orgId", "workspaceId", role, "updatedAt")
                SELECT 'artseed-wso-' || w.id, w."orgId", w.id,
                       {sq(role_of('WorkspaceOrgRole', 'MEMBER'))}, now()
                FROM public.workspaces w WHERE w.id LIKE 'artseed-%'
                ON CONFLICT DO NOTHING;""",
            f"""INSERT INTO public.workspace_organizations (id, "orgId", "workspaceId", role, "updatedAt")
                SELECT 'artseed-wso-' || w.id || '-' || o."orgId", o."orgId", w.id,
                       {sq(role_of('WorkspaceOrgRole', 'MEMBER'))}, now()
                FROM public.workspaces w, public.organizations o
                WHERE w.id LIKE 'artseed-%' AND o."orgId" LIKE 'artseed%'
                ON CONFLICT DO NOTHING;""",
            # identity bookmarks every artseed ticket (bookmark.remove target)
            f"""INSERT INTO public.bookmarks (id, "userId", "entityId", "entityType")
                SELECT 'artseed-bm-' || t.id, {sq(uid)}, t.id,
                       {sq(role_of('BookmarkEntityType', 'TICKET'))}
                FROM public.tickets t WHERE t.id LIKE 'artseed-%'
                ON CONFLICT DO NOTHING;""",
        ]
        for s in link_sql:
            rc, _ = psql(a, s, quiet=False)
            if rc == 0:
                links += 1
    n_links = 17

    psql(a, "ANALYZE", quiet=True)
    print(f"\nseeded {len(seeded)} tables "
          f"({sum(seeded.values())} rows), links {links}/{n_links}, "
          f"pass-limit leftovers: {len(remaining)}")
    for t in sorted(remaining):
        print(f"  UNSEEDED {t}: {failed.get(t, '?')}")
    return 0


def wipe(a) -> int:
    cols, pks, fks, _ = load_meta(a)
    # child-first: repeat sweeps until nothing deletes (FK ordering the lazy way)
    total = 0
    for _ in range(6):
        deleted = 0
        for t in cols:
            if t in SKIP_TABLES:
                continue
            pk = pks.get(t, [])
            conds = [f'"{c}" LIKE \'artseed-%\'' for c in pk
                     if next((x for x in cols[t] if x["name"] == c), {}).get("dt") == "text"]
            # non-text-pk tables: wipe by any seeded text column signature
            if not conds:
                tcols = [x["name"] for x in cols[t]
                         if x["dt"] == "text" and x["name"] not in fks.get(t, {})]
                conds = [f'"{c}" LIKE \'artseed %\'' for c in tcols[:2]]
            if not conds:
                continue
            rc, out = psql(a, f'WITH d AS (DELETE FROM public."{t}" WHERE '
                              f'{" OR ".join(conds)} RETURNING 1) '
                              f'SELECT count(*) FROM d', quiet=True)
            if rc == 0 and out and out != "0":
                deleted += int(out)
        total += deleted
        if deleted == 0:
            break
    print(f"wiped {total} artseed rows")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed every empty public table.")
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--pg-user", default="xyne")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--dsn", default=None)
    ap.add_argument("--rows", type=int, default=5)
    ap.add_argument("--identity-user", default=None,
                    help="users.id that seeded rows should belong to "
                         "(default: user with most channel memberships)")
    ap.add_argument("--wipe", action="store_true")
    a = ap.parse_args()
    return wipe(a) if a.wipe else seed(a)


if __name__ == "__main__":
    raise SystemExit(main())
