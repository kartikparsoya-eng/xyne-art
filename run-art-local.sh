#!/usr/bin/env bash
# run-art-local.sh — ART Mode A against a LOCAL docker sandbox (no VPN/Grafana).
#
# Auto-discovers everything from the running sandbox:
#   1. JWT secret        <- backend container env (ZERO_AUTH_SECRET)
#   2. test identity     <- sandbox DB (user with the most channel memberships,
#                           + org_members.memberId + users.workspaceId claims)
#   3. JWT               <- tools/mint_local_jwt.py (HS256)
#   4. id-pool           <- tools/gen_id_pool_db.py (real IDs from the sandbox DB,
#                           scoped to that user so mutators pass participation checks)
#   5. clientSchema      <- CVR instances table (required by initConnection)
#   6. drives the replay <- harness/replay.py
#
#   ./run-art-local.sh                                   # 50 conns, 3 min, reads only
#   ./run-art-local.sh --mutations                       # + read-tracking writes (sandbox is disposable)
#   ./run-art-local.sh --connections 200 --duration 600  # prod-scale
#   ./run-art-local.sh --lifecycle                       # realistic sessions: abrupt drops, cookie resumes, zombies
#   ./run-art-local.sh --users 5 --mutations             # multi-user write contention (auth pool)
#   ./run-art-local.sh --zipf 1.1 --users 3 --mutations  # hot-key skew: everyone hammers the hottest channels
#   ./run-art-local.sh --soak                            # 1h leak hunt: 20 conns + lifecycle + slope gate
#   ./run-art-local.sh --oracle                          # + G8: differential oracle vs the TS reference
#                                                        #   (zero-cache-ts container; self-diff if absent)
#   ./run-art-local.sh --chaos                           # + G10: fault injection (docker pause zero-cache/
#                                                        #   postgres mid-run; implies --lifecycle)
#   ./run-art-local.sh --negative                        # + G11: adversarial negative suite (forged
#                                                        #   cookies, cross-user probe, reconnect storm,
#                                                        #   auth rotation, TTL purge; --users 2 recommended)
#   ./run-art-local.sh --clean                           # purge art-% CVR rows + restart zero-cache first
#   ./run-art-local.sh --prod-profile --duration 1800    # PROD-cadence realism (profiles/prod-7d.json:
#                                                        #   churn ~1/3min, muts ~1/18min, 12.7min sessions,
#                                                        #   lifecycle on — vs the default ~250x-hot torture
#                                                        #   shape; needs >=15-30min to accumulate samples)
#   ./run-art-local.sh --trace raw/traces/trace-last10m.ndjson --clean
#                                                        # TRACE-FAITHFUL replay (harness/trace_replay.py):
#                                                        #   real prod session sequences/timing/interleaving,
#                                                        #   101-identity auth pool, unscoped trace id-pool,
#                                                        #   auto-seeds user_groups/canvases (artseed-%).
#                                                        #   G13 log window + gate wired like any run.
#                                                        #   --clean strongly recommended: accumulated art-%
#                                                        #   CVR groups poison trace latencies (52ms->26s class)
#   ./run-art-local.sh --trace T --time-compress 4       # same trace at 4x intensity
#   ./run-art-local.sh --sandbox other-name --refresh    # different sandbox / re-harvest
#
# Every run samples zero-cache resources (docker stats + pprof + CVR rows) and
# finishes with tools/local_gate.py — a PASS/FAIL verdict, no Grafana needed.
# Bless a known-good run as the latency baseline with:
#   python3 tools/local_gate.py --update-baseline
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

SANDBOX="rust-test"; CONNS=50; WORKING_SET=12; CHURN_MS=750; DURATION=180
MUTATIONS=0; MUT_RATE=10; REFRESH=0; USER_ID=""; USERS=1; LIFECYCLE=0; SOAK=0; CLEAN=0; ZIPF=0; ORACLE=0; CHAOS=0; NEGATIVE=0; MUTMATRIX=0
PROTOCOL=0; TELEMETRY=0; COLDSTART=0; READINESS=0; DRAIN=0; DETERMINISM=0; CAPACITY=0; IMAGEAUDIT=0; UPGRADE=0; PARITY=0; PARITY_FACTOR=2.0; CASCADE=0; OVERSAMPLE=0
IMAGE=""; HTTP_PORT=""; CAPACITY_LADDER="10,25,50,100,200"; CAPACITY_BLESSED=0; DRAIN_BUDGET=30
PROFILE=""; WS_SET=0; CHURN_SET=0; MUT_SET=0; SWAP=0
CONNS_SET=0; DUR_SET=0; TRACE=""; TCOMPRESS=1
while [ $# -gt 0 ]; do
  case "$1" in
    --sandbox) SANDBOX="$2"; shift 2;;
    --connections) CONNS="$2"; CONNS_SET=1; shift 2;;
    --working-set) WORKING_SET="$2"; WS_SET=1; shift 2;;
    --churn-ms) CHURN_MS="$2"; CHURN_SET=1; shift 2;;
    --duration) DURATION="$2"; DUR_SET=1; shift 2;;
    --mutations) MUTATIONS=1; shift;;
    --mutations-per-min) MUT_RATE="$2"; MUT_SET=1; shift 2;;
    --prod-profile) PROFILE="profiles/prod-7d.json"; shift;;
    --swap) SWAP=1; shift;;
    --profile) PROFILE="$2"; shift 2;;
    --trace) TRACE="$2"; shift 2;;
    --time-compress) TCOMPRESS="$2"; shift 2;;
    --user-id) USER_ID="$2"; shift 2;;
    --users) USERS="$2"; shift 2;;
    --lifecycle) LIFECYCLE=1; shift;;
    --soak) SOAK=1; LIFECYCLE=1; shift;;
    --clean) CLEAN=1; shift;;
    --zipf) ZIPF="${2:-1.1}"; shift 2;;
    --oracle) ORACLE=1; shift;;
    --chaos) CHAOS=1; LIFECYCLE=1; shift;;
    --negative) NEGATIVE=1; shift;;
    --mutation-matrix) MUTMATRIX=1; shift;;
    --refresh) REFRESH=1; shift;;
    --protocol) PROTOCOL=1; shift;;
    --telemetry) TELEMETRY=1; shift;;
    --cold-start) COLDSTART=1; shift;;
    --readiness) READINESS=1; shift;;
    --drain) DRAIN=1; shift;;
    --determinism) DETERMINISM=1; shift;;
    --capacity) CAPACITY=1; shift;;
    --capacity-ladder) CAPACITY_LADDER="$2"; shift 2;;
    --capacity-blessed) CAPACITY_BLESSED="$2"; shift 2;;
    --image) IMAGE="$2"; shift 2;;
    --image-audit) IMAGEAUDIT=1; shift;;
    --http-port) HTTP_PORT="$2"; shift 2;;
    --drain-budget) DRAIN_BUDGET="$2"; shift 2;;
    --upgrade) UPGRADE=1; shift;;
    --parity) PARITY=1; shift;;
    --parity-factor) PARITY_FACTOR="$2"; shift 2;;
    --cascade) CASCADE=1; PARITY=1; shift;;
    --oversample) OVERSAMPLE=1; PARITY=1; shift;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
# Trace mode replays REAL prod sessions — every statistical-shape knob would
# be silently ignored; make the conflict loud instead.
if [ -n "$TRACE" ]; then
  [ -f "$TRACE" ] || { echo "ERROR: trace file not found: $TRACE" >&2; exit 2; }
  if [ -n "$PROFILE" ] || [ "$ZIPF" != "0" ] || [ "$LIFECYCLE" = "1" ] || \
     [ "$SOAK" = "1" ] || [ "$CONNS_SET" = "1" ] || [ "$WS_SET" = "1" ] || \
     [ "$CHURN_SET" = "1" ] || [ "$MUT_SET" = "1" ] || [ "$DUR_SET" = "1" ]; then
    echo "ERROR: --trace is incompatible with statistical-shape flags" >&2
    echo "  (--profile/--prod-profile/--zipf/--lifecycle/--soak/--connections/" >&2
    echo "   --working-set/--churn-ms/--mutations-per-min/--duration — the" >&2
    echo "   trace itself defines shape, timing and duration)" >&2
    exit 2
  fi
  if [ "$CLEAN" = "0" ]; then
    echo "NOTE: --trace without --clean — accumulated art-% CVR groups skew" >&2
    echo "  trace latencies (the 52ms->26s class); pass --clean for A/B runs" >&2
  fi
fi
# --soak defaults (20 conns / 1h) yield to explicit flags so a smoke sweep can
# shrink the window (leak slopes still need >=15min to be gated, see G6).
if [ "$SOAK" = "1" ]; then
  [ "$CONNS_SET" = "1" ] || CONNS=20
  [ "$DUR_SET" = "1" ] || DURATION=3600
fi

# Derived sandbox names (see xy-repo/xyne-spaces/.sandboxes/<name>/docker-compose.yml)
SLUG="${SANDBOX//-/_}"                       # rust-test -> rust_test
BACKEND="xyne-sandbox-${SANDBOX}-backend"
ZCACHE="xyne-sandbox-${SANDBOX}-zero-cache"
PG="xyne-sandbox-postgres"
DB="sandbox_${SLUG}_db"
CVR_SCHEMA="sandbox_${SLUG}_0/cvr"
TARGET="ws://${SANDBOX}.localhost/zero"
MIRROR_POD="xyne-sandbox-${SANDBOX}-zero-cache-ts"
MIRROR_URL="ws://${SANDBOX}.localhost/zero-ts"
PPROF_FLAGS=()
if [ "$SWAP" = "1" ]; then
  # --swap: TS 1.7 becomes the PRIMARY (replay/negative/sampler/clean target),
  # Go becomes the oracle MIRROR. Validates the reference itself: TS latency
  # profile, TS error semantics under the negative suite, and the symmetric
  # G8 comparison. NB: G5 then compares TS against the Go-blessed baseline —
  # read it as the A/B ratio gate, not an absolute regression.
  ZCACHE="xyne-sandbox-${SANDBOX}-zero-cache-ts"
  CVR_SCHEMA="sandbox_${SLUG}_ts_0/cvr"
  TARGET="ws://${SANDBOX}.localhost/zero-ts"
  MIRROR_POD="xyne-sandbox-${SANDBOX}-zero-cache"
  MIRROR_URL="ws://${SANDBOX}.localhost/zero"
  PPROF_FLAGS=(--pprof '')   # TS pod is Node — no Go pprof endpoint
fi
POOL="harness/id-pool.sandbox.json"
if [ -n "$TRACE" ]; then POOL="harness/id-pool.trace.json"; fi
CSCHEMA="harness/client-schema.json"
PY=".venv/bin/python"; [ -x "$PY" ] || PY="python3"

psql_q() { docker exec "$PG" psql -U xyne -d "$DB" -Atc "$1"; }

# Wait for a (re)started TS mirror to finish replaying its changelog backlog.
# A cold mirror is minutes behind the primary — diffing against it produces
# false G8 mismatches, and its catch-up load pollutes latency numbers.
# Ready when: a recent "replication lag: N ms" line shows N < 5s, or the
# catch-up chatter (subscriber waits / purges) has gone quiet for 30s.
wait_mirror_ready() {  # $1 = container, $2 = timeout seconds (default 180)
  local c="$1" t="${2:-180}" waited=0 lag recent
  while [ "$waited" -lt "$t" ]; do
    lag="$(docker logs --since 60s "$c" 2>&1 | grep -o 'replication lag: [0-9]*' | tail -1 | grep -o '[0-9]*$' || true)"
    if [ -n "$lag" ] && [ "$lag" -lt 5000 ]; then
      echo "  mirror ready (replication lag ${lag}ms)"; return 0
    fi
    if [ "$waited" -ge 30 ]; then
      recent="$(docker logs --since 30s "$c" 2>&1 | grep -c 'waiting for at least\|Purging changes' || true)"
      if [ "${recent:-0}" -eq 0 ] && [ -z "$lag" ]; then
        echo "  mirror settled (no catch-up chatter for 30s)"; return 0
      fi
    fi
    sleep 5; waited=$((waited + 5))
  done
  echo "  WARNING: mirror still catching up after ${t}s — G8 may report false mismatches" >&2
  return 1
}

# --- 0) sandbox up? -----------------------------------------------------------
for c in "$BACKEND" "$ZCACHE" "$PG"; do
  if ! docker ps --format '{{.Names}}' | grep -qx "$c"; then
    echo "ERROR: container $c is not running." >&2
    # zero-cache silently exits whenever the backend is recreated (compose dep)
    echo "  hint: cd <xy-repo>/xyne-spaces/.sandboxes/${SANDBOX} && docker compose up -d ${c##*-}" >&2
    exit 1
  fi
done

# --- 1) JWT secret from the backend container ---------------------------------
SECRET="$(docker exec "$BACKEND" printenv ZERO_AUTH_SECRET)"
[ -n "$SECRET" ] || { echo "ERROR: ZERO_AUTH_SECRET not set in $BACKEND" >&2; exit 1; }

# --- 1b) optional clean slate ---------------------------------------------------
if [ "$CLEAN" = "1" ]; then
  echo "== purging art-% CVR rows (both pods) + restarting primary =="
  psql_q "DELETE FROM \"sandbox_${SLUG}_0/cvr\".instances WHERE \"clientGroupID\" LIKE 'art-%';" >/dev/null 2>&1 || true
  psql_q "DELETE FROM \"sandbox_${SLUG}_ts_0/cvr\".instances WHERE \"clientGroupID\" LIKE 'art-%';" >/dev/null 2>&1 || true
  docker restart "$ZCACHE" >/dev/null
  for _ in $(seq 1 45); do
    ST="$(docker inspect -f '{{.State.Health.Status}}' "$ZCACHE" 2>/dev/null || echo none)"
    [ "$ST" = "healthy" ] && break
    # TS pod defines no healthcheck — "running" is the best signal; then let
    # the replication-drain wait below cover actual readiness.
    if [ "$ST" = "none" ] || [ -z "$ST" ]; then
      docker ps --format '{{.Names}}' | grep -qx "$ZCACHE" && { sleep 3; break; }
    fi
    sleep 2
  done
  if [ "$SWAP" = "1" ]; then wait_mirror_ready "$ZCACHE" 120 || true; fi
  if [ "$ST" != "healthy" ]; then
    # no-healthcheck pods (TS) pass on "running"
    docker ps --format '{{.Names}}' | grep -qx "$ZCACHE" \
      || { echo "ERROR: $ZCACHE not running after restart ($ST)" >&2; exit 1; }
  fi
fi

# --- 2) pick the best-connected user(s) (most channel memberships) ------------
if [ -n "$USER_ID" ]; then
  ROWS="$(psql_q "SELECT u.id, u.email, coalesce(u.name,''), m.\"memberId\", u.\"workspaceId\"
                 FROM users u JOIN org_members m ON m.email = u.email
                 WHERE u.id = '$USER_ID' LIMIT 1;")"
else
  ROWS="$(psql_q "SELECT u.id, u.email, coalesce(u.name,''), m.\"memberId\", u.\"workspaceId\"
                 FROM users u
                 JOIN org_members m ON m.email = u.email
                 LEFT JOIN channel_user_status cus ON cus.\"userId\" = u.id
                 WHERE u.\"workspaceId\" IS NOT NULL
                 GROUP BY u.id, u.email, u.name, m.\"memberId\", u.\"workspaceId\"
                 ORDER BY count(cus.id) DESC LIMIT $USERS;")"
fi
[ -n "$ROWS" ] || { echo "ERROR: no suitable user found in $DB" >&2; exit 1; }

# --- 3) mint JWT(s); build auth pool for multi-user runs -----------------------
AUTH_POOL="harness/auth-pool.json"
ALL_UIDS=""; FIRST_UID=""; N_IDENT=0
printf '[' > "$AUTH_POOL.tmp"
while IFS='|' read -r UID_ EMAIL NAME MEMBER_ID WORKSPACE_ID; do
  [ -n "$UID_" ] || continue
  TOKEN="$("$PY" tools/mint_local_jwt.py --secret "$SECRET" --sub "$UID_" --email "$EMAIL" \
          --name "${NAME:-ART}" --member-id "$MEMBER_ID" --workspace-id "$WORKSPACE_ID" | tail -1)"
  [ "$N_IDENT" -gt 0 ] && printf ',' >> "$AUTH_POOL.tmp"
  "$PY" -c "import json,sys; print(json.dumps({'token': sys.argv[1], 'userID': sys.argv[2]}))" \
      "$TOKEN" "$UID_" >> "$AUTH_POOL.tmp"
  ALL_UIDS="${ALL_UIDS:+$ALL_UIDS,}$UID_"
  [ -z "$FIRST_UID" ] && FIRST_UID="$UID_" && FIRST_EMAIL="$EMAIL" && JWT="$TOKEN"
  N_IDENT=$((N_IDENT+1))
done <<< "$ROWS"
printf ']' >> "$AUTH_POOL.tmp"
mv "$AUTH_POOL.tmp" "$AUTH_POOL"
if [ "$N_IDENT" -gt 1 ]; then
  echo "== identities: $N_IDENT users (first: $FIRST_EMAIL $FIRST_UID) =="
else
  echo "== identity: $FIRST_EMAIL ($FIRST_UID) =="
fi
# Trace mode: overwrite the auth pool with the FULL identity sweep (sandbox
# admin + all bulk users) so prod users map onto distinct real visibilities —
# the 4x run collapsed 278 prod users onto 2 identities. $JWT/$FIRST_UID from
# above still serve the oracle/mutation-matrix steps.
if [ -n "$TRACE" ]; then
  echo "== trace mode: minting full auth pool (build_auth_pool.py) =="
  "$PY" tools/build_auth_pool.py --backend-container "$BACKEND" \
    --pg-container "$PG" --db "$DB"
  N_IDENT="$("$PY" -c "import json;print(len(json.load(open('$AUTH_POOL'))))")"
fi

# --- 3b) source-derived arg schemas + impact matrix (from the backend image) ---
# Both extracted from the DEPLOYED container (same authority rule as the
# clientSchema step below). arg-schemas kill the stale-scalar class — enum
# values are derived, not hand-maintained (viewMode:"kanban" survived three
# backend upgrades as a hand default). The impact matrix powers replay's
# impact-aware mutation targeting (G14) and matrix_oracle's dark-table
# attribution. Best-effort: on failure the pool falls back to hand scalars
# and G14 reads SKIP.
if [ "$REFRESH" = "1" ] || [ ! -f raw/arg-schemas.source.json ]; then
  ./tools/gen_arg_schemas.sh "$BACKEND" \
    || echo "  (arg-schema extraction failed — pool uses hand scalars)" >&2
fi
if [ "$REFRESH" = "1" ] || [ ! -f raw/query-mutator-impact.json ]; then
  ./tools/gen_impact_matrix.sh "$BACKEND" \
    || echo "  (impact matrix extraction failed — G14 will SKIP)" >&2
fi

# --- 4) id-pool from the sandbox DB (user-scoped for mutation participation) ---
# Multi-user: intersect memberships so every identity passes participation checks.
# Trace mode: UNSCOPED harvest into its own pool file — scoping to 101 users'
# intersection would empty the channel pool; trace replay needs the global
# hotness ranking for its rank-to-rank id mapping. Also auto-seeds the tables
# bulk-seed leaves empty (user_groups/canvases; idempotent artseed-% rows) so
# the top trace keys keep mapping after a BULK_WIPE reseed.
# Mutation-matrix (G15) additionally seeds EVERY empty table + the curated
# destructive-target tables (artseed-% rows, identity-linked) and FORCES a
# pool re-harvest: the previous matrix run's destructive phase CONSUMED
# artseed rows, so both the rows and the pool entries must be regenerated
# per run or destructive coverage silently decays back to skips.
if [ "$MUTMATRIX" = "1" ]; then
  "$PY" tools/seed_aux_tables.py --pg-container "$PG" --db "$DB" \
    --groups 250 --canvases 100 | tail -1 | sed 's/^/  aux-seed: /'
  "$PY" tools/seed_all_tables.py --pg-container "$PG" --db "$DB" \
    --identity-user "$ALL_UIDS" | tail -2 | sed 's/^/  all-seed: /'
fi
if [ -n "$TRACE" ]; then
  "$PY" tools/seed_aux_tables.py --pg-container "$PG" --db "$DB" \
    --groups 250 --canvases 100 | sed 's/^/  seed: /'
  if [ "$REFRESH" = "1" ] || [ ! -f "$POOL" ]; then
    echo "== harvesting UNSCOPED trace id-pool from $DB =="
    "$PY" tools/gen_id_pool_db.py --container "$PG" --db "$DB" --out "$POOL"
  fi
elif [ "$REFRESH" = "1" ] || [ ! -f "$POOL" ] || [ "$N_IDENT" -gt 1 ] || [ "$MUTMATRIX" = "1" ]; then
  echo "== harvesting id-pool from $DB (users: $N_IDENT) =="
  "$PY" tools/gen_id_pool_db.py --container "$PG" --db "$DB" --user-id "$ALL_UIDS" --out "$POOL"
fi

# --- 5) clientSchema — authoritative extraction from the backend image ---------
# The wire clientSchema is derived from the SHARED package schema (@xyne/shared,
# what the dashboard bundles) via zero's own clientSchemaFrom(). Extracting it
# from the backend container is authoritative for the DEPLOYED backend commit —
# no "open the app in a browser first" dependency, and it can never go stale
# against the running image. Falls back to the legacy CVR extraction (which
# only knows schemas of clients that already connected) if the exec fails.
if [ "$REFRESH" = "1" ] || [ ! -f "$CSCHEMA" ]; then
  echo "== extracting clientSchema from $BACKEND (@xyne/shared -> clientSchemaFrom) =="
  CS="$(docker exec "$BACKEND" sh -c 'cat > /app/.art-extract-cs.mts << "EOF"
// @ts-nocheck
import {clientSchemaFrom} from "./node_modules/@rocicorp/zero/out/zero-schema/src/builder/schema-builder.js";
import {schema} from "@xyne/shared";
console.log(JSON.stringify(clientSchemaFrom(schema).clientSchema));
EOF
cd /app && npx tsx .art-extract-cs.mts; rc=$?; rm -f /app/.art-extract-cs.mts; exit $rc' 2>/dev/null | tail -1 || true)"
  if [ -n "$CS" ] && printf '%s' "$CS" | "$PY" -c 'import json,sys; d=json.load(sys.stdin); assert "tables" in d and len(d["tables"])>0' 2>/dev/null; then
    printf '%s' "$CS" > "$CSCHEMA"
    echo "  clientSchema: $("$PY" -c "import json;print(len(json.load(open('$CSCHEMA'))['tables']))") tables (from backend image)"
  else
    echo "  backend extraction failed — falling back to CVR \"$CVR_SCHEMA\".instances"
    CS="$(psql_q "SELECT \"clientSchema\" FROM \"$CVR_SCHEMA\".instances
                  WHERE \"clientSchema\" IS NOT NULL ORDER BY \"lastActive\" DESC LIMIT 1;")"
    [ -n "$CS" ] || { echo "ERROR: CVR has no clientSchema — open the sandbox app once in a browser first" >&2; exit 1; }
    printf '%s' "$CS" > "$CSCHEMA"
  fi
fi

# --- 6) drive the replay (resource sampler runs alongside) ---------------------
MUTFLAGS=()
if [ "$MUTATIONS" = "1" ]; then
  MUTFLAGS=(--enable-mutations --i-know-this-writes)
  # under a behavior profile the mutation rate comes from the profile unless
  # the user explicitly set one (explicit CLI wins over profile in replay.py)
  if [ -z "$PROFILE" ] || [ "$MUT_SET" = "1" ]; then
    MUTFLAGS+=(--mutations-per-min "$MUT_RATE")
  fi
fi
LIFEFLAGS=()
if [ "$LIFECYCLE" = "1" ]; then LIFEFLAGS=(--lifecycle); fi
ZIPFFLAGS=()
if [ "$ZIPF" != "0" ]; then ZIPFFLAGS=(--zipf-s "$ZIPF"); fi
# impact-aware mutation targeting (G14) — only meaningful with mutations on
IMPACTFLAGS=()
if [ "$MUTATIONS" = "1" ] && [ -f raw/query-mutator-impact.json ]; then
  IMPACTFLAGS=(--impact raw/query-mutator-impact.json)
fi
AUTHFLAGS=(--auth-token "$JWT" --extra-param "userID=$FIRST_UID")
if [ "$N_IDENT" -gt 1 ]; then AUTHFLAGS=(--auth-pool "$AUTH_POOL"); fi

TAG="$(date +%Y%m%d-%H%M%S)"
SAMPLES="reports/resources-$TAG.ndjson"
# Trace mode: run length is the trace's own span / compression (+ tail), not
# --duration. Compute it so the resource sampler covers the whole run.
if [ -n "$TRACE" ]; then
  DURATION="$("$PY" - "$TRACE" "$TCOMPRESS" <<'PYEOF'
import json, math, sys
path, compress = sys.argv[1], float(sys.argv[2])
with open(path) as f:
    f.readline()
    end = 0
    for ln in f:
        s = json.loads(ln)
        end = max(end, s["offset_ms"] + (s["events"][-1]["dt"] if s["events"] else 0))
print(int(math.ceil(end / 1000.0 / compress)) + 60)
PYEOF
)"
  echo "== trace run window: ${DURATION}s (span/compress + tail) =="
fi
"$PY" tools/resource_sampler.py --container "$ZCACHE" --pg-container "$PG" \
  --db "$DB" --cvr-schema "$CVR_SCHEMA" --out "$SAMPLES" \
  ${PPROF_FLAGS[@]+"${PPROF_FLAGS[@]}"} \
  --interval 10 --duration $((DURATION + 60)) &
SAMPLER_PID=$!
CHAOS_PID=""
CHAOS_REPORT=""
if [ "$CHAOS" = "1" ]; then
  # Inject faults only in the middle of the run: skip the first 20s (let
  # connections open — keeps G1 meaningful) and leave 30s tail for recovery.
  CHAOS_REPORT="reports/chaos-$TAG.json"
  ( sleep 20 && "$PY" tools/chaos.py --zc-container "$ZCACHE" --pg-container "$PG" \
      --duration $((DURATION > 60 ? DURATION - 50 : 30)) --out "$CHAOS_REPORT" ) &
  CHAOS_PID=$!
  echo "== chaos injection armed (pause-zc/pause-pg, report: $CHAOS_REPORT) =="
fi
# NB: the unpause is best-effort cleanup for --chaos; when nothing was paused it
# returns non-zero. Without `|| true` that becomes the script's exit status and
# masks a genuine PASS (gate exit 0) as a failure — the Finding-B false-negative
# class. Keep the real gate verdict authoritative.
trap '[ -n "${SAMPLER_PID:-}" ] && kill "$SAMPLER_PID" 2>/dev/null; [ -n "${CHAOS_PID:-}" ] && kill "$CHAOS_PID" 2>/dev/null; docker unpause "$ZCACHE" "$PG" 2>/dev/null || true' EXIT

PROFILEFLAGS=()
SHAPEFLAGS=(--working-set "$WORKING_SET" --churn-ms "$CHURN_MS")
if [ -n "$PROFILE" ]; then
  if [ ! -f "$PROFILE" ] && [ "$PROFILE" = "profiles/prod-7d.json" ]; then
    echo "== deriving prod profile from art-baseline.json =="
    "$PY" tools/derive_prod_profile.py
  fi
  PROFILEFLAGS=(--profile "$PROFILE")
  # profile provides working_set/churn/lifecycle; only forward what the user
  # explicitly set, so script defaults don't stomp the profile.
  # (if, not `[ ] &&` — a false AND-list at top level trips set -e)
  SHAPEFLAGS=()
  if [ "$WS_SET" = "1" ]; then SHAPEFLAGS+=(--working-set "$WORKING_SET"); fi
  if [ "$CHURN_SET" = "1" ]; then SHAPEFLAGS+=(--churn-ms "$CHURN_MS"); fi
fi

if [ -n "$TRACE" ]; then
  echo "== trace-replaying $TRACE against $TARGET (compress ${TCOMPRESS}x, ~${DURATION}s) =="
else
  echo "== replaying against $TARGET (${CONNS} conns, ${DURATION}s${PROFILE:+, profile=$PROFILE}) =="
fi
# G13 window start: everything the pods log from here until the verdict is
# attributable to this run (30s pre-margin applied by log_gate.py --since).
RUN_START_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
if [ -n "$TRACE" ]; then
  # trace mutation timing comes from the trace itself — no rate flag
  TRACE_MUTFLAGS=()
  if [ "$MUTATIONS" = "1" ]; then
    TRACE_MUTFLAGS=(--enable-mutations --i-know-this-writes)
  fi
  "$PY" harness/trace_replay.py --trace "$TRACE" --target "$TARGET" \
    --auth-pool "$AUTH_POOL" --id-pool "$POOL" --client-schema "$CSCHEMA" \
    --time-compress "$TCOMPRESS" \
    ${TRACE_MUTFLAGS[@]+"${TRACE_MUTFLAGS[@]}"}
else
  "$PY" harness/replay.py \
    --target "$TARGET" --id-pool "$POOL" --client-schema "$CSCHEMA" \
    --connections "$CONNS" \
    --duration "$DURATION" "${AUTHFLAGS[@]}" \
    ${SHAPEFLAGS[@]+"${SHAPEFLAGS[@]}"} ${PROFILEFLAGS[@]+"${PROFILEFLAGS[@]}"} \
    ${LIFEFLAGS[@]+"${LIFEFLAGS[@]}"} ${ZIPFFLAGS[@]+"${ZIPFFLAGS[@]}"} ${MUTFLAGS[@]+"${MUTFLAGS[@]}"} \
    ${IMPACTFLAGS[@]+"${IMPACTFLAGS[@]}"}
fi
set -e

# let the sampler catch the post-run settle (GC behavior), then stop it
sleep 30
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=""
if [ -n "$CHAOS_PID" ]; then
  wait "$CHAOS_PID" 2>/dev/null || true
  CHAOS_PID=""
fi

# --- 6a) pod health check between replay and oracle/negative --------------------
zc_st="$(docker inspect -f '{{.State.Status}}' "$ZCACHE" 2>/dev/null || echo missing)"
if [ "$zc_st" != "running" ]; then
  echo "WARNING: $ZCACHE is '$zc_st' after replay (likely OOM) — restarting for oracle/negative" >&2
  docker start "$ZCACHE" >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    docker ps --format '{{.Names}}' | grep -qx "$ZCACHE" && break
    sleep 2
  done
  sleep 5
fi

# --- 6b) optional differential oracle (gate G8) ---------------------------------
ORACLE_REPORT=""
if [ "$ORACLE" = "1" ]; then
  ZCACHETS="$MIRROR_POD"
  MIRRORFLAGS=()
  # The mirror pod silently exits whenever the backend is recreated (compose
  # dep) — same failure mode as zero-cache itself. A dead mirror must NOT
  # silently degrade to self-diff: with writes on, self-diff compares the pod
  # against itself across mutations and produces guaranteed false mismatches
  # (seen: 386 "mismatches" that were just rows written between passes).
  if ! docker ps --format '{{.Names}}' | grep -qx "$ZCACHETS"; then
    if docker ps -a --format '{{.Names}}' | grep -qx "$ZCACHETS"; then
      echo "NOTE: $ZCACHETS exited (backend recreate?) — restarting for the oracle" >&2
      docker start "$ZCACHETS" >/dev/null 2>&1 || true
      for _ in $(seq 1 15); do
        docker ps --format '{{.Names}}' | grep -qx "$ZCACHETS" && sleep 5 && break
        sleep 2
      done
      wait_mirror_ready "$ZCACHETS" 180 || true
    fi
  fi
  if docker ps --format '{{.Names}}' | grep -qx "$ZCACHETS"; then
    MIRRORFLAGS=(--mirror "$MIRROR_URL")
  elif [ "$MUTATIONS" = "1" ]; then
    echo "NOTE: $ZCACHETS unavailable and mutations are ON — SKIPPING oracle" >&2
    echo "  (self-diff with writes is not a valid oracle; G8 will read SKIP)" >&2
    echo "  hint: cd <xy-repo>/xyne-spaces/.sandboxes/${SANDBOX} && docker compose up -d zero-cache-ts" >&2
    ORACLE=0
  else
    echo "NOTE: $ZCACHETS not running — oracle falls back to self-diff" >&2
    echo "  hint: cd <xy-repo>/xyne-spaces/.sandboxes/${SANDBOX} && docker compose up -d zero-cache-ts" >&2
  fi
fi
if [ "$ORACLE" = "1" ]; then
  ORACLE_MUTFLAGS=()
  if [ "$MUTATIONS" = "1" ]; then
    ORACLE_MUTFLAGS=(--enable-mutations --i-know-this-writes)
  fi
  ORACLE_REPORT="reports/diff-$TAG.json"
  echo "== differential oracle (G8) =="
  set +e
  "$PY" harness/diff_oracle.py --primary "$TARGET" \
    ${MIRRORFLAGS[@]+"${MIRRORFLAGS[@]}"} \
    --id-pool "$POOL" --client-schema "$CSCHEMA" \
    --auth-token "$JWT" --extra-param "userID=$FIRST_UID" \
    --pairs 3 --duration 45 --quiesce-s 20 \
    ${ZIPFFLAGS[@]+"${ZIPFFLAGS[@]}"} ${ORACLE_MUTFLAGS[@]+"${ORACLE_MUTFLAGS[@]}"} \
    --out "$ORACLE_REPORT"
  set -e
fi

# --- 6c) optional negative suite (gate G11) --------------------------------------
# Runs AFTER the replay so its adversarial connections can't skew G1/G2/G5
# counters, and sequentially (scenarios must not interfere with each other).
# READ-MOSTLY: desired queries only; ttl-purge flips one art-% CVR row it made.
NEGATIVE_REPORT=""
if [ "$NEGATIVE" = "1" ]; then
  NEGATIVE_REPORT="reports/negative-$TAG.json"
  echo "== negative suite (G11) =="
  set +e
  "$PY" harness/negative.py --target "$TARGET" \
    --id-pool "$POOL" --client-schema "$CSCHEMA" \
    --auth-pool "$AUTH_POOL" \
    --pg-container "$PG" --pg-user xyne --pg-db "$DB" --cvr-schema "$CVR_SCHEMA" \
    --out "$NEGATIVE_REPORT"
  set -e
  if [ "$N_IDENT" -lt 2 ]; then
    echo "NOTE: wrong-user-pinned-group needs 2 identities — rerun with --users 2" >&2
  fi
fi

# --- 6c2) optional mutation matrix (gate G15) -------------------------------------
# Push-path mutator TYPE coverage: every synthesizable mutator fired through
# the real client push path, wave-converged Go-vs-TS (harness/mutation_matrix.py).
# Runs AFTER negative (its pushes must not skew G11's forged-state scenarios)
# and needs the mirror pod for the diff (same requirement as the oracle).
MUTMATRIX_REPORT=""
if [ "$MUTMATRIX" = "1" ]; then
  if ! docker ps --format '{{.Names}}' | grep -qx "$MIRROR_POD"; then
    echo "NOTE: $MIRROR_POD not running — SKIPPING mutation matrix (G15 reads SKIP)" >&2
  else
    MUTMATRIX_REPORT="reports/mutmatrix-$TAG.json"
    echo "== mutation matrix (G15) =="
    set +e
    "$PY" harness/mutation_matrix.py \
      --primary "$TARGET" --mirror "$MIRROR_URL" \
      --auth-token "$JWT" --extra-param "userID=$FIRST_UID" \
      --id-pool "$POOL" --client-schema "$CSCHEMA" \
      --pg-container "$PG" --pg-user xyne --pg-db "$DB" \
      --i-know-this-writes \
      --out "$MUTMATRIX_REPORT"
    set -e
  fi
fi

# --- 6d) server-log health scan (gate G13) ---------------------------------------
# Adopted from staging-regression (feature/art): client-side gates can't see a
# sidecar crash + silent fallback-to-TS — which would turn the G8 oracle into a
# TS-vs-TS no-op and invalidate every A/B latency number. Scan the primary (and
# the mirror, when it's up — its advance-resets corrupt G8 too) for blocking
# patterns over exactly this run's window. Always on: it's a 2s docker-logs grep.
LOG_REPORT="reports/logs-$TAG.json"
LOG_CONTAINERS="$ZCACHE"
if docker ps --format '{{.Names}}' | grep -qx "$MIRROR_POD"; then
  LOG_CONTAINERS="$ZCACHE,$MIRROR_POD"
fi
set +e
"$PY" tools/log_gate.py --containers "$LOG_CONTAINERS" \
  --since "$RUN_START_ISO" --out "$LOG_REPORT"
set -e

# --- 6e) image/lifecycle probes (G16-G24) ---------------------------------------
# Read-only probes run first; cold-start RESTARTS zero-cache; drain SIGTERMs
# it (pod dies) so it runs LAST. capacity drives a multi-rung replay sweep.
PROTOCOL_REPORT=""; TELEMETRY_REPORT=""; READINESS_REPORT=""; DETERMINISM_REPORT=""
CAPACITY_REPORT=""; IMAGEAUDIT_REPORT=""; UPGRADE_REPORT=""; PARITY_REPORT=""; COLDSTART_REPORT=""; DRAIN_REPORT=""
AUTHFLAGS=(--auth-token "$JWT" --extra-param "userID=$FIRST_UID")

if [ "$PROTOCOL" = "1" ]; then
  PROTOCOL_REPORT="reports/protocol-$TAG.json"
  echo "== protocol-version probe (G16) =="
  set +e; "$PY" tools/probe_protocol.py --target "$TARGET" "${AUTHFLAGS[@]}" --out "$PROTOCOL_REPORT"; set -e
fi
if [ "$TELEMETRY" = "1" ]; then
  TELEMETRY_REPORT="reports/telemetry-$TAG.json"
  echo "== telemetry-contract test (G17) =="
  set +e; "$PY" tools/telemetry_contract.py --container "$ZCACHE" --since "${DURATION}s" --baseline art-baseline.json --out "$TELEMETRY_REPORT"; set -e
fi
if [ "$READINESS" = "1" ]; then
  READINESS_REPORT="reports/readiness-$TAG.json"
  : "${HTTP_PORT:=8080}"
  echo "== readiness/liveness contract (G19) =="
  set +e; "$PY" tools/probe_readiness.py --http "http://${SANDBOX}.localhost:${HTTP_PORT}" --ws-target "$TARGET" "${AUTHFLAGS[@]}" --out "$READINESS_REPORT"; set -e
fi
if [ "$DETERMINISM" = "1" ]; then
  DETERMINISM_REPORT="reports/determinism-$TAG.json"
  echo "== determinism oracle (G21) =="
  set +e; "$PY" tools/determinism_oracle.py --target "$TARGET" "${AUTHFLAGS[@]}" --id-pool "$POOL" --client-schema "$CSCHEMA" --out "$DETERMINISM_REPORT"; set -e
fi
if [ "$UPGRADE" = "1" ]; then
  UPGRADE_REPORT="reports/upgrade-$TAG.json"
  echo "== upgrade-path test (G24) =="
  if docker ps --format '{{.Names}}' | grep -qx "$MIRROR_POD"; then
    set +e; "$PY" tools/upgrade_path.py --baseline-target "$MIRROR_URL" --candidate-target "$TARGET" "${AUTHFLAGS[@]}" --id-pool "$POOL" --client-schema "$CSCHEMA" --out "$UPGRADE_REPORT"; set -e
  else
    echo "NOTE: $MIRROR_POD not running — G24 needs a second image target (start zero-cache-ts)" >&2
  fi
fi
if [ "$IMAGEAUDIT" = "1" ]; then
  IMAGEAUDIT_REPORT="reports/image-$TAG.json"
  if [ -z "$IMAGE" ]; then
    IMAGE="$(docker inspect -f '{{.Config.Image}}' "$ZCACHE" 2>/dev/null || true)"
  fi
  if [ -n "$IMAGE" ]; then
    echo "== image supply-chain audit (G23): $IMAGE =="
    set +e; "$PY" tools/image_audit.py --image "$IMAGE" --out "$IMAGEAUDIT_REPORT"; set -e
  else
    echo "NOTE: could not determine image ref for $ZCACHE — pass --image" >&2
  fi
fi
if [ "$CAPACITY" = "1" ]; then
  CAPACITY_REPORT="reports/capacity-$TAG.json"
  echo "== capacity-cliff sweep (G22): ladder $CAPACITY_LADDER =="
  set +e; "$PY" tools/capacity_gate.py --drive --target "$TARGET" "${AUTHFLAGS[@]}" --id-pool "$POOL" --ladder "$CAPACITY_LADDER" --blessed-conns "$CAPACITY_BLESSED" --out "$CAPACITY_REPORT"; set -e
fi
if [ "$PARITY" = "1" ]; then
  PARITY_REPORT="reports/parity-$TAG.json"
  echo "== latency-parity gate (G25): Go vs TS =="
  if docker ps --format '{{.Names}}' | grep -qx "$MIRROR_POD"; then
    PARITY_FLAGS=(--drive --primary-target "$TARGET" --mirror-target "$MIRROR_URL")
    [ "$CASCADE" = "1" ] && PARITY_FLAGS+=(--cascade)
    [ "$OVERSAMPLE" = "1" ] && PARITY_FLAGS+=(--oversample)
    set +e; "$PY" tools/parity_gate.py "${PARITY_FLAGS[@]}" \
      "${AUTHFLAGS[@]}" --id-pool "$POOL" --client-schema "$CSCHEMA" \
      --factor "$PARITY_FACTOR" --out "$PARITY_REPORT"; set -e
  else
    echo "NOTE: $MIRROR_POD not running — G25 needs the TS reference (start zero-cache-ts)" >&2
  fi
fi
if [ "$COLDSTART" = "1" ]; then
  COLDSTART_REPORT="reports/coldstart-$TAG.json"
  echo "== cold-start timing (G18) — RESTARTS $ZCACHE =="
  set +e; "$PY" tools/cold_start.py --target "$TARGET" --container "$ZCACHE" "${AUTHFLAGS[@]}" --id-pool "$POOL" --client-schema "$CSCHEMA" --out "$COLDSTART_REPORT"; set -e
  sleep 5  # let zero-cache come back up for the drain probe / verdict
fi
if [ "$DRAIN" = "1" ]; then
  DRAIN_REPORT="reports/drain-$TAG.json"
  echo "== SIGTERM drain test (G20) — KILLS $ZCACHE =="
  set +e; "$PY" tools/drain_test.py --target "$TARGET" --container "$ZCACHE" "${AUTHFLAGS[@]}" --id-pool "$POOL" --client-schema "$CSCHEMA" --drain-budget-s "$DRAIN_BUDGET" --out "$DRAIN_REPORT"; set -e
fi

# --- 7) the verdict --------------------------------------------------------------
echo ""
set +e
"$PY" tools/local_gate.py --resources "reports/resources-$TAG.summary.json" \
  --out "reports/gate-$TAG.json" \
  --logs "$LOG_REPORT" \
  ${ORACLE_REPORT:+--oracle "$ORACLE_REPORT"} \
  ${CHAOS_REPORT:+--chaos "$CHAOS_REPORT"} \
  ${NEGATIVE_REPORT:+--negative "$NEGATIVE_REPORT"} \
  ${MUTMATRIX_REPORT:+--mut-matrix "$MUTMATRIX_REPORT"}
  ${PROTOCOL_REPORT:+--protocol "$PROTOCOL_REPORT"} \
  ${TELEMETRY_REPORT:+--telemetry "$TELEMETRY_REPORT"} \
  ${COLDSTART_REPORT:+--coldstart "$COLDSTART_REPORT"} \
  ${READINESS_REPORT:+--readiness "$READINESS_REPORT"} \
  ${DRAIN_REPORT:+--drain "$DRAIN_REPORT"} \
  ${DETERMINISM_REPORT:+--determinism "$DETERMINISM_REPORT"} \
  ${CAPACITY_REPORT:+--capacity "$CAPACITY_REPORT"} \
  ${IMAGEAUDIT_REPORT:+--image-audit "$IMAGEAUDIT_REPORT"} \
  ${UPGRADE_REPORT:+--upgrade "$UPGRADE_REPORT"}
  ${PARITY_REPORT:+--parity "$PARITY_REPORT"}
GATE=$?
set -e
echo ""
# best-effort heap growth report (needs `go` on PATH; never affects the verdict)
if [ -f "reports/resources-$TAG.heap-first.pb.gz" ] && [ -f "reports/resources-$TAG.heap-last.pb.gz" ]; then
  "$PY" tools/heap_diff.py --tag "$TAG" 2>&1 | sed 's/^/  /' || true
  echo ""
fi
echo "heap snapshots for go tool pprof -diff_base: reports/resources-$TAG.heap-{first,last}.pb.gz"
exit "$GATE"
