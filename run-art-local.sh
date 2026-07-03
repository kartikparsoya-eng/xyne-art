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
MUTATIONS=0; MUT_RATE=10; REFRESH=0; USER_ID=""; USERS=1; LIFECYCLE=0; SOAK=0; CLEAN=0; ZIPF=0; ORACLE=0; CHAOS=0; NEGATIVE=0
PROFILE=""; WS_SET=0; CHURN_SET=0; MUT_SET=0
CONNS_SET=0; DUR_SET=0
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
    --profile) PROFILE="$2"; shift 2;;
    --user-id) USER_ID="$2"; shift 2;;
    --users) USERS="$2"; shift 2;;
    --lifecycle) LIFECYCLE=1; shift;;
    --soak) SOAK=1; LIFECYCLE=1; shift;;
    --clean) CLEAN=1; shift;;
    --zipf) ZIPF="${2:-1.1}"; shift 2;;
    --oracle) ORACLE=1; shift;;
    --chaos) CHAOS=1; LIFECYCLE=1; shift;;
    --negative) NEGATIVE=1; shift;;
    --refresh) REFRESH=1; shift;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
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
POOL="harness/id-pool.sandbox.json"
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
  echo "== purging art-% CVR rows + restarting zero-cache =="
  psql_q "DELETE FROM \"$CVR_SCHEMA\".instances WHERE \"clientGroupID\" LIKE 'art-%';" >/dev/null
  docker restart "$ZCACHE" >/dev/null
  for _ in $(seq 1 45); do
    ST="$(docker inspect -f '{{.State.Health.Status}}' "$ZCACHE" 2>/dev/null || echo none)"
    [ "$ST" = "healthy" ] && break
    sleep 2
  done
  [ "$ST" = "healthy" ] || { echo "ERROR: $ZCACHE not healthy after restart ($ST)" >&2; exit 1; }
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

# --- 4) id-pool from the sandbox DB (user-scoped for mutation participation) ---
# Multi-user: intersect memberships so every identity passes participation checks.
if [ "$REFRESH" = "1" ] || [ ! -f "$POOL" ] || [ "$N_IDENT" -gt 1 ]; then
  echo "== harvesting id-pool from $DB (users: $N_IDENT) =="
  "$PY" tools/gen_id_pool_db.py --container "$PG" --db "$DB" --user-id "$ALL_UIDS" --out "$POOL"
fi

# --- 5) clientSchema from the CVR (any previously-connected client group) ------
if [ "$REFRESH" = "1" ] || [ ! -f "$CSCHEMA" ]; then
  echo "== extracting clientSchema from \"$CVR_SCHEMA\".instances =="
  CS="$(psql_q "SELECT \"clientSchema\" FROM \"$CVR_SCHEMA\".instances
                WHERE \"clientSchema\" IS NOT NULL ORDER BY \"lastActive\" DESC LIMIT 1;")"
  [ -n "$CS" ] || { echo "ERROR: CVR has no clientSchema — open the sandbox app once in a browser first" >&2; exit 1; }
  printf '%s' "$CS" > "$CSCHEMA"
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
AUTHFLAGS=(--auth-token "$JWT" --extra-param "userID=$FIRST_UID")
if [ "$N_IDENT" -gt 1 ]; then AUTHFLAGS=(--auth-pool "$AUTH_POOL"); fi

TAG="$(date +%Y%m%d-%H%M%S)"
SAMPLES="reports/resources-$TAG.ndjson"
"$PY" tools/resource_sampler.py --container "$ZCACHE" --pg-container "$PG" \
  --db "$DB" --cvr-schema "$CVR_SCHEMA" --out "$SAMPLES" \
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

echo "== replaying against $TARGET (${CONNS} conns, ${DURATION}s${PROFILE:+, profile=$PROFILE}) =="
set +e
"$PY" harness/replay.py \
  --target "$TARGET" --id-pool "$POOL" --client-schema "$CSCHEMA" \
  --connections "$CONNS" \
  --duration "$DURATION" "${AUTHFLAGS[@]}" \
  ${SHAPEFLAGS[@]+"${SHAPEFLAGS[@]}"} ${PROFILEFLAGS[@]+"${PROFILEFLAGS[@]}"} \
  ${LIFEFLAGS[@]+"${LIFEFLAGS[@]}"} ${ZIPFFLAGS[@]+"${ZIPFFLAGS[@]}"} ${MUTFLAGS[@]+"${MUTFLAGS[@]}"}
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
  ZCACHETS="xyne-sandbox-${SANDBOX}-zero-cache-ts"
  MIRRORFLAGS=()
  # The TS mirror silently exits whenever the backend is recreated (compose
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
    MIRRORFLAGS=(--mirror "ws://${SANDBOX}.localhost/zero-ts")
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

# --- 7) the verdict --------------------------------------------------------------
echo ""
set +e
"$PY" tools/local_gate.py --resources "reports/resources-$TAG.summary.json" \
  --out "reports/gate-$TAG.json" \
  ${ORACLE_REPORT:+--oracle "$ORACLE_REPORT"} \
  ${CHAOS_REPORT:+--chaos "$CHAOS_REPORT"} \
  ${NEGATIVE_REPORT:+--negative "$NEGATIVE_REPORT"}
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
