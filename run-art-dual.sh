#!/usr/bin/env bash
# run-art-dual.sh — SIMULTANEOUS trace A/B: the same prod trace replayed
# against the Go primary AND the TS mirror in the same wall-clock window,
# by two independent trace_replay processes.
#
# Why simultaneous (complements run-art-local.sh's sequential A/B):
#   * identical window -> identical host/postgres/background conditions;
#     time-of-day drift between sides disappears entirely
#   * the pods share host CPU + postgres, so contention is shared: read the
#     RATIOS (lesson 8 — A/B ratios on same rig beat absolutes). Absolutes
#     are inflated vs solo runs; a G5 FAIL against a solo-blessed baseline
#     here means "contention", not "regression".
#
#   ./run-art-dual.sh --trace raw/traces/trace-last10m.ndjson
#   ./run-art-dual.sh --trace T --time-compress 2
#
# Both pods get: art-% CVR purge + restart + readiness/drain wait. Then:
# full auth pool, aux seed, unscoped trace pool, two samplers, two replays,
# per-side G13 + gate, and a head-to-head table.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

SANDBOX="rust-test"; TRACE=""; TCOMPRESS=1
while [ $# -gt 0 ]; do
  case "$1" in
    --sandbox) SANDBOX="$2"; shift 2;;
    --trace) TRACE="$2"; shift 2;;
    --time-compress) TCOMPRESS="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
[ -n "$TRACE" ] && [ -f "$TRACE" ] || { echo "ERROR: --trace <file> required" >&2; exit 2; }

SLUG="${SANDBOX//-/_}"
BACKEND="xyne-sandbox-${SANDBOX}-backend"
GO_POD="xyne-sandbox-${SANDBOX}-zero-cache"
TS_POD="xyne-sandbox-${SANDBOX}-zero-cache-ts"
PG="xyne-sandbox-postgres"
DB="sandbox_${SLUG}_db"
GO_URL="ws://${SANDBOX}.localhost/zero"
TS_URL="ws://${SANDBOX}.localhost/zero-ts"
POOL="harness/id-pool.trace.json"
CSCHEMA="harness/client-schema.json"
AUTH_POOL="harness/auth-pool.json"
PY=".venv/bin/python"; [ -x "$PY" ] || PY="python3"
psql_q() { docker exec "$PG" psql -U xyne -d "$DB" -Atc "$1"; }

for c in "$BACKEND" "$GO_POD" "$TS_POD" "$PG"; do
  docker ps --format '{{.Names}}' | grep -qx "$c" \
    || { echo "ERROR: container $c is not running" >&2; exit 1; }
done

# --- clean slate, BOTH pods (dual runs are always A/B — always clean) --------
echo "== purging art-% CVR rows (both schemas) + restarting both pods =="
psql_q "DELETE FROM \"sandbox_${SLUG}_0/cvr\".instances WHERE \"clientGroupID\" LIKE 'art-%';" >/dev/null 2>&1 || true
psql_q "DELETE FROM \"sandbox_${SLUG}_ts_0/cvr\".instances WHERE \"clientGroupID\" LIKE 'art-%';" >/dev/null 2>&1 || true
docker restart "$GO_POD" "$TS_POD" >/dev/null
for _ in $(seq 1 45); do
  ST="$(docker inspect -f '{{.State.Health.Status}}' "$GO_POD" 2>/dev/null || echo none)"
  [ "$ST" = "healthy" ] && break
  sleep 2
done
[ "$ST" = "healthy" ] || echo "WARNING: $GO_POD not healthy yet ($ST)" >&2
# TS pod has no healthcheck: wait for replication drain (cold pod = minutes
# behind; its catch-up would poison the first minutes of the A/B)
waited=0
while [ "$waited" -lt 150 ]; do
  lag="$(docker logs --since 60s "$TS_POD" 2>&1 | grep -o 'replication lag: [0-9]*' | tail -1 | grep -o '[0-9]*$' || true)"
  if [ -n "$lag" ] && [ "$lag" -lt 5000 ]; then echo "  TS drained (lag ${lag}ms)"; break; fi
  if [ "$waited" -ge 30 ] && [ -z "$lag" ] && \
     [ "$(docker logs --since 30s "$TS_POD" 2>&1 | grep -c 'waiting for at least\|Purging changes' || true)" -eq 0 ]; then
    echo "  TS settled (no catch-up chatter for 30s)"; break
  fi
  sleep 5; waited=$((waited + 5))
done

# --- identities / seeds / pool (same steps as --trace in run-art-local.sh) ---
"$PY" tools/build_auth_pool.py --backend-container "$BACKEND" \
  --pg-container "$PG" --db "$DB"
"$PY" tools/seed_aux_tables.py --pg-container "$PG" --db "$DB" \
  --groups 250 --canvases 100 | sed 's/^/  seed: /'
if [ ! -f "$POOL" ]; then
  "$PY" tools/gen_id_pool_db.py --container "$PG" --db "$DB" --out "$POOL"
fi
[ -f "$CSCHEMA" ] || { echo "ERROR: $CSCHEMA missing — run run-art-local.sh once first" >&2; exit 1; }

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
TAG="$(date +%Y%m%d-%H%M%S)"
echo "== dual trace A/B: $TRACE @ ${TCOMPRESS}x, ~${DURATION}s, tag $TAG =="

# --- samplers (Go gets pprof; TS is Node — no Go pprof endpoint) --------------
"$PY" tools/resource_sampler.py --container "$GO_POD" --pg-container "$PG" \
  --db "$DB" --cvr-schema "sandbox_${SLUG}_0/cvr" \
  --out "reports/resources-$TAG-go.ndjson" --interval 10 --duration $((DURATION + 60)) &
SAMPLER_GO=$!
"$PY" tools/resource_sampler.py --container "$TS_POD" --pg-container "$PG" \
  --db "$DB" --cvr-schema "sandbox_${SLUG}_ts_0/cvr" --pprof '' \
  --out "reports/resources-$TAG-ts.ndjson" --interval 10 --duration $((DURATION + 60)) &
SAMPLER_TS=$!
trap 'kill "$SAMPLER_GO" "$SAMPLER_TS" 2>/dev/null || true' EXIT

RUN_START_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
"$PY" harness/trace_replay.py --trace "$TRACE" --target "$GO_URL" \
  --auth-pool "$AUTH_POOL" --id-pool "$POOL" --client-schema "$CSCHEMA" \
  --time-compress "$TCOMPRESS" --run-tag "$TAG-go" \
  > "reports/dual-$TAG-go.log" 2>&1 &
REPLAY_GO=$!
"$PY" harness/trace_replay.py --trace "$TRACE" --target "$TS_URL" \
  --auth-pool "$AUTH_POOL" --id-pool "$POOL" --client-schema "$CSCHEMA" \
  --time-compress "$TCOMPRESS" --run-tag "$TAG-ts" \
  > "reports/dual-$TAG-ts.log" 2>&1 &
REPLAY_TS=$!
echo "== both replays launched (logs: reports/dual-$TAG-{go,ts}.log) =="
wait "$REPLAY_GO"; RC_GO=$?
wait "$REPLAY_TS"; RC_TS=$?
set -e
echo "replay exit: go=$RC_GO ts=$RC_TS"
sleep 30                                  # let samplers catch the settle
kill "$SAMPLER_GO" "$SAMPLER_TS" 2>/dev/null || true
wait "$SAMPLER_GO" "$SAMPLER_TS" 2>/dev/null || true
trap - EXIT

# --- per-side G13 + gate -------------------------------------------------------
set +e
"$PY" tools/log_gate.py --containers "$GO_POD" --since "$RUN_START_ISO" \
  --out "reports/logs-$TAG-go.json"
"$PY" tools/log_gate.py --containers "$TS_POD" --since "$RUN_START_ISO" \
  --out "reports/logs-$TAG-ts.json"
echo ""
echo "===== Go gate ====="
"$PY" tools/local_gate.py --run "reports/run-$TAG-go.json" \
  --resources "reports/resources-$TAG-go.summary.json" \
  --logs "reports/logs-$TAG-go.json" --out "reports/gate-$TAG-go.json"
echo ""
echo "===== TS gate ====="
"$PY" tools/local_gate.py --run "reports/run-$TAG-ts.json" \
  --resources "reports/resources-$TAG-ts.summary.json" \
  --logs "reports/logs-$TAG-ts.json" --out "reports/gate-$TAG-ts.json"
set -e

# --- head-to-head ---------------------------------------------------------------
echo ""
"$PY" - "$TAG" <<'PYEOF'
import json, sys
tag = sys.argv[1]
go = json.load(open(f"reports/run-{tag}-go.json"))
ts = json.load(open(f"reports/run-{tag}-ts.json"))
def logrep(side):
    try:
        d = json.load(open(f"reports/logs-{tag}-{side}.json"))
        c = next(iter(d["containers"].values()))
        heal = sum(h["count"] for h in c.get("self_heal_hits", {}).values())
        block = sum(h["count"] for h in c.get("blocking_hits", {}).values())
        return heal, block
    except Exception:
        return "-", "-"
gh, gb = logrep("go"); th, tb = logrep("ts")
print(f"== head-to-head (same wall-clock window; read RATIOS, not absolutes) ==")
print(f"{'metric':26} {'Go':>10} {'TS':>10} {'Go/TS':>7}")
for label, key, sub in [("steady p50", "client_latency_steady_ms", "p50"),
                        ("steady p95", "client_latency_steady_ms", "p95"),
                        ("steady p99", "client_latency_steady_ms", "p99"),
                        ("initial p50", "client_latency_initial_ms", "p50"),
                        ("initial p95", "client_latency_initial_ms", "p95"),
                        ("sched-lag p95", "scheduling_lag_ms", "p95")]:
    a, b = go[key].get(sub), ts[key].get(sub)
    r = f"{a/b:.2f}" if a and b else "-"
    print(f"{label:26} {a!s:>10} {b!s:>10} {r:>7}")
for label, key in [("errors", "errors"), ("failed_open", "failed_open"),
                   ("pokes", "pokes"), ("dedup_puts", "dedup_puts")]:
    print(f"{label:26} {go['counters'][key]!s:>10} {ts['counters'][key]!s:>10}")
print(f"{'self-heal / blocking':26} {f'{gh}/{gb}':>10} {f'{th}/{tb}':>10}")
gq, tq = go.get("latency_by_query", {}), ts.get("latency_by_query", {})
common = [q for q in gq if q in tq
          and gq[q]["samples"] >= 10 and tq[q]["samples"] >= 10]
worst = sorted(common, key=lambda q: -(gq[q]["p50"] / max(tq[q]["p50"], .1)))[:6]
print(f"\nworst Go/TS per-query p50 ratios (n>=10 both):")
for q in worst:
    print(f"  {q:38} {gq[q]['p50']:>8} vs {tq[q]['p50']:>8}  "
          f"{gq[q]['p50']/max(tq[q]['p50'],.1):.2f}x")
PYEOF
