#!/usr/bin/env bash
# run-art-prodcal.sh — PROD-CALIBRATED experiment matrix (2026-07-09).
#
# Transfer model: prod pod = 16 cores; local zc pinned to cpus:8 => S = 0.5.
#   INTENSIVE (never scale): per-client cadence (profiles/prod-7d.json),
#     working-set 15, replication evt/s (every prod pod replicates the whole
#     stream), GOGC/P/pullWindow.
#   EXTENSIVE (scale by S): CG count, expected busy-cores, memory.
#   Transfers: Go-vs-TS ratios, reset/error counts, leak slopes, per-CG heap
#     (x350 -> prod), saturation points. Does NOT transfer: absolute
#     latencies (sandbox DB is tiny vs the 32GiB prod replica).
#
# Prod anchors (measured): R~350 CGs/pod peak, A~11 busy-cores, 40 evt/s
# steady / 206 bulk replication, 425 resets/h chronic, hydration p99 5-7s.
#
#   ./run-art-prodcal.sh e0   # baseline bless: E1 shape Go + TS(--swap), bless Go
#   ./run-art-prodcal.sh e1   # weekday steady: 125c(=250xS) 5 users, writer@40
#   ./run-art-prodcal.sh e2   # peak: 175c(=350xS), writer@40
#   ./run-art-prodcal.sh e3   # bulk day (Jul 4): E2 shape, writer@200
#   ./run-art-prodcal.sh e4   # real concurrency: 1x prod trace (release gate)
#   ./run-art-prodcal.sh e5   # topology proof: E1 at W=1 then W=6
#   ./run-art-prodcal.sh e5w3 # E5 resume: W=3 leg only + ladder (W=6 leg reused)
#   ./run-art-prodcal.sh e6   # leak/lifecycle: 1h soak 60c, writer@40
#
# Prereqs: compose already carries the candidate topology (W=1 P=4 GOGC=400
# GOMAXPROCS=8 cpus:8); docker VM at 18GiB (zc 8g + ts 4g + pg + backend
# don't fit in 12); kubeflow paused automatically here.
# Deduce prod claims after any run:  tools/deduce_prod.py --run <run.json> ...
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"; cd "$DIR"
PY=".venv/bin/python"; [ -x "$PY" ] || PY="python3"
COMPOSE_DIR="/Users/kartik.parsoya/Documents/xy-repo/xyne-spaces/.sandboxes/rust-test"

EXP="${1:-}"; shift || true
[ -n "$EXP" ] || { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

WRITER_PID=""
KUBEFLOW_PAUSED=0
cleanup() {
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null || true
  [ "$KUBEFLOW_PAUSED" = "1" ] && docker unpause kubeflow-control-plane 2>/dev/null || true
}
trap cleanup EXIT

pause_kubeflow() {
  if docker ps --format '{{.Names}}' | grep -qx kubeflow-control-plane; then
    docker pause kubeflow-control-plane && KUBEFLOW_PAUSED=1
    echo "== kubeflow paused =="
  fi
}

start_writer() { # $1=rate
  echo "== bg writer @$1 evt/s (replication axis — intensive, unscaled) =="
  "$PY" tools/bg_writer.py --rate "$1" --i-know-this-writes \
    > "reports/bgwriter-$(date +%H%M%S).log" 2>&1 &
  WRITER_PID=$!
  sleep 3
  kill -0 "$WRITER_PID" 2>/dev/null || { echo "ERROR: writer died at start" >&2; exit 1; }
}

stop_writer() {
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null && wait "$WRITER_PID" 2>/dev/null || true
  WRITER_PID=""
}

## NOTE (2026-07-09, post-E5): the candidate topology is W=4 (E5 knee).
## All steady-state experiments below guard on verify_topology 4 — the pod
## must already be on the candidate (plain `docker compose up` now gives W=4).
## The historical W=1 data lives in run-20260709-130055.json (E4 / ladder col).
verify_topology() { # $1=expected W
  W="$(docker exec xyne-sandbox-rust-test-zero-cache printenv ZERO_NUM_SYNC_WORKERS 2>/dev/null || echo '?')"
  P="$(docker exec xyne-sandbox-rust-test-zero-cache printenv GO_IVM_PARALLELISM 2>/dev/null || echo '?')"
  M="$(docker exec xyne-sandbox-rust-test-zero-cache printenv GOMAXPROCS 2>/dev/null || echo '?')"
  echo "== topology: W=$W P=$P GOMAXPROCS=$M (want W=$1) =="
  [ "$W" = "$1" ] || { echo "ERROR: pod runs W=$W, want W=$1 — recreate:" >&2
    echo "  (cd $COMPOSE_DIR && ZC_WORKERS=$1 docker compose up -d --force-recreate zero-cache)" >&2
    exit 1; }
}

set_topology() { # $1=W
  echo "== recreating zero-cache with W=$1 =="
  # CRITICAL (found 2026-07-09, invalidated the first W=6 leg): least-loaded
  # routing persists CG->worker assignments in /var/zero/syncer-assignments.json
  # NEXT TO THE REPLICA — it survives recreates. Accumulated stale entries
  # skew the load counts so ALL fresh CGs land on worker 0 (verified: 238/238
  # CGs on workerIndex=0 while 5 workers idled — a W=6 pod running as W=1).
  # Wipe it on every topology change so each arm starts with fair routing.
  docker exec xyne-sandbox-rust-test-zero-cache rm -f /var/zero/syncer-assignments.json 2>/dev/null || true
  (cd "$COMPOSE_DIR" && ZC_WORKERS="$1" docker compose up -d --force-recreate zero-cache >/dev/null 2>&1)
  # readiness + replication settle
  for _ in $(seq 1 45); do
    ST="$(docker inspect -f '{{.State.Health.Status}}' xyne-sandbox-rust-test-zero-cache 2>/dev/null || echo none)"
    [ "$ST" = "healthy" ] && break; sleep 2
  done
  sleep 20
}

# routing sanity: fail fast if CGs are not spreading across workers.
# LESSON (2026-07-09, killed E5 after the W=6 leg): the old pattern
# 'workerIndex=N,component=view-syncer' only matches sparse "Slow SQLite
# query" warnings — 0 matches in a quiet 3m tail => grep rc=1 => set -e +
# pipefail silently aborted the script BEFORE the echo. Use the dense
# syncer-worker prefixes (both formats: "worker=syncer,workerIndex=N" and
# "'worker=syncer', 'workerIndex=N'"), a 15m window, and never let an
# empty grep kill the run.
check_spread() {
  SPREAD="$(docker logs --since 15m xyne-sandbox-rust-test-zero-cache 2>&1 \
    | grep -oE "worker=syncer.{0,4}workerIndex=[0-9]+" \
    | grep -oE 'workerIndex=[0-9]+' | sort -u | wc -l | tr -d ' ' || true)"
  echo "== routing spread: syncer traffic on ${SPREAD:-0} distinct worker(s) (last 15m) =="
}

print_ladder() { # $1=W1_run $2=W3_run $3=W6_run
  echo ""; echo "== topology ladder: W=1 (E4) vs W=3 vs W=6, same build/trace =="
  "$PY" - "$1" "$2" "$3" <<'PYEOF'
import json, sys
runs = [("W=1 (E4)", sys.argv[1]), ("W=3", sys.argv[2]), ("W=6", sys.argv[3])]
data = [(n, json.load(open(p))) for n, p in runs]
print(f"{'metric':16}" + "".join(f"{n:>14}" for n, _ in data))
for label, sec, key in [
    ("steady p50", "client_latency_steady_ms", "p50"),
    ("steady p95", "client_latency_steady_ms", "p95"),
    ("steady p99", "client_latency_steady_ms", "p99"),
    ("initial p50", "client_latency_initial_ms", "p50"),
    ("initial p95", "client_latency_initial_ms", "p95"),
]:
    vals = [r.get(sec, {}).get(key) for _, r in data]
    print(f"{label:16}" + "".join(f"{v!s:>14}" for v in vals))
for k in ("pokes", "errors"):
    vals = [r.get("counters", {}).get(k) for _, r in data]
    print(f"{k:16}" + "".join(f"{v!s:>14}" for v in vals))
w1p, w6p = (d[1]["client_latency_steady_ms"]["p95"] for d in (data[0], data[2]))
print(f"\np95 W1/W6 = {w1p/w6p:.1f}x   (serial-loop prediction: ~linear in W, CPU flat)")
PYEOF
}

E1_SHAPE=(--prod-profile --connections 125 --users 5 --mutations --duration 1800)

case "$EXP" in
  e0)
    # Baseline bless: run the E1 shape against BOTH engines (writer on for
    # both — the baseline must include the replication axis), bless the Go
    # run as the G5 baseline for this shape.
    pause_kubeflow; verify_topology 4
    start_writer 40
    ./run-art-local.sh "${E1_SHAPE[@]}" --clean "$@" \
      || echo "== Go leg gate rc!=0 (G2 expected from lifecycle churn; bless proceeds) =="
    stop_writer
    GO_RUN="$(ls -t reports/run-*.json | head -1)"
    start_writer 40
    ./run-art-local.sh "${E1_SHAPE[@]}" --clean --swap "$@" \
      || echo "== TS swap leg gate rc!=0 (ratio reference; not gated) =="
    stop_writer
    "$PY" tools/local_gate.py --update-baseline --run "$GO_RUN"
    echo "blessed Go run: $GO_RUN (TS swap run is the ratio reference)"
    ;;
  e1)  # weekday steady: prod R=250, A=2-4 busy-cores -> local 125c, expect ~1-2 busy-cores
    pause_kubeflow; verify_topology 4
    start_writer 40
    ./run-art-local.sh "${E1_SHAPE[@]}" --clean "$@"
    ;;
  e2)  # peak: prod R=350, A=11 -> local 175c, expect ~5.5 busy-cores
    pause_kubeflow; verify_topology 4
    start_writer 40
    ./run-art-local.sh --prod-profile --connections 175 --users 5 --mutations --duration 1800 --clean "$@"
    ;;
  e3)  # bulk day (Jul 4: 206 evt/s): JS-drain worst case — evt rate NOT scaled
    pause_kubeflow; verify_topology 4
    start_writer 200
    ./run-art-local.sh --prod-profile --connections 175 --users 5 --mutations --duration 1800 --clean "$@"
    ;;
  e4)  # real concurrency at 1x — RELEASE GATE: if circuit-breaker teardowns
       # persist at 1x (the 4x run produced 1,626 resets/81 teardowns), that's
       # a prod-blocking escalation-policy bug, not a load artifact.
    pause_kubeflow; verify_topology 4
    ./run-art-local.sh --trace raw/traces/trace-last10m.ndjson --clean "$@"
    ;;
  e5)  # TOPOLOGY LADDER on the real 1x trace (upgraded 2026-07-09 after E4
       # FALSIFIED W=1: lockstep-ramping advance elapsed across unrelated CGs
       # + ~180%/1400% container CPU + ZERO napi stalls + p50 (not just tail)
       # 11x = queueing on the single JS orchestration loop. CGs-per-JS-LOOP
       # is the scarce resource — and it does NOT scale with cores, so prod
       # W=1 at R=350 fails HARDER, not better. Workers are the only
       # mechanism that adds JS loops (one TSFN + one loop per process).
       # Ladder: W=6 first (re-bless G5 denominator on TODAY's build), then
       # W=3 (midpoint — the scaling SHAPE is the verdict: p95 ~linear in W
       # with flat container CPU = serial-loop confirmed; flat W3->W6 = knee
       # found). Per-thread CPU captured each leg (the direct proof: one
       # node thread pegged ~100% while ~10 cores idle) + postgres CPU (the
       # one alternative suspect: 301 CGs' CVR flushes on 2 PG cores).
       # Compare against the E4 W=1 run. Rig left at W=4 (post-E5 candidate:
       # the knee is at W=3; W=4 adds headroom, leaving 4 cores for Go compute).
    pause_kubeflow
    TRACE="raw/traces/trace-last10m.ndjson"
    E4_W1_RUN="${E4_RUN:-reports/run-20260709-130055.json}"
    RUN_W6=""; RUN_W3=""
    for W in 6 3; do
      set_topology "$W"
      THR_OUT="reports/threads-w$W-$(date +%H%M%S)"
      "$PY" tools/thread_sampler.py --container xyne-sandbox-rust-test-zero-cache \
        --pg-container xyne-sandbox-postgres --interval 5 --duration 1200 \
        --out "$THR_OUT.ndjson" > "$THR_OUT.summary.txt" 2>&1 &
      THR_PID=$!
      # NOTE: gate rc must NOT kill the ladder — the W=3 leg is EXPECTED to
      # fail G5 vs the W=6-blessed baseline (that gap IS the measurement).
      ./run-art-local.sh --trace "$TRACE" --clean "$@" \
        || echo "== leg W=$W gate rc!=0 (expected for W<6; ladder is the verdict) =="
      check_spread
      kill "$THR_PID" 2>/dev/null || true; wait "$THR_PID" 2>/dev/null || true
      LATEST="$(ls -t reports/run-*.json | head -1)"
      if [ "$W" = "6" ]; then
        RUN_W6="$LATEST"
        "$PY" tools/local_gate.py --update-baseline --run "$RUN_W6"
        echo "== W=6 re-blessed as G5 denominator (same-build) =="
      else
        RUN_W3="$LATEST"
      fi
      echo "== thread summary (W=$W): $THR_OUT.summary.txt =="
      tail -16 "$THR_OUT.summary.txt"
    done
    set_topology 4      # leave the rig on the candidate (W=4, post-E5 knee)
    print_ladder "$E4_W1_RUN" "$RUN_W3" "$RUN_W6"
    ;;
  e5w3)  # RESUME for E5 (2026-07-09): first pass died silently in
         # check_spread (sparse grep pattern + set -e/pipefail) right AFTER
         # a VALID, spread-verified W=6 leg (workers 0/1/2/3/5). This case
         # skips W=6, re-blesses that run, and runs ONLY the missing W=3
         # leg + ladder table. Override inputs: E4_RUN / E5_W6_RUN env vars.
    pause_kubeflow
    TRACE="raw/traces/trace-last10m.ndjson"
    E4_W1_RUN="${E4_RUN:-reports/run-20260709-130055.json}"
    RUN_W6="${E5_W6_RUN:-reports/run-20260709-134012.json}"
    [ -f "$RUN_W6" ] || { echo "ERROR: W=6 run not found: $RUN_W6" >&2; exit 1; }
    "$PY" tools/local_gate.py --update-baseline --run "$RUN_W6"
    echo "== W=6 re-blessed as G5 denominator (same-build): $RUN_W6 =="
    set_topology 3
    THR_OUT="reports/threads-w3-$(date +%H%M%S)"
    "$PY" tools/thread_sampler.py --container xyne-sandbox-rust-test-zero-cache \
      --pg-container xyne-sandbox-postgres --interval 5 --duration 1200 \
      --out "$THR_OUT.ndjson" > "$THR_OUT.summary.txt" 2>&1 &
    THR_PID=$!
    # W=3 is a data point, not a gate: G5 WILL fail vs the W=6 baseline.
    ./run-art-local.sh --trace "$TRACE" --clean "$@" \
      || echo "== W=3 gate rc!=0 (expected vs W=6 baseline; ladder is the verdict) =="
    check_spread
    kill "$THR_PID" 2>/dev/null || true; wait "$THR_PID" 2>/dev/null || true
    RUN_W3="$(ls -t reports/run-*.json | head -1)"
    echo "== thread summary (W=3): $THR_OUT.summary.txt =="
    tail -16 "$THR_OUT.summary.txt"
    set_topology 4      # leave the rig on the candidate (W=4, post-E5 knee)
    print_ladder "$E4_W1_RUN" "$RUN_W3" "$RUN_W6"
    ;;
  e6)  # leak/lifecycle: 1h prod-cadence soak with the replication axis on
    pause_kubeflow; verify_topology 4
    start_writer 40
    ./run-art-local.sh --soak --prod-profile --connections 60 --duration 3600 --clean "$@"
    ;;
  *) echo "unknown experiment: $EXP (e0..e6, e5w3)" >&2; exit 2;;
esac
