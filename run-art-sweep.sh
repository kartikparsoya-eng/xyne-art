#!/usr/bin/env bash
# run-art-sweep.sh — the FULL G1–G11 gate sweep, automated.
#
# One run cannot exercise every gate:
#   - G10 chaos docker-pauses pods mid-run -> pollutes G5 latency + G6 slopes
#   - G6 leak slopes need a >=15-min soak window
#   - G5 latency only fires at a blessed baseline shape (default: 10 conns)
#   - G4/G8/G11 (mutations/oracle/negative) belong in the functional run
# So the sweep is 3 sequenced runs, then ONE consolidated verdict:
#   A functional : 10 conns, 2 users, --mutations --oracle --negative
#   B chaos      : --chaos (fault injection, lifecycle churn)
#   C soak       : --soak (20 conns, 1h, leak slopes)      [skip: --skip-soak]
#
#   ./run-art-sweep.sh                          # full sweep (~1h20m, writes to sandbox)
#   ./run-art-sweep.sh --skip-soak              # pre-merge quick loop (~10m, no G6)
#   ./run-art-sweep.sh --soak-duration 1200     # shorter soak (>=900s for G6 slopes)
#   ./run-art-sweep.sh --no-writes              # drop --mutations (G4/G8-writes SKIP)
#   ./run-art-sweep.sh --no-clean               # keep accumulated art-% CVR state
#
# Leg A runs --clean BY DEFAULT: art-% client groups accumulate across sessions
# (155 -> 318 rows in one day) and the server recomputes IVM for every group on
# every write — a polluted sandbox produced a 500x latency false-FAIL. A fresh
# CVR + restarted pod gives every sweep the same starting conditions.
#
# The noisy neighbor (kubeflow-control-plane burns ~10 cores and invalidates
# every latency/leak number) is paused for the WHOLE sweep and unpaused on
# exit — even on failure (trap; `|| true` so an unpaused container can't
# clobber the real exit code).
#
# Exit code = consolidated verdict: 0 PASS, 1 FAIL, 2 ERROR/infra.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

SANDBOX="rust-test"; FDUR=180; SOAK_DUR=3600; SKIP_SOAK=0; WRITES=1; CLEAN=1
NOISY="kubeflow-control-plane"
while [ $# -gt 0 ]; do
  case "$1" in
    --sandbox) SANDBOX="$2"; shift 2;;
    --duration) FDUR="$2"; shift 2;;
    --soak-duration) SOAK_DUR="$2"; shift 2;;
    --skip-soak) SKIP_SOAK=1; shift;;
    --no-writes) WRITES=0; shift;;
    --clean) CLEAN=1; shift;;
    --no-clean) CLEAN=0; shift;;
    --noisy-container) NOISY="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
PY=".venv/bin/python"; [ -x "$PY" ] || PY="python3"
TS="$(date +%Y%m%d-%H%M%S)"

# --- quiet the noisy neighbor for the whole sweep -------------------------------
PAUSED=""
if docker ps --format '{{.Names}}' | grep -qx "$NOISY"; then
  echo "== pausing noisy neighbor $NOISY for the whole sweep =="
  if docker pause "$NOISY" >/dev/null 2>&1; then PAUSED="$NOISY"; fi
fi
trap 'if [ -n "$PAUSED" ]; then docker unpause "$PAUSED" >/dev/null 2>&1 || true; echo "== unpaused $PAUSED =="; fi' EXIT

newest_gate() { ls -t reports/gate-*.json 2>/dev/null | head -1 || true; }

# --- preflight: the functional leg's oracle (G8) A/B-diffs against the TS
# mirror, which silently exits whenever the backend is recreated. Restart it
# up-front AND wait out its changelog catch-up: a cold mirror is minutes
# behind the primary — diffing against it produced 61 false G8 mismatches,
# and its replay load polluted the leg's latency numbers.
ZCTS="xyne-sandbox-${SANDBOX}-zero-cache-ts"
if ! docker ps --format '{{.Names}}' | grep -qx "$ZCTS"; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$ZCTS"; then
    echo "== TS mirror $ZCTS is down — restarting for G8 =="
    docker start "$ZCTS" >/dev/null 2>&1 || true
    sleep 8
    WAITED=0
    while [ "$WAITED" -lt 300 ]; do
      LAG="$(docker logs --since 60s "$ZCTS" 2>&1 | grep -o 'replication lag: [0-9]*' | tail -1 | grep -o '[0-9]*$' || true)"
      if [ -n "$LAG" ] && [ "$LAG" -lt 5000 ]; then
        echo "== mirror ready (replication lag ${LAG}ms) =="; break
      fi
      if [ "$WAITED" -ge 30 ] && [ -z "$LAG" ]; then
        CHATTER="$(docker logs --since 30s "$ZCTS" 2>&1 | grep -c 'waiting for at least\|Purging changes' || true)"
        if [ "${CHATTER:-0}" -eq 0 ]; then
          echo "== mirror settled (no catch-up chatter for 30s) =="; break
        fi
      fi
      sleep 5; WAITED=$((WAITED + 5))
    done
    if [ "$WAITED" -ge 300 ]; then
      echo "WARNING: mirror still catching up after 300s — G8 may be degraded" >&2
    fi
  else
    echo "WARNING: TS mirror $ZCTS does not exist — G8 oracle will be degraded" >&2
  fi
fi
if [ "$FDUR" -lt 120 ]; then
  echo "WARNING: --duration $FDUR < 120s is a plumbing test, not a valid gating" >&2
  echo "         shape: mutations sent near run-end miss their acks (G4) and the" >&2
  echo "         initial-hydration burst dominates combined latency (G5)." >&2
fi

GATES=()
LABELS=()
run_leg() {  # run_leg <label> <args...>
  local label="$1"; shift
  local before after rc
  before="$(newest_gate)"
  echo ""
  echo "==================================================================="
  echo "== sweep leg: $label  ($*)"
  echo "==================================================================="
  set +e
  ./run-art-local.sh --sandbox "$SANDBOX" "$@"
  rc=$?
  set -e
  after="$(newest_gate)"
  if [ -n "$after" ] && [ "$after" != "$before" ]; then
    GATES+=("$after"); LABELS+=("$label")
    echo "== leg $label done (exit=$rc, gate: $after) =="
  else
    echo "WARNING: leg $label produced no gate report (exit=$rc) — it will" >&2
    echo "         show as a coverage hole in the consolidated verdict" >&2
  fi
  sleep 30  # settle: zero-cache recovers, sampler files flush
}

# A functional: matches the blessed 10c baseline shape so G5 fires; 2 users so
# the negative suite's wrong-user scenario runs; oracle A/B-diffs vs the TS pod.
# NB: `[ cond ] && cmd` at top level trips `set -e` when cond is false — use if.
AFLAGS=(--connections 10 --users 2 --oracle --negative --duration "$FDUR")
if [ "$WRITES" = "1" ]; then AFLAGS+=(--mutations); fi
if [ "$CLEAN" = "1" ]; then AFLAGS+=(--clean); fi
run_leg functional "${AFLAGS[@]}"

# B chaos: own leg — pod pauses would pollute A's latency and C's slopes.
run_leg chaos --chaos --duration "$FDUR"

# C soak: leak slopes (G6) need the long window.
if [ "$SKIP_SOAK" = "1" ]; then
  echo ""
  echo "== soak skipped (--skip-soak): G6 leaks will be NOT COVERED =="
else
  SFLAGS=(--soak)
  if [ "$SOAK_DUR" != "3600" ]; then SFLAGS+=(--duration "$SOAK_DUR"); fi
  run_leg soak "${SFLAGS[@]}"
fi

# --- one verdict over all legs ---------------------------------------------------
echo ""
if [ "${#GATES[@]}" -eq 0 ]; then
  echo "ERROR: no leg produced a gate report — sandbox down?" >&2
  exit 2
fi
IFS=,; LBL="${LABELS[*]}"; unset IFS
set +e
"$PY" tools/consolidate_gates.py --labels "$LBL" \
  --json "reports/sweep-$TS.verdict.json" "${GATES[@]}"
RC=$?
set -e
exit "$RC"
