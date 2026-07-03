#!/usr/bin/env bash
# run-art.sh — one-command ART Mode A: (optionally harvest IDs) -> replay load -> gate.
#
#   export GR_KEY='glsa_...'                       # Grafana token, on VPN (for id harvest + gate)
#   ./run-art.sh --target wss://zero-canary/zero --auth-token "$JWT"
#   ./run-art.sh --target wss://zero-canary/zero --cookie "user_session_id=..." --refresh-ids
#   # writes (read-tracking mutations, staging/disposable envs only):
#   ./run-art.sh --target wss://zero-staging/zero --auth-token "$JWT" \
#       --enable-mutations --i-know-this-writes --mutations-per-min 4
#
# Requires for the live driver: pip install websockets
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

TARGET=""; AUTH_TOKEN=""; COOKIE=""; CONNS=200; WORKING_SET=15; CHURN_MS=500
DURATION=600; REFRESH_IDS=0; EXTRA=(); MUTFLAGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --target) TARGET="$2"; shift 2;;
    --auth-token) AUTH_TOKEN="$2"; shift 2;;
    --cookie) COOKIE="$2"; shift 2;;
    --connections) CONNS="$2"; shift 2;;
    --working-set) WORKING_SET="$2"; shift 2;;
    --churn-ms) CHURN_MS="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --extra-param) EXTRA+=(--extra-param "$2"); shift 2;;
    --enable-mutations) MUTFLAGS+=(--enable-mutations); shift;;
    --i-know-this-writes) MUTFLAGS+=(--i-know-this-writes); shift;;
    --mutations-per-min) MUTFLAGS+=(--mutations-per-min "$2"); shift 2;;
    --refresh-ids) REFRESH_IDS=1; shift;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
[ -n "$TARGET" ] || { echo "ERROR: --target is required" >&2; exit 2; }

# 1) id-pool (harvest real IDs if missing or requested)
if [ "$REFRESH_IDS" = "1" ] || [ ! -f harness/id-pool.json ]; then
  : "${GR_KEY:?export GR_KEY to harvest the id-pool}"
  echo "== harvesting id-pool from telemetry =="
  python3 tools/gen_id_pool.py --window 24h
fi

# 2) auth flag
AUTH=()
if [ -n "$AUTH_TOKEN" ]; then AUTH=(--auth-token "$AUTH_TOKEN");
elif [ -n "$COOKIE" ]; then AUTH=(--cookie "$COOKIE");
else echo "WARN: no --auth-token/--cookie; target must accept anonymous connections"; fi

# 3) drive load
echo "== replaying workload against $TARGET =="
python3 harness/replay.py \
  --target "$TARGET" --id-pool harness/id-pool.json \
  --connections "$CONNS" --working-set "$WORKING_SET" --churn-ms "$CHURN_MS" \
  --duration "$DURATION" ${AUTH[@]+"${AUTH[@]}"} ${EXTRA[@]+"${EXTRA[@]}"} \
  ${MUTFLAGS[@]+"${MUTFLAGS[@]}"}

# 4) gate on the run window
: "${GR_KEY:?export GR_KEY to evaluate gates}"
WINDOW_MIN=$(( (DURATION + 120) / 60 )); [ "$WINDOW_MIN" -lt 1 ] && WINDOW_MIN=1
echo "== evaluating gates over last ${WINDOW_MIN}m =="
set +e
python3 tools/evaluate_gates.py --window "${WINDOW_MIN}m"
CODE=$?
set -e
echo ""
[ "$CODE" = "0" ] && echo "ART: PASS" || echo "ART: FAIL (see reports/)"
exit "$CODE"
