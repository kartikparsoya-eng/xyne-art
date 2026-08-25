#!/usr/bin/env bash
# TS ⇄ Rust full-breadth diff-surface gate (G32-G42).
# See docs/ts-rust-diff-surface-proposal.md. Requires the live sandbox
# (both zero-cache images + shared PG) and a fresh harness/auth-pool.json
# (regenerate with tools/build_auth_pool.py if expired).
#
# Usage: ./run-diff-surface.sh [--drain] [--trials N]
set -uo pipefail
cd "$(dirname "$0")"
DRAIN=""; TRIALS=6
while [[ $# -gt 0 ]]; do case "$1" in
  --drain) DRAIN="--drain";; --trials) TRIALS="$2"; shift;;
  *) echo "unknown arg $1"; exit 2;; esac; shift; done

echo "══ diff-surface: state gates (G32-G41) ══"
python3 harness/diff_surface.py $DRAIN
S1=$?
echo
echo "══ diff-surface: latency A/B (G42) ══"
python3 harness/latency_ab.py --trials "$TRIALS"
S2=$?
exit $(( S1 || S2 ))
