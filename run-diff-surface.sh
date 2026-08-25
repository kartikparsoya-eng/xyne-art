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
# Use the direct host port for rust (compose maps 4850:4848) so the `connect`
# class doesn't carry Traefik proxy overhead the TS side (direct :4849) skips.
# Fall back to the Traefik route if the direct port isn't published.
RUST_DIRECT="ws://localhost:4850"
if curl -s -o /dev/null --max-time 3 "http://localhost:4850/"; then
  echo "(G42 rust target: $RUST_DIRECT — direct, Traefik bypassed)"
  AB_RUST_WS="$RUST_DIRECT" python3 harness/latency_ab.py --trials "$TRIALS"
else
  echo "(G42 rust target: default Traefik route — :4850 not published)"
  python3 harness/latency_ab.py --trials "$TRIALS"
fi
S2=$?
exit $(( S1 || S2 ))
