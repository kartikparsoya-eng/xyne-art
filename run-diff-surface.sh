#!/usr/bin/env bash
# TS ⇄ Rust full-breadth diff-surface gate (G32-G48).
# G32-G43: steady-state state/latency/concurrent differentials.
# G44-G48 (B/A/C/D/E): lifecycle-adversarial + non-data-channel differentials
#   — see harness/DIFFERENTIAL-GATES-RUN.md. These close the "green-but-divergent"
#   blind spot the steady-state oracles have (idle-window ghost work, error-frame
#   bytes, metric label-sets, lifecycle logs, inspector state).
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
echo
echo "══ diff-surface: W-mode concurrent differential (G43) ══"
python3 harness/diff_concurrent.py
S3=$?

# ── Differential correctness gates (B/A/C/D/E) ────────────────────────────────
# The steady-state result-channel diff above never visits the disconnect/idle/
# purge lifecycle edges, nor asserts error-frame bytes, metric label-sets, logs,
# or inspector state against TS — the exact gaps that let a divergent image go
# green. Each gate is SKIP-safe (no false-FAIL when the pair/endpoint is down).
echo
echo "══ diff-surface: lifecycle-window differential (G44/B — idle-window ghost work) ══"
python3 harness/lifecycle_window.py
S4=$?
echo
echo "══ diff-surface: error-frame differential (G45/A — purge/stale-cookie frame bytes) ══"
python3 harness/error_frame_oracle.py
S5=$?
echo
echo "══ diff-surface: metric-series differential (G46/C — family + label-set parity) ══"
python3 harness/metric_series_diff.py
S6=$?
echo
echo "══ diff-surface: log-sequence oracle (G47/D — lifecycle log parity) ══"
python3 harness/log_sequence_oracle.py
S7=$?
echo
echo "══ diff-surface: inspector state differential (G48/E — inspect content parity) ══"
python3 harness/inspector_state_diff.py
S8=$?

exit $(( S1 || S2 || S3 || S4 || S5 || S6 || S7 || S8 ))
