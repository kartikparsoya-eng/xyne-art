#!/usr/bin/env bash
# gen_impact_matrix.sh — extract the query/mutator/table impact matrix from the
# DEPLOYED backend image (same authority argument as the clientSchema step in
# run-art-local.sh: the container's baked sources can't drift from what's
# actually serving transforms).
#
# Runs the vendored staging-regression analyzer (vendor/staging-regression/
# analyze-impact.mjs — see its header) inside the backend container, where
# `typescript` is resolvable from /app/node_modules and the shared zero source
# tree is baked at /shared/src/zero. Writes raw/query-mutator-impact.json.
#
# Consumers:
#   harness/matrix_oracle.py --impact raw/query-mutator-impact.json
#     -> dark-table attribution (no-covering-query vs unresolvable-args vs
#        covered-but-zero-rows)
#
#   ./tools/gen_impact_matrix.sh [backend-container]
set -euo pipefail
DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND="${1:-xyne-sandbox-rust-test-backend}"
OUT="$DIR/raw/query-mutator-impact.json"

docker exec "$BACKEND" node -e "require.resolve('typescript')" >/dev/null 2>&1 \
  || { echo "ERROR: 'typescript' not resolvable in $BACKEND:/app — cannot run the analyzer" >&2; exit 1; }

# /app so node resolves typescript from /app/node_modules; --repo / so the
# analyzer's shared/src/zero/*.ts paths hit the baked /shared tree. dashboard/
# and backend/ call-site scan roots don't exist in the image — the analyzer
# skips missing roots, so mutator usage counts undercount (metadata only).
docker cp "$DIR/vendor/staging-regression/analyze-impact.mjs" "$BACKEND":/app/.art-analyze-impact.mjs
docker exec -w /app "$BACKEND" sh -c \
  "node .art-analyze-impact.mjs --repo / --out /tmp/impact.json && rm -f .art-analyze-impact.mjs"
mkdir -p "$DIR/raw"
docker cp "$BACKEND":/tmp/impact.json "$OUT"
docker exec "$BACKEND" rm -f /tmp/impact.json

python3 - "$OUT" << 'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
s = d["summary"]
print(f"impact matrix -> {sys.argv[1]}")
print(f"  queries={s['queryCount']} mutators={s['mutatorCount']} "
      f"edges={s['queryMutatorEdgeCount']} tables={s['tableCount']} "
      f"lowConfidenceQueries={s['lowConfidenceQueryCount']}")
EOF
