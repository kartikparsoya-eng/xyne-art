#!/usr/bin/env bash
# gen_arg_schemas.sh — extract per-query/mutator Zod arg schemas from the
# DEPLOYED backend image (tools/extract-arg-schemas.mjs runs inside the
# container, so the schemas are exactly what the server validates against —
# the authority argument that already governs clientSchema + impact-matrix
# extraction in this harness).
#
# Consumers:
#   tools/gen_id_pool_db.py --arg-schemas raw/arg-schemas.source.json
#     -> enum scalars DERIVED from source (kills the stale-scalar bug class:
#        viewMode:"kanban" survived three backend upgrades as a hand default)
#
#   ./tools/gen_arg_schemas.sh [backend-container]
set -euo pipefail
DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND="${1:-xyne-sandbox-rust-test-backend}"
OUT="$DIR/raw/arg-schemas.source.json"

docker cp "$DIR/tools/extract-arg-schemas.mjs" "$BACKEND":/app/.art-extract-args.mjs
docker exec -w /app "$BACKEND" sh -c \
  "node .art-extract-args.mjs --queries src/zero/queries.ts --mutators src/zero/mutators.ts --out /tmp/args.json; rc=\$?; rm -f .art-extract-args.mjs; exit \$rc"
mkdir -p "$DIR/raw"
docker cp "$BACKEND":/tmp/args.json "$OUT"
docker exec "$BACKEND" rm -f /tmp/args.json

python3 - "$OUT" << 'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
q, m, e = d.get("queries", {}), d.get("mutators", {}), d.get("enums", {})
enum_args = sum(1 for x in q.values()
                for s in (x.get("args") or {}).values()
                if s.get("type") in ("enum", "nativeEnum"))
print(f"arg schemas -> {sys.argv[1]}")
print(f"  {len(q)} queries, {len(m)} mutators, {len(e)} enums runtime-resolved, "
      f"{enum_args} enum-typed query args")
EOF
