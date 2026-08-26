#!/usr/bin/env bash
# Merge the .profraw files from a coverage-instrumented rust-syncer run and
# emit (a) the llvm-cov per-file summary and (b) uncovered-functions.txt —
# the MEASURED "which paths did the harness never trigger" artifact.
#
# Prereqs:
#   1. Image built with:  docker build --build-arg RUST_SYNCER_COVERAGE=1 \
#        --build-arg GIT_REVISION=$(git rev-parse HEAD) -t zero-cache-rust-syncer:coverage .
#   2. Deployed with docker-compose.coverage.yml (profraw lands in
#      <sandbox>/coverage-out/) and driven with the correctness suites.
#   3. Container STOPPED or at least quiesced (continuous mode syncs live,
#      but a final flush on graceful stop is the clean cut).
#
# Usage: tools/coverage-report.sh [--image zero-cache-rust-syncer:coverage]
set -euo pipefail
DIR="$(cd "$(dirname "$0")/.." && pwd)"
MONO_ROOT="${MONO_ROOT:-$DIR/../Go-RS/mono-v1.7}"
COV_DIR="${COV_DIR:-$DIR/../xyne-spaces-test/.sandboxes/rust-test/coverage-out}"
IMAGE="${1:-zero-cache-rust-syncer:coverage}"
OUT="$DIR/reports/coverage-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"

ls "$COV_DIR"/*.profraw >/dev/null 2>&1 || {
  echo "ERROR: no .profraw in $COV_DIR — was the coverage overlay deployed?" >&2
  exit 1
}

# Extract the instrumented binary (embeds the coverage mapping).
cid=$(docker create "$IMAGE")
docker cp "$cid":/usr/local/bin/rust-syncer "$OUT/rust-syncer"
docker rm "$cid" >/dev/null

# Merge + report inside the same toolchain family that built the binary.
# Compile-time paths were /build/{rust-syncer,rust-cvr,rust-ivm} — remap to
# the mounted mono packages so sources resolve.
docker run --rm --platform linux/arm64 \
  -v "$COV_DIR":/cov:ro -v "$MONO_ROOT/packages":/mono-packages:ro \
  -v "$OUT":/out rust:1-slim-bookworm bash -c '
  set -e
  rustup component add llvm-tools-preview >/dev/null 2>&1
  LLVM_BIN=$(dirname "$(find "$(rustc --print sysroot)" -name llvm-profdata | head -1)")
  "$LLVM_BIN/llvm-profdata" merge -sparse /cov/*.profraw -o /out/merged.profdata
  "$LLVM_BIN/llvm-cov" report /out/rust-syncer \
     -instr-profile=/out/merged.profdata \
     -path-equivalence=/build,/mono-packages \
     -ignore-filename-regex="cargo/registry|rustc/" \
     > /out/summary.txt
  "$LLVM_BIN/llvm-cov" export /out/rust-syncer \
     -instr-profile=/out/merged.profdata \
     -path-equivalence=/build,/mono-packages \
     -ignore-filename-regex="cargo/registry|rustc/" \
     -summary-only -format=text > /out/export-summary.json
  "$LLVM_BIN/llvm-cov" export /out/rust-syncer \
     -instr-profile=/out/merged.profdata \
     -path-equivalence=/build,/mono-packages \
     -ignore-filename-regex="cargo/registry|rustc/" \
     -format=text > /out/export-full.json
'

# Uncovered-functions list (count == 0), demangled best-effort.
python3 - "$OUT" <<'EOF'
import json, sys, os
out = sys.argv[1]
data = json.load(open(os.path.join(out, "export-full.json")))
uncovered = []
for exp in data.get("data", []):
    for fn in exp.get("functions", []):
        if fn.get("count", 0) == 0:
            files = ",".join(fn.get("filenames", [])[:1])
            uncovered.append(f"{files}\t{fn.get('name','?')}")
with open(os.path.join(out, "uncovered-functions.txt"), "w") as f:
    f.write("\n".join(sorted(set(uncovered))) + "\n")
print(f"uncovered functions: {len(set(uncovered))}")
EOF

echo
tail -20 "$OUT/summary.txt"
echo
echo "report dir: $OUT"
echo "  summary.txt              per-file region/line coverage"
echo "  uncovered-functions.txt  the untriggered-path list"
