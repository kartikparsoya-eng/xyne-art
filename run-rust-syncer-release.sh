#!/usr/bin/env bash
# Production gate for the full Rust syncer in the xyne-spaces-test sandbox.
# The candidate must run ZERO_SYNCER=rust; the mirror must remain TypeScript.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
MONO_ROOT="${MONO_ROOT:-$DIR/../Go-RS/mono-v1.7}"
XYNE_SPACES_ROOT="${XYNE_SPACES_ROOT:-$DIR/../xyne-spaces-test}"
SANDBOX="rust-test"
MODE="smoke"
SKIP_CODE=0
EXTRA=()
EXTRA_COUNT=0

usage() {
  cat <<'EOF'
Usage: ./run-rust-syncer-release.sh [options] [-- ART_OPTIONS]

Production gate for the full Rust syncer in the xyne-spaces-test sandbox.
The candidate must run ZERO_SYNCER=rust; the mirror must remain TypeScript.

Options:
  --mode MODE       code-only, smoke (default), soak, release, or proxy
  --sandbox NAME    xyne-spaces-test sandbox name (default: rust-test)
  --skip-code       skip local Rust/TypeScript code gates
  -h, --help        show this help

Environment:
  TEST_CVR_PG_URI   disposable PostgreSQL database for the real CVR tests
  MONO_ROOT         mono-v1.7 checkout (defaults to ../Go-RS/mono-v1.7)
  XYNE_SPACES_ROOT  xyne-spaces-test checkout (defaults to ../xyne-spaces-test)
  RUST_SYNCER_IMAGE candidate image (default: zero-cache-rust-syncer:local)
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --sandbox) SANDBOX="$2"; shift 2;;
    --mode) MODE="$2"; shift 2;;
    --skip-code) SKIP_CODE=1; shift;;
    --) shift; EXTRA=("$@"); EXTRA_COUNT=$#; break;;
    -h|--help)
      usage
      exit 0
      ;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done

case "$MODE" in
  code-only|smoke|soak|release|proxy) ;;
  *) echo "ERROR: --mode must be code-only, smoke, soak, release, or proxy" >&2; exit 2;;
esac

[ -d "$MONO_ROOT/packages/rust-syncer" ] || {
  echo "ERROR: MONO_ROOT is not the Rust syncer checkout: $MONO_ROOT" >&2
  exit 2
}
[ -f "$XYNE_SPACES_ROOT/docker-compose.dev.yml" ] || {
  echo "ERROR: XYNE_SPACES_ROOT is not xyne-spaces-test: $XYNE_SPACES_ROOT" >&2
  exit 2
}

if [ "$SKIP_CODE" = "0" ]; then
  : "${TEST_CVR_PG_URI:?set TEST_CVR_PG_URI to a disposable Postgres database}"
  echo "== Rust code and real-Postgres lifecycle gate =="
  # Parity-fixture freshness: fail if the committed TS-golden fixtures no longer
  # match current TS output (i.e. TS drifted and the Rust parity tests would be
  # validating against stale captured behavior).
  bash "$MONO_ROOT/packages/rust-cvr/agentic/parity/check-parity-fixtures.sh" "$MONO_ROOT"
  cargo test --manifest-path "$MONO_ROOT/packages/rust-cvr/Cargo.toml" --all-targets
  cargo test --manifest-path "$MONO_ROOT/packages/rust-ivm/Cargo.toml" --all-targets
  cargo test --manifest-path "$MONO_ROOT/packages/rust-syncer/Cargo.toml" --all-targets
  cargo check --manifest-path "$MONO_ROOT/packages/rust-syncer/Cargo.toml" \
    --all-targets --no-default-features
  pnpm --dir "$MONO_ROOT" --filter zero-cache run check-types
fi

if [ "$MODE" = "code-only" ]; then
  echo "Rust syncer code gate: PASS"
  exit 0
fi

MIRROR="xyne-sandbox-${SANDBOX}-zero-cache-ts"
IMAGE="${RUST_SYNCER_IMAGE:-zero-cache-rust-syncer:local}"
docker image inspect "$IMAGE" >/dev/null 2>&1 || {
  echo "ERROR: candidate image is missing: $IMAGE" >&2
  exit 1
}
IMAGE_REVISION="$(docker image inspect -f '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$IMAGE")"
if [ -z "$IMAGE_REVISION" ] || [ "$IMAGE_REVISION" = "<no value>" ] || [ "$IMAGE_REVISION" = "unknown" ]; then
  echo "ERROR: $IMAGE has no immutable org.opencontainers.image.revision label" >&2
  echo "Rebuild with --build-arg GIT_REVISION=\"\$(git rev-parse HEAD)\" --build-arg RUST_SYNCER_FEATURES=profiling" >&2
  exit 1
fi
docker run --rm --entrypoint sh "$IMAGE" -lc '
  test "$ZERO_SYNCER" = rust &&
  test -x "${ZERO_RUST_SYNCER_PATH:-/usr/local/bin/rust-syncer}" &&
  test -z "${USE_RUST_IVM:-}" &&
  test -z "${RUST_IVM_ADDON_PATH:-}" &&
  test ! -e /app/mono/packages/rust-ivm/napi/rust-ivm.node
' || {
  echo "ERROR: $IMAGE is not a Rust-only syncer image" >&2
  exit 1
}

docker inspect "$MIRROR" >/dev/null 2>&1 || {
  echo "ERROR: required TypeScript mirror is missing: $MIRROR" >&2
  exit 1
}

MIRROR_MODE="$(docker exec "$MIRROR" printenv ZERO_SYNCER 2>/dev/null || true)"
if [ "$MIRROR_MODE" = "rust" ]; then
  echo "ERROR: $MIRROR must remain the TypeScript source-of-truth reference" >&2
  exit 1
fi

echo "== Xyne Spaces ART: full Rust candidate vs TS reference ($MODE) =="
run_art() {
  if [ "$EXTRA_COUNT" -gt 0 ]; then
    "$DIR/run-art-local.sh" "$@" "${EXTRA[@]}"
  else
    "$DIR/run-art-local.sh" "$@"
  fi
}
case "$MODE" in
  proxy)
    # Encryption sync-proxy E2E (encryption OFF, mirror ON): stands up the proxy pod on
    # sandbox-net (Traefik enc.<sandbox>.localhost) and runs transparency + lifecycle +
    # mirror-convergence + mirror-failure-isolation. Proves the MITM proxy is safe in the
    # critical path before it fronts real traffic.
    SANDBOX="$SANDBOX" exec "$DIR/test-encryption-proxy.sh"
    ;;
  smoke)
    run_art --sandbox "$SANDBOX" --bootstrap --clean \
      --image "$IMAGE" \
      --target "ws://${SANDBOX}.localhost/zero-art" \
      --container "xyne-sandbox-${SANDBOX}-zero-cache-art" \
      --cvr-schema "sandbox_${SANDBOX//-/_}_art_0/cvr" \
      --connections 25 --duration 300 --users 2 --lifecycle \
      --mutations --oracle --negative --port-probes
    ;;
  soak)
    run_art --sandbox "$SANDBOX" --bootstrap --clean \
      --image "$IMAGE" \
      --target "ws://${SANDBOX}.localhost/zero-art" \
      --container "xyne-sandbox-${SANDBOX}-zero-cache-art" \
      --cvr-schema "sandbox_${SANDBOX//-/_}_art_0/cvr" \
      --soak --users 10 --negative
    ;;
  release)
    run_art --sandbox "$SANDBOX" --bootstrap --clean \
      --image "$IMAGE" \
      --target "ws://${SANDBOX}.localhost/zero-art" \
      --container "xyne-sandbox-${SANDBOX}-zero-cache-art" \
      --cvr-schema "sandbox_${SANDBOX//-/_}_art_0/cvr" \
      --release --users 10
    ;;
esac
