#!/usr/bin/env bash
# bootstrap-ts19-mirror.sh — stand up the diff-oracle TS mirror from the SAME
# mono build as the rust candidate, running in TS-syncer mode (zero 1.9),
# instead of the stale pinned rocicorp/zero:1.7.0.
#
# WHY (B0 / G8, 2026-08-27): the rust syncer is a 1:1 port of the mono TS
# (currently 1.9.0). Diffing it against a 1.7.0 mirror surfaces every 1.7->1.9
# behavior change as a false G8 divergence. The candidate image
# (zero-cache-rust-syncer:local) bundles BOTH the TS zero-cache (node) and the
# rust binary; ZERO_SYNCER=rust selects rust, anything else runs pure TS. So the
# SAME image, TS-mode, is the correct version-matched mirror.
#
# NOTE: even with the version-matched 1.9 mirror, `myChannelParticipations` still
# diverges (rust syncs 18 channel rows vs TS 9) — proving that G8 channels delta
# is a genuine rust vs TS-same-version divergence, NOT a version skew. See
# GATE-OBSERVATIONS.md "B0 / G8".
set -euo pipefail

IMAGE="${1:-zero-cache-rust-syncer:local}"
MIRROR_NAME="${MIRROR_NAME:-xyne-sandbox-rust-test-zero-cache-ts}"
SRC_TS="${SRC_TS:-xyne-sandbox-rust-test-zero-cache-ts}"   # container to clone env from
NET="${NET:-sandbox-net}"
VOL="${VOL:-ts19_zero}"

echo "== capturing env from $SRC_TS (strip ZERO_SYNCER*/ZERO_SERVER_VERSION) =="
docker inspect "$SRC_TS" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep -vE "^ZERO_SYNCER|^ZERO_SERVER_VERSION=|^PATH=|^HOME=|^HOSTNAME=" > /tmp/ts19.env

echo "== replacing mirror $MIRROR_NAME with $IMAGE in TS mode =="
docker rm -f "$MIRROR_NAME" >/dev/null 2>&1 || true
docker volume create "$VOL" >/dev/null 2>&1 || true
docker run -d --name "$MIRROR_NAME" --network "$NET" \
  --env-file /tmp/ts19.env -e ZERO_SYNCER=ts \
  -v "$VOL:/var/zero" "$IMAGE" >/dev/null

echo "== waiting for mirror WS + replica sync =="
for i in $(seq 1 30); do
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$MIRROR_NAME" 2>/dev/null || true)
  if [ -n "$ip" ] && nc -z -w2 "$ip" 4848 2>/dev/null; then echo "mirror up at $ip"; break; fi
  sleep 4
done
sleep 20   # let the fresh replica catch up from PG before diffing
echo "== mirror ready: $(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$MIRROR_NAME"):4848 (TS-syncer, image $IMAGE) =="
