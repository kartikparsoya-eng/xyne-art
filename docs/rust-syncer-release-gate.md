# Full Rust syncer release gate for Xyne Spaces

This is the promotion flow for `xyne-spaces-test`. It validates the complete
Rust read path—not only the Rust IVM addon—against the TypeScript syncer as the
behavioral source of truth.

## 1. Build the candidate image

From the `mono-v1.7` checkout on `rust-cvr-v1.0.0`:

```bash
docker build \
  --build-arg GIT_REVISION="$(git rev-parse HEAD)" \
  --build-arg RUST_SYNCER_FEATURES=profiling \
  -t zero-cache-rust-syncer:local \
  -f Dockerfile .
```

The root Dockerfile packages `/usr/local/bin/rust-syncer`, sets
`ZERO_RUST_SYNCER_PATH`, and defaults `ZERO_SYNCER=rust`. Selecting this
dedicated image is the rollout opt-in; the TS control remains on the upstream
Zero image. Build only from a committed tree: ART records the
`org.opencontainers.image.revision` label and the immutable image digest in
every report.

**Profiling is baked into this candidate image** via
`--build-arg RUST_SYNCER_FEATURES=profiling` (the Dockerfile threads it into
`cargo build --features`). This enables the in-process CPU flamegraph endpoint
`GET http://<HTTP_PORT>/debug/pprof/flamegraph?seconds=N` (see the rust-syncer
`OPERATIONS.md` §9 Profiling). The pprof sampler only runs during an active
request, so it adds negligible overhead while idle — safe for the
initial-testing / candidate image. **Do NOT add this arg to the upstream prod
release build** (`.github/workflows/release.yml`): profiling stays opt-in there,
and the release image should ship the plain production binary. To also attribute
a process-level RSS climb, rebuild once with
`--build-arg RUST_SYNCER_FEATURES=dhat-heap` instead (heavier — per-allocation
overhead — so only for a targeted heap-profiling run, not the standard gate).

## 2. Opt only the candidate into Rust

In the `xyne-spaces-test/.sandboxes/rust-test/docker-compose.override.yml`
candidate service, use the candidate image and add:

```yaml
services:
  zero-cache:
    image: zero-cache-rust-syncer:local
    environment:
      - ZERO_SYNCER=rust
      - ZERO_RUST_SYNCER_PATH=/usr/local/bin/rust-syncer
```

Keep `zero-cache-ts` on the matching upstream Zero release, a distinct
`ZERO_APP_ID`, and `ZERO_SYNCER=ts` (or unset). Both sides must share the same
upstream application database but use separate CVR/change schemas and replica
files. Remove `USE_RUST_IVM` and `RUST_IVM_ADDON_PATH` from the candidate: the
full `rust-syncer` links `rust-ivm` directly and does not use NAPI. The NAPI path
is the older TypeScript-syncer/Rust-IVM hybrid and is outside this release gate.

Restart the two cache services and verify the candidate log contains
`Starting rust-syncer` once per sync worker.

## 3. Mandatory gates

Create a disposable local Postgres database, then run the wrapper from
`xyne-art`:

```bash
export TEST_CVR_PG_URI='postgresql://USER@127.0.0.1:5432/rust_syncer_test'

# Five-minute functional gate: hydration, mutation-driven advances,
# reconnect/catch-up, permissions, auth rotation, and TS differential parity.
./run-rust-syncer-release.sh --mode smoke

# One-hour resource/liveness gate with reconnect churn and writes.
./run-rust-syncer-release.sh --mode soak --skip-code

# Full ART release matrix (protocol, full catalog, upgrade, drain, capacity,
# mutation matrix, determinism, telemetry, and image audit).
./run-rust-syncer-release.sh --mode release --skip-code
```

The wrapper fails before load generation unless the primary container really has
`ZERO_SYNCER=rust`, the packaged binary is executable, its startup is visible in
logs, and the reference container is not Rust.

The smoke/release run must demonstrate all of these flows:

| Flow | Required evidence |
|---|---|
| Connect and hydrate | Every expected query is acknowledged; Rust/TS converged rows match |
| Advance | Mutations replicate to both caches and produce identical materialized state |
| Reconnect and catch-up | Lifecycle churn and reconnect storm complete without missing rows or `Internal` errors |
| Permissions | Cross-user pinned group cannot hydrate; auth rotation re-transforms safely |
| Multi-CG load | No client-group limit breach, wedge marker, unbounded queue, or ownership corruption |
| Restart/upgrade | Old cookies resume after candidate restart and TS↔Rust upgrade paths converge |
| Resource behavior | RSS, CVR rows, goroutines/threads, and latency slopes remain inside ART gates |

The app uses Zero's direct-mutation mode. ART therefore POSTs push bodies to
`/api/zero/push` (or the explicit `--mutate-url`) with the same bearer identity
as the sync client; it does not send `push` messages over the Rust sync socket.
This exercises the production path: backend transaction → PostgreSQL change →
both replicas → Rust/TS IVM advance → differential row comparison.

Do not promote on a self-diff when writes are enabled. The independent TS mirror
is mandatory because it catches silent hydration/advance omissions that health
and latency metrics cannot see.

## 4. Canary and rollback

Deploy Rust to one canary cache task while the control cohort remains TS. Give
the canary its own `ZERO_APP_ID`/CVR schema and route only an explicit test
cohort to it. Run the smoke gate against the canary and the differential oracle
against the TS control, then hold the canary for at least one soak window.

Promote gradually only while row parity is exact and error/resource gates remain
green. Rollback is configuration-only: set `ZERO_SYNCER=ts` (or remove the
variable) and restart the cache task. Preserve the failed canary's logs, CVR
schema, replica, and ART reports for diagnosis; do not reuse its CVR schema for
the TS control.
