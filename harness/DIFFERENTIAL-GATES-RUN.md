# Differential correctness gates (G44–G48 / B·A·C·D·E)

These close the **"green-but-divergent"** blind spot: a candidate image can pass
every steady-state ART gate (wedge, determinism, upgrade, metrics-emitted) while
carrying real port divergences, because those gates assert *"rust works"* — not
*"rust behaves byte/timing/metric-for-metric like TS."* Three divergences fixed
on 2026-08-28 (expire-timer idle-window eviction, P11-a `ClientNotFound` message
bytes, missing `flush.type=async` series) all passed the correctness suite green.
Each gate below is the runtime counterpart that would catch its class.

They are **differential** (run the identical scripted session against the rust
candidate *and* the TS reference image, then diff) and **lifecycle-adversarial**
(visit the disconnect/idle/purge edges the steady-state oracles never reach).

| Gate | Script | Catches (bug class) |
|------|--------|---------------------|
| **B** G44 | `lifecycle_window.py` | Server does work in the 0-client keepalive window that TS suppresses — the expire-timer poll (prod-outage class). Diffs `/metrics` deltas + eviction/flush log counts during the window; asserts `rust_delta ≤ ts_delta`. |
| **A** G45 | `error_frame_oracle.py` | Wrong `["error",{kind,message,origin}]` bytes on unhappy edges — P11-a class. Scenario matrix: **purge** (tombstone `instances.deleted=TRUE` then reload) and **stale-cookie**; byte-diffs the frame (ids/versions normalized). |
| **C** G46 | `metric_series_diff.py` | A whole metric family/label-series missing — the `flush.type=async` class. Scrapes both `/metrics`; asserts family parity + label-value parity on differential-sensitive families. |
| **D** G47 | `log_sequence_oracle.py` | Divergent lifecycle log sequence (e.g. a ghost eviction log in the idle window). Maps each side's logs to a canonical vocabulary; asserts presence parity + `rust expiry_ran ≤ ts`. Leverages the #142 signature classification. |
| **E** G48 | `inspector_state_diff.py` | Internal state drift invisible in data frames — a query stuck `got=false`, wrong `deleted`, missing query. Diffs the *content* of `inspect` replies (queries/version/metrics), where the old G37 only checked *that* both replied. |

## Prerequisites

Same as the diff-surface gates (`run-diff-surface.sh`):

- **Live sandbox pair** — both zero-cache images (rust candidate + TS reference)
  and the shared PG, up and reachable at the `ab_common` defaults:
  - rust `ws://rust-test.localhost/zero`, TS `ws://localhost:4849`
  - CVR schemas `sandbox_rust_test_0/cvr` (rust), `sandbox_rust_test_ts_0/cvr` (TS)
  - PG `postgresql://xyne:xyne123@localhost:5433/sandbox_rust_test_db`
- A fresh `harness/auth-pool.json` (regenerate with `tools/build_auth_pool.py`).
- **Container access** for B/C/D: `docker logs` and `docker exec … curl /metrics`
  on the containers named by `AB_RUST_CONTAINER` / `AB_TS_CONTAINER`.

**SKIP-safe:** every gate SKIPs (never false-FAILs) when the pair is down, a
`/metrics` endpoint is unreachable, PG is unavailable, or `inspect` is
unauthorized. A `SKIP` is not a `FAIL` — the runner exit stays 0.

## Run

All five, wired into the diff-surface runner (after G32–G43):

```bash
./run-diff-surface.sh            # G32–G48
```

Individually:

```bash
python3 harness/lifecycle_window.py       # B / G44
python3 harness/error_frame_oracle.py     # A / G45
python3 harness/metric_series_diff.py     # C / G46
python3 harness/log_sequence_oracle.py    # D / G47
python3 harness/inspector_state_diff.py   # E / G48
```

Each writes `reports/<gate>-<TAG>.json` and prints `✅/❌/⚠️/⏭️` per sub-check;
exit is 1 iff any sub-check `FAIL`ed.

## Tunables (env)

| Var | Default | Gate | Meaning |
|-----|---------|------|---------|
| `LW_TTL_MS` / `LS_TTL_MS` | 2500 | B/D | short query TTL so an eviction is due inside the window |
| `LW_HOLD_S` / `LS_HOLD_S` | 8.0 | B/D | seconds to hold through the keepalive window |
| `LW_TABLE`/`EF_TABLE`/`LS_TABLE`/`IN_TABLE` | `channels` | all | table for the scan query |
| `AB_METRICS_PORTS` | `8081,4848,9090,4849` | B/C | in-container ports to probe for `/metrics` |
| `IN_OPS` | `queries,version,metrics` | E | inspect ops to diff |

## G49 — invention-contract differentials (I-1/I-3/I-4 + ownership)

`invention_oracles.py` gives each `parity/INVENTIONS.md` construct a RUNTIME
differential (they were pinned only by in-process unit tests). Both prod outages
lived here.

| Sub-gate | Contract | Live result on gates-9fe30a683 |
|---|---|---|
| I-1/I-2 connect-ack | `connected` precedes hydrate (not serialized behind CG hydrate) | ✅ PASS (rust 137ms, ts 104ms) — **prod-bug-1 fix confirmed** |
| I-3 push-parity | same mutation → same result + lmid, **zero 401** (stale-auth) | ✅ PASS (`markChannelAsViewed`, lmid advanced, no 401) — **prod-bug-2 fix confirmed** |
| I-4 shed | slow consumer shed with a `Rehome` frame, not a bare drop | ⏭️ SKIP unless backpressure triggers — raise `IV_SHED_HOLD_S` / query breadth |
| I-1 ownership | double-connect resolves with matching client frames | finding: rust sends `Rehome` on same-client supersede, TS none — investigate |

Needs `harness/id-pool.sandbox.json` for the I-3 mutation args (`IV_ID_POOL`).

## Live-run recipe (the pair the gates actually need)

The gates are differential — they need BOTH a rust candidate and a client-serving
TS reference, host-reachable, on the shared PG. In the rust-test sandbox:

```bash
# rust candidate on the new image, via the /zero-art traefik route
#   (bootstrap_art_container in run-art-local.sh, or docker run … -l traefik…/zero-art)
# TS reference: the /zero-ts route is MIRROR-ONLY (WS handshake times out), so
#   republish the -ts container with a host port + its replica volume:
docker run -d --name xyne-sandbox-rust-test-zero-cache-ts --network sandbox-net \
  --env-file <ts.env> -e ZERO_SYNCER=ts -p 4849:4848 \
  -v rust-test_zero_cache_ts_rust_test:/var/zero  <ts-image>

export AB_RUST_WS=ws://rust-test.localhost/zero-art  AB_TS_WS=ws://localhost:4849
export AB_RUST_CVR=sandbox_rust_test_art_0/cvr       AB_TS_CVR=sandbox_rust_test_ts_0/cvr
export AB_RUST_CONTAINER=xyne-sandbox-rust-test-zero-cache-art
export AB_TS_CONTAINER=xyne-sandbox-rust-test-zero-cache-ts
# B/C metrics: zero-cache pushes via OTLP → scrape the collector, filter by host:
export AB_METRICS_URL=http://localhost:9464/metrics
export AB_RUST_METRIC_HOST=<rust-art container id>  AB_TS_METRIC_HOST=<ts container id>
./run-diff-surface.sh          # G32–G49
```

## Extending

- **D**: enrich the `CANON` vocabulary from the #142 signature DB as coverage grows.
- **A**: add scenarios (ownership-steal via two connects to one cgid; older-replica
  via a replica-version mismatch) as they become deterministically triggerable.
- **E**: expose timer/purge state via new `inspect` ops to widen state coverage.
