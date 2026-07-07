## Xyne Spaces — Application Regression Test (ART)

A **production-derived baseline** of what the app actually does and how fast it does it, so that
major changes (to `zero-cache`, the ZQL/IVM query engine, the Zero schema, or the backend) can be
validated against real user behavior instead of guesswork.

> **What "ART" means here:** an Application Regression Test — a representative workload
> (the queries/mutations users really run, weighted by frequency, with real argument shapes) plus a
> set of baseline SLOs and health gates. You run it before/after a change and fail the change if it
> regresses the baseline.
>
> **v2 covers the full breadth**: every query, mutation and one-shot seen in **7 days** of PROD —
> **151 query types, 151 mutation types, 21 one-shot queries** — not just the heavy hitters. The rare
> tail matters: a seldom-used operation (e.g. `rcaById`, `attachmentsByImpact` at ~1 call/week) is
> exactly what a schema/engine change silently breaks. Canonical machine-readable baseline:
> [`art-baseline.json`](./art-baseline.json).

---

### 1. How this baseline was built

All numbers come from production telemetry via Grafana (`grafana.spaces.xyne.juspay.net`).
The baseline is **generated**, not hand-written — [`tools/build_baseline.py`](./tools/build_baseline.py)
assembles [`art-baseline.json`](./art-baseline.json) from raw pulls in `raw/`. Reproduce with
[`refresh-baseline.sh`](./refresh-baseline.sh).

| Signal | Source | Window | Why |
|---|---|---|---|
| Query / mutation / one-shot **mix + weights** | Victoria Logs `stats by (query\|mutation) count()` | **7d** | full breadth + stable weekly weights |
| **Arg schemas** (all 151 queries) | raw event sample + targeted per-query lookups | 7d | replayable inputs |
| Client **latency** percentiles | Victoria Logs `quantile(latency)` | **72h** | see note ↓ |
| **Server engine** timings | VictoriaMetrics `histogram_quantile(increase(zero_sync_*[7d]))` | 7d | primary SLO |
| Health / event volume | Victoria Logs `stats by (event)` + backend `level` | 7d | gates |

> **Why client latency is 72h, not 7d:** the `quantile()` aggregation over the full 7-day
> `zero_query_complete` stream (~3.5M events) overloads the Victoria Logs backend and returns empty.
> 72h (~1.5M events) is the widest window that reliably returns. **Counts/weights are true 7d**;
> only the client latency percentiles use 72h. Server histograms are pre-bucketed so 7d is cheap.
> (`ivm_advance` uses a 1h rate sample — `increase[7d]` returns null for that one series.)

Snapshot: **2026-07-02**, prod app version **`1.181.0-release-20260630.2`**.
**Retention check:** 7 full days of logs are present (the 6–7-days-ago bucket has 308k events). The
weekly query total (**2.16M**) is below 24h×7 purely because of the **weekend dip** (~308k/weekend-day
vs ~506k/weekday) — not missing data.

---

### 2. The workload (what to replay)

Traffic is a strong power law, so a compact core covers most volume — **but the ART carries the whole
tail too** (full lists in `art-baseline.json`).

- **Queries**: 2,162,008 reactive completions/7d across **151 types**. Coverage: **top 18 = 80%**,
  **top 42 = 95%**, remaining 109 types form the long tail.
- **Mutations**: 364,565/7d across **151 types**. Top 2 (auto-fired read-tracking) = **78%**.
- **One-shot `.run()`**: 408,876/7d across **21 types**, dominated by `channelConversationsPaginatedV3`.

**Top queries** (7d weight · args · client p50):

| Query | wt | calls/7d | args | p50 |
|---|---|---|---|---|
| `subTicketsByMappedTicketId` | 13.72% | 296,617 | `mappedTicketId` | 148ms |
| `channelConversationsPaginatedV3` | 12.53% | 270,982 | `channelId, direction, isMember, limit, start` | 215ms |
| `userBookmarks` | 8.70% | 188,041 | — | 49ms |
| `channelLatestMultipleConversationsV3` | 7.28% | 157,436 | `channelId, isMember, limit` | 283ms |
| `threadConversation` | 6.85% | 148,100 | `channelId, conversationId, isMember` | 158ms |
| `activeCallsInChannel` | 6.51% | 140,676 | `channelId` | 280ms |
| `conversationParticipantByConversationId` | 6.18% | 133,677 | `conversationId` | 160ms |
| `getCanvas` | 2.68% | 57,980 | `canvasId` | 156ms |
| `getAllUserGroups` | 2.63% | 56,830 | `lastUpdatedAt` | 276ms |
| `ticketByIdV2` | 2.46% | 53,109 | `ticketId` | 180ms |

**Top mutations** (7d weight · p50 · flag):

| Mutation | wt | p50 | flag |
|---|---|---|---|
| `channel.markChannelAsViewed` | 39.51% | 353ms | auto-fired |
| `activities.markThreadActivitiesAsReadV2` | 38.66% | 296ms | auto-fired |
| `activities.markAsRead` | 5.60% | 261ms | |
| `conversations.send` | 5.09% | 650ms | **critical UX** |
| `messages.send` | 4.18% | 546ms | **critical UX** |
| `messages.react` | 1.46% | 286ms | critical UX |

---

### 3. The baseline SLOs (what to assert against)

#### 3a. Server engine — the PRIMARY signal ✅

The zero-cache engine is **healthy and low-variance** (7d). A major change must not push these past the
`pass_*` thresholds (`pass_p95 = p95×1.20`, `pass_p99 = p99×1.25`):

| Metric (ms) | p50 | p95 | p99 | fail p95 > | fail p99 > |
|---|---|---|---|---|---|
| `zero_sync_hydration_time` | 20.8 | 1252 | 5380 | 1503 | 6725 |
| `zero_sync_advance_time` | 1.2 | 14.4 | 41.7 | 17.3 | 52.1 |
| `zero_sync_ivm_advance_time` | 0.5 | 3.0 | 9.2 | 3.7 | 11.5 |
| `zero_sync_poke_time` | 70.3 | 550 | 1605 | 660 | 2006 |
| `zero_sync_cvr_flush_time` | 29.8 | 370 | 481 | 444 | 602 |
| `zero_sync_query_transformation_time` | 18.5 | 102 | 931 | 122 | 1164 |

#### 3b. Client-perceived — the SECONDARY signal ⚠️

Use client **p50 per query** (robust). **Do NOT hard-gate on client p95/p99** — see §5.

#### 3c. Health gates (7d — must not regress)

| Gate | Baseline (7d) | Fail if |
|---|---|---|
| Query completion (`complete/called`) | 95.6% | < 86% |
| Run completion | 96.1% | < 88% |
| Mutation completion | 95.2% | < 92% |
| Mutation error rate | 5.2% | > 8% |
| API success rate | 97.9% | < 95% |
| Backend log error rate | 0.35% | > 1% |
| Socket fail:success ratio | **3.4** (WATCH) | worsens > 30% |

---

### 4. How to run the ART against a change

The harness lives in [`harness/`](./harness/) + [`tools/`](./tools/) and is runnable today. Two modes,
one shared gate (`tools/evaluate_gates.py`). Full harness docs: [`harness/README.md`](./harness/README.md).

**Mode A — Replay load test (pre-merge, deterministic).** Drive the real weighted workload at a
candidate `zero-cache`, then gate on its engine histograms.
```bash
export GR_KEY='glsa_...'                                    # Grafana token, on VPN
python3 tools/gen_id_pool.py --window 24h                   # harvest real IDs -> harness/id-pool.json
pip install websockets                                       # live driver dep
python3 harness/replay.py \
    --target wss://<candidate-zero-cache>/zero \
    --id-pool harness/id-pool.json \
    --connections 200 --working-set 15 --churn-ms 500 --duration 600 \
    --auth-token "$JWT"                                      # or --cookie "user_session_id=..."
python3 tools/evaluate_gates.py --window 12m                # PASS/FAIL vs pass_*/gates
```
The driver fires **named custom queries by name+args** (`{op:put,hash,name,args}`, protocol v49) —
zero-cache transforms them server-side, so no Xyne schema is needed. Each virtual client keeps a
working set of desired queries and churns them, exercising query-transform → hydration → advance →
poke → cvr. **The long tail is included** (weighted over all 151 queries) — replaying only the top-N
misses exactly the operations most likely to regress unnoticed. Preview the exact wire messages with
`--dry-run` (no deps, no network).

Writes are OFF by default. To also replay the dominant read-tracking mutations
(`channel.markChannelAsViewed` + `activities.markThreadActivitiesAsReadV2` = ~78% of prod write
volume, wired as protocol-v49 custom-mutator pushes) add `--enable-mutations --i-know-this-writes`
(both required) — **disposable/staging targets only**, they write as the authenticated user.

**Mode B — Prod / canary shadow compare (post-deploy).** No driver; compare the canary against the
rest of the fleet over the SAME window (time-of-day effects cancel out, unlike the static baseline):
```bash
python3 tools/shadow_compare.py --window 30m \
    --canary-selector  'host_name=~"xyne-spaces-zero-canary.*"' \
    --control-selector 'host_name!~"xyne-spaces-zero-canary.*"'
# NB: the zero_sync_* histograms carry per-pod identity as host_name (not pod).
python3 tools/evaluate_gates.py --window 30m            # + client-side health gates (pod-agnostic)
```
A metric **FAILs** only when the canary is both relatively worse than control (>25% p95 / >30% p99,
>5ms absolute, ≥100 samples) **and** above the baseline `pass_*` absolute threshold; one of the two
alone is a WATCH. Validated live: a single prod pod flagged at hydration p95 +28.8% vs fleet.

**The gate (`evaluate_gates.py`)** scrapes the same signals the baseline is built from and exits
non-zero on any failure (so CI can block a merge/deploy). A change is a **regression** if any §3a
server SLO (`> pass_p95`/`pass_p99`) or any §3c health gate fails. `socket_failure_ratio` is WATCH-only.
Client p50 moving up > 25% on a top-40 query is a **warning** (investigate, don't necessarily block).

---

### 5. Critical caveat: client latency ≠ engine latency

The single most important finding for interpreting this data:

- **The server is fast.** 7d: hydration p99 = 5.4s, advance p99 = 42ms, poke p99 = 1.6s.
- **Client-perceived tails are huge** (e.g. `userVisibleEmailChannels` p95 = 38 min; `userDrafts`
  p99 = 877s; many queries share *identical* maxima around 1,437,0xx / 34,100,5xx / 66,101,0xx ms).
- Those identical, absurd maxima recur across *unrelated* queries → **backgrounded/suspended tabs
  resolving on refocus and socket reconnect waits**, not query compute.
- Corroborated by `websocket_connection_failed` = **1.80M/7d** vs successful **530k/7d** (ratio 3.4 —
  and it worsened from 2.0 in the 24h view, a real signal worth chasing).

**Implication:** build the ART's hard gates on the **server histograms** (§3a) and **health rates**
(§3c). Treat client p95/p99 as directional only. To attack the *user-perceived* tail, the lever is the
**socket/reconnect + tab-background** path, not query optimization.

---

### 6. Regression watchlist (highest-value assertions)

Highest client-tail impact (calls × p95), anomalous in prod **today**. A good change should improve
these; none should worsen them (full computed list in `art-baseline.json → regression_watchlist`):

- `userVisibleEmailChannels` — p95 ~38 min (worst-in-class).
- `userBookmarks` — 188k calls/7d, p95 9s (high volume × bad tail = highest user impact).
- `getAllUserGroups` — p95 14s / p99 168s, reactive **and** one-shot.
- `userDrafts` — p99 877s.
- `dmChannelsLatestMessagesPaginated`, `userActiveCalls`, `userActivitiesPaginatedV2` — multi-second p95.
- `messages.send` / `conversations.send` — critical-UX mutations to keep honest.

---

### 7. Files & refreshing

```
xyne-art/
├── art-baseline.json          # canonical baseline (generated) — assert against this
├── ART.md                     # this document
├── run-art.sh                 # one-command Mode A: (ids) → replay → gate
├── run-art-local.sh           # one-command Mode A vs the LOCAL docker sandbox (no VPN/Grafana)
├── refresh-baseline.sh        # re-pull PROD telemetry → raw/ → rebuild art-baseline.json
├── harness/                   # Mode A replay load driver
│   ├── replay.py              # weighted named-query load driver (asyncio websockets); --dry-run
│   ├── workload.py            # pure sampler + wire-message builder (protocol v49)
│   ├── diff_oracle.py         # differential correctness oracle — converged-state diff vs a reference build (G8)
│   ├── negative.py            # adversarial negative suite — documented-error-path assertions (G11)
│   ├── id-pool.json           # real IDs harvested from telemetry (git-ignored)
│   ├── id-pool.sandbox.json   # IDs harvested from the local sandbox DB
│   ├── client-schema.json     # clientSchema from the sandbox CVR (initConnection needs it)
│   └── README.md              # harness usage + design + caveats (incl. local-sandbox flow)
├── tools/
│   ├── gen_id_pool.py         # harvest real entity IDs from telemetry → harness/id-pool.json
│   ├── gen_id_pool_db.py      # harvest IDs from a Postgres DB (local sandbox; --user-id scoping)
│   ├── mint_local_jwt.py      # HS256 JWT for the LOCAL sandbox only
│   ├── evaluate_gates.py      # THE GATE: scrape zero_sync_* + health → PASS/FAIL report
│   ├── local_gate.py          # local-sandbox gate G1–G11 (no Grafana); consumes run/resource/oracle/chaos/negative reports
│   ├── resource_sampler.py    # docker stats + pprof + CVR-row sampler → slopes for the leak gates
│   ├── chaos.py               # fault injector: docker-pause zero-cache/postgres mid-run (G10)
│   ├── heap_diff.py           # go tool pprof -diff_base wrapper over a run's heap snapshots
│   ├── shadow_compare.py      # Mode B: canary vs control fleet over the same window
│   ├── build_baseline.py      # assembles art-baseline.json from raw/
│   ├── extract_arg_schemas.py # parses raw events → per-query arg schema map
│   └── parse_query_args.py    # ad-hoc human-readable arg inspector
├── reports/                   # gate + run reports (git-ignored)
└── raw/                       # raw telemetry pulls (regenerated by refresh)
```

```bash
export GR_KEY='<service-account-token>'   # must be on Juspay VPN
./refresh-baseline.sh                       # rebuild the baseline: pull 7d → art-baseline.json
./run-art.sh --target wss://<zero-cache>/zero --auth-token "$JWT"   # Mode A: replay → gate
./run-art-local.sh --mutations              # Mode A vs the local docker sandbox (auto JWT/pool/schema)
./run-art-local.sh --lifecycle --mutations  # + session churn: resume-from-cookie, abrupt closes, zombies
./run-art-local.sh --soak --mutations       # 1h leak hunt: resource sampler + slope gates (G6)
./run-art-local.sh --users 3 --mutations    # multi-identity write contention
./run-art-local.sh --oracle                 # + G8: differential oracle vs the TS reference build
./run-art-local.sh --chaos                  # + G10: fault injection (docker-pause zero-cache/postgres mid-run)
./run-art-local.sh --negative --users 2     # + G11: adversarial negative suite (forged cookies, cross-user
                                            #   hijack probe, reconnect storm, auth rotation, TTL purge)
```

Refresh **weekly** and **after every release**. If the traffic mix shifts materially, the weights and
`pass_*` margins update automatically on rebuild. Bump `art_version` in `build_baseline.py` per refresh.

## Relationship to `scripts/staging-regression` (xyne-spaces `feature/art`)

A parallel regression effort (XYNE-12332, commit `81c133fa2`) lives in the app repo. The two
suites are **complementary, not competing** — different isolation models catch different bugs:

| | staging-regression | xyne-art |
|---|---|---|
| Model | sequential A/B: TS pass then Go pass, **two isolated DBs** seeded identically, same deterministic mutation log | live mirror: one client → two pods on the **same DB**, byte-diff of materialized state |
| Client | real Zero JS client (`zero-node` driver) | protocol-v49 reimplementation (cheap 50–200 conns) |
| Catches | **write-path divergence end-to-end** (a Go push bug that corrupts the DB is invisible to a shared-DB mirror) | load/lifecycle/leak/latency regressions, protocol edges, chaos recovery, adversarial paths |
| Workload | curated + Zod-synthesized fixtures | prod-mined weights/args/behavior (Grafana) |

Adopted from it (2026-07-06):
- **G13 log-health gate** (`tools/log_gate.py`, always on in `run-art-local.sh`): scans both pods'
  docker logs over the exact run window for their release-block list — sidecar crash, fallback-to-TS,
  `advance reset for clientGroup`, `resetting pipelines`, `Advancement exceeded timeout` (FAIL), plus
  slow-SQLite/ERROR-volume thresholds (WATCH). Closes the "sidecar died, pod silently fell back to TS,
  G8 certified TS-vs-TS" hole that no client-side gate can see.
- **Query→mutator→table impact matrix** (`tools/gen_impact_matrix.sh` runs the vendored
  `vendor/staging-regression/analyze-impact.mjs` inside the deployed backend container →
  `raw/query-mutator-impact.json`). First consumer: `matrix_oracle.py --impact` attributes every dark
  table to an actionable cause (`no-covering-query` / `covering-args-unresolvable` /
  `covered-but-zero-rows` / `not-in-catalog`) instead of one undifferentiated bucket.

- **Impact-aware mutation targeting + G14 edge coverage** (`replay.py --impact`, auto in
  `run-art-local.sh --mutations`): with P=0.75 a mutation is aimed at a currently-subscribed query's
  OWN entity id (channelId/conversationId), so writes provably intersect live pipelines instead of
  random unwatched rows; G14 gates exercised/reachable query-mutator edges and names uncovered pairs.
- **Zod-derived arg synthesis** (`tools/gen_arg_schemas.sh` → `raw/arg-schemas.source.json`;
  `gen_id_pool_db.py` derives enum scalars from it): enum values come from the deployed image's own
  zod schemas (nativeEnums resolved via @prisma/client, pg_enum fallback), shape-aware
  (array-of-enum ≠ scalar enum), per-key enum conflicts kept hand-curated. Kills the stale-scalar
  class (`viewMode:"kanban"` survived three backend upgrades as a hand default).
- **G5b per-query latency multipliers** (`local_gate.py --per-query-factor`, default 3x + 100ms
  absolute + ≥10 samples both sides): compares `latency_by_query` steady p50 vs the blessed shape;
  offenders named with ratios. Validated: a TS-blessed scratch baseline vs the slow Go run flags 50
  offenders (getUserGroupMappingsByUserId x114.8, ...) that aggregate G5 collapsed into one number.

## G15 — push-path mutator matrix (`harness/mutation_matrix.py`)

Closes the biggest honest coverage gap: mutation TYPE coverage was 2/218 (hand-built args, ~78% of
prod write *volume* but ~1% of the type surface). The zod-schema-driven synthesizer
(`workload.SchemaSynthesizer`, fed by `raw/arg-schemas.source.json`) now fires **every
synthesizable mutator** through the real push path — zero-cache → backend mutator (zod +
permissions + prisma) → replication → both pods' advance — with a Go-vs-TS byte-diff after every
wave (equality-twice convergence + persistent-mismatch re-check).

Mechanics: phase-ordered chains (CREATE with fresh `artmx…` ids → UPDATE on pool/overlay entities →
DESTRUCTIVE on overlay-owned entities ONLY; `org.*` hard-denylisted); per-mutation lmid ack
barriers make pod-log error lines exactly attributable (this build forwards no `pushResponse`
frames — the pusher's `returned a mutation error` log line is the only per-mutation detail
channel); post-run `artmx%` cleanup sweep over the impact matrix's writeTables.

First full run (2026-07-07): **PASS — 146/220 fired, 57 mutator types APPLIED real writes,
83 app-rejected (validation paths exercised), 6 synth-invalid (synthesizer backlog), 0 diverged
waves, 0 unmatched error lines, 0 cleanup leftovers.** Push-path write coverage 2 → 57 types.

Caveats: update-phase applied mutators aiming at pool ids mutate SEEDED rows (renames/flags — never
deletes); the report's `shared_updates_applied` field audits them — re-run `--refresh --clean` and
re-bless G5 if identities were touched. Skip backlog: 40 destructive-on-shared (by design),
~30 unresolvable ids (empty tables: calls, forms, nudges…) — seeding unlocks them.

Wired: `run-art-local.sh --mutation-matrix` → `local_gate.py --mut-matrix` (G15; FAIL on
divergence/protocol errors, WATCH on applied-fraction <30%, app-rejections are coverage data).

## Trace-faithful replay (`tools/mine_traces.py` + `harness/trace_replay.py`)

The third workload tier — beyond statistical (weights) and behavioral (profile): replay REAL prod
sessions event-for-event. Prod logs carry per-event `clientSessionId`, `zeroClientGroupId`, ms
timestamps, and (for queries/one-shots) the FULL nested args object in `_msg` — so per-session
command streams are exactly reconstructable. 10 min of prod = 301 sessions / 14,200 events / 278
users.

Preserved exactly: sequences, ms timing (optionally `--time-compress N`), session interleaving,
entity-reuse topology (frequency-ranked bijection: hottest prod channel → hottest pool channel,
wrap counted), cgid continuity (shared mapped cgid + cookie jar → real resume + real multi-tab).
Deliberately synthetic: data content (rank-mapped ids), epoch-ms cursors rebased to replay-now,
mutation args (prod logs none — builders fire where available), removals (TTL-expired).

First A/B (301 real sessions, 4x compression, 2026-07-07):

| | Go | TS 1.7 |
|---|---|---|
| sessions opened | 301/301 | 301/301 |
| pokes | 9,245 | 9,315 |
| client-visible errors | **81 (reset circuit breaker → CG teardowns)** | **0** |
| pipeline resets (pod logs) | 1,626 advancement-timeout | 364 |
| steady p50 / p95 | **1.0s / 11.6s** | 2.6s / 36.4s |

Finding no statistical run produced: at REAL prod session concurrency (301 CGs vs our synthetic
50), background replication advances hit the economic-abort floor fleet-wide — TS absorbs this as
self-healing resets; Go escalates 4.5x more resets into 81 client-visible circuit-breaker
teardowns. (Go is simultaneously 2.5x faster on hydration latency.) Caveat: 4x compression = 4x
prod intensity; re-run at 1x before treating as a prod-blocking claim.

Usage: `GR_KEY=... tools/mine_traces.py --window 60m` → `harness/trace_replay.py --trace
raw/traces/trace-<tag>.ndjson --target ws://... [--time-compress N] [--dry-run]`. Run summaries are
gate-schema-compatible (G5 shape = `trace:<name>`); `scheduling_lag_ms` proves trace fidelity
(p50 0.5ms) and is itself a slowness signal. Traces live in raw/traces/ (gitignored).
