# ART Mode-A replay harness

Drives the **production-derived workload** (`../art-baseline.json`) at a candidate `zero-cache`, then
gates on its engine histograms. This is how you test a major change (zero-cache, ZQL/IVM, schema,
backend) against real user behavior before shipping.

```
harness/
├── replay.py             # the load driver (asyncio websockets) — fires the weighted named-query mix
├── workload.py           # pure sampler + Zero wire-message builder (protocol v49); stdlib only
├── diff_oracle.py        # differential correctness oracle (G8) — converged-state diff vs a reference build
├── negative.py           # adversarial negative suite (G11) — forged cookies, hijack probe, storms
├── id-pool.json          # real entity IDs harvested from telemetry (gitignored; make with gen_id_pool.py)
├── id-pool.sandbox.json  # IDs harvested from the local sandbox DB (gen_id_pool_db.py)
├── client-schema.json    # clientSchema extracted from the sandbox CVR (required by initConnection)
└── README.md             # you are here
../tools/
├── gen_id_pool.py     # harvest real IDs (channelId, ticketId, ...) from zero_query_complete.args
├── gen_id_pool_db.py  # harvest IDs straight from a Postgres DB (local sandbox; --user-id scoping)
├── mint_local_jwt.py  # HS256 JWT for the LOCAL sandbox only (sub/email/memberId/workspaceId claims)
├── evaluate_gates.py  # THE GATE (Mode A): scrape zero_sync_* + health, PASS/FAIL vs the baseline
└── shadow_compare.py  # Mode B: canary vs control fleet over the same window (host_name selectors)
```

## Why this works without the Xyne app code

The Xyne query/mutation/schema definitions live in a **separate repo** (`xyne-spaces-login`). We don't
need them: a **named custom query** goes on the Zero wire as
`{"op":"put","hash":..,"name":"subTicketsByMappedTicketId","args":[{...}]}` and **zero-cache resolves
name+args → AST server-side** via the app's query endpoint. Verified against the mono repo:

- `packages/zero-protocol/src/queries-patch.ts` — `upPutOpSchema` has optional `name`/`args` ("filled
  in for custom queries").
- `packages/zero-protocol/src/connect.ts` — `encodeSecProtocols` + `initConnection` body.
- `packages/zero-protocol/src/protocol-version.ts` — `PROTOCOL_VERSION = 49`.
- Template tools: `tools/client-simulator`, `tools/ivm-parity/query-runner.ts`.

So the driver replays **all 151 queries by name+args** with realistic IDs — no schema required.

## The load model

Each virtual client opens one WebSocket and keeps a **working set** of `--working-set` desired queries
(weighted over the full 151-query catalogue). Every `--churn-ms` it swaps one out (`del` + `put`),
which forces zero-cache through **query-transform → hydration → advance → poke → cvr-flush** — the exact
paths the server SLOs measure. `--connections` sets concurrency (prod: avg ~668 / peak ~1147 clients).

Steady-state query rate ≈ `connections × 1000/churn_ms` puts/sec (e.g. 200 × 1000/500 = ~400/s).

## Usage

**Preview the plan — no deps, no network (verify wire messages + arg coverage):**
```bash
python3 harness/replay.py --dry-run --id-pool harness/id-pool.json \
    --target wss://example/zero --extra-param userID=u1 --extra-param profileID=p1
```

**Live run (needs `pip install websockets`, a reachable zero-cache, valid auth):**
```bash
export GR_KEY='glsa_...'
python3 ../tools/gen_id_pool.py --window 24h          # -> harness/id-pool.json (real IDs)
python3 harness/replay.py \
    --target wss://<candidate-zero-cache>/zero \
    --id-pool harness/id-pool.json \
    --connections 200 --working-set 15 --churn-ms 500 --duration 600 \
    --auth-token "$JWT"            # xyne uses cookies: --cookie "user_session_id=...; google_access_token=..."
python3 ../tools/evaluate_gates.py --window 12m       # PASS/FAIL, exit code gates CI
```

Or the one-liner: `../run-art.sh --target wss://<zero-cache>/zero --auth-token "$JWT"`.

## Local sandbox (no VPN, no Grafana)

The fastest way to ART a zero-cache change is the local docker sandbox
(`xy-repo/xyne-spaces/.sandboxes/<name>/docker-compose.yml` — e.g. the custom Go build behind
`ws://rust-test.localhost/zero` via traefik). One command does everything:

```bash
../run-art-local.sh                                    # 50 conns, 3 min, reads only
../run-art-local.sh --mutations                        # + writes (sandbox DB is disposable)
../run-art-local.sh --connections 200 --duration 600   # prod-scale
../run-art-local.sh --refresh                          # re-harvest pool + clientSchema (after DB reseed)
../run-art-local.sh --lifecycle --mutations            # session churn: reconnects, resume-from-cookie, abrupt closes, zombies
../run-art-local.sh --soak --mutations                 # 20 conns / 1h / lifecycle — activates leak gates (G6)
../run-art-local.sh --users 3 --mutations              # 3 real identities round-robined across clients (write contention)
../run-art-local.sh --oracle --mutations               # + G8: differential oracle vs the TS reference (self-diff if zero-cache-ts is down)
../run-art-local.sh --chaos                            # + G10: fault injection — docker-pauses zero-cache/postgres mid-run (implies --lifecycle)
../run-art-local.sh --negative --users 2               # + G11: adversarial negative suite after the replay (2 identities enable the cross-user probe)
../run-art-local.sh --clean ...                        # purge art-% CVR rows + restart zero-cache first (cold start)
```

Every run now also:

- **samples resources** in the background (`tools/resource_sampler.py`: docker CPU/RSS, Go pprof
  goroutines/heap, CVR instance counts, every 10s → `reports/resources-<tag>.ndjson` +
  `.summary.json` with per-metric least-squares slopes; first/last heap snapshots saved as
  `.heap-{first,last}.pb.gz` for `go tool pprof -diff_base`);
- **waits 30s post-run** to observe whether zero-cache GCs departed client groups;
- **evaluates a PASS/FAIL gate** (`tools/local_gate.py`, exit code = verdict):

| gate | check |
|---|---|
| G1 connectivity | `failed_open == 0` |
| G2 errors | 0 unexpected (known sandbox drift `transformError: Query not found` excluded) |
| G3 protocol | 0 invariant violations (poke framing, pokeID matching, lmid monotonicity, undesired-hash patches) |
| G4 mutations | ≥90% acked when writes enabled |
| G5 latency | p50/p95 ≤ 1.5× `reports/local-baseline.json` (bless a good run: `tools/local_gate.py --update-baseline`); SKIPs when the run's `--connections` differs from the baseline's — latency is not comparable across concurrency shapes |
| G6 leaks | soak only (≥15 min): RSS < 200MB/h, goroutines < 300/h, heapInuse < 100MB/h slopes |
| G7 cvr-gc | WATCH-only: art client groups still growing at run end ⇒ check server GC |
| G8 diff-oracle | `--oracle` runs only: 0 mismatches + 0 connect errors in the differential oracle report |
| G9 coverage | WATCH-only: driven queries that never hydrated (`gotQueriesPatch`) — blind-spot list; usually sandbox build drift, but a delivery bug looks identical |
| G10 chaos | `--chaos` runs only: all pauses reverted + zero-cache healthy at run end |
| G11 negative | `--negative` runs only: every adversarial scenario gets the *documented* error path — any FAIL fails the gate (SKIPs allowed) |

Lifecycle mode (`--lifecycle`, auto in `--soak`) gives each client exponentially-distributed session
lifetimes (mean 45s): 70% of reconnects resume from the last `pokeEnd` cookie (`baseCookie`), 50% of
session ends are abrupt TCP aborts (no close frame), and 10% of clients become zombies that never
return — exercising resume paths, dirty-disconnect cleanup, and CVR garbage collection.

After the gate, the wrapper best-effort runs `tools/heap_diff.py` (wraps
`go tool pprof -top -diff_base` over the run's first/last heap snapshots and prints the top growth
sites + total in-use growth; needs `go` on PATH, never affects the verdict). Standalone:
`python3 tools/heap_diff.py [--tag <run-tag>] [--fail-over-mb 300]` — with `--fail-over-mb` the
exit code gates on total positive growth.

### Chaos mode (`--chaos`, `tools/chaos.py`)

Infra fault injection: `docker pause`/`unpause` of zero-cache (`pause-zc` — dead sockets, forced
reconnect/resume) and postgres (`pause-pg` — replication + CVR-write stall) at exponentially-spaced
intervals (mean 45s, 8s holds) during the replay. The wrapper arms it 20s in (so G1 stays
meaningful) and stops ≥50s before run end so recovery is observable. Containers are always
unpaused (finally-block + safety sweep + EXIT trap). Interpret chaos runs via **G1/G3/G10** —
G2/G5 noise during faults is expected (e.g. `Rehome: Reconnect required`); a real regression is an
invariant violation, a client that never recovers, or zero-cache not returning to healthy.
Standalone: `python3 tools/chaos.py --duration 120 [--actions pause-zc,pause-pg] [--mean-gap-s 45
--pause-s 8]`. First live run (2026-07-02) surfaced 8×
`Internal: newClient must match existing client` on post-pause reconnects. Root cause was the
**harness, not the server**: we omitted the optional `wsid` connect param (server defaults it to
`''`), so reconnects for the same clientID were indistinguishable and a stale queued
initConnection passed zero's wsID guard and hit its "can't happen" assert. Real zero clients send
a fresh nanoid `wsid` per connection attempt; both drivers now do too (unique `wsid` per session),
and a chaos re-run showed 0 occurrences. Lesson: protocol params that are "optional" server-side
may still be load-bearing under churn. NOTE: pausing the shared postgres briefly affects every
sandbox on the host.

### Negative suite (`--negative`, `harness/negative.py`, gate G11)

Where the replay proves the happy path scales, the negative suite proves the server fails
**correctly**: each scenario violates the sync protocol the way real clients do by accident and
asserts the *documented, structured* error comes back — not an `Internal`, a hang, a crash, or
(worst) wrong data. Pinned to rocicorp/mono zero 1.6.x code paths:

| scenario | violates | must observe |
|---|---|---|
| `cookie-on-empty-cvr` | claims a baseCookie for a group the server never saw (server-side wipe + surviving browser storage) | `ClientNotFound` |
| `stale-cookie-ahead` | reconnects with a cookie AHEAD of the CVR (corrupt storage / server restored from backup) | `InvalidConnectionRequestBaseCookie` |
| `missing-client-schema` | new client group, initConnection without clientSchema | `InvalidConnectionRequest` |
| `wrong-user-pinned-group` | user B connects to user A's client group — THE permission-leak canary | `Unauthorized` (error frame **or** close 3000 with the pin reason) |
| `reconnect-storm` | 8 near-simultaneous reconnects of the same clientID | newest socket wins, 0 `Internal` |
| `update-auth-valid` | mid-session `updateAuth` token rotation | connection survives, queries still hydrate |
| `update-auth-invalid` | `updateAuth` with a garbage token | auth error or clean close, never `Internal` |
| `ttl-purge` | reconnect after the CVR was purged for inactivity (flips `instances.deleted` like the GC) | `ClientNotFound` ("purged due to inactivity") |

`./run-art-local.sh --negative` runs it **after** the replay (so its deliberate failures can't skew
G1/G2/G5) and folds the report into the gate as **G11**. Run with `--users 2` or the cross-user
scenario SKIPs; ttl-purge needs the postgres container (the wrapper passes it automatically).
Standalone:

```bash
python3 harness/negative.py --target ws://rust-test.localhost/zero \
    --id-pool harness/id-pool.sandbox.json --client-schema harness/client-schema.json \
    --auth-pool harness/auth-pool.json \
    --pg-container xyne-sandbox-postgres --pg-db sandbox_rust_test_db \
    --cvr-schema "sandbox_rust_test_0/cvr" [--only reconnect-storm,ttl-purge]
```

First full run vs the Go build (2026-07-02): **8/8 PASS**. Getting there surfaced three
harness-side traps worth knowing (the server was right every time):

- **Forged cookies must be valid Lexi encodings.** A cookie's stateVersion is LexiVersion-encoded
  (`types/lexi-version.ts`: one base36 char declaring length-1, then base36 digits). The server
  *parses* baseCookie (`versionFromString` → `versionFromLexi`) **before** the semantic
  `checkClientAndCVRVersions` comparison — a malformed cookie (e.g. `"7zzzzzzz"`, which declares 8
  digits but carries 7) dies in the codec's assert and surfaces as `Internal`, masking the
  structured error you're testing for. `negative.py` ports the codec (`version_to_lexi`/`lexi_bump`).
- **"Ahead" must out-run the live watermark.** The replica's stateVersion tracks the PG LSN and
  advances continuously under background traffic; leg-1's learned cookie may also be a pre-hydration
  `00:xx` config-poke cookie. A small bump lands *below* the live DB version — which is a perfectly
  valid stale cookie (normal catchup, no error). The suite bumps by 36^12.
- **ttl-purge must wait out the view-syncer keepalive** (`DEFAULT_KEEPALIVE_MS` 5s, ~10s worst
  case) after flipping `deleted=true`: reconnecting sooner reaches the still-alive service's
  in-memory CVR snapshot and syncs fine. The deleted flag is only honored on a fresh cvr-store
  load — which is also the only state the real GC purges in.

### Differential correctness oracle (`harness/diff_oracle.py`)

Catches the bug class nothing else can: a candidate build silently returning **wrong or missing
rows** while latency and error gates stay green. Same DB + same queries + same auth ⇒ identical
converged state, regardless of implementation. Each logical client opens one socket per side, sends
byte-identical `initConnection`/`changeDesiredQueries` to both, materializes each side's `rowsPatch`
stream into `state[table][primaryKey] → row` (primary keys come from the clientSchema), quiesces,
then diffs the converged row-sets. Pokes are never compared one-to-one — batching/ordering legally
differ.

```bash
# 1. Self-diff smoke test (both sockets on the SAME server — must be 0 mismatches;
#    proves the comparator has no false positives). PASSED 2026-07-02.
JWT=$(python3 -c "import json; print(json.load(open('harness/auth-pool.json'))[0]['token'])")
UID_=$(python3 -c "import json; print(json.load(open('harness/auth-pool.json'))[0]['userID'])")
python3 harness/diff_oracle.py \
    --primary ws://rust-test.localhost/zero \
    --id-pool harness/id-pool.sandbox.json --client-schema harness/client-schema.json \
    --auth-token "$JWT" --extra-param userID=$UID_ \
    --pairs 2 --duration 30 --quiesce-s 15

# 2. Real oracle run: TS reference vs Go candidate (needs the TS sandbox, see below)
python3 harness/diff_oracle.py \
    --primary ws://rust-test.localhost/zero \
    --mirror  ws://rust-test.localhost/zero-ts \
    ... same flags ...
```

Verdict: exit 0 = converged states identical; exit 1 = mismatches, with per-table counts
(`only_primary` / `only_mirror` / `value_mismatch`) and example rows in `reports/diff-<ts>.json`.
Reads-only by default. Add `--enable-mutations --i-know-this-writes [--mutations-per-min 6]` to
also drive read-tracking writes on the primary socket — they land in the shared upstream DB and
replicate to BOTH caches, extending coverage from hydration-only to the advance/invalidation poke
paths (converged states must still match). Validated 2026-07-02: 3 pairs, 15/15 mutations acked,
0 mismatches.

Quiesce is **adaptive**: `--quiesce-s` is the required poke-quiet time on BOTH sides before
diffing, capped at `--quiesce-max-s` (default 120; pairs that never go quiet are flagged with a
WARN). A fixed sleep is not a convergence check — a loaded/slow side still mid-hydration at diff
time produces false `only_primary` mismatches (seen live when the Go build was saturated at 246%
CPU). If the oracle FAILs, check both containers' CPU first; a genuinely wrong build shows
`value_mismatch` or persistent one-sided rows on an idle system. Other key flags: `--pairs`
(logical clients), `--duration`, `--zipf-s`.

The oracle is also wired into the local gate as **G8**: `./run-art-local.sh --oracle` runs it after
the replay (vs `zero-cache-ts` if that container is up, else self-diff), saves
`reports/diff-<tag>.json`, and `tools/local_gate.py --oracle <report>` folds the verdict into the
PASS/FAIL. With `--mutations` the oracle run inherits mutation mode.

**TS reference infra (LIVE since 2026-07-02):** `zero-cache-ts` in the rust-test
`docker-compose.override.yml` — upstream `rocicorp/zero:1.6.1`, same upstream DB, but
`ZERO_APP_ID=sandbox_rust_test_ts` (own CVR/CDC schemas + replication slot `sandbox_rust_test_ts_0_a`
— zero collision with the Go build), routed via traefik at `/zero-ts` (router priority 25 — it must
beat the `/zero` router's 20 because `PathPrefix(/zero)` also matches `/zero-ts`). Bring it up with
`docker compose up -d zero-cache-ts`; first boot does an initial sync (~1 min on the sandbox DB).
First real run: 3 pairs, 422–658 rows/pair, **0 mismatches** — Go and TS converge identically.

It auto-discovers, in order:

1. **JWT secret** — `ZERO_AUTH_SECRET` from the backend container env.
2. **Identity** — the DB user with the most `channel_user_status` rows (mutators enforce channel
   participation), joined to `org_members` for the `memberId` claim the deployed
   `extractAuthDataFromJWT` requires. Override with `--user-id`.
3. **JWT** — `mint_local_jwt.py` (HS256, `sub/email/name/memberId/workspaceId`, `iss=xyne`,
   `aud=xyne-user`). Passed as `--auth-token`; the connect URL also gets `userID=<sub>`.
4. **id-pool** — `gen_id_pool_db.py` harvests real IDs from the sandbox Postgres, scoped to that user.
5. **clientSchema** — newest non-null `clientSchema` from the CVR `instances` table
   (`"sandbox_<slug>_0/cvr"`). zero-cache rejects `initConnection` from a new client group without it;
   if the CVR is empty, open the sandbox app once in a browser first.

Local-sandbox gotchas learned the hard way:

- **The verdict is the driver summary**, not Grafana — nothing scrapes the sandbox zero-cache (the Go
  build only exposes pprof on `:6060`). Watch `errors`, `rehomes`, mutation `ok/sent`, and
  `client_latency_ms` percentiles in `reports/run-*.json`.
- **Rate limiter**: the backend caps zero query/mutate calls per user per minute
  (`zeroRateLimiter.ts`, `ZERO_MAX_REQUESTS`, default 300). All synthetic clients share one user, so
  the sandbox compose sets `ZERO_MAX_REQUESTS=100000` — without it every transform 429s.
- **Recreating the backend stops zero-cache** (compose dependency). `docker compose up -d zero-cache`
  after any backend change.
- **Rehome is normal**: zero-cache rebalances client groups across syncer workers and asks clients to
  reconnect. The driver reconnects with the same clientGroupID/clientID (tracked as `rehomes`/
  `reconnects`, not errors).
- **The Go build sends no `pushResponse`** — mutation acks are counted from `lastMutationIDChanges`
  in pokes.
- **`transformError: Query not found`** for a handful of queries (`ticketByIdV2`,
  `userChannelSections`, `kanbanTicketsPage`, ...) is genuine drift between the prod-derived baseline
  and the sandbox build's query set — expected, not a harness bug.
- **CVR garbage collection lags**: departed `art-%` client groups linger in
  `"sandbox_<slug>_0/cvr".instances` (observed 294 stale groups keeping an *idle* zero-cache at 44%
  CPU). G7 watches for this; `--clean` purges them
  (`DELETE FROM "...cvr".instances WHERE "clientGroupID" LIKE 'art-%'`) and restarts zero-cache.
- **Host-mounted dev TS files must be complete**: the sandbox compose mounts individual files from
  the mono working tree into the container. If a mounted file gains a new import (e.g.
  `go-ivm-client.ts` → `napi-records.ts`), the syncer dies on boot with `ERR_MODULE_NOT_FOUND` —
  add the new file to `docker-compose.override.yml` mounts.

Reference numbers from the rust-test sandbox (Go zero-cache, 2026-07-02): 10 conns → p50 69ms /
p95 2.9s, all mutations acked, 0 errors. 50 conns → the Go build saturates (84% CPU, hydration p50
26s), which is exactly the kind of finding this exists to catch.

### Key flags (`replay.py --help`)

| flag | meaning |
|---|---|
| `--target` | zero-cache ws/wss base (xyne: end in `/zero`) |
| `--connections` / `--working-set` / `--churn-ms` / `--duration` | load shape |
| `--auth-token` \| `--cookie` | JWT (zbugs-style) or session cookies (xyne) |
| `--extra-param k=v` | extra connect-URL params (xyne needs `userID`, `profileID`) |
| `--enable-mutations --i-know-this-writes` | ALSO drive read-tracking writes (both flags required; see caveat 3) |
| `--mutations-per-min` | write rate per client when enabled (default 4) |
| `--no-post-handshake` | send initConnection in the header instead of after open (small working sets only) |
| `--lifecycle` | session churn mode (see above); tune with `--session-mean-s / --zombie-pct / --abrupt-pct / --resume-pct` |
| `--auth-pool file.json` | JSON array of `{token, userID}` round-robined across clients (multi-user) |
| `--dry-run` | print plan + sample wire messages, exit (no deps) |

## Auth

- **zbugs-style**: mint a PS256 JWT (`sub` = userID) — see `apps/zbugs/api/index.ts` + `npm run
  create-keys`. Pass `--auth-token`.
- **xyne**: the browser sends session **cookies** on the WS upgrade; zero-cache validates them. Grab a
  real logged-in cookie and pass `--cookie`, plus `--extra-param userID=... --extra-param profileID=...`.

## Important caveats

1. **Point the id-pool at the environment you test.** `gen_id_pool.py` harvests **prod** IDs; they only
   resolve against the same DB. For a fresh staging DB, seed the pool from that DB (or run against a
   prod-shaped replica). ~4% of args (`filters`, `dynamicFieldDateRanges`, RCA queries) don't resolve
   from telemetry alone — the harness reports which; add them to `id-pool.json` `scalars`/`ids` if you
   need those specific queries.
2. **Server histograms are the verdict, not the driver's client latency.** The driver's per-query
   latency is best-effort (attributed via `pokePart.rowsPatch.queryHash`). The authoritative signal is
   `evaluate_gates.py` scraping `zero_sync_*` — same as the baseline. (See ART.md §5.)
3. **Mutations are OFF by default — they write real data.** Enabling requires BOTH
   `--enable-mutations` and `--i-know-this-writes`. What's wired (protocol-v49 custom-mutator
   `push`, pushVersion 1, per-client mutation ids from 1): `channel.markChannelAsViewed` and
   `activities.markThreadActivitiesAsReadV2` — together ~78% of prod write volume (weights
   renormalized from the baseline). Signatures verified against the Xyne app's
   `dashboard/src/zero/mutators.ts`. Blast radius: they update the **authenticated user's own**
   `channel_user_status` (lastViewedAt/unreadCount), mark their activities read, and upsert one
   **deterministic** empty draft row per touched channel/conversation (`artdraft<hash>` ids, so
   re-runs reuse rows instead of accumulating). Still: **disposable/staging envs only.**
4. **`encode_sec_protocols` is vendored** from `packages/zero-protocol/src/connect.ts`. If that file or
   `PROTOCOL_VERSION` changes in a mono upgrade, update `workload.py`/`replay.py` (`--protocol-version`
   overrides the default 49).
