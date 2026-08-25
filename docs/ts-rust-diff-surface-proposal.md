# TS ⇄ Rust full-breadth differential surface — proposal (post rust-cvr/rust-syncer)

**Context.** The ART gates were built when only the IVM was at risk: G8 (diff-oracle)
materializes each side's `rowsPatch` stream and diffs the **converged app row-sets** —
deliberately ignoring pokes, CVR internals, and per-op latency deltas. Now the CVR
store and the whole syncer are Rust too, so the sync engine's **own state** is
producible by both images and everything it persists or emits is diffable. This doc
enumerates the whole diffable breadth, in priority order, with gate numbers continuing
from G31.

**Two run modes** (both reuse the existing sandbox: TS image on :4849, Rust on :4848,
same upstream PG; the encryption-proxy mirror can tee identical client traffic):

- **L (lockstep-deterministic):** one scripted client per side, identical op sequence,
  quiesce between steps. CVR version strings then align 1:1 → *byte-level* diffs.
- **W (production workload):** the existing weighted ART workload on both sides →
  *normalized* diffs (sets, not sequences; versions compared via bijective remapping).

---

## Tier 1 — state the engine persists (highest signal per line of harness code)

### G32 · CVR Postgres schema differential (mode L, then W)
Both images write the same `{app}_{shard}/cvr` schema. After an identical session,
dump both CVR schemas and canonically diff:

| Table | Compare | Normalization |
|---|---|---|
| `instances` | version, replicaVersion, clientSchema, profileID | ttlClock/lastActive within tolerance (W); exact version only in L |
| `clients` | full rows | — |
| `queries` | queryHash, clientAST, transformationHash, patchVersion, deleted, internal, **rowSetSignature** | transformationVersion via version-remap (W) |
| `desires` | (clientID, queryHash), deleted, ttlMs, inactivatedAtMs | patchVersion via remap (W) |
| `rows` | rowKey, rowVersion, refCounts, tombstone presence | patchVersion via remap (W) |
| `rowsVersion` | lockstep with instances.version | — |

This one gate end-to-end covers most of what the unit-level parity ledgers pin
(received/refCounts merge, flush pruning, tombstone retention, desire lifecycle) —
against the *real* images. The `flush_pg_test` DDL + dump code in
`packages/rust-cvr/agentic/parity/` is reusable for the canonical dump.

### G33 · rowSetSignature cross-impl equality (sharpest single invariant)
Join both sides' `queries` on (queryHash, transformationHash): **signatures must be
equal**. Same query + same data ⇒ same row set ⇒ same signature — this single value
transitively pins the hydrate row-set, rowIDString canonicalization, h64, and CVR
persistence in one check. (The FxHasher-vs-h64 bug this year would have been a
one-line FAIL here; instead it needed a manual hunt.) Cheap enough to run inside G32
but worth its own verdict line because a mismatch means *silent* cross-impl drift
that mass-rehydrates on rolling deploys.

### G34 · Catchup/reconnect differential
After a session, reconnect to each side with the same **older** cookie (and a
mid-stream cookie, and a cookie from before a table-wide change):
compare the catchup patch **set** (row put/del + got/desired query patches +
deletedClients). Exercises `catchupRowPatches`/`catchupConfigPatches` end-to-end —
the tombstone-retention and spurious-del class of bugs. In L mode also assert both
sides used the same number of pokes ≤ N.

## Tier 2 — the wire

### G35 · Poke-stream differential (mode L only)
G8 deliberately never compares pokes. In lockstep mode we can: per poke boundary,
compare pokePart **content as sets** — rowsPatch (set; in-poke row order is a
registered deliberate divergence D-6), lastMutationIDChanges, desiredQueriesPatches,
gotQueriesPatch, pokeStart/pokeEnd cookie pairing and monotonicity. Catches
poke-assembly divergences (lmid routing, patch routing to the right sub-table,
baseVersion dropping) that converged-state diffing masks.

### G36 · Error-semantics A/B (upgrade of G11 negative)
G11 currently checks the candidate behaves sanely. Upgrade: fire the identical
adversarial input at **both** sides — forged/garbage cookie, cookie > 2^53
configVersion, invalid AST, unknown protocol version, oversized payload, wrong-user
token, revoked token mid-session — and diff `ErrorKind` + error-body **shape**
(message text differences are registered exceptions). Pins the recoverable-vs-fatal
boundary that decides whether clients reset or retry.

### G37 · Inspect protocol differential
Same `inspect` queries op at both sides → normalized row diff (ttl/rowCount fields
with tolerance). The unit fixture exists (`inspect_pg_test`); this runs it against
the real images with real data.

## Tier 3 — lifecycle & ops

### G38 · Query TTL / expiry differential
Short-TTL desired queries, disconnect the client, wait past TTL on both sides:
compare which queries expire (desires.deleted / got-del patches) and that
`instances.ttlClock` advances on **both** (the standalone 60s updateTTLClock
persistence is new in Rust — this is its end-to-end pin, plus the reload-after-
restart TTL deferral behavior).

### G39 · deleteClients / client-GC differential
Drive `deleteClients` + client-group GC flows → compare CVR cleanup (clients/desires
rows gone, queries de-desired) and the `deletedClients` fields in pokes.

### G40 · Drain/rehome state differential
SIGTERM both, then dump CVR: ownership released the same way (owner/grantedAt),
ttlClock synced, no partial flush artifacts, both sides' clients reconnect to a
fresh instance with only a catchup (no full rehydrate). Bounded-drain (25s cap) is
a registered exception D-1 — gate on end-state, not timing.

### G41 · Metrics-surface parity
Scrape both metrics endpoints under load and diff the **instrument name sets** and
which fire (presence, not values): `zero.sync.*` histograms/counters,
cvr.load_attempts / flush_attempts / poke.* etc. Catches the missing-telemetry class
(F-RRC-4) automatically on every gate run instead of by audit.

## Latency — paired A/B, not just SLO

### G42 · Whole-workload latency A/B
G5/G5b/G25 gate the candidate against the **prod baseline** (absolute). Add a
**paired** comparison: same box, same PG, same workload seed, alternating trials
(the `ab_channel_latest.py` pattern generalized to the full weighted workload):

| Op class | Measure |
|---|---|
| connect → `connected` | handshake |
| initConnection → first gotQueriesPatch | cold hydrate |
| changeDesiredQueries put → pokeEnd | incremental hydrate (per query type, weighted) |
| upstream commit → pokeEnd on subscribed client | e2e serving lag (rust exports `zero.sync.e2e_serving_lag`; sample TS the same way from the harness side) |
| push mutation → ack → poke | write round-trip |
| reconnect(cookie K ops behind) → caught up | catchup, as f(K) |

Gate: rust pX ≤ TS pX × factor per class (defaults: 1.5× p50, 2× p95 — tighten
after first bless), plus report the win-ratio table. This makes "rust is faster"
a *regression-gated* claim instead of a one-off knee experiment.

---

## Suggested build order

1. **G32+G33** (CVR schema + signature) in L mode — biggest new surface, mostly
   dump+canonicalize code, reuses parity-fixture DDL. Then W mode with remapping.
2. **G34** catchup (reuses G32's dump for the cookie source).
3. **G42** latency A/B (generalize `ab_channel_latest.py`; the workload sampler
   already exists).
4. **G35/G36** wire + errors (needs the lockstep client — shared with G32-L).
5. **G37–G41** lifecycle/metrics (small, mostly independent).

**Known normalization exceptions to encode once, centrally** (from
`mono/parity/PARITY-EXCEPTIONS.md`): D-6 in-poke row-patch order; error Display
texts; ttlClock ms precision (i64 vs double); `cvr.flush_attempts{flush.type=async}`
absent in Rust (D-7); drain hard-cap timing (D-1).
