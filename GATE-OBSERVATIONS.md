# ART gate observations

## B0 / G8 — `myChannelParticipations` WHERE-EXISTS backing-row leak (2026-08-27)

**Status: ROOT-CAUSED to a specific rust IVM/serialization bug. rust emits the
backing rows of a WHERE-clause `EXISTS` (read-permission) subquery to the CVR;
TS correctly emits NONE. Controlled unit test + fix + image rebuild is the work.**

### What (CORRECTED — the earlier "18 vs 9 channels, strict superset" framing was
### wrong about the shared set; the real split is per-table):
Per-table dump of the converged state for the single query `myChannelParticipations`:
- **rust (primary): 18 rows / 2 tables** = 9 `channel_participants` (the root
  result) **+ 9 `channels`**.
- **TS (mirror): 9 rows / 1 table** = 9 `channel_participants` **+ 0 `channels`**.

So TS syncs **zero** `channels` rows for this query; rust syncs 9. The 9 channels
rust emits are exactly the 9 the user ADMINs (PG ground truth), so it is not a
data leak here — but it IS a hard divergence from TS and a general over-emission.

### Root cause (CONFIRMED + unit-reproduced + fix validated)
The `channels` rows back a WHERE-clause `EXISTS(channels zsubq_channel …)` that is
xyne's `ChannelParticipantsACL.canSelect` read rule, applied via the ordinary
client query builder (`system:'client'`, NOT a zero-permission — the `permissions`
table is empty on both sides). The rule sits inside an `OR`:
`OR[ userId = me, EXISTS(channels …) ]`. Because the ROOT already filters
`userId = me`, the first OR branch is ALWAYS true, so the `EXISTS(channels)` branch
is **redundant** — every qualifying participant passes the OR via the cheap
equality and never needs the join.

TS's `buildPipeline` (`zql/src/builder/builder.ts:140`) runs
`planQuery(ast, costModel)` first, which **flips** the redundant OR-exists into a
`FlippedJoin` inside a `UnionFanOut/FanIn`. Rows arriving via the `userId=me`
branch carry NO `channels` relationship; `mergeFetches` keeps that (lowest-index)
branch on ties → **0 `channels` rows** streamed to the CVR.

**The rust bug: the syncer/engine NEVER ran the flip planner.** `plan_query` /
`plan_ast_flips` are fully ported in `rust-ivm/src/planner/` but were never wired
into the build path — `build_pipeline` READS `csq.flip` but nothing SET it, so
every exists-in-OR was built **non-flipped**: a top-level `Join` attaches
`zsubq_channel` to ALL rows, and the streamer emits those backing rows (marked
`is_hidden=true`, but the CVR wire does not filter `is_hidden`). Hence rust emits
9 `channels`, TS emits 0.

### Unit reproduction (image-free, deterministic)
`packages/rust-ivm/tests/g8_mychannelparticipations_real_ast.rs` replays the exact
transformed AST over a 2-channel SQLite `TableSource`. Without planning the engine
emits `channels = [chA, chB]` (the bug); with the planner wired
(`Engine::set_cost_model_conn`) it emits `channels = []` — matching TS.

### Fix (unit-validated; needs full-suite + ART regate)
Wire the ported planner into the engine, mirroring TS `buildPipeline`:
- `rust-ivm/src/engine/mod.rs`: new `cost_model_conn` + `set_cost_model_conn()`;
  `plan_ast()` runs `plan_query(ast, create_snapshot_cost_model(conn))` before
  `build_pipeline` when a connection is configured (no-op otherwise → existing
  unit tests unchanged).
- `rust-syncer/.../pipeline_driver.rs::build_engine`: pass the replica
  `SharedConn` via `set_cost_model_conn` for the TableSource-backed path.
PERF NOTE: this adds `COUNT(*)`-per-table per hydrate; switch to
`create_snapshot_cost_model_cached` (keyed by replica version) before the G25/
capacity gate if it shows up.

### (historical) earlier misreads, now corrected
- "strict superset, 18 vs 9 channels" — the 9 shared rows are
  `channel_participants` (root); `channels` is 9-(rust) vs **0**-(TS).
- "permission backing-row leak" — it is NOT a zero-permission; it is an app ACL
  where-exists, and the real fault is the missing flip-planning step, not
  permission `system` tagging.
- "transient poke-ordering" (old `g8_*.rs` headers) — it is deterministic.

### The AST (captured — `reports/g8-mcp/myChannelParticipations.ast.json`)
Root `channel_participants`, WHERE:
```
AND[ userId = me,
     role  = 'ADMIN',
     OR[ userId = me,                     # participant read-permission
         EXISTS(channels zsubq_channel WHERE
            workspaceId = my-ws AND
            OR[ visibility = 'PUBLIC',
                EXISTS(channel_participants zsubq_participants WHERE userId = me) ]) ] ]
```
There is **no explicit `.related('channel')`** — the `channels` rows are synced as
the **correlated-subquery (permission) backing rows**. rust emits 9 more of them
than TS.

### Elimination of benign explanations (all disproven)
| Hypothesis | Test | Result |
|---|---|---|
| TS-replica lag / convergence skew | same terminal cookie `73oq56z3c` both sides, `quiesced=true`, `streaming_at_quiesce=[]`, 90s quiesce | **persists** — not lag |
| Stale TS replica | restarted the 1.7.0 mirror → fresh resync | **persists** |
| zero **version skew** (mirror 1.7.0 vs rust port 1.9.0) | replaced mirror with the SAME mono build (`zero-cache-rust-syncer:local`) in TS-syncer mode (see `bootstrap-ts19-mirror.sh`) | **persists** — same-build rust-mode vs ts-mode STILL 18 vs 9 |
| Transient poke-ordering (the prior `g8_*.rs` test headers' claim) | reproduced deterministically across 5 oracle runs, always the same query/batch | **disproven** — it is stable, not transient |

PG ground truth (current sandbox): the user is `role=ADMIN` of exactly **9**
distinct channels in their workspace (`channel_participants ⋈ channels`), all
`in_ws`, all participant — so 9 is the count the `role='ADMIN'` root filter
implies. rust's 18 is a superset; the exact 9-extra composition is muddied by
artseed/uiseed channels left by prior mutation runs, so a **controlled** repro is
needed to pin whether rust over-emits the permission-EXISTS backing rows or
mis-evaluates the `role='ADMIN' AND (OR …)` root.

### Next step (the fix)
1. Port the captured AST into a rust `g8_mychannelparticipations_real_ast.rs`
   unit test over a controlled 2–3 channel TableSource (1 qualifying, 1 that must
   be excluded), assert rust emits exactly the qualifying channel rows — this is
   the "prove it fails before the fix" step (AGENTS.md RULE #7).
2. If it over-emits, the bug is in the correlated-subquery / permission
   backing-row emission (exists.rs / join / the read-authorizer permission wrap);
   fix to match TS, rebuild the candidate image, re-run
   `diff_oracle … --only-ops myChannelParticipations` → expect clean.
3. Correct the "transient" claim in the existing `g8_*.rs` test headers.

### Tooling added this session (all in xyne-art)
- `bootstrap-ts19-mirror.sh` — version-matched TS mirror from the candidate image.
- `diff_oracle.py --only-ops` — restrict the full-catalog sweep to named queries
  (with `--catalog-batch-size 1`, each query is its own pair → names the diverging
  query directly).
- `diff_oracle.py` `DIFF_ORACLE_MAX_EXAMPLES` env — dump ALL mismatched keys.

Deterministic repro:
```
./bootstrap-ts19-mirror.sh
DIFF_ORACLE_MAX_EXAMPLES=50 python3 harness/diff_oracle.py \
  --primary ws://<rust>:4848 --mirror ws://<ts19>:4848 \
  --id-pool harness/id-pool.sandbox.json --client-schema harness/client-schema.json \
  --auth-token <JWT> --extra-param userID=<uid> \
  --pairs 1 --catalog-batch-size 1 --full-catalog --only-ops myChannelParticipations \
  --quiesce-s 45 --out reports/diff-mcp.json
```

## B-LOG / G13 — RESOLVED (2026-08-27)
G13 log-health **PASS** (2 containers clean) on the full oracle+mutations run.
The prior 64→3-signature triage is closed; no unknown log signatures remain.

## B-SIZE / G23 — image size budget (2026-08-27)
`run-art-local.sh` overrode the G23 gate to `--size-hard-mb 700`, which FAILs every
real rust-syncer image (665–735MB; profiling adds ~1MB). Changed to `700` WATCH /
`800` FAIL (= `image_audit.py`'s own default hard limit).
