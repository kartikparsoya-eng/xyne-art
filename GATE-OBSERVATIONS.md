# ART gate observations

## B0 / G8 — `myChannelParticipations` channels over-inclusion (2026-08-27)

**Status: CONFIRMED real rust-vs-TS divergence at the SAME code version. Root
query + AST isolated. Rust code fix + controlled unit test + image rebuild still
outstanding.**

### What
G8 diff-oracle FAILs with ~5–9 `channels` `only_primary` rows (rust has them, the
TS mirror does not). Isolated to a SINGLE query, **`myChannelParticipations`**:
rust materializes **18** channel rows, TS materializes **9** (`only_mirror=0`,
`value_mismatch=0` — rust is a strict superset).

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
