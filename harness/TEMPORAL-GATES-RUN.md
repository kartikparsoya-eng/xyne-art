# L5 Temporal ART Gates — Run Guide

These three gates add **time-space** differential testing to the ART suite. The
existing diff-oracle compares result-set VALUES and is time-blind — it cannot see
"the connect-ack arrived 254s late" (prod bug-1) or "a push 401'd after the token
expired" (prod bug-2). Both prod bugs passed every value-space gate.

| Gate | File | Catches | Contract (INVENTIONS.md) |
|---|---|---|---|
| **G-slow** | `temporal_slow.py` | bug-1: connect-ack serialized behind hydrate | I-1: `connected` must be hydrate-independent |
| **G-ttl** | `temporal_ttl.py` | bug-2: stale connect-time auth on relayed pushes | I-3: relay uses CURRENT auth, refreshed on updateAuth |
| **frame-seq oracle** | `frame_sequence_oracle.py` | ordering / over-emission divergence over time | I-1: same ordered frames + ack latency class |

## Why they are scripts, not a live-run result here

A full ART cycle needs the sandbox topology up: the candidate **rust** image + a
**TS** zero-cache built from the same catalog, a seeded Postgres with CVR slots,
and the mirror wiring (`bootstrap-ts19-mirror.sh`, `run-art-dual.sh`). That is a
multi-hour infra operation, not an inline step. These gates are written against
the shared `ab_common.py` harness so they run exactly like the existing gates
once the pair is up. Each gate **SKIPs with a clear message** if the sandbox (or,
for G-ttl, the signing secret) is unreachable — it never false-passes.

## Prerequisites

1. Bring up the TS+rust pair (same as the diff-oracle):
   ```bash
   ./bootstrap-ts19-mirror.sh          # TS side + mirror
   ./run-rust-syncer-release.sh <img>  # candidate rust image
   ```
   `ab_common.py` reads `RUST_WS` / `TS_WS` / `*_CVR_SCHEMA` / `*_CONTAINER` from
   the environment (see `harness/README.md`).
2. For **G-ttl** only: export the sandbox HS256 signing secret so the gate can
   mint a short-TTL token:
   ```bash
   export ZERO_AUTH_SECRET="$(the sandbox JWT_SECRET / ZERO_AUTH_SECRET)"
   export ART_TTL_SUB="<a valid userID/sub>"   # else taken from auth-pool[0]
   ```

## Run

```bash
cd harness
python3 frame_sequence_oracle.py      # per-client ordered frames + ack class
python3 temporal_slow.py              # G-slow: ART_SLOW_STORM=20 ART_ACK_P99_MS=2000
ZERO_AUTH_SECRET=... python3 temporal_ttl.py   # G-ttl: ART_TTL_SECONDS=8
```

Each writes a `reports/*.json` verdict via the shared `Report` class and exits
non-zero on FAIL (SKIP is exit 0 with a reason — wire into `run-art.sh` the same
way as the other gates, treating SKIP as "infra absent", not "passed").

## What each asserts

- **G-slow**: primes a hydrating group on both sides, fires `ART_SLOW_STORM`
  concurrent reconnects into it, and asserts the connect-ack **p99 ≤
  ART_ACK_P99_MS on BOTH** sides (hydrate-independent) and that the frame
  sequences match. Regression form of bug-1: rust's ack-p99 would blow past TS's.
- **G-ttl**: mints a JWT with TTL `ART_TTL_SECONDS` (< session), connects both
  sides, lets it EXPIRE, refreshes via `["updateAuth",{"auth":<fresh>}]`, drives
  post-refresh work, and asserts **zero** Unauthorized/AuthInvalidated/401 frames
  on both. Regression form of bug-2: rust would 401 the post-refresh relayed op.
  (Substitute a real mutation push for the seeded schema where noted in the file
  to exercise the actual push-relay path end-to-end.)
- **frame-seq oracle**: collapses each poke CYCLE to one token (so an EXTRA cycle
  on one side — the G8 over-emission class — is caught) and diffs the ordered tag
  sequence + connect-ack latency class, TS vs rust.

## Pure-logic self-check (no sandbox)

The oracle's sequence/ack-class logic and G-ttl's HS256 mint are stdlib-only and
were unit-smoked (two poke cycles → two tokens; an extra cycle flags; a slow ack
flags bug-1; mint round-trips its own HMAC signature + `sub`). The end-to-end
verdicts require the live pair.
