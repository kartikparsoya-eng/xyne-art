"""
temporal_slow.py — L5 ART gate G-slow (connect-ack hydrate-independence).

Reproduces the prod bug-1 pathology as a gate: a client group whose hydrate is
expensive, plus a reconnect storm landing mid-hydrate. Asserts the connect-ack
(`connected` frame) latency is INDEPENDENT of hydrate cost on BOTH TS and rust,
and that the two sides produce equal frame sequences.

Before the fix, rust queued `connected` on the serial CG thread behind
config_and_hydrate, so a reconnect arriving mid-hydrate was acked only after the
(multi-second) hydrate → 10s client connect-timeout → reap → cold-rehydrate
thrash. This gate would have caught it: rust's connect-ack p99 would blow past
TS's while both hydrate-heavy.

Prereqs (see RUN.md): a live TS+rust cache pair with a client group that hydrates
non-trivially. The "slow" is naturally provided by a heavy catalog; optionally
point SLOW_INIT at a desiredQueriesPatch that hydrates a large query.

Env:
  ART_SLOW_STORM   number of storm reconnects (default 20)
  ART_ACK_P99_MS   max acceptable connect-ack p99, ms (default 2000)
"""
from __future__ import annotations

import asyncio
import os
import sys

from ab_common import make_sides, open_side, reader, Report, Side
from frame_sequence_oracle import compare_sequences, ack_class

STORM = int(os.environ.get("ART_SLOW_STORM", "20"))
ACK_P99_MS = float(os.environ.get("ART_ACK_P99_MS", "2000"))
SLOW_INIT = ["initConnection", {"desiredQueriesPatch": []}]


def p99(xs: list[float]) -> float:
    if not xs:
        return float("inf")
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(0.99 * (len(s) - 1))))]


async def storm_side(name: str, template: Side) -> list[float]:
    """Open STORM connections concurrently to the same target/group as `template`
    (simulating a reconnect storm), return their connect-ack latencies."""
    async def one(i: int) -> float | None:
        s = Side(name, template.target, template.cvr_schema, template.container)
        s.cgid, s.cid = template.cgid, f"storm-{i}-{template.cid}"
        try:
            await open_side(s, "", SLOW_INIT)
        except Exception:
            return None
        return s.connected_ms

    got = await asyncio.gather(*(one(i) for i in range(STORM)))
    return [x for x in got if x is not None]


async def main() -> int:
    rep = Report("reports/g-slow.json")
    rust, ts = make_sides()
    # Prime each group with a hydrating client, then storm-reconnect into it.
    try:
        await asyncio.gather(open_side(rust, "", SLOW_INIT),
                             open_side(ts, "", SLOW_INIT))
    except Exception as e:
        rep.add("G-slow", "SKIP", f"sandbox not reachable: {e!r}")
        return rep.finish()

    stop = asyncio.Event()
    readers = [asyncio.create_task(reader(rust, stop)),
               asyncio.create_task(reader(ts, stop))]

    rust_acks, ts_acks = await asyncio.gather(storm_side("rust", rust),
                                              storm_side("ts", ts))
    await asyncio.sleep(3.0)
    stop.set()
    await asyncio.gather(*readers, return_exceptions=True)

    verdict = "PASS"
    details = []
    for label, acks in (("rust", rust_acks), ("ts", ts_acks)):
        pv = p99(acks)
        cls = ack_class(pv)
        details.append(f"{label}: n={len(acks)} ack-p99={pv:.0f}ms ({cls})")
        if pv > ACK_P99_MS:
            verdict = "FAIL"
            details.append(f"  !! {label} connect-ack p99 {pv:.0f}ms exceeds "
                           f"{ACK_P99_MS:.0f}ms — hydrate serialized the ack (bug-1)")
    seq = compare_sequences(rust, ts)
    if seq:
        verdict = "FAIL"
        details.append("frame-seq: " + "; ".join(seq))
    rep.add("G-slow", verdict, " | ".join(details))
    return rep.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
