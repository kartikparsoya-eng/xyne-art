"""
frame_sequence_oracle.py — L5 temporal differential oracle (per-client frame
ordering + latency-class equivalence, TS vs rust).

The existing diff-oracle (diff_oracle.py) compares result-set VALUES; it is
time-blind (it could not see "connected arrived 254s late" — prod bug-1). This
oracle compares, per client, the ORDERED sequence of downstream frame *types*
(connected, pokeStart/pokePart/pokeEnd, error, pong) plus a coarse latency class
for the connect-ack, and diffs TS against rust.

Contract pinned (INVENTIONS.md I-1): both sides must emit the SAME ordered frame
tag sequence, and the connect-ack must land in the same coarse latency class
regardless of hydrate cost (bug-1 would put rust's `connected` in a much slower
class than TS's).

Stdlib + the shared ab_common harness only. Import and call `frame_signature`
/ `compare_sequences` from a gate, or run standalone against a live sandbox.
"""
from __future__ import annotations

from typing import Any

# Coarse connect-ack latency classes (ms). The point is hydrate-INDEPENDENCE:
# the ack should be "instant" on both sides even when hydrate is slow. Any client
# landing in SLOW/TIMEOUT is a bug-1 signature.
ACK_CLASSES = [
    ("instant", 250.0),
    ("fast", 1000.0),
    ("slow", 5000.0),
    ("timeout", float("inf")),
]

# Each poke CYCLE (pokeStart -> pokePart* -> pokeEnd) collapses to a single
# "poke" token: pokePart count can legitimately differ by batching, but the
# NUMBER of cycles must not (an extra cycle on one side is the G8 over-emission
# class). We emit one "poke" per pokeStart and drop pokePart/pokeEnd, so two
# distinct cycles remain two tokens. Set COLLAPSE_POKES=False for exact frames.
COLLAPSE_POKES = True
_POKE_BEGIN = "pokeStart"
_POKE_SKIP = {"pokePart", "pokeEnd"}


def ack_class(connected_ms: float | None) -> str:
    if connected_ms is None:
        return "timeout"
    for name, hi in ACK_CLASSES:
        if connected_ms <= hi:
            return name
    return "timeout"


def frame_signature(frames: list[Any]) -> list[str]:
    """Ordered list of frame tags. Consecutive poke sub-frames collapse to one
    'poke' token (order preserved) when COLLAPSE_POKES."""
    sig: list[str] = []
    for f in frames:
        tag = getattr(f, "tag", None) if not isinstance(f, dict) else f.get("tag")
        if tag is None:
            continue
        if COLLAPSE_POKES and tag == _POKE_BEGIN:
            sig.append("poke")               # one token per poke CYCLE
        elif COLLAPSE_POKES and tag in _POKE_SKIP:
            continue                          # fold pokePart/pokeEnd into the cycle
        else:
            sig.append(tag)
    return sig


def compare_sequences(rust_side, ts_side) -> list[str]:
    """Return a list of human-readable divergences ([] == parity)."""
    problems: list[str] = []
    rsig, tsig = frame_signature(rust_side.frames), frame_signature(ts_side.frames)
    if rsig != tsig:
        problems.append(
            f"frame-tag sequence differs:\n    rust={rsig}\n    ts  ={tsig}")
    rcls, tcls = ack_class(rust_side.connected_ms), ack_class(ts_side.connected_ms)
    if rcls != tcls:
        problems.append(
            f"connect-ack latency class differs: rust={rcls} "
            f"({rust_side.connected_ms}ms) vs ts={tcls} ({ts_side.connected_ms}ms) "
            f"— bug-1 signature if rust is slower")
    # Hydrate-independence: the ack itself must be instant/fast on BOTH even if
    # pokes (hydrate result) arrive much later.
    for side, cls in ((rust_side, rcls), (ts_side, tcls)):
        if cls in ("slow", "timeout"):
            problems.append(
                f"{side.name}: connect-ack was {cls} ({side.connected_ms}ms) — "
                f"the ack must be hydrate-independent (I-1 contract)")
    return problems


if __name__ == "__main__":
    import asyncio
    import sys
    from ab_common import make_sides, open_side, reader, Report
    from protocol import DEFAULT_PROTOCOL_VERSION

    async def main() -> int:
        rep = Report("reports/frame-sequence-oracle.json")
        rust, ts = make_sides()
        init = ["initConnection", {"desiredQueriesPatch": []}]
        stop = asyncio.Event()
        try:
            await asyncio.gather(open_side(rust, "", init), open_side(ts, "", init))
        except Exception as e:
            rep.add("frame-seq", "SKIP", f"sandbox not reachable: {e!r}")
            return rep.finish()
        tasks = [asyncio.create_task(reader(rust, stop)),
                 asyncio.create_task(reader(ts, stop))]
        await asyncio.sleep(5.0)          # let hydrate pokes flow
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        problems = compare_sequences(rust, ts)
        rep.add("frame-seq", "PASS" if not problems else "FAIL",
                "; ".join(problems) or "TS==rust ordered frames + ack class")
        return rep.finish()

    sys.exit(asyncio.run(main()))
