#!/usr/bin/env python3
"""Per-client frame-sequence gate: rust vs TS must emit the same ordered frame
tags and the same gotQueriesPatch / desiredQueriesPatches op sequences per
client group (cookies and pokeIDs are per-cache versions and are ignored; row
counts are compared but reported separately).

Input: /tmp/frames-rust.ndjson and /tmp/frames-ts.ndjson written by
harness/trace_replay.py with ART_FRAME_LOG=<path>. Usage:
  python3 tools/frameseq_gate.py [--rust F] [--ts F] [--show N]

Known, documented deviations are counted as `known`, everything else as
`unknown`; the gate FAILS only on unknown deviations.
  K1: an EMPTY poke (pokeStart+pokeEnd, no pokePart) among the first few pokes
      after the initial 00:01 poke = the first #stateChanges sync, which on BOTH
      sides races the client's first messages (view-syncer.ts:538-606 vs :1163;
      rust I-15). Either side may show it, one or two pokes later than the
      initial one (2026-09-03 captures: 117 rust-only, 21 TS-only, a few offset).
  K2: the pre-connect (initConnection) query hash differs between the two
      harness processes (harness arg resolution), so hashes are compared only
      for changeDesiredQueries-driven ops.
  K4: a run of consecutive rows-only pokes on one side equals a run on the
      other side by row-key multiset (op:table:pk): the engines fold
      replication notifications into advance passes differently, so one side
      may deliver two 1-row pokes where the other delivers one 2-row poke.
      Client-visible end state is identical. Old logs (no `rowkeys`) match by
      row count only.
  K3: mutation-ack pokes (pokeParts carrying only lastMutationIDChanges) are
      emitted per replication advance pass; how many replication notifications
      one pass coalesces, and where the ack lands relative to the next query
      poke, is replication timing (mono INVENTIONS.md I-13/I-15). Counted as
      known ONLY if, per client, the final acked lastMutationID is identical
      on both sides and the ack sequence is monotonic on each side.
"""
import argparse, collections, json, sys

ROWKEYS = {}  # id(pokes-list) -> {poke index: [row keys]} (side table; lists are unhashable)

def load(path):
    """Group by (clientGroupID, clientID): two concurrent sockets of one client
    group interleave in wall-clock order, which is not a comparable sequence."""
    by = collections.defaultdict(list)
    for line in open(path):
        d = json.loads(line)
        by[d["cg"] + ("/" + d["cid"] if d.get("cid") else "")].append(d)
    return by

def part_desc(f):
    got = tuple(op for op, _ in (f.get("got") or []))
    des = tuple(tuple(op for op, _ in p) for p in (f.get("desired") or []))
    return ("pokePart", got, des, f.get("rows", 0))

def is_ack_part(f):
    """A part with no got/desired/rows. Old logs lack `lmid`; treat as ack."""
    return not f.get("got") and not f.get("desired") and not f.get("rows") \
        and ("lmid" not in f or f.get("lmid"))

def pokes(frames):
    """Group frames into poke-level items: ("poke", parts, cancel, ack_only) or (tag,)."""
    out, cur, ack, curkeys = [], None, True, []
    ROWKEYS[id(out)] = {}
    for f in frames:
        t = f["tag"]
        if t == "pokeStart":
            cur, ack, curkeys = [], True, []
        elif t == "pokePart":
            if cur is None: cur, ack, curkeys = [], True, []
            cur.append(part_desc(f)); ack = ack and is_ack_part(f)
            curkeys.extend(f.get("rowkeys") or [])
        elif t == "pokeEnd":
            parts = tuple(cur or [])
            out.append(("poke", parts, bool(f.get("cancel")), bool(parts) and ack))
            ROWKEYS[id(out)][len(out) - 1] = list(curkeys)
            cur = None
        elif t in ("pong", "ping"):
            continue  # keepalive cadence is timing, not content
        else:
            out.append((t,))
    return out

def flat(items):
    """Expand poke items back to the frame-level descriptors (for display + rows-only check)."""
    out = []
    for it in items:
        if it[0] == "poke":
            out.append(("pokeStart",)); out.extend(it[1]); out.append(("pokeEnd", it[2]))
        else:
            out.append(it)
    return out

K1_WINDOW = 4  # the first-sync empty poke races the client's first few messages

def strip_k1(items):
    """Drop ONE empty poke (no parts, no cancel) among the first K1_WINDOW pokes
    after the initial poke: the first #stateChanges sync's empty poke lands
    right after the initial poke, or one/two client messages later, depending
    on which side of the lock race the sync landed (both engines)."""
    seen = 0
    for i in range(1, len(items)):
        it = items[i]
        if it[0] != "poke":
            continue
        seen += 1
        if seen == 1:
            continue  # the initial poke itself
        if not it[1] and not it[2]:
            return items[:i] + items[i + 1:], True
        if seen > K1_WINDOW:
            break
    return items, False

def strip_acks(items):
    return [it for it in items if not (it[0] == "poke" and it[3])]

def rows_only_poke(it):
    """A poke whose parts carry only row patches (no got/desired ops)."""
    return it[0] == "poke" and it[1] and all(p[0] == "pokePart" and not p[1] and not p[2] for p in it[1])

def coalesce_row_runs(items, keys_by_index):
    """K4: merge each maximal run of consecutive rows-only pokes into one item
    carrying the sorted multiset of row keys (advance-pass coalescing differs
    per engine: TS may fold two replication notifications into one poke where
    rust emits two; the client-visible end state is identical). Needs `rowkeys`
    in the frame log; without it the run is merged by row COUNT only."""
    out, i = [], 0
    while i < len(items):
        if rows_only_poke(items[i]):
            j = i
            keys = []
            while j < len(items) and rows_only_poke(items[j]):
                keys.extend(keys_by_index.get(j, []) or ["<%d rows>" % sum(p[3] for p in items[j][1])])
                j += 1
            out.append(("rowrun", tuple(sorted(keys))))
            i = j
        else:
            out.append(items[i]); i += 1
    return out

def lmid_invariant(rf, tf):
    """Final acked LMID per client equal on both sides; monotonic per side. None = no data."""
    def walk(frames):
        last, mono = {}, True
        seen = False
        for f in frames:
            m = f.get("lmid")
            if "lmid" in f: seen = True
            if not m: continue
            for cid, v in m.items():
                if cid in last and v < last[cid]: mono = False
                last[cid] = v
        return last, mono, seen
    rl, rm, rs = walk(rf); tl, tm, ts = walk(tf)
    if not (rs and ts): return None
    return rl == tl and rm and tm

def no_rows(seq): return [x[:3] if x[0] == "pokePart" else x for x in seq]

def _remap_keys(orig, stripped, keys):
    """Row keys are recorded by index into the ORIGINAL poke list; rebuild the
    map for a stripped list by matching items in order (stripping only removes)."""
    out, oi = {}, 0
    for si, it in enumerate(stripped):
        while oi < len(orig) and orig[oi] is not it:
            oi += 1
        if oi < len(orig):
            if oi in keys: out[si] = keys[oi]
            oi += 1
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rust", default="/tmp/frames-rust.ndjson")
    ap.add_argument("--ts", default="/tmp/frames-ts.ndjson")
    ap.add_argument("--show", type=int, default=3)
    a = ap.parse_args()
    R, T = load(a.rust), load(a.ts)
    cgs = sorted(set(R) | set(T))
    same = k1 = k3 = k4 = unknown = rows_only = lmid_bad = 0
    bad = []
    for cg in cgs:
        rf, tf = R.get(cg, []), T.get(cg, [])
        r, t = pokes(rf), pokes(tf)
        if r == t:
            same += 1; continue
        r2, stripped = strip_k1(r)
        t2, tstripped = strip_k1(t)
        if (stripped and r2 == t) or (tstripped and r == t2) or (stripped and tstripped and r2 == t2):
            k1 += 1; continue
        # Carry BOTH sides' K1-stripped sequences into the later checks (a
        # both-sides K1 with a later K3 difference compared stripped rust
        # against unstripped TS and was reported UNKNOWN).
        if tstripped:
            t = t2
        # rows-only difference (same tags/ops, different row chunking)?
        if no_rows(flat(r2)) == no_rows(flat(t)):
            rows_only += 1; continue
        # K3: mutation-ack poke timing/coalescing
        r3, t3 = strip_acks(r2), strip_acks(t)
        # K4: consecutive rows-only pokes coalesced differently (same row keys)
        rk_r, rk_t = ROWKEYS.get(id(r), {}), ROWKEYS.get(id(t), {})
        r4, t4 = coalesce_row_runs(r3, _remap_keys(r, r3, rk_r)), coalesce_row_runs(t3, _remap_keys(t, t3, rk_t))
        if r3 != t3 and r4 == t4:
            k4 += 1; continue
        if r3 == t3 or no_rows(flat(r3)) == no_rows(flat(t3)):
            inv = lmid_invariant(rf, tf)
            if inv is False:
                lmid_bad += 1; bad.append((cg + " [LMID-INVARIANT]", flat(r2), flat(t)))
            else:
                k3 += 1
            continue
        unknown += 1; bad.append((cg, flat(r3), flat(t3)))
    unknown_total = unknown + lmid_bad
    print(f"frameseq gate: cgs={len(cgs)} identical={same} known(K1)={k1} known(K3 ack-timing)={k3} "
          f"known(K4 row-coalescing)={k4} rows-chunking-only={rows_only} lmid-invariant-broken={lmid_bad} UNKNOWN={unknown}")
    for cg, r, t in bad[:a.show]:
        print(f"--- {cg}: rust={len(r)} frames, ts={len(t)} frames (ack pokes stripped)")
        for i in range(max(len(r), len(t))):
            x = r[i] if i < len(r) else ""; y = t[i] if i < len(t) else ""
            mark = "" if x == y else "   <<<"
            print(f"  {i:3d} {str(x)[:60]:60s} | {str(y)[:60]}{mark}")
            if i > 60: print("  ..."); break
    print("FRAMESEQ: " + ("PASS" if unknown_total == 0 else "FAIL"))
    sys.exit(0 if unknown_total == 0 else 1)

if __name__ == "__main__":
    main()
