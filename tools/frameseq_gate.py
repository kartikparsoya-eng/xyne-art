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
  K1: rust emits an extra EMPTY poke (pokeStart+pokeEnd, no pokePart) right
      after the initial 00:01 poke = TS's first #stateChanges sync, which in
      TS races the next client message (view-syncer.ts:538-606 vs :1163).
  K2: the pre-connect (initConnection) query hash differs between the two
      harness processes (harness arg resolution), so hashes are compared only
      for changeDesiredQueries-driven ops.
  K3: mutation-ack pokes (pokeParts carrying only lastMutationIDChanges) are
      emitted per replication advance pass; how many replication notifications
      one pass coalesces, and where the ack lands relative to the next query
      poke, is replication timing (mono INVENTIONS.md I-13/I-15). Counted as
      known ONLY if, per client, the final acked lastMutationID is identical
      on both sides and the ack sequence is monotonic on each side.
"""
import argparse, collections, json, sys

def load(path):
    by = collections.defaultdict(list)
    for line in open(path):
        d = json.loads(line)
        by[d["cg"]].append(d)
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
    out, cur, ack = [], None, True
    for f in frames:
        t = f["tag"]
        if t == "pokeStart":
            cur, ack = [], True
        elif t == "pokePart":
            if cur is None: cur, ack = [], True
            cur.append(part_desc(f)); ack = ack and is_ack_part(f)
        elif t == "pokeEnd":
            parts = tuple(cur or [])
            out.append(("poke", parts, bool(f.get("cancel")), bool(parts) and ack))
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

def strip_k1(items):
    """Drop one empty poke (no parts, no cancel) that directly follows the first poke."""
    for i in range(len(items) - 1):
        if items[i][0] == "poke":
            nxt = items[i + 1]
            if nxt[0] == "poke" and not nxt[1] and not nxt[2]:
                return items[:i + 1] + items[i + 2:], True
            return items, False
    return items, False

def strip_acks(items):
    return [it for it in items if not (it[0] == "poke" and it[3])]

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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rust", default="/tmp/frames-rust.ndjson")
    ap.add_argument("--ts", default="/tmp/frames-ts.ndjson")
    ap.add_argument("--show", type=int, default=3)
    a = ap.parse_args()
    R, T = load(a.rust), load(a.ts)
    cgs = sorted(set(R) | set(T))
    same = k1 = k3 = unknown = rows_only = lmid_bad = 0
    bad = []
    for cg in cgs:
        rf, tf = R.get(cg, []), T.get(cg, [])
        r, t = pokes(rf), pokes(tf)
        if r == t:
            same += 1; continue
        r2, stripped = strip_k1(r)
        if stripped and r2 == t:
            k1 += 1; continue
        # rows-only difference (same tags/ops, different row chunking)?
        if no_rows(flat(r2)) == no_rows(flat(t)):
            rows_only += 1; continue
        # K3: mutation-ack poke timing/coalescing
        r3, t3 = strip_acks(r2), strip_acks(t)
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
          f"rows-chunking-only={rows_only} lmid-invariant-broken={lmid_bad} UNKNOWN={unknown}")
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
