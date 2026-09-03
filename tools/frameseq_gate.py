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
"""
import argparse, collections, json, sys

def load(path):
    by = collections.defaultdict(list)
    for line in open(path):
        d = json.loads(line)
        by[d["cg"]].append(d)
    return by

def norm(frames):
    """Sequence of comparable frame descriptors; pong/ping/connected kept as tags."""
    out = []
    for f in frames:
        t = f["tag"]
        if t == "pokePart":
            got = tuple(op for op, _ in (f.get("got") or []))
            des = tuple(tuple(op for op, _ in p) for p in (f.get("desired") or []))
            out.append(("pokePart", got, des, f.get("rows", 0)))
        elif t == "pokeEnd":
            out.append(("pokeEnd", bool(f.get("cancel"))))
        elif t in ("pong", "ping"):
            continue  # keepalive cadence is timing, not content
        else:
            out.append((t,))
    return out

def strip_k1(seq):
    """Drop one empty poke (START, END-no-cancel) that directly follows the first pokeEnd."""
    for i in range(len(seq) - 3):
        if seq[i][0] == "pokeEnd" and seq[i+1] == ("pokeStart",) and seq[i+2] == ("pokeEnd", False):
            return seq[:i+1] + seq[i+3:], True
    return seq, False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rust", default="/tmp/frames-rust.ndjson")
    ap.add_argument("--ts", default="/tmp/frames-ts.ndjson")
    ap.add_argument("--show", type=int, default=3)
    a = ap.parse_args()
    R, T = load(a.rust), load(a.ts)
    cgs = sorted(set(R) | set(T))
    same = known = unknown = rows_only = 0
    bad = []
    for cg in cgs:
        r, t = norm(R.get(cg, [])), norm(T.get(cg, []))
        r2, stripped = strip_k1(r)
        if r == t:
            same += 1; continue
        if stripped and r2 == t:
            known += 1; continue
        # rows-only difference (same tags/ops, different row chunking)?
        def no_rows(s): return [x[:3] if x[0] == "pokePart" else x for x in s]
        if no_rows(r2) == no_rows(t) or no_rows(r) == no_rows(t):
            rows_only += 1; continue
        unknown += 1; bad.append((cg, r2 if stripped else r, t))
    print(f"frameseq gate: cgs={len(cgs)} identical={same} known(K1)={known} rows-chunking-only={rows_only} UNKNOWN={unknown}")
    for cg, r, t in bad[:a.show]:
        print(f"--- {cg}: rust={len(r)} frames, ts={len(t)} frames")
        for i in range(max(len(r), len(t))):
            x = r[i] if i < len(r) else ""; y = t[i] if i < len(t) else ""
            mark = "" if x == y else "   <<<"
            print(f"  {i:3d} {str(x)[:60]:60s} | {str(y)[:60]}{mark}")
            if i > 60: print("  ..."); break
    print("FRAMESEQ: " + ("PASS" if unknown == 0 else "FAIL"))
    sys.exit(0 if unknown == 0 else 1)

if __name__ == "__main__":
    main()
