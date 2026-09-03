#!/usr/bin/env python3
# Per-client frame-sequence oracle: diff /tmp/frames-rust.ndjson vs /tmp/frames-ts.ndjson
# (written by harness/trace_replay.py when ART_FRAME_LOG=<path> is set). Usage: python3 tools/frameseq_diff.py [rows-per-cg]
import json, collections, sys
def load(side):
    by = collections.defaultdict(list)
    for l in open(f"/tmp/frames-{side}.ndjson"):
        d = json.loads(l); by[d["cg"]].append(d)
    return by
R = load("rust"); T = load("ts")
def summ(f):
    t = f["tag"]
    if t == "pokeStart": return "START(%s)" % (f.get("pokeID") or "")[-6:]
    if t == "pokeEnd": return "END(%s%s)" % ((f.get("cookie") or "")[-6:], ",CANCEL" if f.get("cancel") else "")
    if t == "pokePart":
        g = "".join(o[0][0] for o in f["got"]) if f["got"] else "-"
        d = ",".join("".join(o[0][0] for o in p) for p in (f["desired"] or [])) if f.get("desired") else "-"
        return "PART(got=%s des=%s rows=%d)" % (g, d, f["rows"])
    return t
limit = int(sys.argv[1]) if len(sys.argv) > 1 else 40
for cg in sorted(set(R) | set(T)):
    r = [summ(f) for f in R.get(cg, [])]; t = [summ(f) for f in T.get(cg, [])]
    print("\n### %s: rust frames=%d pokeEnd=%d | ts frames=%d pokeEnd=%d" % (cg, len(r), sum(x.startswith("END") for x in r), len(t), sum(x.startswith("END") for x in t)))
    for i in range(min(max(len(r), len(t)), limit)):
        a = r[i] if i < len(r) else ""; b = t[i] if i < len(t) else ""
        mark = "" if a.split("(")[0] == b.split("(")[0] else "  <<<"
        print("  %3d %-40s | %s%s" % (i, a, b, mark))
