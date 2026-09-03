#!/usr/bin/env python3
"""Merge per-slice full-catalog diff-oracle reports (run SEQUENTIALLY with
--pairs 1 --catalog-pair-offset K to bound peak IVM memory) into the single
report shape that tools/local_gate.py's G8 reads. Counters are summed, lists
concatenated, per-side dicts merged, booleans OR'd, verdict = FAIL if any FAIL.
Usage: merge_oracle_reports.py OUT IN1.json IN2.json ..."""
import json, sys

def merge(docs):
    out = {}
    for d in docs:
        for k, v in d.items():
            if k not in out:
                out[k] = v if not isinstance(v, (dict, list)) else (dict(v) if isinstance(v, dict) else list(v))
                continue
            cur = out[k]
            if isinstance(v, bool):
                out[k] = cur or v
            elif isinstance(v, (int, float)) and k not in ("duration_s", "quiesce_s"):
                out[k] = cur + v
            elif isinstance(v, list):
                out[k] = cur + v
            elif isinstance(v, dict):
                for side, sv in v.items():
                    if isinstance(sv, dict) and isinstance(cur.get(side), dict):
                        cur[side] = {**cur[side], **sv}
                    elif isinstance(sv, (int, float)) and isinstance(cur.get(side), (int, float)) and not isinstance(sv, bool):
                        cur[side] = cur[side] + sv
                    else:
                        cur[side] = sv
            elif k == "verdict":
                out[k] = "FAIL" if "FAIL" in (cur, v) else cur
            # str/None: keep first
    out["merged_from"] = len(docs)
    return out

if __name__ == "__main__":
    outp, ins = sys.argv[1], sys.argv[2:]
    docs = [json.load(open(p)) for p in ins]
    m = merge(docs)
    json.dump(m, open(outp, "w"), indent=1)
    print(f"merged {len(docs)} reports -> {outp}: verdict={m.get('verdict')} mismatches={m.get('total_mismatches')} "
          f"parity_gap={m.get('hydration_parity_gap')} catalog={m.get('catalog_driven')}/{m.get('catalog_expected')} "
          f"protocol_errors={m.get('protocol_errors')} pairs={m.get('pairs')}")
