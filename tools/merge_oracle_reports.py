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
    # Per-slice verdicts are meaningless: each slice checks catalog completeness
    # against the WHOLE catalog. Recompute over the union.
    miss = [set(d.get("catalog_missing") or []) for d in docs if d.get("full_catalog")]
    out["catalog_missing"] = sorted(set.intersection(*miss)) if miss else []
    results = out.get("results") or []
    terr = {side: sum((r.get(side, {}).get("errors") or {}).get("transformError", 0)
                      for r in results if isinstance(r, dict)) for side in ("primary", "mirror")}
    out["transform_errors"] = terr
    # protocol errors: every non-transformError kind (both sides) + transformError ASYMMETRY only
    out["protocol_errors"] = sum(
        c for r in results if isinstance(r, dict) for side in ("primary", "mirror")
        for k, c in (r.get(side, {}).get("errors") or {}).items()
        if ":" not in k and k != "transformError") + abs(terr["primary"] - terr["mirror"])
    out["connect_errors"] = sum(1 for r in results if isinstance(r, dict) and "error" in r)
    failed = (out.get("total_mismatches", 0) > 0 or out.get("hydration_parity_gap", 0) > 0
              or out["connect_errors"] > 0 or out["protocol_errors"] > 0
              or out.get("cookie_violations", 0) > 0 or bool(out.get("resume_errors"))
              or bool(out["catalog_missing"]) or not out.get("catalog_expected"))
    # Coverage against the WHOLE catalog (a sequential run that lost slices to
    # OOM/mem-guard must not report PASS on the slices it happened to finish).
    total = max((d.get("catalog_total") or 0) for d in docs)
    accounted = set()
    for d in docs:
        for k in ("catalog_unresolved", "catalog_stale", "catalog_excluded"):
            accounted.update(d.get(k) or [])
        for r in d.get("results") or []:
            if isinstance(r, dict): accounted.update(r.get("catalog_hydrated") or [])
    out["catalog_total"] = total
    out["catalog_coverage"] = f"{len(accounted)}/{total}"
    if total and len(accounted) < total:
        failed = True
        out["catalog_missing"] = sorted(set(out.get("catalog_missing") or []) | {"<%d catalog names not covered>" % (total - len(accounted))})
    out["verdict"] = "FAIL" if failed else "PASS"
    return out

if __name__ == "__main__":
    outp, ins = sys.argv[1], sys.argv[2:]
    docs = [json.load(open(p)) for p in ins]
    if not docs:
        m = {"verdict": "FAIL", "merged_from": 0, "total_mismatches": 0,
             "note": "no slice reports (every slice aborted — mem-guard/OOM)"}
    else:
        m = merge(docs)
    json.dump(m, open(outp, "w"), indent=1)
    print(f"merged {len(docs)} reports -> {outp}: verdict={m.get('verdict')} mismatches={m.get('total_mismatches')} "
          f"parity_gap={m.get('hydration_parity_gap')} catalog={m.get('catalog_driven')}/{m.get('catalog_expected')} coverage={m.get('catalog_coverage')} "
          f"protocol_errors={m.get('protocol_errors')} pairs={m.get('pairs')}")
