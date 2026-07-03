#!/usr/bin/env python3
"""
consolidate_gates.py — merge per-run gate verdicts into ONE G1–G11 table.

Why: the full gate set cannot run in a single pass —
  - chaos (G10) docker-pauses pods mid-run, polluting latency (G5) and leak
    slopes (G6), so it needs its own run;
  - leak slopes (G6) need a >=15-min soak window;
  - latency (G5) only fires at a blessed baseline shape;
  - mutations/oracle/negative (G4/G8/G11) belong in the functional run.
A full sweep is therefore 2–3 sequenced runs (see run-art-sweep.sh), and the
release verdict is the union of their gate reports.

Rules:
  - per gate, worst verdict wins:  FAIL > ERROR > WATCH > PASS > SKIP
  - a gate that is SKIP in every run is NOT COVERED — the sweep never
    exercised it, which is a coverage hole, not a pass
  - overall: FAIL if any gate FAIL; else ERROR if any ERROR; else PASS
    (WATCH findings and NOT-COVERED gates are listed but don't fail)

    python3 tools/consolidate_gates.py reports/gate-A.json reports/gate-B.json ...
    python3 tools/consolidate_gates.py --labels functional,chaos,soak g1.json g2.json g3.json
    python3 tools/consolidate_gates.py ... --json reports/sweep-verdict.json

Exit 0 = PASS, 1 = FAIL, 2 = ERROR (mirrors local_gate.py).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

RANK = {"FAIL": 4, "ERROR": 3, "WATCH": 2, "PASS": 1, "SKIP": 0}


def gate_sort_key(name: str) -> tuple:
    m = re.match(r"G(\d+)", name)
    return (int(m.group(1)) if m else 999, name)


def main() -> int:
    ap = argparse.ArgumentParser(description="Merge gate-*.json reports into one verdict.")
    ap.add_argument("gates", nargs="+", help="gate report JSONs from local_gate.py --out")
    ap.add_argument("--labels", default=None,
                    help="comma-separated run labels (default: derived from filenames)")
    ap.add_argument("--json", dest="json_out", default=None,
                    help="also write the consolidated verdict as JSON")
    a = ap.parse_args()

    runs = []
    for p in a.gates:
        try:
            with open(p) as f:
                doc = json.load(f)
        except Exception as e:
            print(f"ERROR: cannot read {p}: {e}", file=sys.stderr)
            return 2
        runs.append((p, doc))

    if a.labels:
        labels = [s.strip() for s in a.labels.split(",")]
        if len(labels) != len(runs):
            print(f"ERROR: {len(labels)} labels for {len(runs)} reports", file=sys.stderr)
            return 2
    else:
        labels = [re.sub(r"^gate-|\.json$", "", os.path.basename(p)) for p, _ in runs]

    # gate -> [(label, verdict, detail)] preserving run order
    by_gate: dict[str, list[tuple[str, str, str]]] = {}
    for (path, doc), label in zip(runs, labels):
        for r in doc.get("results", []):
            by_gate.setdefault(r["gate"], []).append(
                (label, r.get("verdict", "SKIP"), r.get("detail", "")))

    rows = []          # (gate, final, per-run-verdicts, deciding detail)
    not_covered = []
    for gate in sorted(by_gate, key=gate_sort_key):
        entries = by_gate[gate]
        final = max(entries, key=lambda e: RANK.get(e[1], 0))[1]
        if final == "SKIP":
            final = "NOT COVERED"
            not_covered.append(gate)
            detail = "SKIP in every run — the sweep never exercised this gate"
        else:
            # detail from the run that decided the verdict (first worst)
            detail = next(d for lbl, v, d in entries if v == final)
        rows.append((gate, final, {lbl: v for lbl, v, _ in entries}, detail))

    n_fail = sum(1 for _, v, _, _ in rows if v == "FAIL")
    n_error = sum(1 for _, v, _, _ in rows if v == "ERROR")
    n_watch = sum(1 for _, v, _, _ in rows if v == "WATCH")
    overall = "FAIL" if n_fail else ("ERROR" if n_error else "PASS")

    gw = max(len(g) for g, _, _, _ in rows)
    vw = max(len("NOT COVERED"), max(len(v) for _, v, _, _ in rows))
    lw = {lbl: max(len(lbl), 5) for lbl in labels}
    header = (f"  {'gate':<{gw}}  {'FINAL':<{vw}}  "
              + "  ".join(f"{lbl:<{lw[lbl]}}" for lbl in labels))
    print(f"consolidated verdict over {len(runs)} run(s): " + ", ".join(labels))
    print(header)
    print("  " + "-" * (len(header) - 2))
    for gate, final, per, detail in rows:
        cells = "  ".join(f"{per.get(lbl, '-'):<{lw[lbl]}}" for lbl in labels)
        print(f"  {gate:<{gw}}  {final:<{vw}}  {cells}  {detail}")

    print(f"\n  {n_fail} FAIL / {n_error} ERROR / {n_watch} WATCH / "
          f"{len(rows) - n_fail - n_error - n_watch - len(not_covered)} PASS"
          + (f" / {len(not_covered)} NOT COVERED" if not_covered else ""))
    if not_covered:
        print(f"  coverage hole: {', '.join(not_covered)} never ran — "
              "add the missing run type (e.g. --soak for G6, --chaos for G10)")

    if a.json_out:
        doc = {"schema": 1,
               "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "runs": [{"label": lbl, "report": os.path.basename(p),
                         "inputs": d.get("inputs"), "overall": d.get("overall")}
                        for (p, d), lbl in zip(runs, labels)],
               "gates": [{"gate": g, "final": v, "per_run": per, "detail": det}
                         for g, v, per, det in rows],
               "not_covered": not_covered,
               "overall": overall}
        os.makedirs(os.path.dirname(os.path.abspath(a.json_out)) or ".", exist_ok=True)
        with open(a.json_out, "w") as f:
            json.dump(doc, f, indent=2)
        print(f"  consolidated report -> {a.json_out}")

    print(f"\nSWEEP VERDICT: {overall}")
    if overall == "FAIL":
        return 1
    if overall == "ERROR":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
