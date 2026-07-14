#!/usr/bin/env python3
"""diff_gates.py — diff two gate reports to see what changed.

Highlights which gates flipped verdict, which latencies moved, which
new errors appeared, and which blind spots appeared/disappeared.

    python3 tools/diff_gates.py reports/gate-A.json reports/gate-B.json
    python3 tools/diff_gates.py gate-A.json gate-B.json --json diff.json

Exit 0 = no regressions; 1 = regressions found; 2 = usage error.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys


def load_gates(path: str) -> dict[str, tuple]:
    with open(path) as f:
        doc = json.load(f)
    return {r["gate"]: (r["verdict"], r.get("detail", ""))
            for r in doc.get("results", [])}


def extract_num(pattern: str, text: str) -> float | None:
    m = re.search(pattern, text)
    return float(m.group(1)) if m else None


def main() -> int:
    ap = argparse.ArgumentParser(description="Diff two gate reports.")
    ap.add_argument("a", help="baseline gate report (older)")
    ap.add_argument("b", help="candidate gate report (newer)")
    ap.add_argument("--json", default=None, help="write diff as JSON")
    a = ap.parse_args()

    if not os.path.exists(a.a) or not os.path.exists(a.b):
        print("ERROR: both files must exist", file=sys.stderr)
        return 2

    ga, gb = load_gates(a.a), load_gates(a.b)
    all_gates = sorted(set(ga) | set(gb))

    flips = []
    improvements = []
    regressions = []
    new_findings = []
    resolved_findings = []
    latency_deltas = []

    RANK = {"FAIL": 4, "ERROR": 3, "WATCH": 2, "PASS": 1, "SKIP": 0}

    for gate in all_gates:
        va, da = ga.get(gate, ("SKIP", ""))
        vb, db = gb.get(gate, ("SKIP", ""))
        if va == vb:
            continue
        if RANK.get(vb, 0) > RANK.get(va, 0):
            flips.append(f"  {gate}: {va} -> {vb} (REGRESSION)")
            regressions.append(gate)
            new_findings.append(f"  {gate}: {db[:120]}")
        else:
            flips.append(f"  {gate}: {va} -> {vb} (improved)")
            improvements.append(gate)
            resolved_findings.append(f"  {gate}: was {da[:80]}")

    # latency deltas
    for gate_name in ("G5 latency", "G5b query-lat"):
        for metric in (r"p50=([\d.]+)", r"p95=([\d.]+)"):
            av = extract_num(metric, ga.get(gate_name, ("", ""))[1])
            bv = extract_num(metric, gb.get(gate_name, ("", ""))[1])
            if av and bv and av != bv:
                d = bv - av
                sign = "+" if d > 0 else ""
                pct = (d / av * 100) if av else 0
                latency_deltas.append(
                    f"  {gate_name} {metric[:3]}: {av:.0f} -> {bv:.0f}ms "
                    f"({sign}{d:.0f}ms, {sign}{pct:.0f}%)")

    # error count delta
    for gate_name in ("G2 errors",):
        av = extract_num(r"(\d+)\s*unexpected", ga.get(gate_name, ("", ""))[1])
        bv = extract_num(r"(\d+)\s*unexpected", gb.get(gate_name, ("", ""))[1])
        if av is not None and bv is not None and av != bv:
            d = bv - av
            sign = "+" if d > 0 else ""
            latency_deltas.append(
                f"  G2 errors: {int(av)} -> {int(bv)} ({sign}{int(d)})")

    # G13 wedge/reset deltas
    for metric, pat in [("wedges", r"(\d+)x?\s*go-wedge"),
                       ("resets", r"(\d+)x?\s*resetting"),
                       ("timeouts", r"(\d+)x?\s*advancement-timeout")]:
        av = extract_num(pat, ga.get("G13 log-health", ("", ""))[1])
        bv = extract_num(pat, gb.get("G13 log-health", ("", ""))[1])
        if av is not None and bv is not None and av != bv:
            d = bv - av
            sign = "+" if d > 0 else ""
            latency_deltas.append(
                f"  G13 {metric}: {int(av)} -> {int(bv)} ({sign}{int(d)})")

    # G9 blind spot delta
    for gate_name in ("G9 coverage",):
        am = re.search(r"(\d+)\s+UNEXPLAINED", ga.get(gate_name, ("", ""))[1])
        bm = re.search(r"(\d+)\s+UNEXPLAINED", gb.get(gate_name, ("", ""))[1])
        av = int(am.group(1)) if am else 0
        bv = int(bm.group(1)) if bm else 0
        if av != bv:
            d = bv - av
            sign = "+" if d > 0 else ""
            latency_deltas.append(
                f"  G9 unexplained blind spots: {av} -> {bv} ({sign}{d})")

    print(f"diff: {os.path.basename(a.a)} -> {os.path.basename(a.b)}")
    print(f"  {len(regressions)} regression(s), {len(improvements)} improvement(s)")

    if flips:
        print("\nGate flips:")
        for f in flips:
            print(f)

    if latency_deltas:
        print("\nMetric deltas:")
        for d in latency_deltas:
            print(d)

    if new_findings:
        print("\nNew findings:")
        for f in new_findings:
            print(f)

    if resolved_findings:
        print("\nResolved:")
        for f in resolved_findings:
            print(f)

    if a.json:
        doc = {
            "a": os.path.basename(a.a), "b": os.path.basename(a.b),
            "regressions": regressions, "improvements": improvements,
            "flips": flips, "latency_deltas": latency_deltas,
            "new_findings": new_findings,
            "resolved_findings": resolved_findings,
        }
        with open(a.json, "w") as f:
            json.dump(doc, f, indent=2)
        print(f"  diff -> {a.json}")

    return 1 if regressions else 0


if __name__ == "__main__":
    raise SystemExit(main())
