#!/usr/bin/env python3
"""trend.py — cross-build trend tracking from gate reports.

Scans reports/gate-*.json and reports/run-*.json, builds a time-ordered
table of key metrics (verdict, p50, p95, errors, wedges, resets, RSS peak)
so regressions show up as deltas across builds, not just per-run snapshots.

    python3 tools/trend.py                    # last 15 runs
    python3 tools/trend.py --n 30             # last 30
    python3 tools/trend.py --json trend.json  # also write JSON
    python3 tools/trend.py --since 20260710   # only runs on/after date

Exit 0 always (reporting tool, not a gate).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re


def extract_gate(doc: dict) -> dict:
    """Extract key metrics from a gate report."""
    gates = {r["gate"]: r for r in doc.get("results", [])}
    out = {"overall": doc.get("overall", "?")}

    def _detail(gate_name: str) -> str:
        r = gates.get(gate_name)
        return r.get("detail", "") if r else ""

    def _verdict(gate_name: str) -> str:
        r = gates.get(gate_name)
        return r.get("verdict", "SKIP") if r else "SKIP"

    out["verdict"] = out["overall"]
    out["g1"] = _verdict("G1 connectivity")
    out["g2"] = _verdict("G2 errors")
    out["g4"] = _verdict("G4 mutations")
    out["g5"] = _verdict("G5 latency")
    out["g6"] = _verdict("G6 leaks")
    out["g8"] = _verdict("G8 diff-oracle")
    out["g9"] = _verdict("G9 coverage")
    out["g11"] = _verdict("G11 negative")
    out["g13"] = _verdict("G13 log-health")
    out["g15"] = _verdict("G15 mut-matrix")
    out["g25"] = _verdict("G25 latency-parity")

    g5_detail = _detail("G5 latency")
    m = re.search(r"p50=([\d.]+)", g5_detail)
    out["p50"] = float(m.group(1)) if m else None
    m = re.search(r"p95=([\d.]+)", g5_detail)
    out["p95"] = float(m.group(1)) if m else None

    g2_detail = _detail("G2 errors")
    m = re.search(r"(\d+)\s*unexpected", g2_detail)
    out["errors"] = int(m.group(1)) if m else 0

    g13_detail = _detail("G13 log-health")
    m = re.search(r"(\d+)x?\s*resetting", g13_detail)
    out["resets"] = int(m.group(1)) if m else 0
    m = re.search(r"(\d+)x?\s*advancement-timeout", g13_detail)
    out["timeouts"] = int(m.group(1)) if m else 0
    m = re.search(r"(\d+)x?\s*go-wedge", g13_detail)
    out["wedges"] = int(m.group(1)) if m else 0

    g6_detail = _detail("G6 leaks")
    m = re.search(r"peak RSS ([\d.]+)GiB", g6_detail)
    out["rss_peak_gib"] = float(m.group(1)) if m else None

    return out


def extract_run(run_path: str) -> dict:
    """Extract shape info from the run report (if it exists)."""
    try:
        with open(run_path) as f:
            d = json.load(f)
        cfg = d.get("config", {})
        counters = d.get("counters", {})
        return {
            "conns": cfg.get("connections", "?"),
            "duration": cfg.get("duration", "?"),
            "pokes": counters.get("pokes", 0),
            "muts": counters.get("muts", 0),
            "msgs": counters.get("msgs", 0),
        }
    except Exception:
        return {}


def main() -> int:
    ap = argparse.ArgumentParser(description="Cross-build trend from gate reports.")
    ap.add_argument("--n", type=int, default=15, help="number of recent runs to show")
    ap.add_argument("--since", default=None, help="only show runs on/after this date (YYYYMMDD)")
    ap.add_argument("--json", default=None, help="also write trend as JSON")
    ap.add_argument("--reports-dir", default="reports")
    a = ap.parse_args()

    gates = sorted(glob.glob(os.path.join(a.reports_dir, "gate-*.json")))
    if a.since:
        gates = [g for g in gates if os.path.basename(g) >= f"gate-{a.since}"]

    if not gates:
        print("no gate reports found")
        return 0

    gates = gates[-a.n:]

    rows = []
    for gp in gates:
        tag = re.sub(r"^gate-|\.json$", "", os.path.basename(gp))
        try:
            with open(gp) as f:
                doc = json.load(f)
        except Exception:
            continue
        g = extract_gate(doc)
        # find matching run report (same timestamp prefix)
        run_pat = os.path.join(a.reports_dir, f"run-{tag}*.json")
        run_paths = sorted(glob.glob(run_pat))
        run_info = extract_run(run_paths[0]) if run_paths else {}
        g["tag"] = tag
        g.update(run_info)
        rows.append(g)

    # print table
    cols = [
        ("tag", 14), ("verdict", 6), ("p50", 8), ("p95", 8),
        ("errs", 5), ("wedge", 5), ("reset", 6), ("rss", 6),
        ("g8", 5), ("g11", 5), ("g13", 5), ("g15", 5), ("g25", 5),
    ]
    hdr = "  ".join(f"{c[0]:<{c[1]}}" for c in cols)
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    for r in rows:
        cells = []
        for c, w in cols:
            v = r.get(c[0])
            if v is None:
                s = "-"
            elif c[0] in ("p50", "p95", "rss_peak_gib") and isinstance(v, float):
                if c[0] in ("p50", "p95"):
                    s = f"{v:.0f}ms" if v < 10000 else f"{v/1000:.0f}s"
                else:
                    s = f"{v:.1f}G"
            elif c[0] == "rss":
                v = r.get("rss_peak_gib")
                s = f"{v:.1f}G" if v else "-"
            else:
                s = str(v)
            cells.append(f"{s:<{w}}")
        print("  ".join(cells))

    # deltas
    if len(rows) >= 2:
        prev, curr = rows[-2], rows[-1]
        print(f"\nDelta ({prev['tag']} -> {curr['tag']}):")
        for k in ("p50", "p95", "errors", "wedges", "resets"):
            pv, cv = prev.get(k), curr.get(k)
            if pv is not None and cv is not None and pv != cv:
                d = cv - pv
                sign = "+" if d > 0 else ""
                print(f"  {k}: {pv} -> {cv} ({sign}{d})")

    # flipped gates
    if len(rows) >= 2:
        gate_names = ["g1", "g2", "g4", "g5", "g6", "g8", "g9", "g11", "g13", "g15", "g25"]
        flipped = []
        for g in gate_names:
            pv, cv = prev.get(g), curr.get(g)
            if pv and cv and pv != cv:
                flipped.append(f"{g.upper()}: {pv}->{cv}")
        if flipped:
            print(f"  gate flips: {', '.join(flipped)}")

    if a.json:
        os.makedirs(os.path.dirname(os.path.abspath(a.json)) or ".", exist_ok=True)
        with open(a.json, "w") as f:
            json.dump({"runs": rows}, f, indent=2)
        print(f"  trend -> {a.json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
