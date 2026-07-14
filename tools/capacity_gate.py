#!/usr/bin/env python3
"""capacity_gate.py — G22: capacity-cliff regression gate.

run-art-sweep.sh runs the G1–G15 gate suite at fixed shapes. It does NOT track
the capacity CLIFF: the highest connection count a build sustains before p95
degrades past a threshold or errors spike. A build that lowers the cliff from
200 -> 120 conns reads as "slower" (G5 latency FAIL) but the actionable number
— "this build serves N fewer concurrent users" — is invisible. This gate
makes the cliff a tracked, blessed regression signal.

Method: collect run reports at a ladder of connection counts, extract
(connections, p95, errors, failed_open) from each, and find the CLIFF = the
highest rung where p95 <= --p95-threshold AND errors == 0 AND failed_open == 0.
Compare to a blessed max_healthy_conns; FAIL if the cliff regressed below it.

Two ways to feed it:
  --runs reports/run-A.json reports/run-B.json ...   (consume existing reports;
        each report's config.connections labels its rung)
  --drive --target ... --auth-token ... --ladder 10,25,50,100,200  (invoke
        harness/replay.py at each rung and collect the reports)

    # consume existing sweep reports:
    .venv/bin/python tools/capacity_gate.py \\
        --runs reports/run-*.json --blessed-conns 200 --p95-threshold 5000 \\
        --out reports/capacity-$TAG.json

    # self-driving sweep:
    .venv/bin/python tools/capacity_gate.py --drive --target ws://host/zero \\
        --auth-token "$JWT" --id-pool harness/id-pool.sandbox.json \\
        --ladder 10,25,50,100,200 --blessed-conns 200 \\
        --out reports/capacity-$TAG.json

Exit 0 = cliff >= blessed; 1 = regressed (FAIL); 2 = ERROR (no usable runs).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
import time


def _run_point(path: str) -> dict | None:
    """Extract (connections, p95, errors, failed_open) from one run report."""
    try:
        d = json.load(open(path))
    except Exception:
        return None
    cfg = d.get("config") or {}
    conns = cfg.get("connections")
    c = d.get("counters") or {}
    lat = d.get("client_latency_ms") or d.get("client_latency_steady_ms") or {}
    p95 = lat.get("p95")
    if conns is None:
        return None
    return {"path": os.path.basename(path), "connections": int(conns),
            "p95": p95, "errors": int(c.get("errors", 0)),
            "failed_open": int(c.get("failed_open", 0))}


def drive_rung(target: str, auth_token: str | None, id_pool: str,
               conns: int, duration: int, extra: list[str], protocol: int,
               tag: str) -> str:
    """Run replay.py at one rung; return the report path."""
    out = f"reports/capacity-{tag}-{conns}c.json"
    cmd = [sys.executable, "harness/replay.py",
           "--target", target, "--id-pool", id_pool,
           "--connections", str(conns), "--working-set", "12",
           "--churn-ms", "750", "--duration", str(duration),
           "--protocol-version", str(protocol), "--out", out]
    if auth_token:
        cmd += ["--auth-token", auth_token]
    cmd += extra
    subprocess.run(cmd, check=False, timeout=duration + 120)
    return out


def find_cliff(points: list[dict], p95_threshold: float) -> dict:
    """Highest rung where p95 <= threshold AND errors==0 AND failed_open==0."""
    healthy = [p for p in points
               if (p["p95"] is None or p["p95"] <= p95_threshold)
               and p["errors"] == 0 and p["failed_open"] == 0]
    if not healthy:
        return {"cliff_conns": 0, "healthy_rungs": []}
    cliff = max(healthy, key=lambda p: p["connections"])
    return {"cliff_conns": cliff["connections"], "healthy_rungs": healthy,
            "cliff_point": cliff}


def main() -> int:
    ap = argparse.ArgumentParser(description="G22: capacity-cliff regression gate.")
    ap.add_argument("--runs", nargs="*", default=[], help="existing run reports to consume")
    ap.add_argument("--drive", action="store_true", help="invoke replay.py at each rung")
    ap.add_argument("--target", default=None)
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--id-pool", default="harness/id-pool.json")
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--ladder", default="10,25,50,100,200", help="comma-sep conn counts")
    ap.add_argument("--duration", type=int, default=120, help="per-rung duration (drive mode)")
    ap.add_argument("--protocol-version", type=int, default=49)
    ap.add_argument("--p95-threshold", type=float, default=5000.0,
                    help="p95 ms above which a rung is 'unhealthy'")
    ap.add_argument("--blessed-conns", type=int, default=0,
                    help="blessed max-healthy-conns; FAIL if cliff drops below")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    tag = time.strftime("%Y%m%d-%H%M%S")
    paths: list[str] = list(a.runs)
    # expand globs
    expanded = []
    for p in paths:
        if any(ch in p for ch in "*?["):
            expanded.extend(glob.glob(p))
        else:
            expanded.append(p)
    paths = expanded

    if a.drive:
        if not a.target:
            print("ERROR: --drive requires --target", file=sys.stderr)
            return 2
        extra = []
        for p in a.extra_param:
            extra += ["--extra-param", p]
        for conns in [int(x) for x in a.ladder.split(",")]:
            paths.append(drive_rung(a.target, a.auth_token, a.id_pool, conns,
                                    a.duration, extra, a.protocol_version, tag))

    points = [rp for rp in (_run_point(p) for p in paths) if rp is not None]
    points.sort(key=lambda p: p["connections"])
    if not points:
        print("ERROR: no usable run reports (need config.connections)", file=sys.stderr)
        return 2

    cliff = find_cliff(points, a.p95_threshold)
    cliff_conns = cliff["cliff_conns"]
    checks: list[dict] = []
    for p in points:
        healthy = p in cliff["healthy_rungs"]
        checks.append({"connections": p["connections"], "p95": p["p95"],
                       "errors": p["errors"], "failed_open": p["failed_open"],
                       "healthy": healthy})
    verdict = "PASS" if cliff_conns >= a.blessed_conns else "FAIL"
    summary = (f"capacity cliff = {cliff_conns} conns "
               f"(blessed {a.blessed_conns}, p95 threshold {a.p95_threshold}ms) "
               f"{'OK' if verdict == 'PASS' else 'REGRESSED'}")
    report = {"schema": 1, "gate": "G22", "name": "capacity-cliff",
              "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "verdict": verdict, "summary": summary,
              "cliff_conns": cliff_conns, "blessed_conns": a.blessed_conns,
              "p95_threshold_ms": a.p95_threshold, "curve": checks,
              "cliff_point": cliff.get("cliff_point")}
    print(summary)
    for c in checks:
        tag_s = "healthy" if c["healthy"] else "UNHEALTHY"
        print(f"  {c['connections']:>4} conns  p95={c['p95']}  "
              f"errors={c['errors']}  failed_open={c['failed_open']}  {tag_s}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[verdict]


if __name__ == "__main__":
    raise SystemExit(main())
