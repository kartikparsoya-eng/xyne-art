#!/usr/bin/env python3
"""
heap_diff.py — automated pprof heap-growth report for the Go zero-cache build.

Wraps `go tool pprof -top -diff_base first.pb.gz last.pb.gz` over the heap
snapshots the resource sampler drops next to each run
(reports/resources-<tag>.heap-{first,last}.pb.gz) and prints the top growth
sites, plus a one-line total. Optionally gates on total in-use growth.

    python3 tools/heap_diff.py                        # newest snapshot pair
    python3 tools/heap_diff.py --tag 20260702-154353  # a specific run
    python3 tools/heap_diff.py --first a.pb.gz --last b.pb.gz
    python3 tools/heap_diff.py --fail-over-mb 300     # exit 1 if growth > 300MB

Exit codes: 0 = ok (or growth under threshold), 1 = growth over threshold,
2 = usage/environment error (missing snapshots or no `go` binary).
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import shutil
import subprocess
import sys

REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")

# pprof -top value column, e.g. "512.01MB", "-3.5kB", "1.2GB", "100B"
_UNIT = {"B": 1, "kB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40}
_VAL_RE = re.compile(r"^\s*(-?[\d.]+)(B|kB|MB|GB|TB)\b")


def newest_pair() -> tuple[str, str] | None:
    firsts = sorted(glob.glob(os.path.join(REPORTS, "resources-*.heap-first.pb.gz")))
    for first in reversed(firsts):
        last = first.replace(".heap-first.", ".heap-last.")
        if os.path.exists(last):
            return first, last
    return None


def parse_bytes(line: str) -> float | None:
    m = _VAL_RE.match(line)
    if not m:
        return None
    return float(m.group(1)) * _UNIT[m.group(2)]


def main() -> int:
    ap = argparse.ArgumentParser(description="pprof heap diff (first vs last snapshot).")
    ap.add_argument("--tag", default=None,
                    help="run tag: uses reports/resources-<tag>.heap-{first,last}.pb.gz")
    ap.add_argument("--first", default=None, help="baseline heap profile (.pb.gz)")
    ap.add_argument("--last", default=None, help="end-of-run heap profile (.pb.gz)")
    ap.add_argument("--top", type=int, default=15, help="growth sites to show")
    ap.add_argument("--sample-index", default="inuse_space",
                    help="pprof sample index (inuse_space|alloc_space|...)")
    ap.add_argument("--fail-over-mb", type=float, default=None,
                    help="exit 1 if total positive in-use growth exceeds this")
    a = ap.parse_args()

    if a.first and a.last:
        first, last = a.first, a.last
    elif a.tag:
        first = os.path.join(REPORTS, f"resources-{a.tag}.heap-first.pb.gz")
        last = os.path.join(REPORTS, f"resources-{a.tag}.heap-last.pb.gz")
    else:
        pair = newest_pair()
        if pair is None:
            print("ERROR: no resources-*.heap-{first,last}.pb.gz pair in reports/",
                  file=sys.stderr)
            return 2
        first, last = pair
    for p in (first, last):
        if not os.path.exists(p):
            print(f"ERROR: missing snapshot: {p}", file=sys.stderr)
            return 2

    go = shutil.which("go")
    if go is None:
        print("ERROR: `go` not found on PATH — install Go or run manually:\n"
              f"  go tool pprof -top -diff_base {first} {last}", file=sys.stderr)
        return 2

    cmd = [go, "tool", "pprof", "-top", f"-nodecount={a.top}",
           f"-sample_index={a.sample_index}", "-diff_base", first, last]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        print("ERROR: pprof timed out", file=sys.stderr)
        return 2
    if out.returncode != 0:
        print(f"ERROR: pprof failed:\n{out.stderr.strip()}", file=sys.stderr)
        return 2

    print(f"heap diff ({a.sample_index}): {os.path.basename(first)} -> "
          f"{os.path.basename(last)}")
    growth = 0.0
    in_rows = False
    for line in out.stdout.splitlines():
        if line.lstrip().startswith("flat "):
            in_rows = True
            print(line)
            continue
        if not in_rows:
            continue
        b = parse_bytes(line)
        if b is None:
            continue
        if b > 0:
            growth += b
        print(line)

    print(f"\ntotal positive in-use growth: {growth / 2**20:.1f} MB")
    if a.fail_over_mb is not None and growth > a.fail_over_mb * 2**20:
        print(f"HEAP DIFF: FAIL (growth > {a.fail_over_mb} MB)")
        return 1
    print("HEAP DIFF: OK" + (f" (limit {a.fail_over_mb} MB)"
                             if a.fail_over_mb is not None else " (report-only)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
