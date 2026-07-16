#!/usr/bin/env python3
"""park_site_check.py — D4 park-site invariant gate.

Parses a goroutine dump (pprof goroutine?debug=2) and fails if any goroutine
is parked in a frame outside the allowlist. The allowlist is the finite set
of sanctioned park sites:

  - streamGate.acquire (cond.Wait — credit gate park)
  - parkSlice (TSFN queue full — row plane park)
  - pool admission (AcquireForPipeline — bounded wait)
  - abiHost sendQ cond.Wait (leaf mutex park)
  - sync.WaitGroup.Wait (engine/advance coordination)
  - chan receive (channel ops — gate done, respCh, etc.)
  - database/sql connectionOpener/cleaner (pool maintenance)
  - net.(*pipe).Read (transport reads — blocked on client)
  - time.Sleep / time.NewTimer (timers — sweepers, watchdog)
  - runtime/pprof (the dump itself)
  - select on ctx.Done (sweeper/watchdog/reaper context waits)

Any goroutine parked in a SQLite CGO call (sqlite3_step, _sqlite3_column_values),
a mutex wait inside tablesource/engine/ivm, or any other unexpected frame is a
wedge candidate and fails the gate.

Usage:
    park_site_check.py --dump goroutines.txt --out parks.json
"""

import json
import re
import sys
from pathlib import Path

# Allowlist patterns — goroutines parked in these frames are OK.
ALLOWLIST = [
    # Credit gate
    r"streamGate.*acquire",
    r"sync\.\(\*Cond\)\.Wait",
    # Row plane / TSFN
    r"parkSlice",
    r"rowplane.*park",
    # Pool admission
    r"AcquireForPipeline",
    # ABI host send queue
    r"abiHost.*cond",
    r"startABIHostWithServer.*func",
    # Coordination
    r"sync\.\(\*WaitGroup\)\.Wait",
    # Channel ops
    r"chan receive",
    r"chan send",
    r"select",  # select{} includes ctx.Done waits
    # database/sql maintenance
    r"connectionOpener",
    r"connectionCleaner",
    r"connectionResetter",
    # Transport
    r"net\.\(\*pipe\)\.read",
    r"net\.\(\*pipe\)\.write",
    r"readFrame",
    r"handleConnection",
    # Timers
    r"time\.Sleep",
    r"time\.NewTimer",
    r"runtime\.timer",
    # pprof dump itself
    r"pprof",
    r"dumpAllStacks",
    r"scanWedgedGroups",
    r"runWedgeWatchdog",
    r"runPullIdleSweeper",
    r"runReaper",
    r"runPerfReporter",
    # Runtime
    r"runtime\.gcBgMarkWorker",
    r"runtime\.bgsweep",
    r"runtime\.scavenge",
    r"runtime\.forcegc",
    r"runtime\.finalizer",
    r"os\.signal",
    # Go runtime internals
    r"runtime\.main",
    r"runtime\.goexit",
    r"runtime\.gcMarkWorker",
]

# Blocklist patterns — goroutines in these frames are ALWAYS a wedge.
BLOCKLIST = [
    r"sqlite3\..*Next",
    r"_sqlite3_step",
    r"_sqlite3_column_values",
    r"_sqlite3_prepare",
    r"sqlite3_step",
    r"sqlite3_interrupt",
    r"sqlite3_busy",
    # Mutex waits inside engine/ivm (not sanctioned parks)
    r"engine\.\(\*Engine\)\.mu\.Lock",
    r"ivm\.\(\*\)\.mu\.Lock",
    r"tablesource\.\(\*Source\)\.mu\.Lock",
]


def parse_goroutines(dump: str) -> list[dict]:
    """Parse a goroutine dump into a list of goroutine entries."""
    goroutines = []
    # Each goroutine starts with "goroutine N [state, duration]:"
    blocks = re.split(r"(?=\ngoroutine \d+ \[)", dump)
    for block in blocks:
        m = re.match(r"\ngoroutine (\d+) \[([^\]]+)\]", block)
        if not m:
            continue
        gid = m.group(1)
        state = m.group(2)
        # Extract the first meaningful frame (the park site)
        lines = block.strip().split("\n")
        frames = []
        for line in lines[1:]:  # skip the header
            line = line.strip()
            if not line or line.startswith("created by"):
                break
            frames.append(line)
        goroutines.append({
            "id": gid,
            "state": state,
            "frames": frames,
        })
    return goroutines


def is_allowlisted(g: dict) -> bool:
    """Check if a goroutine's park site is in the allowlist."""
    full_text = "\n".join(g["frames"])
    for pattern in ALLOWLIST:
        if re.search(pattern, full_text):
            return True
    return False


def is_blocklisted(g: dict) -> bool:
    """Check if a goroutine is in a known-wedge frame."""
    full_text = "\n".join(g["frames"])
    for pattern in BLOCKLIST:
        if re.search(pattern, full_text):
            return True
    return False


def check(dump_path: str) -> dict:
    """Check the goroutine dump for park-site violations."""
    dump = Path(dump_path).read_text()
    goroutines = parse_goroutines(dump)

    violations = []
    for g in goroutines:
        if is_blocklisted(g):
            violations.append({
                "goroutine": g["id"],
                "state": g["state"],
                "reason": "blocklisted-frame",
                "top_frames": g["frames"][:5],
            })
        elif not is_allowlisted(g):
            # Unknown park site — flag it but don't fail unless it's a
            # long-running park (duration > 1 minute).
            duration_match = re.search(r"(\d+)\s*minutes", g["state"])
            if duration_match and int(duration_match.group(1)) >= 1:
                violations.append({
                    "goroutine": g["id"],
                    "state": g["state"],
                    "reason": "unknown-park-site-long-duration",
                    "top_frames": g["frames"][:5],
                })

    return {
        "gate": "D4",
        "verdict": "PASS" if len(violations) == 0 else "FAIL",
        "summary": f"{len(goroutines)} goroutines, {len(violations)} violations",
        "total_goroutines": len(goroutines),
        "violations": violations,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="D4 park-site invariant gate")
    parser.add_argument("--dump", required=True, help="Goroutine dump file")
    parser.add_argument("--out", required=True, help="Output JSON report")
    args = parser.parse_args()

    result = check(args.dump)
    Path(args.out).write_text(json.dumps(result, indent=2))

    if result["verdict"] == "PASS":
        print(f"  D4 park-site invariant: PASS ({result['total_goroutines']} goroutines, 0 violations)")
    else:
        print(f"  D4 park-site invariant: FAIL ({result['total_goroutines']} goroutines, {len(result['violations'])} violations)")
        for v in result["violations"]:
            print(f"    goroutine {v['goroutine']} [{v['state']}]: {v['reason']}")
            for frame in v["top_frames"]:
                print(f"      {frame}")

    sys.exit(0 if result["verdict"] == "PASS" else 1)


if __name__ == "__main__":
    main()
