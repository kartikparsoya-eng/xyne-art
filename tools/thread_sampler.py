#!/usr/bin/env python3
"""
thread_sampler.py — per-thread CPU inside the zero-cache container + postgres
container CPU, sampled during a run leg.

Why (E4 falsification, 2026-07-09): E4 (W=1, 301-CG prod trace) showed
lockstep-ramping advance latencies across unrelated CGs, container CPU at
~180% of 1400%, ZERO napi stalls, RSS comfortable — every signature of
queueing on ONE serial resource. The candidate: the single Node event loop
that orchestrates every view-syncer at W=1 (per-CG advance driving, row
decode, CVR merge, poke assembly, socket frames — Go finishes its part and
the rest waits its turn on one JS thread). This tool turns that inference
into a direct observation: one thread pinned ~100% while 10+ cores idle.

Reads /proc/<pid>/task/<tid>/stat inside the container (one `docker exec`
per sample — no tooling assumptions beyond sh+grep), computes per-thread
CPU% from utime+stime jiffy deltas (USER_HZ=100), tags tid==pid as "main".
postgres CPU via `docker stats` each sample — 301 CGs' CVR flushes on 2 PG
cores is the one alternative suspect worth excluding.

    tools/thread_sampler.py --container xyne-sandbox-rust-test-zero-cache \
        --pg-container xyne-sandbox-postgres --interval 5 --duration 900 \
        --out reports/threads-w6.ndjson
Summary (top threads by mean CPU) prints on exit/SIGTERM and every 60s.
"""
from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import time
from collections import defaultdict

HZ = 100.0


def read_threads(container: str) -> dict[tuple[int, int], tuple[str, int]]:
    """{(pid, tid): (comm, jiffies)} for every thread in the container."""
    out = subprocess.run(
        ["docker", "exec", container, "sh", "-c",
         'grep -H "" /proc/[0-9]*/task/[0-9]*/stat 2>/dev/null || true'],
        capture_output=True, text=True, timeout=20)
    threads: dict[tuple[int, int], tuple[str, int]] = {}
    for ln in out.stdout.splitlines():
        try:
            path, content = ln.split(":", 1)
            parts = path.split("/")
            pid, tid = int(parts[2]), int(parts[4])
            comm = content.split("(", 1)[1].rsplit(")", 1)[0]
            rest = content.rsplit(")", 1)[1].split()
            jiffies = int(rest[11]) + int(rest[12])      # utime + stime
            threads[(pid, tid)] = (comm, jiffies)
        except (IndexError, ValueError):
            continue
    return threads


def pg_cpu(container: str) -> float | None:
    try:
        out = subprocess.run(["docker", "stats", "--no-stream", "--format",
                              "{{.CPUPerc}}", container],
                             capture_output=True, text=True, timeout=15)
        return float(out.stdout.strip().rstrip("%"))
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Per-thread CPU sampler.")
    ap.add_argument("--container", required=True)
    ap.add_argument("--pg-container", default=None)
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--duration", type=float, default=900.0)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    stop = False

    def on_sig(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, on_sig)
    signal.signal(signal.SIGINT, on_sig)

    stats = defaultdict(list)         # (pid, tid, comm) -> [cpu%...]
    pg_samples: list[float] = []
    prev: dict[tuple[int, int], tuple[str, int]] = {}
    prev_t = 0.0
    t0 = time.monotonic()
    last_summary = t0
    f = open(a.out, "w")

    def summary(final: bool = False) -> None:
        rows = sorted(((sum(v) / len(v), max(v), k) for k, v in stats.items()
                       if v), reverse=True)[:12]
        tag = "FINAL" if final else f"t+{time.monotonic()-t0:.0f}s"
        print(f"--- top threads by mean CPU%% [{tag}] ---")
        for mean, mx, (pid, tid, comm) in rows:
            role = " <== main JS thread" if tid == pid else ""
            print(f"  pid={pid} tid={tid} {comm:18} mean={mean:5.1f}% max={mx:5.1f}%{role}")
        if pg_samples:
            print(f"  postgres container: mean={sum(pg_samples)/len(pg_samples):.0f}% "
                  f"max={max(pg_samples):.0f}%")
        sys.stdout.flush()

    while not stop and time.monotonic() - t0 < a.duration:
        try:
            now = time.monotonic()
            cur = read_threads(a.container)
            if prev and cur:
                dt = now - prev_t
                ts = round(time.time(), 1)
                for key, (comm, j) in cur.items():
                    if key in prev:
                        dj = j - prev[key][1]
                        cpu = dj / HZ / dt * 100.0
                        if cpu > 1.0:
                            stats[(key[0], key[1], comm)].append(cpu)
                            f.write(json.dumps({"ts": ts, "pid": key[0],
                                                "tid": key[1], "comm": comm,
                                                "cpu_pct": round(cpu, 1)}) + "\n")
                if a.pg_container:
                    p = pg_cpu(a.pg_container)
                    if p is not None:
                        pg_samples.append(p)
                        f.write(json.dumps({"ts": ts, "pg_cpu_pct": p}) + "\n")
                f.flush()
            prev, prev_t = cur, now
        except Exception as e:      # pod restarting mid-leg: keep going
            print(f"  (sample failed, retrying: {str(e)[:60]})", file=sys.stderr)
            prev = {}
        if time.monotonic() - last_summary >= 60:
            summary()
            last_summary = time.monotonic()
        time.sleep(a.interval)
    summary(final=True)
    f.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
