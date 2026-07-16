#!/usr/bin/env python3
"""
resource_sampler.py — samples zero-cache resource + state metrics during an ART
run so leaks show up as SLOPES, not just snapshots.

Per sample (default every 10s):
  - docker stats     : CPU %, RSS bytes of the zero-cache container
  - pprof (Go build) : goroutine count, HeapAlloc/HeapInuse/HeapSys/NumGC
                       from /debug/pprof/{goroutine,heap}?debug=1
  - CVR state        : total client-group rows + art-% (harness) rows —
                       the leak we've already caught once: departed client
                       groups that never get GC'd keep burning CPU

Writes ndjson while running; on exit (SIGTERM/duration) writes
<out>.summary.json with first/last/max and per-hour linear-regression slopes.
Also snapshots the binary heap profile at start+end (heap-first.pb.gz /
heap-last.pb.gz next to --out) for `go tool pprof -diff_base` drill-down.

Stdlib only. Run standalone or let run-art-local.sh manage it:
    python3 tools/resource_sampler.py --out reports/soak.ndjson --duration 3600
"""
from __future__ import annotations

import argparse
import json
import re
import signal
import subprocess
import sys
import time
import urllib.request


def sh(cmd: list[str], timeout: int = 20) -> str:
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout).stdout.strip()


def parse_size(s: str) -> float:
    """'1.589GiB' / '17.79MiB' / '712KiB' / '512B' -> bytes."""
    m = re.match(r"([\d.]+)\s*([KMGT]?i?B)", s.strip())
    if not m:
        return 0.0
    n = float(m.group(1))
    unit = m.group(2)
    mult = {"B": 1, "KiB": 2**10, "KB": 1e3, "MiB": 2**20, "MB": 1e6,
            "GiB": 2**30, "GB": 1e9, "TiB": 2**40, "TB": 1e12}.get(unit, 1)
    return n * mult


def docker_stats(container: str) -> dict:
    out = sh(["docker", "stats", "--no-stream", "--format", "{{json .}}", container])
    try:
        d = json.loads(out)
        mem = d["MemUsage"].split("/")
        return {"cpu_pct": float(d["CPUPerc"].rstrip("%")),
                "rss_bytes": parse_size(mem[0]),
                # container limit: lets the gate flag OOM proximity (a 2 GiB
                # limit OOM-killed the Go syncer mid-run once — invisible until
                # the pod died because nothing tracked peak-vs-limit headroom)
                "mem_limit_bytes": parse_size(mem[1]) if len(mem) > 1 else None}
    except Exception:
        return {"cpu_pct": None, "rss_bytes": None, "mem_limit_bytes": None}


def pprof_text(base: str, profile: str) -> str:
    with urllib.request.urlopen(f"{base}/debug/pprof/{profile}?debug=1",
                                timeout=10) as r:
        return r.read().decode("utf-8", "replace")


def go_runtime(base: str) -> dict:
    out: dict = {}
    try:
        g = pprof_text(base, "goroutine")
        m = re.match(r"goroutine profile: total (\d+)", g)
        out["goroutines"] = int(m.group(1)) if m else None
    except Exception:
        out["goroutines"] = None
    try:
        h = pprof_text(base, "heap")
        for key in ("HeapAlloc", "HeapInuse", "HeapSys", "NumGC"):
            m = re.search(rf"# {key} = (\d+)", h)
            out[key.lower()] = int(m.group(1)) if m else None
    except Exception:
        pass
    return out


def snapshot_heap(base: str, path: str) -> None:
    try:
        with urllib.request.urlopen(f"{base}/debug/pprof/heap", timeout=15) as r:
            with open(path, "wb") as f:
                f.write(r.read())
    except Exception as e:
        print(f"  (heap snapshot failed: {e})", file=sys.stderr)


def snapshot_cpu_profile(base: str, path: str, duration_s: int = 30) -> None:
    """Capture a 30s CPU profile for pprof drill-down (#3).
    Latency spikes in G5 could be GC pauses or hot loops — a CPU profile
    attributes them to their root cause."""
    try:
        url = f"{base}/debug/pprof/profile?seconds={duration_s}"
        with urllib.request.urlopen(url, timeout=duration_s + 10) as r:
            with open(path, "wb") as f:
                f.write(r.read())
    except Exception as e:
        print(f"  (cpu profile failed: {e})", file=sys.stderr)


def snapshot_trace(base: str, path: str, duration_s: int = 10) -> None:
    """Capture a runtime/trace for GC pause analysis (#3).
    go tool trace shows GC pause durations, syscall blocking, and scheduler
    latency — attributes G5 spikes to GC vs engine vs network."""
    try:
        url = f"{base}/debug/pprof/trace?seconds={duration_s}"
        with urllib.request.urlopen(url, timeout=duration_s + 10) as r:
            with open(path, "wb") as f:
                f.write(r.read())
    except Exception as e:
        print(f"  (trace capture failed: {e})", file=sys.stderr)


def gc_stats(base: str) -> dict:
    """Extract GC pause statistics from pprof heap text (#3).
    NumGC is already captured; this adds PauseNs (total GC pause time)
    and LastGC (when the last GC happened) so we can compute per-sample
    GC pause rate — a 2s GC pause shows as a latency spike in G5 but
    isn't attributed to GC without this."""
    out: dict = {}
    try:
        h = pprof_text(base, "heap")
        for key in ("NumGC", "PauseNs", "LastGC", "PauseTotalNs", "GCPauseNs"):
            m = re.search(rf"# {key} = (\d+)", h)
            if m:
                out[key.lower()] = int(m.group(1))
        # Also try goroutine count from goroutine profile
        g = pprof_text(base, "goroutine?debug=2")
        m = re.search(r"goroutine profile: total (\d+)", g)
        if m:
            out["goroutine_count_detailed"] = int(m.group(1))
    except Exception:
        pass
    return out


def conn_pool_stats(container: str) -> dict:
    """Read connection pool config from container env (#7).
    If the pool is exhausted, queries queue silently — the ART sees latency
    but doesn't know it's pool contention vs engine slowness."""
    out: dict = {}
    try:
        env = sh(["docker", "inspect", "--format", "{{range .Config.Env}}{{println .}}{{end}}",
                  container])
        for line in env.split("\n"):
            if "ZERO_CVR_MAX_CONNS" in line:
                out["cvr_max_conns"] = int(line.split("=")[1])
            elif "ZERO_UPSTREAM_MAX_CONNS" in line:
                out["upstream_max_conns"] = int(line.split("=")[1])
            elif "ZERO_GO_SIDECAR_PULL_WINDOW" in line:
                out["pull_window"] = int(line.split("=")[1])
            elif "ZERO_NUM_SYNC_WORKERS" in line:
                out["sync_workers"] = int(line.split("=")[1])
    except Exception:
        pass
    return out


def cvr_counts(pg_container: str, db: str, cvr_schema: str) -> dict:
    q = (f'SELECT count(*), count(*) FILTER (WHERE "clientGroupID" LIKE \'art-%\') '
         f'FROM "{cvr_schema}".instances;')
    out = sh(["docker", "exec", pg_container, "psql", "-U", "xyne", "-d", db, "-Atc", q])
    try:
        total, art = out.split("|")
        return {"cvr_instances": int(total), "cvr_art_instances": int(art)}
    except Exception:
        return {"cvr_instances": None, "cvr_art_instances": None}


def wal_size(container: str) -> dict:
    """Get WAL file size and DB file size from the container.
    WAL growth is the early warning signal for WAL-pin starvation (W5)."""
    try:
        out = sh(["docker", "exec", container, "ls", "-la", "/var/zero/zero.db-wal"])
        # Parse: -rw-r--r-- 1 root root 1234567 Jul 16 ... /var/zero/zero.db-wal
        parts = out.split()
        wal_bytes = int(parts[4]) if len(parts) > 4 else 0
    except Exception:
        wal_bytes = 0
    try:
        out = sh(["docker", "exec", container, "ls", "-la", "/var/zero/zero.db"])
        parts = out.split()
        db_bytes = int(parts[4]) if len(parts) > 4 else 0
    except Exception:
        db_bytes = 0
    return {"wal_bytes": wal_bytes, "db_bytes": db_bytes}


def slope_per_hour(samples: list[tuple[float, float]]) -> float | None:
    """Least-squares slope in units/hour over (ts, value) points."""
    pts = [(t, v) for t, v in samples if v is not None]
    if len(pts) < 3:
        return None
    n = len(pts)
    mt = sum(t for t, _ in pts) / n
    mv = sum(v for _, v in pts) / n
    denom = sum((t - mt) ** 2 for t, _ in pts)
    if denom == 0:
        return None
    return sum((t - mt) * (v - mv) for t, v in pts) / denom * 3600.0


def main() -> int:
    ap = argparse.ArgumentParser(description="ART resource/leak sampler.")
    ap.add_argument("--container", default="xyne-sandbox-rust-test-zero-cache")
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--cvr-schema", default="sandbox_rust_test_0/cvr")
    ap.add_argument("--pprof", default="http://localhost:6060",
                    help="Go pprof base URL ('' to disable)")
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--duration", type=float, default=0, help="0 = until SIGTERM")
    ap.add_argument("--out", required=True, help="ndjson output path")
    a = ap.parse_args()

    stop = False

    def on_sig(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, on_sig)
    signal.signal(signal.SIGINT, on_sig)

    heap_prefix = re.sub(r"\.ndjson$", "", a.out)
    if a.pprof:
        snapshot_heap(a.pprof, heap_prefix + ".heap-first.pb.gz")
        snapshot_cpu_profile(a.pprof, heap_prefix + ".cpu-profile.pb.gz", duration_s=30)
        snapshot_trace(a.pprof, heap_prefix + ".trace.pb.gz", duration_s=10)

    t0 = time.time()
    rows: list[dict] = []
    with open(a.out, "w") as f:
        while not stop and (a.duration <= 0 or time.time() - t0 < a.duration):
            s: dict = {"ts": round(time.time() - t0, 1)}
            s.update(docker_stats(a.container))
            if a.pprof:
                s.update(go_runtime(a.pprof))
                # GC pause tracking (#3): sample gc stats every interval
                # so we can see if a latency spike correlates with a GC pause
                gc = gc_stats(a.pprof)
                if gc:
                    s["gc_num"] = gc.get("numgc")
                    s["gc_pause_total_ns"] = gc.get("pausetotalns")
            s.update(cvr_counts(a.pg_container, a.db, a.cvr_schema))
            s.update(wal_size(a.container))
            f.write(json.dumps(s) + "\n")
            f.flush()
            rows.append(s)
            # sleep in small steps so SIGTERM lands promptly
            for _ in range(int(a.interval * 10)):
                if stop:
                    break
                time.sleep(0.1)

    if a.pprof:
        snapshot_heap(a.pprof, heap_prefix + ".heap-last.pb.gz")
        snapshot_cpu_profile(a.pprof, heap_prefix + ".cpu-profile-end.pb.gz", duration_s=30)

    metrics = ["cpu_pct", "rss_bytes", "goroutines", "heapalloc", "heapinuse",
               "heapsys", "cvr_instances", "cvr_art_instances",
               "wal_bytes", "db_bytes"]
    summary: dict = {"samples": len(rows),
                     "window_s": round(rows[-1]["ts"], 1) if rows else 0}

    # GC stats: track pause time delta across the run (#3)
    if a.pprof:
        first_gc_num = next((r.get("gc_num") for r in rows if r.get("gc_num") is not None), None)
        last_gc_num = next((r.get("gc_num") for r in reversed(rows) if r.get("gc_num") is not None), None)
        first_gc_pause = next((r.get("gc_pause_total_ns") for r in rows if r.get("gc_pause_total_ns") is not None), None)
        last_gc_pause = next((r.get("gc_pause_total_ns") for r in reversed(rows) if r.get("gc_pause_total_ns") is not None), None)
        if first_gc_num is not None and last_gc_num is not None:
            gc_pauses = last_gc_num - first_gc_num
            gc_pause_total_ms = ((last_gc_pause - first_gc_pause) / 1e6
                                 if first_gc_pause is not None and last_gc_pause is not None
                                 and last_gc_pause > first_gc_pause else 0)
            gc_pause_avg_ms = gc_pause_total_ms / gc_pauses if gc_pauses > 0 else 0
            summary["gc"] = {
                "pauses_during_run": gc_pauses,
                "total_pause_ms": round(gc_pause_total_ms, 1),
                "avg_pause_ms": round(gc_pause_avg_ms, 1),
            }

    # connection pool stats (#7)
    pool = conn_pool_stats(a.container)
    if pool:
        summary["conn_pool"] = pool

    limits = [r.get("mem_limit_bytes") for r in rows if r.get("mem_limit_bytes")]
    if limits:
        summary["mem_limit_bytes"] = limits[-1]
    for m in metrics:
        vals = [(r["ts"], r.get(m)) for r in rows]
        present = [v for _, v in vals if v is not None]
        if not present:
            continue
        summary[m] = {
            "first": present[0], "last": present[-1], "max": max(present),
            "slope_per_hour": (round(slope_per_hour(vals), 1)
                               if slope_per_hour(vals) is not None else None),
        }
    spath = heap_prefix + ".summary.json"
    
    # G6b goroutine leak detection: if the run was >=15min, assert
    # goroutine count delta is bounded. A growing goroutine count with
    # flat connection count = leaked goroutines (pool readers, hydrate
    # lanes, progress handler flags not freed).
    if summary.get("window_s", 0) >= 900 and "goroutines" in summary:
        g = summary["goroutines"]
        delta = g["last"] - g["first"]
        summary["goroutine_leak_check"] = {
            "delta": delta,
            "first": g["first"],
            "last": g["last"],
            "verdict": "PASS" if delta < 50 else "FAIL",
            "note": (f"goroutine count grew by {delta} over {g['last']:.0f}s window "
                     f"({g['first']} -> {g['last']}) — "
                     f"{'bounded' if delta < 50 else 'possible leak'}"),
        }
    
    # WAL growth alert: WAL-pin starvation (W5) shows as WAL file growth
    # without corresponding DB growth. >100MB/h = WATCH.
    if "wal_bytes" in summary:
        w = summary["wal_bytes"]
        if w.get("slope_per_hour") and w["slope_per_hour"] > 100_000_000:
            summary["wal_growth_alert"] = {
                "slope_per_hour": w["slope_per_hour"],
                "verdict": "WATCH",
                "note": f"WAL growing {w['slope_per_hour'] / 1e6:.0f}MB/h — possible WAL-pin starvation",
            }
    
    with open(spath, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"resource summary: {spath}")
    for m in ("rss_bytes", "goroutines", "heapinuse", "cvr_art_instances", "wal_bytes"):
        if m in summary:
            v = summary[m]
            print(f"  {m}: {v['first']} -> {v['last']} "
                  f"(slope {v['slope_per_hour']}/h)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
