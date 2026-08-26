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
    """Get SQLite WAL file sizes from the container.
    WAL growth is the early warning signal for checkpoint starvation.
    wal2 is the previous-WAL retained by litefs-style backup; it should stay
    flat. If wal grows while wal2 stays flat, checkpointing is starving."""
    out = {"wal_bytes": 0, "wal2_bytes": 0, "db_bytes": 0, "wal_ratio": 0.0}
    for key, path in (("wal_bytes", "/var/zero/replica.db-wal"),
                      ("wal2_bytes", "/var/zero/replica.db-wal2"),
                      ("db_bytes", "/var/zero/replica.db")):
        try:
            sh_out = sh(["docker", "exec", container, "ls", "-la", path])
            parts = sh_out.split()
            out[key] = int(parts[4]) if len(parts) > 4 else 0
        except Exception:
            pass
    if out["db_bytes"] > 0:
        out["wal_ratio"] = round(out["wal_bytes"] / out["db_bytes"], 4)
    return out


# better-sqlite3 ships inside the zero-cache image (zero-cache dependency),
# so we can probe sqlite checkpoint state without adding host-side tools.
# PRAGMA wal_checkpoint(PASSIVE) returns (busy, log, checkpointed):
#   busy=1          -> checkpoint could not run, readers are blocking
#   log             -> total frames currently in WAL
#   checkpointed    -> frames flushed back to the main db file this attempt
CKPT_SCRIPT = (
    "const D=require('@rocicorp/zero-sqlite3');"
    "const db=new D('/var/zero/replica.db');"
    "try{console.log(JSON.stringify(db.pragma('wal_checkpoint(PASSIVE)')[0]))}"
    "catch(e){console.log(JSON.stringify({error:String(e)}))}"
    "db.close();"
)
CKPT_CWD = "/app/mono/packages/zero-cache"


def sqlite_ckpt(container: str) -> dict:
    """Probe sqlite checkpoint state via node+better-sqlite3 in the container.
    Failure signature: ckpt_done stays 0 while wal_bytes keeps growing."""
    out = {"ckpt_busy": None, "ckpt_log": None, "ckpt_done": None}
    try:
        r = sh(["docker", "exec", "-w", CKPT_CWD, container,
                "node", "-e", CKPT_SCRIPT],
               timeout=15)
        d = json.loads(r)
        out["ckpt_busy"] = d.get("busy")
        out["ckpt_log"] = d.get("log")
        out["ckpt_done"] = d.get("checkpointed")
    except Exception:
        pass
    return out


def pg_slot_lag(pg_container: str, db: str) -> dict:
    """Sample Postgres replication slot lag in bytes.
    Lag > 0 that doesn't shrink = a change-streamer consumer stalling.
    Lag > 1GB is always a problem; slot never advances = orphaned slot."""
    q = ("SELECT slot_name, "
         "pg_wal_lsn_diff(pg_current_wal_flush_lsn(), restart_lsn) "
         "FROM pg_replication_slots ORDER BY slot_name;")
    out = sh(["docker", "exec", pg_container, "psql", "-U", "xyne", "-d", db,
              "-Atc", q, "--no-psqlrc"])
    slots = []
    total_lag = 0
    max_lag = 0
    for line in out.splitlines():
        if "|" not in line:
            continue
        name, lag_s = line.split("|", 1)
        name, lag_s = name.strip(), lag_s.strip()
        try:
            lag = int(lag_s)
        except ValueError:
            continue
        slots.append({"name": name, "lag_bytes": lag})
        total_lag += lag
        max_lag = max(max_lag, lag)
    return {"pg_slots": slots, "pg_slot_total_lag": total_lag,
            "pg_slot_max_lag": max_lag}


def pg_business_metrics(pg_container: str, app_id: str, db: str) -> dict:
    out: dict = {}
    try:
        # Combined single query to keep sample overhead low
        q = (
            f"SELECT "
            f"(SELECT count(*) FROM \"{app_id}_0/cvr\".instances),"
            f"(SELECT count(*) FROM \"{app_id}_0/cvr\".desires),"
            f"(SELECT count(*) FROM \"{app_id}_0/cvr\".rows),"
            f"(SELECT count(*) FROM \"{app_id}_0\".mutations),"
            f"(SELECT cast(coalesce(max(\"dataVersion\"), 0) as bigint) "
            f" FROM \"{app_id}_0/cvr\".\"versionHistory\")"
        )
        raw = sh(["docker", "exec", pg_container, "psql", "-U", "xyne",
                  "-d", db, "-Atc", q, "--no-psqlrc"])
        parts = raw.strip().split("|")
        if len(parts) >= 5:
            out["active_clients"] = int(parts[0])
            out["active_queries"] = int(parts[1])
            out["rows_tracked"] = int(parts[2])
            out["mutations_total"] = int(parts[3])
            # dataVersion = CVR replication watermark (proxy for hydration progress)
            out["cvr_data_version"] = int(parts[4])
    except Exception:
        pass
    return out


def prom_metrics(url: str) -> dict:
    """Scrape a Prometheus /metrics endpoint and extract avg latencies
    and rates from OTel histograms and counters.

    Looks for zero.* names. Returns avg latency in ms for histograms,
    rate-per-second for counters."""
    out: dict = {}
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            text = r.read().decode("utf-8", "replace")
    except Exception:
        return out

    # Parse all histogram _sum/_count pairs, then match by suffix
    hist_sum = {}   # suffix -> total value
    hist_count = {} # suffix -> total observations
    counters = {}   # suffix -> cumulative count

    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        raw_name = parts[0].strip()
        # Strip label block: name{label="v",...} -> name
        name = raw_name.split("{")[0] if "{" in raw_name else raw_name
        try:
            val = float(parts[1])
        except ValueError:
            continue
        if name.endswith("_sum"):
            base = name[:-4]
            hist_sum[base] = val
        elif name.endswith("_count"):
            base = name[:-6]
            hist_count[base] = val
        elif name.startswith("zero_"):
            # Capture all zero_* gauges/counters for direct export
            counters[name] = val

    # Direct-mapped gauge/counter names from the OTel pipeline.
    # These appear as e.g. zero_replication_total_lag_millisecond
    direct_map = {
        "zero_replication_total_lag_millisecond":   "replication_lag_ms",
        "zero_replication_upstream_lag_millisecond": "replication_upstream_lag_ms",
        "zero_replication_replica_lag_millisecond":  "replication_replica_lag_ms",
        "zero_replication_events_total":             "replication_events_total",
        "zero_replica_db_size_bytes":                "replica_db_bytes",
        "zero_replica_wal_size_bytes":               "replica_wal_bytes",
        "zero_sync_active_client_groups":            "sync_active_cgs",
        "zero_sync_queries":                         "sync_queries",
        "zero_sync_rows":                             "sync_rows",
        "zero_server_uptime_seconds":                "server_uptime_s",
    }
    for src, dst in direct_map.items():
        if src in counters:
            out[dst] = counters[src]

    # For every histogram with both sum+count, compute avg latency (ms)
    def hist_avg_ms(base: str) -> float | None:
        total = hist_sum.get(base, 0)
        cnt = hist_count.get(base, 0)
        if cnt <= 0:
            return None
        return round(total / cnt * 1000, 2)

    def hist_rate_per_min(base: str) -> float | None:
        total = hist_sum.get(base, 0)
        cnt = hist_count.get(base, 0)
        if total <= 0:
            cnt = hist_count.get(base, 0)
            if cnt <= 0:
                return None
        return round(cnt * 60, 1)  # if the value is total, convert

    # The OTel SDK names metrics like:
    #   zero.sync.poke_time{...} -> _sum (ms), _count (observations)
    # For histograms computed as avg latency, use sum/count directly

    # Map OTel metric names to our output keys
    hist_map = {
        "advance_time_seconds":             "advance_avg_ms",
        "cvr_flush_time_seconds":           "cvr_flush_avg_ms",
        "hydration_time_seconds":           "hydration_avg_ms",
        "lock_wait_time_seconds":           "lock_wait_avg_ms",
        "poke_time_seconds":                "poke_avg_ms",
        "query_transformation_time_seconds": "query_transform_avg_ms",
        "transaction_advance_time":         "txn_advance_avg_ms",
    }
    for suffix, out_key in hist_map.items():
        for base in list(hist_sum.keys()):
            if base == suffix or base.endswith(suffix) or base.endswith(suffix.replace("_seconds", "")):
                avg = hist_avg_ms(base)
                if avg is not None:
                    out[out_key] = avg

    # Counters: extract zero.* totals as rates
    for name, val in counters.items():
        short = name.replace("zero.", "").replace("zero.sync.", "")
        if short.endswith("_total") or short.endswith("_processed") or short.endswith("_synced"):
            key = short.replace("_total", "").replace("_processed", "")
            out[f"{key}_per_min"] = round(val * 60, 1) if val else 0

    return out


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


def steady_slope_per_hour(
    samples: list[tuple[float, float]],
    warmup_s: float = 900.0,
    warmup_frac: float = 0.5,
) -> float | None:
    """Leak-oriented slope: fit only the STEADY-STATE window, excluding the
    cold-start ramp. A full-window linear fit is dominated by warmup — on a
    flat 1h 20-conn soak (2026-08-11) RSS ramped 1.07→3.3GB in the first
    3 min of hydration then plateaued: full-window fit read +495MB/h while
    the steady-state fit read -32MB/h. A leak gate fed the full-window
    number fails every cold-started soak regardless of leak reality.
    Warmup = the larger of `warmup_s` or `warmup_frac` of the window (i.e.
    the fit covers the LAST HALF of the run); falls back to the full-window
    fit when the steady window is too thin. Fitting only the last half loses
    no sensitivity to real leaks — a genuine leak is linear and shows the
    same slope in any window — while excluding slow multi-phase ramps that a
    fixed 10-20% cutoff still catches (measured 2026-08-11: a run whose ramp
    ran ~20min read +253MB/h with a 20% cutoff vs -44MB/h over its second
    half, with the container settling to 0.6GB after teardown — no leak).
    """
    pts = [(t, v) for t, v in samples if v is not None]
    if len(pts) < 3:
        return None
    t0, t1 = pts[0][0], pts[-1][0]
    cut = t0 + max(warmup_s, (t1 - t0) * warmup_frac)
    steady = [p for p in pts if p[0] >= cut]
    if len(steady) < 3:
        return slope_per_hour(pts)
    return slope_per_hour(steady)


# --------------------------------------------------------------------------- #
# WAL reclaim verdict — the wal2 zombie-pin / lagging-snapshot detector.
#
# WHY the old ckpt_done==0 heuristic is not enough (established empirically on
# real wal2, rust-ivm wal2-probe-matrix.mjs): on wal2 `wal_checkpoint(PASSIVE)`
# only ever checkpoints the INACTIVE file, and a read-mark blocks the file
# SWITCH, not the pragma. So a connection leaked with its read transaction still
# open (the "zombie pin" that grew a prod pod's WAL to 5.2GB, linear, at the
# write rate) still reports ckpt_done > 0 and ckpt_busy = 0 every sample — the
# WAL just never gets RECLAIMED. The faithful signature is therefore not "the
# checkpoint is busy/stuck" but "the WAL grew large and never came back down".
#
# Healthy wal2 under write load ping-pongs: -wal grows, the engine switches to
# -wal2, -wal is reset -> a SAWTOOTH with many reclaim events (wal_bytes drops).
# A zombie/lagging pin produces MONOTONIC growth with zero reclaim events even
# though checkpoints keep running. This function classifies that from the WAL
# byte series the sampler already collects — no synthetic writes into the live
# replica, no docker; it is a pure function so tests/ can drive it directly.
# --------------------------------------------------------------------------- #
def wal_reclaim_verdict(
    samples: "list[dict]",
    *,
    reclaim_min_bytes: int = 4 * 1024 * 1024,
    wal_floor_bytes: int = 64 * 1024 * 1024,
    wal_ratio_fail: float = 0.05,
) -> dict:
    """Classify WAL reclaimability over a run.

    A reclaim event = wal_bytes dropped by >= reclaim_min_bytes between samples
    (a wal2 file switch + checkpoint reused the file). FAIL when the WAL grew
    past wal_floor_bytes AND ratio past wal_ratio_fail AND NO reclaim ever
    happened while checkpoints were being attempted — the zombie/lagging-pin
    signature the ckpt_done==0 gate is blind to on wal2.

    Returns a verdict dict; verdict == 'skip' when there is not enough signal
    (too few samples, or WAL never got large enough to require a reclaim).
    """
    wal = [(s.get("ts"), s.get("wal_bytes")) for s in samples
           if s.get("wal_bytes") is not None]
    wal = [(t, v) for t, v in wal if t is not None]
    if len(wal) < 3:
        return {"verdict": "skip", "note": "not enough wal_bytes samples"}

    first = wal[0][1]
    last = wal[-1][1]
    peak = max(v for _, v in wal)

    reclaim_events = 0
    max_drop = 0
    for (_, prev), (_, cur) in zip(wal, wal[1:]):
        drop = prev - cur
        if drop >= reclaim_min_bytes:
            reclaim_events += 1
            max_drop = max(max_drop, drop)

    # Did checkpointing actually run? (distinguishes "pinned" from "idle db")
    ckpt_done = [s.get("ckpt_done") for s in samples
                 if s.get("ckpt_done") is not None]
    ckpt_attempted = any(v and v > 0 for v in ckpt_done)

    # WAL/DB ratio at the peak — a large WAL relative to the db file.
    db_last = next((s.get("db_bytes") for s in reversed(samples)
                    if s.get("db_bytes")), 0) or 0
    ratio_peak = round(peak / db_last, 4) if db_last else 0.0

    grew_large = peak >= wal_floor_bytes and (
        ratio_peak >= wal_ratio_fail or db_last == 0)
    slope = slope_per_hour(wal)

    base = {
        "wal_first": first, "wal_last": last, "wal_peak": peak,
        "db_last": db_last, "wal_ratio_peak": ratio_peak,
        "reclaim_events": reclaim_events, "max_reclaim_bytes": max_drop,
        "ckpt_attempted": ckpt_attempted,
        "wal_slope_per_hour": round(slope, 1) if slope is not None else None,
    }

    if not grew_large:
        return {**base, "verdict": "skip",
                "note": (f"WAL stayed small (peak {peak/1e6:.0f}MB, "
                         f"ratio {ratio_peak:.3f}) — no reclaim required")}

    if reclaim_events == 0:
        # Grew large, checkpoints ran (or nothing at all reclaimed it) yet the
        # WAL never dropped once = a read-mark is pinned below head. This is the
        # zombie-connection / lagging-snapshot class, invisible to ckpt_busy on
        # wal2.
        return {**base, "verdict": "FAIL",
                "note": (f"WAL grew to {peak/1e6:.0f}MB (ratio {ratio_peak:.3f}) "
                         f"and NEVER reclaimed (0 reclaim events) while "
                         f"checkpoints {'ran' if ckpt_attempted else 'did not run'} "
                         f"— zombie/lagging read-mark pinning wal2 rotation")}

    # It grew large but did reclaim at least once — bounded sawtooth. Still
    # WATCH if the trend is strongly upward despite reclaims (slow leak).
    if slope is not None and slope > 100_000_000 and last > first * 1.5:
        return {**base, "verdict": "WATCH",
                "note": (f"WAL reclaims ({reclaim_events}x) but trends up "
                         f"{slope/1e6:.0f}MB/h — partial pin / checkpoint lag")}

    return {**base, "verdict": "PASS",
            "note": (f"WAL bounded: peak {peak/1e6:.0f}MB, "
                     f"{reclaim_events} reclaim events")}


def main() -> int:
    ap = argparse.ArgumentParser(description="ART resource/leak sampler.")
    ap.add_argument("--container", default="xyne-sandbox-rust-test-zero-cache")
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--app-id", default="sandbox_rust_test",
                    help="ZERO_APP_ID — used to construct the shard and cvr schema names")
    ap.add_argument("--cvr-schema", default="sandbox_rust_test_0/cvr")
    ap.add_argument("--pprof", default="http://localhost:6060",
                    help="Go pprof base URL ('' to disable)")
    ap.add_argument("--prom", default="",
                    help="Prometheus /metrics URL (e.g. http://localhost:9464/metrics) for OTel latency histograms")
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
            s.update(sqlite_ckpt(a.container))
            s.update(pg_slot_lag(a.pg_container, a.db))
            s.update(pg_business_metrics(a.pg_container, a.app_id, a.db))
            if a.prom:
                s.update(prom_metrics(a.prom))
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
               "wal_bytes", "wal2_bytes", "db_bytes", "wal_ratio",
               "pg_slot_total_lag", "pg_slot_max_lag",
               "active_clients", "active_queries", "rows_tracked",
               "mutations_total", "cvr_data_version",
               "ckpt_busy", "ckpt_log", "ckpt_done",
               "replication_lag_ms", "replication_upstream_lag_ms",
               "replication_replica_lag_ms", "replication_events_total",
               "replica_db_bytes", "replica_wal_bytes",
               "sync_active_cgs", "sync_queries", "sync_rows",
               "server_uptime_s",
               "poke_avg_ms", "hydration_avg_ms", "advance_avg_ms",
               "cvr_flush_avg_ms", "txn_advance_avg_ms",
               "query_transform_avg_ms"]
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
        full_slope = slope_per_hour(vals)
        steady_slope = steady_slope_per_hour(vals)
        summary[m] = {
            "first": present[0], "last": present[-1], "max": max(present),
            "slope_per_hour": (round(full_slope, 1)
                               if full_slope is not None else None),
            # Warmup-excluded fit — what the G6 leak gate should read (the
            # full-window slope is kept for reference / ramp diagnosis).
            "steady_slope_per_hour": (round(steady_slope, 1)
                                      if steady_slope is not None else None),
        }
    # G7 CVR drain metrics: for the ART client-group instance count, measure
    # whether it DRAINS after its peak (clients leaving + server CVR-GC
    # reclaiming) and how long the post-peak window was. The gate needs this to
    # tell "GC never had a chance (post-peak window < inactivity threshold)"
    # apart from "GC ran but reclaimed nothing" — the #113 distinction that a
    # first/last/max summary alone cannot make. Computed off the full series.
    art_series = [(r["ts"], r.get("cvr_art_instances")) for r in rows
                  if r.get("cvr_art_instances") is not None]
    if art_series and "cvr_art_instances" in summary:
        peak_val = max(v for _, v in art_series)
        peak_ts = next(ts for ts, v in art_series if v == peak_val)
        after_peak = [v for ts, v in art_series if ts >= peak_ts]
        min_after_peak = min(after_peak) if after_peak else peak_val
        end_ts = art_series[-1][0]
        post_peak_window_s = round(end_ts - peak_ts, 1)
        drain_frac = (round((peak_val - min_after_peak) / peak_val, 4)
                      if peak_val else 0.0)
        summary["cvr_art_instances"].update({
            "min_after_peak": min_after_peak,
            "peak_ts": round(peak_ts, 1),
            "post_peak_window_s": post_peak_window_s,
            "drain_frac": drain_frac,
        })

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

    # WAL ratio alert: WAL/DB ratio > 0.05 means WAL is accumulating but
    # not being checkpointed — a long-lived reader is holding the WAL open.
    if "wal_ratio" in summary:
        ratio = summary["wal_ratio"]
        if ratio.get("max") and ratio["max"] > 0.05:
            summary["wal_ratio_alert"] = {
                "max_ratio": ratio["max"],
                "wal_max": summary.get("wal_bytes", {}).get("max"),
                "db": summary.get("db_bytes", {}).get("max"),
                "verdict": "WATCH",
                "note": (f"WAL/DB ratio peaked at {ratio['max']:.3f} — "
                         f"long-lived reader likely pinning WAL"),
            }

    # PG slot lag alerts. Total lag > 1GB is always a problem; lag that
    # grows over the run means the streamer is falling behind.
    if "pg_slot_total_lag" in summary:
        lag = summary["pg_slot_total_lag"]
        if lag.get("max") and lag["max"] > 1_000_000_000:
            summary["pg_lag_alert"] = {
                "total_lag_max": lag["max"],
                "total_lag_first": lag.get("first"),
                "total_lag_last": lag.get("last"),
                "slope_per_hour": lag.get("slope_per_hour"),
                "verdict": "WATCH",
                "note": (f"PG slot total lag hit {lag['max'] / 1e9:.2f}GB — "
                         f"change-streamer likely falling behind or stalled slot"),
            }

    # Checkpoint starvation (legacy signature): WAL grows but ckpt_done stays
    # zero. Kept as a subset — it only fires on plain-WAL or a totally-stuck
    # checkpointer; on wal2 a zombie pin still shows ckpt_done > 0 (it
    # checkpoints the inactive file), so this alone misses the prod class.
    ckpt_rows = [r for r in rows if r.get("ckpt_done") is not None]
    if len(ckpt_rows) >= 3 and "wal_bytes" in summary:
        wal_first = summary["wal_bytes"].get("first") or 0
        wal_last = summary["wal_bytes"].get("last") or 0
        wal_slope = summary["wal_bytes"].get("slope_per_hour") or 0
        ckpt_done_max = max(r["ckpt_done"] for r in ckpt_rows)
        ckpt_log_max = max(r.get("ckpt_log") or 0 for r in ckpt_rows)
        if wal_last > wal_first * 1.2 and ckpt_done_max == 0:
            summary["ckpt_starvation_alert"] = {
                "wal_first": wal_first, "wal_last": wal_last,
                "wal_slope_per_hour": wal_slope,
                "ckpt_done_max": ckpt_done_max, "ckpt_log_max": ckpt_log_max,
                "verdict": "FAIL",
                "note": (f"WAL grew {wal_first}B -> {wal_last}B but sqlite "
                         f"checkpoint never completed (done_max=0, log_max={ckpt_log_max})"),
            }

    # WAL reclaim gate (wal2-faithful zombie/lagging-pin detector): the WAL grew
    # large and NEVER reclaimed. This is the signature the ckpt_done==0 gate
    # above is blind to on wal2 — the connection-leak-with-open-read-txn class
    # that grew a prod pod's WAL to 5.2GB linearly. See wal_reclaim_verdict.
    reclaim = wal_reclaim_verdict(rows)
    if reclaim.get("verdict") not in (None, "skip"):
        summary["wal_reclaim_gate"] = reclaim

    # Orphaned slot: a slot whose lag never changes while the run proceeds
    # is a leak — probably a previous ART run that crashed without cleaning up
    # its replication slot. Catch lag_first == lag_last > 0.
    if "pg_slots" in summary:
        # pg_slots is a list over rows, not a scalar — need to re-summarize
        orphan_candidates = []
        if rows:
            for slot in set(s["name"] for r in rows for s in r.get("pg_slots", [])):
                slot_rows = [(r["ts"], next((s["lag_bytes"] for s in r.get("pg_slots", []) if s["name"] == slot), None)) for r in rows]
                slot_rows = [(t, v) for t, v in slot_rows if v is not None]
                if len(slot_rows) >= 3:
                    first, last = slot_rows[0][1], slot_rows[-1][1]
                    if first > 10_000_000 and abs(last - first) < 1024:
                        orphan_candidates.append({"slot": slot, "lag_bytes": first})
        if orphan_candidates:
            summary["orphaned_slots"] = {
                "slots": orphan_candidates,
                "verdict": "FAIL",
                "note": (f"{len(orphan_candidates)} slot(s) with lag "
                         f">{10e6:.0f}B that never advanced — likely orphaned from crashed runs"),
            }

    with open(spath, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"resource summary: {spath}")
    for m in ("rss_bytes", "goroutines", "heapinuse", "cvr_art_instances",
              "wal_bytes", "wal2_bytes", "wal_ratio", "pg_slot_total_lag",
              "active_clients", "active_queries", "rows_tracked",
              "mutations_total",
              "replication_lag_ms", "sync_active_cgs",
              "poke_avg_ms", "hydration_avg_ms", "advance_avg_ms",
              "cvr_flush_avg_ms", "txn_advance_avg_ms",
              "ckpt_log", "ckpt_done", "ckpt_busy"):
        if m in summary:
            v = summary[m]
            if isinstance(v, dict) and "first" in v:
                print(f"  {m}: {v['first']} -> {v['last']} "
                      f"(slope {v['slope_per_hour']}/h)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
