#!/usr/bin/env python3
"""
Capacity ladder for Rust IVM: pins container to N cores,
runs ART replay at increasing CG counts, and finds the p95 knee.

Usage:
  python3 capacity-ladder.py --cpus 1 --sync-workers 1 --steps 5,10,15,20,25,30 --duration 120
  python3 capacity-ladder.py --cpus 2 --sync-workers 2 --steps 10,20,30,40,50,60 --duration 120

The script:
  1. Backs up the current docker-compose.override.yml
  2. Writes a temp override with the given CPU/worker config
  3. Restarts the container, waits for healthy
  4. For each CG count: runs replay-only ART, collects p50/p95/RSS/CPU/errors
  5. Restores the original override
  6. Prints a summary table and identifies the knee

No gates, no mutations, no oracle — pure replay load to measure IVM throughput.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ART_DIR = Path(__file__).resolve().parent
SANDBOX_DIR = Path(os.environ.get(
    "SANDBOX_DIR",
    "/Users/kartik.parsoya/Documents/xyne-spaces-test/.sandboxes/rust-test",
))
OVERRIDE = SANDBOX_DIR / "docker-compose.override.yml"
BACKUP = SANDBOX_DIR / "docker-compose.override.yml.bak-capladder"
CONTAINER = "xyne-sandbox-rust-test-zero-cache"
TARGET = "ws://localhost:4848/zero"


def build_override(cpus, sync_workers):
    return f"""# TEMPORARY: capacity-ladder.py — {cpus} CPU budget, read_lanes=2, sync_workers={sync_workers}
services:
  zero-cache:
    image: zero-cache-rust-ivm:latest
    platform: linux/arm64
    pull_policy: never
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:4848/"]
      interval: 5s
      timeout: 3s
      retries: 10
    ports:
      - "4848:4848"
    mem_limit: 24g
    cpus: {cpus}
    environment:
      - USE_RUST_IVM=true
      - RUST_IVM_ADDON_PATH=/app/mono/packages/rust-ivm/napi/rust-ivm.node
      - RUST_IVM_READ_LANES=2
      - RUST_IVM_PLANNER=1
      - ZERO_APP_ID=sandbox_rust_test
      - ZERO_NUM_SYNC_WORKERS={sync_workers}
      - UV_THREADPOOL_SIZE=64
      - ZERO_CVR_MAX_CONNS=8
      - ZERO_UPSTREAM_MAX_CONNS=16
      - ZERO_SYNCER_LOAD_AWARE_ROUTING=1
      - ZERO_SYNCER_CONTROLLED_REHOME=1
      - ZERO_LEAST_LOADED_ROUTING=1
      - ZERO_LOG_LEVEL=info
      - NODE_OPTIONS=--max-old-space-size=8192 --max-http-header-size=262144
"""


def run(cmd, check=True, timeout=None, capture=True):
    """Run a command, return CompletedProcess."""
    if capture:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    else:
        r = subprocess.run(cmd, shell=True, timeout=timeout)
    if check and r.returncode != 0:
        print(f"  Command failed: {cmd}", file=sys.stderr)
        if capture:
            print(f"  stderr: {r.stderr[:500]}", file=sys.stderr)
        raise RuntimeError(f"Command failed: {cmd}")
    return r


def wait_healthy(timeout=60):
    """Wait for the zero-cache container to be healthy."""
    deadline = time.time() + timeout
    status = ""
    while time.time() < deadline:
        r = run(f"docker inspect --format='{{{{json .State.Health.Status}}}}' {CONTAINER}",
                check=False)
        status = r.stdout.strip().strip('"')
        if status == "healthy":
            return True
        time.sleep(2)
    print(f"  Container not healthy after {timeout}s (status={status})", file=sys.stderr)
    return False


def docker_stats():
    """Get current CPU% and RSS from docker stats."""
    r = run(f"docker stats --no-stream --format '{{{{.CPUPerc}}}} {{{{.MemUsage}}}}' {CONTAINER}",
            check=False)
    if r.returncode != 0 or not r.stdout.strip():
        return None, None
    parts = r.stdout.strip().split()
    cpu_pct = float(parts[0].rstrip("%"))
    # MemUsage: "1.2GiB / 24GiB"
    mem_str = parts[1]
    if mem_str.endswith("GiB"):
        rss = float(mem_str[:-3]) * 1024**3
    elif mem_str.endswith("MiB"):
        rss = float(mem_str[:-3]) * 1024**2
    elif mem_str.endswith("KiB"):
        rss = float(mem_str[:-3]) * 1024
    else:
        rss = 0
    return cpu_pct, rss


def setup_ladder_override(cpus, sync_workers):
    """Back up current override and write the ladder override."""
    if OVERRIDE.exists():
        content = OVERRIDE.read_text()
        if "capacity-ladder.py" in content:
            print(f"  WARNING: current override is already a ladder override — "
                  f"no original to back up")
        else:
            shutil.copy2(OVERRIDE, BACKUP)
            print(f"  Backed up {OVERRIDE.name} -> {BACKUP.name}")
    OVERRIDE.write_text(build_override(cpus, sync_workers))
    print(f"  Wrote ladder override (cpus={cpus}, read_lanes=2, sync_workers={sync_workers})")


def restore_override():
    """Restore the original override."""
    if not BACKUP.exists():
        print(f"  No backup found — left ladder override in place")
        return
    content = BACKUP.read_text()
    if "capacity-ladder.py" in content:
        print(f"  WARNING: backup is also a ladder override — "
              f"original override was lost. Leaving ladder override in place.")
        return
    shutil.move(str(BACKUP), str(OVERRIDE))
    print(f"  Restored original override")


def restart_container():
    """Recreate the zero-cache container with the new override."""
    print("  Recreating zero-cache container...")
    run(f"cd {SANDBOX_DIR} && docker compose up -d zero-cache", timeout=60)
    print("  Waiting for healthy...", end="", flush=True)
    if wait_healthy():
        print(" OK")
    else:
        print(" FAILED")
        return False
    return True


def purge_cvr():
    """Purge art-% CVR rows for a clean run."""
    print("  Purging art-% CVR rows...")
    run(f"docker exec {CONTAINER} node -e \""
        f"const D=require('/app/mono/node_modules/.pnpm/@rocicorp+zero-sqlite3@1.1.2/node_modules/@rocicorp/zero-sqlite3');"
        f"const db=new D('/var/zero/replica.db');"
        f"db.exec(\\\"DELETE FROM cvr WHERE clientGroupID LIKE 'art-%'\\\");"
        f"db.close();\" 2>/dev/null", check=False)


def run_art_step(cgs, duration):
    """Run a single ART replay step with the given CG count.

    Note: run-art-local.sh may return exit 1 even on successful replay
    (e.g. from downstream gate failures). We treat the run as successful
    if a run-*.json report was produced.
    """
    print(f"\n{'='*60}")
    print(f"  CGs={cgs}  duration={duration}s")
    print(f"{'='*60}")

    purge_cvr()
    time.sleep(2)

    # Count existing reports so we can detect the new one
    existing = set((ART_DIR / "reports").glob("run-*.json"))

    cmd = (
        f"cd {ART_DIR} && ./run-art-local.sh "
        f"--connections {cgs} --duration {duration} "
        f"--target {TARGET} --users 3"
    )
    r = run(cmd, check=False, timeout=duration + 120)

    # Find the new run report (not in existing set)
    new_reports = sorted(set((ART_DIR / "reports").glob("run-*.json")) - existing)
    res_reports = sorted((ART_DIR / "reports").glob("resources-*.summary.json"))

    if not new_reports:
        print(f"  No run report produced (exit={r.returncode})", file=sys.stderr)
        return None

    run_data = json.loads(new_reports[-1].read_text())
    # Use the latest resource report
    res_data = json.loads(res_reports[-1].read_text()) if res_reports else {}

    # Snapshot docker stats right after the run
    cpu_pct, rss_now = docker_stats()

    counters = run_data.get("counters", {})
    lat = run_data.get("client_latency_steady_ms", run_data.get("client_latency_ms", {}))

    result = {
        "cgs": cgs,
        "p50": lat.get("p50", 0),
        "p95": lat.get("p95", 0),
        "p99": lat.get("p99", 0),
        "errors": counters.get("errors", 0),
        "reconnects": counters.get("reconnects", 0),
        "rehomes": counters.get("rehomes", 0),
        "opened": counters.get("opened", 0),
        "pokes": counters.get("pokes", 0),
        "rss_end_gb": res_data.get("rss_bytes", {}).get("last", 0) / 1e9,
        "rss_max_gb": res_data.get("rss_bytes", {}).get("max", 0) / 1e9,
        "cpu_pct_last": res_data.get("cpu_pct", {}).get("last", 0),
        "cpu_pct_max": res_data.get("cpu_pct", {}).get("max", 0),
        "cpu_pct_snapshot": cpu_pct or 0,
        "rss_snapshot_gb": (rss_now or 0) / 1e9,
    }

    print(f"  p50={result['p50']:.1f}ms  p95={result['p95']:.1f}ms  "
          f"errors={result['errors']}  reconnects={result['reconnects']}  "
          f"RSS={result['rss_end_gb']:.2f}GB  CPU_max={result['cpu_pct_max']:.1f}%")

    return result


def find_knee(results):
    """Find the knee: first CG count where p95 jumps >50% above the baseline median."""
    if len(results) < 3:
        return None

    baseline_p95s = [r["p95"] for r in results[:3]]
    baseline = sorted(baseline_p95s)[len(baseline_p95s) // 2]

    threshold = baseline * 1.5  # 50% above baseline

    for i, r in enumerate(results):
        if r["p95"] > threshold and r["p95"] > baseline + 50:
            return {
                "cgs": r["cgs"],
                "baseline_p95": baseline,
                "knee_p95": r["p95"],
                "threshold": threshold,
            }
    return None


def print_table(results, cpus, sync_workers):
    """Print the summary table."""
    print(f"\n{'='*90}")
    print(f"  CAPACITY LADDER RESULTS — {cpus} CPU(s), read_lanes=2, sync_workers={sync_workers}")
    print(f"{'='*90}")
    print(f"{'CGs':>5} {'p50(ms)':>8} {'p95(ms)':>8} {'p99(ms)':>8} "
          f"{'errors':>7} {'reconn':>7} {'RSS(GB)':>8} {'CPU_max%':>9} "
          f"{'pokes':>7}")
    print("-" * 90)

    for r in results:
        flag = ""
        if r["errors"] > 0:
            flag = " ← ERRORS"
        elif r["p95"] > results[0]["p95"] * 2:
            flag = " ← HIGH"
        print(f"{r['cgs']:>5} {r['p50']:>8.1f} {r['p95']:>8.1f} {r['p99']:>8.1f} "
              f"{r['errors']:>7} {r['reconnects']:>7} {r['rss_end_gb']:>8.2f} "
              f"{r['cpu_pct_max']:>9.1f} {r['pokes']:>7}{flag}")

    print("-" * 90)

    knee = find_knee(results)
    if knee:
        print(f"\n  KNEE: p95 jumps at {knee['cgs']} CGs "
              f"({knee['baseline_p95']:.1f}ms → {knee['knee_p95']:.1f}ms, "
              f"threshold={knee['threshold']:.1f}ms)")
        # Per-core extrapolation
        cgs_per_core = knee["cgs"] / cpus
        print(f"  Per-core capacity: ~{cgs_per_core:.0f} CGs/core (with {sync_workers} sync worker(s) per {cpus} core(s))")
        print(f"  On 32 cores: ~{cgs_per_core * 32:.0f} CGs max (single worker pool)")
        print(f"  On 32 cores × 4 sync workers: ~{cgs_per_core * 32 * 4:.0f} CGs max")
    else:
        print(f"\n  No knee detected — p95 stayed flat through {results[-1]['cgs']} CGs")
        cgs_per_core = results[-1]["cgs"] / cpus
        print(f"  Per-core capacity: >= {cgs_per_core:.0f} CGs/core")
        print(f"  On 32 cores: >= {cgs_per_core * 32:.0f} CGs max")


def main():
    ap = argparse.ArgumentParser(description="Rust IVM capacity ladder")
    ap.add_argument("--cpus", type=int, default=1, help="CPU budget (default 1)")
    ap.add_argument("--sync-workers", type=int, default=1,
                    help="ZERO_NUM_SYNC_WORKERS (default 1)")
    ap.add_argument("--start", type=int, default=5, help="Starting CG count")
    ap.add_argument("--step", type=int, default=5, help="CG count increment")
    ap.add_argument("--max", type=int, default=50, help="Max CG count")
    ap.add_argument("--steps", type=str, default=None,
                    help="Comma-separated CG counts (overrides start/step/max)")
    ap.add_argument("--duration", type=int, default=120,
                    help="Seconds per run (default 120)")
    ap.add_argument("--out", type=str, default=None,
                    help="Output JSON file (default reports/capladder-<ts>.json)")
    ap.add_argument("--no-restore", action="store_true",
                    help="Don't restore the original override after (debug)")
    args = ap.parse_args()

    if args.steps:
        steps = [int(x) for x in args.steps.split(",")]
    else:
        steps = list(range(args.start, args.max + 1, args.step))

    print(f"Capacity ladder: {steps} CGs, {args.duration}s each")
    print(f"Config: cpus={args.cpus}, sync_workers={args.sync_workers}, read_lanes=2")
    print(f"Container: {CONTAINER}")
    print(f"Target: {TARGET}")

    # 1. Setup override
    print("\n--- Setup ---")
    setup_ladder_override(args.cpus, args.sync_workers)

    # 2. Restart container
    if not restart_container():
        restore_override()
        sys.exit(1)

    # 3. Run ladder
    results = []
    try:
        for cgs in steps:
            r = run_art_step(cgs, args.duration)
            if r:
                results.append(r)
            else:
                print(f"  Step failed, skipping {cgs} CGs")
    except KeyboardInterrupt:
        print("\n  Interrupted by user")
    finally:
        # 4. Restore override. Never let cleanup crash the run — otherwise the
        # results table/JSON below is lost. The post-restore restart can fail
        # legitimately (e.g. the original override pins linux/amd64 and only the
        # local arm64 image is present); that must not mask a good ladder.
        if not args.no_restore:
            print("\n--- Cleanup ---")
            try:
                restore_override()
                print("  Restarting with original config...")
                r = run(f"cd {SANDBOX_DIR} && docker compose up -d zero-cache",
                        timeout=60, check=False)
                if r.returncode != 0:
                    print(f"  NOTE: post-restore restart failed (rc={r.returncode}); "
                          f"the rust container from the ladder run is left in place.")
                else:
                    wait_healthy()
            except Exception as e:  # noqa: BLE001 — cleanup must never crash results
                print(f"  NOTE: cleanup hit {type(e).__name__}: {e}")

    # 5. Results
    if results:
        print_table(results, args.cpus, args.sync_workers)

        ts = time.strftime("%Y%m%d-%H%M%S")
        out_file = args.out or str(ART_DIR / "reports" / f"capladder-{ts}.json")
        Path(out_file).parent.mkdir(parents=True, exist_ok=True)
        Path(out_file).write_text(json.dumps({
            "config": {
                "cpus": args.cpus,
                "read_lanes": 2,
                "sync_workers": args.sync_workers,
                "duration_s": args.duration,
            },
            "results": results,
            "knee": find_knee(results),
        }, indent=2))
        print(f"\n  Report: {out_file}")


if __name__ == "__main__":
    main()
