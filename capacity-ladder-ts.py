#!/usr/bin/env python3
"""
Capacity ladder for TS zero-cache (rocicorp/zero:1.7.0).
Same methodology as capacity-ladder.py but targets the TS mirror container.

Usage:
  python3 capacity-ladder-ts.py --cpus 1 --sync-workers 1 --steps 5,10,15,20,25,30 --duration 120
  python3 capacity-ladder-ts.py --cpus 2 --sync-workers 2 --steps 10,20,30,40,50,60 --duration 120
  python3 capacity-ladder-ts.py --cpus 2 --sync-workers 1 --steps 10,20,30,40,50,60 --duration 120
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
CONTAINER = "xyne-sandbox-rust-test-zero-cache-ts"
TARGET = "ws://localhost:4849/zero"


def build_override(cpus, sync_workers):
    return f"""# TEMPORARY: capacity-ladder-ts.py — {cpus} CPU budget, sync_workers={sync_workers}
services:
  zero-cache-ts:
    image: rocicorp/zero:1.7.0
    container_name: xyne-sandbox-rust-test-zero-cache-ts
    mem_limit: 12g
    cpus: {cpus}
    environment:
      - ZERO_UPSTREAM_DB=postgresql://xyne:xyne123@xyne-sandbox-postgres:5432/sandbox_rust_test_db
      - ZERO_CVR_DB=postgresql://xyne:xyne123@xyne-sandbox-postgres:5432/sandbox_rust_test_db
      - ZERO_CHANGE_DB=postgresql://xyne:xyne123@xyne-sandbox-postgres:5432/sandbox_rust_test_db
      - ZERO_REPLICA_FILE=/var/zero/replica.db
      - ZERO_LOG_LEVEL=info
      - ZERO_AUTH_SECRET=dc811fedf0e830b571ccea97a504e6121e176eec60b1b5feaa121dcd1c46b7986a13953f66b21cffc3f3b8f3d8bbb0e3420b52d4141925a9f5d34416cfbf80b5
      - ZERO_ADMIN_PASSWORD=dev-admin-password
      - ZERO_MUTATE_URL=http://xyne-sandbox-rust-test-backend:3001/api/zero/push
      - ZERO_QUERY_URL=http://xyne-sandbox-rust-test-backend:3001/api/zero/query
      - ZERO_QUERY_FORWARD_COOKIES=true
      - ZERO_MUTATE_FORWARD_COOKIES=true
      - ZERO_CVR_MAX_CONNS=8
      - ZERO_UPSTREAM_MAX_CONNS=16
      - ZERO_NUM_SYNC_WORKERS={sync_workers}
      - UV_THREADPOOL_SIZE=64
      - NODE_ENV=development
      - NODE_OPTIONS=--max-old-space-size=8192 --max-http-header-size=262144
      - ZERO_PORT=4848
      - ZERO_APP_ID=sandbox_rust_test_ts
    networks:
      - sandbox-net
    ports:
      - "4849:4848"
    volumes:
      - zero_cache_ts_rust_test:/var/zero

volumes:
  zero_cache_ts_rust_test:
    driver: local
"""


def run(cmd, check=True, timeout=None, capture=True):
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
    """Wait for container to be running and responding."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = run(f"docker inspect --format='{{{{json .State.Running}}}}' {CONTAINER}", check=False)
        running = r.stdout.strip().strip('"')
        if running == "true":
            # TS container has no healthcheck — verify it responds on the port
            r2 = run(f"curl -sf http://localhost:4849/ -o /dev/null", check=False)
            if r2.returncode == 0:
                return True
        time.sleep(2)
    print(f"  Container not ready after {timeout}s", file=sys.stderr)
    return False


def docker_stats():
    r = run(f"docker stats --no-stream --format '{{{{.CPUPerc}}}} {{{{.MemUsage}}}}' {CONTAINER}", check=False)
    if r.returncode != 0 or not r.stdout.strip():
        return None, None
    parts = r.stdout.strip().split()
    cpu_pct = float(parts[0].rstrip("%"))
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
    if OVERRIDE.exists():
        content = OVERRIDE.read_text()
        if "capacity-ladder" in content:
            print(f"  WARNING: current override is already a ladder override — no original to back up")
        else:
            shutil.copy2(OVERRIDE, BACKUP)
            print(f"  Backed up {OVERRIDE.name} -> {BACKUP.name}")
    OVERRIDE.write_text(build_override(cpus, sync_workers))
    print(f"  Wrote TS ladder override (cpus={cpus}, sync_workers={sync_workers})")


def restore_override():
    if not BACKUP.exists():
        print(f"  No backup found — left ladder override in place")
        return
    content = BACKUP.read_text()
    if "capacity-ladder" in content:
        print(f"  WARNING: backup is also a ladder override — original was lost. Leaving in place.")
        return
    shutil.move(str(BACKUP), str(OVERRIDE))
    print(f"  Restored original override")


def restart_container():
    print("  Recreating TS container...")
    run(f"cd {SANDBOX_DIR} && docker compose up -d zero-cache-ts", timeout=60)
    print("  Waiting for ready...", end="", flush=True)
    if wait_healthy():
        print(" OK")
    else:
        print(" FAILED")
        return False
    return True


def purge_cvr():
    print("  Purging art-% CVR rows...")
    run(f"docker exec {CONTAINER} node -e \""
        f"const D=require('/opt/app/node_modules/.pnpm/@rocicorp+zero-sqlite3@1.1.2/node_modules/@rocicorp/zero-sqlite3');"
        f"const db=new D('/var/zero/replica.db');"
        f"db.exec(\\\"DELETE FROM cvr WHERE clientGroupID LIKE 'art-%'\\\");"
        f"db.close();\" 2>/dev/null", check=False)


def run_art_step(cgs, duration):
    print(f"\n{'='*60}")
    print(f"  CGs={cgs}  duration={duration}s")
    print(f"{'='*60}")

    purge_cvr()
    time.sleep(2)

    existing = set((ART_DIR / "reports").glob("run-*.json"))

    cmd = (
        f"cd {ART_DIR} && ./run-art-local.sh "
        f"--connections {cgs} --duration {duration} "
        f"--target {TARGET} --users 3"
    )
    r = run(cmd, check=False, timeout=duration + 120)

    new_reports = sorted(set((ART_DIR / "reports").glob("run-*.json")) - existing)
    res_reports = sorted((ART_DIR / "reports").glob("resources-*.summary.json"))

    if not new_reports:
        print(f"  No run report produced (exit={r.returncode})", file=sys.stderr)
        return None

    run_data = json.loads(new_reports[-1].read_text())
    res_data = json.loads(res_reports[-1].read_text()) if res_reports else {}

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
    if len(results) < 3:
        return None
    baseline_p95s = [r["p95"] for r in results[:3]]
    baseline = sorted(baseline_p95s)[len(baseline_p95s) // 2]
    threshold = baseline * 1.5
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
    print(f"\n{'='*90}")
    print(f"  CAPACITY LADDER RESULTS (TS zero-cache 1.7.0) — {cpus} CPU(s), sync_workers={sync_workers}")
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
    ap = argparse.ArgumentParser(description="TS zero-cache capacity ladder")
    ap.add_argument("--cpus", type=int, default=1, help="CPU budget (default 1)")
    ap.add_argument("--sync-workers", type=int, default=1, help="ZERO_NUM_SYNC_WORKERS (default 1)")
    ap.add_argument("--start", type=int, default=5, help="Starting CG count")
    ap.add_argument("--step", type=int, default=5, help="CG count increment")
    ap.add_argument("--max", type=int, default=50, help="Max CG count")
    ap.add_argument("--steps", type=str, default=None, help="Comma-separated CG counts")
    ap.add_argument("--duration", type=int, default=120, help="Seconds per run (default 120)")
    ap.add_argument("--out", type=str, default=None, help="Output JSON file")
    ap.add_argument("--no-restore", action="store_true", help="Don't restore original override")
    args = ap.parse_args()

    if args.steps:
        steps = [int(x) for x in args.steps.split(",")]
    else:
        steps = list(range(args.start, args.max + 1, args.step))

    print(f"TS Capacity ladder: {steps} CGs, {args.duration}s each")
    print(f"Config: cpus={args.cpus}, sync_workers={args.sync_workers}")
    print(f"Container: {CONTAINER}")
    print(f"Target: {TARGET}")

    print("\n--- Setup ---")
    setup_ladder_override(args.cpus, args.sync_workers)

    if not restart_container():
        restore_override()
        sys.exit(1)

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
        if not args.no_restore:
            print("\n--- Cleanup ---")
            restore_override()
            print("  Restarting with original config...")
            run(f"cd {SANDBOX_DIR} && docker compose up -d zero-cache-ts", timeout=60)
            wait_healthy()

    if results:
        print_table(results, args.cpus, args.sync_workers)

        ts_str = time.strftime("%Y%m%d-%H%M%S")
        out_file = args.out or str(ART_DIR / "reports" / f"capladder-ts-{ts_str}.json")
        Path(out_file).parent.mkdir(parents=True, exist_ok=True)
        Path(out_file).write_text(json.dumps({
            "config": {
                "cpus": args.cpus,
                "sync_workers": args.sync_workers,
                "duration_s": args.duration,
                "image": "rocicorp/zero:1.7.0",
            },
            "results": results,
            "knee": find_knee(results),
        }, indent=2))
        print(f"\n  Report: {out_file}")


if __name__ == "__main__":
    main()
