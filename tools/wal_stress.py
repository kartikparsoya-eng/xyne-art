#!/usr/bin/env python3
"""wal_stress.py — drive sustained pg writes to tickle the WAL growth path.

Inserts rows into a dedicated `wal_stress` table in the sandbox DB.
The changes propagate via the replication slot into the sqlite replica,
causing sqlite-side WAL growth. Used with the resource sampler to detect
sqlite checkpoint starvation.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time


def sh(cmd, timeout=30):
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout).stdout.strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--duration", type=float, default=900)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--sleep-between-batches", type=float, default=0.5)
    a = ap.parse_args()

    # Create the table
    sh(["docker", "exec", a.pg_container, "psql", "-U", "xyne", "-d", a.db,
        "-c", "CREATE TABLE IF NOT EXISTS wal_stress (id bigserial PRIMARY KEY, payload text NOT NULL, created_at timestamptz DEFAULT now());"])

    payload = "x" * 200  # 200-byte payload per row
    values = ",".join(f"('{payload}')" for _ in range(a.batch_size))
    insert_sql = f"INSERT INTO wal_stress (payload) VALUES {values};"

    t0 = time.time()
    total = 0
    n = 0
    while time.time() - t0 < a.duration:
        sh(["docker", "exec", a.pg_container, "psql", "-U", "xyne", "-d", a.db,
            "-c", insert_sql])
        total += a.batch_size
        n += 1
        if n % 20 == 0:
            dt = time.time() - t0
            print(f"  {dt:.0f}s: {n} batches, {total} rows", file=sys.stderr)
        time.sleep(a.sleep_between_batches)

    dt = time.time() - t0
    print(f"DONE: {total} rows in {dt:.0f}s ({total / dt:.0f} rows/s)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
