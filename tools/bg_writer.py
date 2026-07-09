#!/usr/bin/env python3
"""
bg_writer.py — rate-controlled background replication load (the missing axis).

Why: prod's replication stream runs ~40 evt/s steady / ~206 evt/s on bulk
days, and it comes mostly from OTHER writers, not the connected clients —
driver mutations at prod cadence generate only ~0.3 evt/s. Every prod pod
replicates the WHOLE stream, so a prod-calibrated local run must too: without
it the advance path (the thing that produces prod's 425 resets/h and the
economic-abort behavior) is never exercised. Replication evt/s is an
INTENSIVE quantity — never scaled by S.

Targets rows the CGs actually subscribe to (hot channels by participant
count): 3× conversations.lastActivityAt + 1× channel_stats.lastActivityAt
per batch = 4 events/batch. channel_stats is deliberate — it's the table the
heaviest prod queries subscribe to (ORDER BY lastActivityAt DESC), so every
touch advances the expensive pipelines. Column names verified against the
sandbox schema (conversations has NO updatedAt — lastActivityAt is the one).

One long-lived psql (docker exec -i) with statement pacing — no per-batch
exec overhead, sustains 200 evt/s.

    .venv/bin/python tools/bg_writer.py --rate 40 --i-know-this-writes
    .venv/bin/python tools/bg_writer.py --rate 200 --duration 1800 --i-know-this-writes
"""
from __future__ import annotations

import argparse
import random
import signal
import subprocess
import sys
import time

BATCH_EVENTS = 4          # 3 conversation rows + 1 channel_stats row


def fetch(pg: str, user: str, db: str, sql: str) -> list[str]:
    out = subprocess.run(["docker", "exec", pg, "psql", "-U", user, "-d", db,
                          "-Atc", sql], capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        print(f"ERROR: {out.stderr.strip()[:200]}", file=sys.stderr)
        raise SystemExit(1)
    return [ln for ln in out.stdout.splitlines() if ln]


def main() -> int:
    ap = argparse.ArgumentParser(description="Background replication writer.")
    ap.add_argument("--rate", type=float, default=40.0,
                    help="events/sec (prod steady=40, bulk-day=206)")
    ap.add_argument("--duration", type=float, default=0,
                    help="seconds (0 = until SIGTERM/SIGINT)")
    ap.add_argument("--hot-channels", type=int, default=50,
                    help="target rows within top-N channels by participation")
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--pg-user", default="xyne")
    ap.add_argument("--db", default="sandbox_rust_test_db")
    ap.add_argument("--i-know-this-writes", action="store_true")
    a = ap.parse_args()
    if not a.i_know_this_writes:
        print("refusing: this tool WRITES to the sandbox DB continuously. "
              "Pass --i-know-this-writes.", file=sys.stderr)
        return 2

    hot = fetch(a.pg_container, a.pg_user, a.db,
                'SELECT "channelId" FROM public.channel_user_status '
                f'GROUP BY 1 ORDER BY count(*) DESC LIMIT {a.hot_channels}')
    conv = fetch(a.pg_container, a.pg_user, a.db,
                 'SELECT "conversationId" FROM public.conversations WHERE '
                 '"channelId" IN (' + ",".join(f"'{c}'" for c in hot) + ") LIMIT 5000")
    if not conv or not hot:
        print("ERROR: no hot conversations/channels found", file=sys.stderr)
        return 1
    print(f"targets: {len(conv)} conversations across {len(hot)} hot channels; "
          f"rate={a.rate:.0f} evt/s ({a.rate / BATCH_EVENTS:.1f} batches/s)")

    proc = subprocess.Popen(
        ["docker", "exec", "-i", a.pg_container, "psql", "-q",
         "-U", a.pg_user, "-d", a.db],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, text=True)

    stop = False

    def on_sig(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, on_sig)
    signal.signal(signal.SIGINT, on_sig)

    rng = random.Random()
    interval = BATCH_EVENTS / a.rate
    t0 = time.monotonic()
    next_at = t0
    sent = 0
    last_report = t0
    while not stop:
        now = time.monotonic()
        if a.duration and now - t0 >= a.duration:
            break
        if now < next_at:
            time.sleep(min(next_at - now, 0.05))
            continue
        next_at += interval
        cids = rng.sample(conv, 3)
        ch = rng.choice(hot)
        try:
            proc.stdin.write(
                'UPDATE public.conversations SET "lastActivityAt"=now() '
                'WHERE "conversationId" IN (' + ",".join(f"'{c}'" for c in cids) + ");\n"
                'UPDATE public.channel_stats SET "lastActivityAt"=now() '
                f"WHERE \"channelId\"='{ch}';\n")
            proc.stdin.flush()
        except BrokenPipeError:
            print("psql pipe died — exiting", file=sys.stderr)
            return 1
        sent += BATCH_EVENTS
        if now - last_report >= 30:
            eff = sent / (now - t0)
            print(f"  [{now - t0:6.0f}s] events={sent} effective={eff:.1f}/s")
            last_report = now
    proc.stdin.close()
    proc.wait(timeout=10)
    dt = time.monotonic() - t0
    print(f"done: {sent} events in {dt:.0f}s ({sent / max(dt, 1):.1f}/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
