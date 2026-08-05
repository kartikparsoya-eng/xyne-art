#!/usr/bin/env python3
"""wal_reclaim_gate.py — the wal2 zombie-pin / lagging-snapshot WAL gate.

Reads a resource_sampler ndjson stream (its --out file: one JSON sample per
line, each carrying wal_bytes / ckpt_done / db_bytes) and applies
wal_reclaim_verdict. FAILs when the SQLite WAL grew large over the run and
NEVER reclaimed — the signature of a connection leaked with its read
transaction still open, which pins wal2 rotation and grows the WAL at the write
rate without bound (the prod incident: a pod's WAL hit 5.2GB, linear, while the
client group logged healthily on a fresh connection).

WHY a dedicated gate (not just the ckpt_busy sampler alert): on wal2,
`wal_checkpoint(PASSIVE)` only checkpoints the INACTIVE file and a read-mark
blocks the file SWITCH, not the pragma — so a zombie pin reports ckpt_busy=0 /
ckpt_done>0 every sample. The faithful signal is reclaimability: healthy wal2
under load SAWTOOTHS (grow, switch, reset); a pin grows MONOTONICALLY. This gate
reads exactly that from the byte series the sampler already collected (no
synthetic writes into the live replica, no docker).

    # after a soak run that produced reports/soak-<tag>.ndjson:
    .venv/bin/python tools/wal_reclaim_gate.py \\
        reports/soak-<tag>.ndjson --out reports/wal-reclaim-<tag>.json

Exit 0 = PASS/WATCH/skip (bounded or insufficient signal), 1 = FAIL (pinned),
2 = ERROR (bad input). WATCH is non-fatal by default; --strict makes it FAIL.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from resource_sampler import wal_reclaim_verdict  # noqa: E402


def load_samples(path: str) -> list[dict]:
    """Load sampler output: ndjson (one sample/line) OR a summary.json with an
    embedded 'wal_reclaim_gate' already computed (re-emit it)."""
    samples: list[dict] = []
    with open(path) as f:
        text = f.read()
    stripped = text.lstrip()
    if stripped.startswith("{") and '"samples"' in stripped[:200]:
        # Looks like a summary.json — surface any precomputed verdict.
        obj = json.loads(text)
        if "wal_reclaim_gate" in obj:
            return [{"__precomputed__": obj["wal_reclaim_gate"]}]
        raise ValueError(
            "summary.json has no wal_reclaim_gate; pass the ndjson stream")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            samples.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return samples


def main() -> int:
    ap = argparse.ArgumentParser(description="wal2 WAL-reclaim (zombie-pin) gate.")
    ap.add_argument("samples", help="resource_sampler ndjson stream (its --out)")
    ap.add_argument("--out", help="write the verdict JSON here")
    ap.add_argument("--strict", action="store_true",
                    help="treat WATCH as FAIL")
    ap.add_argument("--wal-floor-mb", type=float, default=64.0,
                    help="WAL must exceed this (MB) before a reclaim is required")
    ap.add_argument("--reclaim-min-mb", type=float, default=4.0,
                    help="a wal_bytes drop >= this (MB) counts as a reclaim")
    a = ap.parse_args()

    try:
        samples = load_samples(a.samples)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        print(f"wal_reclaim_gate: cannot read {a.samples}: {e}", file=sys.stderr)
        return 2

    if len(samples) == 1 and "__precomputed__" in samples[0]:
        verdict = samples[0]["__precomputed__"]
    else:
        verdict = wal_reclaim_verdict(
            samples,
            reclaim_min_bytes=int(a.reclaim_min_mb * 1024 * 1024),
            wal_floor_bytes=int(a.wal_floor_mb * 1024 * 1024),
        )

    if a.out:
        with open(a.out, "w") as f:
            json.dump(verdict, f, indent=2)

    v = verdict.get("verdict", "skip")
    print(f"WAL reclaim gate: {v} — {verdict.get('note', '')}")
    if v == "FAIL" or (v == "WATCH" and a.strict):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
