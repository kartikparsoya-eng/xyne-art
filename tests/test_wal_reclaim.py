"""Regression tests for the wal2 zombie-pin / lagging-snapshot WAL gate.

These encode what the rust-ivm investigation established on real wal2
(wal2-probe-matrix.mjs + the zombie_pin_repro): a connection leaked with its
read transaction still open pins wal2 rotation and grows the WAL at the write
rate WITHOUT ever reclaiming, while `wal_checkpoint(PASSIVE)` keeps reporting
ckpt_done > 0 / ckpt_busy = 0 (it checkpoints the inactive file). So the gate
must key on RECLAIM (did wal_bytes ever come back down?), not on ckpt_busy.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from resource_sampler import wal_reclaim_verdict  # noqa: E402

MB = 1024 * 1024
DB = 500 * MB  # a 500MB replica


def _s(ts, wal_mb, ckpt_done=10, db=DB):
    return {"ts": ts, "wal_bytes": int(wal_mb * MB),
            "ckpt_done": ckpt_done, "db_bytes": db}


def test_zombie_pin_monotonic_growth_fails():
    # WAL climbs 10 -> 900MB, never drops, checkpoints keep "running"
    # (ckpt_done > 0) — the exact prod zombie signature the ckpt_busy gate
    # is blind to on wal2.
    samples = [_s(i * 60, 10 + i * 90, ckpt_done=5) for i in range(10)]
    v = wal_reclaim_verdict(samples)
    assert v["verdict"] == "FAIL", v
    assert v["reclaim_events"] == 0
    assert v["ckpt_attempted"] is True


def test_ckpt_done_nonzero_does_not_rescue_a_pin():
    # Explicitly assert the wal2 blindness fix: a large, never-reclaimed WAL
    # FAILs even though every sample reported a healthy-looking ckpt_done.
    samples = [_s(i * 30, 64 + i * 50, ckpt_done=200) for i in range(8)]
    assert wal_reclaim_verdict(samples)["verdict"] == "FAIL"


def test_healthy_sawtooth_passes():
    # wal2 ping-pong: grow to ~120MB, switch+reset back down, repeat.
    seq = [10, 60, 120, 8, 70, 130, 6, 80, 140, 9]
    samples = [_s(i * 60, mb) for i, mb in enumerate(seq)]
    v = wal_reclaim_verdict(samples)
    assert v["verdict"] == "PASS", v
    assert v["reclaim_events"] >= 2


def test_small_wal_is_skipped_not_failed():
    # WAL never got large enough to require a reclaim — no signal, not a fail.
    samples = [_s(i * 60, 2 + i * 0.5) for i in range(6)]
    assert wal_reclaim_verdict(samples)["verdict"] == "skip"


def test_upward_trend_despite_reclaims_is_watch():
    # Reclaims happen, but each cycle floors higher — a slow/partial pin.
    seq = [64, 200, 120, 320, 240, 460, 380, 620, 540, 780]
    samples = [_s(i * 30, mb) for i, mb in enumerate(seq)]  # 30s => strong /h slope
    v = wal_reclaim_verdict(samples)
    assert v["verdict"] == "WATCH", v
    assert v["reclaim_events"] >= 2


def test_too_few_samples_is_skip():
    assert wal_reclaim_verdict([_s(0, 100), _s(60, 900)])["verdict"] == "skip"


def test_ratio_floor_requires_large_wal_relative_to_db():
    # 80MB WAL is over the absolute floor but tiny vs a 4GB db (ratio 0.02):
    # not the pin signature (db dwarfs it), so skip rather than fail.
    big_db = 4096 * MB
    samples = [_s(i * 60, 80, db=big_db) for i in range(6)]
    assert wal_reclaim_verdict(samples)["verdict"] == "skip"
