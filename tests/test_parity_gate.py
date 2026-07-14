"""Unit tests for tools/parity_gate.py — the pure server-free logic.

compute_ratios, find_undersampled, and compute_cascade_multiplier are the
pieces where a silent bug would mask a parity regression (exactly the
userAllChannels class of failure this gate exists to catch).
"""
from __future__ import annotations

import sys
from pathlib import Path

TOOLS = str(Path(__file__).resolve().parent.parent / "tools")
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

from parity_gate import compute_ratios, find_undersampled, compute_cascade_multiplier  # noqa: E402


def _pq(name, samples, p50, p95=None, p99=None):
    return {name: {"samples": samples, "p50": p50,
                   "p95": p95 if p95 is not None else p50 * 2,
                   "p99": p99 if p99 is not None else p50 * 3}}


# --------------------------------------------------------------------------- #
# compute_ratios
# --------------------------------------------------------------------------- #
def test_compute_ratios_passes_when_within_factor():
    go = _pq("q1", 50, 200, 400)
    ts = _pq("q1", 50, 150, 300)
    r = compute_ratios(go, ts, factor=2.0, min_delta_ms=100)
    assert r["verdict"] == "PASS"
    assert r["compared"] == 1
    assert r["offenders"] == []


def test_compute_ratios_fails_on_large_ratio():
    # userAllChannels class: Go 1237ms vs TS 250ms = ~5x
    go = _pq("userAllChannels", 100, 600, 1237)
    ts = _pq("userAllChannels", 100, 120, 250)
    r = compute_ratios(go, ts, factor=2.0, min_delta_ms=100)
    assert r["verdict"] == "FAIL"
    assert len(r["offenders"]) == 1
    off = r["offenders"][0]
    assert off["query"] == "userAllChannels"
    assert off["ratio"] >= 4.0
    assert off["direction"] == "primary-slower"


def test_compute_ratios_bidirectional_catches_ts_slower():
    # parity is bidirectional — TS slower than Go is also a finding
    go = _pq("q1", 50, 100, 200)
    ts = _pq("q1", 50, 500, 1000)
    r = compute_ratios(go, ts, factor=2.0, min_delta_ms=100)
    assert r["verdict"] == "FAIL"
    assert r["offenders"][0]["direction"] == "mirror-slower"


def test_compute_ratios_noise_floor_skips_small_delta():
    # 2x ratio but only 20ms delta — under the 100ms noise floor
    go = _pq("q1", 50, 40, 60)
    ts = _pq("q1", 50, 20, 30)
    r = compute_ratios(go, ts, factor=2.0, min_delta_ms=100)
    assert r["verdict"] == "PASS"  # delta too small to be signal


def test_compute_ratios_noise_floor_skips_small_baseline():
    # mirror is 5ms — sub-10ms baselines make ratios meaningless
    go = _pq("q1", 50, 25, 50)
    ts = _pq("q1", 50, 5, 10)
    r = compute_ratios(go, ts, factor=2.0, min_baseline_ms=10)
    assert r["verdict"] == "PASS"


def test_compute_ratios_requires_min_samples_both_sides():
    go = _pq("q1", 5, 600, 1237)   # only 5 samples (below 10)
    ts = _pq("q1", 100, 120, 250)
    r = compute_ratios(go, ts, min_samples=10)
    assert r["verdict"] == "PASS"
    assert r["compared"] == 0  # not compared at all


def test_compute_ratios_offenders_sorted_by_ratio():
    go = {**_pq("slow", 50, 500, 1000), **_pq("slower", 50, 1200, 2400)}
    ts = {**_pq("slow", 50, 100, 200), **_pq("slower", 50, 100, 200)}
    r = compute_ratios(go, ts, factor=2.0, min_delta_ms=100)
    assert r["offenders"][0]["query"] == "slower"
    assert r["offenders"][0]["ratio"] > r["offenders"][1]["ratio"]


def test_compute_ratios_uses_p95_by_default():
    go = _pq("q1", 50, 100, 500)
    ts = _pq("q1", 50, 100, 200)
    r = compute_ratios(go, ts, factor=2.0, min_delta_ms=100)
    # p50 is identical (100), but p95 diverges (500 vs 200 = 2.5x)
    assert r["verdict"] == "FAIL"
    assert r["ratios"][0]["ratio"] == 2.5


# --------------------------------------------------------------------------- #
# find_undersampled
# --------------------------------------------------------------------------- #
def test_find_undersampled_finds_low_sample_queries():
    pq = {"heavy": {"samples": 500}, "light": {"samples": 11}, "rare": {"samples": 3}}
    under = find_undersampled(pq, min_samples=100)
    names = [u["query"] for u in under]
    assert "light" in names and "rare" in names
    assert "heavy" not in names


def test_find_undersampled_ignores_zero_samples():
    pq = {"never": {"samples": 0}, "few": {"samples": 5}}
    under = find_undersampled(pq, min_samples=100)
    assert all(u["query"] != "never" for u in under)


# --------------------------------------------------------------------------- #
# compute_cascade_multiplier
# --------------------------------------------------------------------------- #
def test_cascade_passes_when_all_under_timeout():
    times = [100, 120, 90, 110]
    r = compute_cascade_multiplier(times, timeout_ms=500)
    assert r["verdict"] == "PASS"
    assert r["overflows"] == 0
    assert r["multiplier"] == 1.0


def test_cascade_fails_on_amplification():
    # userAllChannels class: most hydrations exceed 500ms timeout
    times = [1237, 1100, 1300, 950, 1200, 1050]
    r = compute_cascade_multiplier(times, timeout_ms=500)
    assert r["verdict"] == "FAIL"
    assert r["overflows"] == 6
    assert r["multiplier"] > 3.0


def test_cascade_watch_on_single_overflow():
    times = [100, 120, 600]  # one overflow, but only 1
    r = compute_cascade_multiplier(times, timeout_ms=500)
    assert r["verdict"] == "WATCH"
    assert r["overflows"] == 1


def test_cascade_multiplier_uses_min_as_single():
    times = [200, 1237, 1100]
    r = compute_cascade_multiplier(times, timeout_ms=500)
    assert r["single_cost_ms"] == 200
    assert r["cascade_cost_ms"] == 2337  # 1237 + 1100
    assert r["multiplier"] == round(2337 / 200, 1)


def test_cascade_empty_samples():
    r = compute_cascade_multiplier([], timeout_ms=500)
    assert r["verdict"] == "SKIP"
