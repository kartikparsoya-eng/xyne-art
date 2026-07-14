"""Unit tests for the pure (server-free) logic in the new image/lifecycle gates.

The G16-G24 tools are mostly async/live-server probes, but two carry clean,
deterministic, side-effect-free logic worth pinning:
  - telemetry_contract.contract_from_baseline  (metric + event name extraction)
  - capacity_gate._run_point + find_cliff      (curve extraction + cliff finder)
These are exactly the pieces where a silent parse bug would mask a regression.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

TOOLS = str(Path(__file__).resolve().parent.parent / "tools")
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

REPO = Path(__file__).resolve().parent.parent
BASELINE_PATH = REPO / "art-baseline.json"


# --------------------------------------------------------------------------- #
# telemetry_contract.contract_from_baseline
# --------------------------------------------------------------------------- #
from telemetry_contract import contract_from_baseline  # noqa: E402


def test_contract_extracts_metric_buckets_and_counts(tmp_path):
    doc = {"server_baselines_ms": {
        "_comment": "x",
        "zero_sync_hydration_time": {"p95": 100, "pass_p95": 200},
        "zero_sync_advance_time": {"p95": 50, "pass_p95": 100},
    }}
    p = tmp_path / "b.json"
    p.write_text(json.dumps(doc))
    metrics, events = contract_from_baseline(str(p))
    assert "zero_sync_hydration_time_seconds_bucket" in metrics
    assert "zero_sync_hydration_time_seconds_count" in metrics
    assert "zero_sync_advance_time_seconds_bucket" in metrics
    # _comment is not a metric
    assert not any(m.startswith("_") for m in metrics)


def test_contract_extracts_event_names(tmp_path):
    doc = {"server_baselines_ms": {}, "health_gates": {
        "_comment": "x",
        "query_completion_rate": {"num": "zero_query_complete",
                                  "den": "zero_query_called", "min_pass": 0.86},
        "api_success_rate": {"num": "api_call_successful",
                             "den": "api_call_successful+api_call_failed",
                             "min_pass": 0.95},
    }}
    p = tmp_path / "b.json"
    p.write_text(json.dumps(doc))
    metrics, events = contract_from_baseline(str(p))
    assert "zero_query_complete" in events
    assert "zero_query_called" in events
    # den with "a+b" splits into both
    assert "api_call_successful" in events
    assert "api_call_failed" in events
    assert not any(e.startswith("_") for e in events)


def test_contract_dedupes_events(tmp_path):
    doc = {"server_baselines_ms": {}, "health_gates": {
        "a": {"num": "evt", "den": "evt"}, "b": {"num": "evt", "den": "evt"}}}
    p = tmp_path / "b.json"
    p.write_text(json.dumps(doc))
    _, events = contract_from_baseline(str(p))
    assert events.count("evt") == 1


@pytest.mark.skipif(not BASELINE_PATH.exists(), reason="art-baseline.json not present")
def test_contract_against_real_baseline():
    metrics, events = contract_from_baseline(str(BASELINE_PATH))
    # the six server SLO metrics the gates scrape
    for m in ("zero_sync_hydration_time", "zero_sync_advance_time",
              "zero_sync_poke_time", "zero_sync_query_transformation_time"):
        assert m + "_seconds_bucket" in metrics
    assert "zero_query_complete" in events
    assert "zero_query_called" in events


# --------------------------------------------------------------------------- #
# capacity_gate._run_point + find_cliff
# --------------------------------------------------------------------------- #
from capacity_gate import _run_point, find_cliff  # noqa: E402


def _write_run(tmp_path, conns, p95, errors=0, failed_open=0, name=None):
    d = {"config": {"connections": conns},
         "counters": {"errors": errors, "failed_open": failed_open},
         "client_latency_ms": {"p50": 50, "p95": p95}}
    p = tmp_path / (name or f"run-{conns}.json")
    p.write_text(json.dumps(d))
    return str(p)


def test_run_point_extracts_connections_and_p95(tmp_path):
    rp = _run_point(_write_run(tmp_path, 50, 750))
    assert rp["connections"] == 50
    assert rp["p95"] == 750
    assert rp["errors"] == 0
    assert rp["failed_open"] == 0


def test_run_point_returns_none_on_bad_json(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("not json")
    assert _run_point(str(p)) is None


def test_run_point_returns_none_without_connections(tmp_path):
    p = tmp_path / "noconns.json"
    p.write_text(json.dumps({"config": {}, "counters": {}}))
    assert _run_point(str(p)) is None


def test_run_point_reads_steady_latency_fallback(tmp_path):
    # when client_latency_ms is absent, fall back to steady
    d = {"config": {"connections": 25},
         "counters": {},
         "client_latency_steady_ms": {"p95": 120}}
    p = tmp_path / "steady.json"
    p.write_text(json.dumps(d))
    assert _run_point(str(p))["p95"] == 120


def test_find_cliff_picks_highest_healthy_rung(tmp_path):
    pts = [_run_point(_write_run(tmp_path, c, p, e, fo))
           for c, p, e, fo in [(10, 100, 0, 0), (25, 200, 0, 0),
                              (50, 400, 0, 0), (100, 6000, 0, 0),
                              (200, 9999, 5, 0)]]
    cliff = find_cliff(pts, p95_threshold=5000)
    assert cliff["cliff_conns"] == 50  # 100 and 200 are unhealthy


def test_find_cliff_zero_when_all_unhealthy(tmp_path):
    pts = [_run_point(_write_run(tmp_path, c, 9999, 1, 0)) for c in (10, 25)]
    assert find_cliff(pts, p95_threshold=5000)["cliff_conns"] == 0


def test_find_cliff_treats_none_p95_as_unknown_not_unhealthy(tmp_path):
    # a run with no latency samples (p95=None) is not auto-failed
    d = {"config": {"connections": 100}, "counters": {"errors": 0, "failed_open": 0},
         "client_latency_ms": {}}
    p = tmp_path / "nolat.json"
    p.write_text(json.dumps(d))
    pts = [_run_point(_write_run(tmp_path, 10, 100)), _run_point(str(p))]
    assert find_cliff(pts, p95_threshold=5000)["cliff_conns"] == 100
