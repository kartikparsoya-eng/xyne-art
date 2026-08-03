from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def run_tool(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args], cwd=REPO, capture_output=True, text=True
    )


def minimal_run(path: Path) -> None:
    path.write_text(json.dumps({
        "config": {"connections": 1},
        "counters": {
            "failed_open": 0, "opened": 1, "errors": 0,
            "invariant_violations": 0, "mutations_sent": 0,
        },
        "client_latency_ms": {},
        "coverage": {
            "queries_driven": 1,
            "queries_hydrated": 1,
            "query_names_hydrated": ["one-shape"],
            "never_hydrated": [],
            "never_hydrated_no_error": [],
        },
    }))


def test_rss_leak_cannot_use_missing_go_metrics_as_a_waiver(tmp_path: Path):
    run = tmp_path / "run.json"
    resources = tmp_path / "resources.json"
    provenance = tmp_path / "provenance.json"
    out = tmp_path / "gate.json"
    minimal_run(run)
    resources.write_text(json.dumps({
        "window_s": 900,
        "rss_bytes": {"slope_per_hour": 2_000_000_000, "max": 3_000_000_000},
    }))
    provenance.write_text(json.dumps({"image_digest": "sha256:one"}))

    result = run_tool(
        "tools/local_gate.py", "--run", str(run), "--resources", str(resources),
        "--provenance", str(provenance), "--out", str(out),
    )
    assert result.returncode == 1
    gates = {g["gate"]: g for g in json.loads(out.read_text())["results"]}
    assert gates["G6 leaks"]["verdict"] == "FAIL"


def test_consolidation_rejects_mixed_image_evidence(tmp_path: Path):
    reports = []
    for index, digest in enumerate(("sha256:one", "sha256:two")):
        path = tmp_path / f"gate-{index}.json"
        path.write_text(json.dumps({
            "overall": "PASS",
            "inputs": {},
            "provenance": {"image_digest": digest, "addon_sha256": digest},
            "results": [{"gate": "G1 connectivity", "verdict": "PASS", "detail": "ok"}],
        }))
        reports.append(path)
    out = tmp_path / "consolidated.json"
    result = run_tool(
        "tools/consolidate_gates.py", *(str(path) for path in reports),
        "--json", str(out),
    )
    assert result.returncode == 1
    doc = json.loads(out.read_text())
    assert doc["overall"] == "FAIL"
    assert next(g for g in doc["gates"] if g["gate"] == "G0 provenance")["final"] == "FAIL"


def test_capacity_gate_rejects_unarmed_zero_baseline(tmp_path: Path):
    run = tmp_path / "run.json"
    minimal_run(run)
    result = run_tool(
        "tools/capacity_gate.py", "--runs", str(run), "--blessed-conns", "0"
    )
    assert result.returncode == 2
    assert "greater than zero" in result.stderr


def test_telemetry_without_observation_sources_is_error(tmp_path: Path):
    baseline = tmp_path / "baseline.json"
    out = tmp_path / "telemetry.json"
    baseline.write_text(json.dumps({
        "server_baselines_ms": {"metric": {}},
        "health_gates": {"event": {"num": "event_a", "den": "event_b"}},
    }))
    result = run_tool(
        "tools/telemetry_contract.py", "--baseline", str(baseline), "--out", str(out)
    )
    assert result.returncode == 2
    assert json.loads(out.read_text())["verdict"] == "ERROR"
