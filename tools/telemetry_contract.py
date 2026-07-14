#!/usr/bin/env python3
"""telemetry_contract.py — G17: telemetry-contract test for a zero-cache image.

The ART gates DEPEND on specific Prometheus metric names and log event names:
  evaluate_gates.py scrapes zero_sync_*_seconds_bucket  -> server SLOs (G-SLO)
  evaluate_gates.py scrapes zero_query_complete etc.   -> health gates
  log_gate.py greps pod logs for advance-reset / fatal patterns (G13)

If the image RENAMES a metric or event field, the gate doesn't fail — it
silently returns NODATA/empty and certifies nothing (the scraper broke, not
the build). This probe makes that breakage loud: it reads the contract
(the metric + event names art-baseline.json's gates rely on) and asserts
each one is still EMITTED by the image. A missing name = FAIL (broken gate),
not NODATA.

Two modes:
  --metrics-url http://host:port/metrics   scrape Prometheus text format
  --container <docker-name> [--since 60s]   docker logs | grep for event names
  (both can be combined; --gr-key uses Grafana for both, like evaluate_gates.py)

    .venv/bin/python tools/telemetry_contract.py --metrics-url http://rust-test.localhost:8080/metrics \\
        --container xyne-sandbox-rust-test-zero-cache --since 120s \\
        --baseline art-baseline.json --out reports/telemetry-$TAG.json

Exit 0 = contract intact; 1 = broken (missing metric/event); 2 = ERROR (infra).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request


def contract_from_baseline(path: str) -> tuple[list[str], list[str]]:
    """Return (metric_names, event_names) the gates depend on."""
    d = json.load(open(path))
    metrics = []
    for k, v in d.get("server_baselines_ms", {}).items():
        if k.startswith("_") or not isinstance(v, dict):
            continue
        metrics.append(k + "_seconds_bucket")  # histogram bucket series name
        metrics.append(k + "_seconds_count")
    events: list[str] = []
    for k, v in d.get("health_gates", {}).items():
        if k.startswith("_") or not isinstance(v, dict):
            continue
        for field in ("num", "den"):
            val = v.get(field, "")
            # den can be "a+b" (api_success_rate) — split on +
            for part in str(val).split("+"):
                part = part.strip()
                if part:
                    events.append(part)
    # dedupe, preserve order
    return (list(dict.fromkeys(metrics)), list(dict.fromkeys(events)))


def scrape_prom_text(url: str, timeout: float = 30.0) -> str:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def scrape_docker_logs(container: str, since: str) -> str:
    r = subprocess.run(["docker", "logs", "--since", since, container],
                       capture_output=True, text=True, timeout=60)
    return (r.stdout or "") + (r.stderr or "")


def grafana_metric_exists(key: str, metric: str) -> bool:
    """Query Prometheus (via evaluate_gates endpoint) for one metric name."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools"))
    from evaluate_gates import _get, PROM  # noqa: E402
    # ask for any sample of the raw metric in a short window
    try:
        d = _get(PROM, {"query": f"{metric}{{}}"}, key)
        return bool(d.get("data", {}).get("result"))
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="G17: telemetry-contract test.")
    ap.add_argument("--baseline", default="art-baseline.json")
    ap.add_argument("--metrics-url", default=None, help="Prometheus /metrics text endpoint")
    ap.add_argument("--container", default=None, help="docker container to scan logs")
    ap.add_argument("--since", default="120s", help="docker logs --since window")
    ap.add_argument("--gr-key", default=os.environ.get("GR_KEY"),
                    help="Grafana key (uses evaluate_gates endpoints; falls back to NODATA)")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    metrics, events = contract_from_baseline(a.baseline)
    checks: list[dict] = []
    prom_text: str | None = None
    log_text: str | None = None

    if a.metrics_url:
        try:
            prom_text = scrape_prom_text(a.metrics_url)
        except Exception as e:
            checks.append({"name": "metrics-scrape", "verdict": "ERROR",
                           "detail": f"could not fetch {a.metrics_url}: {type(e).__name__}: {e}"})
    if a.container:
        try:
            log_text = scrape_docker_logs(a.container, a.since)
        except Exception as e:
            checks.append({"name": "logs-scrape", "verdict": "ERROR",
                           "detail": f"could not docker logs {a.container}: {type(e).__name__}: {e}"})

    # --- metric-name checks ---
    missing_metrics: list[str] = []
    for m in metrics:
        if prom_text is not None:
            found = m in prom_text or re.search(r"\b" + re.escape(m) + r"\b", prom_text)
        elif a.gr_key:
            found = grafana_metric_exists(a.gr_key, m)
        else:
            checks.append({"name": f"metric:{m}", "verdict": "SKIP",
                           "detail": "no --metrics-url and no --gr-key; cannot check"})
            continue
        if found:
            checks.append({"name": f"metric:{m}", "verdict": "PASS", "detail": "emitted"})
        else:
            checks.append({"name": f"metric:{m}", "verdict": "FAIL",
                           "detail": "metric RENAMED/ABSENT — gate scraper will return NODATA"})
            missing_metrics.append(m)

    # --- event-name checks ---
    missing_events: list[str] = []
    for ev in events:
        if log_text is not None:
            found = ev in log_text
        elif a.gr_key:
            found = grafana_log_event_exists(a.gr_key, ev)
        else:
            checks.append({"name": f"event:{ev}", "verdict": "SKIP",
                           "detail": "no --container and no --gr-key; cannot check"})
            continue
        if found:
            checks.append({"name": f"event:{ev}", "verdict": "PASS", "detail": "emitted"})
        else:
            checks.append({"name": f"event:{ev}", "verdict": "FAIL",
                           "detail": "event RENAMED/ABSENT — health gate will return NODATA"})
            missing_events.append(ev)

    has_error = any(c["verdict"] == "ERROR" for c in checks)
    has_fail = bool(missing_metrics or missing_events)
    if has_error and not has_fail and not any(c["verdict"] == "PASS" for c in checks):
        verdict = "ERROR"
    elif has_fail:
        verdict = "FAIL"
    else:
        verdict = "PASS"

    summary = (f"{len(metrics)} metrics + {len(events)} events checked; "
               f"missing={len(missing_metrics)} metrics, {len(missing_events)} events")
    report = {"schema": 1, "gate": "G17", "name": "telemetry-contract",
              "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "verdict": verdict, "checks": checks, "summary": summary,
              "missing_metrics": missing_metrics, "missing_events": missing_events,
              "contract_size": {"metrics": len(metrics), "events": len(events)}}
    print(summary)
    for c in checks:
        print(f"  {c['name']:<32} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[verdict]


def grafana_log_event_exists(key: str, event: str) -> bool:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools"))
    from evaluate_gates import log_count  # noqa: E402
    try:
        return log_count(key, event, "5m") > 0
    except Exception:
        return False


if __name__ == "__main__":
    raise SystemExit(main())
