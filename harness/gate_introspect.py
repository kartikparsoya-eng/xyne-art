#!/usr/bin/env python3
"""
gate_introspect.py — shared server-introspection helpers for the differential
correctness gates (B lifecycle-window, C metric-series, D log-sequence).

These read SERVER-SIDE effects that are invisible in the client frame stream:
Prometheus `/metrics` series+values and container logs. Every helper degrades
to an empty/None result rather than raising, so a gate SKIPs (never false-FAILs)
when the sandbox pair or an endpoint is unavailable.

Env overrides:
  AB_RUST_CONTAINER / AB_TS_CONTAINER   docker container names (from ab_common)
  AB_METRICS_PORTS                      comma list of in-container metrics ports
                                        to probe (default "8081,4848,9090,4849")
"""
from __future__ import annotations

import os
import re
import subprocess
import time
from typing import Optional

METRICS_PORTS = [p for p in os.environ.get(
    "AB_METRICS_PORTS", "8081,4848,9090,4849").split(",") if p.strip()]


def docker_logs_since(container: str, since_s: float) -> str:
    """Return stdout+stderr logs for the last `since_s` seconds, or "" on error."""
    if not container:
        return ""
    try:
        r = subprocess.run(
            ["docker", "logs", "--since", f"{int(since_s) + 1}s", container],
            capture_output=True, text=True, timeout=20,
        )
        return (r.stdout or "") + "\n" + (r.stderr or "")
    except Exception:
        return ""


def scrape_metrics_text(container: str) -> str:
    """docker exec curl the in-container metrics endpoint. Tries several ports;
    returns the Prometheus exposition text, or "" if none answered."""
    if not container:
        return ""
    for port in METRICS_PORTS:
        for path in ("/metrics", "/"):
            try:
                r = subprocess.run(
                    ["docker", "exec", container, "sh", "-c",
                     f"curl -s --max-time 5 http://localhost:{port}{path} || "
                     f"wget -qO- http://localhost:{port}{path}"],
                    capture_output=True, text=True, timeout=15,
                )
                txt = r.stdout or ""
                if "# HELP" in txt or "# TYPE" in txt or "zero_sync" in txt:
                    return txt
            except Exception:
                continue
    return ""


def parse_prom(text: str) -> dict[str, float]:
    """Parse Prometheus exposition text into {series_key: value}. The series key
    is `name{sorted,labels}` so a missing label combination (e.g. a whole
    `flush.type="async"` series) is a distinct, diffable key."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([^\s]+)', line)
        if not m:
            continue
        name, labels, val = m.group(1), m.group(2) or "", m.group(3)
        try:
            v = float(val)
        except ValueError:
            continue
        # canonicalize label order so rust/ts keys align
        if labels:
            inner = labels[1:-1]
            parts = [p for p in inner.split(",") if p]
            labels = "{" + ",".join(sorted(parts)) + "}"
        out[name + labels] = v
    return out


def scrape_metrics(container: str) -> dict[str, float]:
    return parse_prom(scrape_metrics_text(container))


def _http_get(url: str) -> str:
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def _filter_host(text: str, host: str) -> str:
    """Keep only exposition lines whose series carries host_name="<host>"."""
    if not host:
        return text
    needle = f'host_name="{host}"'
    return "\n".join(ln for ln in text.splitlines()
                     if ln.startswith("#") or needle in ln)


def scrape_metrics_for(side: str) -> dict[str, float]:
    """Metrics for one side ("rust"|"ts"). Prefers a shared Prometheus endpoint
    (the otel-collector at AB_METRICS_URL) filtered by that side's `host_name`
    (AB_{RUST,TS}_METRIC_HOST) — since zero-cache exports via OTLP push, not a
    per-container scrape endpoint. Falls back to `docker exec … curl` on the
    side's container (AB_{RUST,TS}_CONTAINER)."""
    url = os.environ.get("AB_METRICS_URL")
    if url:
        host = os.environ.get(f"AB_{side.upper()}_METRIC_HOST", "")
        return parse_prom(_filter_host(_http_get(url), host))
    container = os.environ.get(f"AB_{side.upper()}_CONTAINER", "")
    return scrape_metrics(container)


def series_names(metrics: dict[str, float]) -> set[str]:
    """The set of (name{labels}) series keys present."""
    return set(metrics.keys())


def sum_by_name(metrics: dict[str, float], name_prefix: str) -> float:
    """Sum every series whose metric name starts with `name_prefix` (across all
    label combinations) — used for counter deltas that shouldn't depend on the
    exact label split."""
    total = 0.0
    for k, v in metrics.items():
        base = k.split("{", 1)[0]
        if base.startswith(name_prefix):
            total += v
    return total


def label_values(metrics: dict[str, float], name: str, label: str) -> set[str]:
    """All distinct values of `label` observed on series named exactly `name`."""
    vals: set[str] = set()
    pat = re.compile(rf'{re.escape(label)}="([^"]*)"')
    for k in metrics:
        base = k.split("{", 1)[0]
        if base != name:
            continue
        m = pat.search(k)
        if m:
            vals.add(m.group(1))
    return vals


def now() -> float:
    return time.time()
