#!/usr/bin/env python3
"""
evaluate_gates.py — the ART pass/fail gate.

Scrapes the SAME signals the baseline is built from, over a chosen window, and
compares them to art-baseline.json:
  * server engine histograms  (zero_sync_*)  vs  server_baselines_ms.pass_p95/pass_p99
  * health event rates                        vs  health_gates.min_pass/max_pass

Used two ways (see ART.md):
  Mode A  — after a replay run, evaluate the window you just drove load in.
  Mode B  — point --metric-selector / --log-filter at a canary and compare.

    export GR_KEY='glsa_...'                 # Grafana token, on VPN
    python3 tools/evaluate_gates.py --window 15m
    python3 tools/evaluate_gates.py --window 30m \
        --metric-selector 'pod=~"xyne-spaces-zero-canary.*"'

Exit code 0 = PASS, 1 = FAIL (so CI can gate a merge/deploy on it).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request

BASE = "https://grafana.spaces.xyne.juspay.net"
PROM = BASE + "/api/datasources/proxy/7/api/v1/query"
LOGS = BASE + "/api/datasources/proxy/8/select/logsql/query"


def _get(url: str, params: dict, key: str) -> dict:
    q = urllib.parse.urlencode(params)
    req = urllib.request.Request(url + "?" + q,
                                 headers={"Authorization": "Bearer " + key})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode())


def promql_quantile(key: str, metric_bucket: str, q: float, window: str,
                    selector: str) -> float | None:
    inner = f"{metric_bucket}{{{selector}}}" if selector else metric_bucket
    expr = f"histogram_quantile({q}, sum(increase({inner}[{window}])) by (le))"
    try:
        d = _get(PROM, {"query": expr}, key)
        res = d.get("data", {}).get("result", [])
        if not res:
            return None
        v = float(res[0]["value"][1])
        return None if v != v else v          # drop NaN
    except Exception:
        return None


def log_count(key: str, event: str, window: str, extra: str = "",
              container: str = "xyne-logging-bridge") -> int:
    filt = f'container:"{container}" AND event:"{event}"'
    if extra:
        filt += f" AND {extra}"
    q = filt + " | stats count() as n"
    try:
        d = _get(LOGS, {"query": q, "start": window, "limit": "5"}, key)
        # LogsQL returns ndjson lines; _get parsed JSON only if single object.
        return int(d.get("n", 0))
    except Exception:
        # Fall back to manual ndjson parse.
        raw = _raw(LOGS, {"query": q, "start": window, "limit": "5"}, key)
        for line in raw.splitlines():
            line = line.strip()
            if line:
                try:
                    return int(json.loads(line).get("n", 0))
                except Exception:
                    pass
        return 0


def log_count_raw(key: str, filt: str, window: str) -> int:
    q = filt + " | stats count() as n"
    raw = _raw(LOGS, {"query": q, "start": window, "limit": "5"}, key)
    for line in raw.splitlines():
        line = line.strip()
        if line:
            try:
                return int(json.loads(line).get("n", 0))
            except Exception:
                pass
    return 0


def _raw(url: str, params: dict, key: str) -> str:
    q = urllib.parse.urlencode(params)
    req = urllib.request.Request(url + "?" + q,
                                 headers={"Authorization": "Bearer " + key})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read().decode()


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #
def evaluate_server(baseline: dict, key: str, window: str, selector: str) -> list[dict]:
    checks = []
    for metric, base in baseline["server_baselines_ms"].items():
        if not isinstance(base, dict) or "p95" not in base:
            continue
        bucket = metric + "_seconds_bucket"
        obs = {}
        for qn, qv in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99)):
            s = promql_quantile(key, bucket, qv, window, selector)
            obs[qn] = round(s * 1000, 2) if s is not None else None   # s -> ms
        for qn, passkey in (("p95", "pass_p95"), ("p99", "pass_p99")):
            thr = base.get(passkey)
            o = obs.get(qn)
            if thr is None:
                continue
            if o is None:
                verdict, detail = "NODATA", "no samples in window"
            else:
                verdict = "PASS" if o <= thr else "FAIL"
                detail = f"observed {o}ms vs threshold {thr}ms (baseline {base[qn]}ms)"
            checks.append({
                "category": "server", "metric": metric, "quantile": qn,
                "observed_ms": o, "baseline_ms": base[qn], "threshold_ms": thr,
                "verdict": verdict, "detail": detail,
            })
    return checks


def evaluate_health(baseline: dict, key: str, window: str, log_filter: str) -> list[dict]:
    gates = baseline["health_gates"]
    checks = []

    def ev(name: str) -> int:
        return log_count(key, name, window, extra=log_filter)

    # ratios of two event counts
    simple = {
        "query_completion_rate": ("zero_query_complete", "zero_query_called", "min_pass"),
        "run_completion_rate": ("zero_run_complete", "zero_run_called", "min_pass"),
        "mutation_completion_rate": ("zero_mutation_complete", "zero_mutation_called", "min_pass"),
        "mutation_error_rate": ("zero_mutation_error", "zero_mutation_called", "max_pass"),
    }
    for gate, (num, den, bound) in simple.items():
        g = gates.get(gate, {})
        n, d = ev(num), ev(den)
        val = round(n / d, 4) if d else None
        checks.append(_bounded_check("health", gate, val, g, bound, f"{num}/{den} = {n}/{d}"))

    # api success = ok / (ok+fail)
    g = gates.get("api_success_rate", {})
    ok = ev("api_call_successful")
    fail = ev("api_call_failed")
    val = round(ok / (ok + fail), 4) if (ok + fail) else None
    checks.append(_bounded_check("health", "api_success_rate", val, g, "min_pass",
                                 f"ok/(ok+fail) = {ok}/{ok+fail}"))

    # backend log error rate (different container)
    g = gates.get("backend_log_error_rate", {})
    err = log_count_raw(key, 'container:"xyne-backend" AND level:"error"', window)
    tot = log_count_raw(key, 'container:"xyne-backend"', window)
    val = round(err / tot, 5) if tot else None
    checks.append(_bounded_check("health", "backend_log_error_rate", val, g, "max_pass",
                                 f"error/total = {err}/{tot}"))

    # socket ratio — WATCH only (never fails the gate)
    g = gates.get("socket_failure_ratio_watch", {})
    f_ = ev("websocket_connection_failed")
    s_ = ev("websocket_connection_successful")
    val = round(f_ / s_, 2) if s_ else None
    base = g.get("value")
    detail = f"failed/successful = {f_}/{s_}; baseline {base}"
    verdict = "WATCH"
    if val is not None and base:
        if val > base * 1.30:
            detail += "  (>30% worse than baseline — investigate socket/reconnect path)"
    checks.append({"category": "health", "metric": "socket_failure_ratio_watch",
                   "observed": val, "baseline": base, "verdict": verdict, "detail": detail})
    return checks


def _bounded_check(cat, name, val, gate, bound, detail):
    base = gate.get("value")
    thr = gate.get(bound)
    if val is None:
        verdict = "NODATA"
    elif thr is None:
        verdict = "WATCH"
    elif bound == "min_pass":
        verdict = "PASS" if val >= thr else "FAIL"
    else:  # max_pass
        verdict = "PASS" if val <= thr else "FAIL"
    return {"category": cat, "metric": name, "observed": val, "baseline": base,
            "threshold": thr, "bound": bound, "verdict": verdict, "detail": detail}


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def render_md(meta: dict, checks: list[dict], verdict: str) -> str:
    lines = [f"# ART gate report — **{verdict}**", ""]
    lines.append(f"- baseline: `{meta['baseline']}` (v{meta['art_version']})")
    lines.append(f"- window: `{meta['window']}`  | metric selector: `{meta['selector'] or '(none)'}`"
                 f"  | log filter: `{meta['log_filter'] or '(none)'}`")
    lines.append(f"- generated: {meta['generated']}")
    counts = {}
    for c in checks:
        counts[c["verdict"]] = counts.get(c["verdict"], 0) + 1
    lines.append(f"- results: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    lines.append("")

    lines.append("## Server engine SLOs (hard gate)")
    lines.append("| metric | q | observed | baseline | threshold | verdict |")
    lines.append("|---|---|---|---|---|---|")
    for c in checks:
        if c["category"] != "server":
            continue
        lines.append(f"| `{c['metric']}` | {c['quantile']} | {c['observed_ms']} | "
                     f"{c['baseline_ms']} | {c['threshold_ms']} | {_badge(c['verdict'])} |")
    lines.append("")
    lines.append("## Health gates")
    lines.append("| gate | observed | baseline | threshold | verdict |")
    lines.append("|---|---|---|---|---|")
    for c in checks:
        if c["category"] != "health":
            continue
        lines.append(f"| `{c['metric']}` | {c.get('observed')} | {c.get('baseline')} | "
                     f"{c.get('threshold', '—')} | {_badge(c['verdict'])} |")
    lines.append("")
    fails = [c for c in checks if c["verdict"] == "FAIL"]
    if fails:
        lines.append("## Failures")
        for c in fails:
            m = c.get("metric")
            lines.append(f"- **{m}** {c.get('quantile','')}: {c['detail']}")
    return "\n".join(lines) + "\n"


def _badge(v: str) -> str:
    return {"PASS": "PASS ✅", "FAIL": "FAIL ❌", "WATCH": "WATCH 👀",
            "NODATA": "NODATA ⚪"}.get(v, v)


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate ART gates against live metrics/logs.")
    ap.add_argument("--baseline", default=os.path.join(os.path.dirname(__file__), "..", "art-baseline.json"))
    ap.add_argument("--window", default="15m", help="evaluation window ending now (e.g. 15m, 1h)")
    ap.add_argument("--metric-selector", default="", help="extra PromQL label selector, e.g. pod=~\"canary.*\"")
    ap.add_argument("--log-filter", default="", help="extra LogsQL AND clause for health events")
    ap.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "..", "reports"))
    a = ap.parse_args()

    key = os.environ.get("GR_KEY")
    if not key:
        print("ERROR: export GR_KEY first (Grafana token, on VPN)", file=sys.stderr)
        return 2

    with open(a.baseline) as f:
        baseline = json.load(f)

    print(f"evaluating window={a.window} selector={a.metric_selector or '(none)'} ...")
    checks = []
    checks += evaluate_server(baseline, key, a.window, a.metric_selector)
    checks += evaluate_health(baseline, key, a.window, a.log_filter)

    has_fail = any(c["verdict"] == "FAIL" for c in checks)
    verdict = "FAIL" if has_fail else "PASS"

    meta = {
        "baseline": os.path.relpath(a.baseline), "art_version": baseline.get("art_version"),
        "window": a.window, "selector": a.metric_selector, "log_filter": a.log_filter,
        "generated": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }
    os.makedirs(os.path.abspath(a.out_dir), exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    md_path = os.path.join(os.path.abspath(a.out_dir), f"gate-{stamp}.md")
    json_path = os.path.join(os.path.abspath(a.out_dir), f"gate-{stamp}.json")
    with open(md_path, "w") as f:
        f.write(render_md(meta, checks, verdict))
    with open(json_path, "w") as f:
        json.dump({"meta": meta, "verdict": verdict, "checks": checks}, f, indent=2)

    # Console summary
    for c in checks:
        if c["verdict"] in ("FAIL", "NODATA"):
            tag = c.get("quantile", "")
            print(f"  {c['verdict']:6} {c['metric']} {tag}: {c['detail']}")
    npass = sum(1 for c in checks if c["verdict"] == "PASS")
    print(f"\n=== {verdict} ===  ({npass}/{len(checks)} checks passed)")
    print(f"report: {md_path}")
    return 1 if has_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
