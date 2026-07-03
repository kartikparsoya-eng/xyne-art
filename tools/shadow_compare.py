#!/usr/bin/env python3
"""
shadow_compare.py — ART Mode B: canary shadow-compare (post-deploy).

Compares a CANARY zero-cache's engine histograms against a CONTROL population
over the SAME time window, so time-of-day / workload drift cancels out (unlike
evaluate_gates.py, which compares against the static 7d baseline).

Two gates per metric/quantile, both must hold:
  RELATIVE  canary must not be worse than control by > margin (and > abs floor)
  ABSOLUTE  canary must also satisfy art-baseline.json pass_p95/pass_p99

Health-event gates (query completion, mutation errors, ...) are CLIENT-side
events and cannot be attributed to a single pod — run evaluate_gates.py for
those; this tool covers the server engine only.

    export GR_KEY='glsa_...'                # Grafana token, on VPN
    python3 tools/shadow_compare.py \
        --canary-selector  'pod=~"xyne-spaces-zero-canary.*"' \
        --control-selector 'pod!~"xyne-spaces-zero-canary.*"' \
        --window 30m

Exit code 0 = PASS, 1 = FAIL (CI can gate promotion on it).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from evaluate_gates import PROM, _get, promql_quantile  # noqa: E402

# Regression margins for the RELATIVE gate (canary vs control, same window).
MARGINS = {"p95": 0.25, "p99": 0.30}
ABS_FLOOR_MS = 5.0        # ignore relative deltas smaller than this (noise)
QUANTILES = (("p50", 0.50), ("p95", 0.95), ("p99", 0.99))


def sample_count(key: str, bucket: str, window: str, selector: str) -> int | None:
    """Total observations in the window for one side (le="+Inf" bucket)."""
    sel = f'le="+Inf"' + (f",{selector}" if selector else "")
    expr = f'sum(increase({bucket}{{{sel}}}[{window}]))'
    try:
        d = _get(PROM, {"query": expr}, key)
        res = d.get("data", {}).get("result", [])
        return int(float(res[0]["value"][1])) if res else None
    except Exception:
        return None


def compare_metric(key: str, metric: str, base: dict, window: str,
                   canary_sel: str, control_sel: str, min_samples: int) -> list[dict]:
    bucket = metric + "_seconds_bucket"
    canary_n = sample_count(key, bucket, window, canary_sel)
    control_n = sample_count(key, bucket, window, control_sel)

    checks = []
    for qn, qv in QUANTILES:
        c = promql_quantile(key, bucket, qv, window, canary_sel)
        b = promql_quantile(key, bucket, qv, window, control_sel)
        c_ms = round(c * 1000, 2) if c is not None else None
        b_ms = round(b * 1000, 2) if b is not None else None
        row = {
            "metric": metric, "quantile": qn,
            "canary_ms": c_ms, "control_ms": b_ms,
            "canary_samples": canary_n, "control_samples": control_n,
            "delta_pct": None, "verdict": "INFO", "detail": "",
        }
        if c_ms is not None and b_ms is not None and b_ms > 0:
            row["delta_pct"] = round(100.0 * (c_ms - b_ms) / b_ms, 1)

        margin = MARGINS.get(qn)
        if margin is None:                       # p50 is informational
            checks.append(row)
            continue

        thin = (canary_n or 0) < min_samples
        pass_thr = base.get("pass_" + qn)        # absolute gate from baseline

        if c_ms is None or b_ms is None:
            row["verdict"], row["detail"] = "NODATA", "missing samples on one side"
        elif thin:
            row["verdict"] = "WATCH"
            row["detail"] = f"only {canary_n} canary samples (<{min_samples}) — inconclusive"
        else:
            rel_bad = (c_ms > b_ms * (1 + margin)) and (c_ms - b_ms > ABS_FLOOR_MS)
            abs_bad = pass_thr is not None and c_ms > pass_thr
            if rel_bad and abs_bad:
                row["verdict"] = "FAIL"
                row["detail"] = (f"canary {c_ms}ms vs control {b_ms}ms "
                                 f"(+{row['delta_pct']}%, margin {int(margin*100)}%) "
                                 f"and above baseline pass_{qn}={pass_thr}ms")
            elif rel_bad or abs_bad:
                row["verdict"] = "WATCH"
                which = "relative" if rel_bad else f"absolute (pass_{qn}={pass_thr}ms)"
                row["detail"] = (f"canary {c_ms}ms vs control {b_ms}ms "
                                 f"(+{row['delta_pct']}%) — {which} margin exceeded only")
            else:
                row["verdict"] = "PASS"
                row["detail"] = f"canary {c_ms}ms vs control {b_ms}ms"
        checks.append(row)
    return checks


def render_md(meta: dict, checks: list[dict], verdict: str) -> str:
    badge = {"PASS": "PASS ✅", "FAIL": "FAIL ❌", "WATCH": "WATCH 👀",
             "NODATA": "NODATA ⚪", "INFO": "—"}
    lines = [f"# ART shadow-compare (Mode B) — **{verdict}**", ""]
    lines.append(f"- window: `{meta['window']}` (same window both sides)")
    lines.append(f"- canary:  `{meta['canary_selector']}`")
    lines.append(f"- control: `{meta['control_selector'] or '(whole fleet)'}`")
    lines.append(f"- gates: relative +{int(MARGINS['p95']*100)}% p95 / "
                 f"+{int(MARGINS['p99']*100)}% p99 (floor {ABS_FLOOR_MS}ms, "
                 f"min {meta['min_samples']} samples) AND absolute pass_p95/pass_p99")
    lines.append(f"- generated: {meta['generated']}")
    lines.append("")
    lines.append("| metric | q | canary | control | Δ% | samples (c/ctl) | verdict |")
    lines.append("|---|---|---|---|---|---|---|")
    for c in checks:
        lines.append(
            f"| `{c['metric']}` | {c['quantile']} | {c['canary_ms']} | {c['control_ms']} "
            f"| {c['delta_pct'] if c['delta_pct'] is not None else '—'} "
            f"| {c['canary_samples']}/{c['control_samples']} "
            f"| {badge.get(c['verdict'], c['verdict'])} |")
    fails = [c for c in checks if c["verdict"] == "FAIL"]
    watches = [c for c in checks if c["verdict"] == "WATCH"]
    if fails:
        lines.append("\n## Failures")
        lines += [f"- **{c['metric']}** {c['quantile']}: {c['detail']}" for c in fails]
    if watches:
        lines.append("\n## Watch")
        lines += [f"- {c['metric']} {c['quantile']}: {c['detail']}" for c in watches]
    lines.append("\n> Health/event gates are client-side and pod-agnostic — run "
                 "`evaluate_gates.py` alongside this for full coverage.")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="ART Mode B: canary vs control shadow-compare.")
    ap.add_argument("--baseline", default=os.path.join(os.path.dirname(__file__), "..", "art-baseline.json"))
    ap.add_argument("--canary-selector", required=True,
                    help='PromQL selector for the canary, e.g. pod=~"xyne-spaces-zero-canary.*"')
    ap.add_argument("--control-selector", default="",
                    help='selector for the control population (default: whole fleet, '
                         'which includes the canary — prefer an explicit pod!~"..." exclusion)')
    ap.add_argument("--window", default="30m", help="comparison window ending now")
    ap.add_argument("--min-samples", type=int, default=100,
                    help="min canary observations per metric before verdicts count")
    ap.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "..", "reports"))
    a = ap.parse_args()

    key = os.environ.get("GR_KEY")
    if not key:
        print("ERROR: export GR_KEY first (Grafana token, on VPN)", file=sys.stderr)
        return 2

    with open(a.baseline) as f:
        baseline = json.load(f)

    print(f"shadow-compare window={a.window}\n  canary : {a.canary_selector}\n"
          f"  control: {a.control_selector or '(whole fleet)'}")
    checks: list[dict] = []
    for metric, base in baseline["server_baselines_ms"].items():
        if not isinstance(base, dict) or "p95" not in base:
            continue
        checks += compare_metric(key, metric, base, a.window,
                                 a.canary_selector, a.control_selector, a.min_samples)

    has_fail = any(c["verdict"] == "FAIL" for c in checks)
    gated = [c for c in checks if c["verdict"] in ("PASS", "FAIL", "WATCH")]
    all_nodata = gated and all(c["verdict"] == "NODATA" for c in checks
                               if c["quantile"] != "p50")
    verdict = "FAIL" if has_fail else ("NODATA" if all_nodata else "PASS")

    meta = {
        "mode": "B-shadow-compare", "baseline": os.path.relpath(a.baseline),
        "art_version": baseline.get("art_version"), "window": a.window,
        "canary_selector": a.canary_selector, "control_selector": a.control_selector,
        "min_samples": a.min_samples,
        "generated": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }
    os.makedirs(os.path.abspath(a.out_dir), exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    md_path = os.path.join(os.path.abspath(a.out_dir), f"shadow-{stamp}.md")
    json_path = os.path.join(os.path.abspath(a.out_dir), f"shadow-{stamp}.json")
    with open(md_path, "w") as f:
        f.write(render_md(meta, checks, verdict))
    with open(json_path, "w") as f:
        json.dump({"meta": meta, "verdict": verdict, "checks": checks}, f, indent=2)

    for c in checks:
        if c["verdict"] in ("FAIL", "WATCH", "NODATA") and c["quantile"] != "p50":
            print(f"  {c['verdict']:6} {c['metric']} {c['quantile']}: {c['detail']}")
    npass = sum(1 for c in checks if c["verdict"] == "PASS")
    print(f"\n=== {verdict} ===  ({npass} passed / "
          f"{sum(1 for c in checks if c['verdict'] == 'FAIL')} failed)")
    print(f"report: {md_path}")
    return 1 if has_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
