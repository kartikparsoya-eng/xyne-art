#!/usr/bin/env python3
"""
log_gate.py — G13: server-log health scan over a run window (both pods).

Idea adopted from xyne-spaces feature/art scripts/staging-regression (commit
81c133fa2): their README's release-block list — sidecar crashes, fallback-to-TS,
"Advancement exceeded timeout", "advance reset for clientGroup", "resetting
pipelines", severe "Slow SQLite query" spikes — is exactly the class of failure
our client-side gates CANNOT see. The sharpest hole it closes here: if the Go
sidecar crashes and the pod silently falls back to the TS path mid-run, the G8
oracle compares TS-vs-TS and happily passes while certifying nothing. A/B
latency numbers are equally invalidated by silent advance-reset loops.

Scans `docker logs --since <run window>` of each container for:
  HARD BLOCKING (any hit => FAIL): Go runtime fatals/panics, sidecar
                                   crash/respawn, fallback-to-TS, reset
                                   circuit breaker, RPC init timeout,
                                   Go backend init failure
  SELF-HEAL (rate-gated):          advance reset / resetting pipelines /
                                   advancement timeout — the 1x trace A/B
                                   proved TS 1.7.0 (the REFERENCE) logs ~8/min
                                   of these as routine self-heal under real
                                   prod load; any-hit=FAIL could never pass
                                   the reference (false-positive class #9).
                                   WATCH when present; FAIL above
                                   --reset-rate-fail per minute (default 30).
  WATCH (thresholded):             Slow SQLite query spikes (count + max ms),
                                   generic ERROR-level volume

Never mutates anything; exit 0 with a JSON report (2 on scan infra failure).
local_gate.py consumes the report via --logs and folds it in as gate G13.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time

# Any hit = the pod is no longer the thing we think we are testing.
HARD_BLOCKING: list[tuple[str, str]] = [
    # (label, regex — matched case-insensitively per line)
    ("go-fatal",            r"\bpanic:|\bfatal error:|\bruntime error\b"),
    ("sidecar-crash",       r"sidecar.*(crash|exited unexpectedly|respawn|restart(ing|ed))"),
    ("fallback-to-ts",      r"fall(ing)?[ -]?back.{0,30}(ts|typescript)|sidecar fallback"),
    ("breaker-tripped",     r"reset circuit breaker tripped"),
    ("rpc-init-timeout",    r"RPC init timed out"),
    ("go-init-failed",      r"Go backend init failed"),
]
# Routine self-heal under load — the reference TS pod produces these too
# (84/10min at 1x prod trace). Signal is the RATE, not the existence.
SELF_HEAL: list[tuple[str, str]] = [
    ("advance-reset",       r"advance reset for clientGroup"),
    ("resetting-pipelines", r"resetting pipelines"),
    ("advancement-timeout", r"Advancement exceeded timeout"),
]
SLOW_RE = re.compile(r"Slow SQLite query[^0-9]*([0-9.]+)")
ERROR_RE = re.compile(r'"level":"ERROR"|level.:.error|\blevel=error\b', re.I)


def scan_container(name: str, since: str, slow_ms_watch: float,
                   slow_count_watch: int, error_count_watch: int) -> dict:
    try:
        out = subprocess.run(
            ["docker", "logs", "--since", since, name],
            capture_output=True, text=True, timeout=120)
    except Exception as e:  # docker gone = scan infra failure, not a verdict
        return {"scan_error": str(e)}
    text = out.stdout + "\n" + out.stderr
    lines = text.splitlines()

    def match(pats: list[tuple[str, str]]) -> dict[str, dict]:
        found: dict[str, dict] = {}
        for label, pat in pats:
            rx = re.compile(pat, re.I)
            matched = [ln for ln in lines if rx.search(ln)]
            if matched:
                found[label] = {"count": len(matched),
                                "samples": [ln[:300] for ln in matched[:3]]}
        return found

    hits = match(HARD_BLOCKING)
    heal = match(SELF_HEAL)

    slow_ms = [float(m.group(1)) for ln in lines if (m := SLOW_RE.search(ln))]
    n_err = sum(1 for ln in lines if ERROR_RE.search(ln))

    watch: list[str] = []
    if slow_ms and max(slow_ms) > slow_ms_watch:
        watch.append(f"slow-sqlite max {max(slow_ms):.0f}ms > {slow_ms_watch:.0f}ms")
    if len(slow_ms) > slow_count_watch:
        watch.append(f"slow-sqlite count {len(slow_ms)} > {slow_count_watch}")
    if n_err > error_count_watch:
        watch.append(f"ERROR-level lines {n_err} > {error_count_watch}")

    return {
        "lines_scanned": len(lines),
        "blocking_hits": hits,
        "self_heal_hits": heal,
        "slow_sqlite": {"count": len(slow_ms),
                        "max_ms": round(max(slow_ms), 1) if slow_ms else 0,
                        "p50_ms": round(sorted(slow_ms)[len(slow_ms) // 2], 1) if slow_ms else 0},
        "error_level_lines": n_err,
        "watch": watch,
    }


def window_minutes(since: str) -> float:
    """--since is RFC3339 or a docker duration (300s/10m/1h). Needed to turn
    self-heal counts into rates."""
    m = re.fullmatch(r"([0-9.]+)([smh])", since.strip())
    if m:
        v = float(m.group(1))
        return {"s": v / 60, "m": v, "h": v * 60}[m.group(2)]
    try:
        t = time.strptime(since.split(".")[0].replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
        return max((time.time() - (time.mktime(t) - time.timezone)) / 60.0, 0.5)
    except Exception:
        return 10.0


def main() -> int:
    ap = argparse.ArgumentParser(description="G13 server-log health gate.")
    ap.add_argument("--containers", required=True,
                    help="comma-separated container names (primary[,mirror,...])")
    ap.add_argument("--since", default=None,
                    help="RFC3339 timestamp or docker duration (e.g. 300s). "
                         "Default: derived from --run's window.start")
    ap.add_argument("--run", default=None,
                    help="run-*.json — window.start (minus 30s margin) becomes --since")
    ap.add_argument("--slow-ms-watch", type=float, default=2000.0)
    ap.add_argument("--slow-count-watch", type=int, default=2000)
    ap.add_argument("--error-count-watch", type=int, default=50)
    ap.add_argument("--reset-rate-fail", type=float, default=30.0,
                    help="self-heal events/min above which the pod is judged "
                         "thrashing, not healing (reference TS: ~8/min at 1x "
                         "prod load; 4x-compressed storms ran >45/min)")
    ap.add_argument("--out", default=None, help="write the JSON report here")
    a = ap.parse_args()

    since = a.since
    if since is None and a.run:
        try:
            start = json.load(open(a.run))["window"]["start"]
            # 30s margin so we see crash/restart fallout from the ramp-up
            t = time.strptime(start.split(".")[0].replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
            since = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                  time.gmtime(time.mktime(t) - time.timezone - 30))
        except Exception as e:
            print(f"WARN: cannot derive window from {a.run}: {e}; using 10m", file=sys.stderr)
    since = since or "10m"
    win_min = window_minutes(since)

    report: dict = {"since": since, "window_minutes": round(win_min, 1),
                    "containers": {}, "verdict": "PASS", "details": []}
    worst = {"PASS": 0, "WATCH": 1, "FAIL": 2, "ERROR": 3}

    def raise_to(v: str) -> None:
        if worst[v] > worst[report["verdict"]]:
            report["verdict"] = v

    for name in [c.strip() for c in a.containers.split(",") if c.strip()]:
        r = scan_container(name, since, a.slow_ms_watch, a.slow_count_watch,
                           a.error_count_watch)
        report["containers"][name] = r
        if "scan_error" in r:
            raise_to("ERROR")
            report["details"].append(f"{name}: scan failed: {r['scan_error']}")
            continue
        if r["blocking_hits"]:
            raise_to("FAIL")
            for label, h in r["blocking_hits"].items():
                report["details"].append(f"{name}: {h['count']}x {label}")
        for label, h in r.get("self_heal_hits", {}).items():
            rate = h["count"] / win_min
            if rate > a.reset_rate_fail:
                raise_to("FAIL")
                report["details"].append(
                    f"{name}: {label} {rate:.1f}/min > {a.reset_rate_fail:.0f}/min "
                    f"({h['count']}x in {win_min:.0f}min) — thrashing")
            else:
                raise_to("WATCH")
                report["details"].append(
                    f"{name}: {h['count']}x {label} ({rate:.1f}/min, self-heal)")
        if r["watch"]:
            raise_to("WATCH")
            report["details"] += [f"{name}: {w}" for w in r["watch"]]

    if a.out:
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
    print(f"log gate: {report['verdict']}"
          + (f" — {'; '.join(report['details'][:4])}" if report["details"] else ""))
    return 0 if report["verdict"] != "ERROR" else 2


if __name__ == "__main__":
    raise SystemExit(main())
