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
    # go-ivm's wedge watchdog (landed 2026-07-08): a per-CG handler running
    # past ~90s logs [GO-IVM][WEDGE] cg=… method=… every tick and dumps all
    # goroutine stacks ONCE per incident between WEDGE-STACKS BEGIN/END
    # sentinels — the blocking frame is named right in the pod log.
    # NOTE: WEDGE is handled by the pairing logic in scan_container (a wedge
    # with a matching WEDGE-CLEAR self-healed => WATCH; unresolved => FAIL),
    # NOT by this any-hit list.
    # pool degraded to serial mode — go-ivm marks this 0-tolerance alongside
    # WEDGE: coread pin denied / reader pool unable to serve parallel builds.
    # Historically the precursor of the starvation family (PoolAcquireTimeout
    # deadlock, keepwarm regression); a healthy run must never log it.
    ("go-pool-serial",      r"\[GO-IVM\]\[POOL-SERIAL\]"),
    # ABI v4 delivery boundary (2026-07-08): every row/group/frame crosses
    # Go->JS through ONE bounded TSFN queue (8192). A delivery parked on a
    # full queue past GO_IVM_DELIVER_TIMEOUT_SEC (150s) with no drain and no
    # cancellation fails the stream and logs this marker — the JS event loop
    # was starved beyond any plausible recovery. This is the successor
    # signature of the pre-v4 permanent wedge (blocked goivm_call_deliver
    # holding rp.mu — diagnosed 2026-07-08 via WEDGE-STACKS dumps).
    ("go-deliver-timeout",  r"\[GO-IVM\]\[DELIVER-TIMEOUT\]"),
    # W6: pump delivery timeout is now stream-fatal — the handler sets
    # deathCause and the stream terminates. If this fires, a client saw
    # a missing row. FAIL: the stream should error, not silently drop.
    ("go-pump-deliver-fatal", r"pump deliver timed out.*stream fatally errored"),
    # Watchdog escalation ladder (2026-07-16): 2x threshold force-cancels
    # the stream gate. 6x threshold kills the process (fatalExit). The
    # ESCALATE marker means the progress handler cancel was needed — if it
    # fires, WATCH (the system self-healed but a wedge was real). The FATAL
    # marker means the process died — that's a blocking FAIL if the pod
    # somehow survived (it shouldn't).
    ("go-wedge-fatal",      r"\[GO-IVM\]\[WEDGE-FATAL\]"),
]
# Routine self-heal under load — the reference TS pod produces these too
# (84/10min at 1x prod trace). Signal is the RATE, not the existence.
SELF_HEAL: list[tuple[str, str]] = [
    ("advance-reset",       r"advance reset for clientGroup"),
    ("resetting-pipelines", r"resetting pipelines"),
    ("advancement-timeout", r"Advancement exceeded timeout"),
]
SLOW_RE = re.compile(r"Slow SQLite query[^0-9]*([0-9.]+)")
SLOW_MAT_RE = re.compile(r"Slow query materialization ([0-9.]+)")
ERROR_RE = re.compile(r'"level":"ERROR"|level.:.error|\blevel=error\b', re.I)
# wedge watchdog pairing: [WEDGE] repeats per tick while stuck; [WEDGE-CLEAR]
# closes the incident. Match by cg.
WEDGE_RE = re.compile(r"\[GO-IVM\]\[WEDGE\] cg=(\S+)")
WEDGE_CLEAR_RE = re.compile(r"\[GO-IVM\]\[WEDGE-CLEAR\] cg=(\S+)")
WEDGE_ESCALATE_RE = re.compile(r"\[GO-IVM\]\[WEDGE-ESCALATE\] cg=(\S+)")
# New markers from the progress handler / watchdog ladder:
IDLE_DAMPER_RE = re.compile(r"\[GO-IVM\]\[IDLE-DAMPER\] (\d+) pull idle-timeouts")
SCAN_WARN_RE = re.compile(r"\[GO-IVM\]\[SCAN-WARN\] table=(\S+) rows=(\d+)")
WEDGE_FATAL_RE = re.compile(r"\[GO-IVM\]\[WEDGE-FATAL\]")
PUMP_FATAL_RE = re.compile(r"pump deliver timed out.*stream fatally errored")
PERF_PULL_RE = re.compile(r"\[GO-IVM\]\[PERF-PULL\].*?pull idle-timeouts=(\d+)")
# ABI v4 boundary health: prints only when nonzero. stalls = enqueue found
# all 8192 TSFN slots full and parked (100µs->5ms retry loop, cancellable);
# timeouts = parked past 150s (also emits DELIVER-TIMEOUT above).
# staged/batchFlushes (added 2026-07-08, staging build): staged = BENIGN
# coalescing — entries buffered while the JS loop is busy then flushed in
# batches; it tracks loop busyness, not distress. Healthy signature:
# staged >> stalls, stalls ~0, timeouts 0. Optional in the regex so pre-batch
# builds still parse.
PERF_NAPI_RE = re.compile(
    r"\[GO-IVM\]\[PERF-NAPI\].*?stalls=(\d+) timeouts=(\d+)"
    r"(?: staged=(\d+) batchFlushes=(\d+))?")


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

    # -- wedge watchdog: pair WEDGE incidents with WEDGE-CLEARs per cg -------
    wedged = {m.group(1) for ln in lines if (m := WEDGE_RE.search(ln))}
    cleared = {m.group(1) for ln in lines if (m := WEDGE_CLEAR_RE.search(ln))}
    unresolved = sorted(wedged - cleared)
    if unresolved:
        # never cleared within the scan window = the pre-v4 permanent-wedge
        # class; the stack dump between WEDGE-STACKS BEGIN/END names the frame
        hits["go-wedge-unresolved"] = {
            "count": len(unresolved),
            "samples": [f"cg={c} wedged, no WEDGE-CLEAR in window"
                        for c in unresolved[:3]]}

    # -- ABI v4 Go->JS delivery boundary -------------------------------------
    stalls = timeouts = staged = batch_flushes = napi_windows = 0
    for ln in lines:
        m = PERF_NAPI_RE.search(ln)
        if m:
            napi_windows += 1
            stalls += int(m.group(1))
            timeouts += int(m.group(2))
            staged += int(m.group(3) or 0)
            batch_flushes += int(m.group(4) or 0)
    slow_mat = [float(m.group(1)) for ln in lines
                if (m := SLOW_MAT_RE.search(ln))]
    slow_mat_10s = sum(1 for v in slow_mat if v > 10_000)

    slow_ms = [float(m.group(1)) for ln in lines if (m := SLOW_RE.search(ln))]
    n_err = sum(1 for ln in lines if ERROR_RE.search(ln))

    watch: list[str] = []
    if slow_ms and max(slow_ms) > slow_ms_watch:
        watch.append(f"slow-sqlite max {max(slow_ms):.0f}ms > {slow_ms_watch:.0f}ms")
    if len(slow_ms) > slow_count_watch:
        watch.append(f"slow-sqlite count {len(slow_ms)} > {slow_count_watch}")
    if n_err > error_count_watch:
        watch.append(f"ERROR-level lines {n_err} > {error_count_watch}")
    if cleared:
        # >90s stall happened but the deliver drained and the handler
        # completed — the pass signature explicitly allows self-clearing
        watch.append(f"go-wedge self-cleared: {len(cleared)} cg(s) "
                     f"({', '.join(sorted(cleared)[:3])})")
    if stalls:
        # Expected under load WHEN the JS loop is provably busy (synchronous
        # TS materializations): Go parks briefly holding no lock, continues.
        # Stalls WITHOUT TS-side slowness = the queue filled for a reason we
        # can't see — a new finding per the v4 contract; flag it louder.
        # (staged is deliberately NOT a watch trigger: batch-coalescing is
        # the designed absorption path — staged >> stalls is the healthy
        # signature, so it only rides along as context here.)
        corr = (f"correlated: {slow_mat_10s} materializations >10s in window"
                if slow_mat_10s else
                "NO slow TS materializations in window — uncorrelated "
                "stall source, new finding worth flagging")
        watch.append(f"napi-deliver stalls={stalls} across {napi_windows} "
                     f"10s-windows (timeouts={timeouts}, staged={staged}, "
                     f"batchFlushes={batch_flushes}) — {corr}")

    # -- progress handler / watchdog ladder markers (2026-07-16) ----------
    escalated = [m.group(1) for ln in lines if (m := WEDGE_ESCALATE_RE.search(ln))]
    if escalated:
        watch.append(f"wedge-escalate: {len(escalated)} cg(s) force-cancelled "
                     f"({', '.join(sorted(set(escalated))[:3])}) — "
                     f"progress handler cancel was needed")
    idle_damper = [int(m.group(1)) for ln in lines if (m := IDLE_DAMPER_RE.search(ln))]
    if idle_damper:
        watch.append(f"idle-damper: {len(idle_damper)} warning(s), "
                     f"max {max(idle_damper)} idle-timeouts in a 5min window")
    scan_warns = [(m.group(1), int(m.group(2))) for ln in lines if (m := SCAN_WARN_RE.search(ln))]
    if scan_warns:
        tables = ', '.join(f"{t}({r})" for t, r in scan_warns[:3])
        watch.append(f"scan-warn: {len(scan_warns)} full table scan(s) detected "
                     f"at plan time — tables: {tables}")
    pull_idle_total = sum(int(m.group(1)) for ln in lines if (m := PERF_PULL_RE.search(ln)))
    if pull_idle_total:
        watch.append(f"pull idle-timeouts: {pull_idle_total} in window")

    return {
        "lines_scanned": len(lines),
        "blocking_hits": hits,
        "self_heal_hits": heal,
        "wedges": {"wedged_cgs": sorted(wedged), "cleared_cgs": sorted(cleared),
                   "unresolved_cgs": unresolved},
        "napi_deliver": {"stall_windows": napi_windows, "stalls": stalls,
                         "timeouts": timeouts, "staged": staged,
                         "batch_flushes": batch_flushes,
                         "slow_materializations_gt10s": slow_mat_10s},
        "progress_handler": {
            "wedge_escalated": sorted(set(escalated)) if escalated else [],
            "idle_damper_count": len(idle_damper),
            "idle_damper_max": max(idle_damper) if idle_damper else 0,
            "scan_warnings": scan_warns if scan_warns else [],
            "pull_idle_total": pull_idle_total,
        },
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


# --- Unknown log-signature detector ---
# Normalizes ERROR/WARN lines to signatures (strip IDs, numbers, durations)
# and flags signatures not seen in a blessed baseline. Catches new failure
# modes the blocklist doesn't know about yet.

SIG_ID_RE = re.compile(r"\b[0-9a-f]{8,}\b")  # hex IDs (8+ chars)
SIG_NUM_RE = re.compile(r"\b\d+\b")  # standalone numbers
SIG_DUR_RE = re.compile(r"\d+(?:\.\d+)?(?:ms|s|m|µs|ns|h)")  # durations
SIG_CG_RE = re.compile(r"cg=[^\s]+")  # client group IDs
SIG_UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
SIG_BASELINE_PATH = os.path.join(os.path.dirname(__file__), "..", "reports",
                                 "log-signatures-baseline.json")


def normalize_signature(line: str) -> str:
    """Strip variable parts from a log line to produce a stable signature."""
    s = line.strip()
    s = SIG_UUID_RE.sub("*", s)
    s = SIG_CG_RE.sub("cg=*", s)
    s = SIG_DUR_RE.sub("*", s)
    s = SIG_ID_RE.sub("*", s)
    s = SIG_NUM_RE.sub("*", s)
    return s[:200]  # cap length


def scan_unknown_signatures(container: str, since: str) -> set:
    """Extract ERROR/WARN signatures from container logs."""
    try:
        out = subprocess.run(
            ["docker", "logs", "--since", since, container],
            capture_output=True, text=True, timeout=30)
    except Exception:
        return set()
    sigs = set()
    for line in out.stderr.splitlines() + out.stdout.splitlines():
        # Match ERROR/WARN level lines (structured logs or plain)
        if not re.search(r"\b(ERROR|WARN|error|warn)\b", line, re.I):
            continue
        sigs.add(normalize_signature(line))
    return sigs


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
    ap.add_argument("--update-baseline", action="store_true",
                    help="bless the current run's ERROR/WARN signatures as the baseline")
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

    # --- Unknown log-signature detector: flag ERROR/WARN lines not seen in
    #     a blessed baseline. Catches new failure modes the blocklist
    #     doesn't know about. First run = --update-baseline to seed. ---
    baseline = set()
    if os.path.exists(SIG_BASELINE_PATH):
        try:
            baseline = set(json.load(open(SIG_BASELINE_PATH)).get("signatures", []))
        except Exception:
            pass
    all_sigs = set()
    for name in [c.strip() for c in a.containers.split(",") if c.strip()]:
        all_sigs |= scan_unknown_signatures(name, since)
    unknown = all_sigs - baseline
    if a.update_baseline:
        os.makedirs(os.path.dirname(SIG_BASELINE_PATH), exist_ok=True)
        with open(SIG_BASELINE_PATH, "w") as f:
            json.dump({"signatures": sorted(all_sigs | baseline)}, f, indent=2)
        report["details"].append(f"baseline updated: {len(all_sigs)} signatures")
    elif unknown:
        if len(unknown) > 5:
            raise_to("FAIL")
            report["details"].append(
                f"unknown-signatures: {len(unknown)} new ERROR/WARN signatures "
                f"(>5 threshold) — possible new failure mode: "
                + "; ".join(list(unknown)[:3]))
        else:
            raise_to("WATCH")
            report["details"].append(
                f"unknown-signatures: {len(unknown)} new ERROR/WARN signature(s): "
                + "; ".join(list(unknown)[:3]))
    report["signature_counts"] = {"total": len(all_sigs), "baseline": len(baseline), "unknown": len(unknown)}

    if a.out:
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
    print(f"log gate: {report['verdict']}"
          + (f" — {'; '.join(report['details'][:4])}" if report["details"] else ""))
    return 0 if report["verdict"] != "ERROR" else 2


if __name__ == "__main__":
    raise SystemExit(main())
