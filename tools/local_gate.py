#!/usr/bin/env python3
"""
local_gate.py — PASS/FAIL verdict for a local-sandbox ART run (no Grafana).

Consumes the driver's run summary (reports/run-*.json) and, if present, the
resource sampler summary (reports/<tag>.summary.json), and applies:

  G1  connectivity   : failed_open == 0
   G2  errors         : 0 unexpected server errors. Excluded (none is a server
                        fault): build-drift "Query not found" transformErrors
                        (sandbox build lacks a prod query, not a regression),
                        "Validation failed" transformErrors (synthetic workload
                        data mismatch, not a server bug), "Rehome: Reconnect
                        required" control messages (operational reshuffle /
                        --chaos recovery, tracked in rehomes counter), and infra
                        blips ("Internal:" timeouts, "InvalidConnectionRequest:")
  G3  protocol       : 0 invariant violations (poke framing, lmid monotonic,
                       gotQueries only for desired hashes)
  G4  mutations      : ack ratio >= 90% of sent (when mutations were driven)
  G5  latency        : p50/p95 within 1.5x of the blessed baseline FOR THE
                       RUN'S SHAPE (connections + lifecycle + zipf). Prefers
                       steady-state latency (churn puts) over combined when
                       both sides have it — session-open puts hydrate behind
                       resume catch-up and pollute the tail (a 1h lifecycle
                       soak showed p95=72s combined vs ~100ms steady p50).
                       Baseline file holds one entry per shape
                       (reports/local-baseline.json; --update-baseline blesses
                       the current run's shape without clobbering others)
   G6  leaks (soak)   : RSS slope < 200MB/h, goroutines slope < 300/h,
                        heapinuse slope < 100MB/h  (only for windows >= 15min).
                        RSS-only breach with flat/decreasing heap & goroutines
                        → WATCH (Go runtime page caching, not a leak).
  G7  gc             : art-% CVR instances stop growing after clients leave
                       (last <= max, i.e. some cleanup observed; WATCH-only)
       + OOM headroom : peak RSS vs container mem_limit (any window length):
                       >=80% WATCH, >=95% FAIL — a 2 GiB limit once OOM-killed
                       the syncer mid-run and masked every downstream gate
  G8  diff-oracle    : differential correctness oracle report (--oracle
                        reports/diff-*.json) has 0 mismatches;
                        connect_errors>0 with 0 mismatches = ERROR (infra),
                        not FAIL (pod unreachable ≠ data regression)
  G9  coverage       : every driven query hydrated at least once (WATCH-only).
                       Blind spots are split: names with a matching
                       transformError (Query not found / Validation failed)
                       are KNOWN build drift; the rest are UNEXPLAINED —
                       desired, never hydrated, no error — i.e. the shape of
                       a real delivery bug. Read UNEXPLAINED loudly.
  G10 chaos          : fault-injection report (--chaos reports/chaos-*.json)
                       shows all pauses reverted + zero-cache healthy at end

Exit 0 = PASS, 1 = FAIL, 2 = ERROR (infra — not a regression; re-run).
WATCH findings print but don't fail the gate. ERROR (e.g. pod OOM'd
mid-run, oracle/negative could not connect) is distinguished from FAIL
so a green build behind a dead pod never reads as a regression.

    python3 tools/local_gate.py                       # newest run report
    python3 tools/local_gate.py --run reports/run-X.json --resources reports/soak.summary.json
    python3 tools/local_gate.py --update-baseline     # bless current latencies (per shape)
    python3 tools/local_gate.py --out reports/gate-X.json   # persist verdicts for
                                                            # tools/consolidate_gates.py
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
import time

DRIFT_RE = re.compile(r"^transformError: .*Query not found")
VALIDATION_RE = re.compile(r"^transformError: .*Validation failed")
INFRA_PREFIXES = ("Internal:", "InvalidConnectionRequest:")
# pulls the query name out of a drift/validation transformError key
DRIFT_NAME_RE = re.compile(r"^transformError: ([\w$.-]+): (?:Query not found|Validation failed)")
BASELINE_DEFAULT = os.path.join(os.path.dirname(__file__), "..", "reports",
                                "local-baseline.json")


def shape_key(config: dict | None) -> str | None:
    """Latency is only comparable across identical load geometry: concurrency,
    lifecycle churn (resume catch-up), hot-key skew, and behavior profile all
    shift the whole distribution. One baseline entry per shape."""
    if not config or not config.get("connections"):
        return None
    key = f"{config['connections']}c"
    if config.get("lifecycle"):
        key += "-life"
    if config.get("zipf_s"):
        key += f"-zipf{config['zipf_s']:g}"
    if config.get("profile"):
        key += f"-{config['profile']}"
    # trace runs: N x time compression = N x intensity — a 2x run's latencies
    # are NOT comparable to a 1x baseline of the same trace (measured: steady
    # p50 83ms @1x vs 128ms @2x on the same build). Separate shape per factor.
    if config.get("time_compress") and config["time_compress"] != 1:
        key += f"-x{config['time_compress']:g}"
    return key


def load_baseline_shapes(path: str) -> dict:
    with open(path) as f:
        doc = json.load(f)
    if "shapes" in doc:
        return doc["shapes"]
    # legacy single-run format -> wrap as its own shape
    return {shape_key(doc.get("config")) or "unknown": doc}


def newest(pattern: str) -> str | None:
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def main() -> int:
    ap = argparse.ArgumentParser(description="Local ART gate (no Grafana).")
    ap.add_argument("--run", default=None, help="run-*.json (default: newest)")
    ap.add_argument("--resources", default=None,
                    help="resource sampler summary.json (default: newest *.summary.json)")
    ap.add_argument("--oracle", default=None,
                    help="diff_oracle report json for gate G8 (default: skip)")
    ap.add_argument("--chaos", default=None,
                    help="chaos.py summary json for gate G10 (default: skip)")
    ap.add_argument("--negative", default=None,
                    help="negative.py report json for gate G11 (default: skip)")
    ap.add_argument("--logs", default=None,
                    help="log_gate.py report json for gate G13 (default: skip)")
    ap.add_argument("--mut-matrix", default=None,
                    help="mutation_matrix.py report json for gate G15 (default: skip)")
    # --- image/lifecycle gates G16-G24 (consumed from probe/gate reports) ---
    ap.add_argument("--protocol", default=None,
                    help="probe_protocol.py report json for gate G16 (default: skip)")
    ap.add_argument("--telemetry", default=None,
                    help="telemetry_contract.py report json for gate G17 (default: skip)")
    ap.add_argument("--coldstart", default=None,
                    help="cold_start.py report json for gate G18 (default: skip)")
    ap.add_argument("--readiness", default=None,
                    help="probe_readiness.py report json for gate G19 (default: skip)")
    ap.add_argument("--drain", default=None,
                    help="drain_test.py report json for gate G20 (default: skip)")
    ap.add_argument("--determinism", default=None,
                    help="determinism_oracle.py report json for gate G21 (default: skip)")
    ap.add_argument("--capacity", default=None,
                    help="capacity_gate.py report json for gate G22 (default: skip)")
    ap.add_argument("--image-audit", default=None,
                    help="image_audit.py report json for gate G23 (default: skip)")
    ap.add_argument("--upgrade", default=None,
                    help="upgrade_path.py report json for gate G24 (default: skip)")
    ap.add_argument("--parity", default=None,
                    help="parity_gate.py report json for gate G25 (default: skip)")
    ap.add_argument("--baseline", default=BASELINE_DEFAULT)
    ap.add_argument("--update-baseline", action="store_true",
                    help="bless this run's latencies as the baseline for its shape and exit")
    ap.add_argument("--latency-factor", type=float, default=1.5)
    ap.add_argument("--per-query-factor", type=float, default=3.0,
                    help="G5b: flag a query whose steady p50 exceeds baseline "
                         "by this multiple (+100ms absolute, >=10 samples both "
                         "sides; default 3.0)")
    ap.add_argument("--out", default=None,
                    help="write verdicts as JSON (consumed by tools/consolidate_gates.py)")
    a = ap.parse_args()

    reports = os.path.join(os.path.dirname(__file__), "..", "reports")
    run_path = a.run or newest(os.path.join(reports, "run-*.json"))
    if not run_path:
        print("ERROR: no run-*.json found", file=sys.stderr)
        return 2
    with open(run_path) as f:
        run = json.load(f)
    c = run["counters"]
    lat = run.get("client_latency_ms") or {}

    if a.update_baseline:
        key = shape_key(run.get("config")) or "unknown"
        shapes: dict = {}
        if os.path.exists(a.baseline):
            try:
                shapes = load_baseline_shapes(a.baseline)
            except Exception:
                shapes = {}
        shapes[key] = {"source_run": os.path.basename(run_path),
                       "config": run.get("config"),
                       "client_latency_ms": lat,
                       "client_latency_steady_ms": run.get("client_latency_steady_ms"),
                       "client_latency_initial_ms": run.get("client_latency_initial_ms"),
                       # per-query steady p50/p95 for G5b (>=10 samples only —
                       # below that the percentiles are noise, not baseline)
                       "latency_by_query": {
                           k: v for k, v in (run.get("latency_by_query") or {}).items()
                           if (v.get("samples") or 0) >= 10}}
        os.makedirs(os.path.dirname(os.path.abspath(a.baseline)), exist_ok=True)
        with open(a.baseline, "w") as f:
            json.dump({"format": 2, "shapes": shapes}, f, indent=2)
        print(f"baseline[{key}] updated from {os.path.basename(run_path)}: "
              f"p50={lat.get('p50')} p95={lat.get('p95')} "
              f"({len(shapes)} shape(s) blessed)")
        return 0

    res_path = a.resources
    if res_path is None:
        cand = newest(os.path.join(reports, "*.summary.json"))
        res_path = cand  # optional
    resources = None
    if res_path and os.path.exists(res_path):
        with open(res_path) as f:
            resources = json.load(f)

    results: list[tuple[str, str, str]] = []  # (gate, PASS/FAIL/WATCH/SKIP, detail)

    # G1 connectivity
    results.append(("G1 connectivity",
                    "PASS" if c.get("failed_open", 0) == 0 else "FAIL",
                    f"failed_open={c.get('failed_open', 0)} opened={c.get('opened')}"))

    # G2 unexpected errors. Excluded keys are not server faults:
    #   - transformError "Query not found": sandbox build lacks a prod query
    #     (build drift), not a regression.
    #   - INVARIANT: harness-side protocol assertions, already gated by G3.
    #   - Rehome "Reconnect required": an operational client-group reshuffle
    #     (view-syncer draining, or recovery after a --chaos pause). replay.py
    #     classifies it as "not a failure" (it never increments stats.errors and
    #     bumps the dedicated `rehomes` counter instead), so counting it here
    #     would make every chaos run fail G2 while G10 passes — a false positive.
    #     A genuine failure to recover from a rehome still surfaces as a
    #     connection error, a G1 open failure, or G10 final_health != healthy.
    unexpected = {k: v for k, v in (run.get("per_error") or {}).items()
                  if not DRIFT_RE.match(k) and not VALIDATION_RE.match(k)
                  and not k.startswith("INVARIANT")
                  and not k.startswith("Rehome")
                  and not any(k.startswith(p) for p in INFRA_PREFIXES)}
    n_unexpected = sum(unexpected.values())
    detail = f"{n_unexpected} unexpected"
    if unexpected:
        top = sorted(unexpected.items(), key=lambda kv: -kv[1])[0]
        detail += f" (top: {top[1]}x {top[0][:60]})"
    results.append(("G2 errors", "PASS" if n_unexpected == 0 else "FAIL", detail))

    # G3 protocol invariants
    inv = c.get("invariant_violations", 0)
    results.append(("G3 protocol", "PASS" if inv == 0 else "FAIL",
                    f"invariant_violations={inv}"))

    # G4 mutation acks
    sent = c.get("mutations_sent", 0)
    if sent == 0:
        results.append(("G4 mutations", "SKIP", "no mutations driven"))
    else:
        ratio = c.get("mutation_ok", 0) / sent
        results.append(("G4 mutations",
                        "PASS" if ratio >= 0.90 else "FAIL",
                        f"acked {c.get('mutation_ok', 0)}/{sent} ({ratio:.0%})"))

    # G5 latency vs the blessed baseline for this run's shape
    g5_entry = None
    if not os.path.exists(a.baseline):
        results.append(("G5 latency", "SKIP",
                        "no local baseline — run --update-baseline on a good run"))
    elif not lat.get("samples"):
        results.append(("G5 latency", "SKIP", "no latency samples"))
    else:
        shapes = load_baseline_shapes(a.baseline)
        key = shape_key(run.get("config"))
        entry = shapes.get(key)
        if entry is None:
            results.append(("G5 latency", "SKIP",
                            f"no baseline for shape {key} (blessed: "
                            f"{', '.join(sorted(shapes))}) — bless a known-good "
                            "run of this shape with --update-baseline"))
        else:
            g5_entry = entry
            # steady-state (churn puts) is the clean signal; session-open puts
            # hydrate behind resume catch-up. Compare steady only when both
            # sides have it (old baselines/reports predate the split).
            base_steady = entry.get("client_latency_steady_ms") or {}
            run_steady = run.get("client_latency_steady_ms") or {}
            if base_steady.get("samples") and run_steady.get("samples"):
                cur_l, ref_l, which = run_steady, base_steady, "steady"
            else:
                cur_l, ref_l, which = lat, entry.get("client_latency_ms") or {}, "all"
            bad = []
            for p in ("p50", "p95"):
                cur, ref = cur_l.get(p), ref_l.get(p)
                if cur is not None and ref:
                    if cur > ref * a.latency_factor:
                        bad.append(f"{p} {cur:.0f}ms > {a.latency_factor}x baseline {ref:.0f}ms")
            results.append(("G5 latency", "PASS" if not bad else "FAIL",
                            "; ".join(f"[{key}/{which}] " + b for b in bad) or
                            f"[{key}/{which}] p50={cur_l.get('p50')} "
                            f"p95={cur_l.get('p95')} (within "
                            f"{a.latency_factor}x of baseline)"))

    # G5b per-query latency multipliers (staging-regression adoption; pairs
    # with replay.py's latency_by_query attribution). The aggregate G5 p50 can
    # hide one query regressing 40x behind thousands of cheap puts — exactly
    # the automationsList-x842 pattern the 2026-07-06 Go-vs-TS diff surfaced.
    # Offenders need ratio > --per-query-factor AND +100ms absolute AND >=10
    # samples on both sides AND a >=10ms baseline (3-min windows are noisy at
    # low counts; sub-10ms baselines make ratios numerically meaningless).
    # WATCH, not FAIL, while the signal is young.
    base_pq = (g5_entry or {}).get("latency_by_query") or {}
    run_pq = run.get("latency_by_query") or {}
    if not base_pq or not run_pq:
        results.append(("G5b query-lat", "SKIP",
                        "blessed shape lacks latency_by_query — re-bless with "
                        "--update-baseline" if g5_entry else
                        "no blessed shape for this run"))
    else:
        offenders = []
        compared = 0
        for qname, b in base_pq.items():
            r = run_pq.get(qname)
            if not r or (r.get("samples") or 0) < 10 or (b.get("samples") or 0) < 10:
                continue
            bp, rp = b.get("p50") or 0, r.get("p50") or 0
            if bp < 10:
                continue
            compared += 1
            if rp > bp * a.per_query_factor and rp - bp > 100:
                offenders.append((rp / bp, qname, bp, rp))
        offenders.sort(reverse=True)
        detail = f"{compared} queries compared (>=10 samples both sides)"
        if offenders:
            detail += " — " + ", ".join(
                f"{q} x{ratio:.1f} (p50 {bp:.0f}->{rp:.0f}ms)"
                for ratio, q, bp, rp in offenders[:4])
            if len(offenders) > 4:
                detail += f" (+{len(offenders) - 4} more)"
        results.append(("G5b query-lat", "WATCH" if offenders else "PASS", detail))

    # G6 leak slopes (soak only) + OOM headroom (any window)
    def headroom_findings(res: dict) -> tuple[list[str], list[str]]:
        bad, watch = [], []
        lim = res.get("mem_limit_bytes")
        peak = (res.get("rss_bytes") or {}).get("max")
        if lim and peak:
            frac = peak / lim
            msg = (f"peak RSS {peak / 2**30:.2f}GiB = {frac:.0%} of "
                   f"{lim / 2**30:.2f}GiB container limit")
            if frac >= 0.95:
                bad.append(msg + " — OOM imminent; a mid-run OOM kill masks "
                                 "every downstream gate. Raise mem_limit or "
                                 "treat as a memory regression")
            elif frac >= 0.80:
                watch.append(msg + " — headroom shrinking")
        return bad, watch

    if resources is None:
        results.append(("G6 leaks", "SKIP", "no resource summary"))
    elif resources.get("window_s", 0) < 900:
        hbad, hwatch = headroom_findings(resources)
        if hbad or hwatch:
            results.append(("G6 leaks", "FAIL" if hbad else "WATCH",
                            "; ".join(hbad + hwatch)
                            + " (window too short for leak slopes)"))
        else:
            results.append(("G6 leaks", "SKIP",
                            f"window {resources.get('window_s', 0):.0f}s < 15min — "
                            "slopes unreliable, run --soak"))
    else:
        bad, watch = headroom_findings(resources)
        limits = {"rss_bytes": 200 * 2**20, "goroutines": 300,
                  "heapinuse": 100 * 2**20}
        for m, lim in limits.items():
            s = (resources.get(m) or {}).get("slope_per_hour")
            if s is not None and s > lim:
                entry = f"{m} +{s:.0f}/h (limit {lim})"
                if m == "rss_bytes":
                    heap_s = (resources.get("heapinuse") or {}).get("slope_per_hour", 0)
                    gor_s = (resources.get("goroutines") or {}).get("slope_per_hour", 0)
                    if heap_s <= 0 and gor_s <= 0:
                        watch.append(entry + " — Go runtime page caching, not a leak")
                        continue
                bad.append(entry)
        detail = "; ".join(bad + watch) or "all slopes within limits"
        verdict = "FAIL" if bad else ("WATCH" if watch else "PASS")
        results.append(("G6 leaks", verdict, detail))

    # G7 CVR GC (WATCH) — enhanced with GC timing (#6)
    if resources and "cvr_art_instances" in resources:
        v = resources["cvr_art_instances"]
        growing = v["last"] >= v["max"] and v["last"] > v["first"]
        detail = (f"art client groups {v['first']} -> {v['last']} "
                  f"(max {v['max']})")
        if growing:
            detail += " — still growing at run end; check GC"
        else:
            # CVR count decreased from peak — measure GC timing (#6)
            declined = v["max"] - v["last"]
            if declined > 0 and v["max"] > v["first"]:
                # estimate GC delay: if count dropped, how much?
                # We don't have per-sample timestamps in the summary, so
                # report the decline amount — the resource sampler ndjson
                # has the full time series for drill-down
                detail += (f" — GC reclaimed {declined} CG(s) "
                           f"(peak {v['max']} -> end {v['last']})")
            else:
                detail += " — stable"
        results.append(("G7 cvr-gc", "WATCH" if growing else "PASS", detail))
    else:
        results.append(("G7 cvr-gc", "SKIP", "no resource summary"))

    # G8 differential oracle
    if a.oracle and os.path.exists(a.oracle):
        with open(a.oracle) as f:
            od = json.load(f)
        mism = od.get("total_mismatches", 0)
        cerr = od.get("connect_errors", 0)
        mode = "self-diff" if od.get("self_diff") else "vs mirror"
        muts = (f" muts={od.get('mutations_sent', 0)}"
                if od.get("mutations") else "")
        if mism > 0 and od.get("self_diff") and od.get("mutations"):
            # Self-diff with writes compares the pod against itself across
            # mutations — differences are the writes themselves, not a
            # regression. This happens when the TS mirror died (it exits
            # whenever the backend is recreated) and the oracle degraded.
            v8 = "ERROR"
            mode += " (INVALID: self-diff with writes — restart the TS mirror)"
        elif mism > 0:
            v8 = "FAIL"
        elif cerr > 0:
            v8 = "ERROR"
        else:
            v8 = "PASS"
        results.append(("G8 diff-oracle", v8,
                        f"{mode}: mismatches={mism} "
                        f"connect_errors={cerr}"
                        f" pairs={od.get('pairs')}{muts}"))
    else:
        results.append(("G8 diff-oracle", "SKIP",
                        "no oracle report (run with --oracle)"))

    # G9 query coverage (WATCH — but split blind spots into KNOWN build drift
    # vs UNEXPLAINED, because "desired, never hydrated, no transformError" is
    # exactly what a real delivery bug looks like). Enhanced (#4): show the
    # actual error message for blind spots that have one, and clearly
    # separate "has error" from "no error at all" (the delivery bug shape).
    cov = run.get("coverage")
    if not cov:
        results.append(("G9 coverage", "SKIP", "run summary has no coverage section"))
    else:
        missing = cov.get("never_hydrated", [])
        driven = cov.get("queries_driven", 0)
        drift_names = {m.group(1) for k in (run.get("per_error") or {})
                       if (m := DRIFT_NAME_RE.match(k))}
        # use the new never_hydrated_errors / never_hydrated_no_error fields
        # if available (replay.py >= this commit), else fall back to regex
        nh_errors = cov.get("never_hydrated_errors", {})
        nh_no_error = cov.get("never_hydrated_no_error", [])
        if nh_errors or nh_no_error:
            unexplained = nh_no_error
            known = len(missing) - len(unexplained)
        else:
            unexplained = [q for q in missing if q not in drift_names]
            known = len(missing) - len(unexplained)
        detail = f"{cov.get('queries_hydrated', 0)}/{driven} driven queries hydrated"
        if missing:
            if unexplained:
                detail += (f" — {len(unexplained)} UNEXPLAINED blind spot(s) "
                           "(no transformError; possible delivery bug): "
                           + ", ".join(unexplained[:6]))
                if len(unexplained) > 6:
                    detail += f" (+{len(unexplained) - 6} more)"
                if known:
                    detail += f"; plus {known} known build-drift"
            else:
                detail += (f" — all {known} blind spots are known build drift "
                           "(matching Query-not-found/Validation transformErrors)")
            # blind spot root cause (#4): show actual error messages
            for q, errs in list(nh_errors.items())[:4]:
                for e in errs[:1]:
                    detail += f"\n  {q}: {e}"
        results.append(("G9 coverage", "WATCH" if missing else "PASS", detail))

    # G10 chaos recovery
    if a.chaos and os.path.exists(a.chaos):
        with open(a.chaos) as f:
            ch = json.load(f)
        ok = ch.get("verdict") == "PASS"
        results.append(("G10 chaos", "PASS" if ok else "FAIL",
                        f"{ch.get('n_events', 0)} events, "
                        f"all_reverted={ch.get('all_reverted')}, "
                        f"final_health={ch.get('final_health')}"))
    else:
        results.append(("G10 chaos", "SKIP", "no chaos report (run with --chaos)"))

    # G11 negative suite (adversarial protocol paths)
    if a.negative and os.path.exists(a.negative):
        with open(a.negative) as f:
            neg = json.load(f)
        n_fail = neg.get("n_fail", 0)
        n_infra = neg.get("n_infra", 0)
        detail = (f"{neg.get('n_pass', 0)} pass / {n_fail} fail / "
                  f"{neg.get('n_skip', 0)} skip"
                  + (f" / {n_infra} infra" if n_infra else ""))
        failing = [s["name"] for s in neg.get("scenarios", [])
                   if s.get("status") == "FAIL"]
        if failing:
            detail += f" — failing: {', '.join(failing[:4])}"
        if n_fail:
            v11 = "FAIL"
        elif n_infra:
            v11 = "ERROR"
        else:
            v11 = "PASS"
        results.append(("G11 negative", v11, detail))
    else:
        results.append(("G11 negative", "SKIP",
                        "no negative report (run with --negative)"))

    # G13 server-log health (adopted from staging-regression, feature/art):
    # blocking patterns — sidecar crash, fallback-to-TS, advance reset,
    # resetting pipelines, advancement timeout — mean the pod under test
    # silently stopped being the thing we're testing, which poisons G5/G8
    # while every client-side gate stays green. FAIL on any blocking hit;
    # WATCH on slow-SQLite / ERROR-volume thresholds (see tools/log_gate.py).
    if a.logs and os.path.exists(a.logs):
        with open(a.logs) as f:
            lg = json.load(f)
        v13 = lg.get("verdict", "ERROR")
        det = "; ".join(lg.get("details", [])[:4]) or \
            f"{len(lg.get('containers', {}))} container(s) clean"
        results.append(("G13 log-health", v13, det))
    else:
        results.append(("G13 log-health", "SKIP",
                        "no log report (tools/log_gate.py --containers ...)"))

    # G14 impact-edge coverage (staging-regression adoption): did our writes
    # actually land on tables that currently-subscribed queries read? Blind
    # weighted sampling can ace G4 while never advancing a single subscribed
    # pipeline; the impact matrix makes that coverage measurable and the
    # uncovered edges nameable.
    imp = run.get("impact")
    if not imp:
        results.append(("G14 impact-cov", "SKIP",
                        "no impact block in run (needs raw/query-mutator-impact.json "
                        "+ --mutations)"))
    elif c.get("mutations_sent", 0) == 0:
        results.append(("G14 impact-cov", "SKIP", "no mutations driven"))
    else:
        ex = imp.get("edges_exercised", 0)
        reach = imp.get("edges_reachable", 0)
        pct = ex / reach if reach else 0.0
        tp, fp = imp.get("targeted_picks", 0), imp.get("fallback_picks", 0)
        detail = (f"{ex}/{reach} reachable query-mutator edges exercised "
                  f"({pct:.0%}); targeted {tp}/{tp + fp} picks")
        unc = imp.get("uncovered_sample") or []
        if unc:
            detail += " — uncovered e.g. " + ", ".join(
                f"{q}<-{m}" for q, m in unc[:3])
        results.append(("G14 impact-cov",
                        "PASS" if reach and pct >= 0.5 else "WATCH", detail))

    # G15 mutator matrix (push-path TYPE coverage): every synthesizable
    # mutator fired through the real push path, wave-converged Go-vs-TS.
    # FAIL = persistent post-mutation divergence or harness protocol errors
    # (the tool's own verdict); app-rejections/synth-invalid are coverage
    # data. WATCH when nothing diverged but the applied fraction is low —
    # a mostly-rejected matrix exercises validation, not writes.
    if a.mut_matrix and os.path.exists(a.mut_matrix):
        with open(a.mut_matrix) as f:
            mm = json.load(f)
        b = mm.get("buckets", {})
        applied = b.get("applied", 0) + b.get("acked-no-detail", 0)
        fired = mm.get("fired", 0)
        detail = (f"{fired} fired / {mm.get('planned')} planned "
                  f"({mm.get('mutators_in_catalog')} in catalog): "
                  f"applied={applied} app-rej={b.get('app-rejected', 0)} "
                  f"synth-inv={b.get('synth-invalid', 0)} "
                  f"not-found={b.get('not-found', 0)} "
                  f"timeouts={b.get('timeout', 0)}; "
                  f"diverged_waves={len(mm.get('diverged_waves') or [])}")
        shared = mm.get("shared_updates_applied") or []
        if shared:
            detail += f"; {len(shared)} shared-entity updates (audit report)"
        v15 = mm.get("verdict", "ERROR")
        if v15 == "INFRA":
            v15 = "ERROR"
        elif v15 == "PASS" and fired and applied / fired < 0.3:
            v15 = "WATCH"
        results.append(("G15 mut-matrix", v15, detail))
    else:
        results.append(("G15 mut-matrix", "SKIP",
                        "no mutation-matrix report (run with --mutation-matrix)"))

    # --- G16-G24: image lifecycle + supply-chain gates. Each consumes a JSON
    #     report from the matching probe/gate tool (uniform schema: verdict +
    #     summary + checks). A report that never ran reads SKIP (not FAIL);
    #     ERROR (infra) is distinguished from FAIL so a dead pod or a missing
    #     scanner never reads as a regression. ---
    def _consume_report(flag_val, gate_id, label, skip_hint):
        if not flag_val or not os.path.exists(flag_val):
            results.append((f"{gate_id} {label}", "SKIP", skip_hint))
            return
        with open(flag_val) as f:
            doc = json.load(f)
        v = doc.get("verdict", "ERROR")
        if v == "INFRA":
            v = "ERROR"
        results.append((f"{gate_id} {label}", v, doc.get("summary", "")))

    _consume_report(a.protocol, "G16", "protocol", "no protocol probe (run probe_protocol.py)")
    _consume_report(a.telemetry, "G17", "telemetry", "no telemetry report (run telemetry_contract.py)")
    _consume_report(a.coldstart, "G18", "coldstart", "no cold-start report (run cold_start.py)")
    _consume_report(a.readiness, "G19", "readiness", "no readiness report (run probe_readiness.py)")
    _consume_report(a.drain, "G20", "drain", "no drain report (run drain_test.py)")
    _consume_report(a.determinism, "G21", "determinism", "no determinism report (run determinism_oracle.py)")
    _consume_report(a.capacity, "G22", "capacity", "no capacity report (run capacity_gate.py)")
    _consume_report(getattr(a, "image_audit"), "G23", "image-audit", "no image audit (run image_audit.py)")
    _consume_report(a.upgrade, "G24", "upgrade", "no upgrade report (run upgrade_path.py)")
    _consume_report(a.parity, "G25", "latency-parity", "no parity report (run parity_gate.py)")

    print(f"run: {os.path.basename(run_path)}"
          + (f" | resources: {os.path.basename(res_path)}" if resources else ""))
    width = max(len(g) for g, _, _ in results)
    fail = False
    error = False
    for gate, verdict, detail in results:
        print(f"  {gate:<{width}}  {verdict:<5}  {detail}")
        fail = fail or verdict == "FAIL"
        error = error or verdict == "ERROR"
    overall = "FAIL" if fail else ("ERROR" if error else "PASS")
    if a.out:
        doc = {"schema": 1,
               "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "inputs": {
                   "run": os.path.basename(run_path),
                   "config": run.get("config"),
                   "resources": os.path.basename(res_path) if resources else None,
                   "oracle": (os.path.basename(a.oracle)
                              if a.oracle and os.path.exists(a.oracle) else None),
                   "chaos": (os.path.basename(a.chaos)
                             if a.chaos and os.path.exists(a.chaos) else None),
                   "negative": (os.path.basename(a.negative)
                                if a.negative and os.path.exists(a.negative) else None),
                   "logs": (os.path.basename(a.logs)
                            if a.logs and os.path.exists(a.logs) else None),
                   "mut_matrix": (os.path.basename(a.mut_matrix)
                                  if a.mut_matrix and os.path.exists(a.mut_matrix)
                                  else None),
                   "protocol": os.path.basename(a.protocol) if a.protocol and os.path.exists(a.protocol) else None,
                   "telemetry": os.path.basename(a.telemetry) if a.telemetry and os.path.exists(a.telemetry) else None,
                   "coldstart": os.path.basename(a.coldstart) if a.coldstart and os.path.exists(a.coldstart) else None,
                   "readiness": os.path.basename(a.readiness) if a.readiness and os.path.exists(a.readiness) else None,
                   "drain": os.path.basename(a.drain) if a.drain and os.path.exists(a.drain) else None,
                   "determinism": os.path.basename(a.determinism) if a.determinism and os.path.exists(a.determinism) else None,
                   "capacity": os.path.basename(a.capacity) if a.capacity and os.path.exists(a.capacity) else None,
                   "image_audit": os.path.basename(getattr(a, "image_audit")) if getattr(a, "image_audit") and os.path.exists(getattr(a, "image_audit")) else None,
                   "upgrade": os.path.basename(a.upgrade) if a.upgrade and os.path.exists(a.upgrade) else None,
                   "parity": os.path.basename(a.parity) if a.parity and os.path.exists(a.parity) else None},
               "results": [{"gate": g, "verdict": v, "detail": d}
                           for g, v, d in results],
               "overall": overall}
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(doc, f, indent=2)
        print(f"  gate report -> {a.out}")
    if fail:
        print("\nLOCAL ART: FAIL")
        return 1
    if error:
        print("\nLOCAL ART: ERROR (infra — not a regression; re-run)")
        return 2
    print("\nLOCAL ART: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
