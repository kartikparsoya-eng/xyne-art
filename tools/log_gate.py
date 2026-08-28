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

  G13b INVERTED (--unknown-errors): every ERROR (and optionally WARN) line in
                                   the window that is NOT allow-listed and NOT
                                   an already-known blocking/self-heal signature
                                   is an UNKNOWN_ERROR. This flips the gate from
                                   "confirm known-good" to "surface unknowns" —
                                   the class of bug (CVR-not-bumped, "Diff is no
                                   longer valid", "Expected object at result")
                                   that passed silently only because its
                                   signature wasn't enumerated. mode: fail =
                                   any unknown FAILs; warn (default) = WATCH;
                                   off = skip. Those four known signatures are
                                   now in HARD_BLOCKING (CONNECTION_FATAL) so
                                   they FAIL hard regardless of this flag.

Never mutates anything; exit 0 with a JSON report (2 on scan infra failure).
local_gate.py consumes the report via --logs and folds it in as gate G13.
"""
from __future__ import annotations

import argparse
import json
import os
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
    ("rust-thread-panic",   r"thread ['\"].*['\"] panicked|engine advance panic"),
    ("rust-advance-panic",  r"\[rust-ivm\] advance(?: streamed)? panicked"),
    ("rust-init-failed",    r"\[rust-ivm\].*(snapshotter|source|engine).*init.*failed"),
    ("rust-ast-parse",      r"AST parse error for qid="),
    ("sqlite-corrupt",      r"database disk image is malformed|SQLITE_CORRUPT"),
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
    # --- CONNECTION_FATAL: real prod bugs that slipped through the old
    #     confirm-known-good model because their signatures weren't in any
    #     list. Each corrupts the CVR / advance / result contract; a hit means
    #     a client saw wrong or missing data. FAIL hard, regardless of the
    #     unknown-error allowlist below. (Kept in HARD_BLOCKING so they FAIL
    #     even when --unknown-errors is off.) ---
    ("cvr-version-not-bumped", r"Expected CVR version to have been bumped"),
    # FATAL only: the napi-wrapped "advance failed:" teardown form that closes
    # the client connection. Do NOT match the bare "Diff is no longer valid"
    # text — since the stale-snapshot fix, that text also appears in the BENIGN
    # INFO self-heal line ("resetting pipelines: Diff is no longer valid. prev
    # db has advanced past X"), which is a recoverable rehydrate, not a teardown
    # (counted in SELF_HEAL / resetting-pipelines below, rate-gated).
    ("diff-invalidated",       r"advance failed: Diff is no longer valid"),
    ("missing-result-object",  r"Expected object at result"),
    ("bad-primary-key",        r"toPrimaryKeyString"),
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
# Structured-log field normalization (JSON key:value pairs with dynamic IDs)
SIG_FIELD_RES = [
    re.compile(r'\"clientGroupID\":\"[^"]+\"'),
    re.compile(r'\"instance\":\"[^"]+\"'),
    re.compile(r'\"lock\":\"[^"]+\"'),
    re.compile(r'\"clientID\":\"[^"]+\"'),
    re.compile(r'\"wsID\":\"[^"]+\"'),
    re.compile(r'\"stateVersion\":\"[^"]+\"'),
    re.compile(r'\"hash\":\"[^"]+\"'),
    re.compile(r'\"queryHash\":\"[^"]+\"'),
    re.compile(r'\"transformationHash\":\"[^"]+\"'),
]
SIG_BASELINE_PATH = os.path.join(os.path.dirname(__file__), "..", "reports",
                                 "log-signatures-baseline.json")
SIG_SCHEMA = 2


def _json_event(line: str) -> dict | None:
    """Parse a log line as a JSON OBJECT, else None. The dict guard matters:
    lines like the rust pod's `["ready", {"ready": true}]` stdout handshake
    parse as a list, and scalar lines parse as int/str — calling `.get` on
    those raises AttributeError and kills the whole scan."""
    try:
        event = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None
    return event if isinstance(event, dict) else None


def extract_message(event: dict) -> str | None:
    """The human message of a structured log event, wherever the log format
    puts it. Two JSON shapes coexist across the pods:
      - TS pods:   {"level":"ERROR", ..., "message": "..."}   (top level)
      - rust pod:  {"timestamp": ..., "level":"ERROR",
                    "fields":{"message":"..."}, "target":"..."}
        (tracing_subscriber's json formatter, emitted since ZERO_LOG_FORMAT=
        json is honored — the message nests under "fields")."""
    msg = event.get("message")
    if isinstance(msg, str):
        return msg
    fields = event.get("fields")
    if isinstance(fields, dict) and isinstance(fields.get("message"), str):
        return fields["message"]
    return None


def normalize_signature(line: str) -> str:
    """Strip variable parts from a log line to produce a stable signature."""
    s = line.strip()
    event = _json_event(s)
    if event is not None:
        # Query ASTs, SQL text, IDs, and stack traces describe the context, not
        # the failure class. Keeping them made one Slow SQLite warning produce
        # thousands of distinct signatures. Retain only stable routing and
        # diagnostic fields, then normalize dynamic values below.
        semantic = {
            key: event[key]
            for key in (
                "level", "worker", "component", "class", "method", "name",
                "errorKind", "error", "errorMsg", "message",
            )
            if key in event and isinstance(event[key], (str, int, float, bool))
        }
        # rust-pod tracing JSON nests the message under "fields" and routes by
        # "target". Without lifting them, every rust ERROR line collapsed to
        # the single useless generic signature {"level":"ERROR"} — distinct
        # failure modes became indistinguishable and un-allowlistable.
        if "message" not in semantic:
            msg = extract_message(event)
            if msg is not None:
                semantic["message"] = msg
        fields = event.get("fields")
        if isinstance(fields, dict):
            for key in ("errorKind", "error", "errorMsg"):
                if (key not in semantic
                        and isinstance(fields.get(key), (str, int, float, bool))):
                    semantic[key] = fields[key]
        if isinstance(event.get("target"), str):
            semantic["target"] = event["target"]
        body = event.get("errorBody")
        if isinstance(body, dict):
            for key in ("kind", "message"):
                if isinstance(body.get(key), (str, int, float, bool)):
                    semantic[f"errorBody.{key}"] = body[key]
        s = json.dumps(semantic, sort_keys=True, separators=(",", ":"))
    s = SIG_UUID_RE.sub("*", s)
    s = SIG_CG_RE.sub("cg=*", s)
    for rx in SIG_FIELD_RES:
        s = rx.sub(lambda m: m.group(0).split(":")[0] + ':"*"', s)
    s = SIG_DUR_RE.sub("*", s)
    s = SIG_ID_RE.sub("*", s)
    s = SIG_NUM_RE.sub("*", s)
    return s[:500]  # cap pathological unstructured messages


def is_error_or_warn_level(line: str) -> bool:
    """Match the event level, not words such as "error" in an INFO message."""
    event = _json_event(line)
    if event is not None:
        level = event.get("level")
        if isinstance(level, str):
            return level.upper() in {"ERROR", "WARN", "WARNING"}
    return bool(re.search(
        r"(?:^|\s)(?:ERROR|WARN|WARNING)(?:\s|:)|\blevel=(?:error|warn|warning)\b",
        line,
        re.I,
    ))


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
        if not is_error_or_warn_level(line):
            continue
        if _known_labeled(line):
            continue
        sigs.add(normalize_signature(line))
    return sigs


# --- Inverted gate (G13b): unrecognized ERROR/WARN => FAIL/WATCH ------------
# The blocklist above answers "did a KNOWN-bad thing happen?". This flips the
# model to "did an UNKNOWN thing happen?" — every ERROR (and WARN) line in the
# window that is NOT explicitly allow-listed and NOT already a known
# HARD_BLOCKING/SELF_HEAL signature is collected as an UNKNOWN_ERROR. Several
# real prod bugs (CVR-not-bumped, "Diff is no longer valid", "Expected object
# at result") passed silently under the old confirm-known-good model precisely
# because their signatures weren't enumerated; this surfaces the next one
# automatically. Unlike the baseline-diff detector below, this needs no blessed
# baseline file to work.
#
# ALLOWLIST: known-benign / expected ERROR|WARN signatures (regex, matched
# case-insensitively). Seeded from local_gate.py's excluded-drift strings
# (Query not found / Validation failed / Internal: / InvalidConnectionRequest:
# / ClientNotFound: / Rehome), the routine SELF_HEAL lines, and clearly-benign
# operational lines (client disconnects, TTL/CVR purges). Keep this list SMALL,
# explicit, and commented — every entry is a deliberate "this ERROR/WARN is not
# a regression" decision.
ALLOWLIST: list[tuple[str, str]] = [
    # (label, regex) — mirrors local_gate.py DRIFT_RE / VALIDATION_RE /
    # INFRA_PREFIXES / Rehome so the two gates agree on what is benign.
    ("query-not-found",     r"Query not found"),          # sandbox build lacks a prod query (build drift)
    ("validation-failed",   r"Validation failed"),        # synthetic workload data mismatch, not a server bug
    ("internal-timeout",    r"\bInternal:\s"),            # infra blip / timeout
    ("invalid-conn-req",    r"InvalidConnectionRequest"), # client sent a stale/rehomed connect request
    ("client-not-found",    r"ClientNotFound"),           # client GC raced a late message
    # rust syncer wording of the same protocol case: the harness purges/expires
    # CVRs between phases; the next connect's CVR load finds no client row and
    # the server answers ClientNotFound, which resets the client — designed
    # protocol behavior, not a fault. Surfaced as "new" only once the rust pod
    # started emitting JSON (ZERO_LOG_FORMAT=json honored) and the old
    # text-format signatures stopped matching.
    ("load-cvr-client-not-found", r"load_cvr failed.{0,60}Client not found"),
    ("rehome-reconnect",    r'Rehome:? Reconnect required|"kind":"Rehome"|kind:\s*Rehome'), # operational reshuffle, tracked in rehomes — JSON `"kind":"Rehome"` (TS) + Rust-debug `kind: Rehome` (supersede/drain/restart, all benign)
    ("unauthorized-client", r"Unauthorized"),             # negative/auth suite deliberately violates ownership
    ("auth-invalidated",    r"AuthInvalidated|Failed to decode auth token"), # invalid-token negative case
    ("cancelled-init",      r"shut down before initialization completed"), # cancel-mid-hydrate teardown
    ("slow-materialize",    r"Slow query materialization"), # timing signal, tracked separately above
    ("slow-sqlite",         r"Slow SQLite query"), # count/duration gated by scan_container
    ("mutation-replayed",   r'"error":"alreadyProcessed"'), # lifecycle resend after lost ack
    # routine operational ERROR/WARN under load — not a fault:
    ("client-disconnect",   r"client (disconnected|closed|gone)|websocket.*(closed|EOF)|connection reset by peer"),
    ("ttl-purge",           r"(TTL|ttl).{0,20}(purge|expire|evict)|purging expired"),
    ("cvr-gc",              r"(CVR|cvr).{0,20}(garbage.?collect|evict|purg)"),
    ("context-canceled",    r"context canceled|context deadline exceeded"),  # client went away mid-request
    ("invalid-message",     r"InvalidMessage"),  # G31 unknown-op / malformed-frame probes deliberately elicit this clean rejection
    # Metrics-EXPORT infra noise from the TS mirror's node OTel exporter (its
    # collector endpoint 404s); never a sync-correctness signal. The candidate's
    # own metric contract is guarded by G17, which scrapes the collector.
    ("otlp-export-404",     r"OTLPExporterError"),
    # Deny-by-default read-permission warn: emitted by BOTH impls for any
    # client-AST query on a table with no `row.select` rules (TS
    # read-authorizer.ts:71; rust read_authorizer.rs — parity warn added with
    # the 2026-08-28 fail-open fix, mono b4754f12d). The #158 oracle rider
    # queries `channels` (rule-less) each full-catalog run, so this fires by
    # design; G8 asserts both sides DENY identically.
    ("no-permission-rules", r"No permission rules found for table"),
    # Client-group user pinning rejection (TS syncer.ts pinnedUser check; rust
    # router.rs check_and_pin_user): a connect with a different userID than the
    # group's pinned user is refused with Unauthorized. The multi-user harness
    # exercises this deliberately; the rejection IS the designed behavior.
    ("user-pin-mismatch",   r"User ID mismatch: pinned="),
    # Sanctioned whole-pipeline reset (TS ResetPipelinesSignal; rust
    # ivm ResetPipelinesSignal port): scalar-subquery / schema-change /
    # truncation / advancement-timeout resets destroy + rebuild the CG's
    # pipelines BY DESIGN (cheaper than advancing; view-syncer.ts:569-586).
    # Frequency is separately bounded by the resetting-pipelines rate check.
    ("pipeline-reset",      r"pipeline reset \((scalar-subquery|schema-change|truncation|advancement-timeout|wall-clock)"),
]


def _known_labeled(line: str) -> str | None:
    """Return a label if the line matches an already-known ALLOWLIST /
    HARD_BLOCKING / SELF_HEAL signature, else None. Used to decide whether an
    ERROR/WARN line is UNKNOWN (nothing recognizes it).

    Patterns are matched against the raw line AND, for structured (JSON)
    lines, against the extracted human message — so allowlist regexes written
    for one format keep matching regardless of which shape carried the
    message (text, TS top-level "message", or rust "fields.message" — where
    JSON escaping in the raw line can defeat a raw-text regex)."""
    texts = [line]
    event = _json_event(line)
    if event is not None:
        msg = extract_message(event)
        if msg is not None:
            texts.append(msg)
    for label, pat in ALLOWLIST:
        if any(re.search(pat, t, re.I) for t in texts):
            return f"allow:{label}"
    for label, pat in HARD_BLOCKING:
        if any(re.search(pat, t, re.I) for t in texts):
            return f"blocking:{label}"
    for label, pat in SELF_HEAL:
        if any(re.search(pat, t, re.I) for t in texts):
            return f"self-heal:{label}"
    return None


def scan_unknown_errors(container: str, since: str, include_warn: bool) -> dict:
    """Collect ERROR (and optionally WARN) lines not matched by ALLOWLIST or a
    known HARD_BLOCKING/SELF_HEAL pattern. Returns distinct normalized
    signatures with counts + a sample line."""
    try:
        out = subprocess.run(
            ["docker", "logs", "--since", since, container],
            capture_output=True, text=True, timeout=120)
    except Exception as e:
        return {"scan_error": str(e)}
    lines = (out.stdout + "\n" + out.stderr).splitlines()
    level_re = (re.compile(r'"level":"(ERROR|WARN)"|\blevel=(error|warn)\b|\b(ERROR|WARN)\b', re.I)
                if include_warn else ERROR_RE)
    unknown: dict[str, dict] = {}
    scanned = 0
    for ln in lines:
        if not level_re.search(ln):
            continue
        scanned += 1
        if _known_labeled(ln):
            continue
        sig = normalize_signature(ln)
        e = unknown.setdefault(sig, {"count": 0, "sample": ln[:300]})
        e["count"] += 1
    return {"scanned": scanned, "unknown": unknown}


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
    ap.add_argument("--unknown-errors", choices=["fail", "warn", "off"],
                    default="warn",
                    help="G13b inverted gate: how to treat ERROR/WARN log lines "
                         "not matched by the ALLOWLIST or a known blocking/"
                         "self-heal pattern. 'fail' => any unknown = FAIL; "
                         "'warn' (default) => WATCH (loud, non-breaking); "
                         "'off' => skip. Known CONNECTION_FATAL signatures still "
                         "FAIL hard via HARD_BLOCKING regardless of this flag.")
    ap.add_argument("--unknown-include-warn", action="store_true",
                    help="G13b: also scan WARN-level lines for unknowns "
                         "(default: ERROR-level only)")
    ap.add_argument("--resources", default=None,
                    help="path to a resource_sampler .summary.json — "
                         "folds WAL growth, WAL/DB ratio, and PG slot lag "
                         "alerts into this gate")
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
            baseline_doc = json.load(open(SIG_BASELINE_PATH))
            if baseline_doc.get("schema") == SIG_SCHEMA:
                baseline = set(baseline_doc.get("signatures", []))
        except Exception:
            pass
    all_sigs = set()
    for name in [c.strip() for c in a.containers.split(",") if c.strip()]:
        all_sigs |= scan_unknown_signatures(name, since)
    unknown = all_sigs - baseline
    if a.update_baseline:
        os.makedirs(os.path.dirname(SIG_BASELINE_PATH), exist_ok=True)
        with open(SIG_BASELINE_PATH, "w") as f:
            json.dump({"schema": SIG_SCHEMA,
                       "signatures": sorted(all_sigs | baseline)}, f, indent=2)
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

    # --- G13b inverted gate: unrecognized ERROR/WARN => FAIL/WATCH. Additive;
    #     the HARD_BLOCKING/SELF_HEAL/WATCH behavior above is untouched. ---
    unknown_errors: dict[str, dict] = {}
    ue_scanned = 0
    if a.unknown_errors != "off":
        for name in [c.strip() for c in a.containers.split(",") if c.strip()]:
            ue = scan_unknown_errors(name, since, a.unknown_include_warn)
            if "scan_error" in ue:
                continue  # scan_container already raised ERROR for this container
            ue_scanned += ue["scanned"]
            for sig, e in ue["unknown"].items():
                agg = unknown_errors.setdefault(sig, {"count": 0, "sample": e["sample"]})
                agg["count"] += e["count"]
        if unknown_errors:
            distinct = len(unknown_errors)
            total = sum(e["count"] for e in unknown_errors.values())
            top = sorted(unknown_errors.items(), key=lambda kv: -kv[1]["count"])
            summary = "; ".join(f"{e['count']}x {sig}" for sig, e in top[:3])
            verdict = "FAIL" if a.unknown_errors == "fail" else "WATCH"
            raise_to(verdict)
            report["details"].append(
                f"unrecognized-server-error [G13b]: {distinct} distinct new "
                f"ERROR{'/WARN' if a.unknown_include_warn else ''} signature(s) "
                f"({total}x total) not in allowlist/blocklist — {summary}")
    report["unknown_errors"] = {
        "mode": a.unknown_errors,
        "include_warn": a.unknown_include_warn,
        "level_lines_scanned": ue_scanned,
        "distinct": len(unknown_errors),
        "total": sum(e["count"] for e in unknown_errors.values()),
        "signatures": [{"signature": sig, "count": e["count"], "sample": e["sample"]}
                       for sig, e in sorted(unknown_errors.items(),
                                            key=lambda kv: -kv[1]["count"])],
    }

    # --- WAL / slot-lag alerts from the resource sampler ---
    if a.resources and os.path.exists(a.resources):
        try:
            rs = json.load(open(a.resources))
            report["resource_alerts"] = {}
            if rs.get("wal_growth_alert"):
                a_info = rs["wal_growth_alert"]
                raise_to("WATCH")
                report["details"].append(f"wal-growth: {a_info['note']}")
                report["resource_alerts"]["wal_growth"] = a_info
            if rs.get("wal_ratio_alert"):
                a_info = rs["wal_ratio_alert"]
                raise_to("WATCH")
                report["details"].append(f"wal-ratio: {a_info['note']}")
                report["resource_alerts"]["wal_ratio"] = a_info
            if rs.get("pg_lag_alert"):
                a_info = rs["pg_lag_alert"]
                raise_to("WATCH")
                report["details"].append(f"pg-slot-lag: {a_info['note']}")
                report["resource_alerts"]["pg_slot_lag"] = a_info
            if rs.get("orphaned_slots"):
                o_info = rs["orphaned_slots"]
                raise_to("FAIL")
                report["details"].append(f"orphaned-slots: {o_info['note']}")
                report["resource_alerts"]["orphaned_slots"] = o_info
            if rs.get("ckpt_starvation_alert"):
                c_info = rs["ckpt_starvation_alert"]
                raise_to("FAIL")
                report["details"].append(f"ckpt-starvation: {c_info['note']}")
                report["resource_alerts"]["ckpt_starvation"] = c_info
        except Exception as e:
            print(f"WARN: cannot load resource summary {a.resources}: {e}", file=sys.stderr)

    if a.out:
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
    print(f"log gate: {report['verdict']}"
          + (f" — {'; '.join(report['details'][:4])}" if report["details"] else ""))
    return 0 if report["verdict"] != "ERROR" else 2


if __name__ == "__main__":
    raise SystemExit(main())
