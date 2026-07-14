"""Assemble the full-breadth ART baseline from raw telemetry pulls in raw/.

Counts/weights  : 7d (168h)
Client latency  : 72h (the 7d quantile aggregation over ~3.5M events overloads the
                  log backend; 72h over ~1.5M events is the widest that returns).
Server hist     : 7d increase (ivm_advance: 1h rate fallback — 7d increase returns null).
"""
import json
import os

RAW = os.path.join(os.path.dirname(__file__), "..", "raw")
OUT = os.path.join(os.path.dirname(__file__), "..", "art-baseline.json")


def load_ndjson(name):
    rows = []
    with open(os.path.join(RAW, name)) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# ---- load raw ----
q_counts = {r["query"]: int(r["calls"]) for r in load_ndjson("queries_7d_counts.ndjson")}
q_stats = {r["query"]: r for r in load_ndjson("queries_72h_stats.ndjson")}
oneshot = {r["query"]: r for r in load_ndjson("oneshot_7d.ndjson")}
muts = load_ndjson("mutations_7d.ndjson")
events = {r["event"]: int(r["n"]) for r in load_ndjson("events_7d.ndjson")}
platform = load_ndjson("platform_7d.ndjson")
arg_schema = json.load(open(os.path.join(RAW, "arg_schemas.json")))["schema"]
srv = json.load(open(os.path.join(RAW, "server_hist_7d.json")))
backend = json.load(open(os.path.join(RAW, "backend_levels_7d.json")))

IVM_1H = {"p50_s": 0.0005465104857905532, "p95_s": 0.003043694608804097, "p99_s": 0.009192803414939239}
srv["zero_sync_ivm_advance_time_seconds"] = IVM_1H

# ---- scale ----
total_q = sum(q_counts.values())
total_m = sum(int(m["calls"]) for m in muts)
total_os = sum(int(o["calls"]) for o in oneshot.values())

# ---- server baselines (ms) ----
def srv_ms(metric):
    s = srv[metric]
    p50 = round(s["p50_s"] * 1000, 2) if s["p50_s"] is not None else None
    p95 = round(s["p95_s"] * 1000, 2) if s["p95_s"] is not None else None
    p99 = round(s["p99_s"] * 1000, 2) if s["p99_s"] is not None else None
    out = {"p50": p50, "p95": p95, "p99": p99}
    if p95 is not None:
        out["pass_p95"] = round(p95 * 1.20, 2)
    if p99 is not None:
        out["pass_p99"] = round(p99 * 1.25, 2)
    return out

server_baselines = {
    "_comment": "PRIMARY regression signal. zero-cache engine histograms. window=7d increase "
                "(ivm_advance=1h rate). A major change must not push p95/p99 past pass_p95/pass_p99.",
    "zero_sync_hydration_time": srv_ms("zero_sync_hydration_time_seconds"),
    "zero_sync_advance_time": srv_ms("zero_sync_advance_time_seconds"),
    "zero_sync_ivm_advance_time": srv_ms("zero_sync_ivm_advance_time_seconds"),
    "zero_sync_poke_time": srv_ms("zero_sync_poke_time_seconds"),
    "zero_sync_cvr_flush_time": srv_ms("zero_sync_cvr_flush_time_seconds"),
    "zero_sync_query_transformation_time": srv_ms("zero_sync_query_transformation_time_seconds"),
    "_pass_margin": "pass_p95 = p95*1.20 ; pass_p99 = p99*1.25",
}

# ---- health gates ----
def g(n):
    return events.get(n, 0)

api_ok, api_fail = g("api_call_successful"), g("api_call_failed")
be_total = backend["info"] + backend["warn"] + backend["error"]
health = {
    "_comment": "Derived rates over 7d that the ART must not let regress.",
    "query_completion_rate": {"value": round(g("zero_query_complete") / g("zero_query_called"), 4),
                               "num": "zero_query_complete", "den": "zero_query_called", "min_pass": 0.86},
    "run_completion_rate": {"value": round(g("zero_run_complete") / g("zero_run_called"), 4),
                             "num": "zero_run_complete", "den": "zero_run_called", "min_pass": 0.88},
    "mutation_completion_rate": {"value": round(g("zero_mutation_complete") / g("zero_mutation_called"), 4),
                                  "num": "zero_mutation_complete", "den": "zero_mutation_called", "min_pass": 0.92},
    "mutation_error_rate": {"value": round(g("zero_mutation_error") / g("zero_mutation_called"), 4),
                             "num": "zero_mutation_error", "den": "zero_mutation_called", "max_pass": 0.08},
    "api_success_rate": {"value": round(api_ok / (api_ok + api_fail), 4),
                          "num": "api_call_successful", "den": "api_call_successful+api_call_failed", "min_pass": 0.95},
    "backend_log_error_rate": {"value": round(backend["error"] / be_total, 5),
                                "num": "backend level:error", "den": "all backend log lines (7d)", "max_pass": 0.01},
    "socket_failure_ratio_watch": {"value": round(g("websocket_connection_failed") / g("websocket_connection_successful"), 2),
                                    "definition": "websocket_connection_failed / websocket_connection_successful",
                                    "note": "Retry-inflated; WATCH not hard-fail. Alert if it worsens >30% vs baseline."},
}

# ---- event volume (curated) ----
vol_keys = ["api_call_successful", "zero_query_called", "zero_query_complete", "zero_run_called",
            "zero_run_complete", "zero_mutation_called", "zero_mutation_complete", "zero_mutation_error",
            "websocket_connection_failed", "websocket_connection_successful", "websocket_connection_closed",
            "zero_socket_disconnected", "frontend_error", "api_call_failed", "conversation_prefetch_error"]
event_volume = {k: g(k) for k in vol_keys}

# ---- platform split ----
plat = {}
for p in platform:
    plat[p["platformName"]] = {"users": int(p["users"]), "cgids": int(p["cgids"]), "events": int(p["events"])}
primary = max(plat.items(), key=lambda kv: kv[1]["events"])[0]

# ---- query workload (ALL 151) ----
AUTO_FIRED = {"activities.markThreadActivitiesAsReadV2", "channel.markChannelAsViewed"}
CRITICAL_UX = {"conversations.send", "messages.send", "messages.react", "messages.update", "messages.delete"}

def qrow(name):
    calls = q_counts[name]
    st = q_stats.get(name)
    row = {
        "name": name,
        "calls_7d": calls,
        "weight_pct": round(calls / total_q * 100, 4),
        "args": arg_schema.get(name, []),
    }
    if st:
        p95 = f(st["p95"])
        p99 = f(st["p99"])
        row["p50_ms"] = round(f(st["p50"]), 1)
        row["p95_ms"] = round(p95, 1)
        row["p99_ms"] = round(p99, 1)
        row["max_ms"] = round(f(st["max"]), 1)
        if (p95 or 0) > 5000 or (p99 or 0) > 30000:
            row["watch"] = True
    else:
        row["latency_72h"] = None
    if name in oneshot:
        row["also_oneshot_calls_7d"] = int(oneshot[name]["calls"])
    return row

queries = [qrow(n) for n in sorted(q_counts, key=lambda n: -q_counts[n])]

# coverage: how many queries to reach 80% / 95%
cum = 0
c80 = c95 = None
for i, r in enumerate(queries, 1):
    cum += r["weight_pct"]
    if c80 is None and cum >= 80:
        c80 = i
    if c95 is None and cum >= 95:
        c95 = i
        break

# ---- one-shot workload (ALL) ----
oneshot_rows = []
for name in sorted(oneshot, key=lambda n: -int(oneshot[n]["calls"])):
    o = oneshot[name]
    def r1(v):
        return round(v, 1) if v is not None else None
    oneshot_rows.append({
        "name": name, "calls_7d": int(o["calls"]),
        "p50_ms": r1(f(o.get("p50"))),
        "p95_ms": r1(f(o.get("p95"))),
        "p99_ms": r1(f(o.get("p99"))),
    })

# ---- mutation workload (ALL 151) ----
mut_rows = []
for m in sorted(muts, key=lambda m: -int(m["calls"])):
    name = m["mutation"]
    calls = int(m["calls"])
    p95 = f(m["p95"])
    p99 = f(m["p99"])
    r = {"name": name, "calls_7d": calls, "weight_pct": round(calls / total_m * 100, 4),
         "p50_ms": round(f(m["p50"]), 1), "p95_ms": round(p95, 1),
         "p99_ms": round(p99, 1), "max_ms": round(f(m["max"]), 1)}
    if name in AUTO_FIRED:
        r["auto_fired"] = True
    if name in CRITICAL_UX:
        r["critical_ux"] = True
    if (p95 or 0) > 20000 or (p99 or 0) > 60000:
        r["watch"] = True
    mut_rows.append(r)

# ---- watchlist (computed) ----
impact = sorted(
    [q for q in queries if q.get("p95_ms") and q["calls_7d"] >= 1000],
    key=lambda q: -(q["calls_7d"] * q["p95_ms"]))[:12]
watch_tail = [f'{q["name"]} (calls_7d={q["calls_7d"]}, p95={q["p95_ms"]}ms, p99={q["p99_ms"]}ms)'
              for q in impact]

baseline = {
    "art_version": "2.0.0",
    "name": "Xyne Spaces / zero-cache Application Regression Test baseline (full breadth, 7-day)",
    "description": "Production-derived workload + SLO baseline covering EVERY query, mutation, and "
                   "one-shot observed in 7 days of PROD telemetry. Replay this workload (or shadow-compare "
                   "against it) when validating major changes to zero-cache, the ZQL/IVM query engine, the "
                   "Zero schema, or the backend; gate on the server SLOs and health thresholds.",
    "provenance": {
        "source": "Grafana / Victoria Logs (ds 8) + VictoriaMetrics (ds 7) @ grafana.spaces.xyne.juspay.net",
        "captured_at": "2026-07-02",
        "window_counts": "7d (168h) — full breadth + weekly weights",
        "window_client_latency": "72h — 7d quantile aggregation over ~3.5M events overloads the log backend",
        "window_server_histograms": "7d increase (ivm_advance: 1h rate fallback)",
        "prod_app_version": "1.181.0-release-20260630.2",
        "retention_note": "7 full days of logs are present. Weekly query volume (2.16M) < 24h*7 because of the "
                          "weekend dip (~308k/weekend-day vs ~506k/weekday).",
        "containers": ["xyne-logging-bridge", "xyne-spaces-zero", "xyne-backend"],
    },
    "scale_7d": {
        "distinct_users": 1544,
        "distinct_client_group_ids": 6206,
        "active_zero_clients_avg": 668,
        "active_zero_clients_peak": 1147,
        "platform_split_by_query_events": plat,
        "primary_platform": primary,
    },
    "event_volume_7d": event_volume,
    "health_gates": health,
    "server_baselines_ms": server_baselines,
    "query_workload": {
        "_comment": "ALL reactive query types (zero_query_complete). weight_pct = calls_7d / total. "
                    "args = full argument schema. p*_ms are 72h and CONTAMINATED by backgrounded tabs / "
                    "socket reconnects (see caveats) — use p50 as the robust client signal; gate engine "
                    "regressions on server_baselines_ms. 'watch':true = p95>5s or p99>30s.",
        "window_counts": "7d", "window_latency": "72h",
        "total_reactive_calls_7d": total_q,
        "distinct_query_types": len(queries),
        "coverage": {"queries_for_80pct": c80, "queries_for_95pct": c95},
        "queries": queries,
    },
    "oneshot_workload": {
        "_comment": "One-shot .run() queries (zero_run_complete). Fired imperatively, not reactive.",
        "total_calls_7d": total_os, "distinct_types": len(oneshot_rows), "queries": oneshot_rows,
    },
    "mutation_workload": {
        "_comment": "ALL mutation types (zero_mutation_complete; fields mutation + duration ms). "
                    "auto_fired = read-tracking fired by the client automatically. critical_ux = user-"
                    "perceived writes. 'watch':true = p95>20s or p99>60s.",
        "window_counts": "7d",
        "total_calls_7d": total_m, "distinct_mutation_types": len(mut_rows), "mutations": mut_rows,
    },
    "regression_watchlist": {
        "_comment": "Highest-value assertions. A good change should improve these; none should worsen them.",
        "highest_impact_client_tail": watch_tail,
        "suspicious_identical_maxima": "Many queries share identical max latencies (~1,437,0xx ms / "
            "~34,100,5xx ms / ~66,101,0xx ms). Recur across unrelated queries => backgrounded/suspended "
            "tabs resolving on refocus, not compute. Exclude client max/p99 from hard gates.",
        "socket_instability": f"websocket_connection_failed ({g('websocket_connection_failed')}/7d) vs "
            f"successful ({g('websocket_connection_successful')}/7d). Root cause of most client-perceived "
            "query tail. Any change to the socket/reconnect path must move this ratio DOWN.",
    },
    "caveats": [
        "Client-perceived latency (zero_query_complete.latency) != server engine time. The zero-cache engine "
        "is HEALTHY (7d: hydration p99 ~5.4s, advance p99 ~42ms, poke p99 ~1.6s). Multi-second/minute client "
        "tails are dominated by socket reconnects and backgrounded tabs.",
        "Gate engine regressions on server_baselines_ms (PRIMARY) and health_gates. Client p50 = robust user "
        "signal; client p95/p99 = directional WATCH only.",
        "Client latency percentiles are 72h (not 7d): the 7d quantile aggregation over ~3.5M query events "
        "overloads the Victoria Logs backend and returns empty. Counts/weights ARE true 7d.",
        "ivm_advance server histogram is a 1h rate sample (increase[7d] returns null for that series).",
        "'skewed:true' on many client events = client/server clock skew; absolute latency on skewed samples "
        "is not authoritative.",
        "Refresh weekly and after each release; recompute pass_* margins if the traffic mix shifts.",
    ],
}

with open(OUT, "w") as fh:
    json.dump(baseline, fh, indent=2)

print("wrote", os.path.relpath(OUT))
print("queries:", len(queries), "| mutations:", len(mut_rows), "| oneshots:", len(oneshot_rows))
print("total_q_7d:", total_q, "| total_m_7d:", total_m, "| total_os_7d:", total_os)
print("coverage: %d queries = 80%%, %d queries = 95%%" % (c80, c95))
print("health:", {k: v.get("value") for k, v in health.items() if isinstance(v, dict) and "value" in v})
