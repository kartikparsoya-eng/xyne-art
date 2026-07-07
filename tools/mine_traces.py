#!/usr/bin/env python3
"""
mine_traces.py — TRACE mining: exact per-session event streams from PROD logs
(Victoria Logs, needs GR_KEY + VPN). This is the "user recordings through logs"
layer: where the statistical profile (mine_session_profile.py) captures
DISTRIBUTIONS, this captures the actual SEQUENCES — which client desired which
query with which args at which millisecond, in what order, interleaved how.

What each prod event contributes (verified live 2026-07-07):
  zero_query_called      query name + FULL nested args (parsed from _msg,
                         which carries the original client JSON — richer than
                         the flattened args.* fields), clientSessionId,
                         zeroClientGroupId, emailId, platform, _time (ms)
  zero_run_called        one-shot queries (same fields)
  zero_mutation_called   mutation NAME + timing — prod does NOT log mutation
                         args, so replay preserves type+timing and synthesizes
                         args (documented per-event as args:null)
  zero_socket_connected/  session lifecycle anchors: connectionAttempts,
  zero_socket_disconnected disconnect reason (tab-hide vs network-loss)

Trace format (one NDJSON line per session, events time-ordered):
  {"sid":..., "cgid":..., "user":..., "platform":..., "offset_ms":...,
   "events":[{"dt":<ms since session t0>, "kind":"query|run|mutation|connect|
              disconnect", "name":..., "args":{...}|null, "meta":{...}}]}
Line 1 is a header: {"_trace_header":1, window, counts, mined_at}.

IDs stay RAW prod ids in the trace file (they're pseudonymous cuids); the
mapping onto sandbox entities happens at REPLAY-load time (replay.py --trace)
so one trace file replays against any environment with its own id-pool.
Trace files land in raw/traces/ (gitignored like all of raw/).

    export GR_KEY='glsa_...'    # on VPN
    python3 tools/mine_traces.py --window 60m           # last hour
    python3 tools/mine_traces.py --start 2026-07-04T09:00:00Z --minutes 60
    -> raw/traces/trace-<window-tag>.ndjson

Guardrails: per-event-type row cap (--limit, default 200k) so a fat window
can't OOM the pull; sessions with fewer than --min-events events are dropped
(connect-only noise).
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
LOGS = BASE + "/api/datasources/proxy/8/select/logsql/query"
RAW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "raw")

EVENT_KINDS = {
    "zero_query_called": "query",
    "zero_run_called": "run",
    "zero_mutation_called": "mutation",
    "zero_socket_connected": "connect",
    "zero_socket_disconnected": "disconnect",
}


def logsql_stream(key: str, query: str, timeout: int = 300):
    data = urllib.parse.urlencode({"query": query}).encode()
    req = urllib.request.Request(LOGS, data=data,
                                 headers={"Authorization": "Bearer " + key})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        for line in resp:
            line = line.strip()
            if line:
                yield json.loads(line)


def parse_ts_ms(iso: str) -> int:
    """'2026-07-07T06:10:16.343Z' -> epoch ms (UTC)."""
    base, _, frac = iso.rstrip("Z").partition(".")
    t = time.strptime(base, "%Y-%m-%dT%H:%M:%S")
    ms = int((frac + "000")[:3]) if frac else 0
    return (int(time.mktime(t)) - time.timezone) * 1000 + ms


def extract(row: dict) -> dict | None:
    """One log row -> one trace event. Prefers the _msg JSON (original client
    payload with NESTED args) over the lossy flattened args.* fields."""
    ev = row.get("event")
    kind = EVENT_KINDS.get(ev)
    if kind is None:
        return None
    try:
        msg = json.loads(row.get("_msg") or "{}")
    except Exception:
        msg = {}
    sid = msg.get("clientSessionId") or row.get("clientSessionId")
    if not sid:
        return None
    name = msg.get("query") or msg.get("mutation") or row.get("query") \
        or row.get("mutation")
    if kind in ("query", "run", "mutation") and not name:
        return None
    out = {
        "ts": parse_ts_ms(msg.get("timestamp") or row.get("_time", "")),
        "sid": sid,
        "cgid": msg.get("zeroClientGroupId") or row.get("zeroClientGroupId") or "",
        "user": msg.get("emailId") or row.get("emailId") or "",
        "platform": msg.get("platformName") or row.get("platformName") or "",
        "kind": kind,
        "name": name,
        # mutations: prod logs NO args -> null (replay synthesizes);
        # queries/runs: the args object verbatim ({} = explicit no-arg call)
        "args": (msg.get("args") if kind in ("query", "run")
                 else None),
    }
    if kind == "connect":
        out["meta"] = {"attempts": msg.get("connectionAttempts")}
    elif kind == "disconnect":
        out["meta"] = {"reason": (msg.get("reason") or "")[:60],
                       "duration_ms": msg.get("sessionDurationMs")}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", default="60m",
                    help="LogsQL relative window, e.g. 60m / 6h (default 60m); "
                         "ignored when --start is given")
    ap.add_argument("--start", default=None,
                    help="absolute window start, ISO8601 Z (with --minutes)")
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--limit", type=int, default=200_000,
                    help="per-event-type row cap (default 200k)")
    ap.add_argument("--min-events", type=int, default=3,
                    help="drop sessions with fewer events (default 3)")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    key = os.environ.get("GR_KEY")
    if not key:
        print("ERROR: export GR_KEY (Grafana token, on VPN)", file=sys.stderr)
        return 2

    if a.start:
        end_s = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(parse_ts_ms(a.start) / 1000 + a.minutes * 60))
        trange = f"_time:[{a.start}, {end_s})"
        tag = a.start.replace(":", "").replace("-", "")[:13] + f"-{a.minutes}m"
    else:
        trange = f"_time:{a.window}"
        tag = "last" + a.window

    sessions: dict[str, dict] = {}
    counts: dict[str, int] = {}
    for ev, kind in EVENT_KINDS.items():
        q = f'{trange} event:="{ev}" | limit {a.limit}'
        n = 0
        try:
            for row in logsql_stream(key, q):
                e = extract(row)
                if e is None:
                    continue
                n += 1
                s = sessions.setdefault(e["sid"], {
                    "sid": e["sid"], "cgid": e["cgid"], "user": e["user"],
                    "platform": e["platform"], "events": []})
                # first non-empty wins (some events omit cgid/user)
                for f in ("cgid", "user", "platform"):
                    if not s[f] and e[f]:
                        s[f] = e[f]
                s["events"].append(
                    {"ts": e["ts"], "kind": kind, "name": e["name"],
                     "args": e["args"],
                     **({"meta": e["meta"]} if "meta" in e else {})})
        except Exception as exc:
            print(f"WARN: pull failed for {ev}: {exc}", file=sys.stderr)
        counts[ev] = n
        print(f"  {ev:28} {n:7} events")
        if n >= a.limit:
            print(f"  WARN: {ev} hit --limit {a.limit} — window truncated, "
                  f"shrink the window for a complete trace", file=sys.stderr)

    # order events inside each session; drop connect-only noise sessions
    kept = []
    t_min = None
    for s in sessions.values():
        s["events"].sort(key=lambda e: e["ts"])
        actions = sum(1 for e in s["events"]
                      if e["kind"] in ("query", "run", "mutation"))
        if len(s["events"]) < a.min_events or actions == 0:
            continue
        t0 = s["events"][0]["ts"]
        t_min = t0 if t_min is None else min(t_min, t0)
        kept.append(s)
    for s in kept:
        t0 = s["events"][0]["ts"]
        s["t0"] = t0
        for e in s["events"]:
            e["dt"] = e.pop("ts") - t0
    kept.sort(key=lambda s: s["t0"])
    for s in kept:
        s["offset_ms"] = s.pop("t0") - t_min

    os.makedirs(os.path.join(RAW, "traces"), exist_ok=True)
    out = a.out or os.path.join(RAW, "traces", f"trace-{tag}.ndjson")
    n_events = sum(len(s["events"]) for s in kept)
    header = {
        "_trace_header": 1,
        "mined_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "window": trange,
        "sessions": len(kept),
        "sessions_dropped": len(sessions) - len(kept),
        "events": n_events,
        "event_counts": counts,
        "distinct_users": len({s["user"] for s in kept if s["user"]}),
        "distinct_cgids": len({s["cgid"] for s in kept if s["cgid"]}),
        "span_ms": max((s["offset_ms"] + s["events"][-1]["dt"]
                        for s in kept), default=0),
        "note": "raw prod ids preserved; mapping to sandbox entities happens "
                "at replay load (replay.py --trace). Mutation args are NOT "
                "logged by prod (args:null) — replay synthesizes them.",
    }
    with open(out, "w") as f:
        f.write(json.dumps(header) + "\n")
        for s in kept:
            f.write(json.dumps(s, separators=(",", ":")) + "\n")
    print(f"\n-> {out}")
    print(f"  {len(kept)} sessions ({header['sessions_dropped']} dropped), "
          f"{n_events} events, {header['distinct_users']} users, "
          f"{header['distinct_cgids']} cgids, "
          f"span {header['span_ms'] / 60000:.1f} min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
