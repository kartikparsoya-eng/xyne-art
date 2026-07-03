#!/usr/bin/env python3
"""
mine_session_profile.py — Layer-B PROD mining: session/behavior DISTRIBUTIONS
from Victoria Logs (needs GR_KEY + VPN). Complements the Layer-A aggregates in
art-baseline.json with the zero-SCOPED per-client behavior that derive_prod_
profile.py refines its knobs from.

What it mines (all zero-socket-scoped, not notification-socket):
  * zero_socket_connected 7d count      -> Little's-law mean session (the
    baseline's websocket_connection_successful mixes in the notification WS)
  * zero_socket_disconnected by reason  -> observable-end abrupt ratio
    (network-loss giveups vs clean tab-hide closes)
  * tab-hide sessionDurationMs quantiles-> short-session subpopulation shape
  * connectionAttempts quantiles        -> client retry/backoff behavior
  * per-cgid connects/day               -> reconnect cadence (resume pressure)
  * per-cgid + per-session distinct query types/day -> working-set anchor

    export GR_KEY='glsa_...'   # on VPN
    python3 tools/mine_session_profile.py          # writes raw/session_profile_7d.json
    python3 tools/derive_prod_profile.py           # then re-derive the profile

Output feeds derive_prod_profile.py automatically (it merges this file when
present); keep both provenance blocks so knob origins stay auditable.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request

BASE = "https://grafana.spaces.xyne.juspay.net"
LOGS = BASE + "/api/datasources/proxy/8/select/logsql/query"
RAW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "raw")


def logsql(key: str, query: str, timeout: int = 120) -> list[dict]:
    data = urllib.parse.urlencode({"query": query}).encode()
    req = urllib.request.Request(LOGS, data=data,
                                 headers={"Authorization": "Bearer " + key})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        out = []
        for line in resp.read().decode().splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
        return out


def one(rows: list[dict]) -> dict:
    return rows[0] if rows else {}


def local_quantiles(rows: list[dict], field: str, key_field: str) -> dict:
    vals = sorted(int(r[field]) for r in rows if r.get(key_field))
    n = len(vals)
    if not n:
        return {}
    q = lambda p: vals[min(n - 1, int(p * n))]  # noqa: E731
    return {"n": n, "p25": q(.25), "p50": q(.5), "p75": q(.75),
            "p90": q(.9), "p99": q(.99), "max": vals[-1],
            "mean": round(sum(vals) / n, 1)}


def main() -> int:
    key = os.environ.get("GR_KEY")
    if not key:
        print("ERROR: export GR_KEY (Grafana token, on VPN)", file=sys.stderr)
        return 2

    print("mining zero-scoped session behavior (7d window)...")

    conn = one(logsql(key, '_time:7d event:="zero_socket_connected" '
                           '| stats count() n, count_uniq(zeroClientGroupId) cgids'))
    attempts = one(logsql(key, '_time:7d event:="zero_socket_connected" '
                               '| stats quantile(0.5, connectionAttempts) p50, '
                               'quantile(0.9, connectionAttempts) p90, '
                               'quantile(0.99, connectionAttempts) p99, '
                               'max(connectionAttempts) mx'))
    reasons = logsql(key, '_time:7d event:="zero_socket_disconnected" '
                          '| stats by (reason) count() n')
    tabhide = one(logsql(key, '_time:7d event:="zero_socket_disconnected" '
                              'sessionDurationMs:>0 '
                              '| stats quantile(0.25, sessionDurationMs) p25, '
                              'quantile(0.5, sessionDurationMs) p50, '
                              'quantile(0.75, sessionDurationMs) p75, '
                              'quantile(0.95, sessionDurationMs) p95, '
                              'quantile(0.99, sessionDurationMs) p99, '
                              'avg(sessionDurationMs) mean, count() n'))
    conns_per_cgid = logsql(key, '_time:1d event:="zero_socket_connected" '
                                 '| stats by (zeroClientGroupId) count() n')
    ws_cgid = logsql(key, '_time:1d event:="zero_query_called" '
                          '| stats by (zeroClientGroupId) count_uniq(query) ws')
    ws_sess = logsql(key, '_time:1d event:="zero_query_called" '
                          '| stats by (clientSessionId) count_uniq(query) ws')

    reason_counts = {r.get("reason", "?"): int(r["n"]) for r in reasons}
    giveups = sum(v for k, v in reason_counts.items() if "unable to connect" in k)
    tab_hides = sum(v for k, v in reason_counts.items() if "tab was hidden" in k)
    observable = sum(reason_counts.values())

    profile = {
        "mined_at": time.strftime("%Y-%m-%d"),
        "window": "7d (per-cgid distributions: 1d)",
        "source": "Victoria Logs (ds 8), zero_socket_* / zero_query_called events",
        "zero_scoped_connects_7d": int(conn.get("n", 0)),
        "distinct_cgids_7d": int(conn.get("cgids", 0)),
        "connection_attempts_to_success": {
            k: float(v) for k, v in attempts.items() if v is not None},
        "disconnect_reasons_7d": reason_counts,
        "observable_end_split": {
            "network_loss_giveups": giveups,
            "clean_tab_hides": tab_hides,
            "abrupt_ratio_of_observable": round(giveups / observable, 3)
            if observable else None,
            "caveat": "most session ends emit NO disconnect event (app quit, "
                      "device sleep) — those are also abrupt (no close frame), "
                      "so this ratio is a lower-ish bound on true abruptness",
        },
        "tab_hide_session_duration_ms": {
            k: float(v) for k, v in tabhide.items() if v is not None},
        "conns_per_cgid_per_day": local_quantiles(conns_per_cgid, "n",
                                                  "zeroClientGroupId"),
        "distinct_queries_per_cgid_per_day": local_quantiles(ws_cgid, "ws",
                                                             "zeroClientGroupId"),
        "distinct_queries_per_client_session": local_quantiles(ws_sess, "ws",
                                                               "clientSessionId"),
    }

    os.makedirs(RAW, exist_ok=True)
    out = os.path.join(RAW, "session_profile_7d.json")
    with open(out, "w") as f:
        json.dump(profile, f, indent=2)
    print(f"-> {out}")
    print(f"  zero connects 7d      : {profile['zero_scoped_connects_7d']} "
          f"across {profile['distinct_cgids_7d']} cgids")
    print(f"  observable-end abrupt : {profile['observable_end_split']['abrupt_ratio_of_observable']:.0%} "
          f"({giveups} giveups vs {tab_hides} tab-hides)")
    print(f"  conns/cgid/day        : {profile['conns_per_cgid_per_day']}")
    print(f"  working set (cgid/day): {profile['distinct_queries_per_cgid_per_day']}")
    print("\nnow re-derive the replay profile:\n  python3 tools/derive_prod_profile.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
