#!/usr/bin/env python3
"""
derive_prod_profile.py — turn the PROD-mined art-baseline.json into a replay
BEHAVIOR PROFILE (profiles/prod-7d.json) that replay.py can load via --profile.

Why: art-baseline.json nails WHAT clients ask for (151-query mix, weights,
mutation mix) but replay's behavioral knobs (churn cadence, session length,
mutation rate, abrupt-close ratio) were invented defaults. The same 7d
telemetry pins them:

  churn/client       = zero_query_called / window / avg_active_clients
  mutations/client   = zero_mutation_called / window / avg_active_clients
  mean session       = avg_active_clients * window / websocket_connection_successful
                       (Little's law: L = lambda * W  =>  W = L / lambda)
  abrupt-close ratio = zero_socket_disconnected / websocket_connection_closed

The default replay profile is a deliberate TORTURE TEST (~250x prod churn);
this profile is the REALISM instrument — use it for capacity questions
("can the candidate hold N prod-shaped clients?") and prod-cadence soaks,
not for fast leak hunting.

Layer B: if raw/session_profile_7d.json exists (tools/mine_session_profile.py,
needs GR_KEY + VPN), the zero-SCOPED numbers there override the Layer-A
approximations — session length from zero_socket_connected (not the
notification-socket websocket_* events), abrupt ratio from actual disconnect
reasons, working set from distinct query types per cgid.

    python3 tools/derive_prod_profile.py            # writes profiles/prod-7d.json
    python3 tools/derive_prod_profile.py --scale 5  # 5x prod per-client rates
"""
from __future__ import annotations

import argparse
import json
import os

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", default=os.path.join(ROOT, "art-baseline.json"))
    ap.add_argument("--out", default=os.path.join(ROOT, "profiles", "prod-7d.json"))
    ap.add_argument("--scale", type=float, default=1.0,
                    help="multiply per-client activity rates (churn, mutations) "
                         "by this factor — e.g. 5 = '5x prod intensity'")
    a = ap.parse_args()

    with open(a.baseline) as f:
        bl = json.load(f)
    sc, ev = bl["scale_7d"], bl["event_volume_7d"]
    window_s = 7 * 24 * 3600.0
    avg = sc["active_zero_clients_avg"]

    q_calls = bl["query_workload"]["total_reactive_calls_7d"]
    m_calls = ev["zero_mutation_called"]
    conns = ev["websocket_connection_successful"]
    closed = ev["websocket_connection_closed"]
    abrupt = ev["zero_socket_disconnected"]

    churn_per_client_s = window_s * avg / q_calls          # s between query swaps
    mut_per_min = m_calls / (window_s / 60.0) / avg
    mean_session_s = avg * window_s / conns                 # Little's law
    abrupt_pct = abrupt / closed
    working_set = 12
    resume_pct = 0.70
    layer_b: dict = {}

    # ---- Layer B refinement (zero-scoped mined distributions) ----
    sess_path = os.path.join(ROOT, "raw", "session_profile_7d.json")
    if os.path.exists(sess_path):
        with open(sess_path) as f:
            layer_b = json.load(f)
        zconns = layer_b.get("zero_scoped_connects_7d")
        if zconns:
            # zero_socket_connected only — the Layer-A number mixed in the
            # notification websocket's connect events
            mean_session_s = avg * window_s / zconns
        ab = (layer_b.get("observable_end_split") or {}).get("abrupt_ratio_of_observable")
        if ab is not None:
            # of OBSERVABLE zero-socket ends: network-loss giveups vs clean
            # tab-hides. Unobservable ends (app quit/sleep) are also abrupt,
            # so this is if anything an underestimate.
            abrupt_pct = ab
        ws50 = (layer_b.get("distinct_queries_per_cgid_per_day") or {}).get("p50")
        if ws50:
            # distinct query TYPES touched per cgid per day — an upper-ish
            # anchor for the simultaneously-held working set
            working_set = int(ws50)
        cp50 = (layer_b.get("conns_per_cgid_per_day") or {}).get("p50")
        if cp50 and cp50 > 1:
            # p50 cgid reconnects ~20x/day; every reconnect with a stored
            # cookie attempts resume — cap at 0.9 (fresh logins/storage
            # clears exist)
            resume_pct = round(min(0.9, (cp50 - 1) / cp50), 2)

    profile = {
        "name": f"prod-7d{'' if a.scale == 1.0 else f'-x{a.scale:g}'}",
        "derived_from": {
            "baseline": os.path.basename(a.baseline),
            "captured_at": bl["provenance"]["captured_at"],
            "prod_app_version": bl["provenance"]["prod_app_version"],
            "window": "7d",
        },
        "prod_scale_reference": {
            "active_zero_clients_avg": avg,
            "active_zero_clients_peak": sc["active_zero_clients_peak"],
            "note": "use these as --connections targets when asking capacity "
                    "questions; per-client behavior below is already prod-rate",
        },
        "scale_factor": a.scale,
        # ---- knobs replay.py consumes (profile-provided defaults; explicit
        # CLI flags always win) ----
        "knobs": {
            "lifecycle": True,
            "churn_ms": int(round(churn_per_client_s * 1000 / a.scale)),
            "mutations_per_min": round(mut_per_min * a.scale, 4),
            "session_mean_s": round(mean_session_s, 1),
            "abrupt_pct": round(abrupt_pct, 3),
            "working_set": working_set,
            "resume_pct": resume_pct,
            # not derivable from aggregate events — keep replay defaults, but
            # pin them here so profile runs are reproducible even if replay's
            # hard defaults change:
            "zombie_pct": 0.10,
            "retire_pct": 0.15,
        },
        "derivation": {
            "churn": f"{q_calls} zero_query_called / 7d / {avg} avg clients "
                     f"= {60 / churn_per_client_s:.3f}/min/client "
                     f"(one per {churn_per_client_s:.0f}s)",
            "mutations": f"{m_calls} zero_mutation_called / 7d / {avg} "
                         f"= {mut_per_min:.4f}/min/client",
            "session": f"Little's law: {avg} avg concurrent * 7d / {conns} "
                       f"connections = {mean_session_s / 60:.1f} min mean",
            "abrupt": f"{abrupt} zero_socket_disconnected / {closed} "
                      f"websocket_connection_closed = {abrupt_pct:.1%}",
        },
        "caveats": [
            "websocket_connection_successful/closed are frontend-wide events and "
            "may include non-zero sockets -> mean session is approximate; "
            "zero_socket_disconnected is zero-scoped, so abrupt_pct mixes scopes "
            "(refine via mine_session_profile.py when on VPN)",
            "zero_query_called is a client reactive-call event, not strictly a "
            "changeDesiredQueries put; treats every call as a swap (upper bound)",
            "zombie/resume/retire/working_set are NOT prod-mined yet — they keep "
            "replay defaults (Layer B mining gap)",
            "prod-cadence runs need duration >= 15-30min for meaningful churn/"
            "mutation sample counts; latency samples come mostly from initial "
            "hydrations (that matches prod, where connect-bursts dominate)",
        ],
    }

    if layer_b:
        profile["layer_b"] = {
            "source": "raw/session_profile_7d.json "
                      f"(mined {layer_b.get('mined_at')}, zero-scoped)",
            "session": f"Little's law over zero_socket_connected: {avg} avg "
                       f"concurrent * 7d / {layer_b.get('zero_scoped_connects_7d')} "
                       f"= {mean_session_s / 60:.1f} min mean",
            "abrupt": "observable zero-socket ends: "
                      f"{(layer_b.get('observable_end_split') or {}).get('network_loss_giveups')} "
                      "network-loss giveups vs "
                      f"{(layer_b.get('observable_end_split') or {}).get('clean_tab_hides')} "
                      "clean tab-hides (unobservable ends are also abrupt)",
            "working_set": "p50 distinct query types per cgid per day "
                           f"= {working_set}",
            "resume": f"p50 cgid connects {  (layer_b.get('conns_per_cgid_per_day') or {}).get('p50')}x/day "
                      "-> nearly every connect is a cookie-resume reconnect",
            "conns_per_cgid_per_day": layer_b.get("conns_per_cgid_per_day"),
            "tab_hide_session_duration_ms": layer_b.get("tab_hide_session_duration_ms"),
            "connection_attempts_to_success": layer_b.get("connection_attempts_to_success"),
        }
        profile["caveats"] = [
            "session length is a MEAN via Little's law; the true distribution is "
            "heavy-tailed (tab-hide closes: p50 85s, p99 976s vs 16.7min mean) — "
            "replay draws expovariate(mean), which under-represents both very "
            "short and multi-hour sessions",
            "zero_query_called treats every reactive call as a swap (upper bound)",
            "zombie/retire are NOT prod-mined (no client-side signal); replay "
            "defaults kept",
            "prod-cadence runs need duration >= 15-30min for meaningful churn/"
            "mutation sample counts",
        ]

    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w") as f:
        json.dump(profile, f, indent=2)

    k = profile["knobs"]
    print(f"profile -> {a.out}" + ("  [Layer B: zero-scoped]" if layer_b else "  [Layer A only]"))
    print(f"  scale_factor      : {a.scale:g}x prod per-client rates")
    print(f"  churn_ms          : {k['churn_ms']}  (one query swap per "
          f"{k['churn_ms'] / 1000:.0f}s per client; hot default 750)")
    print(f"  mutations_per_min : {k['mutations_per_min']}  (hot default 4)")
    print(f"  session_mean_s    : {k['session_mean_s']}  (hot default 45)")
    print(f"  abrupt_pct        : {k['abrupt_pct']}  (hot default 0.50)")
    print(f"  working_set       : {k['working_set']}  (hot default 12)")
    print(f"  resume_pct        : {k['resume_pct']}  (hot default 0.70)")
    print(f"  reference conns   : avg={avg} peak={sc['active_zero_clients_peak']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
