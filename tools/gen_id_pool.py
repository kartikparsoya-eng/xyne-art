#!/usr/bin/env python3
"""
gen_id_pool.py — seed harness/id-pool.json with REAL entity IDs harvested from
production telemetry.

The `args` object logged on every `zero_query_complete` event contains real IDs
(channelId, conversationId, mappedTicketId, ticketId, canvasId, boardId, ...).
We sample those events and collect distinct values per argument key, so the
replay harness fires queries with IDs that actually resolve in the target DB.

    export GR_KEY='glsa_...'        # Grafana service-account token, on VPN
    python3 tools/gen_id_pool.py --window 24h --sample 40000 --per-key 300

IMPORTANT: harvest from the SAME environment you will load-test. Prod IDs won't
resolve against a fresh staging DB — point --container/--query-endpoint at the
target's telemetry, or seed the pool from the target DB instead.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

BASE = "https://grafana.spaces.xyne.juspay.net"
LOGS = BASE + "/api/datasources/proxy/8/select/logsql/query"

# Values for these keys are behavioural scalars, not entity IDs -> scalars pool.
SCALAR_KEYS = {
    "limit", "start", "direction", "isMember", "isRead", "showOverdueOnly",
    "viewMode", "columnType", "groupBy", "classification", "types",
    "lastUpdatedAt", "updatedAt", "recapDate", "contextType", "entityType",
    "stageName", "groupKey",
}


def fetch_events(key: str, window: str, sample: int) -> list[dict]:
    q = 'container:"xyne-logging-bridge" AND event:"zero_query_complete"'
    data = urllib.parse.urlencode(
        {"query": q, "start": window, "limit": str(sample)}
    ).encode()
    req = urllib.request.Request(LOGS, data=data,
                                 headers={"Authorization": "Bearer " + key})
    with urllib.request.urlopen(req, timeout=180) as resp:
        body = resp.read().decode()
    out = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            outer = json.loads(line)
            msg = outer.get("_msg")
            if msg:
                out.append(json.loads(msg))
        except Exception:
            continue
    return out


def harvest(events: list[dict], per_key: int):
    ids: dict[str, list] = {}
    id_seen: dict[str, set] = {}
    scalars: dict[str, list] = {}
    scalar_seen: dict[str, set] = {}

    def add_id(k, v):
        if not isinstance(v, str) or not v:
            return
        s = id_seen.setdefault(k, set())
        if v not in s and len(s) < per_key:
            s.add(v)
            ids.setdefault(k, []).append(v)

    def add_scalar(k, v):
        try:
            key = json.dumps(v, sort_keys=True)
        except Exception:
            return
        s = scalar_seen.setdefault(k, set())
        if key not in s and len(s) < 20:
            s.add(key)
            scalars.setdefault(k, []).append(v)

    n_args = 0
    for e in events:
        a = e.get("args")
        if not isinstance(a, dict):
            continue
        n_args += 1
        for k, v in a.items():
            if k in SCALAR_KEYS:
                add_scalar(k, v)
            elif isinstance(v, str):
                add_id(k, v)
            elif isinstance(v, list):
                for item in v:                       # e.g. channelIds -> channelId pool
                    if isinstance(item, str):
                        add_id(k[:-1] if k.endswith("s") else k, item)
            elif isinstance(v, (bool, int, float)) or v is None:
                add_scalar(k, v)
    return ids, scalars, n_args


def main() -> int:
    ap = argparse.ArgumentParser(description="Harvest a real id-pool from telemetry.")
    ap.add_argument("--window", default="24h")
    ap.add_argument("--sample", type=int, default=40000, help="max raw events to pull")
    ap.add_argument("--per-key", type=int, default=300, help="max distinct IDs kept per key")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "harness", "id-pool.json"))
    a = ap.parse_args()

    key = os.environ.get("GR_KEY")
    if not key:
        print("ERROR: export GR_KEY first (Grafana token, on VPN)", file=sys.stderr)
        return 2

    print(f"pulling up to {a.sample} zero_query_complete events over {a.window} ...")
    events = fetch_events(key, a.window, a.sample)
    print(f"  got {len(events)} events")
    ids, scalars, n_args = harvest(events, a.per_key)

    pool = {
        "_provenance": {
            "source": "xyne-logging-bridge zero_query_complete.args",
            "window": a.window, "events_sampled": len(events), "events_with_args": n_args,
            "warning": "IDs are from PROD; only valid against the same environment's DB.",
        },
        "ids": {k: sorted(v) for k, v in sorted(ids.items())},
        "scalars": {k: v for k, v in sorted(scalars.items())},
    }
    out = os.path.abspath(a.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(pool, f, indent=2)

    print(f"wrote {out}")
    print(f"  id keys   : {len(ids)}  ({sum(len(v) for v in ids.values())} distinct IDs)")
    top = sorted(ids.items(), key=lambda kv: -len(kv[1]))[:12]
    for k, v in top:
        print(f"    {k:<28} {len(v)} ids   e.g. {v[0] if v else '-'}")
    print(f"  scalar keys: {len(scalars)}  {sorted(scalars)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
