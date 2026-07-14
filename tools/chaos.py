#!/usr/bin/env python3
"""
chaos.py — infra fault injection for ART runs (docker pause/unpause).

Runs alongside a replay and periodically freezes a container (SIGSTOP-style
via `docker pause`), holds it for --pause-s, then unpauses. This exercises
the bug classes normal replay can't reach:

  pause-zc : zero-cache freeze — clients see dead sockets, must reconnect /
             resume-from-cookie; server must recover its workers cleanly.
  pause-pg : postgres freeze  — replication stream + CVR writes stall; the
             cache must absorb the stall and catch up without corruption.

Events are exponentially spaced (mean --mean-gap-s). Containers are ALWAYS
unpaused (finally-block), even on crash/ctrl-C. After the last event the tool
waits for the zero-cache healthcheck and writes a summary JSON.

    python3 tools/chaos.py --duration 120 --out reports/chaos.json
    python3 tools/chaos.py --actions pause-zc --mean-gap-s 30 --pause-s 10 ...

Exit 0 = all events reverted + zero-cache healthy at end; 1 = otherwise.
NOTE: pausing postgres briefly affects every sandbox on this host — local
dev use only. Run with --lifecycle replay so clients expect reconnects.
"""
from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import time


def sh(*cmd: str, timeout: int = 30) -> tuple[int, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return p.returncode, (p.stdout + p.stderr).strip()


def health(container: str) -> str:
    rc, out = sh("docker", "inspect", "-f", "{{.State.Health.Status}}", container)
    return out if rc == 0 else "unknown"


def main() -> int:
    ap = argparse.ArgumentParser(description="ART chaos injector (docker pause).")
    ap.add_argument("--zc-container", default="xyne-sandbox-rust-test-zero-cache")
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--duration", type=float, required=True,
                    help="injection window in seconds (stop injecting after this)")
    ap.add_argument("--mean-gap-s", type=float, default=45.0,
                    help="mean seconds between events (exponential)")
    ap.add_argument("--pause-s", type=float, default=8.0,
                    help="how long each pause lasts")
    ap.add_argument("--actions", default="pause-zc,pause-pg",
                    help="comma list: pause-zc,pause-pg,netem-latency,netem-loss,netem-partition")
    ap.add_argument("--netem-latency-ms", type=float, default=200.0,
                    help="network latency to inject (ms) for netem actions")
    ap.add_argument("--netem-loss-pct", type=float, default=5.0,
                    help="packet loss percentage for netem-loss")
    ap.add_argument("--netem-duration-s", type=float, default=10.0,
                    help="how long netem injection lasts before cleanup")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--out", default=f"reports/chaos-{time.strftime('%Y%m%d-%H%M%S')}.json")
    a = ap.parse_args()

    rng = random.Random(a.seed)
    targets = {"pause-zc": a.zc_container, "pause-pg": a.pg_container}
    actions = [x.strip() for x in a.actions.split(",")
               if x.strip() in targets or x.strip().startswith("netem-")]
    # netem targets: the zero-cache container's network interface
    netem_container = a.zc_container
    if not actions:
        print("ERROR: no valid actions", file=sys.stderr)
        return 1

    events: list[dict] = []
    t_end = time.monotonic() + a.duration
    all_reverted = True

    while time.monotonic() < t_end:
        gap = rng.expovariate(1.0 / a.mean_gap_s)
        if time.monotonic() + gap + a.pause_s > t_end:
            break  # leave the tail of the window for recovery
        time.sleep(gap)
        action = rng.choice(actions)
        ev = {"t": time.strftime("%H:%M:%S"), "action": action,
              "pause_s": a.pause_s, "paused": False, "unpaused": False}

        if action in targets:
            # docker pause action (original)
            container = targets[action]
            ev["container"] = container
            rc, out = sh("docker", "pause", container)
            ev["paused"] = rc == 0
            if rc != 0:
                ev["error"] = out
            try:
                if ev["paused"]:
                    print(f"[chaos] {ev['t']} {action}: paused {container} "
                          f"for {a.pause_s:.0f}s", flush=True)
                    time.sleep(a.pause_s)
            finally:
                rc, out = sh("docker", "unpause", container)
                ev["unpaused"] = rc == 0 or "is not paused" in out
                if not ev["unpaused"]:
                    ev["unpause_error"] = out
                    all_reverted = False
        elif action.startswith("netem-"):
            # network chaos via tc netem (#8)
            ev["container"] = netem_container
            ev["netem"] = True
            # find the container's eth0 interface
            rc, iface = sh("docker", "exec", netem_container,
                           "sh", "-c", "ip -o link show | grep -m1 'eth0' | awk '{print $2}' | tr -d ':'")
            if not iface:
                iface = "eth0"
            if action == "netem-latency":
                cmd = ["tc", "qdisc", "add", "dev", iface, "root",
                       "netem", "delay", f"{a.netem_latency_ms}ms"]
                desc = f"+{a.netem_latency_ms}ms latency"
            elif action == "netem-loss":
                cmd = ["tc", "qdisc", "add", "dev", iface, "root",
                       "netem", "loss", f"{a.netem_loss_pct}%"]
                desc = f"{a.netem_loss_pct}% packet loss"
            elif action == "netem-partition":
                cmd = ["tc", "qdisc", "add", "dev", iface, "root",
                       "netem", "delay", "5000ms", "loss", "100%"]
                desc = "network partition"
            else:
                events.append(ev)
                continue
            rc, out = sh("docker", "exec", netem_container, *cmd)
            ev["applied"] = rc == 0
            if rc != 0:
                ev["error"] = out
            try:
                if ev["applied"]:
                    print(f"[chaos] {ev['t']} {action}: {desc} on {iface} "
                          f"for {a.netem_duration_s:.0f}s", flush=True)
                    time.sleep(a.netem_duration_s)
            finally:
                rc2, out2 = sh("docker", "exec", netem_container,
                               "tc", "qdisc", "del", "dev", iface, "root")
                ev["reverted"] = rc2 == 0 or "No such file" in out2
                if not ev["reverted"]:
                    ev["revert_error"] = out2
                    all_reverted = False
        events.append(ev)

    # Safety sweep: make sure nothing is left paused.
    for c in set(targets.values()):
        rc, out = sh("docker", "inspect", "-f", "{{.State.Paused}}", c)
        if rc == 0 and out == "true":
            sh("docker", "unpause", c)
            all_reverted = False  # something needed the sweep — flag it

    # Safety sweep: clean up any lingering tc netem qdisc (#8)
    if any(a.startswith("netem-") for a in actions):
        for iface in ("eth0", "ens3"):
            sh("docker", "exec", netem_container,
               "tc", "qdisc", "del", "dev", iface, "root")

    # Recovery check: zero-cache healthcheck must go green.
    final_health = "unknown"
    for _ in range(45):
        final_health = health(a.zc_container)
        if final_health == "healthy":
            break
        time.sleep(2)

    ok = all_reverted and final_health == "healthy"
    summary = {"events": events, "n_events": len(events),
               "all_reverted": all_reverted, "final_health": final_health,
               "verdict": "PASS" if ok else "FAIL"}
    with open(a.out, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[chaos] {len(events)} events, all_reverted={all_reverted}, "
          f"zero-cache={final_health} -> {a.out}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
