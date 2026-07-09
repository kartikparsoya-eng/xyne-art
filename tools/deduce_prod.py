#!/usr/bin/env python3
"""
deduce_prod.py — turn a prod-calibrated local run into prod claims.

The transfer model (see run-art-prodcal.sh header): local zc is pinned to 8
cores vs prod's 16 => S = 0.5. Extensive quantities scale by 1/S; intensive
ones were held at prod value by the experiment design. Absolute latencies do
NOT transfer (sandbox DB << 32GiB prod replica) — only ratios and structure.

Rules implemented (from the prod sizing work):
  prod_pod_busy_cores = local_busy_cores x (1/S)          assert <= 16
  prod_go_heap        = per_CG_heap x 350                 assert <= 10 GiB
  prod_resets/h       = NOT absolute — assert Go ~ 0 while TS (swap run at
                        the same shape) reproduces the chronic loop (425/h)
  prod_hydrate_p99    = 5-7s x (Go/TS local ratio)        structure only
  JS-drain verdict    = stalls/staged at 200 evt/s local ~ prod bulk worst
                        case (evt rate is intensive — never scaled)

    tools/deduce_prod.py --run reports/run-X.json \
        --resources reports/resources-X.summary.json --logs reports/logs-X.json \
        [--swap-run reports/run-Y.json --swap-logs reports/logs-Y.json]
"""
from __future__ import annotations

import argparse
import json
import subprocess


PROD = {
    "pod_cores": 16, "peak_cgs": 350, "heap_budget_gib": 10.0,
    "hydrate_p99_s": (5.0, 7.0), "chronic_resets_per_h": 425,
}


def load(path):
    with open(path) as f:
        return json.load(f)


def zc_cpus(container: str) -> float:
    try:
        out = subprocess.run(["docker", "inspect", container, "--format",
                              "{{.HostConfig.NanoCpus}}"],
                             capture_output=True, text=True, timeout=10)
        n = int(out.stdout.strip() or 0)
        return n / 1e9 if n else 8.0
    except Exception:
        return 8.0


def heal_count(logrep: dict) -> int:
    tot = 0
    for c in (logrep or {}).get("containers", {}).values():
        tot += sum(h.get("count", 0) for h in c.get("self_heal_hits", {}).values())
    return tot


def napi(logrep: dict) -> dict:
    for c in (logrep or {}).get("containers", {}).values():
        if c.get("napi_deliver"):
            return c["napi_deliver"]
    return {}


def main() -> int:
    ap = argparse.ArgumentParser(description="Local run -> prod claims.")
    ap.add_argument("--run", required=True)
    ap.add_argument("--resources", required=True)
    ap.add_argument("--logs", default=None)
    ap.add_argument("--swap-run", default=None, help="TS run at the SAME shape")
    ap.add_argument("--swap-logs", default=None)
    ap.add_argument("--container", default="xyne-sandbox-rust-test-zero-cache")
    ap.add_argument("--writer-rate", type=float, default=None,
                    help="bg-writer evt/s during the run (for the JS-drain rule)")
    a = ap.parse_args()

    run = load(a.run)
    res = load(a.resources)
    logs = load(a.logs) if a.logs else {}
    cpus = zc_cpus(a.container)
    S = cpus / PROD["pod_cores"]
    cgs = run.get("config", {}).get("connections") or 0
    claims: list[tuple[str, str, str]] = []   # (claim, value, verdict)

    # --- busy cores ---------------------------------------------------------
    cpu = res.get("cpu_pct", {}) if isinstance(res.get("cpu_pct"), dict) else {}
    avg_pct = cpu.get("avg") or res.get("cpu_pct_avg")
    max_pct = cpu.get("max") or res.get("cpu_pct_max")
    if avg_pct is None:  # fall back: derive from the raw ndjson next to summary
        raw = a.resources.replace(".summary.json", ".ndjson")
        try:
            vals = [float(str(json.loads(ln)["cpu_pct"]).rstrip("%"))
                    for ln in open(raw) if ln.strip()]
            avg_pct = sum(vals) / len(vals)
            max_pct = max(vals)
        except Exception:
            avg_pct = max_pct = None
    if avg_pct is not None:
        for label, pct in (("avg", avg_pct), ("peak", max_pct)):
            local_busy = pct / 100.0
            prod_busy = local_busy / S
            ok = prod_busy <= PROD["pod_cores"]
            claims.append((f"prod pod busy-cores ({label})",
                           f"{local_busy:.1f} local x {1/S:.0f} = {prod_busy:.1f}",
                           "PASS" if ok else "FAIL (> 16-core pod)"))

    # --- per-CG heap -> prod heap -------------------------------------------
    rss = res.get("rss_bytes", {})
    peak = rss.get("max") or rss.get("last")
    if peak and cgs:
        per_cg = peak / cgs
        prod_heap_gib = per_cg * PROD["peak_cgs"] / 2**30
        ok = prod_heap_gib <= PROD["heap_budget_gib"]
        claims.append(("prod Go heap @350 CGs",
                       f"{per_cg/2**20:.1f} MiB/CG x 350 = {prod_heap_gib:.1f} GiB",
                       "PASS" if ok else f"FAIL (> {PROD['heap_budget_gib']} GiB)"))

    # --- reset loop (needs the TS swap run) ----------------------------------
    go_heals = heal_count(logs)
    claims.append(("Go self-heal events (resets/adv-timeouts)", str(go_heals),
                   "PASS (~0)" if go_heals <= 5 else "WATCH — prod chronic loop is a TS artifact only if Go stays ~0"))
    if a.swap_logs:
        ts_heals = heal_count(load(a.swap_logs))
        claims.append(("TS self-heal at same shape", str(ts_heals),
                       "reproduces prod chronic loop" if ts_heals > go_heals * 3
                       else "did NOT reproduce — shape may miss the loop trigger"))

    # --- hydrate p99 structure (ratio) ----------------------------------------
    if a.swap_run:
        ts = load(a.swap_run)
        g99 = run.get("client_latency_initial_ms", {}).get("p95")
        t99 = ts.get("client_latency_initial_ms", {}).get("p95")
        if g99 and t99:
            ratio = g99 / t99
            lo, hi = (r * ratio for r in PROD["hydrate_p99_s"])
            claims.append(("prod hydrate p99 projection (structure)",
                           f"Go/TS ratio {ratio:.2f} -> {lo:.1f}-{hi:.1f}s vs prod 5-7s",
                           "improves" if ratio < 1 else "regresses"))

    # --- JS drain at replication rate ----------------------------------------
    nd = napi(logs)
    if nd:
        st, sg, to = nd.get("stalls", 0), nd.get("staged", 0), nd.get("timeouts", 0)
        rate_note = f"@{a.writer_rate:.0f} evt/s" if a.writer_rate else ""
        verdict = "FAIL (deliver timeout)" if to else (
            "healthy (staged>>stalls)" if sg > 10 * max(st, 1) else "WATCH — parks significant")
        claims.append((f"JS-drain {rate_note} (bulk-day proxy at 200)",
                       f"stalls={st} staged={sg} timeouts={to}", verdict))

    # --- output ---------------------------------------------------------------
    print(f"S = {cpus:.0f}/{PROD['pod_cores']} = {S:.2f}   run: {a.run}   CGs: {cgs}")
    print(f"{'claim':44} {'value':44} verdict")
    for c, v, verdict in claims:
        print(f"{c:44} {v:44} {verdict}")
    print("\nNOT transferable: absolute latencies (sandbox DB << prod 32GiB replica).")
    out = {"S": S, "cgs": cgs, "claims": [
        {"claim": c, "value": v, "verdict": verdict} for c, v, verdict in claims]}
    outp = a.run.replace("run-", "prodclaims-")
    with open(outp, "w") as f:
        json.dump(out, f, indent=1)
    print(f"-> {outp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
