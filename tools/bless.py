#!/usr/bin/env python3
"""bless.py — automated baseline re-bless workflow (#4).

Verifies a gate report has no FAIL verdicts, then auto-blesses the run's
shape in reports/local-baseline.json by invoking local_gate.py --update-baseline.

Prevents stale baselines from masking regressions: when a new image passes
all gates, bless.py confirms the pass and blesses the new shape so future
runs gate against it.

    python3 tools/bless.py reports/gate-20260711-104326.json
    python3 tools/bless.py --run reports/run-20260711-103428.json
    python3 tools/bless.py reports/gate-X.json --dry-run  # check without blessing

Exit 0 = blessed; 1 = cannot bless (gates failed); 2 = error.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys


def config_hash() -> str:
    """Hash of the catalog (art-baseline.json) + seeder config (seed_prod_scale.py)
    + replay harness (replay.py, workload.py). A baseline blessed against one
    config is invalid if the config changes — tie the hash so a stale baseline
    refuses to gate rather than silently gating wrong."""
    h = hashlib.sha256()
    for path in ["art-baseline.json", "tools/seed_prod_scale.py",
                 "harness/replay.py", "harness/workload.py"]:
        if os.path.exists(path):
            with open(path, "rb") as f:
                h.update(f.read())
    return h.hexdigest()[:16]


def main() -> int:
    ap = argparse.ArgumentParser(description="Automated baseline re-bless.")
    ap.add_argument("gate", nargs="?", help="gate report JSON to verify before blessing")
    ap.add_argument("--run", default=None, help="run report to bless (alternative to gate)")
    ap.add_argument("--dry-run", action="store_true", help="check only, don't bless")
    ap.add_argument("--baseline", default="reports/local-baseline.json")
    a = ap.parse_args()

    # find the gate report
    gate_path = a.gate
    if not gate_path and a.run:
        # derive gate from run tag
        import re
        m = re.search(r"run-(\d{8}-\d{6})", a.run)
        if m:
            tag = m.group(1)
            candidates = sorted(
                __import__("glob").glob(f"reports/gate-{tag}*.json"))
            gate_path = candidates[-1] if candidates else None
    if not gate_path:
        # use newest gate report
        import glob
        gates = sorted(glob.glob("reports/gate-*.json"))
        if not gates:
            print("ERROR: no gate reports found", file=sys.stderr)
            return 2
        gate_path = gates[-1]
        print(f"  using newest gate report: {gate_path}")

    if not os.path.exists(gate_path):
        print(f"ERROR: gate report not found: {gate_path}", file=sys.stderr)
        return 2

    with open(gate_path) as f:
        doc = json.load(f)

    # verify no FAIL
    results = doc.get("results", [])
    fails = [r for r in results if r.get("verdict") == "FAIL"]
    errors = [r for r in results if r.get("verdict") == "ERROR"]
    watches = [r for r in results if r.get("verdict") == "WATCH"]

    print(f"gate report: {gate_path}")
    print(f"  {len(results)} gates: "
          f"{sum(1 for r in results if r.get('verdict') == 'PASS')} PASS, "
          f"{len(watches)} WATCH, "
          f"{len(errors)} ERROR, "
          f"{len(fails)} FAIL")

    if fails:
        print("\nCANNOT BLESS — FAIL gates:")
        for r in fails:
            print(f"  {r.get('gate', '?')}: {r.get('detail', '')[:100]}")
        return 1

    if errors:
        print("\nCANNOT BLESS — ERROR gates (infra issue):")
        for r in errors:
            print(f"  {r.get('gate', '?')}: {r.get('detail', '')[:100]}")
        return 1

    if watches:
        print("\n  WATCH findings (non-blocking):")
        for r in watches:
            print(f"  {r.get('gate', '?')}: {r.get('detail', '')[:100]}")
        return 1

    if errors:
        print("\nCANNOT BLESS — ERROR gates (infra issue):")
        for r in errors:
            print(f"  {r[0]}: {r[2][:100]}")
        return 1

    if watches:
        print("\n  WATCH findings (non-blocking):")
        for r in watches:
            print(f"  {r.get('gate', '?')}: {r.get('detail', '')[:100]}")

    # find the run report to determine the shape
    import re
    tag = re.sub(r"^gate-|\.json$", "", os.path.basename(gate_path))
    import glob
    run_paths = sorted(glob.glob(f"reports/run-{tag}*.json"))
    if not run_paths:
        print("ERROR: no matching run report found for blessing", file=sys.stderr)
        return 2

    run_path = run_paths[0]
    with open(run_path) as f:
        run_doc = json.load(f)
    cfg = run_doc.get("config", {})
    shape = f"{cfg.get('connections', '?')}c"
    if cfg.get("lifecycle"):
        shape += "-life"
    if cfg.get("profile"):
        shape += f"-{os.path.basename(cfg['profile']).replace('.json', '')}"
    elif cfg.get("trace"):
        shape += f"-trace:{os.path.basename(cfg['trace'])}"

    print(f"\n  shape: {shape}")
    print(f"  run: {run_path}")

    # check current baseline
    cfg_hash = config_hash()
    if os.path.exists(a.baseline):
        with open(a.baseline) as f:
            bl = json.load(f)
        bl_hash = bl.get("config_hash")
        if bl_hash and bl_hash != cfg_hash:
            print(f"  WARNING: baseline config_hash mismatch (baseline={bl_hash}, current={cfg_hash})")
            print(f"  catalog/seeder/harness changed since this baseline was blessed.")
            print(f"  Re-bless required: the stale baseline may gate wrong.")
        if shape in bl.get("shapes", {}):
            old = bl["shapes"][shape]
            print(f"  existing baseline: p50={old.get('p50')} p95={old.get('p95')}")
    else:
        print(f"  no existing baseline file ({a.baseline})")

    if a.dry_run:
        print("\n  --dry-run: would bless this shape")
        return 0

    # invoke local_gate.py --update-baseline
    print(f"\n  blessing shape {shape} (config_hash={cfg_hash})...")
    cmd = [sys.executable, "tools/local_gate.py",
           "--run", run_path, "--update-baseline",
           "--baseline", a.baseline]
    rc = subprocess.run(cmd, capture_output=True, text=True).returncode
    if rc != 0:
        print(f"  bless failed (rc={rc})", file=sys.stderr)
        return 2

    # stamp the config hash into the baseline so future runs can detect staleness
    if os.path.exists(a.baseline):
        with open(a.baseline) as f:
            bl = json.load(f)
        bl["config_hash"] = cfg_hash
        with open(a.baseline, "w") as f:
            json.dump(bl, f, indent=2)

    print(f"  BLESSED: {shape} (config_hash={cfg_hash})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
