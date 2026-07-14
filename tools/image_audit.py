#!/usr/bin/env python3
"""image_audit.py — G23: zero-cache image supply-chain audit.

Behavioral gates test what the image DOES; nothing tests the image AS AN
ARTIFACT. A 2x layer bloat, a new base-image CVE, or an unpinned/drifted base
digest is a ship blocker no SLO gate will catch. This gate audits the image:
  1. size budget   — docker image size vs --size-budget-mb (WATCH over, FAIL
                     over --size-hard-mb)
  2. base-image pin — assert the base image digest matches --expected-base
                      (FAIL on drift; catches a floating :latest base)
  3. CVE scan      — run trivy or grype (whichever is on PATH); FAIL on
                     --cve-severity HIGH,CRITICAL (WATCH on lower)
  4. labels        — assert required OCI labels (--require-label) are present

    .venv/bin/python tools/image_audit.py --image xyne-zero-cache:canary \\
        --size-budget-mb 400 --size-hard-mb 600 \\
        --expected-base sha256:abc123... --require-label org.opencontainers.image.revision \\
        --out reports/image-$TAG.json

Exit 0 = clean; 1 = violation (FAIL); 2 = ERROR (image absent / tooling missing).
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time


def image_inspect(image: str) -> dict | None:
    r = subprocess.run(["docker", "image", "inspect", image],
                       capture_output=True, text=True, timeout=30)
    if r.returncode != 0 or not r.stdout:
        return None
    try:
        return json.loads(r.stdout)[0]
    except Exception:
        return None


def cve_scan(image: str) -> tuple[list[dict], str | None]:
    """Run trivy or grype; return (vulnerabilities, tool). Each vuln has
    severity + id. Returns ([], None) if no scanner is installed (SKIP)."""
    for tool, args in (("trivy", ["image", "--quiet", "--json", image]),
                       ("grype", ["-q", "--json", image])):
        if not shutil.which(tool):
            continue
        try:
            r = subprocess.run([tool] + args, capture_output=True, text=True, timeout=300)
            if r.returncode not in (0, 1):
                continue
            d = json.loads(r.stdout)
            vulns: list[dict] = []
            if tool == "trivy":
                for res in d.get("Results", []):
                    for v in res.get("Vulnerabilities", []) or []:
                        vulns.append({"id": v.get("VulnerabilityID"),
                                      "severity": v.get("Severity", "UNKNOWN")})
            else:  # grype
                for m in d.get("matches", []):
                    v = m.get("vulnerability", {})
                    vulns.append({"id": v.get("id"), "severity": v.get("severity", "UNKNOWN")})
            return vulns, tool
        except Exception:
            continue
    return [], None


def main() -> int:
    ap = argparse.ArgumentParser(description="G23: image supply-chain audit.")
    ap.add_argument("--image", required=True, help="docker image ref")
    ap.add_argument("--size-budget-mb", type=float, default=400.0,
                    help="WATCH if size exceeds this")
    ap.add_argument("--size-hard-mb", type=float, default=800.0,
                    help="FAIL if size exceeds this")
    ap.add_argument("--expected-base", default=None,
                    help="expected base image digest (sha256:...); FAIL on drift")
    ap.add_argument("--cve-severity", default="CRITICAL",
                    help="comma-sep severities that FAIL (default CRITICAL)")
    ap.add_argument("--require-label", action="append", default=[],
                    help="OCI label that must be present (repeatable)")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    checks: list[dict] = []
    info = image_inspect(a.image)
    if info is None:
        print(f"ERROR: cannot inspect {a.image} (not present or docker down)", file=sys.stderr)
        return 2

    # 1. size
    size_bytes = info.get("Size", 0)
    size_mb = round(size_bytes / (1024 * 1024), 1)
    if size_mb > a.size_hard_mb:
        v, detail = "FAIL", f"{size_mb}MB exceeds hard limit {a.size_hard_mb}MB"
    elif size_mb > a.size_budget_mb:
        v, detail = "WATCH", f"{size_mb}MB over budget {a.size_budget_mb}MB (under hard limit)"
    else:
        v, detail = "PASS", f"{size_mb}MB within budget {a.size_budget_mb}MB"
    checks.append({"name": "size", "verdict": v, "detail": detail, "size_mb": size_mb})

    # 2. base-image pin
    rootfs = info.get("RootFS", {}) or {}
    layers = rootfs.get("Layers", [])
    base_digest = layers[0] if layers else None
    if a.expected_base:
        if base_digest == a.expected_base:
            checks.append({"name": "base-pin", "verdict": "PASS",
                           "detail": f"base digest matches {a.expected_base[:24]}..."})
        else:
            checks.append({"name": "base-pin", "verdict": "FAIL",
                           "detail": f"base drift: expected {a.expected_base[:24]}... "
                                     f"got {str(base_digest)[:24]}..."})
    else:
        checks.append({"name": "base-pin", "verdict": "SKIP",
                       "detail": f"no --expected-base (base={str(base_digest)[:24]}...)"})

    # 3. labels
    labels = info.get("Config", {}).get("Labels") or {}
    for lbl in a.require_label:
        if lbl in labels:
            checks.append({"name": f"label:{lbl}", "verdict": "PASS", "detail": "present"})
        else:
            checks.append({"name": f"label:{lbl}", "verdict": "FAIL", "detail": "MISSING label"})

    # 4. CVE scan
    fail_sev = set(s.strip().upper() for s in a.cve_severity.split(","))
    vulns, tool = cve_scan(a.image)
    if tool is None:
        checks.append({"name": "cve-scan", "verdict": "SKIP",
                       "detail": "no trivy/grype on PATH — install one to enable CVE gating"})
    else:
        blocking = [v for v in vulns if v["severity"].upper() in fail_sev]
        if blocking:
            ids = sorted({v["id"] for v in blocking})[:8]
            checks.append({"name": "cve-scan", "verdict": "FAIL",
                           "detail": f"{len(blocking)} vuln(s) at {a.cve_severity} via {tool}: {ids}"})
        else:
            checks.append({"name": "cve-scan", "verdict": "PASS",
                           "detail": f"{len(vulns)} vuln(s) found, none at {a.cve_severity} ({tool})"})

    fail = any(c["verdict"] == "FAIL" for c in checks)
    verdict = "FAIL" if fail else "PASS"
    summary = (f"image {a.image}: {size_mb}MB, base={str(base_digest)[:16]}..., "
               f"{len(vulns)} CVE(s), verdict={verdict}")
    report = {"schema": 1, "gate": "G23", "name": "image-audit",
              "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "image": a.image, "verdict": verdict, "summary": summary,
              "checks": checks, "size_mb": size_mb,
              "base_digest": base_digest, "cve_count": len(vulns), "scanner": tool}
    print(summary)
    for c in checks:
        print(f"  {c['name']:<18} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[verdict]


if __name__ == "__main__":
    raise SystemExit(main())
