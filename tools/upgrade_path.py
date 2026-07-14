#!/usr/bin/env python3
"""upgrade_path.py — G24: zero-cache image upgrade-path / CVR-compat test.

The diff oracle (G8) compares two BUILDS side by side. It never tests the
UPGRADE sequence: a client group whose CVR state was written by the OLD image
must resume correctly under the NEW image. A CVR-format change, a schema
migration, or a baseCookie-version mismatch between images breaks exactly this
path — and nothing currently exercises it. A rolling deploy is literally
millions of clients resuming across an image boundary; this gate tests that.

Method:
  1. connect a client to --baseline-target (the reference image), drive a
     small desired-query set, capture the baseCookie the server returns in its
     poke (this is CVR state written by the OLD image)
  2. connect to --candidate-target (the new image under test) with that
     baseCookie (simulating a client resuming after the image swap), and
     materialize the converged state
  3. connect a FRESH client to --candidate-target (no baseCookie) and
     materialize — this is what a brand-new client sees on the new image
  4. diff: resumed state must equal fresh state (zero data loss on upgrade)
     AND the resume must not error (no CVR-incompat rejection)

Reuses harness/diff_oracle.py::Materializer + diff_states.

    .venv/bin/python tools/upgrade_path.py \\
        --baseline-target ws://rust-test.localhost/zero \\
        --candidate-target ws://rust-test.localhost/zero-new \\
        --auth-token "$JWT" --id-pool harness/id-pool.sandbox.json \\
        --client-schema harness/client-schema.json --extra-param userID=$UID \\
        --out reports/upgrade-$TAG.json

Exit 0 = clean upgrade (resume + converge, zero loss); 1 = broken (CVR
incompat / data loss); 2 = ERROR (infra — target unreachable).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from protocol import DEFAULT_PROTOCOL_VERSION, encode_sec_protocols  # noqa: E402
from workload import ArgResolver, WeightedSampler, load_baseline, query_put, init_connection_message  # noqa: E402
from diff_oracle import Materializer, diff_states  # noqa: E402


def rid(rng: random.Random) -> str:
    return "art-upg-" + "".join(rng.choice("abcdefghijklmnop0123456789") for _ in range(10))


async def _connect_and_drive(target: str, version: int, auth_token: str | None,
                              extra_params: list[tuple[str, str]], puts: list[dict],
                              client_schema: dict | None, base_cookie: str,
                              duration_s: float, pks: dict, seed: int
                              ) -> tuple[Materializer, str | None, str | None]:
    """Connect, drive, materialize. Returns (materializer, error_kind, new_base_cookie)."""
    import websockets
    rng = random.Random(seed)
    cgid, cid = rid(rng), rid(rng)
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": base_cookie,
              "ts": str(time.time() * 1000), "lmid": "0"}
    params.update(extra_params)
    url = (target.rstrip("/") + f"/sync/v{version}/connect?"
           + urllib.parse.urlencode(params))
    sec = encode_sec_protocols(None, auth_token)
    mat = Materializer(pks)
    error_kind = None
    new_cookie = None
    try:
        async with websockets.connect(url, subprotocols=[sec], open_timeout=20,
                                       max_size=None, ping_interval=None) as ws:
            await ws.send(json.dumps(init_connection_message(
                puts, client_schema=client_schema)))
            deadline = time.perf_counter() + duration_s
            while time.perf_counter() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(msg, list) or not msg:
                    continue
                if msg[0] == "error":
                    body = msg[1] if len(msg) > 1 else {}
                    error_kind = body.get("kind") or str(body)
                    break
                if msg[0] == "poke":
                    body = msg[1] if len(msg) > 1 else {}
                    new_cookie = body.get("baseCookie") or new_cookie
                    for part in (body.get("pokeParts") or []):
                        mat.apply_rows_patch(part.get("rowsPatch"))
    except Exception as e:
        error_kind = f"connect:{type(e).__name__}"
    return mat, error_kind, new_cookie


async def probe(a: argparse.Namespace) -> dict:
    checks: list[dict] = []
    rng = random.Random(a.seed)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng)
    baseline = load_baseline(a.baseline)
    sampler = WeightedSampler(baseline.all_read_ops, rng)
    puts = [query_put(sampler.sample().name, resolver.resolve(sampler.sample())[0])
            for _ in range(a.queries)]
    # re-roll deterministically for a stable set
    rng = random.Random(a.seed)
    sampler2 = WeightedSampler(baseline.all_read_ops, rng)
    resolver2 = ArgResolver.from_pool_file(a.id_pool, rng)
    puts = [query_put(sampler2.sample().name, resolver2.resolve(sampler2.sample())[0])
            for _ in range(a.queries)]
    client_schema = json.load(open(a.client_schema)) if a.client_schema else None
    pks = {}
    if client_schema and isinstance(client_schema.get("tables"), list):
        for t in client_schema["tables"]:
            if t.get("tableName") and t.get("primaryKey"):
                pks[t["tableName"]] = t["primaryKey"]

    # 1. baseline image: drive + capture baseCookie (CVR state from OLD image)
    mat_old, err_old, cookie = await _connect_and_drive(
        a.baseline_target, a.protocol_version, a.auth_token, a.extra_param,
        puts, client_schema, "", a.duration_s, pks, a.seed)
    if err_old:
        checks.append({"name": "baseline-connect", "verdict": "ERROR",
                       "detail": f"baseline target errored: {err_old}"})
        return {"verdict": "ERROR", "checks": checks,
                "summary": f"baseline target unreachable: {err_old}"}
    if not cookie:
        checks.append({"name": "baseline-cvr", "verdict": "ERROR",
                       "detail": "baseline never returned a baseCookie (no CVR state captured)"})
        return {"verdict": "ERROR", "checks": checks,
                "summary": "no baseCookie from baseline — cannot test resume"}
    checks.append({"name": "baseline-cvr", "verdict": "PASS",
                   "detail": f"captured baseCookie from baseline image ({cookie[:16]}...)"})

    # 2. candidate image: RESUME with the old-image baseCookie
    mat_resumed, err_resume, _ = await _connect_and_drive(
        a.candidate_target, a.protocol_version, a.auth_token, a.extra_param,
        puts, client_schema, cookie, a.duration_s, pks, a.seed + 1)
    if err_resume:
        checks.append({"name": "resume-compat", "verdict": "FAIL",
                       "detail": f"candidate REJECTED old-image CVR: {err_resume} "
                                 f"— upgrade breaks existing clients"})
        return {"verdict": "FAIL", "checks": checks,
                "summary": f"CVR incompat: candidate rejected resume ({err_resume})"}
    checks.append({"name": "resume-compat", "verdict": "PASS",
                   "detail": "candidate accepted old-image baseCookie (resumed)"})

    # 3. candidate image: FRESH client (no cookie) — what a new client sees
    mat_fresh, err_fresh, _ = await _connect_and_drive(
        a.candidate_target, a.protocol_version, a.auth_token, a.extra_param,
        puts, client_schema, "", a.duration_s, pks, a.seed + 2)
    if err_fresh:
        checks.append({"name": "fresh-connect", "verdict": "ERROR",
                       "detail": f"candidate fresh connect errored: {err_fresh}"})
        return {"verdict": "ERROR", "checks": checks,
                "summary": f"candidate fresh connect failed: {err_fresh}"}
    checks.append({"name": "fresh-connect", "verdict": "PASS",
                   "detail": f"fresh client hydrated {mat_fresh.rows_applied} rows"})

    # 4. resumed state must equal fresh state (zero data loss on upgrade)
    d = diff_states(mat_resumed, mat_fresh, max_examples=10)
    total = d.get("total", 0)
    if total == 0:
        checks.append({"name": "data-loss", "verdict": "PASS",
                       "detail": "resumed state == fresh state (zero data loss on upgrade)"})
        summary = "upgrade path clean: resume + converge, zero data loss"
        verdict = "PASS"
    else:
        checks.append({"name": "data-loss", "verdict": "FAIL",
                       "detail": f"{total} row(s) differ between resumed and fresh "
                                 f"— DATA LOSS on image upgrade"})
        summary = f"UPGRADE BROKEN: {total} mismatches (data loss)"
        verdict = "FAIL"
    return {"verdict": verdict, "checks": checks, "summary": summary,
            "mismatches": total, "diff": d,
            "resumed_rows": mat_resumed.rows_applied, "fresh_rows": mat_fresh.rows_applied}


def main() -> int:
    ap = argparse.ArgumentParser(description="G24: image upgrade-path / CVR-compat test.")
    ap.add_argument("--baseline-target", required=True, help="reference image ws target")
    ap.add_argument("--candidate-target", required=True, help="new image ws target under test")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--baseline", default="art-baseline.json")
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--queries", type=int, default=6)
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    a.extra_param = [tuple(p.split("=", 1)) for p in a.extra_param]
    report = asyncio.run(probe(a))
    report.update({"schema": 1, "gate": "G24", "name": "upgrade-path",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    print(report["summary"])
    for c in report["checks"]:
        print(f"  {c['name']:<16} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
