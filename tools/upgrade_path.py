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
import copy
import json
import os
import random
import sys
import time
import urllib.parse
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "harness"))
from protocol import DEFAULT_PROTOCOL_VERSION, encode_sec_protocols  # noqa: E402
from workload import (  # noqa: E402
    ArgResolver, SchemaSynthesizer, WeightedSampler, init_connection_message,
    load_baseline, query_put,
)
from diff_oracle import Materializer, diff_states  # noqa: E402


def rid(rng: random.Random) -> str:
    return "art-upg-" + "".join(rng.choice("abcdefghijklmnop0123456789") for _ in range(10))


async def _connect_and_drive(target: str, version: int, auth_token: str | None,
                              extra_params: list[tuple[str, str]], puts: list[dict],
                              client_schema: dict | None, base_cookie: str,
                              duration_s: float, pks: dict, seed: int,
                              client_group_id: str,
                              initial_state: Materializer | None = None,
                              ) -> tuple[Materializer, str | None, str | None]:
    """Connect, drive, materialize. Returns (materializer, error_kind, new_base_cookie)."""
    import websockets
    rng = random.Random(seed)
    cgid, cid = client_group_id, rid(rng)
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": base_cookie,
              "ts": str(time.time() * 1000), "lmid": "0"}
    params.update(extra_params)
    url = (target.rstrip("/") + f"/sync/v{version}/connect?"
           + urllib.parse.urlencode(params))
    sec = encode_sec_protocols(None, auth_token)
    mat = copy.deepcopy(initial_state) if initial_state is not None else Materializer(pks)
    starting_rows = mat.rows_applied
    error_kind = None
    new_cookie = None
    completed_poke = False
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
                tag = msg[0]
                body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
                if tag == "error":
                    error_kind = body.get("kind") or str(body)
                    break
                if tag == "transformError":
                    query_name = body.get("queryName") or body.get("name") or "unknown"
                    error_kind = f"transformError:{query_name}"
                    break
                if tag == "pokePart":
                    mat.apply_rows_patch(body.get("rowsPatch"))
                elif tag == "pokeEnd":
                    if not body.get("cancel"):
                        new_cookie = body.get("cookie") or new_cookie
                        completed_poke = True
                        if initial_state is not None or mat.rows_applied > starting_rows:
                            break
                elif tag == "poke":
                    # Older protocol compatibility for historical reference images.
                    new_cookie = body.get("baseCookie") or new_cookie
                    for part in (body.get("pokeParts") or []):
                        mat.apply_rows_patch(part.get("rowsPatch"))
                    completed_poke = True
                    if initial_state is not None or mat.rows_applied > starting_rows:
                        break
    except Exception as e:
        error_kind = f"connect:{type(e).__name__}"
    if error_kind is None and not completed_poke:
        error_kind = "incomplete:no-poke-end"
    return mat, error_kind, new_cookie


async def probe(a: argparse.Namespace) -> dict:
    checks: list[dict] = []
    rng = random.Random(a.seed)
    baseline = load_baseline(a.baseline)
    schema_doc = None
    if a.current_query_schema and os.path.exists(a.current_query_schema):
        with open(a.current_query_schema) as f:
            schema_doc = json.load(f)
        current = schema_doc.get("queries") or {}
        baseline.queries = [op for op in baseline.queries if op.name in current]
        baseline.oneshots = [op for op in baseline.oneshots if op.name in current]
    sampler = WeightedSampler(baseline.all_read_ops, rng)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng)
    query_synth = None
    if schema_doc is not None:
        minimal_queries = {}
        for name, entry in (schema_doc.get("queries") or {}).items():
            args_schema = entry.get("args")
            if args_schema is None:
                minimal_queries[name] = {**entry, "args": {}}
                continue
            minimal_queries[name] = {
                **entry,
                "args": {key: schema for key, schema in args_schema.items()
                         if not schema.get("optional") and not schema.get("hasDefault")},
            }
        query_synth = SchemaSynthesizer(
            {"mutators": minimal_queries,
             "enums": schema_doc.get("enums") or {}},
            resolver.ids, resolver.scalars, {}, rng,
        )
    puts = []
    query_names = []
    seen_hashes = set()
    attempts = 0
    while len(puts) < a.queries and attempts < a.queries * 100:
        attempts += 1
        op = sampler.sample()
        if query_synth is not None:
            args, _ = query_synth.synth(op.name, int(time.time() * 1000))
            ok = args is not None
        else:
            args, ok = resolver.resolve(op)
        if not ok:
            continue
        put = query_put(op.name, args)
        if put["hash"] in seen_hashes:
            continue
        seen_hashes.add(put["hash"])
        puts.append(put)
        query_names.append(op.name)
    if len(puts) != a.queries:
        return {"verdict": "ERROR", "checks": [{
            "name": "setup", "verdict": "ERROR",
            "detail": f"resolved only {len(puts)}/{a.queries} distinct queries",
        }], "summary": "upgrade setup could not build a complete query set"}
    client_schema = json.load(open(a.client_schema)) if a.client_schema else None
    pks = {}
    if client_schema:
        tables = client_schema.get("tables")
        if isinstance(tables, dict):
            pks = {name: spec.get("primaryKey", [])
                   for name, spec in tables.items() if spec.get("primaryKey")}
        elif isinstance(tables, list):
            for table in tables:
                name = table.get("tableName")
                pk = table.get("primaryKey")
                if name and pk:
                    pks[name] = pk

    shared_cgid = "art-upg-" + uuid.uuid4().hex[:16]
    fresh_cgid = "art-upg-" + uuid.uuid4().hex[:16]

    # 1. baseline image: drive + capture baseCookie (CVR state from OLD image)
    mat_old, err_old, cookie = await _connect_and_drive(
        a.baseline_target, a.protocol_version, a.auth_token, a.extra_param,
        puts, client_schema, "", a.duration, pks, a.seed, shared_cgid)
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
    if mat_old.rows_applied == 0:
        checks.append({"name": "baseline-cvr", "verdict": "FAIL",
                       "detail": "baseline completed but materialized zero rows"})
        return {"verdict": "FAIL", "checks": checks,
                "summary": "upgrade run produced no usable baseline row corpus",
                "queries": query_names}
    checks.append({"name": "baseline-cvr", "verdict": "PASS",
                   "detail": f"captured baseCookie from baseline image ({cookie[:16]}...)"})

    # 2. candidate image: RESUME with the old-image baseCookie
    mat_resumed, err_resume, _ = await _connect_and_drive(
        a.candidate_target, a.protocol_version, a.auth_token, a.extra_param,
        puts, client_schema, cookie, a.duration, pks, a.seed, shared_cgid,
        initial_state=mat_old)
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
        puts, client_schema, "", a.duration, pks, a.seed + 2, fresh_cgid)
    if err_fresh:
        checks.append({"name": "fresh-connect", "verdict": "ERROR",
                       "detail": f"candidate fresh connect errored: {err_fresh}"})
        return {"verdict": "ERROR", "checks": checks,
                "summary": f"candidate fresh connect failed: {err_fresh}"}
    if mat_fresh.rows_applied == 0:
        checks.append({"name": "fresh-connect", "verdict": "FAIL",
                       "detail": "candidate completed but materialized zero rows"})
        return {"verdict": "FAIL", "checks": checks,
                "summary": "upgrade run produced no usable candidate row corpus",
                "queries": query_names}
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
            "resumed_rows": mat_resumed.rows_applied, "fresh_rows": mat_fresh.rows_applied,
            "queries": query_names}


def main() -> int:
    ap = argparse.ArgumentParser(description="G24: image upgrade-path / CVR-compat test.")
    ap.add_argument("--baseline-target", required=True, help="reference image ws target")
    ap.add_argument("--candidate-target", required=True, help="new image ws target under test")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--current-query-schema", default="raw/arg-schemas.source.json")
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
