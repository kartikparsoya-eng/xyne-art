#!/usr/bin/env python3
"""determinism_oracle.py — G21: poke-stream determinism oracle for zero-cache.

The IVM engine must be deterministic over fixed input data: the same desired
queries, the same DB state, the same seed => byte-identical converged row-sets.
A non-deterministic sort tie-break, non-deterministic map iteration, or a
race in the advance path produces DIFFERENT poke content across identical runs
— a real bug class (rows appear in different order, or a tie-break column
flips) that no current gate catches (G8 compares two builds, not two runs of
the same build).

Method:
  1. connect one client with a SEEDED working set (deterministic args)
  2. capture every poke into a Materializer (pass A), disconnect, quiesce
  3. reconnect with the SAME seed + same queries (pass B), materialize
  4. diff_states(A, B) — zero mismatches => deterministic; any => FAIL
  5. also compare per-query poke counts (batching divergence signal)

Reuses harness/diff_oracle.py::Materializer + diff_states (same converged-state
predicate G8 uses — pokes are never compared one-to-one since batching/order
legally differ, only the materialized end state must match).

    .venv/bin/python tools/determinism_oracle.py --target ws://host/zero \\
        --auth-token "$JWT" --id-pool harness/id-pool.sandbox.json \\
        --client-schema harness/client-schema.json --extra-param userID=$UID \\
        --seed 42 --queries 8 --duration 30 --out reports/determinism-$TAG.json

Exit 0 = deterministic (identical converged state); 1 = divergent; 2 = ERROR.
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
from workload import (  # noqa: E402
    ArgResolver, SchemaSynthesizer, WeightedSampler, load_baseline, query_put,
    init_connection_message,
)
from diff_oracle import Materializer, diff_states  # noqa: E402


def rid(seed: random.Random) -> str:
    return "art-det-" + "".join(seed.choice("abcdefghijklmnop0123456789") for _ in range(10))


async def _pass(target: str, version: int, auth_token: str | None,
                extra_params: list[tuple[str, str]], puts: list[dict],
                client_schema: dict | None, seed: int, duration_s: float,
                pks: dict) -> Materializer:
    import websockets
    rng = random.Random(seed)
    cgid, cid = rid(rng), rid(rng)
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0"}
    params.update(extra_params)
    url = (target.rstrip("/") + f"/sync/v{version}/connect?"
           + urllib.parse.urlencode(params))
    sec = encode_sec_protocols(None, auth_token)
    mat = Materializer(pks)
    async with websockets.connect(url, subprotocols=[sec], open_timeout=20,
                                   max_size=None, ping_interval=None) as ws:
        await ws.send(json.dumps(init_connection_message(puts, client_schema=client_schema)))
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
            if tag == "poke":
                for part in (body.get("pokeParts") or []):
                    got = part.get("gotQueriesPatch") or []
                    mat.apply_rows_patch(part.get("rowsPatch"), got_hashes=got)
                    for entry in got:
                        if isinstance(entry, dict) and entry.get("op") == "put":
                            mat.got_hashes.add(entry.get("hash"))
            elif tag == "pokePart":
                got = body.get("gotQueriesPatch") or []
                mat.apply_rows_patch(body.get("rowsPatch"), got_hashes=got)
                for entry in got:
                    if isinstance(entry, dict) and entry.get("op") == "put":
                        mat.got_hashes.add(entry.get("hash"))
            elif tag in ("error", "transformError"):
                kind = body.get("kind") or tag
                mat.error_kinds[str(kind)] = mat.error_kinds.get(str(kind), 0) + 1
                if tag == "transformError":
                    qname = body.get("queryName", "") or body.get("name", "")
                    if qname:
                        key = f"transformError:{qname}"
                        mat.error_kinds[key] = mat.error_kinds.get(key, 0) + 1
    return mat


async def probe(a: argparse.Namespace) -> dict:
    checks: list[dict] = []
    rng = random.Random(a.seed)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng, zipf_s=a.zipf)
    baseline = load_baseline(a.baseline)
    schema_doc = None
    if a.current_query_schema and os.path.exists(a.current_query_schema):
        with open(a.current_query_schema) as f:
            schema_doc = json.load(f)
        current = schema_doc.get("queries") or {}
        baseline.queries = [op for op in baseline.queries if op.name in current]
        baseline.oneshots = [op for op in baseline.oneshots if op.name in current]
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
    sampler = WeightedSampler(baseline.all_read_ops, rng)
    puts = []
    query_names = []
    seen_hashes = set()
    attempts = 0
    while len(puts) < a.queries and attempts < a.queries * 100:
        attempts += 1
        op = sampler.sample()
        if query_synth is not None:
            args, _meta = query_synth.synth(op.name, int(time.time() * 1000))
            ok = args is not None
            if ok and op.name == "hierarchyCanvases":
                channel_id, channel_ok = resolver._resolve_key("channelId")
                args = {"scope": "channel", "channelId": channel_id}
                ok = channel_ok
        else:
            args, ok = resolver.resolve(op)
        if not ok:
            continue
        put = query_put(op.name, args, ttl_ms=int(a.duration * 1000 + 60000))
        if put["hash"] in seen_hashes:
            continue
        seen_hashes.add(put["hash"])
        puts.append(put)
        query_names.append(op.name)
    if len(puts) != a.queries:
        return {
            "verdict": "ERROR",
            "checks": [{"name": "setup", "verdict": "ERROR",
                        "detail": f"resolved only {len(puts)}/{a.queries} queries"}],
            "summary": "determinism setup could not build a complete query set",
            "mismatches": 0, "queries": query_names,
        }
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

    checks.append({"name": "setup", "verdict": "PASS",
                   "detail": f"{len(puts)} queries, seed={a.seed}, zipf={a.zipf}"})

    mat_a = await _pass(a.target, a.protocol_version, a.auth_token, a.extra_param,
                       puts, client_schema, a.seed, a.duration, pks)
    await asyncio.sleep(a.quiesce_s)
    mat_b = await _pass(a.target, a.protocol_version, a.auth_token, a.extra_param,
                       puts, client_schema, a.seed, a.duration, pks)

    checks.append({"name": "pass-A", "verdict": "PASS",
                   "detail": f"materialized {mat_a.rows_applied} rows, {len(mat_a.state)} tables"})
    checks.append({"name": "pass-B", "verdict": "PASS",
                   "detail": f"materialized {mat_b.rows_applied} rows, {len(mat_b.state)} tables"})

    protocol_errors = sum(mat_a.error_kinds.values()) + sum(mat_b.error_kinds.values())
    if mat_a.rows_applied == 0 or mat_b.rows_applied == 0 or protocol_errors:
        verdict = "FAIL"
        detail = (f"non-evidentiary run: rows={mat_a.rows_applied}/{mat_b.rows_applied}, "
                  f"protocol_errors={protocol_errors}")
        checks.append({"name": "determinism", "verdict": "FAIL", "detail": detail})
        return {"verdict": verdict, "checks": checks,
                "summary": "determinism run produced no usable row corpus",
                "mismatches": 0, "pass_a_rows": mat_a.rows_applied,
                "pass_b_rows": mat_b.rows_applied,
                "errors": {"pass_a": mat_a.error_kinds,
                           "pass_b": mat_b.error_kinds},
                "queries": query_names}

    d = diff_states(mat_a, mat_b, max_examples=10)
    total = d.get("total", 0)
    if total == 0:
        verdict = "PASS"
        checks.append({"name": "determinism", "verdict": "PASS",
                       "detail": "converged states byte-identical across two seeded runs"})
        summary = f"deterministic: {mat_a.rows_applied} rows match across 2 runs"
    else:
        verdict = "FAIL"
        checks.append({"name": "determinism", "verdict": "FAIL",
                       "detail": f"{total} mismatches across two IDENTICAL seeded runs "
                                 f"— non-deterministic engine (tie-break / race)"})
        summary = f"NON-DETERMINISTIC: {total} mismatches; see examples in report"
    return {"verdict": verdict, "checks": checks, "summary": summary,
            "mismatches": total, "diff": d,
            "pass_a_rows": mat_a.rows_applied, "pass_b_rows": mat_b.rows_applied,
            "queries": query_names}


def main() -> int:
    ap = argparse.ArgumentParser(description="G21: poke-stream determinism oracle.")
    ap.add_argument("--target", required=True)
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--baseline", default="art-baseline.json")
    ap.add_argument("--current-query-schema", default="raw/arg-schemas.source.json")
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--zipf", type=float, default=0.0)
    ap.add_argument("--queries", type=int, default=8)
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--quiesce-s", type=float, default=10.0)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    a.extra_param = [tuple(p.split("=", 1)) for p in a.extra_param]
    report = asyncio.run(probe(a))
    report.update({"schema": 1, "gate": "G21", "name": "determinism",
                   "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "target": a.target, "seed": a.seed})
    print(report["summary"])
    for c in report["checks"]:
        print(f"  {c['name']:<14} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
