#!/usr/bin/env python3
"""
matrix_oracle.py — exhaustive ADVANCEMENT matrix: every visible table x every
column x every data-type edge x insert/update/delete, byte-diffed Go-vs-TS.

Why direct PG writes and not custom mutators:
  * both pods share ONE backend, so mutator VALIDATION cannot diverge per-pod;
    the pod-differing surface is advancement (replication -> IVM -> poke)
  * the replication stream doesn't care who wrote — a psql UPDATE is
    indistinguishable from a mutator's write
  * 151 mined mutators have hand-built args for only 2; direct writes cover
    every replicated column type TODAY, including values no mutator emits
    (empty strings, unicode, 2^53+1 bigints, deep JSON, every enum label)

Method (per table with >=1 visible row):
  1. both sides subscribe the FULL query catalog (same hashes) and hydrate
  2. clone a hydrated sample row IN SQL (INSERT..SELECT — visibility inherited,
     no FK constraints in this schema)
  3. drive every client-visible column of the clone through typed edge values
     (one UPDATE per value = one replication txn = one IVM advancement each)
  4. DELETE the clone (remove-patch path)
  5. converge: poll until both materialized states are byte-identical twice in
     a row (background sandbox traffic makes "quiet" impossible; equality is
     the correct predicate) — timeout => persistent mismatch => FAIL + examples

Writes ONLY to clones (art-prefixed PKs); sample rows are never modified.
Requires --i-know-this-writes; refuses non-localhost targets w/o --allow-remote.

    .venv/bin/python harness/matrix_oracle.py \
        --primary ws://rust-test.localhost/zero \
        --mirror  ws://rust-test.localhost/zero-ts \
        --auth-token "$JWT" --extra-param userID=<uid> \
        --i-know-this-writes [--tables bookmarks,channels] [--max-tables N]

Exit 0 = all fuzzed tables converged identical; 1 = persistent mismatch;
2 = infra (connect failure / nothing fuzzable).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diff_oracle import (  # noqa: E402
    Materializer, Side, canon, connect_url, diff_states, reader,
)
from workload import (  # noqa: E402
    ArgResolver, change_desired_queries_message, init_connection_message,
    load_baseline, query_put,
)
from replay import DEFAULT_PROTOCOL_VERSION, encode_sec_protocols  # noqa: E402

ZERO_INTERNAL_PREFIX = "_"


# ---------------------------------------------------------------- SQL helpers
def sq(s: str) -> str:
    """Single-quoted SQL literal."""
    return "'" + str(s).replace("'", "''") + "'"


def qi(ident: str) -> str:
    """Quoted identifier (camelCase columns)."""
    return '"' + ident.replace('"', '""') + '"'


def psql(a, sql_text: str) -> tuple[int, str, str]:
    """Run statements via psql -f - : autocommit => one replication txn EACH."""
    p = subprocess.run(
        ["docker", "exec", "-i", a.pg_container, "psql", "-X", "-q",
         "-v", "ON_ERROR_STOP=0", "-U", a.pg_user, "-d", a.pg_db, "-f", "-"],
        input=sql_text, capture_output=True, text=True, timeout=120)
    return p.returncode, p.stdout, p.stderr


def psql_rows(a, query: str) -> list[str]:
    p = subprocess.run(
        ["docker", "exec", a.pg_container, "psql", "-X", "-At",
         "-U", a.pg_user, "-d", a.pg_db, "-c", query],
        capture_output=True, text=True, timeout=30)
    return [ln for ln in p.stdout.splitlines() if ln]


# ------------------------------------------------------------------ edge sets
LONG_STR = "A" * 280
UNICODE_STR = "汉字 🔥 émoji ñ ∞"
QUOTE_STR = "it's \"quoted\" \\back\\slash %s $1"
JSONISH_STR = '{"looks":"like json","n":1}'
DEEP_JSON = '{"a":{"b":[1,"x",null,true],"u":"汉🔥","n":9007199254740991}}'


def edges_for(data_type: str, udt: str, nullable: bool,
              enum_labels: dict[str, list[str]], a) -> list[str]:
    dt = data_type.lower()
    out: list[str] = []
    if dt in ("text", "character varying", "character", "citext"):
        out = [sq(""), sq("x"), sq(LONG_STR), sq(UNICODE_STR),
               sq(QUOTE_STR), sq(JSONISH_STR)]
    elif dt == "user-defined":
        labels = enum_labels.get(udt)
        if labels is None:
            labels = psql_rows(a, "SELECT e.enumlabel FROM pg_enum e JOIN "
                                  "pg_type t ON e.enumtypid=t.oid WHERE "
                                  f"t.typname={sq(udt)} ORDER BY e.enumsortorder")
            enum_labels[udt] = labels
        out = [sq(x) for x in labels[:8]]           # every real enum label
    elif dt in ("integer", "smallint"):
        out = ["0", "-1", "2147483647" if dt == "integer" else "32767"]
    elif dt == "bigint":
        # 2^53-1 / 2^53+1: the JS float-precision boundary. PG stores both
        # exactly; if Go(int64) and TS(number) serialize them differently on
        # the wire, the diff catches a REAL cross-engine format divergence.
        out = ["0", "-1", "9007199254740991", "9007199254740993"]
    elif dt in ("double precision", "real", "numeric"):
        out = ["0", "-1.5", "0.30000000000000004", "1e15"]
    elif dt == "boolean":
        out = ["true", "false"]
    elif dt.startswith("timestamp") or dt == "date":
        out = [sq("1970-01-01 00:00:00"), sq("2099-12-31 23:59:59")]
    elif dt in ("json", "jsonb"):
        out = [sq("{}") + "::jsonb", sq("[]") + "::jsonb",
               sq(DEEP_JSON) + "::jsonb"]
    elif dt == "uuid":
        out = [sq("00000000-0000-4000-8000-0000000000aa")]
    else:
        return []                                    # arrays/intervals: skip
    if nullable:
        out.append("NULL")
    return out


# ------------------------------------------------------------------ fuzz core
def new_clone_id() -> str:
    r = random.SystemRandom()
    return "artmx" + "".join(r.choice("abcdefghijklmnopqrstuvwxyz0123456789")
                             for _ in range(19))


def build_table_ops(a, table: str, spec: dict, sample_id: str,
                    enum_cache: dict) -> tuple[str | None, list[str], dict]:
    """Returns (clone_id, statements, per-column op counts)."""
    meta_rows = psql_rows(a, "SELECT column_name||'|'||data_type||'|'||"
                             "is_nullable||'|'||udt_name FROM "
                             "information_schema.columns WHERE "
                             f"table_schema='public' AND table_name={sq(table)}")
    meta = {}
    for ln in meta_rows:
        col, dt, nullable, udt = ln.split("|", 3)
        meta[col] = (dt, nullable == "YES", udt)
    if not meta:
        return None, [], {}

    pk = (spec.get("primaryKey") or ["id"])[0]
    pk_dt = meta.get(pk, ("", False, ""))[0].lower()
    if pk_dt not in ("text", "character varying", "citext"):
        return None, [], {}                          # non-text PK: not clonable

    clone_id = new_clone_id()
    cols = [c for c in meta if c != pk]
    stmts = [f"INSERT INTO {qi(table)} ({qi(pk)}, "
             + ", ".join(qi(c) for c in cols)
             + f") SELECT {sq(clone_id)}, "
             + ", ".join(qi(c) for c in cols)
             + f" FROM {qi(table)} WHERE {qi(pk)}={sq(sample_id)};"]

    per_col: dict[str, int] = {}
    client_cols = spec.get("columns", {})
    for col, (dt, nullable, udt) in meta.items():
        if col == pk or col.startswith(ZERO_INTERNAL_PREFIX):
            continue
        if col not in client_cols:
            continue                                 # invisible to clients
        vals = edges_for(dt, udt, nullable, enum_cache, a)
        for lit in vals:
            stmts.append(f"UPDATE {qi(table)} SET {qi(col)}={lit} "
                         f"WHERE {qi(pk)}={sq(clone_id)};")
        if vals:
            per_col[col] = len(vals)
    stmts.append(f"DELETE FROM {qi(table)} WHERE {qi(pk)}={sq(clone_id)};")
    return clone_id, stmts, per_col


async def converge(sides: list[Side], timeout_s: float,
                   poll_s: float = 1.5) -> tuple[bool, float]:
    """Equal-canon twice in a row => converged (never-quiet-proof)."""
    t0 = time.perf_counter()
    streak = 0
    while time.perf_counter() - t0 < timeout_s:
        await asyncio.sleep(poll_s)
        if canon(sides[0].mat.state) == canon(sides[1].mat.state):
            streak += 1
            if streak >= 2:
                return True, time.perf_counter() - t0
        else:
            streak = 0
    return False, time.perf_counter() - t0


async def amain(a: argparse.Namespace) -> int:
    import websockets

    if not a.i_know_this_writes:
        print("REFUSING: matrix mode writes to the DB. Pass --i-know-this-writes "
              "(sandbox only).", file=sys.stderr)
        return 2
    for t in (a.primary, a.mirror):
        if "localhost" not in t and "127.0.0.1" not in t and not a.allow_remote:
            print(f"REFUSING non-local target {t} without --allow-remote",
                  file=sys.stderr)
            return 2

    baseline = load_baseline(a.baseline)
    rng = random.Random(a.seed)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng, zipf_s=0.0)
    cschema = json.load(open(a.client_schema))
    tables_spec = cschema.get("tables", {})
    pks = {t: s.get("primaryKey", []) for t, s in tables_spec.items()}

    sides = [Side(a.primary, Materializer(pks)),
             Side(a.mirror, Materializer(pks))]
    idrng = random.SystemRandom()
    for s in sides:
        s.cgid = "artmx-" + "".join(idrng.choice("abcdef0123456789") for _ in range(10))
        s.cid = "artmx-" + "".join(idrng.choice("abcdef0123456789") for _ in range(10))

    extra = [tuple(p.split("=", 1)) for p in (a.extra_param or [])]
    sec = encode_sec_protocols(None, a.auth_token)
    init_msg = init_connection_message([], client_schema=cschema)
    stop = asyncio.Event()
    try:
        for s in sides:
            url = connect_url(s.target, s.cgid, s.cid, extra, a.protocol_version)
            s.ws = await websockets.connect(url, subprotocols=[sec],
                                            open_timeout=20, max_size=None,
                                            ping_interval=None)
            await s.ws.send(json.dumps(init_msg))
    except Exception as e:
        print(f"INFRA: connect failed: {e}", file=sys.stderr)
        return 2
    readers = [asyncio.create_task(reader(s, stop)) for s in sides]

    # ---- MAP: desire the ENTIRE catalog (same hashes both sides) ----
    puts, unresolvable_names = [], set()
    for op in baseline.queries:
        args, ok = resolver.resolve(op)
        if not ok:
            unresolvable_names.add(op.name)
            continue
        puts.append(query_put(op.name, args, ttl_ms=3_600_000))
    print(f"MAP: desiring {len(puts)}/{len(baseline.queries)} catalog queries "
          f"({len(unresolvable_names)} unresolvable args) on both pods")
    for i in range(0, len(puts), 25):
        msg = json.dumps(change_desired_queries_message(puts[i:i + 25]))
        for s in sides:
            await s.ws.send(msg)
        await asyncio.sleep(0.4)

    hyd_deadline = time.perf_counter() + a.hydrate_max_s
    while time.perf_counter() < hyd_deadline:
        await asyncio.sleep(1.0)
        if min(time.perf_counter() - s.last_activity for s in sides) >= 5.0:
            break
    ok0, _ = await converge(sides, 30)
    visible = {t: rows for t, rows in sides[0].mat.state.items() if rows}
    print(f"MAP: hydrated {sides[0].mat.rows_applied} rows across "
          f"{len(visible)} visible tables (pre-fuzz states equal: {ok0})")

    # ---- dark-table attribution via the source impact matrix ----
    # (tools/gen_impact_matrix.sh, adopted from staging-regression) Without
    # this, "78 tables dark" is one undifferentiated bucket; with the static
    # table->queries map every dark table gets a actionable cause:
    #   no-covering-query          no query in the source reads it — only DB
    #                              seed + new queries (or multi-user) can help
    #   covering-args-unresolvable all covering queries were skipped by the
    #                              ArgResolver — fix = id-pool mapping entries
    #   covered-but-zero-rows      we desired a covering query and it hydrated
    #                              nothing — fix = seed rows / identity access
    #   not-in-catalog             query exists in source but not in the mined
    #                              baseline — fix = refresh art-baseline.json
    dark_attr: dict[str, dict] = {}
    if a.impact and os.path.exists(a.impact):
        imp = json.load(open(a.impact))
        tbl_q = {t["table"]: t["queries"] for t in imp.get("tables", [])}
        desired_names = {p["name"] for p in puts}
        catalog_names = {op.name for op in baseline.queries}
        for t in sorted(set(tables_spec) - set(visible)):
            cov = tbl_q.get(t, [])
            if not cov:
                cause = "no-covering-query"
            elif any(q in desired_names for q in cov):
                cause = "covered-but-zero-rows"
            elif any(q in catalog_names for q in cov):
                cause = "covering-args-unresolvable"
            else:
                cause = "not-in-catalog"
            dark_attr[t] = {"cause": cause, "covering_queries": cov[:8]}
        by_cause: dict[str, int] = {}
        for v in dark_attr.values():
            by_cause[v["cause"]] = by_cause.get(v["cause"], 0) + 1
        print("DARK TABLES: " + ", ".join(f"{k}={v}" for k, v in
                                          sorted(by_cause.items(), key=lambda kv: -kv[1])))

    # ---- FUZZ ----
    want = ([t.strip() for t in a.tables.split(",")] if a.tables
            else sorted(visible))
    enum_cache: dict[str, list[str]] = {}
    results, cleanup = [], []
    fuzzed = mismatched = 0
    skipped_pk, skipped_norows = [], []
    try:
        for table in want:
            if a.max_tables and fuzzed >= a.max_tables:
                break
            if table not in visible:
                skipped_norows.append(table)
                continue
            spec = tables_spec.get(table) or {}
            pk = (spec.get("primaryKey") or ["id"])[0]
            sample = next(iter(visible[table].values()))
            sample_id = sample.get(pk)
            if not isinstance(sample_id, str):
                skipped_pk.append(table)
                continue
            clone_id, stmts, per_col = build_table_ops(
                a, table, spec, sample_id, enum_cache)
            if clone_id is None or len(stmts) <= 2:
                skipped_pk.append(table)
                continue
            cleanup.append((table, pk, clone_id))
            t0 = time.perf_counter()
            rc, out, err = await asyncio.to_thread(psql, a, "\n".join(stmts))
            rejected = err.count("ERROR:")
            okc, dt = await converge(sides, a.converge_timeout_s)
            fuzzed += 1
            rec = {"table": table, "ops": len(stmts), "rejected": rejected,
                   "columns_fuzzed": len(per_col), "converged": okc,
                   "converge_s": round(dt, 1),
                   "wall_s": round(time.perf_counter() - t0, 1)}
            if not okc:
                mismatched += 1
                rec["diff"] = diff_states(sides[0].mat, sides[1].mat)
            results.append(rec)
            print(f"  {table:38s} ops={len(stmts):4d} rej={rejected:3d} "
                  f"cols={len(per_col):3d} "
                  f"{'CONVERGED' if okc else 'PERSISTENT-MISMATCH'} "
                  f"({dt:.1f}s)")
    finally:
        if cleanup:
            sql = "\n".join(f"DELETE FROM {qi(t)} WHERE {qi(pk)}={sq(cid)};"
                            for t, pk, cid in cleanup)
            await asyncio.to_thread(psql, a, sql)
        stop.set()
        for r in readers:
            r.cancel()
        for s in sides:
            try:
                await s.ws.close()
            except Exception:
                pass

    report = {
        "primary": a.primary, "mirror": a.mirror,
        "when": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "catalog_queries_desired": len(puts),
        "visible_tables": len(visible),
        "tables_fuzzed": fuzzed,
        "tables_mismatched": mismatched,
        "tables_skipped_no_rows": skipped_norows,
        "tables_skipped_unclonable_pk": skipped_pk,
        "dark_table_attribution": dark_attr,
        "total_ops": sum(r["ops"] for r in results),
        "total_rejected": sum(r["rejected"] for r in results),
        "results": results,
        "verdict": "FAIL" if mismatched else ("PASS" if fuzzed else "INFRA"),
    }
    out_path = a.out or os.path.join(
        os.path.dirname(__file__), "..", "reports",
        "matrix-" + time.strftime("%Y%m%d-%H%M%S") + ".json")
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nMATRIX ORACLE: {report['verdict']} — {fuzzed} tables, "
          f"{report['total_ops']} ops ({report['total_rejected']} rejected by PG), "
          f"{mismatched} persistent mismatches -> {out_path}")
    return 0 if report["verdict"] == "PASS" else (1 if mismatched else 2)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary", required=True)
    ap.add_argument("--mirror", required=True)
    ap.add_argument("--auth-token", required=True)
    ap.add_argument("--extra-param", action="append", default=[])
    ap.add_argument("--baseline", default=os.path.join(
        os.path.dirname(__file__), "..", "art-baseline.json"))
    ap.add_argument("--id-pool", default=os.path.join(
        os.path.dirname(__file__), "id-pool.sandbox.json"))
    ap.add_argument("--client-schema", default=os.path.join(
        os.path.dirname(__file__), "client-schema.json"))
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--pg-user", default="xyne")
    ap.add_argument("--pg-db", default="sandbox_rust_test_db")
    ap.add_argument("--tables", default=None,
                    help="comma list; default = every visible table")
    ap.add_argument("--impact", default=os.path.join(
        os.path.dirname(__file__), "..", "raw", "query-mutator-impact.json"),
                    help="query-mutator impact matrix (tools/gen_impact_matrix.sh) "
                         "for dark-table attribution; skipped if missing")
    ap.add_argument("--max-tables", type=int, default=0)
    ap.add_argument("--hydrate-max-s", type=float, default=90.0)
    ap.add_argument("--converge-timeout-s", type=float, default=60.0)
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default=None)
    ap.add_argument("--i-know-this-writes", action="store_true")
    ap.add_argument("--allow-remote", action="store_true")
    return asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
