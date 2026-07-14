#!/usr/bin/env python3
"""
diff_oracle.py — differential correctness oracle for zero-cache builds.

Drives IDENTICAL desired-query traffic at two zero-cache targets that sync the
same upstream DB, materializes each side's rowsPatch stream into converged
row-sets, and diffs them after a quiesce period. Same DB + same queries + same
auth => identical converged state, regardless of implementation. Pokes are
never compared one-to-one (batching/ordering legally differ) — only the
materialized end state must match.

Self-diff smoke test (proves the comparator has no false positives — both
sockets hit the SAME server):

    python3 harness/diff_oracle.py \
        --primary ws://rust-test.localhost/zero \
        --id-pool harness/id-pool.sandbox.json \
        --client-schema harness/client-schema.json \
        --auth-token "$JWT" --extra-param userID=$UID \
        --pairs 2 --duration 30

Real oracle run (TS reference vs Go candidate, once the TS sandbox exists):

    ... --primary ws://rust-test.localhost/zero \
        --mirror  ws://rust-test.localhost/zero-ts ...

Exit 0 = converged states identical; 1 = mismatches (see reports/diff-*.json).

Mutations (optional, off by default): --enable-mutations --i-know-this-writes
drives read-tracking custom mutations on the PRIMARY socket only. The write
lands in the shared upstream DB and replicates to BOTH caches, so converged
states must still match — this extends coverage from hydration-only to the
advance/invalidation poke paths. Bump --quiesce-s (>= 20s recommended) so the
last write replicates to both sides before diffing. Without those flags the
oracle is reads-only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from workload import (  # noqa: E402
    ArgResolver, WeightedSampler, MutationSampler, load_baseline,
    query_put, query_del, change_desired_queries_message, init_connection_message,
    custom_mutation, push_message,
)
from protocol import encode_sec_protocols, DEFAULT_PROTOCOL_VERSION  # noqa: E402


def canon(v: Any) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)


def _diff_columns(primary_row: dict, mirror_row: dict) -> list[dict]:
    """Column-level diff between two rows. Returns list of {column, primary, mirror}."""
    cols = sorted(set(primary_row) | set(mirror_row))
    diffs = []
    for c in cols:
        pv, mv = primary_row.get(c), mirror_row.get(c)
        if canon(pv) != canon(mv):
            diffs.append({"column": c, "primary": pv, "mirror": mv})
    return diffs


# --------------------------------------------------------------------------- #
class Materializer:
    """Applies pokePart rowsPatch ops into state[table][pk] = row.

    Also tracks query->table attribution (which desired query produced rows
    for which table), per-query hydration latency, and per-table row counts
    for diagnostic dumps.
    """

    def __init__(self, pks: dict[str, list[str]]):
        self.pks = pks                       # table -> primaryKey columns
        self.state: dict[str, dict[str, dict]] = {}
        self.got_hashes: set[str] = set()
        self.error_kinds: dict[str, int] = {}
        self.unknown_ops: dict[str, int] = {}
        self.rows_applied = 0
        # diagnostics
        self.query_tables: dict[str, set[str]] = {}   # query_name -> {table_names}
        self.hash_query: dict[str, str] = {}             # hash -> query_name (for attribution)
        self.query_latency: dict[str, list[float]] = {}  # query_name -> [hydrate_ms]
        self.table_row_count: dict[str, int] = {}        # table -> total rows seen
        self.zero_result_tables: set[str] = set()         # tables that got 0 rows
        self.streaming_queries: set[str] = set()         # queries still streaming at quiesce

    def _key(self, table: str, obj: dict) -> str:
        pk = self.pks.get(table)
        if pk:
            return canon([obj.get(c) for c in pk])
        return canon(obj)  # unknown table: whole row is the key

    def apply_rows_patch(self, patch: list, got_hashes: list = None,
                         poke_start_time: float = None) -> None:
        tables_in_this_poke: set[str] = set()
        for op in patch or []:
            if not isinstance(op, dict):
                continue
            kind = op.get("op")
            table = op.get("tableName", "?")
            tables_in_this_poke.add(table)
            rows = self.state.setdefault(table, {})
            if kind == "put":
                val = op.get("value") or {}
                rows[self._key(table, val)] = val
                self.rows_applied += 1
                self.table_row_count[table] = self.table_row_count.get(table, 0) + 1
            elif kind == "update":
                rid = op.get("id") or {}
                k = self._key(table, rid)
                merged = dict(rows.get(k) or rid)
                merged.update(op.get("merge") or {})
                constrain = op.get("constrain")
                if constrain:
                    merged = {c: merged.get(c) for c in constrain}
                rows[k] = merged
                self.rows_applied += 1
            elif kind == "del":
                rid = op.get("id") or {}
                rows.pop(self._key(table, rid), None)
                self.rows_applied += 1
            elif kind == "clear":
                self.state.clear()
            else:
                self.unknown_ops[str(kind)] = self.unknown_ops.get(str(kind), 0) + 1
        # attribute tables to queries via gotQueriesPatch hashes
        if got_hashes:
            for gh in got_hashes:
                if isinstance(gh, dict) and gh.get("op") == "put":
                    h = gh.get("hash")
                    qname = self.hash_query.get(h)
                    if qname and tables_in_this_poke:
                        self.query_tables.setdefault(qname, set()).update(tables_in_this_poke)
                        if poke_start_time is not None:
                            elapsed = round((time.perf_counter() - poke_start_time) * 1000, 1)
                            self.query_latency.setdefault(qname, []).append(elapsed)


def diff_states(a: Materializer, b: Materializer, max_examples: int = 3) -> dict:
    """Diff two converged states. Returns {mismatches, per_table, examples,
    column_diffs, zero_result_dumps}.

    Enchanced diagnostics:
    - Sample mismatch rows with actual values (not just counts)
    - Column-level diff for value_mismatch rows (which columns differ)
    - First-row dump for 0-result tables (one side has rows, other doesn't)
    """
    per_table: dict[str, dict] = {}
    examples: list[dict] = []
    column_diffs: list[dict] = []
    zero_dumps: list[dict] = []
    total = 0
    for table in sorted(set(a.state) | set(b.state)):
        ra, rb = a.state.get(table, {}), b.state.get(table, {})
        only_a = [k for k in ra if k not in rb]
        only_b = [k for k in rb if k not in ra]
        differ = [k for k in ra if k in rb and canon(ra[k]) != canon(rb[k])]
        n = len(only_a) + len(only_b) + len(differ)
        if n == 0:
            continue
        total += n
        per_table[table] = {"only_primary": len(only_a), "only_mirror": len(only_b),
                            "value_mismatch": len(differ)}
        # sample rows: only_primary
        for k in only_a[:max_examples]:
            examples.append({"table": table, "kind": "only_primary", "key": k,
                             "primary": ra[k], "mirror": None})
        # sample rows: only_mirror
        for k in only_b[:max_examples]:
            examples.append({"table": table, "kind": "only_mirror", "key": k,
                             "primary": None, "mirror": rb[k]})
        # sample rows: value_mismatch + column-level diff
        for k in differ[:max_examples]:
            prow, mrow = ra[k], rb[k]
            examples.append({"table": table, "kind": "value_mismatch", "key": k,
                             "primary": prow, "mirror": mrow})
            cd = _diff_columns(prow, mrow)
            if cd:
                column_diffs.append({"table": table, "key": k, "columns": cd})
        # zero-result dump: one side has rows, other doesn't — dump first row
        if only_a and not rb:
            first_k = only_a[0]
            zero_dumps.append({"table": table, "side": "primary-only",
                              "first_row": ra[first_k], "count_primary": len(ra),
                              "count_mirror": 0})
        elif only_b and not ra:
            first_k = only_b[0]
            zero_dumps.append({"table": table, "side": "mirror-only",
                              "first_row": rb[first_k], "count_primary": 0,
                              "count_mirror": len(rb)})
    return {"mismatches": total, "per_table": per_table,
            "examples": examples, "column_diffs": column_diffs,
            "zero_result_dumps": zero_dumps}


# --------------------------------------------------------------------------- #
@dataclass
class Side:
    """One socket to one target, with its own identity and materializer."""
    target: str
    mat: Materializer
    ws: Any = None
    cgid: str = ""
    cid: str = ""
    open_ok: bool = False
    pokes: int = 0
    lmid_acked: int = 0
    last_activity: float = field(default_factory=time.perf_counter)


def connect_url(target: str, cgid: str, cid: str, extra: list[tuple[str, str]],
                pv: int) -> str:
    import urllib.parse
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": "",
              "ts": str(time.time() * 1000), "lmid": "0",
              "wsid": uuid.uuid4().hex[:12]}
    for k, v in extra:
        params[k] = v
    return (target.rstrip("/") + f"/sync/v{pv}/connect?"
            + urllib.parse.urlencode(params))


async def reader(side: Side, stop: asyncio.Event) -> None:
    poke_start_time = time.perf_counter()
    while not stop.is_set():
        try:
            raw = await asyncio.wait_for(side.ws.recv(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        except Exception:
            return
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        if not isinstance(msg, list) or not msg:
            continue
        tag, body = msg[0], (msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {})
        if tag in ("pokeStart", "pokePart", "pokeEnd"):
            side.last_activity = time.perf_counter()
        if tag == "pokeStart":
            poke_start_time = time.perf_counter()
        if tag == "pokePart":
            got = body.get("gotQueriesPatch", []) or []
            side.mat.apply_rows_patch(body.get("rowsPatch"), got_hashes=got,
                                       poke_start_time=poke_start_time)
            for g in got:
                if isinstance(g, dict) and g.get("op") == "put":
                    side.mat.got_hashes.add(g.get("hash"))
            lm = body.get("lastMutationIDChanges") or {}
            if side.cid in lm:
                side.lmid_acked = max(side.lmid_acked, int(lm[side.cid]))
        elif tag == "pokeEnd":
            side.pokes += 1
        elif tag == "error":
            kind = body.get("kind", "unknown")
            side.mat.error_kinds[kind] = side.mat.error_kinds.get(kind, 0) + 1
        elif tag == "transformError":
            side.mat.error_kinds["transformError"] = \
                side.mat.error_kinds.get("transformError", 0) + 1
            # capture which query had the transformError for G9 diagnosis
            qname = body.get("queryName", "") or body.get("name", "")
            if qname:
                side.mat.error_kinds[f"transformError:{qname}"] = \
                    side.mat.error_kinds.get(f"transformError:{qname}", 0) + 1


async def run_pair(pair_idx: int, a: argparse.Namespace, baseline, results: list) -> None:
    import websockets

    rng = random.Random(a.seed + pair_idx)  # deterministic per pair
    sampler = WeightedSampler(baseline.queries, rng)
    resolver = ArgResolver.from_pool_file(a.id_pool, rng, zipf_s=a.zipf_s)
    cschema = json.load(open(a.client_schema)) if a.client_schema else None
    pks = {t: spec.get("primaryKey", []) for t, spec in
           (cschema or {}).get("tables", {}).items()}

    sides = [Side(a.primary, Materializer(pks)),
             Side(a.mirror or a.primary, Materializer(pks))]
    idrng = random.SystemRandom()
    for s in sides:
        s.cgid = "artdiff-" + "".join(idrng.choice("abcdef0123456789") for _ in range(10))
        s.cid = "artdiff-" + "".join(idrng.choice("abcdef0123456789") for _ in range(10))

    # ONE set of initial puts, sent to BOTH sides.
    initial_puts = []
    for _ in range(a.working_set):
        for _ in range(10):
            op = sampler.sample()
            args, ok = resolver.resolve(op)
            if ok:
                break
        put = query_put(op.name, args, ttl_ms=300_000)
        initial_puts.append(put)
        # wire hash->query name for query->table attribution (#3)
        for s in sides:
            s.mat.hash_query[put["hash"]] = op.name

    extra = [tuple(p.split("=", 1)) for p in (a.extra_param or [])]
    init_msg = init_connection_message(initial_puts, client_schema=cschema)
    sec = encode_sec_protocols(None, a.auth_token)  # post-handshake init

    stop = asyncio.Event()
    try:
        for s in sides:
            url = connect_url(s.target, s.cgid, s.cid, extra, a.protocol_version)
            s.ws = await websockets.connect(
                url, subprotocols=[sec], open_timeout=20, max_size=None,
                ping_interval=None)
            await s.ws.send(json.dumps(init_msg))
            s.open_ok = True
    except Exception as e:
        results.append({"pair": pair_idx, "error": f"connect failed: {e}"})
        for s in sides:
            if s.ws is not None:
                await s.ws.close()
        return

    readers = [asyncio.create_task(reader(s, stop)) for s in sides]

    # Churn: build each patch ONCE, send to both sides.
    active = [p["hash"] for p in initial_puts]
    t_end = time.perf_counter() + a.duration

    # Optional writes: pushed on the PRIMARY socket only. They land in the
    # shared upstream DB and replicate to BOTH caches, so converged states
    # must still match — covers advance/invalidation, not just hydration.
    muts = {"sent": 0}
    msampler = None
    if a.enable_mutations:
        try:
            msampler = MutationSampler(baseline.mutations, rng)
        except ValueError:
            pass  # baseline has no supported mutations; stay reads-only

    async def mutator() -> None:
        if msampler is None or a.mutations_per_min <= 0:
            return
        interval = 60.0 / a.mutations_per_min
        mid = 0
        await asyncio.sleep(rng.uniform(0, interval))  # de-sync pairs
        while time.perf_counter() < t_end and not stop.is_set():
            now_ms = int(time.time() * 1000)
            built = msampler.build(resolver, now_ms)
            if built is not None:
                name, args = built
                mid += 1
                msg = push_message(
                    sides[0].cgid,
                    [custom_mutation(mid, sides[0].cid, name, args, now_ms)],
                    request_id=f"{sides[0].cid}-{mid}", now_ms=now_ms)
                try:
                    await sides[0].ws.send(json.dumps(msg))
                except Exception:
                    return
                muts["sent"] += 1
            await asyncio.sleep(interval)

    mut_task = asyncio.create_task(mutator())
    while time.perf_counter() < t_end:
        await asyncio.sleep(a.churn_ms / 1000.0)
        patch = []
        if len(active) >= a.working_set:
            patch.append(query_del(active.pop(0)))
        for _ in range(10):
            op = sampler.sample()
            args, ok = resolver.resolve(op)
            if ok:
                break
        put = query_put(op.name, args, ttl_ms=300_000)
        patch.append(put)
        active.append(put["hash"])
        for s in sides:
            s.mat.hash_query[put["hash"]] = op.name
        msg = json.dumps(change_desired_queries_message(patch))
        try:
            for s in sides:
                await s.ws.send(msg)
        except Exception:
            break

    # Quiesce: no more desired-set changes or writes. A fixed sleep is NOT a
    # convergence check — a slow/loaded side may still be hydrating (seen live:
    # TS mirror mid-hydration at quiesce expiry => false "missing rows").
    # Instead wait until BOTH sides have been poke-quiet for --quiesce-s,
    # capped at --quiesce-max-s.
    quiesce_deadline = time.perf_counter() + a.quiesce_max_s
    quiesced = False
    while time.perf_counter() < quiesce_deadline:
        await asyncio.sleep(1.0)
        quiet = min(time.perf_counter() - s.last_activity for s in sides)
        if quiet >= a.quiesce_s:
            quiesced = True
            break
    # quiescence diagnosis (#8): if never went quiet, capture which queries
    # were still streaming (got hash acked but pokeEnd not received for that
    # poke batch) — helps distinguish infinite loop from slow eval
    if not quiesced:
        for s in sides:
            s.mat.streaming_queries = set(s.mat.query_latency.keys()) - {
                q for q in s.mat.query_latency
                if s.mat.query_latency[q]
                and time.perf_counter() - s.last_activity > a.quiesce_s
            }
    stop.set()
    mut_task.cancel()
    for t in readers:
        t.cancel()
    for s in sides:
        try:
            await s.ws.close()
        except Exception:
            pass

    d = diff_states(sides[0].mat, sides[1].mat)
    # build query->table attribution for mismatched tables (#3)
    mismatch_tables = set(d.get("per_table", {}).keys())
    query_attribution = {}
    for qname, tables in sides[0].mat.query_tables.items():
        hit = tables & mismatch_tables
        if hit:
            query_attribution[qname] = sorted(hit)
    # per-query latency summary (#5)
    def _lat_summary(lat_map):
        out = {}
        for q, times in lat_map.items():
            if times:
                s = sorted(times)
                out[q] = {"samples": len(s), "p50": s[len(s)//2],
                          "p95": s[min(len(s)-1, int(len(s)*0.95))],
                          "max": s[-1]}
        return out
    d.update({
        "pair": pair_idx,
        "primary": {"pokes": sides[0].pokes, "rows_applied": sides[0].mat.rows_applied,
                    "tables": len(sides[0].mat.state),
                    "rows": sum(len(r) for r in sides[0].mat.state.values()),
                    "got_hashes": len(sides[0].mat.got_hashes),
                    "errors": sides[0].mat.error_kinds,
                    "unknown_ops": sides[0].mat.unknown_ops,
                    "query_latency": _lat_summary(sides[0].mat.query_latency)},
        "mirror": {"pokes": sides[1].pokes, "rows_applied": sides[1].mat.rows_applied,
                   "tables": len(sides[1].mat.state),
                   "rows": sum(len(r) for r in sides[1].mat.state.values()),
                   "got_hashes": len(sides[1].mat.got_hashes),
                   "errors": sides[1].mat.error_kinds,
                   "unknown_ops": sides[1].mat.unknown_ops,
                   "query_latency": _lat_summary(sides[1].mat.query_latency)},
        "got_hash_diff": len(sides[0].mat.got_hashes ^ sides[1].mat.got_hashes),
        "mutations_sent": muts["sent"],
        "mutations_acked": sides[0].lmid_acked,
        "quiesced": quiesced,
        "query_attribution": query_attribution,
        "streaming_at_quiesce": sorted(sides[0].mat.streaming_queries |
                                        sides[1].mat.streaming_queries),
    })
    results.append(d)


def check_versions(target: str) -> str | None:
    """Quick NAPI/SQLite version probe before a full run (#7).
    Extracts the container name from the ws target URL and checks the
    Go-IVM NABI version and SQLite version via docker exec. Returns a
    summary string or None if the target can't be probed."""
    import subprocess
    # derive container name from target URL (rust-test.localhost/zero -> xyne-sandbox-rust-test-zero-cache)
    host = target.split("//")[-1].split("/")[0].split(".")[0]
    container = f"xyne-sandbox-{host}-zero-cache"
    try:
        # check if container is running
        r = subprocess.run(["docker", "ps", "--format", "{{.Names}}", "--filter",
                            f"name={container}"], capture_output=True, text=True, timeout=5)
        if container not in r.stdout.strip():
            return None
        # probe NAPI version from logs (boot line: "abi vN" or "NAPI vN")
        r = subprocess.run(["docker", "logs", "--tail", "50", container],
                           capture_output=True, text=True, timeout=10)
        logs = r.stdout + r.stderr
        abi = "?"
        for line in logs.split("\n"):
            if "abi v" in line.lower() or "napi v" in line.lower():
                import re
                m = re.search(r"(?:abi|napi)\s+v?(\d+)", line, re.I)
                if m:
                    abi = m.group(1)
                    break
        # probe SQLite version via the replica (if accessible)
        sqlite_ver = "?"
        try:
            r = subprocess.run(["docker", "exec", container, "sh", "-c",
                                "strings /var/zero/replica.db | grep -m1 'SQLite format'"],
                               capture_output=True, text=True, timeout=5)
            if r.stdout.strip():
                sqlite_ver = "WAL2 (replica.db present)"
        except Exception:
            pass
        return f"container={container} abi=v{abi} sqlite={sqlite_ver}"
    except Exception:
        return None


async def amain(a: argparse.Namespace) -> int:
    baseline = load_baseline(a.baseline)

    # version check (#7): verify NAPI/SQLite before wasting a full run
    if a.version_check:
        vinfo = check_versions(a.primary)
        if vinfo:
            print(f"  version: {vinfo}")

    results: list[dict] = []
    await asyncio.gather(*(run_pair(i, a, baseline, results)
                           for i in range(a.pairs)))

    total_mismatch = sum(r.get("mismatches", 0) for r in results)
    conn_errors = [r for r in results if "error" in r]
    out = {
        "primary": a.primary, "mirror": a.mirror or a.primary,
        "self_diff": not a.mirror or a.mirror == a.primary,
        "pairs": a.pairs, "duration_s": a.duration, "quiesce_s": a.quiesce_s,
        "mutations": bool(a.enable_mutations),
        "mutations_sent": sum(r.get("mutations_sent", 0) for r in results),
        "mutations_acked": sum(r.get("mutations_acked", 0) for r in results),
        "total_mismatches": total_mismatch,
        "connect_errors": len(conn_errors),
        "results": results,
    }
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1, default=str)

    mode = "SELF-DIFF" if out["self_diff"] else "DIFFERENTIAL"
    for r in results:
        if "error" in r:
            print(f"  pair {r['pair']}: CONNECT ERROR {r['error']}")
            continue
        p, m = r["primary"], r["mirror"]
        mut_note = (f" | muts={r.get('mutations_sent', 0)}"
                    f"/acked={r.get('mutations_acked', 0)}"
                    if a.enable_mutations else "")
        if not r.get("quiesced", True):
            mut_note += " | WARN: never went quiet (quiesce-max hit)"
        print(f"  pair {r['pair']}: mismatches={r['mismatches']} "
              f"got_hash_diff={r['got_hash_diff']} | "
              f"primary rows={p['rows']}/{p['tables']}t pokes={p['pokes']} | "
              f"mirror rows={m['rows']}/{m['tables']}t pokes={m['pokes']}"
              + mut_note)
        if r["mismatches"]:
            for t, c in r["per_table"].items():
                print(f"    {t}: {c}")
            # sample mismatch rows (#1)
            for ex in r.get("examples", [])[:6]:
                if ex["kind"] == "value_mismatch":
                    print(f"    SAMPLE {ex['table']} [{ex['kind']}] key={ex['key']}")
                    print(f"      primary: {json.dumps(ex['primary'], default=str)[:200]}")
                    print(f"      mirror:  {json.dumps(ex['mirror'], default=str)[:200]}")
                elif ex["kind"] in ("only_primary", "only_mirror"):
                    row = ex.get("primary") or ex.get("mirror") or {}
                    print(f"    SAMPLE {ex['table']} [{ex['kind']}] key={ex['key']}")
                    print(f"      row: {json.dumps(row, default=str)[:200]}")
            # column-level diff (#2)
            for cd in r.get("column_diffs", [])[:4]:
                cols = ", ".join(f"{c['column']}: {c['primary']!r} vs {c['mirror']!r}"
                                  for c in cd["columns"][:5])
                print(f"    COLDIFF {cd['table']} key={cd['key']}: {cols}")
            # zero-result dump (#6)
            for zd in r.get("zero_result_dumps", [])[:3]:
                print(f"    ZERO {zd['table']} [{zd['side']}] first_row: "
                      f"{json.dumps(zd['first_row'], default=str)[:200]}")
            # query->table attribution (#3)
            for q, ts in r.get("query_attribution", {}).items():
                print(f"    QUERY {q} -> mismatched tables: {ts}")
        # quiescence diagnosis (#8)
        if not r.get("quiesced", True):
            sq = r.get("streaming_at_quiesce", [])
            if sq:
                print(f"    QUIESCE FAIL: {len(sq)} queries still streaming: {sq[:8]}")
    verdict = "PASS" if total_mismatch == 0 and not conn_errors else "FAIL"
    print(f"{mode} ORACLE: {verdict} ({total_mismatch} mismatches, "
          f"{len(conn_errors)} connect errors) -> {a.out}")
    return 0 if verdict == "PASS" else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--primary", required=True, help="candidate zero-cache ws base")
    ap.add_argument("--mirror", default=None,
                    help="reference zero-cache ws base (omit = self-diff smoke test)")
    ap.add_argument("--baseline", default=os.path.join(
        os.path.dirname(__file__), "..", "art-baseline.json"))
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--client-schema", default=None,
                    help="clientSchema JSON (also supplies primary keys for row diffing)")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[],
                    help="k=v connect-URL params (xyne: userID=...)")
    ap.add_argument("--pairs", type=int, default=2,
                    help="logical clients (each opens one socket per side)")
    ap.add_argument("--working-set", type=int, default=10)
    ap.add_argument("--churn-ms", type=int, default=1500)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--quiesce-s", type=float, default=12.0,
                    help="required poke-quiet time on BOTH sides before diffing")
    ap.add_argument("--quiesce-max-s", type=float, default=120.0,
                    help="hard cap on the quiesce wait (diff anyway + warn)")
    ap.add_argument("--zipf-s", type=float, default=0.0)
    ap.add_argument("--enable-mutations", action="store_true",
                    help="drive read-tracking writes on the primary socket "
                         "(replicates to both sides via the shared DB)")
    ap.add_argument("--i-know-this-writes", action="store_true",
                    help="required confirmation for --enable-mutations")
    ap.add_argument("--mutations-per-min", type=float, default=6.0,
                    help="per-pair mutation rate (default 6)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--version-check", action="store_true",
                    help="probe NAPI/SQLite versions before running (fails fast on mismatch)")
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--out", default=f"reports/diff-{time.strftime('%Y%m%d-%H%M%S')}.json")
    a = ap.parse_args()
    if a.enable_mutations and not a.i_know_this_writes:
        ap.error("--enable-mutations writes real data; add --i-know-this-writes")
    return asyncio.run(amain(a))


if __name__ == "__main__":
    raise SystemExit(main())
