#!/usr/bin/env python3
"""
mutation_matrix.py — PUSH-PATH mutator matrix (gate G15): fire every
synthesizable mutator type through the real client push path — zero-cache ->
backend mutator (zod validation + permission checks + prisma writes) ->
replication -> BOTH pods' advance — and byte-diff the resulting materialized
state Go-vs-TS after every wave.

Complement to matrix_oracle.py (G12), not a replacement:
  * G12 fuzzes the ADVANCEMENT surface with direct SQL (every column x typed
    edge values) — maximal replication coverage, zero mutator coverage.
  * G15 exercises the MUTATOR TYPE surface (previously 2/218 via hand-built
    args): each mutator's arg schema, validation path, write pattern, and the
    advancement its writes trigger. Values are best-effort synthetics, so the
    per-column depth is shallower than G12 — by design.

Method:
  1. both pods subscribe the full resolvable query catalog (same hashes) and
     hydrate — mutation effects must be VISIBLE to be diffable
  2. plan phases: CREATE (fresh artmx ids, recorded in an overlay) ->
     UPDATE (pool/overlay entities) -> DESTRUCTIVE (overlay-owned entities
     ONLY — we never delete seeded data; org.* is hard-denylisted)
  3. fire in waves; collect per-mutation results from pushResponse frames
     (['pushResponse', {mutations:[{id,result}]}] — result {} = applied,
     {error:'app',message} = rejected, oooMutation/alreadyProcessed = harness
     protocol bug) with lastMutationIDChanges as the no-detail fallback ack
  4. converge after each wave: poll until both sides' canon states are equal
     twice in a row; persistent inequality => re-check => FAIL with the wave's
     members named
  5. cleanup: DELETE artmx% rows from every write-table the applied creates
     touched (impact matrix writeTables + clientSchema primaryKey)

Outcome buckets per mutator:
  applied          backend accepted; write happened (or was a no-op)
  app-rejected     backend refused (permissions/state rules) — validation
                   path exercised; legitimate coverage
  synth-invalid    zod rejected OUR synthesized args — synthesizer backlog
  not-found        mutator unknown to the deployed backend (build drift)
  zero-rejected    oooMutation/alreadyProcessed — harness protocol bug => FAIL
  timeout          no ack within window — INFRA signal
  skipped-*        never fired (unresolvable args / destructive-on-shared /
                   denylist / non-object schema)

Verdict: FAIL on persistent divergence or zero-rejected; INFRA on connect
failure or timeout epidemic (>25%); PASS otherwise. app-rejected/synth-invalid
are coverage data, not failures (FAIL-vs-ERROR discipline).

    .venv/bin/python harness/mutation_matrix.py \
        --primary ws://rust-test.localhost/zero \
        --mirror  ws://rust-test.localhost/zero-ts \
        --auth-token "$JWT" --extra-param userID=<uid> \
        --i-know-this-writes [--include 'channel.*'] [--max-mutators N]

Exit 0 PASS / 1 FAIL / 2 INFRA.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from diff_oracle import (  # noqa: E402
    Materializer, Side, canon, connect_url, diff_states,
)
from workload import (  # noqa: E402
    ArgResolver, SchemaSynthesizer, change_desired_queries_message,
    custom_mutation, init_connection_message, load_baseline, push_message,
    query_put,
)
from replay import DEFAULT_PROTOCOL_VERSION, encode_sec_protocols  # noqa: E402

CREATE_RE = re.compile(r"\.(create|add|new)[A-Za-z]*$|\.create$", re.I)
DESTRUCTIVE_RE = re.compile(
    r"delete|remove|revoke|leave|unshare|archive|wipe|purge|cleanup", re.I)
# Hard denylist regardless of tier: org mutations can invalidate the sandbox
# workspace itself (the identity, JWT workspaceId, and every membership row
# hang off it) — no cleanup can un-break that mid-run.
DENYLIST_RE = re.compile(r"^org\.", re.I)
# zod-rejection fingerprints => the SYNTHESIZER is wrong, not the backend
ZOD_MSG_RE = re.compile(
    r"invalid_type|invalid_enum|too_small|too_big|unrecognized_keys|"
    r"\bRequired\b|Expected|Invalid input|invalid input value|invalid_string|"
    r"invalid_union|is required", re.I)
# mutator UNKNOWN to the deployed backend (build drift) — deliberately narrow:
# entity not-founds ("Canvas folder not found") are app rejections, not drift
NOT_FOUND_RE = re.compile(r"mutator .*not (found|registered)|unknown mutator|"
                          r"no such mutator", re.I)


def sq(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def psql(a, sql_text: str) -> tuple[int, str, str]:
    p = subprocess.run(
        ["docker", "exec", "-i", a.pg_container, "psql", "-X", "-q",
         "-v", "ON_ERROR_STOP=0", "-U", a.pg_user, "-d", a.pg_db, "-f", "-"],
        input=sql_text, capture_output=True, text=True, timeout=120)
    return p.returncode, p.stdout, p.stderr


# --------------------------------------------------------------------------- #
async def primary_reader(side: Side, stop: asyncio.Event,
                         push_results: dict[int, dict],
                         error_frames: list[dict]) -> None:
    """diff_oracle.reader + pushResponse/mutation-result capture. The primary
    side both materializes state (for the diff) AND carries the push traffic,
    so it needs the richer reader; the mirror keeps diff_oracle's."""
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
        tag = msg[0]
        body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
        if tag in ("pokeStart", "pokePart", "pokeEnd"):
            side.last_activity = time.perf_counter()
        if tag == "pokePart":
            side.mat.apply_rows_patch(body.get("rowsPatch"))
            for got in body.get("gotQueriesPatch", []) or []:
                if isinstance(got, dict) and got.get("op") == "put":
                    side.mat.got_hashes.add(got.get("hash"))
            lm = body.get("lastMutationIDChanges") or {}
            if side.cid in lm:
                side.lmid_acked = max(side.lmid_acked, int(lm[side.cid]))
        elif tag == "pokeEnd":
            side.pokes += 1
        elif tag == "pushResponse":
            for m in body.get("mutations", []) or []:
                if not isinstance(m, dict):
                    continue
                mid_obj = m.get("id") or {}
                mid = int(mid_obj.get("id", 0)) if isinstance(mid_obj, dict) else 0
                if mid:
                    push_results[mid] = m.get("result") if isinstance(
                        m.get("result"), dict) else {}
        elif tag == "error":
            error_frames.append(body)
            side.mat.error_kinds[body.get("kind", "?")] = \
                side.mat.error_kinds.get(body.get("kind", "?"), 0) + 1
        elif tag == "transformError":
            side.mat.error_kinds["transformError"] = \
                side.mat.error_kinds.get("transformError", 0) + 1


async def converge(sides: list[Side], timeout_s: float,
                   poll_s: float = 1.5) -> tuple[bool, float]:
    """Equal-canon twice in a row => converged (matrix_oracle's predicate —
    equality, not quiet: background traffic makes quiet impossible)."""
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


def classify(result: dict | None) -> tuple[str, str]:
    """pushResponse result -> (bucket, detail)."""
    if result is None:
        return "acked-no-detail", ""     # lmid advanced, no pushResponse seen
    err = result.get("error")
    if err is None:
        return "applied", ""
    msg = str(result.get("message") or result.get("details") or "")[:160]
    if err in ("oooMutation", "alreadyProcessed"):
        return "zero-rejected", f"{err}: {msg}"
    if NOT_FOUND_RE.search(msg):
        return "not-found", msg
    if ZOD_MSG_RE.search(msg):
        return "synth-invalid", msg
    return "app-rejected", msg


# --------------------------------------------------------------------------- #
# Pod-log result channel. This zero-cache build does NOT forward pushResponse
# frames to the client (verified: 21k-poke replay runs show zero pushResponse
# in per_tag) — the ONLY per-mutation error detail is the pusher's log line:
#   ... clientGroupID=<cgid>,component=pusher The server behind ZERO_MUTATE_URL
#   returned a mutation error. {"error":"app","message":"..."}
# The line carries NO mutation id, so attribution needs two properties we
# enforce ourselves: (1) pushes are processed SEQUENTIALLY per client group,
# and (2) the driver holds an lmid ack barrier after EVERY mutation before
# sending the next. Under those, each mutation owns a disjoint [sent, acked]
# wall-clock window and each error line's timestamp lands in exactly one
# window (±skew tolerance; same host clock).
LOG_ERR_RE = re.compile(r"returned a mutation error\.\s*(\{.*)$")
LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)")


def scrape_mutation_errors(container: str, since_iso: str,
                           cgid: str) -> list[tuple[float, dict]]:
    """[(epoch_ts, error_obj)] for our CG's mutation errors, log order."""
    try:
        p = subprocess.run(["docker", "logs", "--since", since_iso, container],
                           capture_output=True, text=True, timeout=120)
    except Exception:
        return []
    out = []
    for ln in (p.stdout + "\n" + p.stderr).splitlines():
        if cgid not in ln or "returned a mutation error" not in ln:
            continue
        m = LOG_ERR_RE.search(ln)
        t = LOG_TS_RE.match(ln.strip())
        if not m or not t:
            continue
        try:
            obj = json.loads(m.group(1))
        except Exception:
            obj = {"error": "app", "message": m.group(1)[:160]}
        ts = time.mktime(time.strptime(t.group(1)[:19], "%Y-%m-%dT%H:%M:%S")) \
            + float("0." + t.group(1).split(".")[1][:3]) - time.timezone
        out.append((ts, obj))
    return out


async def amain(a: argparse.Namespace) -> int:
    import websockets

    if not a.i_know_this_writes:
        print("REFUSING: mutation matrix WRITES through the backend. Pass "
              "--i-know-this-writes (sandbox only).", file=sys.stderr)
        return 2
    for t in (a.primary, a.mirror):
        if "localhost" not in t and "127.0.0.1" not in t and not a.allow_remote:
            print(f"REFUSING non-local target {t} without --allow-remote",
                  file=sys.stderr)
            return 2

    # ---- inputs ----
    schemas_doc = json.load(open(a.arg_schemas))
    pool = json.load(open(a.id_pool))
    baseline = load_baseline(a.baseline)
    cschema = json.load(open(a.client_schema))
    tables_spec = cschema.get("tables", {})
    pks = {t: s.get("primaryKey", []) for t, s in tables_spec.items()}
    rng = random.Random(a.seed)
    extra = [tuple(p.split("=", 1)) for p in (a.extra_param or [])]
    user_id = dict(extra).get("userID", "")
    identity = {"userId": user_id,
                "workspaceId": (pool["ids"].get("workspaceId") or [""])[0]}
    synth = SchemaSynthesizer(schemas_doc, pool["ids"], pool.get("scalars", {}),
                              identity, rng)
    write_tables = {m["mutatorName"]: m.get("writeTables") or []
                    for m in (json.load(open(a.impact)).get("mutators") or [])} \
        if a.impact and os.path.exists(a.impact) else {}

    # ---- plan: phase-ordered mutator list ----
    inc = re.compile(a.include) if a.include else None
    all_names = sorted(schemas_doc.get("mutators") or {})
    plan: list[tuple[str, str]] = []          # (phase, name)
    skipped: dict[str, str] = {}
    for name in all_names:
        if inc and not inc.search(name):
            continue
        if DENYLIST_RE.search(name):
            skipped[name] = "skipped-denylist"
            continue
        if DESTRUCTIVE_RE.search(name):
            plan.append(("destructive", name))
        elif CREATE_RE.search(name):
            plan.append(("create", name))
        else:
            plan.append(("update", name))
    order = {"create": 0, "update": 1, "destructive": 2}
    plan.sort(key=lambda pn: (order[pn[0]], pn[1]))
    if a.max_mutators:
        plan = plan[:a.max_mutators]

    # ---- connect both sides ----
    sides = [Side(a.primary, Materializer(pks)),
             Side(a.mirror, Materializer(pks))]
    idrng = random.SystemRandom()
    for s in sides:
        s.cgid = "artmm-" + "".join(idrng.choice("abcdef0123456789") for _ in range(10))
        s.cid = "artmm-" + "".join(idrng.choice("abcdef0123456789") for _ in range(10))
    sec = encode_sec_protocols(None, a.auth_token)
    init_msg = init_connection_message([], client_schema=cschema)
    stop = asyncio.Event()
    push_results: dict[int, dict] = {}
    error_frames: list[dict] = []
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
    readers = [
        asyncio.create_task(primary_reader(sides[0], stop, push_results,
                                           error_frames)),
        asyncio.create_task(__import__("diff_oracle").reader(sides[1], stop)),
    ]

    # ---- MAP: subscribe resolvable catalog on both sides ----
    resolver = ArgResolver.from_pool_file(a.id_pool, rng, zipf_s=0.0)
    puts = []
    for op in baseline.queries:
        args, ok = resolver.resolve(op)
        if ok:
            puts.append(query_put(op.name, args, ttl_ms=3_600_000))
    print(f"MAP: desiring {len(puts)}/{len(baseline.queries)} catalog queries "
          f"on both pods")
    for i in range(0, len(puts), 25):
        msg = json.dumps(change_desired_queries_message(puts[i:i + 25]))
        for s in sides:
            await s.ws.send(msg)
        await asyncio.sleep(0.4)
    deadline = time.perf_counter() + a.hydrate_max_s
    while time.perf_counter() < deadline:
        await asyncio.sleep(1.0)
        if min(time.perf_counter() - s.last_activity for s in sides) >= 5.0:
            break
    ok0, _ = await converge(sides, 30)
    print(f"MAP: hydrated {sides[0].mat.rows_applied} rows "
          f"(pre-mutation states equal: {ok0})")
    if not ok0:
        print("INFRA: sides never converged before any mutation was sent",
              file=sys.stderr)
        stop.set()
        return 2

    # ---- fire: sequential, ack-barriered (see pod-log channel note) ----
    run_start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    mid = 0
    results: list[dict] = []               # per fired mutation
    diverged_waves: list[dict] = []
    cleanup: list[tuple[str, str]] = []    # (mutator, fresh_id) of APPLIED creates
    fired = timeouts = 0
    wave: list[dict] = []                  # converge batch (attribution is per-mutation)

    async def converge_wave() -> None:
        nonlocal wave
        if not wave:
            return
        okc, _ = await converge(sides, a.converge_timeout_s)
        if not okc:
            # persistent-mismatch re-check: replication/advance lag self-heals;
            # only a SECOND failed converge counts (oracle-hardening lesson)
            okc, _ = await converge(sides, a.converge_timeout_s)
            if not okc:
                diverged_waves.append({
                    "members": [w["name"] for w in wave],
                    "diff": diff_states(sides[0].mat, sides[1].mat),
                })
                print(f"  WAVE DIVERGED after re-check: {[w['name'] for w in wave]}")
        wave = []

    now_phase = None
    for phase, name in plan:
        if phase != now_phase:
            await converge_wave()
            now_phase = phase
            print(f"-- phase: {phase} --")
        args, meta = synth.synth(
            name, int(time.time() * 1000),
            allow_fresh=(phase == "create"),
            overlay_only=(phase == "destructive"))
        if args is not None and phase == "create" and meta["fresh_ids"]:
            # optimistic in-run overlay commit: the authoritative applied/
            # rejected classification only exists post-hoc (log scrape), but
            # the destructive phase needs overlay targets DURING the run.
            # A rejected create's id in the overlay just turns the dependent
            # destructive mutator into an app-rejected "not found" — recorded
            # coverage, no false FAIL.
            synth.commit_fresh(meta["fresh_ids"])
        if args is None:
            reason = meta["skip_reason"]
            skipped[name] = ("skipped-destructive-shared"
                             if phase == "destructive"
                             and str(reason).startswith("required-arg-unresolvable")
                             else f"skipped-{reason}")
            continue
        mid += 1
        fired += 1
        msg = push_message(sides[0].cgid,
                           [custom_mutation(mid, sides[0].cid, name, args,
                                            int(time.time() * 1000))],
                           request_id=f"artmm-{mid}",
                           now_ms=int(time.time() * 1000))
        rec = {"name": name, "phase": phase, "mid": mid,
               "provenance": meta["provenance"],
               "fresh_ids": meta["fresh_ids"],
               "sent_ts": time.time(), "acked_ts": None}
        try:
            await sides[0].ws.send(json.dumps(msg))
        except Exception as e:
            print(f"INFRA: push send failed at {name}: {e}", file=sys.stderr)
            break
        # ack barrier: the NEXT push waits until this one is fully processed
        # (lmid advance or pushResponse) — this is what makes the log-line
        # timestamp windows disjoint and the attribution exact
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < a.ack_timeout_s:
            if sides[0].lmid_acked >= mid or mid in push_results:
                rec["acked_ts"] = time.time()
                break
            await asyncio.sleep(0.15)
        if rec["acked_ts"] is None:
            rec["outcome"], rec["detail"] = "timeout", ""
            timeouts += 1
        results.append(rec)
        wave.append(rec)
        if len(wave) >= a.wave_size:
            await converge_wave()
        await asyncio.sleep(a.gap_ms / 1000.0)
    await converge_wave()

    # ---- attribute pod-logged mutation errors to ack windows ----
    # Two-pass: strict [sent, acked] containment first (the pusher logs the
    # error BEFORE the lmid ack can flow, so a mutation's own error is always
    # inside its barrier window), then a ±2s padded pass for clock-skew
    # stragglers. Greedy single-pass with padding risked off-by-one grabs:
    # windows are ~1s apart but padding made them ~4s wide.
    err_lines = scrape_mutation_errors(a.primary_container, run_start_iso,
                                       sides[0].cgid)
    used = [False] * len(err_lines)
    matched: dict[int, dict] = {}
    for pad in (0.0, 2.0):
        for rec in results:
            if rec["mid"] in matched or rec.get("outcome") == "timeout":
                continue
            lo = rec["sent_ts"] - pad
            hi = (rec["acked_ts"] or rec["sent_ts"]) + pad
            for i, (ts, obj) in enumerate(err_lines):
                if not used[i] and lo <= ts <= hi:
                    used[i] = True
                    matched[rec["mid"]] = obj
                    break
    for rec in results:
        if rec.get("outcome") == "timeout":
            continue
        res = push_results.get(rec["mid"]) or matched.get(rec["mid"])
        if res is None:
            rec["outcome"], rec["detail"] = "applied", ""
        else:
            rec["outcome"], rec["detail"] = classify(res)
        if rec["outcome"] == "applied" and rec["fresh_ids"]:
            # only APPLIED creations enter the cleanup list — a rejected
            # create's id points at nothing (the overlay itself was committed
            # optimistically in-run; see the create-phase note above)
            cleanup.extend((rec["name"], fid) for _, fid in rec["fresh_ids"])
    unmatched_errors = [obj for i, (ts, obj) in enumerate(err_lines)
                        if not used[i]]

    # ---- cleanup (best-effort, prefix-scoped) ----
    leftovers = 0
    if cleanup and not a.keep_rows:
        stmts = []
        touched: set[str] = set()
        for mname, _fid in cleanup:
            touched |= set(write_tables.get(mname) or [])
        for t in sorted(touched):
            pk = (pks.get(t) or ["id"])[0]
            if t in tables_spec:
                stmts.append(f"DELETE FROM {qi(t)} WHERE {qi(pk)} LIKE 'artmx%';")
        if stmts:
            rc, out, err = await asyncio.to_thread(psql, a, "\n".join(stmts))
            leftovers = err.count("ERROR:")
        await converge(sides, 20)      # let the deletes replicate + settle

    stop.set()
    for r in readers:
        r.cancel()
    for s in sides:
        try:
            await s.ws.close()
        except Exception:
            pass

    # ---- report ----
    buckets: dict[str, int] = {}
    for w in results:
        buckets[w["outcome"]] = buckets.get(w["outcome"], 0) + 1
    for reason in skipped.values():
        key = reason if reason.startswith("skipped") else f"skipped-{reason}"
        buckets[key] = buckets.get(key, 0) + 1
    verdict = "PASS"
    if diverged_waves or buckets.get("zero-rejected"):
        verdict = "FAIL"
    elif fired == 0 or (fired and timeouts / fired > 0.25):
        verdict = "INFRA"
    # Shared-entity mutation audit: update-phase applied mutators that aimed
    # at POOL ids mutated SEEDED sandbox rows (renames, role flips, content
    # overwrites — no deletes; destructive is overlay-only). Sandbox-
    # acceptable by definition of --i-know-this-writes, but it must be
    # AUDITABLE: these can shift permission-dependent hydration for affected
    # identities in later runs (e.g. users.updateRole), which would look like
    # a baseline shift. --refresh --clean regenerates pools; re-bless G5
    # after a matrix run if identities were touched.
    shared_updates = sorted(
        r["name"] for r in results
        if r.get("outcome") == "applied" and r["phase"] == "update"
        and "pool" in set(r.get("provenance", {}).values()))
    report = {
        "primary": a.primary, "mirror": a.mirror,
        "when": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mutators_in_catalog": len(all_names),
        "planned": len(plan), "fired": fired,
        "buckets": dict(sorted(buckets.items(), key=lambda kv: -kv[1])),
        "diverged_waves": diverged_waves,
        "unmatched_error_lines": [str(e)[:160] for e in unmatched_errors[:8]],
        "error_frames": [str(e)[:200] for e in error_frames[:8]],
        "cleanup_rows_created": len(cleanup),
        "cleanup_sql_errors": leftovers,
        "shared_updates_applied": shared_updates,
        "results": results,
        "skipped": skipped,
        "verdict": verdict,
    }
    out_path = a.out or os.path.join(
        os.path.dirname(__file__), "..", "reports",
        "mutmatrix-" + time.strftime("%Y%m%d-%H%M%S") + ".json")
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nMUTATION MATRIX: {verdict} — fired {fired}/{len(plan)} planned "
          f"({len(all_names)} in catalog), "
          f"{len(diverged_waves)} diverged waves -> {out_path}")
    print("  " + ", ".join(f"{k}={v}" for k, v in
                           sorted(buckets.items(), key=lambda kv: -kv[1])))
    if shared_updates:
        print(f"  NOTE: {len(shared_updates)} applied updates touched SHARED "
              f"seeded entities (audit: shared_updates_applied in the report)")
    return 0 if verdict == "PASS" else (1 if verdict == "FAIL" else 2)


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
    ap.add_argument("--arg-schemas", default=os.path.join(
        os.path.dirname(__file__), "..", "raw", "arg-schemas.source.json"))
    ap.add_argument("--impact", default=os.path.join(
        os.path.dirname(__file__), "..", "raw", "query-mutator-impact.json"))
    ap.add_argument("--pg-container", default="xyne-sandbox-postgres")
    ap.add_argument("--primary-container", default="xyne-sandbox-rust-test-zero-cache",
                    help="docker container of the PRIMARY pod — its logs are the "
                         "per-mutation error channel (this build forwards no "
                         "pushResponse frames)")
    ap.add_argument("--pg-user", default="xyne")
    ap.add_argument("--pg-db", default="sandbox_rust_test_db")
    ap.add_argument("--include", default=None,
                    help="regex filter on mutator names")
    ap.add_argument("--max-mutators", type=int, default=0)
    ap.add_argument("--wave-size", type=int, default=8)
    ap.add_argument("--gap-ms", type=int, default=120)
    ap.add_argument("--ack-timeout-s", type=float, default=30.0)
    ap.add_argument("--converge-timeout-s", type=float, default=45.0)
    ap.add_argument("--hydrate-max-s", type=float, default=90.0)
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--keep-rows", action="store_true",
                    help="skip the artmx%% cleanup sweep")
    ap.add_argument("--out", default=None)
    ap.add_argument("--i-know-this-writes", action="store_true")
    ap.add_argument("--allow-remote", action="store_true")
    return asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
