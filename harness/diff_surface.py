#!/usr/bin/env python3
"""
diff_surface.py — G32-G41: full-breadth TS ⇄ Rust engine-state differential.

Runs an IDENTICAL lockstep scripted session against the TS reference and the
Rust candidate (each with a fresh clientGroupID against the same upstream DB),
then diffs every piece of state the sync engine persists or emits:

  G32 cvr-schema      canonical diff of the whole /cvr schema (version-insensitive)
  G33 row-signature   rowSetSignature equality where transformationHash matches
  G34 catchup         reconnect-with-older-cookie patch-set diff
  G35 poke-stream     per-lockstep-step poke delta diff (batching-insensitive)
  G36 error-ab        identical adversarial inputs → same ErrorKind on both
  G37 inspect         inspect-protocol response diff (best-effort)
  G38 ttl-lifecycle   short-TTL desire inactivation/expiry parity
  G39 delete-clients  deleteClients cleanup parity
  G40 drain           SIGTERM end-state parity (--drain only; restarts containers)
  G41 metrics-surface instrument-name-set diff (best-effort)

Usage:
    python3 harness/diff_surface.py [--queries 6] [--drain] [--report PATH]

Requires: live sandbox (both images + shared PG), harness/auth-pool.json fresh.
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
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    Side, make_sides, open_side, reader, quiesce, step_delta, diff_deltas,
    dump_cvr, canon_cvr, diff_canon, diff_signatures, Report, load_auth,
    load_client_schema, load_pks, canon, connect_url,
)
from protocol import encode_sec_protocols, DEFAULT_PROTOCOL_VERSION  # noqa: E402
from workload import (  # noqa: E402
    load_baseline, ArgResolver, query_put, query_del,
    init_connection_message, change_desired_queries_message,
)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def pick_queries(n: int, seed: int = 42) -> list[dict]:
    """Deterministically pick the top-N fully-resolvable read queries."""
    rng = random.Random(seed)
    baseline = load_baseline(os.path.join(ROOT, "art-baseline.json"))
    resolver = ArgResolver.from_pool_file(
        os.path.join(HERE, "id-pool.sandbox.json"), rng)
    puts = []
    for op in sorted(baseline.queries, key=lambda o: -getattr(o, "weight", 0.0)):
        args, ok = resolver.resolve(op)
        if not ok:
            continue
        puts.append(query_put(op.name, args))
        if len(puts) >= n:
            break
    return puts


async def run_lockstep(a, rep: Report) -> dict:
    """The shared scripted session. Returns artifacts for later gates."""
    auth = load_auth()
    schema = load_client_schema()
    pks = load_pks(schema)
    rust, ts = make_sides()
    sides = [rust, ts]
    puts = pick_queries(a.queries)
    extra_puts = pick_queries(a.queries + 2)[a.queries:]
    ttl_put = None
    for candidate in pick_queries(a.queries + 6)[a.queries + 2:]:
        ttl_put = dict(candidate, ttl=8_000)
        break
    assert puts and extra_puts and ttl_put, "not enough resolvable queries"

    stop = asyncio.Event()
    readers = []

    async def step_barrier(step_no: int, label: str) -> None:
        ok = await quiesce(sides, quiet_s=a.quiet_s, max_s=a.quiesce_max_s)
        for s in sides:
            s.cookies_by_step[step_no] = s.last_cookie
            s.step = step_no + 1
        if not ok:
            rep.add("G35 poke-stream", "WATCH",
                    f"step {step_no} ({label}) did not quiesce in "
                    f"{a.quiesce_max_s}s")

    # ---- step 0: connect + initial hydration -----------------------------
    init = init_connection_message(puts, client_schema=schema)
    uext = {"userID": auth["userID"]}
    for s in sides:
        await open_side(s, auth["token"], init, extra=uext)
        readers.append(asyncio.create_task(reader(s, stop)))
    await step_barrier(0, "initial hydrate")

    # ---- step 1: incremental adds ----------------------------------------
    msg = change_desired_queries_message(extra_puts)
    for s in sides:
        await s.ws.send(json.dumps(msg))
    await step_barrier(1, "incremental add")

    # ---- step 2: delete one query ----------------------------------------
    del_hash = puts[0]["hash"]
    msg = change_desired_queries_message([query_del(del_hash)])
    for s in sides:
        await s.ws.send(json.dumps(msg))
    await step_barrier(2, "query del")

    # ---- step 3: short-TTL put then del (G38 setup) ----------------------
    for s in sides:
        await s.ws.send(json.dumps(change_desired_queries_message([ttl_put])))
    await quiesce(sides, quiet_s=a.quiet_s, max_s=a.quiesce_max_s)
    for s in sides:
        await s.ws.send(json.dumps(
            change_desired_queries_message([query_del(ttl_put["hash"])])))
    await step_barrier(3, "ttl put+del")
    # keep sockets open so ttlClock advances past the 8s TTL on both sides
    await asyncio.sleep(a.ttl_wait_s)
    await quiesce(sides, quiet_s=a.quiet_s, max_s=20)
    for s in sides:
        s.step = 5

    # ---- G37: inspect (best-effort) --------------------------------------
    inspect_replies = {}
    for s in sides:
        try:
            await s.ws.send(json.dumps(
                ["inspect", {"op": "queries", "id": uuid.uuid4().hex[:8]}]))
        except Exception:
            pass
    await asyncio.sleep(3)
    for s in sides:
        replies = [f.body for f in s.frames if f.tag == "inspect"]
        inspect_replies[s.name] = replies[-1] if replies else None

    # ---- close ------------------------------------------------------------
    stop.set()
    for s in sides:
        try:
            await s.ws.close()
        except Exception:
            pass
    for r in readers:
        r.cancel()
    await asyncio.sleep(1.5)          # let final CVR flush land

    return {"rust": rust, "ts": ts, "pks": pks, "auth": auth, "schema": schema,
            "puts": puts, "extra_puts": extra_puts, "ttl_put": ttl_put,
            "del_hash": del_hash, "inspect": inspect_replies}


# --------------------------------------------------------------------------- #
def gate_cvr(art: dict, rep: Report) -> tuple[dict, dict]:
    rust, ts = art["rust"], art["ts"]
    dr = dump_cvr(rust.cvr_schema, [rust.cgid])
    dt = dump_cvr(ts.cvr_schema, [ts.cgid])
    # Later of the two replica initial-sync watermarks: rowVersions at or
    # below it are provenance, not behavior (see canon_cvr docstring).
    rvs = [i.get("replicaVersion")
           for d in (dr, dt) for i in d["instances"] if i.get("replicaVersion")]
    watermark = max(rvs) if rvs else None
    cr, ct = canon_cvr(dr, watermark), canon_cvr(dt, watermark)
    diffs = diff_canon(cr, ct)
    if not dr["instances"] or not dt["instances"]:
        rep.add("G32 cvr-schema", "FAIL",
                f"missing instance row rust={bool(dr['instances'])} "
                f"ts={bool(dt['instances'])}")
    elif diffs:
        rep.add("G32 cvr-schema", "FAIL",
                f"{len(diffs)} differences; first: {diffs[0]}", diffs[:50])
    else:
        rep.add("G32 cvr-schema", "PASS",
                f"queries={len(cr['queries'])} desires={len(cr['desires'])} "
                f"rows={len(cr['rows'])} — canonical state identical")
    sig_diffs, compared = diff_signatures(cr, ct)
    if sig_diffs:
        rep.add("G33 row-signature", "FAIL",
                f"{len(sig_diffs)}/{compared} signature mismatches; "
                f"first: {sig_diffs[0]}", sig_diffs[:20])
    elif compared == 0:
        rep.add("G33 row-signature", "WATCH",
                "no comparable (same transformationHash, non-null) signatures")
    else:
        rep.add("G33 row-signature", "PASS",
                f"{compared} signatures identical across implementations")
    return cr, ct


def gate_poke_stream(art: dict, rep: Report) -> None:
    rust, ts, pks = art["rust"], art["ts"], art["pks"]
    all_diffs = []
    for step in range(4):
        d = diff_deltas(step_delta(rust, step, pks), step_delta(ts, step, pks))
        all_diffs += [f"step{step}: {x}" for x in d]
    if all_diffs:
        rep.add("G35 poke-stream", "FAIL",
                f"{len(all_diffs)} per-step delta differences; "
                f"first: {all_diffs[0]}", all_diffs[:50])
    else:
        n = sum(1 for f in rust.frames if f.tag == "pokeEnd")
        rep.add("G35 poke-stream", "PASS",
                f"per-step deltas identical (rust pokes={n})")


def gate_ttl(art: dict, cr: dict, ct: dict, rep: Report) -> None:
    h = art["ttl_put"]["hash"]
    state = []
    for name, c in (("rust", cr), ("ts", ct)):
        dq = c["desires"].get(h)
        qq = c["queries"].get(h)
        state.append((name, dq and (dq["deleted"], dq["inactive"], dq["ttlMs"]),
                      qq and qq["deleted"]))
    if state[0][1:] == state[1][1:]:
        rep.add("G38 ttl-lifecycle", "PASS",
                f"short-TTL query end-state identical: desire={state[0][1]} "
                f"query_deleted={state[0][2]}")
    else:
        rep.add("G38 ttl-lifecycle", "FAIL", f"rust={state[0]} ts={state[1]}")


def gate_inspect(art: dict, rep: Report) -> None:
    ir, it = art["inspect"]["rust"], art["inspect"]["ts"]
    if ir is None and it is None:
        rep.add("G37 inspect", "WATCH", "no inspect reply from either side "
                "(op may require admin auth) — parity vacuous")
    elif (ir is None) != (it is None):
        rep.add("G37 inspect", "FAIL",
                f"inspect replied on {'rust' if ir else 'ts'} only")
    else:
        rep.add("G37 inspect", "PASS", "both sides replied to inspect")


async def gate_catchup(art: dict, rep: Report) -> None:
    """Reconnect each side with its OWN step-0 cookie; diff catchup deltas."""
    auth, pks = art["auth"], art["pks"]
    deltas = {}
    for s in (art["rust"], art["ts"]):
        cookie = s.cookies_by_step.get(0) or ""
        if not cookie:
            rep.add("G34 catchup", "WATCH", f"{s.name}: no step-0 cookie")
            return
        s2 = Side(s.name, s.target, s.cvr_schema, s.container,
                  cgid=s.cgid, cid=s.cid)
        s2.step = 0
        stop = asyncio.Event()
        try:
            await open_side(s2, auth["token"], None, base_cookie=cookie,
                            extra={"userID": auth["userID"]})
        except Exception as e:
            rep.add("G34 catchup", "FAIL", f"{s.name}: resume connect failed: {e}")
            return
        rt = asyncio.create_task(reader(s2, stop))
        await quiesce([s2], quiet_s=2.0, max_s=30)
        stop.set()
        try:
            await s2.ws.close()
        except Exception:
            pass
        rt.cancel()
        deltas[s.name] = step_delta(s2, 0, pks)
    d = diff_deltas(deltas["rust"], deltas["ts"])
    if d:
        rep.add("G34 catchup", "FAIL",
                f"{len(d)} catchup delta differences; first: {d[0]}", d[:30])
    else:
        rep.add("G34 catchup", "PASS",
                f"catchup patch sets identical "
                f"(rust rows={len(deltas['rust']['rows'])})")


async def gate_delete_clients(art: dict, rep: Report) -> None:
    """Fresh clientID sends deleted.clientIDs=[old] — CVR cleanup must match."""
    auth, schema = art["auth"], art["schema"]
    end = {}
    for s in (art["rust"], art["ts"]):
        s2 = Side(s.name, s.target, s.cvr_schema, s.container,
                  cgid=s.cgid, cid="c-" + uuid.uuid4().hex[:8])
        stop = asyncio.Event()
        init = init_connection_message([], client_schema=schema,
                                       deleted_client_ids=[s.cid])
        try:
            await open_side(s2, auth["token"], init,
                            extra={"userID": auth["userID"]})
        except Exception as e:
            rep.add("G39 delete-clients", "FAIL", f"{s.name}: connect failed: {e}")
            return
        rt = asyncio.create_task(reader(s2, stop))
        await quiesce([s2], quiet_s=2.0, max_s=25)
        acks = [f.body for f in s2.frames if f.tag == "deleteClients"]
        stop.set()
        try:
            await s2.ws.close()
        except Exception:
            pass
        rt.cancel()
        await asyncio.sleep(1.0)
        dump = dump_cvr(s.cvr_schema, [s.cgid])
        end[s.name] = {
            "acked": bool(acks),
            "old_client_rows": sum(1 for c in dump["clients"]
                                   if c["clientID"] == s.cid),
            "old_desires": sum(1 for d in dump["desires"]
                               if d["clientID"] == s.cid and not d["deleted"]),
        }
    if end["rust"] == end["ts"]:
        rep.add("G39 delete-clients", "PASS", f"cleanup parity: {end['rust']}")
    else:
        rep.add("G39 delete-clients", "FAIL", f"rust={end['rust']} ts={end['ts']}")


# --------------------------------------------------------------------------- #
NEGATIVE_CASES = [
    ("garbage-cookie", {"base_cookie": "!!notlexi!!"}),
    ("overlarge-configversion", {"base_cookie": "00:b100000000000"}),
    ("bad-protocol-version", {"pv": 999}),
    ("tampered-token", {"token_suffix": "xx"}),
    ("malformed-init", {"init": ["initConnection",
                                 {"desiredQueriesPatch": "not-a-list"}]}),
    ("unknown-message", {"post": ["definitelyNotAThing", {}]}),
    # Post-init adversarial messages (post_builder receives the auth token).
    ("malformed-change-desired", {"post": ["changeDesiredQueries",
                                           {"desiredQueriesPatch": "nope"}]}),
    ("malformed-ack-mutations", {"post": ["ackMutationResponses",
                                          {"bogus": True}]}),
    ("pull-op", {"post": ["pull", {"clientGroupID": "x", "cookie": "",
                                   "requestID": "r1"}]}),
    ("updateauth-tampered", {"post_builder": lambda tok:
                             ["updateAuth", {"auth": tok[:-2] + "xx"}]}),
    # Both images cap ws frames (rust DEFAULT_MAX_PAYLOAD_BYTES=10MiB,
    # TS websocketMaxPayloadBytes) — a 12MB frame must be rejected the
    # same way on both (close code compared, no error frame expected).
    ("oversized-payload", {"post": ["changeDesiredQueries",
                                    {"desiredQueriesPatch": [],
                                     "pad": "x" * (12 * 1024 * 1024)}]}),
]


async def observe_negative(side_tpl: Side, auth_token: str, schema: dict,
                           case: dict, user_id: str = "") -> str:
    """Fire one adversarial case; return the observed outcome signature."""
    s = Side(side_tpl.name, side_tpl.target, side_tpl.cvr_schema,
             side_tpl.container)
    s.fresh_ids()
    token = auth_token + case.get("token_suffix", "") \
        if "token_suffix" in case else auth_token
    if "token_suffix" in case:
        token = auth_token[:-2] + case["token_suffix"]
    init = case.get("init",
                    init_connection_message([], client_schema=schema))
    try:
        await open_side(s, token, init,
                        pv=case.get("pv", DEFAULT_PROTOCOL_VERSION),
                        base_cookie=case.get("base_cookie", ""),
                        extra={"userID": user_id})
    except ConnectionError:
        err = [f for f in s.frames if f.tag == "error"]
        return f"error:{err[-1].body.get('kind')}" if err else "connect-error"
    except Exception as e:
        code = getattr(e, "status_code", None) or getattr(
            getattr(e, "response", None), "status_code", None)
        return f"handshake-reject:{code or type(e).__name__}"
    if "post_builder" in case:
        case = dict(case)
        case["post"] = case["post_builder"](auth_token)
    outcome = "connected"
    try:
        if "post" in case:
            # Quiesce first: init-hydration pokes already in flight would
            # otherwise race the post-message's error frame and read as
            # "accepted" (both images DO answer error:InvalidMessage once
            # drained — verified live).
            await asyncio.sleep(1.5)
            try:
                while True:
                    await asyncio.wait_for(s.ws.recv(), timeout=0.3)
            except Exception:
                pass
            await s.ws.send(json.dumps(case["post"]))
        deadline = time.perf_counter() + 6
        while time.perf_counter() < deadline:
            raw = await asyncio.wait_for(s.ws.recv(), timeout=6)
            m = json.loads(raw)
            if isinstance(m, list) and m and m[0] == "error":
                outcome = f"error:{m[1].get('kind')}"
                break
            if (isinstance(m, list) and m and m[0] == "pokeEnd"
                    and "post" not in case):
                outcome = "accepted"        # server treated input as fine
                break
    except asyncio.TimeoutError:
        outcome = outcome if outcome != "connected" else "accepted-silent"
    except Exception as e:
        # A protocol-level rejection (e.g. oversized frame) closes the socket
        # without an error frame — the close CODE is the comparable outcome.
        code = getattr(e, "code", None) or getattr(
            getattr(e, "rcvd", None), "code", None)
        if code:
            outcome = f"closed:{code}"
    if outcome in ("connected",):
        outcome = "accepted-silent"
    try:
        await s.ws.close()
    except Exception:
        pass
    return outcome


async def gate_negative(art: dict, rep: Report) -> None:
    auth, schema = art["auth"], art["schema"]
    diffs, table = [], {}
    for name, case in NEGATIVE_CASES:
        o_rust = await observe_negative(art["rust"], auth["token"], schema,
                                        case, auth["userID"])
        o_ts = await observe_negative(art["ts"], auth["token"], schema, case,
                                      auth["userID"])
        table[name] = {"rust": o_rust, "ts": o_ts}
        if o_rust != o_ts:
            diffs.append(f"{name}: rust={o_rust} ts={o_ts}")
    if diffs:
        rep.add("G36 error-ab", "FAIL",
                f"{len(diffs)}/{len(NEGATIVE_CASES)} divergent; "
                f"first: {diffs[0]}", table)
    else:
        rep.add("G36 error-ab", "PASS",
                f"{len(NEGATIVE_CASES)} adversarial cases → identical outcomes",
                table)


# --------------------------------------------------------------------------- #
def scrape_metrics(container: str) -> set[str]:
    """Best-effort in-container scrape; returns instrument-name set."""
    names: set[str] = set()
    for port_path in ("8081/metrics", "4849/metrics", "4848/metrics",
                      "8081/statz", "4849/statz"):
        cmd = (f"docker exec {container} sh -c "
               f"'curl -sf http://localhost:{port_path} 2>/dev/null "
               f"|| wget -qO- http://localhost:{port_path} 2>/dev/null' ")
        try:
            out = subprocess.run(cmd, shell=True, capture_output=True,
                                 timeout=10).stdout.decode(errors="replace")
        except Exception:
            continue
        for line in out.splitlines():
            line = line.strip()
            if line and not line.startswith("#") and ("{" in line or " " in line):
                names.add(line.split("{")[0].split(" ")[0])
        if names:
            break
    return {n for n in names if n.startswith(("zero", "target_info"))}


def gate_metrics(art: dict, rep: Report) -> None:
    mr = scrape_metrics(art["rust"].container)
    mt = scrape_metrics(art["ts"].container)
    if not mr and not mt:
        rep.add("G41 metrics-surface", "WATCH",
                "neither side exposes a scrapeable metrics endpoint "
                "(sandbox has OTLP export disabled) — configure "
                "OTEL/prometheus to activate this gate")
    elif not mr or not mt:
        rep.add("G41 metrics-surface", "WATCH",
                f"only {'ts' if mt else 'rust'} scrapeable "
                f"({len(mt or mr)} instruments) — parity not comparable")
    else:
        only_r, only_t = sorted(mr - mt), sorted(mt - mr)
        if only_r or only_t:
            rep.add("G41 metrics-surface", "WATCH",
                    f"instrument sets differ: rust-only={only_r[:8]} "
                    f"ts-only={only_t[:8]}",
                    {"rust_only": only_r, "ts_only": only_t})
        else:
            rep.add("G41 metrics-surface", "PASS",
                    f"{len(mr)} instruments identical")


def gate_drain(art: dict, rep: Report) -> None:
    """--drain only: SIGTERM both images, compare persisted end-state, restart."""
    rust, ts = art["rust"], art["ts"]
    for s in (rust, ts):
        subprocess.run(f"docker kill -s TERM {s.container}", shell=True,
                       capture_output=True)
    time.sleep(12)
    state = {}
    for s in (rust, ts):
        d = dump_cvr(s.cvr_schema, [s.cgid])
        inst = d["instances"][0] if d["instances"] else {}
        state[s.name] = {
            "instance_present": bool(inst),
            "ttlClock_positive": (inst.get("ttlClock") or 0) > 0,
            "deleted": inst.get("deleted"),
        }
        subprocess.run(f"docker start {s.container}", shell=True,
                       capture_output=True)
    deadline = time.time() + 60
    while time.time() < deadline:
        r = subprocess.run("curl -s -o /dev/null -w '%{http_code}' "
                           + rust.target.replace("ws://", "http://") + "/",
                           shell=True, capture_output=True)
        if r.stdout.decode().strip() == "200":
            break
        time.sleep(2)
    if state["rust"] == state["ts"]:
        rep.add("G40 drain", "PASS", f"post-SIGTERM end-state parity: {state['rust']}")
    else:
        rep.add("G40 drain", "FAIL", f"rust={state['rust']} ts={state['ts']}")


# --------------------------------------------------------------------------- #
async def amain(a) -> int:
    rep = Report(a.report)
    print(f"lockstep session: {a.queries} initial queries, "
          f"ttl_wait={a.ttl_wait_s}s\n", flush=True)
    art = await run_lockstep(a, rep)
    cr, ct = gate_cvr(art, rep)
    gate_poke_stream(art, rep)
    gate_ttl(art, cr, ct, rep)
    gate_inspect(art, rep)
    await gate_catchup(art, rep)
    await gate_delete_clients(art, rep)
    await gate_negative(art, rep)
    gate_metrics(art, rep)
    if a.drain:
        gate_drain(art, rep)
    else:
        rep.add("G40 drain", "SKIP", "pass --drain to enable (restarts containers)")
    return rep.finish()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", type=int, default=6)
    ap.add_argument("--quiet-s", type=float, default=2.5)
    ap.add_argument("--quiesce-max-s", type=float, default=45.0)
    ap.add_argument("--ttl-wait-s", type=float, default=15.0)
    ap.add_argument("--drain", action="store_true")
    ap.add_argument("--report", default=os.path.join(
        os.path.dirname(HERE), "reports",
        f"diff-surface-{time.strftime('%Y%m%d-%H%M%S')}.json"))
    return asyncio.run(amain(ap.parse_args()))


HERE = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    sys.exit(main())
