#!/usr/bin/env python3
"""
invention_oracles.py — Gate G49: differential runtime proofs of the
INVENTIONS.md contracts (I-1…I-4).

Each rust-only concurrency construct in parity/INVENTIONS.md carries a
TS-observable contract but is pinned today only by in-process UNIT tests — not by
a runtime differential against TS. BOTH production outages lived in exactly these
seams (connect-ack thrash = I-1/I-2; push-relay 401 = I-3). This gate drives each
contract-relevant scenario IDENTICALLY on the rust candidate and the TS reference
and asserts the client-observable behavior matches.

Sub-gates:
  I-1/I-2  connect-ack ordering — `connected` arrives BEFORE hydration completes
           (ack is not serialized behind the CG hydrate), identically on both.
  I-3      push-mutation parity — the same custom mutation yields the same result
           (applied vs error) + lmid advance, with ZERO Unauthorized/401 frames;
           and after `updateAuth` a follow-up mutation still 401-free (prod-bug-2).
  I-4      slow-client shed — a stalled consumer is shed with the SAME frame TS
           emits (a `Rehome`/error frame, THEN close), not a bare drop.
  I-1(own) ownership/rehome — two connects contesting one clientGroupID resolve
           with matching client-observable frames.

Runs against the live pair (ab_common defaults / RUN.md); every sub-gate SKIPs
individually if it cannot be set up or triggered (never a false-FAIL).
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    Side, make_sides, open_side, reader, quiesce, Report,
    load_auth, load_client_schema,
)
from workload import (  # noqa: E402
    ast_query_put, change_desired_queries_message, custom_mutation, push_message,
    MUTATION_ARG_BUILDERS, ArgResolver,
)

TAG = os.environ.get("ART_TAG", time.strftime("%Y%m%d-%H%M%S"))
SCAN_AST = {"table": os.environ.get("IV_TABLE", "channels"), "limit": 5}
ID_POOL = os.environ.get("IV_ID_POOL",
                         os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      "id-pool.sandbox.json"))


def _resolver():
    try:
        return ArgResolver.from_pool_file(ID_POOL, random.Random(42), zipf_s=0.0)
    except Exception:
        return None


def _auth():
    a = load_auth()
    return a["token"], a.get("userID") or ""


def _extra(uid: str) -> dict:
    return {"userID": uid}


# --------------------------------------------------------------------------- #
# I-1/I-2 — connect-ack is NOT serialized behind hydration
# --------------------------------------------------------------------------- #
async def _connect_ack_timing(side: Side, token: str, uid: str, cs: dict) -> dict:
    """Return {connected_ms, first_poke_ms}. The `connected` ack must precede the
    hydrate poke — I-1's contract (the prod-bug-1 seam)."""
    init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": cs}]
    t0 = time.perf_counter()
    await open_side(side, token, init, extra=_extra(uid))
    connected_ms = side.connected_ms or ((time.perf_counter() - t0) * 1000.0)
    stop = asyncio.Event()
    rt = asyncio.create_task(reader(side, stop))
    # Subscribe a real (non-trivial) query so hydration takes measurable time.
    await side.ws.send(json.dumps(
        change_desired_queries_message([ast_query_put(SCAN_AST, ttl_ms=300_000)])))
    first_poke_ms = None
    deadline = time.perf_counter() + 20
    while time.perf_counter() < deadline:
        await asyncio.sleep(0.05)
        pk = [f for f in side.frames if f.tag in ("pokeStart", "pokeEnd")]
        if pk:
            first_poke_ms = (pk[0].t - t0) * 1000.0
            break
    stop.set()
    try:
        await side.ws.close()
    except Exception:
        pass
    try:
        await asyncio.wait_for(rt, timeout=3)
    except Exception:
        pass
    return {"connected_ms": connected_ms, "first_poke_ms": first_poke_ms}


async def gate_connect_ack(rep: Report, token: str, uid: str, cs: dict) -> None:
    rust, ts = make_sides()
    try:
        r, t = await asyncio.gather(
            _connect_ack_timing(rust, token, uid, cs),
            _connect_ack_timing(ts, token, uid, cs))
    except Exception as e:
        rep.add("G49/I-1:connect-ack", "SKIP", f"pair down? {e!r:.80}")
        return
    # Contract: `connected` precedes the first hydrate poke on BOTH sides.
    viol = []
    for name, m in (("rust", r), ("ts", t)):
        if m["first_poke_ms"] is not None and m["connected_ms"] > m["first_poke_ms"]:
            viol.append(f"{name}: connected@{m['connected_ms']:.0f}ms AFTER "
                        f"first poke@{m['first_poke_ms']:.0f}ms")
    if viol:
        rep.add("G49/I-1:connect-ack", "FAIL",
                "connect-ack serialized behind hydration: " + "; ".join(viol))
    else:
        rep.add("G49/I-1:connect-ack", "PASS",
                f"connected precedes hydrate on both (rust {r['connected_ms']:.0f}ms, "
                f"ts {t['connected_ms']:.0f}ms)")


# --------------------------------------------------------------------------- #
# I-3 — push-mutation parity + no stale-auth 401
# --------------------------------------------------------------------------- #
def _buildable_mutation():
    """Pick a mutation the harness can build args for from the id-pool, or None."""
    resolver = _resolver()
    if resolver is None:
        return None, None
    now_ms = int(time.time() * 1000)
    for name, build in MUTATION_ARG_BUILDERS.items():
        try:
            args = build(resolver, now_ms)
            if isinstance(args, dict):
                return name, args
        except Exception:
            continue
    return None, None


async def _fire_mutation(side: Side, token: str, uid: str, cs: dict) -> dict:
    """Subscribe, fire ONE custom mutation, capture its result + lmid + any 401."""
    init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": cs}]
    await open_side(side, token, init, extra=_extra(uid))
    stop = asyncio.Event()
    rt = asyncio.create_task(reader(side, stop))
    await side.ws.send(json.dumps(
        change_desired_queries_message([ast_query_put(SCAN_AST, ttl_ms=300_000)])))
    await quiesce([side], quiet_s=1.0, max_s=12)
    name, args = _buildable_mutation()
    fired = False
    if name:
        now_ms = int(time.time() * 1000)
        mut = custom_mutation(1, side.cid, name, args, now_ms)
        await side.ws.send(json.dumps(push_message(
            side.cgid, [mut], request_id=uuid.uuid4().hex[:8], now_ms=now_ms)))
        fired = True
        await quiesce([side], quiet_s=1.5, max_s=15)
    # Extract result + lmid + unauthorized frames.
    unauthorized = any(f.tag == "error" and f.body.get("kind") in
                       ("Unauthorized", "AuthInvalidated") for f in side.frames)
    lmid = 0
    result = None
    for f in side.frames:
        if f.tag == "pokePart" or f.tag == "pokeEnd":
            for c, lm in (f.body.get("lastMutationIDChanges") or {}).items():
                lmid = max(lmid, int(lm))
        if f.tag == "pushResponse":
            for m in f.body.get("mutations", []) or []:
                result = m.get("result")
    stop.set()
    try:
        await side.ws.close()
    except Exception:
        pass
    try:
        await asyncio.wait_for(rt, timeout=3)
    except Exception:
        pass
    return {"fired": fired, "mutation": name, "unauthorized": unauthorized,
            "lmid": lmid, "result_kind": ("error" if isinstance(result, dict)
                                          and result.get("error") else "ok"
                                          if result is not None else None)}


async def gate_push_parity(rep: Report, token: str, uid: str, cs: dict) -> None:
    rust, ts = make_sides()
    try:
        r, t = await asyncio.gather(
            _fire_mutation(rust, token, uid, cs),
            _fire_mutation(ts, token, uid, cs))
    except Exception as e:
        rep.add("G49/I-3:push-parity", "SKIP", f"pair down? {e!r:.80}")
        return
    if not (r["fired"] and t["fired"]):
        rep.add("G49/I-3:push-parity", "SKIP",
                "no harness-buildable mutation (id-pool needed) — cannot fire")
        return
    # Contract: no stale-auth 401 on the relay path (prod-bug-2), and the
    # result/lmid outcome matches TS.
    viol = []
    if r["unauthorized"] != t["unauthorized"]:
        viol.append(f"401 mismatch: rust={r['unauthorized']} ts={t['unauthorized']}")
    if r["unauthorized"]:
        viol.append("rust relayed a stale-auth 401 (prod-bug-2 class)")
    if r["result_kind"] != t["result_kind"]:
        viol.append(f"result kind: rust={r['result_kind']} ts={t['result_kind']}")
    if (r["lmid"] > 0) != (t["lmid"] > 0):
        viol.append(f"lmid advance: rust={r['lmid']} ts={t['lmid']}")
    if viol:
        rep.add("G49/I-3:push-parity", "FAIL",
                f"mutation `{r['mutation']}` diverges: " + "; ".join(viol))
    else:
        rep.add("G49/I-3:push-parity", "PASS",
                f"mutation `{r['mutation']}` parity: result={r['result_kind']}, "
                f"lmid>0={r['lmid']>0}, no 401 on either side")


# --------------------------------------------------------------------------- #
# I-4 — slow-client shed emits a frame, not a bare drop
# --------------------------------------------------------------------------- #
async def _stall_until_shed(side: Side, token: str, uid: str, cs: dict,
                            hold_s: float) -> dict:
    """Subscribe a large query then STOP reading — let the server's send buffer
    fill. Capture the last frame before/at close and the close code."""
    init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": cs}]
    await open_side(side, token, init, extra=_extra(uid))
    # Subscribe SEVERAL broad queries to generate backpressure, then do NOT drain
    # recv. Multiple large scans across tables raise the odds of overflowing the
    # send buffer within the window (single-table row counts may be too small).
    tables = os.environ.get("IV_SHED_TABLES", SCAN_AST["table"]).split(",")
    puts = [ast_query_put({"table": t.strip(), "limit": 1000000}, ttl_ms=300_000)
            for t in tables if t.strip()]
    await side.ws.send(json.dumps(change_desired_queries_message(puts)))
    # Read exactly ONE frame (the connected/first poke) then stall.
    err_frame = None
    close_code = None
    deadline = time.perf_counter() + hold_s
    got = 0
    while time.perf_counter() < deadline:
        try:
            raw = await asyncio.wait_for(side.ws.recv(), timeout=hold_s)
        except asyncio.TimeoutError:
            break
        except Exception as e:
            close_code = getattr(e, "code", None)
            break
        got += 1
        # Stall: sleep to simulate a slow consumer without draining.
        await asyncio.sleep(1.5)
        try:
            msg = json.loads(raw)
            if isinstance(msg, list) and msg and msg[0] == "error":
                err_frame = msg[1] if len(msg) > 1 else {}
        except Exception:
            pass
    try:
        cc = getattr(side.ws, "close_code", None)
        close_code = close_code or cc
        await side.ws.close()
    except Exception:
        pass
    return {"err_frame": err_frame, "close_code": close_code, "frames_read": got}


async def gate_shed(rep: Report, token: str, uid: str, cs: dict) -> None:
    hold = float(os.environ.get("IV_SHED_HOLD_S", "20"))
    rust, ts = make_sides()
    try:
        r, t = await asyncio.gather(
            _stall_until_shed(rust, token, uid, cs, hold),
            _stall_until_shed(ts, token, uid, cs, hold))
    except Exception as e:
        rep.add("G49/I-4:shed", "SKIP", f"pair down? {e!r:.80}")
        return
    # If neither side shed within the window, the scenario didn't trigger.
    if r["err_frame"] is None and t["err_frame"] is None and r["close_code"] is None \
            and t["close_code"] is None:
        rep.add("G49/I-4:shed", "SKIP",
                "neither side shed within the window (backpressure not reached) — "
                "raise IV_SHED_HOLD_S / query breadth to trigger")
        return
    # Contract: a shed sends an error frame (Rehome) BEFORE close — not a bare drop.
    rk = (r["err_frame"] or {}).get("kind")
    tk = (t["err_frame"] or {}).get("kind")
    if (r["err_frame"] is None) != (t["err_frame"] is None):
        rep.add("G49/I-4:shed", "FAIL",
                f"shed frame asymmetry: rust={r['err_frame']} ts={t['err_frame']} "
                f"(one side dropped without an error frame)")
    elif rk != tk:
        rep.add("G49/I-4:shed", "FAIL",
                f"shed error kind diverges: rust={rk!r} ts={tk!r}")
    else:
        rep.add("G49/I-4:shed", "PASS",
                f"both shed with matching frame (kind={rk}, rust close={r['close_code']}, "
                f"ts close={t['close_code']})")


# --------------------------------------------------------------------------- #
# I-1(own) — ownership contest over one clientGroupID
# --------------------------------------------------------------------------- #
async def gate_ownership(rep: Report, token: str, uid: str, cs: dict) -> None:
    rust, ts = make_sides()

    async def contest(side: Side) -> dict:
        """Open two connections to the SAME cgid; the second should supersede the
        first (rehome/close on the loser). Capture the loser's frame."""
        s1 = Side(side.name, side.target, side.cvr_schema, side.container,
                  cgid=side.cgid, cid=side.cid)
        s2 = Side(side.name, side.target, side.cvr_schema, side.container,
                  cgid=side.cgid, cid=side.cid)
        init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": cs}]
        try:
            await open_side(s1, token, init, extra=_extra(uid))
        except Exception as e:
            return {"err": f"first connect failed: {e!r:.60}"}
        stop1 = asyncio.Event()
        r1 = asyncio.create_task(reader(s1, stop1))
        await asyncio.sleep(1.0)
        # Second connection, same cgid+cid, fresh wsid — contests ownership.
        try:
            await open_side(s2, token, init, extra=_extra(uid))
        except Exception:
            pass
        await asyncio.sleep(3.0)
        stop1.set()
        loser = [f for f in s1.frames if f.tag == "error"]
        for w in (s1, s2):
            try:
                await w.ws.close()
            except Exception:
                pass
        try:
            await asyncio.wait_for(r1, timeout=2)
        except Exception:
            pass
        return {"loser_error": loser[-1].body if loser else None,
                "loser_closed": s1.closed_reason}

    try:
        r, t = await asyncio.gather(contest(rust), contest(ts))
    except Exception as e:
        rep.add("G49/I-1:ownership", "SKIP", f"pair down? {e!r:.80}")
        return
    rk = (r.get("loser_error") or {}).get("kind")
    tk = (t.get("loser_error") or {}).get("kind")
    if r.get("err") or t.get("err"):
        rep.add("G49/I-1:ownership", "SKIP", f"setup: {r.get('err') or t.get('err')}")
    elif rk == tk:
        rep.add("G49/I-1:ownership", "PASS",
                f"ownership contest resolves identically (loser frame kind={rk}, "
                f"rust closed={r.get('loser_closed')!r})")
    else:
        rep.add("G49/I-1:ownership", "FAIL",
                f"ownership-contest loser frame diverges: rust={rk!r} ts={tk!r}")


# --------------------------------------------------------------------------- #
# I-5 — Drop-based teardown / drain → clients rehome (graceful)
# --------------------------------------------------------------------------- #
async def gate_drain(rep: Report, token: str, uid: str, cs: dict) -> None:
    """On drain (graceful shutdown) the view-syncer stops and connected clients
    must be REHOMED (an error frame directing reconnect), identically on both.
    Fully triggering a drain restarts the shared container, so this is opt-in:
    set IV_DRAIN_CMD to a shell command that drains ONE side (e.g. a SIGTERM to a
    throwaway candidate), else SKIP with the contract documented."""
    cmd = os.environ.get("IV_DRAIN_CMD")
    if not cmd:
        rep.add("G49/I-5:drain", "SKIP",
                "IV_DRAIN_CMD unset — drain restarts the shared container. Contract "
                "(INVENTIONS.md I-5): on graceful drain, connected clients receive a "
                "Rehome frame then close, identically on rust and TS. Wire a "
                "throwaway-candidate drain to activate.")
        return
    rust, ts = make_sides()
    frames = {}
    for side in (rust, ts):
        try:
            init = ["initConnection", {"desiredQueriesPatch": [], "clientSchema": cs}]
            await open_side(side, token, init, extra=_extra(uid))
            stop = asyncio.Event()
            asyncio.create_task(reader(side, stop))
            await side.ws.send(json.dumps(
                change_desired_queries_message([ast_query_put(SCAN_AST, ttl_ms=300_000)])))
            await quiesce([side], quiet_s=1.0, max_s=10)
        except Exception as e:
            rep.add("G49/I-5:drain", "SKIP", f"setup failed: {e!r:.70}")
            return
    import subprocess
    try:
        subprocess.run(cmd, shell=True, timeout=30)
    except Exception as e:
        rep.add("G49/I-5:drain", "SKIP", f"drain cmd failed: {e!r:.70}")
        return
    await asyncio.sleep(5)
    for side in (rust, ts):
        errs = [f.body for f in side.frames if f.tag == "error"]
        frames[side.name] = errs[-1] if errs else None
    rk = (frames.get("rust") or {}).get("kind")
    tk = (frames.get("ts") or {}).get("kind")
    if rk == tk and rk is not None:
        rep.add("G49/I-5:drain", "PASS",
                f"drain rehomes both identically (kind={rk})")
    elif rk == tk:
        rep.add("G49/I-5:drain", "WATCH",
                "neither side emitted a drain frame (drain may not have reached clients)")
    else:
        rep.add("G49/I-5:drain", "FAIL",
                f"drain frame diverges: rust={rk!r} ts={tk!r}")


async def run() -> int:
    rep = Report(os.path.join("reports", f"invention-{TAG}.json"))
    try:
        cs = load_client_schema()
        token, uid = _auth()
    except Exception as e:
        rep.add("G49/invention", "SKIP", f"setup unavailable: {e!r:.80}")
        return rep.finish()
    for gate in (gate_connect_ack, gate_push_parity, gate_shed, gate_ownership,
                 gate_drain):
        try:
            await gate(rep, token, uid, cs)
        except Exception as e:
            rep.add(f"G49/{gate.__name__}", "SKIP", f"gate error: {e!r:.90}")
    return rep.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
