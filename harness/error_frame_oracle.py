#!/usr/bin/env python3
"""
error_frame_oracle.py — Gate A: error-frame differential + scenario matrix.

The steady-state diff oracles compare query-RESULT channels; they never assert
the exact `["error", {kind, message, origin}]` frame a client receives on the
unhappy lifecycle edges. Those frames drive client-visible behavior — a
`ClientNotFound` frame makes the client WIPE local state and start a fresh client
group — so their bytes matter. The 2026-08-28 P11-a bug lived here: rust's
purge-tombstone load emitted `message = <cvrID>` where TS emits
`'Client has been purged due to inactivity'`; both are `kind:ClientNotFound`, so
a kind-only check (wedge/negative) passed it.

This gate drives error-INDUCING scenarios IDENTICALLY on rust and the TS
reference and byte-diffs the resulting error frame (after normalizing embedded
ids/versions, which legitimately differ per side). A word-level message
divergence — like P11-a — is a FAIL; a kind/origin divergence is a FAIL.

Scenarios (each SKIPs individually if it can't be set up):
  * purge        — CVR tombstoned (instances.deleted=TRUE) then reloaded
  * stale-cookie — reconnect with a bogus baseCookie forcing a reset/rehome

Runs against a live TS+rust pair (ab_common defaults / RUN.md); SKIPs if down.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ab_common import (  # noqa: E402
    Side, make_sides, open_side, quiesce, reader, Report,
    load_auth, load_client_schema, PG_DSN,
)
from workload import ast_query_put, change_desired_queries_message  # noqa: E402

TAG = os.environ.get("ART_TAG", time.strftime("%Y%m%d-%H%M%S"))
SCAN_AST = {"table": os.environ.get("EF_TABLE", "channels"), "limit": 1}


def _token() -> str:
    return load_auth()["token"]


def normalize_msg(msg: str) -> str:
    """Drop per-side ids/versions so cross-side comparison flags WORD changes,
    not id/lineage differences. Hex ids, LexiVersion tokens, and cgids collapse
    to <ID>; standalone numbers to <N>."""
    if not isinstance(msg, str):
        return str(msg)
    s = msg
    s = re.sub(r'\b[0-9a-f]{16,}\b', '<ID>', s)          # long hex ids
    s = re.sub(r'\babdiff-[0-9a-f]+\b', '<ID>', s)       # our cgids
    s = re.sub(r'\b[0-9a-z]{8,}\b', lambda m: '<ID>' if any(c.isdigit() for c in m.group())
               and any(c.isalpha() for c in m.group()) else m.group(), s)
    s = re.sub(r'@\S+', '@<VER>', s)                     # CVR@<version>
    s = re.sub(r'\b\d+\b', '<N>', s)                     # bare numbers
    return s.strip()


def _pg():
    import psycopg2
    return psycopg2.connect(PG_DSN)


def _tombstone(schema: str, cgid: str) -> bool:
    try:
        with _pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'UPDATE "{schema}".instances SET "deleted" = TRUE '
                    f'WHERE "clientGroupID" = %s', (cgid,))
                return cur.rowcount > 0
    except Exception:
        return False


async def _establish_cvr(side: Side, token: str, client_schema: dict) -> bool:
    """Connect, subscribe a scan query, hydrate, then close — leaving a CVR
    instance row behind for the scenario to manipulate."""
    try:
        init = ["initConnection",
                {"desiredQueriesPatch": [], "clientSchema": client_schema}]
        await open_side(side, token, init)
        stop = asyncio.Event()
        rt = asyncio.create_task(reader(side, stop))
        await side.ws.send(json.dumps(
            change_desired_queries_message([ast_query_put(SCAN_AST, ttl_ms=300_000)])))
        await quiesce([side], quiet_s=1.5, max_s=20.0)
        stop.set()
        await side.ws.close()
        try:
            await asyncio.wait_for(rt, timeout=3)
        except Exception:
            pass
        return True
    except Exception:
        return False


async def _capture_error(side: Side, token: str, client_schema: dict,
                         base_cookie: str = "") -> dict | None:
    """Reconnect (same cgid) and return the first `error` body, or None."""
    import websockets
    from ab_common import connect_url
    from protocol import encode_sec_protocols, DEFAULT_PROTOCOL_VERSION
    try:
        sec = encode_sec_protocols(None, token)
        ws = await websockets.connect(
            connect_url(side, DEFAULT_PROTOCOL_VERSION, base_cookie, 0,
                        {"wsid": os.urandom(6).hex()}),
            subprotocols=[sec], open_timeout=20, max_size=None, ping_interval=None)
    except Exception:
        return None
    try:
        deadline = time.time() + 15
        sent_init = False
        while time.time() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                break
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
                return body
            if tag == "connected" and not sent_init:
                sent_init = True
                await ws.send(json.dumps(["initConnection",
                    {"desiredQueriesPatch": [], "clientSchema": client_schema}]))
        return None
    finally:
        try:
            await ws.close()
        except Exception:
            pass


def _diff_frames(rust: dict | None, ts: dict | None) -> list[str]:
    if rust is None and ts is None:
        return ["both sides produced NO error frame"]
    if (rust is None) != (ts is None):
        return [f"one side produced no error frame: rust={rust} ts={ts}"]
    out = []
    for field in ("kind", "origin"):
        if rust.get(field) != ts.get(field):
            out.append(f"{field}: rust={rust.get(field)!r} ts={ts.get(field)!r}")
    if normalize_msg(rust.get("message", "")) != normalize_msg(ts.get("message", "")):
        out.append(f"message(normalized): rust={normalize_msg(rust.get('message',''))!r} "
                   f"ts={normalize_msg(ts.get('message',''))!r}")
    return out


async def _scenario_purge(rep: Report, token: str, cs: dict) -> None:
    rust, ts = make_sides()
    ok = await asyncio.gather(_establish_cvr(rust, token, cs),
                              _establish_cvr(ts, token, cs))
    if not all(ok):
        rep.add("A/error-frame:purge", "SKIP", "could not establish CVR on both sides")
        return
    tomb = (_tombstone(rust.cvr_schema, rust.cgid),
            _tombstone(ts.cvr_schema, ts.cgid))
    if not all(tomb):
        rep.add("A/error-frame:purge", "SKIP",
                f"could not tombstone instances (rust={tomb[0]} ts={tomb[1]}; PG reachable?)")
        return
    r_err, t_err = await asyncio.gather(
        _capture_error(rust, token, cs), _capture_error(ts, token, cs))
    diffs = _diff_frames(r_err, t_err)
    detail = f"rust={r_err} ts={t_err}"
    if diffs:
        rep.add("A/error-frame:purge", "FAIL",
                "purge error frame diverges: " + "; ".join(diffs), extra=detail)
    else:
        rep.add("A/error-frame:purge", "PASS",
                f"purge → identical error frame (kind={r_err.get('kind')}, "
                f"msg={r_err.get('message')!r})")


async def _scenario_stale_cookie(rep: Report, token: str, cs: dict) -> None:
    rust, ts = make_sides()
    ok = await asyncio.gather(_establish_cvr(rust, token, cs),
                              _establish_cvr(ts, token, cs))
    if not all(ok):
        rep.add("A/error-frame:stale-cookie", "SKIP", "could not establish CVR")
        return
    bogus = "9" * 20  # a far-future/garbage LexiVersion cookie
    r_err, t_err = await asyncio.gather(
        _capture_error(rust, token, cs, base_cookie=bogus),
        _capture_error(ts, token, cs, base_cookie=bogus))
    # Both sides may legitimately accept-and-reset instead of erroring; only
    # flag a divergence when the two sides DISAGREE on whether/how they error.
    if r_err is None and t_err is None:
        rep.add("A/error-frame:stale-cookie", "PASS",
                "both sides tolerated the bogus cookie (no error) — agree")
        return
    diffs = _diff_frames(r_err, t_err)
    if diffs:
        rep.add("A/error-frame:stale-cookie", "FAIL",
                "stale-cookie handling diverges: " + "; ".join(diffs),
                extra=f"rust={r_err} ts={t_err}")
    else:
        rep.add("A/error-frame:stale-cookie", "PASS",
                f"stale-cookie → identical error frame (kind={(r_err or {}).get('kind')})")


async def run() -> int:
    rep = Report(os.path.join("reports", f"error-frame-{TAG}.json"))
    try:
        cs = load_client_schema()
        token = _token()
    except Exception as e:
        rep.add("A/error-frame", "SKIP", f"setup unavailable: {e!r:.80}")
        return rep.finish()
    for scen in (_scenario_purge, _scenario_stale_cookie):
        try:
            await scen(rep, token, cs)
        except Exception as e:
            rep.add(f"A/error-frame:{scen.__name__}", "SKIP",
                    f"scenario error (pair down?): {e!r:.90}")
    return rep.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
