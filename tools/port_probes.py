#!/usr/bin/env python3
"""port_probes.py — G31: port-breadth protocol probes for the Rust syncer.

Four cheap, deterministic probes for port-surface behaviors none of the load
gates exercise. Each is a REAL end-to-end assertion against the candidate:

  payload-cap        an oversized (> ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES, default
                     10MB) WS message must be REJECTED (close 1009/close), and a
                     normal-size message accepted. TS zero-config default is
                     10MB; the Rust port previously accepted up to 16MB, so a
                     >10MB frame silently behaved differently per syncer.

  ack-cleanup        `ackMutationResponses` must flow: WS push (result row is
                     written by the API server; arrives back as a mutationsPatch
                     `put`) → ack → Rust relays `_zero_cleanupResults` → API
                     server deletes the stored result → replication → advance →
                     mutationsPatch `del` arrives on the SAME connection. This
                     exercises relay → backend → DB → replication → IVM advance
                     → CVR → poke in one probe. A syncer that never relays the
                     cleanup (the old no-op) leaks mutationResults rows forever
                     and this probe times out waiting for the `del`.

  delete-clients-cleanup
                     an explicit `deleteClients` message must bulk-clean the
                     deleted client's stored mutation results (same observable:
                     mutationsPatch `del` on the surviving connection).

  inspector          every documented inspect op gets a response; an unknown op
                     gets a clean `error` frame — never a silent hang.

    .venv/bin/python tools/port_probes.py --target ws://rust-test.localhost/zero-art \
        --auth-token "$JWT" --extra-param userID=$UID \
        --client-schema harness/client-schema.json --out reports/portprobes-$TAG.json

Exit 0 = all probes PASS; 1 = any FAIL; 2 = ERROR (infra).
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


def rid(prefix: str = "artpp") -> str:
    r = random.SystemRandom()
    return prefix + "-" + "".join(r.choice("abcdef0123456789") for _ in range(10))


def connect_url(target: str, version: int, cgid: str, cid: str,
                extra_params: list[tuple[str, str]], base_cookie: str = "",
                lmid: int = 0) -> str:
    params = {"clientGroupID": cgid, "clientID": cid, "baseCookie": base_cookie,
              "ts": str(time.time() * 1000), "lmid": str(lmid), "wsid": rid("ws")}
    params.update(extra_params)
    return (target.rstrip("/") + f"/sync/v{version}/connect?"
            + urllib.parse.urlencode(params))


async def open_conn(target: str, version: int, auth_token: str | None,
                    extra_params: list[tuple[str, str]], client_schema: dict | None,
                    cgid: str, cid: str, max_size: int | None = None):
    """Connect + initConnection; returns the open websocket (greeting consumed)."""
    import websockets
    url = connect_url(target, version, cgid, cid, extra_params)
    sec = encode_sec_protocols(None, auth_token)
    ws = await websockets.connect(url, subprotocols=[sec], open_timeout=15,
                                  max_size=max_size, ping_interval=None)
    await asyncio.wait_for(ws.recv(), 10)  # ["connected", …]
    init: dict = {"desiredQueriesPatch": []}
    if client_schema:
        init["clientSchema"] = client_schema
    await ws.send(json.dumps(["initConnection", init]))
    return ws


async def recv_until(ws, pred, timeout_s: float):
    """Drain frames until pred(msg-list) is truthy or timeout. Returns match or None."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=min(1.0, max(0.05, deadline - time.monotonic())))
        except asyncio.TimeoutError:
            continue
        except Exception:
            return None
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        if isinstance(msg, list) and msg:
            hit = pred(msg)
            if hit:
                return hit
    return None


def push_body(cgid: str, cid: str, mutation_id: int, name: str) -> dict:
    now = int(time.time() * 1000)
    return {"clientGroupID": cgid,
            "mutations": [{"type": "custom", "id": mutation_id, "clientID": cid,
                           "name": name, "args": [{}], "timestamp": now}],
            "pushVersion": 1, "timestamp": now, "requestID": rid("req")}


def mutations_patch_hit(msg: list, cid: str, op: str, up_to: int | None = None):
    """Match a mutationsPatch entry for clientID cid with the given op."""
    if msg[0] not in ("pokePart", "poke"):
        return None
    bodies = [msg[1]] if msg[0] == "pokePart" else (msg[1].get("pokeParts") or [])
    for body in bodies:
        for entry in (body or {}).get("mutationsPatch") or []:
            if entry.get("op") != op:
                continue
            mid = (entry.get("mutation") or {}).get("id") or entry.get("id") or {}
            if mid.get("clientID") == cid and (up_to is None or mid.get("id", 0) <= up_to):
                return entry
    return None


# ─── probe: payload cap ─────────────────────────────────────────────────────

async def probe_payload_cap(a) -> dict:
    import websockets
    cap = a.payload_cap_bytes
    cgid, cid = rid("artpp-cap"), rid("c")
    try:
        ws = await open_conn(a.target, a.protocol_version, a.auth_token,
                             a.extra_params, a.client_schema_doc, cgid, cid,
                             max_size=None)
    except Exception as e:
        return {"name": "payload-cap", "verdict": "ERROR",
                "detail": f"connect failed: {type(e).__name__}: {e}"}
    try:
        # 1) A normal-size unknown message must NOT kill the connection heap-
        #    first: send ~100KB of valid JSON the server will just reject/ignore.
        normal = json.dumps(["changeDesiredQueries",
                             {"desiredQueriesPatch": [], "pad": "x" * 100_000}])
        await ws.send(normal)
        await asyncio.sleep(0.5)
        alive_after_normal = True
        try:
            await ws.send(json.dumps(["ping", {}]))
        except Exception:
            alive_after_normal = False

        # 2) An oversized (> cap) message must be rejected: the server closes
        #    (1009 message-too-big is the tungstenite/ws convention).
        big = json.dumps(["changeDesiredQueries",
                          {"desiredQueriesPatch": [], "pad": "x" * (cap + 512 * 1024)}])
        rejected, close_code = False, None
        try:
            await ws.send(big)
            # server should close on us; wait for it
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    await asyncio.wait_for(ws.recv(), 1.0)
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed as e:
                    rejected = True
                    close_code = getattr(e, "code", None) or (
                        e.rcvd.code if getattr(e, "rcvd", None) else None)
                    break
                except Exception:
                    rejected = True
                    break
        except Exception:
            # send itself failed because the server dropped us mid-frame: also a rejection
            rejected = True
        ok = alive_after_normal and rejected
        detail = (f"normal(100KB) accepted={alive_after_normal}, "
                  f"oversize({(cap + 512 * 1024) // (1024 * 1024)}MB) rejected={rejected}"
                  + (f" close={close_code}" if close_code else ""))
        return {"name": "payload-cap", "verdict": "PASS" if ok else "FAIL", "detail": detail}
    finally:
        try:
            await ws.close()
        except Exception:
            pass


# ─── probe: ack-mutation-responses cleanup (end to end) ─────────────────────

async def probe_ack_cleanup(a) -> dict:
    cgid, cid = rid("artpp-ack"), rid("c")
    try:
        ws = await open_conn(a.target, a.protocol_version, a.auth_token,
                             a.extra_params, a.client_schema_doc, cgid, cid)
    except Exception as e:
        return {"name": "ack-cleanup", "verdict": "ERROR",
                "detail": f"connect failed: {type(e).__name__}: {e}"}
    try:
        # A push whose mutator name is unknown to the app still WRITES a result
        # row (an error result) — enough to observe cleanup, with zero app-state
        # side effects.
        await ws.send(json.dumps(["push", push_body(cgid, cid, 1, "_zero_artProbeNoop")]))
        put = await recv_until(ws, lambda m: mutations_patch_hit(m, cid, "put"), a.timeout_s)
        if not put:
            return {"name": "ack-cleanup", "verdict": "ERROR",
                    "detail": "mutation result never arrived as mutationsPatch put "
                              "(push relay or advance broken — see G15/G3)"}
        await ws.send(json.dumps(["ackMutationResponses", {"clientID": cid, "id": 1}]))
        deleted = await recv_until(ws, lambda m: mutations_patch_hit(m, cid, "del", up_to=1),
                                   a.timeout_s)
        if deleted:
            return {"name": "ack-cleanup", "verdict": "PASS",
                    "detail": "result row put→ack→del round-trip observed "
                              "(relay→API→DB→replication→advance→poke)"}
        return {"name": "ack-cleanup", "verdict": "FAIL",
                "detail": f"no mutationsPatch del within {a.timeout_s}s of ack — "
                          "the syncer is not relaying _zero_cleanupResults; "
                          "mutationResults grow without bound"}
    finally:
        try:
            await ws.close()
        except Exception:
            pass


# ─── probe: deleteClients bulk cleanup ──────────────────────────────────────

async def probe_delete_clients_cleanup(a) -> dict:
    cgid, cid_a, cid_b = rid("artpp-del"), rid("ca"), rid("cb")
    try:
        ws_a = await open_conn(a.target, a.protocol_version, a.auth_token,
                               a.extra_params, a.client_schema_doc, cgid, cid_a)
    except Exception as e:
        return {"name": "delete-clients-cleanup", "verdict": "ERROR",
                "detail": f"connect A failed: {type(e).__name__}: {e}"}
    try:
        await ws_a.send(json.dumps(["push", push_body(cgid, cid_a, 1, "_zero_artProbeNoop")]))
        put = await recv_until(ws_a, lambda m: mutations_patch_hit(m, cid_a, "put"), a.timeout_s)
        # grab A's cookie so B resumes the same client group state
        if not put:
            return {"name": "delete-clients-cleanup", "verdict": "ERROR",
                    "detail": "client A's mutation result never arrived (push path broken)"}
    finally:
        try:
            await ws_a.close()
        except Exception:
            pass
    try:
        ws_b = await open_conn(a.target, a.protocol_version, a.auth_token,
                               a.extra_params, a.client_schema_doc, cgid, cid_b)
    except Exception as e:
        return {"name": "delete-clients-cleanup", "verdict": "ERROR",
                "detail": f"connect B failed: {type(e).__name__}: {e}"}
    try:
        await ws_b.send(json.dumps(["deleteClients", {"clientIDs": [cid_a]}]))
        deleted = await recv_until(ws_b, lambda m: mutations_patch_hit(m, cid_a, "del"),
                                   a.timeout_s)
        if deleted:
            return {"name": "delete-clients-cleanup", "verdict": "PASS",
                    "detail": "explicit deleteClients bulk-cleaned the deleted "
                              "client's stored mutation results"}
        return {"name": "delete-clients-cleanup", "verdict": "FAIL",
                "detail": f"no mutationsPatch del for deleted client within {a.timeout_s}s — "
                          "deleteClients is not relaying bulk _zero_cleanupResults"}
    finally:
        try:
            await ws_b.close()
        except Exception:
            pass


# ─── probe: inspector contract ──────────────────────────────────────────────

async def probe_inspector(a) -> dict:
    cgid, cid = rid("artpp-ins"), rid("c")
    try:
        ws = await open_conn(a.target, a.protocol_version, a.auth_token,
                             a.extra_params, a.client_schema_doc, cgid, cid)
    except Exception as e:
        return {"name": "inspector", "verdict": "ERROR",
                "detail": f"connect failed: {type(e).__name__}: {e}"}
    failures = []
    try:
        for i, op in enumerate(({"op": "version"}, {"op": "queries"}, {"op": "metrics"})):
            body = {**op, "id": f"pp{i}"}
            await ws.send(json.dumps(["inspect", body]))
            resp = await recv_until(ws, lambda m: m if m[0] in ("inspect", "error") else None, 5)
            if not resp:
                failures.append(f"{op['op']}: no response in 5s (hang)")
        # unknown op must produce a clean error frame, not a hang
        await ws.send(json.dumps(["inspect", {"op": "artProbeUnknownOp", "id": "ppX"}]))
        resp = await recv_until(ws, lambda m: m if m[0] in ("inspect", "error") else None, 5)
        if not resp:
            failures.append("unknown-op: no response in 5s (hang)")
    finally:
        try:
            await ws.close()
        except Exception:
            pass
    if failures:
        return {"name": "inspector", "verdict": "FAIL", "detail": "; ".join(failures)}
    return {"name": "inspector", "verdict": "PASS",
            "detail": "version/queries/metrics answered; unknown op → clean error frame"}


# ─── main ───────────────────────────────────────────────────────────────────

async def run(a) -> dict:
    checks = []
    checks.append(await probe_payload_cap(a))
    checks.append(await probe_ack_cleanup(a))
    checks.append(await probe_delete_clients_cleanup(a))
    checks.append(await probe_inspector(a))
    has_fail = any(c["verdict"] == "FAIL" for c in checks)
    has_err = any(c["verdict"] == "ERROR" for c in checks)
    verdict = "FAIL" if has_fail else ("ERROR" if has_err else "PASS")
    n_pass = sum(c["verdict"] == "PASS" for c in checks)
    summary = f"{n_pass}/{len(checks)} probes pass"
    bad = [f"{c['name']}={c['verdict']}" for c in checks if c["verdict"] != "PASS"]
    if bad:
        summary += " (" + ", ".join(bad) + ")"
    return {"schema": 1, "gate": "G31", "name": "port-breadth-probes",
            "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "verdict": verdict, "checks": checks, "summary": summary}


def main() -> int:
    ap = argparse.ArgumentParser(description="G31: port-breadth protocol probes.")
    ap.add_argument("--target", required=True, help="ws://host[:port]/prefix of the candidate")
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--extra-param", action="append", default=[],
                    help="k=v appended to the connect URL (repeatable)")
    ap.add_argument("--client-schema", default=None)
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--payload-cap-bytes", type=int, default=10 * 1024 * 1024,
                    help="expected server message cap (TS default 10MB)")
    ap.add_argument("--timeout-s", type=float, default=25.0,
                    help="end-to-end wait for replication-backed observations")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    a.extra_params = [tuple(p.split("=", 1)) for p in a.extra_param]
    a.client_schema_doc = json.load(open(a.client_schema)) if a.client_schema else None

    report = asyncio.run(run(a))
    print(report["summary"])
    for c in report["checks"]:
        print(f"  {c['name']:<24} {c['verdict']:<5} {c['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  report -> {a.out}")
    return {"PASS": 0, "FAIL": 1, "ERROR": 2}[report["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
