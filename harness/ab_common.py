#!/usr/bin/env python3
"""
ab_common.py — shared machinery for the TS ⇄ Rust diff-surface gates
(G32-G42, docs/ts-rust-diff-surface-proposal.md).

A `Side` is one zero-cache image (TS reference or Rust candidate) with its own
WS target and its own CVR schema in the SHARED sandbox Postgres. The lockstep
driver runs an IDENTICAL scripted session against both sides and records every
downstream frame; the CVR dumper reads each side's `/cvr` schema scoped to the
session's clientGroupIDs and canonicalizes it for diffing.

Sandbox defaults (xyne-sandbox-rust-test):
  Rust:  ws://rust-test.localhost/zero      CVR schema sandbox_rust_test_0/cvr
  TS:    ws://localhost:4849                CVR schema sandbox_rust_test_ts_0/cvr
  PG:    postgresql://xyne:xyne123@localhost:5433/sandbox_rust_test_db
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import encode_sec_protocols, DEFAULT_PROTOCOL_VERSION  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))

PG_DSN = os.environ.get(
    "AB_PG_DSN", "postgresql://xyne:xyne123@localhost:5433/sandbox_rust_test_db")
RUST_WS = os.environ.get("AB_RUST_WS", "ws://rust-test.localhost/zero")
TS_WS = os.environ.get("AB_TS_WS", "ws://localhost:4849")
RUST_CVR_SCHEMA = os.environ.get("AB_RUST_CVR", "sandbox_rust_test_0/cvr")
TS_CVR_SCHEMA = os.environ.get("AB_TS_CVR", "sandbox_rust_test_ts_0/cvr")
RUST_CONTAINER = os.environ.get("AB_RUST_CONTAINER", "xyne-sandbox-rust-test-zero-cache")
TS_CONTAINER = os.environ.get("AB_TS_CONTAINER", "xyne-sandbox-rust-test-zero-cache-ts")


def canon(v: Any) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)


def load_auth() -> dict:
    with open(os.path.join(HERE, "auth-pool.json")) as f:
        return json.load(f)[0]


def load_client_schema() -> dict:
    with open(os.path.join(HERE, "client-schema.json")) as f:
        return json.load(f)


# --------------------------------------------------------------------------- #
@dataclass
class Frame:
    tag: str
    body: dict
    t: float           # perf_counter at receipt
    step: int          # lockstep step index during which it arrived


@dataclass
class Side:
    name: str                      # "rust" | "ts"
    target: str                    # ws base
    cvr_schema: str
    container: str
    cgid: str = ""
    cid: str = ""
    ws: Any = None
    frames: list = field(default_factory=list)     # list[Frame]
    last_activity: float = 0.0
    last_cookie: str = ""
    cookies_by_step: dict = field(default_factory=dict)   # step -> cookie
    connected_ms: Optional[float] = None
    step: int = 0
    closed_reason: str = ""

    def fresh_ids(self) -> None:
        self.cgid = "abdiff-" + uuid.uuid4().hex[:12]
        self.cid = "c-" + uuid.uuid4().hex[:8]


def make_sides() -> tuple[Side, Side]:
    rust = Side("rust", RUST_WS, RUST_CVR_SCHEMA, RUST_CONTAINER)
    ts = Side("ts", TS_WS, TS_CVR_SCHEMA, TS_CONTAINER)
    rust.fresh_ids()
    ts.fresh_ids()
    return rust, ts


def connect_url(side: Side, pv: int, base_cookie: str = "", lmid: int = 0,
                extra: Optional[dict] = None) -> str:
    params = {"clientGroupID": side.cgid, "clientID": side.cid,
              "baseCookie": base_cookie, "ts": str(time.time() * 1000),
              "lmid": str(lmid), "wsid": uuid.uuid4().hex[:12]}
    if extra:
        params.update(extra)
    return (side.target.rstrip("/")
            + f"/sync/v{pv}/connect?" + urllib.parse.urlencode(params))


async def open_side(side: Side, auth_token: str, init_msg: Optional[list],
                    pv: int = DEFAULT_PROTOCOL_VERSION, base_cookie: str = "",
                    lmid: int = 0, extra: Optional[dict] = None) -> None:
    """Open the socket, wait for 'connected', optionally send initConnection."""
    import websockets
    sec = encode_sec_protocols(None, auth_token)
    t0 = time.perf_counter()
    side.ws = await websockets.connect(
        connect_url(side, pv, base_cookie, lmid, extra),
        subprotocols=[sec], open_timeout=20, max_size=None, ping_interval=None)
    # First downstream frame is ['connected', ...] (or ['error', ...]).
    raw = await asyncio.wait_for(side.ws.recv(), timeout=15)
    msg = json.loads(raw)
    tag = msg[0] if isinstance(msg, list) and msg else "?"
    body = msg[1] if isinstance(msg, list) and len(msg) > 1 else {}
    side.frames.append(Frame(tag, body if isinstance(body, dict) else {},
                             time.perf_counter(), side.step))
    if tag == "error":
        side.closed_reason = f"error:{body.get('kind')}"
        raise ConnectionError(f"{side.name}: connect error {body}")
    if tag != "connected":
        raise ConnectionError(f"{side.name}: expected connected, got {tag}")
    side.connected_ms = (time.perf_counter() - t0) * 1000.0
    side.last_activity = time.perf_counter()
    if init_msg is not None:
        await side.ws.send(json.dumps(init_msg))


async def reader(side: Side, stop: asyncio.Event) -> None:
    """Record every downstream frame; track activity + pokeEnd cookies."""
    while not stop.is_set():
        try:
            raw = await asyncio.wait_for(side.ws.recv(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        except Exception as e:                          # socket closed
            side.closed_reason = side.closed_reason or f"closed:{e!r:.80}"
            return
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        if not isinstance(msg, list) or not msg:
            continue
        tag = msg[0]
        body = msg[1] if len(msg) > 1 and isinstance(msg[1], dict) else {}
        side.frames.append(Frame(tag, body, time.perf_counter(), side.step))
        if tag in ("pokeStart", "pokePart", "pokeEnd"):
            side.last_activity = time.perf_counter()
        if tag == "pokeEnd" and not body.get("cancel"):
            ck = body.get("cookie")
            if isinstance(ck, str) and ck:
                side.last_cookie = ck


async def quiesce(sides: list[Side], quiet_s: float = 2.5,
                  max_s: float = 45.0) -> bool:
    """Wait until every side has been poke-quiet for quiet_s (cap max_s)."""
    deadline = time.perf_counter() + max_s
    while time.perf_counter() < deadline:
        await asyncio.sleep(0.4)
        if min(time.perf_counter() - s.last_activity for s in sides) >= quiet_s:
            return True
    return False


# --------------------------------------------------------------------------- #
# Per-step delta extraction (batching-insensitive poke comparison, G35)
# --------------------------------------------------------------------------- #
def step_delta(side: Side, step: int, pks: dict[str, list[str]]) -> dict:
    """Collapse all pokeParts of one lockstep step into a canonical delta:
    final row op per (table,pk-key), got/desired patch sets, lmid changes."""
    rows: dict[tuple, Any] = {}
    got: set[tuple] = set()
    desired: set[tuple] = set()
    lmids: dict[str, int] = {}
    for f in side.frames:
        if f.step != step or f.tag != "pokePart":
            continue
        for op in f.body.get("rowsPatch") or []:
            if not isinstance(op, dict):
                continue
            table = op.get("tableName", "?")
            pk = pks.get(table)
            if op.get("op") == "put":
                val = op.get("value") or {}
                key = canon([val.get(c) for c in pk] if pk else val)
                rows[(table, key)] = ("put", canon(val))
            elif op.get("op") in ("del", "update"):
                idobj = op.get("id") or {}
                key = canon([idobj.get(c) for c in pk] if pk else idobj)
                rows[(table, key)] = (op.get("op"), canon(idobj))
        for g in f.body.get("gotQueriesPatch") or []:
            if isinstance(g, dict):
                got.add((g.get("op"), g.get("hash")))
        for pf in ("desiredQueriesPatches", "desiredQueriesPatch"):
            dq = f.body.get(pf)
            if isinstance(dq, dict):                    # clientID -> patch[]
                for cid, patch in dq.items():
                    for p in patch or []:
                        if isinstance(p, dict):
                            desired.add((cid, p.get("op"), p.get("hash")))
            elif isinstance(dq, list):
                for p in dq:
                    if isinstance(p, dict):
                        desired.add((side.cid, p.get("op"), p.get("hash")))
        for cid, lm in (f.body.get("lastMutationIDChanges") or {}).items():
            lmids[cid] = max(lmids.get(cid, 0), int(lm))
    return {"rows": rows, "got": got, "desired": desired, "lmids": lmids}


def diff_deltas(a: dict, b: dict) -> list[str]:
    """Human-readable differences between two step deltas (a=rust, b=ts)."""
    out = []
    ra, rb = a["rows"], b["rows"]
    for k in sorted(set(ra) | set(rb), key=str):
        va, vb = ra.get(k), rb.get(k)
        if va != vb:
            # `update` vs `put` for the same content is a legal wire variant;
            # only flag if the terminal CONTENT differs or presence differs.
            if va and vb and va[1] == vb[1]:
                continue
            out.append(f"row {k}: rust={va and va[0]} ts={vb and vb[0]}")
    if a["got"] != b["got"]:
        out.append(f"gotQueries: rust-only={sorted(a['got']-b['got'])} "
                   f"ts-only={sorted(b['got']-a['got'])}")
    if a["desired"] != b["desired"]:
        # clientIDs differ per side by construction; compare (op, hash) only.
        da = {(o, h) for _, o, h in a["desired"]}
        db = {(o, h) for _, o, h in b["desired"]}
        if da != db:
            out.append(f"desired: rust-only={sorted(da-db)} ts-only={sorted(db-da)}")
    la = set(a["lmids"].values())
    lb = set(b["lmids"].values())
    if la != lb:
        out.append(f"lmids: rust={sorted(la)} ts={sorted(lb)}")
    return out


# --------------------------------------------------------------------------- #
# CVR dump + canonicalization (G32/G33)
# --------------------------------------------------------------------------- #
def _pg():
    import psycopg2  # noqa
    return psycopg2.connect(PG_DSN)


def dump_cvr(schema: str, cgids: list[str]) -> dict:
    """Dump one side's CVR schema scoped to the given clientGroupIDs."""
    import psycopg2.extras
    out: dict[str, list] = {}
    q = lambda s: '"' + schema + '"."' + s + '"'  # noqa: E731
    with _pg() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f'SELECT "clientGroupID","version","replicaVersion",'
                        f'"clientSchema","profileID","ttlClock",'
                        f'extract(epoch from "lastActive") AS la,'
                        f'"owner", extract(epoch from "grantedAt") AS ga,'
                        f'COALESCE("deleted",false) AS deleted '
                        f'FROM {q("instances")} WHERE "clientGroupID" = ANY(%s)',
                        (cgids,))
            out["instances"] = [dict(r) for r in cur.fetchall()]
            cur.execute(f'SELECT "clientGroupID","clientID" FROM {q("clients")} '
                        f'WHERE "clientGroupID" = ANY(%s)', (cgids,))
            out["clients"] = [dict(r) for r in cur.fetchall()]
            cur.execute(f'SELECT "clientGroupID","queryHash","clientAST",'
                        f'"queryName","queryArgs","patchVersion",'
                        f'"transformationHash","transformationVersion",'
                        f'COALESCE("internal",false) AS internal,'
                        f'COALESCE("deleted",false) AS deleted,"rowSetSignature" '
                        f'FROM {q("queries")} WHERE "clientGroupID" = ANY(%s)',
                        (cgids,))
            out["queries"] = [dict(r) for r in cur.fetchall()]
            cur.execute(f'SELECT "clientGroupID","clientID","queryHash",'
                        f'"patchVersion",COALESCE("deleted",false) AS deleted,'
                        f'"ttlMs","inactivatedAtMs" '
                        f'FROM {q("desires")} WHERE "clientGroupID" = ANY(%s)',
                        (cgids,))
            out["desires"] = [dict(r) for r in cur.fetchall()]
            cur.execute(f'SELECT "clientGroupID","schema","table","rowKey",'
                        f'"rowVersion","patchVersion","refCounts" '
                        f'FROM {q("rows")} WHERE "clientGroupID" = ANY(%s)',
                        (cgids,))
            out["rows"] = [dict(r) for r in cur.fetchall()]
            cur.execute(f'SELECT "clientGroupID","version" FROM {q("rowsVersion")} '
                        f'WHERE "clientGroupID" = ANY(%s)', (cgids,))
            out["rowsVersion"] = [dict(r) for r in cur.fetchall()]
    return out


def canon_cvr(dump: dict) -> dict:
    """Version-INSENSITIVE canonical form of a single-client-group CVR dump.
    patchVersion / transformationVersion / version strings are dropped (they
    legally differ across independent CVR lineages); everything else must
    match. rowSetSignature is kept separately (G33)."""
    inst = dump["instances"][0] if dump["instances"] else {}
    queries = {}
    for r in dump["queries"]:
        queries[r["queryHash"]] = {
            "name": r["queryName"],
            "args": canon(r["queryArgs"]),
            "hasAST": r["clientAST"] is not None,
            "internal": r["internal"],
            "deleted": r["deleted"],
            "hasTransformation": r["transformationHash"] is not None,
        }
    desires = {}
    for r in dump["desires"]:
        desires[r["queryHash"]] = {          # single client per side → key by hash
            "deleted": r["deleted"],
            "ttlMs": r["ttlMs"],
            "inactive": r["inactivatedAtMs"] is not None,
        }
    rows = {}
    for r in dump["rows"]:
        rows[(r["schema"], r["table"], canon(r["rowKey"]))] = {
            "rowVersion": r["rowVersion"],
            "refCounts": canon(r["refCounts"]),
            "tombstone": r["refCounts"] is None,
        }
    return {
        "instance": {
            "clientSchema": canon(inst.get("clientSchema")),
            "profileID_set": inst.get("profileID") is not None,
            "replicaVersion_set": inst.get("replicaVersion") is not None,
            "deleted": inst.get("deleted"),
        },
        "clients_n": len(dump["clients"]),
        "queries": queries,
        "desires": desires,
        "rows": {canon(list(k)): v for k, v in rows.items()},
        "rowsVersion_lockstep": bool(
            dump["rowsVersion"] and dump["instances"]
            and dump["rowsVersion"][0]["version"] == dump["instances"][0]["version"]),
        "signatures": {r["queryHash"]: (r["transformationHash"], r["rowSetSignature"])
                       for r in dump["queries"]},
    }


def diff_canon(a: dict, b: dict, skip: tuple = ("signatures",)) -> list[str]:
    """Diff two canon_cvr() outputs (a=rust, b=ts). Returns difference lines."""
    out = []
    for section in ("instance", "clients_n", "rowsVersion_lockstep"):
        if canon(a[section]) != canon(b[section]):
            out.append(f"{section}: rust={a[section]} ts={b[section]}")
    for section in ("queries", "desires", "rows"):
        if section in skip:
            continue
        ka, kb = set(a[section]), set(b[section])
        for k in sorted(ka - kb, key=str):
            out.append(f"{section} rust-only: {k}")
        for k in sorted(kb - ka, key=str):
            out.append(f"{section} ts-only: {k}")
        for k in sorted(ka & kb, key=str):
            if canon(a[section][k]) != canon(b[section][k]):
                out.append(f"{section}[{k}]: rust={a[section][k]} ts={b[section][k]}")
    return out


def diff_signatures(a: dict, b: dict) -> tuple[list[str], int]:
    """G33: for hashes present on both sides with EQUAL transformationHash,
    rowSetSignature must be equal. Returns (mismatches, compared_count)."""
    out = []
    compared = 0
    sa, sb = a["signatures"], b["signatures"]
    for h in sorted(set(sa) & set(sb)):
        (tha, siga), (thb, sigb) = sa[h], sb[h]
        if tha is None or thb is None or tha != thb:
            continue                        # different transformation → not comparable
        if siga is None and sigb is None:
            continue
        compared += 1
        if siga != sigb:
            out.append(f"signature[{h}] th={tha}: rust={siga} ts={sigb}")
    return out, compared


# --------------------------------------------------------------------------- #
# Gate report
# --------------------------------------------------------------------------- #
class Report:
    def __init__(self, path: str):
        self.path = path
        self.gates: list[dict] = []

    def add(self, gate: str, verdict: str, detail: str, extra: Any = None) -> None:
        self.gates.append({"gate": gate, "verdict": verdict, "detail": detail,
                           **({"extra": extra} if extra is not None else {})})
        mark = {"PASS": "✅", "FAIL": "❌", "WATCH": "⚠️ ", "SKIP": "⏭️ "}.get(verdict, "?")
        print(f"{mark} {gate:28s} {verdict:5s} {detail}", flush=True)

    def finish(self) -> int:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w") as f:
            json.dump({"ts": time.time(), "gates": self.gates}, f, indent=1,
                      default=str)
        fails = [g for g in self.gates if g["verdict"] == "FAIL"]
        print(f"\nreport: {self.path}  —  "
              f"{sum(g['verdict'] == 'PASS' for g in self.gates)} PASS, "
              f"{len(fails)} FAIL, "
              f"{sum(g['verdict'] == 'WATCH' for g in self.gates)} WATCH")
        return 1 if fails else 0


def load_pks(client_schema: dict) -> dict[str, list[str]]:
    """table -> primaryKey columns from the client schema."""
    pks = {}
    for tname, tdef in (client_schema.get("tables") or {}).items():
        pk = tdef.get("primaryKey")
        if isinstance(pk, list) and pk:
            pks[tname] = pk
    return pks
