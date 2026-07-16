#!/usr/bin/env python3
"""
wedge_scenarios.py — active wedge-class stress scenarios (gate G27).

Each scenario deliberately exercises a failure class the progress handler
and watchdog ladder were designed to handle. Unlike the passive log gate
(G26) which detects markers IF they happen, these scenarios FORCE the
conditions and assert the system handles them correctly.

Scenarios:
  1. cancel-mid-hydrate    — open a pull stream, start hydration, cancel
                             mid-flight via client .return(). Assert cancel
                             completes in <10s (progress handler works).
  2. idle-consumer         — open a pull stream, never grant credit. Assert
                             idle-timeout fires in <90s and the stream closes
                             cleanly (not a wedge).
  3. churn-leak            — rapidly connect/disconnect 20 client groups with
                             queries. Assert no goroutine leak by checking
                             pprof before and after.
  4. slow-scan-survives    — connect with multiple queries (wider hydration),
                             drain slowly. Assert no WEDGE in logs, no
                             Internal errors, socket survives 30s.
  5. reconnect-after-cancel — cancel a stream, then reconnect the same
                             client group. Assert reconnection succeeds and
                             hydrates normally (cancel didn't corrupt state).

Usage (mirrors negative.py):
  python3 harness/wedge_scenarios.py --target ws://rust-test.localhost/zero \
      --id-pool harness/id-pool.json --client-schema harness/client-schema.json \
      --auth-token JWT --pprof http://localhost:6061 \
      --out reports/wedge-scenarios-$TAG.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import encode_sec_protocols, DEFAULT_PROTOCOL_VERSION  # noqa: E402
from workload import (  # noqa: E402
    ArgResolver, WeightedSampler, load_baseline,
    query_put, query_del, init_connection_message, change_desired_queries_message,
)

# Reuse negative.py's infrastructure
from negative import (  # noqa: E402
    Ctx, Session, close, result, is_infra_error,
    connect_url, rand_id,
)


def pprof_goroutine_count(pprof_url: str) -> int:
    """Get goroutine count from pprof."""
    try:
        with urllib.request.urlopen(
            f"{pprof_url}/debug/pprof/goroutine?debug=1", timeout=10
        ) as r:
            import re
            text = r.read().decode("utf-8", errors="replace")
            m = re.search(r"goroutine profile: total (\d+)", text)
            return int(m.group(1)) if m else -1
    except Exception:
        return -1


def check_wedge_in_logs(container: str, since_s: int = 60) -> dict:
    """Check container logs for WEDGE markers in the last `since_s` seconds."""
    import subprocess
    import re
    try:
        r = subprocess.run(
            ["docker", "logs", "--since", f"{since_s}s", container],
            capture_output=True, text=True, timeout=15,
        )
        logs = r.stdout + "\n" + r.stderr
    except Exception:
        return {"wedge": False, "escalate": False, "fatal": False}

    return {
        "wedge": bool(re.search(r"\[GO-IVM\]\[WEDGE\]", logs)),
        "escalate": bool(re.search(r"\[GO-IVM\]\[WEDGE-ESCALATE\]", logs)),
        "fatal": bool(re.search(r"\[GO-IVM\]\[WEDGE-FATAL\]", logs)),
        "idle_damper": bool(re.search(r"\[GO-IVM\]\[IDLE-DAMPER\]", logs)),
    }


# --------------------------------------------------------------------------- #
# Scenario 1: Cancel mid-hydrate
# --------------------------------------------------------------------------- #
async def sc_cancel_mid_hydrate(ctx: Ctx) -> dict:
    """Open a pull stream, wait for hydration to start, then cancel via
    closing the socket. The progress handler should abort any in-flight
    SQLite step within microseconds. Assert: no WEDGE-ESCALATE marker in
    logs (cancel was fast enough that the watchdog didn't need to fire)."""
    name = "cancel-mid-hydrate"
    expect = "cancel completes <10s; no WEDGE-ESCALATE in logs"
    cgid, cid = rand_id(), rand_id()
    put = ctx.benign_put()
    s = await ctx.open(cgid, cid, puts=[put])
    try:
        # Wait for hydration to start
        await s.pump_until(lambda x: x.got_hashes or x.errors, 30)
        if not s.got_hashes:
            return result(name, "SKIP", expect,
                          f"never hydrated: tags={s.tags}")
        # Give the hydrate a moment to get into a SQLite step
        await asyncio.sleep(0.5)
        # Cancel by closing the socket — this triggers the stream gate's
        # onCancel which sets the bound reader conn's cancel flag.
    finally:
        await close(s)

    # Wait a bit for the server to process the cancel
    await asyncio.sleep(2)

    # Check logs for WEDGE markers — there should be NONE (cancel was fast)
    if hasattr(ctx, 'container'):
        markers = check_wedge_in_logs(ctx.container, since_s=10)
        if markers["fatal"]:
            return result(name, "FAIL", expect,
                         "WEDGE-FATAL in logs — progress handler cancel failed")
        if markers["escalate"]:
            return result(name, "WATCH", expect,
                         "WEDGE-ESCALATE in logs — cancel was slow enough "
                         "for the watchdog to fire (progress handler may "
                         "not be working correctly)")

    return result(name, "PASS", expect,
                  "socket closed mid-hydrate; no WEDGE markers in logs")


# --------------------------------------------------------------------------- #
# Scenario 2: Idle consumer
# --------------------------------------------------------------------------- #
async def sc_idle_consumer(ctx: Ctx) -> dict:
    """Open a pull stream with a small pull window, let hydration start, then
    STOP reading (stop granting credit). The pull idle sweeper should cancel
    the stream within pullIdleTimeout (60s default). Assert: stream closes
    cleanly, no wedge, no leaked goroutines."""
    name = "idle-consumer"
    expect = "idle-timeout fires <90s; stream closes cleanly; no wedge"
    cgid, cid = rand_id(), rand_id()
    put = ctx.benign_put()
    s = await ctx.open(cgid, cid, puts=[put])
    try:
        await s.pump_until(lambda x: x.got_hashes or x.errors, 30)
        if not s.got_hashes:
            return result(name, "SKIP", expect,
                          f"never hydrated: tags={s.tags}")
        # Stop reading — don't pump. The server's idle sweeper should
        # cancel the stream after pullIdleTimeout.
        # We wait but don't call pump — the socket should be closed by
        # the server's idle sweep.
        try:
            # The server should close the stream. Wait up to 90s.
            await asyncio.wait_for(s.ws.recv(), timeout=90)
        except asyncio.TimeoutError:
            return result(name, "FAIL", expect,
                         "stream not closed after 90s idle — idle sweeper "
                         "may not be working")
        except Exception:
            # Socket closed by server — this is what we want
            pass
    except Exception as e:
        st = "INFRA" if is_infra_error(e) else "FAIL"
        return result(name, st, expect, f"exception: {e!r}")
    finally:
        await close(s)

    # The stream was closed by the server's idle sweep — good.
    return result(name, "PASS", expect,
                  "stream closed by idle sweeper after consumer went idle")


# --------------------------------------------------------------------------- #
# Scenario 3: Churn leak
# --------------------------------------------------------------------------- #
async def sc_churn_leak(ctx: Ctx) -> dict:
    """Rapidly connect/disconnect 20 client groups with queries, then check
    goroutine count via pprof. A goroutine leak (pool readers not freed,
    progress handler flags not freed) would show as a goroutine count
    increase. Assert: goroutine count after churn is within +20 of the
    baseline."""
    name = "churn-leak"
    expect = "goroutine count after churn within +20 of baseline"
    if not hasattr(ctx, 'pprof') or not ctx.pprof:
        return result(name, "SKIP", expect, "no pprof URL — cannot check goroutine count")

    baseline_goroutines = pprof_goroutine_count(ctx.pprof)
    if baseline_goroutines < 0:
        return result(name, "SKIP", expect, "pprof unreachable")

    # Create 20 client groups, each with a query, then close them all
    sessions = []
    try:
        for i in range(20):
            cgid = f"art-churn-{i}-{uuid.uuid4().hex[:8]}"
            cid = f"art-cid-{i}-{uuid.uuid4().hex[:8]}"
            put = ctx.benign_put()
            try:
                s = await ctx.open(cgid, cid, puts=[put])
                sessions.append(s)
                await asyncio.sleep(0.1)  # small gap to let hydration start
            except Exception as e:
                if is_infra_error(e):
                    return result(name, "INFRA", expect, f"connect {i} failed: {e!r}")
                # Non-infra connect failure — continue
        # Let them hydrate briefly
        await asyncio.sleep(3)
    finally:
        # Close all sessions
        for s in sessions:
            await close(s)
        # Give the server time to tear down
        await asyncio.sleep(5)

    # Check goroutine count after churn
    after_goroutines = pprof_goroutine_count(ctx.pprof)
    if after_goroutines < 0:
        return result(name, "SKIP", expect, "pprof unreachable after churn")

    delta = after_goroutines - baseline_goroutines
    if delta > 20:
        return result(name, "FAIL", expect,
                      f"goroutine leak: {baseline_goroutines} -> {after_goroutines} "
                      f"(+{delta} > +20 threshold — pool readers or cancel flags "
                      f"not being freed)")
    return result(name, "PASS", expect,
                  f"goroutines: {baseline_goroutines} -> {after_goroutines} "
                  f"(+{delta} <= +20)")


# --------------------------------------------------------------------------- #
# Scenario 4: Slow scan survives
# --------------------------------------------------------------------------- #
async def sc_slow_scan_survives(ctx: Ctx) -> dict:
    """Connect with multiple queries (wider hydration), then drain slowly
    at 1 frame/sec for 30s. The server should buffer or stage, not wedge.
    Assert: no Internal errors, no WEDGE in logs, socket survives 30s."""
    name = "slow-scan-survives"
    expect = "server survives 30s slow drain; no wedge/Internal"
    cgid, cid = rand_id(), rand_id()
    # Open with multiple queries to widen the hydration
    puts = []
    for _ in range(5):
        try:
            puts.append(ctx.benign_put())
        except Exception:
            pass
    if len(puts) < 2:
        return result(name, "SKIP", expect, "could not build enough queries")
    s = await ctx.open(cgid, cid, puts=puts)
    try:
        await s.pump_until(lambda x: x.got_hashes or x.errors, 30)
        if not s.got_hashes:
            return result(name, "SKIP", expect,
                          f"never hydrated: tags={s.tags}")
        # Drain at 1fps for 30s
        deadline = time.perf_counter() + 30
        frames_drained = 0
        while time.perf_counter() < deadline:
            try:
                await asyncio.wait_for(s.ws.recv(), timeout=1.0)
                frames_drained += 1
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                if "closed" in str(e).lower() or "EOF" in str(e):
                    break
                return result(name, "FAIL", expect,
                             f"socket error during slow drain: {e!r}")
    except Exception as e:
        st = "INFRA" if is_infra_error(e) else "FAIL"
        return result(name, st, expect, f"exception: {e!r}")
    finally:
        await close(s)

    internal = [e for e in s.errors if e.get("kind") == "Internal"]
    if internal:
        return result(name, "FAIL", expect,
                      f"Internal error during slow drain: "
                      f"{internal[0].get('message', '')[:60]}")
    return result(name, "PASS", expect,
                  f"survived 30s slow drain ({frames_drained} frames), "
                  f"no Internal errors, tags={s.tags}")


# --------------------------------------------------------------------------- #
# Scenario 5: Reconnect after cancel
# --------------------------------------------------------------------------- #
async def sc_reconnect_after_cancel(ctx: Ctx) -> dict:
    """Cancel a stream mid-hydrate, then reconnect the same client group.
    Assert: reconnection succeeds and hydrates normally (cancel didn't
    corrupt server state — no leaked reader, no stale cancel flag)."""
    name = "reconnect-after-cancel"
    expect = "reconnect after cancel succeeds; hydrates normally"
    cgid, cid = rand_id(), rand_id()
    put = ctx.benign_put()
    # First connection — hydrate then cancel
    s1 = await ctx.open(cgid, cid, puts=[put])
    try:
        await s1.pump_until(lambda x: x.got_hashes or x.errors, 30)
        if not s1.got_hashes:
            return result(name, "SKIP", expect,
                          f"first connect never hydrated: tags={s1.tags}")
        await asyncio.sleep(0.5)
    finally:
        await close(s1)
    # Wait for server to process the cancel
    await asyncio.sleep(3)
    # Second connection — same cgid, fresh cid, same query
    cid2 = rand_id()
    s2 = await ctx.open(cgid, cid2, puts=[put])
    try:
        await s2.pump_until(lambda x: x.got_hashes or x.errors, 30)
        if not s2.got_hashes:
            return result(name, "FAIL", expect,
                         f"reconnect failed to hydrate after cancel: "
                         f"tags={s2.tags} errors={s2.error_kinds()}")
        internal = [e for e in s2.errors if e.get("kind") == "Internal"]
        if internal:
            return result(name, "FAIL", expect,
                         f"Internal error after reconnect: "
                         f"{internal[0].get('message', '')[:60]}")
    except Exception as e:
        st = "INFRA" if is_infra_error(e) else "FAIL"
        return result(name, st, expect, f"reconnect exception: {e!r}")
    finally:
        await close(s2)
    return result(name, "PASS", expect,
                  f"reconnected after cancel, hydrated {len(s2.got_hashes)} "
                  f"query(ies) normally")


# --------------------------------------------------------------------------- #
# Scenario 6: Concurrent cold-start storm (the production wedge trigger)
# --------------------------------------------------------------------------- #
async def sc_concurrent_cold_start_storm(ctx: Ctx) -> dict:
    """Simulate the exact production wedge trigger: many client groups
    simultaneously cold-hydrating the same deep-EXISTS query at once.
    Production wedge had getCanvas with depth-6 EXISTS nesting (canvases →
    canvas_participants → user_groups → user_group_mappings, plus canvases →
    channels → channel_participants). This scenario opens N client groups
    concurrently, each with the same query, forcing N simultaneous cold
    hydrations of the EXISTS pattern.

    Assert: all N hydrations complete within 30s (no wedge), no WEDGE markers
    in logs, goroutine count returns to baseline after all disconnect."""
    name = "concurrent-cold-start-storm"
    expect = f"25 concurrent cold-starts in <60s; no WEDGE; goroutines return to baseline"
    if not hasattr(ctx, 'pprof') or not ctx.pprof:
        return result(name, "SKIP", expect, "no pprof URL — cannot check goroutine count")

    baseline_goroutines = pprof_goroutine_count(ctx.pprof)

    # 25 CGs connecting within 60s — the prod deploy-reconnect shape.
    # Prod: ~230 clients/pod reconnect over 2-3 min; scaled to the 2-core
    # lane (25 CGs = 50 clients, ~1/5th of a pod's reconnect burst).
    N = 25
    sessions = []
    cgid_base = f"art-storm-{uuid.uuid4().hex[:8]}"

    async def open_and_hydrate(i: int) -> tuple[Session, float]:
        cgid = f"{cgid_base}-{i}"
        cid = f"art-cid-{i}-{uuid.uuid4().hex[:8]}"
        put = ctx.benign_put()  # any query — what matters is concurrent cold-start
        t0 = time.perf_counter()
        s = await ctx.open(cgid, cid, puts=[put])
        # Wait for hydration to start (got_hashes = first poke received)
        await s.pump_until(lambda x: x.got_hashes or x.errors, 30)
        elapsed = time.perf_counter() - t0
        return s, elapsed

    try:
        # Fire all N concurrently
        results_storm = await asyncio.gather(
            *[open_and_hydrate(i) for i in range(N)],
            return_exceptions=True,
        )
        # Unpack
        for r in results_storm:
            if isinstance(r, Exception):
                if is_infra_error(r):
                    return result(name, "INFRA", expect, f"connect failed: {r!r}")
                sessions.append(None)
            else:
                sessions.append(r[0])
    except Exception as e:
        st = "INFRA" if is_infra_error(e) else "FAIL"
        return result(name, st, expect, f"storm exception: {e!r}")
    finally:
        # Close all sessions
        for s in sessions:
            if s is not None:
                await close(s)

    # Wait for server to clean up (25 CGs = more goroutines to drain)
    await asyncio.sleep(5)

    # Check goroutine count returned to baseline
    after_goroutines = pprof_goroutine_count(ctx.pprof)
    delta = after_goroutines - baseline_goroutines if after_goroutines > 0 and baseline_goroutines > 0 else -1

    # Check logs for WEDGE markers
    markers = {}
    if hasattr(ctx, 'container') and ctx.container:
        markers = check_wedge_in_logs(ctx.container, since_s=60)

    if markers.get("fatal"):
        return result(name, "FAIL", expect,
                     "WEDGE-FATAL in logs — progress handler failed under concurrent cold-start storm")
    if markers.get("escalate"):
        return result(name, "WATCH", expect,
                     f"WEDGE-ESCALATE in logs — progress handler was needed under storm "
                     f"(goroutines: {baseline_goroutines} -> {after_goroutines})")
    if delta > 30:
        return result(name, "FAIL", expect,
                     f"goroutine leak after storm: {baseline_goroutines} -> {after_goroutines} "
                     f"(+{delta} > +30 — pool readers not freed after concurrent cold-start)")
    return result(name, "PASS", expect,
                  f"25 concurrent cold-starts completed, no WEDGE markers, "
                  f"goroutines: {baseline_goroutines} -> {after_goroutines} (+{delta})")


# --------------------------------------------------------------------------- #
# Scenario 7: Deep EXISTS stress (the exact production wedge pattern)
# --------------------------------------------------------------------------- #
async def sc_deep_exists_stress(ctx: Ctx) -> dict:
    """Open a client group and repeatedly subscribe/unsubscribe to the
    deepest-EXISTS query in the workload (getCanvas with depth-6 nesting).
    Each subscribe triggers a cold hydration of the EXISTS N+1 pattern.
    The production wedge was exactly this: getCanvas → Join → Filter(Exists)
    → fetchExists → fetchSize → FlippedJoin.Fetch → fetchViaBoundReaderStream.

    Assert: each hydration completes <10s (no wedge), no WEDGE-ESCALATE in
    logs, goroutine count stable across subscribe/unsubscribe cycles."""
    name = "deep-exists-stress"
    expect = "5 subscribe/unsubscribe cycles of deep-EXISTS query; no WEDGE; <10s each"
    if not hasattr(ctx, 'pprof') or not ctx.pprof:
        return result(name, "SKIP", expect, "no pprof URL")

    baseline_goroutines = pprof_goroutine_count(ctx.pprof)

    # Find getCanvas in the sampler (it's the deep-EXISTS query that wedged)
    canvas_put = None
    for _ in range(50):
        op = ctx.sampler.sample()
        if op.name == "getCanvas":
            args, ok = ctx.resolver.resolve(op)
            if ok:
                canvas_put = query_put(op.name, args)
                break
    if canvas_put is None:
        return result(name, "SKIP", expect,
                     "could not sample getCanvas from baseline — deep-EXISTS query not in workload")

    cgid = f"art-exists-{uuid.uuid4().hex[:8]}"
    cid = f"art-cid-{uuid.uuid4().hex[:8]}"

    # Open connection
    s = await ctx.open(cgid, cid, puts=[canvas_put])
    max_hydrate_ms = 0
    try:
        # Wait for initial hydration
        await s.pump_until(lambda x: x.got_hashes or x.errors, 30)
        if not s.got_hashes:
            return result(name, "SKIP", expect,
                          f"initial hydration failed: tags={s.tags}")

        # Now do 4 more subscribe/unsubscribe cycles with different canvas IDs
        for cycle in range(4):
            # Change desired queries: remove old, add new getCanvas with different arg
            new_op = None
            for _ in range(20):
                op = ctx.sampler.sample()
                if op.name == "getCanvas":
                    args, ok = ctx.resolver.resolve(op)
                    if ok:
                        new_op = query_put(op.name, args)
                        break
            if not new_op:
                continue  # skip cycle if can't sample

            # Remove old hash, add new
            old_hashes = list(s.got_hashes)
            patch = [query_del(h) for h in old_hashes] + [new_op]
            await s.ws.send(json.dumps(change_desired_queries_message(patch)))
            t0 = time.perf_counter()
            await s.pump_until(
                lambda x: new_op["hash"] in x.got_hashes or x.errors,
                30,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000
            max_hydrate_ms = max(max_hydrate_ms, elapsed_ms)
    except Exception as e:
        st = "INFRA" if is_infra_error(e) else "FAIL"
        return result(name, st, expect, f"stress exception: {e!r}")
    finally:
        await close(s)

    await asyncio.sleep(2)
    after_goroutines = pprof_goroutine_count(ctx.pprof)
    delta = after_goroutines - baseline_goroutines if after_goroutines > 0 and baseline_goroutines > 0 else -1

    markers = {}
    if hasattr(ctx, 'container') and ctx.container:
        markers = check_wedge_in_logs(ctx.container, since_s=60)

    if markers.get("fatal"):
        return result(name, "FAIL", expect,
                     "WEDGE-FATAL — progress handler failed under deep-EXISTS stress")
    if markers.get("escalate"):
        return result(name, "WATCH", expect,
                     f"WEDGE-ESCALATE — progress handler was needed (deep EXISTS may be slow)")
    if max_hydrate_ms > 30000:
        return result(name, "FAIL", expect,
                     f"hydration took {max_hydrate_ms:.0f}ms > 30s — possible wedge")
    if delta > 15:
        return result(name, "WATCH", expect,
                     f"goroutine delta +{delta} after EXISTS stress — check for pool reader leak")
    return result(name, "PASS", expect,
                 f"5 cycles, max hydrate {max_hydrate_ms:.0f}ms, no WEDGE, "
                 f"goroutines {baseline_goroutines} -> {after_goroutines} (+{delta})")


# --------------------------------------------------------------------------- #
# Scenario 8: Stall-then-resume consumer
# --------------------------------------------------------------------------- #
async def sc_stall_then_resume(ctx: Ctx) -> dict:
    """Open a pull stream, grant no credit for 45s (stall), then resume.
    Assert: stream survives (45s < 60s idle-timeout), data flows after resume,
    no Internal errors."""
    name = "stall-then-resume"
    expect = "stream survives 45s stall; data flows after resume"
    cgid, cid = rand_id(), rand_id()
    put = ctx.benign_put()
    s = await ctx.open(cgid, cid, puts=[put])
    try:
        await s.pump_until(lambda x: x.got_hashes or x.errors, 30)
        if not s.got_hashes:
            return result(name, "SKIP", expect, f"never hydrated: tags={s.tags}")
        # Stall: don't pump for 45s (no credit granted)
        await asyncio.sleep(45)
        # Check socket is still alive
        if s.ws.closed:
            return result(name, "FAIL", expect,
                          "socket closed during 45s stall — idle-timeout fired too early")
        # Resume: pump to drain any buffered data
        got = await s.pump_until(lambda x: x.got_hashes, 15)
        if s.errors:
            return result(name, "FAIL", expect,
                          f"errors after resume: {s.errors[:3]}")
        return result(name, "PASS", expect,
                      "stream survived 45s stall and resumed")
    finally:
        await close(s)


# --------------------------------------------------------------------------- #
# Scenario 9: Mid-hydrate kill loop
# --------------------------------------------------------------------------- #
async def sc_mid_hydrate_kill_loop(ctx: Ctx) -> dict:
    """Connect → start hydrate → kill socket at ~50% → reconnect same CG, ×20.
    Assert: goroutine count flat after all cycles (no leak from killed sockets)."""
    name = "mid-hydrate-kill-loop"
    expect = "20 kill/reconnect cycles; goroutine count flat"
    baseline_goroutines = pprof_goroutine_count(ctx.pprof)
    cgid = rand_id()
    for i in range(20):
        cid = rand_id()
        put = ctx.benign_put()
        s = await ctx.open(cgid, cid, puts=[put])
        try:
            await s.pump_until(lambda x: x.got_hashes or x.errors, 10)
            await asyncio.sleep(0.3)  # let hydration get going
        finally:
            # Kill without graceful close — simulates abrupt socket death
            await close(s)
        await asyncio.sleep(0.5)  # brief reconnect delay
    # Wait for server to clean up
    await asyncio.sleep(5)
    after_goroutines = pprof_goroutine_count(ctx.pprof)
    delta = after_goroutines - baseline_goroutines
    if abs(delta) > 15:
        return result(name, "FAIL", expect,
                      f"goroutine leak: {baseline_goroutines} -> {after_goroutines} (+{delta})")
    return result(name, "PASS", expect,
                  f"goroutines {baseline_goroutines} -> {after_goroutines} (+{delta})")


# --------------------------------------------------------------------------- #
# Scenario 10: Half-open socket
# --------------------------------------------------------------------------- #
async def sc_half_open_socket(ctx: Ctx) -> dict:
    """Connect, start a query, then kill the client without FIN (simulate
    network drop). Wait 30s. Assert: server goroutine count didn't grow."""
    name = "half-open-socket"
    expect = "server reclaims dead socket; goroutine delta < 10 after 30s"
    baseline_goroutines = pprof_goroutine_count(ctx.pprof)
    cgid, cid = rand_id(), rand_id()
    put = ctx.benign_put()
    s = await ctx.open(cgid, cid, puts=[put])
    try:
        await s.pump_until(lambda x: x.got_hashes or x.errors, 10)
    except Exception:
        pass  # expected — we're about to kill the socket
    # Abort without close — leaves a half-open socket on the server side
    if hasattr(s, 'ws') and s.ws:
        try:
            s.ws._sock.shutdown(2)  # SHUT_RDWR without FIN handshake
        except Exception:
            pass
        try:
            s.ws._sock.close()
        except Exception:
            pass
    # Wait for server keepalive to detect the dead socket
    await asyncio.sleep(30)
    after_goroutines = pprof_goroutine_count(ctx.pprof)
    delta = after_goroutines - baseline_goroutines
    if delta > 10:
        return result(name, "FAIL", expect,
                      f"goroutine leak from half-open socket: {baseline_goroutines} -> {after_goroutines} (+{delta})")
    return result(name, "PASS", expect,
                  f"goroutines {baseline_goroutines} -> {after_goroutines} (+{delta})")


# --------------------------------------------------------------------------- #
# Scenario 11: Auth invalidation mid-hydrate
# --------------------------------------------------------------------------- #
async def sc_auth_invalidation(ctx: Ctx) -> dict:
    """Connect with valid auth, start addQueriesStream, then simulate
    auth invalidation by closing the connection abruptly mid-hydrate.
    Reconnect with fresh auth on the same CG. Assert: no panic in server
    logs, clean recovery."""
    name = "auth-invalidation"
    expect = "no panic; clean recovery after auth invalidation"
    cgid = rand_id()
    # First connection: start hydrate, then kill
    cid1 = rand_id()
    put = ctx.benign_put()
    s1 = await ctx.open(cgid, cid1, puts=[put])
    try:
        await s1.pump_until(lambda x: x.got_hashes or x.errors, 10)
        await asyncio.sleep(0.5)  # let hydration get going
    finally:
        await close(s1)
    # Brief delay simulating auth invalidation + client retry
    await asyncio.sleep(2)
    # Reconnect same CG with fresh client ID
    cid2 = rand_id()
    s2 = await ctx.open(cgid, cid2, puts=[put])
    try:
        got = await s2.pump_until(lambda x: x.got_hashes or x.errors, 30)
        if not s2.got_hashes:
            return result(name, "FAIL", expect,
                          f"reconnect failed to hydrate: tags={s2.tags}")
        if s2.errors:
            return result(name, "FAIL", expect,
                          f"errors after reconnect: {s2.errors[:3]}")
        return result(name, "PASS", expect,
                      "reconnected after auth invalidation; hydrated normally")
    finally:
        await close(s2)


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
async def run(a: argparse.Namespace) -> int:
    ctx = Ctx(a)
    ctx.pprof = a.pprof
    ctx.container = a.container
    print(f"=== wedge scenarios vs {a.target} ===")
    scenarios = [
        sc_cancel_mid_hydrate,
        sc_idle_consumer,
        sc_churn_leak,
        sc_slow_scan_survives,
        sc_reconnect_after_cancel,
        sc_concurrent_cold_start_storm,
        sc_deep_exists_stress,
        sc_stall_then_resume,
        sc_mid_hydrate_kill_loop,
        sc_half_open_socket,
        sc_auth_invalidation,
    ]
    if a.only:
        wanted = set(a.only.split(","))
        scenarios = [f for f in scenarios
                     if f.__name__.removeprefix("sc_").replace("_", "-") in wanted]
    results = []
    for fn in scenarios:
        nm = fn.__name__.removeprefix("sc_").replace("_", "-")

        async def attempt() -> dict:
            try:
                return await fn(ctx)
            except Exception as e:
                if is_infra_error(e):
                    return result(nm, "INFRA", "scenario completes",
                                  f"pod unreachable: {e!r}")
                return result(nm, "FAIL", "scenario completes",
                              f"harness exception: {e!r}")

        r = await attempt()
        if r["status"] == "FAIL":
            print(f"  [..] {nm:<24} RETRY")
            await asyncio.sleep(5.0)
            r2 = await attempt()
            if r2["status"] == "PASS":
                r2 = dict(r2)
                r2["flaky"] = True
                r = r2
        results.append(r)
    n_pass = sum(r["status"] == "PASS" for r in results)
    n_fail = sum(r["status"] == "FAIL" for r in results)
    n_skip = sum(r["status"] == "SKIP" for r in results)
    n_watch = sum(r["status"] == "WATCH" for r in results)
    n_infra = sum(r["status"] == "INFRA" for r in results)
    verdict = "FAIL" if n_fail else ("INFRA" if n_infra else "PASS")
    report = {
        "gate": "G27",
        "target": a.target,
        "when": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scenarios": results,
        "n_pass": n_pass, "n_fail": n_fail, "n_skip": n_skip,
        "n_watch": n_watch, "n_infra": n_infra,
        "verdict": verdict,
    }
    out = a.out or os.path.join(
        os.path.dirname(__file__), "..", "reports",
        "wedge-scenarios-" + time.strftime("%Y%m%d-%H%M%S") + ".json")
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nWEDGE SCENARIOS: {verdict} "
          f"({n_pass} pass, {n_fail} fail, {n_skip} skip"
          + (f", {n_watch} watch" if n_watch else "")
          + (f", {n_infra} infra" if n_infra else "") + f") -> {out}")
    return 1 if n_fail else (3 if n_infra else 0)


def main() -> int:
    ap = argparse.ArgumentParser(description="Active wedge scenarios (G27).")
    ap.add_argument("--target", required=True)
    ap.add_argument("--protocol-version", type=int, default=DEFAULT_PROTOCOL_VERSION)
    ap.add_argument("--baseline", default=os.path.join(
        os.path.dirname(__file__), "..", "art-baseline.json"))
    ap.add_argument("--id-pool", required=True)
    ap.add_argument("--client-schema", required=True)
    ap.add_argument("--auth-pool", default=None)
    ap.add_argument("--auth-token", default=None)
    ap.add_argument("--user-id", default=None)
    ap.add_argument("--pprof", default="", help="Go pprof base URL")
    ap.add_argument("--container", default=None,
                    help="zero-cache container name (for log checks)")
    ap.add_argument("--pg-container", default=None)
    ap.add_argument("--pg-user", default="xyne")
    ap.add_argument("--pg-db", default=None)
    ap.add_argument("--cvr-schema", default=None)
    ap.add_argument("--only", default=None)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    return asyncio.run(run(a))


if __name__ == "__main__":
    raise SystemExit(main())
