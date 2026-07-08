# Go-IVM per-CG init wedge — root cause (2026-07-08)

Self-captured by the sidecar's wedge watchdog during ART soak
(`./run-art-local.sh --soak --duration 1200 --clean`, onset ~min 9-12).
Full goroutine dumps live in `reports/wedge-dumps/` (gitignored; regenerate
by re-running the soak on a pre-v4 build).

## Blocking chain (incident 1, cg=art-i26192j8ja, addQueriesStream)

```
goroutine 1071548 [syscall, 1+ min]          <- THE ROOT HOLDER
  main._Cfunc_goivm_call_deliver(...)         <- cgo into Node, NEVER RETURNS
  main.goivm_start.func1        napi_lib.go:162
  main.(*rowPlane).emitChanges  rowplane.go:141
  main.(*rowPlane).emitHydratePartial  rowplane.go:200   <- holds rp.mu

goroutine 1071412, 1071431 [sync.Mutex.Lock]  <- cascade victims
  emitHydratePartial  rowplane.go:198  (waiting on rp.mu)

goroutine 1070684 [sync.WaitGroup.Wait]       <- outer frame
  engine.addQueriesStreamChunked  engine.go:1154  wg.Wait()
  main.handleAddQueriesStream     main.go:2160
  main.(*ClientGroup).worker      main.go:1534   <- inFlight forever
```

Mechanism: a starved Node event loop (43-46s synchronous TS materializations
observed in the same windows) parks the TSFN deliver; rp.mu stays held; all
producers of that RPC pile onto rp.mu with waiters==0 (invisible to the gate
sweeper); wg.Wait never completes; worker stays inFlight; reaper skips it;
every re-init for that CG queues behind it -> the 120s-lockstep
`RPC init timed out` client signature.

## Resolution (ABI v4)
Bounded TSFN queue (8192) + nonblocking enqueue with cancellable park
(100us->5ms) + 150s deliver timeout. Post-v4 telemetry:
`[GO-IVM][PERF-NAPI] ... stalls=N timeouts=M` per 10s window;
`[GO-IVM][DELIVER-TIMEOUT]` on the unrecoverable path.
G13 (tools/log_gate.py) hard-blocks DELIVER-TIMEOUT/POOL-SERIAL/unresolved
WEDGE, pairs WEDGE with WEDGE-CLEAR, and flags uncorrelated stall bursts.

## v4 verification (build c58306d5, same repro)
0 wedges / 0 init-timeouts / 0 deliver-timeouts — wedge class dead.
Open finding: 19,242 stalls/20min with NO >10s TS materializations
(uncorrelated with the incident-shaped producer) and steady p50/p95
7-8.6x worse than the watchdog build — the park loop taxes ordinary load.
