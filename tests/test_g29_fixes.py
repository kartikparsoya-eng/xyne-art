"""Regression tests for the two G29 fixes (reports/g29-shape-analysis.md).

Fix 1 — coverage sweep (harness/replay.py run_sweep_client): a dedicated extra
client round-robins the full catalog so every resolvable shape is desired at
least once per rotation, within the 100-desired-queries-per-client server cap.

Fix 2 — optional-unknown args (harness/workload.py ArgResolver): an arg key
unknown to the id-pool/scalars no longer fails the shape when the CURRENT
source schema (raw/arg-schemas.source.json) marks it optional (or no longer
knows it); required-unknown keys keep failing exactly as before.

These run fully in-process (no network, no live zero-cache).
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from workload import ArgResolver, Op, load_baseline

REPO = Path(__file__).resolve().parent.parent
BASELINE_PATH = REPO / "art-baseline.json"
SCHEMAS_PATH = REPO / "raw" / "arg-schemas.source.json"
ID_POOL_PATH = REPO / "harness" / "id-pool.sandbox.json"
CLIENT_SCHEMA_PATH = REPO / "harness" / "client-schema.json"

# The 5 resolver-suppression victims from the G29 analysis (section b).
SUPPRESSED_SHAPES = [
    "kanbanTicketsPage",
    "hierarchyCanvases",
    "userCanvasesPaginated",
    "channelCanvasesPaginated",
    "supportTicketsFilteredV3",
]

needs_fixtures = pytest.mark.skipif(
    not (BASELINE_PATH.exists() and SCHEMAS_PATH.exists() and ID_POOL_PATH.exists()),
    reason="needs art-baseline.json + raw/arg-schemas.source.json + id-pool.sandbox.json",
)


def _catalog_and_resolver(seed: int = 1):
    """The exact setup replay.py main() performs: stale-filter the baseline
    against the current source catalog, build the schema-aware resolver."""
    schemas = json.load(open(SCHEMAS_PATH)).get("queries") or {}
    bl = load_baseline(str(BASELINE_PATH))
    bl.queries = [op for op in bl.queries if op.name in schemas]
    resolver = ArgResolver.from_pool_file(
        str(ID_POOL_PATH), random.Random(seed), query_schemas=schemas)
    return bl.queries, resolver


# --------------------------------------------------------------------------- #
# Fix 2: optional-unknown arg keys are omitted, not shape-fatal
# --------------------------------------------------------------------------- #
@needs_fixtures
def test_all_five_suppressed_shapes_now_resolve():
    catalog, resolver = _catalog_and_resolver()
    ops = {op.name: op for op in catalog}
    # sanity: client-schema fixture is loadable alongside (replay.py sends it
    # in initConnection for new client groups)
    if CLIENT_SCHEMA_PATH.exists():
        assert json.load(open(CLIENT_SCHEMA_PATH)).get("tables")
    for name in SUPPRESSED_SHAPES:
        assert name in ops, f"{name} missing from the stale-filtered catalog"
        args, ok = resolver.resolve(ops[name])
        assert ok, f"{name} still unresolvable: unresolved={resolver.unresolved}"
    # the omitted keys are the exact suppression keys from the analysis
    assert set(resolver.optional_omitted) >= {
        "includeQuartoDocs", "filters", "formEntityValueFieldIds", "assignedTo"}
    # ...and none of them leaked into any args object
    for name in SUPPRESSED_SHAPES:
        args, _ = resolver.resolve(ops[name])
        for k in ("includeQuartoDocs", "filters", "dynamicFieldDateRanges",
                  "formEntityValueFieldIds", "assignedTo"):
            assert k not in args, f"{name} sent unresolvable key {k}"


@needs_fixtures
def test_full_catalog_is_resolvable():
    """Post-fix, every in-catalog shape resolves — the precondition for the
    coverage sweep to reach 100% shape coverage in one rotation."""
    catalog, resolver = _catalog_and_resolver()
    unresolvable = [op.name for op in catalog if not resolver.resolve(op)[1]]
    assert unresolvable == [], f"unresolvable shapes: {unresolvable}"


def test_required_unknown_key_still_fails():
    """A REQUIRED key unknown to the pool must keep failing (real signal)."""
    resolver = ArgResolver(
        ids={}, scalars={}, rng=random.Random(0),
        arg_schemas={"q": {"mysteryKey": {"type": "string"}}})
    op = Op(name="q", weight=1.0, args_keys=["mysteryKey"], calls=0)
    args, ok = resolver.resolve(op)
    assert not ok
    assert "mysteryKey" not in args
    assert resolver.unresolved.get("mysteryKey") == 1


def test_optional_unknown_key_is_omitted_ok():
    resolver = ArgResolver(
        ids={}, scalars={}, rng=random.Random(0),
        arg_schemas={"q": {"mysteryKey": {"type": "string", "optional": True}}})
    op = Op(name="q", weight=1.0, args_keys=["mysteryKey"], calls=0)
    args, ok = resolver.resolve(op)
    assert ok
    assert args == {}
    assert resolver.unresolved == {}
    assert resolver.optional_omitted.get("mysteryKey") == 1


def test_schema_absent_key_is_omitted_ok():
    """A baseline key the current schema no longer knows (e.g.
    kanbanTicketsPage.dynamicFieldDateRanges) cannot be required by it."""
    resolver = ArgResolver(
        ids={}, scalars={}, rng=random.Random(0),
        arg_schemas={"q": {"other": {"type": "string", "optional": True}}})
    op = Op(name="q", weight=1.0, args_keys=["goneKey"], calls=0)
    args, ok = resolver.resolve(op)
    assert ok and args == {}


def test_no_schema_for_query_keeps_old_behavior():
    """Without a current-schema entry there is no optionality evidence: an
    unknown key fails exactly as before the fix."""
    resolver = ArgResolver(ids={}, scalars={}, rng=random.Random(0))
    op = Op(name="q", weight=1.0, args_keys=["mysteryKey"], calls=0)
    _, ok = resolver.resolve(op)
    assert not ok


# --------------------------------------------------------------------------- #
# Fix 1: coverage-sweep scheduler
# --------------------------------------------------------------------------- #
@needs_fixtures
def test_sweep_one_rotation_covers_every_catalog_shape():
    """Simulate exactly one rotation of run_sweep_client's sweeper() loop
    (same round-robin + resolve + rolling-window logic, no network) and assert
    every catalog shape is put and the desired-set never exceeds the window."""
    from replay import SWEEP_WINDOW
    from workload import query_put

    catalog, resolver = _catalog_and_resolver()
    n = len(catalog)
    assert n > 0

    window: list[str] = []
    put_names: set[str] = set()
    max_in_flight = 0
    for pos in range(n):  # one full rotation
        op = catalog[pos % n]
        args, ok = resolver.resolve(op)
        if not ok:
            continue
        put = query_put(op.name, args)
        window.append(put["hash"])
        put_names.add(op.name)
        while len(window) > SWEEP_WINDOW:
            window.pop(0)
        max_in_flight = max(max_in_flight, len(window))

    assert put_names == {op.name for op in catalog}, (
        "one rotation did not cover the catalog; missing: "
        f"{sorted({op.name for op in catalog} - put_names)}")
    # server caps desired queries at 100 per client
    assert max_in_flight <= SWEEP_WINDOW < 100


@needs_fixtures
def test_sweep_interval_fits_one_rotation_in_run_window():
    from replay import Config, sweep_interval_s

    catalog, _ = _catalog_and_resolver()
    cfg = Config(target="ws://x", path_prefix="", protocol_version=1,
                 connections=50, working_set=15, churn_ms=750, duration_s=210,
                 ttl_ms=300000, auth_token=None, cookie=None, extra_params=[],
                 post_handshake=True, user_query_url=None, seed=1)
    interval = sweep_interval_s(cfg, len(catalog))
    assert 0.15 <= interval <= 2.0
    # one rotation of puts must complete inside the run window
    assert interval * len(catalog) <= cfg.duration_s * 0.6 + 1e-9


def test_sweep_is_on_by_default_and_disableable():
    """--no-coverage-sweep must exist and default to ON."""
    from replay import Config
    import replay
    assert Config.__dataclass_fields__["coverage_sweep"].default is True
    src = open(REPO / "harness" / "replay.py").read()
    assert "--no-coverage-sweep" in src
    assert hasattr(replay, "run_sweep_client")


def test_run_report_tags_sweep_coverage_separately(tmp_path):
    """write_summary must expose sweep-driven coverage apart from organic
    while G29's query_names_hydrated stays the MERGED set."""
    from replay import Config, Stats, write_summary

    cfg = Config(target="ws://x", path_prefix="", protocol_version=1,
                 connections=1, working_set=1, churn_ms=750, duration_s=10,
                 ttl_ms=300000, auth_token=None, cookie=None, extra_params=[],
                 post_handshake=True, user_query_url=None, seed=1)
    stats = Stats()
    stats.per_query = {"organicQ": 3, "bothQ": 1}
    stats.hydrated_queries = {"organicQ", "bothQ"}
    stats.sweep_per_query = {"sweepOnlyQ": 1, "bothQ": 1, "neverQ": 1}
    stats.sweep_hydrated = {"sweepOnlyQ", "bothQ"}
    stats.sweep_puts, stats.sweep_dels, stats.sweep_rotations = 3, 1, 1

    path = write_summary(cfg, stats, "t0", "t1", str(tmp_path))
    doc = json.load(open(path))
    cov = doc["coverage"]
    # G29 input: merged organic+sweep
    assert set(cov["query_names_hydrated"]) == {"organicQ", "bothQ", "sweepOnlyQ"}
    assert cov["never_hydrated"] == ["neverQ"]
    # sweep attribution
    assert cov["sweep"]["enabled"] is True
    assert cov["sweep"]["query_names_hydrated_sweep_only"] == ["sweepOnlyQ"]
    assert set(cov["sweep"]["query_names_hydrated_organic"]) == {"organicQ", "bothQ"}
    assert cov["sweep"]["rotations_completed"] == 1
    # driven-query sets stay tagged apart
    assert doc["queries_driven"] == {"bothQ": 1, "organicQ": 3}
    assert doc["queries_driven_sweep"] == {"bothQ": 1, "neverQ": 1, "sweepOnlyQ": 1}
    assert doc["config"]["coverage_sweep"] is True


def test_stats_merge_carries_sweep_fields():
    from replay import Stats
    a, b = Stats(), Stats()
    b.sweep_puts, b.sweep_rotations = 5, 2
    b.sweep_per_query = {"q": 5}
    b.sweep_hydrated = {"q"}
    b.sweep_unresolvable = {"r"}
    a.merge(b)
    assert a.sweep_puts == 5 and a.sweep_rotations == 2
    assert a.sweep_per_query == {"q": 5}
    assert a.sweep_hydrated == {"q"} and a.sweep_unresolvable == {"r"}
