"""Unit tests for harness/workload.py — the pure, stdlib-only workload model.

workload.py is intentionally dependency-free and side-effect free so it can be
unit-tested without a live zero-cache. These tests pin: sampling determinism
under a seed, argument resolution, and the byte shape of every Zero wire
message the drivers/oracles emit (protocol v49).
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from workload import (
    ArgResolver,
    Baseline,
    DEFAULT_SCALARS,
    ImpactIndex,
    MutationSampler,
    Op,
    PUSH_VERSION,
    WeightedSampler,
    change_desired_queries_message,
    custom_mutation,
    init_connection_message,
    load_baseline,
    push_message,
    query_del,
    query_put,
    stable_draft_id,
    stable_hash,
    stable_participant_id,
)

REPO = Path(__file__).resolve().parent.parent
BASELINE_PATH = REPO / "art-baseline.json"


# --------------------------------------------------------------------------- #
# WeightedSampler
# --------------------------------------------------------------------------- #
def _ops(weights):
    return [Op(name=f"q{i}", weight=w, args_keys=[], calls=0, kind="query") for i, w in enumerate(weights)]


def test_weighted_sampler_deterministic_under_seed():
    ops = _ops([10, 20, 30, 40])
    a = [WeightedSampler(ops, random.Random(7)).sample().name for _ in range(500)]
    b = [WeightedSampler(ops, random.Random(7)).sample().name for _ in range(500)]
    assert a == b


def test_weighted_sampler_respects_weights():
    ops = _ops([1, 99])
    s = WeightedSampler(ops, random.Random(2024))
    counts = {o.name: 0 for o in ops}
    for _ in range(20000):
        counts[s.sample().name] += 1
    # the 99-weight op should dominate (allow tolerance)
    assert counts["q1"] > counts["q0"] * 20


def test_weighted_sampler_covers_all_ops():
    ops = _ops([5, 5, 5, 5])
    s = WeightedSampler(ops, random.Random(3))
    seen = {s.sample().name for _ in range(2000)}
    assert seen == {"q0", "q1", "q2", "q3"}


def test_weighted_sampler_rejects_zero_total():
    with pytest.raises(ValueError):
        WeightedSampler(_ops([0, 0]), random.Random(0))


def test_weighted_sampler_coverage_bound():
    ops = _ops([1, 1, 1])
    s = WeightedSampler(ops, random.Random(0))
    assert s.coverage_after(5) == 3
    assert s.coverage_after(2) == 2


# --------------------------------------------------------------------------- #
# stable_hash + query_put / query_del
# --------------------------------------------------------------------------- #
def test_stable_hash_deterministic_and_distinct():
    h1 = stable_hash("threadConversation", {"channelId": "c1"})
    h2 = stable_hash("threadConversation", {"channelId": "c1"})
    h3 = stable_hash("threadConversation", {"channelId": "c2"})
    assert h1 == h2
    assert h1 != h3
    assert len(h1) == 16  # truncated sha256


def test_stable_hash_arg_order_independent():
    # sort_keys=True means key order doesn't change the hash
    a = stable_hash("q", {"a": 1, "b": 2})
    b = stable_hash("q", {"b": 2, "a": 1})
    assert a == b


def test_query_put_structure():
    put = query_put("subTicketsByMappedTicketId", {"mappedTicketId": "t1"})
    assert put["op"] == "put"
    assert put["name"] == "subTicketsByMappedTicketId"
    assert put["args"] == [{"mappedTicketId": "t1"}]  # wrapped in 1-elem array
    assert put["ttl"] == 300_000
    assert put["hash"] == stable_hash("subTicketsByMappedTicketId", [{"mappedTicketId": "t1"}])


def test_query_put_custom_ttl():
    assert query_put("q", {}, ttl_ms=1000)["ttl"] == 1000


def test_query_del_structure():
    assert query_del("abc123") == {"op": "del", "hash": "abc123"}


# --------------------------------------------------------------------------- #
# init_connection_message + change_desired_queries_message
# --------------------------------------------------------------------------- #
def test_init_connection_minimal():
    msg = init_connection_message([{"op": "put"}])
    assert msg == ["initConnection", {"desiredQueriesPatch": [{"op": "put"}]}]


def test_init_connection_optional_fields():
    msg = init_connection_message(
        [{"op": "put"}],
        user_query_url="https://app/zero",
        client_schema={"tables": {}},
        active_clients=["c1"],
        deleted_client_ids=["c2"],
    )
    body = msg[1]
    assert body["userQueryURL"] == "https://app/zero"
    assert body["clientSchema"] == {"tables": {}}
    assert body["activeClients"] == ["c1"]
    assert body["deleted"] == {"clientIDs": ["c2"]}


def test_init_connection_omits_absent_optionals():
    body = init_connection_message([])[1]
    for k in ("userQueryURL", "userQueryHeaders", "activeClients", "clientSchema", "deleted"):
        assert k not in body


def test_change_desired_queries_message():
    msg = change_desired_queries_message([{"op": "del", "hash": "h"}])
    assert msg == ["changeDesiredQueries", {"desiredQueriesPatch": [{"op": "del", "hash": "h"}]}]


# --------------------------------------------------------------------------- #
# custom_mutation + push_message
# --------------------------------------------------------------------------- #
def test_custom_mutation_structure():
    m = custom_mutation(7, "client-1", "channel.markChannelAsViewed", {"channelId": "c1"}, 1000)
    assert m == {
        "type": "custom",
        "id": 7,
        "clientID": "client-1",
        "name": "channel.markChannelAsViewed",
        "args": [{"channelId": "c1"}],
        "timestamp": 1000,
    }


def test_push_message_structure():
    mut = custom_mutation(1, "c1", "m", {}, 100)
    msg = push_message("cg-1", [mut], request_id="c1-1", now_ms=100)
    assert msg[0] == "push"
    body = msg[1]
    assert body["clientGroupID"] == "cg-1"
    assert body["mutations"] == [mut]
    assert body["pushVersion"] == PUSH_VERSION == 1
    assert body["timestamp"] == 100
    assert body["requestID"] == "c1-1"


def test_push_message_is_json_serializable():
    # the driver json.dumps this onto the wire — must round-trip cleanly
    msg = push_message("cg", [custom_mutation(1, "c", "m", {"a": 1}, 9)], "r1", 9)
    again = json.loads(json.dumps(msg))
    assert again == msg


# --------------------------------------------------------------------------- #
# ArgResolver
# --------------------------------------------------------------------------- #
def test_arg_resolver_resolves_scalar_keys():
    r = ArgResolver(ids={}, scalars={k: list(v) for k, v in DEFAULT_SCALARS.items()},
                    rng=random.Random(0))
    op = Op(name="q", weight=1, args_keys=["limit", "direction"], calls=0)
    args, ok = r.resolve(op)
    assert ok
    assert args["limit"] in DEFAULT_SCALARS["limit"]
    assert args["direction"] in DEFAULT_SCALARS["direction"]


def test_arg_resolver_resolves_id_keys():
    r = ArgResolver(ids={"channelId": ["ch1", "ch2"]}, scalars={}, rng=random.Random(0))
    op = Op(name="q", weight=1, args_keys=["channelId"], calls=0)
    args, ok = r.resolve(op)
    assert ok
    assert args["channelId"] in ("ch1", "ch2")


def test_arg_resolver_omits_unresolved_keys():
    r = ArgResolver(ids={}, scalars={}, rng=random.Random(0))
    op = Op(name="q", weight=1, args_keys=["totallyUnknownKey"], calls=0)
    args, ok = r.resolve(op)
    assert not ok
    assert args == {}  # unresolved keys are omitted, not null
    assert r.unresolved["totallyUnknownKey"] == 1


def test_arg_resolver_partial_resolution_reports_unresolved():
    r = ArgResolver(ids={"channelId": ["c1"]}, scalars={"limit": [25]}, rng=random.Random(0))
    op = Op(name="q", weight=1, args_keys=["channelId", "limit", "missing"], calls=0)
    args, ok = r.resolve(op)
    assert not ok  # one key unresolved
    assert "channelId" in args and "limit" in args
    assert "missing" not in args


def test_arg_resolver_plural_ids_from_singular_pool():
    r = ArgResolver(ids={"channelId": ["c1", "c2", "c3"]}, scalars={}, rng=random.Random(0))
    op = Op(name="q", weight=1, args_keys=["channelIds"], calls=0)
    args, ok = r.resolve(op)
    assert ok
    assert isinstance(args["channelIds"], list)
    assert all(c in ("c1", "c2", "c3") for c in args["channelIds"])


def test_arg_resolver_from_pool_file(tmp_path):
    pool = tmp_path / "pool.json"
    pool.write_text(json.dumps({"ids": {"ticketId": ["t1"]}, "scalars": {"limit": [10]}}))
    r = ArgResolver.from_pool_file(str(pool), random.Random(0))
    assert r.ids == {"ticketId": ["t1"]}
    assert r.scalars["limit"] == [10]
    # defaults still present
    assert "direction" in r.scalars


def test_arg_resolver_from_pool_file_none():
    # no pool file (None) is valid — only defaults, no ids
    r = ArgResolver.from_pool_file(None, random.Random(0))
    assert r.ids == {}
    assert r.scalars  # defaults present


def test_arg_resolver_zipf_skews_to_hot_keys():
    ids = {"channelId": [f"c{i}" for i in range(20)]}  # index 0 = hottest
    r = ArgResolver(ids=ids, scalars={}, rng=random.Random(1), zipf_s=1.1)
    op = Op(name="q", weight=1, args_keys=["channelId"], calls=0)
    counts = {}
    for _ in range(5000):
        a, _ = r.resolve(op)
        counts[a["channelId"]] = counts.get(a["channelId"], 0) + 1
    # the hottest key (index 0) must be sampled more than the coldest (index 19)
    assert counts["c0"] > counts["c19"]


# --------------------------------------------------------------------------- #
# MutationSampler + stable ids
# --------------------------------------------------------------------------- #
def _mutation_baseline():
    return [
        Op(name="channel.markChannelAsViewed", weight=39, args_keys=[], calls=0, kind="mutation"),
        Op(name="activities.markThreadActivitiesAsReadV2", weight=38, args_keys=[], calls=0, kind="mutation"),
        Op(name="someOtherMutation", weight=10, args_keys=[], calls=0, kind="mutation"),
    ]


def test_mutation_sampler_builds_supported_args():
    r = ArgResolver(ids={"channelId": ["c1"], "conversationId": ["v1"]}, scalars={},
                    rng=random.Random(0))
    ms = MutationSampler(_mutation_baseline(), random.Random(0))
    name, args = ms.build(r, now_ms=1234)
    assert name in ("channel.markChannelAsViewed", "activities.markThreadActivitiesAsReadV2")
    assert "timestamp" in args
    assert args["timestamp"] == 1234
    assert args["draftMessageId"].startswith("artdraft")


def test_mutation_sampler_only_samples_supported():
    ms = MutationSampler(_mutation_baseline(), random.Random(0))
    seen = {ms.sample().name for _ in range(500)}
    assert seen == {"channel.markChannelAsViewed", "activities.markThreadActivitiesAsReadV2"}
    assert "someOtherMutation" not in seen


def test_mutation_sampler_rejects_no_supported():
    baseline = [Op(name="unsupported", weight=1, args_keys=[], calls=0, kind="mutation")]
    with pytest.raises(ValueError):
        MutationSampler(baseline, random.Random(0))


def test_stable_draft_id_deterministic_and_prefixed():
    a = stable_draft_id("c1")
    b = stable_draft_id("c1")
    assert a == b
    assert a.startswith("artdraft")
    assert stable_draft_id("c1") != stable_draft_id("c2")


def test_stable_participant_id_prefixed():
    assert stable_participant_id("v1").startswith("artpart")
    assert stable_participant_id("v1") == stable_participant_id("v1")


# --------------------------------------------------------------------------- #
# ImpactIndex
# --------------------------------------------------------------------------- #
def test_impact_index_loads_edges(tmp_path):
    doc = {"pairs": [
        {"queryName": "threadConversation", "mutatorName": "channel.markChannelAsViewed"},
        {"queryName": "threadConversation", "mutatorName": "unsupported.mutator"},
    ]}
    p = tmp_path / "impact.json"
    p.write_text(json.dumps(doc))
    idx = ImpactIndex.from_file(str(p), supported_mutators={"channel.markChannelAsViewed"})
    assert idx is not None
    assert ("threadConversation", "channel.markChannelAsViewed") in idx.edges
    assert ("threadConversation", "unsupported.mutator") not in idx.edges


def test_impact_index_missing_file_returns_none():
    assert ImpactIndex.from_file("/no/such/file.json", set()) is None


# --------------------------------------------------------------------------- #
# load_baseline integration (against the committed art-baseline.json)
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(not BASELINE_PATH.exists(), reason="art-baseline.json not present")
def test_load_baseline_structure():
    b = load_baseline(str(BASELINE_PATH))
    assert isinstance(b, Baseline)
    assert b.queries, "baseline must have queries"
    assert len(b.queries) == 151
    assert len(b.mutations) == 151
    assert len(b.oneshots) == 21
    assert b.server_baselines_ms
    assert b.health_gates
    # every op carries its catalogue name
    assert all(op.name for op in b.queries)
    assert b.all_read_ops == b.queries + b.oneshots
