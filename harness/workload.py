"""
workload.py — pure workload model for the ART Mode-A replay harness.

Turns art-baseline.json (the production-derived workload) into:
  * a weighted sampler over the 151 named queries / 21 one-shots / 151 mutations, and
  * concrete Zero wire messages (`initConnection` + desired-query puts/dels).

This module is intentionally dependency-free (stdlib only) and side-effect free
so it can be unit-tested and `--dry-run` inspected without a live zero-cache.

Wire format (verified against the mono repo, protocol v49):
  packages/zero-protocol/src/queries-patch.ts  -> a NAMED custom query put is
      {"op":"put","hash":<str>,"name":<str>,"args":[<argsObj>],"ttl":<ms>}
  packages/zero-protocol/src/connect.ts        -> initConnection body carries
      {"desiredQueriesPatch":[...puts...], "userQueryURL"?, ...}
zero-cache resolves name+args -> AST server-side via the app query endpoint, so
the harness does NOT need the Xyne schema or query definitions locally.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import random
from dataclasses import dataclass, field
from typing import Any, Optional


# --------------------------------------------------------------------------- #
# Baseline loading
# --------------------------------------------------------------------------- #
@dataclass
class Op:
    """One catalogue entry (query, one-shot, or mutation)."""
    name: str
    weight: float          # weight_pct from the baseline (falls back to calls share)
    args_keys: list[str]   # arg-schema keys (queries only; [] for mutations/one-shots here)
    calls: int
    p50_ms: Optional[float] = None
    kind: str = "query"    # "query" | "oneshot" | "mutation"


@dataclass
class Baseline:
    path: str
    version: str
    queries: list[Op]
    oneshots: list[Op]
    mutations: list[Op]
    server_baselines_ms: dict[str, Any]
    health_gates: dict[str, Any]
    scale: dict[str, Any]

    @property
    def all_read_ops(self) -> list[Op]:
        return self.queries + self.oneshots


def load_baseline(path: str) -> Baseline:
    with open(path) as f:
        d = json.load(f)

    def op(row: dict, kind: str) -> Op:
        calls = int(row.get("calls_7d", 0))
        return Op(
            name=row["name"],
            weight=float(row.get("weight_pct", 0.0)) or float(calls),
            args_keys=list(row.get("args", [])),
            calls=calls,
            p50_ms=row.get("p50_ms"),
            kind=kind,
        )

    return Baseline(
        path=path,
        version=d.get("art_version", "?"),
        queries=[op(r, "query") for r in d["query_workload"]["queries"]],
        oneshots=[op(r, "oneshot") for r in d["oneshot_workload"]["queries"]],
        mutations=[op(r, "mutation") for r in d["mutation_workload"]["mutations"]],
        server_baselines_ms=d["server_baselines_ms"],
        health_gates=d["health_gates"],
        scale=d.get("scale_7d", {}),
    )


# --------------------------------------------------------------------------- #
# Weighted sampling (deterministic under a seeded random.Random)
# --------------------------------------------------------------------------- #
class WeightedSampler:
    """O(log n) weighted pick over a list of Ops using a cumulative table."""

    def __init__(self, ops: list[Op], rng: random.Random):
        self._ops = list(ops)
        self._rng = rng
        self._cum: list[float] = []
        total = 0.0
        for o in self._ops:
            total += max(o.weight, 0.0)
            self._cum.append(total)
        self._total = total
        if total <= 0:
            raise ValueError("WeightedSampler: total weight is zero")

    def sample(self) -> Op:
        x = self._rng.random() * self._total
        i = bisect.bisect_left(self._cum, x)
        if i >= len(self._ops):
            i = len(self._ops) - 1
        return self._ops[i]

    def coverage_after(self, n: int) -> int:
        """How many distinct ops are expected to appear in n samples (probabilistic upper bound helper for tests)."""
        return min(n, len(self._ops))


# --------------------------------------------------------------------------- #
# Argument resolution (id-pool -> concrete arg objects)
# --------------------------------------------------------------------------- #
# Scalar defaults for non-ID argument keys seen across the 151-query catalogue.
# gen_id_pool.py fills the ID-valued keys from telemetry; these cover the rest.
DEFAULT_SCALARS: dict[str, list[Any]] = {
    "limit": [25, 50],
    # Pagination cursor: every zod schema is z.object({...}).nullable() — null
    # means "first page", which is also what most real first loads send.
    "start": [None],
    "direction": ["forward", "backward"],
    "isMember": [True],
    "isRead": [False, True],
    "showOverdueOnly": [False],
    "viewMode": ["kanban"],
    "columnType": ["stage"],
    "groupBy": ["stage"],
    "classification": [[]],
    "types": [[]],
    "lastUpdatedAt": [0],
    "updatedAt": [0],
    "recapDate": [0],
    "contextType": ["BOARD", "STAGE"],
    "entityType": ["TICKET"],
}


@dataclass
class ArgResolver:
    ids: dict[str, list[Any]]
    scalars: dict[str, list[Any]]
    rng: random.Random
    # Zipf exponent for ID sampling: 0 = uniform; ~1.1 approximates prod hot-key
    # skew (gen_id_pool_db.py hotness-ranks pools so index 0 = hottest).
    zipf_s: float = 0.0
    unresolved: dict[str, int] = field(default_factory=dict)
    _zipf_cdf: dict[int, list[float]] = field(default_factory=dict)

    @classmethod
    def from_pool_file(cls, path: Optional[str], rng: random.Random,
                       zipf_s: float = 0.0) -> "ArgResolver":
        ids: dict[str, list[Any]] = {}
        scalars = {k: list(v) for k, v in DEFAULT_SCALARS.items()}
        if path:
            with open(path) as f:
                pool = json.load(f)
            ids = {k: list(v) for k, v in pool.get("ids", {}).items() if v}
            for k, v in pool.get("scalars", {}).items():
                if v:
                    scalars[k] = list(v)
        return cls(ids=ids, scalars=scalars, rng=rng, zipf_s=zipf_s)

    def _pick(self, pool: list[Any]) -> Any:
        """Rank-based Zipf pick over a hotness-ordered pool (uniform when zipf_s<=0)."""
        n = len(pool)
        if self.zipf_s <= 0 or n < 2:
            return self.rng.choice(pool)
        cdf = self._zipf_cdf.get(n)
        if cdf is None:
            w = [1.0 / (k ** self.zipf_s) for k in range(1, n + 1)]
            total, acc, cdf = sum(w), 0.0, []
            for x in w:
                acc += x
                cdf.append(acc / total)
            self._zipf_cdf[n] = cdf
        return pool[bisect.bisect_left(cdf, self.rng.random())]

    def _resolve_key(self, key: str) -> tuple[Any, bool]:
        """Return (value, resolved?). Handles Id/Ids plurals and scalar fallbacks."""
        if key in self.ids:
            return self._pick(self.ids[key]), True
        if key in self.scalars:
            return self.rng.choice(self.scalars[key]), True
        # plural id list, e.g. channelIds <- channelId pool, ticketsByIds <- ticketId
        if key.endswith("Ids"):
            singular = key[:-3] + "Id"
            if singular in self.ids:
                pool = self.ids[singular]
                k = min(len(pool), self.rng.randint(1, 3))
                return self.rng.sample(pool, k), True
        if key.endswith("s"):
            singular = key[:-1]
            if singular in self.ids:
                return [self._pick(self.ids[singular])], True
        # Unknown key: record and emit null so the query still forms.
        self.unresolved[key] = self.unresolved.get(key, 0) + 1
        return None, False

    def resolve(self, op: Op) -> tuple[dict[str, Any], bool]:
        """Build an args object for `op`. Returns (argsObj, fully_resolved?)."""
        args: dict[str, Any] = {}
        ok = True
        for key in op.args_keys:
            val, resolved = self._resolve_key(key)
            args[key] = val
            ok = ok and resolved
        return args, ok


# --------------------------------------------------------------------------- #
# Wire-message construction (protocol v49)
# --------------------------------------------------------------------------- #
def stable_hash(name: str, args: Any) -> str:
    """Deterministic desired-query hash so identical (name,args) dedupe like the real client."""
    payload = name + "\x00" + json.dumps(args, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def query_put(name: str, args_obj: dict[str, Any], ttl_ms: int = 300_000) -> dict[str, Any]:
    """A named custom-query 'put' op for desiredQueriesPatch (args wrapped in a 1-elem array)."""
    args_array = [args_obj]
    return {
        "op": "put",
        "hash": stable_hash(name, args_array),
        "name": name,
        "args": args_array,
        "ttl": ttl_ms,
    }


def query_del(hash_: str) -> dict[str, Any]:
    return {"op": "del", "hash": hash_}


def init_connection_message(
    desired_puts: list[dict[str, Any]],
    user_query_url: Optional[str] = None,
    user_query_headers: Optional[dict[str, str]] = None,
    active_clients: Optional[list[str]] = None,
    client_schema: Optional[dict[str, Any]] = None,
    deleted_client_ids: Optional[list[str]] = None,
) -> list[Any]:
    """Build ['initConnection', {...}] per packages/zero-protocol/src/connect.ts."""
    body: dict[str, Any] = {"desiredQueriesPatch": desired_puts}
    if user_query_url:
        body["userQueryURL"] = user_query_url
    if user_query_headers:
        body["userQueryHeaders"] = user_query_headers
    if active_clients:
        body["activeClients"] = active_clients
    if deleted_client_ids:
        # DeleteClientsBody (delete-clients.ts): server GCs these clients'
        # CVR state and acks with a downstream ['deleteClients', ...].
        body["deleted"] = {"clientIDs": deleted_client_ids}
    if client_schema:
        # Required by zero-cache for NEW client groups (view-syncer rejects
        # otherwise). Harvest from a real client group's CVR row or the app.
        body["clientSchema"] = client_schema
    return ["initConnection", body]


def change_desired_queries_message(patch: list[dict[str, Any]]) -> list[Any]:
    """Upstream 'changeDesiredQueries' to add/remove desired queries mid-connection."""
    return ["changeDesiredQueries", {"desiredQueriesPatch": patch}]


# --------------------------------------------------------------------------- #
# Mutations (protocol v49 custom-mutator push)
# --------------------------------------------------------------------------- #
# Wire format verified against the mono repo:
#   packages/zero-protocol/src/push.ts  -> pushMessageSchema / customMutationSchema
#     ["push", {clientGroupID, mutations:[{type:"custom", id, clientID, name,
#               args:[argsObj], timestamp}], pushVersion, timestamp, requestID}]
#   packages/zero-client/src/client/zero.ts asserts pushVersion === 1.
PUSH_VERSION = 1

# Arg builders for the mutations the harness knows how to fire safely.
# Signatures verified against the Xyne app (dashboard/src/zero/mutators.ts):
#   channel.markChannelAsViewed         {channelId, conversationId?, timestamp,
#                                        draftMessageId, draftMessage}
#   activities.markThreadActivitiesAsReadV2
#                                       {conversationId, draftMessageId,
#                                        draftMessage, timestamp}
# Together these are ~78% of prod write volume (read-tracking; they only touch
# the calling user's own channel_user_status / activities / draft rows).
#
# draftMessageId is DETERMINISTIC per entity ("art-" prefixed) so repeated runs
# upsert the same (empty) draft row instead of creating one per call.
def stable_draft_id(entity_id: str) -> str:
    return "artdraft" + hashlib.sha256(entity_id.encode("utf-8")).hexdigest()[:16]


def stable_participant_id(entity_id: str) -> str:
    return "artpart" + hashlib.sha256(entity_id.encode("utf-8")).hexdigest()[:16]


def _args_mark_channel_as_viewed(resolver: "ArgResolver", now_ms: int) -> Optional[dict]:
    cid, ok = resolver._resolve_key("channelId")
    if not ok:
        return None
    return {
        "channelId": cid,
        "timestamp": now_ms,
        "draftMessageId": stable_draft_id(cid),
        "draftMessage": "",
    }


def _args_mark_thread_activities_read(resolver: "ArgResolver", now_ms: int) -> Optional[dict]:
    cid, ok = resolver._resolve_key("conversationId")
    if not ok:
        return None
    return {
        "conversationId": cid,
        "draftMessageId": stable_draft_id(cid),
        "draftMessage": "",
        "timestamp": now_ms,
        # Newer builds insert a conversation_participants row with this id when
        # the user isn't a participant yet; deterministic so re-runs reuse it.
        # Older builds' zod schema strips the unknown key harmlessly.
        "participantId": stable_participant_id(cid),
    }


MUTATION_ARG_BUILDERS = {
    "channel.markChannelAsViewed": _args_mark_channel_as_viewed,
    "activities.markThreadActivitiesAsReadV2": _args_mark_thread_activities_read,
}


class MutationSampler:
    """Weighted sampler over the baseline mutations the harness can build args
    for (see MUTATION_ARG_BUILDERS). Weights renormalized from prod frequency."""

    def __init__(self, mutations: list[Op], rng: random.Random):
        supported = [m for m in mutations if m.name in MUTATION_ARG_BUILDERS]
        if not supported:
            raise ValueError("MutationSampler: no supported mutations in baseline")
        self._sampler = WeightedSampler(supported, rng)
        self.supported = supported

    def sample(self) -> Op:
        return self._sampler.sample()

    def build(self, resolver: "ArgResolver", now_ms: int) -> Optional[tuple[str, dict]]:
        """Sample a mutation and build its args. Returns (name, argsObj) or None."""
        op = self.sample()
        args = MUTATION_ARG_BUILDERS[op.name](resolver, now_ms)
        if args is None:
            return None
        return op.name, args


def custom_mutation(mutation_id: int, client_id: str, name: str,
                    args_obj: dict[str, Any], now_ms: int) -> dict[str, Any]:
    """One custom mutation for a push body (args wrapped in a 1-elem array)."""
    return {
        "type": "custom",
        "id": mutation_id,
        "clientID": client_id,
        "name": name,
        "args": [args_obj],
        "timestamp": now_ms,
    }


def push_message(client_group_id: str, mutations: list[dict[str, Any]],
                 request_id: str, now_ms: int) -> list[Any]:
    """Build ['push', {...}] per packages/zero-protocol/src/push.ts."""
    return ["push", {
        "clientGroupID": client_group_id,
        "mutations": mutations,
        "pushVersion": PUSH_VERSION,
        "timestamp": now_ms,
        "requestID": request_id,
    }]


# --------------------------------------------------------------------------- #
# Self-test / dry-run inspector
# --------------------------------------------------------------------------- #
def _self_test(baseline_path: str, pool_path: Optional[str], n: int, seed: int) -> None:
    rng = random.Random(seed)
    bl = load_baseline(baseline_path)
    resolver = ArgResolver.from_pool_file(pool_path, rng)
    qsampler = WeightedSampler(bl.queries, rng)

    print(f"baseline v{bl.version}: {len(bl.queries)} queries, "
          f"{len(bl.oneshots)} one-shots, {len(bl.mutations)} mutations")
    print(f"id-pool: {'none (scalars only)' if not pool_path else pool_path} "
          f"({sum(len(v) for v in resolver.ids.values())} ids across {len(resolver.ids)} keys)\n")

    counts: dict[str, int] = {}
    fully = 0
    print(f"--- {min(n, 8)} sample wire messages (of {n} drawn) ---")
    for i in range(n):
        op = qsampler.sample()
        counts[op.name] = counts.get(op.name, 0) + 1
        args, ok = resolver.resolve(op)
        fully += ok
        if i < 8:
            print(json.dumps(query_put(op.name, args)))

    distinct = len(counts)
    top = sorted(counts.items(), key=lambda kv: -kv[1])[:5]
    print(f"\ndistinct queries hit: {distinct}/{len(bl.queries)}")
    print(f"fully-resolved args : {fully}/{n} ({100*fully//max(n,1)}%)")
    print(f"top sampled         : {top}")
    if resolver.unresolved:
        miss = sorted(resolver.unresolved.items(), key=lambda kv: -kv[1])[:10]
        print(f"unresolved arg keys : {miss}")
        print("  (fill these in id-pool.json via gen_id_pool.py or scalars overrides)")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Inspect the ART workload model (pure, no network).")
    ap.add_argument("--baseline", default="../art-baseline.json")
    ap.add_argument("--id-pool", default=None)
    ap.add_argument("--samples", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    _self_test(a.baseline, a.id_pool, a.samples, a.seed)
