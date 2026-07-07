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
    # ticketsQueryV2.viewMode: z.enum(['project','board','my-tickets',
    # 'user-tickets','group-tickets']) — "kanban" predates the V2 enum and
    # only surfaced once the main-93410d5c backend actually defined the
    # query (before that it 404'd at name lookup, masking arg validation).
    "viewMode": ["project", "board", "my-tickets"],
    # stagesByBoards.boardType: z.nativeEnum(BoardType) — real labels from
    # prisma enum BoardType {DEFAULT RELEASE NON_LINEAR}.
    "boardType": ["DEFAULT", "RELEASE"],
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
        # Unknown key: record it and OMIT the key entirely. Real clients omit
        # optional args they don't use; sending an explicit null fails zod
        # .optional() schemas (optional != nullable) and no real client sends
        # null IDs for required fields either — so a null teaches us nothing
        # beyond the validation error path. Omission lets optional-arg queries
        # hydrate and turns required-arg misses into plain "Required" errors
        # (same unresolvable-args class as before, cleaner attribution).
        self.unresolved[key] = self.unresolved.get(key, 0) + 1
        return None, False

    def resolve(self, op: Op) -> tuple[dict[str, Any], bool]:
        """Build an args object for `op`. Returns (argsObj, fully_resolved?).
        Unresolved keys are omitted from the args object (see _resolve_key)."""
        args: dict[str, Any] = {}
        ok = True
        for key in op.args_keys:
            val, resolved = self._resolve_key(key)
            if resolved:
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


def _args_mark_channel_as_viewed(resolver: "ArgResolver", now_ms: int,
                                 entity_id: Optional[str] = None) -> Optional[dict]:
    cid = entity_id
    if cid is None:
        cid, ok = resolver._resolve_key("channelId")
        if not ok:
            return None
    return {
        "channelId": cid,
        "timestamp": now_ms,
        "draftMessageId": stable_draft_id(cid),
        "draftMessage": "",
    }


def _args_mark_thread_activities_read(resolver: "ArgResolver", now_ms: int,
                                      entity_id: Optional[str] = None) -> Optional[dict]:
    cid = entity_id
    if cid is None:
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

# ---------------------------------------------------------------------------
# Impact-aware mutation targeting (adopted from staging-regression,
# feature/art XYNE-12332). The static query->mutator edge map
# (raw/query-mutator-impact.json, tools/gen_impact_matrix.sh) records which
# mutators can affect which queries via read/write table overlap. Blind
# weighted sampling fires writes at RANDOM pool entities, so the resulting
# poke usually lands on rows no subscribed query watches — G4 passes while
# the advance path of the pipelines under test goes nearly unexercised.
# Targeting flips that: with probability `bias`, pick a mutator that has an
# edge to a CURRENTLY SUBSCRIBED query on this client and aim it at that
# query's own entity id, so the write provably intersects a live pipeline.
# ---------------------------------------------------------------------------
# Which arg key of a subscribed query's args can seed each mutator's target.
ENTITY_KEYS = {
    "channel.markChannelAsViewed": "channelId",
    "activities.markThreadActivitiesAsReadV2": "conversationId",
}


class ImpactIndex:
    """(queryName, mutatorName) edges, filtered to mutators the harness can
    actually build args for. None-safe loader: a missing/stale file just
    disables targeting (G14 reads SKIP)."""

    def __init__(self, edges: set, path: str):
        self.edges = edges
        self.path = path

    @classmethod
    def from_file(cls, path: str, supported_mutators: set) -> Optional["ImpactIndex"]:
        try:
            with open(path) as f:
                doc = json.load(f)
        except Exception:
            return None
        edges = {(p.get("queryName"), p.get("mutatorName"))
                 for p in doc.get("pairs", [])
                 if p.get("mutatorName") in supported_mutators}
        return cls(edges, path)


class MutationSampler:
    """Weighted sampler over the baseline mutations the harness can build args
    for (see MUTATION_ARG_BUILDERS). Weights renormalized from prod frequency."""

    def __init__(self, mutations: list[Op], rng: random.Random):
        supported = [m for m in mutations if m.name in MUTATION_ARG_BUILDERS]
        if not supported:
            raise ValueError("MutationSampler: no supported mutations in baseline")
        self._sampler = WeightedSampler(supported, rng)
        self.supported = supported
        self.impact: Optional[ImpactIndex] = None  # set by the driver

    def sample(self) -> Op:
        return self._sampler.sample()

    def build(self, resolver: "ArgResolver", now_ms: int) -> Optional[tuple[str, dict]]:
        """Sample a mutation and build its args. Returns (name, argsObj) or None."""
        op = self.sample()
        args = MUTATION_ARG_BUILDERS[op.name](resolver, now_ms)
        if args is None:
            return None
        return op.name, args

    def build_targeted(self, subscribed: list, resolver: "ArgResolver",
                       now_ms: int, rng: random.Random,
                       bias: float) -> tuple[Optional[tuple[str, dict]], set, bool]:
        """Impact-aware build. `subscribed` = [(query_name, args_obj)] currently
        active on this client. With probability `bias` (when >=1 candidate
        exists) aim a mutator with an impact edge to a subscribed query AT THAT
        QUERY'S OWN entity id. Returns (built|None, edges_exercised, targeted?).
        edges_exercised = every (subscribed query, chosen mutator) impact edge
        co-active at fire time — the G14 coverage numerator."""
        idx = self.impact
        cands = []
        if idx is not None:
            for qname, qargs in subscribed:
                for op in self.supported:
                    ent = (qargs or {}).get(ENTITY_KEYS[op.name])
                    if ent and isinstance(ent, str) and (qname, op.name) in idx.edges:
                        cands.append((op, ent))
        if cands and rng.random() < bias:
            op, ent = rng.choice(cands)
            args = MUTATION_ARG_BUILDERS[op.name](resolver, now_ms, entity_id=ent)
            if args is not None:
                edges = {(q, op.name) for q, _ in subscribed
                         if (q, op.name) in idx.edges}
                return (op.name, args), edges, True
        built = self.build(resolver, now_ms)
        if built is None:
            return None, set(), False
        edges = set()
        if idx is not None:
            edges = {(q, built[0]) for q, _ in subscribed
                     if (q, built[0]) in idx.edges}
        return built, edges, False


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
# Schema-driven mutation arg synthesis (push-path matrix, gate G15)
# --------------------------------------------------------------------------- #
# MUTATION_ARG_BUILDERS covers 2 mutator types (~78% of prod write VOLUME but
# ~1% of the TYPE surface). The other ~216 mutators' push path — client push ->
# zero-cache -> backend mutator (zod validation + prisma writes) -> replication
# -> BOTH pods' advance — had never been exercised by this harness. This
# synthesizer generalizes the hand-built approach: it walks each mutator's
# SOURCE zod arg schema (raw/arg-schemas.source.json, extracted from the
# deployed backend image by tools/gen_arg_schemas.sh) and builds a best-effort
# args object from the id-pool, the run identity, entities created earlier in
# the same run (the overlay), and typed synthetics. Idea adopted from
# staging-regression's Zod-driven fixture synthesis (feature/art XYNE-12332);
# implementation is pool-aware and chain-aware where theirs is curated-first.
#
# Design rules (each encodes a lesson):
#   * Unresolvable OPTIONAL key -> omit (zod .optional() != nullable — the
#     same rule ArgResolver.resolve learned; explicit nulls only measure the
#     validation error path).
#   * Unresolvable REQUIRED id -> the whole mutator is SKIPPED, not fired
#     with a fabricated id: a fabricated id on an update/delete measures only
#     "entity not found" app-rejections, which teaches nothing new after the
#     first one.
#   * CREATE-phase mutators get FRESH "artmx…" ids for their self-id args
#     (client-generated-id convention) — recorded in the overlay so later
#     update/delete mutators can target entities THIS RUN owns, and recorded
#     for post-run SQL cleanup by prefix.
#   * Destructive mutators are only synthesized when every entity ref resolves
#     from the overlay (delete what we created; never seeded data).
FRESH_ID_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"

# mutator namespace -> the id-pool key its bare `id` / `<entity>Id` args mean
NS_ID_KEY = {
    "channel": "channelId", "channels": "channelId",
    "conversation": "conversationId", "conversations": "conversationId",
    "message": "messageId", "messages": "messageId",
    "ticket": "ticketId", "tickets": "ticketId", "subTicket": "ticketId",
    "project": "projectId", "projects": "projectId",
    "board": "boardId", "boards": "boardId",
    "canvas": "canvasId", "call": "callId", "calls": "callId",
    "userGroup": "userGroupId", "draftMessages": "draftMessageId",
    "draft": "draftMessageId", "activities": "activityId",
    "bookmark": "messageId", "bookmarks": "messageId",
}

_SYN_STR_BY_KEY = [
    # (predicate on lowercase key, value factory) — first match wins
    (lambda k: k in ("name", "title", "label", "tagname"), lambda m: f"art-mx {m}"[:60]),
    (lambda k: k in ("emoji",), lambda m: "🔥"),
    (lambda k: "email" in k, lambda m: "art-mx@example.invalid"),
    (lambda k: "url" in k or "link" in k, lambda m: "https://example.invalid/art-mx"),
    (lambda k: "color" in k, lambda m: "#8080ff"),
    (lambda k: k in ("position", "rank", "sortkey", "sortorder"), lambda m: "a0"),
    (lambda k: "timezone" in k, lambda m: "UTC"),
    (lambda k: k.endswith("date"), lambda m: "2026-01-01"),
    (lambda k: True, lambda m: "art matrix synthetic"),
]


def fresh_entity_id(rng: random.Random) -> str:
    return "artmx" + "".join(rng.choice(FRESH_ID_ALPHABET) for _ in range(19))


class SchemaSynthesizer:
    """Best-effort args from a mutator's serialized zod schema.

    synth() returns (args | None, meta) where meta carries:
      provenance  {argKey: overlay|pool|identity|fresh|scalar|synthetic|omitted}
      fresh_ids   [(argKey, id)] newly minted entity ids (record in overlay
                  + cleanup list only if the mutation is later APPLIED)
      skip_reason str when args is None
    """

    def __init__(self, schemas_doc: dict, ids: dict[str, list],
                 scalars: dict[str, list], identity: dict[str, str],
                 rng: random.Random):
        self.mutators = schemas_doc.get("mutators") or {}
        self.enums = schemas_doc.get("enums") or {}
        self.ids = ids
        self.scalars = scalars
        self.identity = identity          # {"userId":…, "workspaceId":…}
        self.rng = rng
        self.overlay: dict[str, list[str]] = {}   # argKey -> created ids (LIFO)

    # -- id resolution ------------------------------------------------------
    def _entity_id(self, key: str, ns: str, allow_fresh: bool,
                   overlay_only: bool) -> tuple[Optional[str], str]:
        """Resolve an id-like arg. Returns (value|None, provenance)."""
        pool_key = key
        if key == "id":
            pool_key = NS_ID_KEY.get(ns, key)
        # own-run entities first: chains (create -> update -> delete) must
        # target what we made, and destructive phases may ONLY use these
        for k in (key, pool_key):
            if self.overlay.get(k):
                return self.overlay[k][-1], "overlay"
        if overlay_only:
            return None, "unresolved"
        if key in ("userId", "workspaceId") and self.identity.get(key):
            return self.identity[key], "identity"
        for k in (key, pool_key):
            vals = self.ids.get(k)
            if vals:
                return vals[self.rng.randrange(len(vals))], "pool"
        if allow_fresh:
            return None, "want-fresh"     # caller mints + records
        return None, "unresolved"

    # -- schema walk ---------------------------------------------------------
    def _value(self, key: str, s: dict, ns: str, now_ms: int, meta: dict,
               allow_fresh: bool, overlay_only: bool, depth: int = 0):
        """Returns (value, ok). ok=False => caller omits (optional) or skips."""
        t = s.get("type")
        lk = key.lower()
        if t == "literal":
            return s.get("value"), True
        if t == "boolean":
            return False, True
        if t == "enum":
            vals = s.get("values") or []
            return (vals[0], True) if vals else (None, False)
        if t == "nativeEnum":
            vals = self.enums.get(s.get("enum") or "")
            if vals:
                return vals[0], True
            sc = self.scalars.get(key)
            if sc:
                return sc[0], True
            return None, False
        if t == "number":
            if "timestamp" in lk or lk.endswith("at") or lk.endswith("time"):
                return now_ms, True
            if lk in ("position", "order", "index", "offset"):
                return 0, True
            return 1, True
        if t == "string":
            if key == "id" or key.endswith("Id"):
                val, prov = self._entity_id(key, ns, allow_fresh, overlay_only)
                if prov == "want-fresh":
                    fid = fresh_entity_id(self.rng)
                    meta["fresh_ids"].append((key, fid))
                    meta["provenance"][key] = "fresh"
                    return fid, True
                if val is None:
                    return None, False
                meta["provenance"][key] = prov
                return val, True
            sc = self.scalars.get(key)
            if sc and isinstance(sc[0], str):
                meta["provenance"][key] = "scalar"
                return sc[0], True
            for pred, mk in _SYN_STR_BY_KEY:
                if pred(lk):
                    meta["provenance"][key] = "synthetic"
                    return mk(meta["mutator"]), True
        if t == "array":
            el = s.get("element") or {}
            singular = key[:-1] if key.endswith("s") else key
            v, ok = self._value(singular, el, ns, now_ms, meta,
                                allow_fresh=False, overlay_only=overlay_only,
                                depth=depth + 1)
            if ok:
                return [v], True
            # zod z.array() accepts [] unless .nonempty(); the extractor
            # records nonempty as a modifier we don't parse — [] is the best
            # available shot and a rejection is classified, not fatal.
            return [], True
        if t == "object":
            keys = s.get("keys") or {}
            out = {}
            for k2, s2 in keys.items():
                v, ok = self._value(k2, s2, ns, now_ms, meta, allow_fresh,
                                    overlay_only, depth + 1)
                if ok:
                    out[k2] = v
                elif not (s2.get("optional") or s2.get("hasDefault")):
                    return None, False
                else:
                    meta["provenance"][k2] = "omitted"
            return out, True
        if t == "union":
            for v2 in s.get("variants") or []:
                v, ok = self._value(key, v2, ns, now_ms, meta, allow_fresh,
                                    overlay_only, depth + 1)
                if ok:
                    return v, ok
            return None, False
        if t in ("record", "any", "unknown"):
            # z.record()/z.any() accept {} — the cheapest valid instance
            return {}, True
        return None, False                # date/bigint/tuple/opaque

    def synth(self, name: str, now_ms: int, allow_fresh: bool = False,
              overlay_only: bool = False) -> tuple[Optional[dict], dict]:
        meta = {"mutator": name, "provenance": {}, "fresh_ids": [],
                "skip_reason": None}
        entry = self.mutators.get(name)
        if entry is None:
            meta["skip_reason"] = "not-in-arg-schemas"
            return None, meta
        args_schema = entry.get("args")
        if args_schema is None:
            meta["skip_reason"] = f"non-object-args:{(entry.get('schema') or {}).get('type')}"
            return None, meta
        ns = name.split(".")[0]
        out: dict[str, Any] = {}
        for key, s in args_schema.items():
            v, ok = self._value(key, s, ns, now_ms, meta, allow_fresh,
                                overlay_only)
            if ok:
                out[key] = v
            elif s.get("optional") or s.get("hasDefault"):
                meta["provenance"][key] = "omitted"
            else:
                meta["skip_reason"] = f"required-arg-unresolvable:{key}"
                return None, meta
        return out, meta

    def commit_fresh(self, fresh_ids: list[tuple[str, str]]) -> None:
        """Record APPLIED creations so later phases can target them."""
        for key, fid in fresh_ids:
            self.overlay.setdefault(key, []).append(fid)


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
