# M1–M15 Simplification Evidence

Manyfold should be a small execution substrate whose complexity is earned at
explicit boundaries. Publishing is a compiled decision, `Graph` is a facade
over bounded owners, one observable model guides application code, and
distributed or profiling capabilities live in explicit namespaces.

This file is the acceptance ledger for that work. An item is complete only when
its implementation, focused evidence, compatibility evidence, and owning pull
request are recorded here.

## Delivery slices

| Slice | Scope | Branch | Commit | Draft PR | Status |
| --- | --- | --- | --- | --- | --- |
| 1 | M1–M5 publish planning and native interface | `agent/compile-publish-plans` | Pending | Pending | In progress |
| 2 | M6–M10 `Graph` owners and test ownership | Pending | Pending | Pending | Pending |
| 3 | M11–M14 observable and namespace surfaces | Pending | Pending | Pending | Pending |
| 4 | M15 profiling and Heart tooling separation | Pending | Pending | Pending | Pending |

## Checklist

| Item | Acceptance evidence | Status |
| --- | --- | --- |
| M1 | Golden table covers every row below, native decision, retention, inline delivery/backpressure, payload release, and API parity | In progress |
| M2 | Frozen per-route `PublishPlan` contains the resolved route, encoder, retention, delivery, materializer, and native decisions | Pending |
| M3 | `publish` and `publish_nowait` execute a current plan; topology, subscriber, taint, retention, and materializer changes invalidate it | Pending |
| M4 | One explicit native publish interface replaces the Python-visible emit-method matrix | Pending |
| M5 | Last-route, last-nowait, process-local-nowait, native alias, profile, and capability-probe caches are removed | Pending |
| M6 | A bounded, disposable subscription owner exclusively owns delivery subjects, direct callbacks, identities, counts, and graph-owned subscriptions | Pending |
| M7 | A bounded, disposable retention owner exclusively owns histories, writers, route retention, payload indexes, process-local tokens, and release | Pending |
| M8 | Query, manifest, diagram, and audit state is absent from the runtime-critical `Graph` path and owned behind explicit helpers | Pending |
| M9 | `Graph` is a facade; moved mutable dictionaries are not retained or mutated through callbacks into `Graph` | Pending |
| M10 | Reactive tests are split by publish, subscription, retention, inspection, and operator ownership | Pending |
| M11 | `PubSubObservable` and `Subscribable` are canonical; `PubSubObservable.from_source` is a real single-source adapter | Pending |
| M12 | `FluentStream`, application-facing `RoutePipeline`, monkeypatch-era operators, and overlapping compatibility stream APIs are removed or explicitly deprecated | Pending |
| M13 | Top-level exports contain only the documented common workflow plus temporary, evidence-backed Heart compatibility | Pending |
| M14 | Advanced APIs are discoverable under explicit `graph`, `stream`, `node`, `transport`, and `security` namespaces | Pending |
| M15 | Profilers and Heart-specific tools are not installed as normal runtime APIs or commands; tooling ownership and invocation are explicit | Pending |

## Draft publish decision table

This provisional table records the first policy inventory. It is not the M1
golden table and is deliberately marked in progress until an executable
`PublishDecisionCase` table covers every compiler leaf and exact native mode.

| # | Target and conditions | `publish` native outcome | `publish_nowait` native outcome | Payload owner and release | Delivery and backpressure | Parity requirement |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `LifecycleBinding` or `WriteBinding` | Routed envelopes | Same routed operation, return discarded | Native retention plus Python payload mirror for recorded routed envelopes; history expiry/disposal releases mirrors | Routed subscribers are delivered synchronously; the configured pending-write bound remains authoritative | Same writes, bindings, retention, and delivery |
| 2 | Raw `RouteRef` bytes, unrouted | Return source envelope | Same operation, return discarded | Native route retention; Python opens retained bytes on demand | Direct route observers receive the same envelope synchronously | Same latest value, sequence, retention, and observers |
| 3 | Raw `RouteRef` bytes, routed | Return routed source envelope and record emitted routes | Same operation, return discarded | Each emitted route owns native retention; Python mirrors only payloads needed by its observers/indexes | Existing native routing and subscriber bounds apply | Same emitted routes and delivery |
| 4 | Typed bytes passthrough, sparse, no delivery subscriber or materializer | Return source envelope without Python payload retention | Drop returned envelope | Native retention owns bytes; no Python materialized payload | No Python delivery work or queue allocation | Same accepted sequence, latest bytes, retention, and writer |
| 5 | Typed bytes passthrough, sparse, direct delivery subscriber | Return source envelope and deliver | Return source envelope internally and deliver, public return discarded | Native retention owns bytes; any temporary Python payload is released before return | Delivery is synchronous and inline; slow/reentrant callbacks apply natural caller backpressure without an unbounded queue | Exactly-once delivery and identical retained state |
| 6 | Typed bytes passthrough with native materializer and at least one source/target delivery subscriber | Return source plus materialized envelopes | Same materialized-envelope operation, public return discarded | Native source/state retention owns bytes; Python receives only envelopes required for delivery and retains no duplicate payload | Source/target callbacks are synchronous and exactly once; no hidden queue | Same source/state sequences, retention, and callback order |
| 7 | Typed bytes passthrough with native materializer and no source/target delivery subscriber | Return source plus materialized envelopes | Materialize source/state and drop returned Python envelopes | Native source/state retention owns bytes; Python payload/envelope ownership stays empty | No Python delivery or callback allocation | Same source/state latest values, sequences, and retention |
| 8 | Typed encoded, non-process-local payload | Return source envelope and decoded typed view | Return source envelope internally, public return discarded | Encoded bytes follow route retention; Python mirror is released on expiry/disposal | Typed subscribers receive decoded values synchronously | Same encoding, latest value, sequence, retention, and observers |
| 9 | `Schema.any()` process-local payload | Return source envelope and identity-preserving typed view | Return source envelope internally, public return discarded | Python process-local token store owns values; route expiry and `Graph.dispose()` must delete every token and secondary index | Subscribers receive the original object identity synchronously | Same identity, history bound, delivery, and complete release |
| 10 | Sparse eligibility disabled by empty bytes, `control_epoch`, route taint, ephemeral layer, or write-request route | Return/record source envelope through the non-sparse path | Same non-sparse operation, public return discarded | Python/native retention follows the resolved route policy; all mirrors and indexes expire together | Existing synchronous delivery and guarded-write bounds apply | Same latest, metadata, retention, and delivery |
| 11 | Source has native or Python routing that rejects a single-unrouted decision | Routed envelopes | Same routed operation, public return discarded | Every emitted route follows its own retention and payload policy | Existing route fan-out and bounded pending-write policy apply | No target or delivery may disappear during fallback |

Lineage arguments are compatibility no-ops in the sparse runtime and therefore
do not create separate publish decisions. Producer identity and
`control_epoch` remain real inputs and are covered in the non-sparse and routed
rows.

## Performance evidence

The checked-in benchmark runs real `Graph` paths from
`manyfold.private.profiling.publish_benchmarks`. It reports an unmeasured
preflight, per-route first-publish cost, a warmed steady-state loop, end-to-end
time, all run results, variance, final-state equality, and repository/native
provenance.

The first-publish metric is named `per_route_first_publish`: the unmeasured
preflight warms process and native globals, while each measured value uses a
fresh `Graph` and route and therefore includes per-route policy compilation.
Formal CLI runs reject a dirty worktree. Provenance is sampled before the output
artifact is created, and includes the loaded native extension's SHA-256 digest.

Frozen before/after command:

```sh
uv run python -m manyfold.private.profiling.publish_benchmarks \
  --iterations 100000 \
  --runs 7 \
  --warmup-iterations 10000 \
  --output-json docs/benchmarks/evidence/publish_plan_before.json
```

The post-change command is identical except for
`publish_plan_after.json`. Run on the same host and interpreter. No warmed
workload may regress by more than 10% in mean event latency; per-route
first-publish may not regress by more than 20%. Relative standard deviation
above 10% requires a quiet-host rerun before drawing a conclusion. Report every
workload, the environment, all seven runs, mean, standard deviation, and
observed variance.

The benchmark currently records whether `Graph.dispose()` releases
`Schema.any()` values and then performs benchmark-owned cleanup to isolate
runs. M1/M7 are not complete until `released_by_graph` is true and the cleanup
is a no-op.

## Ownership gates for M6–M9

- Moved mutable state is absent from `Graph.__dict__`.
- Each owner documents hard bounds, mutation authority, and disposal.
- Owner callbacks receive narrow values or callables and never mutate hidden
  `Graph` dictionaries.
- Disposal clears callbacks, subscriptions, payloads, native registrations,
  and every secondary index; repeated disposal is safe.
- Stress tests cross every configured bound and show flat counts after expiry
  and disposal.
- `Graph` facade methods adapt inputs and delegate one coherent operation.

## Observable gates for M11–M14

`PubSubObservable.from_source(source)` is the canonical single-source adapter
and is not implemented through `merge`. Heart-shaped tests must prove:

- replay-enabled and replay-disabled subscribers can coexist on one route;
- disposal followed by resubscription is independent;
- synchronous reentrant publication preserves the documented order;
- error and completion signals propagate once;
- operators return the canonical observable/subscribable types.

The stable namespace containing this adapter must survive top-level export
narrowing.

## Heart compatibility

- Historical pinned evidence: Heart depends on Manyfold
  `726f64d72b36d8bd134bda63e29ebd80472736b6`. Preserve its exact imports and
  focused integration behavior from a clean snapshot.
- The local Heart checkout contains user changes and is read-only evidence.
- Heart commit `8d34cd85` is superseded. It may supply policy evidence but must
  never be pinned, merged, or used to preserve hidden `Graph` fallbacks.
- Heart PR #954 head `d86556c6` is the producer-identity prerequisite.
- Manyfold PR #282 head `4ae2d835cc6f462851eec7f23e4d6131c2bc0589`
  is candidate-only pending its dedicated supervisor and hosted checks. No M or
  Heart branch may treat it as an accepted base yet.
- The authoritative forward consumer gate is current Heart after #954 and the
  H work, pinned to the settled Manyfold stack. Record that SHA and focused
  command before M11–M14 are complete.
