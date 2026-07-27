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

This is the reviewed schema for the executable M1 table. The implementation
will define `PublishMode`, `PublishDecisionLeaf`, and frozen
`PublishDecisionCase` rows in one source of truth. Tests must assert that the
case table covers the exact enum sets, then execute every row through a real
`Graph`. Adding a mode or compiler match arm without a case must fail.

The canonical native interface has exactly these modes:

| `PublishMode` | Native contract |
| --- | --- |
| `ROUTED_ENVELOPES` | Execute general native routing and return every emitted envelope. |
| `SOURCE_ENVELOPE` | Accept one known-unrouted source and return its envelope. |
| `SOURCE_DROP` | Accept one known-unrouted source without constructing a Python envelope. |
| `MATERIALIZED_ENVELOPES` | Accept one source, run all registered native materializers, and return source then targets in registration order. |
| `SINGLE_MATERIALIZED_DROP` | Accept one source and its sole materialized target without Python envelopes. |
| `ALL_MATERIALIZED_DROP` | Accept one source and all materialized targets without Python envelopes. |

The golden rows below name the existing native call sequence that M1 freezes
and the single mode that replaces it in M4. `Graph.` denotes the native PyO3
graph, not the Python facade.

### Binding, raw, sparse, and routed leaves

| Case | API | Conditions and `PublishDecisionLeaf` | Current native call sequence | `PublishMode` | Golden state contract |
| --- | --- | --- | --- | --- | --- |
| `lifecycle_publish` | `publish` | `LifecycleBinding`; `LIFECYCLE_BINDING` | `Graph.emit(request)` and, only if unresolved, `Graph.emit(desired)` | `ROUTED_ENVELOPES` | Mutate lifecycle and write-binding registries before resolution; record every routed envelope; expire mirrors with route history. |
| `lifecycle_nowait` | `publish_nowait` | `LifecycleBinding`; `LIFECYCLE_BINDING` | Call Python `publish`, then the same `Graph.emit` sequence | `ROUTED_ENVELOPES` | Same registry mutations, native writes, delivery, and release; only the public return differs. |
| `write_binding_publish` | `publish` | `WriteBinding`; `WRITE_BINDING` | `Graph.emit(request)` and, only if unresolved, `Graph.emit(desired)` | `ROUTED_ENVELOPES` | Resolve/register the binding without adding a lifecycle entry; record all emitted routes. |
| `write_binding_nowait` | `publish_nowait` | `WriteBinding`; `WRITE_BINDING` | Call Python `publish`, then the same `Graph.emit` sequence | `ROUTED_ENVELOPES` | Same binding, writes, delivery, history, and payload release. |
| `raw_unrouted_publish` | `publish` | raw `RouteRef`; `RAW_ROUTE_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Eager Python byte mirror follows resolved history and is released on expiry/disposal. |
| `raw_unrouted_nowait_fallback` | `publish_nowait` | raw `RouteRef`; `RAW_ROUTE_NOWAIT_FALLBACK` | Call Python `publish`, then `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Intentionally materialize the internal return; parity covers latest, sequence, replay, and release. |
| `raw_routed_publish` | `publish` | raw route with native routing; `ROUTED_RETURN` | `Graph.emit_single_if_unrouted` returns `None`, then `Graph.emit` | `ROUTED_ENVELOPES` | Record each emitted route once and retain each payload under that route's policy. |
| `raw_routed_nowait_fallback` | `publish_nowait` | raw route with native routing; `RAW_ROUTE_NOWAIT_FALLBACK` | Call Python `publish`; `Graph.emit_single_if_unrouted`, then `Graph.emit` | `ROUTED_ENVELOPES` | No routed output, subscriber delivery, or release may differ from `publish`. |
| `typed_sparse_publish` | `publish` | nonempty bytes, eligible route, no observer/materializer/control/taint; `SPARSE_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Native history owns the bytes; Python history and payload indexes remain zero. |
| `typed_sparse_nowait` | `publish_nowait` | same sparse conditions; `SPARSE_DROP` | `Graph.emit_single_if_unrouted_drop` | `SOURCE_DROP` | Accepted sequence/latest/native replay equal `publish`; no Python envelope or payload allocation. |
| `typed_sparse_source_subscriber_publish` | `publish` | direct source subscriber; `OBSERVED_SPARSE_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Temporary payload exists only during one inline callback; native history remains authoritative. |
| `typed_sparse_source_subscriber_nowait` | `publish_nowait` | direct source subscriber; `OBSERVED_SPARSE_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Exactly one inline callback before return; reentrant publication is caller-backpressured and queue-free. |
| `typed_routed_publish` | `publish` | typed route with native routing; `ROUTED_RETURN` | `Graph.emit_single_if_unrouted` returns `None`, then `Graph.emit` | `ROUTED_ENVELOPES` | Every routed envelope is recorded/delivered once under its own retention policy. |
| `typed_routed_nowait` | `publish_nowait` | typed route with native routing; `ROUTED_NOWAIT_FALLBACK` | `Graph.emit_single_if_unrouted` returns `None`; Python `publish` retries it, then calls `Graph.emit` | `ROUTED_ENVELOPES` | Same outputs and state as `publish`; M3 removes the duplicate native probe. |

### Native materializer leaves

| Case | API | Conditions and `PublishDecisionLeaf` | Current native call | `PublishMode` | Golden state and callback contract |
| --- | --- | --- | --- | --- | --- |
| `materializer_one_publish_unobserved` | `publish` | one target, no delivery subscribers; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Source and target native histories advance once; Python payload indexes remain zero. |
| `materializer_many_publish_unobserved` | `publish` | two targets, no delivery subscribers; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Source and both targets advance once in registration order; no Python delivery. |
| `materializer_one_nowait_default_producer` | `publish_nowait` | one target, no delivery, default producer; `MATERIALIZE_DROP_ONE_DEFAULT` | `Graph.emit_single_if_unrouted_and_materializer_drop_python` | `SINGLE_MATERIALIZED_DROP` | Source/target native retention advances once; no returned or retained Python envelope. |
| `materializer_one_nowait_explicit_producer` | `publish_nowait` | one target, no delivery, explicit producer; `MATERIALIZE_DROP_ONE_EXPLICIT` | `Graph.emit_single_if_unrouted_and_materializer_drop` | `SINGLE_MATERIALIZED_DROP` | Same state with the supplied producer identity on source and target. |
| `materializer_many_nowait_default_producer` | `publish_nowait` | two targets, no delivery, default producer; `MATERIALIZE_DROP_ALL_DEFAULT` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop_python` | `ALL_MATERIALIZED_DROP` | Source and both targets advance once; Python envelope/payload indexes remain zero. |
| `materializer_many_nowait_explicit_producer` | `publish_nowait` | two targets, no delivery, explicit producer; `MATERIALIZE_DROP_ALL_EXPLICIT` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop` | `ALL_MATERIALIZED_DROP` | Same state with exact producer identity and no Python delivery. |
| `materializer_source_delivery` | both | one target; source subscriber only; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Callback order is `[source]`; count one; target history still advances. |
| `materializer_target_delivery` | both | one target; target subscriber only; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Callback order is `[target]`; count one; source history still advances. |
| `materializer_source_and_target_delivery` | both | one target; source and target subscribers; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Callback order is `[source, target]`; each count one; callbacks finish before return. |
| `materializer_many_delivery_order` | both | two targets; subscribers on source and both targets; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Callback order is source then targets in registration order; each count one. |
| `materializer_control_fallback` | both | materializer present and `control_epoch` supplied; `CONTROL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Control validation and source retention occur; the materializer-drop modes are not selected. |

### Encoded, process-local, and sparse-disable leaves

| Case | API | Conditions and `PublishDecisionLeaf` | Current native call | `PublishMode` | Golden ownership and invalidation contract |
| --- | --- | --- | --- | --- | --- |
| `typed_encoded_publish` | `publish` | non-process-local encoder; `ENCODED_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Python history owns the encoded mirror; decode, expiry, and disposal are exact. |
| `typed_encoded_nowait` | `publish_nowait` | non-process-local encoder; `ENCODED_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Same encoding, replay, delivery, bounds, and release as `publish`. |
| `process_local_publish` | `publish` | `Schema.any()`; `PROCESS_LOCAL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Original object identity is retained; expiry/disposal releases its token and every secondary index. |
| `process_local_nowait_unobserved` | `publish_nowait` | `Schema.any()`, no observer/taint/control; `PROCESS_LOCAL_NOWAIT_CACHED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Compiled bound `N` applies equally to token order, payload set, route-by-payload, and per-route payload set; all release on disposal. |
| `process_local_nowait_late_observer` | `publish_nowait` | observer added after cached publish; `PROCESS_LOCAL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Subscriber generation invalidates the cached leaf before the next write; one inline identity-preserving callback; all four indexes remain at `N`. |
| `process_local_nowait_observed` | `publish_nowait` | existing observer; `PROCESS_LOCAL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | General observed history path, identity-preserving callback, bounded token ownership, complete disposal. |
| `process_local_nowait_tainted` | `publish_nowait` | route tainted; `TAINTED_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Taint generation invalidates the cached leaf; history, token indexes, and taint secondary indexes expire together. |
| `process_local_nowait_control` | `publish_nowait` | `control_epoch` supplied; `CONTROL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Per-call control validation bypasses the cached leaf; general history and token release apply. |
| `empty_bytes_publish` | `publish` | eligible typed bytes encode to `b\"\"`; `EMPTY_BYTES_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Python envelope history follows the resolved bound; sparse native-only ownership is disabled. |
| `empty_bytes_nowait` | `publish_nowait` | eligible typed bytes encode to `b\"\"`; `EMPTY_BYTES_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Same history and release as `publish`; `SOURCE_DROP` is forbidden. |
| `control_epoch_publish` | `publish` | eligible typed bytes with control epoch; `CONTROL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Validate control epoch, retain metadata/history, and bypass sparse ownership. |
| `control_epoch_nowait` | `publish_nowait` | same; `CONTROL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Exact state parity and no sparse/materializer drop selection. |
| `tainted_publish` | `publish` | eligible typed bytes on tainted route; `TAINTED_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Python history and taint indexes share one bound and disposal path. |
| `tainted_nowait` | `publish_nowait` | same; `TAINTED_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Taint invalidation precedes the write; exact parity with `publish`. |
| `ephemeral_publish` | `publish` | `Layer.Ephemeral`; `EPHEMERAL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Resolved replay bound is zero; the returned envelope is caller-owned and the graph retains no payload. |
| `ephemeral_nowait` | `publish_nowait` | `Layer.Ephemeral`; `EPHEMERAL_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Same zero graph-retention result; the internal return is discarded and sparse drop remains forbidden. |
| `write_request_publish` | `publish` | `Plane.Write` and `Variant.Request`; `WRITE_REQUEST_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Guarded-write metadata/retention applies through the general path. |
| `write_request_nowait` | `publish_nowait` | same; `WRITE_REQUEST_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Same validation, writer, delivery, retention, and release. |
| `lineage_arguments_discarded_publish` | `publish` | every legacy lineage input populated on a sparse route; `SPARSE_RETURN` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Result equals the no-lineage case; native lineage/correlation counts remain zero. |
| `lineage_arguments_discarded_nowait` | `publish_nowait` | every legacy lineage input populated on a sparse route; `SPARSE_DROP` | `Graph.emit_single_if_unrouted_drop` | `SOURCE_DROP` | No ID-specialized call or mode exists; M5 deletes those unreachable native branches. |

The exact `PublishDecisionLeaf` set is therefore:
`LIFECYCLE_BINDING`, `WRITE_BINDING`, `RAW_ROUTE_RETURN`,
`RAW_ROUTE_NOWAIT_FALLBACK`, `ROUTED_RETURN`, `ROUTED_NOWAIT_FALLBACK`,
`SPARSE_RETURN`, `SPARSE_DROP`, `OBSERVED_SPARSE_RETURN`,
`MATERIALIZE_RETURN`, `MATERIALIZE_DROP_ONE_DEFAULT`,
`MATERIALIZE_DROP_ONE_EXPLICIT`, `MATERIALIZE_DROP_ALL_DEFAULT`,
`MATERIALIZE_DROP_ALL_EXPLICIT`, `ENCODED_RETURN`,
`PROCESS_LOCAL_RETURN`, `PROCESS_LOCAL_NOWAIT_CACHED`,
`EMPTY_BYTES_RETURN`, `CONTROL_RETURN`, `TAINTED_RETURN`,
`EPHEMERAL_RETURN`, and `WRITE_REQUEST_RETURN`.

### Retention and invalidation cases

Every applicable decision row runs through both APIs with these orthogonal
retention cases. Counts are observed after publishing more than the bound:

| Retention case | Resolved plan bound | Native history | Python history/indexes | Replay and release assertion |
| --- | ---: | ---: | --- | --- |
| `none` | 0 | 0 | 0 | Replay count 0; no graph-owned retained payload/index after the write. A returned `publish` envelope remains caller-owned. |
| `latest` | 1 | 1 | Every applicable Python/index count is 1 | Replay count 1; replaced graph-owned payload released. |
| `bounded_three` | 3 | 3 | Every applicable Python/index count is 3 | Replay/history count 3; oldest graph-owned token/payload released on each overflow. |
| `native_only_sparse_three` | 3 | 3 | Python history, materialized payloads, and payload secondary indexes are 0 | Native replay count 3; native expiry/disposal owns release. |
| `process_local_three` | 3 | 3 | Process-local order, materialized payloads, process-local payload set, route-by-payload, and per-route payload set are each 3 | Old tokens are absent after overflow; all counts and tokens are 0 after `Graph.dispose()`. |
| `materializer_independent_bounds` | source 1, target A 3, target B 0 | 1, 3, 0 | 0 on sparse paths | Source and each target replay/release exactly their own policy. |

`latest_replay_policy="none"` is the supported public expression of a resolved
history bound of zero; a literal `RouteRetentionPolicy(history_limit=0)` remains
invalid input and has its own validation test.

The plan cache key is route identity plus topology, subscriber, materializer,
retention, and taint generations. The golden invalidation matrix mutates each
one independently and asserts a new immutable plan before the next publish:

| Mutation | Required invalidation evidence |
| --- | --- |
| Native route/topology registration or removal | `ROUTED_*` versus unrouted leaf and mode change before the next write. |
| Native materializer registration, removal, or target-count change | Return/drop and one/all materializer leaves change before the next write. |
| Source or target subscriber add/remove | Delivery versus drop leaf changes; late observer receives exactly subsequent events. |
| Retention reconfiguration | Compiled bound and owner/index counts change before the next write. |
| Route taint add/repair | Sparse/cached leaf changes and taint indexes rebuild/expire before the next write. |

Context membership, debug emission, write audit emission, producer identity, and
`control_epoch` are explicit per-call effects, not hidden plan-key state.
Producer identity selects the precompiled default/explicit materializer leaf;
control selects the precompiled fallback leaf. Context/debug/audit run around
the chosen execution without changing its native mode. Legacy lineage values
are discarded before selection and cannot create a plan, mode, cache key, or
retained index.

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

| Artifact | Input commit | SHA-256 | Verdict |
| --- | --- | --- | --- |
| `publish_plan_before_noisy_attempt_1.json` | `982a2eecc3588e6c8eacde4ff4e3eac4d020e393` | `d309e5e0476a6d661150a54ca67a11677f95ded3e2f4addfd3bb9d2fec108af9` | Semantics and provenance accepted. Performance rejected: every per-route first-publish row and seven warmed rows exceeded 10% RSD. Preserved as noisy evidence, never used for comparison. |
| `publish_plan_before.json` | Pending | Pending | A quiet-host run must satisfy the declared variance gate before M2/M3. |

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
- Manyfold PR #282 heads `4ae2d835cc6f462851eec7f23e4d6131c2bc0589`
  and `b137014594ccd5e1e31ed58c2712deb81db03d70` are blocked. The latter's race
  tests use private closure invocation and a monkeypatched `stream.latest`
  instead of real threaded close/dispose-versus-publish behavior. No M or Heart
  branch may compose over #282 until corrected production-faithful tests, a
  fresh dedicated verdict, and hosted green.
- The authoritative forward consumer gate is current Heart after #954 and the
  H work, pinned to the settled Manyfold stack. Record that SHA and focused
  command before M11–M14 are complete.
