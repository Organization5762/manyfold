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
| M1 | Golden table covers every row below, exact current branch/native call, stable decision/mode, replay/latest/payload ownership, inline delivery/backpressure, release, composite topology, and API parity | In progress |
| M2 | Frozen per-route `PublishPlan` contains the resolved route, encoder, numeric replay/latest/payload bounds, delivery, transitive topology/materializer dependencies, and native decision | Pending |
| M3 | `publish` and `publish_nowait` execute a current plan; relevant route-scoped topology, subscriber, taint, retention, and materializer changes invalidate it while unrelated churn preserves it | Pending |
| M4 | One explicit native publish interface replaces the Python-visible emit-method matrix and executes composite topology plus materializers without lost downstream work | Pending |
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

This remains a draft schema for the executable M1 table. The implementation
will define stable semantic `PublishMode` and `PublishDecisionLeaf` enums plus
frozen `PublishDecisionCase` rows in one source of truth. A separate
`CurrentPublishBranch` records the cache, fallback, and PyO3 overload selected
before M3–M5; those implementation names are forbidden from the future leaf
enum. Tests must assert exact enum coverage and execute every row through a
real `Graph`. Adding a mode or compiler match arm without a case must fail.

The canonical native interface has exactly these modes:

| `PublishMode` | Native contract |
| --- | --- |
| `ROUTED_ENVELOPES` | Execute the general composite graph: source topology, registered materializers, and downstream topology from every materialized target; return every emitted envelope in deterministic traversal order. |
| `SOURCE_ENVELOPE` | Accept one known-unrouted source and return its envelope. |
| `SOURCE_DROP` | Accept one known-unrouted source without constructing a Python envelope. |
| `MATERIALIZED_ENVELOPES` | For topology-isolated source and targets, accept one source, run all registered native materializers, and return source then targets in registration order. |
| `SINGLE_MATERIALIZED_DROP` | For topology-isolated source and sole target with no delivery/taint requirement, accept both without Python envelopes. |
| `ALL_MATERIALIZED_DROP` | For topology-isolated source and targets with no delivery/taint requirement, accept all without Python envelopes. |

The golden rows below name the existing native call sequence and the stable
semantic leaf/mode that replaces it. Rows marked as defects freeze evidence,
not the bug: their future contract intentionally differs. `Graph.` denotes the
native PyO3 graph, not the Python facade.

### Binding, raw, sparse, and routed leaves

| Case | API | Conditions and `PublishDecisionLeaf` | Current native call sequence | `PublishMode` | Golden state contract |
| --- | --- | --- | --- | --- | --- |
| `lifecycle_publish` | `publish` | `LifecycleBinding`; `LIFECYCLE_BINDING` | `Graph.emit(request)` and, only if unresolved, `Graph.emit(desired)` | `ROUTED_ENVELOPES` | Mutate lifecycle and write-binding registries before resolution; record every routed envelope; expire mirrors with route history. |
| `lifecycle_nowait` | `publish_nowait` | `LifecycleBinding`; `LIFECYCLE_BINDING` | Call Python `publish`, then the same `Graph.emit` sequence | `ROUTED_ENVELOPES` | Same registry mutations, native writes, delivery, and release; only the public return differs. |
| `write_binding_publish` | `publish` | `WriteBinding`; `WRITE_BINDING` | `Graph.emit(request)` and, only if unresolved, `Graph.emit(desired)` | `ROUTED_ENVELOPES` | Resolve/register the binding without adding a lifecycle entry; record all emitted routes. |
| `write_binding_nowait` | `publish_nowait` | `WriteBinding`; `WRITE_BINDING` | Call Python `publish`, then the same `Graph.emit` sequence | `ROUTED_ENVELOPES` | Same binding, writes, delivery, history, and payload release. |
| `raw_unrouted_publish` | `publish` | raw `RouteRef`; `RAW_UNROUTED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Eager Python byte mirror follows resolved history and is released on expiry/disposal. |
| `raw_unrouted_nowait_fallback` | `publish_nowait` | raw `RouteRef`; `RAW_UNROUTED` | Current branch `RAW_NOWAIT_VIA_PUBLISH`: call Python `publish`, then `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Intentionally materialize the internal return; parity covers latest, sequence, replay, and release without preserving the fallback as a future leaf. |
| `raw_routed_publish` | `publish` | raw route with native routing; `ROUTED_GRAPH` | Current branch `SINGLE_THEN_ROUTED`: `Graph.emit_single_if_unrouted` returns `None`, then `Graph.emit` | `ROUTED_ENVELOPES` | Record each emitted route once and retain each payload under that route's policy. |
| `raw_routed_nowait_fallback` | `publish_nowait` | raw route with native routing; `ROUTED_GRAPH` | Current branch `RAW_NOWAIT_VIA_PUBLISH`: Python `publish`; `Graph.emit_single_if_unrouted`, then `Graph.emit` | `ROUTED_ENVELOPES` | No routed output, subscriber delivery, or release may differ from `publish`. |
| `typed_sparse_publish` | `publish` | nonempty bytes, eligible route, no observer/materializer/control/taint; `SPARSE_UNOBSERVED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Native history owns the bytes; Python history and payload indexes remain zero. |
| `typed_sparse_nowait` | `publish_nowait` | same sparse conditions; `SPARSE_UNOBSERVED` | `Graph.emit_single_if_unrouted_drop` | `SOURCE_DROP` | Accepted sequence/latest/native replay equal `publish`; no Python envelope or payload allocation. |
| `typed_sparse_source_subscriber_publish` | `publish` | direct source subscriber; `SPARSE_OBSERVED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Native payload storage owns encoded bytes; a Python payload view exists only during one inline callback. |
| `typed_sparse_source_subscriber_nowait` | `publish_nowait` | direct source subscriber; `SPARSE_OBSERVED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Exactly one inline callback before return; reentrant publication is caller-backpressured and queue-free. |
| `typed_routed_publish` | `publish` | typed route with native routing; `ROUTED_GRAPH` | Current branch `SINGLE_THEN_ROUTED`: `Graph.emit_single_if_unrouted` returns `None`, then `Graph.emit` | `ROUTED_ENVELOPES` | Every routed envelope is recorded/delivered once under its own retention policy. |
| `typed_routed_nowait` | `publish_nowait` | typed route with native routing; `ROUTED_GRAPH` | Current branch `NOWAIT_SINGLE_THEN_PUBLISH`: `Graph.emit_single_if_unrouted` returns `None`; Python `publish` retries it, then calls `Graph.emit` | `ROUTED_ENVELOPES` | Same outputs and state as `publish`; M3 removes the duplicate native probe without preserving it as a leaf. |

### Native materializer leaves

| Case | API | Conditions and `PublishDecisionLeaf` | Current native call | `PublishMode` | Golden state and callback contract |
| --- | --- | --- | --- | --- | --- |
| `materializer_one_publish_unobserved` | `publish` | one target, no delivery subscribers; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Source and target native histories advance once; Python payload indexes remain zero. |
| `materializer_many_publish_unobserved` | `publish` | two targets, no delivery subscribers; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Source and both targets advance once in registration order; no Python delivery. |
| `materializer_explicit_producer_publish` | `publish` | one target, explicit producer; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Source envelope/writer use the supplied producer; derived target envelope and audit writer both use authoritative producer `python`. |
| `materializer_one_nowait_default_producer` | `publish_nowait` | one target, no delivery, default producer; `MATERIALIZE_DROP_ONE` | Current branch `DROP_ONE_PYTHON`: `Graph.emit_single_if_unrouted_and_materializer_drop_python` | `SINGLE_MATERIALIZED_DROP` | Source/target native retention advances once; no returned or retained Python envelope. |
| `materializer_one_nowait_explicit_producer` | `publish_nowait` | one target, no delivery, explicit producer; `MATERIALIZE_DROP_ONE` | Current branch `DROP_ONE_KWARGS`: `Graph.emit_single_if_unrouted_and_materializer_drop` | `SINGLE_MATERIALIZED_DROP` | Fix current disagreement: source envelope/writer use the supplied producer; derived target envelope and audit writer both use authoritative producer `python` (the current audit writer incorrectly records the supplied producer). |
| `materializer_many_nowait_default_producer` | `publish_nowait` | two targets, no delivery, default producer; `MATERIALIZE_DROP_ALL` | Current branch `DROP_ALL_PYTHON`: `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop_python` | `ALL_MATERIALIZED_DROP` | Source and both targets advance once; Python envelope/payload indexes remain zero. |
| `materializer_many_nowait_explicit_producer` | `publish_nowait` | two targets, no delivery, explicit producer; `MATERIALIZE_DROP_ALL` | Current branch `DROP_ALL_KWARGS`: `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop` | `ALL_MATERIALIZED_DROP` | Fix the same current writer mismatch: source uses the supplied producer; every derived envelope and audit writer consistently uses `python`; no Python delivery. |
| `materializer_source_delivery` | both | one target; source subscriber only; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Callback order is `[source]`; count one; target history still advances. |
| `materializer_target_delivery` | both | one target; target subscriber only; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Callback order is `[target]`; count one; source history still advances. |
| `materializer_source_and_target_delivery` | both | one target; source and target subscribers; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Callback order is `[source, target]`; each count one; callbacks finish before return. |
| `materializer_many_delivery_order` | both | two targets; subscribers on source and both targets; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Callback order is source then targets in registration order; each count one. |
| `materializer_control` | both | materializer present and `control_epoch` supplied; `MATERIALIZE_RETURN` | Defect branch `CONTROL_SOURCE_ONLY`: `Graph.emit_single_if_unrouted`, currently leaving the target stale | `MATERIALIZED_ENVELOPES` | Future source and every target advance exactly once with control metadata; `publish`/`publish_nowait` state and delivery are equal. |
| `materializer_source_routed_publish` | `publish` | source has outgoing topology and one materializer; `COMPOSITE_GRAPH` | Defect branch: `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` returns `None`, then `Graph.emit`; routed target advances but materialized target stays stale | `ROUTED_ENVELOPES` | Source topology and materialized target both advance once in deterministic order. |
| `materializer_source_routed_nowait` | `publish_nowait` | one target, default producer, same composite source; `COMPOSITE_GRAPH` | Defect sequence: `Graph.emit_single_if_unrouted_and_materializer_drop_python` returns false; `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` and `Graph.emit_single_if_unrouted` return `None`; Python `publish` retries the materializer call, then reaches `Graph.emit`; materialized target stays stale | `ROUTED_ENVELOPES` | Exact state/delivery parity with composite `publish`, with no duplicate probe. |
| `materialized_target_routed_publish` | `publish` | materialized target has outgoing topology; `COMPOSITE_GRAPH` | Defect branch `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` advances source/target but leaves downstream stale | `ROUTED_ENVELOPES` | Source, materialized target, and its downstream route each advance once. |
| `materialized_target_routed_nowait` | `publish_nowait` | one target, default producer, same target topology; `COMPOSITE_GRAPH` | Defect `Graph.emit_single_if_unrouted_and_materializer_drop_python` returns true but leaves downstream stale | `ROUTED_ENVELOPES` | Same composite traversal and state as `publish`; drop modes are unsafe. |
| `materializer_source_tainted` | both | source route tainted; `MATERIALIZE_RETURN` | Defect branch `Graph.emit_single_if_unrouted` advances only source | `MATERIALIZED_ENVELOPES` | Source and targets advance; source taint propagates according to materializer policy and bounded indexes. |
| `materializer_target_tainted_publish` | `publish` | sole materialized target tainted; `MATERIALIZE_RETURN` | `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers` | `MATERIALIZED_ENVELOPES` | Source/target advance once and target taint propagation/index ownership matches the declared policy. |
| `materializer_target_tainted` | `publish_nowait` | sole materialized target tainted, default producer; `MATERIALIZE_RETURN` | Defect `Graph.emit_single_if_unrouted_and_materializer_drop_python` is selected without consulting target taint | `MATERIALIZED_ENVELOPES` | Target taint is observed/propagated before delivery; drop modes are forbidden. |
| `materializer_one_lineage_discarded_nowait` | `publish_nowait` | one target, every legacy lineage input populated; `MATERIALIZE_DROP_ONE` | After discard, `Graph.emit_single_if_unrouted_and_materializer_drop_python`; no ID-specialized call | `SINGLE_MATERIALIZED_DROP` | State equals no-lineage case and lineage/correlation indexes remain zero. |
| `materializer_many_lineage_discarded_nowait` | `publish_nowait` | two targets, every legacy lineage input populated; `MATERIALIZE_DROP_ALL` | After discard, `Graph.emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop_python`; no ID-specialized call | `ALL_MATERIALIZED_DROP` | State equals no-lineage case and lineage/correlation indexes remain zero. |

### Encoded, process-local, and sparse-disable leaves

| Case | API | Conditions and `PublishDecisionLeaf` | Current native call | `PublishMode` | Golden ownership and invalidation contract |
| --- | --- | --- | --- | --- | --- |
| `typed_encoded_publish` | `publish` | non-process-local encoder; `ENCODED_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Native payload storage owns encoded bytes. Python owns bounded envelope/history secondary indexes; `_materialized_payloads` exists only during observed delivery. |
| `typed_encoded_nowait` | `publish_nowait` | non-process-local encoder; `ENCODED_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Same native byte ownership, Python envelope/index counts, replay, delivery, and release as `publish`. |
| `process_local_publish_unobserved` | `publish` | `Schema.any()`, no observer; `PROCESS_LOCAL_UNOBSERVED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Original object identity is retained; expiry/disposal releases its token and every secondary index. |
| `process_local_publish_observed` | `publish` | `Schema.any()`, existing observer; `PROCESS_LOCAL_OBSERVED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Exactly one inline identity-preserving callback; bounded token/index ownership is otherwise equal to unobserved publish. |
| `process_local_nowait_unobserved` | `publish_nowait` | `Schema.any()`, no observer/taint/control; `PROCESS_LOCAL_UNOBSERVED` | Current branch `PROCESS_LOCAL_NOWAIT_CACHE`: `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Resolved latest/payload bound applies equally to token order, payload set, route-by-payload, and per-route payload set; all release on disposal. The cache name is not a future leaf. |
| `process_local_nowait_late_observer` | `publish_nowait` | observer added after an unobserved publish; `PROCESS_LOCAL_OBSERVED` | Current cache is invalidated, then `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Route-local subscriber dependency selects the observed leaf before the next write; one inline identity-preserving callback; every index remains at its resolved bound. |
| `process_local_nowait_observed` | `publish_nowait` | existing observer; `PROCESS_LOCAL_OBSERVED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Observed envelope path, identity-preserving callback, bounded token ownership, complete disposal. |
| `process_local_nowait_tainted` | `publish_nowait` | route tainted; `TAINTED_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Route-local taint dependency invalidates the unobserved leaf; history, token indexes, and taint secondary indexes expire together. |
| `process_local_nowait_control` | `publish_nowait` | `control_epoch` supplied; `CONTROLLED_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Per-call control selection bypasses the unobserved leaf; general history and token release apply. |
| `empty_bytes_publish` | `publish` | eligible typed bytes encode to `b\"\"`; `EMPTY_PAYLOAD` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Python envelope history follows the resolved bound; sparse native-only ownership is disabled. |
| `empty_bytes_nowait` | `publish_nowait` | eligible typed bytes encode to `b\"\"`; `EMPTY_PAYLOAD` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Same history and release as `publish`; `SOURCE_DROP` is forbidden. |
| `control_epoch_publish` | `publish` | eligible typed bytes with control epoch; `CONTROLLED_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Validate control epoch, retain metadata/history, and bypass sparse ownership. |
| `control_epoch_nowait` | `publish_nowait` | same; `CONTROLLED_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Exact state parity and no sparse/materializer drop selection. |
| `tainted_publish` | `publish` | eligible typed bytes on tainted route; `TAINTED_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Python history and taint indexes share one bound and disposal path. |
| `tainted_nowait` | `publish_nowait` | same; `TAINTED_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Route-local taint invalidation precedes the write; exact parity with `publish`. |
| `ephemeral_publish` | `publish` | `Layer.Ephemeral`; `EPHEMERAL_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Replay history bound is 0 while latest envelope/payload bound is 1. `latest` remains readable; replay is empty; replaced payload/index ownership is released. |
| `ephemeral_nowait` | `publish_nowait` | `Layer.Ephemeral`; `EPHEMERAL_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Same 0 replay/1 latest-payload ownership and release; the internal return is discarded and sparse drop remains forbidden. |
| `write_request_publish` | `publish` | `Plane.Write` and `Variant.Request`; `WRITE_REQUEST_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Guarded-write metadata/retention applies through the general path. |
| `write_request_nowait` | `publish_nowait` | same; `WRITE_REQUEST_SOURCE` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Same validation, writer, delivery, retention, and release. |
| `lineage_arguments_discarded_publish` | `publish` | every legacy lineage input populated on a sparse route; `SPARSE_UNOBSERVED` | `Graph.emit_single_if_unrouted` | `SOURCE_ENVELOPE` | Result equals the no-lineage case; native lineage/correlation counts remain zero. |
| `lineage_arguments_discarded_nowait` | `publish_nowait` | every legacy lineage input populated on a sparse route; `SPARSE_UNOBSERVED` | `Graph.emit_single_if_unrouted_drop` | `SOURCE_DROP` | No ID-specialized call or mode exists; M5 deletes those unreachable native branches. |

The exact `PublishDecisionLeaf` set is therefore:
`LIFECYCLE_BINDING`, `WRITE_BINDING`, `RAW_UNROUTED`, `ROUTED_GRAPH`,
`COMPOSITE_GRAPH`, `SPARSE_UNOBSERVED`, `SPARSE_OBSERVED`,
`MATERIALIZE_RETURN`, `MATERIALIZE_DROP_ONE`, `MATERIALIZE_DROP_ALL`,
`ENCODED_SOURCE`, `PROCESS_LOCAL_UNOBSERVED`, `PROCESS_LOCAL_OBSERVED`,
`EMPTY_PAYLOAD`, `CONTROLLED_SOURCE`, `TAINTED_SOURCE`,
`EPHEMERAL_SOURCE`, and `WRITE_REQUEST_SOURCE`.

`CurrentPublishBranch` is a diagnostic string in M1 evidence, not a future
compiler enum. The table names `RAW_NOWAIT_VIA_PUBLISH`,
`SINGLE_THEN_ROUTED`, `NOWAIT_SINGLE_THEN_PUBLISH`,
`PROCESS_LOCAL_NOWAIT_CACHE`, `DROP_ONE_PYTHON`, `DROP_ONE_KWARGS`,
`DROP_ALL_PYTHON`, and `DROP_ALL_KWARGS`; instrumentation also proves which
legacy profile/ID-specialized native method was not selected. M3–M5 tests prove
all such branches disappear; none may leak into `PublishDecisionLeaf`.

### Retention and invalidation cases

`PublishPlan` resolves replay history separately from latest-envelope and
payload/index ownership. Python owners consume numeric bounds; they never
interpret `None` as unbounded:

| Retention case | Replay history bound | Latest/payload bound | Native counts after overflow | Python counts after overflow | Replay/latest/release assertion |
| --- | ---: | ---: | --- | --- | --- |
| `none_encoded` | 0 | 1 | replay 0, latest 1, payload 1 | replay history 0, latest envelope/index 1, temporary materialized payload 0 | Replay empty; `latest` decodes newest bytes; replaced graph-owned indexes release. |
| `none_process_local` | 0 | 1 | replay 0, latest 1, payload 1 | replay history 0; latest envelope, token order/set, route-by-payload, and per-route payload set each 1 | Replay empty; `latest` preserves newest identity; prior token releases on every write and final token on disposal. |
| `latest` | 1 | 1 | replay/latest/payload 1 | Every applicable history/index count 1 | Replay count 1; replaced graph-owned payload/token releases. |
| `bounded_three` | 3 | 3 | replay 3, latest 1, payload 3 | Every applicable replay/index count 3 | Replay/history count 3; oldest graph-owned token/payload releases on overflow. |
| `native_only_sparse_none` | 0 | 1 | replay 0, latest 1, payload 1 | Python replay history, materialized payloads, and payload secondary indexes 0 | Native latest remains readable while replay is empty; native replacement/disposal owns release. |
| `native_only_sparse_three` | 3 | 3 | replay 3, latest 1, payload 3 | Python replay history, materialized payloads, and payload secondary indexes 0 | Native expiry/disposal exclusively owns release. |
| `materializer_independent_bounds` | source 1, target A 3, target B 0 | source 1, target A 3, target B 1 | replay 1, 3, 0; payload 1, 3, 1 | 0 on sparse paths | Source and every target replay/latest/release exactly their own policy. |

The executable table also freezes these current defects as failing acceptance
gates, not future behavior:

| Current defect probe | Observed current state | Required resolved state |
| --- | --- | --- |
| Default Ephemeral bytes, 3 writes | Native `history_limit=0`, replay 0, latest seq 3, payload 1; Python `_history=1`, per-route payload IDs 1; `latest=b"c"`, replay 0 | Represent replay 0 and latest/payload 1 as separate owners; public replay remains empty and latest remains readable. |
| Public `latest_replay_policy="none"` on encoded Logical route, 5 writes | Resolved `history_limit=None`; native replay 0/payload 1; Python history 5 and per-route payload IDs 5 | Python replay history 0 and latest/index ownership exactly 1. |
| Same public override on `Schema.any()`, 5 writes | Native replay 0/payload 1; Python history, payload IDs, materialized values, and process-local tokens each 5 | Python replay history 0; every latest/token secondary index exactly 1; old tokens released and disposal returns every count to 0. |
| Default Ephemeral `Schema.any()` | Replay is 0 but one latest process-local token remains owned | Preserve exactly one latest token, release its predecessor on write and itself on disposal. |

A literal `RouteRetentionPolicy(history_limit=0)` remains invalid public input
and has a separate validation case; it is not the representation used by the
resolved numeric replay bound.

The plan key is route identity plus route-scoped dependency versions. A
materializer plan depends on the source and every target's topology,
subscriptions, retention, and taint versions. The golden matrix asserts both
positive and negative invalidation:

| Mutation | Required plan evidence |
| --- | --- |
| Source topology registration/removal | Isolated versus `ROUTED_GRAPH`/`COMPOSITE_GRAPH` leaf changes before the next write. |
| Any materialized target topology registration/removal | Drop/return becomes `COMPOSITE_GRAPH` before the next write; downstream output is never skipped. |
| Materializer registration/removal or target-count change | No/one/all materializer leaf and mode change before the next write. |
| Relevant source subscriber add/remove | Delivery versus drop leaf changes; a late observer receives exactly subsequent events. |
| Relevant target subscriber add/remove | Drop becomes envelope return before the next write; target callback count/order is exact. |
| Source or target retention reconfiguration | The affected route's replay/latest/payload numeric bounds change before the next write. |
| Source taint add/repair | Drop becomes envelope return and source taint indexes rebuild/expire before the next write. |
| Any target taint add/repair | Drop becomes envelope return and target taint propagation/indexes update before the next write. |
| Unrelated route topology or materializer mutation | Existing plan object and all dependency versions remain current. |
| Unrelated route subscriber churn | Existing plan object remains current; no global subscriber generation recompiles it. |
| Unrelated route retention reconfiguration | Existing plan object and bounds remain current. |
| Unrelated route taint add/repair | Existing plan object remains current; no global taint generation recompiles it. |

Context membership, debug emission, write audit emission, producer identity, and
`control_epoch` are explicit per-call effects, not hidden plan-key state.
Producer identity is an argument to the selected stable leaf, never a separate
leaf or PyO3 overload contract. Control selects the precompiled controlled or
materializer-return semantic leaf. Context/debug/audit run around the chosen
execution without changing its native mode. Legacy lineage values are
discarded before selection and cannot create a plan, mode, cache key, or
retained index.

## Performance evidence

The checked-in benchmark runs real `Graph` paths from
`manyfold.private.profiling.publish_benchmarks`. It reports an unmeasured
preflight, per-route first-publish cost, a warmed steady-state loop, end-to-end
time, all run results, variance, final-state equality, and repository/native
provenance.

The first-publish metric is named `per_route_first_publish`: the unmeasured
preflight warms process and native globals, while each measured value uses a
mean of 64 fresh `Graph`/fresh-route samples. Each sample times only its first
publish; setup, verification, disposal, and process-local cleanup are outside
the hot timer. The reported RSD compares seven run means and the artifact keeps
all 448 raw samples per workload. Formal CLI runs reject a dirty worktree.
Provenance is sampled before the output artifact is created, and includes the
loaded native extension's SHA-256 digest.

Frozen before/after command:

```sh
uv run python -m manyfold.private.profiling.publish_benchmarks \
  --first-publish-samples 64 \
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
| `publish_plan_before_noisy_attempt_2.json` | `febb45b39666db7c052d963e921a2bb2e3890f76` | `8c3e0be8944bfe8b4719e5b15dd201b58f714f383f53562f00d958abfe739727` | Semantics and provenance accepted. Performance rejected: all ten single-sample per-route first-publish rows exceeded 10% RSD; three warmed rows also failed. Preserved as the evidence that required repeated fresh-route samples. |
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

`manyfold.stream` stably exports `PubSubObservable`, structural
`Subscribable[T]`, and structural `Subscription`. The canonical signatures are
`Subscribable.subscribe(..., *, replay_latest: bool = False) -> Subscription`,
`Subscription.dispose() -> bool | None`, and
`PubSubObservable.from_source(source)`. The adapter forwards each downstream
subscription's replay choice to the Graph-owned route source returned by
`Graph.observe(route)`; it is not implemented through `merge`, a `latest()`
peek, or a private subscribe factory. Heart-shaped tests must prove:

- replay-enabled and replay-disabled subscribers coexist on one source created
  once; only the enabled subscriber replays and both receive future values once;
- disposal followed by resubscription is independent;
- subscriber ownership/count returns to zero after independent disposal;
- synchronous reentrant publication preserves the documented order;
- error and completion signals propagate once;
- operators return the canonical observable/subscribable types.
- an independently declared Heart-shaped source/subscription typechecks without
  importing/re-exporting Manyfold protocols, casts, or ignores.

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
- Manyfold PR #282 head
  `fa1d1336c504a8f6f5ded57e4b0a60e7570b44ea` is candidate-only pending a
  dedicated supervisor verdict and hosted CI. Earlier heads `4ae2d835` and
  `b1370145` are blocked/stale. No M or Heart branch may compose over #282
  without the new exact verdict.
- Published #281 remains `3c62dd1`; unpublished `a92efa1` is blocked on
  STARTING/start-close races and leaked worker, subscription, and name-registry
  state. PR3 remains independent and must record exact base/head ancestry; no
  Heart pin guidance is valid yet.
- The authoritative forward consumer gate is current Heart after #954 and the
  H work, pinned to the settled Manyfold stack. Record that SHA and focused
  command before M11–M14 are complete.
