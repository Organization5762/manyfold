"""Dependency-first catalog of distributed-systems legos.

The catalog is intentionally broader than the components implemented by
Manyfold today. It is a queryable map of conceptual building blocks so examples,
docs, and future components can talk about the same dependency graph.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from operator import attrgetter


@dataclass(frozen=True)
class Lego:
    """One conceptual distributed-systems building block."""

    name: str
    role: str
    layer: str
    contract: str
    requires: tuple[str, ...] = ()
    provides: tuple[str, ...] = ()
    knobs: tuple[str, ...] = ()


_LEGO_NAME_KEY = attrgetter("name")

_LEGOS = (
    # Atoms.
    Lego("Bytes", "data", "atom", "Opaque byte payload."),
    Lego("Key", "data", "atom", "Stable address for data or work."),
    Lego("NodeId", "identity", "atom", "Stable identity for one node."),
    Lego("ServiceId", "identity", "atom", "Stable identity for one service."),
    Lego("TenantId", "identity", "atom", "Stable identity for one tenant."),
    Lego("PrincipalId", "identity", "atom", "Stable identity for an actor."),
    Lego("RequestId", "identity", "atom", "Stable identity for one request."),
    Lego("MessageId", "identity", "atom", "Stable identity for one message."),
    Lego("CorrelationId", "observability", "atom", "Groups related events."),
    Lego("CausalityId", "observability", "atom", "Names a causal chain."),
    Lego("TraceId", "observability", "atom", "Names a trace."),
    Lego("SpanId", "observability", "atom", "Names one trace span."),
    Lego("Token", "security", "atom", "Bearer or capability token."),
    Lego("Version", "ordering", "atom", "Version attached to state."),
    Lego("Generation", "ordering", "atom", "Incarnation of an owner or map."),
    Lego("Epoch", "ordering", "atom", "Ordered control-plane generation."),
    Lego("Term", "ordering", "atom", "Consensus election generation."),
    Lego("SequenceNumber", "ordering", "atom", "Monotonic sequence within a scope."),
    Lego("Offset", "ordering", "atom", "Position within a log or stream."),
    Lego("Timestamp", "time", "atom", "Time value from a clock."),
    Lego("Duration", "time", "atom", "Length of time."),
    Lego("Deadline", "time", "atom", "Absolute time boundary."),
    Lego("TTL", "time", "atom", "Relative validity duration."),
    Lego("Priority", "flow", "atom", "Relative work importance."),
    Lego("Capacity", "flow", "atom", "Bound on stored or active work."),
    Lego("Cost", "flow", "atom", "Work cost for admission decisions."),
    Lego("Hash", "data", "atom", "Stable digest used for routing or integrity."),
    Lego("Checksum", "data", "atom", "Digest for integrity checks."),
    Lego("Status", "observability", "atom", "Current condition of a component."),
    Lego("Error", "recovery", "atom", "Failure value or exception."),
    Lego("Result", "compute", "atom", "Success or failure outcome."),
    # Policies.
    Lego("RetryPolicy", "recovery", "policy", "Describes retry attempt limits."),
    Lego("BackoffPolicy", "recovery", "policy", "Describes delay between retries."),
    Lego("TimeoutPolicy", "time", "policy", "Describes when an operation expires."),
    Lego("CancellationPolicy", "compute", "policy", "Describes when work is canceled."),
    Lego("OrderingPolicy", "ordering", "policy", "Describes event ordering guarantees."),
    Lego("OverflowPolicy", "flow", "policy", "Describes behavior when capacity is full."),
    Lego("RetentionPolicy", "persistence", "policy", "Describes how long state is kept."),
    Lego("DurabilityPolicy", "persistence", "policy", "Describes persistence strength."),
    Lego("ConsistencyPolicy", "coordination", "policy", "Describes read/write agreement."),
    Lego("ReplicationPolicy", "replication", "policy", "Describes copy behavior."),
    Lego("PartitionPolicy", "partitioning", "policy", "Describes keyspace splitting."),
    Lego("RoutingPolicy", "routing", "policy", "Describes destination selection."),
    Lego("PlacementPolicy", "routing", "policy", "Describes owner placement."),
    Lego("AdmissionPolicy", "flow", "policy", "Describes whether work may enter."),
    Lego("RatePolicy", "flow", "policy", "Describes allowed rate and burst."),
    Lego("ConcurrencyPolicy", "flow", "policy", "Describes active-work bounds."),
    Lego("PriorityPolicy", "flow", "policy", "Describes priority handling."),
    Lego("FairnessPolicy", "flow", "policy", "Describes sharing across identities."),
    Lego("SecurityPolicy", "security", "policy", "Describes security rules."),
    Lego("AuthPolicy", "security", "policy", "Describes authentication rules."),
    Lego("IsolationPolicy", "security", "policy", "Describes blast-radius boundaries."),
    Lego("CompactionPolicy", "persistence", "policy", "Describes state compaction."),
    Lego("ConflictPolicy", "replication", "policy", "Describes conflict resolution."),
    # Properties.
    Lego("Idempotent", "property", "property", "Repeated application has one effect."),
    Lego("Commutative", "property", "property", "Order does not change the result."),
    Lego("Associative", "property", "property", "Grouping does not change the result."),
    Lego("Deterministic", "property", "property", "Same input produces same output."),
    Lego("Monotonic", "property", "property", "State only advances."),
    Lego("Atomic", "property", "property", "Effect is all-or-nothing."),
    Lego("Durable", "property", "property", "State survives crashes."),
    Lego("Replayable", "property", "property", "History can be replayed."),
    Lego("Observable", "property", "property", "Behavior can be inspected."),
    Lego("Ordered", "property", "property", "Events have a meaningful order."),
    Lego("Causal", "property", "property", "Cause and effect are tracked."),
    Lego("Bounded", "property", "property", "Resource use has a limit."),
    Lego("Authenticated", "property", "property", "Identity is proven."),
    Lego("Authorized", "property", "property", "Action is allowed."),
    Lego("Encrypted", "property", "property", "Data is confidential on a boundary."),
    Lego("Fenced", "property", "property", "Stale owners cannot act."),
    Lego("Exclusive", "property", "property", "Only one owner can act at a time."),
    Lego("AtMostOnce", "property", "property", "Effect happens zero or one times."),
    Lego("AtLeastOnce", "property", "property", "Effect happens one or more times."),
    Lego("ExactlyOnceEffect", "property", "property", "Externally visible effect happens once."),
    Lego("BestEffort", "property", "property", "No durable delivery guarantee."),
    # Capabilities.
    Lego("Read", "capability", "capability", "Read one value."),
    Lego("Write", "capability", "capability", "Write one value."),
    Lego("Delete", "capability", "capability", "Delete one value."),
    Lego("Scan", "capability", "capability", "Read values by range or prefix."),
    Lego("List", "capability", "capability", "List names or keys."),
    Lego("Append", "capability", "capability", "Append one ordered record."),
    Lego("CompareAndSwap", "capability", "capability", "Write if version matches."),
    Lego("Publish", "capability", "capability", "Emit one event."),
    Lego("Subscribe", "capability", "capability", "Observe future events."),
    Lego("Ack", "capability", "capability", "Accept responsibility for an item."),
    Lego("Nack", "capability", "capability", "Reject or requeue an item."),
    Lego("Reserve", "capability", "capability", "Temporarily claim a resource."),
    Lego("Release", "capability", "capability", "Give up a claim."),
    Lego("Renew", "capability", "capability", "Extend a claim."),
    Lego("Cancel", "capability", "capability", "Stop scheduled or active work."),
    Lego("Encode", "capability", "capability", "Turn a value into bytes."),
    Lego("Decode", "capability", "capability", "Turn bytes into a value."),
    Lego("Sign", "capability", "capability", "Attach authenticity proof."),
    Lego("Verify", "capability", "capability", "Check authenticity proof."),
    # Local runtime.
    Lego("Schema", "data", "local", "Typed encode/decode contract.", ("Encode", "Decode")),
    Lego("Envelope", "communication", "local", "Closed event with identity and payload metadata.", ("Schema", "MessageId", "SequenceNumber")),
    Lego("Clock", "time", "local", "Returns current time.", ("Timestamp",), ("Timestamp",)),
    Lego("ManualClock", "time", "local", "Deterministic test clock.", ("Clock",)),
    Lego("SystemClock", "time", "local", "Wall-clock-backed clock.", ("Clock",)),
    Lego("Timer", "time", "local", "Fires after a duration or at a deadline.", ("Clock", "Deadline", "Duration")),
    Lego("Timeout", "time", "local", "Fails work after a timeout.", ("Timer", "TimeoutPolicy")),
    Lego("Cancellation", "compute", "local", "Stops work when cancellation is requested.", ("CancellationPolicy", "Cancel")),
    Lego("Backoff", "recovery", "local", "Computes retry delay.", ("BackoffPolicy", "Clock")),
    Lego("RetryLoop", "recovery", "local", "Retries local work according to policy.", ("RetryPolicy", "BackoffPolicy", "Timeout", "Cancellation")),
    Lego("SequenceCounter", "ordering", "local", "Assigns monotonic local sequence numbers.", ("SequenceNumber",)),
    Lego("Buffer", "flow", "local", "Temporarily stores items in memory.", ("Capacity",)),
    Lego("RingBuffer", "flow", "local", "Bounded cyclic staging buffer.", ("Buffer", "OverflowPolicy", "OrderingPolicy")),
    Lego("Queue", "handoff", "local", "Orders items for later consumption.", ("Buffer", "OrderingPolicy")),
    Lego("BoundedQueue", "flow", "local", "Queue with explicit capacity.", ("Queue", "Capacity", "OverflowPolicy")),
    Lego("TokenBucket", "flow", "local", "Tracks rate and burst credits.", ("Clock", "RatePolicy")),
    Lego("Semaphore", "flow", "local", "Bounds concurrent holders.", ("Capacity", "ConcurrencyPolicy")),
    Lego("RateLimiter", "flow", "local", "Allows, delays, or rejects work by rate.", ("TokenBucket", "Cost")),
    Lego("Watchdog", "observability", "local", "Emits status when expected events stop.", ("Timer", "Status")),
    Lego("Sampler", "compute", "local", "Selects values according to a sampling policy.", ("Clock", "OrderingPolicy")),
    Lego("ChangeFilter", "flow", "local", "Suppresses unchanged values.", ("Deterministic",)),
    Lego("ThresholdFilter", "flow", "local", "Suppresses changes below a numeric threshold.", ("ChangeFilter",)),
    Lego("DelimitedMessageBuffer", "communication", "local", "Reassembles delimited byte or text messages.", ("Buffer", "Bytes")),
    Lego("JsonEventDecoder", "communication", "local", "Decodes JSON messages into sensor events.", ("Schema", "SensorEvent", "SequenceCounter")),
    Lego("DoubleBuffer", "flow", "local", "Alternates active and ready buffers for bursty capture.", ("Buffer", "Capacity")),
    Lego("FrameAssembler", "communication", "local", "Aggregates samples into complete frames.", ("SequenceNumber",)),
    Lego("XorChecksum", "data", "local", "Computes a small XOR checksum over bytes.", ("Bytes", "Checksum")),
    Lego("RouteRef", "communication", "local", "Typed route identity.", ("Schema",)),
    Lego("PortRef", "communication", "local", "Typed endpoint identity.", ("RouteRef",)),
    Lego("Graph", "communication", "local", "Connects typed routes and operators.", ("RouteRef", "Envelope", "Publish", "Subscribe")),
    Lego("Mailbox", "handoff", "local", "Bounded graph-visible async boundary.", ("BoundedQueue", "RouteRef", "Ack")),
    Lego("Pipe", "communication", "local", "Moves events between source and route.", ("RouteRef", "Publish", "Subscribe")),
    Lego("Bridge", "communication", "local", "Connects routes across an adapter boundary.", ("Pipe", "Schema")),
    Lego("PubSub", "communication", "local", "Broadcasts events to subscribers.", ("RouteRef", "Envelope")),
    # Durable.
    Lego("ByteStore", "persistence", "durable", "Durably stores bytes by key.", ("Key", "Bytes", "Read", "Write", "Delete")),
    Lego("Keyspace", "persistence", "durable", "Scoped structured prefix over a byte store.", ("ByteStore", "Key")),
    Lego("ObjectStore", "persistence", "durable", "Durably stores large objects.", ("ByteStore", "RetentionPolicy")),
    Lego("BlobStore", "persistence", "durable", "Stores opaque byte blobs.", ("ObjectStore", "Checksum")),
    Lego("DurableCell", "persistence", "durable", "Durable single value.", ("ByteStore", "DurabilityPolicy")),
    Lego("VersionedRegister", "persistence", "durable", "Durable value with compare-and-swap version.", ("DurableCell", "Version", "CompareAndSwap")),
    Lego("WriteAheadLog", "persistence", "durable", "Appends intent before applying state.", ("Keyspace", "Append", "SequenceNumber", "DurabilityPolicy")),
    Lego("EventLog", "persistence", "durable", "Typed append-only durable event history.", ("WriteAheadLog", "Envelope", "Offset")),
    Lego("SnapshotStore", "persistence", "durable", "Durable latest state image.", ("Keyspace", "Schema")),
    Lego("CheckpointStore", "recovery", "durable", "Durable restart position.", ("SnapshotStore", "Offset")),
    Lego("MetadataStore", "configuration", "durable", "Durable control-plane facts.", ("Keyspace", "VersionedRegister")),
    Lego("IdempotencyStore", "recovery", "durable", "Remembers completed request IDs.", ("Keyspace", "RequestId")),
    Lego("DeduplicationStore", "recovery", "durable", "Remembers seen message IDs.", ("Keyspace", "MessageId")),
    Lego("TombstoneStore", "replication", "durable", "Remembers deletes across repair.", ("Keyspace", "RetentionPolicy")),
    Lego("MaterializedView", "persistence", "durable", "Queryable projection from event history.", ("EventLog", "SnapshotStore")),
    # Communication and handoff.
    Lego("AckTracker", "handoff", "local", "Tracks ack, nack, and visibility deadlines.", ("Ack", "Nack", "Deadline", "IdempotencyStore")),
    Lego("VisibilityDeadline", "handoff", "local", "Time boundary for a claimed item.", ("Deadline", "TTL")),
    Lego("DurableQueue", "handoff", "durable", "Durably accepts work and later delivers it.", ("EventLog", "CheckpointStore", "AckTracker", "VisibilityDeadline", "RetryLoop")),
    Lego("PriorityQueue", "handoff", "local", "Queue ordered by priority.", ("Queue", "Priority", "PriorityPolicy")),
    Lego("DelayQueue", "handoff", "local", "Queue that releases items after time.", ("Queue", "Timer")),
    Lego("WorkQueue", "handoff", "durable", "Queue specialized for executable work.", ("DurableQueue", "IdempotencyStore")),
    Lego("RetryQueue", "recovery", "durable", "Durably stores work waiting for retry.", ("DurableQueue", "Backoff")),
    Lego("DeadLetterQueue", "recovery", "durable", "Stores work that cannot be completed.", ("DurableQueue", "Error")),
    Lego("Outbox", "handoff", "durable", "Durably records outgoing side effects.", ("EventLog", "IdempotencyStore")),
    Lego("Inbox", "handoff", "durable", "Durably records incoming side effects.", ("EventLog", "DeduplicationStore")),
    Lego("ConsumerGroup", "handoff", "distributed", "Shares queue partitions across consumers.", ("Membership", "CheckpointStore", "PartitionMap")),
    # Distributed.
    Lego("Heartbeat", "membership", "distributed", "Periodically signals liveness.", ("Timer", "NodeId")),
    Lego("FailureDetector", "membership", "distributed", "Suspects nodes after missed heartbeats.", ("Heartbeat", "Timeout")),
    Lego("Membership", "membership", "distributed", "Tracks known live or suspected nodes.", ("NodeId", "FailureDetector")),
    Lego("FencingToken", "coordination", "distributed", "Monotonic token preventing stale owners.", ("SequenceNumber",)),
    Lego("Lease", "coordination", "distributed", "Grants temporary ownership.", ("Deadline", "Renew", "Release", "FencingToken", "VersionedRegister")),
    Lego("Quorum", "coordination", "distributed", "Requires enough members to agree.", ("Membership", "ConsistencyPolicy")),
    Lego("LeaderElection", "coordination", "distributed", "Selects one leader for a term.", ("Membership", "Quorum", "Term", "Timer")),
    Lego("Transport", "communication", "distributed", "Moves messages between nodes.", ("Envelope", "TimeoutPolicy")),
    Lego("Consensus", "coordination", "distributed", "Agrees on critical facts.", ("EventLog", "SnapshotStore", "Membership", "Quorum", "LeaderElection", "Term", "Timer", "Transport")),
    Lego("ReplicatedLog", "replication", "distributed", "Copies an ordered log across replicas.", ("Consensus", "EventLog")),
    Lego("ReplicaSet", "replication", "distributed", "Names a group of replicas.", ("Membership", "ReplicationPolicy")),
    Lego("Replicator", "replication", "distributed", "Copies state or work between replicas.", ("EventLog", "ReplicaSet", "CheckpointStore")),
    Lego("AntiEntropyRepair", "replication", "distributed", "Repairs divergent replicas.", ("Replicator", "Checksum", "ConflictPolicy")),
    Lego("ServiceRegistry", "routing", "distributed", "Maps service names to endpoints.", ("MetadataStore", "ServiceId")),
    Lego("Router", "routing", "distributed", "Chooses a destination for a key or route.", ("RouteRef", "RoutingPolicy", "MetadataStore")),
    Lego("LoadBalancer", "routing", "distributed", "Spreads requests across healthy endpoints.", ("Router", "HealthStatus")),
    Lego("PartitionMap", "partitioning", "distributed", "Maps partitions to owners.", ("MetadataStore", "Epoch")),
    Lego("ShardMap", "partitioning", "distributed", "Maps keys to shards.", ("PartitionMap", "PartitionPolicy")),
    Lego("ConsistentHashRing", "partitioning", "distributed", "Maps keys to owners with low churn.", ("ShardMap", "Hash")),
    Lego("MigrationFence", "partitioning", "distributed", "Prevents stale writes during movement.", ("FencingToken", "Epoch")),
    Lego("Rebalancer", "partitioning", "distributed", "Moves partitions to improve balance.", ("ShardMap", "Lease", "CheckpointStore", "MigrationFence")),
    # Application and operations.
    Lego("HealthStatus", "observability", "application", "Current health of a component.", ("Status", "Timestamp")),
    Lego("LogEvent", "observability", "application", "Human-readable event record.", ("Timestamp", "Status")),
    Lego("MetricSample", "observability", "application", "Numeric operational measurement.", ("Timestamp",)),
    Lego("TraceSpan", "observability", "application", "Timed trace segment.", ("TraceId", "SpanId", "Timestamp")),
    Lego("AuditRecord", "observability", "application", "Durable security or control event.", ("PrincipalId", "Timestamp")),
    Lego("LineageRecord", "observability", "application", "Causal explanation for an event.", ("Envelope", "CausalityId", "CorrelationId")),
    Lego("Worker", "compute", "application", "Claims and executes work.", ("WorkQueue", "Lease", "IdempotencyStore")),
    Lego("WorkerPool", "compute", "application", "Coordinates multiple workers.", ("Worker", "Membership", "FlowControl")),
    Lego("WorkflowStep", "workflow", "application", "One durable unit in a workflow.", ("EventLog", "RetryLoop")),
    Lego("Workflow", "workflow", "application", "Durably orchestrates multi-step work.", ("EventLog", "DurableQueue", "CheckpointStore", "IdempotencyStore", "RetryLoop")),
    Lego("Saga", "workflow", "application", "Workflow with compensation steps.", ("Workflow", "Compensation")),
    Lego("Compensation", "recovery", "application", "Undo or counteract a completed effect.", ("IdempotencyStore", "EventLog")),
    Lego("SensorSample", "compute", "application", "Normalized local sensor reading.", ("Timestamp", "SequenceNumber", "Status")),
    Lego("SensorTag", "identity", "application", "Metadata tag for a sensor or peripheral.", ("Key",)),
    Lego("SensorLocation", "identity", "application", "Physical coordinate for a sensor.", ("Timestamp",)),
    Lego("SensorIdentity", "identity", "application", "Identity, tags, location, and group for a sensor.", ("SensorTag", "SensorLocation")),
    Lego("SensorEvent", "communication", "application", "Normalized event payload for sensor and peripheral streams.", ("SensorIdentity", "Timestamp", "SequenceNumber")),
    Lego("SensorDebugTap", "observability", "application", "Records and broadcasts sensor debug emissions.", ("SensorEvent", "Clock")),
    Lego("LocalSensorSource", "compute", "application", "Polls a local sensor and publishes samples.", ("Clock", "RetryLoop", "SequenceCounter", "SensorSample", "RouteRef")),
    Lego("ReactiveSensorSource", "compute", "application", "Publishes observable sensor emissions to a route.", ("Clock", "SequenceCounter", "RouteRef", "SensorSample")),
    Lego("PeripheralAdapter", "communication", "application", "Adapts Heart-style peripheral envelopes to routes.", ("SensorIdentity", "SensorEvent", "RouteRef")),
    Lego("DuplexSensorPeripheral", "communication", "application", "Adapts peripherals with both output events and control input.", ("PeripheralAdapter", "RouteRef")),
    Lego("RateMatchedSensor", "flow", "application", "Buffers sensor samples and emits by demand.", ("RingBuffer", "Clock", "Graph")),
    Lego("SensorHealthWatchdog", "observability", "application", "Reports stale or failing sensor input.", ("Clock", "Watchdog", "HealthStatus")),
    Lego("LocalDurableSpool", "recovery", "application", "File-backed local sample retention.", ("EventLog", "Keyspace")),
    Lego("DesiredState", "configuration", "application", "Target configuration or topology.", ("Version",)),
    Lego("ObservedState", "configuration", "application", "Measured current configuration or topology.", ("Version",)),
    Lego("Reconciler", "control-plane", "application", "Moves observed state toward desired state.", ("DesiredState", "ObservedState", "RetryLoop", "EventLog")),
    Lego("FeatureFlag", "configuration", "application", "Runtime-switchable behavior flag.", ("MetadataStore", "Version")),
    Lego("RolloutController", "control-plane", "application", "Rolls out changes with health gates.", ("Reconciler", "HealthStatus", "MetricSample")),
    Lego("KillSwitch", "control-plane", "application", "Emergency disable control.", ("FeatureFlag", "AuthPolicy")),
    Lego("FlowControl", "flow", "application", "Coordinates backpressure, admission, and limits.", ("RateLimiter", "Semaphore", "BoundedQueue")),
)


def _duplicate_names(names: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicate_seen: set[str] = set()
    duplicates: list[str] = []
    for name in names:
        if name not in seen:
            seen.add(name)
            continue
        if name in duplicate_seen:
            continue
        duplicates.append(name)
        duplicate_seen.add(name)
    return tuple(duplicates)


def _duplicate_requirements_by_lego(legos: Iterable[Lego]) -> tuple[str, ...]:
    duplicates: list[str] = []
    for lego in legos:
        duplicate_requirements = _duplicate_names(lego.requires)
        if duplicate_requirements:
            duplicates.append(f"{lego.name}: {', '.join(duplicate_requirements)}")
    return tuple(duplicates)


_DUPLICATE_NAMES = _duplicate_names(lego.name for lego in _LEGOS)
if _DUPLICATE_NAMES:
    duplicate = ", ".join(_DUPLICATE_NAMES)
    raise RuntimeError(f"lego catalog declares duplicate names: {duplicate}")

_DUPLICATE_REQUIREMENTS = _duplicate_requirements_by_lego(_LEGOS)
if _DUPLICATE_REQUIREMENTS:
    duplicate = "; ".join(_DUPLICATE_REQUIREMENTS)
    raise RuntimeError(f"lego catalog declares duplicate dependencies: {duplicate}")

_BY_NAME = {lego.name: lego for lego in _LEGOS}
_UNKNOWN_REQUIREMENTS = tuple(
    sorted(
        {
            requirement
            for lego in _LEGOS
            for requirement in lego.requires
            if requirement not in _BY_NAME
        }
    )
)
if _UNKNOWN_REQUIREMENTS:
    unknown = ", ".join(_UNKNOWN_REQUIREMENTS)
    raise RuntimeError(f"lego catalog references unknown dependencies: {unknown}")

# Query helpers return immutable tuples, so precompute the small static indexes once.
_ALL_LEGOS = tuple(sorted(_LEGOS, key=_LEGO_NAME_KEY))
_DEPENDENCIES_BY_NAME = {
    lego.name: tuple(_BY_NAME[requirement] for requirement in lego.requires)
    for lego in _LEGOS
}


def _dependency_closure_for(name: str) -> tuple[Lego, ...]:
    ordered: list[Lego] = []
    visited: set[str] = set()
    visiting: list[str] = []

    def collect(current_name: str) -> None:
        if current_name in visiting:
            cycle = " -> ".join((*visiting, current_name))
            raise RuntimeError(f"lego catalog declares cyclic dependencies: {cycle}")

        visiting.append(current_name)
        for requirement in _BY_NAME[current_name].requires:
            if requirement in visited:
                continue
            collect(requirement)
            visited.add(requirement)
            ordered.append(_BY_NAME[requirement])
        visiting.pop()

    collect(name)
    return tuple(ordered)


_DEPENDENCY_CLOSURE_BY_NAME = {
    lego.name: _dependency_closure_for(lego.name)
    for lego in _LEGOS
}

_dependents_by_name: dict[str, list[Lego]] = {lego.name: [] for lego in _LEGOS}
_legos_by_role: dict[str, list[Lego]] = {}
_legos_by_layer: dict[str, list[Lego]] = {}
for lego in _LEGOS:
    for requirement in lego.requires:
        _dependents_by_name[requirement].append(lego)
    _legos_by_role.setdefault(lego.role, []).append(lego)
    _legos_by_layer.setdefault(lego.layer, []).append(lego)

_DEPENDENTS_BY_NAME = {
    name: tuple(sorted(dependents, key=_LEGO_NAME_KEY))
    for name, dependents in _dependents_by_name.items()
}
_LEGOS_BY_ROLE = {
    role: tuple(sorted(legos, key=_LEGO_NAME_KEY))
    for role, legos in _legos_by_role.items()
}
_LEGOS_BY_LAYER = {
    layer: tuple(sorted(legos, key=_LEGO_NAME_KEY))
    for layer, legos in _legos_by_layer.items()
}
del _dependents_by_name, _legos_by_layer, _legos_by_role


def all_legos() -> tuple[Lego, ...]:
    """Return every known lego sorted by name."""
    return _ALL_LEGOS


def get_lego(name: str) -> Lego:
    """Return one lego by exact name."""
    try:
        return _BY_NAME[name]
    except KeyError as exc:
        raise KeyError(f"unknown lego: {name}") from exc


def dependencies_of(name: str) -> tuple[Lego, ...]:
    """Return direct dependencies for one lego in declared order."""
    get_lego(name)
    return _DEPENDENCIES_BY_NAME[name]


def dependency_closure_of(name: str) -> tuple[Lego, ...]:
    """Return transitive dependencies in dependency-first order."""
    get_lego(name)
    return _DEPENDENCY_CLOSURE_BY_NAME[name]


def dependents_of(name: str) -> tuple[Lego, ...]:
    """Return legos that directly depend on ``name`` sorted by name."""
    get_lego(name)
    return _DEPENDENTS_BY_NAME[name]


def legos_by_role(role: str) -> tuple[Lego, ...]:
    """Return legos with the requested primary role sorted by name."""
    return _LEGOS_BY_ROLE.get(role, ())


def legos_by_layer(layer: str) -> tuple[Lego, ...]:
    """Return legos in the requested dependency layer sorted by name."""
    return _LEGOS_BY_LAYER.get(layer, ())


__all__ = (
    "Lego",
    "all_legos",
    "dependencies_of",
    "dependency_closure_of",
    "dependents_of",
    "get_lego",
    "legos_by_layer",
    "legos_by_role",
)
