# Transport mesh

`manyfold.architecture.transport_mesh` turns concrete `TcpTransport` links into
a bounded multi-peer PubSub mesh. It owns every link and reader thread, applies
typed static-discovery snapshots, propagates subscriptions, and forwards a
publication only along next hops that advertised interest.

```python
from manyfold.architecture.transport import (
    NodeIdentity,
    TransportConfig,
    TransportSecurity,
)
from manyfold.architecture.transport_mesh import (
    PeerDiscovery,
    TransportMesh,
)

development_transport = TransportConfig(
    security=TransportSecurity.insecure_local_development(),
)
source = TransportMesh(
    NodeIdentity("development", "source"),
    connector_config=development_transport,
)
relay = TransportMesh(
    NodeIdentity("development", "relay"),
    connector_config=development_transport,
)
sink = TransportMesh(
    NodeIdentity("development", "sink"),
    connector_config=development_transport,
)

try:
    relay_address = relay.listen("source")
    source.apply_discovery((PeerDiscovery("relay", relay_address),))
    sink_address = sink.listen("relay")
    relay.apply_discovery((PeerDiscovery("sink", sink_address),))

    subscription = sink.subscribe("sensors.temperature")
    source.publish("sensors.temperature", b"72.4")
    publication = sink.receive(timeout=2.0)
    print(publication.source_node_id, publication.topic, publication.payload.decode())
    subscription.dispose()
finally:
    source.close()
    relay.close()
    sink.close()
```

Sample output:

```text
source sensors.temperature 72.4
```

Real deployments supply mutual-TLS connector and listener configurations as
described in [Cross-process transport](architecture_transport.md). Each
certificate still binds its immediate Manyfold peer identity; the mesh carries
the original publisher ID across relays. That origin is transitively trusted
through authenticated mesh peers, not signed end to end.

## Concrete API

| Operation | Behavior |
| --- | --- |
| `listen(peer_node_id, ...)` | Own one listener dedicated to an expected peer. |
| `apply_discovery(entries)` | Atomically validate a typed static snapshot, then add, replace, or dispose connector-owned peers. |
| `remove_peer(node_id)` | Close the link, join its reader, withdraw routes learned through it, and request resynchronization from surviving peers. |
| `subscribe(topic)` | Allocate one bounded subscription ID and flood its route advertisement once through the mesh. |
| `MeshSubscription.dispose()` | Propagate the exact subscription withdrawal; failed bounded sends raise without losing the local subscription state. |
| `synchronize()` | Explicitly retry subscription state after caller-observed backpressure. Reconnected links synchronize automatically. |
| `publish(topic, payload)` | Deliver locally and fan out to unique interested next hops; no subscriber is an explicit routing error. |
| `receive(...)` | Remove one publication from the bounded local queue. |
| `bind(pubsub, policy=...)` | Bind one named typed `PubSub` handle once, before peers join. |
| `lifecycle_events(after_sequence=...)` | Read the retained suffix of ordered, typed local lifecycle events. |
| `subscribe_lifecycle(...)` | Pull lifecycle events from a bounded, non-durable local queue. |
| `lifecycle_health()` | Report lifecycle retention and subscriber-drop counts. |
| `durable_topic_diagnostics()` | Report each binding's delivery class, current journal retention, and transition counters. |
| `peer_health()` | Report each underlying link, discovery source, interested topics, and latest routing error. |
| `health()` | Report bounded peer/subscription/queue counts and duplicate suppression. |
| `close()` | Dispose links and readers, clear every routing index and payload reference, and unblock receivers. |

`PeerDiscovery.transport_config` can override the mesh connector configuration
for peers with different TLS hostname or trust settings. Listener configuration
is similarly explicit per `listen(...)` call.

## Durable topic bindings

Bind named, schema-validated `PubSub` handles directly at startup. The mesh
remains the sole transport and receive-loop owner; the durable classes compose
the existing `DurableDelivery` journal behind that dispatcher.

```python
from dataclasses import dataclass
from pathlib import Path

from manyfold.architecture.pubsub import PubSubTopic
from manyfold.architecture.transport_topics import (
    MeshDurabilityConfig,
    MeshTopicPolicy,
)

@dataclass(frozen=True)
class Navigation:
    command: str
    source: str

navigation = PubSubTopic("navigation.commands", schema=Navigation)
sensor_state = PubSubTopic("navigation.states", schema=Navigation)
frame_ticks = PubSubTopic("heart.frame_ticks", schema=Navigation)
mesh = TransportMesh(
    identity,
    connector_config=transport_config,
    durability=MeshDurabilityConfig(Path("manyfold-delivery")),
)
mesh.bind(
    navigation,
    policy=MeshTopicPolicy.commands("navigation.commands"),
)
mesh.bind(
    sensor_state,
    policy=MeshTopicPolicy.latest(
        "navigation.states",
        max_sources=32,
        max_bytes=1024 * 1024,
        ttl_seconds=5.0,
        key_field="source",
    ),
)
mesh.bind(
    frame_ticks,
    policy=MeshTopicPolicy.live_latest(
        "heart.frame_ticks",
        max_sources=1,
    ),
)

navigation.publish(
    Navigation("open-settings", "controller-1"),
    key="navigation:request-42",
)
```

`MeshTopicPolicy.commands()` is bounded durable append. A PubSub `key=` becomes
the stable correlation and deduplication identity; without a key, a
`<machine>.commands` topic uses `<machine>:<fabric-offset>`. A repeated ID with
different content is a conflict rather than silent replacement.

`MeshTopicPolicy.latest()` is bounded durable latest-per-source. It atomically
replaces the pending journal row for each `key_field`, expires stale rows by
TTL, and applies both item and byte soft watermarks and hard caps.

`MeshTopicPolicy.live_latest()` is process-local, non-journaled latest-per-source.
It retains at most `max_sources` current values in memory, discards older
outbound frames on the same source/topic slot, and resends only current state
after subscription recovery. It never creates outage or restart replay rows.
Use it for frame ticks, rendered frames, audio, and bounded debug streams. Use
durable append for navigation/input commands, durable latest with TTL for
low-rate sensor state, and the separate Raft path for coordinated world/device
state.

`MeshTopicBinding.retains_journal_rows` describes whether the configured class
may journal. `durable_topic_diagnostics()` reports whether rows actually exist
now, plus outbox items and coalesced, expired, retried, sender-acknowledged,
storage-rejected, and recovery-loaded counts. SQLite uses full-synchronous
commits, so durable fanout pays one transaction per interested peer. Live-latest
fanout never touches SQLite.

State machines can bind their four actual handles without an adapter:
`<name>.commands` as commands, `<name>.states` as durable latest,
`<name>.transitions` as commands when replay is desired, and `<name>.events`
left local by default. Transport replay does not make command consumption,
state revision, transition publication, and audit publication atomic; that
consumer/state boundary remains separate work.

## Typed lifecycle events

`MeshLifecycleEvent` is the public transport telemetry record. Its
`MeshLifecycleKind` covers runtime start/ready/stopping/stopped, peer
discovery/connect/disconnect/reconnect, durable enqueue/coalesce/drop/expire,
retry/send/sender-ACK/replay, watermark crossing/recovery, and terminal delivery
failure. `MeshLifecycleReason` gives the stable cause.

Every record has a node-local monotonic `sequence` and transition timestamp.
Applicable records carry exact topic, peer, message, correlation and related
message IDs, attempt, item count, byte count, and detail. The sequence is
assigned synchronously at the transition boundary, so consumers use it—not
wall-clock timestamps—for ordering.

```python
with mesh.subscribe_lifecycle(after_sequence=0, queue_limit=1024) as events:
    event = events.receive(timeout=1.0)
    print(event.sequence, event.kind, event.correlation_id)
```

Lifecycle telemetry is local and non-durable by policy. Publication only
appends to bounded in-memory storage and bounded pull queues with non-blocking
`put_nowait`; it never calls application callbacks, enters topic delivery, or
waits for a telemetry consumer. A full subscriber queue drops its oldest event,
and `lifecycle_health()` exposes both retention and subscriber drops. Heart and
qualification consumers should read this surface instead of polling private
transport state or republishing lifecycle records through a durable binding.

## Routing and bounds

Every local subscription receives a unique ID. A node retains the first next hop
seen for each remote ID and forwards the advertisement to its other peers.
Repeated advertisements are idempotent, so cyclic topologies do not grow
subscription state. Unsubscribe follows the retained next-hop tree and removes
the exact record.

Publication frames preserve the original node ID and message ID. Each node keeps
a fixed-size FIFO plus membership set of recent message IDs. Duplicate arrivals
are discarded, and both structures evict together at `duplicate_window`.
Different subscriptions for the same topic share one publication fanout per
next hop.

The following `MeshConfig` limits are mandatory hard bounds:

- `max_peers` includes active peer reservations during link creation.
- `max_subscriptions` includes local and relayed subscription records.
- `duplicate_window` bounds loop-prevention history.
- `publication_queue_limit` bounds locally retained payloads.

Underlying transport queues remain independently bounded. Partial fanout raises
`MeshBackpressureError` with accepted and rejected peer IDs. Subscription
creation raises `MeshSubscriptionBackpressureError` with the still-owned
subscription handle when propagation is only partially accepted. Disposal keeps
the subscription live when unsubscribe propagation is rejected, so neither path
orphans state and the caller can retry `synchronize()` or disposal.

## Operational boundary

This mesh owns each `TcpTransport` and has exactly one receive loop per peer.
Durable bindings share that dispatcher; applications must not attach
`DurableDelivery` or another reader to a mesh-owned transport.

The mesh deliberately accepts typed static discovery snapshots rather than
embedding DNS, Consul, Kubernetes, or another control plane. The deployment
owner must feed authoritative updates and provision one listener per expected
inbound peer.
