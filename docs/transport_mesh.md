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
| `peer_health()` | Report each underlying link, discovery source, interested topics, and latest routing error. |
| `health()` | Report bounded peer/subscription/queue counts and duplicate suppression. |
| `close()` | Dispose links and readers, clear every routing index and payload reference, and unblock receivers. |

`PeerDiscovery.transport_config` can override the mesh connector configuration
for peers with different TLS hostname or trust settings. Listener configuration
is similarly explicit per `listen(...)` call.

## Durable topic bindings

Applications that need brief disconnect and restart recovery can bind a named,
schema-validated `PubSub` directly to the mesh. The mesh remains the sole
transport/session owner and its existing reader dispatches data, ACKs, retries,
and subscription control.

```python
from dataclasses import dataclass
from pathlib import Path

from manyfold.architecture.pubsub import PubSub
from manyfold.architecture.transport_topics import (
    DurableTopicPolicy,
    MeshDurabilityConfig,
)

@dataclass(frozen=True)
class Navigation:
    command: str
    source: str

navigation = PubSub(topic="navigation", schema=Navigation, schedule=False)
mesh = TransportMesh(
    identity,
    connector_config=transport_config,
    durability=MeshDurabilityConfig(Path("manyfold-delivery")),
)
mesh.bind(navigation, policy=DurableTopicPolicy.append())
```

Bindings follow the mesh lifecycle: create them before peers join and call
`mesh.close()` once. `DurableTopicPolicy.append()` retains distinct,
deduplicated commands in order. `DurableTopicPolicy.latest()` keeps one pending
value for the topic, or one per bounded `key_field`, from enqueue time—not only
after capacity pressure. Both policies expire stale rows and have strict item,
byte, payload, and per-peer caps.

Recommended initial policies deliberately favor freshness over replay:

| Stream | Policy | Default operational TTL |
| --- | --- | --- |
| Navigation commands | append | 10 s |
| Frame ticks | latest, one slot | one cadence, never above 50 ms |
| Rendered frames | latest, one slot | measured 100–250 ms |
| Microphone samples | latest per source | 500 ms |
| Debug/input taps | latest per source | 500 ms |
| Sensor snapshots | latest per source | 5 s |

A frame tick is a clock impulse, not a historical sample: a newer tick replaces
the pending tick immediately and an expired tick is never replayed. Latest
topics similarly bound outage storage by active source count, at the cost of
discarding intermediate samples. Append topics preserve commands until ACK or
TTL, but reject new commands with `MeshBackpressureError` at a hard cap rather
than silently losing retained work.

`durable_topic_diagnostics()` reports retained rows and bytes plus replaced,
expired, retried, acknowledged, hard-cap-rejected, and recovery-loaded counts
per peer and topic. SQLite uses full-synchronous commits; each accepted publish
therefore pays one durable transaction per interested peer. Expiry and
incremental compaction run while connected or disconnected.

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
