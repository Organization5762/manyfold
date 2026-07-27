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
| `close()` / context exit | Dispose links and readers, clear every routing index and payload reference, and unblock receivers. |

`PeerDiscovery.transport_config` can override the mesh connector configuration
for peers with different TLS hostname or trust settings. Listener configuration
is similarly explicit per `listen(...)` call.

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

This mesh owns its `TcpTransport` instances exclusively and consumes only
PubSub frames. Coordinator RPC should use a separate transport owner or a future
common frame dispatcher; sharing one link between independent readers would
race.

The mesh deliberately accepts typed static discovery snapshots rather than
embedding DNS, Consul, Kubernetes, or another control plane. The deployment
owner must feed authoritative updates and provision one listener per expected
inbound peer. Durable acknowledgement/replay remains the responsibility of the
transport delivery layer rather than this routing index.
