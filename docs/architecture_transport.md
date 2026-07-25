# Cross-process transport

`manyfold.architecture.transport` provides one concrete cross-process building
block: a bounded, single-peer TCP link for PubSub payloads and coordinator RPC
messages. It uses only the Python standard library and performs real socket I/O;
it is not a graph-only transport declaration.

The module is intentionally imported directly until the package integration
change exports it:

```python
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)

development_config = TransportConfig(
    security=TransportSecurity.insecure_local_development(),
)
coordinator = TcpTransport.listen(
    NodeIdentity("development", "coordinator"),
    config=development_config,
    expected_peer_node_id="worker-1",
)
worker = TcpTransport.connect(
    NodeIdentity("development", "worker-1"),
    coordinator.address,
    config=development_config,
    expected_peer_node_id="coordinator",
)

try:
    if not worker.wait_until_connected(timeout=2.0):
        raise RuntimeError("worker could not reach coordinator")
    worker.send(
        TransportMessage(
            FrameKind.PUBSUB,
            "sensors.temperature",
            b"72.4",
        )
    )
    message = coordinator.receive(timeout=2.0)
    print(message.channel, message.payload.decode())
finally:
    worker.close()
    coordinator.close()
```

Sample output:

```text
sensors.temperature 72.4
```

## API shape

| Type or operation | Purpose |
| --- | --- |
| `NodeIdentity(cluster_id, node_id, instance_id)` | Identity sent by both peers before application data. A random process-instance ID is generated when one is not supplied. |
| `TransportSecurity.mutual_tls(...)` | Inject a certificate-verifying `SSLContext`; listeners require client certificates, connectors require hostname verification, and each certificate must bind the claimed node identity. |
| `TransportSecurity.insecure_local_development()` | Explicitly opt into cleartext for a loopback-only development link. It cannot bind or connect to a non-loopback address. |
| `TcpTransport.listen(...)` | Bind immediately, then accept and re-accept one validated peer. |
| `TcpTransport.connect(...)` | Connect in the background and keep reconnecting with capped exponential backoff. |
| `TransportMessage` | Opaque bytes plus a typed PubSub/RPC kind, channel, optional RPC correlation ID, and received sequence. |
| `send(...)` | Retain a frame only when bounded outbound capacity is available; otherwise raise `TransportQueueFull`. |
| `receive(...)` | Remove one bounded inbound frame, or raise `TimeoutError`/`TransportClosed`. |
| `flush(...)` | Wait for accepted outbound frames to reach the local socket before disposal. |
| `health()` | Return an immutable `LinkHealth` snapshot with state, peer identity, counters, queue depths, timestamps, and the latest error. |
| `wait_for_health_change(...)` | Observe state/counter changes without retaining callbacks or an event history. |
| `as_link(...)` | Produce the existing graph `Link` metadata for this concrete TCP transport. |

`TransportConfig` requires an explicit security policy and owns every memory and
time limit. Production callers pass separate server and client contexts created
for mutual TLS: both contexts use `CERT_REQUIRED`, the server context trusts and
requires client certificates, and the client context enables hostname checking,
trusts the server CA, and presents its own certificate. Outbound capacity
includes the writer's in-flight frame. The inbound queue has a hard item limit
and the reader can hold at most one additional decoded frame while applying TCP
backpressure. Each frame is capped by `max_payload_bytes`. `close()` stops
reconnects, closes accepted/listening sockets, drains both queues, releases
payload references, and joins the owned threads.

## Handshake and framing

Production connections start with mutual TLS; the explicit loopback development
mode starts in cleartext. Both then perform a symmetric Manyfold identity
handshake. The binary header and JSON identity payload both carry
`manyfold.transport` protocol version `1.0`. Peers reject bad magic, flags, frame
sizes, protocol versions, cluster IDs, duplicate local node IDs, and unexpected
peer node IDs before exposing the connection as healthy.
For mutual TLS, the peer certificate must contain the URI subject alternative
name `manyfold://identity/<percent-encoded-cluster>/<percent-encoded-node>`;
the transport rejects a handshake whose claimed identity does not match it. In
insecure local development mode, the claimed cluster and node fields are
validation fields, not authentication.

Application frames use a fixed-size binary header followed by UTF-8 channel and
correlation metadata and an opaque byte payload. PubSub, RPC request, RPC
response, and RPC error are distinct frame kinds. Monotonic sequence numbers
preserve ordering and suppress a retried duplicate across reconnects to the
same peer instance. Idle writers send heartbeats; readers disconnect silent or
partially framed peers after `peer_timeout`.

The advertised `LinkCapabilities(ordered=True, reliable=True)` describes the
active TCP session. Mutual-TLS links additionally advertise
`encrypted=True, authenticated=True`; the explicitly insecure development mode
does not. Neither mode is replayable.

## Production boundary

This module is a transport foundation, not a complete distributed PubSub or RPC
service. Production integration still needs:

- certificate issuance, rotation, revocation, and policy that binds mutual-TLS
  certificate identities to the expected Manyfold cluster and node IDs.
- application acknowledgements, durable replay, and a defined retry/deduplication
  policy for process crashes; a successful local `sendall` is not proof that the
  remote application handled a frame.
- service discovery and routing when a node needs more than one simultaneous
  peer.
- PubSub subscription propagation and coordinator RPC request tracking,
  serialization contracts, authorization, cancellation, and deadlines.
- deployment-specific capacity, latency, failure-injection, and security
  validation.

Those concerns can consume `TransportMessage` directly without replacing the
bounded framing, lifecycle, identity, reconnect, or health mechanisms here.
