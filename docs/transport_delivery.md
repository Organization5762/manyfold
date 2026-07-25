# Durable transport delivery

`manyfold.architecture.transport_delivery` adds bounded application-level
delivery to one `TcpTransport`. It journals outbound data before sending,
journals inbound data before exposing it, retries across reconnects, and keeps
ACK and duplicate state across process restarts.

```python
from pathlib import Path

from manyfold.architecture.transport import (
    FrameKind,
    TransportMessage,
)
from manyfold.architecture.transport_delivery import (
    DeliveryConfig,
    DurableDelivery,
)

# `transport` is an already configured and authenticated TcpTransport.
delivery = DurableDelivery(
    transport,
    DeliveryConfig(
        journal_path=Path("worker-delivery.sqlite3"),
        max_outbox_items=10_000,
        max_inbox_items=10_000,
        max_storage_bytes=512 * 1024 * 1024,
    ),
)
try:
    message_id = delivery.send(
        TransportMessage(
            FrameKind.PUBSUB,
            "orders.created",
            b"order-7",
        )
    )
    received = delivery.receive(timeout=5.0)
    process_order(received.message.payload)
    delivery.ack(received.message_id)
    print(message_id, delivery.health().outbox_items)
finally:
    delivery.close(graceful_timeout=2.0)
```

Sample output after the peer ACK arrives:

```text
51d1b846e43f4b7c87aafaa81f4ef591 0
```

## Delivery contract

`send()` commits the complete message to SQLite before returning its stable
message ID. The sender retains it until an ACK arrives or its TTL expires.
Reconnects and restarts read the same outbox and resume capped exponential
retry.

The receiver commits a new message ID before `receive()` can expose it.
Application code must then choose:

- `ack(message_id)` after successful processing. The ACK is durably scheduled
  and retried until the sender returns a confirmation.
- `nack(message_id, reason=...)` after a retryable rejection. The pending inbox
  record is removed so the sender can redeliver it.

This gives at-least-once application processing. A process crash after side
effects but before `ack()` can expose the message again. Consumers that need
effectively-once behavior must make their domain write and processed-message-ID
record atomic in the same database transaction.

ACKed inbox records remain for `dedupe_retention_seconds`. A duplicate with
identical content is suppressed and ACKed again. Reusing a stable ID for
different content is rejected as `DeliveryConflict`.

## Bounds and lifecycle

`DeliveryConfig` makes every retention decision explicit:

| Limit | Effect |
| --- | --- |
| `max_outbox_items` | Maximum unacknowledged sender records. |
| `max_inbox_items` | Maximum pending and ACKed duplicate-suppression records. |
| `max_storage_bytes` | Hard SQLite page cap and logical record-byte cap. |
| `receive_queue_limit` | Maximum process-memory deliveries awaiting `receive()`. |
| `max_message_bytes` | Maximum application payload before delivery framing. |
| `message_ttl_seconds` | Sender expiry for unacknowledged messages. |
| `dedupe_retention_seconds` | Receiver expiry for pending and ACKed IDs. |

SQLite runs in full-synchronous rollback-journal mode. Expired records are
deleted, expired queue/in-flight references are released, and incremental
compaction runs while the delivery worker is active. An OS-level sidecar lock
rejects a second live process using the same journal, and the database carries
an application ID and schema version so incompatible files fail at startup.
Capacity exhaustion raises `DeliveryStorageFull`; the implementation does not
evict live data to make room.

`max_storage_bytes` caps the main SQLite file and logical retained records.
SQLite's rollback journal is temporary but may require up to approximately one
additional database-sized allocation during a transaction; provision the
filesystem for that bounded write-ahead overhead.

`close()` stops both owned workers, clears the in-memory receive queue and ID
sets, and closes SQLite. Journaled outbox and inbox records intentionally
remain for reopen recovery. Pass `owns_transport=True` when closing the delivery
layer should also close its `TcpTransport`.

`flush()` waits for the durable outbox to become empty, which requires peer
ACKs or expiry. It is stronger than the underlying transport's socket flush.
`DeliveryHealth` reports journal counts and bytes, queue/inflight counts,
accepted/sent/retried/delivered/ACK/NACK/duplicate/expiry counters, and the most
recent worker error. `wait_for_health_change()` provides bounded-state
observation without retaining an event history.

## Operational boundary

One `DurableDelivery` instance exclusively owns its transport receive stream
and the journal lock enforces one live process per SQLite journal. Multi-peer
routing, PubSub
subscription propagation, coordinator RPC request state, authorization policy,
and certificate operations remain separate layers.

Operators must size TTL and dedupe retention from the longest supported outage,
provision storage above the bounded worst case, alert on journal saturation and
expiry, back up journals when loss is unacceptable, and validate recovery on
the target filesystem. SQLite durability cannot compensate for lost disks,
misconfigured storage, or domain side effects performed outside an atomic
processed-ID transaction.
