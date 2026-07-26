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
    TopicDeliveryPolicy,
)

# `transport` is an already configured and authenticated TcpTransport.
policies = (
    TopicDeliveryPolicy.frame_ticks(
        "frame.tick",
        max_bytes=64 * 1024,
        cadence_seconds=1 / 60,
    ),
    TopicDeliveryPolicy.rendered_frames(
        "frame.rendered",
        max_sources=2,
        max_bytes=16 * 1024 * 1024,
    ),
    TopicDeliveryPolicy.latest(
        "sensor.state",
        max_sources=64,
        max_bytes=1024 * 1024,
        ttl_seconds=1.0,
    ),
    TopicDeliveryPolicy.commands(
        "navigation.command",
        max_items=256,
        max_bytes=1024 * 1024,
        ttl_seconds=30.0,
    ),
)
delivery = DurableDelivery(
    transport,
    DeliveryConfig(
        journal_path=Path("worker-delivery.sqlite3"),
        max_outbox_items=10_000,
        max_inbox_items=10_000,
        max_storage_bytes=512 * 1024 * 1024,
        topic_policies=policies,
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
51d1b846e43f4b7c87aafaa81f4ef591-0000000000000001 0
```

## Delivery contract

`send()` commits the complete message to SQLite before returning its stable
message ID. The sender retains it until an ACK arrives or its TTL expires.
Reconnects and restarts read the same outbox and resume capped exponential
retry within the row's finite attempt budget. Automatically assigned IDs use a
persisted journal namespace and monotonic sequence. The journal reserves
sequence high-watermark blocks to avoid a second full-synchronous commit per
send; a crash can leave gaps but cannot reuse an ID. Callers should still
provide a domain-stable `message_id` when a command may be resubmitted after a
process restart.

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

## Topic policy

Topics without a `TopicDeliveryPolicy` retain the original append behavior.
Configured topics choose one of two explicit semantics:

| Semantics | Retention and identity |
| --- | --- |
| `commands()` | Append each distinct ID. Repeated identical IDs deduplicate; the same ID with different content conflicts. This fits bounded navigation and other commands that must not be silently coalesced. |
| `latest()` | Keep one atomic replaceable outbox row per `source`. This fits microphone state, debug/input state, and sensor values where only the newest value remains useful. |

`frame_ticks()` is a single shared latest slot, not one slot per source. Its TTL
is the smaller of the declared cadence and 50 ms, and its attempt budget is one,
so ticks cannot accumulate or retry into a stale tick backlog. `send()` must not
receive `source` for this shared-slot profile.

`rendered_frames()` uses latest-per-source slots and defaults to a 200 ms TTL,
inside the provisional 100–250 ms range. That default is deliberately pending
measurement on target hardware; shorten it when measured render-to-display
latency makes older frames useless. Generic `latest()` policies require an
explicit `source`, making microphone, debug, input, and sensor coalescing
independent per producer.

Replacement deletes the previous latest row inside the same `BEGIN IMMEDIATE`
transaction before checking topic and peer capacity. A replacement can
therefore succeed at a one-item hard cap. If the new row is too large or SQLite
cannot commit it, rollback restores the previous row.

Coalescing is an outbox policy. A stale value already handed to the TCP
transport can still arrive before its replacement; consumers must apply their
normal source ordering or timestamp rule when that distinction matters. No
policy metadata is added to the wire protocol, and no transport or consensus
layer is introduced.

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
| `max_delivery_attempts` | Default finite send-attempt budget for topics without an explicit policy. |
| `soft_limit_ratio` | Default utilization ratio that triggers an expiry sweep before append. |
| `topic_policies` | Per-topic semantics, item/byte hard caps, TTL, attempt budget, and soft watermark. |

SQLite runs in full-synchronous rollback-journal mode. Expired records are
deleted, expired queue/in-flight references are released, and incremental
compaction runs while the delivery worker is active. An OS-level sidecar lock
rejects a second live process using the same journal, and the database carries
an application ID and schema version so incompatible files fail at startup.
Startup also runs SQLite's structural quick check and fails closed on detected
truncation or B-tree corruption rather than replaying a suspect journal.
Schema-v1 journals migrate in place to schema v2 without rewriting retained
payloads. Capacity exhaustion raises `DeliveryStorageFull`; soft-watermark
sweeps delete only expired rows and never evict live append commands.

The peer-wide item and byte limits remain the outer hard caps because one
`DurableDelivery` journal belongs to one peer link. Topic hard caps are stricter
inner limits. Logical byte accounting includes payload and conservative row
metadata overhead, while `max_storage_bytes` also sets SQLite's page cap.

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
append/latest outbox counts, accepted/sent/retried/delivered/ACK/NACK counters,
sender deduplication, coalescing, soft compactions, hard-cap rejections,
soft-watermark crossings, retry-budget exhaustion, recovered rows, expiry, and
the most recent worker error. Counters describe the current process lifetime;
retained item and byte gauges are read from SQLite after restart.
`wait_for_health_change()` provides bounded-state observation without retaining
an event history.

Pass an optional `observer` to `DurableDelivery` when a caller such as the mesh
must publish message-level lifecycle telemetry. The callback receives immutable
`DeliveryEvent` values with a process-monotonic sequence, timestamp, kind,
message ID, raw topic, source, correlation ID, attempt, optional related ID,
detail, and typed `DeliveryCapacity` evidence when applicable.
`DeliveryEventKind` distinguishes `ENQUEUED`, `COALESCED`, `DEDUPLICATED`,
`DROPPED`, `DUPLICATE_SUPPRESSED`, `EXPIRED`, `RETRY_SCHEDULED`, `SENT`,
`SOFT_WATERMARK`, `ACKNOWLEDGED`, and `REPLAYED`. A coalescing event relates the
new ID to the replaced ID; duplicate suppression identifies the exact inbound
ID/topic/correlation outcome; replay events identify rows recovered when the
journal opens. Soft-watermark and capacity-rejection events carry projected
peer/topic item and byte use beside the corresponding soft ratio and hard caps.

Events are synchronous observations and are not retained by the delivery layer.
Worker and caller threads can invoke the observer concurrently, so observers
must use the event sequence for ordering and return quickly. Observer failures
do not roll back or alter delivery; `last_error` records the failure. This keeps
typed mesh lifecycle publication separate from journal correctness and avoids
inferring message transitions from aggregate health counters.

Run the storage microbenchmark against full-synchronous SQLite writes with:

```sh
uv run python scripts/benchmark_delivery_journal.py \
  --iterations 10000 \
  --latest-sources 64 \
  --payload-bytes 256
```

It reports append and latest-slot throughput, p50/p95 commit latency, logical
bytes, retained rows, expected retained rows, and main database file size. It
fails if latest retention exceeds one row per source. Results are environment
specific; use the target filesystem and payload distribution for capacity
decisions. Set `--latest-sources 1` to model the single shared frame-tick slot
at Heart's 1,000-ticks/s ceiling; increase it to validate debug/input source
cardinality.

## Operational boundary

One `DurableDelivery` instance exclusively owns its transport receive stream
and the journal lock enforces one live process per SQLite journal. Existing
point-to-point users can keep this API. New multi-peer applications should use
`TransportMesh.bind(...)` with `DurableTopicPolicy`; the mesh reuses the durable
journal/protocol contract while retaining sole ownership of every peer receive
loop. Do not attach `DurableDelivery` to a mesh-owned transport.

Coordinator RPC request state, authorization policy, certificate operations,
and Raft-replicated world/device state remain separate layers.

Operators must size TTL and dedupe retention from the longest supported outage,
provision storage above the bounded worst case, alert on journal saturation and
expiry, back up journals when loss is unacceptable, and validate recovery on
the target filesystem. SQLite durability cannot compensate for lost disks,
misconfigured storage, undetected bit flips that preserve SQLite's structure,
or domain side effects performed outside an atomic processed-ID transaction.
