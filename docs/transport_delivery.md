# Durable transport delivery

`manyfold.architecture.transport_delivery` provides bounded, application-level
delivery over one exclusively owned `TcpTransport`. It commits outbound data
before sending, commits inbound data before exposing it, and preserves
acknowledgements and typed terminal outcomes across reconnects and process
restarts.

## Configure durable topics explicitly

Every new durable send needs a `TopicDeliveryPolicy`. Command topics append and
deduplicate by message ID. Latest-value topics retain exactly one outbound slot
per source; a shared durable latest slot cannot be constructed.

```python
from pathlib import Path

from manyfold.architecture.transport import FrameKind, TransportMessage
from manyfold.architecture.transport_delivery import (
    DeliveryConfig,
    DurableDelivery,
    TopicDeliveryPolicy,
)

policies = (
    TopicDeliveryPolicy.commands(
        "orders.created",
        max_items=512,
        max_bytes=8 * 1024 * 1024,
        ttl_seconds=60 * 60,
    ),
    TopicDeliveryPolicy.latest(
        "worker.status",
        max_sources=64,
        max_bytes=1024 * 1024,
        ttl_seconds=5 * 60,
    ),
)
config = DeliveryConfig(
    journal_path=Path("worker-delivery.sqlite3"),
    max_outbox_items=512,
    max_inbox_items=512,
    max_storage_bytes=8 * 1024 * 1024,
    message_ttl_seconds=60 * 60,
    topic_policies=policies,
)

# `transport` is an already configured and authenticated TcpTransport whose
# payload limit can hold config.max_message_bytes plus delivery framing.
with DurableDelivery(transport, config) as delivery:
    command_id = delivery.send(
        TransportMessage(
            FrameKind.PUBSUB,
            "orders.created",
            b"order-7",
        ),
        message_id="order-7",
    )
    status_id = delivery.send(
        TransportMessage(
            FrameKind.PUBSUB,
            "worker.status",
            b"ready",
        ),
        message_id="worker-status-42",
        source="worker-7",
    )
    print(command_id, status_id)
```

Sample output:

```text
order-7 worker-status-42
```

`source=` is required for latest-value sends and rejected for command sends.
Repeated latest sends from `worker-7` atomically replace only that source's
prior slot; other sources remain independent. Unconfigured new topics fail
before a journal row is written. This intentional change replaces the previous
implicit append policy.

## Receive and choose a typed outcome

`receive()` returns a `ReceivedDelivery` and transfers one bounded inbox row to
the application. Finish it with one of these durable decisions:

```python
from manyfold.architecture.transport_delivery import DeliveryOutcome

received = delivery.receive(timeout=5.0)
try:
    process(received.message.payload)
except TemporaryDependencyFailure as error:
    delivery.nack(
        received.message_id,
        outcome=DeliveryOutcome.retryable(str(error)),
    )
except InvalidOrder as error:
    delivery.nack(
        received.message_id,
        outcome=DeliveryOutcome.terminal(str(error)),
    )
else:
    delivery.ack(received.message_id)
```

- `ack()` persists ACKED, resends it under the ACK attempt cap, and retains the
  confirmed ID for duplicate suppression.
- `DeliveryOutcome.retryable(...)` releases the pending inbox record so the
  sender can retry with capped exponential backoff.
- `DeliveryOutcome.terminal(...)` and `DeliveryOutcome.expired(...)` persist
  bounded outcomes. Lost control frames and restart therefore replay the same
  outcome without exposing the application payload again.

`nack(reason="...")` remains a deprecated compatibility path. It always maps to
`DeliveryOutcome.retryable(...)`; it cannot express a terminal outcome.
Outcome reasons are nonblank, canonical text capped at 1,024 UTF-8 bytes.

This is at-least-once application processing. A crash after a domain side
effect but before `ack()` may expose the message again. Effectively-once
consumers must atomically record the processed message ID with their domain
write.

## Bounds

Every retained or in-memory collection has a configured hard bound. Defaults
are:

| Setting | Default and enforced meaning |
| --- | --- |
| `max_outbox_items` / `max_inbox_items` | 1,024 retained rows per journal side. |
| `receive_queue_limit` | 256 total queued plus application-inflight deliveries. |
| `max_message_bytes` | 8 MiB application payload; the attached transport must also fit the complete DATA frame. |
| `max_storage_bytes` | 64 MiB by default and as the absolute maximum for both SQLite pages and logical retained bytes. |
| `message_ttl_seconds` / `dedupe_retention_seconds` | 24 hours. |
| `max_delivery_attempts` / `max_ack_attempts` | 64 accepted network sends; values must fit the wire `uint32`. |
| `work_batch_size` / `recovery_batch_size` | 32 rows per steady-state bounded query or lifecycle category, and 64 rows per startup query. Recovery batches cannot exceed either retained-item bound. |
| retry schedule | 0.1 seconds × 2, capped at 5 seconds. The local-pressure delay exponent is capped at 16. |
| `soft_limit_ratio` | 0.7 for peer-wide dimensions; each topic has its own ratio. |
| `worker_join_timeout_seconds` | 2 seconds for admitted operations and both worker joins. A caller-selected graceful-drain timeout is a separate budget. |

Each topic's outbound/inbox item and byte limits must fit the corresponding
peer limits. A topic TTL and attempt cap must fit the peer-wide caps. All
numeric time and ratio values are finite. A send-specific TTL may only shorten,
not extend, its topic TTL.

Transport queue pressure and disconnects do not spend DATA or ACK network
attempts. They use the separately bounded local-pressure schedule. An accepted
transport send spends exactly one attempt. The final DATA attempt remains
eligible for its complete response window before retry exhaustion can delete
it. TTL remains the outer bound.

Capacity exhaustion raises `DeliveryStorageFull`; live data is never evicted to
make room. SQLite uses full-synchronous rollback-journal mode. The main database
is page-capped before schema or migration writes. A transaction can
temporarily require a rollback journal of approximately one additional
database size, so provision the filesystem for that bounded overhead.

## Logical-byte contract

Logical sizes are versioned V2 values over the columns that carry application
identity and content. Every UTF-8 field is counted exactly once:

```text
outbox = 160 + utf8(message_id) + utf8(topic)
             + utf8(optional correlation_id)
             + utf8(source only for latest)
             + payload bytes

inbox  = 128 + utf8(message_id) + utf8(topic)
             + utf8(optional correlation_id)
             + payload bytes
```

The fixed 160/128-byte overheads cover the remaining schema values, row
bookkeeping, and V2 sizing margin; status, outcome reason, and retry metadata
are not counted again. Health, topic capacity, insert rejection, recovery, and
migration all use these formulas.

## Recovery and migration

Startup validates SQLite integrity, application identity, the exact schema and
indexes, canonical UTF-8 text, storage classes, logical sizes, timestamps,
attempt state, and wire metadata. Validation and replay scan by covering
keyset indexes in `recovery_batch_size` batches without loading payloads.
Hydration materializes at most `receive_queue_limit` payloads across queue plus
inflight state.

V1 migration is one atomic transaction:

1. Validate legacy storage and static protocol integrity without mutation.
2. Copy rows in bounded batches, recompute exact UTF-8 V2 logical sizes, and
   build the V2 schema.
3. Compact already-expired rows in bounded batches.
4. Validate surviving rows against current item, logical-byte, payload, TTL,
   delivery-attempt, ACK-attempt, and topic-policy bounds.
5. Commit the V2 tables, indexes, metadata, application ID, and schema version.

A malformed row or live current-cap mismatch rolls the transaction back to the
intact V1 journal. Expired rows may therefore be removed before a smaller
current payload or attempt cap is applied. A later incompatibility with the
attached transport's complete frame limit fails closed as `DeliveryError`.

V2 startup first validates static retained-row integrity, then performs bounded
expiry cleanup, then applies current configuration and transport limits to
surviving live rows. Capacity/policy drift is `DeliveryStorageFull`; damaged
metadata or wire incompatibility is `DeliveryError`. Corrupt data is never
silently normalized or deleted before static validation.

The reserved outer channel remains `__manyfold.delivery.v1`, while the frame
header is protocol version 2. A V1 peer receives an explicit incompatible
version error; mixed V1/V2 delivery peers are not supported.

## Volatile topics are never durable

Frame, render, tick, audio, microphone, debug, and input streams are hot paths,
not journal traffic. Topic classification uses deterministic, case-folded,
separator-delimited V1 tokens—not substring matching. For example,
`heart.frame_tick`, `heart.rendered_frame`, `heart.microphone.level`, and
`heart.input` are rejected even when explicitly configured. A benign name such
as `debuggable.command` remains eligible for an explicit policy.

Outbound rejection writes zero rows. Inbound volatile DATA also writes zero
rows; repeated unique hot frames cannot grow the inbox or logical-byte count.
Use the normal bounded transport, coalescing, or a mesh-owned typed volatile
path for those streams.

## Events, health, and observer ordering

Observers receive payload-free immutable `DeliveryEvent` values synchronously
and in strict sequence order. Every message event identifies
`DeliveryStore.OUTBOX` or `DeliveryStore.INBOX`. Watermark events identify the
exact peer/topic item/logical-byte dimension that crossed or was recovered.

One committed capacity transition publishes one indivisible causal batch:
dimension-specific `WATERMARK_CROSSED` events, per-row lifecycle events for
expiry and retry exhaustion, an `EXPIRY_SWEEP` result (including zero
deletions), then ENQUEUED or COALESCED. Rolled-back writes publish nothing. A
threshold does not cross again until use falls below it.
Recovered-above-watermark is a separate startup fact.

Observers may inspect health. Mutation, blocking delivery operations, and
`close()` are rejected during a callback so reentry cannot change a causal
batch. Observer failures are isolated into a bounded `last_error`; there is no
observer thread, history, or queue. Validators follow the same read-only
callback rule.

`DeliveryHealth` separates retained APPEND/LATEST and
ACKED/TERMINAL/EXPIRED rows, local application outcomes from peer control
outcomes, watermark crossings from recovered watermarks, expiry transitions
from `sweep_deleted_rows`, retry exhaustion, local pressure, queue/inflight
state, and the bounded last error. `wait_for_health_change()` returns only a
strictly newer generation; closure wakes waiters with `DeliveryClosed` rather
than allowing a busy loop.

## Ownership and shutdown

One `DurableDelivery` owns one sender loop (the only `transport.send` owner),
one receive loop (the only `transport.receive` owner), and one SQLite
connection owner. The canonical regular-file path is exclusive across
processes and aliases.

Cross-process exclusion uses one supervised helper process that opens the same
journal identity and holds a platform file lock outside both the 64 MiB content
cap and SQLite's own lock range. It opens no SQLite connection, socket, or
sidecar artifact. Parent/child control uses anonymous pipes; input is discarded
in fixed 4 KiB chunks. The startup token detects response mix-ups but is not
authentication or a security boundary. Startup, normal close,
terminate/kill fallback, and reap all have explicit deadlines. Parent exit
closes the pipe so the helper exits and releases the lock.

`close()` first stops admitting public mutations, optionally drains for the
caller-supplied graceful budget, wakes both loops and application receivers,
then requires both non-daemon workers to stop before closing SQLite. If
operations or workers exceed the 2-second teardown budget,
`DeliveryCloseFailed` leaves ownership intact rather than pretending to be
closed. Cleanup clears receive/inflight data, pending controls, observers,
validators, and wake callbacks. Journal close is retryable if a resource could
not be released on the first attempt.

`flush()` waits for the durable outbox to reach final outcomes and fails
promptly after endpoint loss; it is stronger than the transport's socket
flush. `owns_transport=True` makes successful delivery shutdown also close the
underlying transport.

Multi-peer routing, subscription propagation, coordinator RPC state,
authorization, and certificate management remain separate layers. See the
[transport architecture](architecture_transport.md) for their ownership
boundaries.
