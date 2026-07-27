"""Typed outcomes and observations for durable transport delivery."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TypeAlias, final

from .transport import TransportMessage

_MAX_DELIVERY_OUTCOME_REASON_BYTES = 1024


class DeliveryError(RuntimeError):
    """Base error for durable delivery failures."""


class DeliveryProtocolError(DeliveryError):
    """Raised for malformed durable-delivery control frames."""


class DeliveryClosed(DeliveryError):
    """Raised when an operation targets a closed delivery layer."""


class DeliveryStorageFull(DeliveryError):
    """Raised when a configured journal item or byte bound is full."""


class DeliveryConflict(DeliveryError):
    """Raised when one stable message ID names different content."""


class DeliveryCloseFailed(DeliveryError):
    """Raised when delivery workers do not stop within the configured bound."""


@final
class DeliveryOutcomeKind(str, Enum):
    """Typed negative or terminal result for one durable message."""

    RETRYABLE = "retryable"
    TERMINAL = "terminal"
    EXPIRED = "expired"


@final
@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    """One typed failure outcome carried by NACKs and lifecycle events."""

    kind: DeliveryOutcomeKind
    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, DeliveryOutcomeKind):
            raise ValueError("kind must be a DeliveryOutcomeKind")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("reason must be a non-empty string")
        reason = self.reason.strip()
        if len(reason.encode("utf-8")) > _MAX_DELIVERY_OUTCOME_REASON_BYTES:
            raise ValueError(
                "encoded delivery outcome reason exceeds "
                f"{_MAX_DELIVERY_OUTCOME_REASON_BYTES} bytes"
            )
        object.__setattr__(self, "reason", reason)

    @classmethod
    def retryable(cls, reason: str) -> DeliveryOutcome:
        """Return a retryable rejection."""
        return cls(DeliveryOutcomeKind.RETRYABLE, reason)

    @classmethod
    def terminal(cls, reason: str) -> DeliveryOutcome:
        """Return a terminal rejection."""
        return cls(DeliveryOutcomeKind.TERMINAL, reason)

    @classmethod
    def expired(cls, reason: str = "delivery expired") -> DeliveryOutcome:
        """Return an expiry outcome."""
        return cls(DeliveryOutcomeKind.EXPIRED, reason)


@final
class DeliveryEventKind(str, Enum):
    """One exact durable-delivery lifecycle fact."""

    ACKNOWLEDGED = "acknowledged"
    COALESCED = "coalesced"
    DEDUPLICATED = "deduplicated"
    DROPPED = "dropped"
    DUPLICATE_SUPPRESSED = "duplicate_suppressed"
    ENQUEUED = "enqueued"
    EXPIRED = "expired"
    EXPIRY_SWEEP = "expiry_sweep"
    REPLAYED = "replayed"
    RETRY_SCHEDULED = "retry_scheduled"
    SENT = "sent"
    WATERMARK_CROSSED = "watermark_crossed"
    WATERMARK_RECOVERED = "watermark_recovered"


@final
class DeliveryStore(str, Enum):
    """Durable journal side associated with an observed fact."""

    OUTBOX = "outbox"
    INBOX = "inbox"


@final
class DeliveryCapacityDimension(str, Enum):
    """Exact soft-limit dimension represented by a capacity event."""

    PEER_ITEMS = "peer_items"
    PEER_LOGICAL_BYTES = "peer_logical_bytes"
    TOPIC_ITEMS = "topic_items"
    TOPIC_LOGICAL_BYTES = "topic_logical_bytes"


@final
@dataclass(frozen=True, slots=True)
class DeliveryCapacity:
    """Projected peer and topic use at one capacity decision."""

    peer_items: int
    peer_item_limit: int
    peer_logical_bytes: int
    peer_byte_limit: int
    topic_items: int
    topic_item_limit: int
    topic_logical_bytes: int
    topic_byte_limit: int
    peer_soft_limit_ratio: float
    topic_soft_limit_ratio: float


@final
@dataclass(frozen=True, slots=True)
class DeliveryEvent:
    """Immutable message or capacity fact emitted without retained history."""

    sequence: int
    occurred_at: float
    kind: DeliveryEventKind
    message_id: str | None
    topic: str | None
    source: str | None
    store: DeliveryStore | None = None
    capacity_dimension: DeliveryCapacityDimension | None = None
    correlation_id: str | None = None
    attempt: int = 0
    related_message_id: str | None = None
    outcome: DeliveryOutcome | None = None
    capacity: DeliveryCapacity | None = None
    local_pressure_count: int = 0
    affected_items: int = 0
    deleted_items: int = 0
    released_logical_bytes: int = 0

    def __post_init__(self) -> None:
        aggregate = {
            DeliveryEventKind.EXPIRY_SWEEP,
            DeliveryEventKind.WATERMARK_CROSSED,
            DeliveryEventKind.WATERMARK_RECOVERED,
        }
        if self.kind in aggregate:
            if self.message_id is not None:
                raise ValueError(
                    f"{self.kind.value} event cannot carry a message_id"
                )
        elif not self.message_id:
            raise ValueError(
                f"{self.kind.value} event requires a non-empty message_id"
            )
        elif self.store is None:
            raise ValueError(
                f"{self.kind.value} message event requires a store"
            )
        if self.kind is DeliveryEventKind.WATERMARK_CROSSED and not self.topic:
            raise ValueError(f"{self.kind.value} event requires a topic")
        if self.kind in {
            DeliveryEventKind.WATERMARK_CROSSED,
            DeliveryEventKind.WATERMARK_RECOVERED,
        } and self.capacity_dimension is None:
            raise ValueError(
                f"{self.kind.value} event requires a capacity_dimension"
            )
        if self.capacity_dimension in {
            DeliveryCapacityDimension.PEER_ITEMS,
            DeliveryCapacityDimension.TOPIC_ITEMS,
            DeliveryCapacityDimension.TOPIC_LOGICAL_BYTES,
        } and self.store is None:
            raise ValueError(
                f"{self.capacity_dimension.value} event requires a store"
            )
        if self.capacity_dimension in {
            DeliveryCapacityDimension.TOPIC_ITEMS,
            DeliveryCapacityDimension.TOPIC_LOGICAL_BYTES,
        } and not self.topic:
            raise ValueError(
                f"{self.capacity_dimension.value} event requires a topic"
            )
        if self.topic is not None and not self.topic:
            raise ValueError("event topic must be non-empty when provided")


DeliveryObserver: TypeAlias = Callable[[DeliveryEvent], None]
DeliveryReceiveValidator: TypeAlias = Callable[[TransportMessage], None]


@final
@dataclass(frozen=True, slots=True)
class ReceivedDelivery:
    """One durable application message awaiting explicit ACK or NACK."""

    message_id: str
    message: TransportMessage
    delivery_attempt: int


@final
@dataclass(frozen=True, slots=True)
class DeliveryHealth:
    """Immutable retry, retention, pressure, and journal health snapshot."""

    generation: int
    closed: bool
    outbox_items: int
    pending_inbox_items: int
    acked_inbox_items: int
    logical_storage_bytes: int
    queued_deliveries: int
    inflight_deliveries: int
    accepted: int
    frames_sent: int
    retries: int
    delivered: int
    acknowledgements: int
    negative_acknowledgements: int
    duplicates_suppressed: int
    expired_outbox: int
    expired_inbox: int
    last_error: str | None
    append_outbox_items: int = 0
    latest_outbox_items: int = 0
    terminal_inbox_items: int = 0
    expired_inbox_items: int = 0
    peer_acknowledgements: int = 0
    peer_negative_acknowledgements: int = 0
    outbox_deduplicated: int = 0
    coalesced: int = 0
    watermark_crossings: int = 0
    expiry_sweeps: int = 0
    sweep_deleted_rows: int = 0
    recovered_watermarks: int = 0
    storage_rejections: int = 0
    terminal_drops: int = 0
    retry_exhausted: int = 0
    ack_retry_exhausted: int = 0
    recovered_outbox: int = 0
    transport_backpressure_failures: int = 0
    transport_backpressure_streak: int = 0


@final
@dataclass(frozen=True, slots=True)
class DeliveryTopicHealth:
    """Exact retained SQLite rows and logical bytes for one topic."""

    topic: str
    retained_items: int
    outbox_items: int
    append_outbox_items: int
    latest_outbox_items: int
    inbox_items: int
    logical_storage_bytes: int


__all__ = [
    "DeliveryCapacity",
    "DeliveryCapacityDimension",
    "DeliveryCloseFailed",
    "DeliveryClosed",
    "DeliveryConflict",
    "DeliveryEvent",
    "DeliveryEventKind",
    "DeliveryHealth",
    "DeliveryObserver",
    "DeliveryOutcome",
    "DeliveryOutcomeKind",
    "DeliveryProtocolError",
    "DeliveryReceiveValidator",
    "DeliveryStorageFull",
    "DeliveryStore",
    "DeliveryTopicHealth",
    "ReceivedDelivery",
]
