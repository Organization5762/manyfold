"""Typed lifecycle observations for durable transport delivery."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TypeAlias

from ._transport_delivery_protocol import DeliveryError
from .transport import TransportMessage


class DeliveryClosed(DeliveryError):
    """Raised when an operation targets a closed delivery layer."""


class DeliveryStorageFull(DeliveryError):
    """Raised when a configured journal item or byte bound is full."""


class DeliveryConflict(DeliveryError):
    """Raised when one stable message ID names different content."""


class DeliveryEventKind(str, Enum):
    """One exact durable-delivery lifecycle transition."""

    ACKNOWLEDGED = "acknowledged"
    COALESCED = "coalesced"
    DEDUPLICATED = "deduplicated"
    DROPPED = "dropped"
    DUPLICATE_SUPPRESSED = "duplicate_suppressed"
    ENQUEUED = "enqueued"
    EXPIRED = "expired"
    REPLAYED = "replayed"
    RETRY_SCHEDULED = "retry_scheduled"
    SENT = "sent"
    SOFT_WATERMARK = "soft_watermark"


@dataclass(frozen=True, slots=True)
class DeliveryCapacity:
    """Projected topic and peer journal use at one outbound transition."""

    peer_items: int
    peer_item_limit: int
    peer_logical_bytes: int
    peer_byte_limit: int
    topic_items: int
    topic_item_limit: int
    topic_bytes: int
    topic_byte_limit: int
    soft_limit_ratio: float


@dataclass(frozen=True, slots=True)
class DeliveryEvent:
    """Immutable message-level transition emitted without retained history."""

    sequence: int
    occurred_at: float
    kind: DeliveryEventKind
    message_id: str
    topic: str
    source: str | None
    correlation_id: str | None = None
    attempt: int = 0
    related_message_id: str | None = None
    detail: str | None = None
    capacity: DeliveryCapacity | None = None


DeliveryObserver: TypeAlias = Callable[[DeliveryEvent], None]


@dataclass(frozen=True, slots=True)
class ReceivedDelivery:
    """One durable application message awaiting explicit ACK or NACK."""

    message_id: str
    message: TransportMessage
    delivery_attempt: int


@dataclass(frozen=True, slots=True)
class DeliveryHealth:
    """Immutable delivery, retry, retention, and journal health snapshot."""

    generation: int
    closed: bool
    outbox_items: int
    append_outbox_items: int
    latest_outbox_items: int
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
    outbox_deduplicated: int
    coalesced: int
    soft_compactions: int
    soft_watermark_crossings: int
    storage_rejections: int
    retry_exhausted: int
    recovered_outbox: int
    expired_outbox: int
    expired_inbox: int
    last_error: str | None


__all__ = [
    "DeliveryCapacity",
    "DeliveryClosed",
    "DeliveryConflict",
    "DeliveryEvent",
    "DeliveryEventKind",
    "DeliveryHealth",
    "DeliveryObserver",
    "DeliveryStorageFull",
    "ReceivedDelivery",
]
