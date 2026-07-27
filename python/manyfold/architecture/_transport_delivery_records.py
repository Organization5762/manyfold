"""Private immutable records exchanged by delivery runtime components."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import final

from ._transport_delivery_events import (
    DeliveryCapacity,
    DeliveryCapacityDimension,
)

# Logical-size V2 is a capacity contract, not a measurement of SQLite pages.
# Its 160/128-byte row reservations budget the fixed lifecycle columns plus
# record/index overhead in the V2 schema; variable UTF-8 fields and payloads are
# then counted exactly once. A changed reservation requires a new logical-size
# version and a bounded migration that recomputes every retained row.
_LOGICAL_SIZE_VERSION = 2
_INBOX_FIXED_LOGICAL_BYTES_V2 = 128
_OUTBOX_FIXED_LOGICAL_BYTES_V2 = 160


def _outbox_logical_bytes(record: _OutboxRecord) -> int:
    return (
        _OUTBOX_FIXED_LOGICAL_BYTES_V2
        + _utf8_size(record.message_id)
        + _utf8_size(record.channel)
        + _optional_utf8_size(record.source_key)
        + _optional_utf8_size(record.correlation_id)
        + len(record.payload)
    )


def _inbox_logical_bytes(record: _InboxRecord) -> int:
    return (
        _INBOX_FIXED_LOGICAL_BYTES_V2
        + _utf8_size(record.message_id)
        + _utf8_size(record.channel)
        + _optional_utf8_size(record.correlation_id)
        + len(record.payload)
    )


def _utf8_size(value: str) -> int:
    return len(value.encode("utf-8"))


def _optional_utf8_size(value: str | None) -> int:
    return 0 if value is None else _utf8_size(value)


@final
class _InboxDisposition(str, Enum):
    NEW = "new"
    PENDING_DUPLICATE = "pending_duplicate"
    ACKED_DUPLICATE = "acked_duplicate"
    TERMINAL_DUPLICATE = "terminal_duplicate"
    EXPIRED_DUPLICATE = "expired_duplicate"


@final
class _OutboxDisposition(str, Enum):
    INSERTED = "inserted"
    DEDUPLICATED = "deduplicated"
    COALESCED = "coalesced"


@final
@dataclass(frozen=True, slots=True)
class _OutboxRecord:
    message_id: str
    channel: str
    semantics: str
    source_key: str | None
    frame_kind: int
    correlation_id: str | None
    payload: bytes
    attempts: int
    max_attempts: int


@final
@dataclass(frozen=True, slots=True)
class _InboxRecord:
    message_id: str
    frame_kind: int
    channel: str
    correlation_id: str | None
    payload: bytes
    delivery_attempt: int
    ack_attempts: int = 0
    status: str = "pending"
    outcome_reason: str | None = None


@final
@dataclass(frozen=True, slots=True)
class _LifecycleRecord:
    message_id: str
    channel: str
    source_key: str | None
    correlation_id: str | None
    attempts: int
    size_bytes: int


@final
@dataclass(frozen=True, slots=True)
class _JournalStats:
    outbox_items: int
    append_outbox_items: int
    latest_outbox_items: int
    pending_inbox_items: int
    acked_inbox_items: int
    terminal_inbox_items: int
    expired_inbox_items: int
    logical_bytes: int

    @property
    def retained_inbox_items(self) -> int:
        return (
            self.pending_inbox_items
            + self.acked_inbox_items
            + self.terminal_inbox_items
            + self.expired_inbox_items
        )


@final
@dataclass(frozen=True, slots=True)
class _TopicStats:
    channel: str
    outbox_items: int
    append_outbox_items: int
    latest_outbox_items: int
    inbox_items: int
    logical_bytes: int


@final
@dataclass(frozen=True, slots=True)
class _CompactionResult:
    expired_outbox: tuple[_LifecycleRecord, ...] = ()
    expired_inbox: tuple[_LifecycleRecord, ...] = ()
    retry_exhausted: tuple[_LifecycleRecord, ...] = ()
    released_outcomes: tuple[_LifecycleRecord, ...] = ()

    @property
    def affected_items(self) -> int:
        return (
            len(self.expired_outbox)
            + len(self.expired_inbox)
            + len(self.retry_exhausted)
            + len(self.released_outcomes)
        )

    @property
    def deleted_items(self) -> int:
        return (
            len(self.expired_outbox)
            + len(self.retry_exhausted)
            + len(self.released_outcomes)
        )

    @property
    def released_logical_bytes(self) -> int:
        return sum(
            record.size_bytes
            for records in (
                self.expired_outbox,
                self.retry_exhausted,
                self.released_outcomes,
            )
            for record in records
        )


@final
@dataclass(frozen=True, slots=True)
class _WatermarkCrossing:
    capacity: DeliveryCapacity
    dimensions: tuple[DeliveryCapacityDimension, ...]


@final
@dataclass(frozen=True, slots=True)
class _OutboxInsertResult:
    disposition: _OutboxDisposition
    capacity: DeliveryCapacity
    crossing: _WatermarkCrossing | None
    sweep: _CompactionResult | None
    replaced: _LifecycleRecord | None = None


@final
@dataclass(frozen=True, slots=True)
class _InboxInsertResult:
    disposition: _InboxDisposition
    capacity: DeliveryCapacity
    crossing: _WatermarkCrossing | None
    sweep: _CompactionResult | None


@final
@dataclass(frozen=True, slots=True)
class _ReplayCursor:
    created_at: float
    message_id: str


@final
@dataclass(frozen=True, slots=True)
class _OutboxReplayRecord:
    message_id: str
    channel: str
    source_key: str | None
    correlation_id: str | None
    attempts: int
    cursor: _ReplayCursor
