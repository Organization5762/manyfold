"""Private value records for durable transport delivery storage."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class _InboxDisposition(str, Enum):
    NEW = "new"
    PENDING_DUPLICATE = "pending_duplicate"
    ACKED_DUPLICATE = "acked_duplicate"


class _OutboxDisposition(str, Enum):
    INSERTED = "inserted"
    DEDUPLICATED = "deduplicated"
    REPLACED = "replaced"


@dataclass(frozen=True, slots=True)
class _OutboxRecord:
    message_id: str
    topic: str
    semantics: str
    source_key: str | None
    frame_kind: int
    channel: str
    correlation_id: str | None
    payload: bytes
    attempts: int
    max_attempts: int


@dataclass(frozen=True, slots=True)
class _OutboxReplayRecord:
    message_id: str
    topic: str
    source_key: str | None
    correlation_id: str | None
    attempts: int


@dataclass(frozen=True, slots=True)
class _InboxRecord:
    message_id: str
    frame_kind: int
    channel: str
    correlation_id: str | None
    payload: bytes
    delivery_attempt: int
    ack_attempts: int = 0


@dataclass(frozen=True, slots=True)
class _JournalStats:
    outbox_items: int
    append_outbox_items: int
    latest_outbox_items: int
    pending_inbox_items: int
    acked_inbox_items: int
    logical_bytes: int


@dataclass(frozen=True, slots=True)
class _TopicStats:
    outbox_items: int
    append_outbox_items: int
    latest_outbox_items: int
    inbox_items: int
    logical_bytes: int


@dataclass(frozen=True, slots=True)
class _OutboxTransition:
    message_id: str
    topic: str
    source_key: str | None
    correlation_id: str | None
    attempts: int


@dataclass(frozen=True, slots=True)
class _InboxTransition:
    message_id: str
    topic: str
    correlation_id: str | None
    delivery_attempt: int


@dataclass(frozen=True, slots=True)
class _CompactionResult:
    expired_outbox: tuple[_OutboxTransition, ...]
    exhausted_outbox: tuple[_OutboxTransition, ...]
    expired_inbox: tuple[_InboxTransition, ...]


@dataclass(frozen=True, slots=True)
class _OutboxUsage:
    items: int
    logical_bytes: int
    topic_items: int
    topic_bytes: int


@dataclass(frozen=True, slots=True)
class _OutboxInsertResult:
    disposition: _OutboxDisposition
    replaced_message_id: str | None = None
    expired_outbox: tuple[_OutboxTransition, ...] = ()
    soft_compaction: bool = False
    capacity: _OutboxUsage | None = None


@dataclass(frozen=True, slots=True)
class _RecoveredTopicPolicy:
    semantics: str
    max_items: int
    max_bytes: int
    max_inbox_items: int
    max_inbox_bytes: int
    latest_per_source: bool = False
