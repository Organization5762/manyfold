"""Typed, bounded, process-local lifecycle events for the transport mesh."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from time import time
from typing import final


class MeshLifecycleKind(str, Enum):
    """Stable event kinds emitted at meaningful mesh transition boundaries."""

    RUNTIME_STARTING = "runtime_starting"
    RUNTIME_READY = "runtime_ready"
    RUNTIME_STOPPING = "runtime_stopping"
    RUNTIME_STOPPED = "runtime_stopped"
    PEER_DISCOVERED = "peer_discovered"
    PEER_CONNECTING = "peer_connecting"
    PEER_CONNECTED = "peer_connected"
    PEER_DISCONNECTED = "peer_disconnected"
    PEER_RECONNECTING = "peer_reconnecting"
    DURABLE_ENQUEUED = "durable_enqueued"
    DURABLE_COALESCED = "durable_coalesced"
    DURABLE_DROPPED = "durable_dropped"
    DURABLE_EXPIRED = "durable_expired"
    DURABLE_RETRY = "durable_retry"
    DURABLE_SENT = "durable_sent"
    DURABLE_ACKED = "durable_acked"
    DURABLE_REPLAYED = "durable_replayed"
    WATERMARK_CROSSED = "watermark_crossed"
    WATERMARK_RECOVERED = "watermark_recovered"
    DELIVERY_FAILED = "delivery_failed"


class MeshLifecycleReason(str, Enum):
    """Stable reasons that explain why one lifecycle transition occurred."""

    STARTUP = "startup"
    SHUTDOWN = "shutdown"
    DISCOVERY = "discovery"
    LISTENER = "listener"
    LINK_STATE_CHANGED = "link_state_changed"
    LOCAL_PUBLICATION = "local_publication"
    REMOTE_PUBLICATION = "remote_publication"
    SUBSCRIPTION = "subscription"
    RETRY = "retry"
    RECONNECT = "reconnect"
    RECOVERY = "recovery"
    ACKNOWLEDGEMENT = "acknowledgement"
    CAPACITY = "capacity"
    EXPIRY = "expiry"
    ERROR = "error"


@final
@dataclass(frozen=True, slots=True)
class MeshLifecycleEvent:
    """One immutable local event ordered by ``sequence`` within a mesh."""

    sequence: int
    kind: MeshLifecycleKind
    reason: MeshLifecycleReason
    node_id: str
    occurred_at: float
    topic: str | None = None
    peer_node_id: str | None = None
    message_id: str | None = None
    correlation_id: str | None = None
    attempt: int | None = None
    item_count: int | None = None
    byte_count: int | None = None
    detail: str | None = None


@final
@dataclass(frozen=True, slots=True)
class MeshLifecycleHealth:
    """Bounded retention state for one process-local lifecycle stream."""

    retained_events: int
    dropped_events: int
    latest_sequence: int


@final
class _MeshLifecycleLog:
    def __init__(self, node_id: str, limit: int) -> None:
        self._node_id = node_id
        self._events: deque[MeshLifecycleEvent] = deque(maxlen=limit)
        self._lock = Lock()
        self._next_sequence = 1
        self._dropped = 0

    def publish(
        self,
        kind: MeshLifecycleKind,
        reason: MeshLifecycleReason,
        *,
        topic: str | None = None,
        peer_node_id: str | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
        attempt: int | None = None,
        item_count: int | None = None,
        byte_count: int | None = None,
        detail: str | None = None,
    ) -> MeshLifecycleEvent:
        with self._lock:
            if len(self._events) == self._events.maxlen:
                self._dropped += 1
            event = MeshLifecycleEvent(
                sequence=self._next_sequence,
                kind=kind,
                reason=reason,
                node_id=self._node_id,
                occurred_at=time(),
                topic=topic,
                peer_node_id=peer_node_id,
                message_id=message_id,
                correlation_id=correlation_id,
                attempt=attempt,
                item_count=item_count,
                byte_count=byte_count,
                detail=detail,
            )
            self._events.append(event)
            self._next_sequence += 1
            return event

    def read(self, *, after_sequence: int = 0) -> tuple[MeshLifecycleEvent, ...]:
        if (
            isinstance(after_sequence, bool)
            or not isinstance(after_sequence, int)
            or after_sequence < 0
        ):
            raise ValueError("after_sequence must be a non-negative integer")
        with self._lock:
            return tuple(
                event for event in self._events if event.sequence > after_sequence
            )

    def health(self) -> MeshLifecycleHealth:
        with self._lock:
            return MeshLifecycleHealth(
                retained_events=len(self._events),
                dropped_events=self._dropped,
                latest_sequence=self._next_sequence - 1,
            )


__all__ = [
    "MeshLifecycleEvent",
    "MeshLifecycleHealth",
    "MeshLifecycleKind",
    "MeshLifecycleReason",
]
