"""Typed, bounded, process-local lifecycle events for the transport mesh."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from queue import Empty, Full, Queue
from threading import RLock
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
    """Stable reasons that explain one lifecycle transition."""

    STARTUP = "startup"
    SHUTDOWN = "shutdown"
    DISCOVERY = "discovery"
    LISTENER = "listener"
    LINK_STATE_CHANGED = "link_state_changed"
    RECONNECT = "reconnect"
    PEER_REMOVED = "peer_removed"
    LOCAL_PUBLICATION = "local_publication"
    REMOTE_PUBLICATION = "remote_publication"
    REPLACED = "replaced"
    DUPLICATE = "duplicate"
    CAPACITY = "capacity"
    EXPIRY = "expiry"
    RETRY_SCHEDULED = "retry_scheduled"
    ACKNOWLEDGEMENT = "acknowledgement"
    RECOVERY = "recovery"
    DELIVERY_ATTEMPTS_EXHAUSTED = "delivery_attempts_exhausted"
    TRANSPORT_UNAVAILABLE = "transport_unavailable"
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
    related_message_id: str | None = None
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
    subscriber_count: int
    subscriber_dropped_events: int
    latest_sequence: int


@final
class MeshLifecycleSubscription:
    """Bounded pull subscription to one mesh's local lifecycle stream."""

    def __init__(
        self,
        lifecycle: "_MeshLifecycleLog",
        subscription_id: int,
        events: Queue[MeshLifecycleEvent],
    ) -> None:
        self._lifecycle = lifecycle
        self._subscription_id = subscription_id
        self._events = events
        self._is_disposed = False
        self._lock = RLock()

    @property
    def is_disposed(self) -> bool:
        """Return whether this subscription has been detached."""
        with self._lock:
            return self._is_disposed

    def __enter__(self) -> "MeshLifecycleSubscription":
        return self

    def __exit__(self, *error: object) -> None:
        self.dispose()

    def receive(self, *, timeout: float | None = None) -> MeshLifecycleEvent:
        """Receive the next event without invoking application code on mesh threads."""
        if (
            timeout is not None
            and (
                isinstance(timeout, bool)
                or not isinstance(timeout, int | float)
                or timeout < 0
            )
        ):
            raise ValueError("timeout must be non-negative or None")
        with self._lock:
            if self._is_disposed:
                raise RuntimeError("mesh lifecycle subscription is disposed")
        try:
            return self._events.get(timeout=timeout)
        except Empty as error:
            raise TimeoutError("no mesh lifecycle event arrived before timeout") from error

    def drain(self) -> tuple[MeshLifecycleEvent, ...]:
        """Remove and return all events currently queued for this subscriber."""
        events: list[MeshLifecycleEvent] = []
        while True:
            try:
                events.append(self._events.get_nowait())
            except Empty:
                return tuple(events)

    def dispose(self) -> bool:
        """Detach the subscriber once."""
        with self._lock:
            if self._is_disposed:
                return False
            self._is_disposed = True
        self._lifecycle.unsubscribe(self._subscription_id)
        return True


@final
class _MeshLifecycleLog:
    def __init__(self, node_id: str, limit: int) -> None:
        self._node_id = node_id
        self._events: deque[MeshLifecycleEvent] = deque(maxlen=limit)
        self._subscribers: dict[int, Queue[MeshLifecycleEvent]] = {}
        self._lock = RLock()
        self._next_sequence = 1
        self._next_subscription_id = 1
        self._dropped = 0
        self._subscriber_dropped = 0

    def publish(
        self,
        kind: MeshLifecycleKind,
        reason: MeshLifecycleReason,
        *,
        topic: str | None = None,
        peer_node_id: str | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
        related_message_id: str | None = None,
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
                related_message_id=related_message_id,
                attempt=attempt,
                item_count=item_count,
                byte_count=byte_count,
                detail=detail,
            )
            self._events.append(event)
            self._next_sequence += 1
            for events in self._subscribers.values():
                try:
                    events.put_nowait(event)
                except Full:
                    events.get_nowait()
                    self._subscriber_dropped += 1
                    events.put_nowait(event)
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

    def subscribe(
        self,
        *,
        after_sequence: int,
        queue_limit: int,
    ) -> MeshLifecycleSubscription:
        if (
            isinstance(after_sequence, bool)
            or not isinstance(after_sequence, int)
            or after_sequence < 0
        ):
            raise ValueError("after_sequence must be a non-negative integer")
        if (
            isinstance(queue_limit, bool)
            or not isinstance(queue_limit, int)
            or queue_limit < 1
        ):
            raise ValueError("queue_limit must be a positive integer")
        with self._lock:
            backlog = tuple(
                event for event in self._events if event.sequence > after_sequence
            )
            events: Queue[MeshLifecycleEvent] = Queue(maxsize=queue_limit)
            retained_backlog = backlog[-queue_limit:]
            self._subscriber_dropped += len(backlog) - len(retained_backlog)
            for event in retained_backlog:
                events.put_nowait(event)
            subscription_id = self._next_subscription_id
            self._next_subscription_id += 1
            self._subscribers[subscription_id] = events
        return MeshLifecycleSubscription(self, subscription_id, events)

    def unsubscribe(self, subscription_id: int) -> None:
        with self._lock:
            self._subscribers.pop(subscription_id, None)

    def health(self) -> MeshLifecycleHealth:
        with self._lock:
            return MeshLifecycleHealth(
                retained_events=len(self._events),
                dropped_events=self._dropped,
                subscriber_count=len(self._subscribers),
                subscriber_dropped_events=self._subscriber_dropped,
                latest_sequence=self._next_sequence - 1,
            )


__all__ = [
    "MeshLifecycleEvent",
    "MeshLifecycleHealth",
    "MeshLifecycleKind",
    "MeshLifecycleReason",
    "MeshLifecycleSubscription",
]
