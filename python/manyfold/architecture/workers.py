"""Worker identity and lifecycle events for attachable Manyfold runtimes."""

from __future__ import annotations

import os
import socket
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from time import time_ns

from .pubsub import PubSub

DEFAULT_WORKER_TOPIC = "manyfold.workers"


@dataclass(frozen=True)
class WorkerRef:
    """Stable identity for one Manyfold worker execution context."""

    name: str
    node: str
    process_id: int
    thread_id: int
    address: str
    roles: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkerEvent:
    """Lifecycle and capacity sample published by an attached worker."""

    worker: str
    node: str
    process_id: int
    thread_id: int
    address: str
    event: str
    capacity: int
    active_jobs: int
    available_jobs: int
    roles: str
    event_time_ns: int


class WorkerHandle:
    """Handle for publishing lifecycle updates from an attached worker."""

    def __init__(
        self,
        *,
        worker: WorkerRef,
        pubsub: PubSub,
        capacity: int,
    ) -> None:
        self.worker = worker
        self._pubsub = pubsub
        self._capacity = capacity
        self._is_detached = False

    @property
    def is_detached(self) -> bool:
        """Return whether this worker has already published detach."""
        return self._is_detached

    def heartbeat(self, *, active_jobs: int = 0, capacity: int | None = None) -> None:
        """Publish a capacity sample for this worker."""
        if self._is_detached:
            raise RuntimeError("detached workers cannot publish heartbeat events")
        if capacity is not None:
            self._capacity = _require_capacity(capacity)
        self._publish("heartbeat", active_jobs=active_jobs)

    def detach(self) -> bool:
        """Publish detach once and mark this handle inactive."""
        if self._is_detached:
            return False
        self._publish("detached", active_jobs=0)
        self._is_detached = True
        return True

    def _publish(self, event: str, *, active_jobs: int) -> None:
        active_jobs = _require_nonnegative_int(active_jobs, "active_jobs")
        available_jobs = max(self._capacity - active_jobs, 0)
        self._pubsub.publish(
            WorkerEvent(
                worker=self.worker.name,
                node=self.worker.node,
                process_id=self.worker.process_id,
                thread_id=self.worker.thread_id,
                address=self.worker.address,
                event=event,
                capacity=self._capacity,
                active_jobs=active_jobs,
                available_jobs=available_jobs,
                roles=",".join(self.worker.roles),
                event_time_ns=time_ns(),
            )
        )


class WorkerRuntime:
    """Attach the current process or thread to a PubSub-backed worker model."""

    def __init__(self, *, pubsub: PubSub | None = None) -> None:
        self.pubsub = pubsub or PubSub(topic=DEFAULT_WORKER_TOPIC, schema=WorkerEvent)

    def attach_current_thread(
        self,
        name: str,
        *,
        roles: Sequence[str] = (),
        capacity: int = 1,
        address: str | None = None,
    ) -> WorkerHandle:
        """Attach the current thread as a distinct Manyfold worker."""
        capacity = _require_capacity(capacity)
        worker = WorkerRef(
            name=_require_text(name, "worker name"),
            node=socket.gethostname(),
            process_id=os.getpid(),
            thread_id=threading.get_ident(),
            address=address or f"thread://{os.getpid()}/{threading.get_ident()}",
            roles=tuple(_require_text(role, "worker role") for role in roles),
        )
        handle = WorkerHandle(worker=worker, pubsub=self.pubsub, capacity=capacity)
        handle._publish("attached", active_jobs=0)
        return handle


def _require_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _require_capacity(value: int) -> int:
    value = _require_nonnegative_int(value, "capacity")
    if value == 0:
        raise ValueError("capacity must be positive")
    return value


def _require_nonnegative_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must not be negative")
    return value


__all__ = [
    "DEFAULT_WORKER_TOPIC",
    "WorkerEvent",
    "WorkerHandle",
    "WorkerRef",
    "WorkerRuntime",
]
