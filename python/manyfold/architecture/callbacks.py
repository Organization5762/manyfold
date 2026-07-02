"""Callback delivery placement for client-facing subscriptions."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread, get_ident
from typing import Any, Literal

CallbackPlacementKind = Literal["inline", "main", "thread"]
DEFAULT_CALLBACK_QUEUE_LIMIT = 2048

_main_thread_callbacks: deque[Callable[[], None]] = deque()
_main_thread_lock = Lock()
_main_thread_ident = None


def drain_main_thread_callbacks(max_items: int | None = None) -> int:
    """Run queued main-thread callback deliveries."""
    _validate_max_items(max_items)
    if max_items == 0:
        return 0

    global _main_thread_ident
    _main_thread_ident = get_ident()
    drained = 0
    while True:
        with _main_thread_lock:
            if not _main_thread_callbacks:
                break
            callback = _main_thread_callbacks.popleft()
        callback()
        drained += 1
        if max_items is not None and drained >= max_items:
            break
    return drained


def on_main_callback_thread() -> bool:
    """Return whether this thread last drained main-thread callbacks."""
    return _main_thread_ident == get_ident()


def reset_callback_delivery_for_tests() -> None:
    """Clear process-local callback delivery queues."""
    global _main_thread_ident
    with _main_thread_lock:
        _main_thread_callbacks.clear()
    _main_thread_ident = None


@dataclass(frozen=True, slots=True)
class CallbackPlacement:
    """Where a subscription callback should run."""

    kind: CallbackPlacementKind = "inline"
    thread_name: str | None = None
    queue_limit: int = DEFAULT_CALLBACK_QUEUE_LIMIT

    @classmethod
    def inline(cls) -> "CallbackPlacement":
        """Deliver callbacks synchronously on the publisher's thread."""
        return cls(kind="inline")

    @classmethod
    def main_thread(
        cls,
        *,
        queue_limit: int = DEFAULT_CALLBACK_QUEUE_LIMIT,
    ) -> "CallbackPlacement":
        """Queue callbacks until ``drain_main_thread_callbacks`` runs."""
        return cls(kind="main", queue_limit=queue_limit)

    @classmethod
    def spawned_thread(
        cls,
        name: str,
        *,
        queue_limit: int = DEFAULT_CALLBACK_QUEUE_LIMIT,
    ) -> "CallbackPlacement":
        """Deliver callbacks on one owned background thread."""
        return cls(
            kind="thread",
            thread_name=_require_thread_name(name),
            queue_limit=queue_limit,
        )

    def __post_init__(self) -> None:
        if self.kind not in ("inline", "main", "thread"):
            raise ValueError("callback placement kind must be inline, main, or thread")
        if self.kind == "thread":
            _require_thread_name(self.thread_name)
        if isinstance(self.queue_limit, bool) or not isinstance(self.queue_limit, int):
            raise ValueError("queue_limit must be an integer")
        if self.queue_limit < 1:
            raise ValueError("queue_limit must be positive")


class CallbackDelivery:
    """Deliver callback invocations according to a placement policy."""

    def __init__(self, placement: CallbackPlacement | None = None) -> None:
        self.placement = placement or CallbackPlacement.inline()
        self._closed = False
        self._lock = Lock()
        self._worker: _CallbackWorker | None = None
        if self.placement.kind == "thread":
            self._worker = _CallbackWorker(
                name=self.placement.thread_name or "manyfold-callback",
                queue_limit=self.placement.queue_limit,
            )

    def deliver(self, callback: Callable[[Any], object], value: Any) -> bool:
        """Deliver one callback invocation and return whether it was accepted."""
        return self.submit(lambda: callback(value))

    def submit(self, callback: Callable[[], object]) -> bool:
        """Submit one no-argument callback for placement-aware execution."""
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            if self._closed:
                return False
        if self.placement.kind == "inline":
            callback()
            return True
        if self.placement.kind == "main":
            return _enqueue_main_thread_callback(callback, self.placement.queue_limit)
        if self._worker is None:
            raise RuntimeError("callback worker is not initialized")
        return self._worker.submit(callback)

    def close(self) -> None:
        """Stop accepting callbacks and release owned worker resources."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if self._worker is not None:
            self._worker.close()


def _enqueue_main_thread_callback(
    callback: Callable[[], object],
    queue_limit: int,
) -> bool:
    with _main_thread_lock:
        if len(_main_thread_callbacks) >= queue_limit:
            return False
        _main_thread_callbacks.append(callback)
    return True


def _require_thread_name(value: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("thread name must be a non-empty string")
    return value.strip()


def _validate_max_items(max_items: int | None) -> None:
    if max_items is None:
        return
    if isinstance(max_items, bool) or not isinstance(max_items, int):
        raise ValueError("max_items must be an integer or None")
    if max_items < 0:
        raise ValueError("max_items must not be negative")


class _CallbackWorker:
    def __init__(self, *, name: str, queue_limit: int) -> None:
        self._queue: Queue[Callable[[], object] | None] = Queue(maxsize=queue_limit)
        self._closed = Event()
        self._thread = Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    def submit(self, callback: Callable[[], object]) -> bool:
        if self._closed.is_set():
            return False
        try:
            self._queue.put_nowait(callback)
        except Full:
            return False
        return True

    def close(self) -> None:
        self._closed.set()
        try:
            self._queue.put_nowait(None)
        except Full:
            pass
        self._thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self._closed.is_set():
            try:
                callback = self._queue.get(timeout=0.1)
            except Empty:
                continue
            try:
                if callback is None:
                    return
                callback()
            finally:
                self._queue.task_done()


__all__ = [
    "CallbackDelivery",
    "CallbackPlacement",
    "DEFAULT_CALLBACK_QUEUE_LIMIT",
    "drain_main_thread_callbacks",
    "on_main_callback_thread",
    "reset_callback_delivery_for_tests",
]
