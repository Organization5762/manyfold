"""Threading helpers for ManyFold data-stream processing.

This module carries the process-local scheduling patterns used by applications
that need to hand data-stream emissions between worker threads and a main thread.
It intentionally lives outside the top-level ``manyfold`` namespace so callers
opt in to the data-processing thread lifecycle explicitly.
"""

from __future__ import annotations

import os
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import IntEnum
from functools import partial
from itertools import count
from queue import Full, PriorityQueue
from threading import Lock, Thread, get_ident
from typing import Any, TypeVar

from . import streams
from .streams import (
    Disposable,
    EventLoopScheduler,
    Observable,
    SchedulerBase,
    Subject,
    ThreadPoolScheduler,
    TimeoutScheduler,
    operators as ops,
)

T = TypeVar("T")

StartableTarget = Callable[..., None]

DataStreamPriorityResolver = Callable[[], "DataStreamPriority"]


DEFAULT_MAIN_THREAD_DATASTREAM_QUEUE_LIMIT = 2048
DEFAULT_BACKGROUND_PRIORITY_QUEUE_LIMIT = 2048


def background_scheduler() -> SchedulerBase:
    """Return the shared scheduler for CPU-light background datastream work."""

    max_workers = _env_int(
        "MANYFOLD_DATASTREAM_BACKGROUND_MAX_WORKERS",
        legacy_name="HEART_DATASTREAM_BACKGROUND_MAX_WORKERS",
        default=4,
    )
    return _build_scheduler(
        _BACKGROUND_SCHEDULER,
        constructor=partial(ThreadPoolScheduler, max_workers=max_workers),
        max_workers=max_workers,
    )


def blocking_io_scheduler() -> SchedulerBase:
    """Return the shared scheduler for blocking data adapters."""

    max_workers = _env_int(
        "MANYFOLD_DATASTREAM_BLOCKING_IO_MAX_WORKERS",
        legacy_name="HEART_DATASTREAM_BLOCKING_IO_MAX_WORKERS",
        default=2,
    )
    return _build_scheduler(
        _BLOCKING_IO_SCHEDULER,
        constructor=partial(ThreadPoolScheduler, max_workers=max_workers),
        max_workers=max_workers,
    )


def input_scheduler() -> SchedulerBase:
    """Return the shared scheduler for human-input datastreams."""

    max_workers = _env_int(
        "MANYFOLD_DATASTREAM_INPUT_MAX_WORKERS",
        legacy_name="HEART_DATASTREAM_INPUT_MAX_WORKERS",
        default=2,
    )
    return _build_scheduler(
        _INPUT_SCHEDULER,
        constructor=partial(ThreadPoolScheduler, max_workers=max_workers),
        max_workers=max_workers,
    )


def interval_scheduler() -> SchedulerBase:
    """Return the shared event-loop scheduler used by interval datastreams."""

    return _build_scheduler(
        _INTERVAL_SCHEDULER,
        constructor=partial(
            EventLoopScheduler,
            thread_factory=create_default_thread_factory("datastream-interval"),
        ),
    )


def coalesce_scheduler() -> SchedulerBase:
    """Return the scheduler used for timer-based datastream coalescing."""

    return _COALESCE_SCHEDULER


def create_default_thread_factory(name: str) -> Callable[[StartableTarget], Thread]:
    """Build a non-daemon thread factory for datastream event-loop schedulers."""

    thread_name = _require_thread_name(name)

    def default_thread_factory(target: StartableTarget) -> Thread:
        return Thread(target=target, daemon=False, name=thread_name)

    return default_thread_factory


def interval_in_background(
    period: timedelta,
    *,
    name: str | None = None,
    scheduler: SchedulerBase | None = None,
) -> Observable[int]:
    """Emit integer ticks on a background scheduler until test reset shutdown fires."""

    period = _require_positive_timedelta(period)
    thread_name = None if name is None else _require_thread_name(name)
    resolved_scheduler = scheduler or (
        EventLoopScheduler(thread_factory=partial(_run_on_thread, name=thread_name))
        if thread_name is not None
        else interval_scheduler()
    )
    return streams.interval(period=period, scheduler=resolved_scheduler).pipe(
        ops.take_until(_interval_reset_signal),
    )


def drain_main_thread_queue(max_items: int | None = None) -> int:
    """Run pending main-thread callbacks and return the number drained."""

    if max_items is not None:
        if isinstance(max_items, bool) or not isinstance(max_items, int):
            raise ValueError("max_items must be an integer or None")
        if max_items < 0:
            raise ValueError("max_items must not be negative")
        if max_items == 0:
            return 0

    global _MAIN_THREAD_IDENT
    _MAIN_THREAD_IDENT = get_ident()
    drained = 0
    while True:
        with _MAIN_THREAD_QUEUE_LOCK:
            if not _MAIN_THREAD_QUEUE:
                break
            callback = _MAIN_THREAD_QUEUE.popleft()
        callback()
        drained += 1
        if max_items is not None and drained >= max_items:
            break
    return drained


def on_main_thread() -> bool:
    """Return whether the current thread last drained the main-thread queue."""

    return _MAIN_THREAD_IDENT == get_ident()


def deliver_on_main_thread(source: Observable[T]) -> Observable[T]:
    """Queue source emissions until ``drain_main_thread_queue`` is called."""
    _require_observable(source, "source")

    def _subscribe(
        observer: Any,
        scheduler: SchedulerBase | None = None,
    ) -> Disposable:
        disposed = False
        dispose_lock = Lock()
        pending_kind: str | None = None
        pending_value: Any = None
        pending_enqueued = False

        def _deliver(kind: str, value: Any = None) -> None:
            nonlocal pending_kind, pending_value, pending_enqueued
            with dispose_lock:
                if disposed:
                    return
                pending_kind = kind
                pending_value = value
                if pending_enqueued:
                    return
                pending_enqueued = True

            def _run() -> None:
                nonlocal disposed, pending_kind, pending_value, pending_enqueued
                with dispose_lock:
                    if disposed:
                        pending_kind = None
                        pending_value = None
                        pending_enqueued = False
                        return
                    kind_to_run = pending_kind
                    value_to_run = pending_value
                    pending_kind = None
                    pending_value = None
                    pending_enqueued = False
                if kind_to_run == "next":
                    observer.on_next(value_to_run)
                elif kind_to_run == "error":
                    observer.on_error(value_to_run)
                elif kind_to_run == "completed":
                    observer.on_completed()

            if not _enqueue_main_thread_task(_run):
                with dispose_lock:
                    pending_kind = None
                    pending_value = None
                    pending_enqueued = False
                observer.on_error(RuntimeError("main thread queue is full"))

        subscription = source.subscribe(
            on_next=lambda value: _deliver("next", value),
            on_error=lambda error: _deliver("error", error),
            on_completed=lambda: _deliver("completed"),
            scheduler=scheduler,
        )

        def _dispose() -> None:
            nonlocal disposed, pending_kind, pending_value, pending_enqueued
            with dispose_lock:
                disposed = True
                pending_kind = None
                pending_value = None
                pending_enqueued = False
            subscription.dispose()

        return Disposable(_dispose)

    return streams.create(_subscribe)


def deliver_on_background(
    source: Observable[T],
    *,
    priority: DataStreamPriority | DataStreamPriorityResolver | None = None,
) -> Observable[T]:
    """Queue source emissions onto a priority-aware background datastream worker."""

    _require_observable(source, "source")
    priority_resolver = _require_datastream_priority_resolver(priority)

    def _subscribe(
        observer: Any,
        scheduler: SchedulerBase | None = None,
    ) -> Disposable:
        disposed = False
        dispose_lock = Lock()
        pending_kind: str | None = None
        pending_value: Any = None
        pending_enqueued = False
        pending_generation = 0
        queued_priority: DataStreamPriority | None = None

        def _deliver(kind: str, value: Any = None) -> None:
            nonlocal pending_kind, pending_value, pending_enqueued
            nonlocal pending_generation, queued_priority
            try:
                resolved_priority = priority_resolver()
            except Exception as exc:
                observer.on_error(exc)
                return
            with dispose_lock:
                if disposed:
                    return
                pending_kind = kind
                pending_value = value
                if pending_enqueued and queued_priority == resolved_priority:
                    return
                pending_generation += 1
                generation_to_run = pending_generation
                pending_enqueued = True
                queued_priority = resolved_priority

            def _run() -> None:
                nonlocal disposed, pending_kind, pending_value, pending_enqueued
                nonlocal queued_priority
                with dispose_lock:
                    if generation_to_run != pending_generation:
                        return
                    if disposed:
                        pending_kind = None
                        pending_value = None
                        pending_enqueued = False
                        queued_priority = None
                        return
                    kind_to_run = pending_kind
                    value_to_run = pending_value
                    pending_kind = None
                    pending_value = None
                    pending_enqueued = False
                    queued_priority = None
                if kind_to_run == "next":
                    observer.on_next(value_to_run)
                elif kind_to_run == "error":
                    observer.on_error(value_to_run)
                elif kind_to_run == "completed":
                    observer.on_completed()

            if not _enqueue_background_priority_task(
                _run,
                priority=resolved_priority,
            ):
                with dispose_lock:
                    pending_kind = None
                    pending_value = None
                    pending_enqueued = False
                    queued_priority = None
                observer.on_error(RuntimeError("background priority queue is full"))

        subscription = source.subscribe(
            on_next=lambda value: _deliver("next", value),
            on_error=lambda error: _deliver("error", error),
            on_completed=lambda: _deliver("completed"),
            scheduler=scheduler,
        )

        def _dispose() -> None:
            nonlocal disposed, pending_kind, pending_value, pending_enqueued
            nonlocal queued_priority
            with dispose_lock:
                disposed = True
                pending_kind = None
                pending_value = None
                pending_enqueued = False
                queued_priority = None
            subscription.dispose()

        return Disposable(_dispose)

    return streams.create(_subscribe)


def reset_datastream_delivery_for_tests() -> None:
    """Reset module-level schedulers, queues, interval shutdown state."""

    global _BACKGROUND_PRIORITY_QUEUE, _MAIN_THREAD_IDENT, _MAIN_THREAD_QUEUE_LIMIT
    global _interval_reset_signal
    for state in (
        _BACKGROUND_SCHEDULER,
        _BLOCKING_IO_SCHEDULER,
        _INPUT_SCHEDULER,
        _INTERVAL_SCHEDULER,
    ):
        with state.lock:
            scheduler = state.scheduler
            state.scheduler = None
            state.max_workers = None
        _dispose_scheduler(scheduler)
    with _MAIN_THREAD_QUEUE_LOCK:
        _MAIN_THREAD_QUEUE.clear()
        _MAIN_THREAD_QUEUE_LIMIT = _main_thread_queue_limit()
    with _BACKGROUND_PRIORITY_LOCK:
        _stop_background_priority_worker_locked()
        _BACKGROUND_PRIORITY_QUEUE = _new_background_priority_queue()
    _MAIN_THREAD_IDENT = None
    _interval_reset_signal = Subject()


class DataStreamPriority(IntEnum):
    """Priority for queued data handoffs."""

    HIGH = 0
    NORMAL = 10
    LOW = 20


def _require_non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    if not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _require_thread_name(value: Any) -> str:
    return _require_non_empty_string(value, "thread name")


def _require_observable(value: Any, field: str) -> None:
    if not isinstance(value, Observable):
        raise ValueError(f"{field} must be an Observable")


def _require_positive_timedelta(value: Any) -> timedelta:
    if not isinstance(value, timedelta):
        raise ValueError("period must be a timedelta")
    if value <= timedelta(0):
        raise ValueError("period must be positive")
    return value


def _require_datastream_priority(value: Any) -> DataStreamPriority:
    if isinstance(value, DataStreamPriority):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("priority must be a DataStreamPriority")
    try:
        return DataStreamPriority(value)
    except ValueError as exc:
        raise ValueError("priority must be a DataStreamPriority") from exc


def _require_datastream_priority_resolver(
    value: DataStreamPriority | DataStreamPriorityResolver | None,
) -> DataStreamPriorityResolver:
    if value is None:
        return lambda: DataStreamPriority.NORMAL
    if callable(value):

        def _resolve() -> DataStreamPriority:
            return _require_datastream_priority(value())

        return _resolve
    priority = _require_datastream_priority(value)
    return lambda: priority


def _env_int(
    name: str,
    *,
    legacy_name: str,
    default: int,
    minimum: int = 1,
) -> int:
    source_name = name if name in os.environ else legacy_name
    raw_value = os.environ.get(source_name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{source_name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{source_name} must be at least {minimum}")
    return value


def _build_scheduler(
    state: _SchedulerState,
    *,
    constructor: Callable[[], SchedulerBase],
    max_workers: int | None = None,
) -> SchedulerBase:
    if state.scheduler is None:
        with state.lock:
            if state.scheduler is None:
                state.scheduler = constructor()
                state.max_workers = max_workers
    scheduler = state.scheduler
    if scheduler is None:
        raise RuntimeError("scheduler construction did not produce a scheduler")
    return scheduler


def _dispose_scheduler(scheduler: SchedulerBase | None) -> None:
    if scheduler is None:
        return
    dispose = getattr(scheduler, "dispose", None)
    if callable(dispose):
        dispose()


def _run_on_thread(target: StartableTarget, name: str) -> Thread:
    return Thread(target=target, daemon=False, name=_require_thread_name(name))


def _enqueue_main_thread_task(callback: Callable[[], None]) -> bool:
    with _MAIN_THREAD_QUEUE_LOCK:
        if len(_MAIN_THREAD_QUEUE) >= _MAIN_THREAD_QUEUE_LIMIT:
            return False
        _MAIN_THREAD_QUEUE.append(callback)
    return True


def _enqueue_background_priority_task(
    callback: Callable[[], None],
    *,
    priority: DataStreamPriority,
) -> bool:
    _ensure_background_priority_worker()
    task = _BackgroundPriorityTask(
        priority=int(priority),
        sequence=next(_BACKGROUND_PRIORITY_SEQUENCE),
        callback=callback,
    )
    try:
        _BACKGROUND_PRIORITY_QUEUE.put_nowait(task)
    except Full:
        return False
    return True


def _ensure_background_priority_worker() -> None:
    with _BACKGROUND_PRIORITY_LOCK:
        if _BACKGROUND_PRIORITY_WORKER.thread is not None:
            return
        thread = Thread(
            target=_run_background_priority_worker,
            args=(_BACKGROUND_PRIORITY_QUEUE,),
            name="manyfold-priority-background",
            daemon=True,
        )
        _BACKGROUND_PRIORITY_WORKER.thread = thread
        thread.start()


def _run_background_priority_worker(
    queue: PriorityQueue[_BackgroundPriorityTask],
) -> None:
    while True:
        task = queue.get()
        try:
            if task.stop:
                return
            task.callback()
        finally:
            queue.task_done()


def _stop_background_priority_worker_locked() -> None:
    thread = _BACKGROUND_PRIORITY_WORKER.thread
    if thread is None:
        return
    _BACKGROUND_PRIORITY_QUEUE.put(
        _BackgroundPriorityTask(
            priority=int(DataStreamPriority.HIGH),
            sequence=next(_BACKGROUND_PRIORITY_SEQUENCE),
            callback=lambda: None,
            stop=True,
        ),
        timeout=1.0,
    )
    thread.join(timeout=1.0)
    _BACKGROUND_PRIORITY_WORKER.thread = None


def _new_background_priority_queue() -> PriorityQueue[_BackgroundPriorityTask]:
    return PriorityQueue(
        maxsize=_env_int(
            "MANYFOLD_DATASTREAM_BACKGROUND_PRIORITY_QUEUE_LIMIT",
            legacy_name="HEART_DATASTREAM_BACKGROUND_PRIORITY_QUEUE_LIMIT",
            default=DEFAULT_BACKGROUND_PRIORITY_QUEUE_LIMIT,
        )
    )


def _main_thread_queue_limit() -> int:
    return _env_int(
        "MANYFOLD_DATASTREAM_MAIN_THREAD_QUEUE_LIMIT",
        legacy_name="HEART_DATASTREAM_MAIN_THREAD_QUEUE_LIMIT",
        default=DEFAULT_MAIN_THREAD_DATASTREAM_QUEUE_LIMIT,
    )


@dataclass
class _SchedulerState:
    lock: Lock
    scheduler: SchedulerBase | None = None
    max_workers: int | None = None


@dataclass(order=True)
class _BackgroundPriorityTask:
    priority: int
    sequence: int
    callback: Callable[[], None] = field(compare=False)
    stop: bool = field(default=False, compare=False)


@dataclass
class _BackgroundPriorityWorker:
    thread: Thread | None = None


_COALESCE_SCHEDULER = TimeoutScheduler()

_BACKGROUND_SCHEDULER = _SchedulerState(lock=Lock())

_BACKGROUND_PRIORITY_LOCK = Lock()

_BACKGROUND_PRIORITY_SEQUENCE = count()

_BACKGROUND_PRIORITY_QUEUE = _new_background_priority_queue()

_BACKGROUND_PRIORITY_WORKER = _BackgroundPriorityWorker()

_BLOCKING_IO_SCHEDULER = _SchedulerState(lock=Lock())

_INPUT_SCHEDULER = _SchedulerState(lock=Lock())

_INTERVAL_SCHEDULER = _SchedulerState(lock=Lock())

_MAIN_THREAD_QUEUE: deque[Callable[[], None]] = deque()

_MAIN_THREAD_QUEUE_LIMIT = _main_thread_queue_limit()

_MAIN_THREAD_QUEUE_LOCK = Lock()

_MAIN_THREAD_IDENT: int | None = None

_interval_reset_signal: Subject[Any] = Subject()


__all__ = (
    "DEFAULT_BACKGROUND_PRIORITY_QUEUE_LIMIT",
    "DEFAULT_MAIN_THREAD_DATASTREAM_QUEUE_LIMIT",
    "DataStreamPriority",
    "background_scheduler",
    "blocking_io_scheduler",
    "coalesce_scheduler",
    "create_default_thread_factory",
    "deliver_on_background",
    "deliver_on_main_thread",
    "drain_main_thread_queue",
    "input_scheduler",
    "interval_in_background",
    "interval_scheduler",
    "on_main_thread",
    "reset_datastream_delivery_for_tests",
)
