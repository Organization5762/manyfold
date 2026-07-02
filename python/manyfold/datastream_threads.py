"""Threading helpers for ManyFold data-stream processing.

This module carries the process-local scheduling patterns used by applications
that need to hand data-stream emissions between worker threads and a main thread.
It intentionally lives outside the top-level ``manyfold`` namespace so callers
opt in to the data-processing thread lifecycle explicitly.
"""

from __future__ import annotations

import logging
import math
import os
import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from enum import IntEnum
from functools import partial
from itertools import count
from queue import Full, PriorityQueue
from threading import Event, Lock, Thread, get_ident
from typing import Any, TypeVar

from . import streams
from .streams import (
    Disposable,
    EventLoopScheduler,
    NewThreadScheduler,
    Observable,
    SchedulerBase,
    Subject,
    ThreadPoolScheduler,
    TimeoutScheduler,
    operators as ops,
    pipe,
)

DisposableBase = Disposable

T = TypeVar("T")

TStarting = TypeVar("TStarting")

StartableTarget = Callable[..., None]

DataStreamPriorityResolver = Callable[[], "DataStreamPriority"]


logger = logging.getLogger(__name__)

MAIN_THREAD_DATASTREAM_LATENCY = "main_thread_datastream_handoff"

DEFAULT_DELIVERY_LATENCY_HISTORY_SIZE = 2048
DEFAULT_MAIN_THREAD_DATASTREAM_QUEUE_LIMIT = 2048
DEFAULT_BACKGROUND_PRIORITY_QUEUE_LIMIT = 2048


class _NoStartingValue:
    pass


_NO_STARTING_VALUE = _NoStartingValue()


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


def replay_scheduler() -> TimeoutScheduler:
    """Return a fresh timeout scheduler for replay subjects."""

    return TimeoutScheduler()


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
    """Emit integer ticks on a background scheduler until ``shutdown_signal`` fires."""

    period = _require_positive_timedelta(period)
    thread_name = None if name is None else _require_thread_name(name)
    resolved_scheduler = scheduler or (
        EventLoopScheduler(thread_factory=partial(_run_on_thread, name=thread_name))
        if thread_name is not None
        else interval_scheduler()
    )
    return streams.interval(period=period, scheduler=resolved_scheduler).pipe(
        ops.take_until(shutdown_signal),
    )


def delivery_latency_snapshot() -> dict[str, DeliveryLatencyStats]:
    """Return latency summaries for queued main-thread deliveries."""

    return _LATENCY_RECORDER.snapshot()


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
            task = _MAIN_THREAD_QUEUE.popleft()
        _LATENCY_RECORDER.record(
            MAIN_THREAD_DATASTREAM_LATENCY,
            time.monotonic() - task.enqueued_monotonic,
        )
        task.callback()
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
    ) -> DisposableBase:
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
    ) -> DisposableBase:
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


def start_with_once(value: TStarting) -> Callable[[Observable[T]], Observable[Any]]:
    """Prepend one value to a datastream for each subscription."""

    def _start(source: Observable[T]) -> Observable[Any]:
        return source.pipe(ops.start_with(value))

    return _start


def pipe_on_background(
    source: Observable[T],
    *operators: Any,
    starting_value: TStarting | _NoStartingValue = _NO_STARTING_VALUE,
) -> Observable[Any]:
    """Apply operators to a source observable."""

    _require_observable(source, "source")
    logger.debug("Building background pipeline.")
    resolved_operators = operators
    if not isinstance(starting_value, _NoStartingValue):
        resolved_operators = (*operators, start_with_once(starting_value))
    return pipe(source, *resolved_operators)


def pipe_on_main_thread(source: Observable[T], *operators: Any) -> Observable[Any]:
    """Deliver source emissions on the main thread and apply operators."""

    _require_observable(source, "source")
    logger.debug("Building main-thread pipeline.")
    return pipe(deliver_on_main_thread(source), *operators)


def pipe_to_background_event_loop(
    source: Observable[T],
    name: str,
    *operators: Any,
) -> Observable[Any]:
    """Pipe a datastream through an event loop running on a background thread."""

    _require_observable(source, "source")
    scheduler = EventLoopScheduler(
        thread_factory=partial(_run_on_thread, name=_require_thread_name(name))
    )
    return pipe(source, ops.observe_on(scheduler), *operators)


def pipe_to_background_thread(
    source: Observable[T],
    name: str,
    *operators: Any,
) -> Observable[Any]:
    """Pipe a datastream through a dedicated background thread executor."""

    _require_observable(source, "source")
    scheduler = NewThreadScheduler(
        thread_factory=partial(_run_on_thread, name=_require_thread_name(name))
    )
    return pipe(source, ops.observe_on(scheduler), *operators)


@contextmanager
def background_datastream_observable(
    observable: Observable[T],
    name: str,
) -> Iterator[Observable[T]]:
    """Subscribe to a data observable on a daemon thread and yield its subject."""

    _require_observable(observable, "observable")
    thread_name = _require_thread_name(name)
    logger.debug("Starting background datastream thread %s", thread_name)
    subject: Subject[T] = Subject()
    ready = Event()
    run = _BackgroundObservableRun()
    thread = Thread(
        name=thread_name,
        target=_run_background_observable,
        args=(subject, observable, ready, run),
        daemon=True,
    )
    thread.start()
    ready.wait()
    if run.error is not None:
        raise run.error
    try:
        yield subject
    finally:
        if run.subscription is not None:
            run.subscription.dispose()
        subject.on_completed()


def scheduler_diagnostics() -> dict[str, int | None]:
    """Return max-worker settings for initialized shared schedulers."""

    with _MAIN_THREAD_QUEUE_LOCK:
        main_thread_queue_depth = len(_MAIN_THREAD_QUEUE)
    return {
        "background_max_workers": _BACKGROUND_SCHEDULER.max_workers,
        "background_priority_queue_limit": _BACKGROUND_PRIORITY_QUEUE.maxsize,
        "blocking_io_max_workers": _BLOCKING_IO_SCHEDULER.max_workers,
        "main_thread_queue_depth": main_thread_queue_depth,
        "main_thread_queue_limit": _MAIN_THREAD_QUEUE_LIMIT,
        "input_max_workers": _INPUT_SCHEDULER.max_workers,
    }


def reset_datastream_delivery_for_tests() -> None:
    """Reset module-level schedulers, queues, shutdown_signal, and latency state."""

    global _BACKGROUND_PRIORITY_QUEUE, _MAIN_THREAD_IDENT, _MAIN_THREAD_QUEUE_LIMIT
    global shutdown_signal
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
    _LATENCY_RECORDER.clear()
    shutdown_signal = Subject()


def materialize_sequence(sequence: Iterable[T]) -> Observable[T]:
    """Return a materialized observable over an iterable sequence."""

    if not isinstance(sequence, Iterable):
        raise ValueError("sequence must be iterable")
    snapshot = tuple(sequence)
    return streams.from_iterable(snapshot)


class DataStreamPriority(IntEnum):
    """Priority for queued data handoffs."""

    HIGH = 0
    NORMAL = 10
    LOW = 20


@dataclass(frozen=True, slots=True)
class DeliveryLatencyStats:
    """Percentile summary of datastream delivery latency."""

    count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float

    def __post_init__(self) -> None:
        if not isinstance(self.count, int) or isinstance(self.count, bool):
            raise ValueError("count must be an integer")
        if self.count <= 0:
            raise ValueError("count must be positive")
        values = (
            _require_finite_non_negative_number(self.p50_ms, "p50_ms"),
            _require_finite_non_negative_number(self.p95_ms, "p95_ms"),
            _require_finite_non_negative_number(self.p99_ms, "p99_ms"),
            _require_finite_non_negative_number(self.max_ms, "max_ms"),
        )
        if values != tuple(sorted(values)):
            raise ValueError("latency percentiles must be ordered")


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


def _require_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a number")
    return float(value)


def _require_finite_non_negative_number(value: Any, field: str) -> float:
    value = _require_number(value, field)
    if not math.isfinite(value):
        raise ValueError(f"{field} must be finite")
    if value < 0.0:
        raise ValueError(f"{field} must not be negative")
    return value


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


def _latency_stats(values: tuple[float, ...]) -> DeliveryLatencyStats:
    ordered = sorted(values)
    return DeliveryLatencyStats(
        count=len(ordered),
        p50_ms=_percentile_ms(ordered, 0.50),
        p95_ms=_percentile_ms(ordered, 0.95),
        p99_ms=_percentile_ms(ordered, 0.99),
        max_ms=ordered[-1] * 1000.0,
    )


def _percentile_ms(ordered_values: list[float], percentile: float) -> float:
    index = max(0, math.ceil(percentile * len(ordered_values)) - 1)
    return ordered_values[index] * 1000.0


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
    task = _MainThreadTask(
        callback=callback,
        enqueued_monotonic=time.monotonic(),
    )
    with _MAIN_THREAD_QUEUE_LOCK:
        if len(_MAIN_THREAD_QUEUE) >= _MAIN_THREAD_QUEUE_LIMIT:
            return False
        _MAIN_THREAD_QUEUE.append(task)
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


def _run_background_observable(
    subject: Subject[T],
    observable: Observable[T],
    ready: Event,
    run: _BackgroundObservableRun,
) -> None:
    try:
        run.subscription = observable.subscribe(subject)
    except BaseException as exc:
        run.error = exc
    finally:
        ready.set()


@dataclass
class _SchedulerState:
    lock: Lock
    scheduler: SchedulerBase | None = None
    max_workers: int | None = None


@dataclass
class _MainThreadTask:
    callback: Callable[[], None]
    enqueued_monotonic: float


@dataclass(order=True)
class _BackgroundPriorityTask:
    priority: int
    sequence: int
    callback: Callable[[], None] = field(compare=False)
    stop: bool = field(default=False, compare=False)


@dataclass
class _BackgroundObservableRun:
    subscription: DisposableBase | None = None
    error: BaseException | None = None


@dataclass
class _BackgroundPriorityWorker:
    thread: Thread | None = None


class _LatencyRecorder:
    def __init__(
        self, history_size: int = DEFAULT_DELIVERY_LATENCY_HISTORY_SIZE
    ) -> None:
        if (
            not isinstance(history_size, int)
            or isinstance(history_size, bool)
            or history_size <= 0
        ):
            raise ValueError("history_size must be a positive integer")
        self._history_size = history_size
        self._history: dict[str, deque[float]] = {}
        self._lock = Lock()

    def record(self, stream_name: str, delay_s: float) -> None:
        stream_name = _require_non_empty_string(stream_name, "stream_name")
        delay_s = _require_number(delay_s, "delay_s")
        if not math.isfinite(delay_s):
            delay_s = 0.0
        with self._lock:
            history = self._history.get(stream_name)
            if history is None:
                history = deque(maxlen=self._history_size)
                self._history[stream_name] = history
            history.append(max(delay_s, 0.0))

    def snapshot(self) -> dict[str, DeliveryLatencyStats]:
        with self._lock:
            history = {
                stream_name: tuple(values)
                for stream_name, values in self._history.items()
                if values
            }
        return {
            stream_name: _latency_stats(values)
            for stream_name, values in sorted(history.items())
        }

    def clear(self) -> None:
        with self._lock:
            self._history.clear()


_COALESCE_SCHEDULER = TimeoutScheduler()

_BACKGROUND_SCHEDULER = _SchedulerState(lock=Lock())

_BACKGROUND_PRIORITY_LOCK = Lock()

_BACKGROUND_PRIORITY_SEQUENCE = count()

_BACKGROUND_PRIORITY_QUEUE = _new_background_priority_queue()

_BACKGROUND_PRIORITY_WORKER = _BackgroundPriorityWorker()

_BLOCKING_IO_SCHEDULER = _SchedulerState(lock=Lock())

_INPUT_SCHEDULER = _SchedulerState(lock=Lock())

_INTERVAL_SCHEDULER = _SchedulerState(lock=Lock())

_MAIN_THREAD_QUEUE: deque[_MainThreadTask] = deque()

_MAIN_THREAD_QUEUE_LIMIT = _main_thread_queue_limit()

_MAIN_THREAD_QUEUE_LOCK = Lock()

_MAIN_THREAD_IDENT: int | None = None

_LATENCY_RECORDER = _LatencyRecorder()

shutdown_signal: Subject[Any] = Subject()


__all__ = (
    "DEFAULT_BACKGROUND_PRIORITY_QUEUE_LIMIT",
    "DEFAULT_DELIVERY_LATENCY_HISTORY_SIZE",
    "DEFAULT_MAIN_THREAD_DATASTREAM_QUEUE_LIMIT",
    "DataStreamPriority",
    "DeliveryLatencyStats",
    "MAIN_THREAD_DATASTREAM_LATENCY",
    "background_datastream_observable",
    "background_scheduler",
    "blocking_io_scheduler",
    "coalesce_scheduler",
    "create_default_thread_factory",
    "deliver_on_background",
    "deliver_on_main_thread",
    "delivery_latency_snapshot",
    "drain_main_thread_queue",
    "input_scheduler",
    "interval_in_background",
    "interval_scheduler",
    "materialize_sequence",
    "on_main_thread",
    "pipe_on_background",
    "pipe_on_main_thread",
    "pipe_to_background_event_loop",
    "pipe_to_background_thread",
    "replay_scheduler",
    "reset_datastream_delivery_for_tests",
    "scheduler_diagnostics",
    "shutdown_signal",
    "start_with_once",
)
