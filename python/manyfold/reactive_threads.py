"""Threading helpers for Rx-backed Manyfold streams.

This module carries the process-local scheduling patterns used by applications
that need to hand stream emissions between worker threads and a frame thread.
It intentionally lives outside the top-level ``manyfold`` namespace so callers
opt in to the thread lifecycle surface explicitly.
"""

from __future__ import annotations

import logging
import math
import os
import time
from collections import defaultdict, deque
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from threading import Event, Lock, Thread, get_ident
from typing import Any, TypeVar

from . import _rx as rx
from ._rx import Observable, Subject, operators as ops, pipe
from ._rx.abc import DisposableBase, SchedulerBase
from ._rx.disposable import Disposable
from ._rx.scheduler import (
    EventLoopScheduler,
    NewThreadScheduler,
    ThreadPoolScheduler,
    TimeoutScheduler,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
TStarting = TypeVar("TStarting")
StartableTarget = Callable[..., None]

FRAME_THREAD_LATENCY_STREAM = "frame_thread_handoff"
DEFAULT_DELIVERY_LATENCY_HISTORY_SIZE = 2048


@dataclass(frozen=True, slots=True)
class DeliveryLatencyStats:
    """Percentile summary of stream delivery latency."""

    count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass
class _SchedulerState:
    lock: Lock
    scheduler: SchedulerBase | None = None
    max_workers: int | None = None


@dataclass
class _FrameThreadTask:
    callback: Callable[[], None]
    enqueued_monotonic: float


@dataclass
class _BackgroundObservableRun:
    subscription: DisposableBase | None = None
    error: BaseException | None = None


class _NoStartingValue:
    pass


_NO_STARTING_VALUE = _NoStartingValue()


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
        self._history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self._history_size)
        )
        self._lock = Lock()

    def record(self, stream_name: str, delay_s: float) -> None:
        if not isinstance(stream_name, str) or not stream_name.strip():
            raise ValueError("stream_name must be a non-empty string")
        if not math.isfinite(delay_s):
            delay_s = 0.0
        with self._lock:
            self._history[stream_name].append(max(delay_s, 0.0))

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
_BLOCKING_IO_SCHEDULER = _SchedulerState(lock=Lock())
_INPUT_SCHEDULER = _SchedulerState(lock=Lock())
_INTERVAL_SCHEDULER = _SchedulerState(lock=Lock())
_FRAME_THREAD_QUEUE: deque[_FrameThreadTask] = deque()
_FRAME_THREAD_QUEUE_LOCK = Lock()
_FRAME_THREAD_IDENT: int | None = None
_LATENCY_RECORDER = _LatencyRecorder()

shutdown: Subject[Any] = Subject()


def background_scheduler() -> SchedulerBase:
    """Return the shared scheduler for CPU-light background stream work."""

    max_workers = _env_int(
        "MANYFOLD_RX_BACKGROUND_MAX_WORKERS",
        legacy_name="HEART_RX_BACKGROUND_MAX_WORKERS",
        default=4,
    )
    return _build_scheduler(
        _BACKGROUND_SCHEDULER,
        constructor=partial(ThreadPoolScheduler, max_workers=max_workers),
        max_workers=max_workers,
    )


def blocking_io_scheduler() -> SchedulerBase:
    """Return the shared scheduler for blocking stream adapters."""

    max_workers = _env_int(
        "MANYFOLD_RX_BLOCKING_IO_MAX_WORKERS",
        legacy_name="HEART_RX_BLOCKING_IO_MAX_WORKERS",
        default=2,
    )
    return _build_scheduler(
        _BLOCKING_IO_SCHEDULER,
        constructor=partial(ThreadPoolScheduler, max_workers=max_workers),
        max_workers=max_workers,
    )


def input_scheduler() -> SchedulerBase:
    """Return the shared scheduler for human-input streams."""

    max_workers = _env_int(
        "MANYFOLD_RX_INPUT_MAX_WORKERS",
        legacy_name="HEART_RX_INPUT_MAX_WORKERS",
        default=2,
    )
    return _build_scheduler(
        _INPUT_SCHEDULER,
        constructor=partial(ThreadPoolScheduler, max_workers=max_workers),
        max_workers=max_workers,
    )


def interval_scheduler() -> SchedulerBase:
    """Return the shared event-loop scheduler used by interval streams."""

    return _build_scheduler(
        _INTERVAL_SCHEDULER,
        constructor=partial(
            EventLoopScheduler,
            thread_factory=create_default_thread_factory("reactive-interval"),
        ),
    )


def coalesce_scheduler() -> SchedulerBase:
    """Return the scheduler used for timer-based stream coalescing."""

    return _COALESCE_SCHEDULER


def replay_scheduler() -> TimeoutScheduler:
    """Return a fresh timeout scheduler for replay subjects."""

    return TimeoutScheduler()


def create_default_thread_factory(name: str) -> Callable[[StartableTarget], Thread]:
    """Build a non-daemon thread factory for Rx event-loop schedulers."""

    def default_thread_factory(target: StartableTarget) -> Thread:
        return Thread(target=target, daemon=False, name=name)

    return default_thread_factory


def interval_in_background(
    period: timedelta,
    *,
    name: str | None = None,
    scheduler: SchedulerBase | None = None,
) -> Observable[int]:
    """Emit integer ticks on a background scheduler until ``shutdown`` fires."""

    resolved_scheduler = scheduler or (
        EventLoopScheduler(thread_factory=partial(_run_on_thread, name=name))
        if name is not None
        else interval_scheduler()
    )
    return rx.interval(period=period, scheduler=resolved_scheduler).pipe(
        ops.take_until(shutdown),
    )


def delivery_latency_snapshot() -> dict[str, DeliveryLatencyStats]:
    """Return latency summaries for queued frame-thread deliveries."""

    return _LATENCY_RECORDER.snapshot()


def drain_frame_thread_queue(max_items: int | None = None) -> int:
    """Run pending frame-thread callbacks and return the number drained."""

    if max_items is not None:
        if isinstance(max_items, bool) or not isinstance(max_items, int):
            raise ValueError("max_items must be an integer or None")
        if max_items <= 0:
            return 0

    global _FRAME_THREAD_IDENT
    _FRAME_THREAD_IDENT = get_ident()
    drained = 0
    while True:
        with _FRAME_THREAD_QUEUE_LOCK:
            if not _FRAME_THREAD_QUEUE:
                break
            task = _FRAME_THREAD_QUEUE.popleft()
        _LATENCY_RECORDER.record(
            FRAME_THREAD_LATENCY_STREAM,
            time.monotonic() - task.enqueued_monotonic,
        )
        task.callback()
        drained += 1
        if max_items is not None and drained >= max_items:
            break
    return drained


def on_frame_thread() -> bool:
    """Return whether the current thread last drained the frame-thread queue."""

    return _FRAME_THREAD_IDENT == get_ident()


def deliver_on_frame_thread(source: Observable[T]) -> Observable[T]:
    """Queue source emissions until ``drain_frame_thread_queue`` is called."""

    def _subscribe(
        observer: Any,
        scheduler: SchedulerBase | None = None,
    ) -> DisposableBase:
        disposed = False
        dispose_lock = Lock()

        def _deliver(callback: Callable[[], None]) -> None:
            def _run() -> None:
                nonlocal disposed
                with dispose_lock:
                    if disposed:
                        return
                callback()

            _enqueue_frame_thread_task(_run)

        subscription = source.subscribe(
            on_next=lambda value: _deliver(lambda: observer.on_next(value)),
            on_error=lambda error: _deliver(lambda: observer.on_error(error)),
            on_completed=lambda: _deliver(observer.on_completed),
            scheduler=scheduler,
        )

        def _dispose() -> None:
            nonlocal disposed
            with dispose_lock:
                disposed = True
            subscription.dispose()

        return Disposable(_dispose)

    return rx.create(_subscribe)


def start_with_once(value: TStarting) -> Callable[[Observable[T]], Observable[Any]]:
    """Prepend one value to a stream for each subscription."""

    def _start(source: Observable[T]) -> Observable[Any]:
        return source.pipe(ops.start_with(value))

    return _start


def pipe_in_background(
    source: Observable[T],
    *operators: Any,
    starting_value: TStarting | _NoStartingValue = _NO_STARTING_VALUE,
) -> Observable[Any]:
    """Apply operators to a source and share the resulting observable."""

    logger.debug("Building background pipeline.")
    resolved_operators = operators
    if not isinstance(starting_value, _NoStartingValue):
        resolved_operators = (*operators, start_with_once(starting_value))
    return pipe(source, *resolved_operators, ops.share())


def pipe_in_main_thread(source: Observable[T], *operators: Any) -> Observable[Any]:
    """Deliver source emissions on the frame thread, apply operators, and share."""

    logger.debug("Building frame-thread pipeline.")
    return pipe(deliver_on_frame_thread(source), *operators, ops.share())


def pipe_to_background_event_loop(
    source: Observable[T],
    name: str,
    *operators: Any,
) -> Observable[Any]:
    """Pipe a stream through an event loop running on a background thread."""

    scheduler = EventLoopScheduler(thread_factory=partial(_run_on_thread, name=name))
    return pipe(source, ops.observe_on(scheduler), *operators)


def pipe_to_background_thread(
    source: Observable[T],
    name: str,
    *operators: Any,
) -> Observable[Any]:
    """Pipe a stream through a dedicated background thread executor."""

    scheduler = NewThreadScheduler(thread_factory=partial(_run_on_thread, name=name))
    return pipe(source, ops.observe_on(scheduler), *operators)


@contextmanager
def background_threaded_observable(
    observable: Observable[T],
    name: str,
) -> Iterator[Observable[T]]:
    """Subscribe to an observable on a daemon thread and yield its subject."""

    logger.debug("Starting background stream thread %s", name)
    subject: Subject[T] = Subject()
    ready = Event()
    run = _BackgroundObservableRun()
    thread = Thread(
        name=name,
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

    return {
        "background_max_workers": _BACKGROUND_SCHEDULER.max_workers,
        "blocking_io_max_workers": _BLOCKING_IO_SCHEDULER.max_workers,
        "input_max_workers": _INPUT_SCHEDULER.max_workers,
    }


def reset_reactive_threading_state_for_tests() -> None:
    """Reset module-level schedulers, queues, and latency state."""

    global _FRAME_THREAD_IDENT
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
    with _FRAME_THREAD_QUEUE_LOCK:
        _FRAME_THREAD_QUEUE.clear()
    _FRAME_THREAD_IDENT = None
    _LATENCY_RECORDER.clear()


def materialize_sequence(sequence: Iterable[T]) -> Observable[T]:
    """Return a materialized observable over an iterable sequence."""

    return rx.from_iterable(sequence).pipe(ops.share())


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
    assert state.scheduler is not None
    return state.scheduler


def _dispose_scheduler(scheduler: SchedulerBase | None) -> None:
    if scheduler is None:
        return
    dispose = getattr(scheduler, "dispose", None)
    if callable(dispose):
        dispose()


def _run_on_thread(target: StartableTarget, name: str) -> Thread:
    return Thread(target=target, daemon=False, name=name)


def _enqueue_frame_thread_task(callback: Callable[[], None]) -> None:
    task = _FrameThreadTask(
        callback=callback,
        enqueued_monotonic=time.monotonic(),
    )
    with _FRAME_THREAD_QUEUE_LOCK:
        _FRAME_THREAD_QUEUE.append(task)


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


__all__ = (
    "DEFAULT_DELIVERY_LATENCY_HISTORY_SIZE",
    "FRAME_THREAD_LATENCY_STREAM",
    "DeliveryLatencyStats",
    "background_scheduler",
    "background_threaded_observable",
    "blocking_io_scheduler",
    "coalesce_scheduler",
    "create_default_thread_factory",
    "deliver_on_frame_thread",
    "delivery_latency_snapshot",
    "drain_frame_thread_queue",
    "input_scheduler",
    "interval_in_background",
    "interval_scheduler",
    "materialize_sequence",
    "on_frame_thread",
    "pipe_in_background",
    "pipe_in_main_thread",
    "pipe_to_background_event_loop",
    "pipe_to_background_thread",
    "replay_scheduler",
    "reset_reactive_threading_state_for_tests",
    "scheduler_diagnostics",
    "start_with_once",
    "shutdown",
)
