"""High-level Python helpers matching the RFC examples.

The top-level :mod:`manyfold` package intentionally exposes only the narrow,
day-to-day API. This module contains those primary wrappers plus the more
specialized planning, query, transport, mesh, and security helpers added for
RFC checklist coverage.
"""

from __future__ import annotations

import json
import logging
import math
import queue
import time
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import timedelta
from enum import Enum
from itertools import count
from threading import Lock, RLock, Thread, Timer as ThreadingTimer, get_ident
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterator,
    Protocol,
    Sequence,
    Sequence as TypingSequence,
    TypeAlias,
    TypeVar,
    Union,
    cast,
    overload,
    runtime_checkable,
)

from . import _rx as rx
from ._manyfold_rust import (
    ClosedEnvelope,
    ControlLoop as NativeControlLoop,
    CreditSnapshot,
    Graph as NativeGraph,
    Layer,
    Mailbox as NativeMailbox,
    MailboxDescriptor as NativeMailboxDescriptor,
    NamespaceRef,
    Plane,
    PortDescriptor,
    ProducerKind,
    ProducerRef,
    ReadablePort as NativeReadablePort,
    RouteRef,
    SchemaRef,
    TaintDomain,
    TaintMark,
    Variant,
    WritablePort as NativeWritablePort,
    WriteBinding,
)
from ._rx import Observable, operators as ops
from ._rx.disposable import Disposable
from ._rx.scheduler import TimeoutScheduler
from ._rx.subject import BehaviorSubject, Subject
from .primitives import (
    OwnerName,
    ReadThenWriteNextEpochStep,
    Schema,
    Sink,
    Source,
    StreamFamily,
    StreamName,
    TypedEnvelope,
    TypedRoute,
    route,
    sink as sink,
    source as source,
)

T = TypeVar("T")
TIn = TypeVar("TIn")
TOut = TypeVar("TOut")
U = TypeVar("U")
AnyTypedRoute = TypedRoute[Any]
AnySource = Source[Any]
AnySink = Sink[Any]
RouteLike = Union[AnyTypedRoute, RouteRef, AnySource, AnySink]
ConnectableTarget = Union[RouteLike, NativeMailbox]
InspectableRoute = Union[RouteLike, NativeReadablePort, NativeWritablePort]
EnvelopeIterator = Iterator[ClosedEnvelope]
StateT = TypeVar("StateT")
TRight = TypeVar("TRight")
DIAGRAM_GROUP_FIELDS = frozenset(
    ("plane", "layer", "owner", "family", "stream", "variant", "thread")
)
_PIPELINE_ROUTE_IDS = count(1)
_THREAD_PLACEMENT_KINDS = frozenset(
    ("main", "background", "pooled", "isolated")
)
_POOLED_THREAD_SCHEDULERS = frozenset(("background", "blocking_io", "input"))
_NO_PENDING: Any = object()
_STATS_SCHEDULER = TimeoutScheduler()
_REPLAY_SCHEDULER = TimeoutScheduler()
_SHARE_GRACE_SCHEDULER = TimeoutScheduler()
_COALESCE_SCHEDULER = TimeoutScheduler()
FRAME_THREAD_LATENCY_STREAM = "frame_thread_handoff"
DEFAULT_DELIVERY_LATENCY_HISTORY_SIZE = 2048
_FRAME_THREAD_QUEUE: deque["_FrameThreadTask"] = deque()
_FRAME_THREAD_QUEUE_LOCK = Lock()
_FRAME_THREAD_IDENT: int | None = None
logger = logging.getLogger(__name__)
shutdown: Subject[Any] = Subject()


@runtime_checkable
class SubscriptionLike(Protocol):
    def dispose(self) -> None: ...


@runtime_checkable
class ObserverLike(Protocol[T]):
    def on_next(self, value: T) -> None: ...
    def on_error(self, error: Exception) -> None: ...
    def on_completed(self) -> None: ...


@runtime_checkable
class ObservableLike(Protocol[T]):
    def subscribe(
        self,
        observer: ObserverLike[T] | Callable[[T], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
        scheduler: object | None = None,
    ) -> SubscriptionLike: ...


StreamNode: TypeAlias = ObservableLike[T]
SubscribeCallback: TypeAlias = Callable[[ObserverLike[T], object | None], SubscriptionLike]


class ConnectableObservableLike(ObservableLike[T], Protocol[T]):
    def connect(self, scheduler: object | None = None) -> SubscriptionLike: ...

    def pipe(self, *operators: Any) -> Observable[T]: ...

    def auto_connect(self, subscriber_count: int = 1) -> Observable[T]: ...


@dataclass(frozen=True, slots=True)
class DeliveryLatencyStats:
    """Percentile summary of stream delivery latency."""

    count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass
class _FrameThreadTask:
    callback: Callable[[], None]
    enqueued_monotonic: float


class CallbackSubscription:
    """Subscription backed by a plain dispose callback."""

    def __init__(self, dispose: Callable[[], None]) -> None:
        self._dispose = dispose
        self._disposed = False

    def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        self._dispose()


class CompositeSubscription:
    """Dispose a group of subscriptions together."""

    def __init__(self, subscriptions: Sequence[SubscriptionLike] = ()) -> None:
        self._subscriptions = tuple(subscriptions)
        self._disposed = False

    def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        for subscription in self._subscriptions:
            subscription.dispose()


class NoopSubscription:
    """Subscription object for sources that have nothing to dispose."""

    def dispose(self) -> None:
        return None


class _CallableObserver(Generic[T]):
    def __init__(
        self,
        on_next: Callable[[T], None],
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
    ) -> None:
        self.on_next = on_next
        self.on_error = on_error or self._ignore_error
        self.on_completed = on_completed or self._ignore_completed

    def _ignore_error(self, _error: Exception) -> None:
        return None

    def _ignore_completed(self) -> None:
        return None


class CallbackObservable(Generic[T]):
    """Observable adapter for callback-first sources."""

    def __init__(self, subscribe: SubscribeCallback[T]) -> None:
        self._subscribe = subscribe

    def subscribe(
        self,
        observer: ObserverLike[T] | Callable[[T], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[T], None] | None = None,
    ) -> SubscriptionLike:
        callback = on_next or observer
        if callback is None:
            resolved_observer: ObserverLike[T] = _CallableObserver(
                lambda _value: None,
                on_error,
                on_completed,
            )
        elif callable(callback):
            resolved_observer = _CallableObserver(callback, on_error, on_completed)
        else:
            resolved_observer = callback
        return self._subscribe(resolved_observer, scheduler)

    def pipe(self, *operators: Any) -> Observable[Any]:
        return self._observable().pipe(*operators)

    def map(self, transform: Callable[[T], U]) -> Observable[U]:
        return self._observable().map(transform)

    def filter(self, predicate: Callable[[T], bool]) -> Observable[T]:
        return self._observable().filter(predicate)

    def scan(
        self,
        accumulator: Callable[[Any, T], Any],
        *,
        seed: Any = None,
    ) -> Observable[Any]:
        return self._observable().scan(accumulator, seed=seed)

    def start_with(self, *values: Any) -> Observable[Any]:
        return self._observable().start_with(*values)

    def distinct_until_changed(
        self,
        key_mapper: Callable[[T], Any] | None = None,
        comparer: Callable[[Any, Any], bool] | None = None,
    ) -> Observable[T]:
        return self._observable().distinct_until_changed(
            key_mapper=key_mapper,
            comparer=comparer,
        )

    def do_action(
        self,
        on_next: Callable[[T], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
    ) -> Observable[T]:
        return self._observable().do_action(
            on_next=on_next,
            on_error=on_error,
            on_completed=on_completed,
        )

    def pairwise(self) -> Observable[tuple[T, T]]:
        return self._observable().pairwise()

    def take(self, count: int) -> Observable[T]:
        return self._observable().take(count)

    def with_latest_from(self, *sources: ObservableLike[Any]) -> Observable[Any]:
        return self._observable().with_latest_from(*sources)

    def flat_map(self, project: Callable[[T], ObservableLike[Any]]) -> Observable[Any]:
        return self._observable().flat_map(project)

    def switch_latest(self) -> Observable[Any]:
        return self._observable().switch_latest()

    def _observable(self) -> Observable[T]:
        return stream_from(self)


class _LatencyRecorder:
    def __init__(self, history_size: int = DEFAULT_DELIVERY_LATENCY_HISTORY_SIZE) -> None:
        self._history_size = history_size
        self._history: dict[str, deque[float]] = {}
        self._lock = Lock()

    def record(self, stream_name: str, delay_s: float) -> None:
        with self._lock:
            values = self._history.setdefault(
                stream_name,
                deque(maxlen=self._history_size),
            )
            values.append(max(delay_s, 0.0))

    def snapshot(self) -> dict[str, DeliveryLatencyStats]:
        with self._lock:
            history = {
                stream_name: tuple(values)
                for stream_name, values in self._history.items()
                if values
            }
        return {
            stream_name: _latency_stats(values)
            for stream_name, values in history.items()
        }

    def clear(self) -> None:
        with self._lock:
            self._history.clear()


_LATENCY_RECORDER = _LatencyRecorder()


def _interval_in_background(period: timedelta, *, name: str | None = None) -> Observable[int]:
    """Emit integer ticks from a recurring timer until disposed or shut down."""

    period_seconds = period.total_seconds()
    if period_seconds < 0:
        raise ValueError("period must be non-negative")

    def subscribe(observer: ObserverLike[int], scheduler: object | None = None) -> Any:
        del scheduler
        lock = Lock()
        disposed = False
        tick = 0
        timer: ThreadingTimer | None = None

        def dispose() -> None:
            nonlocal disposed, timer
            with lock:
                disposed = True
                if timer is not None:
                    timer.cancel()
                    timer = None

        def emit() -> None:
            nonlocal tick
            with lock:
                if disposed:
                    return
                value = tick
                tick += 1
            observer.on_next(value)
            schedule()

        def schedule() -> None:
            nonlocal timer
            with lock:
                if disposed:
                    return
                timer = ThreadingTimer(period_seconds, emit)
                timer.daemon = True
                if name is not None:
                    timer.name = name
                timer.start()

        shutdown_subscription = shutdown.subscribe(lambda _: dispose())
        schedule()

        def dispose_all() -> None:
            dispose()
            shutdown_subscription.dispose()

        return Disposable(dispose_all)

    return rx.create(subscribe)


def deliver_on_frame_thread(source: Observable[T]) -> Observable[T]:
    """Queue source emissions until ``drain_frame_thread_queue`` is called."""

    def subscribe(observer: ObserverLike[T], scheduler: object | None = None) -> Any:
        disposed = False
        dispose_lock = Lock()

        def deliver(callback: Callable[[], None]) -> None:
            def run() -> None:
                with dispose_lock:
                    if disposed:
                        return
                callback()

            _enqueue_frame_thread_task(run)

        subscription = source.subscribe(
            on_next=lambda value: deliver(lambda: observer.on_next(value)),
            on_error=lambda error: deliver(lambda: observer.on_error(error)),
            on_completed=lambda: deliver(observer.on_completed),
            scheduler=scheduler,
        )

        def dispose() -> None:
            nonlocal disposed
            with dispose_lock:
                disposed = True
            subscription.dispose()

        return Disposable(dispose)

    return rx.create(subscribe)


def drain_frame_thread_queue(max_items: int | None = None) -> int:
    """Run pending frame-thread callbacks and return the number drained."""

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


def delivery_latency_snapshot() -> dict[str, DeliveryLatencyStats]:
    """Return latency summaries for queued frame-thread deliveries."""

    return _LATENCY_RECORDER.snapshot()


def _start_with_once(value: U) -> Callable[[Observable[T]], Observable[Any]]:
    """Prepend one value to a stream for each subscription."""

    def start(source: Observable[T]) -> Observable[Any]:
        return source.pipe(ops.start_with(value))

    return start


def _pipe_in_background(
    source: Observable[T],
    *operators: Any,
    starting_value: Any = _NO_PENDING,
) -> Observable[Any]:
    """Apply operators to a source and share the resulting observable."""

    resolved_operators = operators
    if starting_value is not _NO_PENDING:
        resolved_operators = (*operators, _start_with_once(starting_value))
    return source.pipe(*resolved_operators, ops.share())


def _pipe_in_blocking_io(source: Observable[T], *operators: Any) -> Observable[Any]:
    """Subscribe to a source on a background thread without exposing schedulers."""

    def subscribe(observer: ObserverLike[Any], scheduler: object | None = None) -> Any:
        del scheduler
        disposed = False
        dispose_lock = Lock()
        inner: Any | None = None

        def run() -> None:
            nonlocal inner
            inner = source.pipe(*operators).subscribe(
                lambda value: _call_if_live(lambda: observer.on_next(value)),
                lambda error: _call_if_live(lambda: observer.on_error(error)),
                lambda: _call_if_live(observer.on_completed),
            )

        def _call_if_live(callback: Callable[[], None]) -> None:
            with dispose_lock:
                if disposed:
                    return
            callback()

        thread = Thread(target=run, name="manyfold-blocking-io", daemon=True)
        thread.start()

        def dispose() -> None:
            nonlocal disposed
            with dispose_lock:
                disposed = True
            if inner is not None:
                inner.dispose()

        return Disposable(dispose)

    return rx.create(subscribe).pipe(ops.share())


def _pipe_in_main_thread(source: Observable[T], *operators: Any) -> Observable[Any]:
    """Deliver source emissions on the frame thread, apply operators, and share."""

    return deliver_on_frame_thread(source).pipe(*operators, ops.share())


def _reset_reactive_threading_state_for_tests() -> None:
    """Reset frame-thread queues and latency state."""

    global _FRAME_THREAD_IDENT
    with _FRAME_THREAD_QUEUE_LOCK:
        _FRAME_THREAD_QUEUE.clear()
    _FRAME_THREAD_IDENT = None
    _LATENCY_RECORDER.clear()


def materialize_sequence(values: Sequence[T]) -> Observable[T]:
    """Expose a finite sequence as a materialized observable."""

    return rx.from_iterable(values).pipe(ops.share())


def stream_from(
    source: ObservableLike[T],
    *,
    unwrap_envelopes: bool = False,
) -> Observable[Any]:
    """Adapt any subscribable stream to Manyfold's local observable surface."""

    def subscribe(observer: ObserverLike[Any], scheduler: object | None = None) -> Any:
        def emit(value: Any) -> None:
            if unwrap_envelopes and hasattr(value, "closed") and hasattr(value, "value"):
                observer.on_next(value.value)
            else:
                observer.on_next(value)

        return source.subscribe(
            emit,
            observer.on_error,
            observer.on_completed,
            scheduler=scheduler,
        )

    return rx.create(subscribe)


def _enqueue_frame_thread_task(callback: Callable[[], None]) -> None:
    with _FRAME_THREAD_QUEUE_LOCK:
        _FRAME_THREAD_QUEUE.append(
            _FrameThreadTask(
                callback=callback,
                enqueued_monotonic=time.monotonic(),
            )
        )


def _latency_stats(values: Sequence[float]) -> DeliveryLatencyStats:
    sorted_values = sorted(values)
    return DeliveryLatencyStats(
        count=len(sorted_values),
        p50_ms=_percentile_ms(sorted_values, 0.50),
        p95_ms=_percentile_ms(sorted_values, 0.95),
        p99_ms=_percentile_ms(sorted_values, 0.99),
        max_ms=sorted_values[-1] * 1000,
    )


def _percentile_ms(sorted_values: Sequence[float], percentile: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(
        len(sorted_values) - 1,
        max(0, math.ceil(percentile * len(sorted_values)) - 1),
    )
    return sorted_values[index] * 1000


def _observe_on_thread(source: Observable[T], name: str) -> Observable[T]:
    def subscribe(observer: ObserverLike[T], scheduler: object | None = None) -> Any:
        del scheduler
        events: queue.Queue[tuple[str, Any]] = queue.Queue()
        disposed = False
        dispose_lock = Lock()

        def worker() -> None:
            while True:
                kind, value = events.get()
                with dispose_lock:
                    if disposed:
                        return
                if kind == "next":
                    observer.on_next(value)
                elif kind == "error":
                    observer.on_error(value)
                    return
                elif kind == "completed":
                    observer.on_completed()
                    return

        thread = Thread(target=worker, name=name, daemon=True)
        thread.start()
        subscription = source.subscribe(
            lambda value: events.put(("next", value)),
            lambda error: events.put(("error", error)),
            lambda: events.put(("completed", None)),
        )

        def dispose() -> None:
            nonlocal disposed
            with dispose_lock:
                disposed = True
            subscription.dispose()
            events.put(("completed", None))

        return Disposable(dispose)

    return rx.create(subscribe)


class StreamMaterializationStrategy(Enum):
    """Supported policies for materializing a local observable."""

    FAN_OUT = "fan_out"
    FAN_OUT_AUTO_CONNECT = "fan_out_auto_connect"
    REPLAY_LATEST = "replay_latest"
    REPLAY_LATEST_AUTO_CONNECT = "replay_latest_auto_connect"
    REPLAY_BUFFER = "replay_buffer"
    REPLAY_BUFFER_AUTO_CONNECT = "replay_buffer_auto_connect"


class StreamConnectMode(Enum):
    """Connect timing for ref-counted materialized observables."""

    LAZY = "lazy"
    EAGER = "eager"


@dataclass(frozen=True)
class StreamMaterializationSettings:
    """Configuration for materializing process-local observables."""

    strategy: StreamMaterializationStrategy = (
        StreamMaterializationStrategy.REPLAY_LATEST
    )
    coalesce_window_ms: int = 0
    stats_log_ms: int = 0
    replay_window_ms: int | None = None
    replay_buffer: int = 16
    auto_connect_min_subscribers: int = 1
    refcount_min_subscribers: int = 1
    refcount_grace_ms: int = 0
    connect_mode: StreamConnectMode = StreamConnectMode.LAZY

    def __post_init__(self) -> None:
        if self.coalesce_window_ms < 0:
            raise ValueError("coalesce_window_ms must be non-negative")
        if self.stats_log_ms < 0:
            raise ValueError("stats_log_ms must be non-negative")
        if self.replay_window_ms is not None and self.replay_window_ms <= 0:
            raise ValueError("replay_window_ms must be positive when set")
        if self.replay_buffer < 1:
            raise ValueError("replay_buffer must be positive")
        if self.auto_connect_min_subscribers < 1:
            raise ValueError("auto_connect_min_subscribers must be positive")
        if self.refcount_min_subscribers < 1:
            raise ValueError("refcount_min_subscribers must be positive")
        if self.refcount_grace_ms < 0:
            raise ValueError("refcount_grace_ms must be non-negative")


def materialize_stream(
    source: ObservableLike[T],
    *,
    stream_name: str,
    settings: StreamMaterializationSettings | None = None,
) -> Observable[T]:
    """Materialize an observable with optional fan-out, replay, and instrumentation."""
    resolved_settings = settings or StreamMaterializationSettings()
    shared_source = CoalesceLatestNode(
        name=f"{stream_name}.coalesce_latest",
        window_ms=resolved_settings.coalesce_window_ms,
        stream_name=stream_name,
    ).observable(source)
    replay_window_seconds = (
        None
        if resolved_settings.replay_window_ms is None
        else resolved_settings.replay_window_ms / 1000
    )

    def replay(buffer_size: int) -> ConnectableObservableLike[T]:
        if replay_window_seconds is None:
            return cast(
                ConnectableObservableLike[T],
                shared_source.pipe(ops.replay(buffer_size=buffer_size)),
            )
        return cast(
            ConnectableObservableLike[T],
            shared_source.pipe(
                ops.replay(
                    buffer_size=buffer_size,
                    window=replay_window_seconds,
                    scheduler=_REPLAY_SCHEDULER,
                ),
            ),
        )

    strategy = resolved_settings.strategy
    if strategy is StreamMaterializationStrategy.FAN_OUT:
        if (
            resolved_settings.refcount_grace_ms > 0
            or resolved_settings.refcount_min_subscribers > 1
        ):
            result = _share_with_refcount_grace(
                cast(ConnectableObservableLike[T], shared_source.pipe(ops.publish())),
                grace_ms=resolved_settings.refcount_grace_ms,
                min_subscribers=resolved_settings.refcount_min_subscribers,
                connect_mode=resolved_settings.connect_mode,
                stream_name=stream_name,
            )
        else:
            result = shared_source.pipe(ops.share())
    elif strategy is StreamMaterializationStrategy.FAN_OUT_AUTO_CONNECT:
        result = shared_source.pipe(ops.publish()).auto_connect(
            resolved_settings.auto_connect_min_subscribers
        )
    elif strategy is StreamMaterializationStrategy.REPLAY_LATEST:
        result = _share_replayed_stream(
            replay(1),
            settings=resolved_settings,
            stream_name=stream_name,
        )
    elif strategy is StreamMaterializationStrategy.REPLAY_LATEST_AUTO_CONNECT:
        result = replay(1).auto_connect(resolved_settings.auto_connect_min_subscribers)
    elif strategy is StreamMaterializationStrategy.REPLAY_BUFFER:
        result = _share_replayed_stream(
            replay(resolved_settings.replay_buffer),
            settings=resolved_settings,
            stream_name=stream_name,
        )
    elif strategy is StreamMaterializationStrategy.REPLAY_BUFFER_AUTO_CONNECT:
        result = replay(resolved_settings.replay_buffer).auto_connect(
            resolved_settings.auto_connect_min_subscribers
        )
    else:
        raise ValueError(f"Unknown stream materialization strategy: {strategy}")
    return instrument_stream(
        result,
        stream_name=stream_name,
        log_interval_ms=resolved_settings.stats_log_ms,
    )


def _share_replayed_stream(
    replayed: ConnectableObservableLike[T],
    *,
    settings: StreamMaterializationSettings,
    stream_name: str,
) -> Observable[T]:
    if settings.refcount_grace_ms > 0 or settings.refcount_min_subscribers > 1:
        return _share_with_refcount_grace(
            replayed,
            grace_ms=settings.refcount_grace_ms,
            min_subscribers=settings.refcount_min_subscribers,
            connect_mode=settings.connect_mode,
            stream_name=stream_name,
        )
    return replayed.pipe(ops.ref_count())


def _share_with_refcount_grace(
    connectable: ConnectableObservableLike[T],
    *,
    grace_ms: int,
    min_subscribers: int,
    connect_mode: StreamConnectMode,
    stream_name: str,
) -> Observable[T]:
    lock = RLock()
    connection: SubscriptionLike | None = None
    disconnect_timer: SubscriptionLike | None = None
    subscriber_count = 0

    def disconnect() -> None:
        nonlocal connection, disconnect_timer
        with lock:
            disconnect_timer = None
            if connection is None or subscriber_count >= min_subscribers:
                return
            logger.debug("Disconnecting %s after refcount grace window", stream_name)
            connection.dispose()
            connection = None

    def subscribe(observer: ObserverLike[T], scheduler: object | None = None) -> Any:
        nonlocal connection, disconnect_timer, subscriber_count
        with lock:
            subscriber_count += 1
            if disconnect_timer is not None:
                disconnect_timer.dispose()
                disconnect_timer = None
            if (
                connection is None
                and connect_mode is StreamConnectMode.EAGER
                and subscriber_count >= min_subscribers
            ):
                connection = connectable.connect()

        subscription = connectable.subscribe(observer, scheduler=scheduler)
        if connect_mode is StreamConnectMode.LAZY:
            with lock:
                if connection is None and subscriber_count >= min_subscribers:
                    connection = connectable.connect()

        def dispose() -> None:
            nonlocal subscriber_count, disconnect_timer
            subscription.dispose()
            with lock:
                subscriber_count -= 1
                if subscriber_count >= min_subscribers or connection is None:
                    return
                if grace_ms <= 0:
                    disconnect()
                    return
                if disconnect_timer is not None:
                    disconnect_timer.dispose()
                    disconnect_timer = None
                disconnect_timer = _SHARE_GRACE_SCHEDULER.schedule_relative(
                    grace_ms / 1000,
                    lambda *_: disconnect(),
                )

        return Disposable(dispose)

    return rx.create(subscribe)


@dataclass(frozen=True)
class CoalesceLatestNode(Generic[T]):
    """Coalesce bursty stream updates to the latest value per time window."""

    name: str
    window_ms: int
    stream_name: str | None = None

    def observable(self, source: ObservableLike[T]) -> Observable[T]:
        if self.window_ms <= 0:
            return cast(Observable[T], source)

        def subscribe(observer: ObserverLike[T], scheduler: object | None = None) -> Any:
            del scheduler

            lock = RLock()
            pending: Any = _NO_PENDING
            timer: Any | None = None
            fallback_timer: ThreadingTimer | None = None
            due_time: float | None = None
            window_seconds = self.window_ms / 1000

            def cancel_timers() -> None:
                nonlocal timer, fallback_timer
                if timer is not None:
                    timer.dispose()
                    timer = None
                if fallback_timer is not None:
                    fallback_timer.cancel()
                    fallback_timer = None

            def flush() -> None:
                nonlocal pending, due_time
                with lock:
                    value = pending
                    pending = _NO_PENDING
                    cancel_timers()
                    due_time = None
                if value is not _NO_PENDING:
                    observer.on_next(value)

            def schedule_flush(now: float) -> None:
                nonlocal timer, fallback_timer, due_time
                if timer is not None or fallback_timer is not None:
                    if due_time is not None and now >= due_time:
                        cancel_timers()
                        due_time = None
                    else:
                        return
                due_time = now + window_seconds
                timer = _COALESCE_SCHEDULER.schedule_relative(
                    window_seconds,
                    lambda *_: flush(),
                )
                fallback_timer = ThreadingTimer(window_seconds, flush)
                fallback_timer.daemon = True
                fallback_timer.start()

            def on_next(value: T) -> None:
                nonlocal pending, due_time
                now = time.monotonic()
                flush_value: Any = _NO_PENDING
                with lock:
                    if (
                        pending is not _NO_PENDING
                        and due_time is not None
                        and now >= due_time
                    ):
                        flush_value = pending
                        pending = _NO_PENDING
                        cancel_timers()
                        due_time = None
                    pending = value
                    schedule_flush(now)
                if flush_value is not _NO_PENDING:
                    observer.on_next(flush_value)

            def on_error(error: Exception) -> None:
                nonlocal pending, due_time
                with lock:
                    cancel_timers()
                    pending = _NO_PENDING
                    due_time = None
                observer.on_error(error)

            def on_completed() -> None:
                nonlocal due_time
                with lock:
                    cancel_timers()
                    due_time = None
                flush()
                observer.on_completed()

            subscription = source.subscribe(
                on_next,
                on_error,
                on_completed,
                scheduler=_COALESCE_SCHEDULER,
            )

            def dispose() -> None:
                nonlocal pending, due_time
                subscription.dispose()
                with lock:
                    pending = _NO_PENDING
                    cancel_timers()
                    due_time = None

            return Disposable(dispose)

        return rx.create(subscribe)


@dataclass(frozen=True)
class LoggingNode(Generic[T]):
    """Log periodic stream delivery statistics for an observable."""

    name: str
    stream_name: str
    interval_ms: int

    def observable(self, source: ObservableLike[T]) -> Observable[T]:
        if self.interval_ms <= 0:
            return cast(Observable[T], source)

        subscriber_count = 0
        event_count = 0
        timer: Any | None = None

        def schedule_log() -> None:
            nonlocal timer
            if timer is not None:
                return
            timer = _STATS_SCHEDULER.schedule_relative(
                self.interval_ms / 1000,
                lambda *_: log_stats(),
            )

        def log_stats() -> None:
            nonlocal event_count, timer
            timer = None
            if subscriber_count <= 0:
                event_count = 0
                return
            count = event_count
            event_count = 0
            logger.debug(
                "Stream stats for %s events=%d subscribers=%d interval_ms=%d",
                self.stream_name,
                count,
                subscriber_count,
                self.interval_ms,
            )
            schedule_log()

        def subscribe(observer: ObserverLike[T], scheduler: object | None = None) -> Any:
            nonlocal event_count, subscriber_count, timer
            subscriber_count += 1
            schedule_log()

            def on_next(value: T) -> None:
                nonlocal event_count
                event_count += 1
                observer.on_next(value)

            def on_error(error: Exception) -> None:
                observer.on_error(error)

            def on_completed() -> None:
                observer.on_completed()

            subscription = source.subscribe(
                on_next,
                on_error,
                on_completed,
                scheduler=scheduler,
            )

            def dispose() -> None:
                nonlocal subscriber_count, timer
                subscription.dispose()
                subscriber_count -= 1
                if subscriber_count <= 0 and timer is not None:
                    timer.dispose()
                    timer = None

            return Disposable(dispose)

        return rx.create(subscribe)


@dataclass(frozen=True)
class ConstantNode(Generic[T]):
    """Source node that holds and replays one constant value to each subscriber."""

    value: T
    name: str = "constant"

    def observable(self) -> StreamNode[T]:
        return BehaviorSubject(self.value)


@dataclass(frozen=True)
class EmptyNode(Generic[T]):
    """Source node that intentionally emits no values."""

    name: str = "empty"

    def observable(self) -> StreamNode[T]:
        return rx.empty()

    def subscribe(
        self,
        observer: ObserverLike[T] | Callable[[T], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
        scheduler: object | None = None,
    ) -> SubscriptionLike:
        return self.observable().subscribe(
            observer,
            on_error,
            on_completed,
            scheduler=scheduler,
        )


@dataclass(frozen=True)
class MainThreadNode(Generic[T]):
    """Deliver an observable through Manyfold's frame-thread queue."""

    name: str = "main-thread"

    def observable(self, source: ObservableLike[T]) -> Observable[T]:
        return deliver_on_frame_thread(cast(Observable[T], source))


@dataclass(frozen=True)
class MergeNode(Generic[T]):
    """Merge several stream sources into one stream node."""

    name: str = "merge"

    @classmethod
    def merge(cls, *sources: ObservableLike[T]) -> StreamNode[T]:
        """Merge several stream sources without explicitly constructing a node."""
        return cls().observable(*sources)

    def observable(self, *sources: ObservableLike[T]) -> StreamNode[T]:
        return rx.merge(*cast(tuple[Observable[T], ...], sources)).pipe(ops.share())


@dataclass(frozen=True)
class CombineLatestNode(Generic[T]):
    """Combine the latest values from several stream sources into tuples."""

    name: str = "combine-latest"

    def observable(self, *sources: ObservableLike[Any]) -> StreamNode[tuple[Any, ...]]:
        return rx.combine_latest(*cast(tuple[Observable[Any], ...], sources)).pipe(
            ops.share()
        )


@dataclass(frozen=True)
class IntervalNode:
    """Source node that emits monotonically increasing ticks on Manyfold scheduling."""

    name: str
    period: timedelta

    def observable(self) -> Observable[int]:
        return _interval_in_background(self.period, name=self.name)


def instrument_stream(
    source: ObservableLike[T],
    *,
    stream_name: str,
    log_interval_ms: int,
) -> Observable[T]:
    """Wrap an observable with periodic stream delivery logging."""
    return LoggingNode(
        name=f"{stream_name}.logging",
        stream_name=stream_name,
        interval_ms=log_interval_ms,
    ).observable(source)


@dataclass(frozen=True)
class JoinInput:
    """Describe one side of a planned join.

    The planner keeps these requirements explicit so it can reject illegal
    cross-partition joins instead of quietly inventing hidden repartition work.
    """

    route: RouteLike
    partition_key_semantics: str
    ordering_guarantee: str = "fifo"
    watermark_semantics: str = "none"
    state_retention: str = "bounded"
    clock_domain: str = "monotonic"
    materialized_view: bool = False
    deterministic_rekey: bool = True
    broadcast_mirror_eligible: bool = False


@dataclass(frozen=True)
class JoinPlan:
    """Planner output for a join registered in the graph."""

    name: str
    join_class: str
    left: RouteRef
    right: RouteRef
    visible_nodes: tuple[RouteRef, ...] = ()
    state_budget: str = "bounded"
    taint_implications: tuple[str, ...] = ()
    largest_partition_size: int = 0
    hot_key_frequency: int = 0


@dataclass(frozen=True)
class Middleware:
    """Declare middleware attached to a route, edge, or namespace."""

    name: str
    kind: str
    attachment_scope: str
    target: str
    introduces_async_boundary: bool = False
    preserves_envelope_identity: bool = True
    updates_taints: bool = True
    updates_causality: bool = True


@dataclass(frozen=True)
class DiagramNode:
    """Lightweight graph-visible node used by diagram renderers."""

    name: str
    input_routes: tuple[str, ...] = ()
    output_routes: tuple[str, ...] = ()
    group: str | None = None
    thread_placement: NodeThreadPlacement | None = None


@dataclass(frozen=True)
class NodeThreadPlacement:
    """Declare which process-local thread context should run a graph node."""

    kind: str
    scheduler_name: str | None = None
    thread_name: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in _THREAD_PLACEMENT_KINDS:
            raise ValueError(f"unknown node thread placement: {self.kind}")
        if (
            self.kind == "pooled"
            and self.scheduler_name not in _POOLED_THREAD_SCHEDULERS
        ):
            supported = ", ".join(sorted(_POOLED_THREAD_SCHEDULERS))
            raise ValueError(f"unknown pooled scheduler: {self.scheduler_name}; use {supported}")

    @classmethod
    def main_thread(cls) -> NodeThreadPlacement:
        """Return a placement that queues node work onto the main frame thread."""
        return cls("main")

    @classmethod
    def background_thread(cls) -> NodeThreadPlacement:
        """Return a placement that runs node work on the shared background pool."""
        return cls("background", scheduler_name="background")

    @classmethod
    def pooled_thread(cls, scheduler_name: str = "background") -> NodeThreadPlacement:
        """Return a placement that runs node work on a named shared pool."""
        return cls("pooled", scheduler_name=scheduler_name)

    @classmethod
    def isolated_thread(cls, thread_name: str | None = None) -> NodeThreadPlacement:
        """Return a placement that gives each installed node a dedicated thread."""
        return cls("isolated", thread_name=thread_name)

    def display(self) -> str:
        """Return a stable label for diagnostics and diagram metadata."""
        if self.kind == "pooled":
            return f"pooled_thread:{self.scheduler_name}"
        if self.kind == "isolated" and self.thread_name is not None:
            return f"isolated_thread:{self.thread_name}"
        return f"{self.kind}_thread"


@dataclass(frozen=True)
class LinkCapabilities:
    """Transport/link semantics the planner may rely on."""

    ordered: bool = False
    reliable: bool = False
    replayable: bool = False
    zero_copy: bool = False
    payload_lazy_open: bool = False
    encrypted: bool = False
    authenticated: bool = False
    clock_sync_support: bool = False
    mtu_bound: bool = False


@dataclass(frozen=True)
class Link:
    """A named transport adapter plus its advertised capabilities."""

    name: str
    link_class: str
    capabilities: LinkCapabilities = field(default_factory=LinkCapabilities)


@dataclass(frozen=True)
class MeshPrimitive:
    """Explicit mesh topology building block.

    These are graph-visible nodes rather than hidden wiring so topology queries
    and debug streams can explain what the runtime is doing.
    """

    name: str
    kind: str
    sources: tuple[RouteLike, ...]
    destinations: tuple[RouteLike, ...]
    link_name: str | None = None
    ordering_policy: str | None = None
    state_budget: str | None = None
    threshold: int | None = None
    ack_policy: str | None = None


@dataclass(frozen=True)
class CapabilityGrant:
    """Per-principal access policy for one route."""

    principal_id: str
    route: RouteLike
    metadata_read: bool = True
    payload_open: bool = False
    write_request: bool = False
    replay_read: bool = False
    debug_read: bool = False
    graph_validation: bool = False


@dataclass(frozen=True)
class QueryRequest:
    """Typed query-plane request description."""

    command: str
    route: RouteLike | None = None
    join_name: str | None = None
    principal_id: str | None = None
    correlation_id: str | None = None
    lineage_trace_id: str | None = None
    lineage_causality_id: str | None = None
    lineage_correlation_id: str | None = None


@dataclass(frozen=True)
class QueryResponse:
    """Typed query-plane response description."""

    command: str
    correlation_id: str
    items: tuple[str, ...]


@dataclass(frozen=True)
class EventRef:
    """Stable identity for one retained event in the in-memory graph."""

    route_display: str
    seq_source: int

    def display(self) -> str:
        return f"{self.route_display}@{self.seq_source}"


@dataclass(frozen=True)
class LineageRecord:
    """Causality/correlation metadata retained alongside one event."""

    event: EventRef
    producer_id: str | None
    trace_id: str
    causality_id: str
    correlation_id: str | None = None
    parent_events: tuple[EventRef, ...] = ()

    def display(self) -> str:
        parents = ",".join(parent.display() for parent in self.parent_events)
        return (
            f"{self.event.display()}|trace={self.trace_id}"
            f"|causality={self.causality_id}"
            f"|correlation={self.correlation_id}"
            f"|producer={self.producer_id}"
            f"|parents={parents}"
        )


@dataclass(frozen=True)
class TaintRepair:
    """Declare an explicit taint repair with a proof note.

    Repairs are the only supported way to clear taints in downstream outputs.
    The caller must name the taint domain, the value ids to clear, and a short
    proof string describing why the repair is valid.
    """

    domain: Any
    cleared: tuple[str, ...] = ()
    added: tuple[Any, ...] = ()
    proof: str = ""

    def __post_init__(self) -> None:
        if self.cleared and not self.proof.strip():
            raise ValueError("taint repairs that clear marks require a proof")


@dataclass(frozen=True)
class DebugEvent:
    """High-level debug/audit event mirrored onto a debug route."""

    event_type: str
    detail: str
    route_display: str | None
    seq_source: int


@dataclass(frozen=True)
class RouteAuditSnapshot:
    """Route-local audit summary for one route or write-binding scope."""

    route_display: str
    scope_routes: tuple[str, ...]
    recent_producers: tuple[str, ...]
    active_subscribers: tuple[str, ...]
    related_write_requests: tuple[str, ...]
    taint_upper_bounds: tuple[str, ...]
    repair_notes: tuple[str, ...]
    recent_debug_events: tuple[str, ...]


@dataclass(frozen=True)
class LazyPayloadSource:
    """Describe payload bytes that should only be opened on demand."""

    open: Callable[
        [], bytes | bytearray | memoryview | Sequence[bytes | bytearray | memoryview]
    ]
    logical_length_bytes: int | None = None
    codec_id: str = "identity"


@dataclass(frozen=True)
class Capacitor:
    """Graph-visible active storage between a source and sink."""

    name: str
    source: RouteLike
    sink: RouteLike
    capacity: int
    demand: RouteLike | None
    immediate: bool
    overflow: str


@dataclass(frozen=True)
class Resistor:
    """Graph-visible flow shaper between a source and sink."""

    name: str
    source: RouteLike
    sink: RouteLike
    gate: Callable[[Any], bool] | None
    release: RouteLike | None


@dataclass(frozen=True)
class Watchdog:
    """Graph-visible missing-flow detector."""

    name: str
    reset_by: RouteLike
    output: RouteLike
    after: int
    clock: RouteLike
    pulse: Any = b"timeout"


@dataclass(frozen=True)
class QueryServiceRoutes:
    """The well-known request/response routes for one query service owner."""

    request: RouteRef
    response: RouteRef


@dataclass(frozen=True)
class LifecycleBinding:
    """Specialized write-binding bundle for RFC lifecycle supervision."""

    binding: WriteBinding
    request: RouteRef
    desired: RouteRef
    reported: RouteRef
    effective: RouteRef
    event: RouteRef
    ack: RouteRef | None = None
    health: RouteRef | None = None

    def scope_routes(self) -> tuple[RouteRef, ...]:
        return tuple(
            route
            for route in (
                self.request,
                self.desired,
                self.reported,
                self.effective,
                self.ack,
                self.event,
                self.health,
            )
            if route is not None
        )


@dataclass(frozen=True)
class ShadowSnapshot:
    """Latest shadow-state view for one write binding."""

    request: ClosedEnvelope | None
    desired: ClosedEnvelope | None
    reported: ClosedEnvelope | None
    effective: ClosedEnvelope | None
    ack: ClosedEnvelope | None
    pending_write: bool
    coherence_taints: tuple[str, ...]


@dataclass(frozen=True)
class FlowSnapshot:
    """Current route-level credit/backpressure view."""

    route_display: str
    credit_class: str
    backpressure_policy: str
    available: int
    blocked_senders: int
    dropped_messages: int
    largest_queue_depth: int


@dataclass(frozen=True)
class FlowPolicy:
    """Partial flow-policy override used when composing edge descriptors."""

    backpressure_policy: str | None = None
    credit_class: str | None = None
    mailbox_policy: str | None = None
    async_boundary_kind: str | None = None
    overflow_policy: str | None = None

    def merged(self, override: FlowPolicy | None) -> FlowPolicy:
        if override is None:
            return self
        return FlowPolicy(
            backpressure_policy=override.backpressure_policy
            if override.backpressure_policy is not None
            else self.backpressure_policy,
            credit_class=override.credit_class
            if override.credit_class is not None
            else self.credit_class,
            mailbox_policy=override.mailbox_policy
            if override.mailbox_policy is not None
            else self.mailbox_policy,
            async_boundary_kind=override.async_boundary_kind
            if override.async_boundary_kind is not None
            else self.async_boundary_kind,
            overflow_policy=override.overflow_policy
            if override.overflow_policy is not None
            else self.overflow_policy,
        )

    def resolve(self) -> DescriptorFlowBlock:
        return DescriptorFlowBlock(
            backpressure_policy=self.backpressure_policy or "propagate",
            credit_class=self.credit_class or "default",
            mailbox_policy=self.mailbox_policy or "none",
            async_boundary_kind=self.async_boundary_kind or "inline",
            overflow_policy=self.overflow_policy or "reject_write",
        )


@dataclass(frozen=True)
class MailboxSnapshot:
    """Current mailbox queue and overflow view."""

    name: str
    ingress_route: str
    egress_route: str
    capacity: int
    delivery_mode: str
    ordering_policy: str
    overflow_policy: str
    depth: int
    available_credit: int
    blocked_writes: int
    dropped_messages: int
    coalesced_messages: int
    delivered_messages: int


@dataclass(frozen=True)
class PayloadDemandSnapshot:
    """Current metadata-versus-payload demand accounting for one route."""

    route_display: str
    metadata_events: int
    payload_open_requests: int
    lazy_source_opens: int
    materialized_payload_bytes: int
    cache_hits: int
    unopened_lazy_payloads: int


@dataclass(frozen=True)
class WatermarkSnapshot:
    """Current event-time or progress watermark view for one route."""

    route_display: str
    partition_spec: str
    clock_domain: str
    event_time_policy: str
    watermark_policy: str
    latest_seq_source: int | None
    latest_control_epoch: int | None
    current_watermark: int | None


@dataclass(frozen=True)
class ScheduledWriteSnapshot:
    """Current scheduler guard state for one queued write."""

    route_display: str
    scheduler_epoch: int
    not_before_epoch: int | None
    wait_for_ack_route: str | None
    expires_at_epoch: int | None
    ack_route: str | None
    ack_observed: bool
    attempt_count: int
    last_attempt_epoch: int | None
    next_retry_epoch: int | None
    ready_now: bool


@dataclass(frozen=True)
class DescriptorIdentityBlock:
    route_ref: RouteRef
    namespace_ref: NamespaceRef
    producer_ref: ProducerRef
    owning_runtime_kind: str
    stream_family: str
    stream_variant: str
    aliases: tuple[str, ...]
    human_description: str


@dataclass(frozen=True)
class DescriptorSchemaBlock:
    schema_ref: SchemaRef
    payload_kind: str
    codec_ref: str
    structured_payload_type: str
    payload_open_policy: str


@dataclass(frozen=True)
class DescriptorTimeBlock:
    clock_domain: str
    event_time_policy: str
    processing_time_allowed: bool
    watermark_policy: str
    control_epoch_policy: str
    ttl_policy: str


@dataclass(frozen=True)
class DescriptorOrderingBlock:
    partition_spec: str
    sequence_source_kind: str
    resequence_policy: str
    dedupe_policy: str
    causality_policy: str


@dataclass(frozen=True)
class DescriptorFlowBlock:
    backpressure_policy: str
    credit_class: str
    mailbox_policy: str
    async_boundary_kind: str
    overflow_policy: str


@dataclass(frozen=True)
class DescriptorRetentionBlock:
    latest_replay_policy: str
    durability_class: str
    replay_window: str
    payload_retention_policy: str


@dataclass(frozen=True)
class RouteRetentionPolicy:
    """Retention and replay policy override for one route."""

    latest_replay_policy: str
    durability_class: str = "memory"
    replay_window: str = "memory"
    payload_retention_policy: str | None = None
    history_limit: int | None = None

    def __post_init__(self) -> None:
        allowed = {"none", "latest_only", "bounded_history"}
        if self.latest_replay_policy not in allowed:
            raise ValueError(
                "latest_replay_policy must be one of "
                "'none', 'latest_only', or 'bounded_history'"
            )
        allowed_payload = {
            None,
            "inline",
            "separate_store",
            "external_store",
            "non_replayable",
        }
        if self.payload_retention_policy not in allowed_payload:
            raise ValueError(
                "payload_retention_policy must be one of "
                "'inline', 'separate_store', 'external_store', or 'non_replayable'"
            )
        if self.history_limit is not None and self.history_limit <= 0:
            raise ValueError("history_limit must be positive when provided")


@dataclass(frozen=True)
class DescriptorSecurityBlock:
    read_capabilities: tuple[str, ...]
    write_capabilities: tuple[str, ...]
    payload_open_capabilities: tuple[str, ...]
    redaction_policy: str
    integrity_policy: str


@dataclass(frozen=True)
class DescriptorVisibilityBlock:
    private_or_exported: str
    third_party_subscription_allowed: bool
    query_plane_visibility: str
    debug_plane_visibility: str


@dataclass(frozen=True)
class DescriptorEnvironmentBlock:
    locality: str
    transport_preferences: tuple[str, ...]
    device_class: str
    resource_class: str
    ephemeral_scope: str | None


@dataclass(frozen=True)
class DescriptorDebugBlock:
    audit_enabled: bool
    trace_enabled: bool
    metrics_enabled: bool
    payload_peek_allowed: bool
    explain_enabled: bool


@dataclass(frozen=True)
class RouteDescriptor:
    identity: DescriptorIdentityBlock
    schema: DescriptorSchemaBlock
    time: DescriptorTimeBlock
    ordering: DescriptorOrderingBlock
    flow: DescriptorFlowBlock
    retention: DescriptorRetentionBlock
    security: DescriptorSecurityBlock
    visibility: DescriptorVisibilityBlock
    environment: DescriptorEnvironmentBlock
    debug: DescriptorDebugBlock

    @property
    def route_display(self) -> str:
        return self.identity.route_ref.display()

    @property
    def human_description(self) -> str:
        return self.identity.human_description

    @property
    def payload_open_policy(self) -> str:
        return self.schema.payload_open_policy

    @property
    def backpressure_policy(self) -> str:
        return self.flow.backpressure_policy

    @property
    def debug_enabled(self) -> bool:
        return self.debug.audit_enabled


@dataclass(frozen=True)
class ScheduledWrite:
    target: RouteLike
    payload: Any
    not_before_epoch: int | None = None
    wait_for_ack: RouteLike | None = None
    expires_at_epoch: int | None = None
    producer: ProducerRef | None = None
    control_epoch: int | None = None
    retry_policy: RetryPolicy | None = None
    ack_route: RouteLike | None = None
    ack_baseline_seq: int = -1
    trace_id: str | None = None
    causality_id: str | None = None
    correlation_id: str | None = None
    parent_events: tuple[EventRef, ...] = ()
    attempt_count: int = 0
    last_attempt_epoch: int | None = None


@dataclass(frozen=True)
class RetryPolicy:
    """Typed retry/backoff policy for guarded write scheduling."""

    max_attempts: int
    backoff_epochs: int = 0

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if self.backoff_epochs < 0:
            raise ValueError("backoff_epochs must be non-negative")

    @classmethod
    def never(cls) -> RetryPolicy:
        """Disable automatic retries after the initial attempt."""
        return cls(max_attempts=1, backoff_epochs=0)

    @classmethod
    def immediate(cls, *, max_attempts: int) -> RetryPolicy:
        """Retry on the next scheduler tick until the attempt budget is exhausted."""
        return cls(max_attempts=max_attempts, backoff_epochs=0)

    @classmethod
    def fixed_backoff(cls, *, max_attempts: int, backoff_epochs: int) -> RetryPolicy:
        """Retry with a fixed scheduler-epoch delay between attempts."""
        return cls(max_attempts=max_attempts, backoff_epochs=backoff_epochs)


WriteTarget = Union[WriteBinding, LifecycleBinding, RouteLike]


@dataclass(frozen=True)
class WriteBindings:
    """Factories for common shadow-route write binding layouts."""

    @staticmethod
    def logical(
        owner: OwnerName,
        family: StreamFamily,
        stream: StreamName,
        schema: Schema[bytes],
    ) -> WriteBinding:
        request = route(
            plane=Plane.Write,
            layer=Layer.Logical,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Request,
            schema=schema,
        )
        desired = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Desired,
            schema=schema,
        )
        reported = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Reported,
            schema=schema,
        )
        effective = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Effective,
            schema=schema,
        )
        ack = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Ack,
            schema=Schema.bytes(f"{schema.schema_id}Ack", version=schema.version),
        )
        return WriteBinding(
            request=request.route_ref,
            desired=desired.route_ref,
            reported=reported.route_ref,
            effective=effective.route_ref,
            ack=ack.route_ref,
        )

    @staticmethod
    def lifecycle(
        owner: OwnerName,
        family: StreamFamily,
        *,
        intent_schema: Schema[bytes],
        observation_schema: Schema[bytes] | None = None,
        layer: Layer = Layer.Raw,
        stream: StreamName | None = None,
        event_schema: Schema[bytes] | None = None,
        ack_schema: Schema[bytes] | None = None,
        health_schema: Schema[bytes] | None = None,
    ) -> LifecycleBinding:
        lifecycle_stream = stream or StreamName("lifecycle")
        observation = observation_schema or intent_schema
        request = route(
            plane=Plane.Write,
            layer=layer,
            owner=owner,
            family=family,
            stream=lifecycle_stream,
            variant=Variant.Request,
            schema=intent_schema,
        )
        desired = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=lifecycle_stream,
            variant=Variant.Desired,
            schema=intent_schema,
        )
        reported = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=lifecycle_stream,
            variant=Variant.Reported,
            schema=observation,
        )
        effective = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=lifecycle_stream,
            variant=Variant.Effective,
            schema=observation,
        )
        event = route(
            plane=Plane.Read,
            layer=Layer.Internal,
            owner=owner,
            family=family,
            stream=lifecycle_stream,
            variant=Variant.Event,
            schema=event_schema
            or Schema.bytes(
                f"{observation.schema_id}Event", version=observation.version
            ),
        )
        ack = (
            None
            if ack_schema is None
            else route(
                plane=Plane.Write,
                layer=Layer.Shadow,
                owner=owner,
                family=family,
                stream=lifecycle_stream,
                variant=Variant.Ack,
                schema=ack_schema,
            )
        )
        health = (
            None
            if health_schema is None
            else route(
                plane=Plane.Read,
                layer=Layer.Internal,
                owner=owner,
                family=family,
                stream=lifecycle_stream,
                variant=Variant.Health,
                schema=health_schema,
            )
        )
        binding = WriteBinding(
            request=request.route_ref,
            desired=desired.route_ref,
            reported=reported.route_ref,
            effective=effective.route_ref,
            ack=None if ack is None else ack.route_ref,
        )
        return LifecycleBinding(
            binding=binding,
            request=binding.request,
            desired=binding.desired,
            reported=binding.reported,
            effective=binding.effective,
            event=event.route_ref,
            ack=binding.ack,
            health=None if health is None else health.route_ref,
        )


@dataclass(frozen=True)
class ControlLoops:
    """Factories for narrow RFC-shaped control loop stubs."""

    @staticmethod
    def with_routes(
        name: str,
        *,
        read_routes: Sequence[TypedRoute[Any]],
        write_request: TypedRoute[Any],
    ) -> NativeControlLoop:
        return NativeControlLoop(
            name=name,
            read_routes=tuple(route.route_ref for route in read_routes),
            write_request=write_request.route_ref,
        )

    @staticmethod
    def speed_pid(
        *,
        read_state: TypedRoute[Any],
        read_feedback: TypedRoute[Any],
        write_request: TypedRoute[Any],
    ) -> NativeControlLoop:
        return ControlLoops.with_routes(
            "SpeedPid",
            read_routes=[read_state, read_feedback],
            write_request=write_request,
        )

    @staticmethod
    def counter_accumulate(
        *, read_state: TypedRoute[Any], write_request: TypedRoute[Any]
    ) -> NativeControlLoop:
        return ControlLoops.with_routes(
            "CounterAccumulate",
            read_routes=[read_state],
            write_request=write_request,
        )


class ReactiveReadablePort:
    """Readable port facade with a live Rx stream for route updates."""

    def __init__(
        self, graph: Graph, route_ref: RouteRef, native: NativeReadablePort
    ) -> None:
        self._graph = graph
        self._route_ref = route_ref
        self._native = native

    def meta(self) -> EnvelopeIterator:
        return iter(tuple(self._native.meta()))

    def open(self) -> Iterator[Any]:
        return iter(tuple(self._native.open()))

    def latest(self) -> ClosedEnvelope | None:
        return self._native.latest()

    def describe(self) -> PortDescriptor:
        return self._native.describe()

    def observe(self, *, replay_latest: bool = True) -> Observable[ClosedEnvelope]:
        def subscribe(
            observer: ObserverLike[ClosedEnvelope],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            if replay_latest:
                latest = self.latest()
                if latest is not None:
                    observer.on_next(latest)
            return self._graph._subject_for(self._route_ref).subscribe(
                observer, scheduler=scheduler
            )

        return rx.create(subscribe)


class ReactiveWritablePort:
    """Writable port facade that can act as an Rx observer."""

    def __init__(
        self, graph: Graph, route_ref: RouteRef, native: NativeWritablePort
    ) -> None:
        self._graph = graph
        self._route_ref = route_ref
        self._native = native

    def write(
        self,
        payload: bytes,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> ClosedEnvelope:
        envelope = self._native.write(
            payload, producer=producer, control_epoch=control_epoch
        )
        producer_id = "python" if producer is None else producer.producer_id
        self._graph._record_envelope(self._route_ref, envelope, producer_id=producer_id)
        return envelope

    def describe(self) -> PortDescriptor:
        return self._native.describe()

    def as_observer(
        self,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> ObserverLike[bytes]:
        port = self

        class _Observer:
            def on_next(self, payload: bytes) -> None:
                port.write(payload, producer=producer, control_epoch=control_epoch)

            def on_error(self, error: Exception) -> None:
                raise error

            def on_completed(self) -> None:
                return None

        return _Observer()

    def bind(
        self,
        source: ObservableLike[bytes],
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> SubscriptionLike:
        return source.subscribe(
            self.as_observer(producer=producer, control_epoch=control_epoch)
        )


class _TrackedSubscription:
    def __init__(
        self,
        graph: Graph,
        route_ref: RouteRef,
        inner: SubscriptionLike,
        subscriber_id: str,
    ) -> None:
        self._graph = graph
        self._route_ref = route_ref
        self._inner = inner
        self._subscriber_id = subscriber_id
        self._disposed = False

    def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        route_key = self._graph._route_key(self._route_ref)
        self._graph._subscriber_count[route_key] -= 1
        subscribers = self._graph._route_subscribers.get(route_key)
        if subscribers is not None:
            subscribers.discard(self._subscriber_id)
        self._inner.dispose()


class _ThreadPlacementSubscription:
    def __init__(
        self,
        inner: SubscriptionLike,
        *,
        owned_scheduler: object | None = None,
    ) -> None:
        self._inner = inner
        self._owned_scheduler = owned_scheduler
        self._disposed = False

    def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        self._inner.dispose()
        if self._owned_scheduler is None:
            return
        dispose = getattr(self._owned_scheduler, "dispose", None)
        if callable(dispose):
            dispose()


class GraphConnection:
    """Handle for graph-installed observation topology."""

    def __init__(
        self,
        graph: Graph,
        *,
        name: str,
        subscriptions: Sequence[SubscriptionLike] = (),
        nodes: Sequence[str] = (),
        connections: Sequence[GraphConnection] = (),
    ) -> None:
        self._graph = graph
        self.name = name
        self._subscriptions = tuple(subscriptions)
        self._nodes = tuple(nodes)
        self._connections = tuple(connections)
        self._removed = False

    def remove(self) -> None:
        """Stop this connection and unregister graph-visible pipeline nodes."""
        if self._removed:
            return
        self._removed = True
        for connection in self._connections:
            connection.remove()
        for subscription in self._subscriptions:
            subscription.dispose()
        for node in self._nodes:
            self._graph._diagram_nodes.pop(node, None)

    def dispose(self) -> None:
        """Compatibility alias for callers still expecting disposable handles."""
        self.remove()


class _ThreadPlaceableNode:
    thread_placement: NodeThreadPlacement | None

    def with_thread_placement(self, placement: NodeThreadPlacement | None) -> Any:
        """Return a copy of this node with explicit thread placement."""
        return replace(self, thread_placement=placement)

    def then_on_main_thread(self) -> Any:
        """Run this node when reached as downstream main-frame work."""
        return self.with_thread_placement(NodeThreadPlacement.main_thread())

    def on_main_thread(self) -> Any:
        """Compatibility alias for ``then_on_main_thread``."""
        return self.then_on_main_thread()

    def then_on_background_thread(self, *, isolated: bool = False) -> Any:
        """Run this node when reached as downstream background work."""
        placement = (
            NodeThreadPlacement.isolated_thread()
            if isolated
            else NodeThreadPlacement.background_thread()
        )
        return self.with_thread_placement(placement)

    def on_background_thread(self, *, isolated: bool = False) -> Any:
        """Compatibility alias for ``then_on_background_thread``."""
        return self.then_on_background_thread(isolated=isolated)

    def then_on_isolated_thread(self, name: str | None = None) -> Any:
        """Run this node when reached on a dedicated downstream thread."""
        return self.with_thread_placement(NodeThreadPlacement.isolated_thread(name))

    def on_isolated_thread(self, name: str | None = None) -> Any:
        """Compatibility alias for ``then_on_isolated_thread``."""
        return self.then_on_isolated_thread(name)

    def then_on_pooled_thread(self, pool_name: str = "background") -> Any:
        """Run this node when reached on a named downstream thread pool."""
        return self.with_thread_placement(NodeThreadPlacement.pooled_thread(pool_name))

    def on_pooled_thread(
        self,
        scheduler_name: str = "background",
        *,
        pool_name: str | None = None,
    ) -> Any:
        """Compatibility alias for ``then_on_pooled_thread``."""
        return self.then_on_pooled_thread(pool_name or scheduler_name)


@dataclass(frozen=True)
class CallbackNode(_ThreadPlaceableNode, Generic[T]):
    """Leaf graph node that delivers route updates to ordinary Python code."""

    name: str
    receive: Callable[[T], None]
    thread_placement: NodeThreadPlacement | None = None


@dataclass(frozen=True)
class MapNode(_ThreadPlaceableNode, Generic[TIn, TOut]):
    """Graph-visible map operation."""

    name: str
    transform: Callable[[TIn], TOut]
    thread_placement: NodeThreadPlacement | None = None


@dataclass(frozen=True)
class FilterNode(_ThreadPlaceableNode, Generic[T]):
    """Graph-visible filter operation."""

    name: str
    predicate: Callable[[T], bool]
    thread_placement: NodeThreadPlacement | None = None


@dataclass(frozen=True)
class PipelineLoggingNode(_ThreadPlaceableNode, Generic[T]):
    """Graph-visible logging operation."""

    name: str
    stream_name: str
    interval_ms: int
    thread_placement: NodeThreadPlacement | None = None

    def observable(self, source: ObservableLike[T]) -> Observable[T]:
        return LoggingNode(
            name=self.name,
            stream_name=self.stream_name,
            interval_ms=self.interval_ms,
        ).observable(source)


class RoutePipeline(Generic[T]):
    """Fluent graph pipeline rooted at one route."""

    def __init__(
        self,
        graph: Graph,
        route_ref: RouteLike,
        *,
        replay_latest: bool = True,
        subscriber_id: str | None = None,
        connections: Sequence[GraphConnection] = (),
        thread_placement: NodeThreadPlacement | None = None,
    ) -> None:
        self._graph = graph
        self._route_ref = route_ref
        self._replay_latest = replay_latest
        self._subscriber_id = subscriber_id
        self._connections = tuple(connections)
        self._thread_placement = thread_placement

    @property
    def route(self) -> RouteLike:
        return self._route_ref

    def connect(
        self,
        target: CallbackNode[T] | Callable[[T], None] | RouteLike,
        *,
        name: str | None = None,
    ) -> GraphConnection:
        """Install this pipeline into a route or callback sink."""
        if isinstance(target, (TypedRoute, RouteRef, Source, Sink)):
            connection = self._connect_route(target, name=name)
        else:
            node = (
                target
                if isinstance(target, CallbackNode)
                else CallbackNode(
                    name or self._graph._next_pipeline_node_name("callback"),
                    target,
                    thread_placement=self._thread_placement,
                )
            )
            connection = self._graph._connect_callback_pipeline(
                self._route_ref,
                node,
                replay_latest=self._replay_latest,
                subscriber_id=self._subscriber_id,
                thread_placement=node.thread_placement or self._thread_placement,
            )
        if not self._connections:
            return connection
        return GraphConnection(
            self._graph,
            name=connection.name,
            connections=(*self._connections, connection),
        )

    def callback(
        self,
        receive: Callable[[T], None],
        *,
        name: str | None = None,
    ) -> GraphConnection:
        """Install a callback sink node for this pipeline."""
        return self.connect(
            CallbackNode(
                name or self._graph._next_pipeline_node_name("callback"),
                receive,
                thread_placement=self._thread_placement,
            )
        )

    def coalesce_latest(
        self,
        *,
        window_ms: int,
        name: str | None = None,
        stream_name: str | None = None,
    ) -> RoutePipeline[T]:
        """Install a graph-visible latest-value coalescing node."""
        node = CoalesceLatestNode(
            name or self._graph._next_pipeline_node_name("coalesce-latest"),
            window_ms=window_ms,
            stream_name=stream_name,
        )
        return self._graph._connect_coalesce_latest_pipeline(self, node)

    def filter(
        self,
        predicate: Callable[[T], bool],
        *,
        name: str | None = None,
    ) -> RoutePipeline[T]:
        """Install a graph-visible filter node and continue from its output."""
        node = FilterNode(
            name or self._graph._next_pipeline_node_name("filter"),
            predicate,
            thread_placement=self._thread_placement,
        )
        return self._graph._connect_transform_pipeline(
            self,
            node,
            lambda value: (node.predicate(value), value),
        )

    def distinct_until_changed(
        self,
        key_mapper: Callable[[T], Any] | None = None,
        *,
        name: str | None = None,
    ) -> RoutePipeline[T]:
        """Install a graph-visible distinct-until-changed node."""
        has_last = False
        last_key: Any = None

        def predicate(value: T) -> bool:
            nonlocal has_last, last_key
            key = key_mapper(value) if key_mapper is not None else value
            if has_last and key == last_key:
                return False
            has_last = True
            last_key = key
            return True

        node = FilterNode(
            name or self._graph._next_pipeline_node_name("distinct-until-changed"),
            predicate,
            thread_placement=self._thread_placement,
        )
        return self._graph._connect_transform_pipeline(
            self,
            node,
            lambda value: (node.predicate(value), value),
        )

    def log(
        self,
        *,
        interval_ms: int,
        name: str | None = None,
        stream_name: str | None = None,
    ) -> RoutePipeline[T]:
        """Install a graph-visible periodic stream logging node."""
        node_name = name or self._graph._next_pipeline_node_name("log")
        node = PipelineLoggingNode(
            node_name,
            stream_name=stream_name or node_name,
            interval_ms=interval_ms,
            thread_placement=self._thread_placement,
        )
        return self._graph._connect_logging_pipeline(self, node)

    def map(
        self,
        transform: Callable[[T], U],
        *,
        name: str | None = None,
    ) -> RoutePipeline[U]:
        """Install a graph-visible map node and continue from its output."""
        node = MapNode(
            name or self._graph._next_pipeline_node_name("map"),
            transform,
            thread_placement=self._thread_placement,
        )
        return self._graph._connect_transform_pipeline(
            self,
            node,
            lambda value: (True, node.transform(value)),
        )

    def then_on_main_thread(self) -> RoutePipeline[T]:
        """Place future pipeline computations on the main frame thread.

        This does not move the current source. For example, ``Timer(...)`` still
        ticks from its timer source; only maps, filters, callbacks, and other
        downstream computations added after this call are handed to the frame
        thread.
        """
        return self._with_thread_placement(NodeThreadPlacement.main_thread())

    def on_main_thread(self) -> RoutePipeline[T]:
        """Compatibility alias for ``then_on_main_thread``."""
        return self.then_on_main_thread()

    def then_on_background_thread(
        self, *, isolated: bool = False
    ) -> RoutePipeline[T]:
        """Place future pipeline computations on background execution."""
        placement = (
            NodeThreadPlacement.isolated_thread()
            if isolated
            else NodeThreadPlacement.background_thread()
        )
        return self._with_thread_placement(placement)

    def on_background_thread(self, *, isolated: bool = False) -> RoutePipeline[T]:
        """Compatibility alias for ``then_on_background_thread``."""
        return self.then_on_background_thread(isolated=isolated)

    def then_on_isolated_thread(self, name: str | None = None) -> RoutePipeline[T]:
        """Place future pipeline computations on a dedicated thread."""
        return self._with_thread_placement(NodeThreadPlacement.isolated_thread(name))

    def on_isolated_thread(self, name: str | None = None) -> RoutePipeline[T]:
        """Compatibility alias for ``then_on_isolated_thread``."""
        return self.then_on_isolated_thread(name)

    def then_on_pooled_thread(
        self,
        pool_name: str = "background",
    ) -> RoutePipeline[T]:
        """Place future pipeline computations on a named shared thread pool."""
        return self._with_thread_placement(NodeThreadPlacement.pooled_thread(pool_name))

    def on_pooled_thread(
        self,
        scheduler_name: str = "background",
        *,
        pool_name: str | None = None,
    ) -> RoutePipeline[T]:
        """Compatibility alias for ``then_on_pooled_thread``."""
        return self.then_on_pooled_thread(pool_name or scheduler_name)

    def start_with(self, *values: Any) -> Observable[Any]:
        """Expose this pipeline with one or more leading values."""
        return self.pipe(ops.start_with(*values))

    def do_action(
        self,
        on_next: Callable[[T], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
    ) -> Observable[T]:
        """Expose this pipeline with side effects for stream notifications."""
        return self.pipe(
            ops.do_action(
                on_next=on_next,
                on_error=on_error,
                on_completed=on_completed,
            )
        )

    def scan(
        self,
        accumulator: Callable[[Any, T], Any],
        *,
        seed: Any = None,
    ) -> Observable[Any]:
        """Expose this pipeline with accumulated state."""
        return self.pipe(ops.scan(accumulator, seed=seed))

    def pairwise(self) -> Observable[tuple[T, T]]:
        """Expose adjacent pairs from this pipeline."""
        return self.pipe(ops.pairwise())

    def take(self, count: int) -> Observable[T]:
        """Expose the first ``count`` values from this pipeline."""
        return self.pipe(ops.take(count))

    def with_latest_from(self, *sources: ObservableLike[Any]) -> Observable[Any]:
        """Expose this pipeline combined with latest values from other streams."""
        return self.pipe(ops.with_latest_from(*sources))

    def flat_map(self, project: Callable[[T], ObservableLike[Any]]) -> Observable[Any]:
        """Expose this pipeline by merging projected inner streams."""
        return self.pipe(ops.flat_map(project))

    def switch_latest(self) -> Observable[Any]:
        """Expose values from only the latest inner stream."""
        return self.pipe(ops.switch_latest())

    def subscribe(
        self,
        observer: ObserverLike[Any] | Callable[[Any], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[Any], None] | None = None,
    ) -> SubscriptionLike:
        """Compatibility shim for existing Rx-style callers."""
        if on_next is not None:
            observer = on_next
        return self._graph._observe_observable(
            self._route_ref,
            replay_latest=self._replay_latest,
            subscriber_id=self._subscriber_id,
            thread_placement=self._thread_placement,
        ).subscribe(
            observer,
            on_error,
            on_completed,
            scheduler=scheduler,
        )

    def pipe(self, *operators: Any) -> Observable[Any]:
        """Expose this graph pipeline as an observable for Rx-style composition."""
        return self._graph._pipeline_value_observable(
            self._route_ref,
            replay_latest=self._replay_latest,
            subscriber_id=self._subscriber_id,
            thread_placement=self._thread_placement,
        ).pipe(*operators)

    def _connect_route(
        self,
        target: RouteLike,
        *,
        name: str | None = None,
    ) -> GraphConnection:
        return self._graph._connect_route_pipeline(
            self._route_ref,
            target,
            replay_latest=self._replay_latest,
            subscriber_id=self._subscriber_id,
            name=name,
            thread_placement=self._thread_placement,
        )

    def _with_thread_placement(
        self,
        placement: NodeThreadPlacement | None,
    ) -> RoutePipeline[T]:
        return RoutePipeline(
            self._graph,
            self._route_ref,
            replay_latest=self._replay_latest,
            subscriber_id=self._subscriber_id,
            connections=self._connections,
            thread_placement=placement,
        )

    def __getattr__(self, operation: str) -> Callable[..., Any]:
        def apply(*args: Any, **kwargs: Any) -> Any:
            return self._graph._apply_registered_pipeline_operation(
                self,
                operation,
                *args,
                **kwargs,
            )

        return apply


class Interval:
    """Graph-backed interval source with the same fluent node API as route pipelines."""

    def __init__(
        self,
        period: timedelta,
        *,
        graph: Graph | None = None,
        name: str = "interval",
    ) -> None:
        self._graph = graph or Graph()
        self._pipeline = self._graph._connect_interval_pipeline(
            IntervalNode(name=name, period=period)
        )

    @property
    def graph(self) -> Graph:
        return self._graph

    @property
    def route(self) -> RouteLike:
        return self._pipeline.route

    def callback(
        self,
        receive: Callable[[int], None],
        *,
        name: str | None = None,
    ) -> GraphConnection:
        return self._pipeline.callback(receive, name=name)

    def coalesce_latest(
        self,
        *,
        window_ms: int,
        name: str | None = None,
        stream_name: str | None = None,
    ) -> RoutePipeline[int]:
        return self._pipeline.coalesce_latest(
            window_ms=window_ms,
            name=name,
            stream_name=stream_name,
        )

    def filter(
        self,
        predicate: Callable[[int], bool],
        *,
        name: str | None = None,
    ) -> RoutePipeline[int]:
        return self._pipeline.filter(predicate, name=name)

    def distinct_until_changed(
        self,
        key_mapper: Callable[[int], Any] | None = None,
        *,
        name: str | None = None,
    ) -> RoutePipeline[int]:
        return self._pipeline.distinct_until_changed(key_mapper, name=name)

    def log(
        self,
        *,
        interval_ms: int,
        name: str | None = None,
        stream_name: str | None = None,
    ) -> RoutePipeline[int]:
        return self._pipeline.log(
            interval_ms=interval_ms,
            name=name,
            stream_name=stream_name,
        )

    def map(
        self,
        transform: Callable[[int], U],
        *,
        name: str | None = None,
    ) -> RoutePipeline[U]:
        return self._pipeline.map(transform, name=name)

    def on_main_thread(self) -> RoutePipeline[int]:
        """Compatibility alias for ``then_on_main_thread``."""
        return self.then_on_main_thread()

    def then_on_main_thread(self) -> RoutePipeline[int]:
        """Place future timer pipeline computations on the main frame thread.

        The timer itself may tick from its source thread; only downstream work
        added after this call moves to the frame thread.
        """
        return self._pipeline.then_on_main_thread()

    def on_background_thread(self, *, isolated: bool = False) -> RoutePipeline[int]:
        """Compatibility alias for ``then_on_background_thread``."""
        return self.then_on_background_thread(isolated=isolated)

    def then_on_background_thread(
        self, *, isolated: bool = False
    ) -> RoutePipeline[int]:
        """Place future timer pipeline computations on background execution."""
        return self._pipeline.then_on_background_thread(isolated=isolated)

    def on_isolated_thread(self, name: str | None = None) -> RoutePipeline[int]:
        """Compatibility alias for ``then_on_isolated_thread``."""
        return self.then_on_isolated_thread(name)

    def then_on_isolated_thread(self, name: str | None = None) -> RoutePipeline[int]:
        """Place future timer pipeline computations on a dedicated thread."""
        return self._pipeline.then_on_isolated_thread(name)

    def then_on_pooled_thread(self, pool_name: str = "background") -> RoutePipeline[int]:
        """Place future timer pipeline computations on a named thread pool."""
        return self._pipeline.then_on_pooled_thread(pool_name)

    def on_pooled_thread(
        self,
        scheduler_name: str = "background",
        *,
        pool_name: str | None = None,
    ) -> RoutePipeline[int]:
        """Compatibility alias for ``then_on_pooled_thread``."""
        return self.then_on_pooled_thread(pool_name or scheduler_name)

    def subscribe(
        self,
        observer: ObserverLike[int] | Callable[[int], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[int], None] | None = None,
    ) -> SubscriptionLike:
        if on_next is not None:
            observer = on_next
        return self._pipeline.subscribe(
            observer,
            on_error,
            on_completed,
            scheduler=scheduler,
        )

    def pipe(self, *operators: Any) -> Observable[Any]:
        """Expose this timer pipeline as an observable for Rx-style composition."""
        return self._pipeline.pipe(*operators)


class Timer(Interval):
    """Graph-backed timer source for fluent stream composition."""


class Graph:
    """Python-facing Manyfold API.

    Primary public calls:
    - observe(route_ref)
    - latest(route_ref)
    - describe_route(route_ref)
    - publish(route_ref, payload)
    - pipe(source, route_ref)
    - run_control_loop(name)
    - install(control_loop)
    - connect(source=..., sink=...)
    More specialized planning and inspection helpers also live here, but they
    stay off the top-level package namespace so the primary API remains small.
    """

    def __init__(self) -> None:
        self._graph = NativeGraph()
        self._subjects: dict[str, Subject[ClosedEnvelope]] = {}
        self._subscriptions: deque[SubscriptionLike] = deque()
        self._history: dict[str, list[ClosedEnvelope]] = {}
        self._writers: dict[str, set[str]] = {}
        self._subscriber_count: dict[str, int] = {}
        self._route_subscribers: dict[str, set[str]] = {}
        self._join_plans: dict[str, JoinPlan] = {}
        self._middlewares: list[Middleware] = []
        self._links: dict[str, Link] = {}
        self._mesh_primitives: dict[str, MeshPrimitive] = {}
        self._query_services: dict[str, QueryServiceRoutes] = {}
        self._debug_routes: dict[str, RouteRef] = {}
        self._diagram_nodes: dict[str, DiagramNode] = {}
        self._diagram_routes: dict[str, RouteRef] = {}
        self._audit_events: list[DebugEvent] = []
        self._capability_grants: dict[tuple[str, str], CapabilityGrant] = {}
        self._route_visibility: dict[str, str] = {}
        self._retention_policies: dict[str, RouteRetentionPolicy] = {}
        self._write_bindings: dict[str, WriteBinding] = {}
        self._lifecycle_bindings: dict[str, LifecycleBinding] = {}
        self._lazy_payload_sources: dict[str, LazyPayloadSource] = {}
        self._materialized_payloads: dict[str, bytes] = {}
        self._payload_route_by_id: dict[str, str] = {}
        self._opened_payload_ids: set[str] = set()
        self._payload_demand_stats: dict[str, dict[str, int]] = {}
        self._stream_taint_upper_bounds: dict[str, dict[tuple[str, str], Any]] = {}
        self._route_repair_notes: dict[str, list[str]] = {}
        self._lineage_by_event: dict[tuple[str, int], LineageRecord] = {}
        self._lineage_events_by_trace: dict[str, list[tuple[str, int]]] = {}
        self._lineage_events_by_causality: dict[str, list[tuple[str, int]]] = {}
        self._lineage_events_by_correlation: dict[str, list[tuple[str, int]]] = {}
        self._mailbox_descriptors: dict[str, NativeMailboxDescriptor] = {}
        self._capacitors: dict[str, Capacitor] = {}
        self._resistors: dict[str, Resistor] = {}
        self._watchdogs: dict[str, Watchdog] = {}
        self._graph_flow_defaults = FlowPolicy()
        self._source_flow_defaults: dict[str, FlowPolicy] = {}
        self._sink_flow_requirements: dict[str, FlowPolicy] = {}
        self._edge_flow_overrides: dict[tuple[str, str], FlowPolicy] = {}
        self._mailbox_flow_policies: dict[str, FlowPolicy] = {}
        self._pending_writes: list[ScheduledWrite] = []
        self._scheduler_epoch = 0
        self._query_sequence = 0
        self._subscriber_sequence = 0
        self._pipeline_node_sequence = 0
        self._pipeline_factories: dict[str, Callable[..., Any]] = {
            "callback": CallbackNode,
            "coalesce_latest": CoalesceLatestNode,
            "filter": FilterNode,
            "log": PipelineLoggingNode,
            "map": MapNode,
        }

    def _coerce_route_ref(self, route_ref: RouteLike) -> RouteRef:
        if isinstance(route_ref, (Source, Sink)):
            return self._coerce_route_ref(route_ref.route)
        if isinstance(route_ref, TypedRoute):
            return route_ref.route_ref
        return route_ref

    @staticmethod
    def _typed_route(route_ref: RouteLike) -> TypedRoute[Any] | None:
        if isinstance(route_ref, (Source, Sink)):
            route_ref = route_ref.route
        if isinstance(route_ref, TypedRoute):
            return route_ref
        return None

    @staticmethod
    def _source_replay_latest(route_ref: RouteLike) -> bool:
        if isinstance(route_ref, Source):
            return route_ref.replay_latest
        return True

    def _connectable_key(self, target: ConnectableTarget, *, edge_role: str) -> str:
        if isinstance(target, NativeMailbox):
            if edge_role == "source":
                return target.egress.describe().route_display
            return target.ingress.describe().route_display
        return self._route_key(target)

    def _route_key(self, route_ref: RouteLike) -> str:
        return self._coerce_route_ref(route_ref).display()

    def _next_pipeline_node_name(self, kind: str) -> str:
        self._pipeline_node_sequence += 1
        return f"{kind}-{self._pipeline_node_sequence}"

    def _auto_node_name(
        self,
        kind: str,
        source: RouteLike,
        sink: RouteLike,
        *policy_parts: str,
    ) -> str:
        policy = ":".join(part for part in policy_parts if part)
        base = f"{kind}:{self._route_key(source)}->{self._route_key(sink)}"
        return f"{base}:{policy}" if policy else base

    @staticmethod
    def _closed_item(item: TypedEnvelope[Any] | ClosedEnvelope) -> ClosedEnvelope:
        return item.closed if isinstance(item, TypedEnvelope) else item

    def _event_ref(self, envelope: ClosedEnvelope) -> EventRef:
        return EventRef(
            route_display=envelope.route.display(),
            seq_source=envelope.seq_source,
        )

    @staticmethod
    def _event_index_key(event: EventRef) -> tuple[str, int]:
        return (event.route_display, event.seq_source)

    def _next_subscriber_id(self) -> str:
        self._subscriber_sequence += 1
        return f"subscriber-{self._subscriber_sequence}"

    @staticmethod
    def _mailbox_value(mailbox: NativeMailbox, name: str) -> Any:
        value = getattr(mailbox, name)
        return value() if callable(value) else value

    def _subject_for(self, route_ref: RouteLike) -> Subject[ClosedEnvelope]:
        key = self._route_key(route_ref)
        if key not in self._subjects:
            self._subjects[key] = Subject()
        return self._subjects[key]

    def _payload_stats(self, route_ref: RouteLike) -> dict[str, int]:
        key = self._route_key(route_ref)
        return self._payload_demand_stats.setdefault(
            key,
            {
                "metadata_events": 0,
                "payload_open_requests": 0,
                "lazy_source_opens": 0,
                "materialized_payload_bytes": 0,
                "cache_hits": 0,
            },
        )

    def _default_flow_policy_for_route(
        self,
        route_ref: RouteRef,
        native: PortDescriptor | None = None,
    ) -> FlowPolicy:
        return FlowPolicy(
            backpressure_policy=getattr(native, "backpressure_policy", None)
            or "propagate",
            credit_class=(
                "bulk_payload"
                if route_ref.namespace.layer == Layer.Bulk
                else "control"
                if route_ref.namespace.plane == Plane.Write
                else "default"
            ),
            mailbox_policy="none",
            async_boundary_kind="inline",
            overflow_policy="reject_write",
        )

    def _resolved_route_flow_policy(
        self,
        route_ref: RouteRef,
        native: PortDescriptor | None = None,
    ) -> DescriptorFlowBlock:
        key = self._route_key(route_ref)
        policy = self._default_flow_policy_for_route(route_ref, native)
        policy = policy.merged(self._graph_flow_defaults)
        policy = policy.merged(self._mailbox_flow_policies.get(key))
        policy = policy.merged(self._source_flow_defaults.get(key))
        policy = policy.merged(self._sink_flow_requirements.get(key))
        return policy.resolve()

    def _resolved_edge_flow_policy(
        self,
        source: ConnectableTarget,
        sink: ConnectableTarget,
    ) -> DescriptorFlowBlock:
        source_key = self._connectable_key(source, edge_role="source")
        sink_key = self._connectable_key(sink, edge_role="sink")
        source_route = (
            None if isinstance(source, NativeMailbox) else self._coerce_route_ref(source)
        )
        policy = (
            self._default_flow_policy_for_route(source_route)
            if source_route is not None
            else FlowPolicy()
        )
        policy = policy.merged(self._graph_flow_defaults)
        policy = policy.merged(self._mailbox_flow_policies.get(source_key))
        policy = policy.merged(self._mailbox_flow_policies.get(sink_key))
        policy = policy.merged(self._source_flow_defaults.get(source_key))
        policy = policy.merged(self._sink_flow_requirements.get(sink_key))
        policy = policy.merged(self._edge_flow_overrides.get((source_key, sink_key)))
        return policy.resolve()

    def _default_retention_policy(self, route_ref: RouteRef) -> RouteRetentionPolicy:
        if route_ref.namespace.layer == Layer.Ephemeral:
            return RouteRetentionPolicy(
                latest_replay_policy="none",
                replay_window="none",
                payload_retention_policy="non_replayable",
                history_limit=1,
            )
        if route_ref.namespace.layer == Layer.Internal:
            return RouteRetentionPolicy(
                latest_replay_policy="latest_only",
                replay_window="latest",
                payload_retention_policy="separate_store",
                history_limit=1,
            )
        return RouteRetentionPolicy(
            latest_replay_policy="bounded_history",
            replay_window="memory",
            payload_retention_policy=(
                "external_store"
                if route_ref.namespace.layer == Layer.Bulk
                else "separate_store"
            ),
        )

    def _retention_policy_for(self, route_ref: RouteLike) -> RouteRetentionPolicy:
        native_route = self._coerce_route_ref(route_ref)
        key = native_route.display()
        return self._retention_policies.get(
            key, self._default_retention_policy(native_route)
        )

    def _payload_retention_policy_for(self, route_ref: RouteLike) -> str:
        return cast(
            str,
            self._retention_policy_for(route_ref).payload_retention_policy,
        )

    def _purge_payload_ref(self, payload_id: str) -> None:
        self._lazy_payload_sources.pop(payload_id, None)
        self._materialized_payloads.pop(payload_id, None)
        self._payload_route_by_id.pop(payload_id, None)
        self._opened_payload_ids.discard(payload_id)

    def _enforce_payload_retention(self, route_ref: RouteLike) -> None:
        key = self._route_key(route_ref)
        retained_payload_ids = {
            envelope.payload_ref.payload_id for envelope in self._history.get(key, ())
        }
        payload_policy = self._payload_retention_policy_for(route_ref)
        if payload_policy == "non_replayable" and retained_payload_ids:
            latest_payload_id = self._history[key][-1].payload_ref.payload_id
            retained_payload_ids = {latest_payload_id}
        for payload_id, payload_route in tuple(self._payload_route_by_id.items()):
            if payload_route != key:
                continue
            if payload_id not in retained_payload_ids:
                self._purge_payload_ref(payload_id)
        if payload_policy in {"external_store", "non_replayable"}:
            for payload_id in tuple(self._materialized_payloads):
                if self._payload_route_by_id.get(payload_id) == key:
                    del self._materialized_payloads[payload_id]

    def _record_envelope(
        self,
        route_ref: RouteLike,
        envelope: ClosedEnvelope,
        *,
        producer_id: str | None = None,
        trace_id: str | None = None,
        causality_id: str | None = None,
        correlation_id: str | None = None,
        parent_events: Sequence[EventRef] = (),
    ) -> ClosedEnvelope:
        """Persist envelope-derived bookkeeping before notifying observers."""
        key = self._route_key(route_ref)
        history = self._history.setdefault(key, [])
        history.append(envelope)
        retention = self._retention_policy_for(route_ref)
        if retention.history_limit is not None and len(history) > retention.history_limit:
            del history[: len(history) - retention.history_limit]
        self._payload_route_by_id[envelope.payload_ref.payload_id] = key
        self._payload_stats(route_ref)["metadata_events"] += 1
        self._enforce_payload_retention(route_ref)
        self._remember_stream_taints(route_ref, envelope)
        if producer_id is not None:
            self._writers.setdefault(key, set()).add(producer_id)
        event = self._event_ref(envelope)
        resolved_trace_id = trace_id or event.display()
        resolved_causality_id = causality_id or event.display()
        record = LineageRecord(
            event=event,
            producer_id=producer_id,
            trace_id=resolved_trace_id,
            causality_id=resolved_causality_id,
            correlation_id=correlation_id,
            parent_events=tuple(parent_events),
        )
        self._lineage_by_event[self._event_index_key(event)] = record
        self._lineage_events_by_trace.setdefault(resolved_trace_id, []).append(
            self._event_index_key(event)
        )
        self._lineage_events_by_causality.setdefault(
            resolved_causality_id, []
        ).append(self._event_index_key(event))
        if correlation_id is not None:
            self._lineage_events_by_correlation.setdefault(correlation_id, []).append(
                self._event_index_key(event)
            )
        self._publish(route_ref, envelope)
        return envelope

    def _publish(
        self, route_ref: RouteLike, envelope: ClosedEnvelope
    ) -> ClosedEnvelope:
        self._subject_for(route_ref).on_next(envelope)
        return envelope

    @staticmethod
    def _normalize_payload_chunks(
        payload: bytes
        | bytearray
        | memoryview
        | Sequence[bytes | bytearray | memoryview],
    ) -> bytes:
        if isinstance(payload, (bytes, bytearray, memoryview)):
            return bytes(payload)
        return b"".join(bytes(chunk) for chunk in payload)

    def _known_payload_bytes(
        self,
        route_ref: RouteLike,
        envelope: ClosedEnvelope,
        *,
        record_open: bool,
    ) -> bytes | None:
        payload_id = envelope.payload_ref.payload_id
        stats = self._payload_stats(route_ref)
        if record_open:
            stats["payload_open_requests"] += 1
        if payload_id in self._materialized_payloads:
            payload = self._materialized_payloads[payload_id]
            if record_open:
                stats["cache_hits"] += 1
        elif payload_id in self._lazy_payload_sources:
            payload = self._normalize_payload_chunks(
                self._lazy_payload_sources[payload_id].open()
            )
            if self._payload_retention_policy_for(route_ref) in {
                "inline",
                "separate_store",
            }:
                self._materialized_payloads[payload_id] = payload
            stats["lazy_source_opens"] += 1
            stats["materialized_payload_bytes"] += len(payload)
        else:
            inline_payload = bytes(envelope.payload_ref.inline_bytes)
            if inline_payload:
                payload = inline_payload
            else:
                payload = None
        if record_open:
            if payload is not None:
                self._opened_payload_ids.add(payload_id)
            self._emit_debug_event(
                "payload_open",
                f"opened payload {payload_id}",
                route_ref,
            )
        return payload

    def _resolve_payload_bytes(
        self,
        route_ref: RouteLike,
        envelope: ClosedEnvelope,
        *,
        record_open: bool,
    ) -> bytes:
        payload = self._known_payload_bytes(route_ref, envelope, record_open=record_open)
        if payload is not None:
            return payload
        opened = tuple(self._read_port(route_ref).open())
        if not opened:
            return b""
        return bytes(opened[-1].payload)

    def _decode_envelope(
        self, route_ref: TypedRoute[T], envelope: ClosedEnvelope
    ) -> TypedEnvelope[T]:
        payload = self._payload_bytes(route_ref, envelope)
        return TypedEnvelope(
            route=route_ref, closed=envelope, value=route_ref.schema.decode(payload)
        )

    def _payload_bytes(
        self, route_ref: TypedRoute[T], envelope: ClosedEnvelope
    ) -> bytes:
        return self._resolve_payload_bytes(route_ref, envelope, record_open=False)

    def _operator_value(
        self,
        route_ref: RouteLike,
        native_route: RouteRef,
        item: TypedEnvelope[T] | ClosedEnvelope,
    ) -> T | bytes:
        if isinstance(item, TypedEnvelope):
            return item.value
        payload = self._resolve_payload_bytes(native_route, item, record_open=False)
        typed_route = self._typed_route(route_ref)
        if typed_route is not None:
            return typed_route.schema.decode(payload)
        return payload

    def _progress_value(
        self,
        route_ref: RouteLike,
        native_route: RouteRef,
        item: TypedEnvelope[T] | ClosedEnvelope,
        *,
        extractor: Callable[[T | bytes], int] | None = None,
    ) -> int:
        """Resolve event-time or watermark progress for one observed item."""
        value = self._operator_value(route_ref, native_route, item)
        if extractor is not None:
            return extractor(value)
        closed = item.closed if isinstance(item, TypedEnvelope) else item
        if closed.control_epoch is not None:
            return closed.control_epoch
        return closed.seq_source

    @staticmethod
    def _taint_key(taint: Any) -> tuple[Any, Any]:
        domain = getattr(taint, "domain", None)
        if hasattr(domain, "as_str"):
            domain_key = domain.as_str()
        elif hasattr(domain, "value"):
            domain_key = domain.value
        else:
            domain_key = str(domain)
        return (
            domain_key,
            getattr(taint, "value_id", None),
        )

    @staticmethod
    def _taint_domain_name(domain: Any) -> str:
        if hasattr(domain, "as_str"):
            return cast(str, domain.as_str())
        if hasattr(domain, "value"):
            return cast(str, domain.value)
        return str(domain)

    def _item_taints(
        self, item: TypedEnvelope[Any] | ClosedEnvelope
    ) -> tuple[Any, ...]:
        closed = item.closed if isinstance(item, TypedEnvelope) else item
        return tuple(getattr(closed, "taints", ()))

    def _lineage_for_item(
        self,
        item: TypedEnvelope[Any] | ClosedEnvelope,
    ) -> LineageRecord:
        closed = item.closed if isinstance(item, TypedEnvelope) else item
        event = self._event_ref(closed)
        record = self._lineage_by_event.get(self._event_index_key(event))
        if record is not None:
            return record
        return LineageRecord(
            event=event,
            producer_id=getattr(getattr(closed, "producer", None), "producer_id", None),
            trace_id=event.display(),
            causality_id=event.display(),
        )

    def _remember_stream_taints(
        self,
        route_ref: RouteLike,
        envelope: ClosedEnvelope,
    ) -> None:
        key = self._route_key(route_ref)
        remembered = self._stream_taint_upper_bounds.setdefault(key, {})
        for taint in getattr(envelope, "taints", ()):
            domain_name = self._taint_domain_name(getattr(taint, "domain", None))
            value_id = getattr(taint, "value_id", None)
            if value_id is None:
                continue
            remembered.setdefault((domain_name, value_id), taint)

    @staticmethod
    def _taint_domain_matches(taint: Any, domain_name: str) -> bool:
        domain = getattr(taint, "domain", None)
        if domain == domain_name:
            return True
        if hasattr(domain, "as_str"):
            return cast(str, domain.as_str()) == domain_name
        if hasattr(domain, "value"):
            return cast(str, domain.value) == domain_name
        return False

    def _set_domain_taints(
        self,
        envelope: ClosedEnvelope | None,
        *,
        domain_name: str,
        values: Sequence[str],
    ) -> None:
        if envelope is None:
            return
        retained = [
            taint
            for taint in getattr(envelope, "taints", ())
            if not self._taint_domain_matches(taint, domain_name)
        ]
        retained.extend(
            TaintMark(TaintDomain.Coherence, value, envelope.route.display())
            for value in values
        )
        try:
            envelope.taints = retained
        except AttributeError:
            return

    def _envelope_payload_bytes(self, envelope: ClosedEnvelope | None) -> bytes | None:
        if envelope is None:
            return None
        return self._resolve_payload_bytes(envelope.route, envelope, record_open=False)

    def _coherence_taints_for_binding(self, binding: WriteBinding) -> tuple[str, ...]:
        desired = self._graph.latest(binding.desired)
        reported = self._graph.latest(binding.reported)
        effective = self._graph.latest(binding.effective)

        desired_payload = self._envelope_payload_bytes(desired)
        reported_payload = self._envelope_payload_bytes(reported)
        effective_payload = self._envelope_payload_bytes(effective)

        taints: list[str] = []
        if desired_payload is None:
            if effective_payload is not None:
                taints.append("COHERENCE_STABLE")
        else:
            if effective_payload != desired_payload:
                taints.append("COHERENCE_WRITE_PENDING")
            if reported_payload is not None and reported_payload != desired_payload:
                taints.append("COHERENCE_STALE_REPORTED")
            if (
                effective_payload is not None
                and reported_payload is not None
                and effective_payload != reported_payload
            ):
                taints.append("COHERENCE_ECHO_UNMATCHED")
            if not taints:
                taints.append("COHERENCE_STABLE")
        return tuple(taints)

    def _refresh_binding_coherence(self, binding: WriteBinding) -> tuple[str, ...]:
        taints = self._coherence_taints_for_binding(binding)
        for binding_route in (
            binding.request,
            binding.desired,
            binding.reported,
            binding.effective,
        ):
            self._set_domain_taints(
                self._graph.latest(binding_route),
                domain_name="coherence",
                values=taints,
            )
            latest = self._graph.latest(binding_route)
            if latest is not None:
                self._remember_stream_taints(binding_route, latest)
        return taints

    def _resolve_write_binding(
        self, binding_or_request: WriteBinding | LifecycleBinding | RouteLike
    ) -> WriteBinding:
        if isinstance(binding_or_request, LifecycleBinding):
            self._write_bindings[binding_or_request.request.display()] = (
                binding_or_request.binding
            )
            self._lifecycle_bindings[binding_or_request.request.display()] = (
                binding_or_request
            )
            return binding_or_request.binding
        if isinstance(binding_or_request, WriteBinding):
            self._write_bindings[binding_or_request.request.display()] = (
                binding_or_request
            )
            return binding_or_request
        request = self._coerce_route_ref(binding_or_request)
        if request.display() not in self._write_bindings:
            raise KeyError(f"no write binding registered for {request.display()}")
        return self._write_bindings[request.display()]

    def _binding_for_route(self, route_ref: RouteLike) -> WriteBinding | None:
        route_display = self._route_key(route_ref)
        for lifecycle in self._lifecycle_bindings.values():
            if any(route.display() == route_display for route in lifecycle.scope_routes()):
                return lifecycle.binding
        for binding in self._write_bindings.values():
            for candidate in (
                binding.request,
                binding.desired,
                binding.reported,
                binding.effective,
                binding.ack,
            ):
                if candidate is not None and candidate.display() == route_display:
                    return binding
        return None

    def _lifecycle_binding_for_request(
        self, binding_or_request: WriteBinding | LifecycleBinding | RouteLike
    ) -> LifecycleBinding | None:
        if isinstance(binding_or_request, LifecycleBinding):
            self._lifecycle_bindings[binding_or_request.request.display()] = (
                binding_or_request
            )
            return binding_or_request
        if isinstance(binding_or_request, WriteBinding):
            return self._lifecycle_bindings.get(binding_or_request.request.display())
        native_route = self._coerce_route_ref(binding_or_request)
        if native_route.variant != Variant.Request:
            binding = self._binding_for_route(native_route)
            if binding is None:
                return None
            native_route = binding.request
        return self._lifecycle_bindings.get(native_route.display())

    def _audit_scope_routes(self, route_ref: RouteLike) -> tuple[RouteRef, ...]:
        binding = self._binding_for_route(route_ref)
        if binding is None:
            return (self._coerce_route_ref(route_ref),)
        lifecycle = self._lifecycle_binding_for_request(binding)
        if lifecycle is not None:
            return lifecycle.scope_routes()
        return tuple(
            route
            for route in (
                binding.request,
                binding.desired,
                binding.reported,
                binding.effective,
                binding.ack,
            )
            if route is not None
        )

    def _extend_taints(
        self,
        emitted: TypedEnvelope[Any] | ClosedEnvelope,
        source_taints: Sequence[Any],
    ) -> TypedEnvelope[Any] | ClosedEnvelope:
        if not source_taints:
            return emitted
        closed = emitted.closed if isinstance(emitted, TypedEnvelope) else emitted
        existing = list(getattr(closed, "taints", ()))
        existing_keys = {self._taint_key(taint) for taint in existing}
        for taint in source_taints:
            key = self._taint_key(taint)
            if key in existing_keys:
                continue
            existing.append(taint)
            existing_keys.add(key)
        try:
            closed.taints = existing
        except AttributeError:
            return emitted
        self._remember_stream_taints(closed.route, closed)
        return emitted

    def _apply_taint_repair(
        self,
        emitted: TypedEnvelope[Any] | ClosedEnvelope,
        source_taints: Sequence[Any],
        *,
        repair: TaintRepair | None = None,
    ) -> TypedEnvelope[Any] | ClosedEnvelope:
        emitted = self._extend_taints(emitted, source_taints)
        if repair is None:
            return emitted
        closed = emitted.closed if isinstance(emitted, TypedEnvelope) else emitted
        domain_name = self._taint_domain_name(repair.domain)
        cleared = set(repair.cleared)
        if cleared:
            absorbing = {
                "determinism": {"DET_NONREPLAYABLE"},
            }
            for taint in getattr(closed, "taints", ()):
                if (
                    self._taint_domain_name(getattr(taint, "domain", None))
                    != domain_name
                ):
                    continue
                if getattr(taint, "value_id", None) not in cleared:
                    continue
                if getattr(taint, "value_id", None) in absorbing.get(
                    domain_name, set()
                ):
                    raise ValueError(
                        f"cannot clear absorbing taint {getattr(taint, 'value_id', None)}"
                    )
            repaired_taints = [
                taint
                for taint in getattr(closed, "taints", ())
                if not (
                    self._taint_domain_name(getattr(taint, "domain", None))
                    == domain_name
                    and getattr(taint, "value_id", None) in cleared
                )
            ]
            try:
                closed.taints = repaired_taints
            except AttributeError:
                return emitted
        existing = list(getattr(closed, "taints", ()))
        existing_keys = {self._taint_key(taint) for taint in existing}
        for taint in repair.added:
            key = self._taint_key(taint)
            if key in existing_keys:
                continue
            existing.append(taint)
            existing_keys.add(key)
        try:
            closed.taints = existing
        except AttributeError:
            return emitted
        self._remember_stream_taints(closed.route, closed)
        return emitted

    def _taint_query_items(self, route_ref: RouteLike) -> tuple[str, ...]:
        key = self._route_key(route_ref)
        upper_bound = self._stream_taint_upper_bounds.get(key, {})
        stream_items = tuple(
            f"stream:{domain}:{value_id}:{getattr(taint, 'origin_id', '')}"
            for (domain, value_id), taint in sorted(upper_bound.items())
        )

        event_items: list[str] = []
        for envelope in self._history.get(key, ()):
            for taint in getattr(envelope, "taints", ()):
                event_items.append(
                    ":".join(
                        (
                            "event",
                            str(envelope.seq_source),
                            self._taint_domain_name(taint.domain),
                            taint.value_id,
                            taint.origin_id,
                        )
                    )
                )

        repair_items = tuple(
            f"repair:{note}" for note in self._route_repair_notes.get(key, ())
        )
        return (*stream_items, *tuple(event_items), *repair_items)

    def _replay_latest_value(
        self,
        route_ref: RouteLike,
        on_next: Callable[[TypedEnvelope[T] | ClosedEnvelope], None],
    ) -> None:
        latest = self.latest(route_ref)
        if latest is not None:
            on_next(latest)

    def _descriptor_defaults(
        self, route_ref: RouteRef, native: PortDescriptor | None = None
    ) -> RouteDescriptor:
        route_display = route_ref.display()
        retention = self._retention_policy_for(route_ref)
        payload_open_policy = (
            "lazy_external"
            if route_ref.namespace.layer == Layer.Bulk
            else "owner_only"
            if route_ref.namespace.layer == Layer.Internal
            else getattr(native, "payload_open_policy", None) or "lazy"
        )
        debug_enabled = bool(getattr(native, "debug_enabled", True))
        visibility = self._route_visibility.get(route_display, "private")
        third_party_subscription_allowed = visibility == "exported"

        if route_ref.namespace.plane in (Plane.Query, Plane.Debug):
            producer_kind = getattr(ProducerKind, "QueryService", "query_service")
        elif (
            route_ref.namespace.plane == Plane.Write
            and route_ref.namespace.layer == Layer.Shadow
        ):
            producer_kind = getattr(ProducerKind, "Transform", "reconciler")
        elif (
            route_ref.namespace.plane == Plane.Read
            and route_ref.namespace.layer == Layer.Raw
        ):
            producer_kind = getattr(ProducerKind, "Device", "device")
        else:
            producer_kind = getattr(ProducerKind, "Application", "application")

        return RouteDescriptor(
            identity=DescriptorIdentityBlock(
                route_ref=route_ref,
                namespace_ref=route_ref.namespace,
                producer_ref=ProducerRef(
                    producer_id=route_ref.namespace.owner,
                    kind=producer_kind,
                ),
                owning_runtime_kind="in_memory",
                stream_family=route_ref.family,
                stream_variant=getattr(
                    route_ref.variant, "value", str(route_ref.variant)
                ).lower(),
                aliases=(route_display,),
                human_description=getattr(
                    native, "human_description", f"Manyfold port for {route_display}"
                ),
            ),
            schema=DescriptorSchemaBlock(
                schema_ref=route_ref.schema,
                payload_kind="structured",
                codec_ref="identity",
                structured_payload_type=route_ref.schema.schema_id,
                payload_open_policy=payload_open_policy,
            ),
            time=DescriptorTimeBlock(
                clock_domain=(
                    "ephemeral"
                    if route_ref.namespace.layer == Layer.Ephemeral
                    else "control_epoch"
                    if route_ref.namespace.plane == Plane.Write
                    else "monotonic"
                ),
                event_time_policy="control_epoch_or_ingest"
                if route_ref.namespace.plane == Plane.Write
                else "ingest",
                processing_time_allowed=True,
                watermark_policy="recommended"
                if route_ref.namespace.layer == Layer.Bulk
                else "none",
                control_epoch_policy="allowed"
                if route_ref.namespace.plane == Plane.Write
                else "optional",
                ttl_policy="ttl_required"
                if route_ref.namespace.layer == Layer.Ephemeral
                else "retain_latest",
            ),
            ordering=DescriptorOrderingBlock(
                partition_spec="unpartitioned",
                sequence_source_kind="route_local",
                resequence_policy="none",
                dedupe_policy="none",
                causality_policy="opaque",
            ),
            flow=self._resolved_route_flow_policy(route_ref, native),
            retention=DescriptorRetentionBlock(
                latest_replay_policy=retention.latest_replay_policy,
                durability_class=retention.durability_class,
                replay_window=retention.replay_window,
                payload_retention_policy=cast(
                    str, retention.payload_retention_policy
                ),
            ),
            security=DescriptorSecurityBlock(
                read_capabilities=("read",),
                write_capabilities=("write",),
                payload_open_capabilities=("payload_open",),
                redaction_policy="none",
                integrity_policy="best_effort",
            ),
            visibility=DescriptorVisibilityBlock(
                private_or_exported=visibility,
                third_party_subscription_allowed=third_party_subscription_allowed,
                query_plane_visibility="exported"
                if third_party_subscription_allowed
                else "owner",
                debug_plane_visibility="exported"
                if third_party_subscription_allowed
                else "owner",
            ),
            environment=DescriptorEnvironmentBlock(
                locality="process",
                transport_preferences=(
                    ("memory", "bulk_link")
                    if route_ref.namespace.layer == Layer.Bulk
                    else ("memory",)
                ),
                device_class="generic",
                resource_class="standard",
                ephemeral_scope=route_ref.namespace.owner
                if route_ref.namespace.layer == Layer.Ephemeral
                else None,
            ),
            debug=DescriptorDebugBlock(
                audit_enabled=debug_enabled,
                trace_enabled=debug_enabled,
                metrics_enabled=True,
                payload_peek_allowed=route_ref.namespace.layer != Layer.Bulk,
                explain_enabled=True,
            ),
        )

    def _watermark_snapshot_for_route(self, route_ref: RouteRef) -> WatermarkSnapshot:
        descriptor = self.describe_route(route_ref)
        latest = self._graph.latest(route_ref)
        latest_seq_source = None if latest is None else latest.seq_source
        latest_control_epoch = None if latest is None else latest.control_epoch
        current_watermark = latest_control_epoch
        if current_watermark is None and descriptor.time.watermark_policy != "none":
            current_watermark = latest_seq_source
        return WatermarkSnapshot(
            route_display=route_ref.display(),
            partition_spec=descriptor.ordering.partition_spec,
            clock_domain=descriptor.time.clock_domain,
            event_time_policy=descriptor.time.event_time_policy,
            watermark_policy=descriptor.time.watermark_policy,
            latest_seq_source=latest_seq_source,
            latest_control_epoch=latest_control_epoch,
            current_watermark=current_watermark,
        )

    def _scheduled_target_route(self, scheduled: ScheduledWrite) -> RouteRef:
        if isinstance(scheduled.target, WriteBinding):
            return scheduled.target.request
        return self._coerce_route_ref(scheduled.target)

    def _latest_closed(self, route_ref: RouteLike) -> ClosedEnvelope | None:
        latest = self.latest(route_ref)
        if isinstance(latest, TypedEnvelope):
            return latest.closed
        return latest

    def _ack_observed_after(self, ack_route: RouteLike, baseline_seq: int) -> bool:
        latest = self._latest_closed(ack_route)
        return latest is not None and latest.seq_source > baseline_seq

    def _scheduled_write_snapshot(
        self,
        scheduled: ScheduledWrite,
    ) -> ScheduledWriteSnapshot | None:
        target_route = self._scheduled_target_route(scheduled)
        if (
            scheduled.expires_at_epoch is not None
            and self._scheduler_epoch > scheduled.expires_at_epoch
        ):
            return None
        ack_observed = False
        next_retry_epoch = None
        if scheduled.attempt_count > 0 and scheduled.ack_route is not None:
            ack_observed = self._ack_observed_after(
                scheduled.ack_route,
                scheduled.ack_baseline_seq,
            )
            if scheduled.retry_policy is not None:
                next_retry_epoch = (
                    self._scheduler_epoch
                    if scheduled.last_attempt_epoch is None
                    else scheduled.last_attempt_epoch
                    + scheduled.retry_policy.backoff_epochs
                    + 1
                )
        else:
            ack_observed = (
                scheduled.wait_for_ack is None
                or self.latest(scheduled.wait_for_ack) is not None
            )
        epoch_ready = (
            scheduled.not_before_epoch is None
            or self._scheduler_epoch >= scheduled.not_before_epoch
        )
        retry_ready = (
            next_retry_epoch is None or self._scheduler_epoch >= next_retry_epoch
        )
        ready_now = epoch_ready and ack_observed and retry_ready
        return ScheduledWriteSnapshot(
            route_display=target_route.display(),
            scheduler_epoch=self._scheduler_epoch,
            not_before_epoch=scheduled.not_before_epoch,
            wait_for_ack_route=None
            if scheduled.wait_for_ack is None
            else self._route_key(scheduled.wait_for_ack),
            expires_at_epoch=scheduled.expires_at_epoch,
            ack_route=None
            if scheduled.ack_route is None
            else self._route_key(scheduled.ack_route),
            ack_observed=ack_observed,
            attempt_count=scheduled.attempt_count,
            last_attempt_epoch=scheduled.last_attempt_epoch,
            next_retry_epoch=next_retry_epoch,
            ready_now=ready_now,
        )

    def _make_internal_route(
        self,
        *,
        plane: Plane,
        layer: Layer,
        owner: str,
        family: str,
        stream: str,
        variant: Variant,
        schema_id: str,
    ) -> RouteRef:
        """Construct graph-owned internal routes for query/debug/state plumbing."""
        return RouteRef(
            namespace=NamespaceRef(plane=plane, layer=layer, owner=owner),
            family=family,
            stream=stream,
            variant=variant,
            schema=SchemaRef(schema_id=schema_id, version=1),
        )

    def _debug_route(self, event_type: str) -> RouteRef:
        """Return the well-known route backing one debug event stream."""
        if event_type not in self._debug_routes:
            self._debug_routes[event_type] = self.register_port(
                self._make_internal_route(
                    plane=Plane.Debug,
                    layer=Layer.Internal,
                    owner="manyfold",
                    family="debug",
                    stream=event_type,
                    variant=Variant.Event,
                    schema_id="DebugEvent",
                )
            )
        return self._debug_routes[event_type]

    def _emit_debug_event(
        self, event_type: str, detail: str, route_ref: RouteLike | None = None
    ) -> DebugEvent:
        """Emit a debug event on both the in-memory audit log and a debug route."""
        debug_route = self._debug_route(event_type)
        payload = json.dumps(
            {
                "event_type": event_type,
                "detail": detail,
                "route_display": None
                if route_ref is None
                else self._route_key(route_ref),
            },
            sort_keys=True,
        ).encode()
        envelope = self._graph.writable_port(debug_route).write(payload)
        self._record_envelope(debug_route, envelope, producer_id="debug")
        event = DebugEvent(
            event_type=event_type,
            detail=detail,
            route_display=None if route_ref is None else self._route_key(route_ref),
            seq_source=envelope.seq_source,
        )
        self._audit_events.append(event)
        return event

    def _correlation_id(self) -> str:
        self._query_sequence += 1
        return f"query-{self._query_sequence}"

    def _authorize(
        self, principal_id: str | None, route_ref: RouteLike | None, capability: str
    ) -> None:
        """Enforce per-route capability checks for third-party access."""
        if principal_id in (None, "", "python", "internal"):
            return
        if capability == "graph_validation":
            for grant in self._capability_grants.values():
                if grant.principal_id == principal_id and grant.graph_validation:
                    return
            raise PermissionError(f"{principal_id} lacks graph validation capability")
        if route_ref is None:
            raise PermissionError(f"{principal_id} lacks {capability} capability")
        key = (principal_id, self._route_key(route_ref))
        grant = self._capability_grants.get(key)
        if grant is None:
            if (
                capability == "metadata_read"
                and self._route_visibility.get(key[1]) == "exported"
            ):
                return
            raise PermissionError(
                f"{principal_id} lacks {capability} capability for {key[1]}"
            )
        if not getattr(grant, capability):
            raise PermissionError(
                f"{principal_id} lacks {capability} capability for {key[1]}"
            )

    def _execute_query(self, request: QueryRequest) -> tuple[str, ...]:
        """Resolve query commands against the current in-memory graph state."""
        command = request.command.lower()
        route_ref = (
            None if request.route is None else self._coerce_route_ref(request.route)
        )
        if command == "catalog":
            return tuple(route.display() for route in self.catalog())
        if command == "describe_route":
            if route_ref is None:
                raise ValueError("describe_route requires a route")
            self._authorize(request.principal_id, route_ref, "metadata_read")
            descriptor = self.describe_route(route_ref)
            return (
                descriptor.route_display,
                descriptor.human_description,
                descriptor.payload_open_policy,
                descriptor.backpressure_policy,
            )
        if command == "latest":
            if route_ref is None:
                raise ValueError("latest requires a route")
            self._authorize(request.principal_id, route_ref, "metadata_read")
            latest = self.latest(route_ref)
            if latest is None:
                return ()
            return (
                latest.route.display(),
                str(latest.seq_source),
                latest.payload_ref.payload_id,
            )
        if command == "topology":
            return tuple(f"{left}->{right}" for left, right in self.topology())
        if command == "replay":
            if route_ref is None:
                raise ValueError("replay requires a route")
            self._authorize(request.principal_id, route_ref, "replay_read")
            return tuple(
                str(envelope.seq_source) for envelope in self.replay(route_ref)
            )
        if command == "subscribers":
            if route_ref is None:
                raise ValueError("subscribers requires a route")
            self._authorize(request.principal_id, route_ref, "metadata_read")
            return (str(self.subscribers(route_ref)),)
        if command == "writers":
            if route_ref is None:
                raise ValueError("writers requires a route")
            self._authorize(request.principal_id, route_ref, "metadata_read")
            return tuple(sorted(self.writers(route_ref)))
        if command == "validate_graph":
            self._authorize(request.principal_id, None, "graph_validation")
            return tuple(self.validate_graph())
        if command == "explain_join":
            if request.join_name is None:
                raise ValueError("explain_join requires a join_name")
            plan = self.explain_join(request.join_name)
            return (
                plan.name,
                plan.join_class,
                *(node.display() for node in plan.visible_nodes),
                *plan.taint_implications,
            )
        if command == "open_payload":
            if route_ref is None:
                raise ValueError("open_payload requires a route")
            self._authorize(request.principal_id, route_ref, "payload_open")
            payload = self.open_payload(route_ref)
            if payload is None:
                return ()
            return (payload.decode("utf-8", errors="replace"),)
        if command == "payload_demand":
            if route_ref is None:
                raise ValueError("payload_demand requires a route")
            self._authorize(request.principal_id, route_ref, "metadata_read")
            snapshot = self.payload_demand_snapshot(route_ref)
            return (
                snapshot.route_display,
                str(snapshot.metadata_events),
                str(snapshot.payload_open_requests),
                str(snapshot.lazy_source_opens),
                str(snapshot.materialized_payload_bytes),
                str(snapshot.cache_hits),
                str(snapshot.unopened_lazy_payloads),
            )
        if command == "watermark":
            if route_ref is None:
                return tuple(
                    f"{snapshot.route_display}|{snapshot.current_watermark}|{snapshot.latest_seq_source}|{snapshot.latest_control_epoch}"
                    for snapshot in self.watermark_snapshot()
                )
            self._authorize(request.principal_id, route_ref, "metadata_read")
            snapshot = self.watermark_snapshot(route_ref)
            return (
                snapshot.route_display,
                snapshot.partition_spec,
                snapshot.clock_domain,
                snapshot.event_time_policy,
                snapshot.watermark_policy,
                str(snapshot.latest_seq_source),
                str(snapshot.latest_control_epoch),
                str(snapshot.current_watermark),
            )
        if command == "audit":
            self._authorize(request.principal_id, route_ref, "debug_read")
            return tuple(
                f"{event.event_type}:{event.detail}" for event in self.audit(route_ref)
            )
        if command == "route_audit":
            if route_ref is None:
                raise ValueError("route_audit requires a route")
            self._authorize(request.principal_id, route_ref, "debug_read")
            snapshot = self.route_audit(route_ref)
            return (
                snapshot.route_display,
                f"scope={','.join(snapshot.scope_routes)}",
                f"producers={','.join(snapshot.recent_producers)}",
                f"subscribers={','.join(snapshot.active_subscribers)}",
                f"writes={','.join(snapshot.related_write_requests)}",
                f"taints={','.join(snapshot.taint_upper_bounds)}",
                f"repairs={','.join(snapshot.repair_notes)}",
                f"events={','.join(snapshot.recent_debug_events)}",
            )
        if command in {"lineage", "trace"}:
            if route_ref is not None:
                self._authorize(request.principal_id, route_ref, "debug_read")
            else:
                self._authorize(request.principal_id, None, "graph_validation")
            return tuple(
                record.display()
                for record in self.lineage(
                    route_ref,
                    trace_id=request.lineage_trace_id,
                    causality_id=request.lineage_causality_id,
                    correlation_id=request.lineage_correlation_id,
                )
            )
        if command == "shadow":
            if route_ref is None:
                raise ValueError("shadow requires a write request route")
            self._authorize(request.principal_id, route_ref, "metadata_read")
            shadow = self.shadow_state(route_ref)
            return tuple(
                item
                for item in (
                    None if shadow.request is None else str(shadow.request.seq_source),
                    None if shadow.desired is None else str(shadow.desired.seq_source),
                    None
                    if shadow.reported is None
                    else str(shadow.reported.seq_source),
                    None
                    if shadow.effective is None
                    else str(shadow.effective.seq_source),
                    None if shadow.ack is None else str(shadow.ack.seq_source),
                    "pending" if shadow.pending_write else "stable",
                    *shadow.coherence_taints,
                )
                if item is not None
            )
        if command == "taints":
            if route_ref is None:
                raise ValueError("taints requires a route")
            self._authorize(request.principal_id, route_ref, "metadata_read")
            return self._taint_query_items(route_ref)
        if command == "credit_snapshot":
            if route_ref is None:
                return tuple(
                    f"{snapshot.route_display}|{snapshot.available}|{snapshot.blocked_senders}|{snapshot.dropped_messages}"
                    for snapshot in self.credit_snapshot()
                )
            self._authorize(request.principal_id, route_ref, "metadata_read")
            snapshot = self.flow_snapshot(route_ref)
            return (
                snapshot.route_display,
                str(snapshot.available),
                str(snapshot.blocked_senders),
                str(snapshot.dropped_messages),
                snapshot.backpressure_policy,
            )
        if command == "scheduler":
            if route_ref is None:
                self._authorize(request.principal_id, None, "graph_validation")
                return tuple(
                    "|".join(
                        (
                            snapshot.route_display,
                            str(snapshot.scheduler_epoch),
                            str(snapshot.attempt_count),
                            str(snapshot.ready_now),
                            str(snapshot.ack_observed),
                            str(snapshot.not_before_epoch),
                            str(snapshot.next_retry_epoch),
                        )
                    )
                    for snapshot in self.scheduler_snapshot()
                )
            self._authorize(request.principal_id, route_ref, "metadata_read")
            return tuple(
                "|".join(
                    (
                        snapshot.route_display,
                        str(snapshot.scheduler_epoch),
                        str(snapshot.attempt_count),
                        str(snapshot.ready_now),
                        str(snapshot.ack_observed),
                        str(snapshot.not_before_epoch),
                        str(snapshot.next_retry_epoch),
                    )
                )
                for snapshot in self.scheduler_snapshot(route_ref)
            )
        raise ValueError(f"unsupported query command: {request.command}")

    def register_port(self, route_ref: RouteLike) -> RouteRef:
        """Register a route in the native graph and return its concrete ref."""
        return self._graph.register_port(self._coerce_route_ref(route_ref))

    def configure_retention(
        self,
        route_ref: RouteLike,
        policy: RouteRetentionPolicy,
    ) -> RouteRetentionPolicy:
        """Override replay and retention semantics for one route."""
        native_route = self.register_port(route_ref)
        key = native_route.display()
        default = self._default_retention_policy(native_route)
        resolved = RouteRetentionPolicy(
            latest_replay_policy=policy.latest_replay_policy,
            durability_class=policy.durability_class,
            replay_window=policy.replay_window,
            payload_retention_policy=(
                default.payload_retention_policy
                if policy.payload_retention_policy is None
                else policy.payload_retention_policy
            ),
            history_limit=policy.history_limit,
        )
        self._retention_policies[key] = resolved
        history = self._history.get(key)
        if history is not None and resolved.history_limit is not None:
            del history[: max(0, len(history) - resolved.history_limit)]
        self._enforce_payload_retention(native_route)
        return resolved

    def _read_port(self, route_ref: RouteLike) -> ReactiveReadablePort:
        native_route = self._coerce_route_ref(route_ref)
        return ReactiveReadablePort(self, native_route, self._graph.read(native_route))

    def _write_port(self, target: WriteTarget) -> WriteBinding | ReactiveWritablePort:
        if isinstance(target, LifecycleBinding):
            self._write_bindings[target.request.display()] = target.binding
            self._lifecycle_bindings[target.request.display()] = target
            return self._graph.register_binding(
                target.request.display(), target.binding
            )
        if isinstance(target, WriteBinding):
            self._write_bindings[target.request.display()] = target
            return self._graph.register_binding(target.request.display(), target)
        native_route = self._coerce_route_ref(target)
        return ReactiveWritablePort(
            self, native_route, self._graph.writable_port(native_route)
        )

    def _emit_native(
        self,
        route_ref: RouteRef,
        payload: bytes,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> list[ClosedEnvelope]:
        if hasattr(self._graph, "emit"):
            return cast(
                list[ClosedEnvelope],
                self._graph.emit(
                    route_ref, payload, producer=producer, control_epoch=control_epoch
                ),
            )
        envelope = self._graph.writable_port(route_ref).write(
            payload,
            producer=producer,
            control_epoch=control_epoch,
        )
        return [envelope]

    @overload
    def observe(
        self,
        route_ref: TypedRoute[T],
        *,
        replay_latest: bool = True,
        subscriber_id: str | None = None,
    ) -> RoutePipeline[T]: ...

    @overload
    def observe(
        self,
        route_ref: RouteRef,
        *,
        replay_latest: bool = True,
        subscriber_id: str | None = None,
    ) -> RoutePipeline[bytes]: ...

    def observe(
        self,
        route_ref: RouteLike,
        *,
        replay_latest: bool = True,
        subscriber_id: str | None = None,
    ) -> RoutePipeline[Any]:
        """Start a graph-owned pipeline from a route."""
        return RoutePipeline(
            self,
            route_ref,
            replay_latest=replay_latest,
            subscriber_id=subscriber_id,
        )

    def interval(
        self,
        period: timedelta,
        *,
        name: str = "interval",
    ) -> RoutePipeline[int]:
        """Start a graph-owned interval source pipeline."""
        return self._connect_interval_pipeline(IntervalNode(name=name, period=period))

    def _observe_observable(
        self,
        route_ref: RouteLike,
        *,
        replay_latest: bool = True,
        subscriber_id: str | None = None,
        thread_placement: NodeThreadPlacement | None = None,
    ) -> Observable[Any]:
        """Compatibility observable for route updates.

        Typed routes decode payloads before delivery; raw route refs expose the
        underlying closed envelopes.
        """
        native_route = self._coerce_route_ref(route_ref)
        typed_route = self._typed_route(route_ref)

        def subscribe(
            observer: ObserverLike[Any],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            key = self._route_key(native_route)
            resolved_subscriber_id = subscriber_id or self._next_subscriber_id()
            self._subscriber_count[key] = self._subscriber_count.get(key, 0) + 1
            self._route_subscribers.setdefault(key, set()).add(resolved_subscriber_id)

            if typed_route is not None:
                # Typed observers see decoded values, but the graph internally
                # continues to fan out closed envelopes on one shared subject.
                class _Observer:
                    def on_next(_, envelope) -> None:
                        observer.on_next(self._decode_envelope(typed_route, envelope))

                    def on_error(_, error: Exception) -> None:
                        observer.on_error(error)

                    def on_completed(_) -> None:
                        observer.on_completed()

                inner = self._subject_for(native_route).subscribe(
                    _Observer(), scheduler=scheduler
                )
            else:
                inner = self._subject_for(native_route).subscribe(
                    observer, scheduler=scheduler
                )
            try:
                if replay_latest:
                    latest = self.latest(route_ref)
                    if latest is not None:
                        observer.on_next(latest)
            except Exception:
                self._subscriber_count[key] -= 1
                subscribers = self._route_subscribers.get(key)
                if subscribers is not None:
                    subscribers.discard(resolved_subscriber_id)
                inner.dispose()
                raise
            return _TrackedSubscription(
                self,
                native_route,
                inner,
                resolved_subscriber_id,
            )

        return self._thread_placed_observable(
            rx.create(subscribe),
            thread_placement,
        )

    def register_pipeline_operation(
        self,
        name: str,
        factory: Callable[..., Any],
        *,
        replace_existing: bool = False,
    ) -> None:
        """Register a fluent pipeline operation factory."""
        if name in self._pipeline_factories and not replace_existing:
            raise ValueError(f"pipeline operation already registered: {name}")
        self._pipeline_factories[name] = factory

    def _pipeline_value_observable(
        self,
        route_ref: RouteLike,
        *,
        replay_latest: bool,
        subscriber_id: str | None,
        thread_placement: NodeThreadPlacement | None = None,
    ) -> Observable[Any]:
        native_route = self._coerce_route_ref(route_ref)

        def subscribe(
            observer: ObserverLike[Any],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            def on_next(item: TypedEnvelope[Any] | ClosedEnvelope) -> None:
                observer.on_next(self._operator_value(route_ref, native_route, item))

            return self._observe_observable(
                route_ref,
                replay_latest=replay_latest,
                subscriber_id=subscriber_id,
            ).subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler,
            )

        return self._thread_placed_observable(
            rx.create(subscribe),
            thread_placement,
        )

    def _thread_placed_observable(
        self,
        observable: Observable[Any],
        placement: NodeThreadPlacement | None,
    ) -> Observable[Any]:
        if placement is None:
            return observable
        if placement.kind == "main":
            return deliver_on_frame_thread(observable)
        thread_name = placement.display()
        if placement.kind == "isolated":
            thread_name = placement.thread_name or self._next_pipeline_node_name("isolated")
        return _observe_on_thread(observable, thread_name)

    def _pipeline_output_route(self, node_name: str) -> TypedRoute[Any]:
        route_id = next(_PIPELINE_ROUTE_IDS)
        safe_name = "".join(
            char if char.isalnum() or char in ("-", "_") else "-"
            for char in node_name
        ).strip("-") or "node"
        return route(
            plane=Plane.Read,
            layer=Layer.Internal,
            owner=OwnerName("manyfold.graph"),
            family=StreamFamily("pipeline"),
            stream=StreamName(f"{safe_name}-{route_id}"),
            variant=Variant.Event,
            schema=Schema.any(f"ManyfoldPipeline{route_id}"),
        )

    def _connect_callback_pipeline(
        self,
        source: RouteLike,
        node: CallbackNode[Any],
        *,
        replay_latest: bool,
        subscriber_id: str | None,
        thread_placement: NodeThreadPlacement | None,
    ) -> GraphConnection:
        self.register_diagram_node(
            node.name,
            input_routes=(source,),
            thread_placement=thread_placement,
        )
        subscription = self._pipeline_value_observable(
            source,
            replay_latest=replay_latest,
            subscriber_id=subscriber_id,
            thread_placement=thread_placement,
        ).subscribe(node.receive)
        return GraphConnection(
            self,
            name=node.name,
            subscriptions=(subscription,),
            nodes=(node.name,),
        )

    def _connect_route_pipeline(
        self,
        source: RouteLike,
        target: RouteLike,
        *,
        replay_latest: bool,
        subscriber_id: str | None,
        name: str | None = None,
        thread_placement: NodeThreadPlacement | None = None,
    ) -> GraphConnection:
        connection_name = name or self._next_pipeline_node_name("route")
        self.connect(source=source, sink=target)

        def publish(value: Any) -> None:
            self.publish(target, value)

        subscription = self._pipeline_value_observable(
            source,
            replay_latest=replay_latest,
            subscriber_id=subscriber_id,
            thread_placement=thread_placement,
        ).subscribe(publish)
        return GraphConnection(
            self,
            name=connection_name,
            subscriptions=(subscription,),
        )

    def _connect_transform_pipeline(
        self,
        pipeline: RoutePipeline[Any],
        node: MapNode[Any, Any] | FilterNode[Any],
        apply: Callable[[Any], tuple[bool, Any]],
    ) -> RoutePipeline[Any]:
        output = self._pipeline_output_route(node.name)
        thread_placement = node.thread_placement or pipeline._thread_placement
        self.register_diagram_node(
            node.name,
            input_routes=(pipeline.route,),
            output_routes=(output,),
            thread_placement=thread_placement,
        )

        def on_next(value: Any) -> None:
            should_emit, next_value = apply(value)
            if should_emit:
                self.publish(output, next_value)

        subscription = self._pipeline_value_observable(
            pipeline.route,
            replay_latest=pipeline._replay_latest,
            subscriber_id=pipeline._subscriber_id,
            thread_placement=thread_placement,
        ).subscribe(on_next)
        connection = GraphConnection(
            self,
            name=node.name,
            subscriptions=(subscription,),
            nodes=(node.name,),
        )
        return RoutePipeline(
            self,
            output,
            replay_latest=False,
            connections=(*pipeline._connections, connection),
            thread_placement=thread_placement,
        )

    def _connect_coalesce_latest_pipeline(
        self,
        pipeline: RoutePipeline[T],
        node: CoalesceLatestNode[T],
    ) -> RoutePipeline[T]:
        output = self._pipeline_output_route(node.name)
        self.register_diagram_node(
            node.name,
            input_routes=(pipeline.route,),
            output_routes=(output,),
        )

        def publish(value: T) -> None:
            self.publish(output, value)

        coalesced = node.observable(
            self._pipeline_value_observable(
                pipeline.route,
                replay_latest=pipeline._replay_latest,
                subscriber_id=pipeline._subscriber_id,
            )
        )
        subscription = coalesced.subscribe(publish)
        connection = GraphConnection(
            self,
            name=node.name,
            subscriptions=(subscription,),
            nodes=(node.name,),
        )
        return RoutePipeline(
            self,
            output,
            replay_latest=False,
            connections=(*pipeline._connections, connection),
        )

    def _connect_interval_pipeline(self, node: IntervalNode) -> RoutePipeline[int]:
        output = self._pipeline_output_route(node.name)
        self.register_diagram_node(
            node.name,
            output_routes=(output,),
            thread_placement=NodeThreadPlacement.background_thread(),
        )

        def publish(value: int) -> None:
            self.publish(output, value)

        subscription = node.observable().subscribe(publish)
        connection = GraphConnection(
            self,
            name=node.name,
            subscriptions=(subscription,),
            nodes=(node.name,),
        )
        return RoutePipeline(
            self,
            output,
            replay_latest=False,
            connections=(connection,),
            thread_placement=None,
        )

    def _connect_logging_pipeline(
        self,
        pipeline: RoutePipeline[T],
        node: PipelineLoggingNode[T],
    ) -> RoutePipeline[T]:
        output = self._pipeline_output_route(node.name)
        thread_placement = node.thread_placement or pipeline._thread_placement
        self.register_diagram_node(
            node.name,
            input_routes=(pipeline.route,),
            output_routes=(output,),
            thread_placement=thread_placement,
        )

        def publish(value: T) -> None:
            self.publish(output, value)

        logged = node.observable(
            self._pipeline_value_observable(
                pipeline.route,
                replay_latest=pipeline._replay_latest,
                subscriber_id=pipeline._subscriber_id,
                thread_placement=thread_placement,
            )
        )
        subscription = logged.subscribe(publish)
        connection = GraphConnection(
            self,
            name=node.name,
            subscriptions=(subscription,),
            nodes=(node.name,),
        )
        return RoutePipeline(
            self,
            output,
            replay_latest=False,
            connections=(*pipeline._connections, connection),
            thread_placement=thread_placement,
        )

    def _apply_registered_pipeline_operation(
        self,
        pipeline: RoutePipeline[Any],
        operation: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        factory = self._pipeline_factories.get(operation)
        if factory is None:
            raise AttributeError(operation)
        node = factory(*args, **kwargs)
        if isinstance(node, MapNode):
            return self._connect_transform_pipeline(
                pipeline,
                node,
                lambda value: (True, node.transform(value)),
            )
        if isinstance(node, FilterNode):
            return self._connect_transform_pipeline(
                pipeline,
                node,
                lambda value: (node.predicate(value), value),
            )
        if isinstance(node, CoalesceLatestNode):
            return self._connect_coalesce_latest_pipeline(pipeline, node)
        if isinstance(node, PipelineLoggingNode):
            return self._connect_logging_pipeline(pipeline, node)
        if isinstance(node, CallbackNode):
            return pipeline.connect(
                node.with_thread_placement(
                    node.thread_placement or pipeline._thread_placement
                )
            )
        raise TypeError(
            f"registered pipeline operation {operation!r} returned unsupported node {type(node).__name__}"
        )

    def pipe(
        self,
        source: ObservableLike[TIn] | ObservableLike[bytes],
        target: RouteLike,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> SubscriptionLike:
        """Bind an observable source into a route."""
        typed_target = self._typed_route(target)
        if typed_target is not None:

            class _Observer:
                def on_next(_, value: TIn) -> None:
                    self.publish(
                        typed_target,
                        value,
                        producer=producer,
                        control_epoch=control_epoch,
                    )

                def on_error(_, error: Exception) -> None:
                    raise error

                def on_completed(_) -> None:
                    return None

            return source.subscribe(_Observer())
        return self._write_port(target).bind(
            source, producer=producer, control_epoch=control_epoch
        )  # type: ignore[union-attr]

    @overload
    def publish(
        self,
        target: TypedRoute[T],
        payload: T,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
        trace_id: str | None = None,
        causality_id: str | None = None,
        correlation_id: str | None = None,
        parent_events: Sequence[EventRef] = (),
    ) -> TypedEnvelope[T]: ...

    @overload
    def publish(
        self,
        target: RouteRef,
        payload: bytes,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
        trace_id: str | None = None,
        causality_id: str | None = None,
        correlation_id: str | None = None,
        parent_events: Sequence[EventRef] = (),
    ) -> ClosedEnvelope: ...

    def publish(
        self,
        target: WriteTarget,
        payload: Any,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
        trace_id: str | None = None,
        causality_id: str | None = None,
        correlation_id: str | None = None,
        parent_events: Sequence[EventRef] = (),
    ) -> TypedEnvelope[Any] | ClosedEnvelope:
        """Write one payload to a route and return the resulting envelope."""
        if isinstance(target, LifecycleBinding):
            self._write_bindings[target.request.display()] = target.binding
            self._lifecycle_bindings[target.request.display()] = target
            target = target.binding
        if isinstance(target, WriteBinding):
            binding = self._resolve_write_binding(target)
            self._graph.register_binding(binding.request.display(), binding)
            emitted = self._emit_native(
                target.request,
                bytes(payload),
                producer=producer,
                control_epoch=control_epoch,
            )
            if not any(envelope.route == binding.desired for envelope in emitted):
                emitted.extend(
                    self._emit_native(
                        binding.desired,
                        bytes(payload),
                        producer=producer,
                        control_epoch=control_epoch,
                    )
                )
            for envelope in emitted:
                producer_id = "python" if producer is None else producer.producer_id
                self._record_envelope(
                    envelope.route,
                    envelope,
                    producer_id=producer_id,
                    trace_id=trace_id,
                    causality_id=causality_id,
                    correlation_id=correlation_id,
                    parent_events=parent_events,
                )
            self._refresh_binding_coherence(binding)
            self._emit_debug_event(
                "write", f"published {binding.request.display()}", binding.request
            )
            return emitted[0]
        typed_target = self._typed_route(target)
        if typed_target is not None:
            encoded = typed_target.schema.encode(payload)
            emitted = self._emit_native(
                typed_target.route_ref,
                encoded,
                producer=producer,
                control_epoch=control_epoch,
            )
            envelope = emitted[0]
            producer_id = "python" if producer is None else producer.producer_id
            for emitted_envelope in emitted:
                self._record_envelope(
                    emitted_envelope.route,
                    emitted_envelope,
                    producer_id=producer_id,
                    trace_id=trace_id,
                    causality_id=causality_id,
                    correlation_id=correlation_id,
                    parent_events=parent_events,
                )
            self._emit_debug_event(
                "write", f"published {typed_target.display()}", typed_target
            )
            return self._decode_envelope(typed_target, envelope)
        emitted = self._emit_native(
            self._coerce_route_ref(target),
            payload,
            producer=producer,
            control_epoch=control_epoch,
        )
        envelope = emitted[0]
        producer_id = "python" if producer is None else producer.producer_id
        for emitted_envelope in emitted:
            self._record_envelope(
                emitted_envelope.route,
                emitted_envelope,
                producer_id=producer_id,
                trace_id=trace_id,
                causality_id=causality_id,
                correlation_id=correlation_id,
                parent_events=parent_events,
            )
        self._emit_debug_event("write", f"published {self._route_key(target)}", target)
        return envelope

    def publish_lazy(
        self,
        target: RouteLike,
        payload_source: LazyPayloadSource,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
        trace_id: str | None = None,
        causality_id: str | None = None,
        correlation_id: str | None = None,
        parent_events: Sequence[EventRef] = (),
    ) -> ClosedEnvelope:
        """Publish metadata now and defer payload materialization until opened."""
        native_route = self._coerce_route_ref(target)
        emitted = self._emit_native(
            native_route, b"", producer=producer, control_epoch=control_epoch
        )
        producer_id = "python" if producer is None else producer.producer_id
        for envelope in emitted:
            self._lazy_payload_sources[envelope.payload_ref.payload_id] = payload_source
            self._record_envelope(
                envelope.route,
                envelope,
                producer_id=producer_id,
                trace_id=trace_id,
                causality_id=causality_id,
                correlation_id=correlation_id,
                parent_events=parent_events,
            )
        self._emit_debug_event(
            "write",
            f"published lazy payload for {native_route.display()}",
            native_route,
        )
        return emitted[0]

    def mailbox(
        self,
        name: str,
        descriptor: NativeMailboxDescriptor | None = None,
    ) -> NativeMailbox:
        """Create or return a named mailbox from the native graph."""
        resolved = descriptor if descriptor is not None else NativeMailboxDescriptor()
        self._mailbox_descriptors[name] = resolved
        mailbox = self._graph.mailbox(name, resolved)
        mailbox_policy = FlowPolicy(
            backpressure_policy="propagate",
            credit_class="mailbox",
            mailbox_policy="queue",
            async_boundary_kind="mailbox",
            overflow_policy=resolved.overflow_policy,
        )
        self._mailbox_flow_policies[
            mailbox.ingress.describe().route_display
        ] = mailbox_policy
        self._mailbox_flow_policies[
            mailbox.egress.describe().route_display
        ] = mailbox_policy
        return mailbox

    def connect(
        self,
        *,
        source: ConnectableTarget,
        sink: ConnectableTarget,
        flow_policy: FlowPolicy | None = None,
    ) -> None:
        """Connect two routes in topology metadata."""
        native_source = (
            source
            if isinstance(source, NativeMailbox)
            else self._coerce_route_ref(source)
        )
        native_sink = (
            sink if isinstance(sink, NativeMailbox) else self._coerce_route_ref(sink)
        )
        self._graph.connect(native_source, native_sink)
        if flow_policy is not None:
            self._edge_flow_overrides[
                (
                    self._connectable_key(source, edge_role="source"),
                    self._connectable_key(sink, edge_role="sink"),
                )
            ] = flow_policy
        self._emit_debug_event(
            "topology",
            f"connected {self._connectable_key(source, edge_role='source')} -> {self._connectable_key(sink, edge_role='sink')}",
            None
            if isinstance(source, NativeMailbox)
            else self._coerce_route_ref(source),
        )

    def capacitor(
        self,
        *,
        source: RouteLike,
        sink: RouteLike,
        capacity: int = 1,
        demand: RouteLike | None = None,
        immediate: bool = False,
        overflow: str = "latest",
        name: str | None = None,
    ) -> Capacitor:
        """Install active bounded storage between source and sink."""
        if capacity <= 0:
            raise ValueError("capacitor capacity must be positive")
        if overflow not in {"latest", "drop_oldest", "reject"}:
            raise ValueError(
                "overflow must be one of 'latest', 'drop_oldest', or 'reject'"
            )
        if immediate and demand is not None:
            raise ValueError("pass either immediate=True or demand, not both")
        resolved_name = name or self._auto_node_name(
            "capacitor",
            source,
            sink,
            f"cap={capacity}",
            "immediate" if immediate else "demand" if demand is not None else "",
            f"overflow={overflow}",
        )
        capacitor = Capacitor(
            name=resolved_name,
            source=source,
            sink=sink,
            capacity=capacity,
            demand=demand,
            immediate=immediate,
            overflow=overflow,
        )
        self._capacitors[resolved_name] = capacitor

        source_route = self._coerce_route_ref(source)
        buffer: deque[
            tuple[T | bytes, ClosedEnvelope, LineageRecord, tuple[Any, ...]]
        ] = deque()

        def retain(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
            if len(buffer) >= capacity:
                if overflow == "reject":
                    return
                buffer.popleft()
            closed = self._closed_item(item)
            buffer.append(
                (
                    self._operator_value(source, source_route, item),
                    closed,
                    self._lineage_for_item(item),
                    self._item_taints(item),
                )
            )
            if immediate:
                discharge_one()

        def discharge_one() -> None:
            if not buffer:
                return
            value, closed, lineage, taints = buffer.popleft()
            emitted = self.publish(
                sink,
                cast(Any, value),
                control_epoch=closed.control_epoch,
                trace_id=lineage.trace_id,
                causality_id=lineage.causality_id,
                correlation_id=lineage.correlation_id,
                parent_events=(lineage.event,),
            )
            self._extend_taints(emitted, taints)

        source_sub = self._observe_observable(
            source, replay_latest=self._source_replay_latest(source)
        ).subscribe(retain)
        self._subscriptions.append(source_sub)
        if demand is not None:
            signal_sub = self._observe_observable(demand, replay_latest=False).subscribe(
                lambda _item: discharge_one()
            )
            self._subscriptions.append(signal_sub)
        return capacitor

    def resistor(
        self,
        *,
        source: RouteLike,
        sink: RouteLike,
        gate: Callable[[Any], bool] | None = None,
        release: RouteLike | None = None,
        name: str | None = None,
    ) -> Resistor:
        """Install a graph-visible flow shaper between source and sink."""
        resolved_name = name or self._auto_node_name(
            "resistor",
            source,
            sink,
            "gated" if gate is not None else "pass",
            "release" if release is not None else "",
        )
        resistor = Resistor(
            name=resolved_name,
            source=source,
            sink=sink,
            gate=gate,
            release=release,
        )
        self._resistors[resolved_name] = resistor
        source_route = self._coerce_route_ref(source)
        latest: tuple[
            T | bytes, ClosedEnvelope, LineageRecord, tuple[Any, ...]
        ] | None = None

        def emit(
            value: T | bytes,
            closed: ClosedEnvelope,
            lineage: LineageRecord,
            taints: tuple[Any, ...],
        ) -> None:
            emitted = self.publish(
                sink,
                cast(Any, value),
                control_epoch=closed.control_epoch,
                trace_id=lineage.trace_id,
                causality_id=lineage.causality_id,
                correlation_id=lineage.correlation_id,
                parent_events=(lineage.event,),
            )
            self._extend_taints(emitted, taints)

        def on_source(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
            nonlocal latest
            value = self._operator_value(source, source_route, item)
            if gate is not None and not gate(value):
                return
            closed = self._closed_item(item)
            lineage = self._lineage_for_item(item)
            taints = self._item_taints(item)
            latest = (value, closed, lineage, taints)
            if release is not None:
                return
            emit(value, closed, lineage, taints)

        source_sub = self._observe_observable(
            source, replay_latest=self._source_replay_latest(source)
        ).subscribe(on_source)
        self._subscriptions.append(source_sub)
        if release is not None:

            def on_signal(_item: TypedEnvelope[Any] | ClosedEnvelope) -> None:
                if latest is not None:
                    emit(*latest)

            signal_sub = self._observe_observable(release, replay_latest=False).subscribe(
                on_signal
            )
            self._subscriptions.append(signal_sub)
        return resistor

    def watchdog(
        self,
        *,
        reset_by: RouteLike,
        output: RouteLike,
        after: int,
        clock: RouteLike,
        pulse: Any = b"timeout",
        name: str | None = None,
    ) -> Watchdog:
        """Emit a timeout pulse when reset signal is absent for too long."""
        if after <= 0:
            raise ValueError("watchdog timeout must be positive")
        resolved_name = name or self._auto_node_name(
            "watchdog",
            reset_by,
            output,
            f"after={after}",
            f"clock={self._route_key(clock)}",
        )
        watchdog = Watchdog(
            name=resolved_name,
            reset_by=reset_by,
            output=output,
            after=after,
            clock=clock,
            pulse=pulse,
        )
        self._watchdogs[resolved_name] = watchdog
        reset_route = self._coerce_route_ref(reset_by)
        clock_route = self._coerce_route_ref(clock)
        last_reset_progress = 0
        emitted_since_reset = False

        def on_reset(item: TypedEnvelope[Any] | ClosedEnvelope) -> None:
            nonlocal last_reset_progress, emitted_since_reset
            last_reset_progress = self._progress_value(reset_by, reset_route, item)
            emitted_since_reset = False

        def on_clock(item: TypedEnvelope[Any] | ClosedEnvelope) -> None:
            nonlocal emitted_since_reset
            progress = self._progress_value(clock, clock_route, item)
            if emitted_since_reset or progress - last_reset_progress < after:
                return
            self.publish(output, pulse, control_epoch=progress)
            emitted_since_reset = True

        reset_sub = self._observe_observable(reset_by, replay_latest=True).subscribe(on_reset)
        clock_sub = self._observe_observable(clock, replay_latest=False).subscribe(on_clock)
        self._subscriptions.append(reset_sub)
        self._subscriptions.append(clock_sub)
        return watchdog

    def lifecycle(
        self,
        owner: OwnerName,
        family: StreamFamily,
        *,
        intent_schema: Schema[bytes],
        observation_schema: Schema[bytes] | None = None,
        layer: Layer = Layer.Raw,
        stream: StreamName | None = None,
        event_schema: Schema[bytes] | None = None,
        ack_schema: Schema[bytes] | None = None,
        health_schema: Schema[bytes] | None = None,
    ) -> LifecycleBinding:
        """Build and register an RFC-shaped lifecycle binding."""
        binding = WriteBindings.lifecycle(
            owner,
            family,
            intent_schema=intent_schema,
            observation_schema=observation_schema,
            layer=layer,
            stream=stream,
            event_schema=event_schema,
            ack_schema=ack_schema,
            health_schema=health_schema,
        )
        self._write_bindings[binding.request.display()] = binding.binding
        self._lifecycle_bindings[binding.request.display()] = binding
        self._graph.register_binding(binding.request.display(), binding.binding)
        self.register_port(binding.event)
        if binding.health is not None:
            self.register_port(binding.health)
        return binding

    def install(
        self, control_loop: NativeControlLoop | ReadThenWriteNextEpochStep[Any, Any]
    ) -> None:
        """Install a native control loop or a shared-stream Python step."""
        if isinstance(control_loop, ReadThenWriteNextEpochStep):
            subscription = control_loop.write.subscribe(
                lambda value: self.publish(control_loop.output, value)
            )
            self._subscriptions.append(subscription)
            self._subscriptions.append(control_loop.start())
            return
        self._graph.install(control_loop)

    def _tick_control_loop(self, name: str) -> ClosedEnvelope:
        return self._graph.tick_control_loop(name)

    def run_control_loop(self, name: str) -> ClosedEnvelope:
        """Advance one installed control loop once."""
        envelope = self._tick_control_loop(name)
        self._record_envelope(envelope.route, envelope, producer_id=name)
        self._emit_debug_event(
            "scheduler", f"ticked control loop {name}", envelope.route
        )
        return envelope

    def catalog(self) -> Iterator[RouteRef]:
        """Return all registered routes."""
        return iter(tuple(self._graph.catalog()))

    def describe_route(self, route_ref: RouteLike) -> RouteDescriptor:
        """Return the descriptor for one route."""
        native_route = self._coerce_route_ref(route_ref)
        native = self._graph.describe_route(native_route)
        return self._descriptor_defaults(native_route, native)

    def configure_flow_defaults(self, policy: FlowPolicy) -> None:
        """Overlay graph-wide flow defaults used by describe_edge()."""
        self._graph_flow_defaults = self._graph_flow_defaults.merged(policy)

    def configure_source_flow(self, route_ref: RouteLike, policy: FlowPolicy) -> None:
        """Register flow defaults that apply when a route is the edge source."""
        key = self._route_key(route_ref)
        current = self._source_flow_defaults.get(key, FlowPolicy())
        self._source_flow_defaults[key] = current.merged(policy)

    def configure_sink_flow(self, route_ref: RouteLike, policy: FlowPolicy) -> None:
        """Register flow requirements that apply when a route is the edge sink."""
        key = self._route_key(route_ref)
        current = self._sink_flow_requirements.get(key, FlowPolicy())
        self._sink_flow_requirements[key] = current.merged(policy)

    def describe_edge(
        self,
        *,
        source: ConnectableTarget,
        sink: ConnectableTarget,
    ) -> DescriptorFlowBlock:
        """Resolve the RFC flow descriptor for one source->sink edge."""
        return self._resolved_edge_flow_policy(source, sink)

    @overload
    def latest(self, route_ref: TypedRoute[T]) -> TypedEnvelope[T] | None: ...

    @overload
    def latest(self, route_ref: RouteRef) -> ClosedEnvelope | None: ...

    def latest(
        self, route_ref: RouteLike
    ) -> TypedEnvelope[Any] | ClosedEnvelope | None:
        """Return the latest envelope seen for one route."""
        latest = self._graph.latest(self._coerce_route_ref(route_ref))
        if latest is None:
            return None
        typed_route = self._typed_route(route_ref)
        if typed_route is not None:
            return self._decode_envelope(typed_route, latest)
        return latest

    def open_payload(self, route_ref: RouteLike | ClosedEnvelope) -> bytes | None:
        """Open payload bytes for the latest route event or one specific envelope."""
        if isinstance(route_ref, ClosedEnvelope):
            return self._known_payload_bytes(
                route_ref.route,
                route_ref,
                record_open=True,
            )
        native_route = self._coerce_route_ref(route_ref)
        latest = self._graph.latest(native_route)
        if latest is None:
            return None
        return self._resolve_payload_bytes(native_route, latest, record_open=True)

    def payload_demand_snapshot(self, route_ref: RouteLike) -> PayloadDemandSnapshot:
        """Return metadata-versus-payload demand accounting for one route."""
        native_route = self._coerce_route_ref(route_ref)
        key = self._route_key(native_route)
        stats = self._payload_stats(native_route)
        unopened_lazy_payloads = 0
        for payload_id, payload_route in self._payload_route_by_id.items():
            if payload_route != key:
                continue
            if (
                payload_id in self._lazy_payload_sources
                and payload_id not in self._opened_payload_ids
            ):
                unopened_lazy_payloads += 1
        return PayloadDemandSnapshot(
            route_display=key,
            metadata_events=stats["metadata_events"],
            payload_open_requests=stats["payload_open_requests"],
            lazy_source_opens=stats["lazy_source_opens"],
            materialized_payload_bytes=stats["materialized_payload_bytes"],
            cache_hits=stats["cache_hits"],
            unopened_lazy_payloads=unopened_lazy_payloads,
        )

    @overload
    def watermark_snapshot(self) -> Iterator[WatermarkSnapshot]: ...

    @overload
    def watermark_snapshot(self, route_ref: RouteLike) -> WatermarkSnapshot: ...

    def watermark_snapshot(
        self,
        route_ref: RouteLike | None = None,
    ) -> WatermarkSnapshot | Iterator[WatermarkSnapshot]:
        """Return watermark progress for one route or the whole catalog."""
        if route_ref is None:
            return iter(
                tuple(
                    self._watermark_snapshot_for_route(route)
                    for route in self.catalog()
                )
            )
        return self._watermark_snapshot_for_route(self._coerce_route_ref(route_ref))

    def scheduler_snapshot(
        self,
        route_ref: RouteLike | None = None,
    ) -> Iterator[ScheduledWriteSnapshot]:
        """Return queued guarded-write state, optionally filtered to one route."""
        route_display = None if route_ref is None else self._route_key(route_ref)
        snapshots: list[ScheduledWriteSnapshot] = []
        for scheduled in self._pending_writes:
            snapshot = self._scheduled_write_snapshot(scheduled)
            if snapshot is None:
                continue
            if route_display is not None and snapshot.route_display != route_display:
                continue
            snapshots.append(snapshot)
        return iter(tuple(snapshots))

    def topology(self) -> Iterator[tuple[str, str]]:
        """Return graph edges as `(source, sink)` display pairs."""
        return iter(tuple(self._graph.topology()))

    def register_diagram_node(
        self,
        name: str,
        *,
        input_routes: Sequence[RouteLike] = (),
        output_routes: Sequence[RouteLike] = (),
        group: str | None = None,
        thread_placement: NodeThreadPlacement | None = None,
    ) -> DiagramNode:
        """Register a graph-visible node for topology diagrams."""
        for route_ref in tuple(input_routes) + tuple(output_routes):
            coerced = self._coerce_route_ref(route_ref)
            self._diagram_routes[coerced.display()] = coerced
        node = DiagramNode(
            name=name,
            input_routes=tuple(self._route_key(route) for route in input_routes),
            output_routes=tuple(self._route_key(route) for route in output_routes),
            group=group,
            thread_placement=thread_placement,
        )
        self._diagram_nodes[name] = node
        return node

    def diagram_nodes(self) -> Iterator[DiagramNode]:
        """Return graph-visible nodes registered for diagram rendering."""
        return iter(tuple(self._diagram_nodes.values()))

    def diagram(
        self,
        *,
        format: str = "mermaid",
        direction: str = "LR",
        group_by: Sequence[str] = ("owner",),
    ) -> str:
        """Render the graph topology as a small human-readable diagram."""
        return self.render_diagram(
            format=format,
            direction=direction,
            group_by=group_by,
        )

    def render_diagram(
        self,
        *,
        format: str = "mermaid",
        direction: str = "LR",
        group_by: Sequence[str] = ("owner",),
    ) -> str:
        """Render topology edges as a Mermaid flowchart.

        Grouping is intentionally descriptor-driven. Callers can cluster by any
        stable route field such as ``("layer", "owner")`` without manually
        placing nodes.
        """
        resolved_format = format.lower()
        if resolved_format not in ("mermaid", "mermaid_flowchart"):
            raise ValueError("only Mermaid diagram rendering is supported")
        unknown_fields = tuple(
            field for field in group_by if field not in DIAGRAM_GROUP_FIELDS
        )
        if unknown_fields:
            raise ValueError(
                "unsupported diagram group fields: " + ", ".join(unknown_fields)
            )

        edges = self._diagram_edges()
        node_names = tuple(
            sorted(
                {
                    *(name for edge in edges for name in edge),
                    *(
                        self._diagram_registered_node_key(node.name)
                        for node in self._diagram_nodes.values()
                    ),
                }
            )
        )
        node_ids = {
            node_name: f"n{index}"
            for index, node_name in enumerate(node_names)
        }
        route_refs = {
            **{route.display(): route for route in self.catalog()},
            **self._diagram_routes,
        }
        metadata = {
            node_name: self._diagram_node_metadata(node_name, route_refs)
            for node_name in node_names
        }

        lines = [f"flowchart {direction}"]
        if not node_names:
            lines.append("  %% graph has no topology edges")
            return "\n".join(lines)

        grouped_nodes: dict[tuple[str, ...], list[str]] = {}
        ungrouped_nodes: list[str] = []
        for node_name in node_names:
            group_key = tuple(metadata[node_name].get(field, "") for field in group_by)
            if group_key and any(group_key):
                grouped_nodes.setdefault(group_key, []).append(node_name)
            else:
                ungrouped_nodes.append(node_name)

        group_index = 0
        for group_key in sorted(grouped_nodes):
            group_label = " / ".join(part for part in group_key if part)
            lines.append(
                f"  subgraph g{group_index}[\"{self._diagram_escape(group_label)}\"]"
            )
            for node_name in grouped_nodes[group_key]:
                lines.append(
                    self._diagram_node_line(node_ids[node_name], metadata[node_name])
                )
            lines.append("  end")
            group_index += 1

        for node_name in ungrouped_nodes:
            lines.append(
                self._diagram_node_line(node_ids[node_name], metadata[node_name])
            )

        for source_name, sink_name in edges:
            lines.append(f"  {node_ids[source_name]} --> {node_ids[sink_name]}")
        return "\n".join(lines)

    def _diagram_edges(self) -> tuple[tuple[str, str], ...]:
        edges = list(self.topology())
        for node in self._diagram_nodes.values():
            node_key = self._diagram_registered_node_key(node.name)
            edges.extend((route, node_key) for route in node.input_routes)
            edges.extend((node_key, route) for route in node.output_routes)
        return tuple(edges)

    @staticmethod
    def _diagram_registered_node_key(name: str) -> str:
        return f"node:{name}"

    def _diagram_node_metadata(
        self,
        node_name: str,
        route_refs: dict[str, RouteRef],
    ) -> dict[str, str]:
        if node_name.startswith("node:"):
            node = self._diagram_nodes.get(node_name.removeprefix("node:"))
            if node is not None:
                return self._diagram_registered_node_metadata(node)
        return self._diagram_route_metadata(node_name, route_refs)

    def _diagram_registered_node_metadata(self, node: DiagramNode) -> dict[str, str]:
        group = node.group or "nodes"
        return {
            "display": self._diagram_registered_node_key(node.name),
            "plane": "node",
            "layer": "node",
            "owner": group,
            "family": group,
            "stream": node.name,
            "variant": "node",
            "label": node.name,
            "thread": (
                ""
                if node.thread_placement is None
                else node.thread_placement.display()
            ),
        }

    def _diagram_route_metadata(
        self,
        route_display: str,
        route_refs: dict[str, RouteRef],
    ) -> dict[str, str]:
        route_ref = route_refs.get(route_display)
        if route_ref is None:
            return {
                "display": route_display,
                "plane": "",
                "layer": "",
                "owner": "",
                "family": "",
                "stream": route_display,
                "variant": "",
                "label": route_display,
            }
        plane = self._diagram_value(route_ref.namespace.plane)
        layer = self._diagram_value(route_ref.namespace.layer)
        owner = self._diagram_value(route_ref.namespace.owner)
        family = self._diagram_value(route_ref.family)
        stream = self._diagram_value(route_ref.stream)
        variant = self._diagram_value(route_ref.variant).lower()
        label = f"{family}.{stream}<br/>{variant}"
        return {
            "display": route_display,
            "plane": plane,
            "layer": layer,
            "owner": owner,
            "family": family,
            "stream": stream,
            "variant": variant,
            "label": label,
        }

    @staticmethod
    def _diagram_value(value: Any) -> str:
        raw = getattr(value, "value", value)
        return str(raw)

    @staticmethod
    def _diagram_escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _diagram_node_line(self, node_id: str, metadata: dict[str, str]) -> str:
        label = self._diagram_escape(metadata["label"])
        return f"    {node_id}[\"{label}\"]"

    def validate_graph(self) -> Iterator[str]:
        """Return graph validation issues detected by the native layer and wrapper semantics."""
        issues = list(self._graph.validate_graph())
        for catalog_route in self.catalog():
            route = self._coerce_route_ref(catalog_route)
            if (
                route.namespace.plane == Plane.Write
                and route.variant == Variant.Request
                and route.namespace.layer != Layer.Internal
                and route.display() not in self._write_bindings
            ):
                issues.append(
                    f"Write request route {route.display()} lacks a shadow binding"
                )
        issues.extend(self._unsafe_write_feedback_issues())
        return iter(tuple(issues))

    def _unsafe_write_feedback_issues(self) -> list[str]:
        adjacency: dict[str, set[str]] = {}
        route_refs: dict[str, RouteRef] = {}
        for left, right in self.topology():
            adjacency.setdefault(left, set()).add(right)
        for catalog_route in self.catalog():
            route_refs[catalog_route.display()] = catalog_route

        issues: list[str] = []
        for binding in self._write_bindings.values():
            request = binding.request.display()
            feedback_sources = (
                binding.reported.display(),
                binding.effective.display(),
            )
            for feedback_source in feedback_sources:
                if not self._path_exists(adjacency, feedback_source, request):
                    continue
                if self._path_has_boundary(
                    adjacency, route_refs, feedback_source, request
                ):
                    continue
                ack_seen = (
                    binding.ack is not None and self.latest(binding.ack) is not None
                )
                if ack_seen:
                    continue
                issues.append(
                    f"Unsafe write-back loop from {feedback_source} to {request} lacks mailbox, internal boundary, epoch guard, or ack barrier"
                )
        return issues

    @staticmethod
    def _path_exists(adjacency: dict[str, set[str]], start: str, goal: str) -> bool:
        pending = [start]
        seen: set[str] = set()
        while pending:
            current = pending.pop()
            if current == goal:
                return True
            if current in seen:
                continue
            seen.add(current)
            pending.extend(adjacency.get(current, ()))
        return False

    @staticmethod
    def _path_has_boundary(
        adjacency: dict[str, set[str]],
        route_refs: dict[str, RouteRef],
        start: str,
        goal: str,
    ) -> bool:
        pending: list[tuple[str, bool]] = [(start, False)]
        seen: set[tuple[str, bool]] = set()
        while pending:
            current, has_boundary = pending.pop()
            if current == goal:
                if has_boundary:
                    return True
                continue
            state = (current, has_boundary)
            if state in seen:
                continue
            seen.add(state)
            route = route_refs.get(current)
            next_has_boundary = has_boundary or (
                route is not None and route.namespace.layer == Layer.Internal
            )
            for neighbor in adjacency.get(current, ()):
                pending.append((neighbor, next_has_boundary))
        return False

    def credit_snapshot(self) -> Iterator[CreditSnapshot]:
        """Expose the current credit/backpressure view for registered routes."""
        return iter(tuple(self._graph.credit_snapshot()))

    def flow_snapshot(self, route_ref: InspectableRoute) -> FlowSnapshot:
        """Return one enriched credit snapshot for a route."""
        if isinstance(route_ref, (NativeReadablePort, NativeWritablePort)):
            descriptor = route_ref.describe()
            route_display = descriptor.route_display
        else:
            native_route = self._coerce_route_ref(route_ref)
            route_display = native_route.display()
            descriptor = self.describe_route(native_route)
        for snapshot in self.credit_snapshot():
            if snapshot.route_display == route_display:
                return FlowSnapshot(
                    route_display=snapshot.route_display,
                    credit_class=snapshot.credit_class,
                    backpressure_policy=descriptor.backpressure_policy,
                    available=snapshot.available,
                    blocked_senders=snapshot.blocked_senders,
                    dropped_messages=snapshot.dropped_messages,
                    largest_queue_depth=getattr(snapshot, "largest_queue_depth", 0),
                )
        raise KeyError(f"no credit snapshot registered for {route_display}")

    def mailbox_snapshot(self, mailbox: NativeMailbox) -> MailboxSnapshot:
        """Return the current queue and overflow state for one mailbox."""
        mailbox_name = cast(str, self._mailbox_value(mailbox, "name"))
        descriptor = self._mailbox_descriptors.get(mailbox_name)
        if descriptor is None:
            raise KeyError(f"unknown mailbox {mailbox_name}")
        ingress = cast(Any, self._mailbox_value(mailbox, "ingress"))
        egress = cast(Any, self._mailbox_value(mailbox, "egress"))

        def descriptor_value(name: str, default: str | int) -> str | int:
            if not hasattr(descriptor, name):
                return default
            return cast(str | int, self._mailbox_value(descriptor, name))

        def mailbox_stat(name: str, default: int = 0) -> int:
            if not hasattr(mailbox, name):
                return default
            return cast(int, self._mailbox_value(mailbox, name))

        return MailboxSnapshot(
            name=mailbox_name,
            ingress_route=ingress.describe().route_display,
            egress_route=egress.describe().route_display,
            capacity=cast(int, descriptor_value("capacity", 128)),
            delivery_mode=cast(str, descriptor_value("delivery_mode", "mpsc_serial")),
            ordering_policy=cast(str, descriptor_value("ordering_policy", "fifo")),
            overflow_policy=cast(str, descriptor_value("overflow_policy", "block")),
            depth=mailbox_stat("depth"),
            available_credit=mailbox_stat("available_credit"),
            blocked_writes=mailbox_stat("blocked_writes"),
            dropped_messages=mailbox_stat("dropped_messages"),
            coalesced_messages=mailbox_stat("coalesced_messages"),
            delivered_messages=mailbox_stat("delivered_messages"),
        )

    def replay(self, route_ref: RouteLike) -> Iterator[ClosedEnvelope]:
        """Return the retained in-memory history for one route."""
        retention = self._retention_policy_for(route_ref)
        if retention.latest_replay_policy == "none":
            return iter(())
        history = tuple(self._history.get(self._route_key(route_ref), ()))
        if retention.latest_replay_policy == "latest_only":
            return iter(history[-1:] if history else ())
        return iter(history)

    def lineage(
        self,
        route_ref: RouteLike | None = None,
        *,
        trace_id: str | None = None,
        causality_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Iterator[LineageRecord]:
        """Return retained lineage records filtered by route or lineage ids."""
        if trace_id is not None:
            keys = tuple(self._lineage_events_by_trace.get(trace_id, ()))
        elif causality_id is not None:
            keys = tuple(self._lineage_events_by_causality.get(causality_id, ()))
        elif correlation_id is not None:
            keys = tuple(self._lineage_events_by_correlation.get(correlation_id, ()))
        elif route_ref is not None:
            keys = tuple(
                self._event_index_key(self._event_ref(envelope))
                for envelope in self.replay(route_ref)
            )
        else:
            keys = tuple(self._lineage_by_event)
        records = tuple(
            self._lineage_by_event[key]
            for key in keys
            if key in self._lineage_by_event
            and (
                route_ref is None
                or self._lineage_by_event[key].event.route_display
                == self._route_key(route_ref)
            )
        )
        return iter(records)

    def subscribers(self, route_ref: RouteLike) -> int:
        """Return the number of active observers on a route."""
        return self._subscriber_count.get(self._route_key(route_ref), 0)

    def route_audit(self, route_ref: RouteLike) -> RouteAuditSnapshot:
        """Summarize producers, subscribers, writes, and taint state for a route."""
        scope_routes = self._audit_scope_routes(route_ref)
        scope_keys = tuple(route.display() for route in scope_routes)
        producers: set[str] = set()
        active_subscribers: set[str] = set()
        taint_upper_bounds: set[str] = set()
        repair_notes: set[str] = set()
        recent_debug_events: list[str] = []

        for scope_route in scope_routes:
            route_display = scope_route.display()
            producers.update(self._writers.get(route_display, ()))
            active_subscribers.update(self._route_subscribers.get(route_display, ()))
            for item in self._taint_query_items(scope_route):
                if item.startswith("stream:"):
                    taint_upper_bounds.add(item)
                elif item.startswith("repair:"):
                    repair_notes.add(item.removeprefix("repair:"))
            recent_debug_events.extend(
                f"{event.event_type}:{event.detail}"
                for event in self.audit(scope_route)
            )

        binding = self._binding_for_route(route_ref)
        related_write_requests = ()
        if binding is not None:
            related_write_requests = tuple(
                f"{binding.request.display()}@{envelope.seq_source}"
                for envelope in self.replay(binding.request)
            )

        return RouteAuditSnapshot(
            route_display=self._route_key(route_ref),
            scope_routes=scope_keys,
            recent_producers=tuple(sorted(producers)),
            active_subscribers=tuple(sorted(active_subscribers)),
            related_write_requests=related_write_requests,
            taint_upper_bounds=tuple(sorted(taint_upper_bounds)),
            repair_notes=tuple(sorted(repair_notes)),
            recent_debug_events=tuple(recent_debug_events),
        )

    def writers(self, route_ref: RouteLike) -> Iterator[str]:
        """Return distinct producer ids that have written to a route."""
        return iter(tuple(sorted(self._writers.get(self._route_key(route_ref), ()))))

    def export_route(
        self, route_ref: RouteLike, *, visibility: str = "exported"
    ) -> None:
        """Mark a route as visible to third-party metadata readers."""
        self.register_port(route_ref)
        self._route_visibility[self._route_key(route_ref)] = visibility

    def grant_access(self, grant: CapabilityGrant) -> CapabilityGrant:
        """Register per-principal access control for one route."""
        route_ref = self._coerce_route_ref(grant.route)
        normalized = CapabilityGrant(
            principal_id=grant.principal_id,
            route=route_ref,
            metadata_read=grant.metadata_read,
            payload_open=grant.payload_open,
            write_request=grant.write_request,
            replay_read=grant.replay_read,
            debug_read=grant.debug_read,
            graph_validation=grant.graph_validation,
        )
        self._capability_grants[(normalized.principal_id, route_ref.display())] = (
            normalized
        )
        return normalized

    def shadow_state(
        self, binding_or_request: WriteBinding | LifecycleBinding | RouteLike
    ) -> ShadowSnapshot:
        """Return the current desired/reported/effective/ack view for one write binding."""
        binding = self._resolve_write_binding(binding_or_request)
        desired = self._graph.latest(binding.desired)
        reported = self._graph.latest(binding.reported)
        effective = self._graph.latest(binding.effective)
        ack = None if binding.ack is None else self._graph.latest(binding.ack)
        request = self._graph.latest(binding.request)
        coherence_taints = self._refresh_binding_coherence(binding)
        pending_write = "COHERENCE_WRITE_PENDING" in coherence_taints
        return ShadowSnapshot(
            request=request,
            desired=desired,
            reported=reported,
            effective=effective,
            ack=ack,
            pending_write=pending_write,
            coherence_taints=coherence_taints,
        )

    def reconcile_write_binding(
        self,
        binding_or_request: WriteBinding | LifecycleBinding | RouteLike,
        *,
        reported: bytes | None = None,
        effective: bytes | None = None,
        ack: bytes | None = None,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> ShadowSnapshot:
        """Publish observed shadow updates for a write binding and return the resulting snapshot."""
        binding = self._resolve_write_binding(binding_or_request)
        if reported is not None:
            self.publish(
                binding.reported,
                reported,
                producer=producer,
                control_epoch=control_epoch,
            )
        if effective is not None:
            self.publish(
                binding.effective,
                effective,
                producer=producer,
                control_epoch=control_epoch,
            )
        if ack is not None:
            if binding.ack is None:
                raise ValueError("write binding does not define an ack route")
            self.publish(
                binding.ack, ack, producer=producer, control_epoch=control_epoch
            )
        self._refresh_binding_coherence(binding)
        return self.shadow_state(binding)

    def publish_guarded(
        self,
        target: WriteTarget,
        payload: Any,
        *,
        not_before_epoch: int | None = None,
        wait_for_ack: RouteLike | None = None,
        expires_at_epoch: int | None = None,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
        retry_policy: RetryPolicy | None = None,
        ack_route: RouteLike | None = None,
        trace_id: str | None = None,
        causality_id: str | None = None,
        correlation_id: str | None = None,
        parent_events: Sequence[EventRef] = (),
    ) -> ScheduledWrite:
        """Queue a guarded write for later scheduler release."""
        if isinstance(target, LifecycleBinding):
            self._write_bindings[target.request.display()] = target.binding
            self._lifecycle_bindings[target.request.display()] = target
            self._graph.register_binding(target.request.display(), target.binding)
            if ack_route is None:
                ack_route = target.ack
            target = target.binding
        if isinstance(target, WriteBinding):
            self._write_bindings[target.request.display()] = target
            self._graph.register_binding(target.request.display(), target)
            if ack_route is None:
                ack_route = target.ack
        if retry_policy is not None and ack_route is None:
            raise ValueError(
                "retry_policy requires an ack_route or a write binding with an ack route"
        )
        ack_baseline_seq = -1
        if ack_route is not None:
            ack_closed = self._latest_closed(ack_route)
            ack_baseline_seq = -1 if ack_closed is None else ack_closed.seq_source
        scheduled = ScheduledWrite(
            target=target,
            payload=payload,
            not_before_epoch=not_before_epoch,
            wait_for_ack=wait_for_ack,
            expires_at_epoch=expires_at_epoch,
            producer=producer,
            control_epoch=control_epoch,
            retry_policy=retry_policy,
            ack_route=ack_route,
            ack_baseline_seq=ack_baseline_seq,
            trace_id=trace_id,
            causality_id=causality_id,
            correlation_id=correlation_id,
            parent_events=tuple(parent_events),
        )
        self._pending_writes.append(scheduled)
        self._emit_debug_event(
            "scheduler",
            f"queued guarded write for {self._route_key(target.request if isinstance(target, WriteBinding) else target)}",
            target.request
            if isinstance(target, WriteBinding)
            else self._coerce_route_ref(target),
        )
        return scheduled

    def run_scheduler(
        self, epoch: int | None = None
    ) -> tuple[TypedEnvelope[Any] | ClosedEnvelope, ...]:
        """Advance scheduler state and release any writes whose guards are satisfied."""
        self._scheduler_epoch = self._scheduler_epoch + 1 if epoch is None else epoch
        ready: list[ScheduledWrite] = []
        pending: list[ScheduledWrite] = []
        for scheduled in self._pending_writes:
            if (
                scheduled.expires_at_epoch is not None
                and self._scheduler_epoch > scheduled.expires_at_epoch
            ):
                self._emit_debug_event(
                    "scheduler",
                    f"expired guarded write for {self._route_key(scheduled.target.request if isinstance(scheduled.target, WriteBinding) else scheduled.target)}",
                    scheduled.target.request
                    if isinstance(scheduled.target, WriteBinding)
                    else self._coerce_route_ref(scheduled.target),
                )
                continue
            if scheduled.attempt_count > 0 and scheduled.ack_route is not None:
                if self._ack_observed_after(
                    scheduled.ack_route,
                    scheduled.ack_baseline_seq,
                ):
                    self._emit_debug_event(
                        "scheduler",
                        f"acknowledged guarded write for {self._route_key(scheduled.target.request if isinstance(scheduled.target, WriteBinding) else scheduled.target)} after {scheduled.attempt_count} attempts",
                        scheduled.target.request
                        if isinstance(scheduled.target, WriteBinding)
                        else self._coerce_route_ref(scheduled.target),
                    )
                    continue
                assert scheduled.retry_policy is not None
                if scheduled.attempt_count >= scheduled.retry_policy.max_attempts:
                    self._emit_debug_event(
                        "scheduler",
                        f"exhausted guarded write retries for {self._route_key(scheduled.target.request if isinstance(scheduled.target, WriteBinding) else scheduled.target)} after {scheduled.attempt_count} attempts",
                        scheduled.target.request
                        if isinstance(scheduled.target, WriteBinding)
                        else self._coerce_route_ref(scheduled.target),
                    )
                    continue
                next_attempt_epoch = (
                    self._scheduler_epoch
                    if scheduled.last_attempt_epoch is None
                    else scheduled.last_attempt_epoch
                    + scheduled.retry_policy.backoff_epochs
                    + 1
                )
                if self._scheduler_epoch >= next_attempt_epoch:
                    ready.append(scheduled)
                else:
                    pending.append(scheduled)
                continue
            ack_ready = True
            if scheduled.wait_for_ack is not None:
                ack_ready = self.latest(scheduled.wait_for_ack) is not None
            epoch_ready = (
                scheduled.not_before_epoch is None
                or self._scheduler_epoch >= scheduled.not_before_epoch
            )
            if ack_ready and epoch_ready:
                ready.append(scheduled)
            else:
                pending.append(scheduled)
        self._pending_writes = pending
        emitted: list[TypedEnvelope[Any] | ClosedEnvelope] = []
        for scheduled in ready:
            target_route = (
                scheduled.target.request
                if isinstance(scheduled.target, WriteBinding)
                else self._coerce_route_ref(scheduled.target)
            )
            emitted.append(
                self.publish(
                    scheduled.target,
                    scheduled.payload,
                    producer=scheduled.producer,
                    control_epoch=scheduled.control_epoch,
                    trace_id=scheduled.trace_id,
                    causality_id=scheduled.causality_id,
                    correlation_id=scheduled.correlation_id,
                    parent_events=scheduled.parent_events,
                )
            )
            if scheduled.retry_policy is not None:
                self._pending_writes.append(
                    replace(
                        scheduled,
                        attempt_count=scheduled.attempt_count + 1,
                        last_attempt_epoch=self._scheduler_epoch,
                    )
                )
                if scheduled.attempt_count > 0:
                    self._emit_debug_event(
                        "scheduler",
                        f"retried guarded write for {target_route.display()} at epoch {self._scheduler_epoch}",
                        target_route,
                    )
        if emitted:
            self._emit_debug_event(
                "scheduler",
                f"released {len(emitted)} guarded writes at epoch {self._scheduler_epoch}",
            )
        return tuple(emitted)

    def stateful_map(
        self,
        source: TypedRoute[TIn] | RouteRef,
        *,
        initial_state: StateT,
        step: Callable[[StateT, TIn], tuple[StateT, TOut]],
        output: TypedRoute[TOut],
    ) -> SubscriptionLike:
        """Apply a stateful step function and publish each emitted value to `output`."""
        state = initial_state
        source_route = self._coerce_route_ref(source)

        def on_next(item: TypedEnvelope[TIn] | ClosedEnvelope) -> None:
            nonlocal state
            value = self._operator_value(source, source_route, item)
            state, out = step(state, value)
            lineage = self._lineage_for_item(item)
            self._extend_taints(
                self.publish(
                    output,
                    out,
                    trace_id=lineage.trace_id,
                    causality_id=lineage.causality_id,
                    correlation_id=lineage.correlation_id,
                    parent_events=(lineage.event,),
                ),
                self._item_taints(item),
            )

        return self._observe_observable(source).subscribe(on_next)  # type: ignore[arg-type]

    def filter(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        predicate: Callable[[T | bytes], bool],
    ) -> Observable[T | bytes]:
        """Emit only the source values that satisfy `predicate`.

        This keeps filtering explicit in the graph-facing API instead of forcing
        callers to drop into raw Rx operators for a core RFC composition
        primitive. Typed routes deliver decoded values to the predicate, while
        raw route refs continue to expose payload bytes.
        """
        source_route = self._coerce_route_ref(source)

        def subscribe(
            observer: ObserverLike[T | bytes],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            def on_next(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
                value = self._operator_value(source, source_route, item)
                if predicate(value):
                    observer.on_next(value)

            self._replay_latest_value(source, on_next)
            return self._observe_observable(source, replay_latest=False).subscribe(
                on_next, scheduler=scheduler
            )  # type: ignore[arg-type]

        return rx.create(subscribe)

    def window(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        size: int,
        trigger: TypedRoute[Any] | RouteRef | None = None,
        partition_by: Callable[[T | bytes], Hashable] | None = None,
    ) -> Observable[list[T] | list[bytes]]:
        """Emit the most recent `size` values on source updates or explicit triggers.

        When ``partition_by`` is provided, each partition keeps its own bounded
        buffer and source updates emit the window for that partition only.
        Trigger routes flush every currently buffered partition in insertion
        order.
        """
        if size <= 0:
            raise ValueError("window size must be positive")
        source_route = self._coerce_route_ref(source)
        trigger_route = None if trigger is None else self._coerce_route_ref(trigger)

        def subscribe(
            observer: ObserverLike[list[T] | list[bytes]],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            buffers: dict[Hashable | None, deque[T | bytes]] = {}

            def buffer_for(value: T | bytes) -> deque[T | bytes]:
                partition_key = None if partition_by is None else partition_by(value)
                return buffers.setdefault(partition_key, deque(maxlen=size))

            def on_next(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
                value = self._operator_value(source, source_route, item)
                buffer = buffer_for(value)
                buffer.append(value)
                if trigger_route is None:
                    observer.on_next(list(buffer))

            def emit_window(_item: TypedEnvelope[Any] | ClosedEnvelope) -> None:
                for buffer in buffers.values():
                    if buffer:
                        observer.on_next(list(buffer))

            self._replay_latest_value(source, on_next)
            if trigger is not None:
                self._replay_latest_value(trigger, emit_window)

            source_sub = self._observe_observable(source, replay_latest=False).subscribe(
                on_next, scheduler=scheduler
            )  # type: ignore[arg-type]
            if trigger is None:
                return source_sub
            trigger_sub = self._observe_observable(trigger, replay_latest=False).subscribe(
                emit_window, scheduler=scheduler
            )  # type: ignore[arg-type]

            class _Subscription:
                def dispose(self) -> None:
                    source_sub.dispose()
                    trigger_sub.dispose()

            return _Subscription()

        return rx.create(subscribe)

    def window_aggregate(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        size: int,
        aggregate: Callable[[TypingSequence[T] | TypingSequence[bytes]], TOut],
        trigger: TypedRoute[Any] | RouteRef | None = None,
        partition_by: Callable[[T | bytes], Hashable] | None = None,
    ) -> Observable[TOut]:
        """Aggregate each rolling window emitted from `source`.

        This keeps windowed aggregation explicit in the graph-facing API while
        still reusing the same bounded sliding-window semantics as `window()`.
        Each subscription owns its own buffer and receives an aggregate for the
        replayed latest value (if any) plus each future update, or only when an
        explicit trigger route advances.
        """

        def subscribe(
            observer: ObserverLike[TOut],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            def on_window(items: list[T] | list[bytes]) -> None:
                observer.on_next(aggregate(tuple(items)))

            windowed = self.window(
                source,
                size=size,
                trigger=trigger,
                partition_by=partition_by,
            )
            return windowed.subscribe(
                on_window,
                scheduler=scheduler,
            )

        return rx.create(subscribe)

    def window_by_time(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        width: int,
        watermark: TypedRoute[Any] | RouteRef | None = None,
        grace: int = 0,
        partition_by: Callable[[T | bytes], Hashable] | None = None,
        event_time: Callable[[T | bytes], int] | None = None,
        watermark_time: Callable[[Any | bytes], int] | None = None,
    ) -> Observable[list[T] | list[bytes]]:
        """Emit a watermark-aware rolling window over event time.

        Event timestamps default to `control_epoch` when present and fall back
        to `seq_source` otherwise. When `watermark` is omitted, source events
        advance watermark progress implicitly. When `watermark` is present, the
        source buffers values and emits only as watermarks advance.
        """
        if width <= 0:
            raise ValueError("window width must be positive")
        if grace < 0:
            raise ValueError("window grace must be non-negative")
        source_route = self._coerce_route_ref(source)
        watermark_route = None if watermark is None else self._coerce_route_ref(watermark)

        def subscribe(
            observer: ObserverLike[list[T] | list[bytes]],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            buffers: dict[Hashable | None, deque[tuple[int, T | bytes]]] = {}
            watermarks: dict[Hashable | None, int] = {}

            def partition_key_for(value: T | bytes) -> Hashable | None:
                if partition_by is None:
                    return None
                return partition_by(value)

            def buffer_for(
                partition_key: Hashable | None,
            ) -> deque[tuple[int, T | bytes]]:
                return buffers.setdefault(partition_key, deque())

            def prune(partition_key: Hashable | None) -> None:
                current_watermark = watermarks.get(partition_key)
                if current_watermark is None:
                    return
                minimum_kept = current_watermark - width - grace + 1
                buffer = buffer_for(partition_key)
                while buffer and buffer[0][0] < minimum_kept:
                    buffer.popleft()

            def emit_window(partition_key: Hashable | None) -> None:
                current_watermark = watermarks.get(partition_key)
                if current_watermark is None:
                    return
                buffer = buffers.get(partition_key)
                if not buffer:
                    return
                window_start = current_watermark - width + 1
                items = [
                    value
                    for event_time_value, value in sorted(buffer, key=lambda item: item[0])
                    if window_start <= event_time_value <= current_watermark
                ]
                if items:
                    observer.on_next(items)

            def on_next(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
                value = self._operator_value(source, source_route, item)
                partition_key = partition_key_for(value)
                progress = self._progress_value(
                    source,
                    source_route,
                    item,
                    extractor=event_time,
                )
                current_watermark = watermarks.get(partition_key)
                if current_watermark is not None:
                    minimum_kept = current_watermark - width - grace + 1
                    if progress < minimum_kept:
                        return
                buffer_for(partition_key).append((progress, value))
                if watermark_route is None:
                    watermarks[partition_key] = progress
                    prune(partition_key)
                    emit_window(partition_key)

            def on_watermark(item: TypedEnvelope[Any] | ClosedEnvelope) -> None:
                assert watermark is not None
                progress = self._progress_value(
                    watermark,
                    self._coerce_route_ref(watermark),
                    item,
                    extractor=watermark_time,
                )
                for partition_key in tuple(buffers):
                    current_watermark = watermarks.get(partition_key)
                    if current_watermark is None or progress > current_watermark:
                        watermarks[partition_key] = progress
                    prune(partition_key)
                    emit_window(partition_key)

            self._replay_latest_value(source, on_next)
            if watermark is not None:
                self._replay_latest_value(watermark, on_watermark)

            source_sub = self._observe_observable(source, replay_latest=False).subscribe(
                on_next, scheduler=scheduler
            )  # type: ignore[arg-type]
            if watermark is None:
                return source_sub
            watermark_sub = self._observe_observable(watermark, replay_latest=False).subscribe(
                on_watermark, scheduler=scheduler
            )  # type: ignore[arg-type]

            class _Subscription:
                def dispose(self) -> None:
                    source_sub.dispose()
                    watermark_sub.dispose()

            return _Subscription()

        return rx.create(subscribe)

    def window_aggregate_by_time(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        width: int,
        aggregate: Callable[[TypingSequence[T] | TypingSequence[bytes]], TOut],
        watermark: TypedRoute[Any] | RouteRef | None = None,
        grace: int = 0,
        partition_by: Callable[[T | bytes], Hashable] | None = None,
        event_time: Callable[[T | bytes], int] | None = None,
        watermark_time: Callable[[Any | bytes], int] | None = None,
    ) -> Observable[TOut]:
        """Aggregate watermark-aware event-time windows from `source`."""

        def subscribe(
            observer: ObserverLike[TOut],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            def on_window(items: list[T] | list[bytes]) -> None:
                observer.on_next(aggregate(tuple(items)))

            windowed = self.window_by_time(
                source,
                width=width,
                watermark=watermark,
                grace=grace,
                partition_by=partition_by,
                event_time=event_time,
                watermark_time=watermark_time,
            )
            return windowed.subscribe(on_window, scheduler=scheduler)

        return rx.create(subscribe)

    def join_latest(
        self,
        left: TypedRoute[TIn] | RouteRef,
        right: TypedRoute[TRight] | RouteRef,
        *,
        combine: Callable[[TIn, TRight], TOut],
    ) -> Observable[TOut]:
        """Combine each incoming side with the latest value from the other side."""
        left_route = self._coerce_route_ref(left)
        right_route = self._coerce_route_ref(right)

        def subscribe(
            observer: ObserverLike[TOut],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            left_latest: TIn | None = None
            right_latest: TRight | None = None

            def on_left(item: TypedEnvelope[TIn] | ClosedEnvelope) -> None:
                nonlocal left_latest
                left_latest = self._operator_value(left, left_route, item)
                if right_latest is not None:
                    observer.on_next(combine(left_latest, right_latest))

            def on_right(item: TypedEnvelope[TRight] | ClosedEnvelope) -> None:
                nonlocal right_latest
                right_latest = self._operator_value(right, right_route, item)
                if left_latest is not None:
                    observer.on_next(combine(left_latest, right_latest))

            self._replay_latest_value(left, on_left)
            self._replay_latest_value(right, on_right)

            left_sub = self._observe_observable(left, replay_latest=False).subscribe(
                on_left, scheduler=scheduler
            )  # type: ignore[arg-type]
            right_sub = self._observe_observable(right, replay_latest=False).subscribe(
                on_right, scheduler=scheduler
            )  # type: ignore[arg-type]

            class _Subscription:
                def dispose(self) -> None:
                    left_sub.dispose()
                    right_sub.dispose()

            return _Subscription()

        return rx.create(subscribe)

    def lookup_join(
        self,
        left: TypedRoute[TIn] | RouteRef,
        right_state: TypedRoute[TRight] | RouteRef,
        *,
        combine: Callable[[TIn | bytes, TRight | bytes], TOut],
    ) -> Observable[TOut]:
        """Join each left-side event against the latest right-hand state view.

        Unlike :meth:`join_latest`, updates on ``right_state`` only refresh the
        materialized lookup value; they do not emit output until a left-side
        event arrives. This matches the RFC's stream-table lookup shape.
        """
        left_route = self._coerce_route_ref(left)
        right_route = self._coerce_route_ref(right_state)

        def subscribe(
            observer: ObserverLike[TOut],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            right_latest: TRight | bytes | None = None

            def on_right(item: TypedEnvelope[TRight] | ClosedEnvelope) -> None:
                nonlocal right_latest
                right_latest = self._operator_value(right_state, right_route, item)

            def on_left(item: TypedEnvelope[TIn] | ClosedEnvelope) -> None:
                if right_latest is None:
                    return
                left_value = self._operator_value(left, left_route, item)
                observer.on_next(combine(left_value, right_latest))

            self._replay_latest_value(right_state, on_right)

            left_sub = self._observe_observable(left, replay_latest=False).subscribe(
                on_left, scheduler=scheduler
            )  # type: ignore[arg-type]
            right_sub = self._observe_observable(right_state, replay_latest=False).subscribe(
                on_right, scheduler=scheduler
            )  # type: ignore[arg-type]

            class _Subscription:
                def dispose(self) -> None:
                    left_sub.dispose()
                    right_sub.dispose()

            return _Subscription()

        return rx.create(subscribe)

    def interval_join(
        self,
        left: TypedRoute[TIn] | RouteRef,
        right: TypedRoute[TRight] | RouteRef,
        *,
        within: int,
        combine: Callable[[TIn | bytes, TRight | bytes], TOut],
    ) -> Observable[TOut]:
        """Join nearby events from both sides within a bounded sequence interval.

        The current scaffold does not model full event timestamps yet, so this
        operator uses `seq_source` as a stable event-order surrogate. It keeps
        a recent buffer on each side and emits joined values whenever the
        opposite side has events within the requested distance.
        """
        if within < 0:
            raise ValueError("interval join distance must be non-negative")
        left_route = self._coerce_route_ref(left)
        right_route = self._coerce_route_ref(right)

        def prune(buffer: deque[tuple[int, Any]], seq: int) -> None:
            minimum = seq - within
            while buffer and buffer[0][0] < minimum:
                buffer.popleft()

        def subscribe(
            observer: ObserverLike[TOut],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            left_buffer: deque[tuple[int, TIn | bytes]] = deque()
            right_buffer: deque[tuple[int, TRight | bytes]] = deque()

            def on_left(item: TypedEnvelope[TIn] | ClosedEnvelope) -> None:
                closed = item.closed if isinstance(item, TypedEnvelope) else item
                seq = closed.seq_source
                value = self._operator_value(left, left_route, item)
                left_buffer.append((seq, value))
                prune(left_buffer, seq)
                prune(right_buffer, seq)
                for right_seq, right_value in tuple(right_buffer):
                    if abs(seq - right_seq) <= within:
                        observer.on_next(combine(value, right_value))

            def on_right(item: TypedEnvelope[TRight] | ClosedEnvelope) -> None:
                closed = item.closed if isinstance(item, TypedEnvelope) else item
                seq = closed.seq_source
                value = self._operator_value(right, right_route, item)
                right_buffer.append((seq, value))
                prune(right_buffer, seq)
                prune(left_buffer, seq)
                for left_seq, left_value in tuple(left_buffer):
                    if abs(seq - left_seq) <= within:
                        observer.on_next(combine(left_value, value))

            left_sub = self._observe_observable(left, replay_latest=False).subscribe(
                on_left, scheduler=scheduler
            )  # type: ignore[arg-type]
            right_sub = self._observe_observable(right, replay_latest=False).subscribe(
                on_right, scheduler=scheduler
            )  # type: ignore[arg-type]

            class _Subscription:
                def dispose(self) -> None:
                    left_sub.dispose()
                    right_sub.dispose()

            return _Subscription()

        return rx.create(subscribe)

    def materialize(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        state_route: TypedRoute[T] | RouteRef,
    ) -> SubscriptionLike:
        """Mirror source updates into a state route owned by the topology."""
        source_route = self._coerce_route_ref(source)

        def on_next(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
            if isinstance(state_route, TypedRoute):
                if isinstance(source, TypedRoute):
                    value = cast(T, self._operator_value(source, source_route, item))
                else:
                    value = state_route.schema.decode(
                        self._resolve_payload_bytes(
                            source_route, item, record_open=False
                        )
                    )
                lineage = self._lineage_for_item(item)
                self._extend_taints(
                    self.publish(
                        state_route,
                        value,
                        trace_id=lineage.trace_id,
                        causality_id=lineage.causality_id,
                        correlation_id=lineage.correlation_id,
                        parent_events=(lineage.event,),
                    ),
                    self._item_taints(item),
                )
            else:
                payload = (
                    source.schema.encode(item.value)
                    if isinstance(source, TypedRoute)
                    and isinstance(item, TypedEnvelope)
                    else self._resolve_payload_bytes(
                        source_route, item, record_open=False
                    )
                )
                lineage = self._lineage_for_item(item)
                self._extend_taints(
                    self.publish(
                        state_route,
                        payload,
                        trace_id=lineage.trace_id,
                        causality_id=lineage.causality_id,
                        correlation_id=lineage.correlation_id,
                        parent_events=(lineage.event,),
                    ),
                    self._item_taints(item),
                )

        return self._observe_observable(source).subscribe(on_next)  # type: ignore[arg-type]

    def repair_taints(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        output: TypedRoute[TOut] | RouteRef,
        repair: TaintRepair,
        transform: Callable[[T | bytes], TOut | bytes] | None = None,
    ) -> SubscriptionLike:
        """Publish repaired downstream events with explicit taint-clearing rules."""
        source_route = self._coerce_route_ref(source)

        def on_next(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
            value = self._operator_value(source, source_route, item)
            emitted_value = value if transform is None else transform(value)
            lineage = self._lineage_for_item(item)
            if isinstance(output, TypedRoute):
                emitted = self.publish(
                    output,
                    cast(TOut, emitted_value),
                    trace_id=lineage.trace_id,
                    causality_id=lineage.causality_id,
                    correlation_id=lineage.correlation_id,
                    parent_events=(lineage.event,),
                )
            else:
                payload = (
                    cast(bytes, emitted_value)
                    if isinstance(emitted_value, (bytes, bytearray))
                    else source.schema.encode(cast(T, emitted_value))
                    if isinstance(source, TypedRoute)
                    else self._resolve_payload_bytes(
                        source_route, item, record_open=False
                    )
                )
                emitted = self.publish(
                    output,
                    payload,
                    trace_id=lineage.trace_id,
                    causality_id=lineage.causality_id,
                    correlation_id=lineage.correlation_id,
                    parent_events=(lineage.event,),
                )
            self._apply_taint_repair(emitted, self._item_taints(item), repair=repair)
            key = self._route_key(output)
            note = (
                f"{self._taint_domain_name(repair.domain)}:"
                f"{repair.proof}"
            )
            notes = self._route_repair_notes.setdefault(key, [])
            if note not in notes:
                notes.append(note)
            self._emit_debug_event(
                "repair",
                f"applied taint repair on {self._route_key(output)} with proof {repair.proof}",
                output,
            )

        return self._observe_observable(source).subscribe(on_next)  # type: ignore[arg-type]

    def plan_join(self, name: str, left: JoinInput, right: JoinInput) -> JoinPlan:
        """Plan a join and reject illegal cross-partition cases.

        The method makes repartition boundaries explicit by registering visible
        internal routes that also appear in topology and debug output.
        """
        left_route = self.register_port(left.route)
        right_route = self.register_port(right.route)
        if left.clock_domain != right.clock_domain:
            raise ValueError("join clock domains must match or be explicitly aligned")
        if left.state_retention == "unbounded" and right.state_retention == "unbounded":
            raise ValueError("unbounded cross-product joins are illegal")
        if left.partition_key_semantics == right.partition_key_semantics:
            if (
                left.ordering_guarantee == right.ordering_guarantee
                and left.watermark_semantics == right.watermark_semantics
            ):
                plan = JoinPlan(
                    name=name,
                    join_class="local_keyed",
                    left=left_route,
                    right=right_route,
                    state_budget="bounded",
                )
            elif left.deterministic_rekey and right.deterministic_rekey:
                left_repartition = self.register_port(
                    self._make_internal_route(
                        plane=Plane.State,
                        layer=Layer.Internal,
                        owner=name,
                        family="join",
                        stream="left_repartition",
                        variant=Variant.State,
                        schema_id="RepartitionState",
                    )
                )
                right_repartition = self.register_port(
                    self._make_internal_route(
                        plane=Plane.State,
                        layer=Layer.Internal,
                        owner=name,
                        family="join",
                        stream="right_repartition",
                        variant=Variant.State,
                        schema_id="RepartitionState",
                    )
                )
                self.connect(source=left_route, sink=left_repartition)
                self.connect(source=right_route, sink=right_repartition)
                plan = JoinPlan(
                    name=name,
                    join_class="repartition",
                    left=left_route,
                    right=right_route,
                    visible_nodes=(left_repartition, right_repartition),
                    state_budget="spill_or_backpressure",
                    taint_implications=("deterministic_rekey",),
                )
            else:
                raise ValueError("join ordering/watermark guarantees are incompatible")
        elif right.materialized_view:
            plan = JoinPlan(
                name=name,
                join_class="lookup",
                left=left_route,
                right=right_route,
                state_budget="right_materialized_view",
                taint_implications=("snapshot_consistency_required",),
            )
        elif left.broadcast_mirror_eligible or right.broadcast_mirror_eligible:
            plan = JoinPlan(
                name=name,
                join_class="broadcast_mirror",
                left=left_route,
                right=right_route,
                state_budget="mirror_memory",
                taint_implications=("order_insensitive_broadcast",),
            )
        elif left.deterministic_rekey and right.deterministic_rekey:
            left_repartition = self.register_port(
                self._make_internal_route(
                    plane=Plane.State,
                    layer=Layer.Internal,
                    owner=name,
                    family="join",
                    stream="left_repartition",
                    variant=Variant.State,
                    schema_id="RepartitionState",
                )
            )
            right_repartition = self.register_port(
                self._make_internal_route(
                    plane=Plane.State,
                    layer=Layer.Internal,
                    owner=name,
                    family="join",
                    stream="right_repartition",
                    variant=Variant.State,
                    schema_id="RepartitionState",
                )
            )
            self.connect(source=left_route, sink=left_repartition)
            self.connect(source=right_route, sink=right_repartition)
            plan = JoinPlan(
                name=name,
                join_class="repartition",
                left=left_route,
                right=right_route,
                visible_nodes=(left_repartition, right_repartition),
                state_budget="spill_or_backpressure",
                taint_implications=("deterministic_rekey",),
            )
        else:
            raise ValueError(
                "join partition keys are incompatible without repartition or broadcast"
            )
        self._join_plans[name] = plan
        self._emit_debug_event(
            "join", f"planned {plan.join_class} join {name}", left_route
        )
        return plan

    def explain_join(self, name: str) -> JoinPlan:
        """Return a previously planned join."""
        return self._join_plans[name]

    def add_middleware(self, middleware: Middleware) -> Middleware:
        """Register middleware after enforcing RFC preservation rules."""
        if not middleware.preserves_envelope_identity:
            raise ValueError(
                "middleware must preserve envelope identity unless explicitly reframing"
            )
        if not middleware.updates_taints:
            raise ValueError("middleware must preserve or update taints")
        if not middleware.updates_causality:
            raise ValueError("middleware must preserve or update causality")
        self._middlewares.append(middleware)
        self._emit_debug_event(
            "middleware", f"attached {middleware.kind} middleware {middleware.name}"
        )
        return middleware

    def middleware(self) -> Iterator[Middleware]:
        """Iterate over registered middleware."""
        return iter(tuple(self._middlewares))

    def register_link(self, link: Link) -> Link:
        """Register a transport/link adapter."""
        self._links[link.name] = link
        self._emit_debug_event(
            "link_health", f"registered {link.link_class} link {link.name}"
        )
        return link

    def links(self) -> Iterator[Link]:
        """Iterate over registered links."""
        return iter(tuple(self._links.values()))

    def add_mesh_primitive(self, primitive: MeshPrimitive) -> MeshPrimitive:
        """Register an explicit mesh primitive in topology metadata."""
        if (
            primitive.kind in {"bridge", "mirror", "replicate"}
            and primitive.link_name is None
        ):
            raise ValueError(f"{primitive.kind} requires a link")
        if primitive.link_name is not None and primitive.link_name not in self._links:
            raise KeyError(f"unknown link {primitive.link_name}")
        normalized = MeshPrimitive(
            name=primitive.name,
            kind=primitive.kind,
            sources=tuple(self._coerce_route_ref(route) for route in primitive.sources),
            destinations=tuple(
                self._coerce_route_ref(route) for route in primitive.destinations
            ),
            link_name=primitive.link_name,
            ordering_policy=primitive.ordering_policy,
            state_budget=primitive.state_budget,
            threshold=primitive.threshold,
            ack_policy=primitive.ack_policy,
        )
        for mesh_source in normalized.sources:
            self.register_port(mesh_source)
        for destination in normalized.destinations:
            self.register_port(destination)
        for mesh_source in normalized.sources:
            for destination in normalized.destinations:
                self.connect(source=mesh_source, sink=destination)
        self._mesh_primitives[normalized.name] = normalized
        self._emit_debug_event(
            "topology", f"registered mesh primitive {normalized.kind}:{normalized.name}"
        )
        return normalized

    def mesh_primitives(self) -> Iterator[MeshPrimitive]:
        """Iterate over registered mesh primitives."""
        return iter(tuple(self._mesh_primitives.values()))

    def query_service(self, owner: str = "query") -> QueryServiceRoutes:
        """Return or lazily create the query request/response routes."""
        if owner not in self._query_services:
            request = self.register_port(
                self._make_internal_route(
                    plane=Plane.Query,
                    layer=Layer.Internal,
                    owner=owner,
                    family="query",
                    stream="request",
                    variant=Variant.QueryRequest,
                    schema_id="QueryRequest",
                )
            )
            response = self.register_port(
                self._make_internal_route(
                    plane=Plane.Query,
                    layer=Layer.Internal,
                    owner=owner,
                    family="query",
                    stream="response",
                    variant=Variant.QueryResponse,
                    schema_id="QueryResponse",
                )
            )
            self._query_services[owner] = QueryServiceRoutes(
                request=request, response=response
            )
        return self._query_services[owner]

    def query(
        self,
        request: QueryRequest,
        *,
        requester_id: str = "python",
        service_owner: str = "query",
    ) -> QueryResponse:
        """Execute a typed query through the query-plane stream model."""
        service = self.query_service(service_owner)
        correlation_id = request.correlation_id or self._correlation_id()
        request_payload = json.dumps(
            {
                "command": request.command,
                "route": None
                if request.route is None
                else self._route_key(request.route),
                "join_name": request.join_name,
                "principal_id": request.principal_id or requester_id,
                "correlation_id": correlation_id,
                "lineage_trace_id": request.lineage_trace_id,
                "lineage_causality_id": request.lineage_causality_id,
                "lineage_correlation_id": request.lineage_correlation_id,
            },
            sort_keys=True,
        ).encode()
        request_envelope = self._graph.writable_port(service.request).write(
            request_payload
        )
        self._record_envelope(
            service.request, request_envelope, producer_id=requester_id
        )
        items = self._execute_query(
            QueryRequest(
                command=request.command,
                route=request.route,
                join_name=request.join_name,
                principal_id=request.principal_id or requester_id,
                correlation_id=correlation_id,
                lineage_trace_id=request.lineage_trace_id,
                lineage_causality_id=request.lineage_causality_id,
                lineage_correlation_id=request.lineage_correlation_id,
            )
        )
        response = QueryResponse(
            command=request.command, correlation_id=correlation_id, items=items
        )
        response_payload = json.dumps(
            {
                "command": response.command,
                "correlation_id": response.correlation_id,
                "items": list(response.items),
            },
            sort_keys=True,
        ).encode()
        response_envelope = self._graph.writable_port(service.response).write(
            response_payload
        )
        self._record_envelope(
            service.response, response_envelope, producer_id="query_service"
        )
        self._emit_debug_event(
            "audit", f"query {request.command} handled", service.response
        )
        return response

    def debug_routes(self) -> Iterator[RouteRef]:
        """Return the well-known debug routes created so far."""
        return iter(tuple(self._debug_routes.values()))

    def audit(self, route_ref: RouteLike | None = None) -> Iterator[DebugEvent]:
        """Return retained audit/debug events, optionally filtered by route."""
        route_display = None if route_ref is None else self._route_key(route_ref)
        return iter(
            tuple(
                event
                for event in self._audit_events
                if route_display is None or event.route_display == route_display
            )
        )
