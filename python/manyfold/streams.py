"""ManyFold-owned in-process observable primitives.

This module implements the small process-local stream surface ManyFold uses
internally without depending on RxPy. PubSub remains the shared application
boundary; these observables are callback views for graph internals and tests.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import ModuleType
from typing import Any, Generic, TypeVar

T = TypeVar("T")
U = TypeVar("U")


def create(subscribe: Callable[[Observer[Any], object | None], Any]) -> Observable[Any]:
    return Observable(subscribe)


def empty() -> Observable[Any]:
    return from_iterable(())


def from_iterable(values: Iterable[T]) -> Observable[T]:
    snapshot = tuple(values)

    def subscribe(
        observer: Observer[T], _scheduler: object | None = None
    ) -> Disposable:
        for value in snapshot:
            observer.on_next(value)
        observer.on_completed()
        return Disposable()

    return Observable(subscribe)


def interval(
    period: timedelta, scheduler: SchedulerBase | None = None
) -> Observable[int]:
    resolved_scheduler = scheduler or TimeoutScheduler()
    period_s = _seconds(period)

    def subscribe(
        observer: Observer[int], _scheduler: object | None = None
    ) -> Disposable:
        cancelled = threading.Event()
        counter = 0

        def emit(_scheduler_arg: Any = None, _state: Any = None) -> None:
            nonlocal counter
            if cancelled.is_set():
                return
            observer.on_next(counter)
            counter += 1
            resolved_scheduler.schedule_relative(period_s, emit)

        first = resolved_scheduler.schedule_relative(period_s, emit)
        return Disposable(lambda: (cancelled.set(), first.dispose()))

    return Observable(subscribe)


def pipe(source: Observable[Any], *operators: Callable[[Observable[Any]], Any]) -> Any:
    return source.pipe(*operators)


def map(
    transform: Callable[[Any], Any],
) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            return source.subscribe(
                lambda value: observer.on_next(transform(value)),
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler,
            )

        return Observable(subscribe)

    return operator


def filter(
    predicate: Callable[[Any], bool],
) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            return source.subscribe(
                lambda value: observer.on_next(value) if predicate(value) else None,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler,
            )

        return Observable(subscribe)

    return operator


def publish() -> Callable[[Observable[Any]], ConnectableObservable[Any]]:
    def operator(source: Observable[Any]) -> ConnectableObservable[Any]:
        return ConnectableObservable(source)

    return operator


def start_with(*values: Any) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            for value in values:
                observer.on_next(value)
            return source.subscribe(
                observer.on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler,
            )

        return Observable(subscribe)

    return operator


def scan(
    accumulator: Callable[[Any, Any], Any], *, seed: Any = None
) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            state = seed

            def receive(value: Any) -> None:
                nonlocal state
                state = accumulator(state, value)
                observer.on_next(state)

            return source.subscribe(
                receive,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler,
            )

        return Observable(subscribe)

    return operator


def distinct_until_changed(
    key_mapper: Callable[[Any], Any] | None = None,
    comparer: Callable[[Any, Any], bool] | None = None,
) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            sentinel = object()
            previous = sentinel

            def receive(value: Any) -> None:
                nonlocal previous
                key = key_mapper(value) if key_mapper is not None else value
                equal = (
                    comparer(previous, key)
                    if comparer and previous is not sentinel
                    else previous == key
                )
                if previous is sentinel or not equal:
                    previous = key
                    observer.on_next(value)

            return source.subscribe(
                receive,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler,
            )

        return Observable(subscribe)

    return operator


def do_action(
    on_next: Callable[[Any], object] | None = None,
    on_error: Callable[[Exception], object] | None = None,
    on_completed: Callable[[], object] | None = None,
) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            def receive(value: Any) -> None:
                if on_next is not None:
                    on_next(value)
                observer.on_next(value)

            def error(exc: Exception) -> None:
                if on_error is not None:
                    on_error(exc)
                observer.on_error(exc)

            def completed() -> None:
                if on_completed is not None:
                    on_completed()
                observer.on_completed()

            return source.subscribe(receive, error, completed, scheduler=scheduler)

        return Observable(subscribe)

    return operator


def take(count: int) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            seen = 0
            subscription: Any | None = None

            def receive(value: Any) -> None:
                nonlocal seen
                if seen >= count:
                    return
                seen += 1
                observer.on_next(value)
                if seen >= count:
                    observer.on_completed()
                    if subscription is not None:
                        subscription.dispose()

            subscription = source.subscribe(
                receive,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler,
            )
            return subscription

        return Observable(subscribe)

    return operator


def take_until(other: Observable[Any]) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            stopped = False

            def stop(_value: Any = None) -> None:
                nonlocal stopped
                stopped = True
                observer.on_completed()

            other_subscription = other.subscribe(stop, observer.on_error)

            def receive(value: Any) -> None:
                if not stopped:
                    observer.on_next(value)

            source_subscription = source.subscribe(
                receive,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler,
            )
            return CompositeDisposable(source_subscription, other_subscription)

        return Observable(subscribe)

    return operator


def switch_latest() -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(observer: Observer[Any], scheduler: object | None = None) -> Any:
            active = SerialDisposable()

            def receive(inner: Observable[Any]) -> None:
                active.disposable = inner.subscribe(
                    observer.on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler=scheduler,
                )

            outer = source.subscribe(receive, observer.on_error, scheduler=scheduler)
            return CompositeDisposable(outer, active)

        return Observable(subscribe)

    return operator


def observe_on(
    scheduler: SchedulerBase,
) -> Callable[[Observable[Any]], Observable[Any]]:
    def operator(source: Observable[Any]) -> Observable[Any]:
        def subscribe(
            observer: Observer[Any], scheduler_arg: object | None = None
        ) -> Any:
            del scheduler_arg

            def schedule(callback: Callable[[], object]) -> None:
                scheduler.schedule(lambda _scheduler, _state: callback())

            return source.subscribe(
                lambda value: schedule(lambda: observer.on_next(value)),
                lambda error: schedule(lambda: observer.on_error(error)),
                lambda: schedule(observer.on_completed),
            )

        return Observable(subscribe)

    return operator


class Disposable:
    def __init__(self, action: Callable[[], object] | None = None) -> None:
        self._action = action
        self._disposed = False
        self._lock = threading.Lock()

    def dispose(self) -> None:
        with self._lock:
            if self._disposed:
                return
            self._disposed = True
            action = self._action
            self._action = None
        if action is not None:
            action()


class SerialDisposable(Disposable):
    def __init__(self) -> None:
        super().__init__()
        self._current: Any | None = None

    @property
    def disposable(self) -> Any | None:
        return self._current

    @disposable.setter
    def disposable(self, value: Any | None) -> None:
        old = self._current
        self._current = value
        if old is not None:
            old.dispose()

    def dispose(self) -> None:
        current = self._current
        self._current = None
        if current is not None:
            current.dispose()
        super().dispose()


class CompositeDisposable(Disposable):
    def __init__(self, *subscriptions: Any) -> None:
        super().__init__()
        self._subscriptions = list(subscriptions)

    def add(self, subscription: Any) -> None:
        self._subscriptions.append(subscription)

    def dispose(self) -> None:
        subscriptions = tuple(self._subscriptions)
        self._subscriptions.clear()
        for subscription in subscriptions:
            subscription.dispose()
        super().dispose()


class Observer(Generic[T]):
    def __init__(
        self,
        on_next: Callable[[T], object] | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
    ) -> None:
        self.on_next = on_next or (lambda _value: None)
        self.on_error = on_error or (lambda _error: None)
        self.on_completed = on_completed or (lambda: None)


class Observable(Generic[T]):
    def __init__(self, subscribe: Callable[[Observer[T], object | None], Any]) -> None:
        self._subscribe = subscribe

    def subscribe(
        self,
        observer: Observer[T] | Callable[[T], object] | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
    ) -> Any:
        callback = on_next or observer
        if callback is None:
            resolved = Observer[T](None, on_error, on_completed)
        elif callable(callback):
            resolved = Observer(callback, on_error, on_completed)
        else:
            resolved = callback
        subscription = self._subscribe(resolved, scheduler)
        return subscription if subscription is not None else Disposable()

    def pipe(self, *operators: Callable[[Observable[Any]], Observable[Any]]) -> Any:
        stream: Any = self
        for operator in operators:
            stream = operator(stream)
        return stream


class Subject(Observable[T], Observer[T]):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: dict[int, Observer[T]] = {}
        self._subscriber_snapshot: tuple[Observer[T], ...] = ()
        self._next_subscription_id = 0
        Observable.__init__(self, self._subscribe_subject)

    def on_next(self, value: T) -> None:
        for subscriber in self._subscriber_snapshot:
            subscriber.on_next(value)

    def on_error(self, error: Exception) -> None:
        for subscriber in self._subscriber_snapshot:
            subscriber.on_error(error)

    def on_completed(self) -> None:
        for subscriber in self._subscriber_snapshot:
            subscriber.on_completed()

    def _subscribe_subject(
        self, observer: Observer[T], _scheduler: object | None = None
    ) -> Disposable:
        with self._lock:
            subscription_id = self._next_subscription_id
            self._next_subscription_id += 1
            self._subscribers[subscription_id] = observer
            self._subscriber_snapshot = tuple(self._subscribers.values())
        return Disposable(lambda: self._unsubscribe(subscription_id))

    def _unsubscribe(self, subscription_id: int) -> None:
        with self._lock:
            if subscription_id not in self._subscribers:
                return
            self._subscribers.pop(subscription_id)
            self._subscriber_snapshot = tuple(self._subscribers.values())


class ConnectableObservable(Observable[T]):
    def __init__(self, source: Observable[T]) -> None:
        self._source = source
        self._subject: Subject[T] = Subject()
        Observable.__init__(self, self._subject.subscribe)

    def connect(self) -> Disposable:
        return self._source.subscribe(self._subject)


class SchedulerBase:
    def schedule(self, action: Callable[[Any, Any], object], state: Any = None) -> Any:
        return self.schedule_relative(0.0, action, state=state)

    def schedule_relative(
        self,
        duetime: timedelta | float,
        action: Callable[[Any, Any], object],
        state: Any = None,
    ) -> Any:
        raise NotImplementedError


class TimeoutScheduler(SchedulerBase):
    def schedule_relative(
        self,
        duetime: timedelta | float,
        action: Callable[[Any, Any], object],
        state: Any = None,
    ) -> Disposable:
        delay = _seconds(duetime)
        timer = threading.Timer(delay, lambda: action(self, state))
        timer.daemon = True
        timer.start()
        return Disposable(timer.cancel)


class ThreadPoolScheduler(SchedulerBase):
    def __init__(self, max_workers: int = 1) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def schedule_relative(
        self,
        duetime: timedelta | float,
        action: Callable[[Any, Any], object],
        state: Any = None,
    ) -> Disposable:
        cancelled = threading.Event()

        def run() -> None:
            delay = _seconds(duetime)
            if delay > 0:
                cancelled.wait(delay)
            if not cancelled.is_set():
                action(self, state)

        future = self._executor.submit(run)
        return Disposable(lambda: (cancelled.set(), future.cancel()))


class NewThreadScheduler(SchedulerBase):
    def __init__(
        self,
        thread_factory: Callable[[Callable[[], None]], threading.Thread] | None = None,
    ) -> None:
        self._thread_factory = thread_factory

    def schedule_relative(
        self,
        duetime: timedelta | float,
        action: Callable[[Any, Any], object],
        state: Any = None,
    ) -> Disposable:
        cancelled = threading.Event()

        def run() -> None:
            delay = _seconds(duetime)
            if delay > 0:
                cancelled.wait(delay)
            if not cancelled.is_set():
                action(self, state)

        thread = (
            self._thread_factory(run)
            if self._thread_factory is not None
            else threading.Thread(target=run, daemon=True)
        )
        thread.start()
        return Disposable(cancelled.set)


class EventLoopScheduler(NewThreadScheduler):
    pass


def _seconds(value: timedelta | float) -> float:
    if isinstance(value, timedelta):
        return max(0.0, value.total_seconds())
    return max(0.0, float(value))


operators = ModuleType("manyfold.streams.operators")
operators.map = map  # type: ignore[attr-defined]
operators.filter = filter  # type: ignore[attr-defined]
operators.publish = publish  # type: ignore[attr-defined]
operators.start_with = start_with  # type: ignore[attr-defined]
operators.scan = scan  # type: ignore[attr-defined]
operators.distinct_until_changed = distinct_until_changed  # type: ignore[attr-defined]
operators.do_action = do_action  # type: ignore[attr-defined]
operators.take = take  # type: ignore[attr-defined]
operators.take_until = take_until  # type: ignore[attr-defined]
operators.switch_latest = switch_latest  # type: ignore[attr-defined]
operators.observe_on = observe_on  # type: ignore[attr-defined]

scheduler = ModuleType("manyfold.streams.scheduler")
scheduler.EventLoopScheduler = EventLoopScheduler  # type: ignore[attr-defined]
scheduler.NewThreadScheduler = NewThreadScheduler  # type: ignore[attr-defined]
scheduler.ThreadPoolScheduler = ThreadPoolScheduler  # type: ignore[attr-defined]
scheduler.TimeoutScheduler = TimeoutScheduler  # type: ignore[attr-defined]

abc = ModuleType("manyfold.streams.abc")
abc.DisposableBase = Disposable  # type: ignore[attr-defined]
abc.ObservableBase = Observable  # type: ignore[attr-defined]
abc.SchedulerBase = SchedulerBase  # type: ignore[attr-defined]
abc.SubjectBase = Subject  # type: ignore[attr-defined]

disposable = ModuleType("manyfold.streams.disposable")
disposable.Disposable = Disposable  # type: ignore[attr-defined]
disposable.SerialDisposable = SerialDisposable  # type: ignore[attr-defined]

__all__ = (
    "CompositeDisposable",
    "ConnectableObservable",
    "Disposable",
    "EventLoopScheduler",
    "NewThreadScheduler",
    "Observable",
    "Observer",
    "SchedulerBase",
    "SerialDisposable",
    "Subject",
    "ThreadPoolScheduler",
    "TimeoutScheduler",
    "create",
    "empty",
    "filter",
    "from_iterable",
    "interval",
    "map",
    "observe_on",
    "operators",
    "pipe",
    "publish",
    "scan",
    "start_with",
    "switch_latest",
    "take",
    "take_until",
)
