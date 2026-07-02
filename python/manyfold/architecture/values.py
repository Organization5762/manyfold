"""Observable architecture values with explicit retention semantics."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from threading import Lock
from typing import Generic, TypeVar, final

T = TypeVar("T")
_VALUE_STREAM_FACTORY: Callable[..., object] | None = None
_VALUE_OBSERVABLE_FACTORY: Callable[[object], object] | None = None
_NO_SCAN_INITIAL = object()


@final
class ValueSubscription:
    """Disposable handle for architecture value observations."""

    def __init__(self, dispose: Callable[[], bool]) -> None:
        self._dispose: Callable[[], bool] | None = dispose

    @property
    def is_disposed(self) -> bool:
        """Return whether this handle has already been disposed."""
        return self._dispose is None

    def dispose(self) -> bool:
        """Stop future delivery for this subscription."""
        if self._dispose is None:
            return False
        dispose = self._dispose
        self._dispose = None
        return dispose()

    def __enter__(self) -> "ValueSubscription":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.dispose()


class NewValues(Generic[T]):
    """Future-only observable values with no retained current value."""

    def __init__(self, *, name: str | None = None) -> None:
        self.name = _validate_optional_name(name)
        self._subscribers = _SubscriberRegistry[T]()

    @property
    def subscriber_count(self) -> int:
        """Return the number of active observers."""
        return self._subscribers.count

    def publish(self, value: T) -> None:
        """Publish a new value to current subscribers only."""
        self._subscribers.publish(value)

    def set(self, value: T) -> None:
        """Alias for ``publish`` for handle-oriented call sites."""
        self.publish(value)

    def emit(self, value: T) -> None:
        """Alias for ``publish`` for callback-stream compatibility."""
        self.publish(value)

    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
    ) -> ValueSubscription:
        """Observe values published after this subscription is created."""
        return self._subscribers.subscribe(_callback_from_observer(callback, on_next))

    def subscribe(
        self,
        callback: Callable[[T], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
    ) -> ValueSubscription:
        """Alias for ``observe``."""
        del on_error, on_completed, scheduler
        return self.observe(callback, on_next=on_next)

    def pipe(self, *operators: Callable[[object], object]) -> object:
        """Apply live-stream operators to future values."""
        return self._observable().pipe(*operators)

    def map(
        self, transform: Callable[[T], object], *, name: str | None = None
    ) -> object:
        """Return a live stream view that transforms future values."""
        return self._observable().map(transform, name=name)

    def filter(
        self,
        predicate: Callable[[T], bool],
        *,
        name: str | None = None,
    ) -> object:
        """Return a live stream view that keeps accepted future values."""
        return self._observable().filter(predicate, name=name)

    def scan(
        self,
        reducer: Callable[[object, T], object],
        initial: object = _NO_SCAN_INITIAL,
        *,
        seed: object = _NO_SCAN_INITIAL,
    ) -> object:
        """Return a live stream of accumulated reducer state."""
        observable = self._observable()
        if initial is _NO_SCAN_INITIAL:
            if seed is _NO_SCAN_INITIAL:
                return observable.scan(reducer)
            return observable.scan(reducer, seed=seed)
        if seed is not _NO_SCAN_INITIAL:
            return observable.scan(reducer, initial, seed=seed)
        return observable.scan(reducer, initial)

    def start_with(self, *values: object) -> object:
        """Return a live stream that emits initial values before future values."""
        return self._observable().start_with(*values)

    def with_latest_from(self, *others: object) -> object:
        """Return a live stream combined with the latest values from others."""
        return self._observable().with_latest_from(*others)

    def do_action(
        self,
        action: Callable[[T], object] | None = None,
        *_args: object,
        on_next: Callable[[T], object] | None = None,
        **_kwargs: object,
    ) -> object:
        """Run a side effect for each future value and forward it."""
        return self._observable().do_action(action, on_next=on_next)

    def distinct_until_changed(
        self,
        key: Callable[[T], object] | None = None,
    ) -> object:
        """Forward future values whose comparison key changed."""
        return self._observable().distinct_until_changed(key)

    def pairwise(self) -> object:
        """Forward adjacent future value pairs."""
        return self._observable().pairwise()

    def take(self, count: int) -> object:
        """Forward at most ``count`` future values."""
        return self._observable().take(count)

    def flat_map(self, mapper: Callable[[T], object]) -> object:
        """Expand each future value into an inner live stream."""
        return self._observable().flat_map(mapper)

    def switch_latest(self, mapper: Callable[[T], object] | None = None) -> object:
        """Forward values from only the latest inner live stream."""
        return self._observable().switch_latest(mapper)

    def from_stream(
        self,
        stream: object | None = None,
        *,
        namespace: str = "default",
        schema: type | None = None,
        retained_messages: int = 1024,
    ) -> "_StreamNewValues[T]":
        """Back this handle with a PubSub stream."""
        return _StreamNewValues(
            _resolve_pubsub_stream(
                stream,
                name=self.name,
                namespace=namespace,
                schema=schema,
                retained_messages=retained_messages,
            ),
            schema=schema,
        )

    def _observable(self) -> object:
        if _VALUE_OBSERVABLE_FACTORY is None:
            raise RuntimeError("no architecture value observable factory is registered")
        return _VALUE_OBSERVABLE_FACTORY(self)


class ImmutableValue(Generic[T]):
    """Read-only current value that retains at most one latest value."""

    def __init__(
        self,
        initial: T | None = None,
        *,
        name: str | None = None,
        has_initial: bool = False,
    ) -> None:
        self.name = _validate_optional_name(name)
        self._lock = Lock()
        self._has_value = has_initial
        self._value = initial

    @property
    def has_value(self) -> bool:
        """Return whether a current value has been set."""
        with self._lock:
            return self._has_value

    @property
    def latest(self) -> T | None:
        """Return the current value, or ``None`` when no value has been set."""
        with self._lock:
            return self._value if self._has_value else None

    @property
    def subscriber_count(self) -> int:
        """Return the number of active observers.

        Immutable values have no future updates, so observations are delivered
        synchronously and never retained.
        """
        return 0

    @classmethod
    def initialized(cls, value: T, *, name: str | None = None) -> "ImmutableValue[T]":
        """Create an ``ImmutableValue`` whose current value is already set."""
        return cls(value, name=name, has_initial=True)

    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription:
        """Observe future updates and optionally receive the current value first."""
        resolved = _callback_from_observer(callback, on_next)
        if replay_latest:
            with self._lock:
                has_value = self._has_value
                value = self._value
            if has_value:
                resolved(value)  # type: ignore[arg-type]
        return ValueSubscription(lambda: False)

    def subscribe(
        self,
        callback: Callable[[T], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription:
        """Alias for ``observe``."""
        del on_error, on_completed, scheduler
        return self.observe(callback, on_next=on_next, replay_latest=replay_latest)

    def map(self, transform: Callable[[T], object]) -> "_MappedValue[T]":
        """Return an observed value view that transforms delivered values."""
        if not callable(transform):
            raise TypeError("ImmutableValue.map requires a callable transform")
        return _MappedValue(self, transform)


class Value(ImmutableValue[T]):
    """Mutable current value that retains at most one latest value."""

    def __init__(
        self,
        initial: T | None = None,
        *,
        name: str | None = None,
        has_initial: bool = False,
    ) -> None:
        super().__init__(initial, name=name, has_initial=has_initial)
        self._subscribers = _SubscriberRegistry[T]()

    @property
    def subscriber_count(self) -> int:
        """Return the number of active observers."""
        return self._subscribers.count

    @classmethod
    def initialized(cls, value: T, *, name: str | None = None) -> "Value[T]":
        """Create a ``Value`` whose current value is already set."""
        return cls(value, name=name, has_initial=True)

    def set(self, value: T) -> None:
        """Set the current value and notify active observers."""
        with self._lock:
            self._value = value
            self._has_value = True
        self._subscribers.publish(value)

    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription:
        """Observe future updates and optionally receive the current value first."""
        resolved = _callback_from_observer(callback, on_next)
        subscription = self._subscribers.subscribe(resolved)
        if replay_latest:
            with self._lock:
                has_value = self._has_value
                value = self._value
            if has_value and not subscription.is_disposed:
                resolved(value)  # type: ignore[arg-type]
        return subscription


class HistoricalValue(Generic[T]):
    """Observable value log with bounded retained history."""

    def __init__(self, *, name: str | None = None, retained_values: int = 1024) -> None:
        if isinstance(retained_values, bool) or not isinstance(retained_values, int):
            raise ValueError("retained_values must be an integer")
        if retained_values < 1:
            raise ValueError("retained_values must be positive")
        self.name = _validate_optional_name(name)
        self.retained_values = retained_values
        self._lock = Lock()
        self._history: deque[T] = deque(maxlen=retained_values)
        self._subscribers = _SubscriberRegistry[T]()

    @property
    def subscriber_count(self) -> int:
        """Return the number of active observers."""
        return self._subscribers.count

    @property
    def latest(self) -> T | None:
        """Return the most recent retained value, if one exists."""
        with self._lock:
            return self._history[-1] if self._history else None

    def append(self, value: T) -> None:
        """Append a value to bounded history and notify active observers."""
        with self._lock:
            self._history.append(value)
        self._subscribers.publish(value)

    def publish(self, value: T) -> None:
        """Alias for ``append``."""
        self.append(value)

    def set(self, value: T) -> None:
        """Alias for ``append`` for handle-oriented call sites."""
        self.append(value)

    def replay(self, *, limit: int | None = None) -> tuple[T, ...]:
        """Return retained values, optionally limited to the latest ``limit``."""
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise ValueError("limit must be an integer or None")
            if limit < 0:
                raise ValueError("limit must not be negative")
        with self._lock:
            history = tuple(self._history)
        if limit is None:
            return history
        if limit == 0:
            return ()
        return history[-limit:]

    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay: bool | int = False,
    ) -> ValueSubscription:
        """Observe future appends and optionally replay retained history first."""
        resolved = _callback_from_observer(callback, on_next)
        subscription = self._subscribers.subscribe(resolved)
        for value in self._replay_for_subscription(replay):
            if subscription.is_disposed:
                break
            resolved(value)
        return subscription

    def subscribe(
        self,
        callback: Callable[[T], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay: bool | int = False,
    ) -> ValueSubscription:
        """Alias for ``observe``."""
        del on_error, on_completed, scheduler
        return self.observe(callback, on_next=on_next, replay=replay)

    def from_stream(
        self,
        stream: object | None = None,
        *,
        namespace: str = "default",
        schema: type | None = None,
        retained_messages: int | None = None,
    ) -> "_StreamHistoricalValue[T]":
        """Back this retained history with a PubSub stream."""
        return _StreamHistoricalValue(
            _resolve_pubsub_stream(
                stream,
                name=self.name,
                namespace=namespace,
                schema=schema,
                retained_messages=retained_messages or self.retained_values,
            ),
            schema=schema,
            retained_values=self.retained_values,
        )

    def _replay_for_subscription(self, replay: bool | int) -> tuple[T, ...]:
        if replay is False:
            return ()
        if replay is True:
            return self.replay()
        if isinstance(replay, bool) or not isinstance(replay, int):
            raise ValueError("replay must be a bool or integer")
        if replay < 0:
            raise ValueError("replay must not be negative")
        return self.replay(limit=replay)


def _callback_from_observer(
    callback: Callable[[T], object] | object | None,
    on_next: Callable[[T], object] | None,
) -> Callable[[T], object]:
    if on_next is not None:
        return on_next
    if callback is None:
        return _ignore_value
    if callable(callback):
        return callback
    observer_callback = getattr(callback, "on_next", None)
    if callable(observer_callback):
        return observer_callback
    raise TypeError("observer must be callable or expose on_next")


def _ignore_value(_value: object) -> None:
    return None


def _resolve_pubsub_stream(
    stream: object | None,
    *,
    name: str | None,
    namespace: str,
    schema: type | None,
    retained_messages: int,
) -> object:
    if stream is not None:
        return stream
    if _VALUE_STREAM_FACTORY is None:
        raise RuntimeError("no architecture value stream factory is registered")
    return _VALUE_STREAM_FACTORY(
        name,
        namespace=namespace,
        schema=schema,
        retained_messages=retained_messages,
    )


def _stream_row_value(row: object, schema: type | None) -> object:
    if schema is None:
        return row
    as_model = getattr(row, "as_model", None)
    if not callable(as_model):
        return row
    return as_model(schema)


def _validate_optional_name(name: str | None) -> str | None:
    if name is None:
        return None
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty string")
    return name.strip()


def _register_value_stream_factory(factory: Callable[..., object]) -> None:
    global _VALUE_STREAM_FACTORY
    _VALUE_STREAM_FACTORY = factory


def _register_value_observable_factory(factory: Callable[[object], object]) -> None:
    global _VALUE_OBSERVABLE_FACTORY
    _VALUE_OBSERVABLE_FACTORY = factory


class _SubscriberRegistry(Generic[T]):
    def __init__(self) -> None:
        self._lock = Lock()
        self._next_id = 0
        self._callbacks: dict[int, Callable[[T], object]] = {}
        self._snapshot: tuple[Callable[[T], object], ...] = ()

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._callbacks)

    def subscribe(self, callback: Callable[[T], object]) -> ValueSubscription:
        with self._lock:
            subscription_id = self._next_id
            self._next_id += 1
            self._callbacks[subscription_id] = callback
            self._snapshot = tuple(self._callbacks.values())
        return ValueSubscription(lambda: self._unsubscribe(subscription_id))

    def publish(self, value: T) -> None:
        with self._lock:
            callbacks = self._snapshot
        for callback in callbacks:
            callback(value)

    def _unsubscribe(self, subscription_id: int) -> bool:
        with self._lock:
            if subscription_id not in self._callbacks:
                return False
            self._callbacks.pop(subscription_id)
            self._snapshot = tuple(self._callbacks.values())
            return True


class _MappedValue(Generic[T]):
    def __init__(
        self,
        source: ImmutableValue[T],
        transform: Callable[[T], object],
    ) -> None:
        self._source = source
        self._transform = transform

    def observe(
        self,
        callback: Callable[[object], object] | object | None = None,
        *,
        on_next: Callable[[object], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription:
        resolved = _callback_from_observer(callback, on_next)
        return self._source.observe(
            lambda value: resolved(self._transform(value)),
            replay_latest=replay_latest,
        )

    def subscribe(
        self,
        callback: Callable[[object], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[object], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription:
        del on_error, on_completed, scheduler
        return self.observe(callback, on_next=on_next, replay_latest=replay_latest)


class _StreamNewValues(Generic[T]):
    def __init__(self, stream: object, *, schema: type | None) -> None:
        self._stream = stream
        self._schema = schema

    @property
    def name(self) -> str:
        return self.topic

    @property
    def topic(self) -> str:
        return str(getattr(self._stream, "topic"))

    def publish(self, value: T) -> None:
        self._stream.publish(value)  # type: ignore[attr-defined]

    def set(self, value: T) -> None:
        self.publish(value)

    def emit(self, value: T) -> None:
        self.publish(value)

    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
    ) -> object:
        resolved = _callback_from_observer(callback, on_next)
        return self._stream.subscribe(  # type: ignore[attr-defined]
            lambda row: resolved(_stream_row_value(row, self._schema)),  # type: ignore[arg-type]
        )

    def subscribe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
    ) -> object:
        return self.observe(callback, on_next=on_next)


class _StreamHistoricalValue(Generic[T]):
    def __init__(
        self,
        stream: object,
        *,
        schema: type | None,
        retained_values: int,
    ) -> None:
        self._stream = stream
        self._schema = schema
        self.retained_values = retained_values

    @property
    def name(self) -> str:
        return self.topic

    @property
    def topic(self) -> str:
        return str(getattr(self._stream, "topic"))

    @property
    def latest(self) -> T | None:
        row = self._stream.latest()  # type: ignore[attr-defined]
        return None if row is None else _stream_row_value(row, self._schema)  # type: ignore[return-value]

    def append(self, value: T) -> None:
        self._stream.publish(value)  # type: ignore[attr-defined]

    def publish(self, value: T) -> None:
        self.append(value)

    def set(self, value: T) -> None:
        self.append(value)

    def replay(self, *, limit: int | None = None) -> tuple[T, ...]:
        resolved_limit = self.retained_values if limit is None else limit
        query = """
        SELECT *
        FROM (
            SELECT *
            FROM stream
            ORDER BY event_time DESC, process_sequence DESC
            LIMIT :__manyfold_limit
        ) latest
        ORDER BY event_time, process_sequence
        """
        if isinstance(resolved_limit, bool) or not isinstance(resolved_limit, int):
            raise ValueError("limit must be an integer or None")
        if resolved_limit < 0:
            raise ValueError("limit must not be negative")
        if resolved_limit == 0:
            return ()
        rows = self._stream.query(  # type: ignore[attr-defined]
            query,
            {"__manyfold_limit": resolved_limit},
        )
        return tuple(_stream_row_value(row, self._schema) for row in rows)  # type: ignore[return-value]

    def observe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay: bool | int = False,
    ) -> object:
        resolved = _callback_from_observer(callback, on_next)
        if replay is False:
            replay_latest = False
        elif replay is True:
            for value in self.replay():
                resolved(value)
            replay_latest = False
        elif isinstance(replay, bool) or not isinstance(replay, int):
            raise ValueError("replay must be a bool or integer")
        elif replay < 0:
            raise ValueError("replay must not be negative")
        else:
            for value in self.replay(limit=replay):
                resolved(value)
            replay_latest = False
        return self._stream.subscribe(  # type: ignore[attr-defined]
            lambda row: resolved(_stream_row_value(row, self._schema)),  # type: ignore[arg-type]
            replay_latest=replay_latest,
        )

    def subscribe(
        self,
        callback: Callable[[T], object] | object | None = None,
        *,
        on_next: Callable[[T], object] | None = None,
        replay: bool | int = False,
    ) -> object:
        return self.observe(callback, on_next=on_next, replay=replay)


__all__ = [
    "HistoricalValue",
    "ImmutableValue",
    "NewValues",
    "Value",
    "ValueSubscription",
]
