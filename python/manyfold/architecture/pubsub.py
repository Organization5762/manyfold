"""Rust-backed PubSub primitives for application architecture."""

from __future__ import annotations

import os
import struct
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, is_dataclass
from threading import get_ident
from time import time_ns
from types import NoneType
from typing import Union, get_args, get_origin, get_type_hints
from uuid import UUID, uuid5

from manyfold._manyfold_rust import (
    FlatBufferField,
    FlatBufferTable,
    InMemoryPubSub as InMemoryPubSub,
    PubSubDelivery as PubSubDelivery,
    PubSubMessage as PubSubMessage,
    PubSubRuntime,
    PubSubSubscription as PubSubSubscription,
)
from manyfold.architecture.locks import ManyFoldLock

_EPHEMERAL_TOPIC_NAMESPACE = UUID("6b9b5ff0-76e8-5c7f-b3b3-854f53ed8a3e")
_FILTERED = object()
_NO_SCAN_INITIAL = object()
_PUBSUB_FABRICS: dict[str, "PubSubFabric"] = {}


def PubSubTopic(
    name: str | None = None,
    *,
    schema: type | None = None,
    namespace: str = "default",
    retained_messages: int = 1024,
    worker_name: str | None = None,
    service_discovery: bool = True,
    pubsub: str | None = None,
    fabric: str | None = None,
) -> "PubSub":
    """Return a topic from a namespace-scoped process-local PubSub runtime."""
    resolved_namespace = _resolve_namespace(namespace, pubsub=pubsub, fabric=fabric)
    topic_name = _resolve_topic_name(name)
    boot_lock = ManyFoldLock.for_resource(f"pubsub:{resolved_namespace}:boot")
    with boot_lock.take(owner=f"PubSubTopic:{resolved_namespace}"):
        runtime = _PUBSUB_FABRICS.get(resolved_namespace)
        if runtime is None:
            runtime = PubSubFabric(
                name=resolved_namespace,
                retained_messages=retained_messages,
                worker_name=worker_name,
                service_discovery=service_discovery,
            )
            _PUBSUB_FABRICS[resolved_namespace] = runtime
    return runtime.topic(topic_name, schema=schema)


class PubSub:
    """Topic-scoped PubSub stream with SQL-backed state."""

    def __init__(
        self,
        *,
        topic: str | None = None,
        schema: type | None = None,
        retained_messages: int = 1024,
        schedule: bool = True,
        worker_name: str | None = None,
        service_discovery: bool = True,
        _runtime: PubSubRuntime | None = None,
        _schedule: "PubSubSchedule | None" = None,
    ) -> None:
        resolved_topic = _resolve_topic(topic)
        self.topic = resolved_topic
        self.schema = schema
        self.schedule = _schedule if _runtime is not None else (
            PubSubSchedule.current_thread(
                topic=resolved_topic,
                worker_name=worker_name,
                service_discovery=service_discovery,
            )
            if schedule
            else None
        )
        self._stream_schema = _coerce_stream_schema(resolved_topic, schema)
        self._runtime = _runtime or PubSubRuntime(
            name=resolved_topic,
            retained_messages=retained_messages,
        )
        if self._stream_schema is not None:
            self._runtime.register_flatbuffer_pad(
                resolved_topic,
                self._stream_schema.table,
            )
        self._callbacks: dict[int, Callable[[StreamRow], object]] = {}
        self._next_callback_id = 1

    def publish(
        self,
        payload: object | None = None,
        *,
        event_time: int | None = None,
        key: str | None = None,
        **fields: object,
    ) -> None:
        """Publish a model value, bytes payload, or schema field values."""
        if fields:
            if payload is not None:
                raise ValueError("publish accepts either a payload or fields, not both")
            if self._stream_schema is None:
                raise ValueError("field publishing requires a stream schema")
            payload_bytes = self._stream_schema.encode(fields)
        elif payload is None:
            raise TypeError("publish requires a payload or fields")
        elif self._stream_schema is None and _is_model_value(payload):
            self._infer_stream_schema(type(payload))
            payload_bytes = self._stream_schema.encode(_value_fields(payload))
        elif self._stream_schema is not None and _is_schema_value(payload, self.schema):
            payload_bytes = self._stream_schema.encode(_value_fields(payload))
        elif self._stream_schema is not None and _is_model_value(payload):
            raise ValueError(
                "stream schema is already fixed; create another PubSub stream "
                "for a different model shape"
            )
        else:
            payload_bytes = _payload_bytes(payload)

        self._runtime.publish(
            self.topic,
            payload_bytes,
            pad_name=self.topic,
            event_time=event_time,
            key=key,
        )
        self._publish_to_callbacks()

    def subscribe(
        self,
        callback: Callable[[StreamRow], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[StreamRow], object] | None = None,
        replay_latest: bool = False,
    ) -> "PubSubCallbackSubscription":
        """Call ``callback`` for rows published after subscription."""
        del on_error, on_completed, scheduler
        callback = _callback_from_observer(
            "PubSub.subscribe",
            callback,
            on_next=on_next,
        )
        if not callable(callback):
            raise TypeError("PubSub.subscribe requires a callable callback")
        callback_id = self._next_callback_id
        self._next_callback_id += 1
        self._callbacks[callback_id] = callback
        subscription = PubSubCallbackSubscription(
            lambda: self._callbacks.pop(callback_id, None) is not None
        )
        if replay_latest:
            latest = self.latest()
            if latest is not None and callback_id in self._callbacks:
                callback(latest)
        return subscription

    def callback(
        self,
        callback: Callable[[StreamRow], object],
        *,
        name: str | None = None,
        replay_latest: bool = False,
    ) -> "PubSubCallbackSubscription":
        """Alias for ``subscribe`` when wiring callback-style stream sinks."""
        del name
        return self.subscribe(callback, replay_latest=replay_latest)

    def pipe(
        self,
        *operators: Callable[["PubSubObservable"], "PubSubObservable"],
    ) -> "PubSubObservable":
        """Apply observable operators to this stream."""
        return PubSubObservable(self).pipe(*operators)

    def start_with(self, *values: object) -> "PubSubObservable":
        """Return a live stream that emits initial values before source rows."""
        return PubSubObservable(self).start_with(*values)

    def map(
        self,
        transform: Callable[[StreamRow], object],
        *,
        name: str | None = None,
    ) -> "PubSubObservable":
        """Return a callback stream that transforms each published row."""
        del name
        if not callable(transform):
            raise TypeError("PubSub.map requires a callable transform")
        return PubSubObservable(self).map(transform)

    def filter(
        self,
        predicate: Callable[[StreamRow], bool],
        *,
        name: str | None = None,
    ) -> "PubSubObservable":
        """Return a callback stream that keeps rows accepted by ``predicate``."""
        del name
        if not callable(predicate):
            raise TypeError("PubSub.filter requires a callable predicate")
        return PubSubObservable(self).filter(predicate)

    def scan(
        self,
        reducer: Callable[[object, StreamRow], object],
        initial: object = _NO_SCAN_INITIAL,
        *,
        seed: object = _NO_SCAN_INITIAL,
    ) -> "PubSubObservable":
        """Return a live stream of accumulated reducer state."""
        initial = _resolve_scan_initial(initial, seed)
        return PubSubObservable(self).scan(reducer, initial)

    def with_latest_from(self, *others: object) -> "PubSubObservable":
        """Return a live stream of ``(value, latest_other)`` tuples."""
        return PubSubObservable(self).with_latest_from(*others)

    def do_action(
        self,
        action: Callable[[StreamRow], object] | None = None,
        *_args: object,
        on_next: Callable[[StreamRow], object] | None = None,
        **_kwargs: object,
    ) -> "PubSubObservable":
        """Return a live stream that taps each value before forwarding it."""
        return PubSubObservable(self).do_action(action, on_next=on_next)

    def flat_map(self, mapper: Callable[[StreamRow], object]) -> "PubSubObservable":
        """Return a live stream expanding inner PubSub streams."""
        return PubSubObservable(self).flat_map(mapper)

    def switch_latest(
        self,
        mapper: Callable[[StreamRow], object] | None = None,
    ) -> "PubSubObservable":
        """Return a live stream from only the latest inner PubSub stream."""
        return PubSubObservable(self).switch_latest(mapper)

    def where(self, **field_values: object) -> list["StreamRow"]:
        """Return rows whose fields equal the provided values."""
        if not field_values:
            raise ValueError("where requires at least one field value")
        parameters: dict[str, object] = {}
        clauses: list[str] = []
        for index, (field, value) in enumerate(field_values.items()):
            _require_sql_identifier(field, "field")
            parameter = f"value_{index}"
            parameters[parameter] = value
            if value is None:
                clauses.append(f"{field} IS :{parameter}")
            else:
                clauses.append(f"{field} = :{parameter}")
        return self.query(
            f"""
            SELECT *
            FROM stream
            WHERE {' AND '.join(clauses)}
            """,
            parameters,
        )

    def project(self, *fields: str) -> list["StreamRow"]:
        """Return rows with only selected fields."""
        if not fields:
            raise ValueError("project requires at least one field")
        for field in fields:
            _require_sql_identifier(field, "field")
        return self.query(
            f"""
            SELECT {', '.join(fields)}
            FROM stream
            """
        )

    def take(self, count: int = 1) -> list["StreamRow"]:
        """Return the first ``count`` rows in event order."""
        if isinstance(count, bool) or not isinstance(count, int):
            raise ValueError("count must be an integer")
        if count < 1:
            raise ValueError("count must be positive")
        return self.query(
            """
            SELECT *
            FROM stream
            ORDER BY event_time, process_sequence
            LIMIT :count
            """,
            {"count": count},
        )

    def distinct_until_changed(
        self,
        field: str | None = None,
    ) -> list["StreamRow"] | "PubSubObservable":
        """Return rows whose field changed from the previous event."""
        if field is None:
            return PubSubObservable(self).distinct_until_changed()
        _require_sql_identifier(field, "field")
        return self.query(
            f"""
            SELECT *
            FROM (
                SELECT *,
                       LAG({field}) OVER (
                           ORDER BY event_time, process_sequence
                       ) AS previous_value
                FROM stream
            ) ordered
            WHERE previous_value IS NULL OR {field} IS NOT previous_value
            """
        )

    def pairwise(self, field: str | None = None) -> list["StreamRow"] | "PubSubObservable":
        """Return adjacent previous/current values for one field."""
        if field is None:
            return PubSubObservable(self).pairwise()
        _require_sql_identifier(field, "field")
        return self.query(
            f"""
            SELECT previous.{field} AS previous_value,
                   current.{field} AS current_value
            FROM stream current
            JOIN stream previous
              ON previous.offset = current.offset - 1
            ORDER BY current.event_time, current.process_sequence
            """
        )

    def latest_join(
        self,
        other: "PubSub",
        *fields: str,
        prefix: str | None = None,
    ) -> list["StreamRow"]:
        """Join each row with the latest earlier row from another topic."""
        if not fields:
            raise ValueError("latest_join requires at least one field")
        if not isinstance(other, PubSub):
            raise TypeError("other must be a PubSub")
        for field in fields:
            _require_sql_identifier(field, "field")
        alias_prefix = prefix or _sql_alias_prefix(other.topic)
        _require_sql_identifier(alias_prefix, "prefix")
        projections = ", ".join(
            f"latest_other.{field} AS {alias_prefix}_{field}" for field in fields
        )
        return self.query(
            f"""
            SELECT stream.*, {projections}
            FROM stream
            LEFT JOIN stream_messages latest_other
              ON latest_other.topic = :__manyfold_other_topic
             AND latest_other.pad_name = :__manyfold_other_topic
             AND latest_other.offset = (
                SELECT MAX(candidate.offset)
                FROM stream_messages candidate
                WHERE candidate.topic = :__manyfold_other_topic
                  AND candidate.pad_name = :__manyfold_other_topic
                  AND candidate.event_time <= stream.event_time
             )
            """,
            {"__manyfold_other_topic": other.topic},
        )

    def recursive_sum(
        self,
        field: str,
        *,
        value: str = "elapsed_ms",
        initial: float = 0.0,
    ) -> list["StreamRow"]:
        """Accumulate one numeric field across contiguous offsets."""
        _require_sql_identifier(field, "field")
        _require_sql_identifier(value, "value")
        return self.query(
            f"""
            WITH RECURSIVE state(seq, {value}) AS (
              SELECT 0, :__manyfold_initial_value
              UNION ALL
              SELECT frame.offset, state.{value} + frame.{field}
              FROM state
              JOIN stream frame ON frame.offset = state.seq + 1
            )
            SELECT *
            FROM state
            """,
            {"__manyfold_initial_value": initial},
        )

    def query(
        self,
        sql: str,
        parameters: Mapping[str, object] | None = None,
    ) -> list[StreamRow]:
        """Run SQL against this stream's scoped ``stream`` relation."""
        query_parameters = dict(parameters or {})
        if "__manyfold_stream_topic" in query_parameters:
            raise ValueError("query parameter '__manyfold_stream_topic' is reserved")
        query_parameters["__manyfold_stream_topic"] = self.topic
        scoped_sql = _scope_stream_query(sql)
        rows = self._runtime.query(
            scoped_sql,
            query_parameters,
        )
        return [StreamRow(row) for row in rows]

    def query_one(
        self,
        sql: str,
        parameters: Mapping[str, object] | None = None,
    ) -> StreamRow | None:
        """Run SQL against this stream with at most one result row."""
        rows = self.query(sql, parameters)
        if len(rows) > 1:
            raise ValueError("stream query returned more than one row")
        return None if not rows else rows[0]

    def latest(self) -> StreamRow | None:
        """Return the latest stream row using SQL ordering."""
        return self.query_one(
            """
            SELECT *, offset + 1 AS seq_source
            FROM stream
            ORDER BY event_time DESC, process_sequence DESC
            LIMIT 1
            """
        )

    def average(self, *, field: str) -> float | None:
        """Return the average of a numeric stream field."""
        _require_sql_identifier(field, "field")
        row = self.query_one(
            f"""
            SELECT AVG({field}) AS average
            FROM stream
            """
        )
        if row is None or row.average is None:
            return None
        return float(row.average)

    def _infer_stream_schema(self, model: type) -> None:
        self.schema = model
        self._stream_schema = _coerce_stream_schema(self.topic, model)
        if self._stream_schema is None:
            raise ValueError("could not infer stream schema")
        self._runtime.register_flatbuffer_pad(
            self.topic,
            self._stream_schema.table,
        )

    def _publish_to_callbacks(self) -> None:
        if not self._callbacks:
            return
        row = self.latest()
        if row is None:
            return
        for callback in tuple(self._callbacks.values()):
            callback(row)


class PubSubCallbackSubscription:
    """Disposable handle returned by a PubSub callback subscription."""

    def __init__(self, dispose_callback: Callable[[], bool]) -> None:
        self._dispose_callback: Callable[[], bool] | None = dispose_callback

    @property
    def is_disposed(self) -> bool:
        """Return whether this subscription has already been disposed."""
        return self._dispose_callback is None

    def dispose(self) -> bool:
        """Stop future callback delivery."""
        if self._dispose_callback is None:
            return False
        dispose_callback = self._dispose_callback
        self._dispose_callback = None
        return dispose_callback()


class PubSubObservable:
    """Small callback stream view over a PubSub topic."""

    def __init__(
        self,
        source: PubSub | None = None,
        transforms: tuple[Callable[[object], object], ...] = (),
        subscribe_factory: Callable[
            [Callable[[object], object], bool],
            PubSubCallbackSubscription,
        ]
        | None = None,
    ) -> None:
        self._source = source
        self._transforms = transforms
        self._subscribe_factory = subscribe_factory

    def subscribe(
        self,
        callback: Callable[[object], object] | object | None = None,
        on_error: Callable[[Exception], object] | None = None,
        on_completed: Callable[[], object] | None = None,
        scheduler: object | None = None,
        *,
        on_next: Callable[[object], object] | None = None,
        replay_latest: bool = False,
    ) -> PubSubCallbackSubscription:
        """Call ``callback`` for each transformed source row."""
        del on_error, on_completed, scheduler
        callback = _callback_from_observer(
            "PubSubObservable.subscribe",
            callback,
            on_next=on_next,
        )
        if not callable(callback):
            raise TypeError("PubSubObservable.subscribe requires a callable callback")
        if self._subscribe_factory is not None:
            return self._subscribe_factory(callback, replay_latest)
        if self._source is None:
            raise RuntimeError("PubSubObservable has no source")

        def receive(row: StreamRow) -> None:
            value: object = row
            for transform in self._transforms:
                value = transform(value)
                if value is _FILTERED:
                    return
            callback(value)

        return self._source.subscribe(receive, replay_latest=replay_latest)

    def callback(
        self,
        callback: Callable[[object], object],
        *,
        name: str | None = None,
        replay_latest: bool = False,
    ) -> PubSubCallbackSubscription:
        """Alias for ``subscribe`` when wiring callback-style stream sinks."""
        del name
        return self.subscribe(callback, replay_latest=replay_latest)

    def pipe(
        self,
        *operators: Callable[["PubSubObservable"], "PubSubObservable"],
    ) -> "PubSubObservable":
        """Apply operators left-to-right."""
        stream = self
        for operator in operators:
            if not callable(operator):
                raise TypeError("pipe operators must be callable")
            stream = operator(stream)
            if not isinstance(stream, PubSubObservable):
                raise TypeError("pipe operators must return PubSubObservable")
        return stream

    def start_with(self, *values: object) -> "PubSubObservable":
        """Emit initial values before subscribing to source rows."""

        def subscribe(
            callback: Callable[[object], object],
            replay_latest: bool,
        ) -> PubSubCallbackSubscription:
            for value in values:
                callback(value)
            return self.subscribe(callback, replay_latest=replay_latest)

        return PubSubObservable(subscribe_factory=subscribe)

    def map(
        self,
        transform: Callable[[object], object],
        *,
        name: str | None = None,
    ) -> "PubSubObservable":
        """Return a stream view that transforms accepted values."""
        del name
        if not callable(transform):
            raise TypeError("PubSubObservable.map requires a callable transform")
        if self._subscribe_factory is not None or self._source is None:
            def subscribe(
                callback: Callable[[object], object],
                replay_latest: bool,
            ) -> PubSubCallbackSubscription:
                return self.subscribe(
                    lambda value: callback(transform(value)),
                    replay_latest=replay_latest,
                )

            return PubSubObservable(subscribe_factory=subscribe)
        return PubSubObservable(self._source, (*self._transforms, transform))

    def filter(
        self,
        predicate: Callable[[object], bool],
        *,
        name: str | None = None,
    ) -> "PubSubObservable":
        """Return a stream view that keeps values accepted by ``predicate``."""
        del name
        if not callable(predicate):
            raise TypeError("PubSubObservable.filter requires a callable predicate")
        if self._subscribe_factory is not None or self._source is None:
            def subscribe(
                callback: Callable[[object], object],
                replay_latest: bool,
            ) -> PubSubCallbackSubscription:
                return self.subscribe(
                    lambda value: callback(value) if predicate(value) else None,
                    replay_latest=replay_latest,
                )

            return PubSubObservable(subscribe_factory=subscribe)

        def apply_filter(value: object) -> object:
            return value if predicate(value) else _FILTERED

        return PubSubObservable(self._source, (*self._transforms, apply_filter))

    def scan(
        self,
        reducer: Callable[[object, object], object],
        initial: object = _NO_SCAN_INITIAL,
        *,
        seed: object = _NO_SCAN_INITIAL,
    ) -> "PubSubObservable":
        """Return a stream of accumulated reducer state."""
        initial = _resolve_scan_initial(initial, seed)
        if not callable(reducer):
            raise TypeError("PubSubObservable.scan requires a callable reducer")

        def subscribe(
            callback: Callable[[object], object],
            replay_latest: bool,
        ) -> PubSubCallbackSubscription:
            has_state = initial is not _NO_SCAN_INITIAL
            state = initial

            def receive(value: object) -> None:
                nonlocal has_state, state
                if has_state:
                    state = reducer(state, value)
                else:
                    state = value
                    has_state = True
                callback(state)

            return self.subscribe(receive, replay_latest=replay_latest)

        return PubSubObservable(subscribe_factory=subscribe)

    def with_latest_from(self, *others: object) -> "PubSubObservable":
        """Combine each source value with the latest value from another stream."""
        if not others:
            raise ValueError("with_latest_from requires at least one source")
        other_streams = tuple(_as_pubsub_observable(other) for other in others)

        def subscribe(
            callback: Callable[[object], object],
            replay_latest: bool,
        ) -> PubSubCallbackSubscription:
            latest: list[object] = [None] * len(other_streams)
            ready = [False] * len(other_streams)

            def remember(value: object, index: int) -> None:
                latest[index] = value
                ready[index] = True

            other_subscriptions = [
                other_stream.subscribe(
                    lambda value, index=index: remember(value, index),
                    replay_latest=True,
                )
                for index, other_stream in enumerate(other_streams)
            ]

            def receive(value: object) -> None:
                if all(ready):
                    callback((value, *latest))

            source_subscription = self.subscribe(receive, replay_latest=replay_latest)
            return _composite_subscription(source_subscription, *other_subscriptions)

        return PubSubObservable(subscribe_factory=subscribe)

    def do_action(
        self,
        action: Callable[[object], object] | None = None,
        *_args: object,
        on_next: Callable[[object], object] | None = None,
        **_kwargs: object,
    ) -> "PubSubObservable":
        """Run a side effect for each value and forward the value unchanged."""
        action = on_next if on_next is not None else action
        if action is None:
            return self
        if not callable(action):
            raise TypeError("PubSubObservable.do_action requires a callable action")

        def tap(value: object) -> object:
            action(value)
            return value

        return self.map(tap)

    def distinct_until_changed(
        self,
        key: Callable[[object], object] | None = None,
    ) -> "PubSubObservable":
        """Forward values whose comparison key changed from the previous value."""
        if key is not None and not callable(key):
            raise TypeError("PubSubObservable.distinct_until_changed key must be callable")

        def subscribe(
            callback: Callable[[object], object],
            replay_latest: bool,
        ) -> PubSubCallbackSubscription:
            sentinel = object()
            previous: object = sentinel

            def receive(value: object) -> None:
                nonlocal previous
                current = key(value) if key is not None else value
                if previous is sentinel or current != previous:
                    previous = current
                    callback(value)

            return self.subscribe(receive, replay_latest=replay_latest)

        return PubSubObservable(subscribe_factory=subscribe)

    def pairwise(self) -> "PubSubObservable":
        """Forward adjacent ``(previous, current)`` value pairs."""

        def subscribe(
            callback: Callable[[object], object],
            replay_latest: bool,
        ) -> PubSubCallbackSubscription:
            sentinel = object()
            previous: object = sentinel

            def receive(value: object) -> None:
                nonlocal previous
                if previous is not sentinel:
                    callback((previous, value))
                previous = value

            return self.subscribe(receive, replay_latest=replay_latest)

        return PubSubObservable(subscribe_factory=subscribe)

    def take(self, count: int) -> "PubSubObservable":
        """Forward at most ``count`` values from the live stream."""
        if isinstance(count, bool) or not isinstance(count, int):
            raise ValueError("count must be an integer")
        if count < 1:
            raise ValueError("count must be positive")

        def subscribe(
            callback: Callable[[object], object],
            replay_latest: bool,
        ) -> PubSubCallbackSubscription:
            seen = 0
            subscription: PubSubCallbackSubscription | None = None

            def receive(value: object) -> None:
                nonlocal seen
                if seen >= count:
                    return
                seen += 1
                callback(value)
                if seen >= count and subscription is not None:
                    subscription.dispose()

            subscription = self.subscribe(receive, replay_latest=replay_latest)
            return subscription

        return PubSubObservable(subscribe_factory=subscribe)

    def flat_map(self, mapper: Callable[[object], object]) -> "PubSubObservable":
        """Expand each source value into an inner PubSub stream."""
        if not callable(mapper):
            raise TypeError("PubSubObservable.flat_map requires a callable mapper")

        def subscribe(
            callback: Callable[[object], object],
            replay_latest: bool,
        ) -> PubSubCallbackSubscription:
            subscriptions: list[PubSubCallbackSubscription] = []

            def receive(value: object) -> None:
                inner = _as_pubsub_observable(mapper(value))
                subscriptions.append(inner.subscribe(callback, replay_latest=True))

            subscriptions.append(self.subscribe(receive, replay_latest=replay_latest))

            def dispose() -> bool:
                disposed = False
                for subscription in tuple(subscriptions):
                    disposed = subscription.dispose() or disposed
                return disposed

            return PubSubCallbackSubscription(dispose)

        return PubSubObservable(subscribe_factory=subscribe)

    def switch_latest(
        self,
        mapper: Callable[[object], object] | None = None,
    ) -> "PubSubObservable":
        """Forward values from only the latest inner PubSub stream."""
        if mapper is not None and not callable(mapper):
            raise TypeError("PubSubObservable.switch_latest mapper must be callable")

        def subscribe(
            callback: Callable[[object], object],
            replay_latest: bool,
        ) -> PubSubCallbackSubscription:
            current_inner: PubSubCallbackSubscription | None = None

            def receive(value: object) -> None:
                nonlocal current_inner
                if current_inner is not None:
                    current_inner.dispose()
                inner_value = mapper(value) if mapper is not None else value
                current_inner = _as_pubsub_observable(inner_value).subscribe(
                    callback,
                    replay_latest=True,
                )

            outer = self.subscribe(receive, replay_latest=replay_latest)

            def dispose() -> bool:
                disposed = outer.dispose()
                if current_inner is not None:
                    disposed = current_inner.dispose() or disposed
                return disposed

            return PubSubCallbackSubscription(dispose)

        return PubSubObservable(subscribe_factory=subscribe)


class PubSubFabric:
    """Shared PubSub runtime that owns multiple topic handles."""

    def __init__(
        self,
        *,
        namespace: str = "default",
        name: str | None = None,
        retained_messages: int = 1024,
        worker_name: str | None = None,
        service_discovery: bool = True,
    ) -> None:
        self.name = _resolve_namespace(namespace if name is None else name)
        self.namespace = self.name
        self.boot_lock = ManyFoldLock.for_resource(f"pubsub:{self.namespace}:boot")
        self.schedule = PubSubSchedule.current_thread(
            topic=self.namespace,
            worker_name=worker_name or f"pubsub:{self.namespace}",
            service_discovery=service_discovery,
        )
        self._runtime = PubSubRuntime(
            name=self.namespace,
            retained_messages=retained_messages,
        )
        self._topics: dict[str, PubSub] = {}

    @property
    def topics(self) -> tuple[str, ...]:
        """Return topics already attached to this fabric."""
        return tuple(self._topics)

    def topic(self, topic: str, *, schema: type | None = None) -> PubSub:
        """Return the topic handle, creating it on first use."""
        resolved_topic = _resolve_topic(topic)
        existing = self._topics.get(resolved_topic)
        if existing is not None:
            if schema is not None and existing.schema is not schema:
                raise ValueError("PubSubTopic schema is already fixed")
            return existing
        created = PubSub(
            topic=resolved_topic,
            schema=schema,
            schedule=True,
            _runtime=self._runtime,
            _schedule=self.schedule,
        )
        self._topics[resolved_topic] = created
        return created


@dataclass(frozen=True)
class ServiceDiscoveryRequirement:
    """Process-level dependency PubSub needs for worker discovery."""

    service: str
    affinity: str
    spawn_policy: str
    address: str


@dataclass(frozen=True)
class PubSubSchedule:
    """Locality metadata for the worker that owns a PubSub runtime."""

    worker: str
    affinity: str
    process_id: int
    thread_id: int
    address: str
    service_discovery: ServiceDiscoveryRequirement | None

    @classmethod
    def current_thread(
        cls,
        *,
        topic: str,
        worker_name: str | None = None,
        service_discovery: bool = True,
    ) -> "PubSubSchedule":
        """Describe a PubSub runtime scheduled on the launching thread."""
        process_id = os.getpid()
        thread_id = get_ident()
        worker = _resolve_worker_name(topic, worker_name)
        return cls(
            worker=worker,
            affinity="current-process-thread",
            process_id=process_id,
            thread_id=thread_id,
            address=f"thread://{process_id}/{thread_id}",
            service_discovery=(
                ServiceDiscoveryRequirement(
                    service="ServiceDiscovery",
                    affinity="separate-process",
                    spawn_policy="spawn_if_missing",
                    address="process://manyfold/service-discovery",
                )
                if service_discovery
                else None
            ),
        )


class StreamRow(Mapping[str, object]):
    """Read-only row returned from PubSub SQL queries."""

    def __init__(self, values: Mapping[str, object]) -> None:
        self._values = dict(values)

    def __getitem__(self, name: str) -> object:
        return self._values[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getattr__(self, name: str) -> object:
        try:
            return self._values[name]
        except KeyError as error:
            raise AttributeError(name) from error

    def __eq__(self, other: object) -> bool:
        if isinstance(other, StreamRow):
            return self._values == other._values
        if isinstance(other, Mapping):
            return self._values == dict(other)
        return False

    def __repr__(self) -> str:
        return f"StreamRow({self._values!r})"

    def as_dict(self) -> dict[str, object]:
        """Return a plain dictionary copy of this row."""
        return dict(self._values)

    def as_model(self, model: type) -> object:
        """Construct a model from matching row fields."""
        fields = _schema_field_types(model)
        return model(**{name: self._values[name] for name in fields})


def _as_pubsub_observable(value: object) -> PubSubObservable:
    if isinstance(value, PubSubObservable):
        return value
    if isinstance(value, PubSub):
        return PubSubObservable(value)
    raise TypeError("value must be a PubSub or PubSubObservable")


def _callback_from_observer(
    method: str,
    callback: object | None,
    *,
    on_next: Callable[[object], object] | None,
) -> Callable[[object], object]:
    if on_next is not None:
        return on_next
    if callback is None:
        return lambda _value: None
    if callable(callback):
        return callback
    observer_callback = getattr(callback, "on_next", None)
    if callable(observer_callback):
        return observer_callback
    raise TypeError(f"{method} requires a callable callback")


def _resolve_scan_initial(initial: object, seed: object) -> object:
    if initial is not _NO_SCAN_INITIAL and seed is not _NO_SCAN_INITIAL:
        raise ValueError("scan accepts either initial or seed, not both")
    if seed is not _NO_SCAN_INITIAL:
        return seed
    return initial


def _composite_subscription(
    *subscriptions: PubSubCallbackSubscription,
) -> PubSubCallbackSubscription:
    def dispose() -> bool:
        disposed = False
        for subscription in tuple(subscriptions):
            disposed = subscription.dispose() or disposed
        return disposed

    return PubSubCallbackSubscription(dispose)


def _coerce_stream_schema(topic: str, value: type | None) -> _StreamSchema | None:
    if value is None:
        return None
    if isinstance(value, type):
        return _StreamSchema(
            name=_topic_schema_name(topic, value.__name__),
            field_types=_schema_field_types(value),
        )
    raise TypeError("schema must be a model class or None")


def _schema_field_types(model: type) -> dict[str, str]:
    model_fields = getattr(model, "model_fields", None)
    if isinstance(model_fields, Mapping):
        return {
            name: _field_type(getattr(field, "annotation", object))
            for name, field in model_fields.items()
        }
    legacy_fields = getattr(model, "__fields__", None)
    if isinstance(legacy_fields, Mapping):
        return {
            name: _field_type(getattr(field, "type_", object))
            for name, field in legacy_fields.items()
        }
    hints = get_type_hints(model)
    if not hints:
        raise ValueError("schema model must declare typed fields")
    return {name: _field_type(annotation) for name, annotation in hints.items()}


def _topic_schema_name(topic: str, schema_name: str) -> str:
    return f"{topic}.{schema_name}"


def _scope_stream_query(sql: str) -> str:
    statement = sql.strip()
    if not statement:
        raise ValueError("stream query must be non-empty")
    stream_cte = """
    stream AS (
        SELECT *
        FROM stream_messages
        WHERE topic = :__manyfold_stream_topic
          AND pad_name = :__manyfold_stream_topic
    )
    """
    lowered = statement.lower()
    if lowered.startswith("with recursive "):
        return f"WITH RECURSIVE {stream_cte}, {statement[15:].strip()}"
    if lowered.startswith("with "):
        return f"WITH {stream_cte}, {statement[5:].strip()}"
    return f"WITH {stream_cte} {statement}"


def _resolve_topic(topic: str | None) -> str:
    if topic is None:
        return f"manyfold.ephemeral.{_ephemeral_uuid5()}"
    return _require_text(topic, "topic")


def _resolve_topic_name(name: str | None) -> str:
    if name is None:
        return str(_ephemeral_uuid5())
    return _require_text(name, "topic name")


def _resolve_namespace(
    namespace: str,
    *,
    pubsub: str | None = None,
    fabric: str | None = None,
) -> str:
    aliases = tuple(alias for alias in (pubsub, fabric) if alias is not None)
    if len(aliases) > 1:
        raise ValueError("use only one of namespace, pubsub, or fabric")
    if aliases:
        namespace = aliases[0]
    return _require_text(namespace, "namespace")


def _ephemeral_uuid5() -> UUID:
    return uuid5(_EPHEMERAL_TOPIC_NAMESPACE, str(time_ns()))


def _resolve_worker_name(topic: str, worker_name: str | None) -> str:
    if worker_name is not None:
        return _require_text(worker_name, "worker_name")
    safe_topic = topic.replace("/", ".").replace(":", ".")
    return f"pubsub:{safe_topic}"


def _sql_alias_prefix(topic: str) -> str:
    alias = "".join(char if char == "_" or char.isalnum() else "_" for char in topic)
    alias = alias.strip("_")
    if not alias or alias[0].isdigit():
        alias = f"topic_{alias}"
    return alias


def _require_sql_identifier(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.isidentifier():
        raise ValueError(f"{field} must be a SQL identifier")


def _field_type(annotation: object) -> str:
    annotation = _unwrap_optional(annotation)
    if annotation in {float}:
        return "float64"
    if annotation in {int}:
        return "int64"
    if annotation in {str}:
        return "string"
    if annotation in {bool}:
        return "bool"
    raise ValueError(f"unsupported schema field type: {annotation!r}")


def _unwrap_optional(annotation: object) -> object:
    arguments = get_args(annotation)
    if not arguments:
        return annotation
    if get_origin(annotation) not in {Union, NoneType} and not _is_pep604_union(annotation):
        return annotation
    non_none = tuple(argument for argument in arguments if argument is not NoneType)
    if len(non_none) == 1 and len(non_none) != len(arguments):
        return non_none[0]
    return annotation


def _is_pep604_union(annotation: object) -> bool:
    return type(annotation).__name__ == "UnionType"


def _is_schema_value(value: object, schema: object) -> bool:
    if isinstance(schema, type):
        return isinstance(value, schema)
    return False


def _is_model_value(value: object) -> bool:
    value_type = type(value)
    return (
        is_dataclass(value)
        or isinstance(getattr(value_type, "model_fields", None), Mapping)
        or isinstance(getattr(value_type, "__fields__", None), Mapping)
        or bool(get_type_hints(value_type))
    )


def _value_fields(value: object) -> Mapping[str, object]:
    if is_dataclass(value):
        return {
            name: getattr(value, name)
            for name in getattr(value, "__dataclass_fields__")
        }
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, Mapping):
            return dumped
    dict_method = getattr(value, "dict", None)
    if callable(dict_method):
        dumped = dict_method()
        if isinstance(dumped, Mapping):
            return dumped
    annotations = get_type_hints(type(value))
    return {name: getattr(value, name) for name in annotations}


def _encode_flatbuffer(table: FlatBufferTable, values: Mapping[str, object]) -> bytes:
    fields = sorted(table.fields, key=lambda field: field.index)
    max_index = max(field.index for field in fields)
    object_bytes = bytearray(b"\x00\x00\x00\x00")
    field_offsets = [0] * (max_index + 1)
    string_patches: list[tuple[int, bytes]] = []

    for field in fields:
        if field.name not in values or values[field.name] is None:
            continue
        alignment = _flatbuffer_alignment(field.field_type)
        object_bytes.extend(
            b"\x00" * (_align(len(object_bytes), alignment) - len(object_bytes))
        )
        field_offset = len(object_bytes)
        field_offsets[field.index] = field_offset
        _append_flatbuffer_value(
            object_bytes,
            string_patches,
            field_offset,
            field.field_type,
            values[field.name],
        )

    vtable_length = 4 + 2 * len(field_offsets)
    table_position = 4 + vtable_length
    object_bytes[0:4] = struct.pack("<i", table_position - 4)
    vtable = bytearray(struct.pack("<HH", vtable_length, len(object_bytes)))
    for offset in field_offsets:
        vtable.extend(struct.pack("<H", offset))

    buffer = bytearray(struct.pack("<I", table_position))
    buffer.extend(vtable)
    buffer.extend(object_bytes)
    for field_offset, encoded in string_patches:
        field_position = table_position + field_offset
        string_position = len(buffer)
        buffer[field_position : field_position + 4] = struct.pack(
            "<I",
            string_position - field_position,
        )
        buffer.extend(struct.pack("<I", len(encoded)))
        buffer.extend(encoded)
        buffer.extend(b"\x00")
    return bytes(buffer)


def _append_flatbuffer_value(
    object_bytes: bytearray,
    string_patches: list[tuple[int, bytes]],
    field_offset: int,
    field_type: str,
    value: object,
) -> None:
    if field_type == "bool":
        object_bytes.extend(struct.pack("<?", bool(value)))
    elif field_type == "float32":
        object_bytes.extend(struct.pack("<f", float(value)))
    elif field_type == "float64":
        object_bytes.extend(struct.pack("<d", float(value)))
    elif field_type == "int32":
        object_bytes.extend(struct.pack("<i", int(value)))
    elif field_type == "int64":
        object_bytes.extend(struct.pack("<q", int(value)))
    elif field_type == "uint32":
        object_bytes.extend(struct.pack("<I", int(value)))
    elif field_type == "uint64":
        object_bytes.extend(struct.pack("<Q", int(value)))
    elif field_type == "string":
        object_bytes.extend(b"\x00\x00\x00\x00")
        string_patches.append((field_offset, str(value).encode("utf-8")))
    else:
        raise ValueError(f"unsupported FlatBuffer field type: {field_type!r}")


def _flatbuffer_alignment(field_type: str) -> int:
    if field_type in {"float64", "int64", "uint64"}:
        return 8
    if field_type in {"float32", "int32", "uint32", "string"}:
        return 4
    if field_type == "bool":
        return 1
    raise ValueError(f"unsupported FlatBuffer field type: {field_type!r}")


def _align(value: int, alignment: int) -> int:
    return value if value % alignment == 0 else value + alignment - value % alignment


def _payload_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    output = getattr(value, "Output", None)
    if callable(output):
        return bytes(output())
    table = getattr(value, "_tab", None)
    if table is not None and hasattr(table, "Bytes"):
        return bytes(table.Bytes)
    try:
        return bytes(value)  # type: ignore[arg-type]
    except TypeError as error:
        raise TypeError(
            "payload must be bytes, generated FlatBuffer output, or a schema value"
        ) from error


def _require_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


@dataclass(frozen=True)
class _StreamSchema:
    name: str
    field_types: Mapping[str, str]

    @property
    def table(self) -> FlatBufferTable:
        return FlatBufferTable(
            self.name,
            [
                FlatBufferField(field_name, index, field_type)
                for index, (field_name, field_type) in enumerate(self.field_types.items())
            ],
        )

    def encode(self, values: Mapping[str, object]) -> bytes:
        return _encode_flatbuffer(self.table, values)


__all__ = [
    "InMemoryPubSub",
    "PubSub",
    "PubSubCallbackSubscription",
    "PubSubDelivery",
    "PubSubFabric",
    "PubSubMessage",
    "PubSubObservable",
    "PubSubSchedule",
    "PubSubSubscription",
    "PubSubTopic",
    "ServiceDiscoveryRequirement",
    "StreamRow",
]
