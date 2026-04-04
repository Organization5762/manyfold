"""High-level Python helpers matching the RFC examples.

This module intentionally keeps the public API narrow and typed. Most helpers are
semi-private so the user-facing surface can stay centered on `Graph`,
`WriteBindings`, and `ControlLoops`.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import cast
from typing import Any
from typing import Callable
from typing import Generic
from typing import Iterator
from typing import Protocol
from typing import Sequence
from typing import TypeVar
from typing import Union
from typing import overload
from typing import runtime_checkable

import reactivex as rx
from reactivex import Observable
from reactivex.subject import Subject

from ._manyfold_rust import ControlLoop as NativeControlLoop
from ._manyfold_rust import ClosedEnvelope
from ._manyfold_rust import Graph as NativeGraph
from ._manyfold_rust import CreditSnapshot
from ._manyfold_rust import Mailbox as NativeMailbox
from ._manyfold_rust import MailboxDescriptor as NativeMailboxDescriptor
from ._manyfold_rust import Plane
from ._manyfold_rust import PortDescriptor
from ._manyfold_rust import ProducerRef
from ._manyfold_rust import ReadablePort as NativeReadablePort
from ._manyfold_rust import RouteRef
from ._manyfold_rust import Variant
from ._manyfold_rust import WritablePort as NativeWritablePort
from ._manyfold_rust import WriteBinding
from ._manyfold_rust import Layer
from .primitives import OwnerName
from .primitives import ReadThenWriteNextEpochStep
from .primitives import Schema
from .primitives import StreamFamily
from .primitives import StreamName
from .primitives import TypedEnvelope
from .primitives import TypedRoute
from .primitives import route

T = TypeVar("T")
TIn = TypeVar("TIn")
TOut = TypeVar("TOut")
AnyTypedRoute = TypedRoute[Any]
RouteLike = Union[AnyTypedRoute, RouteRef]
WriteTarget = Union[WriteBinding, RouteLike]
EnvelopeIterator = Iterator[ClosedEnvelope]
StateT = TypeVar("StateT")
TRight = TypeVar("TRight")


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
        scheduler: object | None = None,
    ) -> SubscriptionLike: ...


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
    def counter_accumulate(*, read_state: TypedRoute[Any], write_request: TypedRoute[Any]) -> NativeControlLoop:
        return ControlLoops.with_routes(
            "CounterAccumulate",
            read_routes=[read_state],
            write_request=write_request,
        )


class ReactiveReadablePort:
    """Readable port facade with a live Rx stream for route updates."""

    def __init__(self, graph: Graph, route_ref: RouteRef, native: NativeReadablePort) -> None:
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
            return self._graph._subject_for(self._route_ref).subscribe(observer, scheduler=scheduler)

        return rx.create(subscribe)


class ReactiveWritablePort:
    """Writable port facade that can act as an Rx observer."""

    def __init__(self, graph: Graph, route_ref: RouteRef, native: NativeWritablePort) -> None:
        self._graph = graph
        self._route_ref = route_ref
        self._native = native

    def write(
        self,
        payload: bytes,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> ClosedEnvelope:
        envelope = self._native.write(payload, producer=producer, control_epoch=control_epoch)
        self._graph._publish(self._route_ref, envelope)
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
        return source.subscribe(self.as_observer(producer=producer, control_epoch=control_epoch))


class Graph:
    """Narrow Python-facing Manyfold API.

    Public methods stay focused on route construction, observation, publishing,
    and installing simple control primitives. Lower-level coercion, decoding,
    and subject management remain semi-private to avoid turning the top-level
    API into a mirror of the native bridge.
    """

    def __init__(self) -> None:
        self._graph = NativeGraph()
        self._subjects: dict[str, Subject[ClosedEnvelope]] = {}
        self._subscriptions: deque[SubscriptionLike] = deque()

    def _coerce_route_ref(self, route_ref: RouteLike) -> RouteRef:
        if isinstance(route_ref, TypedRoute):
            return route_ref.route_ref
        return route_ref

    def _route_key(self, route_ref: RouteLike) -> str:
        return self._coerce_route_ref(route_ref).display()

    def _subject_for(self, route_ref: RouteLike) -> Subject[ClosedEnvelope]:
        key = self._route_key(route_ref)
        if key not in self._subjects:
            self._subjects[key] = Subject()
        return self._subjects[key]

    def _publish(self, route_ref: RouteLike, envelope: ClosedEnvelope) -> ClosedEnvelope:
        self._subject_for(route_ref).on_next(envelope)
        return envelope

    def _payload_bytes(self, route_ref: TypedRoute[T], envelope: ClosedEnvelope) -> bytes:
        # Closed envelopes are metadata-only when the native runtime chooses lazy
        # payload storage, so typed reads reopen the payload only when decode is
        # actually needed.
        inline_payload = bytes(envelope.payload_ref.inline_bytes)
        if inline_payload:
            return inline_payload
        opened = tuple(self._read_port(route_ref).open())
        if not opened:
            return b""
        return bytes(opened[-1].payload)

    def _decode_envelope(self, route_ref: TypedRoute[T], envelope: ClosedEnvelope) -> TypedEnvelope[T]:
        payload = self._payload_bytes(route_ref, envelope)
        return TypedEnvelope(route=route_ref, closed=envelope, value=route_ref.schema.decode(payload))

    def register_port(self, route_ref: RouteLike) -> RouteRef:
        """Register a route and return the canonical native route reference."""
        return self._graph.register_port(self._coerce_route_ref(route_ref))

    def _read_port(self, route_ref: RouteLike) -> ReactiveReadablePort:
        native_route = self._coerce_route_ref(route_ref)
        return ReactiveReadablePort(self, native_route, self._graph.read(native_route))

    def _write_port(self, target: WriteTarget) -> WriteBinding | ReactiveWritablePort:
        if isinstance(target, WriteBinding):
            return self._graph.register_binding(target.request.display(), target)
        native_route = self._coerce_route_ref(target)
        return ReactiveWritablePort(self, native_route, self._graph.writable_port(native_route))

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
                self._graph.emit(route_ref, payload, producer=producer, control_epoch=control_epoch),
            )
        envelope = self._graph.writable_port(route_ref).write(
            payload,
            producer=producer,
            control_epoch=control_epoch,
        )
        return [envelope]

    @overload
    def observe(self, route_ref: TypedRoute[T], *, replay_latest: bool = True) -> Observable[TypedEnvelope[T]]: ...

    @overload
    def observe(self, route_ref: RouteRef, *, replay_latest: bool = True) -> Observable[ClosedEnvelope]: ...

    def observe(self, route_ref: RouteLike, *, replay_latest: bool = True) -> Observable[Any]:
        """Observe a route as raw envelopes or decoded typed envelopes."""
        if isinstance(route_ref, TypedRoute):
            def subscribe(
                observer: ObserverLike[TypedEnvelope[T]],
                scheduler: object | None = None,
            ) -> SubscriptionLike:
                if replay_latest:
                    latest = self.latest(route_ref)
                    if latest is not None:
                        observer.on_next(latest)

                class _Observer:
                    def on_next(_, envelope) -> None:
                        observer.on_next(self._decode_envelope(route_ref, envelope))

                    def on_error(_, error: Exception) -> None:
                        observer.on_error(error)

                    def on_completed(_) -> None:
                        observer.on_completed()

                return self._subject_for(route_ref).subscribe(_Observer(), scheduler=scheduler)

            return rx.create(subscribe)

        return self._read_port(route_ref).observe(replay_latest=replay_latest)

    def pipe(
        self,
        source: ObservableLike[TIn] | ObservableLike[bytes],
        target: TypedRoute[TIn] | RouteRef,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> SubscriptionLike:
        """Bind an observable source into a typed route or native writable port."""
        if isinstance(target, TypedRoute):
            class _Observer:
                def on_next(_, value: TIn) -> None:
                    self.publish(target, value, producer=producer, control_epoch=control_epoch)

                def on_error(_, error: Exception) -> None:
                    raise error

                def on_completed(_) -> None:
                    return None

            return source.subscribe(_Observer())
        return self._write_port(target).bind(source, producer=producer, control_epoch=control_epoch)  # type: ignore[union-attr]

    @overload
    def publish(
        self,
        target: TypedRoute[T],
        payload: T,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> TypedEnvelope[T]: ...

    @overload
    def publish(
        self,
        target: RouteRef,
        payload: bytes,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> ClosedEnvelope: ...

    def publish(
        self,
        target: WriteTarget,
        payload: Any,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> TypedEnvelope[Any] | ClosedEnvelope:
        """Publish either typed payloads or raw bytes, depending on the target."""

        if isinstance(target, WriteBinding):
            self._graph.register_binding(target.request.display(), target)
            emitted = self._emit_native(target.request, bytes(payload), producer=producer, control_epoch=control_epoch)
            for envelope in emitted:
                self._publish(envelope.route, envelope)
            return emitted[0]
        if isinstance(target, TypedRoute):
            encoded = target.schema.encode(payload)
            emitted = self._emit_native(target.route_ref, encoded, producer=producer, control_epoch=control_epoch)
            envelope = emitted[0]
            for emitted in emitted[1:]:
                self._publish(emitted.route, emitted)
            self._publish(target.route_ref, envelope)
            return self._decode_envelope(target, envelope)
        emitted = self._emit_native(self._coerce_route_ref(target), payload, producer=producer, control_epoch=control_epoch)
        for envelope in emitted:
            self._publish(envelope.route, envelope)
        return emitted[0]

    def mailbox(
        self,
        name: str,
        descriptor: NativeMailboxDescriptor | None = None,
    ) -> NativeMailbox:
        return self._graph.mailbox(name, descriptor)

    def connect(self, source: RouteLike, sink: RouteLike) -> None:
        """Connect two registered routes through the native graph."""
        self._graph.connect(self._coerce_route_ref(source), self._coerce_route_ref(sink))

    def install(self, control_loop: NativeControlLoop | ReadThenWriteNextEpochStep[Any, Any]) -> None:
        """Install either a native control loop or a typed shared-stream step."""
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
        """Advance one installed control loop epoch and publish the emitted write."""
        envelope = self._tick_control_loop(name)
        return self._publish(envelope.route, envelope)

    def catalog(self) -> Iterator[RouteRef]:
        return iter(tuple(self._graph.catalog()))

    def describe_route(self, route_ref: RouteLike) -> PortDescriptor:
        return self._graph.describe_route(self._coerce_route_ref(route_ref))

    @overload
    def latest(self, route_ref: TypedRoute[T]) -> TypedEnvelope[T] | None: ...

    @overload
    def latest(self, route_ref: RouteRef) -> ClosedEnvelope | None: ...

    def latest(self, route_ref: RouteLike) -> TypedEnvelope[Any] | ClosedEnvelope | None:
        """Return the latest route value, decoding through the route schema when typed."""
        latest = self._graph.latest(self._coerce_route_ref(route_ref))
        if latest is None:
            return None
        if isinstance(route_ref, TypedRoute):
            return self._decode_envelope(route_ref, latest)
        return latest

    def topology(self) -> Iterator[tuple[str, str]]:
        return iter(tuple(self._graph.topology()))

    def validate_graph(self) -> Iterator[str]:
        return iter(tuple(self._graph.validate_graph()))

    def credit_snapshot(self) -> Iterator[CreditSnapshot]:
        """Expose the current credit/backpressure view for registered routes."""

        return iter(tuple(self._graph.credit_snapshot()))

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

        def on_next(item: TypedEnvelope[TIn] | ClosedEnvelope) -> None:
            nonlocal state
            value = item.value if isinstance(item, TypedEnvelope) else bytes(item.payload_ref.inline_bytes)
            state, out = step(state, cast(TIn, value))
            self.publish(output, out)

        return self.observe(source).subscribe(on_next)  # type: ignore[arg-type]

    def window(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        size: int,
    ) -> Observable[list[T] | list[bytes]]:
        """Emit the most recent `size` values each time the source produces a new item."""

        buffer: deque[T | bytes] = deque(maxlen=size)

        def subscribe(
            observer: ObserverLike[list[T] | list[bytes]],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            def on_next(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
                value = item.value if isinstance(item, TypedEnvelope) else bytes(item.payload_ref.inline_bytes)
                buffer.append(value)
                observer.on_next(list(buffer))

            return self.observe(source, replay_latest=False).subscribe(on_next, scheduler=scheduler)  # type: ignore[arg-type]

        return rx.create(subscribe)

    def join_latest(
        self,
        left: TypedRoute[TIn] | RouteRef,
        right: TypedRoute[TRight] | RouteRef,
        *,
        combine: Callable[[TIn, TRight], TOut],
    ) -> Observable[TOut]:
        """Combine each side with the latest value seen on the other side."""

        left_latest: TIn | None = None
        right_latest: TRight | None = None

        def subscribe(
            observer: ObserverLike[TOut],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            def on_left(item: TypedEnvelope[TIn] | ClosedEnvelope) -> None:
                nonlocal left_latest
                left_latest = item.value if isinstance(item, TypedEnvelope) else cast(TIn, bytes(item.payload_ref.inline_bytes))
                if right_latest is not None:
                    observer.on_next(combine(left_latest, right_latest))

            def on_right(item: TypedEnvelope[TRight] | ClosedEnvelope) -> None:
                nonlocal right_latest
                right_latest = item.value if isinstance(item, TypedEnvelope) else cast(TRight, bytes(item.payload_ref.inline_bytes))
                if left_latest is not None:
                    observer.on_next(combine(left_latest, right_latest))

            left_sub = self.observe(left, replay_latest=False).subscribe(on_left, scheduler=scheduler)  # type: ignore[arg-type]
            right_sub = self.observe(right, replay_latest=False).subscribe(on_right, scheduler=scheduler)  # type: ignore[arg-type]

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
        """Mirror stream updates into a state route without mutating that route out of band."""

        def on_next(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
            if isinstance(state_route, TypedRoute):
                value = item.value if isinstance(item, TypedEnvelope) else state_route.schema.decode(bytes(item.payload_ref.inline_bytes))
                self.publish(state_route, value)
            else:
                payload = bytes(item.payload_ref.inline_bytes)
                self.publish(state_route, payload)

        return self.observe(source).subscribe(on_next)  # type: ignore[arg-type]
