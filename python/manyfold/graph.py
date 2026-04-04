"""High-level Python helpers matching the RFC examples.

The top-level :mod:`manyfold` package intentionally exposes only the narrow,
day-to-day API. This module contains those primary wrappers plus the more
specialized planning, query, transport, mesh, and security helpers added for
RFC checklist coverage.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from dataclasses import field
import json
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
from ._manyfold_rust import CreditSnapshot
from ._manyfold_rust import Graph as NativeGraph
from ._manyfold_rust import Mailbox as NativeMailbox
from ._manyfold_rust import MailboxDescriptor as NativeMailboxDescriptor
from ._manyfold_rust import NamespaceRef
from ._manyfold_rust import Plane
from ._manyfold_rust import PortDescriptor
from ._manyfold_rust import ProducerRef
from ._manyfold_rust import ReadablePort as NativeReadablePort
from ._manyfold_rust import RouteRef
from ._manyfold_rust import SchemaRef
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
ConnectableTarget = Union[RouteLike, NativeMailbox]
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


@dataclass(frozen=True)
class QueryResponse:
    """Typed query-plane response description."""

    command: str
    correlation_id: str
    items: tuple[str, ...]


@dataclass(frozen=True)
class DebugEvent:
    """High-level debug/audit event mirrored onto a debug route."""

    event_type: str
    detail: str
    route_display: str | None
    seq_source: int


@dataclass(frozen=True)
class QueryServiceRoutes:
    """The well-known request/response routes for one query service owner."""

    request: RouteRef
    response: RouteRef


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
        return source.subscribe(self.as_observer(producer=producer, control_epoch=control_epoch))


class _TrackedSubscription:
    def __init__(self, graph: Graph, route_ref: RouteRef, inner: SubscriptionLike) -> None:
        self._graph = graph
        self._route_ref = route_ref
        self._inner = inner
        self._disposed = False

    def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        self._graph._subscriber_count[self._graph._route_key(self._route_ref)] -= 1
        self._inner.dispose()


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
    - connect(source, sink)
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
        self._join_plans: dict[str, JoinPlan] = {}
        self._middlewares: list[Middleware] = []
        self._links: dict[str, Link] = {}
        self._mesh_primitives: dict[str, MeshPrimitive] = {}
        self._query_services: dict[str, QueryServiceRoutes] = {}
        self._debug_routes: dict[str, RouteRef] = {}
        self._audit_events: list[DebugEvent] = []
        self._capability_grants: dict[tuple[str, str], CapabilityGrant] = {}
        self._route_visibility: dict[str, str] = {}
        self._query_sequence = 0

    def _coerce_route_ref(self, route_ref: RouteLike) -> RouteRef:
        if isinstance(route_ref, TypedRoute):
            return route_ref.route_ref
        return route_ref

    def _connectable_key(self, target: ConnectableTarget, *, edge_role: str) -> str:
        if isinstance(target, NativeMailbox):
            if edge_role == "source":
                return target.egress.describe().route_display
            return target.ingress.describe().route_display
        return self._route_key(target)

    def _route_key(self, route_ref: RouteLike) -> str:
        return self._coerce_route_ref(route_ref).display()

    def _subject_for(self, route_ref: RouteLike) -> Subject[ClosedEnvelope]:
        key = self._route_key(route_ref)
        if key not in self._subjects:
            self._subjects[key] = Subject()
        return self._subjects[key]

    def _record_envelope(
        self,
        route_ref: RouteLike,
        envelope: ClosedEnvelope,
        *,
        producer_id: str | None = None,
    ) -> ClosedEnvelope:
        """Persist envelope-derived bookkeeping before notifying observers."""
        key = self._route_key(route_ref)
        self._history.setdefault(key, []).append(envelope)
        if producer_id is not None:
            self._writers.setdefault(key, set()).add(producer_id)
        self._publish(route_ref, envelope)
        return envelope

    def _publish(self, route_ref: RouteLike, envelope: ClosedEnvelope) -> ClosedEnvelope:
        self._subject_for(route_ref).on_next(envelope)
        return envelope

    def _decode_envelope(self, route_ref: TypedRoute[T], envelope: ClosedEnvelope) -> TypedEnvelope[T]:
        payload = self._payload_bytes(route_ref, envelope)
        return TypedEnvelope(route=route_ref, closed=envelope, value=route_ref.schema.decode(payload))

    def _payload_bytes(self, route_ref: TypedRoute[T], envelope: ClosedEnvelope) -> bytes:
        # Some runtimes keep ClosedEnvelope metadata separate from payload bytes.
        inline_payload = bytes(envelope.payload_ref.inline_bytes)
        if inline_payload:
            return inline_payload
        opened = tuple(self._read_port(route_ref).open())
        if not opened:
            return b""
        return bytes(opened[-1].payload)

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

    def _emit_debug_event(self, event_type: str, detail: str, route_ref: RouteLike | None = None) -> DebugEvent:
        """Emit a debug event on both the in-memory audit log and a debug route."""
        debug_route = self._debug_route(event_type)
        payload = json.dumps(
            {
                "event_type": event_type,
                "detail": detail,
                "route_display": None if route_ref is None else self._route_key(route_ref),
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

    def _authorize(self, principal_id: str | None, route_ref: RouteLike | None, capability: str) -> None:
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
            if capability == "metadata_read" and self._route_visibility.get(key[1]) == "exported":
                return
            raise PermissionError(f"{principal_id} lacks {capability} capability for {key[1]}")
        if not getattr(grant, capability):
            raise PermissionError(f"{principal_id} lacks {capability} capability for {key[1]}")

    def _execute_query(self, request: QueryRequest) -> tuple[str, ...]:
        """Resolve query commands against the current in-memory graph state."""
        command = request.command.lower()
        route_ref = None if request.route is None else self._coerce_route_ref(request.route)
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
            return tuple(str(envelope.seq_source) for envelope in self.replay(route_ref))
        if command == "trace":
            return tuple(
                f"{event.event_type}:{event.detail}"
                for event in self.audit(route_ref)
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
            latest = self.latest(route_ref)
            if latest is None:
                return ()
            return (bytes(latest.payload_ref.inline_bytes).decode("utf-8", errors="replace"),)
        if command == "audit":
            self._authorize(request.principal_id, route_ref, "debug_read")
            return tuple(
                f"{event.event_type}:{event.detail}"
                for event in self.audit(route_ref)
            )
        raise ValueError(f"unsupported query command: {request.command}")

    def register_port(self, route_ref: RouteLike) -> RouteRef:
        """Register a route in the native graph and return its concrete ref."""
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
        """Observe route updates as an Rx stream.

        Typed routes decode payloads before delivery; raw route refs expose the
        underlying closed envelopes.
        """
        native_route = self._coerce_route_ref(route_ref)

        def subscribe(
            observer: ObserverLike[Any],
            scheduler: object | None = None,
        ) -> SubscriptionLike:
            key = self._route_key(native_route)
            self._subscriber_count[key] = self._subscriber_count.get(key, 0) + 1
            if replay_latest:
                latest = self.latest(route_ref)
                if latest is not None:
                    observer.on_next(latest)

            if isinstance(route_ref, TypedRoute):
                # Typed observers see decoded values, but the graph internally
                # continues to fan out closed envelopes on one shared subject.
                class _Observer:
                    def on_next(_, envelope) -> None:
                        observer.on_next(self._decode_envelope(route_ref, envelope))

                    def on_error(_, error: Exception) -> None:
                        observer.on_error(error)

                    def on_completed(_) -> None:
                        observer.on_completed()

                inner = self._subject_for(native_route).subscribe(_Observer(), scheduler=scheduler)
            else:
                inner = self._subject_for(native_route).subscribe(observer, scheduler=scheduler)
            return _TrackedSubscription(self, native_route, inner)

        return rx.create(subscribe)

    def pipe(
        self,
        source: ObservableLike[TIn] | ObservableLike[bytes],
        target: TypedRoute[TIn] | RouteRef,
        *,
        producer: ProducerRef | None = None,
        control_epoch: int | None = None,
    ) -> SubscriptionLike:
        """Bind an observable source into a route."""
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
        """Write one payload to a route and return the resulting envelope."""
        if isinstance(target, WriteBinding):
            self._graph.register_binding(target.request.display(), target)
            emitted = self._emit_native(target.request, bytes(payload), producer=producer, control_epoch=control_epoch)
            for envelope in emitted:
                producer_id = "python" if producer is None else producer.producer_id
                self._record_envelope(envelope.route, envelope, producer_id=producer_id)
            self._emit_debug_event("write", f"published {target.request.display()}", target.request)
            return emitted[0]
        if isinstance(target, TypedRoute):
            encoded = target.schema.encode(payload)
            emitted = self._emit_native(target.route_ref, encoded, producer=producer, control_epoch=control_epoch)
            envelope = emitted[0]
            producer_id = "python" if producer is None else producer.producer_id
            for emitted_envelope in emitted:
                self._record_envelope(emitted_envelope.route, emitted_envelope, producer_id=producer_id)
            self._emit_debug_event("write", f"published {target.display()}", target)
            return self._decode_envelope(target, envelope)
        emitted = self._emit_native(self._coerce_route_ref(target), payload, producer=producer, control_epoch=control_epoch)
        envelope = emitted[0]
        producer_id = "python" if producer is None else producer.producer_id
        for emitted_envelope in emitted:
            self._record_envelope(emitted_envelope.route, emitted_envelope, producer_id=producer_id)
        self._emit_debug_event("write", f"published {self._route_key(target)}", target)
        return envelope

    def mailbox(
        self,
        name: str,
        descriptor: NativeMailboxDescriptor | None = None,
    ) -> NativeMailbox:
        """Create or return a named mailbox from the native graph."""
        return self._graph.mailbox(name, descriptor)

    def connect(self, source: ConnectableTarget, sink: ConnectableTarget) -> None:
        """Connect two routes in topology metadata."""
        self._graph.connect(source, sink)
        self._emit_debug_event(
            "topology",
            f"connected {self._connectable_key(source, edge_role='source')} -> {self._connectable_key(sink, edge_role='sink')}",
            None if isinstance(source, NativeMailbox) else self._coerce_route_ref(source),
        )

    def install(self, control_loop: NativeControlLoop | ReadThenWriteNextEpochStep[Any, Any]) -> None:
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
        self._emit_debug_event("scheduler", f"ticked control loop {name}", envelope.route)
        return envelope

    def catalog(self) -> Iterator[RouteRef]:
        """Return all registered routes."""
        return iter(tuple(self._graph.catalog()))

    def describe_route(self, route_ref: RouteLike) -> PortDescriptor:
        """Return the descriptor for one route."""
        return self._graph.describe_route(self._coerce_route_ref(route_ref))

    @overload
    def latest(self, route_ref: TypedRoute[T]) -> TypedEnvelope[T] | None: ...

    @overload
    def latest(self, route_ref: RouteRef) -> ClosedEnvelope | None: ...

    def latest(self, route_ref: RouteLike) -> TypedEnvelope[Any] | ClosedEnvelope | None:
        """Return the latest envelope seen for one route."""
        latest = self._graph.latest(self._coerce_route_ref(route_ref))
        if latest is None:
            return None
        if isinstance(route_ref, TypedRoute):
            return self._decode_envelope(route_ref, latest)
        return latest

    def topology(self) -> Iterator[tuple[str, str]]:
        """Return graph edges as `(source, sink)` display pairs."""
        return iter(tuple(self._graph.topology()))

    def validate_graph(self) -> Iterator[str]:
        """Return graph validation issues detected by the native layer."""
        return iter(tuple(self._graph.validate_graph()))

    def credit_snapshot(self) -> Iterator[CreditSnapshot]:
        """Expose the current credit/backpressure view for registered routes."""
        return iter(tuple(self._graph.credit_snapshot()))

    def replay(self, route_ref: RouteLike) -> Iterator[ClosedEnvelope]:
        """Return the retained in-memory history for one route."""
        return iter(tuple(self._history.get(self._route_key(route_ref), ())))

    def subscribers(self, route_ref: RouteLike) -> int:
        """Return the number of active observers on a route."""
        return self._subscriber_count.get(self._route_key(route_ref), 0)

    def writers(self, route_ref: RouteLike) -> Iterator[str]:
        """Return distinct producer ids that have written to a route."""
        return iter(tuple(sorted(self._writers.get(self._route_key(route_ref), ()))))

    def export_route(self, route_ref: RouteLike, *, visibility: str = "exported") -> None:
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
        self._capability_grants[(normalized.principal_id, route_ref.display())] = normalized
        return normalized

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
            value = item.value if isinstance(item, TypedEnvelope) else cast(TIn, bytes(item.payload_ref.inline_bytes))
            state, out = step(state, value)
            self.publish(output, out)

        return self.observe(source).subscribe(on_next)  # type: ignore[arg-type]

    def window(
        self,
        source: TypedRoute[T] | RouteRef,
        *,
        size: int,
    ) -> Observable[list[T] | list[bytes]]:
        """Emit the most recent `size` values on each source update."""
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
        """Combine each incoming side with the latest value from the other side."""
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
        """Mirror source updates into a state route owned by the topology."""
        def on_next(item: TypedEnvelope[T] | ClosedEnvelope) -> None:
            if isinstance(state_route, TypedRoute):
                value = item.value if isinstance(item, TypedEnvelope) else state_route.schema.decode(bytes(item.payload_ref.inline_bytes))
                self.publish(state_route, value)
            else:
                self.publish(state_route, bytes(item.payload_ref.inline_bytes))

        return self.observe(source).subscribe(on_next)  # type: ignore[arg-type]

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
                self.connect(left_route, left_repartition)
                self.connect(right_route, right_repartition)
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
            self.connect(left_route, left_repartition)
            self.connect(right_route, right_repartition)
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
            raise ValueError("join partition keys are incompatible without repartition or broadcast")
        self._join_plans[name] = plan
        self._emit_debug_event("join", f"planned {plan.join_class} join {name}", left_route)
        return plan

    def explain_join(self, name: str) -> JoinPlan:
        """Return a previously planned join."""
        return self._join_plans[name]

    def add_middleware(self, middleware: Middleware) -> Middleware:
        """Register middleware after enforcing RFC preservation rules."""
        if not middleware.preserves_envelope_identity:
            raise ValueError("middleware must preserve envelope identity unless explicitly reframing")
        if not middleware.updates_taints:
            raise ValueError("middleware must preserve or update taints")
        if not middleware.updates_causality:
            raise ValueError("middleware must preserve or update causality")
        self._middlewares.append(middleware)
        self._emit_debug_event("middleware", f"attached {middleware.kind} middleware {middleware.name}")
        return middleware

    def middleware(self) -> Iterator[Middleware]:
        """Iterate over registered middleware."""
        return iter(tuple(self._middlewares))

    def register_link(self, link: Link) -> Link:
        """Register a transport/link adapter."""
        self._links[link.name] = link
        self._emit_debug_event("link_health", f"registered {link.link_class} link {link.name}")
        return link

    def links(self) -> Iterator[Link]:
        """Iterate over registered links."""
        return iter(tuple(self._links.values()))

    def add_mesh_primitive(self, primitive: MeshPrimitive) -> MeshPrimitive:
        """Register an explicit mesh primitive in topology metadata."""
        if primitive.kind in {"bridge", "mirror", "replicate"} and primitive.link_name is None:
            raise ValueError(f"{primitive.kind} requires a link")
        if primitive.link_name is not None and primitive.link_name not in self._links:
            raise KeyError(f"unknown link {primitive.link_name}")
        normalized = MeshPrimitive(
            name=primitive.name,
            kind=primitive.kind,
            sources=tuple(self._coerce_route_ref(route) for route in primitive.sources),
            destinations=tuple(self._coerce_route_ref(route) for route in primitive.destinations),
            link_name=primitive.link_name,
            ordering_policy=primitive.ordering_policy,
            state_budget=primitive.state_budget,
            threshold=primitive.threshold,
            ack_policy=primitive.ack_policy,
        )
        for source in normalized.sources:
            self.register_port(source)
        for destination in normalized.destinations:
            self.register_port(destination)
        for source in normalized.sources:
            for destination in normalized.destinations:
                self.connect(source, destination)
        self._mesh_primitives[normalized.name] = normalized
        self._emit_debug_event("topology", f"registered mesh primitive {normalized.kind}:{normalized.name}")
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
            self._query_services[owner] = QueryServiceRoutes(request=request, response=response)
        return self._query_services[owner]

    def query(self, request: QueryRequest, *, requester_id: str = "python", service_owner: str = "query") -> QueryResponse:
        """Execute a typed query through the query-plane stream model."""
        service = self.query_service(service_owner)
        correlation_id = request.correlation_id or self._correlation_id()
        request_payload = json.dumps(
            {
                "command": request.command,
                "route": None if request.route is None else self._route_key(request.route),
                "join_name": request.join_name,
                "principal_id": request.principal_id or requester_id,
                "correlation_id": correlation_id,
            },
            sort_keys=True,
        ).encode()
        request_envelope = self._graph.writable_port(service.request).write(request_payload)
        self._record_envelope(service.request, request_envelope, producer_id=requester_id)
        items = self._execute_query(
            QueryRequest(
                command=request.command,
                route=request.route,
                join_name=request.join_name,
                principal_id=request.principal_id or requester_id,
                correlation_id=correlation_id,
            )
        )
        response = QueryResponse(command=request.command, correlation_id=correlation_id, items=items)
        response_payload = json.dumps(
            {
                "command": response.command,
                "correlation_id": response.correlation_id,
                "items": list(response.items),
            },
            sort_keys=True,
        ).encode()
        response_envelope = self._graph.writable_port(service.response).write(response_payload)
        self._record_envelope(service.response, response_envelope, producer_id="query_service")
        self._emit_debug_event("audit", f"query {request.command} handled", service.response)
        return response

    def debug_routes(self) -> Iterator[RouteRef]:
        """Return the well-known debug routes created so far."""
        return iter(tuple(self._debug_routes.values()))

    def audit(self, route_ref: RouteLike | None = None) -> Iterator[DebugEvent]:
        """Return retained audit/debug events, optionally filtered by route."""
        route_display = None if route_ref is None else self._route_key(route_ref)
        return iter(tuple(event for event in self._audit_events if route_display is None or event.route_display == route_display))
