"""Primary Manyfold nouns and verbs.

The goal in this module is a grokkable, typed surface: small value objects for
route construction and a minimal set of composition primitives that are useful
enough to deserve first-class status.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generic,
    Protocol,
    TypeVar,
    cast,
    overload,
    runtime_checkable,
)

from ._manyfold_rust import (
    ClosedEnvelope,
    Layer,
    NamespaceRef,
    Plane,
    RouteRef,
    SchemaRef,
    Variant,
)
from ._rx import Observable, operators as ops

T = TypeVar("T")
TRead = TypeVar("TRead")
TWrite = TypeVar("TWrite")
TProto = TypeVar("TProto", bound="ProtobufMessage")


@runtime_checkable
class ProtobufMessage(Protocol):
    def SerializeToString(self) -> bytes: ...


@runtime_checkable
class ProtobufMessageType(Protocol[TProto]):
    __name__: str

    @staticmethod
    def FromString(payload: bytes) -> TProto: ...


@runtime_checkable
class SubscriptionLike(Protocol):
    def dispose(self) -> None: ...


@dataclass(frozen=True)
class OwnerName:
    """Typed owner identifier used when building a namespace."""

    value: str


@dataclass(frozen=True)
class StreamFamily:
    """Typed stream family segment."""

    value: str


@dataclass(frozen=True)
class StreamName:
    """Typed stream name segment."""

    value: str


@dataclass(frozen=True)
class RouteNamespace:
    """Typed namespace wrapper for the route plane/layer pair."""

    plane: Plane
    layer: Layer


@dataclass(frozen=True)
class RouteIdentity:
    """Typed wrapper for the human-owned parts of a route identity."""

    owner: OwnerName
    family: StreamFamily
    stream: StreamName
    variant: Variant

    @classmethod
    def of(
        cls,
        *,
        owner: str,
        family: str,
        stream: str,
        variant: Variant,
    ) -> RouteIdentity:
        return cls(
            owner=OwnerName(owner),
            family=StreamFamily(family),
            stream=StreamName(stream),
            variant=variant,
        )


@dataclass(frozen=True)
class Schema(Generic[T]):
    """Encode/decode contract for a typed route payload."""

    schema_id: str
    version: int
    encode: Callable[[T], bytes]
    decode: Callable[[bytes], T]

    @classmethod
    def bytes(cls, schema_id: str, version: int = 1) -> Schema[bytes]:
        return cls(
            schema_id=schema_id,
            version=version,
            encode=bytes,
            decode=bytes,
        )

    @classmethod
    def protobuf(
        cls,
        message_type: ProtobufMessageType[TProto],
        schema_id: str | None = None,
        version: int = 1,
    ) -> Schema[TProto]:
        schema_name = schema_id or message_type.__name__
        return cls(
            schema_id=schema_name,
            version=version,
            encode=lambda value: value.SerializeToString(),
            decode=lambda payload: message_type.FromString(payload),
        )


@dataclass(frozen=True)
class TypedRoute(Generic[T]):
    """Fully typed route description used by the ergonomic Python API."""

    plane: Plane
    layer: Layer
    owner: OwnerName
    family: StreamFamily
    stream: StreamName
    variant: Variant
    schema: Schema[T]

    @property
    def route_ref(self) -> RouteRef:
        """Materialize the native route reference only when needed."""
        return RouteRef(
            NamespaceRef(plane=self.plane, layer=self.layer, owner=self.owner.value),
            family=self.family.value,
            stream=self.stream.value,
            variant=self.variant,
            schema=SchemaRef(
                schema_id=self.schema.schema_id, version=self.schema.version
            ),
        )

    def display(self) -> str:
        return self.route_ref.display()


@dataclass(frozen=True)
class Source(Generic[T]):
    """Signal source role for a route.

    Sources represent latest-value signal potential by default. The wrapper is
    intentionally lightweight: graph operations unwrap it to the underlying
    typed route or native route ref.
    """

    route: TypedRoute[T] | RouteRef
    replay_latest: bool = True

    def display(self) -> str:
        return self.route.display()


@dataclass(frozen=True)
class Sink(Generic[T]):
    """Signal sink role for a route.

    Sinks consume signal and make downstream demand explicit without adding a
    separate runtime node.
    """

    route: TypedRoute[T] | RouteRef

    def display(self) -> str:
        return self.route.display()


@dataclass(frozen=True)
class TypedEnvelope(Generic[T]):
    """Decoded view of a closed envelope plus its typed payload value."""

    route: TypedRoute[T]
    closed: ClosedEnvelope
    value: T


def source(route: TypedRoute[T] | RouteRef, *, replay_latest: bool = True) -> Source[T]:
    """Mark a route as a signal source."""
    return Source(route=route, replay_latest=replay_latest)


def sink(route: TypedRoute[T] | RouteRef) -> Sink[T]:
    """Mark a route as a signal sink."""
    return Sink(route=route)


SchemaLike = Any


def _coerce_schema(
    schema: SchemaLike[T],
    *,
    schema_id: str | None = None,
    version: int | None = None,
) -> Schema[T]:
    if isinstance(schema, Schema):
        resolved_schema_id = schema.schema_id if schema_id is None else schema_id
        resolved_version = schema.version if version is None else version
        if (
            resolved_schema_id != schema.schema_id
            or resolved_version != schema.version
        ):
            return Schema(
                schema_id=resolved_schema_id,
                version=resolved_version,
                encode=schema.encode,
                decode=schema.decode,
            )
        return schema
    if schema is bytes:
        if schema_id is None:
            raise ValueError("schema_id is required when schema=bytes")
        return cast(
            Schema[T],
            Schema.bytes(
                schema_id=schema_id, version=1 if version is None else version
            ),
        )
    return cast(
        Schema[T],
        Schema.protobuf(
            cast(ProtobufMessageType[TProto], schema),
            schema_id=schema_id,
            version=1 if version is None else version,
        ),
    )


@overload
def route(
    *,
    plane: Plane,
    layer: Layer,
    owner: OwnerName,
    family: StreamFamily,
    stream: StreamName,
    variant: Variant,
    schema: SchemaLike[T],
    schema_id: str | None = None,
    version: int | None = None,
) -> TypedRoute[T]: ...


@overload
def route(
    *,
    namespace: RouteNamespace,
    identity: RouteIdentity,
    schema: SchemaLike[T],
    schema_id: str | None = None,
    version: int | None = None,
) -> TypedRoute[T]: ...


def route(
    *,
    plane: Plane | None = None,
    layer: Layer | None = None,
    owner: OwnerName | None = None,
    family: StreamFamily | None = None,
    stream: StreamName | None = None,
    variant: Variant | None = None,
    namespace: RouteNamespace | None = None,
    identity: RouteIdentity | None = None,
    schema: SchemaLike[T],
    schema_id: str | None = None,
    version: int | None = None,
) -> TypedRoute[T]:
    """Construct a typed route without exposing native identity plumbing."""
    if namespace is not None:
        if plane is not None or layer is not None:
            raise ValueError("pass either namespace or plane/layer, not both")
        plane = namespace.plane
        layer = namespace.layer
    if identity is not None:
        if (
            owner is not None
            or family is not None
            or stream is not None
            or variant is not None
        ):
            raise ValueError(
                "pass either identity or owner/family/stream/variant, not both"
            )
        owner = identity.owner
        family = identity.family
        stream = identity.stream
        variant = identity.variant
    if None in (plane, layer, owner, family, stream, variant):
        raise ValueError("route requires namespace and identity information")
    return TypedRoute(
        plane=cast(Plane, plane),
        layer=cast(Layer, layer),
        owner=cast(OwnerName, owner),
        family=cast(StreamFamily, family),
        stream=cast(StreamName, stream),
        variant=cast(Variant, variant),
        schema=_coerce_schema(schema, schema_id=schema_id, version=version),
    )


@dataclass
class ReadThenWriteNextEpochStep(Generic[TRead, TWrite]):
    """Composable shared-stream step with one input stream and one output route."""

    name: str
    read: Observable[TRead]
    output: TypedRoute[TWrite]
    write: Observable[TWrite]
    _connect: Callable[[], SubscriptionLike]
    _connection: SubscriptionLike | None = None

    @classmethod
    def map(
        cls,
        *,
        name: str,
        read: Observable[TRead],
        output: TypedRoute[TWrite],
        transform: Callable[[TRead], TWrite],
    ) -> ReadThenWriteNextEpochStep[TRead, TWrite]:
        """Map one observable input into one typed output stream."""
        write_stream = read.pipe(
            ops.map(transform),
            ops.publish(),
        )
        return cls(
            name=name,
            read=read,
            output=output,
            write=write_stream,
            _connect=write_stream.connect,
        )

    def start(self) -> SubscriptionLike:
        """Connect the shared write stream once and return the live subscription."""
        if self._connection is None:
            self._connection = self._connect()
        return self._connection
