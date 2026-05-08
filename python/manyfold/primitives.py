"""Primary Manyfold nouns and verbs.

The goal in this module is a grokkable, typed surface: small value objects for
route construction and a minimal set of composition primitives that are useful
enough to deserve first-class status.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from functools import cached_property
from itertools import count
from threading import Lock
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
U = TypeVar("U")
TRead = TypeVar("TRead")
TWrite = TypeVar("TWrite")
TProto = TypeVar("TProto", bound="ProtobufMessage")
_ANY_SCHEMA_IDS = count(1)
_ANY_SCHEMA_LOCK = Lock()
_ANY_SCHEMA_VALUES: dict[tuple[str, int, str], Any] = {}


def _require_non_empty_string(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


def _require_positive_int(value: int, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")


def _encode_finite_float(value: Any) -> bytes:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("float schema values must be finite")
    return repr(number).encode("ascii")


def _decode_finite_float(payload: bytes) -> float:
    number = float(payload.decode("ascii"))
    if not math.isfinite(number):
        raise ValueError("float schema values must be finite")
    return number


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

    def __post_init__(self) -> None:
        _require_non_empty_string(self.value, "owner")


@dataclass(frozen=True)
class StreamFamily:
    """Typed stream family segment."""

    value: str

    def __post_init__(self) -> None:
        _require_non_empty_string(self.value, "family")


@dataclass(frozen=True)
class StreamName:
    """Typed stream name segment."""

    value: str

    def __post_init__(self) -> None:
        _require_non_empty_string(self.value, "stream")


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

    def __post_init__(self) -> None:
        _require_non_empty_string(self.schema_id, "schema_id")
        _require_positive_int(self.version, "schema version")

    @classmethod
    def any(cls, schema_id: str = "Any", version: int = 1) -> Schema[Any]:
        """Create a process-local schema for arbitrary Python objects.

        The encoded bytes are opaque object references valid only inside the
        current Python process. Use this for in-memory graph streams carrying
        local handles or rich Python objects; do not use it for durable storage
        or cross-process transport.
        """

        def encode(value: Any) -> bytes:
            # Tokens are intentionally tiny because the process-local table owns
            # the actual object lifetime and identity. The schema id stays in
            # the lookup key with the version so unrelated local schemas cannot
            # decode each other's opaque references by accident.
            with _ANY_SCHEMA_LOCK:
                key = str(next(_ANY_SCHEMA_IDS))
                _ANY_SCHEMA_VALUES[(schema_id, version, key)] = value
            return key.encode("ascii")

        def decode(payload: bytes) -> Any:
            key = payload.decode("ascii")
            with _ANY_SCHEMA_LOCK:
                try:
                    return _ANY_SCHEMA_VALUES[(schema_id, version, key)]
                except KeyError as error:
                    raise ValueError(
                        f"unknown process-local object token for schema {schema_id!r}"
                    ) from error

        return cls(
            schema_id=schema_id,
            version=version,
            encode=encode,
            decode=decode,
        )

    @classmethod
    def bytes(cls, *, name: str, version: int = 1) -> Schema[bytes]:
        """Create a schema for byte payloads with a readable domain name."""
        return cls(
            schema_id=name,
            version=version,
            encode=bytes,
            decode=bytes,
        )

    @classmethod
    def float(cls, *, name: str, version: int = 1) -> Schema[float]:
        """Create a schema for finite ASCII-encoded floating point values."""
        return cls(
            schema_id=name,
            version=version,
            encode=_encode_finite_float,
            decode=_decode_finite_float,
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

    @cached_property
    def route_ref(self) -> RouteRef:
        """Materialize the native route reference once when needed."""
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

    def derivative_route(
        self,
        *,
        stream: StreamName | str,
        schema: SchemaLike[U],
        owner: OwnerName | str | None = None,
        family: StreamFamily | str | None = None,
        plane: Plane | None = None,
        layer: Layer | None = None,
        variant: Variant | None = None,
        schema_id: str | None = None,
        version: int | None = None,
    ) -> TypedRoute[U]:
        """Create a related route that keeps this route's context by default."""
        return route(
            plane=self.plane if plane is None else plane,
            layer=self.layer if layer is None else layer,
            owner=self.owner if owner is None else owner,
            family=self.family if family is None else family,
            stream=stream,
            variant=self.variant if variant is None else variant,
            schema=schema,
            schema_id=schema_id,
            version=version,
        )


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

    def close(self) -> ClosedEnvelope:
        """Return the immutable closed envelope carried by this decoded view."""
        return self.closed


def source(route: TypedRoute[T] | RouteRef, *, replay_latest: bool = True) -> Source[T]:
    """Mark a route as a signal source."""
    return Source(route=route, replay_latest=replay_latest)


def sink(route: TypedRoute[T] | RouteRef) -> Sink[T]:
    """Mark a route as a signal sink."""
    return Sink(route=route)


SchemaLike = Any


def _coerce_owner_name(owner: OwnerName | str) -> OwnerName:
    return owner if isinstance(owner, OwnerName) else OwnerName(owner)


def _coerce_stream_family(family: StreamFamily | str) -> StreamFamily:
    return family if isinstance(family, StreamFamily) else StreamFamily(family)


def _coerce_stream_name(stream: StreamName | str) -> StreamName:
    return stream if isinstance(stream, StreamName) else StreamName(stream)


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
                name=schema_id, version=1 if version is None else version
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


def _missing_route_parts(
    *,
    plane: Plane | None,
    layer: Layer | None,
    owner: OwnerName | str | None,
    family: StreamFamily | str | None,
    stream: StreamName | str | None,
    variant: Variant | None,
) -> tuple[str, ...]:
    parts = (
        ("plane", plane),
        ("layer", layer),
        ("owner", owner),
        ("family", family),
        ("stream", stream),
        ("variant", variant),
    )
    return tuple(name for name, value in parts if value is None)


@overload
def route(
    *,
    owner: OwnerName | str,
    family: StreamFamily | str,
    stream: StreamName | str,
    schema: SchemaLike[T],
    plane: Plane = Plane.Read,
    layer: Layer = Layer.Logical,
    variant: Variant = Variant.Meta,
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
    owner: OwnerName | str | None = None,
    family: StreamFamily | str | None = None,
    stream: StreamName | str | None = None,
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
    else:
        plane = Plane.Read if plane is None else plane
        layer = Layer.Logical if layer is None else layer
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
    else:
        variant = Variant.Meta if variant is None else variant
    missing = _missing_route_parts(
        plane=plane,
        layer=layer,
        owner=owner,
        family=family,
        stream=stream,
        variant=variant,
    )
    if missing:
        raise ValueError(f"route requires: {', '.join(missing)}")
    return TypedRoute(
        plane=cast(Plane, plane),
        layer=cast(Layer, layer),
        owner=_coerce_owner_name(cast(OwnerName | str, owner)),
        family=_coerce_stream_family(cast(StreamFamily | str, family)),
        stream=_coerce_stream_name(cast(StreamName | str, stream)),
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
