"""Primary Manyfold nouns and verbs.

The goal in this module is a grokkable, typed surface: small value objects for
route construction and a minimal set of composition primitives that are useful
enough to deserve first-class status.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from functools import cached_property, lru_cache
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


SchemaLike = Any


def source(route: TypedRoute[T] | RouteRef, *, replay_latest: bool = True) -> Source[T]:
    """Mark a route as a signal source."""
    return Source(route=route, replay_latest=replay_latest)


def sink(route: TypedRoute[T] | RouteRef) -> Sink[T]:
    """Mark a route as a signal sink."""
    return Sink(route=route)


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
        if not isinstance(namespace, RouteNamespace):
            raise ValueError("namespace must be a RouteNamespace")
        if plane is not None or layer is not None:
            raise ValueError("pass either namespace or plane/layer, not both")
        plane = namespace.plane
        layer = namespace.layer
    else:
        plane = Plane.Read if plane is None else plane
        layer = Layer.Logical if layer is None else layer
    if identity is not None:
        if not isinstance(identity, RouteIdentity):
            raise ValueError("identity must be a RouteIdentity")
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
    _require_enum_member(plane, Plane, "plane")
    _require_enum_member(layer, Layer, "layer")
    _require_enum_member(variant, Variant, "variant")
    return TypedRoute(
        plane=cast(Plane, plane),
        layer=cast(Layer, layer),
        owner=_coerce_owner_name(cast(OwnerName | str, owner)),
        family=_coerce_stream_family(cast(StreamFamily | str, family)),
        stream=_coerce_stream_name(cast(StreamName | str, stream)),
        variant=cast(Variant, variant),
        schema=_coerce_schema(schema, schema_id=schema_id, version=version),
    )


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

    def __post_init__(self) -> None:
        _require_enum_member(self.plane, Plane, "plane")
        _require_enum_member(self.layer, Layer, "layer")


@dataclass(frozen=True)
class RouteIdentity:
    """Typed wrapper for the human-owned parts of a route identity."""

    owner: OwnerName
    family: StreamFamily
    stream: StreamName
    variant: Variant

    def __post_init__(self) -> None:
        if not isinstance(self.owner, OwnerName):
            raise ValueError("owner must be an OwnerName")
        if not isinstance(self.family, StreamFamily):
            raise ValueError("family must be a StreamFamily")
        if not isinstance(self.stream, StreamName):
            raise ValueError("stream must be a StreamName")
        _require_enum_member(self.variant, Variant, "variant")

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
        if not callable(self.encode):
            raise ValueError("schema encode must be callable")
        if not callable(self.decode):
            raise ValueError("schema decode must be callable")

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
            try:
                key = _coerce_bytes_payload(payload).decode("ascii")
            except (UnicodeDecodeError, ValueError) as exc:
                raise ValueError(
                    f"unknown process-local object token for schema {schema_id!r}"
                ) from exc
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
            encode=_coerce_bytes_payload,
            decode=_coerce_bytes_payload,
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
        if not isinstance(message_type, ProtobufMessageType) or not callable(
            getattr(message_type, "FromString", None)
        ):
            raise ValueError(
                "protobuf schema message_type must provide __name__ and FromString"
            )
        schema_name = schema_id or message_type.__name__
        return cls(
            schema_id=schema_name,
            version=version,
            encode=_encode_protobuf_message,
            decode=lambda payload: _decode_protobuf_message(message_type, payload),
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

    def __post_init__(self) -> None:
        _require_enum_member(self.plane, Plane, "plane")
        _require_enum_member(self.layer, Layer, "layer")
        _require_enum_member(self.variant, Variant, "variant")
        if not isinstance(self.owner, OwnerName):
            raise ValueError("owner must be an OwnerName")
        if not isinstance(self.family, StreamFamily):
            raise ValueError("family must be a StreamFamily")
        if not isinstance(self.stream, StreamName):
            raise ValueError("stream must be a StreamName")
        if not isinstance(self.schema, Schema):
            raise ValueError("schema must be a Schema")

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

    def __post_init__(self) -> None:
        if not isinstance(self.route, (TypedRoute, RouteRef)):
            raise ValueError("source route must be a TypedRoute or RouteRef")
        if not isinstance(self.replay_latest, bool):
            raise ValueError("source replay_latest must be a boolean")

    def display(self) -> str:
        return self.route.display()


@dataclass(frozen=True)
class Sink(Generic[T]):
    """Signal sink role for a route.

    Sinks consume signal and make downstream demand explicit without adding a
    separate runtime node.
    """

    route: TypedRoute[T] | RouteRef

    def __post_init__(self) -> None:
        if not isinstance(self.route, (TypedRoute, RouteRef)):
            raise ValueError("sink route must be a TypedRoute or RouteRef")

    def display(self) -> str:
        return self.route.display()


@dataclass(frozen=True)
class TypedEnvelope(Generic[T]):
    """Decoded view of a closed envelope plus its typed payload value."""

    route: TypedRoute[T]
    closed: ClosedEnvelope
    value: T

    def __post_init__(self) -> None:
        if not isinstance(self.route, TypedRoute):
            raise ValueError("envelope route must be a TypedRoute")
        if not isinstance(self.closed, ClosedEnvelope):
            raise ValueError("envelope closed value must be a ClosedEnvelope")

    def close(self) -> ClosedEnvelope:
        """Return the immutable closed envelope carried by this decoded view."""
        return self.closed


@dataclass
class ReadThenWriteNextEpochStep(Generic[TRead, TWrite]):
    """Composable shared-stream step with one input stream and one output route."""

    name: str
    read: Observable[TRead]
    output: TypedRoute[TWrite]
    write: Observable[TWrite]
    _connect: Callable[[], SubscriptionLike]
    _connection: SubscriptionLike | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string(self.name, "step name")
        _require_observable(self.read, "read")
        if not isinstance(self.output, TypedRoute):
            raise ValueError("output must be a TypedRoute")
        _require_observable(self.write, "write")
        if not callable(self._connect):
            raise ValueError("connect must be callable")
        if self._connection is not None:
            _require_subscription(self._connection, "connection")

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
        if not callable(transform):
            raise ValueError("transform must be callable")
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


def _require_non_empty_string(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


def _require_positive_int(value: int, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")


def _require_enum_member(value: object, enum_type: type[Any], field: str) -> None:
    if type(value) is str:
        raise ValueError(f"{field} must be a {enum_type.__name__}")
    value_token = getattr(value, "value", value)
    if not any(
        value is member
        or value == member
        or value_token == getattr(member, "value", member)
        for member in _enum_members(enum_type)
    ):
        raise ValueError(f"{field} must be a {enum_type.__name__}")


@lru_cache(maxsize=None)
def _enum_members(enum_type: type[Any]) -> tuple[Any, ...]:
    """Cache PyO3 enum-like class members for hot route validation paths."""

    return tuple(
        getattr(enum_type, name) for name in dir(enum_type) if not name.startswith("_")
    )


def _require_observable(value: object, field: str) -> None:
    if not isinstance(value, Observable):
        raise ValueError(f"{field} must be an Observable")


def _require_subscription(value: object, field: str) -> None:
    if not isinstance(value, SubscriptionLike):
        raise ValueError(f"{field} must provide dispose")


def _encode_finite_float(value: Any) -> bytes:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("float schema values must be finite numbers")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("float schema values must be finite numbers")
    return repr(number).encode("ascii")


def _decode_finite_float(payload: bytes) -> float:
    try:
        raw_payload = _coerce_bytes_payload(payload)
        number = float(raw_payload.decode("ascii"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError("float schema values must be finite numbers") from exc
    if not math.isfinite(number):
        raise ValueError("float schema values must be finite numbers")
    return number


def _coerce_bytes_payload(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise ValueError("bytes schema values must be bytes-like")


def _encode_protobuf_message(value: Any) -> bytes:
    serializer = getattr(value, "SerializeToString", None)
    if not callable(serializer):
        raise ValueError("protobuf schema values must provide SerializeToString")
    return _coerce_protobuf_bytes(serializer())


def _decode_protobuf_message(
    message_type: ProtobufMessageType[TProto],
    payload: bytes,
) -> TProto:
    return message_type.FromString(_coerce_protobuf_bytes(payload))


def _coerce_protobuf_bytes(value: Any) -> bytes:
    try:
        return _coerce_bytes_payload(value)
    except ValueError as exc:
        raise ValueError("protobuf schema payloads must be bytes-like") from exc


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
        if resolved_schema_id != schema.schema_id or resolved_version != schema.version:
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
            Schema.bytes(name=schema_id, version=1 if version is None else version),
        )
    if not isinstance(schema, ProtobufMessageType):
        raise ValueError("schema must be a Schema, bytes, or protobuf message type")
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
