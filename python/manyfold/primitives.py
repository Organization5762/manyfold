"""Primary Manyfold nouns and verbs.

The goal in this module is a grokkable, typed surface: small value objects for
route construction and a minimal set of composition primitives that are useful
enough to deserve first-class status.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field, fields, is_dataclass
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

T = TypeVar("T")
TAccepted = TypeVar("TAccepted")
U = TypeVar("U")
TRead = TypeVar("TRead")
TWrite = TypeVar("TWrite")
TProto = TypeVar("TProto", bound="ProtobufMessage")
SchemaLike = Any

_ANY_SCHEMA_IDS = count(1)
_ANY_SCHEMA_LOCK = Lock()
_ANY_SCHEMA_PREFIX = "any:"
_ANY_SCHEMA_PREFIX_BYTES = _ANY_SCHEMA_PREFIX.encode("ascii")
_ANY_SCHEMA_VALUES: dict[bytes, Any] = {}


def source(route: TypedRoute[T] | RouteRef, *, replay_latest: bool = True) -> Source[T]:
    """Mark a route as a signal source."""
    return Source(route=route, replay_latest=replay_latest)


def sink(route: TypedRoute[T] | RouteRef) -> Sink[T]:
    """Mark a route as a signal sink."""
    return Sink(route=route)


def logical_routes(*, owner: OwnerName | str, family: StreamFamily | str) -> RouteScope:
    """Create a logical route scope for one service or component family."""
    return RouteScope.logical(owner=owner, family=family)


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


@runtime_checkable
class StreamLike(Protocol[T]):
    def subscribe(
        self,
        observer: Callable[[T], None] | object | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
        scheduler: object | None = None,
    ) -> SubscriptionLike: ...


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
class RouteScope:
    """Route naming context for one owner/family/layer.

    A scope carries only route organization. Methods such as
    ``write_request`` and ``read_event`` choose the executable direction and
    message role explicitly at the call site.
    """

    owner: OwnerName
    family: StreamFamily
    layer: Layer = field(default_factory=lambda: Layer.Logical)

    def __post_init__(self) -> None:
        if not isinstance(self.owner, OwnerName):
            raise ValueError("owner must be an OwnerName")
        if not isinstance(self.family, StreamFamily):
            raise ValueError("family must be a StreamFamily")
        _require_enum_member(self.layer, Layer, "layer")

    @classmethod
    def logical(
        cls,
        *,
        owner: OwnerName | str,
        family: StreamFamily | str,
    ) -> RouteScope:
        return cls(
            owner=_coerce_owner_name(owner),
            family=_coerce_stream_family(family),
            layer=Layer.Logical,
        )

    def read_event(
        self,
        stream: StreamName | str,
        schema: SchemaLike[T],
        *,
        schema_id: str | None = None,
        version: int | None = None,
    ) -> TypedRoute[T]:
        """Build a read-side event route in this scope."""
        return self._route(
            plane=Plane.Read,
            stream=stream,
            variant=Variant.Event,
            schema=schema,
            schema_id=schema_id,
            version=version,
        )

    def read_state(
        self,
        stream: StreamName | str,
        schema: SchemaLike[T],
        *,
        schema_id: str | None = None,
        version: int | None = None,
    ) -> TypedRoute[T]:
        """Build a read-side latest-state route in this scope."""
        return self._route(
            plane=Plane.Read,
            stream=stream,
            variant=Variant.Meta,
            schema=schema,
            schema_id=schema_id,
            version=version,
        )

    def write_request(
        self,
        stream: StreamName | str,
        schema: SchemaLike[T],
        *,
        schema_id: str | None = None,
        version: int | None = None,
    ) -> TypedRoute[T]:
        """Build a write-side request route in this scope."""
        return self._route(
            plane=Plane.Write,
            stream=stream,
            variant=Variant.Request,
            schema=schema,
            schema_id=schema_id,
            version=version,
        )

    def state(
        self,
        stream: StreamName | str,
        schema: SchemaLike[T],
        *,
        schema_id: str | None = None,
        version: int | None = None,
    ) -> TypedRoute[T]:
        """Build a state-plane route in this scope."""
        return self._route(
            plane=Plane.State,
            stream=stream,
            variant=Variant.State,
            schema=schema,
            schema_id=schema_id,
            version=version,
        )

    def _route(
        self,
        *,
        plane: Plane,
        stream: StreamName | str,
        variant: Variant,
        schema: SchemaLike[T],
        schema_id: str | None,
        version: int | None,
    ) -> TypedRoute[T]:
        return route(
            plane=plane,
            layer=self.layer,
            owner=self.owner,
            family=self.family,
            stream=stream,
            variant=variant,
            schema=schema,
            schema_id=schema_id,
            version=version,
        )


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
    process_local: bool = False

    def __post_init__(self) -> None:
        _require_non_empty_string(self.schema_id, "schema_id")
        _require_positive_int(self.version, "schema version")
        if not callable(self.encode):
            raise ValueError("schema encode must be callable")
        if not callable(self.decode):
            raise ValueError("schema decode must be callable")
        if not isinstance(self.process_local, bool):
            raise ValueError("schema process_local must be a boolean")

    def is_bytes_passthrough(self) -> bool:
        """Return true when this schema preserves byte payloads unchanged."""
        return (
            self.encode is _coerce_bytes_payload
            and self.decode is _coerce_bytes_payload
        )

    @classmethod
    def any(cls, schema_id: str = "Any", version: int = 1) -> Schema[Any]:
        """Create a process-local schema for arbitrary Python objects.

        The encoded bytes are opaque object references valid only inside the
        current Python process. Use this for in-memory graph streams carrying
        local handles or rich Python objects; do not use it for durable storage
        or cross-process transport.
        """
        schema_prefix = f"{_ANY_SCHEMA_PREFIX}{schema_id}:{version}:".encode("ascii")

        def encode(value: Any) -> bytes:
            # Tokens are intentionally tiny because the process-local table owns
            # the actual object lifetime and identity. The schema id stays in
            # the lookup key with the version so unrelated local schemas cannot
            # decode each other's opaque references by accident.
            with _ANY_SCHEMA_LOCK:
                key = schema_prefix + str(next(_ANY_SCHEMA_IDS)).encode("ascii")
                _ANY_SCHEMA_VALUES[key] = value
            return key

        def decode(payload: bytes) -> Any:
            try:
                key = _coerce_bytes_payload(payload)
            except ValueError as exc:
                raise ValueError(
                    f"unknown process-local object token for schema {schema_id!r}"
                ) from exc
            if not key.startswith(schema_prefix):
                raise ValueError(
                    f"unknown process-local object token for schema {schema_id!r}"
                )
            with _ANY_SCHEMA_LOCK:
                try:
                    return _ANY_SCHEMA_VALUES[key]
                except KeyError as error:
                    raise ValueError(
                        f"unknown process-local object token for schema {schema_id!r}"
                    ) from error

        return cls(
            schema_id=schema_id,
            version=version,
            encode=encode,
            decode=decode,
            process_local=True,
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


@dataclass(frozen=True, init=False)
class Contract(Generic[T]):
    """Type-first value contract for a Manyfold IO knob or route."""

    value_type: type[T]
    _adapters: tuple[_ContractAdapter[Any, T], ...] = field(default=(), repr=False)

    def __init__(self, value_type: type[T]) -> None:
        object.__setattr__(self, "value_type", value_type)
        object.__setattr__(self, "_adapters", ())
        self._validate()

    @classmethod
    def of(cls, value_type: type[T]) -> Contract[T]:
        """Create a strict contract from a Python or protobuf value type."""
        return cls(value_type=value_type)

    @classmethod
    def proto(cls, message_type: ProtobufMessageType[TProto]) -> Contract[TProto]:
        """Create a strict contract for a generated protobuf message type."""
        if not isinstance(message_type, ProtobufMessageType) or not callable(
            getattr(message_type, "FromString", None)
        ):
            raise ValueError(
                "protobuf contract message_type must provide __name__ and FromString"
            )
        return cls(value_type=message_type)

    def accepts(
        self,
        source_type: type[TAccepted],
        mapper: Callable[[TAccepted], T],
    ) -> Contract[T]:
        """Return a copy that can migrate ``source_type`` into this contract."""
        return Contract._with_adapters(
            self.value_type,
            (*self._adapters, _ContractAdapter(source_type, mapper)),
        )

    def convert(self, value: object) -> T:
        """Convert a current or accepted historical value into this contract."""
        if isinstance(value, self.value_type):
            return value
        for adapter in self._adapters:
            if adapter.accepts(value):
                return adapter.map(value)
        raise ValueError(
            f"value of type {_type_display(type(value))} is not compatible with "
            f"{_type_display(self.value_type)}"
        )

    def schema(self, *, schema_id: str | None = None, version: int = 1) -> Schema[T]:
        """Materialize a route schema from the contract's value type."""
        return _schema_from_type(self.value_type, schema_id=schema_id, version=version)

    @classmethod
    def _with_adapters(
        cls,
        value_type: type[U],
        adapters: tuple[_ContractAdapter[Any, U], ...],
    ) -> Contract[U]:
        contract = cls(value_type)
        object.__setattr__(contract, "_adapters", adapters)
        contract._validate()
        return contract

    def _validate(self) -> None:
        _require_type(self.value_type, "contract value_type")
        if not isinstance(self._adapters, tuple):
            raise ValueError("contract adapters must be a tuple")
        for adapter in self._adapters:
            if not isinstance(adapter, _ContractAdapter):
                raise ValueError("contract adapters must be migration adapters")


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

    @cached_property
    def route_display(self) -> str:
        """Materialize the stable route display label once when needed."""
        return self.route_ref.display()

    def display(self) -> str:
        return self.route_display

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


@dataclass(frozen=True, slots=True)
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

    @classmethod
    def _trusted(
        cls: type[TypedEnvelope[T]],
        route: TypedRoute[T],
        closed: ClosedEnvelope,
        value: T,
    ) -> TypedEnvelope[T]:
        envelope = cls.__new__(cls)
        object.__setattr__(envelope, "route", route)
        object.__setattr__(envelope, "closed", closed)
        object.__setattr__(envelope, "value", value)
        return envelope


@dataclass
class ReadThenWriteNextEpochStep(Generic[TRead, TWrite]):
    """Composable shared-stream step with one input stream and one output route."""

    name: str
    read: StreamLike[TRead]
    output: TypedRoute[TWrite]
    write: StreamLike[TWrite]
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
        read: StreamLike[TRead],
        output: TypedRoute[TWrite],
        transform: Callable[[TRead], TWrite],
    ) -> ReadThenWriteNextEpochStep[TRead, TWrite]:
        """Map one observable input into one typed output stream."""
        if not callable(transform):
            raise ValueError("transform must be callable")
        write_stream = _ConnectableMappedStream(read, transform)
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
            _require_subscription(self._connection, "connection")
        return self._connection


def _any_schema_value_count() -> int:
    with _ANY_SCHEMA_LOCK:
        return len(_ANY_SCHEMA_VALUES)


def _release_any_schema_value(payload: bytes) -> None:
    if not _is_any_schema_payload(payload):
        return
    _release_known_any_schema_value(payload)


def _release_known_any_schema_value(payload: bytes) -> None:
    with _ANY_SCHEMA_LOCK:
        _ANY_SCHEMA_VALUES.pop(payload, None)


def _is_any_schema_payload(payload: bytes) -> bool:
    return payload.startswith(_ANY_SCHEMA_PREFIX_BYTES)


def _require_non_empty_string(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


def _require_positive_int(value: int, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")


def _require_enum_member(value: object, enum_type: type[Any], field: str) -> None:
    if not any(
        value is member
        or (isinstance(value, enum_type) and value == member)
        or (
            type(value) is not str
            and isinstance(value, str)
            and str(value) == getattr(member, "value", member)
        )
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
    if not isinstance(value, StreamLike):
        raise ValueError(f"{field} must be a subscribable stream")


def _require_subscription(value: object, field: str) -> None:
    if not isinstance(value, SubscriptionLike):
        raise ValueError(f"{field} must provide dispose")


def _require_type(value: object, field: str) -> None:
    if not isinstance(value, type):
        raise ValueError(f"{field} must be a type")


def _type_display(value_type: type[object]) -> str:
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _contract_schema_id(value_type: type[object]) -> str:
    return _type_display(value_type)


def _schema_from_type(
    value_type: type[T],
    *,
    schema_id: str | None,
    version: int,
) -> Schema[T]:
    _require_type(value_type, "contract value_type")
    resolved_schema_id = schema_id or _contract_schema_id(value_type)
    if value_type is bytes:
        return cast(Schema[T], Schema.bytes(name=resolved_schema_id, version=version))
    if isinstance(value_type, ProtobufMessageType):
        return cast(
            Schema[T],
            Schema.protobuf(
                cast(ProtobufMessageType[TProto], value_type),
                schema_id=resolved_schema_id,
                version=version,
            ),
        )
    if is_dataclass(value_type):
        return Schema(
            schema_id=resolved_schema_id,
            version=version,
            encode=lambda value: _encode_dataclass_value(value_type, value),
            decode=lambda payload: _decode_dataclass_value(value_type, payload),
        )
    raise ValueError(
        "contract value_type must be bytes, a dataclass type, or protobuf message type"
    )


def _encode_dataclass_value(value_type: type[T], value: object) -> bytes:
    if not isinstance(value, value_type):
        raise ValueError(
            f"dataclass contract values must be {_type_display(value_type)}"
        )
    payload = {
        field_value.name: getattr(value, field_value.name)
        for field_value in fields(value_type)
    }
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    except TypeError as exc:
        raise ValueError("dataclass contract values must be JSON-serializable") from exc


def _decode_dataclass_value(value_type: type[T], payload: bytes) -> T:
    try:
        decoded = json.loads(_coerce_bytes_payload(payload).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("dataclass contract payloads must be JSON objects") from exc
    if not isinstance(decoded, dict):
        raise ValueError("dataclass contract payloads must be JSON objects")
    try:
        return value_type(**decoded)
    except TypeError as exc:
        raise ValueError(
            f"dataclass contract payload does not match {_type_display(value_type)}"
        ) from exc


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
    if isinstance(schema, Contract):
        return schema.schema(
            schema_id=schema_id,
            version=1 if version is None else version,
        )
    if schema is bytes:
        if schema_id is None:
            return cast(
                Schema[T],
                Schema.bytes(
                    name=_contract_schema_id(bytes),
                    version=1 if version is None else version,
                ),
            )
        return cast(
            Schema[T],
            Schema.bytes(name=schema_id, version=1 if version is None else version),
        )
    if not isinstance(schema, ProtobufMessageType):
        if isinstance(schema, type) and is_dataclass(schema):
            return _schema_from_type(
                cast(type[T], schema),
                schema_id=schema_id,
                version=1 if version is None else version,
            )
        raise ValueError(
            "schema must be a Schema, Contract, bytes, dataclass type, "
            "or protobuf message type"
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


class _CallbackSubscription:
    def __init__(self, dispose: Callable[[], None]) -> None:
        self._dispose = dispose
        self._disposed = False

    def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        self._dispose()


class _ConnectableMappedStream(Generic[TRead, TWrite]):
    def __init__(
        self,
        source: StreamLike[TRead],
        transform: Callable[[TRead], TWrite],
    ) -> None:
        self._source = source
        self._transform = transform
        self._lock = Lock()
        self._subscribers: dict[int, Callable[[TWrite], None]] = {}
        self._subscriber_snapshot: tuple[Callable[[TWrite], None], ...] = ()
        self._next_subscription_id = 0

    def subscribe(
        self,
        observer: Callable[[TWrite], None] | object | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_completed: Callable[[], None] | None = None,
        scheduler: object | None = None,
    ) -> SubscriptionLike:
        del on_error, on_completed, scheduler
        if observer is None:

            def callback(_value: TWrite) -> None:
                return None
        elif callable(observer):
            callback = observer
        else:
            callback = getattr(observer, "on_next")
        with self._lock:
            subscription_id = self._next_subscription_id
            self._next_subscription_id += 1
            self._subscribers[subscription_id] = callback
            self._subscriber_snapshot = tuple(self._subscribers.values())
        return _CallbackSubscription(lambda: self._unsubscribe(subscription_id))

    def connect(self) -> SubscriptionLike:
        return self._source.subscribe(lambda value: self._emit(self._transform(value)))

    def _emit(self, value: TWrite) -> None:
        with self._lock:
            subscribers = self._subscriber_snapshot
        for subscriber in subscribers:
            subscriber(value)

    def _unsubscribe(self, subscription_id: int) -> None:
        with self._lock:
            if subscription_id not in self._subscribers:
                return
            self._subscribers.pop(subscription_id)
            self._subscriber_snapshot = tuple(self._subscribers.values())


@dataclass(frozen=True)
class _ContractAdapter(Generic[TAccepted, T]):
    """Typed migration from an accepted upstream value into the current type."""

    source_type: type[TAccepted]
    mapper: Callable[[TAccepted], T]

    def __post_init__(self) -> None:
        _require_type(self.source_type, "adapter source_type")
        if not callable(self.mapper):
            raise ValueError("adapter mapper must be callable")

    def accepts(self, value: object) -> bool:
        return isinstance(value, self.source_type)

    def map(self, value: object) -> T:
        if not self.accepts(value):
            raise ValueError(
                f"adapter expected {self.source_type.__module__}."
                f"{self.source_type.__qualname__}"
            )
        return self.mapper(cast(TAccepted, value))
