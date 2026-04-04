"""Primary Manyfold nouns and verbs.

The goal in this module is a grokkable, typed surface: small value objects for
route construction and a minimal set of composition primitives that are useful
enough to deserve first-class status.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
from typing import Generic
from typing import Protocol
from typing import TypeVar
from typing import runtime_checkable

from reactivex import Observable
from reactivex import operators as ops

from ._manyfold_rust import ClosedEnvelope
from ._manyfold_rust import Layer
from ._manyfold_rust import NamespaceRef
from ._manyfold_rust import Plane
from ._manyfold_rust import RouteRef
from ._manyfold_rust import SchemaRef
from ._manyfold_rust import Variant

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
            schema=SchemaRef(schema_id=self.schema.schema_id, version=self.schema.version),
        )

    def display(self) -> str:
        return self.route_ref.display()


@dataclass(frozen=True)
class TypedEnvelope(Generic[T]):
    """Decoded view of a closed envelope plus its typed payload value."""

    route: TypedRoute[T]
    closed: ClosedEnvelope
    value: T


def route(
    *,
    plane: Plane,
    layer: Layer,
    owner: OwnerName,
    family: StreamFamily,
    stream: StreamName,
    variant: Variant,
    schema: Schema[T],
) -> TypedRoute[T]:
    """Construct a typed route without exposing native identity plumbing."""
    return TypedRoute(
        plane=plane,
        layer=layer,
        owner=owner,
        family=family,
        stream=stream,
        variant=variant,
        schema=schema,
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
