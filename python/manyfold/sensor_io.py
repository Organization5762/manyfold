"""Local sensor IO components for single-device Manyfold projects."""

from __future__ import annotations

import base64
import binascii
import codecs
import json
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    TypeVar,
)

from .components import EventLog, Keyspace
from .graph import Graph
from .primitives import Schema, SubscriptionLike, TypedRoute

T = TypeVar("T")
TFrame = TypeVar("TFrame")

OverflowMode = Literal["drop_oldest", "drop_newest", "reject", "latest"]
SensorBufferMode = Literal["latest", "fifo"]
MessageBufferMode = Literal["bytes", "text"]
__all__ = (
    "BackoffPolicy",
    "BoundedRingBuffer",
    "ChangeFilter",
    "Clock",
    "DelimitedMessageBuffer",
    "DetectionNode",
    "DetectionNodeHandle",
    "DoubleBuffer",
    "DuplexSensorPeripheral",
    "FrameAssembler",
    "GraphAccessNode",
    "HealthStatus",
    "JsonEventDecoder",
    "LocalDurableSpool",
    "LocalSensorSource",
    "ManagedGraphNode",
    "ManagedGraphNodeHandle",
    "ManagedRunLoop",
    "ManagedRunLoopHandle",
    "ManualClock",
    "PeripheralAdapter",
    "PeripheralAdapterHandle",
    "RateMatchedSensor",
    "ReactiveSensorHandle",
    "ReactiveSensorSource",
    "RetryLoop",
    "RetryPolicy",
    "SensorDebugEnvelope",
    "SensorDebugStage",
    "SensorDebugTap",
    "SensorEvent",
    "SensorFrame",
    "SensorHealthHandle",
    "SensorHealthWatchdog",
    "SensorIdentity",
    "SensorLocation",
    "SensorSample",
    "SensorSourceHandle",
    "SensorTag",
    "SequenceCounter",
    "StopToken",
    "SystemClock",
    "ThresholdFilter",
    "health_status_schema",
    "sensor_event_schema",
    "sensor_sample_schema",
    "xor_checksum",
)


def sensor_sample_schema(
    value_schema: Schema[T], schema_id: str | None = None
) -> Schema[SensorSample[T]]:
    """Build a JSON envelope schema for ``SensorSample[T]`` values."""

    if not isinstance(value_schema, Schema):
        raise ValueError("sensor sample value_schema must be a Schema")
    resolved_schema_id = schema_id or f"SensorSample[{value_schema.schema_id}]"

    def encode(sample: SensorSample[T]) -> bytes:
        value_payload = value_schema.encode(sample.value)
        return _compact_json_bytes(
            {
                "value": base64.b64encode(value_payload).decode("ascii"),
                "source_timestamp": sample.source_timestamp,
                "ingest_timestamp": sample.ingest_timestamp,
                "sequence_number": sample.sequence_number,
                "quality": sample.quality,
                "status": sample.status,
            }
        )

    def decode(payload: bytes) -> SensorSample[T]:
        data = _decode_json_mapping(
            json.loads(payload.decode("utf-8")), "sensor sample"
        )
        value = value_schema.decode(_decode_base64_field(data["value"], "value"))
        return SensorSample(
            value=value,
            source_timestamp=_decode_json_number(
                data["source_timestamp"], "source_timestamp"
            ),
            ingest_timestamp=_decode_json_number(
                data["ingest_timestamp"], "ingest_timestamp"
            ),
            sequence_number=_decode_json_int(
                data["sequence_number"], "sequence_number"
            ),
            quality=_decode_optional_json_string(data.get("quality"), "quality"),
            status=_decode_optional_json_string(data.get("status"), "status"),
        )

    return Schema(
        schema_id=resolved_schema_id,
        version=value_schema.version,
        encode=encode,
        decode=decode,
    )


def sensor_event_schema(schema_id: str = "SensorEvent") -> Schema[SensorEvent]:
    """Return a JSON schema for normalized sensor events."""

    def encode(event: SensorEvent) -> bytes:
        payload = {
            "event_type": event.event_type,
            "data": _json_safe(event.data),
            "observed_at": event.observed_at,
            "identity": _json_safe(event.identity),
            "sequence_number": event.sequence_number,
            "raw": None
            if event.raw is None
            else base64.b64encode(event.raw).decode("ascii"),
            "metadata": _json_safe(dict(event.metadata)),
        }
        return _compact_json_bytes(
            payload,
        )

    def decode(payload: bytes) -> SensorEvent:
        data = _decode_json_mapping(json.loads(payload.decode("utf-8")), "sensor event")
        raw = data.get("raw")
        metadata = _decode_json_mapping(data.get("metadata", {}), "metadata")
        return SensorEvent(
            event_type=_decode_json_string(data["event_type"], "event_type"),
            data=_json_restore(data.get("data")),
            observed_at=_decode_json_number(data["observed_at"], "observed_at"),
            identity=_sensor_identity_from_json(data.get("identity")),
            sequence_number=None
            if data.get("sequence_number") is None
            else _decode_json_int(data["sequence_number"], "sequence_number"),
            raw=None if raw is None else _decode_base64_field(raw, "raw"),
            metadata=_json_restore(metadata),
        )

    return Schema(schema_id=schema_id, version=1, encode=encode, decode=decode)


def health_status_schema(schema_id: str = "HealthStatus") -> Schema[HealthStatus]:
    """Return a JSON schema for local sensor health events."""

    def encode(status: HealthStatus) -> bytes:
        return _compact_json_bytes(
            {
                "status": status.status,
                "observed_at": status.observed_at,
                "message": status.message,
                "stale": status.stale,
                "error_count": status.error_count,
            }
        )

    def decode(payload: bytes) -> HealthStatus:
        data = _decode_json_mapping(
            json.loads(payload.decode("utf-8")), "health status"
        )
        return HealthStatus(
            status=_decode_health_status(data["status"]),
            observed_at=_decode_json_number(data["observed_at"], "observed_at"),
            message=_decode_json_string(data.get("message", ""), "message"),
            stale=_decode_json_bool(data.get("stale", False), "stale"),
            error_count=_decode_json_int(data.get("error_count", 0), "error_count"),
        )

    return Schema(schema_id=schema_id, version=1, encode=encode, decode=decode)


def xor_checksum(data: bytes | bytearray | Sequence[int]) -> int:
    """Return a small XOR checksum for packet/frame tests and adapters."""

    checksum = 0
    for value in data:
        checksum ^= int(value) & 0xFF
    return checksum


class Clock(Protocol):
    """Clock used by local components for timestamps and deterministic tests."""

    def now(self) -> float: ...


@dataclass
class SystemClock:
    """Wall-clock implementation of ``Clock``."""

    group: str | None = None

    def now(self) -> float:
        return time.time()


@dataclass
class ManualClock:
    """Deterministic clock for tests and simulated sensor loops."""

    current: float = 0.0
    group: str | None = None

    def __post_init__(self) -> None:
        self.current = _require_finite_number(self.current, "clock value")

    def now(self) -> float:
        return self.current

    def set(self, value: float) -> None:
        self.current = _require_finite_number(value, "clock value")

    def advance(self, delta: float) -> float:
        resolved_delta = _require_finite_number(delta, "clock delta")
        if resolved_delta < 0:
            raise ValueError("delta must be non-negative")
        self.current = _require_finite_number(
            self.current + resolved_delta,
            "clock value",
        )
        return self.current


@dataclass(frozen=True)
class RetryPolicy:
    """Pure local retry policy for sensor reads and local effects."""

    max_attempts: int = 1
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "max_attempts",
            _require_int(self.max_attempts, "max_attempts"),
        )
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        object.__setattr__(self, "retry_on", tuple(self.retry_on))
        if not self.retry_on:
            raise ValueError("retry_on must contain at least one exception type")
        if not all(_is_exception_class(exception) for exception in self.retry_on):
            raise ValueError("retry_on must contain only exception types")

    @classmethod
    def never(cls) -> RetryPolicy:
        return cls(max_attempts=1)

    @classmethod
    def attempts(cls, max_attempts: int) -> RetryPolicy:
        return cls(max_attempts=max_attempts)


@dataclass(frozen=True)
class BackoffPolicy:
    """Pure delay policy between retry attempts."""

    initial_delay: float = 0.0
    multiplier: float = 1.0
    max_delay: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "initial_delay",
            _require_finite_number(self.initial_delay, "initial_delay"),
        )
        object.__setattr__(
            self,
            "multiplier",
            _require_finite_number(self.multiplier, "multiplier"),
        )
        if self.max_delay is not None:
            object.__setattr__(
                self,
                "max_delay",
                _require_finite_number(self.max_delay, "max_delay"),
            )
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be non-negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if self.max_delay is not None and self.max_delay < 0:
            raise ValueError("max_delay must be non-negative")

    @classmethod
    def none(cls) -> BackoffPolicy:
        return cls()

    @classmethod
    def fixed(cls, delay: float) -> BackoffPolicy:
        return cls(initial_delay=delay)

    def delay_for_attempt(self, attempt_index: int) -> float:
        attempt_index = _require_int(attempt_index, "attempt_index")
        if attempt_index <= 0:
            raise ValueError("attempt_index must be positive")
        if attempt_index <= 1:
            return 0.0
        try:
            delay = self.initial_delay * (self.multiplier ** (attempt_index - 2))
        except OverflowError:
            if self.max_delay is None:
                raise
            return self.max_delay
        if not math.isfinite(delay):
            if self.max_delay is None:
                raise OverflowError("backoff delay overflowed")
            return self.max_delay
        if self.max_delay is not None:
            delay = min(delay, self.max_delay)
        return delay


@dataclass
class RetryLoop:
    """Executable local retry behavior built from retry and backoff policies."""

    retry: RetryPolicy = field(default_factory=RetryPolicy)
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    sleep: Callable[[float], None] = time.sleep
    group: str | None = None

    def __post_init__(self) -> None:
        self.retry = _require_retry_policy(self.retry, "retry loop retry")
        self.backoff = _require_backoff_policy(self.backoff, "retry loop backoff")
        _require_callable(self.sleep, "retry loop sleep")
        self.group = _require_optional_string(self.group, "retry loop group")

    def run(self, operation: Callable[[], T]) -> T:
        _require_callable(operation, "retry loop operation")
        last_error: BaseException | None = None
        for attempt in range(1, self.retry.max_attempts + 1):
            delay = self.backoff.delay_for_attempt(attempt)
            if delay > 0:
                self.sleep(delay)
            try:
                return operation()
            except self.retry.retry_on as exc:
                last_error = exc
                if attempt >= self.retry.max_attempts:
                    raise
        assert last_error is not None
        raise last_error


@dataclass
class StopToken:
    """Cooperative stop signal for long-lived local loops."""

    group: str | None = None
    _event: threading.Event = field(
        default_factory=threading.Event, init=False, repr=False
    )

    def is_set(self) -> bool:
        return self._event.is_set()

    def set(self) -> None:
        self._event.set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


@dataclass
class ManagedRunLoop:
    """Run one long-lived operation until stopped, retrying transient failures.

    The loop body receives a ``StopToken`` and should return when its current
    connection/session ends. Returning normally starts the next iteration unless
    the token was stopped. Retry delays use ``StopToken.wait`` so shutdown wakes
    sleeping loops immediately.
    """

    body: Callable[[StopToken], None]
    retry: RetryPolicy = field(
        default_factory=lambda: RetryPolicy(max_attempts=1_000_000)
    )
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    on_error: Callable[[BaseException, int], None] | None = None
    group: str | None = None

    def __post_init__(self) -> None:
        _require_callable(self.body, "managed run loop body")
        self.retry = _require_retry_policy(self.retry, "managed run loop retry")
        self.backoff = _require_backoff_policy(
            self.backoff,
            "managed run loop backoff",
        )
        if self.on_error is not None:
            _require_callable(self.on_error, "managed run loop on_error")
        self.group = _require_optional_string(self.group, "managed run loop group")

    def run(self, stop: StopToken | None = None) -> None:
        if stop is not None and not isinstance(stop, StopToken):
            raise ValueError("managed run loop stop must be a StopToken")
        token = stop or StopToken(group=self.group)
        consecutive_failures = 0
        while not token.is_set():
            try:
                self.body(token)
                consecutive_failures = 0
            except self.retry.retry_on as exc:
                consecutive_failures += 1
                if self.on_error is not None:
                    self.on_error(exc, consecutive_failures)
                if consecutive_failures >= self.retry.max_attempts:
                    raise
            if token.is_set():
                return
            delay = self.backoff.delay_for_attempt(consecutive_failures + 1)
            if delay > 0 and token.wait(delay):
                return

    def start_thread(
        self,
        *,
        name: str,
        daemon: bool = True,
    ) -> "ManagedRunLoopHandle":
        token = StopToken(group=self.group)
        thread = threading.Thread(
            target=self.run,
            args=(token,),
            name=name,
            daemon=daemon,
        )
        handle = ManagedRunLoopHandle(loop=self, token=token, thread=thread)
        thread.start()
        return handle


@dataclass
class ManagedRunLoopHandle:
    """Owned thread handle for a ``ManagedRunLoop``."""

    loop: ManagedRunLoop
    token: StopToken
    thread: threading.Thread
    disposed: bool = False

    def stop(self) -> None:
        self.token.set()

    def join(self, timeout: float | None = None) -> None:
        if self.thread.ident is not None:
            self.thread.join(timeout=timeout)

    def dispose(self, timeout: float | None = None) -> None:
        if self.disposed:
            return
        self.stop()
        self.join(timeout=timeout)
        self.disposed = True


@dataclass
class GraphAccessNode:
    """Graph mutation capability passed to nodes that can install other nodes.

    Manyfold treats access to the graph itself as a node-like capability rather
    than ambient authority. Spawner-style nodes receive this capability and use
    it to publish detection facts or install downstream nodes, while ordinary
    consumers continue to depend on routes.
    """

    graph: Graph
    owned_handles: list[Any] = field(default_factory=list)

    def publish(self, route: TypedRoute[Any], payload: Any) -> Any:
        return self.graph.publish(route, payload)

    def install(self, node: Any) -> Any:
        handle = node.install(self.graph)
        self.owned_handles.extend(_flatten_handles(handle))
        return handle

    def own(self, handles: Any) -> Any:
        self.owned_handles.extend(_flatten_handles(handles))
        return handles


GraphNodeBody = Callable[[StopToken, Graph], None]
GraphNodeControlHandler = Callable[[Any, Graph], None]
GraphNodeErrorMapper = Callable[[BaseException, int], Any]
Detector = Callable[[], Iterable[Any]]
DetectionMapper = Callable[[Any], Any]
DetectionCallback = Callable[[Any, GraphAccessNode], None]
DetectionSpawner = Callable[[Any, GraphAccessNode], Any]


@dataclass
class ManagedGraphNodeHandle:
    """Installed self-running graph node with owned loop and subscriptions."""

    node: ManagedGraphNode
    graph: Graph
    loop_handle: ManagedRunLoopHandle
    control_subscription: SubscriptionLike | None = None
    disposed: bool = False

    @property
    def group(self) -> str | None:
        return self.node.group

    def stop(self) -> None:
        self.loop_handle.stop()

    def join(self, timeout: float | None = None) -> None:
        self.loop_handle.join(timeout=timeout)

    def dispose(self, timeout: float | None = None) -> None:
        if self.disposed:
            return
        if self.control_subscription is not None:
            self.control_subscription.dispose()
        self.loop_handle.dispose(timeout=timeout)
        self.disposed = True


@dataclass
class ManagedGraphNode:
    """Install a self-running local effect as a graph-visible node.

    The node owns lifecycle/retry mechanics internally, while its public
    boundary remains graph routes: the body publishes outputs, optional control
    input is observed from a route, and exceptions can be published as normal
    Python values on an error route for downstream functional handling.
    """

    name: str
    body: GraphNodeBody
    output_routes: tuple[TypedRoute[Any], ...] = ()
    control_route: TypedRoute[Any] | None = None
    error_route: TypedRoute[Any] | None = None
    on_control: GraphNodeControlHandler | None = None
    map_error: GraphNodeErrorMapper = lambda exc, _attempt: exc
    retry: RetryPolicy = field(
        default_factory=lambda: RetryPolicy(max_attempts=1_000_000)
    )
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    clock: Clock = field(default_factory=SystemClock)
    group: str | None = None
    start_immediately: bool = True
    daemon: bool = True

    def __post_init__(self) -> None:
        self.name = _require_non_empty_string(self.name, "managed graph node name")
        _require_callable(self.body, "managed graph node body")
        self.output_routes = _require_typed_route_tuple(
            self.output_routes, "managed graph node output_routes"
        )
        self.control_route = _require_optional_typed_route(
            self.control_route, "managed graph node control_route"
        )
        self.error_route = _require_optional_typed_route(
            self.error_route, "managed graph node error_route"
        )
        if self.on_control is not None:
            _require_callable(self.on_control, "managed graph node on_control")
        _require_callable(self.map_error, "managed graph node map_error")
        if not isinstance(self.retry, RetryPolicy):
            raise ValueError("managed graph node retry must be a RetryPolicy")
        if not isinstance(self.backoff, BackoffPolicy):
            raise ValueError("managed graph node backoff must be a BackoffPolicy")
        _require_clock(self.clock, "managed graph node clock")
        self.group = _require_optional_string(self.group, "managed graph node group")
        self.start_immediately = _require_bool(
            self.start_immediately, "managed graph node start_immediately"
        )
        self.daemon = _require_bool(self.daemon, "managed graph node daemon")
        _adopt_group(self.clock, self.group)
        if self.control_route is not None and self.on_control is None:
            raise ValueError("on_control is required when control_route is provided")

    def install(self, graph: Graph) -> ManagedGraphNodeHandle:
        diagram_outputs = tuple(self.output_routes) + (
            (self.error_route,) if self.error_route is not None else ()
        )
        graph.register_diagram_node(
            self.name,
            input_routes=() if self.control_route is None else (self.control_route,),
            output_routes=diagram_outputs,
            group=self.group,
        )

        def on_error(exc: BaseException, attempt: int) -> None:
            if self.error_route is not None:
                graph.publish(self.error_route, self.map_error(exc, attempt))

        loop = ManagedRunLoop(
            body=lambda stop: self.body(stop, graph),
            retry=self.retry,
            backoff=self.backoff,
            on_error=on_error,
            group=self.group,
        )
        control_subscription = None
        if self.control_route is not None and self.on_control is not None:
            control_subscription = graph.observe(
                self.control_route,
                replay_latest=False,
            ).subscribe(lambda item: self.on_control(item, graph))

        if self.start_immediately:
            loop_handle = loop.start_thread(name=self.name, daemon=self.daemon)
        else:
            token = StopToken(group=self.group)
            loop_handle = ManagedRunLoopHandle(
                loop=loop,
                token=token,
                thread=threading.Thread(
                    target=loop.run,
                    args=(token,),
                    name=self.name,
                    daemon=self.daemon,
                ),
            )
        return ManagedGraphNodeHandle(
            node=self,
            graph=graph,
            loop_handle=loop_handle,
            control_subscription=control_subscription,
        )


@dataclass
class DetectionNode:
    """Run a detector as a graph source.

    A detector that finds nothing emits nothing. Detected items are optionally
    mapped before publication. The node also receives graph access as an
    explicit capability, so it can own downstream source nodes without making
    graph mutation ambient or renderer-specific.
    """

    name: str
    detector: Detector
    output_route: TypedRoute[Any]
    mapper: DetectionMapper = lambda item: item
    on_detect: DetectionCallback | None = None
    spawn: DetectionSpawner | None = None
    error_route: TypedRoute[Any] | None = None
    retry: RetryPolicy = field(default_factory=RetryPolicy.never)
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    group: str | None = None
    start_immediately: bool = True
    daemon: bool = True

    def __post_init__(self) -> None:
        self.name = _require_non_empty_string(self.name, "detection node name")
        _require_callable(self.detector, "detection node detector")
        self.output_route = _require_typed_route(
            self.output_route, "detection node output_route"
        )
        _require_callable(self.mapper, "detection node mapper")
        if self.on_detect is not None:
            _require_callable(self.on_detect, "detection node on_detect")
        if self.spawn is not None:
            _require_callable(self.spawn, "detection node spawn")
        self.error_route = _require_optional_typed_route(
            self.error_route, "detection node error_route"
        )
        if not isinstance(self.retry, RetryPolicy):
            raise ValueError("detection node retry must be a RetryPolicy")
        if not isinstance(self.backoff, BackoffPolicy):
            raise ValueError("detection node backoff must be a BackoffPolicy")
        self.group = _require_optional_string(self.group, "detection node group")
        self.start_immediately = _require_bool(
            self.start_immediately, "detection node start_immediately"
        )
        self.daemon = _require_bool(self.daemon, "detection node daemon")

    def install(self, graph: Graph) -> "DetectionNodeHandle":
        graph_access = GraphAccessNode(graph=graph)

        def body(stop: StopToken, graph: Graph) -> None:
            for item in self.detector():
                if stop.is_set():
                    break
                if self.on_detect is not None:
                    self.on_detect(item, graph_access)
                graph_access.publish(self.output_route, self.mapper(item))
                if self.spawn is not None:
                    graph_access.own(self.spawn(item, graph_access))
            stop.set()

        node_handle = ManagedGraphNode(
            name=self.name,
            body=body,
            output_routes=(self.output_route,),
            error_route=self.error_route,
            retry=self.retry,
            backoff=self.backoff,
            group=self.group,
            start_immediately=self.start_immediately,
            daemon=self.daemon,
        ).install(graph)
        return DetectionNodeHandle(
            node=self,
            graph=graph,
            node_handle=node_handle,
            graph_access=graph_access,
        )


@dataclass
class DetectionNodeHandle:
    """Installed detection node plus downstream nodes spawned by detections."""

    node: DetectionNode
    graph: Graph
    node_handle: ManagedGraphNodeHandle
    graph_access: GraphAccessNode
    disposed: bool = False

    @property
    def spawned_handles(self) -> list[Any]:
        return self.graph_access.owned_handles

    @property
    def loop_handle(self) -> ManagedRunLoopHandle:
        return self.node_handle.loop_handle

    @property
    def group(self) -> str | None:
        return self.node.group

    def stop(self) -> None:
        self.node_handle.stop()
        for handle in tuple(self.spawned_handles):
            _call_if_present(handle, "stop")

    def join(self, timeout: float | None = None) -> None:
        self.node_handle.join(timeout=timeout)
        for handle in tuple(self.spawned_handles):
            _call_if_present(handle, "join", timeout)

    def dispose(self, timeout: float | None = None) -> None:
        if self.disposed:
            return
        for handle in reversed(tuple(self.spawned_handles)):
            _dispose_handle(handle, timeout=timeout)
        self.node_handle.dispose(timeout=timeout)
        self.disposed = True


@dataclass
class BoundedRingBuffer(Generic[T]):
    """Bounded local staging buffer with explicit overflow semantics."""

    capacity: int
    overflow: OverflowMode = "drop_oldest"
    ordering: Literal["fifo"] = "fifo"
    group: str | None = None
    _items: deque[T] = field(default_factory=deque, init=False, repr=False)
    dropped: int = 0
    rejected: int = 0

    def __post_init__(self) -> None:
        self.capacity = _require_int(self.capacity, "capacity")
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.overflow not in {"drop_oldest", "drop_newest", "reject", "latest"}:
            raise ValueError(
                "overflow must be one of 'drop_oldest', 'drop_newest', 'reject', or 'latest'"
            )
        if self.ordering != "fifo":
            raise ValueError("only fifo ordering is currently supported")

    def push(self, item: T) -> bool:
        if len(self._items) < self.capacity:
            self._items.append(item)
            return True
        if self.overflow == "reject":
            self.rejected += 1
            return False
        if self.overflow == "drop_newest":
            self.dropped += 1
            return False
        if self.overflow == "latest":
            self.dropped += len(self._items)
            self._items.clear()
        else:
            self._items.popleft()
            self.dropped += 1
        self._items.append(item)
        return True

    def pop(self) -> T | None:
        if not self._items:
            return None
        return self._items.popleft()

    def drain(self) -> tuple[T, ...]:
        items = tuple(self._items)
        self._items.clear()
        return items

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[T]:
        return iter(tuple(self._items))


@dataclass
class SequenceCounter:
    """Monotonic local sequence-number component."""

    current: int = 0
    step: int = 1
    group: str | None = None

    def __post_init__(self) -> None:
        self.current = _require_int(self.current, "current")
        self.step = _require_int(self.step, "step")
        if self.step <= 0:
            raise ValueError("step must be positive")

    def next(self) -> int:
        self.current += self.step
        return self.current

    def peek(self) -> int:
        return self.current

    def reset(self, value: int = 0) -> None:
        self.current = _require_int(value, "current")


@dataclass(frozen=True)
class SensorTag:
    """Small metadata tag for a physical or logical sensor."""

    name: str
    variant: str
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_string(self.name, "tag.name"))
        object.__setattr__(
            self,
            "variant",
            _require_string(self.variant, "tag.variant"),
        )
        metadata = _require_mapping(self.metadata, "tag.metadata")
        object.__setattr__(
            self,
            "metadata",
            {
                _require_string(key, "tag.metadata key"): _require_string(
                    value,
                    "tag.metadata value",
                )
                for key, value in metadata.items()
            },
        )


@dataclass(frozen=True)
class SensorLocation:
    """Physical-space coordinate for a sensor relative to its installation."""

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    timestamp: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "x", _require_finite_number(self.x, "location.x"))
        object.__setattr__(self, "y", _require_finite_number(self.y, "location.y"))
        object.__setattr__(self, "z", _require_finite_number(self.z, "location.z"))
        if self.timestamp is not None:
            object.__setattr__(
                self,
                "timestamp",
                _require_finite_number(self.timestamp, "location.timestamp"),
            )


@dataclass(frozen=True)
class SensorIdentity:
    """Identity and metadata for a sensor or peripheral source."""

    id: str | None = None
    tags: tuple[SensorTag, ...] = ()
    location: SensorLocation = field(default_factory=SensorLocation)
    group: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "id",
            _require_optional_string(self.id, "identity.id"),
        )
        try:
            tags = tuple(self.tags)
        except TypeError as exc:
            raise ValueError("identity.tags must be iterable") from exc
        if not all(isinstance(tag, SensorTag) for tag in tags):
            raise ValueError("identity.tags[] must be a SensorTag")
        object.__setattr__(self, "tags", tags)
        if not isinstance(self.location, SensorLocation):
            raise ValueError("identity.location must be a SensorLocation")
        object.__setattr__(
            self,
            "group",
            _require_optional_string(self.group, "identity.group"),
        )


@dataclass(frozen=True)
class SensorSample(Generic[T]):
    """Normalized local sensor reading published as a typed payload."""

    value: T
    source_timestamp: float
    ingest_timestamp: float
    sequence_number: int
    quality: str | None = None
    status: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_timestamp",
            _require_finite_number(self.source_timestamp, "source_timestamp"),
        )
        object.__setattr__(
            self,
            "ingest_timestamp",
            _require_finite_number(self.ingest_timestamp, "ingest_timestamp"),
        )
        sequence_number = _require_int(self.sequence_number, "sequence_number")
        if sequence_number < 0:
            raise ValueError("sequence_number must be non-negative")
        object.__setattr__(self, "sequence_number", sequence_number)
        object.__setattr__(
            self,
            "quality",
            _require_optional_string(self.quality, "quality"),
        )
        object.__setattr__(
            self,
            "status",
            _require_optional_string(self.status, "status"),
        )


@dataclass(frozen=True)
class SensorEvent:
    """Normalized event payload for Heart-style sensor and peripheral streams."""

    event_type: str
    data: Any
    observed_at: float
    identity: SensorIdentity = field(default_factory=SensorIdentity)
    sequence_number: int | None = None
    raw: bytes | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        event_type = _require_string(self.event_type, "event_type")
        if not event_type.strip():
            raise ValueError("event_type must be a non-empty string")
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(
            self,
            "observed_at",
            _require_finite_number(self.observed_at, "observed_at"),
        )
        if not isinstance(self.identity, SensorIdentity):
            raise ValueError("identity must be a SensorIdentity")
        if self.sequence_number is not None:
            sequence_number = _require_int(self.sequence_number, "sequence_number")
            if sequence_number < 0:
                raise ValueError("sequence_number must be non-negative")
            object.__setattr__(self, "sequence_number", sequence_number)
        if self.raw is not None:
            if not isinstance(self.raw, bytes | bytearray | memoryview):
                raise ValueError("raw must be bytes-like")
            object.__setattr__(self, "raw", bytes(self.raw))
        object.__setattr__(
            self,
            "metadata",
            _require_mapping(self.metadata, "metadata"),
        )


@dataclass(frozen=True)
class HealthStatus:
    """Local health event emitted by sensor watchdogs."""

    status: Literal["ok", "stale", "error"]
    observed_at: float
    message: str = ""
    stale: bool = False
    error_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", _decode_health_status(self.status))
        object.__setattr__(
            self,
            "observed_at",
            _require_finite_number(self.observed_at, "observed_at"),
        )
        if not isinstance(self.message, str):
            raise ValueError("message must be a string")
        if not isinstance(self.stale, bool):
            raise ValueError("stale must be a boolean")
        object.__setattr__(
            self,
            "error_count",
            _require_int(self.error_count, "error_count"),
        )
        if self.error_count < 0:
            raise ValueError("error_count must be non-negative")


class SensorDebugStage(str, Enum):
    """Observable stages for sensor input debugging."""

    RAW = "raw"
    VIEW = "view"
    LOGICAL = "logical"
    FRAME = "frame"


@dataclass(frozen=True)
class SensorDebugEnvelope:
    """One retained debug event for a sensor input stream."""

    stage: SensorDebugStage
    stream_name: str
    source_id: str
    timestamp: float
    payload: Any
    upstream_ids: tuple[str, ...] = ()


@dataclass
class SensorDebugTap:
    """Small in-memory debug tap for sensor emissions."""

    clock: Clock = field(default_factory=SystemClock)
    history_size: int = 512
    _history: deque[SensorDebugEnvelope] = field(
        default_factory=deque, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.history_size = _require_int(self.history_size, "history_size")
        if self.history_size <= 0:
            raise ValueError("history_size must be positive")
        self._history = deque(maxlen=self.history_size)

    def publish(
        self,
        *,
        stage: SensorDebugStage,
        stream_name: str,
        source_id: str,
        payload: Any,
        upstream_ids: Iterable[str] = (),
    ) -> SensorDebugEnvelope:
        envelope = SensorDebugEnvelope(
            stage=stage,
            stream_name=stream_name,
            source_id=source_id,
            timestamp=self.clock.now(),
            payload=payload,
            upstream_ids=tuple(upstream_ids),
        )
        if self._history.maxlen != self.history_size:
            self.history_size = _require_int(self.history_size, "history_size")
            if self.history_size <= 0:
                raise ValueError("history_size must be positive")
            self._history = deque(self._history, maxlen=self.history_size)
        self._history.append(envelope)
        return envelope

    def snapshot(self) -> tuple[SensorDebugEnvelope, ...]:
        return tuple(self._history)


@dataclass
class DelimitedMessageBuffer:
    """Incrementally buffer delimited bytes or text messages."""

    delimiter: bytes = b"\n"
    mode: MessageBufferMode = "bytes"
    _bytes_buffer: bytearray = field(default_factory=bytearray, init=False, repr=False)
    _text_buffer: str = field(default="", init=False, repr=False)
    _text_decoder: codecs.IncrementalDecoder = field(
        default_factory=lambda: codecs.getincrementaldecoder("utf-8")(errors="strict"),
        init=False,
        repr=False,
    )
    _text_delimiter: str = field(default="", init=False, repr=False)
    _delimiter_len: int = field(default=0, init=False, repr=False)
    _text_delimiter_len: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.delimiter = _require_bytes_like(self.delimiter, "delimiter")
        if not self.delimiter:
            raise ValueError("delimiter must not be empty")
        if self.mode not in ("bytes", "text"):
            raise ValueError("mode must be 'bytes' or 'text'")
        self._delimiter_len = len(self.delimiter)
        if self.mode == "text":
            try:
                self._text_delimiter = self.delimiter.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError("text delimiter must be valid UTF-8") from exc
            self._text_delimiter_len = len(self._text_delimiter)

    @property
    def buffer_size(self) -> int:
        if self.mode == "text":
            return len(self._text_buffer)
        return len(self._bytes_buffer)

    def append(
        self,
        data: bytes | bytearray | memoryview | str,
    ) -> tuple[bytes | str, ...]:
        if self.mode == "text":
            if isinstance(data, str):
                if self._text_decoder.getstate()[0]:
                    raise ValueError(
                        "cannot append text while a UTF-8 byte sequence is pending"
                    )
                self._text_buffer += data
            else:
                self._text_buffer += self._text_decoder.decode(
                    _require_bytes_like(data, "data"),
                    final=False,
                )
            return tuple(self._drain_text())
        if isinstance(data, str):
            self._bytes_buffer.extend(data.encode("utf-8"))
        else:
            self._bytes_buffer.extend(_require_bytes_like(data, "data"))
        return tuple(self._drain_bytes())

    def clear(self) -> None:
        self._bytes_buffer.clear()
        self._text_buffer = ""
        self._text_decoder.reset()

    def _drain_bytes(self) -> list[bytes]:
        messages: list[bytes] = []
        while True:
            index = self._bytes_buffer.find(self.delimiter)
            if index < 0:
                return messages
            messages.append(bytes(self._bytes_buffer[:index]))
            del self._bytes_buffer[: index + self._delimiter_len]

    def _drain_text(self) -> list[str]:
        messages: list[str] = []
        while True:
            index = self._text_buffer.find(self._text_delimiter)
            if index < 0:
                return messages
            messages.append(self._text_buffer[:index])
            self._text_buffer = self._text_buffer[index + self._text_delimiter_len :]


@dataclass
class JsonEventDecoder:
    """Decode newline-framed JSON messages into ``SensorEvent`` values."""

    clock: Clock = field(default_factory=SystemClock)
    identity: SensorIdentity = field(default_factory=SensorIdentity)
    sequence: SequenceCounter = field(default_factory=SequenceCounter)
    default_event_type: str = "sensor.event"
    group: str | None = None

    def __post_init__(self) -> None:
        _require_clock(self.clock, "json event decoder clock")
        self.identity = _require_sensor_identity(
            self.identity,
            "json event decoder identity",
        )
        self.sequence = _require_sequence_counter(
            self.sequence,
            "json event decoder sequence",
        )
        self.default_event_type = _require_non_empty_string(
            self.default_event_type,
            "default_event_type",
        )
        self.group = _require_optional_string(self.group, "group")
        _adopt_group(self.clock, self.group)
        _adopt_group(self.sequence, self.group)
        if self.group is not None and self.identity.group is None:
            self.identity = SensorIdentity(
                id=self.identity.id,
                tags=self.identity.tags,
                location=self.identity.location,
                group=self.group,
            )

    def decode(
        self,
        message: bytes | bytearray | memoryview | str,
    ) -> SensorEvent | None:
        raw = (
            message.encode("utf-8")
            if isinstance(message, str)
            else _require_bytes_like(message, "message")
        )
        try:
            text = raw.decode("utf-8").strip()
        except UnicodeDecodeError:
            return None
        if not text:
            return None
        try:
            parsed = json.loads(text, parse_constant=_reject_json_constant)
        except (json.JSONDecodeError, ValueError):
            return None
        if not isinstance(parsed, Mapping):
            return None
        event_type_value = parsed.get("event_type", self.default_event_type)
        if (
            event_type_value is None
            or (isinstance(event_type_value, str) and not event_type_value.strip())
        ):
            event_type = self.default_event_type
        elif isinstance(event_type_value, str):
            event_type = event_type_value
        else:
            return None
        data = parsed.get("data", {})
        return SensorEvent(
            event_type=event_type,
            data=data,
            observed_at=self.clock.now(),
            identity=self.identity,
            sequence_number=self.sequence.next(),
            raw=raw,
            metadata={
                key: value
                for key, value in parsed.items()
                if key not in {"event_type", "data"}
            },
        )


@dataclass
class ChangeFilter(Generic[T]):
    """Allow first value and later values that differ from the previous value."""

    key: Callable[[T], Any] = lambda value: value
    _last: Any = field(default=None, init=False, repr=False)
    _has_last: bool = field(default=False, init=False, repr=False)

    def accepts(self, value: T) -> bool:
        current = self.key(value)
        if not self._has_last or current != self._last:
            self._last = current
            self._has_last = True
            return True
        return False


@dataclass
class ThresholdFilter(Generic[T]):
    """Allow values whose numeric delta exceeds a threshold."""

    threshold: float
    key: Callable[[T], Any] = lambda value: value
    _last: Any = field(default=None, init=False, repr=False)
    _has_last: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.threshold = _require_finite_number(self.threshold, "threshold")
        if self.threshold < 0:
            raise ValueError("threshold must be non-negative")

    def accepts(self, value: T) -> bool:
        current = self.key(value)
        if not self._has_last:
            self._last = current
            self._has_last = True
            return True
        if _changed_enough(current, self._last, self.threshold):
            self._last = current
            return True
        return False


@dataclass(frozen=True)
class SensorFrame(Generic[TFrame]):
    """A completed frame assembled from multiple sensor samples."""

    frame_id: Any
    samples: tuple[TFrame, ...]


@dataclass
class DoubleBuffer(Generic[TFrame]):
    """Double-buffered packet staging for bursty sample capture."""

    buffer_size: int
    group: str | None = None
    _buffers: list[list[TFrame]] = field(
        default_factory=lambda: [[], []], init=False, repr=False
    )
    _active: int = 0
    _ready: deque[tuple[int, tuple[TFrame, ...]]] = field(
        default_factory=deque, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.buffer_size = _require_int(self.buffer_size, "buffer_size")
        if self.buffer_size <= 0:
            raise ValueError("buffer_size must be positive")

    def push(self, sample: TFrame) -> tuple[int, tuple[TFrame, ...]] | None:
        buffer = self._buffers[self._active]
        buffer.append(sample)
        if len(buffer) < self.buffer_size:
            return None
        return self._finalize_active()

    def flush(self) -> tuple[int, tuple[TFrame, ...]] | None:
        if not self._buffers[self._active]:
            return None
        return self._finalize_active()

    def pop(self) -> tuple[int, tuple[TFrame, ...]] | None:
        if not self._ready:
            return None
        return self._ready.popleft()

    def _finalize_active(self) -> tuple[int, tuple[TFrame, ...]]:
        buffer_id = self._active
        samples = tuple(self._buffers[buffer_id])
        self._buffers[buffer_id] = []
        self._active = 1 - self._active
        packet = (buffer_id, samples)
        self._ready.append(packet)
        return packet


@dataclass
class FrameAssembler(Generic[TFrame]):
    """Aggregate samples into frames by frame id and slot id."""

    expected_count: int
    frame_id: Callable[[TFrame], Any]
    slot_id: Callable[[TFrame], Any]
    _pending: dict[Any, dict[Any, TFrame]] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.expected_count = _require_int(self.expected_count, "expected_count")
        if self.expected_count <= 0:
            raise ValueError("expected_count must be positive")

    def add(self, sample: TFrame) -> tuple[SensorFrame[TFrame], ...]:
        resolved_frame_id = self.frame_id(sample)
        bucket = self._pending.setdefault(resolved_frame_id, {})
        bucket[self.slot_id(sample)] = sample
        if len(bucket) < self.expected_count:
            return ()
        samples = tuple(
            bucket[key]
            for key in sorted(bucket, key=_mapping_key_sort_key)[: self.expected_count]
        )
        del self._pending[resolved_frame_id]
        return (SensorFrame(frame_id=resolved_frame_id, samples=samples),)


@dataclass
class SensorSourceHandle(Generic[T]):
    """Installed local sensor source bound to one graph."""

    source: LocalSensorSource[T]
    graph: Graph
    disposed: bool = False

    @property
    def group(self) -> str | None:
        return self.source.group

    def poll(self) -> SensorSample[T] | None:
        if self.disposed:
            return None
        return self.source.poll(self.graph)

    def dispose(self) -> None:
        self.disposed = True


@dataclass
class LocalSensorSource(Generic[T]):
    """Read a local sensor function and publish normalized samples."""

    route: TypedRoute[SensorSample[T]]
    read: Callable[[], T]
    clock: Clock = field(default_factory=SystemClock)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    sequence: SequenceCounter = field(default_factory=SequenceCounter)
    group: str | None = None
    quality: str | None = None
    status: str | None = None
    error_count: int = 0

    def __post_init__(self) -> None:
        self.route = _require_typed_route(self.route, "local sensor source route")
        _require_callable(self.read, "local sensor source read")
        _require_clock(self.clock, "local sensor source clock")
        self.retry = _require_retry_policy(self.retry, "local sensor source retry")
        self.backoff = _require_backoff_policy(
            self.backoff, "local sensor source backoff"
        )
        self.sequence = _require_sequence_counter(
            self.sequence, "local sensor source sequence"
        )
        self.group = _require_optional_string(self.group, "local sensor source group")
        self.quality = _require_optional_string(
            self.quality, "local sensor source quality"
        )
        self.status = _require_optional_string(
            self.status, "local sensor source status"
        )
        self.error_count = _require_int(
            self.error_count, "local sensor source error_count"
        )
        if self.error_count < 0:
            raise ValueError("local sensor source error_count must be non-negative")
        _adopt_group(self.sequence, self.group)
        _adopt_group(self.clock, self.group)

    def install(self, graph: Graph, *, read_now: bool = True) -> SensorSourceHandle[T]:
        handle = SensorSourceHandle(source=self, graph=graph)
        if read_now:
            handle.poll()
        return handle

    def poll(self, graph: Graph) -> SensorSample[T]:
        loop = RetryLoop(retry=self.retry, backoff=self.backoff, group=self.group)
        try:
            value = loop.run(self.read)
        except Exception:
            self.error_count += 1
            raise
        now = self.clock.now()
        sample = SensorSample(
            value=value,
            source_timestamp=now,
            ingest_timestamp=now,
            sequence_number=self.sequence.next(),
            quality=self.quality,
            status=self.status,
        )
        graph.publish(self.route, sample)
        return sample


@dataclass
class ReactiveSensorHandle:
    """Installed observable sensor source bound to one graph."""

    source: ReactiveSensorSource
    graph: Graph
    subscription: SubscriptionLike
    disposed: bool = False

    @property
    def group(self) -> str | None:
        return self.source.group

    def dispose(self) -> None:
        if not self.disposed:
            self.subscription.dispose()
            self.disposed = True


@dataclass
class ReactiveSensorSource:
    """Publish values from a reactivex-style observable into a route."""

    route: TypedRoute[Any]
    observable: Any
    mapper: Callable[[Any], Any] = lambda value: value
    wrap_sample: bool = False
    clock: Clock = field(default_factory=SystemClock)
    sequence: SequenceCounter = field(default_factory=SequenceCounter)
    identity: SensorIdentity = field(default_factory=SensorIdentity)
    group: str | None = None
    debug_tap: SensorDebugTap | None = None
    debug_stage: SensorDebugStage = SensorDebugStage.RAW
    stream_name: str | None = None
    source_id: str | None = None

    def __post_init__(self) -> None:
        self.route = _require_typed_route(self.route, "reactive sensor source route")
        _require_observable(self.observable, "reactive sensor source observable")
        _require_callable(self.mapper, "reactive sensor source mapper")
        self.wrap_sample = _require_bool(
            self.wrap_sample, "reactive sensor source wrap_sample"
        )
        _require_clock(self.clock, "reactive sensor source clock")
        self.sequence = _require_sequence_counter(
            self.sequence, "reactive sensor source sequence"
        )
        self.identity = _require_sensor_identity(
            self.identity, "reactive sensor source identity"
        )
        self.group = _require_optional_string(self.group, "reactive sensor source group")
        self.debug_tap = _require_optional_debug_tap(
            self.debug_tap, "reactive sensor source debug_tap"
        )
        self.debug_stage = _require_sensor_debug_stage(
            self.debug_stage, "reactive sensor source debug_stage"
        )
        self.stream_name = _require_optional_string(
            self.stream_name, "reactive sensor source stream_name"
        )
        self.source_id = _require_optional_string(
            self.source_id, "reactive sensor source source_id"
        )
        _adopt_group(self.clock, self.group)
        _adopt_group(self.sequence, self.group)

    def install(self, graph: Graph) -> ReactiveSensorHandle:
        def on_next(item: Any) -> None:
            value = self.mapper(item)
            now = self.clock.now()
            payload = (
                SensorSample(
                    value=value,
                    source_timestamp=now,
                    ingest_timestamp=now,
                    sequence_number=self.sequence.next(),
                )
                if self.wrap_sample
                else value
            )
            if self.debug_tap is not None:
                self.debug_tap.publish(
                    stage=self.debug_stage,
                    stream_name=self.stream_name or self.route.stream.value,
                    source_id=self.source_id
                    or self.identity.id
                    or self.group
                    or self.route.owner.value,
                    payload=payload,
                )
            graph.publish(self.route, payload)

        subscription = self.observable.subscribe(on_next)
        return ReactiveSensorHandle(source=self, graph=graph, subscription=subscription)


@dataclass
class PeripheralAdapterHandle:
    """Installed Heart-style peripheral adapter."""

    adapter: PeripheralAdapter
    graph: Graph
    subscription: SubscriptionLike
    control_subscription: SubscriptionLike | None = None
    disposed: bool = False

    @property
    def group(self) -> str | None:
        return self.adapter.group

    def dispose(self) -> None:
        if self.disposed:
            return
        self.subscription.dispose()
        if self.control_subscription is not None:
            self.control_subscription.dispose()
        if self.adapter.stop_on_dispose:
            _call_if_present(self.adapter.peripheral, "stop")
            _call_if_present(self.adapter.peripheral, "close")
        self.disposed = True


@dataclass
class PeripheralAdapter:
    """Adapt a Heart-style ``Peripheral.observe`` stream into a Manyfold route."""

    peripheral: Any
    route: TypedRoute[Any]
    mapper: Callable[[Any], Any] | None = None
    event_type: str | None = None
    wrap_sample: bool = False
    control_route: TypedRoute[Any] | None = None
    clock: Clock = field(default_factory=SystemClock)
    sequence: SequenceCounter = field(default_factory=SequenceCounter)
    identity: SensorIdentity | None = None
    group: str | None = None
    run_on_install: bool = True
    stop_on_dispose: bool = False
    debug_tap: SensorDebugTap | None = None

    def __post_init__(self) -> None:
        _require_peripheral(self.peripheral, "peripheral adapter peripheral")
        self.route = _require_typed_route(self.route, "peripheral adapter route")
        if self.mapper is not None:
            _require_callable(self.mapper, "peripheral adapter mapper")
        self.event_type = _require_optional_string(
            self.event_type, "peripheral adapter event_type"
        )
        self.wrap_sample = _require_bool(
            self.wrap_sample, "peripheral adapter wrap_sample"
        )
        self.control_route = _require_optional_typed_route(
            self.control_route, "peripheral adapter control_route"
        )
        _require_clock(self.clock, "peripheral adapter clock")
        self.sequence = _require_sequence_counter(
            self.sequence, "peripheral adapter sequence"
        )
        if self.identity is not None:
            self.identity = _require_sensor_identity(
                self.identity, "peripheral adapter identity"
            )
        self.group = _require_optional_string(self.group, "peripheral adapter group")
        self.run_on_install = _require_bool(
            self.run_on_install, "peripheral adapter run_on_install"
        )
        self.stop_on_dispose = _require_bool(
            self.stop_on_dispose, "peripheral adapter stop_on_dispose"
        )
        self.debug_tap = _require_optional_debug_tap(
            self.debug_tap, "peripheral adapter debug_tap"
        )
        _adopt_group(self.clock, self.group)
        _adopt_group(self.sequence, self.group)

    def install(self, graph: Graph) -> PeripheralAdapterHandle:
        def on_next(item: Any) -> None:
            identity = self.identity or _identity_from_peripheral_item(item, self.group)
            payload = self._payload_for_item(item, identity)
            if self.debug_tap is not None:
                self.debug_tap.publish(
                    stage=SensorDebugStage.RAW,
                    stream_name=self.route.stream.value,
                    source_id=identity.id or self.group or self.route.owner.value,
                    payload=payload,
                )
            graph.publish(self.route, payload)

        subscription = self.peripheral.observe.subscribe(on_next)
        control_subscription = None
        if self.control_route is not None:

            def on_control(item: Any) -> None:
                _call_if_present(self.peripheral, "handle_input", _event_value(item))

            try:
                control_subscription = graph.observe(
                    self.control_route, replay_latest=False
                ).subscribe(on_control)
            except Exception:
                subscription.dispose()
                raise
        if self.run_on_install:
            try:
                _call_if_present(self.peripheral, "run")
            except BaseException:
                subscription.dispose()
                if control_subscription is not None:
                    control_subscription.dispose()
                raise
        return PeripheralAdapterHandle(
            adapter=self,
            graph=graph,
            subscription=subscription,
            control_subscription=control_subscription,
        )

    def _payload_for_item(self, item: Any, identity: SensorIdentity) -> Any:
        if self.mapper is not None:
            payload = self.mapper(item)
        elif self.event_type is not None:
            payload = SensorEvent(
                event_type=self.event_type,
                data=_unwrap_peripheral_data(item),
                observed_at=self.clock.now(),
                identity=identity,
                sequence_number=self.sequence.next(),
            )
        else:
            payload = _unwrap_peripheral_data(item)

        if not self.wrap_sample:
            return payload
        now = self.clock.now()
        return SensorSample(
            value=payload,
            source_timestamp=now,
            ingest_timestamp=now,
            sequence_number=self.sequence.next(),
        )


@dataclass
class DuplexSensorPeripheral(PeripheralAdapter):
    """Named adapter for peripherals that both emit events and accept commands."""


@dataclass
class RateMatchedSensor:
    """Install bounded rate matching between a raw sensor route and a sink route."""

    source: TypedRoute[Any]
    sink: TypedRoute[Any]
    capacity: int
    mode: SensorBufferMode = "latest"
    demand: TypedRoute[Any] | None = None
    clock: Clock = field(default_factory=SystemClock)
    name: str | None = None
    group: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.source, TypedRoute):
            raise ValueError("rate matched sensor source must be a TypedRoute")
        if not isinstance(self.sink, TypedRoute):
            raise ValueError("rate matched sensor sink must be a TypedRoute")
        if self.demand is not None and not isinstance(self.demand, TypedRoute):
            raise ValueError("rate matched sensor demand must be a TypedRoute")
        self.capacity = _require_int(self.capacity, "capacity")
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.mode not in {"latest", "fifo"}:
            raise ValueError("mode must be 'latest' or 'fifo'")
        if not callable(getattr(self.clock, "now", None)):
            raise ValueError("rate matched sensor clock must provide now()")
        self.name = _require_optional_string(self.name, "name")
        self.group = _require_optional_string(self.group, "group")
        _adopt_group(self.clock, self.group)

    def install(self, graph: Graph) -> Any:
        if self.mode == "latest":
            # Keep one latest value no matter what capacity was requested.
            capacity = 1
            overflow = "latest"
        else:
            capacity = self.capacity
            overflow = "drop_oldest"
        return graph.capacitor(
            source=self.source,
            sink=self.sink,
            capacity=capacity,
            demand=self.demand,
            immediate=self.demand is None,
            overflow=overflow,
            name=self.name or _group_name(self.group, "rate_matched_sensor"),
        )


@dataclass
class SensorHealthHandle:
    """Installed health watchdog bound to one graph."""

    watchdog: SensorHealthWatchdog
    graph: Graph
    subscription: SubscriptionLike
    last_seen: float | None = None
    error_count: int = 0
    disposed: bool = False

    @property
    def group(self) -> str | None:
        return self.watchdog.group

    def check(self) -> HealthStatus:
        now = self.watchdog.clock.now()
        stale = (
            self.last_seen is None or now - self.last_seen >= self.watchdog.stale_after
        )
        if stale:
            status = HealthStatus(
                status="stale",
                observed_at=now,
                message=self.watchdog.stale_message,
                stale=True,
                error_count=self.error_count,
            )
        else:
            status = HealthStatus(
                status="ok",
                observed_at=now,
                message="",
                stale=False,
                error_count=self.error_count,
            )
        self.graph.publish(self.watchdog.health_route, status)
        return status

    def dispose(self) -> None:
        if not self.disposed:
            self.subscription.dispose()
            self.disposed = True


@dataclass
class SensorHealthWatchdog:
    """Report stale sensor input after no samples arrive for a configured time."""

    source: TypedRoute[Any]
    health_route: TypedRoute[HealthStatus]
    stale_after: float
    clock: Clock = field(default_factory=SystemClock)
    stale_message: str = "sensor input is stale"
    group: str | None = None

    def __post_init__(self) -> None:
        self.source = _require_typed_route(self.source, "sensor health source")
        self.health_route = _require_typed_route(
            self.health_route, "sensor health route"
        )
        self.stale_after = _require_finite_number(self.stale_after, "stale_after")
        if self.stale_after < 0:
            raise ValueError("stale_after must be non-negative")
        _require_clock(self.clock, "sensor health clock")
        self.stale_message = _require_string(
            self.stale_message, "sensor health stale_message"
        )
        self.group = _require_optional_string(self.group, "sensor health group")
        _adopt_group(self.clock, self.group)

    def install(self, graph: Graph) -> SensorHealthHandle:
        handle = SensorHealthHandle(
            watchdog=self,
            graph=graph,
            subscription=_NoopSubscription(),
        )

        def on_sample(_sample: Any) -> None:
            handle.last_seen = self.clock.now()

        handle.subscription = graph.observe(self.source, replay_latest=False).subscribe(
            on_sample
        )
        return handle


@dataclass
class LocalDurableSpool(Generic[T]):
    """File-backed local retention for sensor samples using ``EventLog``."""

    name: str
    keyspace: Keyspace
    schema: Schema[T]
    group: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("local durable spool name must be a non-empty string")
        if not isinstance(self.keyspace, Keyspace):
            raise ValueError("local durable spool keyspace must be a Keyspace")
        if not isinstance(self.schema, Schema):
            raise ValueError("local durable spool schema must be a Schema")

    def event_log(self) -> EventLog[T]:
        return EventLog(self.name, self.keyspace, self.schema)

    def install(self, graph: Graph, source: TypedRoute[T]) -> SubscriptionLike:
        if not isinstance(source, TypedRoute):
            raise ValueError("local durable spool source must be a TypedRoute")
        log = self.event_log()
        log_subscription = log.install(graph)

        def append(sample: Any) -> None:
            graph.publish(
                log.input(), sample.value if hasattr(sample, "value") else sample
            )

        try:
            source_subscription = graph.observe(source, replay_latest=False).subscribe(
                append
            )
        except Exception:
            log_subscription.dispose()
            raise
        return _CompositeSubscription((log_subscription, source_subscription))

    def replay(self, graph: Graph) -> tuple[T, ...]:
        return tuple(record.value for record in self.event_log().replay(graph))


@dataclass
class _NoopSubscription:
    def dispose(self) -> None:
        return None


@dataclass
class _CompositeSubscription:
    subscriptions: tuple[SubscriptionLike, ...]
    disposed: bool = False

    def dispose(self) -> None:
        if self.disposed:
            return
        self.disposed = True
        first_error: Exception | None = None
        for subscription in self.subscriptions:
            try:
                subscription.dispose()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error


def _group_name(group: str | None, suffix: str) -> str | None:
    if group is None:
        return None
    return f"{group}.{suffix}"


def _adopt_group(component: object, group: str | None) -> None:
    if group is None:
        return
    if hasattr(component, "group") and getattr(component, "group") is None:
        setattr(component, "group", group)


def _changed_enough(new: Any, old: Any, threshold: float) -> bool:
    if isinstance(new, Mapping) and isinstance(old, Mapping):
        keys = sorted(set(new) | set(old), key=_mapping_key_sort_key)
        return any(
            _changed_enough(new.get(key), old.get(key), threshold) for key in keys
        )
    if isinstance(new, tuple) and isinstance(old, tuple):
        if len(new) != len(old):
            return True
        return any(_changed_enough(n, o, threshold) for n, o in zip(new, old))
    if isinstance(new, list) and isinstance(old, list):
        if len(new) != len(old):
            return True
        return any(_changed_enough(n, o, threshold) for n, o in zip(new, old))
    if isinstance(new, bool) or isinstance(old, bool):
        return new != old
    if isinstance(new, (int, float)) and isinstance(old, (int, float)):
        new_number = float(new)
        old_number = float(old)
        if not math.isfinite(new_number) or not math.isfinite(old_number):
            return new != old
        return abs(new_number - old_number) > threshold
    return new != old


def _require_finite_number(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be a finite number")
    return number


def _is_exception_class(value: object) -> bool:
    return isinstance(value, type) and issubclass(value, BaseException)


def _require_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    return value


def _require_string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value


def _require_non_empty_string(value: Any, field: str) -> str:
    text = _require_string(value, field)
    if not text.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return text


def _require_optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field)


def _require_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _require_bytes_like(value: Any, field: str) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError(f"{field} must be bytes-like")
    return bytes(value)


def _require_callable(value: Any, field: str) -> None:
    if not callable(value):
        raise ValueError(f"{field} must be callable")


def _require_observable(value: Any, field: str) -> None:
    if not callable(getattr(value, "subscribe", None)):
        raise ValueError(f"{field} must provide subscribe()")


def _require_peripheral(value: Any, field: str) -> None:
    observe = getattr(value, "observe", None)
    if not callable(getattr(observe, "subscribe", None)):
        raise ValueError(f"{field} must provide observe.subscribe()")


def _require_clock(value: Any, field: str) -> None:
    if not callable(getattr(value, "now", None)):
        raise ValueError(f"{field} must provide now()")


def _require_retry_policy(value: Any, field: str) -> RetryPolicy:
    if not isinstance(value, RetryPolicy):
        raise ValueError(f"{field} must be a RetryPolicy")
    return value


def _require_backoff_policy(value: Any, field: str) -> BackoffPolicy:
    if not isinstance(value, BackoffPolicy):
        raise ValueError(f"{field} must be a BackoffPolicy")
    return value


def _require_sequence_counter(value: Any, field: str) -> SequenceCounter:
    if not isinstance(value, SequenceCounter):
        raise ValueError(f"{field} must be a SequenceCounter")
    return value


def _require_sensor_identity(value: Any, field: str) -> SensorIdentity:
    if not isinstance(value, SensorIdentity):
        raise ValueError(f"{field} must be a SensorIdentity")
    return value


def _require_optional_debug_tap(value: Any, field: str) -> SensorDebugTap | None:
    if value is None:
        return None
    if not isinstance(value, SensorDebugTap):
        raise ValueError(f"{field} must be a SensorDebugTap")
    return value


def _require_sensor_debug_stage(value: Any, field: str) -> SensorDebugStage:
    if not isinstance(value, SensorDebugStage):
        raise ValueError(f"{field} must be a SensorDebugStage")
    return value


def _require_typed_route(value: Any, field: str) -> TypedRoute[Any]:
    if not isinstance(value, TypedRoute):
        raise ValueError(f"{field} must be a TypedRoute")
    return value


def _require_optional_typed_route(value: Any, field: str) -> TypedRoute[Any] | None:
    if value is None:
        return None
    return _require_typed_route(value, field)


def _require_typed_route_tuple(value: Any, field: str) -> tuple[TypedRoute[Any], ...]:
    if isinstance(value, str):
        raise ValueError(f"{field} must be a tuple of TypedRoute values")
    try:
        routes = tuple(value)
    except TypeError as exc:
        raise ValueError(f"{field} must be a tuple of TypedRoute values") from exc
    if not all(isinstance(route_ref, TypedRoute) for route_ref in routes):
        raise ValueError(f"{field} must contain only TypedRoute values")
    return routes


def _require_mapping(value: Any, field: str) -> Mapping[Any, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _mapping_key_sort_key(key: Any) -> tuple[str, str, str]:
    key_type = type(key)
    return (key_type.__module__, key_type.__qualname__, str(key))


def _compact_json_bytes(value: Mapping[str, Any]) -> bytes:
    """Encode deterministic JSON for schemas that compare or persist bytes."""

    return json.dumps(
        value, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, bytes | bytearray | memoryview):
        return {"__bytes_b64__": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, Mapping):
        # Coerce after sorting so keys that collapse to the same string resolve
        # the same way regardless of the original mapping's insertion order.
        return {
            str(key): _json_safe(item)
            for key, item in sorted(
                value.items(), key=lambda pair: _mapping_key_sort_key(pair[0])
            )
        }
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _json_restore(value: Any) -> Any:
    if isinstance(value, Mapping):
        if set(value) == {"__bytes_b64__"}:
            return _decode_base64_field(value["__bytes_b64__"], "__bytes_b64__")
        return {
            str(key): _json_restore(item)
            for key, item in sorted(
                value.items(), key=lambda pair: _mapping_key_sort_key(pair[0])
            )
        }
    if isinstance(value, list):
        return [_json_restore(item) for item in value]
    return value


def _decode_json_bool(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field} must be a JSON boolean")


def _decode_json_int(value: Any, field: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise ValueError(f"{field} must be a JSON integer")


def _decode_json_number(value: Any, field: str) -> float:
    if (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(value)
    ):
        return float(value)
    raise ValueError(f"{field} must be a JSON number (finite)")


def _decode_json_string(value: Any, field: str) -> str:
    if isinstance(value, str):
        return value
    raise ValueError(f"{field} must be a JSON string")


def _decode_json_mapping(value: Any, field: str) -> Mapping[Any, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError(f"{field} must be a JSON object")


def _decode_optional_json_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _decode_json_string(value, field)


def _decode_health_status(value: Any) -> Literal["ok", "stale", "error"]:
    status = _decode_json_string(value, "status")
    if status in ("ok", "stale", "error"):
        return status
    raise ValueError("status must be one of: ok, stale, error")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _sensor_identity_from_json(value: Any) -> SensorIdentity:
    if value is None:
        return SensorIdentity()
    identity_value = _decode_json_mapping(value, "identity")
    raw_tags = identity_value.get("tags", ())
    if not isinstance(raw_tags, list | tuple):
        raise ValueError("identity.tags must be a JSON array")
    tags = tuple(
        SensorTag(
            name=_decode_json_string(tag_value.get("name"), "identity.tags[].name"),
            variant=_decode_json_string(
                tag_value.get("variant"), "identity.tags[].variant"
            ),
            metadata={
                _decode_json_string(
                    key, "identity.tags[].metadata key"
                ): _decode_json_string(item, "identity.tags[].metadata value")
                for key, item in _decode_json_mapping(
                    tag_value.get("metadata", {}), "identity.tags[].metadata"
                ).items()
            },
        )
        for tag in raw_tags
        for tag_value in (_decode_json_mapping(tag, "identity.tags[]"),)
    )
    location_value = identity_value.get("location", {})
    location = (
        _sensor_location_from_json(location_value)
        if location_value is not None
        else SensorLocation()
    )
    return SensorIdentity(
        id=_decode_optional_json_string(identity_value.get("id"), "identity.id"),
        tags=tags,
        location=location,
        group=_decode_optional_json_string(
            identity_value.get("group"), "identity.group"
        ),
    )


def _sensor_location_from_json(value: Any) -> SensorLocation:
    location_value = _decode_json_mapping(value, "identity.location")
    return SensorLocation(
        x=_decode_json_number(location_value.get("x", 0.0), "identity.location.x"),
        y=_decode_json_number(location_value.get("y", 0.0), "identity.location.y"),
        z=_decode_json_number(location_value.get("z", 0.0), "identity.location.z"),
        timestamp=None
        if location_value.get("timestamp") is None
        else _decode_json_number(
            location_value["timestamp"], "identity.location.timestamp"
        ),
    )


def _decode_base64_field(value: Any, field: str) -> bytes:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except binascii.Error as exc:
        raise ValueError(f"{field} must be valid base64") from exc


def _identity_from_peripheral_item(item: Any, group: str | None) -> SensorIdentity:
    info = getattr(item, "peripheral_info", None)
    if info is None:
        return SensorIdentity(group=group)
    tags = tuple(
        SensorTag(
            name=str(getattr(tag, "name", "")),
            variant=str(getattr(tag, "variant", "")),
            metadata={
                str(key): str(value)
                for key, value in dict(getattr(tag, "metadata", {}) or {}).items()
            },
        )
        for tag in tuple(getattr(info, "tags", ()) or ())
    )
    location_info = getattr(info, "location", None)
    location = SensorLocation(
        x=float(getattr(location_info, "x", 0.0)),
        y=float(getattr(location_info, "y", 0.0)),
        z=float(getattr(location_info, "z", 0.0)),
        timestamp=getattr(location_info, "timestamp", None),
    )
    return SensorIdentity(
        id=getattr(info, "id", None),
        tags=tags,
        location=location,
        group=group,
    )


def _unwrap_peripheral_data(item: Any) -> Any:
    return getattr(item, "data", item)


def _event_value(item: Any) -> Any:
    return getattr(item, "value", item)


def _call_if_present(target: Any, name: str, *args: Any) -> Any:
    method = getattr(target, name, None)
    if callable(method):
        return method(*args)
    return None


def _flatten_handles(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (str, bytes, bytearray)):
        return [value]
    if isinstance(value, Iterable):
        return [item for item in value if item is not None]
    return [value]


def _dispose_handle(handle: Any, *, timeout: float | None = None) -> None:
    dispose = getattr(handle, "dispose", None)
    if callable(dispose):
        try:
            dispose(timeout=timeout)
        except TypeError:
            dispose()
        return
    _call_if_present(handle, "stop")
    _call_if_present(handle, "join", timeout)
