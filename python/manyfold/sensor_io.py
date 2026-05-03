"""Local sensor IO components for single-device Manyfold projects."""

from __future__ import annotations

import base64
import codecs
import json
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

    def now(self) -> float:
        return self.current

    def set(self, value: float) -> None:
        self.current = float(value)

    def advance(self, delta: float) -> float:
        if delta < 0:
            raise ValueError("delta must be non-negative")
        self.current += float(delta)
        return self.current


@dataclass(frozen=True)
class RetryPolicy:
    """Pure local retry policy for sensor reads and local effects."""

    max_attempts: int = 1
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if not self.retry_on:
            raise ValueError("retry_on must contain at least one exception type")

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
        if attempt_index <= 1:
            return 0.0
        delay = self.initial_delay * (self.multiplier ** (attempt_index - 2))
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

    def run(self, operation: Callable[[], T]) -> T:
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
    _event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)

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
    retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_attempts=1_000_000))
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    on_error: Callable[[BaseException, int], None] | None = None
    group: str | None = None

    def run(self, stop: StopToken | None = None) -> None:
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
        self.thread.join(timeout=timeout)

    def dispose(self, timeout: float | None = None) -> None:
        if self.disposed:
            return
        self.stop()
        self.join(timeout=timeout)
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
        if self.step <= 0:
            raise ValueError("step must be positive")

    def next(self) -> int:
        self.current += self.step
        return self.current

    def peek(self) -> int:
        return self.current

    def reset(self, value: int = 0) -> None:
        self.current = value


@dataclass(frozen=True)
class SensorTag:
    """Small metadata tag for a physical or logical sensor."""

    name: str
    variant: str
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SensorLocation:
    """Physical-space coordinate for a sensor relative to its installation."""

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    timestamp: float | None = None


@dataclass(frozen=True)
class SensorIdentity:
    """Identity and metadata for a sensor or peripheral source."""

    id: str | None = None
    tags: tuple[SensorTag, ...] = ()
    location: SensorLocation = field(default_factory=SensorLocation)
    group: str | None = None


@dataclass(frozen=True)
class SensorSample(Generic[T]):
    """Normalized local sensor reading published as a typed payload."""

    value: T
    source_timestamp: float
    ingest_timestamp: float
    sequence_number: int
    quality: str | None = None
    status: str | None = None


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


@dataclass(frozen=True)
class HealthStatus:
    """Local health event emitted by sensor watchdogs."""

    status: Literal["ok", "stale", "error"]
    observed_at: float
    message: str = ""
    stale: bool = False
    error_count: int = 0


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
    _history: deque[SensorDebugEnvelope] = field(default_factory=deque, init=False, repr=False)

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
            self._history = deque(self._history, maxlen=self.history_size)
        self._history.append(envelope)
        return envelope

    def snapshot(self) -> tuple[SensorDebugEnvelope, ...]:
        return tuple(self._history)


def sensor_sample_schema(value_schema: Schema[T], schema_id: str | None = None) -> Schema[SensorSample[T]]:
    """Build a JSON envelope schema for ``SensorSample[T]`` values."""

    resolved_schema_id = schema_id or f"SensorSample[{value_schema.schema_id}]"

    def encode(sample: SensorSample[T]) -> bytes:
        value_payload = value_schema.encode(sample.value)
        return json.dumps(
            {
                "value": base64.b64encode(value_payload).decode("ascii"),
                "source_timestamp": sample.source_timestamp,
                "ingest_timestamp": sample.ingest_timestamp,
                "sequence_number": sample.sequence_number,
                "quality": sample.quality,
                "status": sample.status,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def decode(payload: bytes) -> SensorSample[T]:
        data = json.loads(payload.decode("utf-8"))
        value = value_schema.decode(base64.b64decode(data["value"]))
        return SensorSample(
            value=value,
            source_timestamp=float(data["source_timestamp"]),
            ingest_timestamp=float(data["ingest_timestamp"]),
            sequence_number=int(data["sequence_number"]),
            quality=data.get("quality"),
            status=data.get("status"),
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
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def decode(payload: bytes) -> SensorEvent:
        data = json.loads(payload.decode("utf-8"))
        raw = data.get("raw")
        return SensorEvent(
            event_type=str(data["event_type"]),
            data=_json_restore(data.get("data")),
            observed_at=float(data["observed_at"]),
            identity=_sensor_identity_from_json(data.get("identity")),
            sequence_number=data.get("sequence_number"),
            raw=None if raw is None else base64.b64decode(raw),
            metadata=_json_restore(data.get("metadata", {})),
        )

    return Schema(schema_id=schema_id, version=1, encode=encode, decode=decode)


def health_status_schema(schema_id: str = "HealthStatus") -> Schema[HealthStatus]:
    """Return a JSON schema for local sensor health events."""

    def encode(status: HealthStatus) -> bytes:
        return json.dumps(
            {
                "status": status.status,
                "observed_at": status.observed_at,
                "message": status.message,
                "stale": status.stale,
                "error_count": status.error_count,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def decode(payload: bytes) -> HealthStatus:
        data = json.loads(payload.decode("utf-8"))
        return HealthStatus(
            status=data["status"],
            observed_at=float(data["observed_at"]),
            message=data.get("message", ""),
            stale=bool(data.get("stale", False)),
            error_count=int(data.get("error_count", 0)),
        )

    return Schema(schema_id=schema_id, version=1, encode=encode, decode=decode)


@dataclass
class DelimitedMessageBuffer:
    """Incrementally buffer delimited bytes or text messages."""

    delimiter: bytes = b"\n"
    mode: MessageBufferMode = "bytes"
    _bytes_buffer: bytearray = field(default_factory=bytearray, init=False, repr=False)
    _text_buffer: str = field(default="", init=False, repr=False)
    _text_decoder: codecs.IncrementalDecoder = field(
        default_factory=lambda: codecs.getincrementaldecoder("utf-8")(errors="ignore"),
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if not self.delimiter:
            raise ValueError("delimiter must not be empty")
        if self.mode not in ("bytes", "text"):
            raise ValueError("mode must be 'bytes' or 'text'")

    @property
    def buffer_size(self) -> int:
        if self.mode == "text":
            return len(self._text_buffer)
        return len(self._bytes_buffer)

    def append(self, data: bytes | bytearray | str) -> tuple[bytes | str, ...]:
        if self.mode == "text":
            if isinstance(data, str):
                self._text_buffer += data
            else:
                self._text_buffer += self._text_decoder.decode(bytes(data), final=False)
            return tuple(self._drain_text())
        if isinstance(data, str):
            self._bytes_buffer.extend(data.encode("utf-8"))
        else:
            self._bytes_buffer.extend(data)
        return tuple(self._drain_bytes())

    def clear(self) -> None:
        self._bytes_buffer.clear()
        self._text_buffer = ""
        self._text_decoder.reset()

    def _drain_bytes(self) -> list[bytes]:
        messages: list[bytes] = []
        delimiter_len = len(self.delimiter)
        while True:
            try:
                index = self._bytes_buffer.index(self.delimiter)
            except ValueError:
                return messages
            messages.append(bytes(self._bytes_buffer[:index]))
            del self._bytes_buffer[: index + delimiter_len]

    def _drain_text(self) -> list[str]:
        messages: list[str] = []
        delimiter = self.delimiter.decode("utf-8", errors="ignore")
        delimiter_len = len(delimiter)
        while True:
            index = self._text_buffer.find(delimiter)
            if index < 0:
                return messages
            messages.append(self._text_buffer[:index])
            self._text_buffer = self._text_buffer[index + delimiter_len :]


@dataclass
class JsonEventDecoder:
    """Decode newline-framed JSON messages into ``SensorEvent`` values."""

    clock: Clock = field(default_factory=SystemClock)
    identity: SensorIdentity = field(default_factory=SensorIdentity)
    sequence: SequenceCounter = field(default_factory=SequenceCounter)
    default_event_type: str = "sensor.event"
    group: str | None = None

    def __post_init__(self) -> None:
        _adopt_group(self.clock, self.group)
        _adopt_group(self.sequence, self.group)
        if self.group is not None and self.identity.group is None:
            self.identity = SensorIdentity(
                id=self.identity.id,
                tags=self.identity.tags,
                location=self.identity.location,
                group=self.group,
            )

    def decode(self, message: bytes | bytearray | str) -> SensorEvent | None:
        raw = message.encode("utf-8") if isinstance(message, str) else bytes(message)
        text = raw.decode("utf-8", errors="ignore").strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, Mapping):
            return None
        event_type = str(parsed.get("event_type") or self.default_event_type)
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
    _pending: dict[Any, dict[Any, TFrame]] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.expected_count <= 0:
            raise ValueError("expected_count must be positive")

    def add(self, sample: TFrame) -> tuple[SensorFrame[TFrame], ...]:
        resolved_frame_id = self.frame_id(sample)
        bucket = self._pending.setdefault(resolved_frame_id, {})
        bucket[self.slot_id(sample)] = sample
        if len(bucket) < self.expected_count:
            return ()
        samples = tuple(bucket[key] for key in sorted(bucket)[: self.expected_count])
        del self._pending[resolved_frame_id]
        return (SensorFrame(frame_id=resolved_frame_id, samples=samples),)


def xor_checksum(data: bytes | bytearray | Sequence[int]) -> int:
    """Return a small XOR checksum for packet/frame tests and adapters."""

    checksum = 0
    for value in data:
        checksum ^= int(value) & 0xFF
    return checksum


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
        _adopt_group(self.clock, self.group)
        _adopt_group(self.sequence, self.group)

    def install(self, graph: Graph) -> PeripheralAdapterHandle:
        if self.run_on_install:
            _call_if_present(self.peripheral, "run")

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

            control_subscription = graph.observe(
                self.control_route, replay_latest=False
            ).subscribe(on_control)
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
        _adopt_group(self.clock, self.group)

    def install(self, graph: Graph) -> Any:
        if self.mode not in {"latest", "fifo"}:
            raise ValueError("mode must be 'latest' or 'fifo'")
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
        stale = self.last_seen is None or now - self.last_seen >= self.watchdog.stale_after
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
        if self.stale_after < 0:
            raise ValueError("stale_after must be non-negative")
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

    def event_log(self) -> EventLog[T]:
        return EventLog(self.name, self.keyspace, self.schema)

    def install(self, graph: Graph, source: TypedRoute[T]) -> SubscriptionLike:
        log = self.event_log()
        log_subscription = log.install(graph)

        def append(sample: Any) -> None:
            graph.publish(log.input(), sample.value if hasattr(sample, "value") else sample)

        source_subscription = graph.observe(source, replay_latest=False).subscribe(append)
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

    def dispose(self) -> None:
        for subscription in self.subscriptions:
            subscription.dispose()


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
        keys = set(new) | set(old)
        return any(_changed_enough(new.get(key), old.get(key), threshold) for key in keys)
    if isinstance(new, tuple) and isinstance(old, tuple):
        if len(new) != len(old):
            return True
        return any(_changed_enough(n, o, threshold) for n, o in zip(new, old))
    if isinstance(new, list) and isinstance(old, list):
        if len(new) != len(old):
            return True
        return any(_changed_enough(n, o, threshold) for n, o in zip(new, old))
    if isinstance(new, (int, float)) and isinstance(old, (int, float)):
        return abs(float(new) - float(old)) > threshold
    return new != old


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, bytes | bytearray | memoryview):
        return {"__bytes_b64__": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _json_restore(value: Any) -> Any:
    if isinstance(value, Mapping):
        if set(value) == {"__bytes_b64__"}:
            return base64.b64decode(str(value["__bytes_b64__"]))
        return {str(key): _json_restore(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_restore(item) for item in value]
    return value


def _sensor_identity_from_json(value: Any) -> SensorIdentity:
    if not isinstance(value, Mapping):
        return SensorIdentity()
    tags = tuple(
        SensorTag(
            name=str(tag.get("name")),
            variant=str(tag.get("variant")),
            metadata={
                str(key): str(item)
                for key, item in dict(tag.get("metadata", {})).items()
            },
        )
        for tag in value.get("tags", ())
        if isinstance(tag, Mapping)
    )
    location_value = value.get("location", {})
    location = (
        SensorLocation(
            x=float(location_value.get("x", 0.0)),
            y=float(location_value.get("y", 0.0)),
            z=float(location_value.get("z", 0.0)),
            timestamp=location_value.get("timestamp"),
        )
        if isinstance(location_value, Mapping)
        else SensorLocation()
    )
    return SensorIdentity(
        id=value.get("id"),
        tags=tags,
        location=location,
        group=value.get("group"),
    )


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


__all__ = [
    "BackoffPolicy",
    "BoundedRingBuffer",
    "ChangeFilter",
    "Clock",
    "DelimitedMessageBuffer",
    "DoubleBuffer",
    "DuplexSensorPeripheral",
    "FrameAssembler",
    "HealthStatus",
    "JsonEventDecoder",
    "LocalDurableSpool",
    "LocalSensorSource",
    "ManualClock",
    "ManagedRunLoop",
    "ManagedRunLoopHandle",
    "PeripheralAdapter",
    "PeripheralAdapterHandle",
    "RateMatchedSensor",
    "ReactiveSensorHandle",
    "ReactiveSensorSource",
    "RetryLoop",
    "RetryPolicy",
    "SequenceCounter",
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
    "StopToken",
    "SystemClock",
    "ThresholdFilter",
    "health_status_schema",
    "sensor_event_schema",
    "sensor_sample_schema",
    "xor_checksum",
]
