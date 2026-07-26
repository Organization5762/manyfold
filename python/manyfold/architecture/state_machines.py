"""Typed single-writer state machines backed by named PubSub topics."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from queue import Full, Queue
from threading import Condition, Lock, RLock, Thread, current_thread
from time import monotonic
from typing import Generic, TypeVar, final, get_type_hints

from .pubsub import PubSubCallbackSubscription, PubSubTopic, StreamRow
from .values import ImmutableValue, Value, ValueSubscription

CommandT = TypeVar("CommandT")
StateT = TypeVar("StateT")
EventT = TypeVar("EventT")

_DEFAULT_COMMAND_QUEUE_LIMIT = 1024
_DEFAULT_RETAINED_MESSAGES = 1024
_ACTIVE_MACHINES: dict[tuple[str, str], "StateMachine[object, object, object]"] = {}
_ACTIVE_MACHINES_LOCK = Lock()
_STOP = object()


class StateMachineError(RuntimeError):
    """Base error for state-machine lifecycle and admission failures."""


@final
class StateMachineClosed(StateMachineError):
    """Raised when an operation requires a running state machine."""


@final
class StateMachineAlreadyRunning(StateMachineError):
    """Raised when a name is already active in one PubSub namespace."""


@final
class StateMachineQueueFull(StateMachineError):
    """Raised after a command was published but bounded admission rejected it."""


@final
class StateMachineEventType(str, Enum):
    """Stable semantic event kinds published by a state machine."""

    STARTING = "starting"
    STARTED = "started"
    COMMAND_RECEIVED = "command_received"
    COMMITTED = "committed"
    REJECTED = "rejected"
    UNCHANGED = "unchanged"
    FAILED = "failed"
    DROPPED = "dropped"
    STOPPING = "stopping"
    STOPPED = "stopped"


@final
@dataclass(frozen=True, slots=True)
class StateTransition(Generic[StateT, EventT]):
    """A reducer result that commits ``next_state`` and domain events."""

    next_state: StateT
    events: tuple[EventT, ...] = ()
    reason: str = "transitioned"

    def __post_init__(self) -> None:
        if not isinstance(self.events, tuple):
            raise TypeError("StateTransition events must be a tuple")
        _require_text(self.reason, "StateTransition reason")


@final
@dataclass(frozen=True, slots=True)
class StateRejection:
    """A reducer result that rejects a command without changing state."""

    reason: str

    def __post_init__(self) -> None:
        _require_text(self.reason, "StateRejection reason")


@final
@dataclass(frozen=True, slots=True)
class StateUnchanged:
    """A reducer result that accepts a command without changing state."""

    reason: str

    def __post_init__(self) -> None:
        _require_text(self.reason, "StateUnchanged reason")


@final
@dataclass(frozen=True, slots=True)
class StateMachineEvent:
    """Deterministic lifecycle and transition audit record."""

    machine: str
    kind: str
    sequence: int
    revision: int
    command_id: str
    correlation_id: str
    command_offset: int
    previous_state_id: str
    next_state_id: str
    reason: str

    @property
    def event_type(self) -> StateMachineEventType:
        """Return the typed semantic event kind."""
        return StateMachineEventType(self.kind)


@final
class StateMachine(Generic[CommandT, StateT, EventT]):
    """Serialize pure reducer transitions over real named PubSub topics."""

    def __init__(
        self,
        *,
        name: str,
        initial: StateT,
        reducer: Callable[
            [StateT, CommandT],
            StateTransition[StateT, EventT] | StateRejection | StateUnchanged,
        ],
        command_schema: type[CommandT],
        state_schema: type[StateT],
        event_schema: type[EventT],
        namespace: str = "default",
        command_queue_limit: int = _DEFAULT_COMMAND_QUEUE_LIMIT,
        retained_messages: int = _DEFAULT_RETAINED_MESSAGES,
    ) -> None:
        self.name = _require_text(name, "state-machine name")
        self.namespace = _require_text(namespace, "state-machine namespace")
        if not callable(reducer):
            raise TypeError("state-machine reducer must be callable")
        _require_schema(command_schema, "command_schema")
        _require_schema(state_schema, "state_schema")
        _require_schema(event_schema, "event_schema")
        if not isinstance(initial, state_schema):
            raise TypeError("initial state must be an instance of state_schema")
        _require_positive_integer(command_queue_limit, "command_queue_limit")
        _require_positive_integer(retained_messages, "retained_messages")

        self.reducer = reducer
        self.command_schema = command_schema
        self.state_schema = state_schema
        self.event_schema = event_schema
        self.commands = PubSubTopic(
            f"{self.name}.commands",
            schema=command_schema,
            namespace=self.namespace,
            retained_messages=retained_messages,
        )
        self.states = PubSubTopic(
            f"{self.name}.states",
            schema=state_schema,
            namespace=self.namespace,
            retained_messages=retained_messages,
        )
        self.transitions = PubSubTopic(
            f"{self.name}.transitions",
            schema=event_schema,
            namespace=self.namespace,
            retained_messages=retained_messages,
        )
        self.events = PubSubTopic(
            f"{self.name}.events",
            schema=StateMachineEvent,
            namespace=self.namespace,
            retained_messages=retained_messages,
        )
        self._initial = initial
        self._current = Value.initialized(initial, name=f"{self.name}.state")
        self.state: ImmutableValue[StateT] = _StateValueView(self._current)
        self._queue: Queue[_QueuedCommand[CommandT] | object] = Queue(
            maxsize=command_queue_limit
        )
        self._condition = Condition(Lock())
        self._audit_lock = RLock()
        self._subscription: PubSubCallbackSubscription | None = None
        self._worker: Thread | None = None
        self._accepting = False
        self._started = False
        self._closed = False
        self._pending_commands = 0
        self._revision = 0
        self._event_sequence = 0
        self._last_error: str | None = None

    @property
    def is_running(self) -> bool:
        """Return whether this machine currently accepts commands."""
        with self._condition:
            return self._accepting

    @property
    def revision(self) -> int:
        """Return the latest committed state revision."""
        with self._condition:
            return self._revision

    @property
    def last_error(self) -> str | None:
        """Return the latest reducer or subscriber failure summary."""
        with self._condition:
            return self._last_error

    def __enter__(self) -> "StateMachine[CommandT, StateT, EventT]":
        self.start()
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def start(self) -> None:
        """Restore retained local state and begin serialized command handling."""
        key = (self.namespace, self.name)
        with self._condition:
            if self._closed:
                raise StateMachineClosed(
                    f"state machine {self.name!r} cannot restart after close"
                )
            if self._started:
                return
        with _ACTIVE_MACHINES_LOCK:
            active = _ACTIVE_MACHINES.get(key)
            if active is not None and active is not self:
                raise StateMachineAlreadyRunning(
                    f"state machine {self.name!r} is already running in "
                    f"namespace {self.namespace!r}"
                )
            _ACTIVE_MACHINES[key] = self  # type: ignore[assignment]

        try:
            self._restore_retained_state()
            self._publish_audit(StateMachineEventType.STARTING, reason="start")
            worker = Thread(
                target=self._run,
                name=f"manyfold-state-machine-{self.name}",
                daemon=True,
            )
            subscription = self.commands.subscribe(self._enqueue_command)
            with self._condition:
                self._worker = worker
                self._subscription = subscription
                self._started = True
            worker.start()
            self._publish_audit(StateMachineEventType.STARTED, reason="ready")
            with self._condition:
                self._accepting = True
        except BaseException:
            subscription = self._subscription
            if subscription is not None:
                subscription.dispose()
            with _ACTIVE_MACHINES_LOCK:
                if _ACTIVE_MACHINES.get(key) is self:
                    del _ACTIVE_MACHINES[key]
            raise

    def flush(self, *, timeout: float | None = None) -> bool:
        """Wait until every command accepted so far has completed."""
        _require_timeout(timeout)
        deadline = None if timeout is None else monotonic() + timeout
        with self._condition:
            while self._pending_commands:
                if self._closed and self._worker is None:
                    return False
                remaining = (
                    None if deadline is None else max(deadline - monotonic(), 0.0)
                )
                if remaining == 0:
                    return False
                self._condition.wait(timeout=remaining)
        return True

    def close(self, *, timeout: float | None = None) -> None:
        """Stop admission, drain accepted commands, and dispose owned runtime state."""
        _require_timeout(timeout)
        with self._condition:
            if self._closed:
                return
            if not self._started:
                self._closed = True
                return
            worker = self._worker
            if worker is current_thread():
                raise StateMachineError(
                    "state machine close cannot run from its callback thread"
                )
            self._accepting = False
            subscription = self._subscription
        if subscription is not None:
            subscription.dispose()
        if not self.flush(timeout=timeout):
            raise TimeoutError(
                f"state machine {self.name!r} did not drain before close timeout"
            )
        self._publish_audit(StateMachineEventType.STOPPING, reason="close")
        self._queue.put(_STOP)
        if worker is not None:
            worker.join()
        self._publish_audit(StateMachineEventType.STOPPED, reason="closed")
        with self._condition:
            self._worker = None
            self._subscription = None
            self._closed = True
            self._condition.notify_all()
        with _ACTIVE_MACHINES_LOCK:
            key = (self.namespace, self.name)
            if _ACTIVE_MACHINES.get(key) is self:
                del _ACTIVE_MACHINES[key]

    def _enqueue_command(self, row: StreamRow) -> None:
        try:
            command = row.as_model(self.command_schema)
            command_offset = int(row.offset)
            correlation_id = (
                str(row.message_key)
                if row.message_key is not None
                else f"{self.name}:{command_offset}"
            )
            queued = _QueuedCommand(
                command=command,  # type: ignore[arg-type]
                command_id=f"{self.name}:{command_offset}",
                correlation_id=correlation_id,
                offset=command_offset,
            )
        except Exception as error:
            self._record_failure("decode_command", error)
            return
        dropped_reason: str | None = None
        with self._condition:
            if not self._accepting:
                dropped_reason = "machine_not_accepting"
            else:
                try:
                    self._queue.put_nowait(queued)
                except Full:
                    dropped_reason = "command_queue_full"
                else:
                    self._pending_commands += 1
                    self._condition.notify_all()
        if dropped_reason is None:
            return
        self._publish_audit(
            StateMachineEventType.DROPPED,
            command=queued,
            reason=dropped_reason,
        )
        if dropped_reason == "machine_not_accepting":
            return
        raise StateMachineQueueFull(
            f"state machine {self.name!r} command queue is full"
        )

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is _STOP:
                    return
                if not isinstance(item, _QueuedCommand):
                    self._record_failure(
                        "invalid_queue_item",
                        TypeError("command queue contained an invalid value"),
                    )
                    continue
                self._process_command(item)
            finally:
                self._queue.task_done()
                if isinstance(item, _QueuedCommand):
                    with self._condition:
                        self._pending_commands -= 1
                        self._condition.notify_all()

    def _process_command(self, command: "_QueuedCommand[CommandT]") -> None:
        current = self._current.latest
        if current is None:
            self._record_failure(
                "missing_current_state",
                StateMachineError("state machine has no current state"),
                command=command,
            )
            return
        previous_state_id = _state_identity(current)
        self._publish_audit(
            StateMachineEventType.COMMAND_RECEIVED,
            command=command,
            previous_state_id=previous_state_id,
            next_state_id=previous_state_id,
            reason="received",
        )
        try:
            result = self.reducer(current, command.command)
        except Exception as error:
            self._record_failure("reducer_raised", error, command=command)
            return
        if isinstance(result, StateRejection):
            self._publish_audit(
                StateMachineEventType.REJECTED,
                command=command,
                previous_state_id=previous_state_id,
                next_state_id=previous_state_id,
                reason=result.reason,
            )
            return
        if isinstance(result, StateUnchanged):
            self._publish_audit(
                StateMachineEventType.UNCHANGED,
                command=command,
                previous_state_id=previous_state_id,
                next_state_id=previous_state_id,
                reason=result.reason,
            )
            return
        if not isinstance(result, StateTransition):
            self._record_failure(
                "invalid_reducer_result",
                TypeError(
                    "reducer must return StateTransition, StateRejection, "
                    "or StateUnchanged"
                ),
                command=command,
            )
            return
        if not isinstance(result.next_state, self.state_schema):
            self._record_failure(
                "invalid_next_state",
                TypeError("StateTransition next_state must match state_schema"),
                command=command,
            )
            return
        if any(not isinstance(event, self.event_schema) for event in result.events):
            self._record_failure(
                "invalid_domain_event",
                TypeError("StateTransition events must match event_schema"),
                command=command,
            )
            return

        next_state_id = _state_identity(result.next_state)
        with self._condition:
            next_revision = self._revision + 1
        state_error = self._publish_state(result.next_state)
        if state_error is not None and not self._state_was_published(
            result.next_state
        ):
            self._record_failure("publish_state", state_error, command=command)
            return
        with self._condition:
            self._revision = next_revision
        self._publish_value(result.next_state)
        if state_error is not None:
            self._record_failure("state_subscriber", state_error, command=command)
        for event in result.events:
            self._publish_domain_event(event, command)
        self._publish_audit(
            StateMachineEventType.COMMITTED,
            command=command,
            previous_state_id=previous_state_id,
            next_state_id=next_state_id,
            reason=result.reason,
        )

    def _publish_state(self, state: StateT) -> Exception | None:
        try:
            self.states.publish(state)
        except Exception as error:
            return error
        return None

    def _state_was_published(self, state: StateT) -> bool:
        latest = self.states.latest()
        return (
            latest is not None
            and latest.as_model(self.state_schema) == state
        )

    def _publish_value(self, state: StateT) -> None:
        try:
            self._current.set(state)
        except Exception as error:
            self._record_failure("state_value_subscriber", error)

    def _publish_domain_event(
        self,
        event: EventT,
        command: "_QueuedCommand[CommandT]",
    ) -> None:
        try:
            self.transitions.publish(event)
        except Exception as error:
            self._record_failure("transition_subscriber", error, command=command)

    def _restore_retained_state(self) -> None:
        latest_state = self.states.latest()
        latest_event = self.events.latest()
        if latest_state is None:
            self.states.publish(self._initial)
            return
        restored = latest_state.as_model(self.state_schema)
        self._current.set(restored)  # type: ignore[arg-type]
        if latest_event is None:
            return
        event = latest_event.as_model(StateMachineEvent)
        with self._condition:
            self._revision = event.revision
            self._event_sequence = event.sequence

    def _record_failure(
        self,
        reason: str,
        error: Exception,
        *,
        command: "_QueuedCommand[object] | None" = None,
    ) -> None:
        summary = f"{type(error).__name__}: {error}"
        with self._condition:
            self._last_error = summary
        self._publish_audit(
            StateMachineEventType.FAILED,
            command=command,
            reason=f"{reason}: {summary}",
        )

    def _publish_audit(
        self,
        kind: StateMachineEventType,
        *,
        command: "_QueuedCommand[object] | None" = None,
        previous_state_id: str | None = None,
        next_state_id: str | None = None,
        reason: str,
    ) -> None:
        with self._audit_lock:
            current = self._current.latest
            current_state_id = "" if current is None else _state_identity(current)
            with self._condition:
                self._event_sequence += 1
                event = StateMachineEvent(
                    machine=self.name,
                    kind=kind.value,
                    sequence=self._event_sequence,
                    revision=self._revision,
                    command_id="" if command is None else command.command_id,
                    correlation_id="" if command is None else command.correlation_id,
                    command_offset=-1 if command is None else command.offset,
                    previous_state_id=previous_state_id or current_state_id,
                    next_state_id=next_state_id or current_state_id,
                    reason=reason,
                )
            try:
                self.events.publish(event)
            except Exception as error:
                with self._condition:
                    self._last_error = f"{type(error).__name__}: {error}"


def _state_identity(state: object) -> str:
    fields = _model_fields(state)
    encoded = json.dumps(
        fields,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def _model_fields(value: object) -> Mapping[str, object]:
    if is_dataclass(value):
        return asdict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        fields = model_dump()
        if isinstance(fields, Mapping):
            return fields
    legacy_dict = getattr(value, "dict", None)
    if callable(legacy_dict):
        fields = legacy_dict()
        if isinstance(fields, Mapping):
            return fields
    return {name: getattr(value, name) for name in get_type_hints(type(value))}


def _require_schema(value: object, field_name: str) -> None:
    if not isinstance(value, type):
        raise TypeError(f"{field_name} must be a model class")
    if not get_type_hints(value):
        raise ValueError(f"{field_name} must declare typed fields")


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_positive_integer(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _require_timeout(value: float | None) -> None:
    if (
        value is not None
        and (
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or value < 0
        )
    ):
        raise ValueError("timeout must be a non-negative number or None")


@dataclass(frozen=True, slots=True)
class _QueuedCommand(Generic[CommandT]):
    command: CommandT
    command_id: str
    correlation_id: str
    offset: int


@final
class _StateValueView(ImmutableValue[StateT]):
    def __init__(self, source: Value[StateT]) -> None:
        self._source = source
        self.name = source.name

    @property
    def has_value(self) -> bool:
        return self._source.has_value

    @property
    def latest(self) -> StateT | None:
        return self._source.latest

    @property
    def subscriber_count(self) -> int:
        return self._source.subscriber_count

    def observe(
        self,
        callback: Callable[[StateT], object] | object | None = None,
        *,
        on_next: Callable[[StateT], object] | None = None,
        replay_latest: bool = True,
    ) -> ValueSubscription:
        return self._source.observe(
            callback,
            on_next=on_next,
            replay_latest=replay_latest,
        )


__all__ = [
    "StateMachine",
    "StateMachineAlreadyRunning",
    "StateMachineClosed",
    "StateMachineError",
    "StateMachineEvent",
    "StateMachineEventType",
    "StateMachineQueueFull",
    "StateRejection",
    "StateTransition",
    "StateUnchanged",
]
