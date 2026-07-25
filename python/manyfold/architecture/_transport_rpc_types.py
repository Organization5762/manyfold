"""Public value types used by the bounded coordinator RPC endpoint."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Event
from time import monotonic
from typing import TypeAlias

from ._transport_rpc_validation import (
    require_bytes as _require_bytes,
    require_nonnegative_number as _require_nonnegative_number,
    require_positive_integer as _require_positive_integer,
    require_positive_number as _require_positive_number,
    require_text as _require_text,
)

RpcHandler: TypeAlias = Callable[["RpcRequest", "RpcCancellation"], bytes]

_DEFAULT_TIMEOUT_SECONDS = 5.0
_DEFAULT_RECEIVE_POLL_SECONDS = 0.05


class RpcError(RuntimeError):
    """Base error for coordinator RPC operations."""


class RpcEndpointClosed(RpcError):
    """Raised when an operation targets a disposed RPC endpoint."""


class RpcOverloaded(RpcError):
    """Raised when bounded client or server capacity is exhausted."""


class RpcTimeout(RpcError):
    """Raised when an RPC deadline elapses before a response arrives."""


class RpcCancelled(RpcError):
    """Raised when an RPC is cancelled locally or by its peer."""


class RpcDisconnected(RpcError):
    """Raised when a request loses the transport session that carried it."""


class RpcProtocolError(RpcError):
    """Raised when a peer sends malformed RPC framing."""


class RpcShutdownTimeout(RpcError):
    """Raised when a handler ignores cancellation during endpoint shutdown."""


class RpcRemoteError(RpcError):
    """Structured error returned by a remote RPC handler."""

    def __init__(self, error: "RpcErrorRecord") -> None:
        self.correlation_id = error.correlation_id
        self.code = error.code
        self.remote_message = error.message
        self.retryable = error.retryable
        super().__init__(f"remote RPC {error.code}: {error.message}")


@dataclass(frozen=True, slots=True)
class RpcConfig:
    """Capacity and lifecycle limits for one coordinator RPC endpoint."""

    max_in_flight: int = 128
    max_handlers: int = 128
    max_workers: int = 8
    request_queue_limit: int = 128
    default_timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    receive_poll_seconds: float = _DEFAULT_RECEIVE_POLL_SECONDS
    send_timeout_seconds: float = 0.1
    shutdown_timeout_seconds: float = 2.0

    def __post_init__(self) -> None:
        _require_positive_integer(self.max_in_flight, "max_in_flight")
        _require_positive_integer(self.max_handlers, "max_handlers")
        _require_positive_integer(self.max_workers, "max_workers")
        _require_positive_integer(self.request_queue_limit, "request_queue_limit")
        _require_positive_number(
            self.default_timeout_seconds,
            "default_timeout_seconds",
        )
        _require_positive_number(self.receive_poll_seconds, "receive_poll_seconds")
        _require_nonnegative_number(self.send_timeout_seconds, "send_timeout_seconds")
        _require_positive_number(
            self.shutdown_timeout_seconds,
            "shutdown_timeout_seconds",
        )


@dataclass(frozen=True, slots=True)
class RpcRequest:
    """Typed request delivered to one registered coordinator handler."""

    correlation_id: str
    service: str
    method: str
    payload: bytes
    timeout_seconds: float
    session_id: str = ""
    target_session_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "correlation_id",
            _require_text(self.correlation_id, "correlation_id"),
        )
        object.__setattr__(self, "service", _require_text(self.service, "service"))
        object.__setattr__(self, "method", _require_text(self.method, "method"))
        object.__setattr__(self, "payload", _require_bytes(self.payload, "payload"))
        _require_positive_number(self.timeout_seconds, "timeout_seconds")
        if self.session_id:
            object.__setattr__(
                self,
                "session_id",
                _require_text(self.session_id, "session_id"),
            )
        if self.target_session_id:
            object.__setattr__(
                self,
                "target_session_id",
                _require_text(self.target_session_id, "target_session_id"),
            )


@dataclass(frozen=True, slots=True)
class RpcResponse:
    """Successful response for one coordinator RPC request."""

    correlation_id: str
    payload: bytes
    session_id: str = ""
    target_session_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "correlation_id",
            _require_text(self.correlation_id, "correlation_id"),
        )
        object.__setattr__(self, "payload", _require_bytes(self.payload, "payload"))
        if self.session_id:
            object.__setattr__(
                self,
                "session_id",
                _require_text(self.session_id, "session_id"),
            )
        if self.target_session_id:
            object.__setattr__(
                self,
                "target_session_id",
                _require_text(self.target_session_id, "target_session_id"),
            )


@dataclass(frozen=True, slots=True)
class RpcErrorRecord:
    """Remote failure with stable machine and human-readable semantics."""

    correlation_id: str
    code: str
    message: str
    retryable: bool = False
    session_id: str = ""
    target_session_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "correlation_id",
            _require_text(self.correlation_id, "correlation_id"),
        )
        object.__setattr__(self, "code", _require_text(self.code, "code"))
        object.__setattr__(self, "message", _require_text(self.message, "message"))
        if not isinstance(self.retryable, bool):
            raise ValueError("retryable must be a boolean")
        if self.session_id:
            object.__setattr__(
                self,
                "session_id",
                _require_text(self.session_id, "session_id"),
            )
        if self.target_session_id:
            object.__setattr__(
                self,
                "target_session_id",
                _require_text(self.target_session_id, "target_session_id"),
            )


@dataclass(frozen=True, slots=True)
class RpcCancel:
    """Cancellation propagated to a queued or active remote handler."""

    correlation_id: str
    reason: str
    session_id: str = ""
    target_session_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "correlation_id",
            _require_text(self.correlation_id, "correlation_id"),
        )
        object.__setattr__(self, "reason", _require_text(self.reason, "reason"))
        if self.session_id:
            object.__setattr__(
                self,
                "session_id",
                _require_text(self.session_id, "session_id"),
            )
        if self.target_session_id:
            object.__setattr__(
                self,
                "target_session_id",
                _require_text(self.target_session_id, "target_session_id"),
            )


@dataclass(frozen=True, slots=True)
class RpcHealth:
    """Immutable counters and lifecycle state for one RPC endpoint."""

    generation: int
    changed_at: float
    is_closed: bool
    is_ready: bool
    handlers: int
    pending_calls: int
    queued_requests: int
    active_requests: int
    calls_started: int
    calls_completed: int
    calls_failed: int
    calls_timed_out: int
    calls_cancelled: int
    requests_received: int
    requests_overloaded: int
    orphaned_responses: int
    receiver_alive: bool
    workers_alive: int
    last_error: str | None


class RpcCancellation:
    """Cooperative deadline and cancellation state passed to a handler."""

    def __init__(self, cancelled: Event, deadline: float) -> None:
        self._cancelled = cancelled
        self._deadline = deadline

    @property
    def is_cancelled(self) -> bool:
        """Return whether cancellation or the request deadline has fired."""
        return self._cancelled.is_set() or monotonic() >= self._deadline

    @property
    def remaining_seconds(self) -> float:
        """Return the non-negative local execution budget."""
        return max(self._deadline - monotonic(), 0.0)

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for cancellation, bounded by both timeout and request deadline."""
        if timeout is not None:
            _require_nonnegative_number(timeout, "timeout")
        remaining = self.remaining_seconds
        wait_seconds = remaining if timeout is None else min(timeout, remaining)
        self._cancelled.wait(wait_seconds)
        return self.is_cancelled

    def raise_if_cancelled(self) -> None:
        """Raise ``RpcCancelled`` when the caller no longer wants the result."""
        if self.is_cancelled:
            raise RpcCancelled("RPC handler was cancelled or exceeded its deadline")


@dataclass(slots=True)
class _PendingCall:
    request: RpcRequest
    deadline: float
    connection: int
    completed: Event
    response: RpcResponse | None
    error: RpcError | None

    def __init__(
        self,
        request: RpcRequest,
        deadline: float,
        connection: int,
    ) -> None:
        self.request = request
        self.deadline = deadline
        self.connection = connection
        self.completed = Event()
        self.response = None
        self.error = None


@dataclass(frozen=True, slots=True)
class _ServerRequest:
    request: RpcRequest
    cancellation: Event
    deadline: float
    connection: int
