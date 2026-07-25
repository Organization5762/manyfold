"""Bounded coordinator RPC over a dedicated :mod:`TcpTransport` link."""

from __future__ import annotations

from dataclasses import replace
from queue import Empty, Full, Queue
from threading import BoundedSemaphore, Condition, Event, Lock, Thread
from time import monotonic, time
from uuid import uuid4

from . import _transport_rpc_codec as _rpc_codec
from ._transport_rpc_types import (
    RpcCancel,
    RpcCancellation,
    RpcCancelled,
    RpcConfig,
    RpcDisconnected,
    RpcEndpointClosed,
    RpcError,
    RpcErrorRecord,
    RpcHandler,
    RpcHealth,
    RpcOverloaded,
    RpcProtocolError,
    RpcRemoteError,
    RpcRequest,
    RpcResponse,
    RpcShutdownTimeout,
    RpcTimeout,
    _PendingCall,
    _ServerRequest,
)
from ._transport_rpc_validation import (
    format_session_id as _format_session_id,
    parse_session_id as _parse_session_id,
    require_bytes as _require_bytes,
    require_nonnegative_number as _require_nonnegative_number,
    require_positive_number as _require_positive_number,
    require_text as _require_text,
)
from .transport import (
    LinkHealth,
    LinkState,
    TcpTransport,
    TransportClosed,
    TransportQueueFull,
)

_SESSION_SERVICE = "manyfold.rpc.control"
_SESSION_METHOD = "session"


class RpcCall:
    """Handle for awaiting or cancelling one in-flight request."""

    def __init__(self, endpoint: "RpcEndpoint", pending: "_PendingCall") -> None:
        self._endpoint = endpoint
        self._pending = pending

    @property
    def correlation_id(self) -> str:
        """Return the stable correlation identifier sent on the wire."""
        return self._pending.request.correlation_id

    @property
    def is_done(self) -> bool:
        """Return whether this call has reached a terminal local outcome."""
        return self._pending.completed.is_set()

    def result(self) -> bytes:
        """Wait through the request deadline and return payload or raise."""
        return self._endpoint._result(self._pending)

    def cancel(self, reason: str = "cancelled by caller") -> bool:
        """Cancel this call once and propagate cancellation to the peer."""
        return self._endpoint._cancel_call(
            self._pending,
            _require_text(reason, "reason"),
        )


class RpcEndpoint:
    """Bounded coordinator RPC endpoint owning one dedicated transport reader."""

    def __init__(
        self,
        transport: TcpTransport,
        *,
        config: RpcConfig | None = None,
        owns_transport: bool = False,
    ) -> None:
        if not isinstance(transport, TcpTransport):
            raise ValueError("transport must be a TcpTransport")
        self.transport = transport
        self.config = config or RpcConfig()
        if not isinstance(self.config, RpcConfig):
            raise ValueError("config must be an RpcConfig")
        if not isinstance(owns_transport, bool):
            raise ValueError("owns_transport must be a boolean")
        self._owns_transport = owns_transport
        self._condition = Condition(Lock())
        self._stop = Event()
        self._closed = False
        self._pending_slots = BoundedSemaphore(self.config.max_in_flight)
        self._pending: dict[str, _PendingCall] = {}
        self._handlers: dict[tuple[str, str], RpcHandler] = {}
        self._server_requests: dict[str, Event] = {}
        self._request_queue: Queue[_ServerRequest] = Queue(
            maxsize=self.config.request_queue_limit
        )
        self._generation = 0
        self._changed_at = time()
        self._calls_started = 0
        self._calls_completed = 0
        self._calls_failed = 0
        self._calls_timed_out = 0
        self._calls_cancelled = 0
        self._requests_received = 0
        self._requests_overloaded = 0
        self._orphaned_responses = 0
        self._active_requests = 0
        self._last_error: str | None = None
        transport_health = transport.health()
        self._observed_connections = transport_health.connections_established
        self._announced_connection = 0
        self._local_session_id = ""
        self._peer_instance_id: str | None = None
        self._peer_connection_number = 0
        self._peer_session_id: str | None = None
        self._peer_transport_connection = 0
        self._peer_confirmed_local = False
        self._workers = tuple(
            Thread(
                target=self._worker_loop,
                name=f"manyfold-rpc-worker-{index}",
                daemon=True,
            )
            for index in range(self.config.max_workers)
        )
        self._receiver = Thread(
            target=self._receive_loop,
            name="manyfold-rpc-receiver",
            daemon=True,
        )
        for worker in self._workers:
            worker.start()
        self._receiver.start()

    def __enter__(self) -> "RpcEndpoint":
        return self

    def __exit__(self, *error: object) -> None:
        self.close()

    def register(
        self,
        service: str,
        method: str,
        handler: RpcHandler,
    ) -> None:
        """Register one bounded service/method handler."""
        key = (
            _require_text(service, "service"),
            _require_text(method, "method"),
        )
        if key[0] == _SESSION_SERVICE:
            raise ValueError(f"service {_SESSION_SERVICE!r} is reserved for RPC control")
        if not callable(handler):
            raise TypeError("handler must be callable")
        with self._condition:
            self._require_open_locked()
            if key not in self._handlers and len(self._handlers) >= self.config.max_handlers:
                raise RpcOverloaded(
                    "RPC handler registry reached configured max_handlers"
                )
            self._handlers[key] = handler
            self._touch_locked()

    def unregister(self, service: str, method: str) -> bool:
        """Remove one handler and return whether it existed."""
        key = (
            _require_text(service, "service"),
            _require_text(method, "method"),
        )
        with self._condition:
            self._require_open_locked()
            removed = self._handlers.pop(key, None) is not None
            if removed:
                self._touch_locked()
            return removed

    def call(
        self,
        service: str,
        method: str,
        payload: bytes,
        *,
        timeout_seconds: float | None = None,
        correlation_id: str | None = None,
    ) -> bytes:
        """Call one remote handler and wait through its deadline."""
        return self.start_call(
            service,
            method,
            payload,
            timeout_seconds=timeout_seconds,
            correlation_id=correlation_id,
        ).result()

    def start_call(
        self,
        service: str,
        method: str,
        payload: bytes,
        *,
        timeout_seconds: float | None = None,
        correlation_id: str | None = None,
    ) -> RpcCall:
        """Start one bounded request and return its cancellation handle."""
        timeout = (
            self.config.default_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        _require_positive_number(timeout, "timeout_seconds")
        transport_health, session_id, target_session_id = (
            self._ready_transport_session()
        )
        request = RpcRequest(
            correlation_id=correlation_id or uuid4().hex,
            service=service,
            method=method,
            payload=payload,
            timeout_seconds=timeout,
            session_id=session_id,
            target_session_id=target_session_id,
        )
        if not self._pending_slots.acquire(blocking=False):
            raise RpcOverloaded(
                "RPC client reached configured max_in_flight; apply backpressure"
            )
        pending = _PendingCall(
            request,
            monotonic() + timeout,
            transport_health.connections_established,
        )
        with self._condition:
            try:
                self._require_open_locked()
                if (
                    not self._is_ready_locked()
                    or self._local_session_id != session_id
                    or self._peer_session_id != target_session_id
                    or self._announced_connection
                    != transport_health.connections_established
                ):
                    raise RpcDisconnected("RPC transport session changed before send")
                if request.correlation_id in self._pending:
                    raise ValueError(
                        f"duplicate RPC correlation_id {request.correlation_id!r}"
                    )
                self._pending[request.correlation_id] = pending
                self._calls_started += 1
                self._touch_locked()
            except BaseException:
                self._pending_slots.release()
                raise
        try:
            self.transport.send(
                _rpc_codec.encode(request),
                timeout=self.config.send_timeout_seconds,
            )
        except TransportQueueFull as error:
            self._finish_pending(
                pending,
                error=RpcOverloaded("RPC transport outbound queue is full"),
            )
            raise RpcOverloaded("RPC transport outbound queue is full") from error
        except TransportClosed as error:
            self._finish_pending(
                pending,
                error=RpcDisconnected("RPC transport closed while sending request"),
            )
            raise RpcDisconnected(
                "RPC transport closed while sending request"
            ) from error
        except (TypeError, ValueError) as error:
            protocol_error = RpcProtocolError(
                f"RPC request cannot be framed: {error}"
            )
            self._finish_pending(pending, error=protocol_error)
            raise protocol_error from error
        return RpcCall(self, pending)

    def health(self) -> RpcHealth:
        """Return immutable RPC capacity, outcome, and worker counters."""
        with self._condition:
            return self._health_locked()

    def wait_until_ready(self, *, timeout: float | None = None) -> bool:
        """Wait for a validated RPC session on the current transport link."""
        if timeout is not None:
            _require_nonnegative_number(timeout, "timeout")
        with self._condition:
            return self._condition.wait_for(
                lambda: self._closed or self._is_ready_locked(),
                timeout=timeout,
            ) and self._is_ready_locked()

    def wait_for_health_change(
        self,
        after_generation: int,
        *,
        timeout: float | None = None,
    ) -> RpcHealth:
        """Wait for counters or lifecycle state to advance."""
        if (
            isinstance(after_generation, bool)
            or not isinstance(after_generation, int)
            or after_generation < 0
        ):
            raise ValueError("after_generation must be a non-negative integer")
        if timeout is not None:
            _require_nonnegative_number(timeout, "timeout")
        with self._condition:
            changed = self._condition.wait_for(
                lambda: self._generation > after_generation,
                timeout=timeout,
            )
            if not changed:
                raise TimeoutError("RPC health did not change before timeout")
            return self._health_locked()

    def close(self) -> None:
        """Cancel owned work, release retained payloads, and join all workers."""
        with self._condition:
            first_close = not self._closed
            if first_close:
                self._closed = True
                self._stop.set()
                pending = tuple(self._pending.values())
                server_cancellations = tuple(self._server_requests.values())
                self._handlers.clear()
                self._touch_locked()
            else:
                pending = ()
                server_cancellations = ()
        if first_close:
            for cancellation in server_cancellations:
                cancellation.set()
            for call in pending:
                self._send_cancel_best_effort(
                    RpcCancel(call.request.correlation_id, "RPC endpoint disposed")
                )
                self._finish_pending(
                    call,
                    error=RpcEndpointClosed(
                        "RPC endpoint closed before call completed"
                    ),
                )
            self._drain_request_queue()
            if self._owns_transport:
                self.transport.close()
        self._receiver.join(timeout=self.config.shutdown_timeout_seconds)
        deadline = monotonic() + self.config.shutdown_timeout_seconds
        for worker in self._workers:
            worker.join(timeout=max(deadline - monotonic(), 0.0))
        alive = tuple(worker.name for worker in self._workers if worker.is_alive())
        if self._receiver.is_alive():
            alive += (self._receiver.name,)
        if alive:
            raise RpcShutdownTimeout(
                "RPC workers ignored cancellation before shutdown timeout: "
                + ", ".join(alive)
            )

    def _ready_transport_session(self) -> tuple[LinkHealth, str, str]:
        health = self.transport.health()
        if health.state is not LinkState.CONNECTED:
            raise RpcDisconnected("RPC transport is not connected")
        with self._condition:
            self._require_open_locked()
            self._condition.wait_for(
                lambda: self._closed or self._is_ready_locked(),
                timeout=self.config.send_timeout_seconds,
            )
            self._require_open_locked()
            health = self.transport.health()
            if (
                not self._is_ready_locked()
                or health.state is not LinkState.CONNECTED
                or self._announced_connection != health.connections_established
            ):
                raise RpcDisconnected("RPC session handshake is not ready")
            peer_session_id = self._peer_session_id
            if peer_session_id is None:
                raise RpcDisconnected("RPC peer session is unavailable")
            return health, self._local_session_id, peer_session_id

    def _result(self, pending: "_PendingCall") -> bytes:
        remaining = max(pending.deadline - monotonic(), 0.0)
        if not pending.completed.wait(remaining):
            timeout = RpcTimeout(
                f"RPC {pending.request.service}.{pending.request.method} "
                f"timed out after {pending.request.timeout_seconds:g} seconds"
            )
            if self._finish_pending(pending, error=timeout, timed_out=True):
                self._send_cancel_best_effort(
                    RpcCancel(pending.request.correlation_id, "deadline exceeded")
                )
        if pending.error is not None:
            raise pending.error
        if pending.response is None:
            raise RpcProtocolError("RPC call completed without a response or error")
        return pending.response.payload

    def _cancel_call(self, pending: "_PendingCall", reason: str) -> bool:
        cancelled = RpcCancelled(
            f"RPC {pending.request.service}.{pending.request.method} was cancelled"
        )
        if not self._finish_pending(pending, error=cancelled, cancelled=True):
            return False
        self._send_cancel_best_effort(
            RpcCancel(pending.request.correlation_id, reason)
        )
        return True

    def _expire_pending_calls(self) -> None:
        now = monotonic()
        with self._condition:
            expired = tuple(
                pending
                for pending in self._pending.values()
                if pending.deadline <= now
            )
        for pending in expired:
            timeout = RpcTimeout(
                f"RPC {pending.request.service}.{pending.request.method} "
                f"timed out after {pending.request.timeout_seconds:g} seconds"
            )
            if self._finish_pending(pending, error=timeout, timed_out=True):
                self._send_cancel_best_effort(
                    RpcCancel(pending.request.correlation_id, "deadline exceeded")
                )

    def _receive_loop(self) -> None:
        while not self._stop.is_set():
            self._check_link_session()
            self._ensure_session_announcement()
            self._expire_pending_calls()
            try:
                message = self.transport.receive(
                    timeout=self.config.receive_poll_seconds
                )
            except TimeoutError:
                continue
            except TransportClosed:
                self._fail_all_pending(
                    RpcDisconnected("RPC transport closed before response")
                )
                return
            try:
                record = _rpc_codec.decode(message)
            except RpcProtocolError as error:
                self._record_error(error)
                continue
            self._dispatch_record(record)

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                work = self._request_queue.get(
                    timeout=self.config.receive_poll_seconds
                )
            except Empty:
                continue
            with self._condition:
                self._active_requests += 1
                self._touch_locked()
            try:
                self._execute_request(work)
            finally:
                with self._condition:
                    self._active_requests -= 1
                    self._server_requests.pop(
                        work.request.correlation_id,
                        None,
                    )
                    self._touch_locked()
                self._request_queue.task_done()

    def _ensure_session_announcement(self) -> None:
        health = self.transport.health()
        if (
            health.state is not LinkState.CONNECTED
            or health.connections_established < 1
        ):
            return
        session_id = _format_session_id(
            self.transport.identity.instance_id,
            health.connections_established,
        )
        with self._condition:
            if (
                self._closed
                or self._announced_connection == health.connections_established
            ):
                return
            self._local_session_id = session_id
            target_session_id = self._peer_session_id or ""
        hello = RpcRequest(
            correlation_id=f"session-{uuid4().hex}",
            service=_SESSION_SERVICE,
            method=_SESSION_METHOD,
            payload=b"",
            timeout_seconds=self.config.default_timeout_seconds,
            session_id=session_id,
            target_session_id=target_session_id,
        )
        try:
            self.transport.send(
                _rpc_codec.encode(hello),
                timeout=self.config.send_timeout_seconds,
            )
        except (TransportClosed, TransportQueueFull, TypeError, ValueError) as error:
            self._record_error(error)
            return
        current = self.transport.health()
        with self._condition:
            if (
                not self._closed
                and current.state is LinkState.CONNECTED
                and current.connections_established
                == health.connections_established
            ):
                self._announced_connection = health.connections_established
                self._touch_locked()

    def _dispatch_record(
        self,
        record: RpcRequest | RpcResponse | RpcErrorRecord | RpcCancel,
    ) -> None:
        if (
            isinstance(record, RpcRequest)
            and record.service == _SESSION_SERVICE
            and record.method == _SESSION_METHOD
        ):
            self._accept_session_hello(record)
            return
        health = self.transport.health()
        with self._condition:
            peer_session_id = self._peer_session_id
            peer_transport_connection = self._peer_transport_connection
        if (
            health.state is not LinkState.CONNECTED
            or health.connections_established != peer_transport_connection
            or record.session_id != peer_session_id
            or record.target_session_id != self._local_session_id
        ):
            self._record_error(
                RpcProtocolError(
                    "discarded RPC record from an inactive transport session"
                )
            )
            return
        if isinstance(record, RpcRequest):
            self._accept_request(record)
        elif isinstance(record, RpcResponse):
            self._complete_response(record)
        elif isinstance(record, RpcErrorRecord):
            self._complete_error(record)
        else:
            with self._condition:
                cancellation = self._server_requests.get(record.correlation_id)
                if cancellation is not None:
                    cancellation.set()
                    self._touch_locked()

    def _accept_session_hello(self, request: RpcRequest) -> None:
        try:
            instance_id, connection_number = _parse_session_id(request.session_id)
        except ValueError as error:
            self._record_error(RpcProtocolError(f"invalid RPC session hello: {error}"))
            return
        health = self.transport.health()
        remote_identity = health.remote_identity
        if (
            request.payload
            or health.state is not LinkState.CONNECTED
            or remote_identity is None
            or remote_identity.instance_id != instance_id
        ):
            self._record_error(
                RpcProtocolError("RPC session hello does not match transport peer")
            )
            return
        with self._condition:
            if self._closed:
                return
            is_duplicate = (
                self._peer_session_id == request.session_id
                and self._peer_transport_connection
                == health.connections_established
            )
            is_stale = (
                self._peer_instance_id == instance_id
                and connection_number <= self._peer_connection_number
                and not is_duplicate
            )
            if is_stale:
                error = RpcProtocolError(
                    "discarded stale RPC session hello after reconnect"
                )
            else:
                confirms_local = (
                    bool(self._local_session_id)
                    and request.target_session_id == self._local_session_id
                )
                self._peer_instance_id = instance_id
                self._peer_connection_number = connection_number
                self._peer_session_id = request.session_id
                self._peer_transport_connection = health.connections_established
                self._peer_confirmed_local = confirms_local
                if not is_duplicate or not confirms_local:
                    self._announced_connection = 0
                self._touch_locked()
                error = None
        if error is not None:
            self._record_error(error)

    def _accept_request(self, request: RpcRequest) -> None:
        cancellation = Event()
        connection = self.transport.health().connections_established
        work = _ServerRequest(
            request=request,
            cancellation=cancellation,
            deadline=monotonic() + request.timeout_seconds,
            connection=connection,
        )
        with self._condition:
            if self._closed:
                return
            self._requests_received += 1
            if request.correlation_id in self._server_requests:
                self._touch_locked()
                error = RpcErrorRecord(
                    request.correlation_id,
                    "duplicate_request",
                    "correlation_id is already queued or active",
                )
            else:
                self._server_requests[request.correlation_id] = cancellation
                try:
                    self._request_queue.put_nowait(work)
                except Full:
                    self._server_requests.pop(request.correlation_id, None)
                    self._requests_overloaded += 1
                    error = RpcErrorRecord(
                        request.correlation_id,
                        "overloaded",
                        "remote RPC request queue is full",
                        retryable=True,
                    )
                else:
                    error = None
                self._touch_locked()
        if error is not None:
            self._send_record_best_effort(error)

    def _execute_request(self, work: "_ServerRequest") -> None:
        request = work.request
        context = RpcCancellation(work.cancellation, work.deadline)
        if context.is_cancelled:
            self._send_server_record(
                work,
                RpcErrorRecord(
                    request.correlation_id,
                    "deadline_exceeded",
                    "RPC deadline elapsed before handler execution",
                    retryable=True,
                ),
            )
            return
        with self._condition:
            handler = self._handlers.get((request.service, request.method))
        if handler is None:
            self._send_server_record(
                work,
                RpcErrorRecord(
                    request.correlation_id,
                    "not_found",
                    f"no RPC handler for {request.service}.{request.method}",
                ),
            )
            return
        try:
            payload = _require_bytes(
                handler(request, context),
                "RPC handler result",
            )
            context.raise_if_cancelled()
        except RpcCancelled:
            deadline_exceeded = monotonic() >= work.deadline
            self._send_server_record(
                work,
                RpcErrorRecord(
                    request.correlation_id,
                    "deadline_exceeded" if deadline_exceeded else "cancelled",
                    (
                        "RPC handler exceeded its deadline"
                        if deadline_exceeded
                        else "RPC handler observed caller cancellation"
                    ),
                ),
            )
        except Exception as error:
            self._record_error(error)
            self._send_server_record(
                work,
                RpcErrorRecord(
                    request.correlation_id,
                    "handler_error",
                    "remote RPC handler failed",
                ),
            )
        else:
            self._send_server_record(
                work,
                RpcResponse(request.correlation_id, payload),
            )

    def _complete_response(self, response: RpcResponse) -> None:
        with self._condition:
            pending = self._pending.get(response.correlation_id)
            if pending is None:
                self._orphaned_responses += 1
                self._touch_locked()
                return
        self._finish_pending(pending, response=response)

    def _complete_error(self, error: RpcErrorRecord) -> None:
        with self._condition:
            pending = self._pending.get(error.correlation_id)
            if pending is None:
                self._orphaned_responses += 1
                self._touch_locked()
                return
        if error.code == "deadline_exceeded":
            self._finish_pending(
                pending,
                error=RpcTimeout(error.message),
                timed_out=True,
            )
        else:
            self._finish_pending(pending, error=RpcRemoteError(error))

    def _finish_pending(
        self,
        pending: "_PendingCall",
        *,
        response: RpcResponse | None = None,
        error: RpcError | None = None,
        timed_out: bool = False,
        cancelled: bool = False,
    ) -> bool:
        with self._condition:
            correlation_id = pending.request.correlation_id
            if self._pending.get(correlation_id) is not pending:
                return False
            self._pending.pop(correlation_id)
            pending.response = response
            pending.error = error
            if response is not None:
                self._calls_completed += 1
            else:
                self._calls_failed += 1
            if timed_out:
                self._calls_timed_out += 1
            if cancelled:
                self._calls_cancelled += 1
            self._pending_slots.release()
            pending.completed.set()
            self._touch_locked()
            return True

    def _check_link_session(self) -> None:
        health = self.transport.health()
        connection_changed = (
            self._observed_connections > 0
            and health.connections_established != self._observed_connections
        )
        disconnected = (
            self._observed_connections > 0
            and health.state is not LinkState.CONNECTED
        )
        if connection_changed:
            self._fail_prior_session_pending(
                health.connections_established,
                RpcDisconnected("RPC transport reconnected before response"),
            )
            self._cancel_server_requests()
        elif disconnected:
            self._fail_all_pending(
                RpcDisconnected("RPC transport disconnected before response")
            )
            self._cancel_server_requests()
        if connection_changed or disconnected:
            with self._condition:
                should_clear_peer = (
                    disconnected
                    or self._peer_transport_connection
                    != health.connections_established
                )
                changed = should_clear_peer and self._peer_session_id is not None
                if should_clear_peer:
                    self._peer_session_id = None
                    self._peer_transport_connection = 0
                    self._peer_confirmed_local = False
                if connection_changed:
                    changed = True
                    self._announced_connection = 0
                    self._local_session_id = ""
                if changed:
                    self._touch_locked()
        self._observed_connections = health.connections_established

    def _fail_all_pending(self, error: RpcError) -> None:
        with self._condition:
            pending = tuple(self._pending.values())
        for call in pending:
            self._finish_pending(call, error=type(error)(str(error)))

    def _fail_prior_session_pending(
        self,
        current_connection: int,
        error: RpcError,
    ) -> None:
        with self._condition:
            pending = tuple(
                call
                for call in self._pending.values()
                if call.connection != current_connection
            )
        for call in pending:
            self._finish_pending(call, error=type(error)(str(error)))

    def _cancel_server_requests(self) -> None:
        with self._condition:
            cancellations = tuple(self._server_requests.values())
        for cancellation in cancellations:
            cancellation.set()

    def _send_server_record(
        self,
        work: "_ServerRequest",
        record: RpcResponse | RpcErrorRecord,
    ) -> None:
        health = self.transport.health()
        with self._condition:
            is_closed = self._closed
            peer_session_id = self._peer_session_id
        if (
            is_closed
            or health.state is not LinkState.CONNECTED
            or health.connections_established != work.connection
            or work.request.session_id != peer_session_id
        ):
            return
        send_error = self._send_record_best_effort(record)
        if isinstance(send_error, (TypeError, ValueError)) and isinstance(
            record,
            RpcResponse,
        ):
            self._send_record_best_effort(
                RpcErrorRecord(
                    record.correlation_id,
                    "response_too_large",
                    "RPC handler result exceeds the transport payload limit",
                )
            )

    def _send_cancel_best_effort(self, cancel: RpcCancel) -> None:
        self._send_record_best_effort(cancel)

    def _send_record_best_effort(
        self,
        record: RpcResponse | RpcErrorRecord | RpcCancel,
    ) -> BaseException | None:
        with self._condition:
            session_id = self._local_session_id
            target_session_id = self._peer_session_id or ""
        record = replace(
            record,
            session_id=session_id,
            target_session_id=target_session_id,
        )
        try:
            self.transport.send(
                _rpc_codec.encode(record),
                timeout=self.config.send_timeout_seconds,
            )
        except (
            TransportClosed,
            TransportQueueFull,
            TypeError,
            ValueError,
        ) as error:
            self._record_error(error)
            return error
        return None

    def _record_error(self, error: BaseException) -> None:
        with self._condition:
            self._last_error = f"{type(error).__name__}: {error}"
            self._touch_locked()

    def _drain_request_queue(self) -> None:
        while True:
            try:
                work = self._request_queue.get_nowait()
            except Empty:
                return
            with self._condition:
                self._server_requests.pop(work.request.correlation_id, None)
                self._touch_locked()
            self._request_queue.task_done()

    def _require_open_locked(self) -> None:
        if self._closed:
            raise RpcEndpointClosed("RPC endpoint is closed")

    def _is_ready_locked(self) -> bool:
        return (
            not self._closed
            and self._announced_connection == self._observed_connections
            and self._announced_connection > 0
            and self._peer_session_id is not None
            and self._peer_confirmed_local
        )

    def _touch_locked(self) -> None:
        self._generation += 1
        self._changed_at = time()
        self._condition.notify_all()

    def _health_locked(self) -> RpcHealth:
        return RpcHealth(
            generation=self._generation,
            changed_at=self._changed_at,
            is_closed=self._closed,
            is_ready=self._is_ready_locked(),
            handlers=len(self._handlers),
            pending_calls=len(self._pending),
            queued_requests=self._request_queue.qsize(),
            active_requests=self._active_requests,
            calls_started=self._calls_started,
            calls_completed=self._calls_completed,
            calls_failed=self._calls_failed,
            calls_timed_out=self._calls_timed_out,
            calls_cancelled=self._calls_cancelled,
            requests_received=self._requests_received,
            requests_overloaded=self._requests_overloaded,
            orphaned_responses=self._orphaned_responses,
            receiver_alive=self._receiver.is_alive(),
            workers_alive=sum(worker.is_alive() for worker in self._workers),
            last_error=self._last_error,
        )


__all__ = [
    "RpcCall",
    "RpcCancel",
    "RpcCancellation",
    "RpcCancelled",
    "RpcConfig",
    "RpcDisconnected",
    "RpcEndpoint",
    "RpcEndpointClosed",
    "RpcError",
    "RpcErrorRecord",
    "RpcHandler",
    "RpcHealth",
    "RpcOverloaded",
    "RpcProtocolError",
    "RpcRemoteError",
    "RpcRequest",
    "RpcResponse",
    "RpcShutdownTimeout",
    "RpcTimeout",
]
