"""Bounded TCP transport for cross-process Manyfold links."""

from __future__ import annotations

import json
import socket
import ssl
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from queue import Empty, Full, Queue
from threading import BoundedSemaphore, Condition, Event, Lock, Thread
from time import monotonic, time
from uuid import uuid4

from manyfold.graph import Link, LinkCapabilities

from . import _transport_config as _config, _transport_wire as _wire
from ._transport_config import (
    DEFAULT_MAX_PAYLOAD_BYTES as DEFAULT_MAX_PAYLOAD_BYTES,
    DEFAULT_QUEUE_LIMIT as DEFAULT_QUEUE_LIMIT,
    ReconnectPolicy as ReconnectPolicy,
    TransportConfig as TransportConfig,
    TransportSecurity as TransportSecurity,
    TransportSecurityMode as TransportSecurityMode,
)

PROTOCOL_NAME = _wire.PROTOCOL_NAME
PROTOCOL_VERSION = _wire.PROTOCOL_VERSION


class TransportError(RuntimeError):
    """Base error for cross-process transport failures."""


class TransportClosed(TransportError):
    """Raised when an operation targets a closed transport."""


class TransportQueueFull(TransportError):
    """Raised when bounded outbound retention has no remaining capacity."""


class TransportProtocolError(TransportError):
    """Raised when a peer sends malformed or incompatible protocol data."""


class TransportIdentityError(TransportError):
    """Raised when a peer identity violates link expectations."""


class FrameKind(IntEnum):
    """Application frame categories supported by the transport."""

    PUBSUB = 1
    RPC_REQUEST = 2
    RPC_RESPONSE = 3
    RPC_ERROR = 4


@dataclass(frozen=True, slots=True)
class NodeIdentity:
    """Explicit identity exchanged by both ends of a transport link."""

    cluster_id: str
    node_id: str
    instance_id: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "cluster_id",
            _require_text(self.cluster_id, "cluster_id"),
        )
        object.__setattr__(self, "node_id", _require_text(self.node_id, "node_id"))
        object.__setattr__(
            self,
            "instance_id",
            _require_text(self.instance_id, "instance_id"),
        )


@dataclass(frozen=True, slots=True)
class TcpAddress:
    """Host and port for one TCP transport endpoint."""

    host: str
    port: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", _require_text(self.host, "host"))
        if isinstance(self.port, bool) or not isinstance(self.port, int):
            raise ValueError("port must be an integer")
        if not 0 <= self.port <= 65535:
            raise ValueError("port must be between 0 and 65535")


@dataclass(frozen=True, slots=True)
class TransportMessage:
    """One PubSub or RPC payload transferred as a bounded wire frame."""

    kind: FrameKind
    channel: str
    payload: bytes
    correlation_id: str | None = None
    sequence: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.kind, FrameKind):
            raise ValueError("kind must be a FrameKind")
        object.__setattr__(self, "channel", _require_text(self.channel, "channel"))
        if not isinstance(self.payload, bytes | bytearray | memoryview):
            raise TypeError("payload must be bytes-like")
        object.__setattr__(self, "payload", bytes(self.payload))
        if self.correlation_id is not None:
            object.__setattr__(
                self,
                "correlation_id",
                _require_text(self.correlation_id, "correlation_id"),
            )
        if self.kind is not FrameKind.PUBSUB and self.correlation_id is None:
            raise ValueError("RPC messages require correlation_id")
        if (
            isinstance(self.sequence, bool)
            or not isinstance(self.sequence, int)
            or self.sequence < 0
        ):
            raise ValueError("sequence must be a non-negative integer")


class LinkState(str, Enum):
    """Current cross-process link lifecycle state."""

    STARTING = "starting"
    LISTENING = "listening"
    RECONNECTING = "reconnecting"
    HANDSHAKING = "handshaking"
    CONNECTED = "connected"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class LinkHealth:
    """Immutable observable snapshot of link state and bounded retention."""

    generation: int
    state: LinkState
    local_identity: NodeIdentity
    remote_identity: NodeIdentity | None
    changed_at: float
    connected_at: float | None
    last_sent_at: float | None
    last_received_at: float | None
    connection_attempts: int
    connections_established: int
    frames_sent: int
    frames_received: int
    bytes_sent: int
    bytes_received: int
    outbound_pending: int
    inbound_queued: int
    last_error: str | None


class TcpTransport:
    """One real, bounded, reconnecting TCP link between two Manyfold nodes."""

    def __init__(
        self,
        *,
        identity: NodeIdentity,
        mode: "_Mode",
        address: TcpAddress,
        config: TransportConfig,
        expected_peer_node_id: str | None,
        listener: socket.socket | None,
    ) -> None:
        if not isinstance(identity, NodeIdentity):
            raise ValueError("identity must be a NodeIdentity")
        if not isinstance(address, TcpAddress):
            raise ValueError("address must be a TcpAddress")
        if not isinstance(config, TransportConfig):
            raise ValueError("config must be a TransportConfig")
        if (
            config.security.mode is TransportSecurityMode.INSECURE_LOCAL_DEVELOPMENT
            and not _is_loopback_host(address.host)
        ):
            raise ValueError(
                "insecure local-development transport requires a loopback address"
            )
        if config.security.mode is TransportSecurityMode.MUTUAL_TLS:
            context = config.security.resolve_ssl_context()
            if mode is _Mode.CONNECTOR and not context.check_hostname:
                raise ValueError(
                    "connector mutual TLS SSLContext must enable hostname checking"
                )
            if mode is _Mode.LISTENER:
                if context.check_hostname:
                    raise ValueError(
                        "listener mutual TLS SSLContext cannot check hostnames"
                    )
                if config.security.server_hostname is not None:
                    raise ValueError("listener mutual TLS does not use server_hostname")
        self.identity = identity
        self.address = address
        self.config = config
        self._mode = mode
        self._expected_peer_node_id = (
            None
            if expected_peer_node_id is None
            else _require_text(expected_peer_node_id, "expected_peer_node_id")
        )
        self._listener = listener
        self._outbound: Queue[_OutboundItem] = Queue(
            maxsize=config.outbound_queue_limit
        )
        self._inbound: Queue[TransportMessage | object] = Queue(
            maxsize=config.inbound_queue_limit
        )
        self._outbound_slots = BoundedSemaphore(config.outbound_queue_limit)
        self._stop = Event()
        self._connection_ready = Event()
        self._condition = Condition(Lock())
        self._connection: socket.socket | None = None
        self._handshake_connection: socket.socket | None = None
        self._state = (
            LinkState.LISTENING if mode is _Mode.LISTENER else LinkState.RECONNECTING
        )
        self._remote_identity: NodeIdentity | None = None
        self._generation = 0
        self._changed_at = time()
        self._connected_at: float | None = None
        self._last_sent_at: float | None = None
        self._last_received_at: float | None = None
        self._connection_attempts = 0
        self._connections_established = 0
        self._frames_sent = 0
        self._frames_received = 0
        self._bytes_sent = 0
        self._bytes_received = 0
        self._outbound_pending = 0
        self._outbound_serial = 0
        self._discarded_outbound_serials: dict[str, int] = {}
        self._last_error: str | None = None
        self._last_remote_sequence = 0
        self._remote_instance_id: str | None = None
        self._supervisor = Thread(
            target=(
                self._run_listener if mode is _Mode.LISTENER else self._run_connector
            ),
            name=f"manyfold-transport-{identity.node_id}-supervisor",
            daemon=True,
        )
        self._writer = Thread(
            target=self._run_writer,
            name=f"manyfold-transport-{identity.node_id}-writer",
            daemon=True,
        )
        self._supervisor.start()
        self._writer.start()

    @classmethod
    def listen(
        cls,
        identity: NodeIdentity,
        address: TcpAddress | None = None,
        *,
        config: TransportConfig,
        expected_peer_node_id: str | None = None,
    ) -> "TcpTransport":
        """Bind a listener and accept one active peer at a time."""
        address = address or TcpAddress("127.0.0.1", 0)
        if not isinstance(address, TcpAddress):
            raise ValueError("address must be a TcpAddress")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((address.host, address.port))
            listener.listen(8)
            listener.settimeout(0.2)
            bound_host, bound_port = listener.getsockname()[:2]
            bound_address = TcpAddress(str(bound_host), int(bound_port))
            return cls(
                identity=identity,
                mode=_Mode.LISTENER,
                address=bound_address,
                config=config,
                expected_peer_node_id=expected_peer_node_id,
                listener=listener,
            )
        except BaseException:
            listener.close()
            raise

    @classmethod
    def connect(
        cls,
        identity: NodeIdentity,
        address: TcpAddress,
        *,
        config: TransportConfig,
        expected_peer_node_id: str | None = None,
    ) -> "TcpTransport":
        """Start a client that reconnects to one TCP endpoint until closed."""
        return cls(
            identity=identity,
            mode=_Mode.CONNECTOR,
            address=address,
            config=config,
            expected_peer_node_id=expected_peer_node_id,
            listener=None,
        )

    def __enter__(self) -> "TcpTransport":
        return self

    def __exit__(self, *error: object) -> None:
        self.close()

    @property
    def link_capabilities(self) -> LinkCapabilities:
        """Return graph Link semantics for the active TCP session."""
        is_mutual_tls = self.config.security.mode is TransportSecurityMode.MUTUAL_TLS
        return LinkCapabilities(
            ordered=True,
            reliable=True,
            encrypted=is_mutual_tls,
            authenticated=is_mutual_tls,
        )

    def as_link(self, name: str) -> Link:
        """Describe this concrete transport for graph topology registration."""
        return Link(
            name=_require_text(name, "link name"),
            link_class=type(self).__name__,
            capabilities=self.link_capabilities,
        )

    def health(self) -> LinkHealth:
        """Return the latest immutable link-health snapshot."""
        with self._condition:
            return self._health_locked()

    def wait_for_health_change(
        self,
        after_generation: int,
        *,
        timeout: float | None = None,
    ) -> LinkHealth:
        """Wait until health advances beyond ``after_generation``."""
        if (
            isinstance(after_generation, bool)
            or not isinstance(after_generation, int)
            or after_generation < 0
        ):
            raise ValueError("after_generation must be a non-negative integer")
        _require_optional_timeout(timeout)
        with self._condition:
            changed = self._condition.wait_for(
                lambda: self._generation > after_generation,
                timeout=timeout,
            )
            if not changed:
                raise TimeoutError("transport health did not change before timeout")
            return self._health_locked()

    def wait_until_connected(self, *, timeout: float | None = None) -> bool:
        """Wait for a validated peer connection, returning false on timeout."""
        _require_optional_timeout(timeout)
        with self._condition:
            return (
                self._condition.wait_for(
                    lambda: self._state in (LinkState.CONNECTED, LinkState.CLOSED),
                    timeout=timeout,
                )
                and self._state is LinkState.CONNECTED
            )

    def send(self, message: TransportMessage, *, timeout: float = 0.0) -> None:
        """Queue one frame, applying bounded backpressure at the caller."""
        if not isinstance(message, TransportMessage):
            raise TypeError("message must be a TransportMessage")
        if message.sequence != 0:
            raise ValueError("outbound message sequence must be zero")
        _require_nonnegative_number(timeout, "timeout")
        if len(message.payload) > self.config.max_payload_bytes:
            raise ValueError(
                "payload exceeds configured max_payload_bytes "
                f"({len(message.payload)} > {self.config.max_payload_bytes})"
            )
        channel_bytes = message.channel.encode("utf-8")
        correlation_bytes = (
            b""
            if message.correlation_id is None
            else message.correlation_id.encode("utf-8")
        )
        if len(channel_bytes) > _wire.MAX_TEXT_BYTES:
            raise ValueError("encoded channel is too long")
        if len(correlation_bytes) > _wire.MAX_TEXT_BYTES:
            raise ValueError("encoded correlation_id is too long")
        acquired = (
            self._outbound_slots.acquire(timeout=timeout)
            if timeout > 0
            else self._outbound_slots.acquire(blocking=False)
        )
        if not acquired:
            raise TransportQueueFull(
                "outbound transport queue is full; retry or apply backpressure"
            )
        with self._condition:
            if self._state is LinkState.CLOSED:
                self._outbound_slots.release()
                raise TransportClosed("transport is closed")
            self._outbound_pending += 1
            self._outbound_serial += 1
            outbound = _OutboundItem(message, self._outbound_serial)
            self._generation += 1
            self._changed_at = time()
            self._condition.notify_all()
            try:
                self._outbound.put_nowait(outbound)
            except Full as error:
                self._outbound_pending -= 1
                self._outbound_slots.release()
                raise TransportQueueFull(
                    "outbound transport queue is full; retry or apply backpressure"
                ) from error

    def flush(self, *, timeout: float | None = None) -> bool:
        """Wait until every accepted outbound frame has reached the socket."""
        _require_optional_timeout(timeout)
        with self._condition:
            return (
                self._condition.wait_for(
                    lambda: (
                        self._outbound_pending == 0 or self._state is LinkState.CLOSED
                    ),
                    timeout=timeout,
                )
                and self._outbound_pending == 0
            )

    def receive(self, *, timeout: float | None = None) -> TransportMessage:
        """Receive one application frame or raise ``TimeoutError``."""
        _require_optional_timeout(timeout)
        try:
            item = self._inbound.get(timeout=timeout)
        except Empty as error:
            if self._stop.is_set():
                raise TransportClosed("transport is closed") from error
            raise TimeoutError("no transport message arrived before timeout") from error
        if item is _CLOSED_SENTINEL:
            raise TransportClosed("transport is closed")
        if not isinstance(item, TransportMessage):
            raise TransportProtocolError("inbound queue contained an invalid frame")
        self._inbound.task_done()
        self._notify_queue_depth_changed()
        return item

    def close(self, *, graceful_timeout: float = 0.0) -> None:
        """Stop reconnecting, close sockets, release queues, and join workers."""
        _require_nonnegative_number(graceful_timeout, "graceful_timeout")
        if graceful_timeout:
            self.flush(timeout=graceful_timeout)
        with self._condition:
            if self._state is LinkState.CLOSED:
                return
            self._state = LinkState.CLOSED
            self._generation += 1
            self._changed_at = time()
            self._stop.set()
            self._connection_ready.set()
            connection = self._connection
            self._connection = None
            handshake_connection = self._handshake_connection
            self._handshake_connection = None
            listener = self._listener
            self._listener = None
            self._condition.notify_all()
        _wire.close_socket(connection)
        if handshake_connection is not connection:
            _wire.close_socket(handshake_connection)
        _wire.close_socket(listener)
        self._drain_queues()
        try:
            self._inbound.put_nowait(_CLOSED_SENTINEL)
        except Full:
            pass
        self._supervisor.join(timeout=2.0)
        self._writer.join(timeout=2.0)

    def _run_connector(self) -> None:
        consecutive_failures = 0
        while not self._stop.is_set():
            self._record_connection_attempt()
            self._set_state(LinkState.RECONNECTING)
            connection: socket.socket | None = None
            try:
                connection = socket.create_connection(
                    (self.address.host, self.address.port),
                    timeout=self.config.connect_timeout,
                )
                self._set_handshake_connection(connection)
                connection.settimeout(self.config.handshake_timeout)
                self._set_state(LinkState.HANDSHAKING)
                connection = self._secure_connection(
                    connection,
                    server_side=False,
                )
                _wire.configure_socket(connection, self.config.peer_timeout)
                remote_identity = self._handshake(connection)
                self._install_connection(connection, remote_identity)
                connection = None
                consecutive_failures = 0
                self._read_connection()
            except (
                OSError,
                TransportError,
                TimeoutError,
                _wire._WireError,
            ) as error:
                self._drop_connection(connection, error)
            if self._stop.is_set():
                return
            consecutive_failures += 1
            delay = self.config.reconnect.delay_for_failure(consecutive_failures)
            self._stop.wait(delay)

    def _run_listener(self) -> None:
        while not self._stop.is_set():
            listener = self._listener
            if listener is None:
                return
            self._set_state(LinkState.LISTENING, preserve_error=True)
            connection: socket.socket | None = None
            try:
                connection, _ = listener.accept()
                self._set_handshake_connection(connection)
                connection.settimeout(self.config.handshake_timeout)
                self._set_state(LinkState.HANDSHAKING)
                connection = self._secure_connection(
                    connection,
                    server_side=True,
                )
                _wire.configure_socket(connection, self.config.peer_timeout)
                remote_identity = self._handshake(connection)
                self._install_connection(connection, remote_identity)
                connection = None
                self._read_connection()
            except socket.timeout as error:
                if connection is not None:
                    self._drop_connection(connection, error)
                continue
            except (
                OSError,
                TransportError,
                TimeoutError,
                _wire._WireError,
            ) as error:
                if self._stop.is_set():
                    _wire.close_socket(connection)
                    return
                self._drop_connection(connection, error)

    def _run_writer(self) -> None:
        pending: _OutboundItem | None = None
        encoded: bytes | None = None
        sequence = 0
        last_write = monotonic()
        try:
            while not self._stop.is_set():
                if not self._connection_ready.wait(timeout=0.1):
                    continue
                if self._stop.is_set():
                    return
                with self._condition:
                    connection = self._connection
                if connection is None:
                    self._connection_ready.clear()
                    continue
                if pending is not None and self._is_outbound_discarded(pending):
                    self._outbound.task_done()
                    self._complete_outbound()
                    pending = None
                    encoded = None
                    continue
                if encoded is None:
                    if pending is None:
                        wait = max(
                            self.config.heartbeat_interval - (monotonic() - last_write),
                            0.0,
                        )
                        try:
                            pending = self._outbound.get(timeout=wait)
                        except Empty:
                            sequence += 1
                            encoded = _wire.encode_heartbeat(sequence)
                    if pending is not None:
                        sequence += 1
                        encoded = _encode_message(
                            pending.message,
                            sequence=sequence,
                            max_payload_bytes=self.config.max_payload_bytes,
                        )
                if encoded is None:
                    continue
                try:
                    connection.sendall(encoded)
                except OSError as error:
                    self._drop_connection(connection, error)
                    continue
                self._record_sent(len(encoded))
                last_write = monotonic()
                if pending is not None:
                    self._outbound.task_done()
                    self._complete_outbound()
                    pending = None
                encoded = None
        finally:
            if pending is not None:
                self._complete_outbound()

    def _read_connection(self) -> None:
        while not self._stop.is_set():
            with self._condition:
                connection = self._connection
            if connection is None:
                return
            deadline = monotonic() + self.config.peer_timeout
            frame = _wire.read_frame(
                connection,
                max_payload_bytes=self.config.max_payload_bytes,
                deadline=deadline,
                stop=self._stop,
            )
            self._record_received(frame.wire_size)
            if frame.kind == _wire.HEARTBEAT_KIND:
                self._accept_remote_sequence(frame.sequence)
                continue
            if not self._accept_remote_sequence(frame.sequence):
                continue
            message = _decode_message(frame)
            while not self._stop.is_set():
                try:
                    self._inbound.put(message, timeout=0.1)
                    self._notify_queue_depth_changed()
                    break
                except Full:
                    continue

    def _handshake(self, connection: socket.socket) -> NodeIdentity:
        hello = json.dumps(
            {
                "protocol": PROTOCOL_NAME,
                "version": list(PROTOCOL_VERSION),
                "cluster_id": self.identity.cluster_id,
                "node_id": self.identity.node_id,
                "instance_id": self.identity.instance_id,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(hello) > _wire.MAX_IDENTITY_BYTES:
            raise TransportIdentityError("encoded local identity is too large")
        connection.sendall(_wire.encode_frame(_wire.HELLO_KIND, b"", b"", hello, 0))
        frame = _wire.read_frame(
            connection,
            max_payload_bytes=_wire.MAX_IDENTITY_BYTES,
            deadline=monotonic() + self.config.handshake_timeout,
            stop=self._stop,
        )
        if frame.kind != _wire.HELLO_KIND or frame.sequence != 0:
            raise TransportProtocolError(
                "peer did not begin with an identity handshake"
            )
        if frame.channel or frame.correlation_id:
            raise TransportProtocolError("identity handshake contains frame metadata")
        identity = _decode_identity(frame.payload)
        if identity.cluster_id != self.identity.cluster_id:
            raise TransportIdentityError(
                "peer cluster_id does not match local cluster "
                f"({identity.cluster_id!r} != {self.identity.cluster_id!r})"
            )
        if identity.node_id == self.identity.node_id:
            raise TransportIdentityError(
                f"peer node_id duplicates local node_id {identity.node_id!r}"
            )
        if (
            self._expected_peer_node_id is not None
            and identity.node_id != self._expected_peer_node_id
        ):
            raise TransportIdentityError(
                "peer node_id does not match expected peer "
                f"({identity.node_id!r} != {self._expected_peer_node_id!r})"
            )
        if self.config.security.mode is TransportSecurityMode.MUTUAL_TLS:
            self._verify_certificate_identity(connection, identity)
        return identity

    def _verify_certificate_identity(
        self,
        connection: socket.socket,
        identity: NodeIdentity,
    ) -> None:
        if not isinstance(connection, ssl.SSLSocket):
            raise TransportIdentityError(
                "mutual TLS connection did not produce an SSL socket"
            )
        if not _config._peer_certificate_matches_identity(
            connection,
            cluster_id=identity.cluster_id,
            node_id=identity.node_id,
        ):
            raise TransportIdentityError(
                "peer certificate does not bind the claimed Manyfold identity"
            )

    def _install_connection(
        self,
        connection: socket.socket,
        remote_identity: NodeIdentity,
    ) -> None:
        with self._condition:
            if self._state is LinkState.CLOSED:
                raise TransportClosed("transport closed during handshake")
            if self._handshake_connection is not connection:
                raise TransportError("transport handshake socket ownership changed")
            self._handshake_connection = None
            previous = self._connection
            self._connection = connection
            self._remote_identity = remote_identity
            self._last_remote_sequence = 0
            self._remote_instance_id = remote_identity.instance_id
            self._state = LinkState.CONNECTED
            self._connections_established += 1
            self._connected_at = time()
            self._last_received_at = self._connected_at
            self._last_error = None
            self._generation += 1
            self._changed_at = self._connected_at
            self._connection_ready.set()
            self._condition.notify_all()
        _wire.close_socket(previous)

    def _secure_connection(
        self,
        connection: socket.socket,
        *,
        server_side: bool,
    ) -> socket.socket:
        security = self.config.security
        if security.mode is TransportSecurityMode.INSECURE_LOCAL_DEVELOPMENT:
            return connection
        try:
            context = security.resolve_ssl_context()
        except (TypeError, ValueError) as error:
            raise TransportError(
                f"mutual TLS SSLContext is unavailable: {error}"
            ) from error
        secured_connection = context.wrap_socket(
            connection,
            server_side=server_side,
            server_hostname=(
                None if server_side else security.server_hostname or self.address.host
            ),
            do_handshake_on_connect=False,
        )
        self._replace_handshake_connection(connection, secured_connection)
        try:
            secured_connection.do_handshake()
        except OSError:
            self._clear_handshake_connection(secured_connection)
            _wire.close_socket(secured_connection)
            raise
        return secured_connection

    def _set_handshake_connection(self, connection: socket.socket) -> None:
        with self._condition:
            if self._state is not LinkState.CLOSED:
                if self._handshake_connection is not None:
                    raise TransportError("transport already owns a handshake socket")
                self._handshake_connection = connection
                return
        _wire.close_socket(connection)
        raise TransportClosed("transport closed before handshake")

    def _replace_handshake_connection(
        self,
        previous: socket.socket,
        connection: socket.socket,
    ) -> None:
        with self._condition:
            if self._state is LinkState.CLOSED:
                error = TransportClosed("transport closed during TLS setup")
            elif self._handshake_connection is not previous:
                error = TransportError("transport handshake socket ownership changed")
            else:
                self._handshake_connection = connection
                return
        _wire.close_socket(connection)
        raise error

    def _clear_handshake_connection(self, connection: socket.socket) -> None:
        with self._condition:
            if self._handshake_connection is connection:
                self._handshake_connection = None

    def _drop_connection(
        self,
        connection: socket.socket | None,
        error: BaseException,
    ) -> None:
        with self._condition:
            if self._handshake_connection is connection:
                self._handshake_connection = None
            active = self._connection
            if (
                active is not None
                and connection is not None
                and active is not connection
            ):
                # A writer can observe an error from the previous socket after the
                # supervisor has already installed its replacement.
                stale_connection = True
            else:
                stale_connection = False
            if stale_connection:
                connection_to_close = connection
            else:
                connection_to_close = active
                self._connection = None
                self._connection_ready.clear()
                self._last_error = f"{type(error).__name__}: {error}"
                if self._state is not LinkState.CLOSED:
                    self._state = (
                        LinkState.LISTENING
                        if self._mode is _Mode.LISTENER
                        else LinkState.RECONNECTING
                    )
                self._generation += 1
                self._changed_at = time()
                self._condition.notify_all()
        _wire.close_socket(connection)
        if connection_to_close is not connection:
            _wire.close_socket(connection_to_close)

    def _set_state(
        self,
        state: LinkState,
        *,
        preserve_error: bool = False,
    ) -> None:
        with self._condition:
            if self._state is LinkState.CLOSED:
                return
            if self._state is state and (preserve_error or self._last_error is None):
                return
            self._state = state
            if not preserve_error:
                self._last_error = None
            self._generation += 1
            self._changed_at = time()
            self._condition.notify_all()

    def _record_connection_attempt(self) -> None:
        with self._condition:
            self._connection_attempts += 1
            self._generation += 1
            self._changed_at = time()
            self._condition.notify_all()

    def _record_sent(self, wire_size: int) -> None:
        with self._condition:
            self._frames_sent += 1
            self._bytes_sent += wire_size
            self._last_sent_at = time()
            self._generation += 1
            self._changed_at = self._last_sent_at
            self._condition.notify_all()

    def _record_received(self, wire_size: int) -> None:
        with self._condition:
            self._frames_received += 1
            self._bytes_received += wire_size
            self._last_received_at = time()
            self._generation += 1
            self._changed_at = self._last_received_at
            self._condition.notify_all()

    def _accept_remote_sequence(self, sequence: int) -> bool:
        if sequence <= 0:
            raise TransportProtocolError("application frame sequence must be positive")
        with self._condition:
            if sequence <= self._last_remote_sequence:
                return False
            self._last_remote_sequence = sequence
            return True

    def _complete_outbound(self) -> None:
        with self._condition:
            self._outbound_pending -= 1
            self._outbound_slots.release()
            self._generation += 1
            self._changed_at = time()
            self._condition.notify_all()

    def _discard_outbound(self, channel: str) -> None:
        """Invalidate queued frames on one channel without discarding other work."""
        channel = _require_text(channel, "channel")
        with self._condition:
            self._discarded_outbound_serials[channel] = self._outbound_serial
            self._generation += 1
            self._changed_at = time()
            self._condition.notify_all()

    def _is_outbound_discarded(self, item: _OutboundItem) -> bool:
        with self._condition:
            return item.serial <= self._discarded_outbound_serials.get(
                item.message.channel,
                0,
            )

    def _notify_queue_depth_changed(self) -> None:
        with self._condition:
            self._generation += 1
            self._changed_at = time()
            self._condition.notify_all()

    def _health_locked(self) -> LinkHealth:
        return LinkHealth(
            generation=self._generation,
            state=self._state,
            local_identity=self.identity,
            remote_identity=self._remote_identity,
            changed_at=self._changed_at,
            connected_at=self._connected_at,
            last_sent_at=self._last_sent_at,
            last_received_at=self._last_received_at,
            connection_attempts=self._connection_attempts,
            connections_established=self._connections_established,
            frames_sent=self._frames_sent,
            frames_received=self._frames_received,
            bytes_sent=self._bytes_sent,
            bytes_received=self._bytes_received,
            outbound_pending=self._outbound_pending,
            inbound_queued=self._inbound.qsize(),
            last_error=self._last_error,
        )

    def _drain_queues(self) -> None:
        while True:
            try:
                self._outbound.get_nowait()
            except Empty:
                break
            self._outbound.task_done()
            self._complete_outbound()
        while True:
            try:
                self._inbound.get_nowait()
            except Empty:
                break
            self._inbound.task_done()


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_nonnegative_number(value: float, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative number")


def _require_optional_timeout(value: float | None) -> None:
    if value is not None:
        _require_nonnegative_number(value, "timeout")


def _is_loopback_host(host: str) -> bool:
    return host.lower() in {"127.0.0.1", "::1", "localhost"}


def _encode_message(
    message: TransportMessage,
    *,
    sequence: int,
    max_payload_bytes: int,
) -> bytes:
    if len(message.payload) > max_payload_bytes:
        raise ValueError("payload exceeds configured max_payload_bytes")
    correlation = (
        b""
        if message.correlation_id is None
        else message.correlation_id.encode("utf-8")
    )
    return _wire.encode_frame(
        int(message.kind),
        message.channel.encode("utf-8"),
        correlation,
        message.payload,
        sequence,
    )


def _decode_identity(payload: bytes) -> NodeIdentity:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TransportProtocolError("peer identity is not valid JSON") from error
    if not isinstance(value, dict):
        raise TransportProtocolError("peer identity must be a JSON object")
    if value.get("protocol") != PROTOCOL_NAME:
        raise TransportProtocolError("peer named an incompatible protocol")
    version = value.get("version")
    if version != list(PROTOCOL_VERSION):
        raise TransportProtocolError(
            f"peer handshake version is incompatible ({version!r})"
        )
    try:
        return NodeIdentity(
            cluster_id=value["cluster_id"],
            node_id=value["node_id"],
            instance_id=value["instance_id"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise TransportIdentityError(f"peer identity is invalid: {error}") from error


def _decode_message(frame: "_wire._WireFrame") -> TransportMessage:
    try:
        kind = FrameKind(frame.kind)
    except ValueError as error:
        raise TransportProtocolError(
            f"peer sent unknown application frame kind {frame.kind}"
        ) from error
    try:
        channel = frame.channel.decode("utf-8")
        correlation_id = (
            None if not frame.correlation_id else frame.correlation_id.decode("utf-8")
        )
    except UnicodeDecodeError as error:
        raise TransportProtocolError("frame metadata is not valid UTF-8") from error
    try:
        return TransportMessage(
            kind=kind,
            channel=channel,
            correlation_id=correlation_id,
            payload=frame.payload,
            sequence=frame.sequence,
        )
    except (TypeError, ValueError) as error:
        raise TransportProtocolError(
            f"peer frame metadata is invalid: {error}"
        ) from error


@dataclass(frozen=True, slots=True)
class _OutboundItem:
    message: TransportMessage
    serial: int


class _Mode(Enum):
    LISTENER = "listener"
    CONNECTOR = "connector"


_CLOSED_SENTINEL = object()

__all__ = [
    "DEFAULT_MAX_PAYLOAD_BYTES",
    "DEFAULT_QUEUE_LIMIT",
    "FrameKind",
    "LinkHealth",
    "LinkState",
    "NodeIdentity",
    "PROTOCOL_NAME",
    "PROTOCOL_VERSION",
    "ReconnectPolicy",
    "TcpAddress",
    "TcpTransport",
    "TransportClosed",
    "TransportConfig",
    "TransportError",
    "TransportIdentityError",
    "TransportMessage",
    "TransportProtocolError",
    "TransportQueueFull",
    "TransportSecurity",
    "TransportSecurityMode",
]
