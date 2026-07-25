"""Authenticated bounded datagram transport for SWIM membership."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import math
import random
import secrets
import socket
import threading
from collections import deque
from collections.abc import Mapping, Sequence as CollectionSequence
from dataclasses import dataclass
from typing import Protocol, final

from .discovery import PeerEndpoint
from .membership import AuthenticatedPeerSession
from .transport import NodeIdentity

DEFAULT_MAX_DATAGRAM_BYTES = 1200
DEFAULT_MAX_INBOUND_DATAGRAMS = 64
DEFAULT_MAX_PEER_CREDENTIALS = 256
DEFAULT_MAX_REPLAY_MESSAGES = 4096
MIN_HMAC_KEY_BYTES = 32
_WIRE_VERSION = 2


class IdentifierSource(Protocol):
    """Produce process-unique opaque identifiers."""

    def next_id(self) -> str: ...


class RandomSource(Protocol):
    """Random selection used for probe targets and indirect helpers."""

    def sample(
        self,
        values: CollectionSequence[str],
        count: int,
    ) -> tuple[str, ...]: ...


class DatagramSocket(Protocol):
    """Minimal non-blocking datagram I/O used by the authenticated transport."""

    def send(self, payload: bytes, endpoint: PeerEndpoint) -> None: ...

    def receive(
        self,
        *,
        max_datagrams: int,
        max_bytes: int,
    ) -> tuple["ReceivedDatagram", ...]: ...

    def close(self) -> bool: ...


class SwimMessageTransport(Protocol):
    """Authenticated message boundary consumed by ``SwimMembership``."""

    def send(
        self,
        endpoint: PeerEndpoint,
        payload: bytes,
        *,
        local_incarnation: int,
    ) -> None: ...

    def receive(self) -> tuple["AuthenticatedDatagram", ...]: ...

    def close(self) -> bool: ...


@final
@dataclass(frozen=True)
class ReceivedDatagram:
    """One raw datagram and its network source."""

    payload: bytes
    source: PeerEndpoint


@final
@dataclass(frozen=True)
class AuthenticatedDatagram:
    """One payload whose sender credential has been validated."""

    session: AuthenticatedPeerSession
    payload: bytes


@final
@dataclass(frozen=True)
class HmacPeerCredentials:
    """Per-node symmetric credentials for one cluster.

    Every verifier holding a peer's key can impersonate that peer. Deployments
    requiring asymmetric node identity should provide another
    ``SwimMessageTransport`` backed by an established authenticated session.
    """

    local_identity: NodeIdentity
    advertised_endpoint: PeerEndpoint
    local_key: bytes
    peer_keys: Mapping[str, bytes]
    max_peers: int = DEFAULT_MAX_PEER_CREDENTIALS

    def __post_init__(self) -> None:
        if not isinstance(self.local_identity, NodeIdentity):
            raise ValueError("local_identity must be a NodeIdentity")
        if not isinstance(self.advertised_endpoint, PeerEndpoint):
            raise ValueError("advertised_endpoint must be a PeerEndpoint")
        _require_text(
            self.local_identity.cluster_id,
            "local cluster_id",
            max_length=128,
        )
        _require_text(
            self.local_identity.node_id,
            "local node_id",
            max_length=128,
        )
        local_key = _require_hmac_key(self.local_key, "local_key")
        max_peers = _require_positive_int(self.max_peers, "max_peers")
        if len(self.peer_keys) > max_peers:
            raise ValueError(
                f"peer_keys contains {len(self.peer_keys)} entries; "
                f"max_peers is {max_peers}"
            )
        keys: dict[str, bytes] = {}
        for node_id, key in self.peer_keys.items():
            node_id = _require_text(node_id, "peer node_id", max_length=128)
            if node_id == self.local_identity.node_id:
                raise ValueError("peer_keys must not contain the local node_id")
            keys[node_id] = _require_hmac_key(key, f"peer key for {node_id!r}")
        object.__setattr__(self, "local_key", local_key)
        object.__setattr__(self, "peer_keys", keys)
        object.__setattr__(self, "max_peers", max_peers)


@final
@dataclass(frozen=True)
class HmacTransportConfig:
    """Hard limits for authenticated datagram parsing and replay retention."""

    max_datagram_bytes: int = DEFAULT_MAX_DATAGRAM_BYTES
    max_inbound_datagrams: int = DEFAULT_MAX_INBOUND_DATAGRAMS
    max_replay_messages: int = DEFAULT_MAX_REPLAY_MESSAGES

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "max_datagram_bytes",
            _require_positive_int(self.max_datagram_bytes, "max_datagram_bytes"),
        )
        object.__setattr__(
            self,
            "max_inbound_datagrams",
            _require_positive_int(
                self.max_inbound_datagrams,
                "max_inbound_datagrams",
            ),
        )
        object.__setattr__(
            self,
            "max_replay_messages",
            _require_positive_int(
                self.max_replay_messages,
                "max_replay_messages",
            ),
        )


@final
@dataclass(frozen=True)
class HmacTransportStats:
    """Authenticated transport counters and retained replay size."""

    accepted: int
    dropped_authentication: int
    dropped_malformed: int
    dropped_oversize: int
    dropped_replay: int
    replay_messages: int


@final
class SecretsIdentifierSource:
    """Process-unique identifiers with a random boot prefix and counter."""

    def __init__(self) -> None:
        self._prefix = secrets.token_hex(16)
        self._sequence = 0

    def next_id(self) -> str:
        """Return an identifier that does not repeat in this source lifetime."""
        self._sequence += 1
        return f"{self._prefix}-{self._sequence}"


@final
class SystemRandomSource:
    """Production selection backed by the operating system RNG."""

    def __init__(self) -> None:
        self._random = random.SystemRandom()

    def sample(
        self,
        values: CollectionSequence[str],
        count: int,
    ) -> tuple[str, ...]:
        """Return at most ``count`` distinct values."""
        if count <= 0 or not values:
            return ()
        return tuple(self._random.sample(tuple(values), min(count, len(values))))


class SwimError(RuntimeError):
    """Base error for SWIM protocol and transport operations."""


@final
class SwimClosedError(SwimError):
    """Raised when a disposed SWIM owner is used."""


@final
class SwimTransportError(SwimError):
    """Raised when an authenticated datagram cannot be sent."""


@final
class UdpDatagramSocket:
    """Real non-blocking UDP I/O with explicit ownership and disposal."""

    def __init__(self, bind_endpoint: PeerEndpoint) -> None:
        family = socket.AF_INET6 if ":" in bind_endpoint.host else socket.AF_INET
        self._socket = socket.socket(family, socket.SOCK_DGRAM)
        try:
            self._socket.bind((bind_endpoint.host, bind_endpoint.port))
            self._socket.setblocking(False)
        except OSError as error:
            self._socket.close()
            raise SwimTransportError(
                f"failed to bind UDP socket to "
                f"{bind_endpoint.host}:{bind_endpoint.port}: {error}"
            ) from error
        address = self._socket.getsockname()
        self._local_endpoint = PeerEndpoint(str(address[0]), int(address[1]))
        self._lock = threading.Lock()
        self._closed = False

    @property
    def local_endpoint(self) -> PeerEndpoint:
        """Return the concrete bound endpoint, including an assigned port."""
        return self._local_endpoint

    def send(self, payload: bytes, endpoint: PeerEndpoint) -> None:
        """Send one datagram or raise a contextual transport error."""
        with self._lock:
            self._require_open()
            try:
                sent = self._socket.sendto(payload, (endpoint.host, endpoint.port))
            except OSError as error:
                raise SwimTransportError(
                    f"failed to send UDP datagram to "
                    f"{endpoint.host}:{endpoint.port}: {error}"
                ) from error
            if sent != len(payload):
                raise SwimTransportError(
                    f"sent {sent} of {len(payload)} UDP bytes to "
                    f"{endpoint.host}:{endpoint.port}"
                )

    def receive(
        self,
        *,
        max_datagrams: int,
        max_bytes: int,
    ) -> tuple[ReceivedDatagram, ...]:
        """Drain at most ``max_datagrams`` currently available datagrams."""
        with self._lock:
            self._require_open()
            received: list[ReceivedDatagram] = []
            for _ in range(max_datagrams):
                try:
                    payload, address = self._socket.recvfrom(max_bytes + 1)
                except BlockingIOError:
                    break
                except OSError as error:
                    raise SwimTransportError(
                        f"failed to receive UDP datagram: {error}"
                    ) from error
                received.append(
                    ReceivedDatagram(
                        payload=payload,
                        source=PeerEndpoint(str(address[0]), int(address[1])),
                    )
                )
            return tuple(received)

    def close(self) -> bool:
        """Close the owned socket exactly once."""
        with self._lock:
            if self._closed:
                return False
            self._closed = True
            self._socket.close()
            return True

    def _require_open(self) -> None:
        if self._closed:
            raise SwimClosedError("UDP datagram socket is closed")


@final
class HmacDatagramTransport:
    """Authenticate bounded datagrams with configured per-node HMAC keys."""

    def __init__(
        self,
        datagrams: DatagramSocket,
        credentials: HmacPeerCredentials,
        *,
        config: HmacTransportConfig | None = None,
        identifier_source: IdentifierSource | None = None,
    ) -> None:
        self._datagrams = datagrams
        self._credentials = credentials
        self._config = config or HmacTransportConfig()
        self._identifier_source = identifier_source or SecretsIdentifierSource()
        self._replay_order: deque[tuple[str, str]] = deque()
        self._replay_set: set[tuple[str, str]] = set()
        self._accepted = 0
        self._dropped_authentication = 0
        self._dropped_malformed = 0
        self._dropped_oversize = 0
        self._dropped_replay = 0
        self._closed = False

    @property
    def stats(self) -> HmacTransportStats:
        """Return counters without exposing retained replay identifiers."""
        return HmacTransportStats(
            accepted=self._accepted,
            dropped_authentication=self._dropped_authentication,
            dropped_malformed=self._dropped_malformed,
            dropped_oversize=self._dropped_oversize,
            dropped_replay=self._dropped_replay,
            replay_messages=len(self._replay_set),
        )

    def send(
        self,
        endpoint: PeerEndpoint,
        payload: bytes,
        *,
        local_incarnation: int,
    ) -> None:
        """Authenticate and send one bounded protocol payload."""
        self._require_open()
        if not isinstance(payload, bytes):
            raise ValueError("payload must be bytes")
        unsigned = {
            "cluster": self._credentials.local_identity.cluster_id,
            "endpoint": {
                "host": self._credentials.advertised_endpoint.host,
                "port": self._credentials.advertised_endpoint.port,
            },
            "incarnation": _require_nonnegative_int(
                local_incarnation,
                "local_incarnation",
            ),
            "instance": self._credentials.local_identity.instance_id,
            "message_id": _require_text(
                self._identifier_source.next_id(),
                "message_id",
                max_length=64,
            ),
            "payload": base64.b64encode(payload).decode("ascii"),
            "sender": self._credentials.local_identity.node_id,
            "version": _WIRE_VERSION,
        }
        authenticated = dict(unsigned)
        authenticated["mac"] = hmac.new(
            self._credentials.local_key,
            _canonical_json(unsigned),
            hashlib.sha256,
        ).hexdigest()
        encoded = _canonical_json(authenticated)
        if len(encoded) > self._config.max_datagram_bytes:
            raise SwimTransportError(
                f"authenticated datagram is {len(encoded)} bytes; limit is "
                f"{self._config.max_datagram_bytes}"
            )
        self._datagrams.send(encoded, endpoint)

    def receive(self) -> tuple[AuthenticatedDatagram, ...]:
        """Return only bounded, authenticated, non-replayed datagrams."""
        self._require_open()
        raw_datagrams = self._datagrams.receive(
            max_datagrams=self._config.max_inbound_datagrams,
            max_bytes=self._config.max_datagram_bytes,
        )
        authenticated: list[AuthenticatedDatagram] = []
        for datagram in raw_datagrams:
            decoded = self._authenticate(datagram)
            if decoded is not None:
                authenticated.append(decoded)
        return tuple(authenticated)

    def close(self) -> bool:
        """Dispose datagram I/O and clear replay state."""
        if self._closed:
            return False
        self._closed = True
        self._replay_order.clear()
        self._replay_set.clear()
        self._datagrams.close()
        return True

    def _authenticate(
        self,
        datagram: ReceivedDatagram,
    ) -> AuthenticatedDatagram | None:
        if len(datagram.payload) > self._config.max_datagram_bytes:
            self._dropped_oversize += 1
            return None
        try:
            envelope = json.loads(datagram.payload)
            if not isinstance(envelope, dict):
                raise ValueError("envelope must be an object")
            if set(envelope) != {
                "cluster",
                "endpoint",
                "incarnation",
                "instance",
                "mac",
                "message_id",
                "payload",
                "sender",
                "version",
            }:
                raise ValueError("envelope fields do not match the protocol")
            if envelope["version"] != _WIRE_VERSION:
                raise ValueError("unsupported wire version")
            cluster_id = _require_text(
                envelope["cluster"],
                "cluster",
                max_length=256,
            )
            endpoint = envelope["endpoint"]
            if not isinstance(endpoint, dict) or set(endpoint) != {"host", "port"}:
                raise ValueError("sender endpoint fields do not match the protocol")
            advertised_endpoint = PeerEndpoint(
                _require_text(endpoint["host"], "endpoint host", max_length=512),
                endpoint["port"],
            )
            sender = _require_text(envelope["sender"], "sender", max_length=256)
            message_id = _require_text(
                envelope["message_id"],
                "message_id",
                max_length=64,
            )
            incarnation = _require_nonnegative_int(
                envelope["incarnation"],
                "incarnation",
            )
            instance_id = _require_text(
                envelope["instance"],
                "instance",
                max_length=256,
            )
            mac = _require_hex_digest(envelope["mac"])
            payload_text = _require_text(
                envelope["payload"],
                "payload",
                max_length=self._config.max_datagram_bytes * 2,
                allow_empty=True,
            )
            payload = base64.b64decode(payload_text, validate=True)
        except (
            binascii.Error,
            json.JSONDecodeError,
            TypeError,
            UnicodeDecodeError,
            ValueError,
        ):
            self._dropped_malformed += 1
            return None

        if cluster_id != self._credentials.local_identity.cluster_id:
            self._dropped_authentication += 1
            return None
        key = self._credentials.peer_keys.get(sender)
        if key is None:
            self._dropped_authentication += 1
            return None
        unsigned = dict(envelope)
        del unsigned["mac"]
        expected = hmac.new(key, _canonical_json(unsigned), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(mac, expected):
            self._dropped_authentication += 1
            return None
        replay_key = (sender, message_id)
        if replay_key in self._replay_set:
            self._dropped_replay += 1
            return None
        self._retain_replay(replay_key)
        self._accepted += 1
        return AuthenticatedDatagram(
            session=AuthenticatedPeerSession(
                identity=NodeIdentity(cluster_id, sender, instance_id),
                endpoint=advertised_endpoint,
                incarnation=incarnation,
            ),
            payload=payload,
        )

    def _retain_replay(self, replay_key: tuple[str, str]) -> None:
        while len(self._replay_order) >= self._config.max_replay_messages:
            expired = self._replay_order.popleft()
            self._replay_set.remove(expired)
        self._replay_order.append(replay_key)
        self._replay_set.add(replay_key)

    def _require_open(self) -> None:
        if self._closed:
            raise SwimClosedError("authenticated datagram transport is closed")


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def _require_hmac_key(value: bytes, field: str) -> bytes:
    if not isinstance(value, bytes) or len(value) < MIN_HMAC_KEY_BYTES:
        raise ValueError(
            f"{field} must be bytes with at least {MIN_HMAC_KEY_BYTES} bytes"
        )
    return value


def _require_hex_digest(value: object) -> str:
    digest = _require_text(value, "mac", max_length=64)
    if len(digest) != 64:
        raise ValueError("mac must be a SHA-256 hex digest")
    try:
        bytes.fromhex(digest)
    except ValueError as error:
        raise ValueError("mac must be a SHA-256 hex digest") from error
    return digest


def _require_nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _require_nonnegative_number(value: object, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite non-negative number")
    return float(value)


def _require_positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _require_positive_number(value: object, field: str) -> float:
    result = _require_nonnegative_number(value, field)
    if result == 0:
        raise ValueError(f"{field} must be positive")
    return result


def _require_text(
    value: object,
    field: str,
    *,
    max_length: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be text")
    result = value.strip()
    if not result and not allow_empty:
        raise ValueError(f"{field} must be non-empty text")
    if len(result) > max_length:
        raise ValueError(f"{field} exceeds {max_length} characters")
    return result


__all__ = [
    "AuthenticatedDatagram",
    "DEFAULT_MAX_DATAGRAM_BYTES",
    "DEFAULT_MAX_INBOUND_DATAGRAMS",
    "DEFAULT_MAX_PEER_CREDENTIALS",
    "DEFAULT_MAX_REPLAY_MESSAGES",
    "DatagramSocket",
    "HmacDatagramTransport",
    "HmacPeerCredentials",
    "HmacTransportConfig",
    "HmacTransportStats",
    "IdentifierSource",
    "RandomSource",
    "ReceivedDatagram",
    "SecretsIdentifierSource",
    "SwimClosedError",
    "SwimError",
    "SwimMessageTransport",
    "SwimTransportError",
    "SystemRandomSource",
    "UdpDatagramSocket",
]
