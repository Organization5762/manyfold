"""Private configuration contracts for architecture transport."""

from __future__ import annotations

import ssl
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import quote

DEFAULT_QUEUE_LIMIT = 1024
DEFAULT_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024


class TransportSecurityMode(str, Enum):
    """Explicit trust mode for one transport endpoint."""

    MUTUAL_TLS = "mutual_tls"
    INSECURE_LOCAL_DEVELOPMENT = "insecure_local_development"


@dataclass(frozen=True, slots=True)
class TransportSecurity:
    """Injected mutual-TLS trust or explicit loopback-only development mode."""

    mode: TransportSecurityMode
    ssl_context: ssl.SSLContext | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    server_hostname: str | None = None

    @classmethod
    def mutual_tls(
        cls,
        ssl_context: ssl.SSLContext,
        *,
        server_hostname: str | None = None,
    ) -> "TransportSecurity":
        """Require certificate-verified TLS, including a client certificate."""
        return cls(
            TransportSecurityMode.MUTUAL_TLS,
            ssl_context=ssl_context,
            server_hostname=server_hostname,
        )

    @classmethod
    def insecure_local_development(cls) -> "TransportSecurity":
        """Allow cleartext only on a loopback address for local development."""
        return cls(TransportSecurityMode.INSECURE_LOCAL_DEVELOPMENT)

    def __post_init__(self) -> None:
        if not isinstance(self.mode, TransportSecurityMode):
            raise ValueError("security mode must be a TransportSecurityMode")
        if self.mode is TransportSecurityMode.MUTUAL_TLS:
            if not isinstance(self.ssl_context, ssl.SSLContext):
                raise ValueError("mutual TLS requires an SSLContext")
            if self.ssl_context.verify_mode != ssl.CERT_REQUIRED:
                raise ValueError(
                    "mutual TLS SSLContext verify_mode must be CERT_REQUIRED"
                )
            if self.server_hostname is not None:
                object.__setattr__(
                    self,
                    "server_hostname",
                    _require_text(self.server_hostname, "server_hostname"),
                )
            return
        if self.ssl_context is not None or self.server_hostname is not None:
            raise ValueError(
                "insecure local-development security cannot include TLS settings"
            )


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    """Capped exponential delay after connection loss or rejection."""

    initial_delay: float = 0.05
    multiplier: float = 2.0
    max_delay: float = 2.0

    def __post_init__(self) -> None:
        _require_positive_number(self.initial_delay, "initial_delay")
        _require_positive_number(self.multiplier, "multiplier")
        _require_positive_number(self.max_delay, "max_delay")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be at least initial_delay")

    def delay_for_failure(self, consecutive_failures: int) -> float:
        """Return the bounded delay after a positive failure count."""
        if (
            isinstance(consecutive_failures, bool)
            or not isinstance(consecutive_failures, int)
            or consecutive_failures < 1
        ):
            raise ValueError("consecutive_failures must be a positive integer")
        try:
            delay = self.initial_delay * (
                self.multiplier ** (consecutive_failures - 1)
            )
        except OverflowError:
            return self.max_delay
        return min(delay, self.max_delay)


@dataclass(frozen=True, slots=True)
class TransportConfig:
    """Memory, timeout, reconnect, and trust limits for one transport."""

    security: TransportSecurity
    outbound_queue_limit: int = DEFAULT_QUEUE_LIMIT
    inbound_queue_limit: int = DEFAULT_QUEUE_LIMIT
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES
    connect_timeout: float = 2.0
    handshake_timeout: float = 2.0
    heartbeat_interval: float = 1.0
    peer_timeout: float = 5.0
    reconnect: ReconnectPolicy = field(default_factory=ReconnectPolicy)

    def __post_init__(self) -> None:
        if not isinstance(self.security, TransportSecurity):
            raise ValueError("security must be a TransportSecurity")
        _require_positive_integer(self.outbound_queue_limit, "outbound_queue_limit")
        _require_positive_integer(self.inbound_queue_limit, "inbound_queue_limit")
        _require_positive_integer(self.max_payload_bytes, "max_payload_bytes")
        _require_positive_number(self.connect_timeout, "connect_timeout")
        _require_positive_number(self.handshake_timeout, "handshake_timeout")
        _require_positive_number(self.heartbeat_interval, "heartbeat_interval")
        _require_positive_number(self.peer_timeout, "peer_timeout")
        if self.peer_timeout <= self.heartbeat_interval:
            raise ValueError("peer_timeout must be greater than heartbeat_interval")
        if not isinstance(self.reconnect, ReconnectPolicy):
            raise ValueError("reconnect must be a ReconnectPolicy")


def _peer_certificate_matches_identity(
    connection: ssl.SSLSocket,
    *,
    cluster_id: str,
    node_id: str,
) -> bool:
    certificate = connection.getpeercert()
    expected_uri = (
        f"manyfold://identity/{quote(cluster_id, safe='')}/{quote(node_id, safe='')}"
    )
    return expected_uri in {
        value
        for kind, value in certificate.get("subjectAltName", ())
        if kind == "URI" and isinstance(value, str)
    }


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_positive_integer(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _require_positive_number(value: float, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{field_name} must be a positive number")
