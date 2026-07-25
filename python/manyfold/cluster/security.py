"""Signer-client boundary for short-lived node transport security."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Protocol, final, runtime_checkable

from manyfold.architecture import (
    NodeIdentity,
    TransportConfig,
    TransportSecurity,
)
from manyfold.architecture.transport import TransportSecurityMode


@runtime_checkable
class TransportSecurityProvider(Protocol):
    """Acquire process-scoped transport security from an external signer."""

    def acquire(
        self,
        identity: NodeIdentity,
        *,
        timeout_seconds: float,
        minimum_lifetime_seconds: float,
    ) -> "ProcessTransportSecurity": ...


@final
@dataclass(frozen=True, slots=True)
class ProcessTransportSecurity:
    """Short-lived listener and connector configuration for one process."""

    listener_transport: TransportConfig
    connector_transport: TransportConfig
    expires_at_epoch_seconds: float | None

    def __post_init__(self) -> None:
        if not isinstance(self.listener_transport, TransportConfig):
            raise ValueError("listener_transport must be a TransportConfig")
        if not isinstance(self.connector_transport, TransportConfig):
            raise ValueError("connector_transport must be a TransportConfig")
        if self.expires_at_epoch_seconds is None:
            return
        if (
            isinstance(self.expires_at_epoch_seconds, bool)
            or not isinstance(self.expires_at_epoch_seconds, int | float)
            or not math.isfinite(self.expires_at_epoch_seconds)
            or self.expires_at_epoch_seconds <= 0
        ):
            raise ValueError(
                "expires_at_epoch_seconds must be a finite positive number"
            )
        object.__setattr__(
            self,
            "expires_at_epoch_seconds",
            float(self.expires_at_epoch_seconds),
        )


@final
@dataclass(frozen=True, slots=True)
class LocalDevelopmentTransportSecurityProvider:
    """Create process-local cleartext config for loopback development only."""

    transport: TransportConfig = TransportConfig(
        security=TransportSecurity.insecure_local_development()
    )

    def __post_init__(self) -> None:
        if not isinstance(self.transport, TransportConfig):
            raise ValueError("transport must be a TransportConfig")
        if (
            self.transport.security.mode
            is not TransportSecurityMode.INSECURE_LOCAL_DEVELOPMENT
        ):
            raise ValueError(
                "local development provider requires insecure local-development "
                "transport security"
            )

    def acquire(
        self,
        identity: NodeIdentity,
        *,
        timeout_seconds: float,
        minimum_lifetime_seconds: float,
    ) -> ProcessTransportSecurity:
        """Return a new keyless local-development config for one process."""
        if not isinstance(identity, NodeIdentity):
            raise ValueError("identity must be a NodeIdentity")
        _require_positive_number(timeout_seconds, "timeout_seconds")
        _require_positive_number(
            minimum_lifetime_seconds,
            "minimum_lifetime_seconds",
        )
        return ProcessTransportSecurity(
            listener_transport=self.transport,
            connector_transport=self.transport,
            expires_at_epoch_seconds=None,
        )


class TransportSecurityProviderError(RuntimeError):
    """Base failure reported by a machine-local signer client."""


@final
class SignerUnavailableError(TransportSecurityProviderError):
    """Raised when the machine-local signer cannot be reached."""


@final
class SignerLockedError(TransportSecurityProviderError):
    """Raised when signer policy requires local unlock or enrollment."""


@final
class CredentialExpiredError(TransportSecurityProviderError):
    """Raised when issued credentials are expired or too close to expiry."""


def _acquire_process_transport_security(
    identity: NodeIdentity,
    provider: TransportSecurityProvider,
    *,
    timeout_seconds: float,
    minimum_lifetime_seconds: float,
) -> ProcessTransportSecurity:
    try:
        process_security = provider.acquire(
            identity,
            timeout_seconds=timeout_seconds,
            minimum_lifetime_seconds=minimum_lifetime_seconds,
        )
    except TransportSecurityProviderError:
        raise
    except Exception as error:
        raise SignerUnavailableError(
            f"signer client failed: {type(error).__name__}: {error}"
        ) from error
    if not isinstance(process_security, ProcessTransportSecurity):
        raise SignerUnavailableError(
            "signer client returned an invalid process security response"
        )
    if isinstance(provider, LocalDevelopmentTransportSecurityProvider):
        return process_security
    if (
        process_security.listener_transport.security.mode
        is not TransportSecurityMode.MUTUAL_TLS
        or process_security.connector_transport.security.mode
        is not TransportSecurityMode.MUTUAL_TLS
    ):
        raise SignerUnavailableError(
            "signer client must return mutual-TLS transport security"
        )
    expires_at = process_security.expires_at_epoch_seconds
    if expires_at is None:
        raise CredentialExpiredError(
            "signer client did not return a credential expiration"
        )
    remaining_seconds = expires_at - time.time()
    if remaining_seconds < minimum_lifetime_seconds:
        raise CredentialExpiredError(
            "signer credential lifetime is insufficient: "
            f"remaining={max(0.0, remaining_seconds):.3f}s "
            f"required={minimum_lifetime_seconds:.3f}s"
        )
    return process_security


def _require_positive_number(value: float, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{field} must be a positive number")


__all__ = [
    "CredentialExpiredError",
    "LocalDevelopmentTransportSecurityProvider",
    "ProcessTransportSecurity",
    "SignerLockedError",
    "SignerUnavailableError",
    "TransportSecurityProvider",
    "TransportSecurityProviderError",
]
