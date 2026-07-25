"""Private typed configuration for the node bootstrap runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import final

from manyfold.architecture.discovery import CompositeDiscovery
from manyfold.architecture.membership import MembershipConfig
from manyfold.architecture.transport import NodeIdentity, TcpAddress

from .dev_cluster import DevelopmentCluster
from .security import TransportSecurityProvider

DEFAULT_DIAGNOSTIC_LIMIT = 128
DEFAULT_MAX_PEERS = 32
DEFAULT_MINIMUM_CREDENTIAL_LIFETIME_SECONDS = 30.0
DEFAULT_PEER_ABSENCE_SECONDS = 15.0
DEFAULT_RECONCILE_INTERVAL_SECONDS = 1.0
DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 5.0
DEFAULT_SIGNER_TIMEOUT_SECONDS = 2.0
DEFAULT_STARTUP_PEER_TIMEOUT_SECONDS = 2.0


@final
@dataclass(frozen=True, slots=True)
class NodeConfig:
    """Typed production objects and hard bounds required to start one node."""

    identity: NodeIdentity
    listen_address: TcpAddress
    discovery: CompositeDiscovery
    transport_security_provider: TransportSecurityProvider
    membership: MembershipConfig = MembershipConfig()
    development_cluster: DevelopmentCluster | None = None
    local_incarnation: int = 0
    max_peers: int = DEFAULT_MAX_PEERS
    diagnostic_limit: int = DEFAULT_DIAGNOSTIC_LIMIT
    reconcile_interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    startup_peer_timeout_seconds: float = DEFAULT_STARTUP_PEER_TIMEOUT_SECONDS
    peer_absence_seconds: float = DEFAULT_PEER_ABSENCE_SECONDS
    signer_timeout_seconds: float = DEFAULT_SIGNER_TIMEOUT_SECONDS
    minimum_credential_lifetime_seconds: float = (
        DEFAULT_MINIMUM_CREDENTIAL_LIFETIME_SECONDS
    )
    shutdown_timeout_seconds: float = DEFAULT_SHUTDOWN_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if not isinstance(self.identity, NodeIdentity):
            raise ValueError("identity must be a NodeIdentity")
        if not isinstance(self.listen_address, TcpAddress):
            raise ValueError("listen_address must be a TcpAddress")
        if not isinstance(self.discovery, CompositeDiscovery):
            raise ValueError("discovery must be a CompositeDiscovery")
        if not isinstance(
            self.transport_security_provider,
            TransportSecurityProvider,
        ):
            raise ValueError(
                "transport_security_provider must implement TransportSecurityProvider"
            )
        if not isinstance(self.membership, MembershipConfig):
            raise ValueError("membership must be a MembershipConfig")
        if self.development_cluster is not None and not isinstance(
            self.development_cluster,
            DevelopmentCluster,
        ):
            raise ValueError("development_cluster must be a DevelopmentCluster")
        _require_nonnegative_int(self.local_incarnation, "local_incarnation")
        _require_positive_int(self.max_peers, "max_peers")
        _require_positive_int(self.diagnostic_limit, "diagnostic_limit")
        _require_positive_number(
            self.reconcile_interval_seconds,
            "reconcile_interval_seconds",
        )
        _require_nonnegative_number(
            self.startup_peer_timeout_seconds,
            "startup_peer_timeout_seconds",
        )
        _require_positive_number(
            self.peer_absence_seconds,
            "peer_absence_seconds",
        )
        _require_positive_number(
            self.signer_timeout_seconds,
            "signer_timeout_seconds",
        )
        _require_positive_number(
            self.minimum_credential_lifetime_seconds,
            "minimum_credential_lifetime_seconds",
        )
        _require_positive_number(
            self.shutdown_timeout_seconds,
            "shutdown_timeout_seconds",
        )
        if self.max_peers >= self.membership.max_members:
            raise ValueError(
                "max_peers must be smaller than membership.max_members because "
                "membership also retains the local node"
            )


def _require_nonnegative_int(value: int, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


def _require_nonnegative_number(value: float, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ValueError(f"{field} must be a non-negative number")


def _require_positive_int(value: int, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")


def _require_positive_number(value: float, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{field} must be a positive number")
