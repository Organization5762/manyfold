"""Bounded client-side bootstrap for one ManyFold node."""

from __future__ import annotations

import ipaddress
import threading
import time
from collections import deque
from dataclasses import dataclass, replace
from enum import Enum
from typing import final

from manyfold.architecture.discovery import (
    DiscoveryFailure,
    PeerCandidate,
    PeerEndpoint,
)
from manyfold.architecture.membership import (
    AuthenticatedPeerSession,
    MemberRecord,
    MembershipChange,
    MembershipTable,
    MemberState,
)
from manyfold.architecture.transport import (
    LinkHealth,
    LinkState,
    NodeIdentity,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportSecurityMode,
)

from ._node_config import (
    DEFAULT_DIAGNOSTIC_LIMIT,
    DEFAULT_MAX_PEERS,
    DEFAULT_MINIMUM_CREDENTIAL_LIFETIME_SECONDS,
    DEFAULT_PEER_ABSENCE_SECONDS,
    DEFAULT_RECONCILE_INTERVAL_SECONDS,
    DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
    DEFAULT_SIGNER_TIMEOUT_SECONDS,
    DEFAULT_STARTUP_PEER_TIMEOUT_SECONDS,
    NodeConfig,
)
from .dev_cluster import DevelopmentCluster
from .security import (
    CredentialExpiredError,
    ProcessTransportSecurity,
    SignerLockedError,
    SignerUnavailableError,
    _acquire_process_transport_security,
)


@final
class NodePhase(str, Enum):
    """Observable phase of the node initialization and reconciliation loop."""

    STOPPED = "stopped"
    STARTING = "starting"
    SIGNER_UNAVAILABLE = "signer_unavailable"
    SIGNER_LOCKED = "signer_locked"
    CREDENTIAL_EXPIRED = "credential_expired"
    DISCOVERING = "discovering"
    AUTHENTICATING = "authenticating"
    JOINING = "joining"
    READY = "ready"
    DEGRADED = "degraded"


@final
class DiagnosticSeverity(str, Enum):
    """Operational significance of one retained node diagnostic."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@final
@dataclass(frozen=True, slots=True)
class NodeDiagnostic:
    """One bounded, actionable node lifecycle diagnostic."""

    sequence: int
    occurred_at: float
    phase: NodePhase
    severity: DiagnosticSeverity
    code: str
    message: str
    action: str
    endpoint: PeerEndpoint | None = None


@final
@dataclass(frozen=True, slots=True)
class NodePeerSnapshot:
    """Current transport health for one discovered peer endpoint."""

    endpoint: PeerEndpoint
    source: str
    health: LinkHealth


@final
@dataclass(frozen=True, slots=True)
class NodeSnapshot:
    """Immutable bounded snapshot of one node runtime."""

    generation: int
    phase: NodePhase
    identity: NodeIdentity
    endpoint: PeerEndpoint | None
    credential_expires_at_epoch_seconds: float | None
    members: tuple[MemberRecord, ...]
    peers: tuple[NodePeerSnapshot, ...]
    diagnostics: tuple[NodeDiagnostic, ...]


@final
class NodeStartError(RuntimeError):
    """Raised after node startup fails and acquired resources are rolled back."""


@final
class NodeRuntime:
    """Idempotent node lifecycle over concrete ManyFold architecture objects."""

    def __init__(self, config: NodeConfig) -> None:
        if not isinstance(config, NodeConfig):
            raise ValueError("config must be a NodeConfig")
        self.config = config
        self._condition = threading.Condition(threading.RLock())
        self._stop = threading.Event()
        self._phase = NodePhase.STOPPED
        self._generation = 0
        self._next_diagnostic_sequence = 1
        self._diagnostics: deque[NodeDiagnostic] = deque(maxlen=config.diagnostic_limit)
        self._has_discovery_failures = False
        self._process_transport_security: ProcessTransportSecurity | None = None
        self._listener: TcpTransport | None = None
        self._membership: MembershipTable | None = None
        self._peers: dict[PeerEndpoint, _PeerLink] = {}
        self._supervisor: threading.Thread | None = None

    @property
    def phase(self) -> NodePhase:
        """Return the latest observable lifecycle phase."""
        with self._condition:
            return self._phase

    @property
    def endpoint(self) -> PeerEndpoint | None:
        """Return the bound listener endpoint while the node is running."""
        with self._condition:
            listener = self._listener
            if listener is None:
                return None
            return PeerEndpoint(listener.address.host, listener.address.port)

    @property
    def listener(self) -> TcpTransport | None:
        """Return the concrete listener owned by this runtime."""
        with self._condition:
            return self._listener

    @property
    def membership(self) -> MembershipTable | None:
        """Return the concrete membership table owned by this runtime."""
        with self._condition:
            return self._membership

    @property
    def peer_transports(self) -> tuple[TcpTransport, ...]:
        """Return the bounded concrete connector transports."""
        with self._condition:
            return tuple(
                peer.transport
                for _endpoint, peer in sorted(
                    self._peers.items(),
                    key=lambda item: (item[0].host, item[0].port),
                )
            )

    @property
    def development_cluster(self) -> DevelopmentCluster | None:
        """Return the configured concrete development control plane."""
        return self.config.development_cluster

    def start(self) -> "NodeRuntime":
        """Start once, returning this running node on duplicate calls."""
        with self._condition:
            if self._phase is not NodePhase.STOPPED:
                return self
            self._stop.clear()
            self._has_discovery_failures = False
            self._set_phase_locked(NodePhase.STARTING)
            self._record_locked(
                DiagnosticSeverity.INFO,
                "node-starting",
                f"starting node {self.config.identity.node_id!r}",
                "Wait for ready or degraded, then inspect retained diagnostics.",
            )
            try:
                process_security = _acquire_process_transport_security(
                    self.config.identity,
                    self.config.transport_security_provider,
                    timeout_seconds=self.config.signer_timeout_seconds,
                    minimum_lifetime_seconds=(
                        self.config.minimum_credential_lifetime_seconds
                    ),
                )
                self._process_transport_security = process_security
                listener = TcpTransport.listen(
                    self.config.identity,
                    self.config.listen_address,
                    config=process_security.listener_transport,
                )
                self._listener = listener
                local_endpoint = PeerEndpoint(
                    listener.address.host,
                    listener.address.port,
                )
                self._membership = MembershipTable(
                    self.config.identity,
                    local_endpoint,
                    local_incarnation=self.config.local_incarnation,
                    config=self.config.membership,
                )
                if self.config.development_cluster is not None:
                    self.config.development_cluster.start()
            except Exception as error:
                self._record_security_failure_locked(error)
                self._rollback_start_locked(error)
                raise NodeStartError(
                    f"could not start node {self.config.identity.node_id!r}: {error}"
                ) from error

        try:
            self._reconcile_once()
            self._wait_for_initial_peer()
            with self._condition:
                if self._stop.is_set() or self._phase is NodePhase.STOPPED:
                    return self
                supervisor = threading.Thread(
                    target=self._run_supervisor,
                    name=(f"manyfold-node-{self.config.identity.node_id}-supervisor"),
                    daemon=True,
                )
                self._supervisor = supervisor
                supervisor.start()
            return self
        except Exception as error:
            with self._condition:
                self._rollback_start_locked(error)
            raise NodeStartError(
                f"could not initialize node {self.config.identity.node_id!r}: {error}"
            ) from error

    def stop(self) -> bool:
        """Stop once, release owned resources, and retain bounded diagnostics."""
        with self._condition:
            if self._phase is NodePhase.STOPPED:
                return False
            self._stop.set()
            supervisor = self._supervisor
            self._supervisor = None

        if supervisor is not None and supervisor is not threading.current_thread():
            supervisor.join(timeout=self.config.shutdown_timeout_seconds)

        errors: list[str] = []
        with self._condition:
            peers = tuple(self._peers.values())
            self._peers.clear()
            membership = self._membership
            self._membership = None
            listener = self._listener
            self._listener = None
            self._process_transport_security = None

        for peer in peers:
            try:
                peer.transport.close()
            except Exception as error:
                errors.append(f"peer {peer.candidate.endpoint}: {error}")
        if membership is not None:
            try:
                if membership.is_participating:
                    membership.leave_local()
                membership.close()
            except Exception as error:
                errors.append(f"membership: {error}")
        if listener is not None:
            try:
                listener.close()
            except Exception as error:
                errors.append(f"listener: {error}")
        if self.config.development_cluster is not None:
            try:
                self.config.development_cluster.stop()
            except Exception as error:
                errors.append(f"development cluster: {error}")

        with self._condition:
            if supervisor is not None and supervisor.is_alive():
                errors.append(
                    "supervisor did not exit before the configured shutdown deadline"
                )
            severity = DiagnosticSeverity.WARNING if errors else DiagnosticSeverity.INFO
            self._set_phase_locked(NodePhase.STOPPED)
            self._record_locked(
                severity,
                "node-stopped",
                (
                    "node stopped with cleanup warnings: " + "; ".join(errors)
                    if errors
                    else "node stopped and released all owned resources"
                ),
                (
                    "Inspect the named resource before restarting."
                    if errors
                    else "Call start() to restart the same configured node."
                ),
            )
        return True

    def snapshot(self) -> NodeSnapshot:
        """Return a bounded immutable lifecycle, membership, and link snapshot."""
        with self._condition:
            membership = self._membership
            members = () if membership is None else membership.snapshot()
            listener = self._listener
            process_security = self._process_transport_security
            endpoint = (
                None
                if listener is None
                else PeerEndpoint(listener.address.host, listener.address.port)
            )
            peers = tuple(
                NodePeerSnapshot(
                    endpoint=peer.candidate.endpoint,
                    source=peer.candidate.source,
                    health=peer.transport.health(),
                )
                for _endpoint, peer in sorted(
                    self._peers.items(),
                    key=lambda item: (item[0].host, item[0].port),
                )
            )
            return NodeSnapshot(
                generation=self._generation,
                phase=self._phase,
                identity=self.config.identity,
                endpoint=endpoint,
                credential_expires_at_epoch_seconds=(
                    None
                    if process_security is None
                    else process_security.expires_at_epoch_seconds
                ),
                members=members,
                peers=peers,
                diagnostics=tuple(self._diagnostics),
            )

    def wait_for_phase(
        self,
        phase: NodePhase,
        *,
        timeout: float | None = None,
    ) -> bool:
        """Wait for one current phase, returning false when the timeout expires."""
        if not isinstance(phase, NodePhase):
            raise ValueError("phase must be a NodePhase")
        _require_optional_timeout(timeout)
        with self._condition:
            return self._condition.wait_for(
                lambda: self._phase is phase,
                timeout=timeout,
            )

    def wait_for_members(
        self,
        minimum: int,
        *,
        timeout: float | None = None,
    ) -> tuple[MemberRecord, ...]:
        """Wait for at least ``minimum`` alive members, including this node."""
        _require_positive_int(minimum, "minimum")
        _require_optional_timeout(timeout)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while True:
                membership = self._membership
                if membership is None:
                    raise RuntimeError("node is stopped")
                members = tuple(
                    member
                    for member in membership.snapshot()
                    if member.state is MemberState.ALIVE
                )
                if len(members) >= minimum:
                    return members
                remaining = (
                    None if deadline is None else max(0.0, deadline - time.monotonic())
                )
                if remaining == 0.0 or not self._condition.wait(remaining):
                    raise TimeoutError(
                        f"node did not reach {minimum} alive members before timeout; "
                        f"observed {len(members)}"
                    )

    def __enter__(self) -> "NodeRuntime":
        return self.start()

    def __exit__(self, *_error: object) -> None:
        self.stop()

    def _run_supervisor(self) -> None:
        while not self._stop.wait(self.config.reconcile_interval_seconds):
            try:
                self._reconcile_once()
            except Exception as error:
                with self._condition:
                    if self._stop.is_set():
                        return
                    self._set_phase_locked(NodePhase.DEGRADED)
                    self._record_locked(
                        DiagnosticSeverity.ERROR,
                        "reconciliation-failed",
                        f"peer reconciliation failed: {type(error).__name__}: {error}",
                        "Inspect discovery, transport, and membership configuration.",
                    )

    def _reconcile_once(self) -> None:
        with self._condition:
            if self._stop.is_set() or self._membership is None:
                return
            self._set_phase_locked(NodePhase.DISCOVERING)
            self._record_once_locked(
                "node-discovering",
                DiagnosticSeverity.INFO,
                "discovering bounded peer endpoint candidates",
                "Wait for authentication, ready, or a source failure diagnostic.",
            )
        report = self.config.discovery.discover()
        if self._stop.is_set():
            return
        now = time.monotonic()
        with self._condition:
            self._has_discovery_failures = bool(report.failures)
        self._record_discovery_failures(report.failures)
        self._reconcile_candidates(report.candidates, now=now)
        self._reconcile_links()
        membership = self._membership
        if membership is None:
            return
        changes = membership.expire()
        self._record_membership_changes(changes)
        self._settle_phase(
            has_discovery_failures=self._has_discovery_failures,
            candidate_count=len(self._peers),
        )

    def _wait_for_initial_peer(self) -> None:
        with self._condition:
            if not self._peers or self._phase is NodePhase.READY:
                return
        deadline = time.monotonic() + self.config.startup_peer_timeout_seconds
        while not self._stop.is_set() and time.monotonic() < deadline:
            self._reconcile_links()
            self._settle_phase(
                has_discovery_failures=self._has_discovery_failures,
                candidate_count=len(self._peers),
            )
            with self._condition:
                if self._phase is NodePhase.READY:
                    return
            self._stop.wait(min(0.02, max(0.0, deadline - time.monotonic())))
        self._settle_phase(
            has_discovery_failures=self._has_discovery_failures,
            candidate_count=len(self._peers),
        )

    def _reconcile_candidates(
        self,
        candidates: tuple[PeerCandidate, ...],
        *,
        now: float,
    ) -> None:
        with self._condition:
            if self._stop.is_set():
                return
            listener = self._listener
            if listener is None:
                return
            local_endpoint = PeerEndpoint(
                listener.address.host,
                listener.address.port,
            )
            seen: set[PeerEndpoint] = set()
            for candidate in candidates:
                if _is_local_candidate(candidate.endpoint, local_endpoint):
                    continue
                seen.add(candidate.endpoint)
                existing = self._peers.get(candidate.endpoint)
                if existing is not None:
                    existing.last_seen_at = now
                    continue
                if len(self._peers) >= self.config.max_peers:
                    self._record_locked(
                        DiagnosticSeverity.WARNING,
                        "peer-limit-reached",
                        (
                            f"ignored discovered endpoint {candidate.endpoint} "
                            f"because max_peers={self.config.max_peers}"
                        ),
                        "Raise max_peers and membership.max_members together if needed.",
                        endpoint=candidate.endpoint,
                    )
                    continue
                try:
                    connector_security = (
                        self._require_process_transport_security_locked()
                    )
                    transport = TcpTransport.connect(
                        self.config.identity,
                        TcpAddress(
                            candidate.endpoint.host,
                            candidate.endpoint.port,
                        ),
                        config=_connector_config(
                            connector_security.connector_transport,
                            candidate,
                        ),
                    )
                except Exception as error:
                    self._record_locked(
                        DiagnosticSeverity.ERROR,
                        "peer-connector-rejected",
                        (
                            f"could not initialize connector for "
                            f"{candidate.endpoint}: {type(error).__name__}: {error}"
                        ),
                        "Correct the endpoint or transport security configuration.",
                        endpoint=candidate.endpoint,
                    )
                    continue
                self._peers[candidate.endpoint] = _PeerLink(
                    candidate=candidate,
                    transport=transport,
                    last_seen_at=now,
                )
                self._set_phase_locked(NodePhase.AUTHENTICATING)
                self._record_locked(
                    DiagnosticSeverity.INFO,
                    "peer-authenticating",
                    f"authenticating discovered endpoint {candidate.endpoint}",
                    "Wait for peer-joined or inspect transport health on failure.",
                    endpoint=candidate.endpoint,
                )

            expired = tuple(
                endpoint
                for endpoint, peer in self._peers.items()
                if endpoint not in seen
                and now - peer.last_seen_at >= self.config.peer_absence_seconds
            )
            for endpoint in expired:
                peer = self._peers.pop(endpoint)
                self._mark_peer_unavailable_locked(peer, "discovery-expired")
                peer.transport.close()

    def _reconcile_links(self) -> None:
        with self._condition:
            membership = self._membership
            if self._stop.is_set() or membership is None:
                return
            for peer in self._peers.values():
                health = peer.transport.health()
                if (
                    health.state is LinkState.CONNECTED
                    and health.remote_identity is not None
                ):
                    self._set_phase_locked(NodePhase.JOINING)
                    session = AuthenticatedPeerSession(
                        health.remote_identity,
                        peer.candidate.endpoint,
                        0,
                    )
                    membership.heartbeat(session)
                    identity_changed = (
                        peer.remote_identity is not None
                        and peer.remote_identity != health.remote_identity
                    )
                    if not peer.is_joined:
                        self._record_locked(
                            DiagnosticSeverity.INFO,
                            "peer-joined",
                            (
                                f"authenticated and joined peer "
                                f"{health.remote_identity.node_id!r}"
                            ),
                            "No action required.",
                            endpoint=peer.candidate.endpoint,
                        )
                    elif peer.is_unavailable:
                        self._record_locked(
                            DiagnosticSeverity.INFO,
                            "peer-recovered",
                            (
                                f"peer {health.remote_identity.node_id!r} "
                                "re-authenticated and renewed membership"
                            ),
                            "No action required.",
                            endpoint=peer.candidate.endpoint,
                        )
                    elif identity_changed:
                        self._record_locked(
                            DiagnosticSeverity.INFO,
                            "peer-restarted",
                            (
                                f"peer {health.remote_identity.node_id!r} "
                                "connected with a new process identity"
                            ),
                            "Confirm the restart was expected.",
                            endpoint=peer.candidate.endpoint,
                        )
                    peer.remote_identity = health.remote_identity
                    peer.is_joined = True
                    peer.is_unavailable = False
                    peer.last_error = None
                    continue

                if peer.is_joined and not peer.is_unavailable:
                    self._mark_peer_unavailable_locked(peer, "transport-disconnected")
                if (
                    health.last_error is not None
                    and health.last_error != peer.last_error
                ):
                    peer.last_error = health.last_error
                    self._record_locked(
                        DiagnosticSeverity.WARNING,
                        "peer-authentication-unavailable",
                        (
                            f"endpoint {peer.candidate.endpoint} is not joined: "
                            f"{health.last_error}"
                        ),
                        "Check reachability, cluster identity, and transport credentials.",
                        endpoint=peer.candidate.endpoint,
                    )

    def _mark_peer_unavailable_locked(
        self,
        peer: "_PeerLink",
        reason: str,
    ) -> None:
        membership = self._membership
        remote_identity = peer.remote_identity
        if membership is not None and remote_identity is not None:
            membership.mark_suspect(remote_identity.node_id, incarnation=0)
        peer.is_unavailable = True
        self._record_locked(
            DiagnosticSeverity.WARNING,
            "peer-unavailable",
            (f"peer endpoint {peer.candidate.endpoint} became unavailable ({reason})"),
            "The connector will retry while discovery continues to return the peer.",
            endpoint=peer.candidate.endpoint,
        )

    def _record_discovery_failures(
        self,
        failures: tuple[DiscoveryFailure, ...],
    ) -> None:
        with self._condition:
            for failure in failures:
                self._record_locked(
                    DiagnosticSeverity.WARNING,
                    "discovery-source-failed",
                    f"discovery source {failure.source!r} failed: {failure.message}",
                    "Check the named resolver or remove the unavailable source.",
                )

    def _record_membership_changes(
        self,
        changes: tuple[MembershipChange, ...],
    ) -> None:
        with self._condition:
            for change in changes:
                if change.record.identity.node_id == self.config.identity.node_id:
                    continue
                self._record_locked(
                    DiagnosticSeverity.WARNING,
                    f"membership-{change.record.state.value}",
                    (
                        f"peer {change.record.identity.node_id!r} became "
                        f"{change.record.state.value}: {change.reason}"
                    ),
                    "Inspect peer transport health and network reachability.",
                    endpoint=change.record.endpoint,
                )

    def _settle_phase(
        self,
        *,
        has_discovery_failures: bool,
        candidate_count: int,
    ) -> None:
        with self._condition:
            membership = self._membership
            if membership is None or self._stop.is_set():
                return
            alive_remote_count = sum(
                member.state is MemberState.ALIVE
                and member.identity.node_id != self.config.identity.node_id
                for member in membership.snapshot()
            )
            unavailable_count = sum(
                peer.is_unavailable
                or peer.transport.health().state is not LinkState.CONNECTED
                for peer in self._peers.values()
            )
            if has_discovery_failures or unavailable_count:
                was_degraded = self._phase is NodePhase.DEGRADED
                self._set_phase_locked(NodePhase.DEGRADED)
                if not was_degraded:
                    self._record_locked(
                        DiagnosticSeverity.WARNING,
                        "node-degraded",
                        (
                            "node is running with unavailable discovery sources "
                            f"or peer links (unavailable_peers={unavailable_count})"
                        ),
                        "Inspect the preceding source and peer diagnostics; retries continue.",
                    )
                return
            if candidate_count and not alive_remote_count:
                self._set_phase_locked(NodePhase.AUTHENTICATING)
                return
            was_ready = self._phase is NodePhase.READY
            self._set_phase_locked(NodePhase.READY)
            if candidate_count == 0:
                self._record_once_locked(
                    "node-ready-local",
                    DiagnosticSeverity.INFO,
                    "no peers were discovered; node is ready in local-only mode",
                    "Add static, DNS, or mDNS discovery when peers are available.",
                )
            elif not was_ready:
                self._record_locked(
                    DiagnosticSeverity.INFO,
                    "node-ready",
                    (
                        f"node is ready with {alive_remote_count} "
                        "authenticated remote member(s)"
                    ),
                    "No action required.",
                )

    def _rollback_start_locked(self, error: Exception) -> None:
        peers = tuple(self._peers.values())
        self._peers.clear()
        membership = self._membership
        self._membership = None
        listener = self._listener
        self._listener = None
        self._process_transport_security = None
        self._stop.set()
        cleanup_errors: list[str] = []
        for peer in peers:
            try:
                peer.transport.close()
            except Exception as cleanup_error:
                cleanup_errors.append(
                    f"peer {peer.candidate.endpoint}: {cleanup_error}"
                )
        if membership is not None:
            try:
                membership.close()
            except Exception as cleanup_error:
                cleanup_errors.append(f"membership: {cleanup_error}")
        if listener is not None:
            try:
                listener.close()
            except Exception as cleanup_error:
                cleanup_errors.append(f"listener: {cleanup_error}")
        if self.config.development_cluster is not None:
            try:
                self.config.development_cluster.stop()
            except Exception as cleanup_error:
                cleanup_errors.append(f"development cluster: {cleanup_error}")
        self._set_phase_locked(NodePhase.STOPPED)
        self._record_locked(
            DiagnosticSeverity.ERROR,
            "startup-rolled-back",
            (
                f"startup failed and acquired resources were released: {error}"
                + (
                    f"; cleanup warnings: {'; '.join(cleanup_errors)}"
                    if cleanup_errors
                    else ""
                )
            ),
            (
                "Inspect cleanup warnings before retrying."
                if cleanup_errors
                else "Correct the reported failure and call start() again."
            ),
        )

    def _require_process_transport_security_locked(
        self,
    ) -> ProcessTransportSecurity:
        process_security = self._process_transport_security
        if process_security is None:
            raise RuntimeError("process transport security is not initialized")
        return process_security

    def _record_security_failure_locked(self, error: Exception) -> None:
        if isinstance(error, SignerLockedError):
            phase = NodePhase.SIGNER_LOCKED
            code = "signer-locked"
            action = "Unlock or enroll the shared machine signer, then retry."
        elif isinstance(error, CredentialExpiredError):
            phase = NodePhase.CREDENTIAL_EXPIRED
            code = "credential-expired"
            action = "Renew signer enrollment or issuance policy, then retry."
        elif isinstance(error, SignerUnavailableError):
            phase = NodePhase.SIGNER_UNAVAILABLE
            code = "signer-unavailable"
            action = "Start or repair the shared machine signer, then retry."
        else:
            return
        self._set_phase_locked(phase)
        self._record_locked(
            DiagnosticSeverity.ERROR,
            code,
            str(error),
            action,
        )

    def _set_phase_locked(self, phase: NodePhase) -> None:
        if self._phase is phase:
            return
        self._phase = phase
        self._generation += 1
        self._condition.notify_all()

    def _record_once_locked(
        self,
        code: str,
        severity: DiagnosticSeverity,
        message: str,
        action: str,
    ) -> None:
        if any(diagnostic.code == code for diagnostic in self._diagnostics):
            return
        self._record_locked(severity, code, message, action)

    def _record_locked(
        self,
        severity: DiagnosticSeverity,
        code: str,
        message: str,
        action: str,
        *,
        endpoint: PeerEndpoint | None = None,
    ) -> None:
        self._diagnostics.append(
            NodeDiagnostic(
                sequence=self._next_diagnostic_sequence,
                occurred_at=time.time(),
                phase=self._phase,
                severity=severity,
                code=code,
                message=message,
                action=action,
                endpoint=endpoint,
            )
        )
        self._next_diagnostic_sequence += 1
        self._generation += 1
        self._condition.notify_all()


def _connector_config(
    config: TransportConfig,
    candidate: PeerCandidate,
) -> TransportConfig:
    security = config.security
    if (
        security.mode is TransportSecurityMode.MUTUAL_TLS
        and candidate.server_name is not None
        and security.server_hostname != candidate.server_name
    ):
        security = replace(security, server_hostname=candidate.server_name)
        return replace(config, security=security)
    return config


def _is_local_candidate(
    candidate: PeerEndpoint,
    local: PeerEndpoint,
) -> bool:
    if candidate.port != local.port:
        return False
    if candidate.host.casefold() == local.host.casefold():
        return True
    return _is_loopback(candidate.host) and _is_loopback(local.host)


def _is_loopback(host: str) -> bool:
    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _require_nonnegative_int(value: int, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


def _require_nonnegative_number(value: float, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
        raise ValueError(f"{field} must be a non-negative number")


def _require_optional_timeout(value: float | None) -> None:
    if value is not None:
        _require_nonnegative_number(value, "timeout")


def _require_positive_int(value: int, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")


def _require_positive_number(value: float, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{field} must be a positive number")


@final
@dataclass(slots=True)
class _PeerLink:
    candidate: PeerCandidate
    transport: TcpTransport
    last_seen_at: float
    remote_identity: NodeIdentity | None = None
    is_joined: bool = False
    is_unavailable: bool = False
    last_error: str | None = None


__all__ = [
    "DEFAULT_DIAGNOSTIC_LIMIT",
    "DEFAULT_MAX_PEERS",
    "DEFAULT_MINIMUM_CREDENTIAL_LIFETIME_SECONDS",
    "DEFAULT_PEER_ABSENCE_SECONDS",
    "DEFAULT_RECONCILE_INTERVAL_SECONDS",
    "DEFAULT_SHUTDOWN_TIMEOUT_SECONDS",
    "DEFAULT_SIGNER_TIMEOUT_SECONDS",
    "DEFAULT_STARTUP_PEER_TIMEOUT_SECONDS",
    "DiagnosticSeverity",
    "NodeConfig",
    "NodeDiagnostic",
    "NodePeerSnapshot",
    "NodePhase",
    "NodeRuntime",
    "NodeSnapshot",
    "NodeStartError",
]
