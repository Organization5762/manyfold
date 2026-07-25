from __future__ import annotations

import dataclasses
import socket
import ssl
import time
import unittest

from manyfold.architecture import (
    CompositeDiscovery,
    MembershipConfig,
    NodeIdentity,
    TcpAddress,
    TransportConfig,
    TransportSecurity,
)
from manyfold.architecture.swim import SwimConfig
from manyfold.cluster import (
    CredentialExpiredError,
    LocalDevelopmentTransportSecurityProvider,
    NodeConfig,
    NodePhase,
    NodeRuntime,
    NodeStartError,
    ProcessTransportSecurity,
    SignerLockedError,
    SignerUnavailableError,
)


class ClusterSecurityTests(unittest.TestCase):
    def test_one_signer_client_issues_process_security_to_multiple_nodes(self) -> None:
        expires_at = time.time() + 300
        signer = _SignerClient(_mutual_tls_security(expires_at))
        first = NodeRuntime(_secure_node_config("node-a", signer))
        second = NodeRuntime(_secure_node_config("node-b", signer))
        try:
            first.start()
            second.start()

            self.assertEqual(first.phase, NodePhase.READY)
            self.assertEqual(second.phase, NodePhase.READY)
            self.assertEqual(signer.node_ids, ["node-a", "node-b"])
            self.assertEqual(
                signer.requests,
                [
                    ("node-a", 0.2, 30),
                    ("node-b", 0.2, 30),
                ],
            )
            self.assertEqual(
                first.snapshot().credential_expires_at_epoch_seconds,
                expires_at,
            )
            self.assertEqual(
                second.snapshot().credential_expires_at_epoch_seconds,
                expires_at,
            )
        finally:
            second.stop()
            first.stop()

        self.assertEqual(signer.node_ids, ["node-a", "node-b"])

    def test_runtime_reacquires_security_after_signer_recovery(self) -> None:
        signer = _SignerClient(SignerLockedError("machine signer is locked"))
        node = NodeRuntime(_secure_node_config("node-a", signer))

        with self.assertRaises(NodeStartError):
            node.start()

        signer.result = _mutual_tls_security(time.time() + 300)
        try:
            self.assertTrue(node.start())
            self.assertEqual(node.phase, NodePhase.READY)
            self.assertEqual(signer.node_ids, ["node-a", "node-a"])
        finally:
            node.stop()

    def test_signer_failures_have_specific_phases_and_rollback(self) -> None:
        failures = (
            (
                SignerUnavailableError("machine signer socket is unavailable"),
                NodePhase.SIGNER_UNAVAILABLE,
                "signer-unavailable",
            ),
            (
                SignerLockedError("machine signer requires local unlock"),
                NodePhase.SIGNER_LOCKED,
                "signer-locked",
            ),
        )
        for error, phase, code in failures:
            with self.subTest(code=code):
                node = NodeRuntime(_secure_node_config("node-a", _SignerClient(error)))

                with self.assertRaises(NodeStartError):
                    node.start()

                snapshot = node.snapshot()
                diagnostic = next(
                    item for item in snapshot.diagnostics if item.code == code
                )
                self.assertEqual(diagnostic.phase, phase)
                self.assertEqual(snapshot.phase, NodePhase.STOPPED)
                self.assertIsNone(node.listener)
                self.assertIsNone(node.membership)
                self.assertIn(
                    "startup-rolled-back",
                    {item.code for item in snapshot.diagnostics},
                )

    def test_expiring_credential_has_specific_phase_and_rollback(self) -> None:
        signer = _SignerClient(_mutual_tls_security(time.time() + 5))
        node = NodeRuntime(_secure_node_config("node-a", signer))

        with self.assertRaisesRegex(NodeStartError, "lifetime is insufficient"):
            node.start()

        snapshot = node.snapshot()
        diagnostic = next(
            item for item in snapshot.diagnostics if item.code == "credential-expired"
        )
        self.assertEqual(diagnostic.phase, NodePhase.CREDENTIAL_EXPIRED)
        self.assertEqual(snapshot.phase, NodePhase.STOPPED)
        self.assertIsNone(snapshot.credential_expires_at_epoch_seconds)
        self.assertIsNone(node.listener)
        self.assertIsNone(node.membership)

    def test_node_config_has_no_direct_transport_config_aliases(self) -> None:
        field_names = {field.name for field in dataclasses.fields(NodeConfig)}

        self.assertNotIn("listener_transport", field_names)
        self.assertNotIn("connector_transport", field_names)
        self.assertIn("transport_security_provider", field_names)

    def test_swim_configuration_requires_restartable_transport_factory(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "swim and swim_transport_factory must be configured together",
        ):
            NodeConfig(
                identity=NodeIdentity("secure-cluster", "node-a"),
                listen_address=TcpAddress("127.0.0.1", _reserve_port()),
                discovery=CompositeDiscovery(()),
                transport_security_provider=_SignerClient(
                    _mutual_tls_security(time.time() + 300)
                ),
                membership=MembershipConfig(max_members=5),
                swim=SwimConfig(),
                max_peers=4,
            )

    def test_local_provider_preserves_bounded_transport_lifecycle_policy(
        self,
    ) -> None:
        transport = TransportConfig(
            security=TransportSecurity.insecure_local_development(),
            outbound_queue_limit=8,
            inbound_queue_limit=8,
            connect_timeout=0.1,
            handshake_timeout=0.2,
            heartbeat_interval=0.05,
            peer_timeout=0.3,
        )
        provider = LocalDevelopmentTransportSecurityProvider(transport)

        process_security = provider.acquire(
            NodeIdentity("development", "node-a"),
            timeout_seconds=0.2,
            minimum_lifetime_seconds=30,
        )

        self.assertIs(process_security.listener_transport, transport)
        self.assertIs(process_security.connector_transport, transport)

    def test_provider_errors_remain_public_for_signer_client_integrations(
        self,
    ) -> None:
        self.assertTrue(issubclass(SignerUnavailableError, RuntimeError))
        self.assertTrue(issubclass(SignerLockedError, RuntimeError))
        self.assertTrue(issubclass(CredentialExpiredError, RuntimeError))


def _secure_node_config(
    node_id: str,
    signer: "_SignerClient",
) -> NodeConfig:
    return NodeConfig(
        identity=NodeIdentity("secure-cluster", node_id),
        listen_address=TcpAddress("127.0.0.1", _reserve_port()),
        discovery=CompositeDiscovery(()),
        transport_security_provider=signer,
        membership=MembershipConfig(max_members=5),
        max_peers=4,
        startup_peer_timeout_seconds=0,
        reconcile_interval_seconds=0.05,
        signer_timeout_seconds=0.2,
        minimum_credential_lifetime_seconds=30,
    )


def _mutual_tls_security(expires_at: float) -> ProcessTransportSecurity:
    listener_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    listener_context.verify_mode = ssl.CERT_REQUIRED
    connector_context = ssl.create_default_context()
    return ProcessTransportSecurity(
        listener_transport=TransportConfig(
            security=TransportSecurity.mutual_tls(listener_context),
        ),
        connector_transport=TransportConfig(
            security=TransportSecurity.mutual_tls(
                connector_context,
                server_hostname="localhost",
            ),
        ),
        expires_at_epoch_seconds=expires_at,
    )


def _reserve_port() -> int:
    reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])
    finally:
        reservation.close()


class _SignerClient:
    def __init__(
        self,
        result: ProcessTransportSecurity | Exception,
    ) -> None:
        self.result = result
        self.node_ids: list[str] = []
        self.requests: list[tuple[str, float, float]] = []

    def acquire(
        self,
        identity: NodeIdentity,
        *,
        timeout_seconds: float,
        minimum_lifetime_seconds: float,
    ) -> ProcessTransportSecurity:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if minimum_lifetime_seconds <= 0:
            raise ValueError("minimum_lifetime_seconds must be positive")
        self.node_ids.append(identity.node_id)
        self.requests.append(
            (identity.node_id, timeout_seconds, minimum_lifetime_seconds)
        )
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


if __name__ == "__main__":
    unittest.main()
