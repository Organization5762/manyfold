from __future__ import annotations

import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from manyfold.architecture import (
    FrameKind,
    LinkState,
    NodeIdentity,
    ReconnectPolicy,
    TcpAddress,
    TcpTransport,
    TransportClosed,
    TransportConfig,
    TransportMessage,
    TransportQueueFull,
    TransportSecurity,
    _transport_wire as transport_wire,
)

from tests.test_support import subprocess_test_env


class ArchitectureTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._transports: list[TcpTransport] = []

    def tearDown(self) -> None:
        for transport in reversed(self._transports):
            transport.close()

    def test_cross_process_rpc_uses_identity_handshake_and_binary_frames(self) -> None:
        server = self._track(
            TcpTransport.listen(
                NodeIdentity("test-cluster", "coordinator", "coordinator-1"),
                config=_test_config(),
                expected_peer_node_id="worker",
            )
        )
        script = """
import sys
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)

config = TransportConfig(
    security=TransportSecurity.insecure_local_development(),
    outbound_queue_limit=4,
    inbound_queue_limit=4,
    max_payload_bytes=1024,
    connect_timeout=0.2,
    handshake_timeout=0.5,
    heartbeat_interval=0.05,
    peer_timeout=0.5,
)
transport = TcpTransport.connect(
    NodeIdentity("test-cluster", "worker", "worker-1"),
    TcpAddress(sys.argv[1], int(sys.argv[2])),
    config=config,
    expected_peer_node_id="coordinator",
)
try:
    if not transport.wait_until_connected(timeout=2.0):
        raise RuntimeError("worker did not connect")
    request = transport.receive(timeout=2.0)
    transport.send(
        TransportMessage(
            FrameKind.RPC_RESPONSE,
            request.channel,
            request.payload.upper(),
            correlation_id=request.correlation_id,
        )
    )
    if not transport.flush(timeout=2.0):
        raise RuntimeError("worker response did not flush")
    print(transport.health().remote_identity.node_id)
finally:
    transport.close()
"""
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                server.address.host,
                str(server.address.port),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=subprocess_test_env(),
            text=True,
        )
        try:
            self.assertTrue(server.wait_until_connected(timeout=2.0))
            server.send(
                TransportMessage(
                    FrameKind.RPC_REQUEST,
                    "coordinator.allocate",
                    b"worker-7",
                    correlation_id="rpc-1",
                )
            )

            response = server.receive(timeout=2.0)
            stdout, stderr = process.communicate(timeout=3.0)
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=1.0)

        self.assertEqual(process.returncode, 0, stderr)
        self.assertEqual(stdout.strip(), "coordinator")
        self.assertEqual(response.kind, FrameKind.RPC_RESPONSE)
        self.assertEqual(response.channel, "coordinator.allocate")
        self.assertEqual(response.correlation_id, "rpc-1")
        self.assertEqual(response.payload, b"WORKER-7")
        self.assertGreater(response.sequence, 0)
        self.assertEqual(server.health().remote_identity.node_id, "worker")

    def test_pubsub_payload_round_trip_and_link_capabilities(self) -> None:
        server, client = self._connected_pair()
        message = TransportMessage(
            FrameKind.PUBSUB,
            "sensors.temperature",
            memoryview(b"72.4"),
        )

        client.send(message)
        received = server.receive(timeout=1.0)
        link = client.as_link("worker-to-coordinator")

        self.assertEqual(received.payload, b"72.4")
        self.assertTrue(link.capabilities.ordered)
        self.assertTrue(link.capabilities.reliable)
        self.assertFalse(link.capabilities.replayable)
        self.assertFalse(link.capabilities.authenticated)
        self.assertFalse(link.capabilities.encrypted)

    def test_mutual_tls_authenticates_and_encrypts_peer_session(self) -> None:
        if shutil.which("openssl") is None:
            self.skipTest("openssl is required to create ephemeral test certificates")
        with tempfile.TemporaryDirectory() as temporary_directory:
            server_security, client_security = _mutual_tls_security(
                Path(temporary_directory)
            )
            server = self._track(
                TcpTransport.listen(
                    NodeIdentity("cluster", "server", "server-tls"),
                    config=_test_config(security=server_security),
                    expected_peer_node_id="client",
                )
            )
            client = self._track(
                TcpTransport.connect(
                    NodeIdentity("cluster", "client", "client-tls"),
                    server.address,
                    config=_test_config(security=client_security),
                    expected_peer_node_id="server",
                )
            )

            self.assertTrue(server.wait_until_connected(timeout=3.0))
            self.assertTrue(client.wait_until_connected(timeout=3.0))
            client.send(TransportMessage(FrameKind.PUBSUB, "secure", b"payload"))

            self.assertEqual(server.receive(timeout=1.0).payload, b"payload")
            self.assertTrue(client.link_capabilities.authenticated)
            self.assertTrue(client.link_capabilities.encrypted)
            client.close()
            server.close()

            rejecting_server = self._track(
                TcpTransport.listen(
                    NodeIdentity("cluster", "server", "server-tls-2"),
                    config=_test_config(security=server_security),
                    expected_peer_node_id="impostor",
                )
            )
            impostor = self._track(
                TcpTransport.connect(
                    NodeIdentity("cluster", "impostor", "impostor-tls"),
                    rejecting_server.address,
                    config=_test_config(security=client_security),
                    expected_peer_node_id="server",
                )
            )

            self.assertTrue(
                _wait_for(
                    lambda: (
                        rejecting_server.health().last_error is not None
                        and "does not bind" in rejecting_server.health().last_error
                    ),
                    timeout=2.0,
                )
            )
            self.assertNotEqual(
                rejecting_server.health().state,
                LinkState.CONNECTED,
            )
            impostor.close()

    def test_outbound_retention_is_bounded_before_connection(self) -> None:
        transport = self._track(
            TcpTransport.listen(
                NodeIdentity("cluster", "listener", "listener-1"),
                config=TransportConfig(
                    security=TransportSecurity.insecure_local_development(),
                    outbound_queue_limit=1,
                    heartbeat_interval=0.05,
                    peer_timeout=0.5,
                ),
            )
        )
        transport.send(TransportMessage(FrameKind.PUBSUB, "events", b"one"))

        with self.assertRaisesRegex(TransportQueueFull, "backpressure"):
            transport.send(TransportMessage(FrameKind.PUBSUB, "events", b"two"))

        self.assertEqual(transport.health().outbound_pending, 1)

    def test_payload_limit_is_checked_before_retention(self) -> None:
        transport = self._track(
            TcpTransport.listen(
                NodeIdentity("cluster", "listener", "listener-1"),
                config=TransportConfig(
                    security=TransportSecurity.insecure_local_development(),
                    outbound_queue_limit=1,
                    max_payload_bytes=3,
                    heartbeat_interval=0.05,
                    peer_timeout=0.5,
                ),
            )
        )

        with self.assertRaisesRegex(ValueError, "max_payload_bytes"):
            transport.send(TransportMessage(FrameKind.PUBSUB, "events", b"four"))

        self.assertEqual(transport.health().outbound_pending, 0)

    def test_cluster_identity_mismatch_never_becomes_connected(self) -> None:
        server = self._track(
            TcpTransport.listen(
                NodeIdentity("cluster-a", "server", "server-1"),
                config=_test_config(),
                expected_peer_node_id="client",
            )
        )
        client = self._track(
            TcpTransport.connect(
                NodeIdentity("cluster-b", "client", "client-1"),
                server.address,
                config=_test_config(),
                expected_peer_node_id="server",
            )
        )

        self.assertTrue(
            _wait_for(
                lambda: (
                    server.health().last_error is not None
                    and "cluster_id" in server.health().last_error
                ),
                timeout=2.0,
            )
        )
        self.assertFalse(client.wait_until_connected(timeout=0.1))
        self.assertNotEqual(server.health().state, LinkState.CONNECTED)

    def test_incompatible_wire_version_is_rejected_before_payload_read(self) -> None:
        server = self._track(
            TcpTransport.listen(
                NodeIdentity("cluster", "server", "server-1"),
                config=_test_config(),
            )
        )
        peer = socket.create_connection(
            (server.address.host, server.address.port),
            timeout=1.0,
        )
        try:
            peer.recv(transport_wire.HEADER.size + 4096)
            peer.sendall(
                transport_wire.HEADER.pack(
                    transport_wire.MAGIC,
                    99,
                    0,
                    transport_wire.HELLO_KIND,
                    0,
                    0,
                    0,
                    0,
                    0,
                )
            )
            self.assertTrue(
                _wait_for(
                    lambda: (
                        server.health().last_error is not None
                        and "version is incompatible" in server.health().last_error
                    ),
                    timeout=1.0,
                )
            )
        finally:
            peer.close()

        self.assertEqual(server.health().state, LinkState.LISTENING)

    def test_connector_reconnects_after_listener_restarts(self) -> None:
        address = _unused_address()
        config = _test_config()
        server_identity = NodeIdentity("cluster", "server", "server-1")
        client = self._track(
            TcpTransport.connect(
                NodeIdentity("cluster", "client", "client-1"),
                address,
                config=config,
                expected_peer_node_id="server",
            )
        )
        self.assertTrue(
            _wait_for(
                lambda: client.health().connection_attempts >= 2,
                timeout=1.0,
            )
        )
        server = self._track(
            TcpTransport.listen(
                server_identity,
                address,
                config=config,
                expected_peer_node_id="client",
            )
        )
        self.assertTrue(client.wait_until_connected(timeout=2.0))
        self.assertTrue(server.wait_until_connected(timeout=2.0))

        for restart_index in range(5):
            server.close()
            self.assertTrue(
                _wait_for(
                    lambda: client.health().state is LinkState.RECONNECTING,
                    timeout=1.0,
                )
            )
            server = self._track(
                TcpTransport.listen(
                    server_identity,
                    address,
                    config=config,
                    expected_peer_node_id="client",
                )
            )
            self.assertTrue(client.wait_until_connected(timeout=2.0))
            self.assertTrue(server.wait_until_connected(timeout=2.0))
            payload = f"reconnected-{restart_index}".encode()
            client.send(TransportMessage(FrameKind.PUBSUB, "events", payload))
            self.assertTrue(client.flush(timeout=2.0))
            self.assertEqual(server.receive(timeout=2.0).payload, payload)

        self.assertGreaterEqual(client.health().connections_established, 6)

    def test_health_generation_is_waitable_and_close_releases_workers(self) -> None:
        transport = self._track(
            TcpTransport.listen(
                NodeIdentity("cluster", "server", "server-1"),
                config=_test_config(),
            )
        )
        initial = transport.health()
        transport.send(TransportMessage(FrameKind.PUBSUB, "events", b"queued"))

        changed = transport.wait_for_health_change(
            initial.generation,
            timeout=1.0,
        )
        transport.close()

        self.assertGreater(changed.generation, initial.generation)
        self.assertEqual(transport.health().state, LinkState.CLOSED)
        self.assertFalse(transport._supervisor.is_alive())
        self.assertFalse(transport._writer.is_alive())
        with self.assertRaisesRegex(TransportClosed, "closed"):
            transport.send(TransportMessage(FrameKind.PUBSUB, "events", b"late"))
        with self.assertRaisesRegex(TransportClosed, "closed"):
            transport.receive(timeout=0.1)

    def test_configuration_and_message_contracts_are_explicit(self) -> None:
        with self.assertRaisesRegex(ValueError, "peer_timeout"):
            TransportConfig(
                security=TransportSecurity.insecure_local_development(),
                heartbeat_interval=1.0,
                peer_timeout=1.0,
            )
        with self.assertRaisesRegex(ValueError, "ReconnectPolicy"):
            TransportConfig(
                security=TransportSecurity.insecure_local_development(),
                reconnect=object(),  # type: ignore[arg-type]
            )
        with self.assertRaisesRegex(ValueError, "correlation_id"):
            TransportMessage(FrameKind.RPC_REQUEST, "service.method", b"request")
        with self.assertRaisesRegex(ValueError, "kind"):
            TransportMessage("pubsub", "events", b"value")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "consecutive_failures"):
            ReconnectPolicy().delay_for_failure(0)
        unverified_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        unverified_context.check_hostname = False
        unverified_context.verify_mode = ssl.CERT_NONE
        with self.assertRaisesRegex(ValueError, "CERT_REQUIRED"):
            TransportSecurity.mutual_tls(unverified_context)
        with self.assertRaisesRegex(ValueError, "loopback"):
            TcpTransport.listen(
                NodeIdentity("cluster", "server", "server-1"),
                TcpAddress("0.0.0.0", 0),
                config=_test_config(),
            )

    def _connected_pair(self) -> tuple[TcpTransport, TcpTransport]:
        config = _test_config()
        server = self._track(
            TcpTransport.listen(
                NodeIdentity("cluster", "server", "server-1"),
                config=config,
                expected_peer_node_id="client",
            )
        )
        client = self._track(
            TcpTransport.connect(
                NodeIdentity("cluster", "client", "client-1"),
                server.address,
                config=config,
                expected_peer_node_id="server",
            )
        )
        self.assertTrue(server.wait_until_connected(timeout=2.0))
        self.assertTrue(client.wait_until_connected(timeout=2.0))
        return server, client

    def _track(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _test_config(
    *,
    security: TransportSecurity | None = None,
) -> TransportConfig:
    return TransportConfig(
        security=security or TransportSecurity.insecure_local_development(),
        outbound_queue_limit=4,
        inbound_queue_limit=4,
        max_payload_bytes=1024,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(
            initial_delay=0.02,
            multiplier=1.5,
            max_delay=0.1,
        ),
    )


def _mutual_tls_security(
    directory: Path,
) -> tuple[TransportSecurity, TransportSecurity]:
    ca_certificate = directory / "ca.pem"
    ca_key = directory / "ca.key"
    _run_openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(ca_key),
        "-out",
        str(ca_certificate),
        "-subj",
        "/CN=Manyfold Test CA",
        "-days",
        "1",
    )
    server_certificate, server_key = _signed_certificate(
        directory,
        name="server",
        common_name="localhost",
        ca_certificate=ca_certificate,
        ca_key=ca_key,
        extensions="subjectAltName=DNS:localhost,IP:127.0.0.1,"
        "URI:manyfold://identity/cluster/server\n"
        "extendedKeyUsage=serverAuth\n",
    )
    client_certificate, client_key = _signed_certificate(
        directory,
        name="client",
        common_name="manyfold-client",
        ca_certificate=ca_certificate,
        ca_key=ca_key,
        extensions="subjectAltName=URI:manyfold://identity/cluster/client\n"
        "extendedKeyUsage=clientAuth\n",
    )
    server_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    server_context.load_verify_locations(cafile=ca_certificate)
    server_context.load_cert_chain(server_certificate, server_key)
    server_context.verify_mode = ssl.CERT_REQUIRED
    client_context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH,
        cafile=ca_certificate,
    )
    client_context.load_cert_chain(client_certificate, client_key)
    return (
        TransportSecurity.mutual_tls(server_context),
        TransportSecurity.mutual_tls(
            client_context,
            server_hostname="localhost",
        ),
    )


def _signed_certificate(
    directory: Path,
    *,
    name: str,
    common_name: str,
    ca_certificate: Path,
    ca_key: Path,
    extensions: str,
) -> tuple[Path, Path]:
    certificate = directory / f"{name}.pem"
    key = directory / f"{name}.key"
    request = directory / f"{name}.csr"
    extension_file = directory / f"{name}.ext"
    extension_file.write_text(extensions, encoding="utf-8")
    _run_openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(key),
        "-out",
        str(request),
        "-subj",
        f"/CN={common_name}",
    )
    _run_openssl(
        "x509",
        "-req",
        "-in",
        str(request),
        "-CA",
        str(ca_certificate),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-out",
        str(certificate),
        "-days",
        "1",
        "-sha256",
        "-extfile",
        str(extension_file),
    )
    return certificate, key


def _run_openssl(*arguments: str) -> None:
    subprocess.run(
        ("openssl", *arguments),
        check=True,
        capture_output=True,
        text=True,
    )


def _unused_address() -> TcpAddress:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        host, port = probe.getsockname()[:2]
        return TcpAddress(str(host), int(port))
    finally:
        probe.close()


def _wait_for(predicate: object, *, timeout: float) -> bool:
    if not callable(predicate):
        raise TypeError("predicate must be callable")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


if __name__ == "__main__":
    unittest.main()
