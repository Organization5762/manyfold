from __future__ import annotations

import os
import shutil
import socket
import ssl
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from time import monotonic

from manyfold.architecture.transport import (
    FrameKind,
    LinkState,
    NodeIdentity,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)
from manyfold.architecture.transport_pki import (
    MutualTlsFiles,
    TlsMaterialError,
    TlsReloaderClosed,
    TlsSecurityReloader,
)

from tests.test_architecture_transport import (
    _run_openssl,
    _signed_certificate,
    _wait_for,
)


@unittest.skipUnless(
    shutil.which("openssl"),
    "openssl is required to create ephemeral test certificates",
)
class ArchitectureTransportPkiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._transports: list[TcpTransport] = []
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._directory = Path(self._temporary_directory.name)
        self._server_files, self._client_files = _mutual_tls_files(self._directory)

    def tearDown(self) -> None:
        for transport in reversed(self._transports):
            transport.close()
        self._temporary_directory.cleanup()

    def test_file_backed_contexts_establish_identity_bound_mutual_tls(self) -> None:
        server_reloader = TlsSecurityReloader.for_server(self._server_files)
        client_reloader = TlsSecurityReloader.for_client(
            self._client_files,
            server_hostname="localhost",
        )
        server_identity = NodeIdentity("cluster", "server", "server-files")
        server = self._track(
            TcpTransport.listen(
                server_identity,
                config=_transport_config(server_reloader.security),
                expected_peer_node_id="client",
            )
        )
        client = self._track(
            TcpTransport.connect(
                NodeIdentity("cluster", "client", "client-files"),
                server.address,
                config=_transport_config(client_reloader.security),
                expected_peer_node_id="server",
            )
        )

        self.assertTrue(server.wait_until_connected(timeout=3.0))
        self.assertTrue(client.wait_until_connected(timeout=3.0))
        client.send(TransportMessage(FrameKind.PUBSUB, "secure", b"rotatable"))

        self.assertTrue(client.flush(timeout=1.0))
        self.assertEqual(server.receive(timeout=1.0).payload, b"rotatable")
        self.assertEqual(
            client.config.security.resolve_ssl_context().minimum_version,
            ssl.TLSVersion.TLSv1_3,
        )

        _touch(self._server_files.certificate)
        _touch(self._client_files.certificate)
        self.assertTrue(server_reloader.reload_if_changed())
        self.assertTrue(client_reloader.reload_if_changed())
        address = server.address
        server.close()
        replacement = self._track(
            TcpTransport.listen(
                server_identity,
                address,
                config=_transport_config(server_reloader.security),
                expected_peer_node_id="client",
            )
        )

        self.assertTrue(client.wait_until_connected(timeout=3.0))
        self.assertTrue(replacement.wait_until_connected(timeout=3.0))
        client.send(TransportMessage(FrameKind.PUBSUB, "secure", b"rotated"))
        self.assertTrue(client.flush(timeout=1.0))
        self.assertEqual(replacement.receive(timeout=1.0).payload, b"rotated")
        self.assertGreaterEqual(client.health().connections_established, 2)

    def test_close_interrupts_pending_mutual_tls_handshake(self) -> None:
        server_reloader = TlsSecurityReloader.for_server(self._server_files)
        server = self._track(
            TcpTransport.listen(
                NodeIdentity("cluster", "server", "server-stalled-handshake"),
                config=replace(
                    _transport_config(server_reloader.security),
                    handshake_timeout=30.0,
                ),
                expected_peer_node_id="client",
            )
        )
        stalled_peer = socket.create_connection(
            (server.address.host, server.address.port),
            timeout=1.0,
        )
        try:
            self.assertTrue(
                _wait_for(
                    lambda: server.health().state is LinkState.HANDSHAKING,
                    timeout=1.0,
                )
            )

            started_at = monotonic()
            server.close()

            self.assertLess(monotonic() - started_at, 1.0)
            self.assertFalse(server._supervisor.is_alive())
            self.assertFalse(server._writer.is_alive())
        finally:
            stalled_peer.close()

    def test_reloader_replaces_changed_context_and_retains_good_on_error(
        self,
    ) -> None:
        reloader = TlsSecurityReloader.for_server(self._server_files)
        initial_security = reloader.security
        initial_context = initial_security.resolve_ssl_context()
        initial_health = reloader.health()
        certificate_stat = self._server_files.certificate.stat()
        os.utime(
            self._server_files.certificate,
            ns=(
                certificate_stat.st_atime_ns,
                certificate_stat.st_mtime_ns + 1_000_000,
            ),
        )

        self.assertTrue(reloader.reload_if_changed())
        rotated_security = reloader.security
        rotated_context = rotated_security.resolve_ssl_context()
        rotated_health = reloader.health()
        self.assertIs(rotated_security, initial_security)
        self.assertIsNot(rotated_context, initial_context)
        self.assertEqual(rotated_health.material_generation, 2)
        self.assertGreater(rotated_health.generation, initial_health.generation)

        self._server_files.certificate.write_text(
            "not a certificate",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(TlsMaterialError, "last-known-good"):
            reloader.reload_if_changed()

        self.assertIs(reloader.security.resolve_ssl_context(), rotated_context)
        self.assertIn("SSLError", reloader.health().last_error or "")
        reloader.close()
        with self.assertRaisesRegex(TlsReloaderClosed, "closed"):
            _ = reloader.security

    def test_unchanged_reloader_does_not_replace_context(self) -> None:
        reloader = TlsSecurityReloader.for_client(
            self._client_files,
            server_hostname="localhost",
        )
        security = reloader.security

        self.assertFalse(reloader.reload_if_changed())
        self.assertIs(reloader.security, security)
        self.assertEqual(reloader.health().material_generation, 1)

    @unittest.skipUnless(os.name == "posix", "POSIX permission bits are required")
    def test_private_key_rejects_group_or_other_access(self) -> None:
        self._server_files.private_key.chmod(0o644)

        with self.assertRaisesRegex(PermissionError, "group/other"):
            self._server_files.server_security()

    def test_tls_material_configuration_rejects_weak_or_invalid_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "TLSv1_2"):
            MutualTlsFiles(
                ca_certificate=self._server_files.ca_certificate,
                certificate=self._server_files.certificate,
                private_key=self._server_files.private_key,
                minimum_version=ssl.TLSVersion.TLSv1_1,
            )
        with self.assertRaisesRegex(TypeError, "pathlib.Path"):
            MutualTlsFiles(
                ca_certificate="ca.pem",  # type: ignore[arg-type]
                certificate=self._server_files.certificate,
                private_key=self._server_files.private_key,
            )
        with self.assertRaisesRegex(TypeError, "callable"):
            MutualTlsFiles(
                ca_certificate=self._server_files.ca_certificate,
                certificate=self._server_files.certificate,
                private_key=self._server_files.private_key,
                private_key_password="secret",  # type: ignore[arg-type]
            )

    def _track(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _mutual_tls_files(
    directory: Path,
) -> tuple[MutualTlsFiles, MutualTlsFiles]:
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
        name="pki-server",
        common_name="localhost",
        ca_certificate=ca_certificate,
        ca_key=ca_key,
        extensions="subjectAltName=DNS:localhost,IP:127.0.0.1,"
        "URI:manyfold://identity/cluster/server\n"
        "extendedKeyUsage=serverAuth\n",
    )
    client_certificate, client_key = _signed_certificate(
        directory,
        name="pki-client",
        common_name="manyfold-client",
        ca_certificate=ca_certificate,
        ca_key=ca_key,
        extensions="subjectAltName=URI:manyfold://identity/cluster/client\n"
        "extendedKeyUsage=clientAuth\n",
    )
    server_key.chmod(0o600)
    client_key.chmod(0o600)
    return (
        MutualTlsFiles(
            ca_certificate=ca_certificate,
            certificate=server_certificate,
            private_key=server_key,
        ),
        MutualTlsFiles(
            ca_certificate=ca_certificate,
            certificate=client_certificate,
            private_key=client_key,
        ),
    )


def _transport_config(security: TransportSecurity) -> TransportConfig:
    return TransportConfig(
        security=security,
        outbound_queue_limit=8,
        inbound_queue_limit=8,
        max_payload_bytes=4096,
        connect_timeout=0.2,
        handshake_timeout=1.0,
        heartbeat_interval=0.05,
        peer_timeout=1.0,
    )


def _touch(path: Path) -> None:
    stat_result = path.stat()
    os.utime(
        path,
        ns=(stat_result.st_atime_ns, stat_result.st_mtime_ns + 1_000_000),
    )


if __name__ == "__main__":
    unittest.main()
