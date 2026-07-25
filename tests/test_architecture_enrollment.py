from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from collections.abc import Iterator
from pathlib import Path

from cryptography import x509
from manyfold.architecture import (
    EnrollmentBundle,
    EnrollmentPolicy,
    EnrollmentRequest,
    FrameKind,
    LinkState,
    MachineSignerClient,
    MachineSignerService,
    NodeIdentity,
    NodeIdentityStore,
    ProcessCredentialState,
    ReconnectPolicy,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)

_UTC = dt.timezone.utc


class ArchitectureEnrollmentTests(unittest.TestCase):
    def test_machine_signer_issues_real_short_lived_tcp_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            authority, client_store = _enrolled_pair(Path(temporary))
            authority_service = MachineSignerService(
                authority,
                authority.root / "signer.sock",
            )
            client_service = MachineSignerService(
                client_store,
                client_store.root / "signer.sock",
            )
            authority_service.start()
            client_service.start()
            authority_client = MachineSignerClient(
                authority.root / "signer.sock",
                authority.identity,
            )
            client = MachineSignerClient(
                client_store.root / "signer.sock",
                client_store.identity,
            )
            listener = TcpTransport.listen(
                authority_client.identity,
                config=_config(authority_client.transport_security(server_side=True)),
                expected_peer_node_id="client",
            )
            connector = TcpTransport.connect(
                client.identity,
                listener.address,
                config=_config(
                    client.transport_security(
                        server_side=False,
                        server_hostname="localhost",
                    )
                ),
                expected_peer_node_id="authority",
            )
            try:
                self.assertTrue(connector.wait_until_connected(timeout=2))
                connector.send(TransportMessage(FrameKind.PUBSUB, "signer", b"short"))
                self.assertEqual(listener.receive(timeout=2).payload, b"short")
                machine_key = (
                    client_store.root
                    / "generations"
                    / json.loads((client_store.root / "active.json").read_text())[
                        "generation"
                    ]
                    / "node.key"
                ).read_bytes()
                process_files = tuple(Path(client._temporary.name).glob("*"))
                self.assertNotIn(
                    machine_key, (path.read_bytes() for path in process_files)
                )
                self.assertFalse(any(path.name == "ca.key" for path in process_files))
                process_certificate = x509.load_pem_x509_certificate(
                    (Path(client._temporary.name) / "process.pem").read_bytes()
                )
                lifetime = (
                    process_certificate.not_valid_after_utc
                    - process_certificate.not_valid_before_utc
                )
                self.assertLessEqual(lifetime, dt.timedelta(minutes=5, seconds=30))
                old_serial = process_certificate.serial_number
                before = client.credential_status()
                self.assertEqual(before.state, ProcessCredentialState.READY)
                renewed_status = client.renew_process_credentials(max_attempts=1)
                self.assertEqual(renewed_status.generation, before.generation + 1)
                client.ssl_context(server_side=True)
                renewed = x509.load_pem_x509_certificate(
                    (Path(client._temporary.name) / "process.pem").read_bytes()
                )
                self.assertNotEqual(renewed.serial_number, old_serial)
                self.assertGreater(
                    len(client.transport_identity_challenge(os.urandom(64))),
                    32,
                )
                self.assertGreater(
                    len(client.enrollment_proof(os.urandom(32))),
                    32,
                )
                token = authority_client.issue_token()
                pending, request = NodeIdentityStore.prepare(
                    Path(temporary) / "issued-through-service",
                    node_id="service-issued",
                    token=token,
                )
                pending.import_enrollment(
                    token,
                    authority_client.issue_certificate(token, request),
                )
                self.assertTrue(pending.status().is_enrolled)
            finally:
                connector.close()
                listener.close()
                client.close()
                authority_client.close()
                client_service.stop()
                authority_service.stop()

    def test_machine_signer_multiprocess_identity_restart_rotation_and_limits(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store, _ = NodeIdentityStore.initialize(root / "machine", node_id="machine")
            socket_path = store.root / "signer.sock"
            service = MachineSignerService(
                store,
                socket_path,
                max_audit_entries=3,
            )
            service.start()
            duplicate = MachineSignerService(store, socket_path)
            with self.assertRaisesRegex(RuntimeError, "another machine signer"):
                duplicate.start()
            code = (
                "import json,sys;"
                "from manyfold.architecture import MachineSignerClient,NodeIdentity;"
                "c=MachineSignerClient(sys.argv[1],NodeIdentity('x','x'));"
                "print(json.dumps(c.status(),sort_keys=True));c.close()"
            )
            processes = [
                subprocess.Popen(
                    [sys.executable, "-c", code, str(socket_path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                for _ in range(2)
            ]
            results = [
                json.loads(process.communicate(timeout=5)[0]) for process in processes
            ]
            self.assertEqual(results[0]["node_id"], results[1]["node_id"])
            self.assertEqual(
                results[0]["public_key_sha256"],
                results[1]["public_key_sha256"],
            )
            old_key = results[0]["public_key_sha256"]
            client = MachineSignerClient(socket_path, store.identity)
            client.rotate()
            rotated_key = client.status()["public_key_sha256"]
            self.assertNotEqual(rotated_key, old_key)
            for _ in range(8):
                client.status()
            self.assertEqual(service.health().audit_entries, 3)
            service.stop()
            unavailable = MachineSignerClient(socket_path, store.identity)
            with self.assertRaisesRegex(RuntimeError, "unexpired"):
                unavailable.ensure_process_credentials(max_attempts=1)
            self.assertEqual(
                unavailable.credential_status().state,
                ProcessCredentialState.UNAVAILABLE,
            )
            unavailable.close()
            service.start()
            restarted = MachineSignerClient(socket_path, store.identity)
            self.assertEqual(restarted.status()["public_key_sha256"], rotated_key)
            restarted.close()
            self.assertGreater(client.revoke("machine"), 0)
            with self.assertRaisesRegex(RuntimeError, "revoked"):
                client.ssl_context(server_side=True)
            service.stop()
            client.close()

    def test_machine_signer_cli_multiprocess_renewal_expiry_and_uid_policy(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store, enrollment_token = NodeIdentityStore.initialize(
                Path(temporary) / "machine",
                node_id="machine",
            )
            socket_path = store.root / "signer.sock"
            service = _start_signer_cli(
                store,
                socket_path,
                allowed_uid=os.getuid(),
                credential_ttl_seconds=2,
            )
            client = MachineSignerClient(socket_path, store.identity)
            try:
                token_file = Path(temporary) / "enrollment.token"
                token_file.write_text(enrollment_token.encode())
                token_file.chmod(0o600)
                enrolled = subprocess.run(
                    [
                        str(Path(sys.executable).with_name("manyfold-enrollment")),
                        "enroll",
                        "--authority-socket",
                        str(socket_path),
                        "--state-dir",
                        str(Path(temporary) / "worker"),
                        "--node-id",
                        "worker",
                        "--token-file",
                        str(token_file),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                self.assertTrue(json.loads(enrolled.stdout)["is_enrolled"])
                first = client.ensure_process_credentials(max_attempts=1)
                self.assertEqual(first.state, ProcessCredentialState.READY)
                code = (
                    "import json,sys;"
                    "from manyfold.architecture import MachineSignerClient,NodeIdentity;"
                    "c=MachineSignerClient(sys.argv[1],NodeIdentity('x','x'));"
                    "m=c.status();p=c.ensure_process_credentials(max_attempts=1);"
                    "print(json.dumps({'node_id':m['node_id'],"
                    "'public_key_sha256':m['public_key_sha256'],"
                    "'serial_number':p.serial_number},sort_keys=True));c.close()"
                )
                processes = [
                    subprocess.Popen(
                        [sys.executable, "-c", code, str(socket_path)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    for _ in range(2)
                ]
                statuses = [
                    json.loads(process.communicate(timeout=5)[0])
                    for process in processes
                ]
                self.assertEqual(statuses[0]["node_id"], statuses[1]["node_id"])
                self.assertNotEqual(
                    statuses[0]["serial_number"],
                    statuses[1]["serial_number"],
                )
                time.sleep(1.1)
                renewed = client.ensure_process_credentials(max_attempts=1)
                self.assertGreater(renewed.generation, first.generation)
                _stop_process(service)
                deadline = time.monotonic() + 3
                while (
                    client.credential_status().state
                    is not ProcessCredentialState.EXPIRED
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.05)
                with self.assertRaisesRegex(RuntimeError, "unexpired"):
                    client.ensure_process_credentials(
                        force_renewal=True,
                        max_attempts=1,
                    )
                self.assertEqual(
                    client.credential_status().state,
                    ProcessCredentialState.EXPIRED,
                )
            finally:
                client.close()
                _stop_process(service)

            unauthorized = _start_signer_cli(
                store,
                socket_path,
                allowed_uid=os.getuid() + 1,
                credential_ttl_seconds=2,
            )
            rejected = MachineSignerClient(socket_path, store.identity)
            try:
                with self.assertRaisesRegex(PermissionError, "not authorized"):
                    rejected.status()
            finally:
                rejected.close()
                _stop_process(unauthorized)

    def test_enrollment_drives_real_tcp_transport_mutual_tls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            authority, client = _enrolled_pair(Path(temporary))
            listener, connector = _transport_pair(authority, client)
            try:
                self.assertTrue(connector.wait_until_connected(timeout=2))
                connector.send(
                    TransportMessage(FrameKind.PUBSUB, "secure", b"enrolled")
                )
                self.assertEqual(listener.receive(timeout=2).payload, b"enrolled")
                self.assertEqual(listener.health().remote_identity.node_id, "client")
            finally:
                connector.close()
                listener.close()

    def test_certificate_cannot_claim_another_node_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            authority, client = _enrolled_pair(Path(temporary))
            listener = TcpTransport.listen(
                authority.identity,
                config=_config(_store_security(authority, server_side=True)),
            )
            connector = TcpTransport.connect(
                NodeIdentity(authority.identity.cluster_id, "impostor"),
                listener.address,
                config=_config(_store_security(client, server_side=False)),
                expected_peer_node_id="authority",
            )
            try:
                connector.wait_until_connected(timeout=0.7)
                health = connector.health()
                while health.state is LinkState.CONNECTED:
                    health = connector.wait_for_health_change(
                        health.generation,
                        timeout=1,
                    )
                self.assertIsNot(health.state, LinkState.CONNECTED)
                self.assertIn("certificate", listener.health().last_error.lower())
            finally:
                connector.close()
                listener.close()

    def test_replayed_and_expired_tokens_are_rejected_with_bounded_skew(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            now = dt.datetime(2026, 7, 25, tzinfo=_UTC)
            authority, token = NodeIdentityStore.initialize(
                root / "authority",
                node_id="authority",
                now=now,
            )
            first, request = NodeIdentityStore.prepare(
                root / "first",
                node_id="first",
                token=token,
                now=now,
            )
            first.import_enrollment(
                token,
                authority.issue(token, request, now=now),
                now=now,
            )
            _, replay = NodeIdentityStore.prepare(
                root / "replay",
                node_id="replay",
                token=token,
                now=now,
            )
            with self.assertRaisesRegex(PermissionError, "already consumed"):
                authority.issue(token, replay, now=now)

            expired = authority.issue_token(now=now)
            after_skew = expired.expires_at + dt.timedelta(minutes=2, seconds=1)
            with self.assertRaisesRegex(PermissionError, "expired"):
                NodeIdentityStore.prepare(
                    root / "expired",
                    node_id="expired",
                    token=expired,
                    now=after_skew,
                )

    def test_request_and_bundle_round_trip_without_private_material(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            authority, token = NodeIdentityStore.initialize(
                root / "authority",
                node_id="authority",
            )
            _, request = NodeIdentityStore.prepare(
                root / "client",
                node_id="client",
                token=token,
            )
            request_document = request.to_json()
            self.assertNotIn(token.secret, request_document)
            restored_request = EnrollmentRequest.from_json(request_document)
            bundle = authority.issue(token, restored_request)
            restored_bundle = EnrollmentBundle.from_json(bundle.to_json())
            self.assertEqual(
                restored_bundle.identity.node_id,
                restored_request.identity.node_id,
            )

    def test_wrong_cluster_request_and_tls_peer_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first, token = NodeIdentityStore.initialize(
                root / "first",
                node_id="first",
            )
            second, _ = NodeIdentityStore.initialize(
                root / "second",
                node_id="second",
            )
            _, request = NodeIdentityStore.prepare(
                root / "candidate",
                node_id="candidate",
                token=token,
            )
            with self.assertRaisesRegex(PermissionError, "another cluster"):
                second.issue(token, request)

            listener = _listener(first)
            connector = TcpTransport.connect(
                second.identity,
                listener.address,
                config=_config(_store_security(second, server_side=False)),
            )
            try:
                self.assertFalse(connector.wait_until_connected(timeout=0.7))
            finally:
                connector.close()
                listener.close()

    def test_rotation_keeps_live_connection_and_reloads_for_reconnect(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            authority, client = _enrolled_pair(Path(temporary))
            old_serial = client.status().certificate_serial
            listener, connector = _transport_pair(authority, client)
            try:
                self.assertTrue(connector.wait_until_connected(timeout=2))
                with _signer_pair(authority, client) as (
                    authority_signer,
                    client_signer,
                ):
                    client_signer.rotate(authority_signer)
                self.assertNotEqual(client.status().certificate_serial, old_serial)
                connector.send(
                    TransportMessage(FrameKind.PUBSUB, "rotation", b"still-live")
                )
                self.assertEqual(listener.receive(timeout=2).payload, b"still-live")
                connector.close()
                connector = _connector(listener, client)
                self.assertTrue(connector.wait_until_connected(timeout=2))
            finally:
                connector.close()
                listener.close()

    def test_revoked_peer_is_rejected_by_real_tls_handshake(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            authority, client = _enrolled_pair(Path(temporary))
            old_crl = (authority.root / "crl.pem").read_text()
            self.assertGreater(authority.revoke(client.identity.node_id), 0)
            client.import_crl((authority.root / "crl.pem").read_text())
            with self.assertRaisesRegex(PermissionError, "rollback"):
                client.import_crl(old_crl)
            listener, connector = _transport_pair(authority, client)
            try:
                self.assertFalse(connector.wait_until_connected(timeout=0.8))
                self.assertIn("certificate", listener.health().last_error.lower())
            finally:
                connector.close()
                listener.close()

    def test_atomic_pointer_ignores_partial_writes_and_permissions_are_strict(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "authority"
            store, _ = NodeIdentityStore.initialize(root, node_id="authority")
            serial = store.status().certificate_serial
            (root / ".active.json.tmp-crash").write_text("{", encoding="utf-8")
            partial = root / "generations" / ".pending-crash"
            partial.mkdir(mode=0o700)
            (partial / "node.key").write_text("partial", encoding="utf-8")

            reopened = NodeIdentityStore.open(root)

            self.assertEqual(reopened.status().certificate_serial, serial)
            self.assertEqual(os.stat(root).st_mode & 0o777, 0o700)
            managed_names = {
                ".identity.lock",
                "active.json",
                "authority.json",
                "ca.key",
                "ca.pem",
                "crl.pem",
                "identity.json",
                "node.key",
                "node.pem",
                "trust.pem",
            }
            for path in root.rglob("*"):
                is_partial = any(part.startswith(".pending-") for part in path.parts)
                if path.is_file() and path.name in managed_names and not is_partial:
                    self.assertEqual(os.stat(path).st_mode & 0o077, 0)

    def test_retained_tokens_certificates_and_generations_stay_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            policy = EnrollmentPolicy(
                max_tokens=3,
                max_active_certificates=2,
            )
            authority, token = NodeIdentityStore.initialize(
                root / "authority",
                node_id="authority",
                policy=policy,
            )
            client = authority.enroll(
                root / "client",
                node_id="client",
                token=token,
            )
            authority.issue_token()
            authority.issue_token()
            with self.assertRaisesRegex(RuntimeError, "token"):
                authority.issue_token()
            with _signer_pair(authority, client) as (
                authority_signer,
                client_signer,
            ):
                for _ in range(7):
                    client_signer.rotate(authority_signer)

            self.assertEqual(client.retained_state_counts()["generations"], 2)
            authority_state = json.loads(
                (authority.root / "authority.json").read_text(encoding="utf-8")
            )
            self.assertLessEqual(
                len(authority_state["nodes"]["client"]["certificates"]),
                2,
            )
            self.assertLessEqual(
                authority.retained_state_counts()["revocations"],
                policy.max_revocations,
            )


def _enrolled_pair(root: Path) -> tuple[NodeIdentityStore, NodeIdentityStore]:
    authority, token = NodeIdentityStore.initialize(
        root / "authority",
        cluster_id="test-cluster",
        node_id="authority",
    )
    return authority, authority.enroll(
        root / "client",
        node_id="client",
        token=token,
    )


@contextlib.contextmanager
def _signer_pair(
    authority: NodeIdentityStore,
    member: NodeIdentityStore,
) -> Iterator[tuple[MachineSignerClient, MachineSignerClient]]:
    authority_socket = authority.root / "rotation-signer.sock"
    member_socket = member.root / "rotation-signer.sock"
    authority_service = MachineSignerService(authority, authority_socket)
    member_service = MachineSignerService(member, member_socket)
    authority_service.start()
    member_service.start()
    authority_client = MachineSignerClient(authority_socket, authority.identity)
    member_client = MachineSignerClient(member_socket, member.identity)
    try:
        yield authority_client, member_client
    finally:
        member_client.close()
        authority_client.close()
        member_service.stop()
        authority_service.stop()


def _start_signer_cli(
    store: NodeIdentityStore,
    socket_path: Path,
    *,
    allowed_uid: int,
    credential_ttl_seconds: int,
) -> subprocess.Popen[str]:
    executable = Path(sys.executable).with_name("manyfold-machine-signer")
    process = subprocess.Popen(
        [
            str(executable),
            "start",
            "--state-dir",
            str(store.root),
            "--socket",
            str(socket_path),
            "--allowed-uid",
            str(allowed_uid),
            "--max-clients",
            "4",
            "--max-audit-entries",
            "8",
            "--credential-ttl-seconds",
            str(credential_ttl_seconds),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 5
    while not socket_path.exists() and process.poll() is None:
        if time.monotonic() >= deadline:
            _stop_process(process)
            raise TimeoutError("machine signer CLI did not create its socket")
        time.sleep(0.02)
    if process.poll() is not None:
        _, error = process.communicate()
        raise RuntimeError(f"machine signer CLI exited early: {error}")
    return process


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
    process.communicate(timeout=5)


def _transport_pair(
    authority: NodeIdentityStore,
    client: NodeIdentityStore,
) -> tuple[TcpTransport, TcpTransport]:
    listener = _listener(authority)
    return listener, _connector(listener, client)


def _listener(authority: NodeIdentityStore) -> TcpTransport:
    return TcpTransport.listen(
        authority.identity,
        config=_config(_store_security(authority, server_side=True)),
        expected_peer_node_id="client",
    )


def _connector(
    listener: TcpTransport,
    client: NodeIdentityStore,
) -> TcpTransport:
    return TcpTransport.connect(
        client.identity,
        listener.address,
        config=_config(_store_security(client, server_side=False)),
        expected_peer_node_id="authority",
    )


def _config(security: TransportSecurity) -> TransportConfig:
    return TransportConfig(
        security=security,
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


def _store_security(
    store: NodeIdentityStore,
    *,
    server_side: bool,
) -> TransportSecurity:
    return TransportSecurity.mutual_tls(
        store._ssl_context(server_side=server_side),
        server_hostname=None if server_side else "localhost",
    )
