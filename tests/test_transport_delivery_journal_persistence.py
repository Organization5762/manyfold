from __future__ import annotations

import socket
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryConfig,
    DeliveryError,
    DurableDelivery,
    TopicDeliveryPolicy,
)

from tests.test_support import subprocess_test_env

_JOURNAL_APPLICATION_ID = 0x4D46444C


class TransportDeliveryJournalPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._root = Path(self._temporary_directory.name)
        self._transports: list[TcpTransport] = []
        self._deliveries: list[DurableDelivery] = []

    def tearDown(self) -> None:
        for delivery in reversed(self._deliveries):
            delivery.close()
        for transport in reversed(self._transports):
            transport.close()
        self._temporary_directory.cleanup()

    def test_v1_journal_migrates_without_losing_pending_outbox(self) -> None:
        journal_path = self._root / "v1.sqlite3"
        _create_v1_journal(journal_path)
        transport = self._track_transport(_disconnected_transport())
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    journal_path,
                    max_storage_bytes=1024 * 1024,
                    max_message_bytes=4096,
                ),
            )
        )

        self.assertEqual(delivery.health().recovered_outbox, 1)
        delivery.close()
        connection = sqlite3.connect(journal_path)
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(outbox)")
        }
        row = connection.execute(
            """
            SELECT message_id, topic, semantics, source_key, payload
            FROM outbox
            """
        ).fetchone()
        schema_version = int(
            connection.execute("PRAGMA user_version").fetchone()[0]
        )
        connection.close()
        self.assertEqual(schema_version, 2)
        self.assertTrue(
            {"topic", "semantics", "source_key", "max_attempts"} <= columns
        )
        self.assertEqual(
            row,
            ("legacy-1", "legacy.command", "append", None, b"pending"),
        )

    def test_hard_exit_recovers_only_latest_value_per_source(self) -> None:
        journal_path = self._root / "latest-crash.sqlite3"
        address = _unused_address()
        script = """
import os
import sys
from pathlib import Path
from manyfold.architecture.transport import (
    FrameKind, NodeIdentity, TcpAddress, TcpTransport, TransportConfig,
    TransportMessage, TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryConfig, DurableDelivery, TopicDeliveryPolicy,
)

policy = TopicDeliveryPolicy.latest(
    "sensor.state",
    max_sources=1,
    max_bytes=4096,
    ttl_seconds=30.0,
)
transport = TcpTransport.connect(
    NodeIdentity("cluster", "sender", "crashing"),
    TcpAddress(sys.argv[1], int(sys.argv[2])),
    config=TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        max_payload_bytes=65536,
    ),
    expected_peer_node_id="receiver",
)
delivery = DurableDelivery(
    transport,
    DeliveryConfig(
        Path(sys.argv[3]),
        max_storage_bytes=1024 * 1024,
        max_message_bytes=4096,
        topic_policies=(policy,),
    ),
)
for index in range(100):
    delivery.send(
        TransportMessage(
            FrameKind.PUBSUB,
            policy.topic,
            f"value-{index}".encode(),
        ),
        source="imu-1",
    )
os._exit(0)
"""
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                address.host,
                str(address.port),
                str(journal_path),
            ],
            check=False,
            env=subprocess_test_env(),
            timeout=10.0,
        )
        self.assertEqual(completed.returncode, 0)

        transport = self._track_transport(_disconnected_transport())
        policy = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=1,
            max_bytes=4096,
            ttl_seconds=30.0,
        )
        recovered = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    journal_path,
                    max_storage_bytes=1024 * 1024,
                    max_message_bytes=4096,
                    topic_policies=(policy,),
                ),
            )
        )
        self.assertEqual(recovered.health().recovered_outbox, 1)
        recovered.close()
        connection = sqlite3.connect(journal_path)
        row = connection.execute(
            "SELECT source_key, payload, COUNT(*) FROM outbox"
        ).fetchone()
        connection.close()
        self.assertEqual(row, ("imu-1", b"value-99", 1))

    def test_structurally_truncated_or_corrupt_journal_fails_closed(self) -> None:
        transport = self._track_transport(_disconnected_transport())
        for damage in ("truncated", "corrupt"):
            with self.subTest(damage=damage):
                journal_path = self._root / f"{damage}.sqlite3"
                delivery = self._track_delivery(
                    DurableDelivery(
                        transport,
                        DeliveryConfig(
                            journal_path,
                            max_storage_bytes=1024 * 1024,
                            max_message_bytes=4096,
                        ),
                    )
                )
                delivery.send(
                    TransportMessage(
                        FrameKind.PUBSUB,
                        "recovery",
                        b"x" * 2048,
                    ),
                    message_id=f"{damage}-1",
                )
                delivery.close()
                connection = sqlite3.connect(journal_path)
                page_size = int(
                    connection.execute("PRAGMA page_size").fetchone()[0]
                )
                root_page = int(
                    connection.execute(
                        """
                        SELECT MAX(rootpage) FROM sqlite_master
                        WHERE rootpage > 0
                        """
                    ).fetchone()[0]
                )
                connection.close()
                if damage == "truncated":
                    with journal_path.open("r+b") as journal_file:
                        journal_file.truncate(
                            journal_path.stat().st_size - page_size
                        )
                else:
                    with journal_path.open("r+b") as journal_file:
                        journal_file.seek((root_page - 1) * page_size)
                        journal_file.write(b"\xff")

                with self.assertRaisesRegex(
                    DeliveryError,
                    "could not open delivery journal",
                ):
                    DurableDelivery(
                        transport,
                        DeliveryConfig(
                            journal_path,
                            max_storage_bytes=1024 * 1024,
                            max_message_bytes=4096,
                        ),
                    )

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _create_v1_journal(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE outbox (
            message_id TEXT PRIMARY KEY,
            frame_kind INTEGER NOT NULL,
            channel TEXT NOT NULL,
            correlation_id TEXT,
            payload BLOB NOT NULL,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            attempts INTEGER NOT NULL,
            next_attempt_at REAL NOT NULL,
            last_error TEXT,
            size_bytes INTEGER NOT NULL
        );
        CREATE TABLE inbox (
            message_id TEXT PRIMARY KEY,
            frame_kind INTEGER NOT NULL,
            channel TEXT NOT NULL,
            correlation_id TEXT,
            payload BLOB NOT NULL,
            delivery_attempt INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            ack_attempts INTEGER NOT NULL,
            next_ack_at REAL NOT NULL,
            ack_confirmed INTEGER NOT NULL,
            size_bytes INTEGER NOT NULL
        );
        """
    )
    connection.execute(
        """
        INSERT INTO outbox (
            message_id, frame_kind, channel, correlation_id, payload,
            created_at, expires_at, attempts, next_attempt_at, last_error,
            size_bytes
        ) VALUES ('legacy-1', 3, 'legacy.command', NULL, ?, 1, 9999999999,
                  0, 1, NULL, 200)
        """,
        (b"pending",),
    )
    connection.execute(f"PRAGMA application_id={_JOURNAL_APPLICATION_ID}")
    connection.execute("PRAGMA user_version=1")
    connection.commit()
    connection.close()


def _disconnected_transport() -> TcpTransport:
    return TcpTransport.connect(
        NodeIdentity("cluster", "sender", "sender-1"),
        _unused_address(),
        config=TransportConfig(
            security=TransportSecurity.insecure_local_development(),
            max_payload_bytes=65536,
        ),
        expected_peer_node_id="receiver",
    )


def _unused_address() -> TcpAddress:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        host, port = probe.getsockname()[:2]
        return TcpAddress(str(host), int(port))
    finally:
        probe.close()


if __name__ == "__main__":
    unittest.main()
