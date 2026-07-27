from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from threading import Event

from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_protocol import (
    _decode_delivery_frame,
    _DeliveryOperation,
)
from manyfold.architecture._transport_delivery_records import (
    _InboxRecord,
    _OutboxRecord,
)
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    ReconnectPolicy,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryConfig,
    DurableDelivery,
    TopicDeliveryPolicy,
)


class TransportDeliveryPerformanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._root = Path(self._temporary_directory.name)
        self._deliveries: list[DurableDelivery] = []
        self._transports: list[TcpTransport] = []

    def tearDown(self) -> None:
        for delivery in reversed(self._deliveries):
            delivery.close()
        for transport in reversed(self._transports):
            transport.close()
        self._temporary_directory.cleanup()

    def test_disconnected_sender_preserves_due_rows_until_peer_connects(
        self,
    ) -> None:
        listener = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "durable-sender", "sender-instance"),
                config=_transport_config(),
                expected_peer_node_id="late-peer",
            )
        )
        config, policy = _config(
            self._root / "disconnected.sqlite3",
            retry_seconds=5.0,
        )
        seeded_at = time.time()
        journal = _DeliveryJournal(config)
        journal.record_inbox(
            _InboxRecord(
                "due-response",
                int(FrameKind.PUBSUB),
                policy.topic,
                None,
                b"response",
                1,
            ),
            created_at=seeded_at,
            expires_at=seeded_at + policy.ttl_seconds,
            now=seeded_at,
            policy=policy,
        )
        self.assertTrue(
            journal.mark_inbox_outcome(
                "due-response",
                status="acked",
                reason=None,
                now=seeded_at,
                retention_seconds=30.0,
            )
        )
        journal.close()
        delivery = self._track_delivery(DurableDelivery(listener, config))
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"data"),
            message_id="due-data",
        )
        before = self._retry_rows(delivery)

        for _ in range(20):
            delivery._sender.wake()
            Event().wait(0.01)

        self.assertEqual(self._retry_rows(delivery), before)
        health = delivery.health()
        self.assertEqual(health.frames_sent, 0)
        self.assertEqual(health.transport_backpressure_failures, 0)
        self.assertEqual(listener.health().outbound_pending, 0)
        self.assertEqual(before[0][0], 0)
        self.assertEqual(before[1][0], 0)

        peer = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "late-peer", "peer-instance"),
                listener.address,
                config=_transport_config(),
                expected_peer_node_id="durable-sender",
            )
        )
        self.assertTrue(listener.wait_until_connected(timeout=2.0))
        self.assertTrue(peer.wait_until_connected(timeout=2.0))
        delivery._sender.wake()
        frames = {
            frame.operation: frame.message_id
            for frame in (
                _decode_delivery_frame(
                    peer.receive(timeout=2.0),
                    max_message_bytes=config.max_message_bytes,
                )
                for _ in range(2)
            )
        }

        self.assertEqual(
            frames,
            {
                _DeliveryOperation.ACK: "due-response",
                _DeliveryOperation.DATA: "due-data",
            },
        )
        after = self._wait_retry_rows(delivery, attempts=1)
        self.assertEqual(after[0][0], 1)
        self.assertEqual(after[1][0], 1)
        self.assertGreater(after[0][1], before[0][1])
        self.assertGreater(after[1][1], before[1][1])
        self.assertEqual(delivery.health().frames_sent, 2)

    def test_no_sweep_insert_reads_each_capacity_snapshot_once(self) -> None:
        config, policy = _config(self._root / "capacity-reads.sqlite3")
        journal = _DeliveryJournal(config)
        statements: list[str] = []
        journal._connection.set_trace_callback(statements.append)
        try:
            journal.insert_outbox(
                _OutboxRecord(
                    "outbox",
                    policy.topic,
                    "append",
                    None,
                    int(FrameKind.PUBSUB),
                    None,
                    b"value",
                    0,
                    policy.max_attempts,
                ),
                created_at=1.0,
                expires_at=31.0,
                now=1.0,
                policy=policy,
            )
            self.assertEqual(_select_count(statements), 5)

            statements.clear()
            inbox = journal.record_inbox(
                _InboxRecord(
                    "inbox",
                    int(FrameKind.PUBSUB),
                    policy.topic,
                    None,
                    b"value",
                    1,
                ),
                created_at=1.0,
                expires_at=31.0,
                now=1.0,
                policy=policy,
            )
            self.assertEqual(_select_count(statements), 5)
            self.assertEqual(inbox.capacity.peer_items, 1)
            self.assertEqual(inbox.capacity.topic_items, 1)

            statements.clear()
            rejection = journal.record_terminal_rejection(
                _InboxRecord(
                    "rejection",
                    int(FrameKind.PUBSUB),
                    "unconfigured",
                    None,
                    b"not retained",
                    1,
                ),
                reason="not durable",
                now=1.0,
            )
            self.assertEqual(_select_count(statements), 6)
            self.assertEqual(rejection.capacity.peer_items, 2)
            self.assertEqual(rejection.capacity.topic_items, 0)
        finally:
            journal._connection.set_trace_callback(None)
            journal.close()

    def _retry_rows(
        self,
        delivery: DurableDelivery,
    ) -> tuple[tuple[int, float], tuple[int, float]]:
        with delivery._runtime.transition():
            outbox = delivery._journal._connection.execute(
                """
                SELECT attempts, next_attempt_at
                FROM outbox WHERE message_id = 'due-data'
                """
            ).fetchone()
            response = delivery._journal._connection.execute(
                """
                SELECT ack_attempts, next_ack_at
                FROM inbox WHERE message_id = 'due-response'
                """
            ).fetchone()
        if outbox is None or response is None:
            raise AssertionError("durable retry rows disappeared")
        return (
            (int(outbox[0]), float(outbox[1])),
            (int(response[0]), float(response[1])),
        )

    def _wait_retry_rows(
        self,
        delivery: DurableDelivery,
        *,
        attempts: int,
    ) -> tuple[tuple[int, float], tuple[int, float]]:
        deadline = time.monotonic() + 1.0
        while True:
            rows = self._retry_rows(delivery)
            if rows[0][0] == attempts and rows[1][0] == attempts:
                return rows
            if time.monotonic() >= deadline:
                raise AssertionError("durable retry attempts did not advance")
            Event().wait(0.01)

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _config(
    path: Path,
    *,
    retry_seconds: float = 0.1,
) -> tuple[DeliveryConfig, TopicDeliveryPolicy]:
    policy = TopicDeliveryPolicy.commands(
        "events",
        max_items=64,
        max_bytes=1024 * 1024,
        ttl_seconds=30.0,
        max_inbox_items=64,
    )
    return (
        DeliveryConfig(
            path,
            max_outbox_items=64,
            max_inbox_items=64,
            max_storage_bytes=1024 * 1024,
            receive_queue_limit=8,
            recovery_batch_size=64,
            max_message_bytes=4096,
            message_ttl_seconds=30.0,
            dedupe_retention_seconds=30.0,
            retry_initial_seconds=retry_seconds,
            retry_max_seconds=retry_seconds,
            topic_policies=(policy,),
        ),
        policy,
    )


def _transport_config() -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=8,
        inbound_queue_limit=8,
        max_payload_bytes=65536,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
    )


def _select_count(statements: list[str]) -> int:
    return sum(
        statement.lstrip().upper().startswith("SELECT ")
        for statement in statements
    )


if __name__ == "__main__":
    unittest.main()
