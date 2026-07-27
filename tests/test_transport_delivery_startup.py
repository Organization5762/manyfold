from __future__ import annotations

import sqlite3
import tempfile
import time
import tracemalloc
import unittest
from contextlib import closing
from pathlib import Path

from manyfold.architecture._transport_delivery_events import DeliveryEvent
from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_records import (
    _InboxRecord,
    _OutboxRecord,
)
from manyfold.architecture.transport import NodeIdentity, TcpTransport
from manyfold.architecture.transport_delivery import (
    DeliveryConfig,
    DeliveryError,
    DeliveryStorageFull,
    DurableDelivery,
    TopicDeliveryPolicy,
)

from tests.test_architecture_transport_delivery import (
    _delivery_config,
    _transport_config,
    _unused_address,
)


class TransportDeliveryStartupTests(unittest.TestCase):
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

    def test_startup_recovery_uses_public_typed_failures(self) -> None:
        now = time.time()
        for name, corrupt in (
            ("policy-drift", False),
            ("bad-size", True),
        ):
            with self.subTest(case=name):
                path = self._root / f"public-recovery-{name}.sqlite3"
                initial_policy = TopicDeliveryPolicy.commands(
                    "events",
                    max_items=8,
                    max_bytes=1024 * 1024,
                    ttl_seconds=10.0,
                    max_attempts=7,
                )
                initial = DeliveryConfig(
                    path,
                    max_outbox_items=8,
                    max_inbox_items=8,
                    max_storage_bytes=1024 * 1024,
                    recovery_batch_size=8,
                    max_message_bytes=4096,
                    message_ttl_seconds=10.0,
                    max_delivery_attempts=7,
                    topic_policies=(initial_policy,),
                )
                journal = _DeliveryJournal(initial)
                journal.insert_outbox(
                    _OutboxRecord(
                        "retained",
                        "events",
                        "append",
                        None,
                        1,
                        None,
                        b"value",
                        0,
                        7,
                    ),
                    created_at=now,
                    expires_at=now + 10.0,
                    now=now,
                    policy=initial_policy,
                )
                journal.close()
                if corrupt:
                    with closing(sqlite3.connect(path)) as connection:
                        connection.execute(
                            """
                            UPDATE outbox SET size_bytes = 160
                            WHERE message_id = 'retained'
                            """
                        )
                        connection.commit()
                current_policy = TopicDeliveryPolicy.commands(
                    "events",
                    max_items=8,
                    max_bytes=1024 * 1024,
                    ttl_seconds=10.0,
                    max_attempts=6 if not corrupt else 7,
                )
                current = DeliveryConfig(
                    path,
                    max_outbox_items=8,
                    max_inbox_items=8,
                    max_storage_bytes=1024 * 1024,
                    recovery_batch_size=8,
                    max_message_bytes=4096,
                    message_ttl_seconds=10.0,
                    max_delivery_attempts=7,
                    topic_policies=(current_policy,),
                )
                transport = self._track_transport(
                    TcpTransport.connect(
                        NodeIdentity("cluster", f"recovery-{name}"),
                        _unused_address(),
                        config=_transport_config(),
                        expected_peer_node_id="missing",
                    )
                )
                expected_error = DeliveryError if corrupt else DeliveryStorageFull
                expected_message = "logical size" if corrupt else "max_attempts"
                with self.assertRaisesRegex(
                    expected_error,
                    expected_message,
                ) as raised:
                    DurableDelivery(transport, current)
                self.assertIsNotNone(raised.exception.__cause__)

    def test_recovery_rejects_row_larger_than_current_transport_frame(
        self,
    ) -> None:
        path = self._root / "lowered-transport.sqlite3"
        topic = "t" * 400
        policy = TopicDeliveryPolicy.commands(
            topic,
            max_items=4,
            max_bytes=1024 * 1024,
            ttl_seconds=5.0,
        )
        config = DeliveryConfig(
            path,
            max_outbox_items=4,
            max_inbox_items=4,
            max_storage_bytes=1024 * 1024,
            recovery_batch_size=4,
            max_message_bytes=100,
            message_ttl_seconds=5.0,
            topic_policies=(policy,),
        )
        journal = _DeliveryJournal(config)
        now = time.time()
        journal.insert_outbox(
            _OutboxRecord(
                "retained",
                topic,
                "append",
                None,
                1,
                None,
                b"x" * 100,
                0,
                64,
            ),
            created_at=now,
            expires_at=now + 5.0,
            now=now,
            policy=policy,
        )
        journal.close()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "lowered-transport"),
                _unused_address(),
                config=_transport_config(max_payload_bytes=512),
                expected_peer_node_id="missing",
            )
        )

        with self.assertRaisesRegex(DeliveryError, "transport limit"):
            DurableDelivery(transport, config)

    def test_startup_hydration_materializes_only_the_bounded_queue(
        self,
    ) -> None:
        payload_size = 128 * 1024
        retained_rows = 20
        for observer_enabled in (False, True):
            with self.subTest(observer_enabled=observer_enabled):
                path = self._root / f"hydration-{observer_enabled}.sqlite3"
                policy = TopicDeliveryPolicy.commands(
                    "events",
                    max_items=32,
                    max_bytes=8 * 1024 * 1024,
                    ttl_seconds=5.0,
                    max_inbox_items=32,
                    max_inbox_bytes=8 * 1024 * 1024,
                )
                config = DeliveryConfig(
                    path,
                    max_outbox_items=32,
                    max_inbox_items=32,
                    max_storage_bytes=8 * 1024 * 1024,
                    receive_queue_limit=4,
                    recovery_batch_size=16,
                    max_message_bytes=256 * 1024,
                    message_ttl_seconds=5.0,
                    topic_policies=(policy,),
                )
                journal = _DeliveryJournal(config)
                now = time.time()
                for index in range(retained_rows):
                    journal.record_inbox(
                        _InboxRecord(
                            f"large-{index:02d}",
                            1,
                            "events",
                            None,
                            bytes([index]) * payload_size,
                            1,
                        ),
                        created_at=now + index * 0.0001,
                        expires_at=now + 5.0,
                        now=now,
                        policy=policy,
                    )
                journal.close()
                transport = self._track_transport(
                    TcpTransport.connect(
                        NodeIdentity(
                            "cluster",
                            f"hydration-{observer_enabled}",
                        ),
                        _unused_address(),
                        config=_transport_config(max_payload_bytes=512 * 1024),
                        expected_peer_node_id="missing",
                    )
                )
                observed: list[DeliveryEvent] = []
                observer = observed.append if observer_enabled else None
                tracemalloc.start()
                before = tracemalloc.get_traced_memory()[0]
                delivery = self._track_delivery(
                    DurableDelivery(
                        transport,
                        config,
                        observer=observer,
                    )
                )
                _, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()

                health = delivery.health()
                self.assertEqual(
                    health.queued_deliveries + health.inflight_deliveries,
                    config.receive_queue_limit,
                )
                received = delivery.receive(timeout=0.2)
                after_receive = delivery.health()
                self.assertEqual(
                    after_receive.queued_deliveries
                    + after_receive.inflight_deliveries,
                    config.receive_queue_limit,
                )
                with delivery._receiver._condition:
                    resident_payload_bytes = sum(
                        len(item.message.payload)
                        for item in delivery._receiver._queue
                    ) + sum(
                        len(item.message.payload)
                        for item in delivery._receiver._inflight.values()
                    )
                self.assertLessEqual(
                    resident_payload_bytes,
                    config.receive_queue_limit * payload_size,
                )
                self.assertLess(peak - before, retained_rows * payload_size)
                self.assertEqual(received.message_id, "large-00")

    def test_journal_rejects_a_second_live_owner(self) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                _unused_address(),
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        config = _delivery_config(self._root / "owned.sqlite3")
        self._track_delivery(DurableDelivery(transport, config))

        with self.assertRaisesRegex(DeliveryError, "already owned"):
            DurableDelivery(transport, config)

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


if __name__ == "__main__":
    unittest.main()
