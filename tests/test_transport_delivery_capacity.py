from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Event

from manyfold.architecture._transport_delivery_events import (
    DeliveryCapacityDimension,
    DeliveryEvent,
    DeliveryEventKind,
)
from manyfold.architecture._transport_delivery_journal import _DeliveryJournal
from manyfold.architecture._transport_delivery_records import (
    _InboxRecord,
    _OutboxRecord,
)
from manyfold.architecture._transport_delivery_recovery import (
    _RecoveredStoreSide,
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


class TransportDeliveryCapacityTests(unittest.TestCase):
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

    def test_noisy_topic_capacity_rejection_does_not_starve_other_topic(
        self,
    ) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        retry_scheduled = Event()
        quiet_enqueued = Event()
        retry_events: list[DeliveryEvent] = []

        def observer(event: DeliveryEvent) -> None:
            if (
                event.kind is DeliveryEventKind.RETRY_SCHEDULED
                and event.topic == "noisy.commands"
            ):
                retry_events.append(event)
                retry_scheduled.set()

        def receiver_observer(event: DeliveryEvent) -> None:
            if (
                event.kind is DeliveryEventKind.ENQUEUED
                and event.message_id == "quiet-first"
            ):
                quiet_enqueued.set()

        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                self._config(
                    self._root / "capacity-sender.sqlite3",
                    noisy_items=4,
                    quiet_items=4,
                    global_items=8,
                ),
                observer=observer,
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                self._config(
                    self._root / "capacity-receiver.sqlite3",
                    noisy_items=1,
                    quiet_items=1,
                    global_items=2,
                ),
                observer=receiver_observer,
            )
        )
        sender.send(
            TransportMessage(
                FrameKind.PUBSUB,
                "noisy.commands",
                b"first",
            ),
            message_id="noisy-first",
        )
        sender.send(
            TransportMessage(
                FrameKind.PUBSUB,
                "noisy.commands",
                b"second",
            ),
            message_id="noisy-second",
        )
        sender.send(
            TransportMessage(
                FrameKind.PUBSUB,
                "quiet.commands",
                b"quiet",
            ),
            message_id="quiet-first",
        )
        self.assertTrue(retry_scheduled.wait(timeout=2.0))
        self.assertTrue(quiet_enqueued.wait(timeout=2.0))
        health = receiver.health()

        self.assertEqual(health.storage_rejections, 1)
        self.assertEqual(health.queued_deliveries, 2)
        self.assertEqual(receiver.topic_health("noisy.commands").inbox_items, 1)
        self.assertEqual(receiver.topic_health("quiet.commands").inbox_items, 1)
        self.assertEqual(len(retry_events), 1)
        self.assertEqual(retry_events[0].message_id, "noisy-second")
        received = [receiver.receive(timeout=1.0) for _ in range(2)]
        self.assertEqual(
            {item.message.channel for item in received},
            {"noisy.commands", "quiet.commands"},
        )
        for item in received:
            receiver.ack(item.message_id)

    def test_recovered_watermarks_identify_peer_and_topic_dimensions_once(
        self,
    ) -> None:
        path = self._root / "watermarks.sqlite3"
        policies = (
            TopicDeliveryPolicy.commands(
                "alpha",
                max_items=2,
                max_bytes=1024 * 1024,
                ttl_seconds=10.0,
                soft_limit_ratio=0.5,
            ),
            TopicDeliveryPolicy.commands(
                "beta",
                max_items=2,
                max_bytes=1024 * 1024,
                ttl_seconds=10.0,
                soft_limit_ratio=0.5,
            ),
        )
        config = DeliveryConfig(
            path,
            max_outbox_items=10,
            max_inbox_items=2,
            max_storage_bytes=1024 * 1024,
            recovery_batch_size=2,
            message_ttl_seconds=10.0,
            soft_limit_ratio=0.5,
            topic_policies=policies,
        )
        journal = _DeliveryJournal(config)
        for index, policy in enumerate(policies):
            journal.insert_outbox(
                _OutboxRecord(
                    f"out-{index}",
                    policy.topic,
                    "append",
                    None,
                    1,
                    None,
                    b"x",
                    0,
                    policy.max_attempts,
                ),
                created_at=1.0 + index,
                expires_at=11.0 + index,
                now=1.0,
                policy=policy,
            )
        journal.record_terminal_rejection(
            _InboxRecord(
                "reject-1",
                1,
                "forbidden",
                None,
                b"ignored",
                1,
            ),
            reason="not durable",
            now=1.0,
        )
        recovered = journal.validate_recovery(
            {policy.topic: policy for policy in policies},
            max_transport_payload_bytes=1 << 30,
            recovery_now=1.0,
        )
        journal.close()

        facts = {
            (item.topic, item.side, item.dimension) for item in recovered
        }
        self.assertIn(
            (
                None,
                _RecoveredStoreSide.INBOX,
                DeliveryCapacityDimension.PEER_ITEMS,
            ),
            facts,
        )
        self.assertIn(
            (
                "alpha",
                _RecoveredStoreSide.OUTBOX,
                DeliveryCapacityDimension.TOPIC_ITEMS,
            ),
            facts,
        )
        self.assertIn(
            (
                "beta",
                _RecoveredStoreSide.OUTBOX,
                DeliveryCapacityDimension.TOPIC_ITEMS,
            ),
            facts,
        )
        self.assertEqual(
            sum(
                dimension is DeliveryCapacityDimension.PEER_ITEMS
                and side is _RecoveredStoreSide.INBOX
                for _, side, dimension in facts
            ),
            1,
        )

    def _config(
        self,
        path: Path,
        *,
        noisy_items: int,
        quiet_items: int,
        global_items: int,
    ) -> DeliveryConfig:
        policies = (
            TopicDeliveryPolicy.commands(
                "noisy.commands",
                max_items=noisy_items,
                max_bytes=1024 * 1024,
                ttl_seconds=5.0,
                max_inbox_items=noisy_items,
            ),
            TopicDeliveryPolicy.commands(
                "quiet.commands",
                max_items=quiet_items,
                max_bytes=1024 * 1024,
                ttl_seconds=5.0,
                max_inbox_items=quiet_items,
            ),
        )
        return DeliveryConfig(
            path,
            max_outbox_items=global_items,
            max_inbox_items=global_items,
            max_storage_bytes=2 * 1024 * 1024,
            receive_queue_limit=global_items,
            recovery_batch_size=global_items,
            max_message_bytes=4096,
            message_ttl_seconds=5.0,
            retry_initial_seconds=0.5,
            retry_max_seconds=0.5,
            topic_policies=policies,
        )

    def _transport_pair(self) -> tuple[TcpTransport, TcpTransport]:
        server = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "receiver", "receiver-capacity"),
                config=_transport_config(),
                expected_peer_node_id="sender",
            )
        )
        client = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-capacity"),
                server.address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        self.assertTrue(server.wait_until_connected(timeout=2.0))
        self.assertTrue(client.wait_until_connected(timeout=2.0))
        return client, server

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _transport_config() -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=16,
        inbound_queue_limit=16,
        max_payload_bytes=65536,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
    )
