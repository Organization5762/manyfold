from __future__ import annotations

import os
import random
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from queue import Queue
from threading import Thread

from manyfold.architecture._transport_delivery_protocol import (
    _DeliveryFrame,
    _DeliveryOperation,
)
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    ReconnectPolicy,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)
from manyfold.architecture.transport_delivery import (
    DeliveryClosed,
    DeliveryConfig,
    DeliveryConflict,
    DeliveryError,
    DeliveryEvent,
    DeliveryEventKind,
    DeliveryStorageFull,
    DurableDelivery,
    TopicDeliveryPolicy,
)

from tests.test_support import subprocess_test_env


class ArchitectureTransportDeliveryTests(unittest.TestCase):
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

    def test_acknowledged_message_drains_durable_outbox(self) -> None:
        sender, receiver = self._delivery_pair()
        message_id = sender.send(
            TransportMessage(FrameKind.PUBSUB, "orders.created", b"order-7"),
            message_id="order-7",
        )

        received = receiver.receive(timeout=2.0)
        receiver.ack(received.message_id)

        self.assertEqual(message_id, "order-7")
        self.assertEqual(received.message.payload, b"order-7")
        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(sender.health().outbox_items, 0)
        self.assertEqual(receiver.health().acknowledgements, 1)

    def test_nack_releases_inbox_and_redelivers(self) -> None:
        sender, receiver = self._delivery_pair()
        sender.send(
            TransportMessage(
                FrameKind.RPC_REQUEST,
                "cache.get",
                b"k",
                correlation_id="request-1",
            ),
            message_id="request-1",
        )
        first = receiver.receive(timeout=2.0)

        receiver.nack(first.message_id, reason="retryable")
        second = receiver.receive(timeout=2.0)
        receiver.ack(second.message_id)

        self.assertEqual(second.message_id, first.message_id)
        self.assertGreaterEqual(second.delivery_attempt, 2)
        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(receiver.health().negative_acknowledgements, 1)

    def test_unacked_retries_are_suppressed_while_application_is_inflight(
        self,
    ) -> None:
        sender, receiver = self._delivery_pair(retry_initial_seconds=0.02)
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"one"),
            message_id="duplicate-1",
        )
        received = receiver.receive(timeout=2.0)

        self.assertTrue(
            _wait_for(
                lambda: receiver.health().duplicates_suppressed >= 1,
                timeout=1.0,
            )
        )
        with self.assertRaises(TimeoutError):
            receiver.receive(timeout=0.1)
        receiver.ack(received.message_id)

        self.assertTrue(sender.flush(timeout=2.0))
        self.assertEqual(receiver.health().delivered, 1)

    def test_hard_process_exit_recovers_outbox_and_delivers(self) -> None:
        address = _unused_address()
        journal_path = self._root / "crashed-sender.sqlite3"
        script = """
import os
import sys
from pathlib import Path
from manyfold.architecture.transport import (
    FrameKind, NodeIdentity, ReconnectPolicy, TcpAddress, TcpTransport,
    TransportConfig, TransportMessage, TransportSecurity,
)
from manyfold.architecture.transport_delivery import DeliveryConfig, DurableDelivery

transport = TcpTransport.connect(
    NodeIdentity("cluster", "sender", "crashed-process"),
    TcpAddress(sys.argv[1], int(sys.argv[2])),
    config=TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        max_payload_bytes=65536,
        connect_timeout=0.05,
        handshake_timeout=0.2,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
    ),
    expected_peer_node_id="receiver",
)
delivery = DurableDelivery(
    transport,
    DeliveryConfig(
        Path(sys.argv[3]),
        max_storage_bytes=1024 * 1024,
        max_message_bytes=4096,
        retry_initial_seconds=0.02,
        retry_max_seconds=0.1,
    ),
)
delivery.send(
    TransportMessage(FrameKind.PUBSUB, "recovery", b"persisted"),
    message_id="crash-recovery-1",
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
            timeout=5.0,
        )
        self.assertEqual(completed.returncode, 0)

        server = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "receiver", "receiver-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="sender",
            )
        )
        client = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-restarted"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                server,
                _delivery_config(self._root / "receiver.sqlite3"),
            )
        )
        recovered_sender = self._track_delivery(
            DurableDelivery(client, _delivery_config(journal_path))
        )

        received = receiver.receive(timeout=3.0)
        receiver.ack(received.message_id)

        self.assertEqual(received.message_id, "crash-recovery-1")
        self.assertEqual(received.message.payload, b"persisted")
        self.assertTrue(recovered_sender.flush(timeout=2.0))

    def test_unacknowledged_message_retries_after_live_link_reconnect(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        receiver_journal = self._root / "receiver.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "sender.sqlite3"),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(receiver_journal),
            )
        )
        address = receiver_transport.address
        receiver.close()
        receiver_transport.close()
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "reconnect", b"waiting"),
            message_id="reconnect-1",
        )

        replacement_transport = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "receiver", "receiver-2"),
                address,
                config=_transport_config(),
                expected_peer_node_id="sender",
            )
        )
        replacement = self._track_delivery(
            DurableDelivery(
                replacement_transport,
                _delivery_config(receiver_journal),
            )
        )
        self.assertTrue(sender_transport.wait_until_connected(timeout=2.0))
        try:
            received = replacement.receive(timeout=2.0)
        except TimeoutError as error:
            self.fail(
                "durable reconnect timed out; "
                f"sender_transport={sender_transport.health()!r}; "
                f"sender_delivery={sender.health()!r}; "
                f"replacement_transport={replacement_transport.health()!r}; "
                f"replacement_delivery={replacement.health()!r}; "
                f"cause={error}"
            )
        replacement.ack(received.message_id)

        self.assertEqual(received.message_id, "reconnect-1")
        self.assertTrue(sender.flush(timeout=2.0))
        self.assertGreaterEqual(sender.health().frames_sent, 1)

    def test_acked_inbox_suppresses_same_id_after_reopen(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        sender_journal = self._root / "sender.sqlite3"
        receiver_journal = self._root / "receiver.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(sender_transport, _delivery_config(sender_journal))
        )
        receiver = self._track_delivery(
            DurableDelivery(receiver_transport, _delivery_config(receiver_journal))
        )
        message = TransportMessage(FrameKind.PUBSUB, "events", b"value")
        sender.send(message, message_id="stable-1")
        first = receiver.receive(timeout=2.0)
        receiver.ack(first.message_id)
        self.assertTrue(sender.flush(timeout=2.0))

        receiver.close()
        reopened_receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(receiver_journal),
            )
        )
        sender.send(message, message_id="stable-1")

        self.assertTrue(sender.flush(timeout=2.0))
        with self.assertRaises(TimeoutError):
            reopened_receiver.receive(timeout=0.2)
        self.assertGreaterEqual(
            reopened_receiver.health().duplicates_suppressed,
            1,
        )

    def test_pending_inbox_is_redelivered_after_layer_reopen(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        receiver_journal = self._root / "receiver.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "sender.sqlite3"),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(receiver_transport, _delivery_config(receiver_journal))
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"pending"),
            message_id="pending-1",
        )
        first = receiver.receive(timeout=2.0)
        receiver.close()

        reopened = self._track_delivery(
            DurableDelivery(receiver_transport, _delivery_config(receiver_journal))
        )
        recovered = reopened.receive(timeout=1.0)
        reopened.ack(recovered.message_id)

        self.assertEqual(recovered.message_id, first.message_id)
        self.assertTrue(sender.flush(timeout=2.0))

    def test_outbox_item_and_byte_bounds_fail_before_accepting_more(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    self._root / "bounded.sqlite3",
                    max_outbox_items=1,
                    max_storage_bytes=128 * 1024,
                    max_message_bytes=4096,
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"first"),
            message_id="first",
        )

        for index in range(32):
            with self.assertRaisesRegex(DeliveryStorageFull, "item limit"):
                delivery.send(
                    TransportMessage(FrameKind.PUBSUB, "events", b"overflow"),
                    message_id=f"overflow-{index}",
                )

        self.assertEqual(delivery.health().outbox_items, 1)
        self.assertLessEqual(
            os.path.getsize(delivery.config.journal_path),
            delivery.config.max_storage_bytes,
        )

    def test_expiry_compacts_unacknowledged_outbox(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(
                    self._root / "expiry.sqlite3",
                    message_ttl_seconds=0.1,
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"expires"),
            message_id="expires-1",
        )

        self.assertTrue(
            _wait_for(
                lambda: delivery.health().expired_outbox == 1,
                timeout=1.5,
            )
        )
        self.assertEqual(delivery.health().outbox_items, 0)

    def test_inbox_expiry_releases_inflight_memory_and_allows_redelivery(
        self,
    ) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "sender.sqlite3"),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(
                    self._root / "receiver.sqlite3",
                    dedupe_retention_seconds=0.1,
                ),
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"expires"),
            message_id="inflight-expiry-1",
        )
        first = receiver.receive(timeout=2.0)

        self.assertTrue(
            _wait_for(
                lambda: receiver.health().expired_inbox >= 1,
                timeout=1.5,
            )
        )
        self.assertEqual(receiver.health().inflight_deliveries, 0)
        redelivered = receiver.receive(timeout=2.0)
        receiver.ack(redelivered.message_id)

        self.assertEqual(redelivered.message_id, first.message_id)
        self.assertGreater(redelivered.delivery_attempt, first.delivery_attempt)
        self.assertTrue(sender.flush(timeout=2.0))

    def test_logical_byte_bound_rejects_payload_before_disk_growth(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(max_payload_bytes=256 * 1024),
                expected_peer_node_id="receiver",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    self._root / "byte-bounded.sqlite3",
                    max_outbox_items=8,
                    max_storage_bytes=128 * 1024,
                    max_message_bytes=70_000,
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"a" * 70_000),
            message_id="large-1",
        )

        with self.assertRaisesRegex(DeliveryStorageFull, "byte"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"b" * 70_000),
                message_id="large-2",
            )

        self.assertEqual(delivery.health().outbox_items, 1)
        self.assertLessEqual(
            os.path.getsize(delivery.config.journal_path),
            delivery.config.max_storage_bytes,
        )

    def test_exact_encoded_transport_limit_rejects_before_journaling(self) -> None:
        events: list[DeliveryEvent] = []
        transport = self._disconnected_transport(max_payload_bytes=1024)
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    self._root / "encoded-limit.sqlite3",
                    max_outbox_items=4,
                    max_storage_bytes=1024 * 1024,
                    max_message_bytes=512,
                ),
                observer=events.append,
            )
        )
        channel = "topic." + ("x" * 220)
        correlation_id = "correlation-" + ("y" * 220)
        message_id = "message-" + ("z" * 80)

        with self.assertRaisesRegex(ValueError, "encoded durable message"):
            delivery.send(
                TransportMessage(
                    FrameKind.PUBSUB,
                    channel,
                    b"x" * 512,
                    correlation_id=correlation_id,
                ),
                message_id=message_id,
            )

        self.assertEqual(delivery.health().outbox_items, 0)
        dropped = _event(events, DeliveryEventKind.DROPPED, message_id)
        self.assertEqual(dropped.topic, channel)
        self.assertEqual(dropped.correlation_id, correlation_id)

    def test_recovered_unsendable_outbox_row_is_terminally_dropped(self) -> None:
        journal_path = self._root / "unsendable-reopen.sqlite3"
        large_transport = self._disconnected_transport(max_payload_bytes=4096)
        large_delivery = self._track_delivery(
            DurableDelivery(
                large_transport,
                DeliveryConfig(
                    journal_path,
                    max_outbox_items=4,
                    max_storage_bytes=1024 * 1024,
                    max_message_bytes=512,
                ),
            )
        )
        channel = "topic." + ("x" * 220)
        correlation_id = "correlation-" + ("y" * 220)
        message_id = "message-" + ("z" * 80)
        large_delivery.send(
            TransportMessage(
                FrameKind.PUBSUB,
                channel,
                b"x" * 512,
                correlation_id=correlation_id,
            ),
            message_id=message_id,
        )
        large_delivery.close()

        events: list[DeliveryEvent] = []
        small_transport = self._disconnected_transport(max_payload_bytes=1024)
        reopened = self._track_delivery(
            DurableDelivery(
                small_transport,
                DeliveryConfig(
                    journal_path,
                    max_outbox_items=4,
                    max_storage_bytes=1024 * 1024,
                    max_message_bytes=512,
                ),
                observer=events.append,
            )
        )
        reopened._send_due_outbox()

        self.assertEqual(reopened.health().outbox_items, 0)
        dropped = _event(events, DeliveryEventKind.DROPPED, message_id)
        self.assertEqual(dropped.topic, channel)
        self.assertEqual(dropped.correlation_id, correlation_id)

    def test_latest_replaces_before_topic_capacity_check_atomically(self) -> None:
        journal_path = self._root / "latest.sqlite3"
        transport = self._disconnected_transport()
        policy = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=1,
            max_bytes=300,
            ttl_seconds=5.0,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(journal_path, topic_policies=(policy,)),
            )
        )
        first_id = delivery.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"old"),
            source="imu-1",
        )
        second_id = delivery.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"new"),
            source="imu-1",
        )

        with self.assertRaisesRegex(DeliveryStorageFull, "byte limit"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, policy.topic, b"x" * 200),
                source="imu-1",
            )

        delivery.close()
        connection = sqlite3.connect(journal_path)
        row = connection.execute(
            "SELECT message_id, payload FROM outbox"
        ).fetchone()
        connection.close()
        self.assertNotEqual(first_id, second_id)
        self.assertEqual(row, (second_id, b"new"))
        health = delivery.health()
        self.assertEqual(health.outbox_items, 1)
        self.assertEqual(health.latest_outbox_items, 1)
        self.assertEqual(health.coalesced, 1)
        self.assertEqual(health.storage_rejections, 1)

    def test_generated_ids_keep_journal_namespace_and_sequence_after_reopen(
        self,
    ) -> None:
        journal_path = self._root / "stable-ids.sqlite3"
        transport = self._disconnected_transport()
        policy = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=1,
            max_bytes=4096,
            ttl_seconds=30.0,
        )
        config = _delivery_config(journal_path, topic_policies=(policy,))
        first = self._track_delivery(DurableDelivery(transport, config))
        first_id = first.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"one"),
            source="imu-1",
        )
        first.close()

        reopened = self._track_delivery(DurableDelivery(transport, config))
        second_id = reopened.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"two"),
            source="imu-1",
        )

        first_namespace, first_sequence = first_id.rsplit("-", 1)
        second_namespace, second_sequence = second_id.rsplit("-", 1)
        self.assertEqual(second_namespace, first_namespace)
        self.assertGreater(int(second_sequence, 16), int(first_sequence, 16))
        self.assertEqual(reopened.health().recovered_outbox, 1)
        self.assertEqual(reopened.health().outbox_items, 1)

    def test_per_topic_limits_are_stricter_than_peer_hard_caps(self) -> None:
        transport = self._disconnected_transport()
        navigation = TopicDeliveryPolicy.commands(
            "navigation.command",
            max_items=2,
            max_bytes=4096,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    self._root / "topic-limits.sqlite3",
                    max_outbox_items=4,
                    max_storage_bytes=1024 * 1024,
                    max_message_bytes=4096,
                    topic_policies=(navigation,),
                ),
            )
        )
        for index in range(2):
            delivery.send(
                TransportMessage(
                    FrameKind.PUBSUB,
                    navigation.topic,
                    f"go-{index}".encode(),
                ),
                message_id=f"nav-{index}",
            )

        with self.assertRaisesRegex(DeliveryStorageFull, "navigation.command"):
            delivery.send(
                TransportMessage(
                    FrameKind.PUBSUB,
                    navigation.topic,
                    b"overflow",
                ),
                message_id="nav-overflow",
            )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "other.command", b"allowed"),
            message_id="other-1",
        )

        self.assertEqual(delivery.health().outbox_items, 3)

    def test_reopen_fails_closed_when_recovered_outbox_exceeds_current_cap(
        self,
    ) -> None:
        journal_path = self._root / "lowered-outbox.sqlite3"
        transport = self._disconnected_transport()
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    journal_path,
                    max_outbox_items=2,
                    max_storage_bytes=1024 * 1024,
                    max_message_bytes=4096,
                ),
            )
        )
        for index in range(2):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "navigation.command", b"go"),
                message_id=f"nav-{index}",
            )
        delivery.close()

        with self.assertRaisesRegex(DeliveryStorageFull, "recovered outbox"):
            DurableDelivery(
                transport,
                DeliveryConfig(
                    journal_path,
                    max_outbox_items=1,
                    max_storage_bytes=1024 * 1024,
                    max_message_bytes=4096,
                ),
            )

    def test_reopen_fails_closed_when_recovered_latest_topic_exceeds_policy(
        self,
    ) -> None:
        journal_path = self._root / "lowered-latest.sqlite3"
        transport = self._disconnected_transport()
        original_policy = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=2,
            max_bytes=4096,
            ttl_seconds=30.0,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(journal_path, topic_policies=(original_policy,)),
            )
        )
        for source in ("imu-1", "imu-2"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, original_policy.topic, b"value"),
                source=source,
            )
        delivery.close()

        lowered_items = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=1,
            max_bytes=4096,
            ttl_seconds=30.0,
        )
        with self.assertRaisesRegex(DeliveryStorageFull, "sensor.state"):
            DurableDelivery(
                transport,
                _delivery_config(journal_path, topic_policies=(lowered_items,)),
            )

        lowered_bytes = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=2,
            max_bytes=100,
            ttl_seconds=30.0,
        )
        with self.assertRaisesRegex(DeliveryStorageFull, "logical bytes"):
            DurableDelivery(
                transport,
                _delivery_config(journal_path, topic_policies=(lowered_bytes,)),
            )

    def test_reopen_fails_closed_when_commands_topic_becomes_latest(
        self,
    ) -> None:
        journal_path = self._root / "commands-to-latest.sqlite3"
        transport = self._disconnected_transport()
        command_policy = TopicDeliveryPolicy.commands(
            "navigation.state",
            max_items=8,
            max_bytes=4096,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(journal_path, topic_policies=(command_policy,)),
            )
        )
        for index in range(2):
            delivery.send(
                TransportMessage(
                    FrameKind.PUBSUB,
                    command_policy.topic,
                    f"command-{index}".encode(),
                ),
                message_id=f"command-{index}",
            )
        delivery.close()

        latest_policy = TopicDeliveryPolicy.latest(
            "navigation.state",
            max_sources=8,
            max_bytes=4096,
            ttl_seconds=30.0,
        )
        with self.assertRaisesRegex(DeliveryStorageFull, "requires 'latest'"):
            DurableDelivery(
                transport,
                _delivery_config(journal_path, topic_policies=(latest_policy,)),
            )

    def test_reopen_fails_closed_when_latest_policy_is_removed(
        self,
    ) -> None:
        journal_path = self._root / "latest-removed.sqlite3"
        transport = self._disconnected_transport()
        latest_policy = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=1,
            max_bytes=4096,
            ttl_seconds=30.0,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(journal_path, topic_policies=(latest_policy,)),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, latest_policy.topic, b"value"),
            source="imu-1",
        )
        delivery.close()

        with self.assertRaisesRegex(DeliveryStorageFull, "requires 'append'"):
            DurableDelivery(
                transport,
                _delivery_config(journal_path, topic_policies=()),
            )

    def test_reopen_fails_closed_when_recovered_inbox_exceeds_current_cap(
        self,
    ) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        receiver_journal = self._root / "lowered-inbox.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "inbox-sender.sqlite3"),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                DeliveryConfig(
                    receiver_journal,
                    max_outbox_items=16,
                    max_inbox_items=2,
                    max_storage_bytes=1024 * 1024,
                    receive_queue_limit=2,
                    max_message_bytes=4096,
                ),
            )
        )
        for index in range(2):
            sender.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"pending"),
                message_id=f"pending-{index}",
            )
        receiver.receive(timeout=2.0)
        receiver.receive(timeout=2.0)
        receiver.close()

        with self.assertRaisesRegex(DeliveryStorageFull, "recovered inbox"):
            DurableDelivery(
                receiver_transport,
                DeliveryConfig(
                    receiver_journal,
                    max_outbox_items=16,
                    max_inbox_items=1,
                    max_storage_bytes=1024 * 1024,
                    receive_queue_limit=2,
                    max_message_bytes=4096,
                ),
            )

    def test_reopen_fails_closed_when_recovered_inbox_exceeds_topic_cap(
        self,
    ) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        receiver_journal = self._root / "lowered-inbox-topic.sqlite3"
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "inbox-topic-sender.sqlite3"),
            )
        )
        topic_policy = TopicDeliveryPolicy.commands(
            "events.noisy",
            max_items=2,
            max_bytes=4096,
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(
                    receiver_journal,
                    topic_policies=(topic_policy,),
                ),
            )
        )
        for index in range(2):
            sender.send(
                TransportMessage(FrameKind.PUBSUB, topic_policy.topic, b"pending"),
                message_id=f"noisy-{index}",
            )
        receiver.receive(timeout=2.0)
        receiver.receive(timeout=2.0)
        receiver.close()

        lowered_topic_policy = TopicDeliveryPolicy.commands(
            "events.noisy",
            max_items=1,
            max_bytes=4096,
            max_inbox_items=1,
            max_inbox_bytes=4096,
        )
        with self.assertRaisesRegex(DeliveryStorageFull, "recovered inbox topic"):
            DurableDelivery(
                receiver_transport,
                _delivery_config(
                    receiver_journal,
                    topic_policies=(lowered_topic_policy,),
                ),
            )

        lowered_byte_policy = TopicDeliveryPolicy.commands(
            "events.noisy",
            max_items=2,
            max_bytes=4096,
            max_inbox_items=2,
            max_inbox_bytes=100,
        )
        with self.assertRaisesRegex(DeliveryStorageFull, "logical bytes"):
            DurableDelivery(
                receiver_transport,
                _delivery_config(
                    receiver_journal,
                    topic_policies=(lowered_byte_policy,),
                ),
            )

    def test_inbox_topic_limit_prevents_noisy_topic_from_starving_others(
        self,
    ) -> None:
        events: list[DeliveryEvent] = []
        sender_transport, receiver_transport = self._transport_pair()
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "inbox-live-sender.sqlite3"),
            )
        )
        noisy = TopicDeliveryPolicy.commands(
            "events.noisy",
            max_items=1,
            max_bytes=4096,
        )
        quiet = TopicDeliveryPolicy.commands(
            "events.quiet",
            max_items=1,
            max_bytes=4096,
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(
                    self._root / "inbox-live-receiver.sqlite3",
                    topic_policies=(noisy, quiet),
                ),
                observer=events.append,
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, noisy.topic, b"first"),
            message_id="noisy-1",
        )
        first = receiver.receive(timeout=2.0)
        sender.send(
            TransportMessage(FrameKind.PUBSUB, noisy.topic, b"overflow"),
            message_id="noisy-2",
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, quiet.topic, b"allowed"),
            message_id="quiet-1",
        )
        second = receiver.receive(timeout=2.0)

        self.assertEqual(first.message_id, "noisy-1")
        self.assertEqual(second.message_id, "quiet-1")
        self.assertEqual(receiver.topic_health(noisy.topic).inbox_items, 1)
        self.assertEqual(receiver.topic_health(quiet.topic).inbox_items, 1)
        self.assertEqual(receiver.health().storage_rejections, 1)
        dropped = _event(events, DeliveryEventKind.DROPPED, "noisy-2")
        self.assertEqual(dropped.topic, noisy.topic)
        self.assertEqual(dropped.attempt, 1)
        self.assertIsNotNone(dropped.capacity)
        self.assertGreater(
            dropped.capacity.topic_items,
            dropped.capacity.topic_item_limit,
        )

    def test_inbox_duplicate_does_not_spend_topic_capacity(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        policy = TopicDeliveryPolicy.commands(
            "events.idempotent",
            max_items=4,
            max_bytes=4096,
            max_inbox_items=1,
            max_inbox_bytes=4096,
        )
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(
                    self._root / "duplicate-capacity-sender.sqlite3",
                    retry_initial_seconds=0.01,
                    topic_policies=(policy,),
                ),
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(
                    self._root / "duplicate-capacity-receiver.sqlite3",
                    topic_policies=(policy,),
                ),
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"first"),
            message_id="same-id",
        )
        receiver.receive(timeout=2.0)

        self.assertTrue(
            _wait_for(
                lambda: receiver.health().duplicates_suppressed >= 1,
                timeout=1.0,
            )
        )
        self.assertEqual(receiver.topic_health(policy.topic).inbox_items, 1)

    def test_receive_validator_rejects_before_inbox_admission(self) -> None:
        events: list[DeliveryEvent] = []
        transport = self._disconnected_transport()

        def reject_large(message: TransportMessage) -> None:
            if len(message.payload) > 3:
                raise ValueError("payload exceeds bound topic limit")

        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(self._root / "validator-receiver.sqlite3"),
                observer=events.append,
                receive_validator=reject_large,
            )
        )
        delivery._handle_data(
            _DeliveryFrame(
                operation=_DeliveryOperation.DATA,
                message_id="rejected-1",
                frame_kind=int(FrameKind.PUBSUB),
                delivery_attempt=1,
                channel="events.large",
                correlation_id="large-key",
                payload=b"large",
            )
        )

        health = delivery.health()
        self.assertEqual(health.storage_rejections, 1)
        self.assertEqual(delivery.topic_health("events.large").inbox_items, 0)
        dropped = _event(events, DeliveryEventKind.DROPPED, "rejected-1")
        self.assertEqual(dropped.topic, "events.large")
        self.assertEqual(dropped.correlation_id, "large-key")
        self.assertEqual(dropped.attempt, 1)

    def test_soft_watermark_expires_old_rows_during_send(self) -> None:
        transport = self._disconnected_transport()
        policy = TopicDeliveryPolicy.commands(
            "short.command",
            max_items=2,
            max_bytes=4096,
            ttl_seconds=0.02,
            soft_limit_ratio=0.5,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(
                    self._root / "soft-watermark.sqlite3",
                    topic_policies=(policy,),
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"expired"),
            message_id="expired",
        )
        time.sleep(0.03)
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"current"),
            message_id="current",
        )

        health = delivery.health()
        self.assertEqual(health.outbox_items, 1)
        self.assertEqual(health.expired_outbox, 1)
        self.assertGreaterEqual(health.soft_compactions, 1)
        self.assertEqual(
            health.soft_watermark_crossings,
            health.soft_compactions,
        )

    def test_retry_budget_compacts_unacknowledged_message(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        policy = TopicDeliveryPolicy.commands(
            "bounded.command",
            max_items=4,
            max_bytes=4096,
            ttl_seconds=5.0,
            max_attempts=2,
        )
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(
                    self._root / "bounded-retry.sqlite3",
                    retry_initial_seconds=0.01,
                    topic_policies=(policy,),
                ),
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"unacked"),
            message_id="bounded-1",
        )

        self.assertTrue(
            _wait_for(
                lambda: sender.health().retry_exhausted == 1,
                timeout=1.5,
            )
        )
        self.assertEqual(sender.health().outbox_items, 0)
        receiver_transport.receive(timeout=0.2)
        receiver_transport.receive(timeout=0.2)
        with self.assertRaises(TimeoutError):
            receiver_transport.receive(timeout=0.1)

    def test_local_transport_backpressure_does_not_consume_delivery_attempts(
        self,
    ) -> None:
        events: list[DeliveryEvent] = []
        sender_transport = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "sender", "sender-1"),
                config=_transport_config(outbound_queue_limit=1),
                expected_peer_node_id="receiver",
            )
        )
        sender_transport.send(
            TransportMessage(FrameKind.PUBSUB, "queue.filler", b"held")
        )
        policy = TopicDeliveryPolicy.commands(
            "bounded.command",
            max_items=4,
            max_bytes=4096,
            ttl_seconds=5.0,
            max_attempts=2,
        )
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(
                    self._root / "queue-pressure.sqlite3",
                    retry_initial_seconds=0.01,
                    topic_policies=(policy,),
                ),
                observer=events.append,
            )
        )
        sender.send(
            TransportMessage(FrameKind.PUBSUB, policy.topic, b"durable"),
            message_id="pressure-1",
        )

        scheduled_delays: list[float] = []
        for _ in range(policy.max_attempts + 3):
            attempt_started_at = time.time()
            sender._send_due_outbox()
            connection = sqlite3.connect(sender.config.journal_path)
            next_attempt_at = float(
                connection.execute(
                    """
                    SELECT next_attempt_at FROM outbox
                    WHERE message_id = 'pressure-1'
                    """
                ).fetchone()[0]
            )
            connection.close()
            scheduled_delays.append(next_attempt_at - attempt_started_at)
            time.sleep(0.012)

        health_under_pressure = sender.health()
        self.assertEqual(health_under_pressure.retry_exhausted, 0)
        self.assertEqual(health_under_pressure.outbox_items, 1)
        self.assertEqual(health_under_pressure.frames_sent, 0)
        self.assertEqual(
            health_under_pressure.transport_backpressure_failures,
            policy.max_attempts + 3,
        )
        self.assertEqual(
            health_under_pressure.transport_backpressure_streak,
            policy.max_attempts + 3,
        )
        self.assertGreater(scheduled_delays[1], scheduled_delays[0])
        self.assertGreater(scheduled_delays[2], scheduled_delays[1])
        self.assertLessEqual(max(scheduled_delays), sender.config.retry_max_seconds)
        retry_events = [
            event
            for event in events
            if event.kind is DeliveryEventKind.RETRY_SCHEDULED
        ]
        self.assertEqual(
            [event.local_pressure_count for event in retry_events],
            [1, 2, 3, 4, 5],
        )
        self.assertEqual({event.attempt for event in retry_events}, {1})

        receiver_transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "receiver", "receiver-1"),
                sender_transport.address,
                config=_transport_config(),
                expected_peer_node_id="sender",
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(self._root / "pressure-receiver.sqlite3"),
            )
        )
        received = receiver.receive(timeout=2.0)
        receiver.ack(received.message_id)

        self.assertEqual(received.message_id, "pressure-1")
        self.assertEqual(received.message.payload, b"durable")
        self.assertTrue(sender.flush(timeout=2.0))
        recovered_health = sender.health()
        self.assertEqual(recovered_health.transport_backpressure_failures, 5)
        self.assertEqual(recovered_health.transport_backpressure_streak, 0)

    def test_randomized_journal_model_stays_bounded_and_deduplicated(self) -> None:
        journal_path = self._root / "property.sqlite3"
        transport = self._disconnected_transport()
        commands = TopicDeliveryPolicy.commands(
            "navigation.command",
            max_items=16,
            max_bytes=64 * 1024,
        )
        sensors = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=4,
            max_bytes=64 * 1024,
            ttl_seconds=5.0,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(
                    journal_path,
                    topic_policies=(commands, sensors),
                ),
            )
        )
        expected_latest: dict[str, bytes] = {}
        generator = random.Random(5762)
        for _ in range(250):
            if generator.random() < 0.65:
                source = f"sensor-{generator.randrange(4)}"
                payload = generator.randbytes(16)
                expected_latest[source] = payload
                delivery.send(
                    TransportMessage(FrameKind.PUBSUB, sensors.topic, payload),
                    source=source,
                )
            else:
                command = generator.randrange(12)
                delivery.send(
                    TransportMessage(
                        FrameKind.PUBSUB,
                        commands.topic,
                        f"command-{command}".encode(),
                    ),
                    message_id=f"command-{command}",
                )

        health = delivery.health()
        self.assertLessEqual(health.latest_outbox_items, sensors.max_items)
        self.assertLessEqual(
            health.append_outbox_items,
            commands.max_items,
        )
        delivery.close()
        connection = sqlite3.connect(journal_path)
        latest_rows = dict(
            connection.execute(
                """
                SELECT source_key, payload FROM outbox
                WHERE topic = ? AND semantics = 'latest'
                """,
                (sensors.topic,),
            )
        )
        command_ids = tuple(
            row[0]
            for row in connection.execute(
                """
                SELECT message_id FROM outbox
                WHERE topic = ? AND semantics = 'append'
                """,
                (commands.topic,),
            )
        )
        connection.close()
        self.assertEqual(latest_rows, expected_latest)
        self.assertEqual(len(command_ids), len(set(command_ids)))
        self.assertLessEqual(len(command_ids), 12)
        self.assertGreater(health.coalesced, 0)
        self.assertGreater(health.outbox_deduplicated, 0)

    def test_topic_health_reports_exact_rows_and_zero_for_volatile_bypass(
        self,
    ) -> None:
        transport = self._disconnected_transport()
        sensor = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=1,
            max_bytes=4096,
            ttl_seconds=30.0,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(
                    self._root / "topic-health.sqlite3",
                    topic_policies=(sensor,),
                ),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, sensor.topic, b"value"),
            source="imu-1",
        )

        sensor_health = delivery.topic_health(sensor.topic)
        volatile_health = delivery.topic_health("frame.tick")
        self.assertEqual(sensor_health.retained_items, 1)
        self.assertEqual(sensor_health.latest_outbox_items, 1)
        self.assertGreater(sensor_health.logical_storage_bytes, 0)
        self.assertEqual(volatile_health.retained_items, 0)
        self.assertEqual(volatile_health.logical_storage_bytes, 0)

    def test_stable_id_conflict_and_close_release_resources(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(self._root / "lifecycle.sqlite3"),
            )
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"one"),
            message_id="same",
        )
        with self.assertRaisesRegex(DeliveryConflict, "different content"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"two"),
                message_id="same",
            )

        receive_errors: Queue[BaseException] = Queue()

        def receive_until_closed() -> None:
            try:
                delivery.receive()
            except DeliveryClosed as error:
                receive_errors.put(error)

        blocked_receivers = [
            Thread(target=receive_until_closed),
            Thread(target=receive_until_closed),
        ]
        for receiver in blocked_receivers:
            receiver.start()
        delivery.close()
        for receiver in blocked_receivers:
            receiver.join(timeout=1.0)

        self.assertFalse(delivery._sender.is_alive())
        self.assertFalse(delivery._receiver.is_alive())
        self.assertTrue(all(not receiver.is_alive() for receiver in blocked_receivers))
        self.assertEqual(receive_errors.qsize(), 2)
        self.assertTrue(
            all(
                isinstance(receive_errors.get_nowait(), DeliveryClosed)
                for _ in blocked_receivers
            )
        )
        self.assertTrue(delivery.health().closed)
        with self.assertRaisesRegex(DeliveryClosed, "closed"):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"late")
            )

    def test_journal_rejects_a_second_live_owner(self) -> None:
        address = _unused_address()
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        config = _delivery_config(self._root / "owned.sqlite3")
        self._track_delivery(DurableDelivery(transport, config))

        with self.assertRaisesRegex(DeliveryError, "already owned"):
            DurableDelivery(transport, config)

    def _delivery_pair(
        self,
        *,
        retry_initial_seconds: float = 0.05,
    ) -> tuple[DurableDelivery, DurableDelivery]:
        sender_transport, receiver_transport = self._transport_pair()
        return (
            self._track_delivery(
                DurableDelivery(
                    sender_transport,
                    _delivery_config(
                        self._root / "sender.sqlite3",
                        retry_initial_seconds=retry_initial_seconds,
                    ),
                )
            ),
            self._track_delivery(
                DurableDelivery(
                    receiver_transport,
                    _delivery_config(
                        self._root / "receiver.sqlite3",
                        retry_initial_seconds=retry_initial_seconds,
                    ),
                )
            ),
        )

    def _transport_pair(self) -> tuple[TcpTransport, TcpTransport]:
        server = self._track_transport(
            TcpTransport.listen(
                NodeIdentity("cluster", "receiver", "receiver-1"),
                config=_transport_config(),
                expected_peer_node_id="sender",
            )
        )
        client = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                server.address,
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        self.assertTrue(server.wait_until_connected(timeout=2.0))
        self.assertTrue(client.wait_until_connected(timeout=2.0))
        return client, server

    def _disconnected_transport(
        self,
        *,
        max_payload_bytes: int = 65536,
    ) -> TcpTransport:
        return self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-1"),
                _unused_address(),
                config=_transport_config(max_payload_bytes=max_payload_bytes),
                expected_peer_node_id="receiver",
            )
        )

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _transport_config(
    *,
    max_payload_bytes: int = 65536,
    outbound_queue_limit: int = 16,
) -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=outbound_queue_limit,
        inbound_queue_limit=16,
        max_payload_bytes=max_payload_bytes,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
    )


def _delivery_config(
    path: Path,
    *,
    retry_initial_seconds: float = 0.05,
    message_ttl_seconds: float = 5.0,
    dedupe_retention_seconds: float = 5.0,
    topic_policies: tuple[TopicDeliveryPolicy, ...] = (),
) -> DeliveryConfig:
    return DeliveryConfig(
        path,
        max_outbox_items=16,
        max_inbox_items=16,
        max_storage_bytes=1024 * 1024,
        receive_queue_limit=4,
        max_message_bytes=4096,
        message_ttl_seconds=message_ttl_seconds,
        dedupe_retention_seconds=dedupe_retention_seconds,
        retry_initial_seconds=retry_initial_seconds,
        retry_multiplier=1.5,
        retry_max_seconds=0.1,
        topic_policies=topic_policies,
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


def _event(
    events: list[DeliveryEvent],
    kind: DeliveryEventKind,
    message_id: str,
) -> DeliveryEvent:
    for event in events:
        if event.kind is kind and event.message_id == message_id:
            return event
    raise AssertionError(f"missing {kind.value} event for {message_id!r}")


if __name__ == "__main__":
    unittest.main()
