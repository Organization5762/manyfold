from __future__ import annotations

import socket
import tempfile
import time
import unittest
from pathlib import Path
from threading import Lock

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
    DeliveryConfig,
    DeliveryEvent,
    DeliveryEventKind,
    DeliveryStorageFull,
    DurableDelivery,
    TopicDeliveryPolicy,
)


class TransportDeliveryEventTests(unittest.TestCase):
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

    def test_observer_identifies_enqueue_coalesce_drop_expire_and_replay(
        self,
    ) -> None:
        journal_path = self._root / "events.sqlite3"
        sensor = TopicDeliveryPolicy.latest(
            "sensor.state",
            max_sources=1,
            max_bytes=4096,
            ttl_seconds=5.0,
        )
        navigation = TopicDeliveryPolicy.commands(
            "navigation.command",
            max_items=1,
            max_bytes=4096,
        )
        short = TopicDeliveryPolicy.commands(
            "debug.latest",
            max_items=2,
            max_bytes=4096,
            ttl_seconds=0.02,
            soft_limit_ratio=0.5,
        )
        config = _delivery_config(
            journal_path,
            topic_policies=(sensor, navigation, short),
        )
        recorder = _EventRecorder()
        transport = self._track_transport(_disconnected_transport())
        delivery = self._track_delivery(
            DurableDelivery(transport, config, observer=recorder)
        )

        delivery.send(
            TransportMessage(FrameKind.PUBSUB, navigation.topic, b"go"),
            message_id="nav-1",
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, navigation.topic, b"go"),
            message_id="nav-1",
        )
        with self.assertRaises(DeliveryStorageFull):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, navigation.topic, b"stop"),
                message_id="nav-2",
            )
        first_sensor_id = delivery.send(
            TransportMessage(
                FrameKind.PUBSUB,
                sensor.topic,
                b"old",
                correlation_id="imu-key",
            ),
            source="imu-1",
        )
        second_sensor_id = delivery.send(
            TransportMessage(
                FrameKind.PUBSUB,
                sensor.topic,
                b"new",
                correlation_id="imu-key",
            ),
            source="imu-1",
        )
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, short.topic, b"expired"),
            message_id="debug-old",
        )
        time.sleep(0.03)
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, short.topic, b"current"),
            message_id="debug-current",
        )

        events = recorder.snapshot()
        self.assertEqual(
            _event(events, DeliveryEventKind.ENQUEUED, "nav-1").topic,
            navigation.topic,
        )
        self.assertEqual(
            _event(events, DeliveryEventKind.DEDUPLICATED, "nav-1").topic,
            navigation.topic,
        )
        self.assertIn(
            "item limit",
            _event(events, DeliveryEventKind.DROPPED, "nav-2").detail or "",
        )
        rejected_capacity = _event(
            events,
            DeliveryEventKind.DROPPED,
            "nav-2",
        ).capacity
        self.assertIsNotNone(rejected_capacity)
        self.assertGreater(
            rejected_capacity.topic_items,
            rejected_capacity.topic_item_limit,
        )
        coalesced = _event(
            events,
            DeliveryEventKind.COALESCED,
            second_sensor_id,
        )
        self.assertEqual(coalesced.related_message_id, first_sensor_id)
        self.assertEqual(coalesced.source, "imu-1")
        self.assertEqual(coalesced.correlation_id, "imu-key")
        watermark = _event(
            events,
            DeliveryEventKind.SOFT_WATERMARK,
            second_sensor_id,
        )
        self.assertEqual(watermark.capacity.topic_items, 1)
        self.assertEqual(watermark.capacity.topic_item_limit, 1)
        self.assertEqual(
            _event(
                events,
                DeliveryEventKind.EXPIRED,
                "debug-old",
            ).topic,
            short.topic,
        )
        self.assertEqual(
            tuple(event.sequence for event in events),
            tuple(range(1, len(events) + 1)),
        )

        delivery.close()
        replay_recorder = _EventRecorder()
        reopened = self._track_delivery(
            DurableDelivery(transport, config, observer=replay_recorder)
        )
        replayed = tuple(
            event
            for event in replay_recorder.snapshot()
            if event.kind is DeliveryEventKind.REPLAYED
        )
        self.assertEqual(
            {event.message_id for event in replayed},
            {"nav-1", second_sensor_id, "debug-current"},
        )
        sensor_replay = _event(
            replayed,
            DeliveryEventKind.REPLAYED,
            second_sensor_id,
        )
        self.assertEqual(sensor_replay.topic, sensor.topic)
        self.assertEqual(sensor_replay.source, "imu-1")
        self.assertEqual(reopened.health().recovered_outbox, 3)

    def test_observer_identifies_send_retry_schedule_and_ack(self) -> None:
        sender_transport, receiver_transport = self._transport_pair()
        recorder = _EventRecorder()
        receiver_recorder = _EventRecorder()
        sender = self._track_delivery(
            DurableDelivery(
                sender_transport,
                _delivery_config(self._root / "sender.sqlite3"),
                observer=recorder,
            )
        )
        receiver = self._track_delivery(
            DurableDelivery(
                receiver_transport,
                _delivery_config(self._root / "receiver.sqlite3"),
                observer=receiver_recorder,
            )
        )
        message = TransportMessage(
            FrameKind.PUBSUB,
            "events",
            b"value",
            correlation_id="publication-key",
        )
        sender.send(
            message,
            message_id="event-1",
        )
        received = receiver.receive(timeout=2.0)
        receiver.ack(received.message_id)
        self.assertTrue(sender.flush(timeout=2.0))
        sender.send(message, message_id="event-1")
        self.assertTrue(sender.flush(timeout=2.0))

        events = recorder.snapshot()
        for kind in (
            DeliveryEventKind.ENQUEUED,
            DeliveryEventKind.SENT,
            DeliveryEventKind.RETRY_SCHEDULED,
            DeliveryEventKind.ACKNOWLEDGED,
        ):
            event = _first_event(events, kind, "event-1")
            self.assertEqual(event.topic, "events")
            self.assertIsNone(event.source)
            self.assertEqual(event.correlation_id, "publication-key")
        self.assertEqual(
            _first_event(events, DeliveryEventKind.SENT, "event-1").attempt,
            1,
        )
        self.assertEqual(
            _first_event(
                events,
                DeliveryEventKind.RETRY_SCHEDULED,
                "event-1",
            ).attempt,
            2,
        )
        suppressed = _event(
            receiver_recorder.snapshot(),
            DeliveryEventKind.DUPLICATE_SUPPRESSED,
            "event-1",
        )
        self.assertEqual(suppressed.topic, "events")
        self.assertEqual(suppressed.correlation_id, "publication-key")
        self.assertEqual(suppressed.detail, "already acknowledged")

    def test_observer_failure_is_isolated_from_durable_send(self) -> None:
        transport = self._track_transport(_disconnected_transport())

        def fail_observation(_event: DeliveryEvent) -> None:
            raise RuntimeError("observer unavailable")

        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(self._root / "observer-failure.sqlite3"),
                observer=fail_observation,
            )
        )
        message_id = delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"retained")
        )

        self.assertTrue(message_id)
        self.assertEqual(delivery.health().outbox_items, 1)
        self.assertIn("observer unavailable", delivery.health().last_error or "")

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

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


def _event(
    events: tuple[DeliveryEvent, ...],
    kind: DeliveryEventKind,
    message_id: str,
) -> DeliveryEvent:
    matches = tuple(
        event
        for event in events
        if event.kind is kind and event.message_id == message_id
    )
    if len(matches) != 1:
        raise AssertionError(
            f"expected one {kind.value} event for {message_id!r}, got {matches!r}"
        )
    return matches[0]


def _first_event(
    events: tuple[DeliveryEvent, ...],
    kind: DeliveryEventKind,
    message_id: str,
) -> DeliveryEvent:
    for event in events:
        if event.kind is kind and event.message_id == message_id:
            return event
    raise AssertionError(f"missing {kind.value} event for {message_id!r}")


def _delivery_config(
    path: Path,
    *,
    topic_policies: tuple[TopicDeliveryPolicy, ...] = (),
) -> DeliveryConfig:
    return DeliveryConfig(
        path,
        max_outbox_items=16,
        max_inbox_items=16,
        max_storage_bytes=1024 * 1024,
        receive_queue_limit=4,
        max_message_bytes=4096,
        message_ttl_seconds=5.0,
        dedupe_retention_seconds=5.0,
        retry_initial_seconds=0.05,
        retry_multiplier=1.5,
        retry_max_seconds=0.1,
        topic_policies=topic_policies,
    )


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


def _disconnected_transport() -> TcpTransport:
    return TcpTransport.connect(
        NodeIdentity("cluster", "sender", "sender-1"),
        _unused_address(),
        config=_transport_config(),
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


class _EventRecorder:
    def __init__(self) -> None:
        self._events: list[DeliveryEvent] = []
        self._lock = Lock()

    def __call__(self, event: DeliveryEvent) -> None:
        with self._lock:
            self._events.append(event)

    def snapshot(self) -> tuple[DeliveryEvent, ...]:
        with self._lock:
            return tuple(self._events)


if __name__ == "__main__":
    unittest.main()
