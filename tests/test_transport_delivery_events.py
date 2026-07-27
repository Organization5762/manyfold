from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Event, Lock, Thread

from manyfold.architecture._transport_delivery_events import (
    DeliveryCapacityDimension,
    DeliveryEvent,
    DeliveryEventKind,
)
from manyfold.architecture._transport_delivery_records import _OutboxRecord
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    TcpTransport,
    TransportMessage,
)
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


class TransportDeliveryEventTests(unittest.TestCase):
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

    def test_concurrent_commits_publish_whole_causal_batches_in_order(
        self,
    ) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-events"),
                _unused_address(),
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        events: list[DeliveryEvent] = []
        events_lock = Lock()
        crossing_observed = Event()
        waiter_observed: list[bool] = []

        def observer(event: DeliveryEvent) -> None:
            with events_lock:
                events.append(event)
            if (
                event.kind is DeliveryEventKind.WATERMARK_CROSSED
                and not crossing_observed.is_set()
            ):
                crossing_observed.set()
                waiter_observed.append(
                    delivery._runtime.wait_for_transition_waiters(
                        1,
                        timeout=0.5,
                    )
                )

        policy = TopicDeliveryPolicy.commands(
            "events",
            max_items=2,
            max_bytes=1024 * 1024,
            ttl_seconds=5.0,
            soft_limit_ratio=0.5,
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    self._root / "event-order.sqlite3",
                    max_outbox_items=2,
                    max_inbox_items=2,
                    max_storage_bytes=1024 * 1024,
                    recovery_batch_size=2,
                    max_message_bytes=4096,
                    message_ttl_seconds=5.0,
                    soft_limit_ratio=0.5,
                    topic_policies=(policy,),
                ),
                observer=observer,
            )
        )

        def send_second() -> None:
            self.assertTrue(crossing_observed.wait(timeout=0.5))
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "events", b"second"),
                message_id="second",
            )

        concurrent_sender = Thread(target=send_second)
        concurrent_sender.start()
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"first"),
            message_id="first",
        )
        concurrent_sender.join(timeout=1.0)

        self.assertFalse(concurrent_sender.is_alive())
        self.assertEqual(waiter_observed, [True])
        relevant = [
            (event.kind, event.message_id, event.capacity_dimension)
            for event in events
            if event.kind
            in {
                DeliveryEventKind.WATERMARK_CROSSED,
                DeliveryEventKind.ENQUEUED,
            }
        ]
        self.assertEqual(
            relevant[:4],
            [
                (
                    DeliveryEventKind.WATERMARK_CROSSED,
                    None,
                    DeliveryCapacityDimension.PEER_ITEMS,
                ),
                (
                    DeliveryEventKind.WATERMARK_CROSSED,
                    None,
                    DeliveryCapacityDimension.TOPIC_ITEMS,
                ),
                (DeliveryEventKind.ENQUEUED, "first", None),
                (DeliveryEventKind.ENQUEUED, "second", None),
            ],
        )

    def test_watermark_batch_reports_causal_capacity_sweep_and_rollback(
        self,
    ) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-watermark"),
                _unused_address(),
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        events: list[DeliveryEvent] = []
        policies = (
            TopicDeliveryPolicy.commands(
                "old",
                max_items=8,
                max_bytes=1024 * 1024,
                ttl_seconds=5.0,
                soft_limit_ratio=0.9,
            ),
            TopicDeliveryPolicy.commands(
                "crossing",
                max_items=1,
                max_bytes=1024 * 1024,
                ttl_seconds=5.0,
                soft_limit_ratio=0.5,
            ),
            TopicDeliveryPolicy.commands(
                "rollback",
                max_items=1,
                max_bytes=160,
                ttl_seconds=5.0,
                soft_limit_ratio=0.5,
            ),
            TopicDeliveryPolicy.commands(
                "zero-sweep",
                max_items=1,
                max_bytes=1024 * 1024,
                ttl_seconds=5.0,
                soft_limit_ratio=0.5,
            ),
        )
        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                DeliveryConfig(
                    self._root / "watermark-order.sqlite3",
                    max_outbox_items=8,
                    max_inbox_items=8,
                    max_storage_bytes=1024 * 1024,
                    recovery_batch_size=8,
                    max_message_bytes=4096,
                    message_ttl_seconds=5.0,
                    soft_limit_ratio=0.9,
                    topic_policies=policies,
                ),
                observer=events.append,
            )
        )
        with delivery._runtime.transition():
            delivery._journal.insert_outbox(
                _OutboxRecord(
                    "expired-before-sweep",
                    "old",
                    "append",
                    None,
                    int(FrameKind.PUBSUB),
                    None,
                    b"expired",
                    0,
                    policies[0].max_attempts,
                ),
                created_at=1.0,
                expires_at=2.0,
                now=1.0,
                policy=policies[0],
            )
            events.clear()
            delivery.send(
                TransportMessage(
                    FrameKind.PUBSUB,
                    "crossing",
                    b"accepted",
                ),
                message_id="crossing-row",
            )

        relevant = [
            event
            for event in events
            if event.kind
            in {
                DeliveryEventKind.WATERMARK_CROSSED,
                DeliveryEventKind.EXPIRED,
                DeliveryEventKind.EXPIRY_SWEEP,
                DeliveryEventKind.ENQUEUED,
            }
        ]
        self.assertEqual(
            [event.kind for event in relevant],
            [
                DeliveryEventKind.WATERMARK_CROSSED,
                DeliveryEventKind.EXPIRED,
                DeliveryEventKind.EXPIRY_SWEEP,
                DeliveryEventKind.ENQUEUED,
            ],
        )
        crossing = relevant[0]
        self.assertIsNotNone(crossing.capacity)
        self.assertGreaterEqual(
            crossing.capacity.topic_items,
            crossing.capacity.topic_item_limit
            * crossing.capacity.topic_soft_limit_ratio,
        )
        self.assertEqual(relevant[1].message_id, "expired-before-sweep")
        self.assertEqual(relevant[2].deleted_items, 1)
        self.assertEqual(relevant[3].message_id, "crossing-row")
        health = delivery.health()
        self.assertEqual(health.watermark_crossings, 1)
        self.assertEqual(health.expiry_sweeps, 1)
        self.assertEqual(health.sweep_deleted_rows, 1)

        events.clear()
        delivery.send(
            TransportMessage(
                FrameKind.PUBSUB,
                "zero-sweep",
                b"accepted",
            ),
            message_id="zero-sweep-row",
        )
        zero_sweep = [
            event
            for event in events
            if event.kind
            in {
                DeliveryEventKind.WATERMARK_CROSSED,
                DeliveryEventKind.EXPIRY_SWEEP,
                DeliveryEventKind.ENQUEUED,
            }
        ]
        self.assertEqual(
            [event.kind for event in zero_sweep],
            [
                DeliveryEventKind.WATERMARK_CROSSED,
                DeliveryEventKind.EXPIRY_SWEEP,
                DeliveryEventKind.ENQUEUED,
            ],
        )
        self.assertEqual(zero_sweep[1].deleted_items, 0)
        self.assertEqual(delivery.health().watermark_crossings, 2)
        self.assertEqual(delivery.health().expiry_sweeps, 2)
        self.assertEqual(delivery.health().sweep_deleted_rows, 1)

        events.clear()
        with self.assertRaises(DeliveryStorageFull):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "rollback", b""),
                message_id="rolled-back",
            )
        self.assertEqual(events, [])
        self.assertEqual(delivery.health().outbox_items, 2)

    def test_observer_reentry_is_read_only_and_does_not_mutate_batch(self) -> None:
        transport = self._track_transport(
            TcpTransport.connect(
                NodeIdentity("cluster", "sender", "sender-observer"),
                _unused_address(),
                config=_transport_config(),
                expected_peer_node_id="receiver",
            )
        )
        holder: dict[str, DurableDelivery] = {}
        errors: list[DeliveryError] = []
        observed_health: list[int] = []

        def observer(event: DeliveryEvent) -> None:
            delivery = holder["delivery"]
            observed_health.append(delivery.health().outbox_items)
            try:
                delivery.send(
                    TransportMessage(FrameKind.PUBSUB, "events", b"reentrant"),
                    message_id="reentrant",
                )
            except DeliveryError as error:
                errors.append(error)

        delivery = self._track_delivery(
            DurableDelivery(
                transport,
                _delivery_config(self._root / "observer.sqlite3"),
                observer=observer,
            )
        )
        holder["delivery"] = delivery
        delivery.send(
            TransportMessage(FrameKind.PUBSUB, "events", b"original"),
            message_id="original",
        )

        self.assertTrue(observed_health)
        self.assertTrue(errors)
        self.assertRegex(str(errors[0]), "read-only")
        self.assertEqual(delivery.health().outbox_items, 1)

    def _track_delivery(self, delivery: DurableDelivery) -> DurableDelivery:
        self._deliveries.append(delivery)
        return delivery

    def _track_transport(self, transport: TcpTransport) -> TcpTransport:
        self._transports.append(transport)
        return transport


if __name__ == "__main__":
    unittest.main()
