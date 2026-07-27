from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Barrier, Lock, Thread

from manyfold import architecture
from manyfold.architecture._transport_delivery_events import (
    DeliveryCapacity,
    DeliveryCapacityDimension,
    DeliveryEvent,
    DeliveryEventKind,
    DeliveryOutcome,
    DeliveryStore,
)
from manyfold.architecture._transport_delivery_policy import (
    DEFAULT_DELIVERY_MAX_ATTEMPTS,
    DEFAULT_DELIVERY_SOFT_LIMIT_RATIO,
    DeliveryConfig,
    TopicDeliveryPolicy,
    _bounded_retry_delay,
)
from manyfold.architecture._transport_delivery_protocol import (
    _DELIVERY_HEADER,
    _DELIVERY_MAGIC,
    DELIVERY_CHANNEL,
    DELIVERY_PROTOCOL_VERSION,
    _decode_delivery_frame,
    _DeliveryOperation,
    _encode_delivery_frame,
)
from manyfold.architecture._transport_delivery_records import _JournalStats
from manyfold.architecture._transport_delivery_runtime import (
    _MAX_DELIVERY_ERROR_BYTES,
    _DeliveryRuntime,
)
from manyfold.architecture.transport import FrameKind, TransportMessage
from manyfold.architecture.transport_delivery import DeliveryProtocolError


class TransportDeliveryContractTests(unittest.TestCase):
    def test_current_main_config_and_health_positional_prefixes_are_stable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            config = DeliveryConfig(
                Path(root) / "journal.sqlite3",
                128,
                129,
                1024 * 1024,
                13,
                4096,
                5.0,
                4.0,
                0.2,
                1.5,
                0.8,
            )
        self.assertEqual(
            (
                config.max_outbox_items,
                config.max_inbox_items,
                config.max_storage_bytes,
                config.receive_queue_limit,
                config.max_message_bytes,
                config.message_ttl_seconds,
                config.dedupe_retention_seconds,
                config.retry_initial_seconds,
                config.retry_multiplier,
                config.retry_max_seconds,
            ),
            (128, 129, 1024 * 1024, 13, 4096, 5.0, 4.0, 0.2, 1.5, 0.8),
        )
        self.assertEqual(
            DeliveryConfig.__match_args__[:11],
            (
                "journal_path",
                "max_outbox_items",
                "max_inbox_items",
                "max_storage_bytes",
                "receive_queue_limit",
                "max_message_bytes",
                "message_ttl_seconds",
                "dedupe_retention_seconds",
                "retry_initial_seconds",
                "retry_multiplier",
                "retry_max_seconds",
            ),
        )
        self.assertEqual(
            architecture.DeliveryHealth.__match_args__[:18],
            (
                "generation",
                "closed",
                "outbox_items",
                "pending_inbox_items",
                "acked_inbox_items",
                "logical_storage_bytes",
                "queued_deliveries",
                "inflight_deliveries",
                "accepted",
                "frames_sent",
                "retries",
                "delivered",
                "acknowledgements",
                "negative_acknowledgements",
                "duplicates_suppressed",
                "expired_outbox",
                "expired_inbox",
                "last_error",
            ),
        )
        legacy_health = architecture.DeliveryHealth(
            1,
            False,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            None,
        )
        self.assertEqual(legacy_health.append_outbox_items, 0)

    def test_storage_bound_is_hard_capped_at_64_mib(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "journal.sqlite3"
            self.assertEqual(
                DeliveryConfig(
                    path,
                    max_storage_bytes=64 * 1024 * 1024,
                ).max_storage_bytes,
                64 * 1024 * 1024,
            )
            with self.assertRaisesRegex(ValueError, "67108864"):
                DeliveryConfig(
                    path,
                    max_storage_bytes=(64 * 1024 * 1024) + 1,
                )

    def test_volatile_channel_tokens_are_rejected_without_substring_matches(
        self,
    ) -> None:
        for topic in (
            "heart.frame_tick",
            "heart.input",
            "heart.microphone.level",
            "heart.rendered_frame",
            "heart.audio.samples",
            "DEBUG/INPUT/EVENTS",
            "scene-render:tick",
        ):
            with self.subTest(topic=topic):
                with self.assertRaisesRegex(ValueError, "volatile V1"):
                    TopicDeliveryPolicy.commands(
                        topic,
                        max_items=4,
                        max_bytes=4096,
                    )
        for topic in (
            "debuggable.state",
            "framed.command",
            "heart.inputs.settings",
            "microphonic.level",
            "rendered.frames.saved",
            "inputvalidation.completed",
            "ticker.settings",
        ):
            with self.subTest(topic=topic):
                policy = TopicDeliveryPolicy.commands(
                    topic,
                    max_items=4,
                    max_bytes=4096,
                )
                self.assertEqual(policy.topic, topic)

    def test_latest_defaults_and_global_policy_bounds_are_exact(self) -> None:
        latest = TopicDeliveryPolicy.latest(
            "state",
            max_sources=4,
            max_bytes=4096,
            ttl_seconds=10.0,
        )
        self.assertEqual(latest.max_attempts, DEFAULT_DELIVERY_MAX_ATTEMPTS)
        self.assertEqual(latest.soft_limit_ratio, DEFAULT_DELIVERY_SOFT_LIMIT_RATIO)
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "journal.sqlite3"
            with self.assertRaisesRegex(ValueError, "ttl_seconds exceeds"):
                DeliveryConfig(
                    path,
                    message_ttl_seconds=10.0,
                    topic_policies=(
                        TopicDeliveryPolicy.commands(
                            "orders",
                            max_items=4,
                            max_bytes=4096,
                            ttl_seconds=10.001,
                        ),
                    ),
                )
            extreme = DeliveryConfig(
                path,
                retry_initial_seconds=1.0,
                retry_multiplier=1e308,
                retry_max_seconds=10.0,
                local_pressure_exponent_limit=16,
            )
            self.assertEqual(_bounded_retry_delay(extreme, 1000), 10.0)
            flat = DeliveryConfig(
                path,
                retry_initial_seconds=1.0,
                retry_multiplier=1.0,
                retry_max_seconds=10.0,
                local_pressure_exponent_limit=16,
            )
            self.assertEqual(_bounded_retry_delay(flat, 10**100), 1.0)
            with self.assertRaisesRegex(ValueError, "cannot exceed 16"):
                DeliveryConfig(
                    path,
                    local_pressure_exponent_limit=17,
                )
            with self.assertRaisesRegex(ValueError, "uint32"):
                TopicDeliveryPolicy.commands(
                    "orders",
                    max_items=4,
                    max_bytes=4096,
                    max_attempts=1 << 32,
                )
            with self.assertRaisesRegex(ValueError, "max_attempts exceeds"):
                DeliveryConfig(
                    path,
                    max_delivery_attempts=4,
                    topic_policies=(
                        TopicDeliveryPolicy.commands(
                            "orders",
                            max_items=4,
                            max_bytes=4096,
                            max_attempts=5,
                        ),
                    ),
                )

    def test_outcome_reason_has_exact_utf8_wire_bound(self) -> None:
        self.assertEqual(
            DeliveryOutcome.terminal("x" * 1024).reason,
            "x" * 1024,
        )
        with self.assertRaisesRegex(ValueError, "1024"):
            DeliveryOutcome.terminal("x" * 1025)
        with self.assertRaisesRegex(ValueError, "1024"):
            DeliveryOutcome.terminal("é" * 513)

    def test_v1_frame_reports_precise_version_incompatibility(self) -> None:
        message_id = b"legacy"
        payload = _DELIVERY_HEADER.pack(
            _DELIVERY_MAGIC,
            1,
            int(_DeliveryOperation.ACK),
            0,
            0,
            len(message_id),
            0,
            0,
            0,
        ) + message_id
        message = TransportMessage(
            FrameKind.PUBSUB,
            DELIVERY_CHANNEL,
            payload,
        )

        with self.assertRaisesRegex(
            DeliveryProtocolError,
            "version 1 is incompatible",
        ):
            _decode_delivery_frame(message, max_message_bytes=1)

    def test_control_outcome_uses_its_own_payload_limit(self) -> None:
        encoded = _encode_delivery_frame(
            _DeliveryOperation.NACK,
            "message-1",
            outcome=DeliveryOutcome.terminal("terminal reason"),
        )
        frame = _decode_delivery_frame(
            TransportMessage(FrameKind.PUBSUB, DELIVERY_CHANNEL, encoded),
            max_message_bytes=1,
        )

        self.assertEqual(frame.operation, _DeliveryOperation.NACK)
        self.assertEqual(frame.outcome, DeliveryOutcome.terminal("terminal reason"))

    def test_wire_outcome_reason_rejects_surrounding_whitespace(self) -> None:
        message_id = b"message-1"
        outcome = b"\x02 padded "
        encoded = _DELIVERY_HEADER.pack(
            _DELIVERY_MAGIC,
            DELIVERY_PROTOCOL_VERSION,
            int(_DeliveryOperation.NACK),
            0,
            0,
            len(message_id),
            0,
            0,
            len(outcome),
        ) + message_id + outcome

        with self.assertRaisesRegex(
            DeliveryProtocolError,
            "surrounding whitespace",
        ):
            _decode_delivery_frame(
                TransportMessage(
                    FrameKind.PUBSUB,
                    DELIVERY_CHANNEL,
                    encoded,
                ),
                max_message_bytes=1,
            )

    def test_wire_text_must_be_canonical_python_stripped_text(self) -> None:
        cases = (
            (" message-1", "events", None),
            ("message-1", "events\t", None),
            ("message-1", "events", "\ncorrelation"),
            ("message-1", "\u2003events", None),
        )
        for message_id, channel, correlation_id in cases:
            with self.subTest(
                message_id=message_id,
                channel=channel,
                correlation_id=correlation_id,
            ):
                encoded = _encode_delivery_frame(
                    _DeliveryOperation.DATA,
                    message_id,
                    frame_kind=int(FrameKind.PUBSUB),
                    channel=channel,
                    correlation_id=correlation_id,
                    payload=b"value",
                    delivery_attempt=1,
                )
                with self.assertRaisesRegex(
                    DeliveryProtocolError,
                    "surrounding whitespace",
                ):
                    _decode_delivery_frame(
                        TransportMessage(
                            FrameKind.PUBSUB,
                            DELIVERY_CHANNEL,
                            encoded,
                        ),
                        max_message_bytes=4096,
                    )

    def test_every_event_kind_has_a_valid_typed_shape(self) -> None:
        capacity = DeliveryCapacity(1, 2, 10, 20, 1, 2, 10, 20, 0.5, 0.5)
        for kind in DeliveryEventKind:
            with self.subTest(kind=kind):
                if kind is DeliveryEventKind.EXPIRY_SWEEP:
                    event = DeliveryEvent(1, 1.0, kind, None, None, None)
                elif kind is DeliveryEventKind.WATERMARK_CROSSED:
                    event = DeliveryEvent(
                        1,
                        1.0,
                        kind,
                        None,
                        "orders",
                        None,
                        store=DeliveryStore.OUTBOX,
                        capacity_dimension=(
                            DeliveryCapacityDimension.TOPIC_ITEMS
                        ),
                        capacity=capacity,
                    )
                elif kind is DeliveryEventKind.WATERMARK_RECOVERED:
                    event = DeliveryEvent(
                        1,
                        1.0,
                        kind,
                        None,
                        None,
                        None,
                        store=DeliveryStore.INBOX,
                        capacity_dimension=DeliveryCapacityDimension.PEER_ITEMS,
                        capacity=capacity,
                    )
                else:
                    event = DeliveryEvent(
                        1,
                        1.0,
                        kind,
                        "message-1",
                        "orders",
                        None,
                        store=DeliveryStore.OUTBOX,
                    )
                self.assertIs(event.kind, kind)
        with self.assertRaisesRegex(ValueError, "cannot carry a message_id"):
            DeliveryEvent(
                1,
                1.0,
                DeliveryEventKind.EXPIRY_SWEEP,
                "not-applicable",
                None,
                None,
            )
        with self.assertRaisesRegex(ValueError, "requires a non-empty message_id"):
            DeliveryEvent(
                1,
                1.0,
                DeliveryEventKind.SENT,
                None,
                "orders",
                None,
            )
        with self.assertRaisesRegex(ValueError, "requires a store"):
            DeliveryEvent(
                1,
                1.0,
                DeliveryEventKind.SENT,
                "message-1",
                "orders",
                None,
            )

    def test_concurrent_observer_callbacks_are_strictly_sequence_ordered(
        self,
    ) -> None:
        sequences: list[int] = []
        callback_lock = Lock()

        def observer(event: DeliveryEvent) -> None:
            with callback_lock:
                sequences.append(event.sequence)

        runtime = _DeliveryRuntime(observer)
        barrier = Barrier(9)

        def emit(index: int) -> None:
            barrier.wait()
            runtime.emit(
                DeliveryEventKind.ENQUEUED,
                f"message-{index}",
                "orders",
                None,
                store=DeliveryStore.OUTBOX,
            )

        threads = [Thread(target=emit, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=1.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(sequences, list(range(1, 9)))

    def test_observer_failure_is_isolated_without_an_observer_thread(self) -> None:
        def observer(event: DeliveryEvent) -> None:
            raise RuntimeError(f"observer rejected {event.sequence}")

        runtime = _DeliveryRuntime(observer)
        runtime.emit(
            DeliveryEventKind.ENQUEUED,
            "message-1",
            "orders",
            None,
            store=DeliveryStore.OUTBOX,
        )
        health = runtime.health(
            _JournalStats(0, 0, 0, 0, 0, 0, 0, 0),
            queued_deliveries=0,
            inflight_deliveries=0,
        )

        self.assertEqual(
            health.last_error,
            "RuntimeError: observer rejected 1",
        )
        self.assertFalse(
            any(
                "observer" in thread.name
                for thread in __import__("threading").enumerate()
            )
        )

    def test_observer_error_detail_is_utf8_bounded(self) -> None:
        detail = "界" * (_MAX_DELIVERY_ERROR_BYTES + 1)

        def observer(event: DeliveryEvent) -> None:
            raise RuntimeError(detail)

        runtime = _DeliveryRuntime(observer)
        runtime.emit(
            DeliveryEventKind.ENQUEUED,
            "message-1",
            "orders",
            None,
            store=DeliveryStore.OUTBOX,
        )
        health = runtime.health(
            _JournalStats(0, 0, 0, 0, 0, 0, 0, 0),
            queued_deliveries=0,
            inflight_deliveries=0,
        )

        self.assertIsNotNone(health.last_error)
        self.assertLessEqual(
            len(str(health.last_error).encode()),
            _MAX_DELIVERY_ERROR_BYTES,
        )

    def test_architecture_facade_exports_complete_delivery_contract(self) -> None:
        expected = {
            "DeliveryCapacity",
            "DeliveryCapacityDimension",
            "DeliveryCloseFailed",
            "DeliveryEvent",
            "DeliveryEventKind",
            "DeliveryObserver",
            "DeliveryOutcome",
            "DeliveryOutcomeKind",
            "DeliveryReceiveValidator",
            "DeliverySemantics",
            "DeliveryStore",
            "DeliveryTopicHealth",
            "TopicDeliveryPolicy",
        }
        self.assertLessEqual(expected, set(architecture.__all__))


if __name__ == "__main__":
    unittest.main()
