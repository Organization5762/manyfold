from __future__ import annotations

import unittest
from dataclasses import dataclass
from uuid import UUID

from manyfold.architecture import (
    CalibratedClock,
    Capacitor,
    Clock,
    ClockCalibrationSample,
    DataStreamProcessor,
    Ground,
    MonotonicLogicalClock,
    NtpTimeProvider,
    Probe,
    PubSub,
    Regulator,
    Relay,
    Resistor,
    StreamRow,
    SystemTimeProvider,
    Via,
)
from manyfold.architecture.pubsub import InMemoryPubSub, PubSubMessage


class ArchitecturePubSubTests(unittest.TestCase):
    def test_in_memory_pubsub_publishes_to_named_subscription(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=8)
        subscription = pubsub.subscribe("orders.created", name="orders")

        delivery = pubsub.publish("orders.created", b"order-123")
        messages = pubsub.poll(subscription.name)

        self.assertEqual(subscription.topic, "orders.created")
        self.assertEqual(delivery.topic, "orders.created")
        self.assertEqual(delivery.offset, 0)
        self.assertEqual(delivery.delivered_to, ["orders"])
        self.assertEqual(delivery.subscriber_count, 1)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].topic, "orders.created")
        self.assertEqual(messages[0].payload, b"order-123")
        self.assertEqual(messages[0].offset, 0)

    def test_pubsub_supports_wildcard_subscription_and_offsets(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=8)
        pubsub.subscribe("*", name="all-events")

        pubsub.publish("orders.created", b"created")
        pubsub.publish("orders.paid", b"paid")

        messages = pubsub.poll("all-events")

        self.assertEqual(
            [message.topic for message in messages],
            ["orders.created", "orders.paid"],
        )
        self.assertEqual([message.offset for message in messages], [0, 1])
        self.assertEqual(pubsub.topic_offsets(), {"orders.created": 0, "orders.paid": 1})

    def test_pubsub_can_replay_retained_messages_from_beginning(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=2)
        pubsub.publish("events", b"first")
        pubsub.publish("events", b"second")
        pubsub.publish("events", b"third")

        pubsub.subscribe("events", name="replay", replay_from_beginning=True)
        messages = pubsub.poll("replay")

        self.assertEqual([message.payload for message in messages], [b"second", b"third"])
        self.assertEqual([message.offset for message in messages], [1, 2])
        self.assertEqual(pubsub.message_count, 2)

    def test_pubsub_latest_reads_latest_matching_topic(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=8)
        pubsub.publish("a", b"one")
        pubsub.publish("b", b"two")
        pubsub.publish("a", b"three")

        latest_any = pubsub.latest()
        latest_a = pubsub.latest("a")

        self.assertIsNotNone(latest_any)
        self.assertIsNotNone(latest_a)
        self.assertEqual(latest_any.payload, b"three")
        self.assertEqual(latest_a.payload, b"three")

    def test_pubsub_rejects_invalid_bounds_and_payloads(self) -> None:
        with self.assertRaisesRegex(ValueError, "retained_messages"):
            InMemoryPubSub(retained_messages=0)

        pubsub = InMemoryPubSub()
        pubsub.subscribe("events", name="events")
        with self.assertRaisesRegex(ValueError, "non-empty bytes"):
            pubsub.publish("events", b"")
        with self.assertRaisesRegex(ValueError, "unknown subscription"):
            pubsub.poll("missing")
        with self.assertRaisesRegex(ValueError, "max_messages"):
            pubsub.poll("events", max_messages=0)

    def test_message_can_be_constructed_for_application_boundaries(self) -> None:
        message = PubSubMessage("client.events", b"payload", offset=7)

        self.assertEqual(message.topic, "client.events")
        self.assertEqual(message.payload, b"payload")
        self.assertEqual(message.offset, 7)


class DataStreamProcessorTests(unittest.TestCase):
    def test_latest_reads_newest_pubsub_message_with_sql_ordering(self) -> None:
        pubsub = InMemoryPubSub(retained_messages=8)
        processor = DataStreamProcessor()
        subscription = pubsub.subscribe(
            "sensor.environment.temperature",
            name="temperature-processor",
        )

        pubsub.publish("sensor.environment.temperature", b"72.4F")
        pubsub.publish("sensor.environment.temperature", b"72.9F")
        processor.ingest_many(pubsub.poll(subscription.name))

        latest = processor.latest("sensor.environment.temperature")

        self.assertIsNotNone(latest)
        self.assertEqual(latest.topic, "sensor.environment.temperature")
        self.assertEqual(latest.payload, b"72.9F")
        self.assertEqual(latest.offset, 1)
        self.assertEqual(latest.process_sequence, 2)
        self.assertEqual(latest.event_time, 2)
        self.assertEqual(latest.seq_source, 2)

    def test_latest_is_scoped_to_topic(self) -> None:
        processor = DataStreamProcessor()
        processor.ingest(PubSubMessage("a", b"one", offset=0), event_time=10)
        processor.ingest(PubSubMessage("b", b"two", offset=1), event_time=20)
        processor.ingest(PubSubMessage("a", b"three", offset=2), event_time=30)

        latest = processor.latest("a")

        self.assertIsNotNone(latest)
        self.assertEqual(latest.payload, b"three")
        self.assertEqual(latest.event_time, 30)
        self.assertEqual(latest.seq_source, 3)
        self.assertIsNone(processor.latest("missing"))

    def test_query_exposes_read_only_sql_stream_table(self) -> None:
        processor = DataStreamProcessor()
        processor.ingest(PubSubMessage("temperature", b"72.4F", offset=0))
        processor.ingest(PubSubMessage("temperature", b"72.9F", offset=1))

        row = processor.query_one(
            """
            SELECT process_sequence, event_time, message_key, payload
            FROM stream_messages
            WHERE topic = :topic
            ORDER BY event_time DESC
            LIMIT 1
            """,
            {"topic": "temperature"},
        )

        self.assertEqual(
            row,
            {
                "process_sequence": 2,
                "event_time": 2,
                "message_key": None,
                "payload": b"72.9F",
            },
        )
        with self.assertRaisesRegex(ValueError, "SELECT or WITH"):
            processor.query("DELETE FROM stream_messages")


class PubSubStreamTests(unittest.TestCase):
    def test_pubsub_stream_latest_returns_row_from_sql_query(self) -> None:
        temperature = PubSub(
            topic="sensor.environment.temperature",
            schema=Temperature,
        )

        temperature.publish(Temperature(degrees=72.4, unit="F"))
        temperature.publish(Temperature(degrees=72.9, unit="F"))

        latest = temperature.latest()
        latest_row = temperature.query_one(
            """
            SELECT offset + 1 AS seq_source, degrees, unit
            FROM stream
            ORDER BY event_time DESC, process_sequence DESC
            LIMIT 1
            """
        )

        self.assertIsInstance(latest, StreamRow)
        self.assertEqual(latest.degrees, 72.9)
        self.assertEqual(latest["unit"], "F")
        self.assertEqual(latest.seq_source, 2)
        self.assertEqual(latest.as_model(Temperature), Temperature(degrees=72.9, unit="F"))
        self.assertEqual(
            latest_row,
            {"seq_source": 2, "degrees": 72.9, "unit": "F"},
        )
        self.assertEqual(temperature.average(field="degrees"), 72.65)

    def test_pubsub_stream_namespaces_inferred_schema_to_topic(self) -> None:
        temperature = PubSub(
            topic="sensor.environment.temperature",
            schema=Temperature,
        )

        self.assertEqual(
            temperature._stream_schema.name,
            "sensor.environment.temperature.Temperature",
        )

    def test_pubsub_stream_publishes_model_fields_directly(self) -> None:
        temperature = PubSub(
            topic="sensor.environment.temperature",
            schema=Temperature,
        )

        temperature.publish(degrees=72.9, unit="F")

        latest = temperature.latest()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.degrees, 72.9)
        self.assertEqual(latest.unit, "F")

    def test_pubsub_stream_accepts_pydantic_shaped_schema(self) -> None:
        temperature = PubSub(
            topic="sensor.environment.temperature",
            schema=_PydanticTemperature,
        )

        temperature.publish(_PydanticTemperature(degrees=72.9, unit="F"))

        latest = temperature.latest()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.degrees, 72.9)
        self.assertEqual(latest.as_model(_PydanticTemperature), _PydanticTemperature(degrees=72.9, unit="F"))

    def test_pubsub_stream_without_schema_returns_latest_payload(self) -> None:
        stream = PubSub(topic="raw")

        stream.publish(b"first")
        stream.publish(b"second")

        latest = stream.latest()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.payload, b"second")

    def test_pubsub_stream_can_use_ephemeral_topic(self) -> None:
        stream = PubSub(schema=Temperature)

        stream.publish(Temperature(degrees=72.9, unit="F"))
        row = stream.query_one(
            """
            SELECT topic, pad_name, degrees, unit
            FROM stream
            LIMIT 1
            """
        )

        self.assertTrue(stream.topic.startswith("manyfold.ephemeral."))
        self.assertEqual(UUID(stream.topic.removeprefix("manyfold.ephemeral.")).version, 5)
        latest = stream.latest()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.degrees, 72.9)
        self.assertEqual(latest.unit, "F")
        self.assertEqual(
            row,
            {
                "topic": stream.topic,
                "pad_name": stream.topic,
                "degrees": 72.9,
                "unit": "F",
            },
        )
        self.assertEqual(
            stream._stream_schema.name,
            f"{stream.topic}.Temperature",
        )

    def test_pubsub_stream_ephemeral_topics_are_unique(self) -> None:
        left = PubSub()
        right = PubSub()

        self.assertNotEqual(left.topic, right.topic)

    def test_pubsub_stream_lazily_infers_schema_from_first_model_publish(self) -> None:
        stream = PubSub()

        stream.publish(Temperature(degrees=72.9, unit="F"))

        latest = stream.latest()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.degrees, 72.9)
        self.assertEqual(latest.as_model(Temperature), Temperature(degrees=72.9, unit="F"))
        self.assertEqual(
            stream._stream_schema.name,
            f"{stream.topic}.Temperature",
        )
        self.assertEqual(
            stream.query_one(
                """
                SELECT degrees, unit
                FROM stream
                LIMIT 1
                """
            ),
            {"degrees": 72.9, "unit": "F"},
        )

    def test_pubsub_stream_rejects_changing_lazily_inferred_schema(self) -> None:
        stream = PubSub()
        stream.publish(Temperature(degrees=72.9, unit="F"))

        with self.assertRaisesRegex(ValueError, "schema is already fixed"):
            stream.publish(_Humidity(percent=45.0))


class NativeArchitectureElementTests(unittest.TestCase):
    def test_native_architecture_namespace_exports_manyfold_elements(self) -> None:
        elements = (
            Capacitor("edge-cache", "sensor", "processor", location="processor"),
            Resistor("network-budget", "edge-to-core", "rate-limit", limit=10),
            Regulator("publish-rate", "token-bucket", limit=10),
            Relay("local-forward", "a", "b"),
            Via("network-hop", "edge", "core", "network"),
            Ground("drop-debug", "debug stream disabled"),
            Probe("traffic-tap", "sensor"),
        )

        self.assertEqual(elements[0].capacity, 1)
        self.assertEqual(elements[1].boundary, "edge-to-core")
        self.assertEqual(elements[3].target, "b")
        self.assertEqual(elements[4].boundary, "network")
        self.assertEqual(Clock().tick(), 1)

    def test_native_clock_uses_specific_time_provider_and_observations(self) -> None:
        system_time = SystemTimeProvider()
        observed_ns = system_time.now_ns()
        calibrated = CalibratedClock(
            [
                ClockCalibrationSample(
                    observed_ns=observed_ns,
                    reference_ns=observed_ns + 3,
                    temperature_c=42.0,
                )
            ]
        )
        logical = MonotonicLogicalClock()
        ntp = NtpTimeProvider("pool.ntp.org", timeout_ms=100)

        self.assertEqual(logical.tick(), 1)
        self.assertGreater(calibrated.now_ns(), 0)
        self.assertEqual(calibrated.samples[0].reference_ns, observed_ns + 3)
        self.assertEqual(ntp.server, "pool.ntp.org")
        self.assertEqual(ntp.port, 123)

@dataclass(frozen=True)
class Temperature:
    degrees: float
    unit: str


@dataclass(frozen=True)
class _Humidity:
    percent: float


@dataclass(frozen=True)
class _PydanticField:
    annotation: object


@dataclass(frozen=True)
class _PydanticTemperature:
    model_fields = {
        "degrees": _PydanticField(float),
        "unit": _PydanticField(str),
    }

    degrees: float
    unit: str

    def model_dump(self) -> dict[str, object]:
        return {"degrees": self.degrees, "unit": self.unit}


if __name__ == "__main__":
    unittest.main()
