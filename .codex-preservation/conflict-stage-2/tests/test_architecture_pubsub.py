from __future__ import annotations

import subprocess
import sys
import unittest
from dataclasses import dataclass
from os import getpid
from queue import Queue
from threading import Event, Thread, get_ident
from uuid import UUID

from manyfold.architecture import (
    CalibratedClock,
    Capacitor,
    Clock,
    ClockCalibrationSample,
    DataStreamProcessor,
    Ground,
    ManyFoldLock,
    ManyFoldLockLease,
    MonotonicLogicalClock,
    NtpTimeProvider,
    Probe,
    PubSub,
    PubSubCallbackSubscription,
    PubSubFabric,
    PubSubObservable,
    PubSubSchedule,
    PubSubTopic,
    Regulator,
    Relay,
    Resistor,
    ServiceDiscoveryRequirement,
    StreamRow,
    SystemTimeProvider,
    Via,
    WorkerEvent,
    WorkerHandle,
    WorkerRuntime,
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

    def test_stream_sql_store_indexes_common_operator_access_paths(self) -> None:
        processor = DataStreamProcessor()

        rows = processor.query(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'index'
              AND tbl_name = 'stream_messages'
              AND name NOT LIKE 'sqlite_autoindex%'
            ORDER BY name
            """
        )

        self.assertEqual(
            [row["name"] for row in rows],
            [
                "stream_messages_offset_idx",
                "stream_messages_topic_pad_event_sequence_idx",
                "stream_messages_topic_pad_offset_idx",
            ],
        )


class PubSubStreamTests(unittest.TestCase):
    def test_pubsub_schedules_current_thread_and_service_discovery_by_default(self) -> None:
        stream = PubSub(topic="heart.events")

        self.assertIsInstance(stream.schedule, PubSubSchedule)
        self.assertEqual(stream.schedule.worker, "pubsub:heart.events")
        self.assertEqual(stream.schedule.affinity, "current-process-thread")
        self.assertEqual(stream.schedule.process_id, getpid())
        self.assertEqual(stream.schedule.thread_id, get_ident())
        self.assertEqual(stream.schedule.address, f"thread://{getpid()}/{get_ident()}")
        self.assertIsInstance(
            stream.schedule.service_discovery,
            ServiceDiscoveryRequirement,
        )
        self.assertEqual(stream.schedule.service_discovery.service, "ServiceDiscovery")
        self.assertEqual(stream.schedule.service_discovery.affinity, "separate-process")
        self.assertEqual(
            stream.schedule.service_discovery.spawn_policy,
            "spawn_if_missing",
        )

    def test_pubsub_schedule_can_be_named_or_disabled(self) -> None:
        named = PubSub(topic="heart.events", worker_name="heart-pubsub")
        unscheduled = PubSub(topic="manual.pubsub", schedule=False)
        without_discovery = PubSub(topic="local.pubsub", service_discovery=False)

        self.assertEqual(named.schedule.worker, "heart-pubsub")
        self.assertIsNone(unscheduled.schedule)
        self.assertIsNone(without_discovery.schedule.service_discovery)

    def test_pubsub_topic_boots_shared_fabric_once_for_many_topics(self) -> None:
        temperature = PubSubTopic(
            "temperature",
            schema=Temperature,
            namespace="test-heart",
        )
        humidity = PubSubTopic(
            "humidity",
            schema=_Humidity,
            namespace="test-heart",
        )

        temperature.publish(Temperature(degrees=72.9, unit="F"))
        humidity.publish(_Humidity(percent=45.0))

        self.assertIs(temperature._runtime, humidity._runtime)
        self.assertEqual(temperature.schedule, humidity.schedule)
        self.assertEqual(temperature.latest().degrees, 72.9)
        self.assertEqual(humidity.latest().percent, 45.0)
        self.assertEqual(
            temperature.query_one(
                """
                SELECT percent
                FROM stream
                LIMIT 1
                """
            ),
            {"percent": None},
        )

    def test_pubsub_topic_reuses_existing_topic_and_schema(self) -> None:
        first = PubSubTopic(
            "input",
            schema=Temperature,
            namespace="test-schema-lock",
        )
        second = PubSubTopic(
            "input",
            schema=Temperature,
            namespace="test-schema-lock",
        )

        self.assertIs(first, second)
        with self.assertRaisesRegex(ValueError, "schema is already fixed"):
            PubSubTopic(
                "input",
                schema=_Humidity,
                namespace="test-schema-lock",
            )

    def test_pubsub_topic_defaults_to_default_namespace_ephemeral_name_and_no_schema(
        self,
    ) -> None:
        topic = PubSubTopic()

        self.assertEqual(topic.schedule.worker, "pubsub:default")
        self.assertIsNone(topic.schema)
        self.assertEqual(UUID(topic.topic).version, 5)
        topic.publish(b"payload")
        self.assertEqual(topic.latest().payload, b"payload")

    def test_pubsub_fabric_records_attached_topics(self) -> None:
        fabric = PubSubFabric(namespace="manual")

        fabric.topic("a")
        fabric.topic("b")

        self.assertEqual(fabric.topics, ("a", "b"))
        self.assertEqual(fabric.schedule.worker, "pubsub:manual")
        boot_lock = ManyFoldLock.for_resource("pubsub:manual:boot")
        self.assertEqual(fabric.boot_lock.name, boot_lock.name)
        self.assertEqual(fabric.boot_lock.path, boot_lock.path)

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

    def test_pubsub_sql_projection_is_bounded_by_retained_messages(self) -> None:
        temperature = PubSub(
            topic="sensor.environment.retained",
            schema=Temperature,
            retained_messages=2,
        )

        temperature.publish(Temperature(degrees=71.0, unit="F"))
        temperature.publish(Temperature(degrees=72.0, unit="F"))
        temperature.publish(Temperature(degrees=73.0, unit="F"))

        rows = temperature.query(
            """
            SELECT offset, degrees
            FROM stream
            ORDER BY offset
            """
        )

        self.assertEqual(
            rows,
            [
                {"offset": 1, "degrees": 72.0},
                {"offset": 2, "degrees": 73.0},
            ],
        )

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

    def test_pubsub_stream_subscribe_delivers_published_rows(self) -> None:
        stream = PubSub(
            topic="sensor.environment.temperature",
            schema=Temperature,
        )
        observed: list[float] = []

        subscription = stream.subscribe(lambda row: observed.append(row.degrees))

        self.assertIsInstance(subscription, PubSubCallbackSubscription)
        stream.publish(Temperature(degrees=72.4, unit="F"))
        stream.publish(Temperature(degrees=72.9, unit="F"))

        self.assertEqual(observed, [72.4, 72.9])

    def test_pubsub_stream_subscription_can_replay_latest_and_dispose(self) -> None:
        stream = PubSub(
            topic="sensor.environment.temperature",
            schema=Temperature,
        )
        observed: list[float] = []
        stream.publish(Temperature(degrees=72.4, unit="F"))

        subscription = stream.subscribe(
            lambda row: observed.append(row.degrees),
            replay_latest=True,
        )

        self.assertEqual(observed, [72.4])
        self.assertFalse(subscription.is_disposed)
        self.assertTrue(subscription.dispose())
        self.assertTrue(subscription.is_disposed)
        self.assertFalse(subscription.dispose())
        stream.publish(Temperature(degrees=72.9, unit="F"))
        self.assertEqual(observed, [72.4])

    def test_pubsub_stream_map_and_filter_can_drive_callbacks(self) -> None:
        stream = PubSub(
            topic="sensor.environment.temperature",
            schema=Temperature,
        )
        observed: list[str] = []

        values = stream.filter(lambda row: row.degrees >= 72.5).map(
            lambda row: f"{row.degrees:.1f}{row.unit}"
        )
        self.assertIsInstance(values, PubSubObservable)
        values.subscribe(observed.append)

        stream.publish(Temperature(degrees=72.4, unit="F"))
        stream.publish(Temperature(degrees=72.9, unit="F"))

        self.assertEqual(observed, ["72.9F"])

    def test_pubsub_live_stream_supports_initial_scan_tap_callback_and_pipe(self) -> None:
        stream = PubSub(
            topic="heart.values",
            schema=GamepadEvent,
        )
        observed: list[object] = []
        tapped: list[int] = []

        subscription = (
            stream.start_with(0)
            .pipe(
                lambda values: values.map(
                    lambda value: value if isinstance(value, int) else value.value
                ),
                lambda values: values.scan(lambda total, value: total + value, 0),
                lambda values: values.do_action(tapped.append),
            )
            .callback(observed.append)
        )

        stream.publish(
            GamepadEvent(
                source_id="gamepad.1",
                event_time=10,
                frame_index=1,
                delta_ms=16.0,
                value=2,
            ),
            event_time=10,
        )
        stream.publish(
            GamepadEvent(
                source_id="gamepad.1",
                event_time=20,
                frame_index=2,
                delta_ms=17.0,
                value=3,
            ),
            event_time=20,
        )

        self.assertFalse(subscription.is_disposed)
        self.assertEqual(observed, [0, 2, 5])
        self.assertEqual(tapped, [0, 2, 5])

    def test_pubsub_live_stream_accepts_heart_operator_compatibility_aliases(
        self,
    ) -> None:
        stream = PubSub(
            topic="heart.compat.values",
            schema=GamepadEvent,
        )
        observed: list[int] = []
        tapped: list[int] = []

        subscription = (
            stream.map(lambda row: row.value, name="value")
            .filter(lambda value: value >= 0, name="non-negative")
            .scan(lambda total, value: total + value, seed=10)
            .do_action(on_next=tapped.append)
            .distinct_until_changed()
            .pairwise()
            .take(2)
            .callback(lambda pair: observed.append(pair[1]), name="collect")
        )

        stream.publish(
            GamepadEvent(
                source_id="gamepad.1",
                event_time=10,
                frame_index=1,
                delta_ms=16.0,
                value=1,
            ),
            event_time=10,
        )
        stream.publish(
            GamepadEvent(
                source_id="gamepad.1",
                event_time=20,
                frame_index=2,
                delta_ms=16.0,
                value=2,
            ),
            event_time=20,
        )
        stream.publish(
            GamepadEvent(
                source_id="gamepad.1",
                event_time=30,
                frame_index=3,
                delta_ms=16.0,
                value=3,
            ),
            event_time=30,
        )

        self.assertTrue(subscription.is_disposed)
        self.assertEqual(tapped, [11, 13, 16])
        self.assertEqual(observed, [13, 16])

    def test_pubsub_subscribe_accepts_observer_and_on_next_callback(self) -> None:
        stream = PubSub(topic="heart.compat.subscribe", schema=Temperature)
        observed: list[float] = []

        class Observer:
            def on_next(self, row: object) -> None:
                observed.append(row.degrees)  # type: ignore[attr-defined]

        stream.subscribe(Observer())
        stream.map(lambda row: row.degrees).subscribe(on_next=observed.append)

        stream.publish(Temperature(degrees=72.4, unit="F"))

        self.assertEqual(observed, [72.4, 72.4])

    def test_pubsub_live_stream_with_latest_from_pairs_source_with_latest_other(
        self,
    ) -> None:
        frames = PubSub(topic="heart.frames", schema=FrameTick)
        acceleration = PubSub(topic="heart.acceleration", schema=Acceleration)
        observed: list[tuple[int, float]] = []

        frames.with_latest_from(acceleration).subscribe(
            lambda pair: observed.append((pair[0].frame_index, pair[1].x))
        )

        frames.publish(FrameTick(event_time=10, frame_index=1, delta_ms=16.0), event_time=10)
        acceleration.publish(Acceleration(event_time=15, x=1.0, y=2.0, z=3.0), event_time=15)
        frames.publish(FrameTick(event_time=20, frame_index=2, delta_ms=17.0), event_time=20)
        acceleration.publish(Acceleration(event_time=25, x=4.0, y=5.0, z=6.0), event_time=25)
        frames.publish(FrameTick(event_time=30, frame_index=3, delta_ms=18.0), event_time=30)

        self.assertEqual(observed, [(2, 1.0), (3, 4.0)])

    def test_pubsub_live_stream_with_latest_from_accepts_multiple_sources(
        self,
    ) -> None:
        frames = PubSub(topic="heart.frames.multi", schema=FrameTick)
        acceleration = PubSub(topic="heart.acceleration.multi", schema=Acceleration)
        temperature = PubSub(topic="heart.temperature.multi", schema=Temperature)
        observed: list[tuple[int, float, float]] = []

        frames.with_latest_from(acceleration, temperature).subscribe(
            lambda latest: observed.append(
                (latest[0].frame_index, latest[1].x, latest[2].degrees)
            )
        )

        acceleration.publish(Acceleration(event_time=10, x=1.0, y=2.0, z=3.0), event_time=10)
        frames.publish(FrameTick(event_time=20, frame_index=1, delta_ms=16.0), event_time=20)
        temperature.publish(Temperature(degrees=72.4, unit="F"), event_time=30)
        frames.publish(FrameTick(event_time=40, frame_index=2, delta_ms=17.0), event_time=40)

        self.assertEqual(observed, [(2, 1.0, 72.4)])

    def test_pubsub_live_stream_flat_map_expands_inner_streams(self) -> None:
        selections = PubSub(topic="heart.selection", schema=StreamSelection)
        left = PubSub(topic="heart.left", schema=Temperature)
        right = PubSub(topic="heart.right", schema=Temperature)
        streams = {"left": left, "right": right}
        observed: list[float] = []

        left.publish(Temperature(degrees=70.0, unit="F"))
        right.publish(Temperature(degrees=80.0, unit="F"))
        selections.flat_map(lambda row: streams[row.target]).subscribe(
            lambda row: observed.append(row.degrees)
        )

        selections.publish(StreamSelection(target="left"))
        selections.publish(StreamSelection(target="right"))

        self.assertEqual(observed, [70.0, 80.0])

    def test_pubsub_live_stream_switch_latest_uses_only_latest_inner_stream(self) -> None:
        selections = PubSub(topic="heart.switch", schema=StreamSelection)
        left = PubSub(topic="heart.switch.left", schema=Temperature)
        right = PubSub(topic="heart.switch.right", schema=Temperature)
        streams = {"left": left, "right": right}
        observed: list[float] = []

        selections.switch_latest(lambda row: streams[row.target]).subscribe(
            lambda row: observed.append(row.degrees)
        )

        selections.publish(StreamSelection(target="left"))
        left.publish(Temperature(degrees=70.0, unit="F"))
        selections.publish(StreamSelection(target="right"))
        left.publish(Temperature(degrees=71.0, unit="F"))
        right.publish(Temperature(degrees=80.0, unit="F"))

        self.assertEqual(observed, [70.0, 80.0])

    def test_pubsub_sql_helpers_cover_common_stream_operations(self) -> None:
        stream = PubSub(
            topic="heart.input",
            schema=GamepadEvent,
        )

        stream.publish(
            GamepadEvent(
                source_id="gamepad.1",
                event_time=10,
                frame_index=1,
                delta_ms=16.0,
                value=0,
            ),
            event_time=10,
        )
        stream.publish(
            GamepadEvent(
                source_id="gamepad.2",
                event_time=20,
                frame_index=2,
                delta_ms=17.0,
                value=1,
            ),
            event_time=20,
        )
        stream.publish(
            GamepadEvent(
                source_id="gamepad.1",
                event_time=30,
                frame_index=3,
                delta_ms=16.5,
                value=1,
            ),
            event_time=30,
        )
        stream.publish(
            GamepadEvent(
                source_id="gamepad.1",
                event_time=40,
                frame_index=4,
                delta_ms=17.5,
                value=2,
            ),
            event_time=40,
        )

        self.assertEqual(
            [row.frame_index for row in stream.where(source_id="gamepad.1")],
            [1, 3, 4],
        )
        self.assertEqual(
            stream.project("event_time", "frame_index", "delta_ms"),
            [
                {"event_time": 10, "frame_index": 1, "delta_ms": 16.0},
                {"event_time": 20, "frame_index": 2, "delta_ms": 17.0},
                {"event_time": 30, "frame_index": 3, "delta_ms": 16.5},
                {"event_time": 40, "frame_index": 4, "delta_ms": 17.5},
            ],
        )
        self.assertEqual(stream.take()[0].frame_index, 1)
        self.assertEqual(
            [row.frame_index for row in stream.distinct_until_changed("value")],
            [1, 2, 4],
        )
        self.assertEqual(
            stream.pairwise("value"),
            [
                {"previous_value": 0, "current_value": 1},
                {"previous_value": 1, "current_value": 1},
                {"previous_value": 1, "current_value": 2},
            ],
        )

    def test_pubsub_sql_helpers_reject_invalid_inputs(self) -> None:
        stream = PubSub(topic="raw")

        with self.assertRaisesRegex(ValueError, "where requires"):
            stream.where()
        with self.assertRaisesRegex(ValueError, "project requires"):
            stream.project()
        with self.assertRaisesRegex(ValueError, "count must be positive"):
            stream.take(0)
        with self.assertRaisesRegex(ValueError, "field must be a SQL identifier"):
            stream.project("not valid")

    def test_pubsub_latest_join_matches_each_row_to_latest_prior_topic_row(self) -> None:
        fabric = PubSubFabric(namespace="test-latest-join")
        frames = fabric.topic("frame_ticks", schema=FrameTick)
        acceleration = fabric.topic("acceleration", schema=Acceleration)

        acceleration.publish(Acceleration(event_time=5, x=1.0, y=2.0, z=3.0), event_time=5)
        frames.publish(FrameTick(event_time=10, frame_index=1, delta_ms=16.0), event_time=10)
        acceleration.publish(Acceleration(event_time=15, x=4.0, y=5.0, z=6.0), event_time=15)
        frames.publish(FrameTick(event_time=20, frame_index=2, delta_ms=17.0), event_time=20)

        rows = frames.latest_join(acceleration, "x", "y", "z", prefix="latest_accel")

        self.assertEqual(
            [
                (
                    row.frame_index,
                    row.latest_accel_x,
                    row.latest_accel_y,
                    row.latest_accel_z,
                )
                for row in rows
            ],
            [
                (1, 1.0, 2.0, 3.0),
                (2, 4.0, 5.0, 6.0),
            ],
        )

    def test_pubsub_recursive_sum_accumulates_numeric_field_by_offset(self) -> None:
        frames = PubSub(topic="frame_ticks", schema=FrameTick)
        frames.publish(FrameTick(event_time=10, frame_index=1, delta_ms=16.0), event_time=10)
        frames.publish(FrameTick(event_time=20, frame_index=2, delta_ms=17.0), event_time=20)
        frames.publish(FrameTick(event_time=30, frame_index=3, delta_ms=18.0), event_time=30)

        self.assertEqual(
            frames.recursive_sum("delta_ms"),
            [
                {"seq": 0, "elapsed_ms": 0.0},
                {"seq": 1, "elapsed_ms": 17.0},
                {"seq": 2, "elapsed_ms": 35.0},
            ],
        )

    def test_pubsub_query_composes_recursive_ctes_with_scoped_stream(self) -> None:
        frames = PubSub(topic="frame_ticks", schema=FrameTick)
        frames.publish(FrameTick(event_time=10, frame_index=1, delta_ms=16.0), event_time=10)
        frames.publish(FrameTick(event_time=20, frame_index=2, delta_ms=17.0), event_time=20)

        rows = frames.query(
            """
            WITH RECURSIVE state(seq, elapsed_ms) AS (
              SELECT 0, 0.0
              UNION ALL
              SELECT frame.offset, state.elapsed_ms + frame.delta_ms
              FROM state
              JOIN stream frame ON frame.offset = state.seq + 1
            )
            SELECT *
            FROM state
            """
        )

        self.assertEqual(
            rows,
            [
                {"seq": 0, "elapsed_ms": 0.0},
                {"seq": 1, "elapsed_ms": 17.0},
            ],
        )

    def test_pubsub_stream_encodes_optional_scalar_fields_as_sql_nulls(self) -> None:
        stream = PubSub(
            topic="sensor.environment.temperature",
            schema=OptionalTemperature,
        )

        stream.publish(OptionalTemperature(degrees=None, unit="F"))
        stream.publish(OptionalTemperature(degrees=72.9, unit="F"))

        rows = stream.query(
            """
            SELECT degrees, unit
            FROM stream
            ORDER BY offset
            """
        )

        self.assertEqual(
            rows,
            [
                {"degrees": None, "unit": "F"},
                {"degrees": 72.9, "unit": "F"},
            ],
        )
        self.assertEqual(
            rows[0].as_model(OptionalTemperature),
            OptionalTemperature(degrees=None, unit="F"),
        )


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


class ManyFoldLockTests(unittest.TestCase):
    def test_lock_registry_returns_single_canonical_lock(self) -> None:
        left = ManyFoldLock.for_resource("pubsub:heart:boot")
        right = ManyFoldLock.for_resource(" pubsub:heart:boot ")

        self.assertEqual(left.name, "pubsub:heart:boot")
        self.assertEqual(right.name, "pubsub:heart:boot")
        self.assertEqual(left.path, right.path)

    def test_lock_take_returns_releasable_lease(self) -> None:
        lock = ManyFoldLock.for_resource("test:lease")

        with lock.take(owner="owner") as lease:
            self.assertIsInstance(lease, ManyFoldLockLease)
            self.assertEqual(lease.lock_name, lock.name)
            self.assertEqual(lease.owner, "owner")
            self.assertFalse(lease.is_released)
            with self.assertRaisesRegex(RuntimeError, "already held"):
                lock.take(blocking=False)

        self.assertTrue(lease.is_released)
        with lock.take(blocking=False) as next_lease:
            self.assertFalse(next_lease.is_released)

    def test_lock_take_excludes_other_processes(self) -> None:
        lock = ManyFoldLock.for_resource("test:cross-process")
        probe = (
            "from manyfold.architecture import ManyFoldLock\n"
            "try:\n"
            "    lease = ManyFoldLock.for_resource('test:cross-process').take(blocking=False)\n"
            "except RuntimeError:\n"
            "    print('blocked')\n"
            "else:\n"
            "    lease.release()\n"
            "    print('acquired')\n"
        )

        with lock.take(owner="parent"):
            blocked = subprocess.run(
                [sys.executable, "-c", probe],
                check=True,
                capture_output=True,
                text=True,
            )

        acquired = subprocess.run(
            [sys.executable, "-c", probe],
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(blocked.stdout.strip(), "blocked")
        self.assertEqual(acquired.stdout.strip(), "acquired")

    def test_lock_is_released_when_holding_process_exits(self) -> None:
        holder = (
            "import os, sys, time\n"
            "from manyfold.architecture import ManyFoldLock\n"
            "lease = ManyFoldLock.for_resource('test:process-death').take(owner='holder')\n"
            "print('ready', flush=True)\n"
            "time.sleep(60)\n"
        )
        probe = (
            "from manyfold.architecture import ManyFoldLock\n"
            "try:\n"
            "    lease = ManyFoldLock.for_resource('test:process-death').take(blocking=False)\n"
            "except RuntimeError:\n"
            "    print('blocked')\n"
            "else:\n"
            "    lease.release()\n"
            "    print('acquired')\n"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", holder],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            self.assertEqual(process.stdout.readline().strip(), "ready")
            blocked = subprocess.run(
                [sys.executable, "-c", probe],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(blocked.stdout.strip(), "blocked")
        finally:
            process.kill()
            process.communicate(timeout=5)

        acquired = subprocess.run(
            [sys.executable, "-c", probe],
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(acquired.stdout.strip(), "acquired")

    def test_lock_lease_release_is_idempotent(self) -> None:
        lock = ManyFoldLock.for_resource("test:idempotent-release")
        lease = lock.take()

        self.assertTrue(lease.release())
        self.assertFalse(lease.release())


class WorkerRuntimeTests(unittest.TestCase):
    def test_worker_runtime_attaches_current_thread_to_pubsub(self) -> None:
        pubsub = PubSub(topic="manyfold.workers.test", schema=WorkerEvent)
        runtime = WorkerRuntime(pubsub=pubsub)

        worker = runtime.attach_current_thread(
            "heart-runtime",
            roles=("pubsub", "heart"),
            capacity=3,
        )
        worker.heartbeat(active_jobs=1)

        self.assertIsInstance(worker, WorkerHandle)
        self.assertEqual(worker.worker.name, "heart-runtime")
        self.assertEqual(worker.worker.roles, ("pubsub", "heart"))
        self.assertEqual(worker.worker.address, f"thread://{worker.worker.process_id}/{worker.worker.thread_id}")
        rows = pubsub.query(
            """
            SELECT worker, event, capacity, active_jobs, available_jobs, roles
            FROM stream
            ORDER BY offset
            """
        )
        self.assertEqual(
            rows,
            [
                {
                    "worker": "heart-runtime",
                    "event": "attached",
                    "capacity": 3,
                    "active_jobs": 0,
                    "available_jobs": 3,
                    "roles": "pubsub,heart",
                },
                {
                    "worker": "heart-runtime",
                    "event": "heartbeat",
                    "capacity": 3,
                    "active_jobs": 1,
                    "available_jobs": 2,
                    "roles": "pubsub,heart",
                },
            ],
        )

    def test_worker_runtime_models_each_thread_as_a_distinct_worker(self) -> None:
        pubsub = PubSub(topic="manyfold.workers.threaded", schema=WorkerEvent)
        runtime = WorkerRuntime(pubsub=pubsub)
        worker_refs: Queue[tuple[str, int]] = Queue()
        release_workers = Event()

        def attach(name: str) -> None:
            handle = runtime.attach_current_thread(name)
            worker_refs.put((handle.worker.name, handle.worker.thread_id))
            release_workers.wait(timeout=5)

        left = Thread(target=attach, args=("left",))
        right = Thread(target=attach, args=("right",))
        left.start()
        right.start()

        workers = {worker_refs.get(), worker_refs.get()}
        release_workers.set()
        left.join()
        right.join()
        self.assertEqual({name for name, _ in workers}, {"left", "right"})
        self.assertEqual(len({thread_id for _, thread_id in workers}), 2)
        self.assertEqual(pubsub.query_one("SELECT COUNT(*) AS count FROM stream").count, 2)

    def test_worker_handle_detaches_once(self) -> None:
        pubsub = PubSub(topic="manyfold.workers.detach", schema=WorkerEvent)
        worker = WorkerRuntime(pubsub=pubsub).attach_current_thread("detachable")

        self.assertTrue(worker.detach())
        self.assertTrue(worker.is_detached)
        self.assertFalse(worker.detach())
        with self.assertRaisesRegex(RuntimeError, "detached workers"):
            worker.heartbeat()
        self.assertEqual(
            [
                row.event
                for row in pubsub.query(
                    """
                    SELECT event
                    FROM stream
                    ORDER BY offset
                    """
                )
            ],
            ["attached", "detached"],
        )


@dataclass(frozen=True)
class Temperature:
    degrees: float
    unit: str


@dataclass(frozen=True)
class OptionalTemperature:
    degrees: float | None
    unit: str


@dataclass(frozen=True)
class GamepadEvent:
    source_id: str
    event_time: int
    frame_index: int
    delta_ms: float
    value: int


@dataclass(frozen=True)
class FrameTick:
    event_time: int
    frame_index: int
    delta_ms: float


@dataclass(frozen=True)
class Acceleration:
    event_time: int
    x: float
    y: float
    z: float


@dataclass(frozen=True)
class StreamSelection:
    target: str


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
