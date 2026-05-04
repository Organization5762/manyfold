from __future__ import annotations

import json
import tempfile
import threading
import unittest
from dataclasses import dataclass
from pathlib import Path

import reactivex as rx

from tests.test_support import load_manyfold_package


def _int_schema(manyfold, schema_id: str = "Int"):
    return manyfold.Schema(
        schema_id=schema_id,
        version=1,
        encode=lambda value: str(value).encode("ascii"),
        decode=lambda payload: int(payload.decode("ascii")),
    )


def _exception_schema(manyfold, schema_id: str = "Exception"):
    def encode(value: BaseException) -> bytes:
        return f"{type(value).__name__}:{value}".encode("utf-8")

    def decode(payload: bytes) -> BaseException:
        return RuntimeError(payload.decode("utf-8"))

    return manyfold.Schema(schema_id=schema_id, version=1, encode=encode, decode=decode)


def _route(manyfold, stream: str, schema):
    return manyfold.route(
        plane=manyfold.Plane.Read,
        layer=manyfold.Layer.Logical,
        owner=manyfold.OwnerName("sensor"),
        family=manyfold.StreamFamily("io"),
        stream=manyfold.StreamName(stream),
        variant=manyfold.Variant.Meta,
        schema=schema,
    )


class SensorIoTests(unittest.TestCase):
    def test_manual_clock_produces_deterministic_timestamps(self) -> None:
        manyfold = load_manyfold_package()
        clock = manyfold.ManualClock(10.0)

        self.assertEqual(clock.now(), 10.0)
        self.assertEqual(clock.advance(2.5), 12.5)
        clock.set(20)
        self.assertEqual(clock.now(), 20.0)

    def test_bounded_ring_buffer_enforces_overflow_policy(self) -> None:
        manyfold = load_manyfold_package()
        buffer = manyfold.BoundedRingBuffer[int](capacity=2, overflow="drop_oldest")

        self.assertTrue(buffer.push(1))
        self.assertTrue(buffer.push(2))
        self.assertTrue(buffer.push(3))

        self.assertEqual(tuple(buffer), (2, 3))
        self.assertEqual(buffer.dropped, 1)

    def test_bounded_ring_buffer_can_reject_when_full(self) -> None:
        manyfold = load_manyfold_package()
        buffer = manyfold.BoundedRingBuffer[int](capacity=1, overflow="reject")

        self.assertTrue(buffer.push(1))
        self.assertFalse(buffer.push(2))

        self.assertEqual(tuple(buffer), (1,))
        self.assertEqual(buffer.rejected, 1)

    def test_bounded_ring_buffer_latest_counts_all_replaced_items(self) -> None:
        manyfold = load_manyfold_package()
        buffer = manyfold.BoundedRingBuffer[int](capacity=3, overflow="latest")

        self.assertTrue(buffer.push(1))
        self.assertTrue(buffer.push(2))
        self.assertTrue(buffer.push(3))
        self.assertTrue(buffer.push(4))

        self.assertEqual(tuple(buffer), (4,))
        self.assertEqual(buffer.dropped, 3)

    def test_delimited_message_buffer_reassembles_bytes_and_text(self) -> None:
        manyfold = load_manyfold_package()
        byte_buffer = manyfold.DelimitedMessageBuffer()
        text_buffer = manyfold.DelimitedMessageBuffer(mode="text")

        self.assertEqual(byte_buffer.append(b'{"a": 1}\n{"b": '), (b'{"a": 1}',))
        self.assertEqual(byte_buffer.buffer_size, len(b'{"b": '))
        self.assertEqual(byte_buffer.append(b"2}\n"), (b'{"b": 2}',))
        self.assertEqual(text_buffer.append('{"ok":'), ())
        self.assertEqual(text_buffer.append(" true}\n"), ('{"ok": true}',))

    def test_delimited_message_buffer_preserves_split_utf8_text_sequences(self) -> None:
        manyfold = load_manyfold_package()
        text_buffer = manyfold.DelimitedMessageBuffer(mode="text")

        self.assertEqual(text_buffer.append(b"caf\xc3"), ())
        self.assertEqual(text_buffer.append(b"\xa9\n"), ("café",))

    def test_delimited_message_buffer_rejects_invalid_utf8_text(self) -> None:
        manyfold = load_manyfold_package()
        text_buffer = manyfold.DelimitedMessageBuffer(mode="text")

        with self.assertRaises(UnicodeDecodeError):
            text_buffer.append(b"ok\xff\n")

    def test_delimited_message_buffer_handles_multi_byte_delimiters(self) -> None:
        manyfold = load_manyfold_package()
        byte_buffer = manyfold.DelimitedMessageBuffer(delimiter=b"\r\n")
        text_buffer = manyfold.DelimitedMessageBuffer(delimiter=b"::", mode="text")

        self.assertEqual(byte_buffer.append(b"one\r\ntwo"), (b"one",))
        self.assertEqual(byte_buffer.append(b"\r\n"), (b"two",))
        self.assertEqual(text_buffer.append("alpha::beta"), ("alpha",))
        self.assertEqual(text_buffer.append("::"), ("beta",))

    def test_delimited_message_buffer_rejects_empty_delimiter(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "delimiter must not be empty"):
            manyfold.DelimitedMessageBuffer(delimiter=b"")

    def test_delimited_message_buffer_rejects_unknown_mode(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "mode must be 'bytes' or 'text'"):
            manyfold.DelimitedMessageBuffer(mode="lines")  # type: ignore[arg-type]

    def test_delimited_message_buffer_rejects_invalid_text_delimiter(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "text delimiter must be valid UTF-8"):
            manyfold.DelimitedMessageBuffer(delimiter=b"\xff", mode="text")

    def test_json_event_decoder_returns_sensor_events(self) -> None:
        manyfold = load_manyfold_package()
        clock = manyfold.ManualClock(5.0)
        decoder = manyfold.JsonEventDecoder(
            clock=clock,
            identity=manyfold.SensorIdentity(id="imu-1"),
            group="ambient",
        )

        event = decoder.decode(
            b'{"event_type":"sensor.acceleration","data":{"x":1,"y":2,"z":3},"quality":"ok"}'
        )

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.event_type, "sensor.acceleration")
        self.assertEqual(event.data, {"x": 1, "y": 2, "z": 3})
        self.assertEqual(event.observed_at, 5.0)
        self.assertEqual(event.sequence_number, 1)
        self.assertEqual(event.identity.id, "imu-1")
        self.assertEqual(event.identity.group, "ambient")
        self.assertEqual(event.metadata, {"quality": "ok"})

    def test_json_event_decoder_rejects_invalid_utf8(self) -> None:
        manyfold = load_manyfold_package()
        decoder = manyfold.JsonEventDecoder()

        event = decoder.decode(b'{"event_type":"sensor.note","data":"ok"}\xff')

        self.assertIsNone(event)
        self.assertEqual(decoder.sequence.peek(), 0)

    def test_sensor_event_schema_round_trips_identity_and_raw_bytes(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        event = manyfold.SensorEvent(
            event_type="radio.packet",
            data={"payload": b"abc"},
            observed_at=1.5,
            identity=manyfold.SensorIdentity(
                id="radio-1",
                tags=(manyfold.SensorTag("input_variant", "radio"),),
                group="bridge",
            ),
            sequence_number=7,
            raw=b'{"payload":[97,98,99]}',
            metadata={"crc_ok": True},
        )

        decoded = schema.decode(schema.encode(event))

        self.assertEqual(decoded.event_type, "radio.packet")
        self.assertEqual(decoded.data, {"payload": b"abc"})
        self.assertEqual(decoded.identity.id, "radio-1")
        self.assertEqual(decoded.identity.tags[0].variant, "radio")
        self.assertEqual(decoded.raw, b'{"payload":[97,98,99]}')

    def test_sensor_schemas_encode_compact_sorted_json(self) -> None:
        manyfold = load_manyfold_package()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        health_schema = manyfold.health_status_schema()

        sample = manyfold.SensorSample(
            value=21,
            source_timestamp=1.0,
            ingest_timestamp=2.0,
            sequence_number=3,
            quality=None,
            status="ok",
        )
        status = manyfold.HealthStatus(
            status="ok",
            observed_at=2.0,
            message="ready",
        )

        self.assertEqual(
            sample_schema.encode(sample),
            b'{"ingest_timestamp":2.0,"quality":null,"sequence_number":3,'
            b'"source_timestamp":1.0,"status":"ok","value":"MjE="}',
        )
        self.assertEqual(
            health_schema.encode(status),
            b'{"error_count":0,"message":"ready","observed_at":2.0,'
            b'"stale":false,"status":"ok"}',
        )

    def test_sensor_sample_schema_rejects_invalid_value_base64(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        payload = {
            "value": "not base64!",
            "source_timestamp": 1.0,
            "ingest_timestamp": 1.5,
            "sequence_number": 1,
            "quality": None,
            "status": None,
        }

        with self.assertRaisesRegex(ValueError, "value must be valid base64"):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_invalid_nested_bytes_base64(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        payload = {
            "event_type": "radio.packet",
            "data": {"payload": {"__bytes_b64__": "not base64!"}},
            "observed_at": 1.5,
            "identity": {},
            "sequence_number": 7,
            "raw": "also not base64!",
            "metadata": {},
        }

        with self.assertRaisesRegex(ValueError, "__bytes_b64__ must be valid base64"):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_invalid_raw_base64(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        payload = {
            "event_type": "radio.packet",
            "data": {},
            "observed_at": 1.5,
            "identity": {},
            "sequence_number": 7,
            "raw": "not base64!",
            "metadata": {},
        }

        with self.assertRaisesRegex(ValueError, "raw must be valid base64"):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sequence_counter_assigns_monotonic_numbers(self) -> None:
        manyfold = load_manyfold_package()
        sequence = manyfold.SequenceCounter(current=40, group="ambient")

        self.assertEqual(sequence.next(), 41)
        self.assertEqual(sequence.next(), 42)
        self.assertEqual(sequence.peek(), 42)
        sequence.reset(100)
        self.assertEqual(sequence.next(), 101)
        self.assertEqual(sequence.group, "ambient")

    def test_retry_loop_retries_transient_failures(self) -> None:
        manyfold = load_manyfold_package()
        attempts = 0

        def read() -> int:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError("not ready")
            return 42

        loop = manyfold.RetryLoop(
            retry=manyfold.SensorRetryPolicy(max_attempts=3),
            backoff=manyfold.SensorBackoffPolicy.none(),
        )

        self.assertEqual(loop.run(read), 42)
        self.assertEqual(attempts, 3)

    def test_retry_loop_stops_at_max_attempts(self) -> None:
        manyfold = load_manyfold_package()
        attempts = 0

        def read() -> int:
            nonlocal attempts
            attempts += 1
            raise RuntimeError("still failing")

        loop = manyfold.RetryLoop(
            retry=manyfold.SensorRetryPolicy(max_attempts=2),
            backoff=manyfold.SensorBackoffPolicy.none(),
        )

        with self.assertRaisesRegex(RuntimeError, "still failing"):
            loop.run(read)
        self.assertEqual(attempts, 2)

    def test_managed_run_loop_retries_until_stopped(self) -> None:
        manyfold = load_manyfold_package()
        attempts = 0
        errors: list[tuple[str, int]] = []
        sleeps: list[float] = []

        def body(stop: manyfold.StopToken) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("not ready")
            stop.set()

        loop = manyfold.ManagedRunLoop(
            body=body,
            retry=manyfold.SensorRetryPolicy(max_attempts=3),
            backoff=manyfold.SensorBackoffPolicy.fixed(0.5),
            on_error=lambda exc, count: errors.append((str(exc), count)),
        )
        stop = manyfold.StopToken()
        original_wait = stop.wait

        def wait(delay: float | None = None) -> bool:
            assert delay is not None
            sleeps.append(delay)
            return original_wait(0)

        stop.wait = wait  # type: ignore[method-assign]

        loop.run(stop)

        self.assertEqual(attempts, 2)
        self.assertEqual(errors, [("not ready", 1)])
        self.assertEqual(sleeps, [0.5])
        self.assertTrue(stop.is_set())

    def test_managed_run_loop_dispose_stops_thread(self) -> None:
        manyfold = load_manyfold_package()
        entered = threading.Event()

        def body(stop: manyfold.StopToken) -> None:
            entered.set()
            stop.wait(10)

        handle = manyfold.ManagedRunLoop(body=body).start_thread(
            name="manyfold-test-managed-loop"
        )
        try:
            self.assertTrue(entered.wait(1))
        finally:
            handle.dispose(timeout=1)

        self.assertTrue(handle.disposed)
        self.assertFalse(handle.thread.is_alive())

    def test_managed_graph_node_publishes_exceptions_to_error_route(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        output = _route(manyfold, "node_output", _int_schema(manyfold, "NodeOutput"))
        errors = _route(manyfold, "node_errors", _exception_schema(manyfold))
        attempts = 0

        def body(stop: manyfold.StopToken, graph: manyfold.Graph) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ValueError("not ready")
            graph.publish(output, 42)
            stop.set()

        handle = manyfold.ManagedGraphNode(
            name="sensor-node",
            body=body,
            output_routes=(output,),
            error_route=errors,
            retry=manyfold.SensorRetryPolicy(max_attempts=2),
            backoff=manyfold.SensorBackoffPolicy.none(),
            start_immediately=False,
        ).install(graph)

        handle.loop_handle.loop.run(handle.loop_handle.token)

        latest_error = graph.latest(errors)
        latest_output = graph.latest(output)
        self.assertIsNotNone(latest_error)
        self.assertIsNotNone(latest_output)
        assert latest_error is not None
        assert latest_output is not None
        self.assertIsInstance(latest_error.value, RuntimeError)
        self.assertIn("ValueError:not ready", str(latest_error.value))
        self.assertEqual(latest_output.value, 42)

    def test_managed_graph_node_observes_control_route(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        control = _route(manyfold, "node_control", _int_schema(manyfold, "Control"))
        received: list[int] = []

        handle = manyfold.ManagedGraphNode(
            name="controlled-node",
            body=lambda stop, _graph: stop.set(),
            control_route=control,
            on_control=lambda item, _graph: received.append(item.value),
            start_immediately=False,
        ).install(graph)

        graph.publish(control, 7)
        handle.dispose(timeout=0)

        self.assertEqual(received, [7])

    def test_managed_graph_node_appears_in_diagram(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        output = _route(manyfold, "diagram_output", _int_schema(manyfold, "Output"))
        errors = _route(manyfold, "diagram_errors", _exception_schema(manyfold))

        manyfold.ManagedGraphNode(
            name="diagram-source",
            body=lambda stop, _graph: stop.set(),
            output_routes=(output,),
            error_route=errors,
            group="sensor",
            start_immediately=False,
        ).install(graph)

        diagram = graph.diagram(group_by=("layer", "owner"))

        self.assertIn('["node / sensor"]', diagram)
        self.assertIn('["diagram-source"]', diagram)
        self.assertIn('["io.diagram_output<br/>meta"]', diagram)
        self.assertIn('["io.diagram_errors<br/>meta"]', diagram)
        self.assertEqual(len(list(graph.diagram_nodes())), 1)

    def test_detection_node_publishes_each_detected_item(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        detected = _route(manyfold, "detected_items", _int_schema(manyfold, "Detected"))
        callbacks: list[int] = []

        handle = manyfold.DetectionNode(
            name="detect-integers",
            detector=lambda: iter([1, 2, 3]),
            output_route=detected,
            mapper=lambda item: item * 10,
            on_detect=lambda item, _access: callbacks.append(item),
            start_immediately=False,
        ).install(graph)

        handle.loop_handle.loop.run(handle.loop_handle.token)

        latest = graph.latest(detected)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, 30)
        self.assertEqual(callbacks, [1, 2, 3])

    def test_detection_node_emits_nothing_when_nothing_is_detected(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        detected = _route(
            manyfold,
            "empty_detection_items",
            _int_schema(manyfold, "EmptyDetected"),
        )

        handle = manyfold.DetectionNode(
            name="detect-none",
            detector=lambda: iter(()),
            output_route=detected,
            start_immediately=False,
        ).install(graph)

        handle.loop_handle.loop.run(handle.loop_handle.token)

        self.assertIsNone(graph.latest(detected))

    def test_detection_node_can_spawn_downstream_sources(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        detected = _route(
            manyfold,
            "spawn_detection_items",
            _int_schema(manyfold, "SpawnDetected"),
        )
        downstream = _route(
            manyfold,
            "spawn_downstream_items",
            _int_schema(manyfold, "SpawnDownstream"),
        )

        def spawn(item: int, access: manyfold.GraphAccessNode):
            access.install(
                manyfold.ManagedGraphNode(
                    name=f"source-{item}",
                    body=lambda stop, graph: (
                        graph.publish(downstream, item * 100),
                        stop.set(),
                    ),
                    output_routes=(downstream,),
                    start_immediately=False,
                )
            )
            return None

        handle = manyfold.DetectionNode(
            name="detect-and-spawn",
            detector=lambda: iter([4]),
            output_route=detected,
            spawn=spawn,
            start_immediately=False,
        ).install(graph)

        handle.loop_handle.loop.run(handle.loop_handle.token)
        self.assertEqual(len(handle.spawned_handles), 1)
        spawned = handle.spawned_handles[0]
        spawned.loop_handle.loop.run(spawned.loop_handle.token)

        latest_detection = graph.latest(detected)
        latest_downstream = graph.latest(downstream)
        self.assertIsNotNone(latest_detection)
        self.assertIsNotNone(latest_downstream)
        assert latest_detection is not None
        assert latest_downstream is not None
        self.assertEqual(latest_detection.value, 4)
        self.assertEqual(latest_downstream.value, 400)

    def test_change_and_threshold_filters_reduce_noise(self) -> None:
        manyfold = load_manyfold_package()
        distinct = manyfold.ChangeFilter[int]()
        threshold = manyfold.ThresholdFilter[tuple[float, float, float]](threshold=0.1)

        self.assertEqual(
            [distinct.accepts(value) for value in (1, 1, 2, 2, 3)],
            [True, False, True, False, True],
        )
        self.assertEqual(
            [
                threshold.accepts(value)
                for value in (
                    (0.0, 0.0, 0.0),
                    (0.05, 0.0, 0.0),
                    (0.2, 0.0, 0.0),
                )
            ],
            [True, False, True],
        )

    def test_threshold_filter_accepts_list_and_tuple_shape_changes(self) -> None:
        manyfold = load_manyfold_package()
        tuple_threshold = manyfold.ThresholdFilter[tuple[float, ...]](threshold=0.5)
        list_threshold = manyfold.ThresholdFilter[list[float]](threshold=0.5)

        self.assertTrue(tuple_threshold.accepts((1.0,)))
        self.assertTrue(tuple_threshold.accepts((1.0, 0.0)))
        self.assertTrue(list_threshold.accepts([1.0, 2.0]))
        self.assertTrue(list_threshold.accepts([1.0]))

    def test_local_sensor_source_publishes_sensor_samples(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        clock = manyfold.ManualClock(100.0)
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        route = _route(manyfold, "temperature_sample", sample_schema)
        source = manyfold.LocalSensorSource(
            route=route,
            read=lambda: 21,
            clock=clock,
            retry=manyfold.SensorRetryPolicy(max_attempts=1),
        )

        sample = source.install(graph).poll()
        latest = graph.latest(route)

        self.assertIsNotNone(sample)
        self.assertIsNotNone(latest)
        assert sample is not None
        assert latest is not None
        self.assertEqual(sample.sequence_number, 2)
        self.assertEqual(latest.value.value, 21)
        self.assertEqual(latest.value.source_timestamp, 100.0)
        self.assertEqual(latest.value.sequence_number, 2)

    def test_local_sensor_source_uses_sequence_component_and_group(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        route = _route(manyfold, "grouped_temperature_sample", sample_schema)
        source = manyfold.LocalSensorSource(
            route=route,
            read=lambda: 25,
            clock=manyfold.ManualClock(1.0),
            sequence=manyfold.SequenceCounter(current=10),
            group="ambient",
        )

        handle = source.install(graph, read_now=False)
        sample = handle.poll()

        self.assertIsNotNone(sample)
        assert sample is not None
        self.assertEqual(sample.sequence_number, 11)
        self.assertEqual(source.sequence.group, "ambient")
        self.assertEqual(source.clock.group, "ambient")
        self.assertEqual(handle.group, "ambient")

    def test_reactive_sensor_source_publishes_observable_samples(self) -> None:
        manyfold = load_manyfold_package()

        graph = manyfold.Graph()
        clock = manyfold.ManualClock(2.0)
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Accel"))
        route = _route(manyfold, "reactive_sample", sample_schema)
        tap = manyfold.SensorDebugTap(clock=clock)

        handle = manyfold.ReactiveSensorSource(
            route=route,
            observable=rx.from_iterable([1, 2]),
            mapper=lambda value: value * 10,
            wrap_sample=True,
            clock=clock,
            group="imu",
            debug_tap=tap,
        ).install(graph)
        handle.dispose()
        latest = graph.latest(route)

        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value.value, 20)
        self.assertEqual(latest.value.sequence_number, 2)
        self.assertEqual(handle.group, "imu")
        self.assertEqual([event.source_id for event in tap.snapshot()], ["imu", "imu"])

    def test_sensor_debug_tap_retains_bounded_history(self) -> None:
        manyfold = load_manyfold_package()
        clock = manyfold.ManualClock(10.0)
        tap = manyfold.SensorDebugTap(clock=clock, history_size=2)

        for source_id in ("one", "two", "three"):
            clock.advance(1.0)
            tap.publish(
                stage=manyfold.SensorDebugStage.RAW,
                stream_name="temperature",
                source_id=source_id,
                payload={"source": source_id},
                upstream_ids=("sensor", source_id),
            )

        snapshot = tap.snapshot()

        self.assertEqual([event.source_id for event in snapshot], ["two", "three"])
        self.assertEqual([event.timestamp for event in snapshot], [12.0, 13.0])
        self.assertEqual(snapshot[-1].upstream_ids, ("sensor", "three"))

    def test_sensor_debug_tap_rejects_empty_history(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "history_size must be positive"):
            manyfold.SensorDebugTap(history_size=0)

    def test_peripheral_adapter_publishes_heart_style_envelopes(self) -> None:
        manyfold = load_manyfold_package()

        @dataclass
        class FakeTag:
            name: str
            variant: str
            metadata: dict[str, str]

        @dataclass
        class FakeInfo:
            id: str
            tags: tuple[FakeTag, ...]

        @dataclass
        class FakeEnvelope:
            peripheral_info: FakeInfo
            data: dict[str, float]

        class FakePeripheral:
            def __init__(self) -> None:
                self.run_count = 0
                self.observe = rx.from_iterable(
                    [
                        FakeEnvelope(
                            peripheral_info=FakeInfo(
                                id="imu-1",
                                tags=(
                                    FakeTag(
                                        name="input_variant",
                                        variant="accelerometer",
                                        metadata={"version": "v1"},
                                    ),
                                ),
                            ),
                            data={"x": 1.0, "y": 2.0, "z": 3.0},
                        )
                    ]
                )

            def run(self) -> None:
                self.run_count += 1

        graph = manyfold.Graph()
        route = _route(manyfold, "peripheral_event", manyfold.sensor_event_schema())
        peripheral = FakePeripheral()
        handle = manyfold.PeripheralAdapter(
            peripheral=peripheral,
            route=route,
            event_type="peripheral.accelerometer.vector",
            clock=manyfold.ManualClock(3.0),
            group="heart",
        ).install(graph)
        handle.dispose()
        latest = graph.latest(route)

        self.assertEqual(peripheral.run_count, 1)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value.event_type, "peripheral.accelerometer.vector")
        self.assertEqual(latest.value.data, {"x": 1.0, "y": 2.0, "z": 3.0})
        self.assertEqual(latest.value.identity.id, "imu-1")
        self.assertEqual(latest.value.identity.tags[0].variant, "accelerometer")

    def test_peripheral_adapter_subscribes_before_starting_hot_peripheral(self) -> None:
        manyfold = load_manyfold_package()

        class FakePeripheral:
            def __init__(self) -> None:
                self.observe = rx.Subject()
                self.run_count = 0

            def run(self) -> None:
                self.run_count += 1
                self.observe.on_next({"temperature": 21})

        graph = manyfold.Graph()
        route = _route(manyfold, "hot_peripheral_event", manyfold.sensor_event_schema())
        peripheral = FakePeripheral()
        handle = manyfold.PeripheralAdapter(
            peripheral=peripheral,
            route=route,
            event_type="peripheral.temperature",
            clock=manyfold.ManualClock(3.0),
            group="ambient",
        ).install(graph)
        handle.dispose()
        latest = graph.latest(route)

        self.assertEqual(peripheral.run_count, 1)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value.data, {"temperature": 21})

    def test_duplex_sensor_peripheral_forwards_control_route(self) -> None:
        manyfold = load_manyfold_package()

        class FakePeripheral:
            def __init__(self) -> None:
                self.observe = rx.from_iterable([])
                self.inputs: list[int] = []

            def handle_input(self, value: int) -> None:
                self.inputs.append(value)

        graph = manyfold.Graph()
        output = _route(manyfold, "duplex_output", manyfold.sensor_event_schema())
        control = _route(manyfold, "duplex_control", _int_schema(manyfold, "Control"))
        peripheral = FakePeripheral()
        handle = manyfold.DuplexSensorPeripheral(
            peripheral=peripheral,
            route=output,
            control_route=control,
            run_on_install=False,
        ).install(graph)

        graph.publish(control, 9)
        handle.dispose()

        self.assertEqual(peripheral.inputs, [9])

    def test_rate_matched_sensor_handles_bursty_input(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        source = _route(manyfold, "raw", _int_schema(manyfold, "Raw"))
        sink = _route(manyfold, "sampled", _int_schema(manyfold, "Sampled"))
        demand = _route(manyfold, "demand", manyfold.Schema.bytes(name="Demand"))
        emitted: list[int] = []

        manyfold.RateMatchedSensor(
            source=source,
            sink=sink,
            demand=demand,
            capacity=2,
            mode="fifo",
        ).install(graph)
        subscription = graph.observe(sink, replay_latest=False).subscribe(
            lambda envelope: emitted.append(envelope.value)
        )

        graph.publish(source, 1)
        graph.publish(source, 2)
        graph.publish(source, 3)
        graph.publish(demand, b"tick-1")
        graph.publish(demand, b"tick-2")
        subscription.dispose()

        self.assertEqual(emitted, [2, 3])

    def test_rate_matched_sensor_uses_group_for_capacitor_name(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        source = _route(manyfold, "grouped_raw", _int_schema(manyfold, "Raw"))
        sink = _route(manyfold, "grouped_sampled", _int_schema(manyfold, "Sampled"))
        clock = manyfold.ManualClock()

        capacitor = manyfold.RateMatchedSensor(
            source=source,
            sink=sink,
            capacity=1,
            clock=clock,
            group="ambient",
        ).install(graph)

        self.assertEqual(capacitor.name, "ambient.rate_matched_sensor")
        self.assertEqual(clock.group, "ambient")

    def test_sensor_health_watchdog_reports_stale_input(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        clock = manyfold.ManualClock(0.0)
        source = _route(manyfold, "sample", _int_schema(manyfold, "Sample"))
        health = _route(manyfold, "health", manyfold.health_status_schema())
        handle = manyfold.SensorHealthWatchdog(
            source=source,
            health_route=health,
            stale_after=5.0,
            clock=clock,
            group="ambient",
        ).install(graph)

        self.assertEqual(handle.group, "ambient")
        self.assertEqual(clock.group, "ambient")
        self.assertTrue(handle.check().stale)
        graph.publish(source, 1)
        self.assertFalse(handle.check().stale)
        clock.advance(5.0)
        stale = handle.check()
        handle.dispose()

        self.assertTrue(stale.stale)
        latest = graph.latest(health)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value.status, "stale")

    def test_double_buffer_frame_assembler_and_checksum(self) -> None:
        manyfold = load_manyfold_package()
        buffer = manyfold.DoubleBuffer[tuple[int, int]](buffer_size=2)

        self.assertIsNone(buffer.push((1, 0)))
        self.assertEqual(buffer.push((1, 1)), (0, ((1, 0), (1, 1))))
        self.assertEqual(buffer.pop(), (0, ((1, 0), (1, 1))))

        assembler = manyfold.FrameAssembler[tuple[int, int, str]](
            expected_count=2,
            frame_id=lambda sample: sample[0],
            slot_id=lambda sample: sample[1],
        )
        self.assertEqual(assembler.add((7, 1, "b")), ())
        frames = assembler.add((7, 0, "a"))

        self.assertEqual(frames[0].frame_id, 7)
        self.assertEqual(frames[0].samples, ((7, 0, "a"), (7, 1, "b")))
        self.assertEqual(manyfold.xor_checksum([0xAA, 0x0F, 0x01]), 0xA4)

    def test_local_durable_spool_restores_samples(self) -> None:
        manyfold = load_manyfold_package()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        route = _route(manyfold, "spooled_sample", sample_schema)

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("sensor", "spool")
            graph = manyfold.Graph()
            spool = manyfold.LocalDurableSpool("sensor_spool", keyspace, sample_schema)
            subscription = spool.install(graph, route)
            graph.publish(
                route,
                manyfold.SensorSample(
                    value=23,
                    source_timestamp=1.0,
                    ingest_timestamp=1.5,
                    sequence_number=1,
                ),
            )
            subscription.dispose()

            replayed_graph = manyfold.Graph()
            replayed = manyfold.LocalDurableSpool(
                "sensor_spool",
                manyfold.FileStore(Path(temp_dir)).prefix("sensor", "spool"),
                sample_schema,
            ).replay(replayed_graph)

        self.assertEqual([sample.value for sample in replayed], [23])
        self.assertEqual([sample.sequence_number for sample in replayed], [1])

    def test_local_durable_spool_cleans_up_log_when_source_subscribe_fails(self) -> None:
        manyfold = load_manyfold_package()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        route = _route(manyfold, "failed_spooled_sample", sample_schema)

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("sensor", "spool")
            graph = manyfold.Graph()
            spool = manyfold.LocalDurableSpool("sensor_spool", keyspace, sample_schema)
            log = spool.event_log()
            original_observe = graph.observe

            def failing_observe(route_ref, *args, **kwargs):
                if route_ref == route:
                    raise RuntimeError("source subscribe failed")
                return original_observe(route_ref, *args, **kwargs)

            graph.observe = failing_observe  # type: ignore[method-assign]

            with self.assertRaisesRegex(RuntimeError, "source subscribe failed"):
                spool.install(graph, route)

            graph.publish(
                log.input(),
                manyfold.SensorSample(
                    value=23,
                    source_timestamp=1.0,
                    ingest_timestamp=1.5,
                    sequence_number=1,
                ),
            )
            records = log.records()

        self.assertEqual(records, ())


if __name__ == "__main__":
    unittest.main()
