from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tempfile
import unittest

from tests.test_support import load_manyfold_package


def _int_schema(manyfold, schema_id: str = "Int"):
    return manyfold.Schema(
        schema_id=schema_id,
        version=1,
        encode=lambda value: str(value).encode("ascii"),
        decode=lambda payload: int(payload.decode("ascii")),
    )


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
        import reactivex as rx

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

    def test_peripheral_adapter_publishes_heart_style_envelopes(self) -> None:
        manyfold = load_manyfold_package()
        import reactivex as rx

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

    def test_duplex_sensor_peripheral_forwards_control_route(self) -> None:
        manyfold = load_manyfold_package()
        import reactivex as rx

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
        demand = _route(manyfold, "demand", manyfold.Schema.bytes("Demand"))
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


if __name__ == "__main__":
    unittest.main()
