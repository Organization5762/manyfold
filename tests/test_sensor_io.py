from __future__ import annotations

import json
import math
import sys
import tempfile
import threading
import unittest
from dataclasses import dataclass
from pathlib import Path

import reactivex as rx

from tests.test_support import load_manyfold_package


class _StableMappingKey:
    __module__ = "sensor.alpha"

    def __str__(self) -> str:
        return "slot"


class _OtherStableMappingKey:
    __module__ = "sensor.beta"
    __qualname__ = "_StableMappingKey"

    def __str__(self) -> str:
        return "slot"


class _NonFiniteClock:
    def now(self) -> float:
        return math.nan


class SensorIoTests(unittest.TestCase):
    def test_sensor_io_exports_are_tuple_shaped(self) -> None:
        load_manyfold_package()
        sensor_io = sys.modules["manyfold.sensor_io"]

        self.assertIsInstance(sensor_io.__all__, tuple)
        self.assertEqual(sensor_io.__all__, tuple(sorted(sensor_io.__all__)))
        for name in sensor_io.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(sensor_io, name))

    def test_manual_clock_produces_deterministic_timestamps(self) -> None:
        manyfold = load_manyfold_package()
        clock = manyfold.ManualClock(10.0)

        self.assertEqual(clock.now(), 10.0)
        self.assertEqual(clock.advance(2.5), 12.5)
        clock.set(20)
        self.assertEqual(clock.now(), 20.0)

    def test_manual_clock_rejects_non_finite_time_values(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "clock value must be a finite number"):
            manyfold.ManualClock(float("nan"))

        clock = manyfold.ManualClock(10.0)
        with self.assertRaisesRegex(ValueError, "clock value must be a finite number"):
            clock.set(float("inf"))
        with self.assertRaisesRegex(ValueError, "clock delta must be a finite number"):
            clock.advance(float("nan"))

    def test_manual_clock_rejects_boolean_and_non_numeric_time_values(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "clock value must be a finite number"):
            manyfold.ManualClock(True)  # type: ignore[arg-type]

        clock = manyfold.ManualClock(10.0)
        with self.assertRaisesRegex(ValueError, "clock value must be a finite number"):
            clock.set("11.0")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "clock delta must be a finite number"):
            clock.advance(False)  # type: ignore[arg-type]

    def test_sensor_location_rejects_invalid_coordinates(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"x": True}, r"location\.x must be a finite number"),
            ({"y": "1.0"}, r"location\.y must be a finite number"),
            ({"z": float("nan")}, r"location\.z must be a finite number"),
            (
                {"timestamp": float("inf")},
                r"location\.timestamp must be a finite number",
            ),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorLocation(**kwargs)

    def test_sensor_tag_rejects_invalid_metadata(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"name": True, "variant": "radio"}, r"tag\.name must be a string"),
            ({"name": "input", "variant": 3}, r"tag\.variant must be a string"),
            (
                {"name": "input", "variant": "radio", "metadata": []},
                r"tag\.metadata must be a mapping",
            ),
            (
                {"name": "input", "variant": "radio", "metadata": {7: "ok"}},
                r"tag\.metadata key must be a string",
            ),
            (
                {"name": "input", "variant": "radio", "metadata": {"unit": 7}},
                r"tag\.metadata value must be a string",
            ),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorTag(**kwargs)

    def test_sensor_tag_metadata_is_key_sorted(self) -> None:
        manyfold = load_manyfold_package()

        tag = manyfold.SensorTag(
            "input",
            "radio",
            metadata={"unit": "celsius", "calibration": "factory"},
        )

        self.assertEqual(
            tuple(tag.metadata),
            ("calibration", "unit"),
        )

    def test_sensor_identity_rejects_invalid_values(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"id": True}, r"identity\.id must be a string"),
            ({"tags": None}, r"identity\.tags must be iterable"),
            ({"tags": ("input_variant",)}, r"identity\.tags\[\] must be a SensorTag"),
            ({"location": "lab"}, r"identity\.location must be a SensorLocation"),
            ({"group": 7}, r"identity\.group must be a string"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorIdentity(**kwargs)

    def test_sensor_identity_accepts_iterable_tags_as_tuple(self) -> None:
        manyfold = load_manyfold_package()
        tag = manyfold.SensorTag("input_variant", "radio")

        identity = manyfold.SensorIdentity(tags=[tag])

        self.assertEqual(identity.tags, (tag,))

    def test_sensor_sample_rejects_invalid_values(self) -> None:
        manyfold = load_manyfold_package()
        valid_kwargs = {
            "value": 21,
            "source_timestamp": 1.0,
            "ingest_timestamp": 1.5,
            "sequence_number": 1,
        }

        for kwargs, message in (
            (
                {"source_timestamp": math.nan},
                "source_timestamp must be a finite number",
            ),
            (
                {"ingest_timestamp": "1.5"},
                "ingest_timestamp must be a finite number",
            ),
            ({"sequence_number": True}, "sequence_number must be an integer"),
            ({"sequence_number": -1}, "sequence_number must be non-negative"),
            ({"quality": 1}, "quality must be a string"),
            ({"status": False}, "status must be a string"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorSample(**{**valid_kwargs, **kwargs})

    def test_sensor_event_rejects_invalid_values(self) -> None:
        manyfold = load_manyfold_package()
        valid_kwargs = {
            "event_type": "radio.packet",
            "data": {},
            "observed_at": 1.5,
        }

        for kwargs, message in (
            ({"event_type": True}, "event_type must be a string"),
            ({"event_type": " "}, "event_type must be a non-empty string"),
            ({"observed_at": math.inf}, "observed_at must be a finite number"),
            ({"identity": "sensor"}, "identity must be a SensorIdentity"),
            ({"sequence_number": False}, "sequence_number must be an integer"),
            ({"sequence_number": -1}, "sequence_number must be non-negative"),
            ({"raw": "payload"}, "raw must be bytes-like"),
            ({"metadata": []}, "metadata must be a mapping"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorEvent(**{**valid_kwargs, **kwargs})

    def test_sensor_event_normalizes_raw_bytes_like_payloads(self) -> None:
        manyfold = load_manyfold_package()

        event = manyfold.SensorEvent(
            event_type="radio.packet",
            data={},
            observed_at=1.5,
            raw=bytearray(b"abc"),
        )

        self.assertEqual(event.raw, b"abc")

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

    def test_bounded_ring_buffer_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"overflow": False}, "overflow must be a string"),
            ({"overflow": "spill"}, "overflow must be one of"),
            ({"ordering": 7}, "ordering must be a string"),
            ({"ordering": "lifo"}, "only fifo ordering is currently supported"),
            ({"group": object()}, "group must be a string"),
            ({"dropped": True}, "dropped must be an integer"),
            ({"dropped": -1}, "dropped must be non-negative"),
            ({"rejected": 1.5}, "rejected must be an integer"),
            ({"rejected": -1}, "rejected must be non-negative"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.BoundedRingBuffer(capacity=1, **kwargs)

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

    def test_delimited_message_buffer_rejects_text_while_utf8_sequence_is_pending(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        text_buffer = manyfold.DelimitedMessageBuffer(mode="text")

        self.assertEqual(text_buffer.append(b"caf\xc3"), ())
        with self.assertRaisesRegex(ValueError, "UTF-8 byte sequence is pending"):
            text_buffer.append("!\n")
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

    def test_delimited_message_buffer_rejects_non_bytes_delimiter(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "delimiter must be bytes-like"):
            manyfold.DelimitedMessageBuffer(delimiter="\\n")  # type: ignore[arg-type]

    def test_delimited_message_buffer_rejects_unknown_mode(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "mode must be 'bytes' or 'text'"):
            manyfold.DelimitedMessageBuffer(mode="lines")  # type: ignore[arg-type]

    def test_delimited_message_buffer_rejects_invalid_text_delimiter(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "text delimiter must be valid UTF-8"):
            manyfold.DelimitedMessageBuffer(delimiter=b"\xff", mode="text")

    def test_delimited_message_buffer_rejects_non_bytes_append_data(self) -> None:
        manyfold = load_manyfold_package()

        byte_buffer = manyfold.DelimitedMessageBuffer()
        text_buffer = manyfold.DelimitedMessageBuffer(mode="text")

        with self.assertRaisesRegex(ValueError, "data must be bytes-like"):
            byte_buffer.append(7)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "data must be bytes-like"):
            text_buffer.append(7)  # type: ignore[arg-type]

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

    def test_json_event_decoder_orders_metadata_deterministically(self) -> None:
        manyfold = load_manyfold_package()
        decoder = manyfold.JsonEventDecoder()

        event = decoder.decode(
            b'{"z":1,"event_type":"sensor.event","data":{},"a":2}'
        )

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(tuple(event.metadata), ("a", "z"))

    def test_json_event_decoder_rejects_invalid_utf8(self) -> None:
        manyfold = load_manyfold_package()
        decoder = manyfold.JsonEventDecoder()

        event = decoder.decode(b'{"event_type":"sensor.note","data":"ok"}\xff')

        self.assertIsNone(event)
        self.assertEqual(decoder.sequence.peek(), 0)

    def test_json_event_decoder_does_not_advance_sequence_for_invalid_event(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        sequence = manyfold.SequenceCounter()
        decoder = manyfold.JsonEventDecoder(clock=_NonFiniteClock(), sequence=sequence)

        with self.assertRaisesRegex(ValueError, "observed_at must be a finite number"):
            decoder.decode(b'{"event_type":"sensor.note","data":"ok"}')

        self.assertEqual(sequence.peek(), 0)

    def test_json_event_decoder_uses_default_for_missing_event_type(self) -> None:
        manyfold = load_manyfold_package()
        decoder = manyfold.JsonEventDecoder(default_event_type="sensor.default")

        missing = decoder.decode(b'{"data":"ok"}')
        null = decoder.decode(b'{"event_type":null,"data":"ok"}')
        empty = decoder.decode(b'{"event_type":"","data":"ok"}')
        whitespace = decoder.decode(b'{"event_type":"   ","data":"ok"}')

        self.assertIsNotNone(missing)
        self.assertIsNotNone(null)
        self.assertIsNotNone(empty)
        self.assertIsNotNone(whitespace)
        assert missing is not None
        assert null is not None
        assert empty is not None
        assert whitespace is not None
        self.assertEqual(missing.event_type, "sensor.default")
        self.assertEqual(null.event_type, "sensor.default")
        self.assertEqual(empty.event_type, "sensor.default")
        self.assertEqual(whitespace.event_type, "sensor.default")
        self.assertEqual(decoder.sequence.peek(), 4)

    def test_json_event_decoder_rejects_non_string_event_type(self) -> None:
        manyfold = load_manyfold_package()
        decoder = manyfold.JsonEventDecoder()

        self.assertIsNone(decoder.decode(b'{"event_type":7,"data":"ok"}'))
        self.assertIsNone(decoder.decode(b'{"event_type":false,"data":"ok"}'))
        self.assertIsNone(decoder.decode(b'{"event_type":[],"data":"ok"}'))
        self.assertEqual(decoder.sequence.peek(), 0)

    def test_json_event_decoder_rejects_non_standard_json_constants(self) -> None:
        manyfold = load_manyfold_package()
        decoder = manyfold.JsonEventDecoder()

        self.assertIsNone(decoder.decode(b'{"event_type":"sensor.temp","data":NaN}'))
        self.assertIsNone(
            decoder.decode(b'{"event_type":"sensor.temp","quality":Infinity}')
        )
        self.assertIsNone(
            decoder.decode(b'{"event_type":"sensor.temp","quality":-Infinity}')
        )
        self.assertEqual(decoder.sequence.peek(), 0)

    def test_json_event_decoder_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"clock": object()}, "json event decoder clock must provide now"),
            ({"identity": object()}, "json event decoder identity must be a SensorIdentity"),
            ({"sequence": object()}, "json event decoder sequence must be a SequenceCounter"),
            ({"default_event_type": " "}, "default_event_type must be a non-empty string"),
            ({"group": 7}, "group must be a string"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.JsonEventDecoder(**kwargs)

    def test_json_event_decoder_rejects_non_bytes_message(self) -> None:
        manyfold = load_manyfold_package()
        decoder = manyfold.JsonEventDecoder()

        with self.assertRaisesRegex(ValueError, "message must be bytes-like"):
            decoder.decode(7)  # type: ignore[arg-type]
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

    def test_sensor_event_schema_serializes_string_colliding_keys_deterministically(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        first_data: dict[object, str] = {}
        second_data: dict[object, str] = {}
        first_data["2"] = "string"
        first_data[2] = "integer"
        second_data[2] = "integer"
        second_data["2"] = "string"

        first = schema.encode(
            manyfold.SensorEvent("radio.packet", first_data, observed_at=1.0)
        )
        second = schema.encode(
            manyfold.SensorEvent("radio.packet", second_data, observed_at=1.0)
        )

        self.assertEqual(first, second)
        self.assertEqual(schema.decode(first).data, {"2": "string"})

    def test_sensor_event_schema_orders_same_named_key_types_deterministically(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        alpha = _StableMappingKey()
        beta = _OtherStableMappingKey()
        first_data: dict[object, str] = {}
        second_data: dict[object, str] = {}
        first_data[alpha] = "alpha"
        first_data[beta] = "beta"
        second_data[beta] = "beta"
        second_data[alpha] = "alpha"

        first = schema.encode(
            manyfold.SensorEvent("radio.packet", first_data, observed_at=1.0)
        )
        second = schema.encode(
            manyfold.SensorEvent("radio.packet", second_data, observed_at=1.0)
        )

        self.assertEqual(first, second)
        self.assertEqual(schema.decode(first).data, {"slot": "beta"})

    def test_sensor_event_schema_restores_mapping_keys_deterministically(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()

        first = schema.decode(
            b'{"event_type":"radio.packet","data":{"z":1,"a":2},'
            b'"observed_at":1.0,"identity":null,"sequence_number":null,'
            b'"raw":null,"metadata":{"stage":"rx","crc_ok":true}}'
        )
        second = schema.decode(
            b'{"event_type":"radio.packet","data":{"a":2,"z":1},'
            b'"observed_at":1.0,"identity":null,"sequence_number":null,'
            b'"raw":null,"metadata":{"crc_ok":true,"stage":"rx"}}'
        )

        self.assertEqual(tuple(first.data), ("a", "z"))
        self.assertEqual(tuple(second.data), ("a", "z"))
        self.assertEqual(tuple(first.metadata), ("crc_ok", "stage"))
        self.assertEqual(tuple(second.metadata), ("crc_ok", "stage"))

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

    def test_health_status_rejects_invalid_values(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"status": "green", "observed_at": 1.0}, "status must be one of"),
            (
                {"status": "ok", "observed_at": math.inf},
                "observed_at must be a finite number",
            ),
            (
                {"status": "ok", "observed_at": 1.0, "message": 7},
                "message must be a string",
            ),
            (
                {"status": "ok", "observed_at": 1.0, "stale": "false"},
                "stale must be a boolean",
            ),
            (
                {"status": "ok", "observed_at": 1.0, "error_count": True},
                "error_count must be an integer",
            ),
            (
                {"status": "ok", "observed_at": 1.0, "error_count": -1},
                "error_count must be non-negative",
            ),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.HealthStatus(**kwargs)

    def test_sensor_debug_envelope_rejects_invalid_values(self) -> None:
        manyfold = load_manyfold_package()
        valid_kwargs = {
            "stage": manyfold.SensorDebugStage.RAW,
            "stream_name": "uart",
            "source_id": "sensor-1",
            "timestamp": 1.0,
            "payload": b"abc",
        }

        for kwargs, message in (
            ({"stage": "raw"}, "debug envelope stage must be a SensorDebugStage"),
            ({"stream_name": " "}, "debug envelope stream_name must be a non-empty string"),
            ({"source_id": 7}, "debug envelope source_id must be a string"),
            ({"timestamp": math.nan}, "debug envelope timestamp must be a finite number"),
            ({"upstream_ids": "sensor-0"}, "debug envelope upstream_ids must be an iterable of strings"),
            ({"upstream_ids": ("sensor-0", " ")}, r"debug envelope upstream_ids\[\] must be a non-empty string"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorDebugEnvelope(**{**valid_kwargs, **kwargs})

    def test_sensor_debug_tap_validates_clock_and_published_labels(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(
            ValueError, "sensor debug tap clock must provide now"
        ):
            manyfold.SensorDebugTap(clock=object())  # type: ignore[arg-type]

        tap = manyfold.SensorDebugTap(clock=manyfold.ManualClock(3.0))
        with self.assertRaisesRegex(
            ValueError, "debug envelope stream_name must be a non-empty string"
        ):
            tap.publish(
                stage=manyfold.SensorDebugStage.RAW,
                stream_name=" ",
                source_id="sensor-1",
                payload=b"abc",
            )

        envelope = tap.publish(
            stage=manyfold.SensorDebugStage.VIEW,
            stream_name="uart",
            source_id="sensor-1",
            payload={"value": 1},
            upstream_ids=["raw-0"],
        )

        self.assertEqual(envelope.timestamp, 3.0)
        self.assertEqual(envelope.upstream_ids, ("raw-0",))

    def test_sensor_schemas_reject_non_object_payloads(self) -> None:
        manyfold = load_manyfold_package()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        event_schema = manyfold.sensor_event_schema()
        health_schema = manyfold.health_status_schema()

        cases = (
            (sample_schema, b"[]", "sensor sample must be a JSON object"),
            (event_schema, b"null", "sensor event must be a JSON object"),
            (health_schema, b'"ok"', "health status must be a JSON object"),
        )

        for schema, payload, message in cases:
            with self.subTest(schema=schema.schema_id):
                with self.assertRaisesRegex(ValueError, message):
                    schema.decode(payload)

    def test_sensor_sample_schema_rejects_invalid_value_schema(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(
            ValueError, "sensor sample value_schema must be a Schema"
        ):
            manyfold.sensor_sample_schema(object())  # type: ignore[arg-type]

    def test_health_status_schema_rejects_string_stale_flag(self) -> None:
        manyfold = load_manyfold_package()
        health_schema = manyfold.health_status_schema()

        with self.assertRaisesRegex(ValueError, "stale must be a JSON boolean"):
            health_schema.decode(
                b'{"error_count":0,"message":"ready","observed_at":2.0,'
                b'"stale":"false","status":"ok"}'
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

    def test_sensor_sample_schema_rejects_non_integer_sequence_number(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        payload = {
            "value": "MjE=",
            "source_timestamp": 1.0,
            "ingest_timestamp": 1.5,
            "sequence_number": "1",
            "quality": None,
            "status": None,
        }

        with self.assertRaisesRegex(
            ValueError, "sequence_number must be a JSON integer"
        ):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_sample_schema_rejects_non_string_quality(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        payload = {
            "value": "MjE=",
            "source_timestamp": 1.0,
            "ingest_timestamp": 1.5,
            "sequence_number": 1,
            "quality": 1,
            "status": None,
        }

        with self.assertRaisesRegex(ValueError, "quality must be a JSON string"):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_sample_schema_rejects_non_string_status(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        payload = {
            "value": "MjE=",
            "source_timestamp": 1.0,
            "ingest_timestamp": 1.5,
            "sequence_number": 1,
            "quality": None,
            "status": False,
        }

        with self.assertRaisesRegex(ValueError, "status must be a JSON string"):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_sample_schema_rejects_string_timestamp(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        payload = {
            "value": "MjE=",
            "source_timestamp": "1.0",
            "ingest_timestamp": 1.5,
            "sequence_number": 1,
            "quality": None,
            "status": None,
        }

        with self.assertRaisesRegex(
            ValueError, "source_timestamp must be a JSON number"
        ):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_sample_schema_rejects_non_finite_timestamp(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))

        with self.assertRaisesRegex(
            ValueError, r"source_timestamp must be a JSON number \(finite\)"
        ):
            schema.decode(
                b'{"ingest_timestamp":1.5,"quality":null,"sequence_number":1,'
                b'"source_timestamp":NaN,"status":null,"value":"MjE="}'
            )

        with self.assertRaisesRegex(
            ValueError, "source_timestamp must be a finite number"
        ):
            manyfold.SensorSample(
                value=21,
                source_timestamp=math.nan,
                ingest_timestamp=1.5,
                sequence_number=1,
            )

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

    def test_sensor_event_schema_rejects_non_string_event_type(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        payload = {
            "event_type": 7,
            "data": {},
            "observed_at": 1.5,
            "identity": {},
            "sequence_number": 7,
            "raw": None,
            "metadata": {},
        }

        with self.assertRaisesRegex(ValueError, "event_type must be a JSON string"):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_non_string_identity_tag(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        payload = {
            "event_type": "radio.packet",
            "data": {},
            "observed_at": 1.5,
            "identity": {"tags": [{"name": "input_variant", "variant": 3}]},
            "sequence_number": 7,
            "raw": None,
            "metadata": {},
        }

        with self.assertRaisesRegex(
            ValueError, r"identity\.tags\[\]\.variant must be a JSON string"
        ):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_malformed_identity_containers(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        base_payload = {
            "event_type": "radio.packet",
            "data": {},
            "observed_at": 1.5,
            "identity": {},
            "sequence_number": 7,
            "raw": None,
            "metadata": {},
        }
        cases = (
            ({"identity": []}, "identity must be a JSON object"),
            (
                {"identity": {"tags": "input_variant"}},
                r"identity\.tags must be a JSON array",
            ),
            (
                {"identity": {"tags": ["input_variant"]}},
                r"identity\.tags\[\] must be a JSON object",
            ),
            (
                {
                    "identity": {
                        "tags": [
                            {
                                "name": "input_variant",
                                "variant": "radio",
                                "metadata": [],
                            }
                        ]
                    }
                },
                r"identity\.tags\[\]\.metadata must be a JSON object",
            ),
            (
                {"identity": {"location": "lab"}},
                r"identity\.location must be a JSON object",
            ),
        )

        for patch, pattern in cases:
            with self.subTest(pattern=pattern):
                payload = {**base_payload, **patch}
                with self.assertRaisesRegex(ValueError, pattern):
                    schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_non_object_metadata(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        payload = {
            "event_type": "radio.packet",
            "data": {},
            "observed_at": 1.5,
            "identity": {},
            "sequence_number": 7,
            "raw": None,
            "metadata": [],
        }

        with self.assertRaisesRegex(ValueError, "metadata must be a JSON object"):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_boolean_sequence_number(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        payload = {
            "event_type": "radio.packet",
            "data": {},
            "observed_at": 1.5,
            "identity": {},
            "sequence_number": True,
            "raw": None,
            "metadata": {},
        }

        with self.assertRaisesRegex(
            ValueError, "sequence_number must be a JSON integer"
        ):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_boolean_observed_at(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        payload = {
            "event_type": "radio.packet",
            "data": {},
            "observed_at": True,
            "identity": {},
            "sequence_number": 7,
            "raw": None,
            "metadata": {},
        }

        with self.assertRaisesRegex(ValueError, "observed_at must be a JSON number"):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_string_location_coordinate(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()
        payload = {
            "event_type": "radio.packet",
            "data": {},
            "observed_at": 1.5,
            "identity": {"location": {"x": "0.0", "y": 0.0, "z": 0.0}},
            "sequence_number": 7,
            "raw": None,
            "metadata": {},
        }

        with self.assertRaisesRegex(
            ValueError, "identity.location.x must be a JSON number"
        ):
            schema.decode(json.dumps(payload).encode("utf-8"))

    def test_sensor_event_schema_rejects_non_finite_location_coordinate(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.sensor_event_schema()

        with self.assertRaisesRegex(
            ValueError, r"identity.location.x must be a JSON number \(finite\)"
        ):
            schema.decode(
                b'{"data":{},"event_type":"radio.packet",'
                b'"identity":{"location":{"x":Infinity,"y":0.0,"z":0.0}},'
                b'"metadata":{},"observed_at":1.5,"raw":null,"sequence_number":7}'
            )

        with self.assertRaisesRegex(ValueError, "observed_at must be a finite number"):
            manyfold.SensorEvent(
                event_type="radio.packet",
                data={},
                observed_at=math.inf,
            )

    def test_health_status_schema_rejects_string_error_count(self) -> None:
        manyfold = load_manyfold_package()
        health_schema = manyfold.health_status_schema()

        with self.assertRaisesRegex(ValueError, "error_count must be a JSON integer"):
            health_schema.decode(
                b'{"error_count":"0","message":"ready","observed_at":2.0,'
                b'"stale":false,"status":"ok"}'
            )

    def test_health_status_schema_rejects_non_string_message(self) -> None:
        manyfold = load_manyfold_package()
        health_schema = manyfold.health_status_schema()

        with self.assertRaisesRegex(ValueError, "message must be a JSON string"):
            health_schema.decode(
                b'{"error_count":0,"message":7,"observed_at":2.0,'
                b'"stale":false,"status":"ok"}'
            )

    def test_health_status_schema_rejects_unknown_status(self) -> None:
        manyfold = load_manyfold_package()
        health_schema = manyfold.health_status_schema()

        with self.assertRaisesRegex(
            ValueError, "status must be one of: ok, stale, error"
        ):
            health_schema.decode(
                b'{"error_count":0,"message":"ready","observed_at":2.0,'
                b'"stale":false,"status":"green"}'
            )

    def test_health_status_schema_rejects_string_observed_at(self) -> None:
        manyfold = load_manyfold_package()
        health_schema = manyfold.health_status_schema()

        with self.assertRaisesRegex(ValueError, "observed_at must be a JSON number"):
            health_schema.decode(
                b'{"error_count":0,"message":"ready","observed_at":"2.0",'
                b'"stale":false,"status":"ok"}'
            )

    def test_health_status_schema_rejects_non_finite_observed_at(self) -> None:
        manyfold = load_manyfold_package()
        health_schema = manyfold.health_status_schema()

        with self.assertRaisesRegex(
            ValueError, r"observed_at must be a JSON number \(finite\)"
        ):
            health_schema.decode(
                b'{"error_count":0,"message":"ready","observed_at":Infinity,'
                b'"stale":false,"status":"ok"}'
            )

        with self.assertRaisesRegex(ValueError, "observed_at must be a finite number"):
            manyfold.HealthStatus(status="ok", observed_at=-math.inf)

    def test_sequence_counter_assigns_monotonic_numbers(self) -> None:
        manyfold = load_manyfold_package()
        sequence = manyfold.SequenceCounter(current=40, group="ambient")

        self.assertEqual(sequence.next(), 41)
        self.assertEqual(sequence.next(), 42)
        self.assertEqual(sequence.peek(), 42)
        sequence.reset(100)
        self.assertEqual(sequence.next(), 101)
        self.assertEqual(sequence.group, "ambient")

    def test_sequence_counter_rejects_non_integer_state(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"current": 1.5}, "current must be an integer"),
            ({"current": True}, "current must be an integer"),
            ({"current": -1}, "current must be non-negative"),
            ({"step": 1.5}, "step must be an integer"),
            ({"step": True}, "step must be an integer"),
            ({"group": 7}, "group must be a string"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SequenceCounter(**kwargs)

    def test_sequence_counter_reset_rejects_non_integer_state(self) -> None:
        manyfold = load_manyfold_package()
        sequence = manyfold.SequenceCounter()

        for value in (1.5, False):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "current must be an integer"):
                    sequence.reset(value)  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "current must be non-negative"):
            sequence.reset(-1)

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

    def test_retry_loop_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"retry": object()}, "retry must be a RetryPolicy"),
            ({"backoff": object()}, "backoff must be a BackoffPolicy"),
            ({"sleep": object()}, "sleep must be callable"),
            ({"group": object()}, "group must be a string"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.RetryLoop(**kwargs)

    def test_retry_loop_rejects_non_callable_operation(self) -> None:
        manyfold = load_manyfold_package()
        loop = manyfold.RetryLoop()

        with self.assertRaisesRegex(ValueError, "operation must be callable"):
            loop.run(object())  # type: ignore[arg-type]

    def test_backoff_policy_rejects_invalid_attempt_indexes(self) -> None:
        manyfold = load_manyfold_package()
        policy = manyfold.SensorBackoffPolicy.fixed(0.5)

        for attempt_index in (0, -1, True, 1.5, "2"):
            with self.subTest(attempt_index=attempt_index):
                with self.assertRaisesRegex(
                    ValueError,
                    "attempt_index must be",
                ):
                    policy.delay_for_attempt(attempt_index)  # type: ignore[arg-type]

    def test_backoff_policy_clamps_overflow_to_max_delay(self) -> None:
        manyfold = load_manyfold_package()
        policy = manyfold.SensorBackoffPolicy(
            initial_delay=1.0,
            multiplier=1e308,
            max_delay=5.0,
        )

        self.assertEqual(policy.delay_for_attempt(4), 5.0)

    def test_retry_policy_normalizes_retryable_exception_classes(self) -> None:
        manyfold = load_manyfold_package()

        policy = manyfold.SensorRetryPolicy(retry_on=[RuntimeError])

        self.assertEqual(policy.retry_on, (RuntimeError,))

    def test_retry_policy_rejects_non_exception_retry_types(self) -> None:
        manyfold = load_manyfold_package()

        for retry_on in ((RuntimeError, object), (ValueError, ValueError())):
            with self.subTest(retry_on=retry_on):
                with self.assertRaisesRegex(
                    ValueError, "retry_on must contain only exception types"
                ):
                    manyfold.SensorRetryPolicy(retry_on=retry_on)  # type: ignore[arg-type]

    def test_stop_token_rejects_invalid_group_and_timeout(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "stop token group must be a string"):
            manyfold.StopToken(group=object())  # type: ignore[arg-type]

        token = manyfold.StopToken()
        for timeout, message in (
            (True, "stop token timeout must be a finite number"),
            ("0.1", "stop token timeout must be a finite number"),
            (float("nan"), "stop token timeout must be a finite number"),
            (-0.1, "stop token timeout must be non-negative"),
        ):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(ValueError, message):
                    token.wait(timeout)  # type: ignore[arg-type]

    def test_retry_policy_rejects_non_iterable_retry_types(self) -> None:
        manyfold = load_manyfold_package()

        for retry_on in (RuntimeError, None, object()):
            with self.subTest(retry_on=retry_on):
                with self.assertRaisesRegex(
                    ValueError,
                    "retry_on must be an iterable of exception types",
                ):
                    manyfold.SensorRetryPolicy(retry_on=retry_on)  # type: ignore[arg-type]

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

    def test_managed_run_loop_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"body": object()}, "body must be callable"),
            ({"retry": object()}, "retry must be a RetryPolicy"),
            ({"backoff": object()}, "backoff must be a BackoffPolicy"),
            ({"on_error": object()}, "on_error must be callable"),
            ({"group": object()}, "group must be a string"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.ManagedRunLoop(
                        **{"body": lambda stop: stop.set(), **kwargs}
                    )

    def test_managed_run_loop_rejects_invalid_stop_token(self) -> None:
        manyfold = load_manyfold_package()
        loop = manyfold.ManagedRunLoop(body=lambda stop: stop.set())

        with self.assertRaisesRegex(ValueError, "stop must be a StopToken"):
            loop.run(object())  # type: ignore[arg-type]

    def test_managed_run_loop_rejects_invalid_thread_options(self) -> None:
        manyfold = load_manyfold_package()
        loop = manyfold.ManagedRunLoop(body=lambda stop: stop.set())

        for kwargs, message in (
            ({"name": " "}, "managed run loop thread name must be a non-empty string"),
            ({"daemon": "yes"}, "managed run loop thread daemon must be a boolean"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    loop.start_thread(
                        **{"name": "manyfold-test-managed-loop", **kwargs}
                    )

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

    def test_managed_graph_node_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()
        output = _route(manyfold, "valid_node_output", _int_schema(manyfold, "Output"))
        errors = _route(manyfold, "valid_node_errors", _exception_schema(manyfold))

        def body(stop, _graph):
            stop.set()

        invalid_inputs = (
            ({"name": ""}, "name must be a non-empty string"),
            ({"body": object()}, "body must be callable"),
            ({"output_routes": object()}, "output_routes must be a tuple"),
            ({"output_routes": (object(),)}, "output_routes must contain only"),
            ({"control_route": object()}, "control_route must be a TypedRoute"),
            ({"error_route": object()}, "error_route must be a TypedRoute"),
            ({"on_control": object()}, "on_control must be callable"),
            ({"map_error": object()}, "map_error must be callable"),
            ({"retry": object()}, "retry must be a RetryPolicy"),
            ({"backoff": object()}, "backoff must be a BackoffPolicy"),
            ({"clock": object()}, r"clock must provide now\(\)"),
            ({"group": object()}, "group must be a string"),
            ({"start_immediately": "yes"}, "start_immediately must be a boolean"),
            ({"daemon": "yes"}, "daemon must be a boolean"),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.ManagedGraphNode(
                        **{
                            "name": "valid-node",
                            "body": body,
                            "output_routes": (output,),
                            "error_route": errors,
                            "start_immediately": False,
                            **kwargs,
                        }
                    )

    def test_managed_graph_node_requires_control_handler_for_control_route(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        control = _route(
            manyfold,
            "unhandled_node_control",
            _int_schema(manyfold, "Control"),
        )

        with self.assertRaisesRegex(
            ValueError, "on_control is required when control_route is provided"
        ):
            manyfold.ManagedGraphNode(
                name="unhandled-control-node",
                body=lambda stop, _graph: stop.set(),
                control_route=control,
                start_immediately=False,
            )

    def test_managed_graph_node_rejects_invalid_graph_on_install(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "graph must be a Graph"):
            manyfold.ManagedGraphNode(
                name="invalid-graph-node",
                body=lambda stop, _graph: stop.set(),
                start_immediately=False,
            ).install(object())

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

    def test_detection_node_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()
        detected = _route(
            manyfold,
            "valid_detection_items",
            _int_schema(manyfold, "Detected"),
        )
        errors = _route(
            manyfold,
            "valid_detection_errors",
            _exception_schema(manyfold),
        )

        invalid_inputs = (
            ({"name": ""}, "name must be a non-empty string"),
            ({"detector": object()}, "detector must be callable"),
            ({"output_route": object()}, "output_route must be a TypedRoute"),
            ({"mapper": object()}, "mapper must be callable"),
            ({"on_detect": object()}, "on_detect must be callable"),
            ({"spawn": object()}, "spawn must be callable"),
            ({"error_route": object()}, "error_route must be a TypedRoute"),
            ({"retry": object()}, "retry must be a RetryPolicy"),
            ({"backoff": object()}, "backoff must be a BackoffPolicy"),
            ({"group": object()}, "group must be a string"),
            ({"start_immediately": "yes"}, "start_immediately must be a boolean"),
            ({"daemon": "yes"}, "daemon must be a boolean"),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.DetectionNode(
                        **{
                            "name": "valid-detection",
                            "detector": lambda: iter(()),
                            "output_route": detected,
                            "error_route": errors,
                            "start_immediately": False,
                            **kwargs,
                        }
                    )

    def test_detection_node_rejects_invalid_graph_on_install(self) -> None:
        manyfold = load_manyfold_package()
        detected = _route(
            manyfold,
            "invalid_detection_graph_items",
            _int_schema(manyfold, "Detected"),
        )

        with self.assertRaisesRegex(ValueError, "graph must be a Graph"):
            manyfold.DetectionNode(
                name="invalid-detection-graph",
                detector=lambda: iter(()),
                output_route=detected,
                start_immediately=False,
            ).install(object())

    def test_graph_access_node_validates_direct_construction(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()

        with self.assertRaisesRegex(ValueError, "graph must be a Graph"):
            manyfold.GraphAccessNode(graph=object())
        with self.assertRaisesRegex(ValueError, "owned_handles must be a list"):
            manyfold.GraphAccessNode(graph=graph, owned_handles=())

        access = manyfold.GraphAccessNode(graph=graph)
        with self.assertRaisesRegex(ValueError, r"target must provide install\(\)"):
            access.install(object())

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

    def test_threshold_filter_detects_boolean_transitions_as_value_changes(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        threshold = manyfold.ThresholdFilter[bool](threshold=10.0)

        self.assertTrue(threshold.accepts(False))
        self.assertTrue(threshold.accepts(True))
        self.assertFalse(threshold.accepts(True))

    def test_threshold_filter_compares_mapping_keys_in_stable_order(self) -> None:
        manyfold = load_manyfold_package()
        accessed_keys: list[str] = []

        class LoggedDict(dict[str, float]):
            def __getitem__(self, key: str) -> float:
                accessed_keys.append(key)
                return super().__getitem__(key)

        threshold = manyfold.ThresholdFilter[dict[str, float]](threshold=0.5)

        self.assertTrue(threshold.accepts({"z": 0.0, "a": 0.0}))
        self.assertFalse(threshold.accepts(LoggedDict({"z": 0.1, "a": 0.1})))
        self.assertEqual(accessed_keys, ["a", "z"])

    def test_threshold_filter_detects_mapping_key_shape_changes(self) -> None:
        manyfold = load_manyfold_package()
        threshold = manyfold.ThresholdFilter[dict[str, None]](threshold=0.5)

        self.assertTrue(threshold.accepts({"present": None}))
        self.assertTrue(threshold.accepts({}))
        self.assertTrue(threshold.accepts({"present": None}))

    def test_threshold_filter_rejects_non_finite_thresholds_and_detects_non_finite_transitions(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "threshold must be a finite number"):
            manyfold.ThresholdFilter[float](threshold=float("nan"))

        threshold = manyfold.ThresholdFilter[float](threshold=0.5)
        self.assertTrue(threshold.accepts(1.0))
        self.assertTrue(threshold.accepts(float("nan")))
        self.assertTrue(threshold.accepts(float("nan")))
        self.assertTrue(threshold.accepts(float("inf")))
        self.assertFalse(threshold.accepts(float("inf")))

    def test_threshold_filter_rejects_boolean_and_non_numeric_thresholds(self) -> None:
        manyfold = load_manyfold_package()

        for threshold in (True, "0.5"):
            with self.subTest(threshold=threshold):
                with self.assertRaisesRegex(
                    ValueError, "threshold must be a finite number"
                ):
                    manyfold.ThresholdFilter[float](threshold=threshold)  # type: ignore[arg-type]

    def test_change_and_threshold_filters_reject_non_callable_keys(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "change filter key must be callable"):
            manyfold.ChangeFilter[int](key=None)  # type: ignore[arg-type]

        with self.assertRaisesRegex(
            ValueError, "threshold filter key must be callable"
        ):
            manyfold.ThresholdFilter[int](
                threshold=1.0,
                key="value",  # type: ignore[arg-type]
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

    def test_local_sensor_source_does_not_advance_sequence_for_invalid_sample(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        sequence = manyfold.SequenceCounter(current=10)
        route = _route(
            manyfold,
            "invalid_local_sample",
            manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp")),
        )
        source = manyfold.LocalSensorSource(
            route=route,
            read=lambda: 25,
            clock=_NonFiniteClock(),
            sequence=sequence,
        )

        with self.assertRaisesRegex(ValueError, "source_timestamp must be a finite number"):
            source.install(graph, read_now=False).poll()

        self.assertEqual(sequence.peek(), 10)
        self.assertIsNone(graph.latest(route))

    def test_local_sensor_source_does_not_advance_sequence_when_publish_fails(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        sequence = manyfold.SequenceCounter(current=10)
        route = _route(
            manyfold,
            "unpublished_local_sample",
            manyfold.sensor_sample_schema(_failing_schema(manyfold, "Temp")),
        )
        source = manyfold.LocalSensorSource(
            route=route,
            read=lambda: 25,
            clock=manyfold.ManualClock(1.0),
            sequence=sequence,
        )

        with self.assertRaisesRegex(RuntimeError, "encode failed"):
            source.install(graph, read_now=False).poll()

        self.assertEqual(sequence.peek(), 10)
        self.assertIsNone(graph.latest(route))

    def test_local_sensor_source_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))
        route = _route(manyfold, "invalid_local_source", sample_schema)

        invalid_inputs = (
            ({"route": object()}, "route must be a TypedRoute"),
            ({"read": object()}, "read must be callable"),
            ({"clock": object()}, r"clock must provide now\(\)"),
            ({"retry": object()}, "retry must be a RetryPolicy"),
            ({"backoff": object()}, "backoff must be a BackoffPolicy"),
            ({"sequence": object()}, "sequence must be a SequenceCounter"),
            ({"group": object()}, "group must be a string"),
            ({"quality": object()}, "quality must be a string"),
            ({"status": object()}, "status must be a string"),
            ({"error_count": True}, "error_count must be an integer"),
            ({"error_count": -1}, "error_count must be non-negative"),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.LocalSensorSource(
                        **{
                            "route": route,
                            "read": lambda: 21,
                            **kwargs,
                        }
                    )

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

    def test_reactive_sensor_source_does_not_advance_sequence_for_invalid_sample(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        sequence = manyfold.SequenceCounter(current=20)
        route = _route(
            manyfold,
            "invalid_reactive_sample",
            manyfold.sensor_sample_schema(_int_schema(manyfold, "Accel")),
        )

        with self.assertRaisesRegex(ValueError, "source_timestamp must be a finite number"):
            manyfold.ReactiveSensorSource(
                route=route,
                observable=rx.from_iterable([1]),
                wrap_sample=True,
                clock=_NonFiniteClock(),
                sequence=sequence,
            ).install(graph)

        self.assertEqual(sequence.peek(), 20)
        self.assertIsNone(graph.latest(route))

    def test_reactive_sensor_source_does_not_advance_sequence_when_publish_fails(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        sequence = manyfold.SequenceCounter(current=20)
        route = _route(
            manyfold,
            "unpublished_reactive_sample",
            manyfold.sensor_sample_schema(_failing_schema(manyfold, "Accel")),
        )

        with self.assertRaisesRegex(RuntimeError, "encode failed"):
            manyfold.ReactiveSensorSource(
                route=route,
                observable=rx.from_iterable([1]),
                wrap_sample=True,
                clock=manyfold.ManualClock(2.0),
                sequence=sequence,
            ).install(graph)

        self.assertEqual(sequence.peek(), 20)
        self.assertIsNone(graph.latest(route))

    def test_reactive_sensor_source_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Accel"))
        route = _route(manyfold, "invalid_reactive_source", sample_schema)
        tap = manyfold.SensorDebugTap()

        invalid_inputs = (
            ({"route": object()}, "route must be a TypedRoute"),
            ({"observable": object()}, r"observable must provide subscribe\(\)"),
            ({"mapper": object()}, "mapper must be callable"),
            ({"wrap_sample": "yes"}, "wrap_sample must be a boolean"),
            ({"clock": object()}, r"clock must provide now\(\)"),
            ({"sequence": object()}, "sequence must be a SequenceCounter"),
            ({"identity": object()}, "identity must be a SensorIdentity"),
            ({"group": object()}, "group must be a string"),
            ({"debug_tap": object()}, "debug_tap must be a SensorDebugTap"),
            ({"debug_stage": "raw"}, "debug_stage must be a SensorDebugStage"),
            ({"stream_name": object()}, "stream_name must be a string"),
            ({"source_id": object()}, "source_id must be a string"),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.ReactiveSensorSource(
                        **{
                            "route": route,
                            "observable": rx.from_iterable(()),
                            "mapper": lambda value: value,
                            "debug_tap": tap,
                            **kwargs,
                        }
                    )

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

    def test_sensor_debug_envelope_rejects_invalid_fields(self) -> None:
        manyfold = load_manyfold_package()

        invalid_inputs = (
            ({"stage": "raw"}, "debug envelope stage must be a SensorDebugStage"),
            (
                {"stream_name": " "},
                "debug envelope stream_name must be a non-empty string",
            ),
            (
                {"source_id": ""},
                "debug envelope source_id must be a non-empty string",
            ),
            (
                {"timestamp": float("inf")},
                "debug envelope timestamp must be a finite number",
            ),
            (
                {"upstream_ids": "sensor"},
                "debug envelope upstream_ids must be an iterable of strings",
            ),
            (
                {"upstream_ids": ("sensor", "")},
                r"debug envelope upstream_ids\[\] must be a non-empty string",
            ),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorDebugEnvelope(
                        **{
                            "stage": manyfold.SensorDebugStage.RAW,
                            "stream_name": "temperature",
                            "source_id": "probe",
                            "timestamp": 10.0,
                            "payload": {"value": 21},
                            **kwargs,
                        }
                    )

    def test_sensor_debug_tap_rejects_invalid_clock_and_publish_metadata(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(
            ValueError, r"sensor debug tap clock must provide now\(\)"
        ):
            manyfold.SensorDebugTap(clock=object())  # type: ignore[arg-type]

        tap = manyfold.SensorDebugTap()

        invalid_inputs = (
            ({"stage": "raw"}, "debug envelope stage must be a SensorDebugStage"),
            (
                {"stream_name": ""},
                "debug envelope stream_name must be a non-empty string",
            ),
            (
                {"source_id": " "},
                "debug envelope source_id must be a non-empty string",
            ),
            (
                {"upstream_ids": ("sensor", object())},
                r"debug envelope upstream_ids\[\] must be a string",
            ),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    tap.publish(
                        **{
                            "stage": manyfold.SensorDebugStage.RAW,
                            "stream_name": "temperature",
                            "source_id": "probe",
                            "payload": {"value": 21},
                            **kwargs,
                        }
                    )

    def test_sensor_debug_tap_rejects_empty_history(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "history_size must be positive"):
            manyfold.SensorDebugTap(history_size=0)
        with self.assertRaisesRegex(ValueError, "history_size must be an integer"):
            manyfold.SensorDebugTap(history_size=True)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "history_size must be an integer"):
            manyfold.SensorDebugTap(history_size=2.5)  # type: ignore[arg-type]

    def test_retry_policy_rejects_non_integer_max_attempts(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "max_attempts must be an integer"):
            manyfold.SensorRetryPolicy(max_attempts=True)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "max_attempts must be an integer"):
            manyfold.SensorRetryPolicy(max_attempts=2.5)  # type: ignore[arg-type]

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

    def test_peripheral_adapter_does_not_advance_sequence_for_invalid_event(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        class FakePeripheral:
            def __init__(self) -> None:
                self.observe = rx.from_iterable([{"temperature": 21}])

        graph = manyfold.Graph()
        sequence = manyfold.SequenceCounter(current=30)
        route = _route(
            manyfold,
            "invalid_peripheral_event",
            manyfold.sensor_event_schema(),
        )

        with self.assertRaisesRegex(ValueError, "observed_at must be a finite number"):
            manyfold.PeripheralAdapter(
                peripheral=FakePeripheral(),
                route=route,
                event_type="peripheral.temperature",
                clock=_NonFiniteClock(),
                sequence=sequence,
            ).install(graph)

        self.assertEqual(sequence.peek(), 30)
        self.assertIsNone(graph.latest(route))

    def test_peripheral_adapter_does_not_advance_sequence_when_publish_fails(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        class FakePeripheral:
            def __init__(self) -> None:
                self.observe = rx.from_iterable([{"temperature": 21}])

        graph = manyfold.Graph()
        sequence = manyfold.SequenceCounter(current=30)
        route = _route(
            manyfold,
            "unpublished_peripheral_event",
            _failing_schema(manyfold, "PeripheralEvent"),
        )

        with self.assertRaisesRegex(RuntimeError, "encode failed"):
            manyfold.PeripheralAdapter(
                peripheral=FakePeripheral(),
                route=route,
                event_type="peripheral.temperature",
                clock=manyfold.ManualClock(3.0),
                sequence=sequence,
            ).install(graph)

        self.assertEqual(sequence.peek(), 30)
        self.assertIsNone(graph.latest(route))

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

    def test_peripheral_adapter_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()

        class FakePeripheral:
            def __init__(self) -> None:
                self.observe = rx.from_iterable(())

        route = _route(manyfold, "invalid_peripheral", manyfold.sensor_event_schema())
        control = _route(
            manyfold,
            "invalid_peripheral_control",
            _int_schema(manyfold, "Control"),
        )
        tap = manyfold.SensorDebugTap()

        invalid_inputs = (
            ({"peripheral": object()}, r"peripheral must provide observe\.subscribe"),
            ({"route": object()}, "route must be a TypedRoute"),
            ({"mapper": object()}, "mapper must be callable"),
            ({"event_type": object()}, "event_type must be a string"),
            ({"wrap_sample": "yes"}, "wrap_sample must be a boolean"),
            ({"control_route": object()}, "control_route must be a TypedRoute"),
            ({"clock": object()}, r"clock must provide now\(\)"),
            ({"sequence": object()}, "sequence must be a SequenceCounter"),
            ({"identity": object()}, "identity must be a SensorIdentity"),
            ({"group": object()}, "group must be a string"),
            ({"run_on_install": "yes"}, "run_on_install must be a boolean"),
            ({"stop_on_dispose": "yes"}, "stop_on_dispose must be a boolean"),
            ({"debug_tap": object()}, "debug_tap must be a SensorDebugTap"),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.PeripheralAdapter(
                        **{
                            "peripheral": FakePeripheral(),
                            "route": route,
                            "control_route": control,
                            "debug_tap": tap,
                            "run_on_install": False,
                            **kwargs,
                        }
                    )

    def test_peripheral_adapter_disposes_output_subscription_when_control_subscribe_fails(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        class FakeSubscription:
            def __init__(self) -> None:
                self.disposed = False

            def dispose(self) -> None:
                self.disposed = True

        class FakeObservable:
            def __init__(self) -> None:
                self.subscription = FakeSubscription()

            def subscribe(self, _on_next):
                return self.subscription

        class FakePeripheral:
            def __init__(self) -> None:
                self.observe = FakeObservable()

        graph = manyfold.Graph()
        output = _route(
            manyfold, "failed_control_output", manyfold.sensor_event_schema()
        )
        control = _route(
            manyfold,
            "failed_control_input",
            _int_schema(manyfold, "Control"),
        )
        peripheral = FakePeripheral()
        original_observe = graph.observe

        def failing_observe(route_ref, *args, **kwargs):
            if route_ref == control:
                raise RuntimeError("control subscribe failed")
            return original_observe(route_ref, *args, **kwargs)

        graph.observe = failing_observe  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "control subscribe failed"):
            manyfold.PeripheralAdapter(
                peripheral=peripheral,
                route=output,
                control_route=control,
                run_on_install=False,
            ).install(graph)

        self.assertTrue(peripheral.observe.subscription.disposed)

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

    def test_rate_matched_sensor_rejects_invalid_capacity(self) -> None:
        manyfold = load_manyfold_package()
        source = _route(manyfold, "invalid_rate_raw", _int_schema(manyfold, "Raw"))
        sink = _route(
            manyfold,
            "invalid_rate_sampled",
            _int_schema(manyfold, "Sampled"),
        )

        invalid_inputs = (
            (True, "capacity must be an integer"),
            ("1", "capacity must be an integer"),
            (0, "capacity must be positive"),
        )

        for capacity, message in invalid_inputs:
            with self.subTest(capacity=capacity):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.RateMatchedSensor(
                        source=source,
                        sink=sink,
                        capacity=capacity,  # type: ignore[arg-type]
                    )

    def test_rate_matched_sensor_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()
        source = _route(manyfold, "invalid_rate_config_raw", _int_schema(manyfold, "Raw"))
        sink = _route(
            manyfold,
            "invalid_rate_config_sampled",
            _int_schema(manyfold, "Sampled"),
        )
        demand = _route(
            manyfold,
            "invalid_rate_config_demand",
            manyfold.Schema.bytes(name="Demand"),
        )

        invalid_inputs = (
            ({"source": object()}, "source must be a TypedRoute"),
            ({"sink": object()}, "sink must be a TypedRoute"),
            ({"demand": object()}, "demand must be a TypedRoute"),
            ({"mode": "oldest"}, "mode must be 'latest' or 'fifo'"),
            ({"clock": object()}, r"clock must provide now\(\)"),
            ({"name": object()}, "name must be a string"),
            ({"group": object()}, "group must be a string"),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.RateMatchedSensor(
                        **{
                            "source": source,
                            "sink": sink,
                            "demand": demand,
                            "capacity": 1,
                            **kwargs,
                        }
                    )

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

    def test_sensor_health_watchdog_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()
        source = _route(
            manyfold,
            "invalid_health_sample",
            _int_schema(manyfold, "Sample"),
        )
        health = _route(
            manyfold,
            "invalid_health",
            manyfold.health_status_schema(),
        )

        invalid_inputs = (
            ({"source": object()}, "source must be a TypedRoute"),
            ({"health_route": object()}, "route must be a TypedRoute"),
            ({"clock": object()}, r"clock must provide now\(\)"),
            ({"stale_message": object()}, "stale_message must be a string"),
            ({"group": object()}, "group must be a string"),
        )

        for kwargs, message in invalid_inputs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorHealthWatchdog(
                        **{
                            "source": source,
                            "health_route": health,
                            "stale_after": 5.0,
                            **kwargs,
                        }
                    )

    def test_sensor_timing_policies_reject_non_finite_values(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(
            ValueError, "initial_delay must be a finite number"
        ):
            manyfold.SensorBackoffPolicy(initial_delay=float("nan"))
        with self.assertRaisesRegex(ValueError, "multiplier must be a finite number"):
            manyfold.SensorBackoffPolicy(multiplier=float("inf"))
        with self.assertRaisesRegex(ValueError, "max_delay must be a finite number"):
            manyfold.SensorBackoffPolicy(max_delay=float("nan"))
        with self.assertRaisesRegex(ValueError, "stale_after must be a finite number"):
            manyfold.SensorHealthWatchdog(
                source=_route(manyfold, "sample", _int_schema(manyfold, "Sample")),
                health_route=_route(
                    manyfold,
                    "health",
                    manyfold.health_status_schema(),
                ),
                stale_after=float("inf"),
            )

    def test_sensor_timing_policies_reject_boolean_and_non_numeric_values(self) -> None:
        manyfold = load_manyfold_package()

        cases = (
            (
                "initial_delay",
                lambda value: manyfold.SensorBackoffPolicy(initial_delay=value),
            ),
            (
                "multiplier",
                lambda value: manyfold.SensorBackoffPolicy(multiplier=value),
            ),
            ("max_delay", lambda value: manyfold.SensorBackoffPolicy(max_delay=value)),
        )

        for field, factory in cases:
            for value in (True, "1.0"):
                with self.subTest(field=field, value=value):
                    with self.assertRaisesRegex(
                        ValueError, f"{field} must be a finite number"
                    ):
                        factory(value)

        source = _route(manyfold, "typed_sample", _int_schema(manyfold, "TypedSample"))
        health_route = _route(
            manyfold,
            "typed_health",
            manyfold.health_status_schema(),
        )
        for value in (False, "1.0"):
            with self.subTest(field="stale_after", value=value):
                with self.assertRaisesRegex(
                    ValueError, "stale_after must be a finite number"
                ):
                    manyfold.SensorHealthWatchdog(
                        source=source,
                        health_route=health_route,
                        stale_after=value,  # type: ignore[arg-type]
                    )

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
        self.assertEqual(manyfold.xor_checksum(memoryview(b"\xAA\x0F\x01")), 0xA4)

    def test_xor_checksum_rejects_non_octet_inputs(self) -> None:
        manyfold = load_manyfold_package()

        for data in ("123", object()):
            with self.subTest(data=data):
                with self.assertRaisesRegex(
                    ValueError,
                    "checksum data must be bytes-like or an integer sequence",
                ):
                    manyfold.xor_checksum(data)  # type: ignore[arg-type]

        for data in ([True], [1.5], [-1], [256]):
            with self.subTest(data=data):
                with self.assertRaisesRegex(
                    ValueError,
                    "checksum data values must be integers between 0 and 255",
                ):
                    manyfold.xor_checksum(data)  # type: ignore[arg-type]

    def test_sensor_batching_helpers_reject_non_integer_sizes(self) -> None:
        manyfold = load_manyfold_package()

        for capacity in (True, 1.5):
            with self.subTest(capacity=capacity):
                with self.assertRaisesRegex(ValueError, "capacity must be an integer"):
                    manyfold.BoundedRingBuffer(capacity=capacity)  # type: ignore[arg-type]

        for buffer_size in (True, 1.5):
            with self.subTest(buffer_size=buffer_size):
                with self.assertRaisesRegex(
                    ValueError, "buffer_size must be an integer"
                ):
                    manyfold.DoubleBuffer(buffer_size=buffer_size)  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "group must be a string"):
            manyfold.DoubleBuffer(buffer_size=1, group=7)  # type: ignore[arg-type]

        for expected_count in (False, 2.5):
            with self.subTest(expected_count=expected_count):
                with self.assertRaisesRegex(
                    ValueError, "expected_count must be an integer"
                ):
                    manyfold.FrameAssembler(
                        expected_count=expected_count,  # type: ignore[arg-type]
                        frame_id=lambda sample: sample[0],
                        slot_id=lambda sample: sample[1],
                    )

    def test_frame_assembler_rejects_non_callable_selectors(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"frame_id": "frame"}, "frame_id must be callable"),
            ({"slot_id": None}, "slot_id must be callable"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.FrameAssembler(
                        expected_count=2,
                        frame_id=kwargs.get("frame_id", lambda sample: sample[0]),
                        slot_id=kwargs.get("slot_id", lambda sample: sample[1]),
                    )

    def test_frame_assembler_rejects_unhashable_selector_results(self) -> None:
        manyfold = load_manyfold_package()

        cases = (
            (
                manyfold.FrameAssembler(
                    expected_count=2,
                    frame_id=lambda sample: [sample[0]],
                    slot_id=lambda sample: sample[1],
                ),
                "frame_id result must be hashable",
            ),
            (
                manyfold.FrameAssembler(
                    expected_count=2,
                    frame_id=lambda sample: sample[0],
                    slot_id=lambda sample: [sample[1]],
                ),
                "slot_id result must be hashable",
            ),
        )
        for assembler, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    assembler.add((7, 1, "sample"))
                self.assertEqual(assembler._pending, {})

    def test_sensor_frame_rejects_invalid_samples(self) -> None:
        manyfold = load_manyfold_package()

        for samples, message in (
            ([], "sensor frame samples must be a tuple"),
            ((), "sensor frame samples must not be empty"),
        ):
            with self.subTest(samples=samples):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.SensorFrame(frame_id=1, samples=samples)  # type: ignore[arg-type]

    def test_frame_assembler_orders_mixed_slot_ids_deterministically(self) -> None:
        manyfold = load_manyfold_package()
        assembler = manyfold.FrameAssembler[tuple[int, object, str]](
            expected_count=2,
            frame_id=lambda sample: sample[0],
            slot_id=lambda sample: sample[1],
        )

        self.assertEqual(assembler.add((7, "0", "string")), ())
        frames = assembler.add((7, 0, "integer"))

        self.assertEqual(frames[0].frame_id, 7)
        self.assertEqual(frames[0].samples, ((7, 0, "integer"), (7, "0", "string")))

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

    def test_local_durable_spool_rejects_invalid_configuration(self) -> None:
        manyfold = load_manyfold_package()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("sensor", "spool")
            with self.assertRaisesRegex(ValueError, "spool name"):
                manyfold.LocalDurableSpool("", keyspace, sample_schema)
            with self.assertRaisesRegex(ValueError, "keyspace must be a Keyspace"):
                manyfold.LocalDurableSpool(
                    "sensor_spool",
                    object(),  # type: ignore[arg-type]
                    sample_schema,
                )
            with self.assertRaisesRegex(ValueError, "schema must be a Schema"):
                manyfold.LocalDurableSpool(
                    "sensor_spool",
                    keyspace,
                    object(),  # type: ignore[arg-type]
                )

    def test_local_durable_spool_rejects_invalid_source_before_installing_log(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        sample_schema = manyfold.sensor_sample_schema(_int_schema(manyfold, "Temp"))

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("sensor", "spool")
            graph = manyfold.Graph()
            spool = manyfold.LocalDurableSpool("sensor_spool", keyspace, sample_schema)

            with self.assertRaisesRegex(ValueError, "source must be a TypedRoute"):
                spool.install(graph, object())  # type: ignore[arg-type]

            self.assertEqual(spool.event_log().records(), ())

    def test_composite_subscription_disposes_all_children_after_failure(self) -> None:
        load_manyfold_package()
        sensor_io = sys.modules["manyfold.sensor_io"]
        disposed: list[str] = []

        class FailingSubscription:
            def dispose(self) -> None:
                disposed.append("failing")
                raise RuntimeError("dispose failed")

        class RecordingSubscription:
            def dispose(self) -> None:
                disposed.append("recording")

        subscription = sensor_io._CompositeSubscription(
            (FailingSubscription(), RecordingSubscription())
        )

        with self.assertRaisesRegex(RuntimeError, "dispose failed"):
            subscription.dispose()

        self.assertEqual(disposed, ["failing", "recording"])
        subscription.dispose()
        self.assertEqual(disposed, ["failing", "recording"])

    def test_composite_subscription_propagates_process_interrupts_immediately(
        self,
    ) -> None:
        load_manyfold_package()
        sensor_io = sys.modules["manyfold.sensor_io"]
        disposed: list[str] = []

        class InterruptingSubscription:
            def dispose(self) -> None:
                disposed.append("interrupting")
                raise KeyboardInterrupt

        class RecordingSubscription:
            def dispose(self) -> None:
                disposed.append("recording")

        subscription = sensor_io._CompositeSubscription(
            (InterruptingSubscription(), RecordingSubscription())
        )

        with self.assertRaises(KeyboardInterrupt):
            subscription.dispose()

        self.assertEqual(disposed, ["interrupting"])

    def test_local_durable_spool_cleans_up_log_when_source_subscribe_fails(
        self,
    ) -> None:
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


def _failing_schema(manyfold, schema_id: str = "Failing"):
    def encode(value) -> bytes:
        raise RuntimeError("encode failed")

    def decode(payload: bytes):
        raise RuntimeError("decode failed")

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
