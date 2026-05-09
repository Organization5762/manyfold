from __future__ import annotations

import math
import subprocess
import sys
import unittest

from tests.test_support import load_manyfold_package, subprocess_test_env


class PrimitiveTests(unittest.TestCase):
    def test_typed_route_reuses_materialized_native_route_ref(self) -> None:
        manyfold = load_manyfold_package()
        route = manyfold.route(
            owner=manyfold.OwnerName("sensor"),
            family=manyfold.StreamFamily("events"),
            stream=manyfold.StreamName("temperature"),
            schema=manyfold.Schema.float(name="Temperature"),
        )

        route_ref = route.route_ref

        self.assertIs(route.route_ref, route_ref)
        self.assertEqual(
            route.display(),
            "read.logical.sensor.events.temperature.meta.v1",
        )

    def test_route_reports_missing_identity_parts_in_stable_order(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(
            ValueError,
            "route requires: owner, family, stream",
        ):
            manyfold.route(schema=manyfold.Schema.float(name="Temperature"))

    def test_route_rejects_empty_identity_parts(self) -> None:
        manyfold = load_manyfold_package()

        for field in ("owner", "family", "stream"):
            kwargs = {"owner": "sensor", "family": "events", "stream": "temperature"}
            kwargs[field] = " "
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    ValueError,
                    f"{field} must be a non-empty string",
                ):
                    manyfold.route(
                        **kwargs,
                        schema=manyfold.Schema.float(name="Temperature"),
                    )

    def test_route_rejects_invalid_native_enum_values_early(self) -> None:
        manyfold = load_manyfold_package()

        cases = (
            ("plane", "read", "plane must be a Plane"),
            ("layer", "logical", "layer must be a Layer"),
            ("variant", "meta", "variant must be a Variant"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                kwargs = {
                    "owner": "sensor",
                    "family": "events",
                    "stream": "temperature",
                    "schema": manyfold.Schema.float(name="Temperature"),
                    field: value,
                }
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.route(**kwargs)

    def test_native_reference_constructors_reject_blank_identifiers(self) -> None:
        script = """
import manyfold._manyfold_rust as native

namespace = native.NamespaceRef(native.Plane.Read, native.Layer.Logical, "owner")
schema = native.SchemaRef("Payload", 1)
route = native.RouteRef(namespace, "family", "stream", native.Variant.Meta, schema)

cases = (
    (lambda: native.NamespaceRef(native.Plane.Read, native.Layer.Logical, " "), "owner"),
    (lambda: native.SchemaRef(" ", 1), "schema_id"),
    (lambda: native.RouteRef(namespace, " ", "stream", native.Variant.Meta, schema), "family"),
    (lambda: native.RouteRef(namespace, "family", "", native.Variant.Meta, schema), "stream"),
    (lambda: native.ProducerRef(" ", native.ProducerKind.Application), "producer_id"),
    (lambda: native.RuntimeRef(" "), "runtime_id"),
    (lambda: native.ClockDomainRef(" "), "clock_domain_id"),
    (lambda: native.PayloadRef(" ", 0), "payload_id"),
    (lambda: native.PayloadRef("payload", 0, " "), "codec_id"),
    (lambda: native.TaintMark(native.TaintDomain.Time, " ", "origin"), "value_id"),
    (lambda: native.TaintMark(native.TaintDomain.Time, "value", ""), "origin_id"),
    (lambda: native.ControlLoop("", [route], route), "control loop name"),
)

for build, field in cases:
    try:
        build()
    except ValueError as exc:
        assert f"{field} must be a non-empty string" in str(exc), str(exc)
    else:
        raise AssertionError(f"expected ValueError for {field}")
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            env=subprocess_test_env(),
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_reference_constructors_reject_invalid_unsigned_fields(self) -> None:
        script = """
import manyfold._manyfold_rust as native

cases = (
    (lambda: native.SchemaRef("Payload", 0), "schema version must be a positive integer"),
    (lambda: native.SchemaRef("Payload", True), "schema version must be a positive integer"),
    (lambda: native.PayloadRef("payload", True), "logical_length_bytes must be a non-negative integer"),
    (lambda: native.ScheduleGuard.not_before_epoch(True), "epoch must be a non-negative integer"),
)

for build, message in cases:
    try:
        build()
    except ValueError as exc:
        assert message in str(exc), str(exc)
    else:
        raise AssertionError(f"expected ValueError for {message}")
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            env=subprocess_test_env(),
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_schema_rejects_empty_id(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(
            ValueError,
            "schema_id must be a non-empty string",
        ):
            manyfold.Schema.bytes(name="")

    def test_schema_rejects_non_positive_versions(self) -> None:
        manyfold = load_manyfold_package()

        for version in (0, -1, False):
            with self.subTest(version=version):
                with self.assertRaisesRegex(
                    ValueError,
                    "schema version must be a positive integer",
                ):
                    manyfold.Schema.bytes(name="Temperature", version=version)

    def test_bytes_schema_preserves_bytes_like_payloads(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema.bytes(name="RawFrame")

        self.assertEqual(schema.encode(b"abc"), b"abc")
        self.assertEqual(schema.encode(bytearray(b"abc")), b"abc")
        self.assertEqual(schema.decode(memoryview(b"abc")), b"abc")

    def test_bytes_schema_rejects_scalar_and_text_payloads(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema.bytes(name="RawFrame")

        for value in (3, True, "abc", ["a"]):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "bytes schema values must be bytes-like",
                ):
                    schema.encode(value)

    def test_route_rejects_unknown_schema_shapes_clearly(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(
            ValueError,
            "schema must be a Schema, bytes, or protobuf message type",
        ):
            manyfold.route(
                owner="sensor",
                family="events",
                stream="temperature",
                schema=object(),
            )

    def test_any_schema_rejects_unknown_process_local_tokens_clearly(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema.any("RuntimeHandle")

        with self.assertRaisesRegex(
            ValueError,
            "unknown process-local object token for schema 'RuntimeHandle'",
        ):
            schema.decode(b"missing")

    def test_any_schema_tokens_are_scoped_by_schema_id(self) -> None:
        manyfold = load_manyfold_package()
        runtime_handles = manyfold.Schema.any("RuntimeHandle")
        other_handles = manyfold.Schema.any("OtherHandle")

        token = runtime_handles.encode(object())

        with self.assertRaisesRegex(
            ValueError,
            "unknown process-local object token for schema 'OtherHandle'",
        ):
            other_handles.decode(token)

    def test_any_schema_tokens_are_scoped_by_schema_version(self) -> None:
        manyfold = load_manyfold_package()
        current_handles = manyfold.Schema.any("RuntimeHandle", version=1)
        next_handles = manyfold.Schema.any("RuntimeHandle", version=2)

        token = current_handles.encode(object())

        with self.assertRaisesRegex(
            ValueError,
            "unknown process-local object token for schema 'RuntimeHandle'",
        ):
            next_handles.decode(token)

    def test_float_schema_rejects_non_finite_values(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema.float(name="Temperature")

        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "float schema values must be finite numbers",
                ):
                    schema.encode(value)

    def test_float_schema_rejects_boolean_and_non_numeric_values(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema.float(name="Temperature")

        for value in (False, True, "1.0", b"1.0"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "float schema values must be finite numbers",
                ):
                    schema.encode(value)

    def test_float_schema_rejects_non_finite_payloads(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema.float(name="Temperature")

        for payload in (b"nan", b"inf", b"-inf", b"not-a-number", b"\xff"):
            with self.subTest(payload=payload):
                with self.assertRaisesRegex(
                    ValueError,
                    "float schema values must be finite numbers",
                ):
                    schema.decode(payload)


if __name__ == "__main__":
    unittest.main()
