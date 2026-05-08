from __future__ import annotations

import math
import unittest

from tests.test_support import load_manyfold_package


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
                    "float schema values must be finite",
                ):
                    schema.encode(value)

    def test_float_schema_rejects_non_finite_payloads(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema.float(name="Temperature")

        for payload in (b"nan", b"inf", b"-inf"):
            with self.subTest(payload=payload):
                with self.assertRaisesRegex(
                    ValueError,
                    "float schema values must be finite",
                ):
                    schema.decode(payload)


if __name__ == "__main__":
    unittest.main()
