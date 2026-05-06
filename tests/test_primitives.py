from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
