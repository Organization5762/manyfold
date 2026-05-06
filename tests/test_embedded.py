from __future__ import annotations

import sys
import unittest

from tests.test_support import load_manyfold_package


class EmbeddedProfileTests(unittest.TestCase):
    def test_load_manyfold_package_reloads_support_modules_cleanly(self) -> None:
        load_manyfold_package()
        setattr(sys.modules["manyfold.reference_examples"], "SENTINEL", object())

        load_manyfold_package()

        self.assertNotIn("SENTINEL", vars(sys.modules["manyfold.reference_examples"]))

    def test_scalar_sensor_profile_validates_without_issues(self) -> None:
        manyfold = load_manyfold_package()
        profile = manyfold.EmbeddedDeviceProfile()
        sensor = profile.scalar_sensor(
            owner=manyfold.OwnerName("uart-temp"),
            family=manyfold.StreamFamily("sensor"),
            stream=manyfold.StreamName("temperature"),
            schema=manyfold.Schema.bytes(name="Temperature"),
        )

        self.assertEqual(sensor.validate(), ())

    def test_bulk_sensor_requires_byte_credits(self) -> None:
        manyfold = load_manyfold_package()
        profile = manyfold.EmbeddedDeviceProfile(
            rules=manyfold.EmbeddedRuntimeRules(bulk_credit_policy="messages"),
        )
        sensor = profile.bulk_sensor(
            owner=manyfold.OwnerName("lidar"),
            family=manyfold.StreamFamily("scan"),
            metadata_stream=manyfold.StreamName("meta"),
            metadata_schema=manyfold.Schema.bytes(name="LidarMeta"),
            payload_stream=manyfold.StreamName("payload"),
            payload_schema=manyfold.Schema.bytes(name="LidarPayload"),
        )

        self.assertIn(
            "bulk payload routes must use byte credits instead of count credits",
            sensor.validate(),
        )

    def test_embedded_module_exports_intentional_profile_surface(self) -> None:
        manyfold = load_manyfold_package()
        embedded = sys.modules["manyfold.embedded"]

        self.assertEqual(
            embedded.__all__,
            [
                "EmbeddedBulkSensor",
                "EmbeddedDeviceProfile",
                "EmbeddedRuntimeRules",
                "EmbeddedScalarSensor",
                "FirmwareAgentProfile",
            ],
        )
        for name in embedded.__all__:
            with self.subTest(name=name):
                self.assertIs(getattr(embedded, name), getattr(manyfold, name))

    def test_bulk_sensor_rejects_metadata_on_bulk_layer(self) -> None:
        manyfold = load_manyfold_package()
        sensor = manyfold.EmbeddedBulkSensor(
            metadata_route=manyfold.route(
                owner=manyfold.OwnerName("lidar"),
                family=manyfold.StreamFamily("scan"),
                stream=manyfold.StreamName("meta"),
                layer=manyfold.Layer.Bulk,
                variant=manyfold.Variant.Meta,
                schema=manyfold.Schema.bytes(name="LidarMeta"),
            ),
            payload_route=manyfold.route(
                owner=manyfold.OwnerName("lidar"),
                family=manyfold.StreamFamily("scan"),
                stream=manyfold.StreamName("payload"),
                layer=manyfold.Layer.Bulk,
                variant=manyfold.Variant.Payload,
                schema=manyfold.Schema.bytes(name="LidarPayload"),
            ),
        )

        self.assertIn(
            "bulk sensor metadata must not use Layer.Bulk",
            sensor.validate(),
        )

    def test_bulk_sensor_rejects_unpaired_payload_identity(self) -> None:
        manyfold = load_manyfold_package()
        sensor = manyfold.EmbeddedBulkSensor(
            metadata_route=manyfold.route(
                owner=manyfold.OwnerName("lidar"),
                family=manyfold.StreamFamily("scan"),
                stream=manyfold.StreamName("meta"),
                layer=manyfold.Layer.Logical,
                variant=manyfold.Variant.Meta,
                schema=manyfold.Schema.bytes(name="LidarMeta"),
            ),
            payload_route=manyfold.route(
                owner=manyfold.OwnerName("camera"),
                family=manyfold.StreamFamily("frames"),
                stream=manyfold.StreamName("payload"),
                layer=manyfold.Layer.Bulk,
                variant=manyfold.Variant.Payload,
                schema=manyfold.Schema.bytes(name="LidarPayload"),
            ),
        )

        self.assertEqual(
            tuple(
                issue
                for issue in sensor.validate()
                if "metadata and payload" in issue
            ),
            (
                "bulk sensor metadata and payload owners must match",
                "bulk sensor metadata and payload families must match",
            ),
        )

    def test_firmware_profile_reports_disabled_local_processing(self) -> None:
        manyfold = load_manyfold_package()
        profile = manyfold.FirmwareAgentProfile(
            local_filtering=False,
            local_aggregation=False,
        )

        self.assertEqual(
            profile.required_issues(),
            (
                "firmware agent should support local filtering",
                "firmware agent should support local aggregation",
            ),
        )

    def test_sensor_validation_includes_firmware_local_processing_issues(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        profile = manyfold.EmbeddedDeviceProfile(
            firmware=manyfold.FirmwareAgentProfile(local_filtering=False),
        )
        sensor = profile.scalar_sensor(
            owner=manyfold.OwnerName("uart-temp"),
            family=manyfold.StreamFamily("sensor"),
            stream=manyfold.StreamName("temperature"),
            schema=manyfold.Schema.bytes(name="Temperature"),
        )

        self.assertIn(
            "firmware agent should support local filtering",
            sensor.validate(),
        )

    def test_embedded_rule_issue_order_is_stable(self) -> None:
        manyfold = load_manyfold_package()
        firmware = manyfold.FirmwareAgentProfile(
            route_descriptors=False,
            sequence_numbering=False,
            source_timestamping=False,
            transport_framing=False,
            shadow_reporting=False,
            local_filtering=False,
            local_aggregation=False,
            ring_buffer_staging=False,
        )
        rules = manyfold.EmbeddedRuntimeRules(
            timestamps_close_to_source=False,
            keep_isr_work_minimal=False,
            use_dma_or_async_peripherals=False,
            bounded_ring_buffers=False,
            avoid_heap_on_hot_paths=False,
            separate_metadata_and_payload_early=False,
            preserve_device_and_ingest_time=False,
            lazy_bulk_payload_open=False,
            prefer_zero_copy_bulk_payloads=False,
            bulk_credit_policy="messages",
        )

        self.assertEqual(
            firmware.required_issues(),
            (
                "firmware agent must provide route descriptors",
                "firmware agent must provide sequence numbering",
                "firmware agent must timestamp close to the source",
                "firmware agent must provide transport framing",
                "firmware agent should expose shadow reporting",
                "firmware agent should support local filtering",
                "firmware agent should support local aggregation",
                "firmware agent should stage through a ring buffer",
            ),
        )
        self.assertEqual(
            rules.bulk_issues(),
            (
                "embedded routes must timestamp close to the source",
                "embedded routes must keep ISR work minimal",
                "embedded routes should prefer DMA or async peripherals",
                "embedded routes must use bounded ring buffers",
                "embedded routes should avoid heap allocation on hot paths",
                "embedded routes should separate metadata and payload early",
                "embedded routes must preserve device time and ingest time separately",
                "bulk payload opening should be lazy",
                "bulk payload paths should prefer zero-copy or shared memory strategies",
                "bulk payload routes must use byte credits instead of count credits",
            ),
        )


if __name__ == "__main__":
    unittest.main()
