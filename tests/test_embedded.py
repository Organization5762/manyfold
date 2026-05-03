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


if __name__ == "__main__":
    unittest.main()
