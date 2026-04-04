from __future__ import annotations

import unittest

from test_support import load_manyfold_package


class EmbeddedProfileTests(unittest.TestCase):
    def test_scalar_sensor_profile_validates_without_issues(self) -> None:
        manyfold = load_manyfold_package()
        profile = manyfold.EmbeddedDeviceProfile()
        sensor = profile.scalar_sensor(
            owner=manyfold.OwnerName("uart-temp"),
            family=manyfold.StreamFamily("sensor"),
            stream=manyfold.StreamName("temperature"),
            schema=manyfold.Schema.bytes("Temperature"),
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
            metadata_schema=manyfold.Schema.bytes("LidarMeta"),
            payload_stream=manyfold.StreamName("payload"),
            payload_schema=manyfold.Schema.bytes("LidarPayload"),
        )

        self.assertIn("bulk payload routes must use byte credits instead of count credits", sensor.validate())


if __name__ == "__main__":
    unittest.main()
