from __future__ import annotations

import unittest

from tests.test_support import load_manyfold_package


class StatsTests(unittest.TestCase):
    def test_average_uses_latest_values_within_window(self) -> None:
        load_manyfold_package()
        from manyfold.stats import Average

        self.assertEqual(Average(window_size=3)([1.0, 2.0, 9.0, 10.0]), 7.0)

    def test_average_rejects_empty_values(self) -> None:
        load_manyfold_package()
        from manyfold.stats import Average

        with self.assertRaisesRegex(ValueError, "at least one value"):
            Average(window_size=3)([])

    def test_average_rejects_non_positive_window_size(self) -> None:
        load_manyfold_package()
        from manyfold.stats import Average

        with self.assertRaisesRegex(ValueError, "must be positive"):
            Average(window_size=0)


if __name__ == "__main__":
    unittest.main()
