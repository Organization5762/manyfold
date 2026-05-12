from __future__ import annotations

import unittest

import manyfold.stats as stats
from manyfold.stats import Average


class StatsTests(unittest.TestCase):
    def test_stats_exports_are_explicit(self) -> None:
        self.assertEqual(stats.__all__, ("Average",))
        for name in stats.__all__:
            self.assertIn(name, stats.__dict__)

    def test_average_uses_latest_values_within_window(self) -> None:
        self.assertEqual(Average(window_size=3)([1.0, 2.0, 9.0, 10.0]), 7.0)

    def test_average_uses_latest_value_directly_for_single_value_window(self) -> None:
        self.assertEqual(Average(window_size=1)([1.0, 2.0, 9.0, 10.0]), 10.0)

    def test_average_uses_all_values_when_window_exceeds_input(self) -> None:
        self.assertEqual(Average(window_size=10)([1.0, 2.0, 9.0]), 4.0)

    def test_average_uses_stable_floating_point_summation(self) -> None:
        self.assertEqual(Average(window_size=4)([1e16, 1.0, -1e16, 3.0]), 1.0)

    def test_average_reads_window_without_slicing(self) -> None:
        class IndexOnlyValues:
            def __len__(self) -> int:
                return 4

            def __getitem__(self, index: int) -> float:
                if isinstance(index, slice):
                    raise AssertionError("average should not copy a slice")
                return (1.0, 2.0, 9.0, 10.0)[index]

        self.assertEqual(Average(window_size=3)(IndexOnlyValues()), 7.0)  # type: ignore[arg-type]

    def test_average_rejects_empty_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least one value"):
            Average(window_size=3)([])

    def test_average_rejects_non_finite_or_non_numeric_values(self) -> None:
        for value in (True, float("nan"), float("inf"), "3"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError, "average values must be finite numbers"
                ):
                    Average(window_size=3)([1.0, value])  # type: ignore[list-item]

    def test_average_rejects_non_positive_window_size(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            Average(window_size=0)

    def test_average_rejects_non_integer_window_size(self) -> None:
        for value in (True, 3.5, "3"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "must be an integer"):
                    Average(window_size=value)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
