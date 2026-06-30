from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from manyfold.private.profiling import benchmark_baselines


class BenchmarkBaselineTests(unittest.TestCase):
    def test_verifier_accepts_matching_non_empty_json_baseline(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            benchmarks = root / "src" / "benchmarks"
            baselines = benchmarks / "baselines"
            baselines.mkdir(parents=True)
            (benchmarks / "sample.rs").write_text("fn main() {}\n", encoding="utf-8")
            (baselines / "sample.json").write_text('{"workload": "sample"}\n', encoding="utf-8")

            sources = benchmark_baselines.verify_benchmark_baselines(root)

        self.assertEqual(tuple(path.name for path in sources), ("sample.rs",))

    def test_verifier_rejects_missing_baseline(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            benchmarks = root / "src" / "benchmarks"
            benchmarks.mkdir(parents=True)
            (benchmarks / "sample.rs").write_text("fn main() {}\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "missing benchmark baseline"):
                benchmark_baselines.verify_benchmark_baselines(root)

    def test_verifier_rejects_empty_json_baseline(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            benchmarks = root / "src" / "benchmarks"
            baselines = benchmarks / "baselines"
            baselines.mkdir(parents=True)
            (benchmarks / "sample.rs").write_text("fn main() {}\n", encoding="utf-8")
            (baselines / "sample.json").write_text("[]\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "baseline list is empty"):
                benchmark_baselines.verify_benchmark_baselines(root)


if __name__ == "__main__":
    unittest.main()
