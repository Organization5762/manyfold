from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from manyfold import benchmark_artifacts


class BenchmarkArtifactTests(unittest.TestCase):
    def test_verify_accepts_native_retention_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 history=8 payloads=8 lineage=0 lineage_values=0 "
                    "trace_index=0 correlation_index=0 live_allocated_bytes=24712 "
                    "peak_allocated_bytes=25136 current_rss_kib=1488 "
                    "elapsed_seconds=0.001 cpu_seconds=0.001 average_event_us=4.0 "
                    "interval_event_us=4.0 input_blocks=0 output_blocks=0",
                    "step=100000 history=8 payloads=8 lineage=0 lineage_values=0 "
                    "trace_index=0 correlation_index=0 live_allocated_bytes=30808 "
                    "peak_allocated_bytes=32745 current_rss_kib=1984 "
                    "elapsed_seconds=0.250 cpu_seconds=0.250 average_event_us=2.5 "
                    "interval_event_us=2.5 input_blocks=0 output_blocks=0",
                    "step=200000 history=8 payloads=8 lineage=0 lineage_values=0 "
                    "trace_index=0 correlation_index=0 live_allocated_bytes=30808 "
                    "peak_allocated_bytes=32745 current_rss_kib=2016 "
                    "elapsed_seconds=0.500 cpu_seconds=0.500 average_event_us=2.5 "
                    "interval_event_us=2.5 input_blocks=0 output_blocks=0",
                    "step=300000 history=8 payloads=8 lineage=0 lineage_values=0 "
                    "trace_index=0 correlation_index=0 live_allocated_bytes=30808 "
                    "peak_allocated_bytes=32745 current_rss_kib=2016 "
                    "elapsed_seconds=0.750 cpu_seconds=0.750 average_event_us=2.5 "
                    "interval_event_us=2.5 input_blocks=0 output_blocks=0",
                ),
            )

            samples = benchmark_artifacts.verify_benchmark_log(
                path,
                min_final_step=300_000,
                min_samples=4,
                required_fields=("live_allocated_bytes", "current_rss_kib"),
                required_final_values={
                    "history": 8,
                    "lineage": 0,
                    "payloads": 8,
                },
                required_final_max_values={
                    "average_event_us": 100.0,
                    "cpu_seconds": 30.0,
                    "elapsed_seconds": 30.0,
                },
                required_max_values={"correlation_index": 0},
                required_numeric_max_values={
                    "input_blocks": 0,
                    "interval_event_us": 100.0,
                    "output_blocks": 0,
                },
                tail_plateaus=(
                    ("current_rss_kib", 64, 3),
                    ("live_allocated_bytes", 0, 3),
                    ("peak_allocated_bytes", 0, 3),
                ),
            )

        self.assertEqual(samples[-1]["step"], 300_000)

    def test_verify_rejects_numeric_final_above_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 average_event_us=10.0 cpu_seconds=0.1",
                    "step=16 average_event_us=101.0 cpu_seconds=0.2",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "average_event_us"):
                benchmark_artifacts.verify_benchmark_log(
                    path,
                    required_final_max_values={"average_event_us": 100.0},
                )

    def test_verify_rejects_numeric_sample_above_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 interval_event_us=10.0 output_blocks=0",
                    "step=16 interval_event_us=1001.0 output_blocks=0",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "interval_event_us"):
                benchmark_artifacts.verify_benchmark_log(
                    path,
                    required_numeric_max_values={"interval_event_us": 1000.0},
                )

    def test_verify_rejects_elapsed_time_above_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 elapsed_seconds=0.1",
                    "step=16 elapsed_seconds=31.0",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "elapsed_seconds"):
                benchmark_artifacts.verify_benchmark_log(
                    path,
                    required_final_max_values={"elapsed_seconds": 30.0},
                )

    def test_verify_rejects_step_moving_backwards(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=16 elapsed_seconds=0.2 cpu_seconds=0.2",
                    "step=8 elapsed_seconds=0.3 cpu_seconds=0.3",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "step moved backwards"):
                benchmark_artifacts.verify_benchmark_log(path)

    def test_verify_rejects_elapsed_time_moving_backwards(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 elapsed_seconds=0.4 cpu_seconds=0.2",
                    "step=16 elapsed_seconds=0.3 cpu_seconds=0.3",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "elapsed_seconds moved backwards"):
                benchmark_artifacts.verify_benchmark_log(path)

    def test_verify_requires_explicit_monotonic_field_in_every_sample(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 elapsed_seconds=0.1",
                    "step=16 elapsed_seconds=0.2 queue_depth=0",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "monotonic field missing"):
                benchmark_artifacts.verify_benchmark_log(
                    path,
                    required_monotonic_fields=("queue_depth",),
                )

    def test_verify_rejects_wrong_final_retained_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 replay=8 lineage=1 correlation_index=0 payloads=8 "
                    "current_rss_kib=32000",
                    "step=16 replay=8 lineage=1 correlation_index=0 payloads=8 "
                    "current_rss_kib=32000",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "final lineage 1"):
                benchmark_artifacts.verify_benchmark_log(
                    path,
                    required_final_values={"lineage": 0},
                )

    def test_verify_rejects_short_tail_plateau(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 current_rss_kib=32000",
                    "step=16 current_rss_kib=33000",
                    "step=24 current_rss_kib=33032",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "tail plateau collected 2"):
                benchmark_artifacts.verify_benchmark_log(
                    path,
                    tail_plateaus=(("current_rss_kib", 64, 3),),
                )

    def test_verify_rejects_late_allocator_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 live_allocated_bytes=1024 peak_allocated_bytes=2048",
                    "step=16 live_allocated_bytes=1024 peak_allocated_bytes=2048",
                    "step=24 live_allocated_bytes=1056 peak_allocated_bytes=2080",
                    "step=32 live_allocated_bytes=1056 peak_allocated_bytes=2080",
                ),
            )

            with self.assertRaisesRegex(SystemExit, "live_allocated_bytes"):
                benchmark_artifacts.verify_benchmark_log(
                    path,
                    tail_plateaus=(("live_allocated_bytes", 0, 3),),
                )

    def test_main_prints_verified_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_log(
                Path(directory),
                (
                    "step=8 replay=8 lineage=0 correlation_index=0 payloads=8 "
                    "current_rss_kib=32000 average_event_us=12.0",
                    "step=16 replay=8 lineage=0 correlation_index=0 payloads=8 "
                    "current_rss_kib=32000 average_event_us=10.0",
                ),
            )
            output = io.StringIO()

            with contextlib.redirect_stdout(output):
                benchmark_artifacts._main(
                    (
                        str(path),
                        "--min-final-step",
                        "16",
                        "--min-samples",
                        "2",
                        "--require-final",
                        "lineage=0",
                        "--require-final-max",
                        "average_event_us=100",
                        "--require-monotonic",
                        "current_rss_kib",
                    )
                )

        self.assertIn("benchmark_log passed=true samples=2 final_step=16", output.getvalue())


def _write_log(directory: Path, lines: tuple[str, ...]) -> Path:
    path = directory / "benchmark.txt"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
