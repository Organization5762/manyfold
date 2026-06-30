from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from manyfold.private.profiling import runtime_benchmarks

from tests.test_support import subprocess_test_env


class RuntimeBenchmarkTests(unittest.TestCase):
    def test_publish_workload_reports_profileable_timing(self) -> None:
        result = runtime_benchmarks.run_runtime_benchmark(
            "publish",
            iterations=16,
            runs=2,
            max_average_event_us=1_000_000.0,
        )

        self.assertEqual(result["workload"], "publish")
        self.assertEqual(result["mode"], "standard")
        self.assertTrue(result["passed"])
        self.assertEqual(len(result["gates"]), 1)
        self.assertEqual(result["events"], 16)
        self.assertEqual(result["latest_payload_bytes"], 16)
        self.assertEqual(result["runs"], 2)
        self.assertGreater(result["average_event_us"], 0.0)

    def test_observe_workload_counts_observer_delivery(self) -> None:
        result = runtime_benchmarks.run_runtime_benchmark(
            "observe",
            iterations=16,
        )

        self.assertEqual(result["workload"], "observe")
        self.assertEqual(result["events"], 16)
        self.assertEqual(result["observed"], 16)
        self.assertGreater(result["average_event_us"], 0.0)

    def test_rpc_workload_runs_in_parallel(self) -> None:
        result = runtime_benchmarks.run_runtime_benchmark(
            "rpc",
            iterations=16,
            runs=2,
            mode="parallel",
            callers=4,
        )

        self.assertEqual(result["workload"], "rpc")
        self.assertEqual(result["mode"], "parallel")
        self.assertEqual(result["events"], 16)
        self.assertEqual(result["callers"], 4)
        self.assertEqual(result["rpc_calls"], 16)
        self.assertEqual(result["retained_audit_payloads"], 16)
        self.assertEqual(result["runs"], 2)
        self.assertGreater(result["average_event_us"], 0.0)

    def test_runtime_benchmark_rejects_invalid_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported runtime workload"):
            runtime_benchmarks.run_runtime_benchmark("missing")  # type: ignore[arg-type]

        with self.assertRaisesRegex(ValueError, "iterations must be positive"):
            runtime_benchmarks.run_runtime_benchmark("publish", iterations=0)

        with self.assertRaisesRegex(TypeError, "callers must be an integer"):
            runtime_benchmarks.run_runtime_benchmark(
                "publish",
                mode="parallel",
                callers=True,  # type: ignore[arg-type]
            )

        with self.assertRaisesRegex(ValueError, "max_average_event_us must be positive"):
            runtime_benchmarks.run_runtime_benchmark(
                "publish",
                max_average_event_us=0.0,
            )

    def test_runtime_benchmark_reports_failed_gates(self) -> None:
        result = runtime_benchmarks.run_runtime_benchmark(
            "publish",
            iterations=16,
            max_average_event_us=0.000001,
        )

        self.assertFalse(result["passed"])
        self.assertEqual(result["gates"][0]["name"], "max_average_event_us")
        self.assertFalse(result["gates"][0]["passed"])

    def test_runtime_benchmark_module_lists_workloads(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "manyfold.private.profiling.runtime_benchmarks",
                "--list",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(tuple(result.stdout.splitlines()), ("publish", "observe", "rpc"))

    def test_runtime_benchmark_script_runs_parallel_rpc_workload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "artifacts" / "runtime-rpc.json"
            result = subprocess.run(
                [
                    "uv",
                    "run",
                    "manyfold-runtime-benchmark",
                    "rpc",
                    "--mode",
                    "parallel",
                    "--iterations",
                    "16",
                    "--callers",
                    "4",
                    "--runs",
                    "2",
                    "--max-average-event-us",
                    "1000000",
                    "--output-json",
                    str(output_path),
                ],
                check=False,
                capture_output=True,
                text=True,
                cwd=Path(__file__).resolve().parents[1],
                env=subprocess_test_env(),
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            output = json.loads(result.stdout)
            self.assertEqual(
                json.loads(output_path.read_text(encoding="utf-8")),
                output,
            )
        self.assertEqual(output["workload"], "rpc")
        self.assertEqual(output["mode"], "parallel")
        self.assertTrue(output["passed"])
        self.assertEqual(output["events"], 16)
        self.assertEqual(output["callers"], 4)
        self.assertEqual(output["rpc_calls"], 16)
        self.assertGreater(output["average_event_us"], 0.0)

    def test_runtime_benchmark_script_fails_failed_gate_with_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "runtime-rpc.json"
            result = subprocess.run(
                [
                    "uv",
                    "run",
                    "manyfold-runtime-benchmark",
                    "publish",
                    "--iterations",
                    "16",
                    "--max-average-event-us",
                    "0.000001",
                    "--output-json",
                    str(output_path),
                ],
                check=False,
                capture_output=True,
                text=True,
                cwd=Path(__file__).resolve().parents[1],
                env=subprocess_test_env(),
            )

            self.assertEqual(result.returncode, 1)
            output = json.loads(result.stdout)
            self.assertEqual(json.loads(output_path.read_text(encoding="utf-8")), output)
        self.assertFalse(output["passed"])
        self.assertFalse(output["gates"][0]["passed"])


if __name__ == "__main__":
    unittest.main()
