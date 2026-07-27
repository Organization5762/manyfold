from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from manyfold import CallbackSubscription, Graph, Layer, Plane, Schema, Variant, route
from manyfold.private.profiling import publish_benchmarks

from tests.test_support import subprocess_test_env


class PublishBenchmarkTests(unittest.TestCase):
    def test_full_matrix_reports_distinct_publish_paths_and_final_state(self) -> None:
        result = publish_benchmarks.run_publish_benchmarks(
            iterations=16,
            require_clean=False,
            runs=2,
            warmup_iterations=2,
        )

        self.assertEqual(result["iterations"], 16)
        self.assertEqual(result["runs"], 2)
        self.assertEqual(result["warmup_iterations"], 2)
        self.assertEqual(len(result["environment"]["native_module_sha256"]), 64)
        results = {
            workload["workload"]: workload
            for workload in result["workloads"]
        }
        self.assertEqual(tuple(sorted(results)), publish_benchmarks.WORKLOADS)
        for workload in results.values():
            self.assertEqual(len(workload["run_final_states"]), 2)
            self.assertEqual(
                workload["run_final_states"],
                (workload["final_state"], workload["final_state"]),
            )
            self.assertEqual(len(workload["steady_publish"]["runs"]), 2)
            self.assertGreater(workload["steady_publish"]["average"], 0.0)
            self.assertGreater(
                workload["per_route_first_publish"]["average"],
                0.0,
            )
            self.assertGreaterEqual(
                workload["steady_publish"]["relative_stdev_percent"],
                0.0,
            )

        self.assertEqual(
            results["raw_route_publish"]["final_state"]["retained_history"],
            8,
        )
        self.assertEqual(
            results["typed_bytes_publish"]["final_state"]["latest_payload_bytes"],
            16,
        )
        self.assertTrue(
            results["typed_encoded_publish"]["final_state"]["decoded_latest"]
        )
        self.assertTrue(
            results["process_local_nowait"]["final_state"]["latest_preserves_identity"]
        )
        self.assertIsInstance(
            results["process_local_nowait"]["final_state"]["released_by_graph"],
            bool,
        )
        self.assertTrue(
            results["process_local_nowait"]["final_state"][
                "released_by_benchmark_cleanup"
            ]
        )
        self.assertEqual(
            results["sparse_drop_nowait"]["final_state"][
                "python_materialized_payloads"
            ],
            0,
        )
        self.assertEqual(
            results["subscriber_delivery_nowait"]["final_state"]["delivered"],
            19,
        )
        self.assertEqual(
            results["materializer_fanout_nowait"]["final_state"]["delivered"],
            19,
        )
        self.assertTrue(
            results["materializer_unobserved_nowait"]["final_state"][
                "all_source_events_accepted"
            ]
        )
        self.assertTrue(
            results["materializer_unobserved_nowait"]["final_state"][
                "all_state_events_accepted"
            ]
        )
        self.assertTrue(
            results["typed_encoded_nowait"]["final_state"]["decoded_latest"]
        )
        self.assertEqual(
            results["retained_history_publish"]["final_state"]["retained_history"],
            19,
        )

    def test_benchmark_rejects_invalid_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least one"):
            publish_benchmarks.run_publish_benchmarks(())
        with self.assertRaisesRegex(ValueError, "unsupported publish workload"):
            publish_benchmarks.run_publish_benchmarks(
                ("missing",),  # type: ignore[arg-type]
                require_clean=False,
            )
        with self.assertRaisesRegex(ValueError, "iterations must be positive"):
            publish_benchmarks.run_publish_benchmarks(
                iterations=0,
                require_clean=False,
            )
        with self.assertRaisesRegex(TypeError, "runs must be an integer"):
            publish_benchmarks.run_publish_benchmarks(  # type: ignore[arg-type]
                require_clean=False,
                runs=True,
            )
        with self.assertRaisesRegex(ValueError, "warmup_iterations must be positive"):
            publish_benchmarks.run_publish_benchmarks(
                require_clean=False,
                warmup_iterations=0,
            )

    def test_benchmark_cli_writes_reusable_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "publish.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "manyfold.private.profiling.publish_benchmarks",
                    "sparse_drop_nowait",
                    "--iterations",
                    "16",
                    "--runs",
                    "2",
                    "--warmup-iterations",
                    "2",
                    "--allow-dirty",
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
        self.assertIn("revision", output["provenance"])
        self.assertEqual(
            output["workloads"][0]["workload"],
            "sparse_drop_nowait",
        )

    def test_teardown_failure_still_releases_process_local_values(self) -> None:
        start_count = publish_benchmarks._any_schema_value_count()
        graph = Graph()
        target = route(
            plane=Plane.Read,
            layer=Layer.Logical,
            owner="publish_benchmark_test",
            family="cleanup",
            stream="process_local",
            variant=Variant.Event,
            schema=Schema.any("PublishBenchmarkCleanup"),
        )
        graph.publish_nowait(target, object())

        def fail_dispose() -> None:
            raise RuntimeError("subscription teardown failed")

        session = publish_benchmarks._session(
            graph,
            publish_one=lambda: None,
            final_state=lambda _events: {},
            disposables=(CallbackSubscription(fail_dispose),),
            process_local_value_baseline=start_count,
        )

        with self.assertRaisesRegex(RuntimeError, "subscription teardown failed"):
            session.dispose()

        self.assertEqual(publish_benchmarks._any_schema_value_count(), start_count)


if __name__ == "__main__":
    unittest.main()
