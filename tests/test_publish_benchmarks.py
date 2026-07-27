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
            first_publish_batch_size=2,
            first_publish_batches=2,
            iterations=16,
            require_clean=False,
            runs=2,
            warmup_iterations=2,
        )

        self.assertEqual(result["first_publish_batch_size"], 2)
        self.assertEqual(result["first_publish_batches"], 2)
        self.assertEqual(result["first_publishes_per_run"], 4)
        self.assertEqual(result["iterations"], 16)
        self.assertEqual(result["runs"], 2)
        self.assertEqual(result["warmup_iterations"], 2)
        self.assertEqual(
            result["run_workload_orders"],
            (
                publish_benchmarks.WORKLOADS,
                publish_benchmarks.WORKLOADS[1:]
                + publish_benchmarks.WORKLOADS[:1],
            ),
        )
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
            self.assertEqual(
                workload["per_route_first_publish"]["batch_size"],
                2,
            )
            self.assertEqual(
                workload["per_route_first_publish"]["batches_per_run"],
                2,
            )
            self.assertEqual(
                workload["per_route_first_publish"]["publishes_per_run"],
                4,
            )
            self.assertEqual(
                tuple(
                    len(batch_means)
                    for batch_means in workload["per_route_first_publish"][
                        "raw_batch_means_us"
                    ]
                ),
                (2, 2),
            )
            self.assertEqual(
                tuple(
                    len(batch_durations)
                    for batch_durations in workload["per_route_first_publish"][
                        "raw_batch_timed_duration_us"
                    ]
                ),
                (2, 2),
            )
            self.assertEqual(
                tuple(
                    len(process_local_deltas)
                    for process_local_deltas in workload[
                        "per_route_first_publish"
                    ]["raw_batch_process_local_value_deltas"]
                ),
                (2, 2),
            )
            self.assertEqual(
                workload["per_route_first_publish"]["run_verified_sessions"],
                (4, 4),
            )
            for run_mean, timed_duration in zip(
                workload["per_route_first_publish"]["runs"],
                workload["per_route_first_publish"][
                    "run_total_timed_duration_us"
                ],
                strict=True,
            ):
                self.assertEqual(run_mean, timed_duration / 4)
            for batch_means, batch_durations in zip(
                workload["per_route_first_publish"]["raw_batch_means_us"],
                workload["per_route_first_publish"][
                    "raw_batch_timed_duration_us"
                ],
                strict=True,
            ):
                self.assertEqual(
                    batch_means,
                    tuple(duration / 2 for duration in batch_durations),
                )
            self.assertEqual(
                workload["per_route_first_publish"]["run_final_states"],
                (
                    workload["per_route_first_publish"]["run_final_state"],
                    workload["per_route_first_publish"]["run_final_state"],
                ),
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
        self.assertEqual(
            results["process_local_nowait"]["per_route_first_publish"][
                "run_final_state"
            ]["retained_process_local_values"],
            1,
        )
        self.assertEqual(
            results["process_local_nowait"]["per_route_first_publish"][
                "raw_batch_process_local_value_deltas"
            ],
            ((2, 2), (2, 2)),
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
        with self.assertRaisesRegex(ValueError, "workloads must be unique"):
            publish_benchmarks.run_publish_benchmarks(
                ("sparse_drop_nowait", "sparse_drop_nowait"),
                require_clean=False,
            )
        with self.assertRaisesRegex(ValueError, "iterations must be positive"):
            publish_benchmarks.run_publish_benchmarks(
                iterations=0,
                require_clean=False,
            )
        with self.assertRaisesRegex(
            ValueError,
            "first_publish_batch_size must be positive",
        ):
            publish_benchmarks.run_publish_benchmarks(
                first_publish_batch_size=0,
                require_clean=False,
            )
        with self.assertRaisesRegex(
            ValueError,
            "first_publish_batches must be positive",
        ):
            publish_benchmarks.run_publish_benchmarks(
                first_publish_batches=0,
                require_clean=False,
            )
        with self.assertRaisesRegex(
            ValueError,
            "batch size must not exceed 16",
        ):
            publish_benchmarks.run_publish_benchmarks(
                first_publish_batch_size=17,
                require_clean=False,
            )
        with self.assertRaisesRegex(
            ValueError,
            "requires at least 512 first publishes per run",
        ):
            publish_benchmarks.run_publish_benchmarks(
                first_publish_batch_size=16,
                first_publish_batches=31,
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
                    "--first-publish-batch-size",
                    "2",
                    "--first-publish-batches",
                    "2",
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
        self.assertEqual(output["first_publish_batch_size"], 2)
        self.assertEqual(output["first_publish_batches"], 2)
        self.assertEqual(output["first_publishes_per_run"], 4)
        self.assertEqual(
            output["workloads"][0]["workload"],
            "sparse_drop_nowait",
        )

    def test_benchmark_cli_rejects_duplicate_workloads(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "manyfold.private.profiling.publish_benchmarks",
                "sparse_drop_nowait",
                "sparse_drop_nowait",
                "--allow-dirty",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
            env=subprocess_test_env(),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("publish workloads must be unique", result.stderr)

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
            tracks_process_local_values=True,
        )

        with self.assertRaisesRegex(RuntimeError, "subscription teardown failed"):
            session.dispose()

        self.assertEqual(publish_benchmarks._any_schema_value_count(), start_count)

    def test_first_publish_failure_disposes_every_created_session(self) -> None:
        start_count = publish_benchmarks._any_schema_value_count()
        created_sessions = 0
        disposed_sessions = 0

        def setup() -> publish_benchmarks._WorkloadSession:
            nonlocal created_sessions
            created_sessions += 1
            graph = Graph()
            target = route(
                plane=Plane.Read,
                layer=Layer.Logical,
                owner="publish_benchmark_test",
                family="cleanup",
                stream="first_publish_failure",
                variant=Variant.Event,
                schema=Schema.any("PublishBenchmarkFirstPublishFailure"),
            )

            def fail_delivery(_value: object) -> None:
                raise RuntimeError("first publish delivery failed")

            subscription = graph.observe(
                target,
                replay_latest=False,
            ).subscribe(fail_delivery)

            def record_disposal() -> dict[str, int | bool]:
                nonlocal disposed_sessions
                disposed_sessions += 1
                return {}

            return publish_benchmarks._session(
                graph,
                publish_one=lambda: graph.publish(target, object()),
                final_state=lambda _events: {},
                disposables=(subscription,),
                after_dispose=record_disposal,
                tracks_process_local_values=True,
            )

        with self.assertRaisesRegex(RuntimeError, "first publish delivery failed"):
            publish_benchmarks._run_first_publish_batches(
                setup,
                batch_size=3,
                batches=1,
            )

        self.assertEqual(created_sessions, 3)
        self.assertEqual(disposed_sessions, created_sessions)
        self.assertEqual(publish_benchmarks._any_schema_value_count(), start_count)

    def test_first_publish_setup_failure_disposes_completed_sessions(self) -> None:
        created_sessions = 0
        disposed_sessions = 0

        def setup() -> publish_benchmarks._WorkloadSession:
            nonlocal created_sessions, disposed_sessions
            if created_sessions == 2:
                raise RuntimeError("third setup failed")
            created_sessions += 1
            graph = Graph()

            def record_disposal() -> dict[str, int | bool]:
                nonlocal disposed_sessions
                disposed_sessions += 1
                return {}

            return publish_benchmarks._session(
                graph,
                publish_one=lambda: None,
                final_state=lambda _events: {},
                after_dispose=record_disposal,
            )

        with self.assertRaisesRegex(RuntimeError, "third setup failed"):
            publish_benchmarks._run_first_publish_batches(
                setup,
                batch_size=4,
                batches=1,
            )

        self.assertEqual(created_sessions, 2)
        self.assertEqual(disposed_sessions, created_sessions)

    def test_first_publish_verification_failure_restores_every_session(self) -> None:
        start_count = publish_benchmarks._any_schema_value_count()
        disposed_sessions = 0

        def setup() -> publish_benchmarks._WorkloadSession:
            graph = Graph()
            target = route(
                plane=Plane.Read,
                layer=Layer.Logical,
                owner="publish_benchmark_test",
                family="cleanup",
                stream="first_publish_verification_failure",
                variant=Variant.Event,
                schema=Schema.any("PublishBenchmarkFirstPublishVerifyFailure"),
            )

            def fail_verification(_events: int) -> dict[str, int | bool]:
                raise RuntimeError("first publish verification failed")

            def record_disposal() -> dict[str, int | bool]:
                nonlocal disposed_sessions
                disposed_sessions += 1
                return {}

            return publish_benchmarks._session(
                graph,
                publish_one=lambda: graph.publish(target, object()),
                final_state=fail_verification,
                after_dispose=record_disposal,
                tracks_process_local_values=True,
            )

        with self.assertRaisesRegex(RuntimeError, "first publish verification failed"):
            publish_benchmarks._run_first_publish_batches(
                setup,
                batch_size=3,
                batches=1,
            )

        self.assertEqual(disposed_sessions, 3)
        self.assertEqual(publish_benchmarks._any_schema_value_count(), start_count)

    def test_first_publish_disposal_failure_restores_every_session(self) -> None:
        start_count = publish_benchmarks._any_schema_value_count()
        created_sessions = 0
        disposed_sessions = 0

        def setup() -> publish_benchmarks._WorkloadSession:
            nonlocal created_sessions
            created_sessions += 1
            session_number = created_sessions
            graph = Graph()
            target = route(
                plane=Plane.Read,
                layer=Layer.Logical,
                owner="publish_benchmark_test",
                family="cleanup",
                stream="first_publish_disposal_failure",
                variant=Variant.Event,
                schema=Schema.any("PublishBenchmarkFirstPublishDisposeFailure"),
            )

            def fail_first_disposal() -> None:
                if session_number == 1:
                    raise RuntimeError("first session disposal failed")

            def record_disposal() -> dict[str, int | bool]:
                nonlocal disposed_sessions
                disposed_sessions += 1
                return {}

            return publish_benchmarks._session(
                graph,
                publish_one=lambda: graph.publish(target, object()),
                final_state=lambda _events: {
                    "retained_process_local_values": len(
                        graph._materialized_payloads
                    )
                },
                disposables=(CallbackSubscription(fail_first_disposal),),
                after_dispose=record_disposal,
                tracks_process_local_values=True,
            )

        with self.assertRaisesRegex(RuntimeError, "first session disposal failed"):
            publish_benchmarks._run_first_publish_batches(
                setup,
                batch_size=3,
                batches=1,
            )

        self.assertEqual(created_sessions, 3)
        self.assertEqual(disposed_sessions, created_sessions)
        self.assertEqual(publish_benchmarks._any_schema_value_count(), start_count)

    def test_first_publish_batches_bound_concurrent_live_sessions(self) -> None:
        active_sessions = 0
        maximum_active_sessions = 0

        def setup() -> publish_benchmarks._WorkloadSession:
            nonlocal active_sessions, maximum_active_sessions
            graph = Graph()
            active_sessions += 1
            maximum_active_sessions = max(maximum_active_sessions, active_sessions)

            def record_disposal() -> dict[str, int | bool]:
                nonlocal active_sessions
                active_sessions -= 1
                return {}

            return publish_benchmarks._session(
                graph,
                publish_one=lambda: None,
                final_state=lambda _events: {},
                after_dispose=record_disposal,
            )

        result = publish_benchmarks._run_first_publish_batches(
            setup,
            batch_size=4,
            batches=3,
        )

        self.assertEqual(maximum_active_sessions, 4)
        self.assertEqual(active_sessions, 0)
        self.assertEqual(result["verified_sessions"], 12)
        self.assertEqual(len(result["batch_means_us"]), 3)

        with self.assertRaisesRegex(
            ValueError,
            "batch size must not exceed 16",
        ):
            publish_benchmarks._run_first_publish_batches(
                setup,
                batch_size=17,
                batches=1,
            )

    def test_first_publish_batch_rejects_session_state_difference(self) -> None:
        created_sessions = 0
        disposed_sessions = 0

        def setup() -> publish_benchmarks._WorkloadSession:
            nonlocal created_sessions
            created_sessions += 1
            session_number = created_sessions
            graph = Graph()

            def record_disposal() -> dict[str, int | bool]:
                nonlocal disposed_sessions
                disposed_sessions += 1
                return {}

            return publish_benchmarks._session(
                graph,
                publish_one=lambda: None,
                final_state=lambda _events: {"session_number": session_number},
                after_dispose=record_disposal,
            )

        with self.assertRaisesRegex(RuntimeError, "final state changed in run 2"):
            publish_benchmarks._run_first_publish_batches(
                setup,
                batch_size=2,
                batches=1,
            )

        self.assertEqual(disposed_sessions, created_sessions)


if __name__ == "__main__":
    unittest.main()
