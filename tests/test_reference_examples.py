from __future__ import annotations

import ast
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

from examples import (
    ARCHIVED_EXAMPLE_ENTRIES,
    EXAMPLE_CATALOG,
    REFERENCE_EXAMPLE_ENTRIES,
    REFERENCE_EXAMPLE_GAPS,
    REFERENCE_EXAMPLE_NUMBERS,
    SUPPORTED_EXAMPLE_ENTRIES,
    reference_example_metadata,
)
from tests.test_support import load_example_module, load_manyfold_package


class ReferenceExampleSuiteTests(unittest.TestCase):
    def _suite_by_number(self, manyfold):
        return {
            example.number: example for example in manyfold.reference_example_suite()
        }

    def test_reference_examples_file_import_uses_repo_path_fallback(self) -> None:
        module_path = (
            Path(__file__).resolve().parents[1]
            / "python"
            / "manyfold"
            / "reference_examples.py"
        )
        spec = importlib.util.spec_from_file_location(
            "manyfold_reference_examples_file_test",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module

        calls: list[str] = []
        fake_manyfold = types.ModuleType("manyfold")
        fake_manyfold.__path__ = []  # type: ignore[attr-defined]
        fake_repo_paths = types.ModuleType("manyfold._repo_paths")
        fake_repo_paths.ensure_repo_import_paths = lambda: calls.append("called")

        with mock.patch.dict(
            sys.modules,
            {
                "manyfold": fake_manyfold,
                "manyfold._repo_paths": fake_repo_paths,
            },
        ):
            spec.loader.exec_module(module)

        self.assertEqual(calls, ["called"])
        self.assertEqual(module.REFERENCE_EXAMPLE_SUITE, module.reference_example_suite())

    def test_manyfold_package_exports_recent_graph_types(self) -> None:
        manyfold = load_manyfold_package()

        self.assertIn("JoinInput", manyfold.__dict__)
        self.assertIn("LineageRecord", manyfold.__dict__)
        self.assertIn("FlowSnapshot", manyfold.__dict__)
        self.assertIn("LifecycleBinding", manyfold.__dict__)
        self.assertIn("MailboxSnapshot", manyfold.__dict__)
        self.assertIn("bridge_version", manyfold.__dict__)

    def test_manyfold_package_re_exports_recent_runtime_and_snapshot_aliases(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        rust_module = sys.modules["manyfold._manyfold_rust"]
        graph_module = sys.modules["manyfold.graph"]

        self.assertIs(manyfold.ControlLoop, rust_module.ControlLoop)
        self.assertIs(manyfold.WriteBinding, rust_module.WriteBinding)
        self.assertIs(manyfold.ControlLoops, graph_module.ControlLoops)
        self.assertIs(manyfold.RouteRetentionPolicy, graph_module.RouteRetentionPolicy)
        self.assertIs(manyfold.WatermarkSnapshot, graph_module.WatermarkSnapshot)
        self.assertIn("ControlLoop", manyfold.__all__)
        self.assertIn("WriteBinding", manyfold.__all__)
        self.assertIn("ControlLoops", manyfold.__all__)
        self.assertIn("RouteRetentionPolicy", manyfold.__all__)
        self.assertIn("WatermarkSnapshot", manyfold.__all__)

    def test_manyfold_package_re_exports_recent_top_level_api_symbols(self) -> None:
        manyfold = load_manyfold_package()
        graph_module = sys.modules["manyfold.graph"]
        primitives_module = sys.modules["manyfold.primitives"]
        stats_module = sys.modules["manyfold.stats"]

        self.assertIs(manyfold.Average, stats_module.Average)
        self.assertIs(manyfold.FlowPolicy, graph_module.FlowPolicy)
        self.assertIs(manyfold.RetryPolicy, graph_module.RetryPolicy)
        self.assertIs(manyfold.LineageRecord, graph_module.LineageRecord)
        self.assertIs(
            manyfold.PayloadDemandSnapshot, graph_module.PayloadDemandSnapshot
        )
        self.assertIs(manyfold.RouteAuditSnapshot, graph_module.RouteAuditSnapshot)
        self.assertIs(manyfold.TaintRepair, graph_module.TaintRepair)
        self.assertIs(manyfold.LifecycleBinding, graph_module.LifecycleBinding)
        self.assertIs(manyfold.RouteIdentity, primitives_module.RouteIdentity)
        self.assertIs(manyfold.RouteNamespace, primitives_module.RouteNamespace)
        self.assertIs(manyfold.ShadowSnapshot, graph_module.ShadowSnapshot)
        self.assertIs(manyfold.Source, primitives_module.Source)
        self.assertIs(manyfold.Sink, primitives_module.Sink)
        self.assertIs(manyfold.source, primitives_module.source)
        self.assertIs(manyfold.sink, primitives_module.sink)
        self.assertTrue(callable(manyfold.bridge_version))
        self.assertIn("Average", manyfold.__all__)
        self.assertIn("FlowPolicy", manyfold.__all__)
        self.assertIn("RetryPolicy", manyfold.__all__)
        self.assertIn("LineageRecord", manyfold.__all__)
        self.assertIn("PayloadDemandSnapshot", manyfold.__all__)
        self.assertIn("RouteAuditSnapshot", manyfold.__all__)
        self.assertIn("ShadowSnapshot", manyfold.__all__)
        self.assertIn("TaintRepair", manyfold.__all__)
        self.assertIn("LifecycleBinding", manyfold.__all__)
        self.assertIn("RouteIdentity", manyfold.__all__)
        self.assertIn("RouteNamespace", manyfold.__all__)
        self.assertIn("Source", manyfold.__all__)
        self.assertIn("Sink", manyfold.__all__)
        self.assertIn("source", manyfold.__all__)
        self.assertIn("sink", manyfold.__all__)
        self.assertIn("CallbackNode", manyfold.__all__)
        self.assertIn("CoalesceLatestNode", manyfold.__all__)
        self.assertIn("FilterNode", manyfold.__all__)
        self.assertIn("GraphConnection", manyfold.__all__)
        self.assertIn("LoggingNode", manyfold.__all__)
        self.assertIn("MapNode", manyfold.__all__)
        self.assertIn("RoutePipeline", manyfold.__all__)
        self.assertIn("instrument_stream", manyfold.__all__)
        self.assertNotIn("DischargePolicy", manyfold.__all__)
        self.assertNotIn("FillPolicy", manyfold.__all__)
        self.assertNotIn("OverflowPolicy", manyfold.__all__)
        self.assertNotIn("ResistorPolicy", manyfold.__all__)
        self.assertNotIn("WatchdogPolicy", manyfold.__all__)

    def test_manyfold_package_re_exports_example_facing_helpers(self) -> None:
        manyfold = load_manyfold_package()
        components_module = sys.modules["manyfold.components"]
        embedded_module = sys.modules["manyfold.embedded"]
        graph_module = sys.modules["manyfold.graph"]

        self.assertIs(manyfold.Consensus, components_module.Consensus)
        self.assertIs(manyfold.Memory, components_module.Memory)
        self.assertIs(manyfold.FileStore, components_module.FileStore)
        self.assertIs(manyfold.Keyspace, components_module.Keyspace)
        self.assertIs(manyfold.EventLog, components_module.EventLog)
        self.assertIs(manyfold.SnapshotStore, components_module.SnapshotStore)
        self.assertIs(manyfold.EmbeddedDeviceProfile, embedded_module.EmbeddedDeviceProfile)
        self.assertIs(manyfold.EmbeddedBulkSensor, embedded_module.EmbeddedBulkSensor)
        self.assertIs(manyfold.LazyPayloadSource, graph_module.LazyPayloadSource)
        self.assertIn("Consensus", manyfold.__all__)
        self.assertIn("Memory", manyfold.__all__)
        self.assertIn("FileStore", manyfold.__all__)
        self.assertIn("Keyspace", manyfold.__all__)
        self.assertIn("EventLog", manyfold.__all__)
        self.assertIn("SnapshotStore", manyfold.__all__)
        self.assertIn("EmbeddedDeviceProfile", manyfold.__all__)
        self.assertIn("EmbeddedBulkSensor", manyfold.__all__)
        self.assertIn("LazyPayloadSource", manyfold.__all__)

    def test_top_level_stub_exports_match_runtime_package_exports(self) -> None:
        stub_path = Path(__file__).resolve().parents[1] / "python" / "manyfold" / "__init__.pyi"
        runtime_init_path = (
            Path(__file__).resolve().parents[1] / "python" / "manyfold" / "__init__.py"
        )
        stub_module = ast.parse(stub_path.read_text(encoding="utf-8"))
        runtime_init_module = ast.parse(runtime_init_path.read_text(encoding="utf-8"))

        def exports_from(module: ast.Module) -> tuple[str, ...]:
            for node in module.body:
                if not isinstance(node, ast.Assign):
                    continue
                if not any(
                    isinstance(target, ast.Name) and target.id == "__all__"
                    for target in node.targets
                ):
                    continue
                return tuple(
                    value.value
                    for value in node.value.elts
                    if isinstance(value, ast.Constant) and isinstance(value.value, str)
                )
            self.fail("module does not define __all__")

        self.assertEqual(exports_from(stub_module), exports_from(runtime_init_module))

    def test_manyfold_modules_keep_imports_at_module_scope(self) -> None:
        package_path = Path(__file__).resolve().parents[1] / "python" / "manyfold"
        violations: list[str] = []

        for module_path in sorted(package_path.rglob("*.py")):
            module = ast.parse(module_path.read_text(encoding="utf-8"))
            parents = {
                child: parent
                for parent in ast.walk(module)
                for child in ast.iter_child_nodes(parent)
            }
            for node in ast.walk(module):
                if not isinstance(node, ast.Import | ast.ImportFrom):
                    continue
                parent = parents.get(node)
                while parent is not None:
                    if isinstance(parent, ast.FunctionDef | ast.AsyncFunctionDef):
                        relative = module_path.relative_to(package_path.parent)
                        violations.append(f"{relative}:{node.lineno}")
                        break
                    parent = parents.get(parent)

        self.assertEqual(violations, [])

    def test_reference_example_suite_declares_all_rfc_examples(self) -> None:
        manyfold = load_manyfold_package()
        suite = manyfold.reference_example_suite()

        self.assertEqual(
            tuple(example.number for example in suite), REFERENCE_EXAMPLE_NUMBERS
        )
        self.assertEqual(sum(example.implemented for example in suite), 10)

    def test_reference_example_suite_export_matches_function_result(self) -> None:
        manyfold = load_manyfold_package()

        self.assertIs(
            manyfold.REFERENCE_EXAMPLE_SUITE, manyfold.reference_example_suite()
        )

    def test_reference_examples_module_exports_cached_suite_symbols(self) -> None:
        manyfold = load_manyfold_package()
        reference_examples = sys.modules["manyfold.reference_examples"]

        self.assertEqual(
            tuple(reference_examples.__all__),
            (
                "REFERENCE_EXAMPLE_SUITE",
                "ReferenceExample",
                "implemented_reference_examples",
                "reference_example_suite",
            ),
        )
        self.assertIs(
            reference_examples.implemented_reference_examples(),
            manyfold.implemented_reference_examples(),
        )

    def test_reference_example_suite_uses_shared_example_catalog(self) -> None:
        manyfold = load_manyfold_package()

        self.assertEqual(
            tuple(
                example.module_name
                for example in manyfold.implemented_reference_examples()
            ),
            tuple(entry.module_name for entry in REFERENCE_EXAMPLE_ENTRIES),
        )

    def test_implemented_reference_examples_preserve_reference_catalog_order(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        self.assertEqual(
            tuple(
                example.number for example in manyfold.implemented_reference_examples()
            ),
            tuple(entry.reference_number for entry in REFERENCE_EXAMPLE_ENTRIES),
        )

    def test_reference_example_suite_preserves_catalog_module_names_and_gaps(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        suite_by_number = self._suite_by_number(manyfold)

        for number in REFERENCE_EXAMPLE_NUMBERS:
            metadata = reference_example_metadata(number)
            example = suite_by_number[number]
            with self.subTest(number=number):
                if hasattr(metadata, "module_name"):
                    self.assertEqual(example.module_name, metadata.module_name)
                    self.assertTrue(example.implemented)
                    self.assertIsNotNone(example.runner)
                else:
                    self.assertIsNone(example.module_name)
                    self.assertFalse(example.implemented)
                    self.assertIsNone(example.runner)

    def test_reference_example_entries_only_include_supported_numbered_examples(
        self,
    ) -> None:
        self.assertEqual(
            REFERENCE_EXAMPLE_ENTRIES,
            tuple(
                entry
                for entry in SUPPORTED_EXAMPLE_ENTRIES
                if entry.is_reference_example
            ),
        )

    def test_supported_and_archived_entry_manifests_partition_catalog(self) -> None:
        self.assertEqual(
            SUPPORTED_EXAMPLE_ENTRIES + ARCHIVED_EXAMPLE_ENTRIES, EXAMPLE_CATALOG
        )

    def test_examples_package_re_exports_reference_entries(self) -> None:
        examples_package = __import__("examples")

        self.assertIs(
            examples_package.REFERENCE_EXAMPLE_ENTRIES, REFERENCE_EXAMPLE_ENTRIES
        )

    def test_reference_example_suite_preserves_catalog_titles_and_summaries(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        suite_by_number = self._suite_by_number(manyfold)

        for entry in EXAMPLE_CATALOG:
            if (
                entry.archived
                or entry.reference_number is None
                or entry.reference_title is None
            ):
                continue
            with self.subTest(number=entry.reference_number):
                example = suite_by_number[entry.reference_number]
                self.assertEqual(example.title, entry.reference_title)
                self.assertEqual(example.summary, entry.summary)
                self.assertEqual(example.module_name, entry.module_name)

    def test_new_reference_examples_are_marked_implemented_with_runners(self) -> None:
        manyfold = load_manyfold_package()
        suite_by_number = self._suite_by_number(manyfold)

        for number in (2, 7, 9, 10):
            with self.subTest(number=number):
                example = suite_by_number[number]
                self.assertTrue(example.implemented)
                self.assertIsNotNone(example.runner)

    def test_reference_example_suite_marks_raft_demo_as_implemented(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        example = self._suite_by_number(manyfold)[9]

        self.assertEqual(REFERENCE_EXAMPLE_GAPS, ())
        self.assertEqual(example.title, "Raft demo")
        self.assertTrue(example.implemented)
        self.assertEqual(example.module_name, "raft_demo")
        self.assertIsNotNone(example.runner)

    def test_reference_example_suite_does_not_eagerly_import_example_modules(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        manyfold.reference_example_suite()

        self.assertNotIn("examples.mailbox_bridge", sys.modules)
        self.assertNotIn("examples.cross_partition_join", sys.modules)

    def test_implemented_reference_examples_do_not_import_modules_until_runner_invoked(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        examples = manyfold.implemented_reference_examples()

        self.assertEqual(len(examples), 10)
        self.assertNotIn("examples.uart_temperature_sensor", sys.modules)
        self.assertNotIn("examples.ephemeral_entropy_stream", sys.modules)

    def test_reference_example_runner_imports_only_target_module_when_called(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        suite_by_number = self._suite_by_number(manyfold)

        lidar = suite_by_number[3]
        assert lidar.runner is not None
        lidar.runner()

        self.assertIn("examples.lazy_lidar_payload", sys.modules)
        self.assertNotIn("examples.ephemeral_entropy_stream", sys.modules)

    def test_reference_example_runner_uses_captured_import_path(
        self,
    ) -> None:
        load_manyfold_package()
        reference_examples = sys.modules["manyfold.reference_examples"]
        fake_module_name = "examples.synthetic_runner_target"
        fake_module = types.ModuleType(fake_module_name)
        fake_module.run_example = lambda: {"synthetic": True}

        with mock.patch.dict(sys.modules, {fake_module_name: fake_module}):
            runner = reference_examples._example_runner(fake_module_name)

            self.assertEqual(runner(), {"synthetic": True})

    def test_reference_example_runner_does_not_requery_catalog_at_call_time(
        self,
    ) -> None:
        load_manyfold_package()
        reference_examples = sys.modules["manyfold.reference_examples"]
        fake_module_name = "examples.synthetic_runner_target_args"
        fake_module = types.ModuleType(fake_module_name)
        fake_module.run_example = lambda: {"synthetic": "args"}

        with mock.patch.dict(sys.modules, {fake_module_name: fake_module}):
            runner = reference_examples._example_runner(fake_module_name)
            reference_examples.reference_example_metadata = mock.Mock(
                side_effect=AssertionError("catalog should not be queried by runner")
            )

            self.assertEqual(runner(), {"synthetic": "args"})

    def test_reference_example_suite_runners_cover_recent_example_result_fields(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        suite_by_number = self._suite_by_number(manyfold)

        lidar = suite_by_number[3]
        counter = suite_by_number[4]
        assert lidar.runner is not None
        assert counter.runner is not None

        lidar_result = lidar.runner()
        counter_result = counter.runner()

        self.assertEqual(lidar_result["matched_frames"], ("frame-2:open",))
        self.assertEqual(lidar_result["payload_open_requests"], 1)
        self.assertEqual(lidar_result["lazy_source_opens"], 1)
        self.assertEqual(lidar_result["unopened_lazy_payloads"], 1)
        self.assertEqual(counter_result["ack_latest"], b"ok")
        self.assertEqual(counter_result["coherence_taints"], ("COHERENCE_STABLE",))

    def test_recent_reference_example_runners_return_expected_results(self) -> None:
        manyfold = load_manyfold_package()
        suite_by_number = self._suite_by_number(manyfold)

        imu_fusion = suite_by_number[2]
        cross_partition = suite_by_number[7]
        raft_demo = suite_by_number[9]
        entropy = suite_by_number[10]
        assert imu_fusion.runner is not None
        assert cross_partition.runner is not None
        assert raft_demo.runner is not None
        assert entropy.runner is not None

        self.assertEqual(
            imu_fusion.runner(),
            {
                "fused_pairs": ((100, 7), (101, 7), (100, 8), (101, 8)),
                "latest_pose": (101, 8),
            },
        )
        self.assertEqual(
            cross_partition.runner(),
            {
                "join_class": "repartition",
                "visible_nodes": (
                    "state.internal.imu_fusion.join.left_repartition.state.v1",
                    "state.internal.imu_fusion.join.right_repartition.state.v1",
                ),
                "topology_edges": (
                    (
                        "read.logical.imu_left.sensor.accel.meta.v1",
                        "state.internal.imu_fusion.join.left_repartition.state.v1",
                    ),
                    (
                        "read.logical.imu_right.sensor.gyro.meta.v1",
                        "state.internal.imu_fusion.join.right_repartition.state.v1",
                    ),
                ),
                "taint_implications": ("deterministic_rekey",),
            },
        )
        self.assertEqual(
            raft_demo.runner(),
            {
                "leader_state": ("node-a", 3, True),
                "quorum_state": (3, "node-a", ("node-a", "node-b"), True),
                "replicated_log": (
                    (1, "set mode=auto"),
                    (2, "set temp=21"),
                ),
            },
        )
        self.assertEqual(
            entropy.runner(),
            {
                "latest_payload": b"nonce-1",
                "replay_count": 0,
                "determinism_taints": ("DET_NONREPLAYABLE",),
                "latest_replay_policy": "none",
            },
        )

    def test_implemented_reference_example_runners_match_direct_example_modules(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        for example in manyfold.implemented_reference_examples():
            assert example.runner is not None
            assert example.module_name is not None
            with self.subTest(number=example.number, module=example.module_name):
                self.assertEqual(
                    example.runner(),
                    load_example_module(example.module_name).run_example(),
                )

    def test_reference_example_runner_metadata_for_raft_demo(self) -> None:
        manyfold = load_manyfold_package()
        raft_demo = self._suite_by_number(manyfold)[9]

        self.assertTrue(raft_demo.implemented)
        self.assertEqual(raft_demo.module_name, "raft_demo")
        self.assertIsNotNone(raft_demo.runner)

    def test_implemented_reference_examples_run(self) -> None:
        manyfold = load_manyfold_package()
        for example in manyfold.implemented_reference_examples():
            with self.subTest(example=example.title):
                self.assertIsNotNone(example.runner)
                example.runner()


if __name__ == "__main__":
    unittest.main()
