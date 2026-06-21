from __future__ import annotations

import json
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock

from manyfold import memory_benchmarks


class MemoryBenchmarkTests(unittest.TestCase):
    def test_benchmark_sample_records_are_slotted(self) -> None:
        records = (
            _rss_sample(0.0, current_rss_kib=33_000),
            _sample(8, traced_current_bytes=100),
            memory_benchmarks._RssSampleSeries(),
            memory_benchmarks._UsageSnapshot(
                cpu_seconds=0.0,
                input_blocks=0,
                output_blocks=0,
            ),
        )

        for record in records:
            with self.subTest(record=type(record).__name__):
                self.assertFalse(hasattr(record, "__dict__"))

    def test_rss_sample_series_materializes_rows_on_demand(self) -> None:
        samples = memory_benchmarks._RssSampleSeries()
        samples.append(elapsed_seconds=0.0, current_rss_kib=32_000)
        samples.append(
            elapsed_seconds=1.0,
            current_rss_kib=32_064,
            private_kib=10_000,
            fd_count=42,
        )
        samples.append(elapsed_seconds=2.0, current_rss_kib=32_064)

        self.assertEqual(len(samples), 3)
        self.assertEqual(samples[1].private_kib, 10_000)
        self.assertEqual(samples[-1].elapsed_seconds, 2.0)
        self.assertEqual(
            [sample.current_rss_kib for sample in samples[1:]],
            [32_064, 32_064],
        )
        self.assertEqual(tuple(sample.fd_count for sample in samples), (None, 42, None))
        memory_benchmarks._check_external_rss_plateau(
            samples,
            warmup_fraction=0.0,
            plateau_kib=64,
        )

    def test_runtime_trace_filter_excludes_benchmark_overhead_filenames(self) -> None:
        excluded = (
            "<frozen abc>",
            memory_benchmarks._BENCHMARK_TRACE_FILENAME,
            "/opt/python/lib/python3.12/fnmatch.py",
            "/opt/python/lib/python3.12/re/_compiler.py",
            "/opt/python/lib/python3.12/subprocess.py",
            "/opt/python/lib/python3.12/tracemalloc.py",
        )

        for filename in excluded:
            with self.subTest(filename=filename):
                self.assertTrue(
                    memory_benchmarks._trace_filename_is_benchmark_overhead(filename)
                )
        self.assertFalse(
            memory_benchmarks._trace_filename_is_benchmark_overhead(
                "/workspace/manyfold/python/manyfold/graph.py"
            )
        )

    def test_projected_growth_uses_steady_tail_slope(self) -> None:
        samples = (
            _sample(10, traced_current_bytes=10_000),
            _sample(20, traced_current_bytes=10_100),
            _sample(30, traced_current_bytes=10_104),
        )

        projected = memory_benchmarks._projected_growth_over_events(
            samples,
            warmup_fraction=0.34,
            project_events=1_000,
            value=lambda sample: sample.traced_current_bytes,
        )

        self.assertEqual(projected, 400)

    def test_projected_traced_growth_rejects_material_billion_event_slope(
        self,
    ) -> None:
        samples = (
            _sample(100_000, traced_current_bytes=10_000),
            _sample(200_000, traced_current_bytes=20_000),
            _sample(300_000, traced_current_bytes=30_000),
        )

        with self.assertRaisesRegex(
            SystemExit,
            "tracemalloc current bytes project to",
        ):
            memory_benchmarks._check_projected_traced_growth(
                samples,
                warmup_fraction=0.0,
                project_events=1_000_000_000,
                limit_bytes=1_000_000,
            )

    def test_segment_projected_traced_growth_rejects_late_resumed_slope(
        self,
    ) -> None:
        samples = (
            _sample(100_000, traced_current_bytes=10_000),
            _sample(200_000, traced_current_bytes=10_000),
            _sample(300_000, traced_current_bytes=10_000),
            _sample(400_000, traced_current_bytes=11_000),
        )

        with self.assertRaisesRegex(
            SystemExit,
            "tracemalloc current byte segment projects to",
        ):
            memory_benchmarks._check_segment_projected_traced_growth(
                samples,
                warmup_fraction=0.0,
                project_events=1_000_000_000,
                limit_bytes=1_000_000,
            )

    def test_projected_rss_growth_allows_flat_tail(self) -> None:
        samples = (
            _sample(100_000, current_rss_kib=33_000),
            _sample(200_000, current_rss_kib=33_128),
            _sample(300_000, current_rss_kib=33_128),
        )

        memory_benchmarks._check_projected_rss_growth(
            samples,
            warmup_fraction=0.34,
            project_events=1_000_000_000,
            limit_kib=1,
        )

    def test_external_projected_rss_growth_rejects_material_slope(self) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=33_000),
            _rss_sample(5.0, current_rss_kib=33_500),
            _rss_sample(10.0, current_rss_kib=34_000),
        )

        with self.assertRaisesRegex(SystemExit, "external RSS projects to"):
            memory_benchmarks._check_external_projected_rss_growth(
                samples,
                warmup_fraction=0.0,
                project_seconds=60.0,
                limit_kib=1_000,
            )

    def test_external_segment_projected_rss_growth_rejects_late_slope(self) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=33_000),
            _rss_sample(5.0, current_rss_kib=33_000),
            _rss_sample(10.0, current_rss_kib=33_500),
        )

        with self.assertRaisesRegex(SystemExit, "external RSS segment projects to"):
            memory_benchmarks._check_external_segment_projected_rss_growth(
                samples,
                warmup_fraction=0.0,
                project_seconds=60.0,
                limit_kib=1_000,
            )

    def test_external_segment_projected_private_growth_rejects_late_slope(
        self,
    ) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=33_000, private_kib=10_000),
            _rss_sample(5.0, current_rss_kib=33_000, private_kib=10_000),
            _rss_sample(10.0, current_rss_kib=33_000, private_kib=10_500),
        )

        with self.assertRaisesRegex(
            SystemExit,
            "external private memory segment projects to",
        ):
            memory_benchmarks._check_external_segment_projected_memory_growth(
                samples,
                warmup_fraction=0.0,
                project_seconds=60.0,
                limit_kib=1_000,
                label="private memory",
                value=lambda sample: sample.private_kib,
            )

    def test_external_segment_projected_pss_growth_rejects_late_slope(
        self,
    ) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=33_000, pss_kib=20_000),
            _rss_sample(5.0, current_rss_kib=33_000, pss_kib=20_000),
            _rss_sample(10.0, current_rss_kib=33_000, pss_kib=20_500),
        )

        with self.assertRaisesRegex(SystemExit, "external PSS segment projects to"):
            memory_benchmarks._check_external_segment_projected_memory_growth(
                samples,
                warmup_fraction=0.0,
                project_seconds=60.0,
                limit_kib=1_000,
                label="PSS",
                value=lambda sample: sample.pss_kib,
            )

    def test_external_segment_projected_anonymous_growth_rejects_late_slope(
        self,
    ) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=33_000, anonymous_kib=8_000),
            _rss_sample(5.0, current_rss_kib=33_000, anonymous_kib=8_000),
            _rss_sample(10.0, current_rss_kib=33_000, anonymous_kib=8_500),
        )

        with self.assertRaisesRegex(
            SystemExit,
            "external anonymous memory segment projects to",
        ):
            memory_benchmarks._check_external_segment_projected_memory_growth(
                samples,
                warmup_fraction=0.0,
                project_seconds=60.0,
                limit_kib=1_000,
                label="anonymous memory",
                value=lambda sample: sample.anonymous_kib,
            )

    def test_final_tail_plateau_ignores_startup_growth(self) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=30_000),
            _rss_sample(1.0, current_rss_kib=250_000),
            _rss_sample(2.0, current_rss_kib=360_000),
            _rss_sample(3.0, current_rss_kib=360_016),
            _rss_sample(4.0, current_rss_kib=360_000),
            _rss_sample(5.0, current_rss_kib=360_032),
        )

        tail = memory_benchmarks._final_tail_plateau(
            samples,
            plateau=64,
            value=lambda sample: sample.current_rss_kib,
        )

        self.assertEqual(tail["range"], 32)
        self.assertEqual(tail["sample_count"], 4)
        self.assertEqual(tail["seconds"], 3.0)

    def test_probe_rss_tail_plateau_ignores_startup_growth(self) -> None:
        samples = (
            _sample(8, current_rss_kib=30_000),
            _sample(25_000, current_rss_kib=34_000),
            _sample(50_000, current_rss_kib=34_064),
            _sample(75_000, current_rss_kib=34_032),
        )

        tail = memory_benchmarks._final_probe_tail_plateau(
            samples,
            plateau=128,
            value=lambda sample: sample.current_rss_kib,
        )

        self.assertEqual(tail["range"], 64)
        self.assertEqual(tail["sample_count"], 3)
        memory_benchmarks._check_probe_tail_plateau(
            samples,
            plateau=128,
            min_samples=3,
            label="current RSS",
            value=lambda sample: sample.current_rss_kib,
        )

    def test_probe_rss_tail_plateau_rejects_short_tail(self) -> None:
        samples = (
            _sample(8, current_rss_kib=30_000),
            _sample(25_000, current_rss_kib=34_000),
            _sample(50_000, current_rss_kib=34_512),
            _sample(75_000, current_rss_kib=34_576),
        )

        with self.assertRaisesRegex(SystemExit, "final tail plateau collected 2"):
            memory_benchmarks._check_probe_tail_plateau(
                samples,
                plateau=128,
                min_samples=3,
                label="current RSS",
                value=lambda sample: sample.current_rss_kib,
            )

    def test_external_tail_plateau_gates_reject_short_tail(self) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=30_000),
            _rss_sample(1.0, current_rss_kib=250_000),
            _rss_sample(2.0, current_rss_kib=360_000),
            _rss_sample(3.0, current_rss_kib=360_016),
        )
        metrics = memory_benchmarks._external_monitor_metric_summary(
            samples,
            warmup_fraction=0.5,
            project_seconds=60.0,
        )

        gates = memory_benchmarks._external_monitor_gate_summary(
            samples,
            metrics,
            Namespace(
                external_anonymous_plateau_kib=None,
                external_anonymous_projected_growth_kib=None,
                external_anonymous_segment_projected_growth_kib=None,
                external_fd_plateau_count=None,
                external_fd_projected_growth_count=None,
                external_fd_segment_projected_growth_count=None,
                external_min_elapsed_seconds=None,
                external_min_samples=None,
                external_private_plateau_kib=None,
                external_private_projected_growth_kib=None,
                external_private_segment_projected_growth_kib=None,
                external_pss_plateau_kib=None,
                external_pss_projected_growth_kib=None,
                external_pss_segment_projected_growth_kib=None,
                external_rss_projected_growth_kib=None,
                external_rss_segment_projected_growth_kib=None,
                external_rss_tail_plateau_kib=64,
                external_tail_min_samples=3,
                external_tail_min_seconds=2.0,
                max_cpu_seconds=None,
                max_disk_input_blocks=None,
                max_disk_output_blocks=None,
                max_elapsed_seconds=None,
                rss_plateau_kib=1_000_000,
            ),
            elapsed_seconds=3.0,
            usage=memory_benchmarks._UsageSnapshot(0.0, 0, 0),
        )

        gate_by_name = {gate["name"]: gate for gate in gates}
        self.assertTrue(gate_by_name["external_rss_tail_plateau_kib"]["passed"])
        self.assertFalse(
            gate_by_name["external_rss_tail_plateau_seconds"]["passed"]
        )
        self.assertFalse(
            gate_by_name["external_rss_tail_plateau_samples"]["passed"]
        )

    def test_external_tail_plateau_check_rejects_short_tail(self) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=30_000),
            _rss_sample(1.0, current_rss_kib=250_000),
            _rss_sample(2.0, current_rss_kib=360_000),
            _rss_sample(3.0, current_rss_kib=360_016),
        )

        with self.assertRaisesRegex(SystemExit, "final tail plateau lasted"):
            memory_benchmarks._check_external_tail_plateau(
                samples,
                plateau=64,
                min_seconds=2.0,
                min_samples=3,
                label="RSS",
                value=lambda sample: sample.current_rss_kib,
            )

    def test_external_segment_projected_fd_growth_rejects_late_slope(self) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=33_000, fd_count=40),
            _rss_sample(5.0, current_rss_kib=33_000, fd_count=40),
            _rss_sample(10.0, current_rss_kib=33_000, fd_count=45),
        )

        with self.assertRaisesRegex(
            SystemExit,
            "external file descriptors segment projects to",
        ):
            memory_benchmarks._check_external_segment_projected_count_growth(
                samples,
                warmup_fraction=0.0,
                project_seconds=60.0,
                limit_count=10,
                label="file descriptors",
                value=lambda sample: sample.fd_count,
            )

    def test_external_projected_fd_growth_requires_available_samples(self) -> None:
        samples = (
            _rss_sample(0.0, current_rss_kib=33_000),
            _rss_sample(5.0, current_rss_kib=33_000),
        )

        with self.assertRaisesRegex(
            SystemExit,
            "no external file descriptors samples collected",
        ):
            memory_benchmarks._check_external_projected_count_growth(
                samples,
                warmup_fraction=0.0,
                project_seconds=60.0,
                limit_count=0,
                label="file descriptors",
                value=lambda sample: sample.fd_count,
            )

    def test_process_tree_rss_sums_descendants(self) -> None:
        rows = (
            (10, 1, 100),
            (11, 10, 50),
            (12, 11, 25),
            (13, 99, 1_000),
        )

        rss = memory_benchmarks._process_tree_rss_from_rows(10, rows)

        self.assertEqual(rss, 175)

    def test_process_tree_rss_rejects_missing_root(self) -> None:
        rows = ((11, 10, 50),)

        with self.assertRaisesRegex(ValueError, "process 10 not found"):
            memory_benchmarks._process_tree_rss_from_rows(10, rows)

    def test_process_tree_fd_count_sums_descendants(self) -> None:
        rows = (
            (10, 1, 100),
            (11, 10, 50),
            (12, 11, 25),
            (13, 99, 1_000),
        )
        by_pid = {10: 4, 11: 5, 12: 6, 13: 1_000}

        count = memory_benchmarks._process_tree_fd_count_from_rows(
            10,
            rows,
            by_pid.get,
        )

        self.assertEqual(count, 15)

    def test_parse_smaps_rollup_reads_private_pss_and_anonymous(self) -> None:
        memory = memory_benchmarks._parse_smaps_rollup(
            "\n".join(
                (
                    "Rss:                120 kB",
                    "Pss:                 90 kB",
                    "Private_Clean:       10 kB",
                    "Private_Dirty:       25 kB",
                    "Anonymous:           40 kB",
                )
            )
        )

        self.assertEqual(memory.rss_kib, 120)
        self.assertEqual(memory.pss_kib, 90)
        self.assertEqual(memory.private_kib, 35)
        self.assertEqual(memory.anonymous_kib, 40)

    def test_process_tree_memory_sums_smaps_descendants(self) -> None:
        rows = (
            (10, 1, 100),
            (11, 10, 50),
            (12, 11, 25),
            (13, 99, 1_000),
        )
        by_pid = {
            10: memory_benchmarks._ProcessMemory(
                rss_kib=100,
                pss_kib=90,
                private_kib=70,
                anonymous_kib=60,
            ),
            11: memory_benchmarks._ProcessMemory(
                rss_kib=50,
                pss_kib=40,
                private_kib=30,
                anonymous_kib=20,
            ),
            12: memory_benchmarks._ProcessMemory(
                rss_kib=25,
                pss_kib=20,
                private_kib=10,
                anonymous_kib=5,
            ),
            13: memory_benchmarks._ProcessMemory(
                rss_kib=1_000,
                pss_kib=1_000,
                private_kib=1_000,
                anonymous_kib=1_000,
            ),
        }

        memory = memory_benchmarks._process_tree_memory_from_rows(
            10,
            rows,
            by_pid.get,
        )

        self.assertIsNotNone(memory)
        assert memory is not None
        self.assertEqual(memory.rss_kib, 175)
        self.assertEqual(memory.pss_kib, 150)
        self.assertEqual(memory.private_kib, 110)
        self.assertEqual(memory.anonymous_kib, 85)

    def test_external_tree_termination_starts_new_session_on_posix(self) -> None:
        kwargs = memory_benchmarks._external_popen_kwargs("tree")

        if memory_benchmarks.os.name == "posix":
            self.assertEqual(kwargs, {"start_new_session": True})
        else:
            self.assertEqual(kwargs, {})

    def test_external_process_termination_keeps_current_session(self) -> None:
        self.assertEqual(memory_benchmarks._external_popen_kwargs("process"), {})

    def test_external_tree_termination_falls_back_on_permission_error(self) -> None:
        class Process:
            pid = 123

            def __init__(self) -> None:
                self.terminated = False

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                raise AssertionError("SIGTERM fallback should terminate")

        process = Process()

        with (
            mock.patch.object(memory_benchmarks.os, "name", "posix"),
            mock.patch.object(
                memory_benchmarks.os,
                "killpg",
                side_effect=PermissionError,
            ),
        ):
            memory_benchmarks._terminate_external_process(
                process, scope="tree", sig=memory_benchmarks.signal.SIGTERM
            )

        self.assertTrue(process.terminated)

    def test_external_popen_options_include_cwd_and_env(self) -> None:
        options = memory_benchmarks._external_popen_options(
            Namespace(
                external_cwd="/tmp/heart",
                external_env={"PYTHONPATH": "/tmp/manyfold/python"},
            ),
            "process",
        )

        self.assertEqual(options["cwd"], "/tmp/heart")
        self.assertEqual(options["env"], {"PYTHONPATH": "/tmp/manyfold/python"})

    def test_external_monitor_summary_writes_samples_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.json"
            memory_benchmarks._write_external_monitor_summary(
                path,
                check_enabled=True,
                command=("totem", "run", "--configuration", "lib_2026"),
                gate_args=Namespace(
                    external_anonymous_plateau_kib=1_000,
                    external_anonymous_projected_growth_kib=None,
                    external_anonymous_segment_projected_growth_kib=None,
                    external_fd_plateau_count=1,
                    external_fd_projected_growth_count=None,
                    external_fd_segment_projected_growth_count=None,
                    external_min_elapsed_seconds=1.0,
                    external_min_samples=2,
                    external_private_plateau_kib=600,
                    external_private_projected_growth_kib=None,
                    external_private_segment_projected_growth_kib=None,
                    external_project_seconds=100.0,
                    external_pss_plateau_kib=None,
                    external_pss_projected_growth_kib=None,
                    external_pss_segment_projected_growth_kib=None,
                    external_rss_projected_growth_kib=20_000,
                    external_rss_segment_projected_growth_kib=20_000,
                    max_cpu_seconds=2.0,
                    max_disk_input_blocks=0,
                    max_disk_output_blocks=0,
                    max_elapsed_seconds=3.0,
                    rss_plateau_kib=2_000,
                    rss_warmup_fraction=0.0,
                ),
                metadata={
                    "manyfold_bridge_version": "0.1.38",
                    "manyfold_source": "/tmp/manyfold/python/manyfold/__init__.py",
                },
                samples=(
                    _rss_sample(
                        0.0,
                        current_rss_kib=32_000,
                        pss_kib=19_500,
                        private_kib=9_500,
                        anonymous_kib=7_500,
                        fd_count=40,
                    ),
                    _rss_sample(
                        10.0,
                        current_rss_kib=33_000,
                        pss_kib=20_000,
                        private_kib=10_000,
                        anonymous_kib=8_000,
                        fd_count=42,
                    ),
                ),
                elapsed_seconds=2.0,
                usage=memory_benchmarks._UsageSnapshot(
                    cpu_seconds=1.25,
                    input_blocks=0,
                    output_blocks=0,
                ),
                project_seconds=100.0,
                returncode=0,
                terminated_by_monitor=False,
                warmup_fraction=0.0,
                output_max_samples=512,
            )

            summary = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(
            summary["command"],
            ["totem", "run", "--configuration", "lib_2026"],
        )
        self.assertEqual(summary["status"]["returncode"], 0)
        self.assertEqual(summary["status"]["cpu_seconds"], 1.25)
        self.assertEqual(
            summary["metadata"]["manyfold_source"],
            "/tmp/manyfold/python/manyfold/__init__.py",
        )
        self.assertEqual(summary["metadata"]["manyfold_bridge_version"], "0.1.38")
        self.assertEqual(summary["samples"][1]["current_rss_kib"], 33_000)
        self.assertEqual(summary["samples"][1]["private_kib"], 10_000)
        self.assertEqual(summary["samples"][1]["fd_count"], 42)
        self.assertEqual(summary["metrics"]["rss_kib"]["steady_range"], 1_000)
        self.assertEqual(summary["metrics"]["rss_kib"]["projected_growth"], 10_000)
        self.assertEqual(
            summary["metrics"]["rss_kib"]["segment_projected_growth"],
            10_000,
        )
        self.assertEqual(summary["metrics"]["private_kib"]["steady_range"], 500)
        self.assertEqual(summary["metrics"]["fd_count"]["steady_range"], 2)
        gates = {gate["name"]: gate for gate in summary["gates"]}
        self.assertTrue(summary["checks_enabled"])
        self.assertFalse(summary["passed"])
        self.assertEqual(summary["failed_gates"], ["external_fd_plateau_count"])
        self.assertEqual(summary["unavailable_gates"], [])
        self.assertTrue(gates["external_command_status"]["passed"])
        self.assertTrue(gates["external_rss_plateau_kib"]["passed"])
        self.assertTrue(gates["external_rss_projected_growth_kib"]["passed"])
        self.assertTrue(gates["external_private_plateau_kib"]["passed"])
        self.assertFalse(gates["external_fd_plateau_count"]["passed"])
        self.assertEqual(gates["external_fd_plateau_count"]["observed"], 2)
        self.assertTrue(gates["max_cpu_seconds"]["passed"])

    def test_external_monitor_summary_retains_bounded_sample_tail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.json"
            memory_benchmarks._write_external_monitor_summary(
                path,
                check_enabled=False,
                command=("totem", "run", "--configuration", "lib_2026"),
                samples=tuple(
                    _rss_sample(float(index), current_rss_kib=32_000 + index)
                    for index in range(5)
                ),
                elapsed_seconds=5.0,
                usage=memory_benchmarks._UsageSnapshot(
                    cpu_seconds=1.0,
                    input_blocks=0,
                    output_blocks=0,
                ),
                project_seconds=100.0,
                returncode=0,
                terminated_by_monitor=False,
                warmup_fraction=0.0,
                output_max_samples=2,
            )

            summary = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(summary["sample_count"], 5)
        self.assertEqual(summary["retained_sample_count"], 2)
        self.assertEqual(summary["sample_retention_limit"], 2)
        self.assertEqual(
            [sample["current_rss_kib"] for sample in summary["samples"]],
            [32_003, 32_004],
        )
        self.assertEqual(summary["metrics"]["rss_kib"]["sample_count"], 5)

    def test_elapsed_seconds_rejects_slow_probe(self) -> None:
        samples = (_sample(100, elapsed_seconds=12.5),)

        with self.assertRaisesRegex(SystemExit, "benchmark elapsed time"):
            memory_benchmarks._check_elapsed_seconds(samples, max_seconds=10.0)

    def test_cpu_seconds_rejects_slow_probe(self) -> None:
        samples = (_sample(100, cpu_seconds=12.5),)

        with self.assertRaisesRegex(SystemExit, "benchmark CPU time"):
            memory_benchmarks._check_cpu_seconds(samples, max_seconds=10.0)

    def test_average_event_latency_rejects_slow_probe(self) -> None:
        samples = (_sample(100, average_event_us=125.0),)

        with self.assertRaisesRegex(SystemExit, "average event latency"):
            memory_benchmarks._check_average_event_latency(samples, max_us=100.0)

    def test_interval_event_latency_rejects_late_slow_sample(self) -> None:
        samples = (
            _sample(100, interval_event_us=10.0),
            _sample(200, interval_event_us=125.0),
        )

        with self.assertRaisesRegex(SystemExit, "sampled interval event latency"):
            memory_benchmarks._check_interval_event_latency(samples, max_us=100.0)

    def test_block_io_rejects_disk_writes(self) -> None:
        samples = (_sample(100, output_blocks=1),)

        with self.assertRaisesRegex(SystemExit, "benchmark block output"):
            memory_benchmarks._check_block_io(
                samples,
                max_input_blocks=None,
                max_output_blocks=0,
            )

    def test_external_cpu_seconds_rejects_slow_child(self) -> None:
        usage = memory_benchmarks._UsageSnapshot(
            cpu_seconds=2.0,
            input_blocks=0,
            output_blocks=0,
        )

        with self.assertRaisesRegex(SystemExit, "external command CPU time"):
            memory_benchmarks._check_external_cpu_seconds(
                usage,
                max_seconds=1.0,
            )

    def test_external_min_elapsed_rejects_short_proof_run(self) -> None:
        with self.assertRaisesRegex(SystemExit, "below required"):
            memory_benchmarks._check_external_min_elapsed_seconds(
                5.0,
                min_seconds=60.0,
            )

    def test_external_min_samples_rejects_short_proof_run(self) -> None:
        samples = (_rss_sample(0.0, current_rss_kib=33_000),)

        with self.assertRaisesRegex(SystemExit, "below required 3"):
            memory_benchmarks._check_external_min_samples(samples, min_samples=3)

    def test_external_block_io_rejects_child_disk_writes(self) -> None:
        usage = memory_benchmarks._UsageSnapshot(
            cpu_seconds=0.0,
            input_blocks=0,
            output_blocks=1,
        )

        with self.assertRaisesRegex(SystemExit, "external command block output"):
            memory_benchmarks._check_external_block_io(
                usage,
                max_input_blocks=None,
                max_output_blocks=0,
            )

    def test_materialized_payload_count_rejects_sparse_payload_retention(self) -> None:
        samples = (_sample(100, materialized_payload_count=1),)

        with self.assertRaisesRegex(SystemExit, "materialized Python payloads"):
            memory_benchmarks._check_materialized_payload_count(samples, max_count=0)

    def test_retained_count_check_rejects_unexpected_correlation_index(self) -> None:
        samples = (
            _sample(
                100,
                lineage_count=8,
                correlation_index_count=1,
            ),
        )

        with self.assertRaisesRegex(SystemExit, "correlation index retained"):
            memory_benchmarks._check_retained_counts_for_lineage(
                samples,
                8,
                8,
                0,
                0,
            )

    def test_run_probe_supports_unrelated_topology_edges(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            unrelated_edges=4,
        )

        self.assertEqual(samples[-1].step, 8)
        self.assertEqual(samples[-1].replay_count, 4)
        self.assertEqual(samples[-1].payload_count, 4)
        self.assertEqual(samples[-1].lineage_count, 0)

    def test_run_probe_supports_nowait_publish_mode(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            publish_mode="nowait",
        )

        self.assertEqual(samples[-1].step, 8)
        self.assertEqual(samples[-1].replay_count, 4)
        self.assertEqual(samples[-1].payload_count, 4)
        self.assertEqual(samples[-1].materialized_payload_count, 0)

    def test_run_probe_sparse_metadata_none_avoids_lineage_indexes(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            metadata_mode="none",
            publish_mode="nowait",
        )

        self.assertEqual(samples[-1].step, 8)
        self.assertEqual(samples[-1].replay_count, 4)
        self.assertEqual(samples[-1].payload_count, 4)
        self.assertEqual(samples[-1].lineage_count, 0)
        self.assertEqual(samples[-1].subscriber_metadata_count, 0)
        self.assertEqual(samples[-1].materialized_payload_count, 0)

    def test_run_probe_supports_process_local_nowait_publish_mode(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            payload_schema="any",
            publish_mode="nowait",
        )

        self.assertEqual(samples[-1].step, 8)
        self.assertEqual(samples[-1].replay_count, 4)
        self.assertEqual(samples[-1].payload_count, 4)
        self.assertEqual(samples[-1].materialized_payload_count, 4)

    def test_run_probe_supports_materialized_state_lineage_path(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            lineage_retention_policy="retained",
            materialize_state=True,
        )

        self.assertEqual(samples[-1].step, 8)
        self.assertEqual(samples[-1].replay_count, 4)
        self.assertEqual(samples[-1].payload_count, 4)
        self.assertEqual(samples[-1].lineage_count, 4)
        self.assertEqual(samples[-1].correlation_index_count, 0)
        self.assertEqual(samples[-1].materialized_payload_count, 0)

    def test_run_probe_native_correlation_store_retains_correlation_index(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            correlation_store="native",
            lineage_retention_policy="retained",
            metadata_mode="static",
        )

        self.assertEqual(samples[-1].lineage_count, 4)
        self.assertEqual(samples[-1].correlation_index_count, 4)

    def test_run_probe_noop_lineage_store_suppresses_retained_lineage(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            lineage_retention_policy="retained",
            lineage_store="noop",
            materialize_state=True,
            metadata_mode="static",
            publish_mode="nowait",
        )

        self.assertEqual(samples[-1].step, 8)
        self.assertEqual(samples[-1].replay_count, 4)
        self.assertEqual(samples[-1].payload_count, 4)
        self.assertEqual(samples[-1].lineage_count, 0)
        self.assertEqual(samples[-1].materialized_payload_count, 0)

    def test_run_probe_reuses_static_byte_payload(self) -> None:
        original_probe_payload = memory_benchmarks._probe_payload

        def unexpected_probe_payload(_index: int, _payload_schema: str) -> object:
            raise AssertionError("byte probes should reuse the static payload")

        memory_benchmarks._probe_payload = unexpected_probe_payload
        try:
            samples = memory_benchmarks.run_probe(
                iterations=8,
                history_limit=4,
                sample_every=4,
                lineage_retention_policy="retained",
                lineage_store="noop",
                materialize_state=True,
                metadata_mode="static",
                publish_mode="nowait",
            )
        finally:
            memory_benchmarks._probe_payload = original_probe_payload

        self.assertEqual(samples[-1].lineage_count, 0)

    def test_run_probe_uses_configured_static_byte_payload_size(self) -> None:
        original_graph = memory_benchmarks.Graph
        payload_lengths: list[int] = []

        class RecordingGraph(original_graph):
            def publish_nowait(self, *args: object, **kwargs: object) -> None:
                payload = args[1]
                if not isinstance(payload, bytes):
                    raise AssertionError("byte probe should publish bytes")
                payload_lengths.append(len(payload))
                return super().publish_nowait(*args, **kwargs)

        memory_benchmarks.Graph = RecordingGraph
        try:
            samples = memory_benchmarks.run_probe(
                iterations=8,
                history_limit=4,
                sample_every=4,
                lineage_retention_policy="retained",
                lineage_store="noop",
                materialize_state=True,
                metadata_mode="static",
                payload_bytes=1024,
                publish_mode="nowait",
            )
        finally:
            memory_benchmarks.Graph = original_graph

        self.assertEqual(samples[-1].lineage_count, 0)
        self.assertEqual(payload_lengths, [1024] * 8)

    def test_run_probe_rejects_non_positive_payload_bytes(self) -> None:
        with self.assertRaisesRegex(ValueError, "payload_bytes must be positive"):
            memory_benchmarks.run_probe(
                iterations=1,
                history_limit=1,
                sample_every=1,
                payload_bytes=0,
            )

    def test_run_probe_rejects_unknown_correlation_store(self) -> None:
        with self.assertRaisesRegex(ValueError, "correlation_store must be one of"):
            memory_benchmarks.run_probe(
                iterations=1,
                history_limit=1,
                sample_every=1,
                correlation_store="debug",
            )

    def test_run_probe_attaches_native_correlation_store_on_request(self) -> None:
        original_graph = memory_benchmarks.Graph
        attached_types: list[type[object]] = []

        class RecordingGraph(original_graph):
            def attach(self, store: object) -> object:
                attached_types.append(type(store))
                return super().attach(store)

        memory_benchmarks.Graph = RecordingGraph
        try:
            memory_benchmarks.run_probe(
                iterations=1,
                history_limit=1,
                sample_every=1,
                correlation_store="native",
                lineage_retention_policy="retained",
                metadata_mode="static",
            )
        finally:
            memory_benchmarks.Graph = original_graph

        self.assertIn(memory_benchmarks.NativeLineageTracingStore, attached_types)
        self.assertIn(memory_benchmarks.NativeCorrelationTracingStore, attached_types)

    def test_run_probe_omits_metadata_kwargs_when_lineage_store_is_noop(self) -> None:
        original_graph = memory_benchmarks.Graph
        publish_kwargs: list[dict[str, object]] = []

        class RecordingGraph(original_graph):
            def publish_nowait(self, *args: object, **kwargs: object) -> None:
                publish_kwargs.append(dict(kwargs))
                return super().publish_nowait(*args, **kwargs)

        memory_benchmarks.Graph = RecordingGraph
        try:
            samples = memory_benchmarks.run_probe(
                iterations=8,
                history_limit=4,
                sample_every=4,
                lineage_retention_policy="retained",
                lineage_store="noop",
                materialize_state=True,
                metadata_mode="static",
                publish_mode="nowait",
            )
        finally:
            memory_benchmarks.Graph = original_graph

        self.assertEqual(samples[-1].lineage_count, 0)
        self.assertEqual(publish_kwargs, [{}] * 8)

    def test_run_probe_keeps_dynamic_payload_for_process_local_schema(self) -> None:
        original_probe_payload = memory_benchmarks._probe_payload
        calls = 0

        def counted_probe_payload(index: int, payload_schema: str) -> object:
            nonlocal calls
            calls += 1
            return original_probe_payload(index, payload_schema)

        memory_benchmarks._probe_payload = counted_probe_payload
        try:
            memory_benchmarks.run_probe(
                iterations=4,
                history_limit=2,
                sample_every=2,
                payload_schema="any",
                publish_mode="nowait",
            )
        finally:
            memory_benchmarks._probe_payload = original_probe_payload

        self.assertEqual(calls, 4)

    def test_traced_current_excludes_benchmark_sample_retention(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=128,
            history_limit=8,
            sample_every=8,
            lineage_retention_policy="retained",
            lineage_store="noop",
            materialize_state=True,
            metadata_mode="unique-all",
            publish_mode="nowait",
        )
        traced_values = tuple(sample.traced_current_bytes for sample in samples)

        self.assertGreater(len(samples), 8)
        self.assertLessEqual(max(traced_values) - min(traced_values), 128)
        self.assertEqual(samples[-1].lineage_count, 0)
        self.assertEqual(samples[-1].materialized_payload_count, 0)

    def test_run_probe_nowait_materialized_state_releases_python_payloads(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            lineage_retention_policy="retained",
            materialize_state=True,
            publish_mode="nowait",
        )

        self.assertEqual(samples[-1].step, 8)
        self.assertEqual(samples[-1].replay_count, 4)
        self.assertEqual(samples[-1].payload_count, 4)
        self.assertEqual(samples[-1].lineage_count, 4)
        self.assertEqual(samples[-1].materialized_payload_count, 0)

    def test_run_probe_supports_unique_all_metadata_mode(self) -> None:
        samples = memory_benchmarks.run_probe(
            iterations=8,
            history_limit=4,
            sample_every=4,
            lineage_retention_policy="retained",
            materialize_state=True,
            metadata_mode="unique-all",
            publish_mode="nowait",
        )

        self.assertEqual(samples[-1].step, 8)
        self.assertEqual(samples[-1].replay_count, 4)
        self.assertEqual(samples[-1].payload_count, 4)
        self.assertEqual(samples[-1].lineage_count, 4)
        self.assertEqual(samples[-1].materialized_payload_count, 0)

    def test_static_lineage_metadata_uses_native_profile_cache(self) -> None:
        graph, stream, _state, subscription = _materialized_nowait_graph()
        try:
            for index in range(4):
                graph.publish_nowait(
                    stream,
                    b"frame",
                    trace_id="static-trace",
                    causality_id="static-chain",
                    correlation_id=f"request-{index:020}",
                )

            self.assertIsNotNone(graph._last_nowait_drop_lineage_profile)
            self.assertEqual(
                graph._last_nowait_drop_lineage_profile_ids,
                ("static-trace", "static-chain"),
            )
        finally:
            subscription.dispose()

    def test_static_lineage_metadata_fast_path_uses_cached_profile(self) -> None:
        graph, stream, state, subscription = _materialized_nowait_graph()
        try:
            for index in range(2):
                graph.publish_nowait(
                    stream,
                    f"warm-{index}".encode("ascii"),
                    trace_id="static-trace",
                    causality_id="static-chain",
                    correlation_id=f"request-warm-{index}",
                )
            self.assertIsNotNone(graph._last_nowait_drop_lineage_profile)

            graph._last_nowait_drop_explicit_lineage_native = None
            graph.publish_nowait(
                stream,
                b"cached",
                trace_id="static-trace",
                causality_id="static-chain",
                correlation_id="request-cached",
            )

            self.assertEqual(graph.latest(state).value, b"cached")
            self.assertEqual(
                graph._last_nowait_drop_lineage_profile_ids,
                ("static-trace", "static-chain"),
            )
        finally:
            subscription.dispose()

    def test_unique_lineage_metadata_avoids_native_profile_cache(self) -> None:
        graph, stream, _state, subscription = _materialized_nowait_graph()
        try:
            for index in range(4):
                graph.publish_nowait(
                    stream,
                    b"frame",
                    trace_id=f"trace-{index:020}",
                    causality_id=f"chain-{index:020}",
                    correlation_id=f"request-{index:020}",
                )

            self.assertIsNone(graph._last_nowait_drop_lineage_profile)
            self.assertIsNone(graph._last_nowait_drop_lineage_profile_ids)
        finally:
            subscription.dispose()

    def test_noop_lineage_store_uses_no_lineage_materializer_drop(self) -> None:
        graph, stream, state, subscription = _materialized_nowait_graph(
            lineage_store="noop",
        )
        try:
            for index in range(4):
                graph.publish_nowait(
                    stream,
                    b"frame",
                    trace_id=f"trace-{index:020}",
                    causality_id=f"chain-{index:020}",
                    correlation_id=f"request-{index:020}",
                )

            stream_snapshot = next(graph.retention_snapshot(stream))
            state_snapshot = next(graph.retention_snapshot(state))

            self.assertEqual(stream_snapshot.lineage_count, 0)
            self.assertEqual(state_snapshot.lineage_count, 0)
            self.assertEqual(graph._graph.lineage_intern_value_count(), 0)
            self.assertIsNone(graph._last_nowait_drop_lineage_profile)
            self.assertIn(
                graph._last_nowait_drop_native_call_kind,
                ("no_lineage", "no_lineage_profile", "no_lineage_python"),
            )
            cached_call_kind = graph._last_nowait_drop_native_call_kind
            cached_native_call = graph._last_nowait_drop_explicit_lineage_native
            self.assertIsNotNone(cached_native_call)
            if cached_call_kind == "no_lineage_profile":
                self.assertIsNotNone(graph._last_nowait_drop_no_lineage_profile)
                self.assertIsNotNone(
                    graph._last_nowait_drop_no_lineage_profile_emit
                )

            def _unexpected_native_lookup(*_args: object, **_kwargs: object) -> bool:
                raise AssertionError(
                    "cached no-lineage publish_nowait should reuse the stored native call"
                )

            if cached_call_kind == "no_lineage_profile":
                graph._last_nowait_drop_explicit_lineage_native = None
                graph._native_emit_no_lineage_materializer_drop_profile_python = (
                    _unexpected_native_lookup
                )
            elif cached_call_kind == "no_lineage_python":
                graph._native_emit_single_if_unrouted_and_materializer_drop_python = (
                    _unexpected_native_lookup
                )
            else:
                graph._native_emit_single_if_unrouted_and_materializer_drop = (
                    _unexpected_native_lookup
                )
            graph.publish_nowait(
                stream,
                b"cached-frame",
                trace_id="trace-after-cache",
                causality_id="chain-after-cache",
                correlation_id="request-after-cache",
            )
            self.assertEqual(graph.latest(state).value, b"cached-frame")
            graph.connect(source=stream, sink=state)
            self.assertIsNone(graph._last_nowait_drop_mode)
        finally:
            subscription.dispose()

    def test_no_lineage_materializer_profile_rejects_after_unregister(self) -> None:
        graph, stream, state, subscription = _materialized_nowait_graph(
            lineage_store="noop",
        )
        try:
            graph.publish_nowait(stream, b"warm")
            profile = graph._last_nowait_drop_no_lineage_profile
            emit_profile = (
                graph._native_emit_no_lineage_materializer_drop_profile_python
            )
            self.assertIsNotNone(profile)
            self.assertIsNotNone(emit_profile)

            subscription.dispose()

            self.assertFalse(emit_profile(profile, b"stale"))
            self.assertEqual(graph.latest(state).value, b"warm")
        finally:
            subscription.dispose()

    def test_lineage_profile_release_follows_metadata_switch(self) -> None:
        graph, stream, _state, subscription = _materialized_nowait_graph(
            history_limit=1,
        )
        try:
            for index in range(4):
                graph.publish_nowait(
                    stream,
                    b"frame-a",
                    trace_id="trace-a",
                    causality_id="chain-a",
                    correlation_id=f"request-a-{index:020}",
                )
            self.assertEqual(
                graph._last_nowait_drop_lineage_profile_ids,
                ("trace-a", "chain-a"),
            )

            for index in range(4):
                graph.publish_nowait(
                    stream,
                    b"frame-b",
                    trace_id="trace-b",
                    causality_id="chain-b",
                    correlation_id=f"request-b-{index:020}",
                )

            self.assertEqual(
                graph._last_nowait_drop_lineage_profile_ids,
                ("trace-b", "chain-b"),
            )
            self.assertEqual(graph._graph.lineage_intern_value_count(), 2)
        finally:
            subscription.dispose()

    def test_lineage_profile_release_follows_materialize_dispose(self) -> None:
        graph, stream, state, subscription = _materialized_nowait_graph(
            history_limit=1,
        )
        for index in range(4):
            graph.publish_nowait(
                stream,
                b"frame-a",
                trace_id="trace-a",
                causality_id="chain-a",
                correlation_id=f"request-a-{index:020}",
            )
        self.assertEqual(
            graph._last_nowait_drop_lineage_profile_ids,
            ("trace-a", "chain-a"),
        )

        subscription.dispose()
        self.assertIsNone(graph._last_nowait_drop_lineage_profile)
        self.assertIsNone(graph._last_nowait_drop_lineage_profile_ids)

        for index in range(4):
            graph.publish_nowait(
                stream,
                b"source-b",
                trace_id="trace-b",
                causality_id="chain-b",
                correlation_id=f"source-b-{index:020}",
            )
            graph.publish_nowait(
                state,
                b"state-b",
                trace_id="trace-b",
                causality_id="chain-b",
                correlation_id=f"state-b-{index:020}",
            )

        self.assertEqual(graph._graph.lineage_intern_value_count(), 2)

    def test_run_probe_rejects_materialize_subscription_metadata_leak(self) -> None:
        graph_class = memory_benchmarks.Graph
        original_materialize = graph_class.materialize

        class LeakySubscription:
            def dispose(self) -> None:
                return None

        def leaky_materialize(self, source, *, state_route):
            subscription = original_materialize(self, source, state_route=state_route)
            self._subscriptions.append(subscription)
            return LeakySubscription()

        graph_class.materialize = leaky_materialize
        try:
            with self.assertRaisesRegex(
                SystemExit,
                "disposed subscriptions retained subscriber metadata",
            ):
                memory_benchmarks.run_probe(
                    iterations=4,
                    history_limit=2,
                    sample_every=2,
                    materialize_state=True,
                )
        finally:
            graph_class.materialize = original_materialize

    def test_parse_probe_sample_reads_elapsed_seconds(self) -> None:
        parsed = memory_benchmarks._parse_probe_sample(
            "step=8 replay=8 lineage=0 correlation_index=3 payloads=8 violations=0 "
            "subscriber_metadata=0 taint_metadata=0 traced_current=100 "
            "traced_peak=120 current_rss_kib=33000 max_rss_native=33000 "
            "materialized_payloads=0 elapsed_seconds=0.125 cpu_seconds=0.110 "
            "average_event_us=15.625 interval_event_us=12.500 "
            "input_blocks=0 output_blocks=0"
        )

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.elapsed_seconds, 0.125)
        self.assertEqual(parsed.cpu_seconds, 0.110)
        self.assertEqual(parsed.average_event_us, 15.625)
        self.assertEqual(parsed.interval_event_us, 12.5)
        self.assertEqual(parsed.input_blocks, 0)
        self.assertEqual(parsed.output_blocks, 0)
        self.assertEqual(parsed.materialized_payload_count, 0)
        self.assertEqual(parsed.correlation_index_count, 3)

    def test_external_command_monitor_samples_short_process(self) -> None:
        samples = memory_benchmarks._run_external_command_monitor(
            Namespace(
                check=True,
                command=(
                    sys.executable,
                    "-c",
                    "import time; time.sleep(0.08)",
                ),
                external_duration_seconds=None,
                external_min_elapsed_seconds=None,
                external_min_samples=None,
                external_project_seconds=60.0,
                external_anonymous_plateau_kib=None,
                external_anonymous_projected_growth_kib=None,
                external_anonymous_segment_projected_growth_kib=None,
                external_fd_plateau_count=None,
                external_fd_projected_growth_count=None,
                external_fd_segment_projected_growth_count=None,
                external_private_plateau_kib=None,
                external_private_projected_growth_kib=None,
                external_private_segment_projected_growth_kib=None,
                external_pss_plateau_kib=None,
                external_pss_projected_growth_kib=None,
                external_pss_segment_projected_growth_kib=None,
                external_rss_projected_growth_kib=None,
                external_rss_segment_projected_growth_kib=None,
                external_rss_scope="tree",
                external_shutdown_timeout_seconds=1.0,
                external_terminate_scope="tree",
                max_cpu_seconds=None,
                max_disk_input_blocks=None,
                max_disk_output_blocks=None,
                max_elapsed_seconds=None,
                monitor_interval=0.02,
                rss_plateau_kib=1_000_000,
                rss_warmup_fraction=0.0,
            )
        )

        self.assertGreaterEqual(len(samples), 1)

    def test_external_command_monitor_rejects_elapsed_limit(self) -> None:
        with self.assertRaisesRegex(SystemExit, "external command elapsed time"):
            memory_benchmarks._run_external_command_monitor(
                Namespace(
                    check=True,
                    command=(
                        sys.executable,
                        "-c",
                        "import time; time.sleep(0.02)",
                    ),
                    external_duration_seconds=None,
                    external_min_elapsed_seconds=60.0,
                    external_min_samples=None,
                    external_project_seconds=60.0,
                    external_anonymous_plateau_kib=None,
                    external_anonymous_projected_growth_kib=None,
                    external_anonymous_segment_projected_growth_kib=None,
                    external_fd_plateau_count=None,
                    external_fd_projected_growth_count=None,
                    external_fd_segment_projected_growth_count=None,
                    external_private_plateau_kib=None,
                    external_private_projected_growth_kib=None,
                    external_private_segment_projected_growth_kib=None,
                    external_pss_plateau_kib=None,
                    external_pss_projected_growth_kib=None,
                    external_pss_segment_projected_growth_kib=None,
                    external_rss_projected_growth_kib=None,
                    external_rss_segment_projected_growth_kib=None,
                    external_rss_scope="tree",
                    external_shutdown_timeout_seconds=1.0,
                    external_terminate_scope="tree",
                    max_cpu_seconds=None,
                    max_disk_input_blocks=None,
                    max_disk_output_blocks=None,
                    max_elapsed_seconds=None,
                    monitor_interval=0.01,
                    rss_plateau_kib=1_000_000,
                    rss_warmup_fraction=0.0,
                )
            )

    def test_external_command_monitor_rejects_sample_floor(self) -> None:
        with self.assertRaisesRegex(SystemExit, "below required 3"):
            memory_benchmarks._run_external_command_monitor(
                Namespace(
                    check=True,
                    command=(
                        sys.executable,
                        "-c",
                        "import time; time.sleep(0.02)",
                    ),
                    external_duration_seconds=None,
                    external_min_elapsed_seconds=None,
                    external_min_samples=3,
                    external_project_seconds=60.0,
                    external_private_plateau_kib=None,
                    external_private_projected_growth_kib=None,
                    external_private_segment_projected_growth_kib=None,
                    external_pss_plateau_kib=None,
                    external_pss_projected_growth_kib=None,
                    external_pss_segment_projected_growth_kib=None,
                    external_anonymous_plateau_kib=None,
                    external_anonymous_projected_growth_kib=None,
                    external_anonymous_segment_projected_growth_kib=None,
                    external_fd_plateau_count=None,
                    external_fd_projected_growth_count=None,
                    external_fd_segment_projected_growth_count=None,
                    external_rss_projected_growth_kib=None,
                    external_rss_segment_projected_growth_kib=None,
                    external_rss_scope="tree",
                    external_shutdown_timeout_seconds=1.0,
                    external_terminate_scope="tree",
                    max_cpu_seconds=None,
                    max_disk_input_blocks=None,
                    max_disk_output_blocks=None,
                    max_elapsed_seconds=None,
                    monitor_interval=0.1,
                    rss_plateau_kib=1_000_000,
                    rss_warmup_fraction=0.0,
                )
            )


def _materialized_nowait_graph(
    *,
    history_limit: int = 8,
    lineage_store: str = "native",
) -> tuple[object, object, object, object]:
    stream = memory_benchmarks._build_stream("bytes")
    state = memory_benchmarks._build_state_stream("bytes")
    graph = memory_benchmarks.Graph()
    if lineage_store == "noop":
        graph.attach(memory_benchmarks.NoopLineageTracingStore())
    elif lineage_store == "native":
        graph.attach(memory_benchmarks.NativeLineageTracingStore())
    elif lineage_store != "native":
        raise ValueError("lineage_store must be one of 'native' or 'noop'")
    retention = memory_benchmarks.RouteRetentionPolicy(
        latest_replay_policy="bounded_history",
        replay_window=f"last_{history_limit}",
        history_limit=history_limit,
        lineage_retention_policy="retained",
    )
    graph.configure_retention(stream, retention)
    graph.configure_retention(state, retention)
    subscription = graph.materialize(stream, state_route=state)
    return graph, stream, state, subscription


def _sample(
    step: int,
    *,
    lineage_count: int = 0,
    correlation_index_count: int = 0,
    traced_current_bytes: int = 0,
    current_rss_kib: int = 0,
    elapsed_seconds: float = 0.0,
    cpu_seconds: float = 0.0,
    average_event_us: float = 0.0,
    interval_event_us: float = 0.0,
    input_blocks: int = 0,
    output_blocks: int = 0,
    materialized_payload_count: int = 0,
) -> memory_benchmarks._ProbeSample:
    return memory_benchmarks._ProbeSample(
        step=step,
        replay_count=8,
        lineage_count=lineage_count,
        correlation_index_count=correlation_index_count,
        payload_count=8,
        violation_count=0,
        subscriber_metadata_count=0,
        taint_metadata_count=0,
        materialized_payload_count=materialized_payload_count,
        traced_current_bytes=traced_current_bytes,
        traced_peak_bytes=traced_current_bytes,
        current_rss_kib=current_rss_kib,
        max_rss_native=current_rss_kib,
        elapsed_seconds=elapsed_seconds,
        cpu_seconds=cpu_seconds,
        average_event_us=average_event_us,
        interval_event_us=interval_event_us,
        input_blocks=input_blocks,
        output_blocks=output_blocks,
    )


def _rss_sample(
    elapsed_seconds: float,
    *,
    current_rss_kib: int,
    pss_kib: int | None = None,
    private_kib: int | None = None,
    anonymous_kib: int | None = None,
    fd_count: int | None = None,
) -> memory_benchmarks._RssSample:
    return memory_benchmarks._RssSample(
        elapsed_seconds=elapsed_seconds,
        current_rss_kib=current_rss_kib,
        pss_kib=pss_kib,
        private_kib=private_kib,
        anonymous_kib=anonymous_kib,
        fd_count=fd_count,
    )


if __name__ == "__main__":
    unittest.main()
