from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from manyfold.private.profiling import jemalloc_leak_check


class JemallocLeakCheckTests(unittest.TestCase):
    def test_parse_leak_summary_extracts_final_profile_counts(self) -> None:
        summary = jemalloc_leak_check._parse_leak_summary(
            '<jemalloc>: Leak summary: 267184 bytes, 473 objects, 20 contexts\n'
            '<jemalloc>: Run jeprof on "jeprof.19678.0.f.heap" for leak detail\n'
        )

        if summary is None:
            self.fail("expected a parsed jemalloc leak summary")
        self.assertEqual(summary.bytes, 267184)
        self.assertEqual(summary.objects, 473)
        self.assertEqual(summary.contexts, 20)

    def test_parse_leak_summary_accepts_singular_counts(self) -> None:
        summary = jemalloc_leak_check._parse_leak_summary(
            "<jemalloc>: Leak summary: 1 byte, 1 object, 1 context\n"
        )

        if summary is None:
            self.fail("expected a parsed jemalloc leak summary")
        self.assertEqual(summary.bytes, 1)
        self.assertEqual(summary.objects, 1)
        self.assertEqual(summary.contexts, 1)

    def test_parse_leak_summary_accepts_approximation_counts(self) -> None:
        summary = jemalloc_leak_check._parse_leak_summary(
            "<jemalloc>: Leak approximation summary: ~82560 bytes, "
            "~2 objects, >= 2 contexts\n"
        )

        if summary is None:
            self.fail("expected a parsed jemalloc leak summary")
        self.assertEqual(summary.bytes, 82_560)
        self.assertEqual(summary.objects, 2)
        self.assertEqual(summary.contexts, 2)

    def test_run_leak_check_filters_below_threshold_summary(self) -> None:
        runner = _RecordingCommandRunner(
            _completed_process(
                stdout="ok\n",
                stderr="<jemalloc>: Leak summary: 16 bytes, 1 objects, 1 contexts\n",
            )
        )
        result = jemalloc_leak_check.run_leak_check(
            ("checked-program",),
            libjemalloc="/tmp/libjemalloc.so.2",
            leak_bytes_threshold=16,
            leak_objects_threshold=1,
            command_runner=runner,
        )

        self.assertIsNone(result.leak_summary)
        self.assertIsNotNone(result.observed_leak_summary)
        self.assertEqual(result.stdout, "ok\n")

    def test_run_leak_check_sets_jemalloc_environment(self) -> None:
        runner = _RecordingCommandRunner(_completed_process())
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            result = jemalloc_leak_check.run_leak_check(
                ("checked-program", "--flag"),
                libjemalloc="/tmp/libjemalloc.so.2",
                output_dir=output_dir,
                command_runner=runner,
            )

        self.assertIsNone(result.leak_summary)
        self.assertEqual(runner.single_call.env["MALLOC_CONF"], (
            "prof_leak:true,lg_prof_sample:0,prof_final:true,"
            f"prof_prefix:{output_dir / 'jeprof'}"
        ))
        self.assertEqual(
            runner.single_call.env[jemalloc_leak_check._preload_env_name()],
            "/tmp/libjemalloc.so.2",
        )
        self.assertEqual(runner.single_call.env.get("PATH"), os.environ.get("PATH"))

    def test_run_leak_check_writes_machine_readable_summary(self) -> None:
        runner = _RecordingCommandRunner(
            _completed_process(
                stderr="<jemalloc>: Leak summary: 0 bytes, 0 objects, 0 contexts\n",
            )
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            jemalloc_leak_check.run_leak_check(
                ("checked-program", "--flag"),
                libjemalloc="/tmp/libjemalloc.so.2",
                output_dir=output_dir,
                leak_bytes_threshold=0,
                leak_objects_threshold=0,
                command_runner=runner,
            )
            summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )

        self.assertEqual(summary["command"], ["checked-program", "--flag"])
        self.assertIs(summary["passed"], True)
        self.assertEqual(
            summary["observed_leak_summary"],
            {"bytes": 0, "contexts": 0, "objects": 0},
        )
        self.assertIsNone(summary["failing_leak_summary"])

    def test_run_leak_check_summary_records_failing_leak(self) -> None:
        runner = _RecordingCommandRunner(
            _completed_process(
                stderr="<jemalloc>: Leak summary: 32 bytes, 2 objects, 1 context\n",
            )
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            jemalloc_leak_check.run_leak_check(
                ("checked-program",),
                libjemalloc="/tmp/libjemalloc.so.2",
                output_dir=output_dir,
                command_runner=runner,
            )
            summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )

        self.assertIs(summary["passed"], False)
        self.assertEqual(
            summary["failing_leak_summary"],
            {"bytes": 32, "contexts": 1, "objects": 2},
        )

    def test_verify_jemalloc_summary_accepts_passing_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.json"
            path.write_text(
                json.dumps(_summary_artifact()),
                encoding="utf-8",
            )

            summary = jemalloc_leak_check.verify_jemalloc_summary(
                path,
                required_command_fragments=("memory_retention_benchmark",),
            )

        self.assertIs(summary["passed"], True)

    def test_verify_jemalloc_summary_rejects_missing_observed_summary(self) -> None:
        artifact = _summary_artifact()
        artifact["observed_leak_summary"] = None
        artifact["passed"] = False
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.json"
            path.write_text(json.dumps(artifact), encoding="utf-8")

            with self.assertRaisesRegex(SystemExit, "observed leak summary"):
                jemalloc_leak_check.verify_jemalloc_summary(path)

    def test_verify_jemalloc_summary_rejects_failing_summary(self) -> None:
        artifact = _summary_artifact()
        artifact["failing_leak_summary"] = {
            "bytes": 32,
            "contexts": 1,
            "objects": 2,
        }
        artifact["passed"] = False
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.json"
            path.write_text(json.dumps(artifact), encoding="utf-8")

            with self.assertRaisesRegex(SystemExit, "failing leak summary"):
                jemalloc_leak_check.verify_jemalloc_summary(path)

    def test_verify_main_prints_summary_counts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "summary.json"
            path.write_text(json.dumps(_summary_artifact()), encoding="utf-8")
            argv = (
                str(path),
                "--require-command-fragment",
                "memory_retention_benchmark",
            )
            stdout = StringIO()
            with redirect_stdout(stdout):
                jemalloc_leak_check._verify_main(argv)

        self.assertIn("jemalloc_summary passed=true", stdout.getvalue())
        self.assertIn("bytes=0 objects=0 contexts=0", stdout.getvalue())

    def test_run_leak_check_resets_output_dir_before_running(self) -> None:
        completed = _completed_process(
            stderr="<jemalloc>: Leak summary: 0 bytes, 0 objects, 0 contexts\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            stale_heap = output_dir / "jeprof.1.0.f.heap"
            stale_heap.write_text("stale heap\n", encoding="utf-8")

            def _run_after_reset(
                command: tuple[str, ...],
                env: dict[str, str],
            ) -> subprocess.CompletedProcess[str]:
                del command, env
                self.assertIs(stale_heap.exists(), False)
                return completed

            result = jemalloc_leak_check.run_leak_check(
                ("checked-program",),
                libjemalloc="/tmp/libjemalloc.so.2",
                output_dir=output_dir,
                command_runner=_run_after_reset,
            )

        self.assertIsNone(result.leak_summary)
        self.assertIsNotNone(result.observed_leak_summary)

    def test_main_rejects_missing_leak_summary_by_default(self) -> None:
        runner = _RecordingCommandRunner(_completed_process())
        with tempfile.TemporaryDirectory() as directory:
            argv = (
                "--libjemalloc",
                "/tmp/libjemalloc.so.2",
                "--output-dir",
                str(Path(directory) / "profiles"),
                "--",
                "checked-program",
            )
            with self.assertRaisesRegex(SystemExit, "no leak summary"):
                jemalloc_leak_check._main(argv, command_runner=runner)

    def test_main_prints_observed_summary_counts(self) -> None:
        runner = _RecordingCommandRunner(
            _completed_process(
                stdout="benchmark stdout\n",
                stderr="<jemalloc>: Leak summary: 0 bytes, 0 objects, 0 contexts\n",
            )
        )
        with tempfile.TemporaryDirectory() as directory:
            argv = (
                "--libjemalloc",
                "/tmp/libjemalloc.so.2",
                "--output-dir",
                str(Path(directory) / "profiles"),
                "--",
                "checked-program",
            )
            stdout = StringIO()
            stderr = StringIO()
            with redirect_stderr(stderr), redirect_stdout(stdout):
                jemalloc_leak_check._main(argv, command_runner=runner)

        output = stdout.getvalue()
        self.assertIn("benchmark stdout", output)
        self.assertIn("jemalloc_leak_check passed=true", output)
        self.assertIn("bytes=0 objects=0 contexts=0", output)

    def test_main_allows_missing_leak_summary_for_diagnostics(self) -> None:
        runner = _RecordingCommandRunner(_completed_process())
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "profiles"
            argv = (
                "--libjemalloc",
                "/tmp/libjemalloc.so.2",
                "--output-dir",
                str(output_dir),
                "--allow-missing-leak-summary",
                "--",
                "checked-program",
            )
            stdout = StringIO()
            with redirect_stdout(stdout):
                jemalloc_leak_check._main(argv, command_runner=runner)
            summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )

        self.assertIs(summary["passed"], False)
        self.assertIsNone(summary["observed_leak_summary"])
        self.assertIn("leak_summary=unavailable", stdout.getvalue())


def _completed_process(
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=("checked-program",),
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _summary_artifact() -> dict[str, object]:
    return {
        "command": [
            "./target/release/memory_retention_benchmark",
            "--iterations",
            "1",
        ],
        "failing_leak_summary": None,
        "leak_bytes_threshold": 0,
        "leak_objects_threshold": 0,
        "malloc_conf": "prof_leak:true,lg_prof_sample:0,prof_final:true",
        "observed_leak_summary": {
            "bytes": 0,
            "contexts": 0,
            "objects": 0,
        },
        "passed": True,
        "returncode": 0,
    }


@dataclass(frozen=True)
class _RecordedCommand:
    command: tuple[str, ...]
    env: dict[str, str]


class _RecordingCommandRunner:
    def __init__(self, completed: subprocess.CompletedProcess[str]) -> None:
        self._completed = completed
        self.calls: list[_RecordedCommand] = []

    @property
    def single_call(self) -> _RecordedCommand:
        if len(self.calls) != 1:
            raise AssertionError(
                f"expected one command call, observed {len(self.calls)}"
            )
        return self.calls[0]

    def __call__(
        self,
        command: tuple[str, ...],
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(_RecordedCommand(command=command, env=env))
        return self._completed


if __name__ == "__main__":
    unittest.main()
