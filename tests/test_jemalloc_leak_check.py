from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from manyfold import jemalloc_leak_check


class JemallocLeakCheckTests(unittest.TestCase):
    def test_parse_leak_summary_extracts_final_profile_counts(self) -> None:
        summary = jemalloc_leak_check._parse_leak_summary(
            '<jemalloc>: Leak summary: 267184 bytes, 473 objects, 20 contexts\n'
            '<jemalloc>: Run jeprof on "jeprof.19678.0.f.heap" for leak detail\n'
        )

        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary.bytes, 267184)
        self.assertEqual(summary.objects, 473)
        self.assertEqual(summary.contexts, 20)

    def test_parse_leak_summary_accepts_singular_counts(self) -> None:
        summary = jemalloc_leak_check._parse_leak_summary(
            "<jemalloc>: Leak summary: 1 byte, 1 object, 1 context\n"
        )

        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary.bytes, 1)
        self.assertEqual(summary.objects, 1)
        self.assertEqual(summary.contexts, 1)

    def test_parse_leak_summary_accepts_approximation_counts(self) -> None:
        summary = jemalloc_leak_check._parse_leak_summary(
            "<jemalloc>: Leak approximation summary: ~82560 bytes, "
            "~2 objects, >= 2 contexts\n"
        )

        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary.bytes, 82_560)
        self.assertEqual(summary.objects, 2)
        self.assertEqual(summary.contexts, 2)

    def test_run_leak_check_filters_below_threshold_summary(self) -> None:
        completed = mock.Mock(
            returncode=0,
            stdout="ok\n",
            stderr="<jemalloc>: Leak summary: 16 bytes, 1 objects, 1 contexts\n",
        )
        with mock.patch.object(jemalloc_leak_check.subprocess, "run", return_value=completed):
            result = jemalloc_leak_check.run_leak_check(
                ("checked-program",),
                libjemalloc="/tmp/libjemalloc.so.2",
                leak_bytes_threshold=16,
                leak_objects_threshold=1,
            )

        self.assertIsNone(result.leak_summary)
        self.assertIsNotNone(result.observed_leak_summary)
        self.assertEqual(result.stdout, "ok\n")

    def test_run_leak_check_sets_jemalloc_environment(self) -> None:
        completed = mock.Mock(returncode=0, stdout="", stderr="")
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            with mock.patch.object(
                jemalloc_leak_check.subprocess, "run", return_value=completed
            ) as run:
                result = jemalloc_leak_check.run_leak_check(
                    ("checked-program", "--flag"),
                    libjemalloc="/tmp/libjemalloc.so.2",
                    output_dir=output_dir,
                )

        self.assertIsNone(result.leak_summary)
        kwargs = run.call_args.kwargs
        self.assertEqual(kwargs["env"]["MALLOC_CONF"], (
            "prof_leak:true,lg_prof_sample:0,prof_final:true,"
            f"prof_prefix:{output_dir / 'jeprof'}"
        ))
        self.assertEqual(
            kwargs["env"][jemalloc_leak_check._preload_env_name()],
            "/tmp/libjemalloc.so.2",
        )
        self.assertEqual(kwargs["env"].get("PATH"), os.environ.get("PATH"))

    def test_run_leak_check_writes_machine_readable_summary(self) -> None:
        completed = mock.Mock(
            returncode=0,
            stdout="",
            stderr="<jemalloc>: Leak summary: 0 bytes, 0 objects, 0 contexts\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            with mock.patch.object(
                jemalloc_leak_check.subprocess,
                "run",
                return_value=completed,
            ):
                jemalloc_leak_check.run_leak_check(
                    ("checked-program", "--flag"),
                    libjemalloc="/tmp/libjemalloc.so.2",
                    output_dir=output_dir,
                    leak_bytes_threshold=0,
                    leak_objects_threshold=0,
                )
            summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )

        self.assertEqual(summary["command"], ["checked-program", "--flag"])
        self.assertTrue(summary["passed"])
        self.assertEqual(
            summary["observed_leak_summary"],
            {"bytes": 0, "contexts": 0, "objects": 0},
        )
        self.assertIsNone(summary["failing_leak_summary"])

    def test_run_leak_check_summary_records_failing_leak(self) -> None:
        completed = mock.Mock(
            returncode=0,
            stdout="",
            stderr="<jemalloc>: Leak summary: 32 bytes, 2 objects, 1 context\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            with mock.patch.object(
                jemalloc_leak_check.subprocess,
                "run",
                return_value=completed,
            ):
                jemalloc_leak_check.run_leak_check(
                    ("checked-program",),
                    libjemalloc="/tmp/libjemalloc.so.2",
                    output_dir=output_dir,
                )
            summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )

        self.assertFalse(summary["passed"])
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

        self.assertTrue(summary["passed"])

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
                "manyfold-jemalloc-verify",
                str(path),
                "--require-command-fragment",
                "memory_retention_benchmark",
            )
            stdout = StringIO()
            with mock.patch.object(sys, "argv", list(argv)):
                with redirect_stdout(stdout):
                    jemalloc_leak_check._verify_main()

        self.assertIn("jemalloc_summary passed=true", stdout.getvalue())
        self.assertIn("bytes=0 objects=0 contexts=0", stdout.getvalue())

    def test_run_leak_check_resets_output_dir_before_running(self) -> None:
        completed = mock.Mock(
            returncode=0,
            stdout="",
            stderr="<jemalloc>: Leak summary: 0 bytes, 0 objects, 0 contexts\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            stale_heap = output_dir / "jeprof.1.0.f.heap"
            stale_heap.write_text("stale heap\n", encoding="utf-8")

            def _run_after_reset(*args: object, **kwargs: object):
                del args, kwargs
                self.assertFalse(stale_heap.exists())
                return completed

            with mock.patch.object(
                jemalloc_leak_check.subprocess,
                "run",
                side_effect=_run_after_reset,
            ):
                result = jemalloc_leak_check.run_leak_check(
                    ("checked-program",),
                    libjemalloc="/tmp/libjemalloc.so.2",
                    output_dir=output_dir,
                )

        self.assertIsNone(result.leak_summary)
        self.assertIsNotNone(result.observed_leak_summary)

    def test_main_rejects_missing_leak_summary_by_default(self) -> None:
        completed = mock.Mock(returncode=0, stdout="", stderr="")
        with tempfile.TemporaryDirectory() as directory:
            argv = (
                "manyfold-jemalloc-leak-check",
                "--libjemalloc",
                "/tmp/libjemalloc.so.2",
                "--output-dir",
                str(Path(directory) / "profiles"),
                "--",
                "checked-program",
            )
            with mock.patch.object(sys, "argv", list(argv)):
                with mock.patch.object(
                    jemalloc_leak_check.subprocess,
                    "run",
                    return_value=completed,
                ):
                    with self.assertRaisesRegex(SystemExit, "no leak summary"):
                        jemalloc_leak_check._main()

    def test_main_prints_observed_summary_counts(self) -> None:
        completed = mock.Mock(
            returncode=0,
            stdout="benchmark stdout\n",
            stderr="<jemalloc>: Leak summary: 0 bytes, 0 objects, 0 contexts\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            argv = (
                "manyfold-jemalloc-leak-check",
                "--libjemalloc",
                "/tmp/libjemalloc.so.2",
                "--output-dir",
                str(Path(directory) / "profiles"),
                "--",
                "checked-program",
            )
            stdout = StringIO()
            stderr = StringIO()
            with mock.patch.object(sys, "argv", list(argv)):
                with mock.patch.object(
                    jemalloc_leak_check.subprocess,
                    "run",
                    return_value=completed,
                ):
                    with redirect_stderr(stderr), redirect_stdout(stdout):
                        jemalloc_leak_check._main()

        output = stdout.getvalue()
        self.assertIn("benchmark stdout", output)
        self.assertIn("jemalloc_leak_check passed=true", output)
        self.assertIn("bytes=0 objects=0 contexts=0", output)

    def test_main_allows_missing_leak_summary_for_diagnostics(self) -> None:
        completed = mock.Mock(returncode=0, stdout="", stderr="")
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "profiles"
            argv = (
                "manyfold-jemalloc-leak-check",
                "--libjemalloc",
                "/tmp/libjemalloc.so.2",
                "--output-dir",
                str(output_dir),
                "--allow-missing-leak-summary",
                "--",
                "checked-program",
            )
            with mock.patch.object(sys, "argv", list(argv)):
                with mock.patch.object(
                    jemalloc_leak_check.subprocess,
                    "run",
                    return_value=completed,
                ):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        jemalloc_leak_check._main()
            summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )

        self.assertFalse(summary["passed"])
        self.assertIsNone(summary["observed_leak_summary"])
        self.assertIn("leak_summary=unavailable", stdout.getvalue())


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


if __name__ == "__main__":
    unittest.main()
