from __future__ import annotations

import io
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from manyfold.private.profiling import native_profilers


class NativeProfilerTests(unittest.TestCase):
    def test_expand_all_profilers_preserves_unique_order(self) -> None:
        self.assertEqual(
            native_profilers._expand_profilers(("perf", "all", "perf")),
            (
                "perf",
                "cachegrind",
                "callgrind",
                "coz",
                "dhat",
                "heaptrack",
                "massif",
            ),
        )

    def test_coz_command_marks_profile_output_and_progress_mode(self) -> None:
        with mock.patch.object(
            native_profilers.shutil,
            "which",
            return_value="/usr/bin/coz",
        ):
            profile_command = native_profilers._profile_command(
                "coz",
                ("./target/profiling/memory_retention_benchmark", "--iterations", "1"),
                Path("profiles/coz"),
            )

        self.assertEqual(
            profile_command.command[:5],
            ("coz", "run", "-b", "MAIN", "--end-to-end"),
        )
        self.assertIn("profiles/coz/profile.coz", profile_command.command)
        self.assertIn("---", profile_command.command)

    def test_missing_profiler_can_be_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(native_profilers.shutil, "which", return_value=None):
                results = native_profilers.run_profiles(
                    ("checked-program",),
                    profilers=("perf",),
                    output_dir=Path(directory),
                    skip_missing=True,
                )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].profiler, "perf")
        self.assertEqual(results[0].status, "skipped")
        self.assertEqual(results[0].returncode, 127)

    def test_run_profiles_writes_command_output_artifacts(self) -> None:
        completed = subprocess.CompletedProcess(
            args=("valgrind",),
            returncode=0,
            stdout="benchmark stdout\n",
            stderr="benchmark stderr\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            profile_dir = output_dir / "dhat"

            def _run_with_profile_artifacts(*args: object, **kwargs: object):
                del args, kwargs
                profile_dir.mkdir(parents=True, exist_ok=True)
                (profile_dir / "dhat.log").write_text("dhat log\n", encoding="utf-8")
                (profile_dir / "dhat.out").write_text("dhat profile\n", encoding="utf-8")
                return completed

            with mock.patch.object(
                native_profilers.shutil,
                "which",
                return_value="/usr/bin/valgrind",
            ):
                with mock.patch.object(
                    native_profilers.subprocess,
                    "run",
                    side_effect=_run_with_profile_artifacts,
                ) as run:
                    results = native_profilers.run_profiles(
                        ("checked-program", "--flag"),
                        profilers=("dhat",),
                        output_dir=output_dir,
                    )

            self.assertEqual(results[0].status, "ok")
            self.assertIn("--tool=dhat", run.call_args.args[0])
            self.assertEqual(
                (output_dir / "dhat" / "stdout.txt").read_text(encoding="utf-8"),
                "benchmark stdout\n",
            )
            self.assertEqual(
                (output_dir / "dhat" / "stderr.txt").read_text(encoding="utf-8"),
                "benchmark stderr\n",
            )

    def test_run_profiles_writes_machine_readable_summary(self) -> None:
        completed = subprocess.CompletedProcess(
            args=("valgrind",),
            returncode=0,
            stdout="benchmark stdout\n",
            stderr="benchmark stderr\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            profile_dir = output_dir / "dhat"

            def _run_with_profile_artifacts(*args: object, **kwargs: object):
                del args, kwargs
                profile_dir.mkdir(parents=True, exist_ok=True)
                (profile_dir / "dhat.log").write_text("dhat log\n", encoding="utf-8")
                (profile_dir / "dhat.out").write_text("dhat profile\n", encoding="utf-8")
                return completed

            with mock.patch.object(
                native_profilers.shutil,
                "which",
                return_value="/usr/bin/valgrind",
            ):
                with mock.patch.object(
                    native_profilers.subprocess,
                    "run",
                    side_effect=_run_with_profile_artifacts,
                ):
                    native_profilers.run_profiles(
                        ("checked-program", "--flag"),
                        profilers=("dhat",),
                        output_dir=output_dir,
                    )

            summary = json.loads(
                (output_dir / "summary.json").read_text(encoding="utf-8")
            )

        self.assertEqual(summary["command"], ["checked-program", "--flag"])
        self.assertEqual(summary["results"][0]["profiler"], "dhat")
        self.assertEqual(summary["results"][0]["status"], "ok")
        self.assertEqual(summary["results"][0]["returncode"], 0)

    def test_run_profiles_fails_when_expected_profile_output_is_missing(self) -> None:
        completed = subprocess.CompletedProcess(
            args=("valgrind",),
            returncode=0,
            stdout="benchmark stdout\n",
            stderr="benchmark stderr\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            with mock.patch.object(
                native_profilers.shutil,
                "which",
                return_value="/usr/bin/valgrind",
            ):
                with mock.patch.object(
                    native_profilers.subprocess,
                    "run",
                    return_value=completed,
                ):
                    results = native_profilers.run_profiles(
                        ("checked-program", "--flag"),
                        profilers=("dhat",),
                        output_dir=output_dir,
                    )

        self.assertEqual(results[0].status, "failed")
        self.assertIn("missing expected profile output", results[0].detail)
        self.assertIn("dhat.out", results[0].detail)

    def test_run_profiles_ignores_stale_profile_outputs(self) -> None:
        completed = subprocess.CompletedProcess(
            args=("valgrind",),
            returncode=0,
            stdout="benchmark stdout\n",
            stderr="benchmark stderr\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            stale_profile_dir = output_dir / "dhat"
            stale_profile_dir.mkdir(parents=True)
            (stale_profile_dir / "dhat.out").write_text(
                "stale profile\n",
                encoding="utf-8",
            )
            with mock.patch.object(
                native_profilers.shutil,
                "which",
                return_value="/usr/bin/valgrind",
            ):
                with mock.patch.object(
                    native_profilers.subprocess,
                    "run",
                    return_value=completed,
                ):
                    results = native_profilers.run_profiles(
                        ("checked-program", "--flag"),
                        profilers=("dhat",),
                        output_dir=output_dir,
                    )

            self.assertEqual(results[0].status, "failed")
            self.assertNotEqual(
                (output_dir / "dhat" / "stdout.txt").read_text(encoding="utf-8"),
                "stale profile\n",
            )
            self.assertFalse((output_dir / "dhat" / "dhat.out").exists())

    def test_heaptrack_summary_does_not_satisfy_raw_profile_output(self) -> None:
        profile_dir = Path("profiles/heaptrack")
        command = native_profilers._ProfilerCommand(
            command=("heaptrack",),
            post_processors=(),
            expected_outputs=(("glob", profile_dir, "heaptrack*"),),
        )
        with tempfile.TemporaryDirectory() as directory:
            real_profile_dir = Path(directory) / "heaptrack"
            real_profile_dir.mkdir()
            (real_profile_dir / "heaptrack.txt").write_text(
                "generated summary\n",
                encoding="utf-8",
            )
            command.expected_outputs = (("glob", real_profile_dir, "heaptrack*"),)

            missing = native_profilers._missing_expected_outputs(command)

        self.assertEqual(missing, (str(real_profile_dir / "heaptrack*"),))

    def test_verify_profiler_summary_requires_raw_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            profile_dir = output_dir / "dhat"
            profile_dir.mkdir()
            (profile_dir / "stdout.txt").write_text("", encoding="utf-8")
            (profile_dir / "stderr.txt").write_text("", encoding="utf-8")
            (profile_dir / "dhat.log").write_text("dhat log\n", encoding="utf-8")
            summary_path = output_dir / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "command": ["memory_retention_benchmark"],
                        "results": [
                            {
                                "detail": "profile captured",
                                "elapsed_seconds": 0.1,
                                "profiler": "dhat",
                                "returncode": 0,
                                "status": "ok",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(SystemExit) as raised:
                native_profilers.verify_profiler_summary(
                    summary_path,
                    required_profilers=("dhat",),
                    required_command_fragments=("memory_retention_benchmark",),
                )

        self.assertIn("dhat.out", str(raised.exception))

    def test_verify_profiler_summary_accepts_required_dhat_and_heaptrack(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            dhat_dir = output_dir / "dhat"
            dhat_dir.mkdir()
            for name in ("stdout.txt", "stderr.txt", "dhat.log", "dhat.out"):
                (dhat_dir / name).write_text("profile\n", encoding="utf-8")
            heaptrack_dir = output_dir / "heaptrack"
            heaptrack_dir.mkdir()
            for name in ("stdout.txt", "stderr.txt", "heaptrack.profile.gz"):
                (heaptrack_dir / name).write_text("profile\n", encoding="utf-8")
            summary_path = output_dir / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "command": [
                            "./target/profiling/memory_retention_benchmark",
                            "--materialize-state",
                        ],
                        "results": [
                            {
                                "detail": "profile captured",
                                "elapsed_seconds": 0.1,
                                "profiler": "dhat",
                                "returncode": 0,
                                "status": "ok",
                            },
                            {
                                "detail": "profile captured",
                                "elapsed_seconds": 0.2,
                                "profiler": "heaptrack",
                                "returncode": 0,
                                "status": "ok",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            summary = native_profilers.verify_profiler_summary(
                summary_path,
                required_profilers=("dhat", "heaptrack"),
                required_command_fragments=(
                    "memory_retention_benchmark",
                    "materialize-state",
                ),
            )

        self.assertEqual(len(summary["results"]), 2)

    def test_verify_profiler_summary_rejects_skipped_required_profiler(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            summary_path = Path(directory) / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "command": ["memory_retention_benchmark"],
                        "results": [
                            {
                                "detail": "heaptrack is not installed",
                                "elapsed_seconds": 0.0,
                                "profiler": "heaptrack",
                                "returncode": 127,
                                "status": "skipped",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(SystemExit) as raised:
                native_profilers.verify_profiler_summary(
                    summary_path,
                    required_profilers=("heaptrack",),
                )

        self.assertIn("profiler skipped: heaptrack", str(raised.exception))

    def test_verify_main_prints_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            dhat_dir = output_dir / "dhat"
            dhat_dir.mkdir()
            for name in ("stdout.txt", "stderr.txt", "dhat.log", "dhat.out"):
                (dhat_dir / name).write_text("profile\n", encoding="utf-8")
            summary_path = output_dir / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "command": ["memory_retention_benchmark"],
                        "results": [
                            {
                                "detail": "profile captured",
                                "elapsed_seconds": 0.1,
                                "profiler": "dhat",
                                "returncode": 0,
                                "status": "ok",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                native_profilers._verify_main(
                    (
                        str(summary_path),
                        "--require-profiler",
                        "dhat",
                    )
                )

        self.assertIn("native_profiler_summary passed=true", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
