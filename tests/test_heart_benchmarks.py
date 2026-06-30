from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from manyfold import heart_benchmarks, memory_benchmarks


class HeartBenchmarkTests(unittest.TestCase):
    def test_monitor_args_builds_lib_2026_totem_command_and_local_pythonpath(
        self,
    ) -> None:
        args = heart_benchmarks._parse_args(
            (
                "--heart-root",
                "/tmp/heart",
                "--local-manyfold-python",
                "/tmp/manyfold/python",
                "--duration-seconds",
                "5",
                "--output-json",
                "/tmp/heart-monitor.json",
                "--external-min-elapsed-seconds",
                "1",
                "--extra-pythonpath",
                "/tmp/extra",
                "--no-check",
            )
        )

        monitor_args = heart_benchmarks._monitor_args(args)
        pythonpath = monitor_args.external_env["PYTHONPATH"].split(os.pathsep)

        self.assertEqual(Path(monitor_args.command[0]).name, "uv")
        self.assertEqual(
            monitor_args.command[1:],
            ("run", "totem", "run", "--configuration", "lib_2026"),
        )
        self.assertEqual(monitor_args.external_cwd, "/tmp/heart")
        self.assertEqual(
            pythonpath[:3],
            ["/tmp/manyfold/python", "/tmp/heart/src", "/tmp/extra"],
        )
        self.assertEqual(monitor_args.external_duration_seconds, 5.0)
        self.assertEqual(
            monitor_args.external_output_json,
            Path("/tmp/heart-monitor.json"),
        )
        self.assertEqual(monitor_args.external_output_max_samples, 512)
        self.assertEqual(monitor_args.external_min_elapsed_seconds, 1.0)
        self.assertEqual(monitor_args.external_rss_projected_growth_kib, 0)
        self.assertEqual(monitor_args.external_rss_segment_projected_growth_kib, 0)
        self.assertEqual(monitor_args.external_rss_tail_plateau_kib, 1024)
        self.assertEqual(monitor_args.external_tail_min_samples, 5)
        self.assertEqual(monitor_args.external_tail_min_seconds, 8.0)
        self.assertIsNone(monitor_args.external_pss_projected_growth_kib)
        self.assertIsNone(monitor_args.external_private_projected_growth_kib)
        self.assertIsNone(monitor_args.external_anonymous_projected_growth_kib)
        self.assertIsNone(monitor_args.external_fd_plateau_count)
        self.assertEqual(monitor_args.rss_warmup_fraction, 0.75)
        self.assertFalse(monitor_args.check)

    def test_projected_growth_defaults_can_be_relaxed_explicitly(self) -> None:
        args = heart_benchmarks._parse_args(
            (
                "--heart-root",
                "/tmp/heart",
                "--external-rss-projected-growth-kib",
                "1024",
                "--external-rss-segment-projected-growth-kib",
                "512",
                "--external-rss-tail-plateau-kib",
                "2048",
                "--external-tail-min-seconds",
                "3",
                "--external-tail-min-samples",
                "4",
            )
        )

        monitor_args = heart_benchmarks._monitor_args(args)

        self.assertEqual(monitor_args.external_rss_projected_growth_kib, 1024)
        self.assertEqual(monitor_args.external_rss_segment_projected_growth_kib, 512)
        self.assertEqual(monitor_args.external_rss_tail_plateau_kib, 2048)
        self.assertEqual(monitor_args.external_tail_min_seconds, 3.0)
        self.assertEqual(monitor_args.external_tail_min_samples, 4)

    def test_strict_device_memory_gates_require_private_memory_signals(self) -> None:
        args = heart_benchmarks._parse_args(
            (
                "--heart-root",
                "/tmp/heart",
                "--strict-device-memory-gates",
            )
        )

        monitor_args = heart_benchmarks._monitor_args(args)

        self.assertEqual(monitor_args.external_pss_projected_growth_kib, 0)
        self.assertEqual(monitor_args.external_pss_segment_projected_growth_kib, 0)
        self.assertEqual(monitor_args.external_private_projected_growth_kib, 0)
        self.assertEqual(
            monitor_args.external_private_segment_projected_growth_kib,
            0,
        )
        self.assertEqual(monitor_args.external_anonymous_projected_growth_kib, 0)
        self.assertEqual(
            monitor_args.external_anonymous_segment_projected_growth_kib,
            0,
        )
        self.assertEqual(monitor_args.external_fd_plateau_count, 0)
        self.assertEqual(monitor_args.external_fd_projected_growth_count, 0)
        self.assertEqual(
            monitor_args.external_fd_segment_projected_growth_count,
            0,
        )

    def test_strict_device_memory_gates_keep_explicit_relaxed_limits(self) -> None:
        args = heart_benchmarks._parse_args(
            (
                "--heart-root",
                "/tmp/heart",
                "--strict-device-memory-gates",
                "--external-pss-projected-growth-kib",
                "128",
                "--external-private-segment-projected-growth-kib",
                "64",
                "--external-fd-plateau-count",
                "2",
            )
        )

        monitor_args = heart_benchmarks._monitor_args(args)

        self.assertEqual(monitor_args.external_pss_projected_growth_kib, 128)
        self.assertEqual(
            monitor_args.external_private_segment_projected_growth_kib,
            64,
        )
        self.assertEqual(monitor_args.external_fd_plateau_count, 2)
        self.assertEqual(monitor_args.external_fd_projected_growth_count, 0)

    def test_heart_monitor_verifier_requires_strict_device_gates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), _heart_monitor_artifact())

            artifact = heart_benchmarks.verify_heart_monitor_artifact(path)

        self.assertTrue(artifact["passed"])

    def test_heart_monitor_verifier_rejects_relaxed_gate_limit(self) -> None:
        artifact = _heart_monitor_artifact()
        artifact["gates"][0]["limit"] = 1
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "limit expected"):
                heart_benchmarks.verify_heart_monitor_artifact(path)

    def test_verify_main_prints_heart_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), _heart_monitor_artifact())
            output = io.StringIO()

            with contextlib.redirect_stdout(output):
                heart_benchmarks._verify_main((str(path),))

        self.assertIn(
            "heart_monitor_artifact passed=true samples=30",
            output.getvalue(),
        )

    def test_main_reuses_external_memory_monitor(self) -> None:
        with (
            mock.patch.object(
                heart_benchmarks,
                "_verify_local_manyfold_import",
                return_value={
                    "manyfold_bridge_version": "0.1.40",
                    "manyfold_source": "/tmp/manyfold/python/manyfold/__init__.py",
                },
            ) as verify,
            mock.patch.object(
                memory_benchmarks,
                "_run_external_command_monitor",
                return_value=(),
            ) as monitor,
        ):
            heart_benchmarks._main(
                (
                    "--heart-root",
                    "/tmp/heart",
                    "--duration-seconds",
                    "5",
                    "--external-min-elapsed-seconds",
                    "1",
                    "--external-min-samples",
                    "1",
                    "--no-check",
                )
            )

        monitor_args = monitor.call_args.args[0]
        self.assertEqual(verify.call_args.args[0], monitor_args)
        self.assertEqual(
            verify.call_args.args[1],
            heart_benchmarks.DEFAULT_VERIFY_PYTHON_COMMAND,
        )
        self.assertEqual(monitor_args.external_cwd, "/tmp/heart")
        self.assertEqual(
            monitor_args.external_metadata,
            {
                "manyfold_bridge_version": "0.1.40",
                "manyfold_source": "/tmp/manyfold/python/manyfold/__init__.py",
            },
        )
        self.assertEqual(monitor_args.external_rss_scope, "tree")
        self.assertEqual(monitor_args.external_terminate_scope, "tree")

    def test_main_can_skip_local_manyfold_preflight(self) -> None:
        with (
            mock.patch.object(
                heart_benchmarks,
                "_verify_local_manyfold_import",
            ) as verify,
            mock.patch.object(
                memory_benchmarks,
                "_run_external_command_monitor",
                return_value=(),
            ) as monitor,
        ):
            heart_benchmarks._main(
                (
                    "--heart-root",
                    "/tmp/heart",
                    "--duration-seconds",
                    "5",
                    "--external-min-elapsed-seconds",
                    "1",
                    "--external-min-samples",
                    "1",
                    "--no-verify-local-manyfold",
                    "--no-check",
                )
            )

        verify.assert_not_called()
        self.assertEqual(monitor.call_args.args[0].external_cwd, "/tmp/heart")

    def test_verify_local_manyfold_import_uses_heart_environment(self) -> None:
        monitor_args = heart_benchmarks._monitor_args(
            heart_benchmarks._parse_args(
                (
                    "--heart-root",
                    "/tmp/heart",
                    "--local-manyfold-python",
                    "/tmp/manyfold/python",
                    "--verify-python-command",
                    "/tmp/uv",
                    "run",
                    "python",
                )
            )
        )
        completed = subprocess.CompletedProcess(
            args=(),
            returncode=0,
            stdout=(
                "manyfold_import source=/tmp/manyfold/python/manyfold/__init__.py\n"
                "manyfold_bridge_version=0.1.40\n"
            ),
            stderr="",
        )

        with mock.patch.object(
            heart_benchmarks.subprocess,
            "run",
            return_value=completed,
        ) as run:
            metadata = heart_benchmarks._verify_local_manyfold_import(
                monitor_args,
                ("/tmp/uv", "run", "python"),
                Path("/tmp/manyfold/python"),
            )

        self.assertEqual(
            metadata,
            {
                "manyfold_bridge_version": "0.1.40",
                "manyfold_source": "/tmp/manyfold/python/manyfold/__init__.py",
            },
        )
        kwargs = run.call_args.kwargs
        self.assertEqual(run.call_args.args[0][:3], ("/tmp/uv", "run", "python"))
        self.assertEqual(kwargs["cwd"], "/tmp/heart")
        self.assertEqual(kwargs["env"], monitor_args.external_env)
        self.assertTrue(kwargs["check"])
        self.assertTrue(kwargs["capture_output"])
        self.assertIn("/tmp/manyfold/python", run.call_args.args[0][-1])

    def test_verify_local_manyfold_import_reports_failure(self) -> None:
        monitor_args = heart_benchmarks._monitor_args(
            heart_benchmarks._parse_args(("--heart-root", "/tmp/heart"))
        )
        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=("python", "-c", "import manyfold"),
            stderr="wrong manyfold",
        )

        with mock.patch.object(
            heart_benchmarks.subprocess,
            "run",
            side_effect=error,
        ):
            with self.assertRaisesRegex(SystemExit, "wrong manyfold"):
                heart_benchmarks._verify_local_manyfold_import(
                    monitor_args,
                    ("python",),
                    Path("/tmp/manyfold/python"),
                )

    def test_verify_local_manyfold_import_rejects_missing_metadata(self) -> None:
        monitor_args = heart_benchmarks._monitor_args(
            heart_benchmarks._parse_args(("--heart-root", "/tmp/heart"))
        )
        completed = subprocess.CompletedProcess(
            args=(),
            returncode=0,
            stdout="manyfold_import source=/tmp/manyfold/python/manyfold/__init__.py\n",
            stderr="",
        )

        with mock.patch.object(
            heart_benchmarks.subprocess,
            "run",
            return_value=completed,
        ):
            with self.assertRaisesRegex(SystemExit, "bridge_version"):
                heart_benchmarks._verify_local_manyfold_import(
                    monitor_args,
                    ("python",),
                    Path("/tmp/manyfold/python"),
                )

    def test_totem_command_requires_prefix(self) -> None:
        with self.assertRaisesRegex(SystemExit, "--totem-command"):
            heart_benchmarks._totem_command((), "lib_2026")

    def test_heart_env_prepends_manyfold_and_heart_sources(self) -> None:
        with mock.patch.dict(os.environ, {"PYTHONPATH": "/existing"}, clear=False):
            env = heart_benchmarks._heart_env(
                Path("/tmp/heart"),
                manyfold_python=Path("/tmp/manyfold/python"),
                extra_pythonpath=("/tmp/extra",),
            )

        self.assertEqual(
            env["PYTHONPATH"].split(os.pathsep),
            ["/tmp/manyfold/python", "/tmp/heart/src", "/tmp/extra", "/existing"],
        )


def _heart_monitor_artifact() -> dict[str, object]:
    return {
        "checks_enabled": True,
        "command": ["totem", "run", "--configuration", "lib_2026"],
        "failed_gates": [],
        "gates": [
            _gate("external_anonymous_projected_growth_kib", "anonymous_kib"),
            _gate(
                "external_anonymous_segment_projected_growth_kib",
                "anonymous_kib",
                field="segment_projected_growth",
            ),
            _gate("external_fd_plateau_count", "fd_count", field="steady_range"),
            _gate(
                "external_fd_segment_projected_growth_count",
                "fd_count",
                field="segment_projected_growth",
            ),
            _gate("external_private_projected_growth_kib", "private_kib"),
            _gate(
                "external_private_segment_projected_growth_kib",
                "private_kib",
                field="segment_projected_growth",
            ),
            _gate("external_pss_projected_growth_kib", "pss_kib"),
            _gate(
                "external_pss_segment_projected_growth_kib",
                "pss_kib",
                field="segment_projected_growth",
            ),
        ],
        "metrics": {
            "anonymous_kib": _metric_summary(40),
            "fd_count": _metric_summary(5),
            "private_kib": _metric_summary(60),
            "pss_kib": _metric_summary(80),
            "rss_kib": _metric_summary(100),
        },
        "metadata": {},
        "passed": True,
        "retained_sample_count": 1,
        "sample_count": 30,
        "sample_retention_limit": 512,
        "samples": [
            {
                "anonymous_kib": 40,
                "current_rss_kib": 100,
                "elapsed_seconds": 300.0,
                "fd_count": 5,
                "private_kib": 60,
                "pss_kib": 80,
            }
        ],
        "status": {"returncode": 0},
        "unavailable_gates": [],
    }


def _gate(
    name: str,
    metric: str,
    *,
    field: str = "projected_growth",
) -> dict[str, object]:
    return {
        "available": True,
        "field": field,
        "limit": 0,
        "metric": metric,
        "mode": "max",
        "name": name,
        "observed": 0,
        "passed": True,
    }


def _metric_summary(value: int) -> dict[str, object]:
    return {
        "project_seconds": 86_400.0,
        "projected_growth": 0,
        "sample_count": 30,
        "segment_projected_growth": 0,
        "steady_max": value,
        "steady_min": value,
        "steady_range": 0,
        "steady_sample_count": 30,
    }


def _write_artifact(directory: Path, artifact: dict[str, object]) -> Path:
    path = directory / "heart-monitor.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
