from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from manyfold import monitor_artifacts


class MonitorArtifactTests(unittest.TestCase):
    def test_verify_accepts_checked_passing_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(
                Path(directory),
                _artifact(sample_count=12, retained_sample_count=1),
            )

            artifact = monitor_artifacts.verify_monitor_artifact(
                path,
                min_samples=10,
                required_command_fragments=("lib_2026",),
                required_gates=(
                    "external_rss_plateau_kib",
                    "external_rss_tail_plateau_kib",
                    "external_rss_tail_plateau_seconds",
                    "external_rss_tail_plateau_samples",
                ),
                required_gate_limits={
                    "external_rss_tail_plateau_kib": 1024,
                    "external_rss_tail_plateau_seconds": 8.0,
                },
                required_gate_modes={
                    "external_rss_tail_plateau_kib": "max",
                    "external_rss_tail_plateau_seconds": "min",
                },
                required_metadata={
                    "manyfold_bridge_version": "0.1.38",
                    "manyfold_source": "/tmp/manyfold/python/manyfold/__init__.py",
                },
                required_metrics=("rss", "private", "fd"),
                required_sample_fields=("rss", "private", "fd"),
            )

        self.assertTrue(artifact["passed"])
        self.assertEqual(artifact["sample_count"], 12)

    def test_verify_rejects_missing_required_sample_field(self) -> None:
        artifact = _artifact()
        artifact["samples"] = [
            {
                "anonymous_kib": None,
                "current_rss_kib": 32_000,
                "elapsed_seconds": 0.0,
                "fd_count": None,
                "private_kib": None,
                "pss_kib": None,
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "sample field unavailable"):
                monitor_artifacts.verify_monitor_artifact(
                    path,
                    required_sample_fields=("private",),
                )

    def test_verify_rejects_unavailable_required_metric(self) -> None:
        artifact = _artifact()
        artifact["metrics"] = {
            **artifact["metrics"],
            "private_kib": {
                "project_seconds": 86_400.0,
                "projected_growth": None,
                "sample_count": 0,
                "segment_projected_growth": None,
                "steady_max": None,
                "steady_min": None,
                "steady_range": None,
                "steady_sample_count": 0,
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "metric unavailable"):
                monitor_artifacts.verify_monitor_artifact(
                    path,
                    required_metrics=("private",),
                )

    def test_verify_uses_sample_count_when_artifact_rows_are_truncated(self) -> None:
        artifact = _artifact(sample_count=10, retained_sample_count=2)
        artifact["samples"] = [
            {"current_rss_kib": 0, "elapsed_seconds": 8.0},
            {"current_rss_kib": 0, "elapsed_seconds": 9.0},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            monitor_artifacts.verify_monitor_artifact(path, min_samples=10)

    def test_verify_rejects_failed_artifact(self) -> None:
        artifact = _artifact(passed=False, failed_gates=("external_rss_plateau_kib",))
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "failed gates"):
                monitor_artifacts.verify_monitor_artifact(path)

    def test_verify_rejects_unavailable_required_signal(self) -> None:
        artifact = _artifact(unavailable_gates=("external_private_plateau_kib",))
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "unavailable gates"):
                monitor_artifacts.verify_monitor_artifact(path)

    def test_verify_rejects_missing_required_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), _artifact())

            with self.assertRaisesRegex(SystemExit, "required gate missing"):
                monitor_artifacts.verify_monitor_artifact(
                    path,
                    required_gates=("external_private_plateau_kib",),
                )

    def test_verify_rejects_wrong_required_gate_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), _artifact())

            with self.assertRaisesRegex(SystemExit, "limit expected"):
                monitor_artifacts.verify_monitor_artifact(
                    path,
                    required_gate_limits={"external_rss_tail_plateau_kib": 0},
                )

    def test_verify_rejects_wrong_required_gate_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), _artifact())

            with self.assertRaisesRegex(SystemExit, "mode expected"):
                monitor_artifacts.verify_monitor_artifact(
                    path,
                    required_gate_modes={"external_rss_tail_plateau_kib": "min"},
                )

    def test_verify_rejects_required_limit_for_missing_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), _artifact())

            with self.assertRaisesRegex(SystemExit, "required gate missing"):
                monitor_artifacts.verify_monitor_artifact(
                    path,
                    required_gate_limits={"external_private_projected_growth_kib": 0},
                )

    def test_verify_rejects_gate_record_failure_even_when_summary_is_wrong(self) -> None:
        artifact = _artifact()
        artifact["gates"][0]["passed"] = False
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "gate records failed"):
                monitor_artifacts.verify_monitor_artifact(path)

    def test_verify_rejects_gate_record_unavailable_even_when_summary_is_wrong(
        self,
    ) -> None:
        artifact = _artifact()
        artifact["gates"][0]["available"] = False
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "gate records unavailable"):
                monitor_artifacts.verify_monitor_artifact(path)

    def test_verify_rejects_wrong_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), _artifact())

            with self.assertRaisesRegex(SystemExit, "command missing"):
                monitor_artifacts.verify_monitor_artifact(
                    path,
                    required_command_fragments=("lib_2025",),
                )

    def test_verify_rejects_unchecked_artifact_by_default(self) -> None:
        artifact = _artifact(checks_enabled=False)
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "checks disabled"):
                monitor_artifacts.verify_monitor_artifact(path)

            monitor_artifacts.verify_monitor_artifact(path, require_checks=False)

    def test_verify_rejects_missing_required_metadata(self) -> None:
        artifact = _artifact()
        artifact["metadata"] = {}
        with tempfile.TemporaryDirectory() as directory:
            path = _write_artifact(Path(directory), artifact)

            with self.assertRaisesRegex(SystemExit, "metadata"):
                monitor_artifacts.verify_monitor_artifact(
                    path,
                    required_metadata={"manyfold_bridge_version": "0.1.38"},
                )

    def test_parse_rejects_malformed_required_metadata(self) -> None:
        with self.assertRaisesRegex(SystemExit, "KEY=VALUE"):
            monitor_artifacts._parse_required_metadata(("manyfold_source",))

    def test_parse_rejects_malformed_required_gate_limit(self) -> None:
        with self.assertRaisesRegex(SystemExit, "NAME=VALUE"):
            monitor_artifacts._parse_required_gate_limits(("external_rss",))

        with self.assertRaisesRegex(SystemExit, "numeric"):
            monitor_artifacts._parse_required_gate_limits(("external_rss=flat",))


def _artifact(
    *,
    checks_enabled: bool = True,
    failed_gates: tuple[str, ...] = (),
    passed: bool = True,
    retained_sample_count: int = 1,
    sample_count: int = 1,
    unavailable_gates: tuple[str, ...] = (),
) -> dict[str, object]:
    return {
        "checks_enabled": checks_enabled,
        "command": ["uv", "run", "totem", "run", "--configuration", "lib_2026"],
        "failed_gates": list(failed_gates),
        "gates": [
            {
                "available": True,
                "field": "steady_range",
                "limit": 1024,
                "metric": "rss_kib",
                "mode": "max",
                "name": "external_rss_plateau_kib",
                "observed": 0,
                "passed": True,
            },
            {
                "available": True,
                "field": "value",
                "limit": 1024,
                "metric": "external_rss_tail_plateau_kib",
                "mode": "max",
                "name": "external_rss_tail_plateau_kib",
                "observed": 288,
                "passed": True,
            },
            {
                "available": True,
                "field": "value",
                "limit": 8.0,
                "metric": "external_rss_tail_plateau_seconds",
                "mode": "min",
                "name": "external_rss_tail_plateau_seconds",
                "observed": 35.8,
                "passed": True,
            },
            {
                "available": True,
                "field": "value",
                "limit": 5,
                "metric": "external_rss_tail_plateau_samples",
                "mode": "min",
                "name": "external_rss_tail_plateau_samples",
                "observed": 35,
                "passed": True,
            },
        ],
        "metrics": {
            "anonymous_kib": _metric_summary(8_000),
            "fd_count": _metric_summary(42),
            "private_kib": _metric_summary(10_000),
            "pss_kib": _metric_summary(20_000),
            "rss_kib": _metric_summary(32_000),
        },
        "metadata": {
            "manyfold_bridge_version": "0.1.38",
            "manyfold_source": "/tmp/manyfold/python/manyfold/__init__.py",
        },
        "passed": passed,
        "retained_sample_count": retained_sample_count,
        "sample_count": sample_count,
        "sample_retention_limit": 512,
        "samples": [
            {
                "anonymous_kib": 8_000,
                "current_rss_kib": 32_000,
                "elapsed_seconds": 0.0,
                "fd_count": 42,
                "private_kib": 10_000,
                "pss_kib": 20_000,
            }
        ],
        "status": {"returncode": 0},
        "unavailable_gates": list(unavailable_gates),
    }


def _metric_summary(value: int) -> dict[str, object]:
    return {
        "project_seconds": 86_400.0,
        "projected_growth": 0,
        "sample_count": 1,
        "segment_projected_growth": 0,
        "steady_max": value,
        "steady_min": value,
        "steady_range": 0,
        "steady_sample_count": 1,
    }


def _write_artifact(directory: Path, artifact: dict[str, object]) -> Path:
    path = directory / "monitor.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
