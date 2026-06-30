from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from manyfold.private.profiling import profile_artifacts

from tests.test_support import subprocess_test_env


class ProfileArtifactTests(unittest.TestCase):
    def test_verify_accepts_application_artifact_with_required_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime-substrate.json"
            path.write_text(
                json.dumps(_artifact(example="runtime_substrate")),
                encoding="utf-8",
            )

            artifact = profile_artifacts.verify_profile_artifact(
                path,
                kind="application",
                name="runtime_substrate",
                mode="parallel",
                require_gate="max_average_event_us",
            )

        self.assertEqual(artifact["example"], "runtime_substrate")

    def test_verify_accepts_runtime_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime-rpc.json"
            path.write_text(json.dumps(_artifact(workload="rpc")), encoding="utf-8")

            artifact = profile_artifacts.verify_profile_artifact(
                path,
                kind="runtime",
                name="rpc",
                mode="parallel",
            )

        self.assertEqual(artifact["workload"], "rpc")

    def test_verify_rejects_failed_artifact_and_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "failed.json"
            path.write_text(
                json.dumps(
                    _artifact(
                        example="runtime_substrate",
                        passed=False,
                        gate_passed=False,
                    )
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "artifact did not pass; gate failed: max_average_event_us",
            ):
                profile_artifacts.verify_profile_artifact(path)

    def test_verify_rejects_missing_required_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing-gate.json"
            artifact = _artifact(workload="rpc")
            artifact["gates"] = []
            path.write_text(json.dumps(artifact), encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError,
                "required gate missing: max_elapsed_seconds",
            ):
                profile_artifacts.verify_profile_artifact(
                    path,
                    require_gate="max_elapsed_seconds",
                )

    def test_verify_rejects_kind_name_and_mode_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime-rpc.json"
            path.write_text(json.dumps(_artifact(workload="rpc")), encoding="utf-8")

            with self.assertRaisesRegex(
                ValueError,
                "artifact kind mismatch: expected application, observed runtime",
            ):
                profile_artifacts.verify_profile_artifact(
                    path,
                    kind="application",
                    name="runtime_substrate",
                    mode="standard",
                )

    def test_cli_accepts_saved_runtime_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "runtime-rpc.json"
            artifact_path.write_text(
                json.dumps(_artifact(workload="rpc")),
                encoding="utf-8",
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "manyfold.private.profiling.profile_artifacts",
                    str(artifact_path),
                    "--kind",
                    "runtime",
                    "--name",
                    "rpc",
                    "--mode",
                    "parallel",
                    "--require-gate",
                    "max_average_event_us",
                ],
                check=False,
                capture_output=True,
                text=True,
                cwd=Path(__file__).resolve().parents[1],
                env=subprocess_test_env(),
            )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("profile_artifact passed=true name=rpc", result.stdout)

    def test_script_rejects_failed_saved_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "failed.json"
            artifact_path.write_text(
                json.dumps(
                    _artifact(
                        example="runtime_substrate",
                        passed=False,
                        gate_passed=False,
                    )
                ),
                encoding="utf-8",
            )
            result = subprocess.run(
                [
                    "uv",
                    "run",
                    "manyfold-profile-artifact-verify",
                    str(artifact_path),
                ],
                check=False,
                capture_output=True,
                text=True,
                cwd=Path(__file__).resolve().parents[1],
                env=subprocess_test_env(),
            )

        self.assertEqual(result.returncode, 1)
        self.assertIn("artifact did not pass", result.stderr)


def _artifact(
    *,
    example: str | None = None,
    workload: str | None = None,
    passed: bool = True,
    gate_passed: bool = True,
) -> dict[str, object]:
    artifact: dict[str, object] = {
        "average_event_us": 10.0,
        "elapsed_seconds": 0.01,
        "gates": [
            {
                "field": "average_event_us",
                "limit": 100.0,
                "name": "max_average_event_us",
                "observed": 10.0,
                "passed": gate_passed,
            }
        ],
        "max_seconds": 0.01,
        "mean_seconds": 0.01,
        "min_seconds": 0.01,
        "mode": "parallel",
        "passed": passed,
        "runs": 1,
        "stdev_seconds": 0.0,
    }
    if example is not None:
        artifact["example"] = example
    if workload is not None:
        artifact["workload"] = workload
    return artifact


if __name__ == "__main__":
    unittest.main()
