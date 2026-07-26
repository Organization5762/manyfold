from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from manyfold.distributed_qualification import (
    REQUIRED_SCENARIOS,
    QualificationConfig,
    run_qualification,
    verify_qualification_artifact,
)


class DistributedQualificationTests(unittest.TestCase):
    def test_release_profile_runs_real_cluster_and_reports_missing_heart_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "qualification"

            artifact = run_qualification(QualificationConfig(output_dir=output))

            scenarios = {
                item["name"]: item
                for item in artifact["scenarios"]
                if isinstance(item, dict)
            }
            self.assertEqual(set(REQUIRED_SCENARIOS), set(scenarios))
            self.assertFalse(
                {
                    name
                    for name, item in scenarios.items()
                    if item["status"] == "fail"
                }
            )
            for name in REQUIRED_SCENARIOS[:12]:
                with self.subTest(name=name):
                    self.assertEqual(scenarios[name]["status"], "pass")
            verified = verify_qualification_artifact(output / "summary.json")
            self.assertEqual(verified["counts"], artifact["counts"])
            with self.assertRaisesRegex(ValueError, "release gate did not pass"):
                verify_qualification_artifact(
                    output / "summary.json",
                    require_pass=True,
                )

    def test_soak_configuration_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            for seconds in (599, 1801):
                with self.subTest(seconds=seconds):
                    config = QualificationConfig(
                        output_dir=Path(directory) / str(seconds),
                        profile="soak",
                        soak_seconds=seconds,
                    )
                    with self.assertRaisesRegex(ValueError, "soak_seconds"):
                        config.validate()

    def test_output_directory_must_be_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "owned").write_text("value", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "must be empty"):
                run_qualification(QualificationConfig(output_dir=output))


if __name__ == "__main__":
    unittest.main()
