"""Release qualification for distributed startup and recovery."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal

from .private.distributed_qualification_scenarios import (
    run_release_scenarios,
    run_soak_scenario,
)

ARTIFACT_SCHEMA_VERSION = 1
DEFAULT_SCENARIO_TIMEOUT_SECONDS = 10.0
DEFAULT_SOAK_SECONDS = 600
MAX_CONCURRENT_PROCESSES = 3
MAX_SCENARIO_TIMEOUT_SECONDS = 30.0
MAX_SOAK_SAMPLES = 512
MAX_SOAK_SECONDS = 1800
MIN_SOAK_SECONDS = 600
REQUIRED_SCENARIOS = (
    "first_node_boot",
    "three_node_convergence",
    "simultaneous_cold_start",
    "duplicate_identities",
    "stale_and_malformed_mdns_dns_candidates",
    "partition_and_asymmetric_loss",
    "leader_kill",
    "process_restart",
    "unavailable_quorum",
    "corrupt_and_truncated_local_state",
    "disk_full_and_write_failure",
    "deterministic_shutdown",
    "authenticated_transport",
    "certificate_expiry_and_rotation",
    "node_bootstrap_lifecycle",
    "machine_signer_identity_and_shared_processes",
    "machine_signer_authorization_and_availability",
    "machine_signer_credential_lifecycle",
    "machine_signer_bounds_and_key_isolation",
    "heart_navigation_sensor_and_hot_paths",
    "heart_machine_signer_lifecycle",
    "heart_raft_rpc_leader_failure",
)


def run_qualification(config: QualificationConfig) -> dict[str, object]:
    """Run the matrix and atomically write its machine-readable summary."""
    config.validate()
    _prepare_output(config.output_dir)
    started = time.monotonic()
    scenarios = list(
        run_release_scenarios(
            config.output_dir,
            timeout_seconds=config.scenario_timeout_seconds,
            heart_artifact_dir=config.heart_artifact_dir,
        )
    )
    if config.profile == "soak":
        scenarios.append(
            run_soak_scenario(
                config.output_dir,
                duration_seconds=config.soak_seconds,
                sample_interval_seconds=config.soak_sample_interval_seconds,
                timeout_seconds=config.scenario_timeout_seconds,
            )
        )
    counts = {
        status: sum(item.status == status for item in scenarios)
        for status in ("pass", "fail", "blocked")
    }
    passed = counts["fail"] == counts["blocked"] == 0
    artifact: dict[str, object] = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "suite": "manyfold_distributed_startup_recovery",
        "profile": config.profile,
        "passed": passed,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "package": {
            "name": "manyfold",
            "version": version("manyfold"),
            "python": platform.python_version(),
            "platform": platform.platform(),
        },
        "limits": {
            "max_concurrent_processes": MAX_CONCURRENT_PROCESSES,
            "scenario_timeout_seconds": config.scenario_timeout_seconds,
            "soak_duration_range_seconds": [MIN_SOAK_SECONDS, MAX_SOAK_SECONDS],
            "max_soak_samples": MAX_SOAK_SAMPLES,
        },
        "counts": counts,
        "scenarios": [asdict(item) for item in scenarios],
        "proven_guarantees": [
            item.guarantee for item in scenarios if item.status == "pass"
        ],
        "release_blockers": [
            {
                "scenario": item.name,
                "status": item.status,
                "reason": item.detail,
            }
            for item in scenarios
            if item.status != "pass"
        ],
    }
    _write_json_atomic(config.output_dir / "summary.json", artifact)
    return artifact


def verify_qualification_artifact(
    path: Path,
    *,
    require_pass: bool = False,
) -> dict[str, object]:
    """Validate one generated qualification summary."""
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid qualification artifact {path}: {error}") from error
    if not isinstance(artifact, dict):
        raise ValueError("qualification artifact must be a JSON object")
    errors = _artifact_errors(artifact, require_pass=require_pass)
    if errors:
        raise ValueError("; ".join(errors))
    return artifact


@dataclass(frozen=True)
class QualificationConfig:
    """Bounded release or soak configuration."""

    output_dir: Path
    profile: Literal["release", "soak"] = "release"
    heart_artifact_dir: Path | None = None
    scenario_timeout_seconds: float = DEFAULT_SCENARIO_TIMEOUT_SECONDS
    soak_seconds: int = DEFAULT_SOAK_SECONDS
    soak_sample_interval_seconds: float = 5.0

    def validate(self) -> None:
        if self.profile not in ("release", "soak"):
            raise ValueError("profile must be release or soak")
        if self.heart_artifact_dir is not None and not self.heart_artifact_dir.is_dir():
            raise ValueError(
                f"heart_artifact_dir must be a directory: {self.heart_artifact_dir}"
            )
        if not 0 < self.scenario_timeout_seconds <= MAX_SCENARIO_TIMEOUT_SECONDS:
            raise ValueError(
                "scenario_timeout_seconds must be in "
                f"(0, {MAX_SCENARIO_TIMEOUT_SECONDS}]"
            )
        if self.profile == "soak" and not (
            MIN_SOAK_SECONDS <= self.soak_seconds <= MAX_SOAK_SECONDS
        ):
            raise ValueError(
                f"soak_seconds must be in [{MIN_SOAK_SECONDS}, {MAX_SOAK_SECONDS}]"
            )
        if not 0 < self.soak_sample_interval_seconds <= 30:
            raise ValueError("soak_sample_interval_seconds must be in (0, 30]")
        samples = (
            math.ceil(self.soak_seconds / self.soak_sample_interval_seconds) + 1
        )
        if self.profile == "soak" and samples > MAX_SOAK_SAMPLES:
            raise ValueError("soak sample interval would exceed the sample bound")


def _main() -> None:
    args = _parser().parse_args()
    config = QualificationConfig(
        output_dir=args.output_dir,
        profile=args.profile,
        heart_artifact_dir=args.heart_artifact_dir,
        scenario_timeout_seconds=args.scenario_timeout_seconds,
        soak_seconds=args.soak_seconds,
        soak_sample_interval_seconds=args.soak_sample_interval_seconds,
    )
    artifact = run_qualification(config)
    counts = artifact["counts"]
    if not isinstance(counts, dict):
        raise RuntimeError("generated qualification counts are not an object")
    print(
        "distributed_qualification "
        f"passed={str(artifact['passed']).lower()} "
        f"pass={counts['pass']} fail={counts['fail']} "
        f"blocked={counts['blocked']} "
        f"artifact={config.output_dir / 'summary.json'}"
    )
    if not args.diagnostic_only and artifact["passed"] is not True:
        raise SystemExit(1)


def _verify_main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify a ManyFold distributed qualification artifact."
    )
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--require-pass", action="store_true")
    args = parser.parse_args()
    artifact = verify_qualification_artifact(
        args.artifact,
        require_pass=args.require_pass,
    )
    counts = artifact["counts"]
    if not isinstance(counts, dict):
        raise RuntimeError("verified qualification counts are not an object")
    print(
        "distributed_qualification_artifact "
        f"passed={str(artifact['passed']).lower()} "
        f"pass={counts['pass']} fail={counts['fail']} blocked={counts['blocked']}"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run bounded distributed startup and recovery qualification."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile", choices=("release", "soak"), default="release")
    parser.add_argument("--heart-artifact-dir", type=Path)
    parser.add_argument(
        "--scenario-timeout-seconds",
        type=float,
        default=DEFAULT_SCENARIO_TIMEOUT_SECONDS,
    )
    parser.add_argument("--soak-seconds", type=int, default=DEFAULT_SOAK_SECONDS)
    parser.add_argument(
        "--soak-sample-interval-seconds",
        type=float,
        default=5.0,
    )
    parser.add_argument(
        "--diagnostic-only",
        action="store_true",
        help="Write diagnostics and return zero even when the release gate fails.",
    )
    return parser


def _prepare_output(path: Path) -> None:
    if path.exists():
        if not path.is_dir():
            raise ValueError(f"output is not a directory: {path}")
        if any(path.iterdir()):
            raise ValueError(f"output directory must be empty: {path}")
    else:
        path.mkdir(parents=True)


def _artifact_errors(
    artifact: dict[str, object],
    *,
    require_pass: bool,
) -> tuple[str, ...]:
    errors: list[str] = []
    if artifact.get("schema_version") != ARTIFACT_SCHEMA_VERSION:
        errors.append("unsupported schema_version")
    if artifact.get("suite") != "manyfold_distributed_startup_recovery":
        errors.append("suite mismatch")
    scenarios = artifact.get("scenarios")
    if not isinstance(scenarios, list):
        return (*errors, "scenarios must be a list")
    names: list[str] = []
    counts = {"pass": 0, "fail": 0, "blocked": 0}
    for item in scenarios:
        if not isinstance(item, dict):
            errors.append("scenario must be an object")
            continue
        name = item.get("name")
        status = item.get("status")
        if isinstance(name, str):
            names.append(name)
        else:
            errors.append("scenario name must be a string")
        if status in counts:
            counts[status] += 1
        else:
            errors.append(f"invalid scenario status: {status!r}")
        if not isinstance(item.get("evidence"), dict):
            errors.append(f"scenario evidence must be an object: {name}")
    missing = sorted(set(REQUIRED_SCENARIOS) - set(names))
    if missing:
        errors.append("missing required scenarios: " + ", ".join(missing))
    if len(names) != len(set(names)):
        errors.append("scenario names must be unique")
    if artifact.get("counts") != counts:
        errors.append("counts do not match outcomes")
    expected_passed = counts["fail"] == counts["blocked"] == 0
    if artifact.get("passed") is not expected_passed:
        errors.append("passed does not match outcomes")
    if require_pass and not expected_passed:
        errors.append("qualification release gate did not pass")
    return tuple(errors)


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    _main()
