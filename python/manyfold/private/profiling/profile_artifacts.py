"""Verify saved Manyfold profile benchmark JSON artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Literal

ProfileArtifactKind = Literal["application", "runtime"]


def verify_profile_artifact(
    path: Path,
    *,
    kind: ProfileArtifactKind | None = None,
    name: str | None = None,
    mode: str | None = None,
    require_gate: str | None = None,
) -> dict[str, object]:
    """Load and verify one saved profile artifact."""

    artifact = _load_json_object(path)
    errors = _profile_artifact_errors(
        artifact,
        kind=kind,
        name=name,
        mode=mode,
        require_gate=require_gate,
    )
    if errors:
        raise ValueError("; ".join(errors))
    return artifact


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--kind", choices=("application", "runtime"))
    parser.add_argument("--name", help="required example or runtime workload name")
    parser.add_argument("--mode", choices=("standard", "parallel"))
    parser.add_argument(
        "--require-gate",
        help="require a passing gate with this name, such as max_average_event_us",
    )
    args = parser.parse_args(argv)
    try:
        artifact = verify_profile_artifact(
            args.artifact,
            kind=args.kind,
            name=args.name,
            mode=args.mode,
            require_gate=args.require_gate,
        )
    except ValueError as error:
        sys.stderr.write(str(error) + "\n")
        return 1
    label = artifact.get("example") or artifact.get("workload")
    gates = artifact.get("gates")
    gate_count = len(gates) if isinstance(gates, list) else 0
    print(
        "profile_artifact passed=true "
        f"name={label} mode={artifact.get('mode')} gates={gate_count}"
    )
    return 0


def _load_json_object(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ValueError(f"profile artifact cannot be read: {path}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"profile artifact is not valid JSON: {path}") from error
    if not isinstance(payload, dict):
        raise ValueError("profile artifact must be a JSON object")
    return payload


def _profile_artifact_errors(
    artifact: dict[str, object],
    *,
    kind: ProfileArtifactKind | None,
    name: str | None,
    mode: str | None,
    require_gate: str | None,
) -> tuple[str, ...]:
    errors: list[str] = []
    observed_kind = _artifact_kind(artifact)
    if observed_kind is None:
        errors.append("artifact must include exactly one of example or workload")
    elif kind is not None and observed_kind != kind:
        errors.append(f"artifact kind mismatch: expected {kind}, observed {observed_kind}")
    observed_name = artifact.get("example") or artifact.get("workload")
    if not isinstance(observed_name, str) or not observed_name:
        errors.append("artifact name must be a non-empty string")
    elif name is not None and observed_name != name:
        errors.append(f"artifact name mismatch: expected {name}, observed {observed_name}")
    observed_mode = artifact.get("mode")
    if observed_mode not in ("standard", "parallel"):
        errors.append("artifact mode must be standard or parallel")
    elif mode is not None and observed_mode != mode:
        errors.append(f"artifact mode mismatch: expected {mode}, observed {observed_mode}")
    if artifact.get("passed") is not True:
        errors.append("artifact did not pass")
    errors.extend(_required_numeric_field_errors(artifact))
    gates = artifact.get("gates")
    if not isinstance(gates, list):
        errors.append("artifact gates must be a list")
    else:
        errors.extend(_gate_errors(gates, require_gate=require_gate))
    return tuple(errors)


def _artifact_kind(artifact: dict[str, object]) -> ProfileArtifactKind | None:
    has_example = "example" in artifact
    has_workload = "workload" in artifact
    if has_example == has_workload:
        return None
    return "application" if has_example else "runtime"


def _required_numeric_field_errors(artifact: dict[str, object]) -> tuple[str, ...]:
    errors = []
    for field in (
        "average_event_us",
        "elapsed_seconds",
        "max_seconds",
        "mean_seconds",
        "min_seconds",
        "runs",
        "stdev_seconds",
    ):
        value = artifact.get(field)
        if not isinstance(value, (float, int)) or isinstance(value, bool):
            errors.append(f"artifact field {field} must be numeric")
    return tuple(errors)


def _gate_errors(
    gates: list[object],
    *,
    require_gate: str | None,
) -> tuple[str, ...]:
    errors: list[str] = []
    seen_required_gate = require_gate is None
    for index, gate in enumerate(gates):
        if not isinstance(gate, dict):
            errors.append(f"gate {index} must be an object")
            continue
        gate_name = gate.get("name")
        if not isinstance(gate_name, str) or not gate_name:
            errors.append(f"gate {index} name must be a non-empty string")
        if gate_name == require_gate:
            seen_required_gate = True
        if gate.get("passed") is not True:
            errors.append(f"gate failed: {gate_name}")
        for field in ("limit", "observed"):
            value = gate.get(field)
            if not isinstance(value, (float, int)) or isinstance(value, bool):
                errors.append(f"gate {gate_name} {field} must be numeric")
    if not seen_required_gate:
        errors.append(f"required gate missing: {require_gate}")
    return tuple(errors)


if __name__ == "__main__":
    raise SystemExit(_main())
