"""Verification helpers for monitor JSON artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

_FIELD_ALIASES = {
    "anonymous": "anonymous_kib",
    "anonymous_kib": "anonymous_kib",
    "fd": "fd_count",
    "fd_count": "fd_count",
    "private": "private_kib",
    "private_kib": "private_kib",
    "pss": "pss_kib",
    "pss_kib": "pss_kib",
    "rss": "current_rss_kib",
    "rss_kib": "current_rss_kib",
    "current_rss_kib": "current_rss_kib",
}
_METRIC_ALIASES = {
    **_FIELD_ALIASES,
    "current_rss_kib": "rss_kib",
    "rss": "rss_kib",
    "rss_kib": "rss_kib",
}


def verify_monitor_artifact(
    path: Path,
    *,
    min_samples: int | None = None,
    require_checks: bool = True,
    required_command_fragments: Sequence[str] = (),
    required_gates: Sequence[str] = (),
    required_metrics: Sequence[str] = (),
    required_sample_fields: Sequence[str] = (),
    required_metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Return a parsed monitor artifact after enforcing regression gates."""
    artifact = json.loads(path.read_text(encoding="utf-8"))
    errors = _monitor_artifact_errors(
        artifact,
        min_samples=min_samples,
        require_checks=require_checks,
        required_command_fragments=required_command_fragments,
        required_gates=required_gates,
        required_metrics=required_metrics,
        required_sample_fields=required_sample_fields,
        required_metadata=required_metadata or {},
    )
    if errors:
        raise SystemExit("; ".join(errors))
    return artifact


def _main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    artifact = verify_monitor_artifact(
        args.artifact,
        min_samples=args.min_samples,
        require_checks=not args.allow_unchecked,
        required_command_fragments=tuple(args.require_command_fragment or ()),
        required_gates=tuple(args.require_gate or ()),
        required_metrics=tuple(args.require_metric or ()),
        required_sample_fields=tuple(args.require_sample_field or ()),
        required_metadata=_parse_required_metadata(args.require_metadata or ()),
    )
    print(
        "monitor_artifact passed=true samples={samples} gates={gates}".format(
            samples=_artifact_sample_count(artifact),
            gates=len(artifact.get("gates", ())),
        )
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a Manyfold external monitor JSON artifact."
    )
    parser.add_argument("artifact", type=Path)
    parser.add_argument(
        "--allow-unchecked",
        action="store_true",
        help="Allow artifacts produced with checks disabled.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        help="Require at least this many sampled rows.",
    )
    parser.add_argument(
        "--require-command-fragment",
        action="append",
        help="Require this string to appear in the recorded command.",
    )
    parser.add_argument(
        "--require-gate",
        action="append",
        help="Require a named gate to be present in the artifact.",
    )
    parser.add_argument(
        "--require-metric",
        action="append",
        help=(
            "Require a metric summary with at least one available value. "
            "Aliases: rss, pss, private, anonymous, fd."
        ),
    )
    parser.add_argument(
        "--require-sample-field",
        action="append",
        help=(
            "Require retained sample rows to contain at least one non-null field. "
            "Aliases: rss, pss, private, anonymous, fd."
        ),
    )
    parser.add_argument(
        "--require-metadata",
        action="append",
        metavar="KEY=VALUE",
        help="Require an exact metadata key/value pair in the artifact.",
    )
    return parser.parse_args(argv)


def _monitor_artifact_errors(
    artifact: dict[str, Any],
    *,
    min_samples: int | None,
    require_checks: bool,
    required_command_fragments: Sequence[str],
    required_gates: Sequence[str],
    required_metrics: Sequence[str],
    required_sample_fields: Sequence[str],
    required_metadata: dict[str, str],
) -> tuple[str, ...]:
    errors: list[str] = []
    if artifact.get("passed") is not True:
        errors.append("artifact did not pass")
    if require_checks and artifact.get("checks_enabled") is not True:
        errors.append("artifact was produced with checks disabled")
    failed_gates = tuple(str(gate) for gate in artifact.get("failed_gates", ()))
    if failed_gates:
        errors.append("failed gates: " + ", ".join(failed_gates))
    unavailable_gates = tuple(
        str(gate) for gate in artifact.get("unavailable_gates", ())
    )
    if unavailable_gates:
        errors.append("unavailable gates: " + ", ".join(unavailable_gates))
    gates = tuple(_gate_items(artifact))
    direct_failed_gates = tuple(
        name for name, gate in gates if gate.get("passed") is not True
    )
    if direct_failed_gates:
        errors.append("gate records failed: " + ", ".join(direct_failed_gates))
    direct_unavailable_gates = tuple(
        name for name, gate in gates if gate.get("available") is not True
    )
    if direct_unavailable_gates:
        errors.append(
            "gate records unavailable: " + ", ".join(direct_unavailable_gates)
        )
    sample_count = _artifact_sample_count(artifact)
    if min_samples is not None and sample_count < min_samples:
        errors.append(f"sample count {sample_count} below required {min_samples}")
    command_text = " ".join(str(part) for part in artifact.get("command", ()))
    for fragment in required_command_fragments:
        if fragment not in command_text:
            errors.append(f"command missing required fragment {fragment!r}")
    present_gates = {name for name, _gate in gates}
    for gate in required_gates:
        if gate not in present_gates:
            errors.append(f"required gate missing: {gate}")
    metrics = artifact.get("metrics", {})
    if not isinstance(metrics, dict):
        errors.append("artifact metrics is not an object")
        metrics = {}
    for metric in required_metrics:
        name = _required_metric_name(metric)
        summary = metrics.get(name)
        if not isinstance(summary, dict):
            errors.append(f"required metric missing: {name}")
            continue
        if not _metric_summary_is_available(summary):
            errors.append(f"required metric unavailable: {name}")
    samples = artifact.get("samples", ())
    if not isinstance(samples, list):
        errors.append("artifact samples is not a list")
        samples = []
    for field in required_sample_fields:
        name = _required_sample_field(field)
        if not any(
            isinstance(sample, dict) and sample.get(name) is not None
            for sample in samples
        ):
            errors.append(f"required sample field unavailable: {name}")
    metadata = artifact.get("metadata", {})
    if not isinstance(metadata, dict):
        errors.append("artifact metadata is not an object")
        metadata = {}
    for key, value in required_metadata.items():
        actual = metadata.get(key)
        if str(actual) != value:
            errors.append(
                f"metadata {key!r} expected {value!r}, found {actual!r}"
            )
    return tuple(errors)


def _metric_summary_is_available(summary: dict[str, Any]) -> bool:
    sample_count = summary.get("sample_count")
    if not isinstance(sample_count, int) or sample_count <= 0:
        return False
    return any(
        summary.get(field) is not None
        for field in (
            "steady_min",
            "steady_max",
            "steady_range",
            "projected_growth",
            "segment_projected_growth",
        )
    )


def _required_metric_name(name: str) -> str:
    try:
        return _METRIC_ALIASES[name]
    except KeyError as error:
        raise SystemExit(f"unknown monitor metric field: {name}") from error


def _required_sample_field(name: str) -> str:
    try:
        return _FIELD_ALIASES[name]
    except KeyError as error:
        raise SystemExit(f"unknown monitor sample field: {name}") from error


def _parse_required_metadata(items: Sequence[str]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for item in items:
        key, separator, value = item.partition("=")
        if not separator or not key:
            raise SystemExit("--require-metadata values must use KEY=VALUE")
        metadata[key] = value
    return metadata


def _artifact_sample_count(artifact: dict[str, Any]) -> int:
    sample_count = artifact.get("sample_count")
    if isinstance(sample_count, int):
        return sample_count
    return len(artifact.get("samples", ()))


def _gate_items(artifact: dict[str, Any]) -> tuple[tuple[str, dict[str, Any]], ...]:
    gates: list[tuple[str, dict[str, Any]]] = []
    for gate in artifact.get("gates", ()):
        if isinstance(gate, dict):
            gates.append((str(gate.get("name")), gate))
    return tuple(gates)


if __name__ == "__main__":
    _main()
