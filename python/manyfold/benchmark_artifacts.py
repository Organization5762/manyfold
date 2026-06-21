"""Verification helpers for key/value benchmark log artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

_Sample = dict[str, int | float | str]
_DEFAULT_MONOTONIC_FIELDS = ("step", "elapsed_seconds", "cpu_seconds")


def verify_benchmark_log(
    path: Path,
    *,
    min_final_step: int | None = None,
    min_samples: int | None = None,
    required_fields: Sequence[str] = (),
    required_monotonic_fields: Sequence[str] = (),
    required_final_values: dict[str, int] | None = None,
    required_final_max_values: dict[str, float] | None = None,
    required_max_values: dict[str, int] | None = None,
    required_numeric_max_values: dict[str, float] | None = None,
    tail_plateaus: Sequence[tuple[str, int, int]] = (),
) -> tuple[_Sample, ...]:
    """Return parsed benchmark samples after enforcing artifact-level checks."""
    samples = tuple(
        sample
        for line in path.read_text(encoding="utf-8").splitlines()
        if (sample := _parse_sample_line(line)) is not None
    )
    errors = _benchmark_log_errors(
        samples,
        min_final_step=min_final_step,
        min_samples=min_samples,
        required_fields=required_fields,
        required_monotonic_fields=required_monotonic_fields,
        required_final_values=required_final_values or {},
        required_final_max_values=required_final_max_values or {},
        required_max_values=required_max_values or {},
        required_numeric_max_values=required_numeric_max_values or {},
        tail_plateaus=tail_plateaus,
    )
    if errors:
        raise SystemExit("; ".join(errors))
    return samples


def _main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    samples = verify_benchmark_log(
        args.artifact,
        min_final_step=args.min_final_step,
        min_samples=args.min_samples,
        required_fields=tuple(args.require_field or ()),
        required_monotonic_fields=tuple(args.require_monotonic or ()),
        required_final_values=_parse_int_assignments(args.require_final or ()),
        required_final_max_values=_parse_float_assignments(
            args.require_final_max or ()
        ),
        required_max_values=_parse_int_assignments(args.require_max or ()),
        required_numeric_max_values=_parse_float_assignments(
            args.require_numeric_max or ()
        ),
        tail_plateaus=_parse_tail_plateaus(args.require_tail_plateau or ()),
    )
    final = samples[-1]
    print(
        "benchmark_log passed=true samples={samples} final_step={step}".format(
            samples=len(samples),
            step=final.get("step", "unknown"),
        )
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a Manyfold key/value benchmark log artifact."
    )
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--min-final-step", type=int)
    parser.add_argument("--min-samples", type=int)
    parser.add_argument(
        "--require-field",
        action="append",
        help="Require every parsed sample to include this field.",
    )
    parser.add_argument(
        "--require-monotonic",
        action="append",
        metavar="FIELD",
        help=(
            "Require this numeric field to be present in every sample and never "
            "move backwards. step, elapsed_seconds, and cpu_seconds are checked "
            "opportunistically by default when present."
        ),
    )
    parser.add_argument(
        "--require-final",
        action="append",
        metavar="FIELD=INT",
        help="Require the final sample to contain this exact integer value.",
    )
    parser.add_argument(
        "--require-final-max",
        action="append",
        metavar="FIELD=NUMBER",
        help="Require the final sample numeric field to be at or below NUMBER.",
    )
    parser.add_argument(
        "--require-max",
        action="append",
        metavar="FIELD=INT",
        help="Require all samples to keep this integer field at or below the value.",
    )
    parser.add_argument(
        "--require-numeric-max",
        action="append",
        metavar="FIELD=NUMBER",
        help="Require all samples to keep this numeric field at or below NUMBER.",
    )
    parser.add_argument(
        "--require-tail-plateau",
        action="append",
        metavar="FIELD=RANGE:COUNT",
        help=(
            "Require the final tail for FIELD to stay within RANGE for at least "
            "COUNT samples."
        ),
    )
    return parser.parse_args(argv)


def _benchmark_log_errors(
    samples: tuple[_Sample, ...],
    *,
    min_final_step: int | None,
    min_samples: int | None,
    required_fields: Sequence[str],
    required_monotonic_fields: Sequence[str],
    required_final_values: dict[str, int],
    required_final_max_values: dict[str, float],
    required_max_values: dict[str, int],
    required_numeric_max_values: dict[str, float],
    tail_plateaus: Sequence[tuple[str, int, int]],
) -> tuple[str, ...]:
    errors: list[str] = []
    if not samples:
        return ("no benchmark samples found",)
    if min_samples is not None and len(samples) < min_samples:
        errors.append(f"sample count {len(samples)} below required {min_samples}")
    final = samples[-1]
    final_step = _int_field(final, "step")
    if (
        min_final_step is not None
        and final_step is not None
        and final_step < min_final_step
    ):
        errors.append(f"final step {final_step} below required {min_final_step}")
    if min_final_step is not None and final_step is None:
        errors.append("final sample missing integer step")
    for field in required_fields:
        if any(field not in sample for sample in samples):
            errors.append(f"required field missing from one or more samples: {field}")
    for field in _monotonic_fields(required_monotonic_fields):
        errors.extend(
            _monotonic_field_errors(
                samples,
                field,
                required=field in required_monotonic_fields,
            )
        )
    for field, expected in required_final_values.items():
        observed = _int_field(final, field)
        if observed is None:
            errors.append(f"final sample missing integer field: {field}")
        elif observed != expected:
            errors.append(f"final {field} {observed} != required {expected}")
    for field, limit in required_final_max_values.items():
        observed = _numeric_field(final, field)
        if observed is None:
            errors.append(f"final sample missing numeric field: {field}")
        elif observed > limit:
            errors.append(f"final {field} {observed:g} exceeds {limit:g}")
    for field, limit in required_max_values.items():
        values = tuple(
            value for sample in samples if (value := _int_field(sample, field)) is not None
        )
        if len(values) != len(samples):
            errors.append(f"integer field missing from one or more samples: {field}")
            continue
        observed = max(values)
        if observed > limit:
            errors.append(f"max {field} {observed} exceeds {limit}")
    for field, limit in required_numeric_max_values.items():
        values = tuple(
            value
            for sample in samples
            if (value := _numeric_field(sample, field)) is not None
        )
        if len(values) != len(samples):
            errors.append(f"numeric field missing from one or more samples: {field}")
            continue
        observed = max(values)
        if observed > limit:
            errors.append(f"max {field} {observed:g} exceeds {limit:g}")
    for field, plateau, min_tail_samples in tail_plateaus:
        tail = _final_tail_plateau(samples, field=field, plateau=plateau)
        if tail["range"] is None:
            errors.append(f"no integer samples collected for tail field: {field}")
            continue
        if tail["sample_count"] < min_tail_samples:
            errors.append(
                f"{field} tail plateau collected {tail['sample_count']} samples, "
                f"below required {min_tail_samples}"
            )
    return tuple(errors)


def _parse_sample_line(line: str) -> _Sample | None:
    if not line.startswith("step="):
        return None
    sample: _Sample = {}
    for item in line.split():
        key, separator, value = item.partition("=")
        if not separator or not key:
            continue
        sample[key] = _parse_value(value)
    return sample


def _monotonic_fields(required_fields: Sequence[str]) -> tuple[str, ...]:
    fields: list[str] = []
    for field in (*_DEFAULT_MONOTONIC_FIELDS, *required_fields):
        if field not in fields:
            fields.append(field)
    return tuple(fields)


def _monotonic_field_errors(
    samples: tuple[_Sample, ...],
    field: str,
    *,
    required: bool,
) -> tuple[str, ...]:
    values = tuple(_numeric_field(sample, field) for sample in samples)
    if any(value is None for value in values):
        if required:
            return (f"monotonic field missing from one or more samples: {field}",)
        return ()
    concrete_values = tuple(value for value in values if value is not None)
    for index, (previous, current) in enumerate(
        zip(concrete_values, concrete_values[1:]),
        start=1,
    ):
        if current < previous:
            return (
                f"{field} moved backwards at sample {index}: "
                f"{current:g} < {previous:g}",
            )
    return ()


def _parse_value(value: str) -> int | float | str:
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _parse_int_assignments(items: Sequence[str]) -> dict[str, int]:
    assignments: dict[str, int] = {}
    for item in items:
        field, separator, value = item.partition("=")
        if not separator or not field:
            raise SystemExit("integer assignments must use FIELD=INT")
        try:
            assignments[field] = int(value)
        except ValueError as error:
            raise SystemExit(f"invalid integer assignment: {item}") from error
    return assignments


def _parse_float_assignments(items: Sequence[str]) -> dict[str, float]:
    assignments: dict[str, float] = {}
    for item in items:
        field, separator, value = item.partition("=")
        if not separator or not field:
            raise SystemExit("numeric assignments must use FIELD=NUMBER")
        try:
            assignments[field] = float(value)
        except ValueError as error:
            raise SystemExit(f"invalid numeric assignment: {item}") from error
    return assignments


def _parse_tail_plateaus(items: Sequence[str]) -> tuple[tuple[str, int, int], ...]:
    plateaus: list[tuple[str, int, int]] = []
    for item in items:
        field, separator, spec = item.partition("=")
        plateau_text, count_separator, count_text = spec.partition(":")
        if not separator or not count_separator or not field:
            raise SystemExit("tail plateau values must use FIELD=RANGE:COUNT")
        try:
            plateaus.append((field, int(plateau_text), int(count_text)))
        except ValueError as error:
            raise SystemExit(f"invalid tail plateau value: {item}") from error
    return tuple(plateaus)


def _int_field(sample: _Sample, field: str) -> int | None:
    value = sample.get(field)
    if isinstance(value, int):
        return value
    return None


def _numeric_field(sample: _Sample, field: str) -> float | None:
    value = sample.get(field)
    if isinstance(value, int | float):
        return float(value)
    return None


def _final_tail_plateau(
    samples: tuple[_Sample, ...],
    *,
    field: str,
    plateau: int,
) -> dict[str, int | None]:
    observed = tuple(value for sample in samples if (value := _int_field(sample, field)) is not None)
    if not observed:
        return {"range": None, "sample_count": 0}
    first_index = len(observed) - 1
    minimum = observed[first_index]
    maximum = minimum
    while first_index > 0:
        previous_value = observed[first_index - 1]
        next_minimum = min(minimum, previous_value)
        next_maximum = max(maximum, previous_value)
        if next_maximum - next_minimum > plateau:
            break
        first_index -= 1
        minimum = next_minimum
        maximum = next_maximum
    return {"range": maximum - minimum, "sample_count": len(observed) - first_index}


if __name__ == "__main__":
    _main()
