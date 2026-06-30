"""Verify each Rust benchmark source has a checked-in baseline artifact."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def verify_benchmark_baselines(root: Path) -> tuple[Path, ...]:
    """Return benchmark source paths after verifying their sibling baselines."""

    benchmark_root = root / "src" / "benchmarks"
    baseline_root = benchmark_root / "baselines"
    sources = tuple(sorted(benchmark_root.glob("*.rs")))
    errors: list[str] = []
    if not sources:
        errors.append(f"no Rust benchmarks found under {benchmark_root}")
    for source in sources:
        baseline = baseline_root / f"{source.stem}.json"
        if not baseline.exists():
            errors.append(f"missing benchmark baseline: {baseline}")
            continue
        errors.extend(_baseline_errors(baseline))
    if errors:
        raise ValueError("; ".join(errors))
    return sources


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="repository root to scan",
    )
    args = parser.parse_args(argv)
    try:
        sources = verify_benchmark_baselines(args.root)
    except ValueError as error:
        sys.stderr.write(str(error) + "\n")
        return 1
    print(f"benchmark_baselines passed=true benchmarks={len(sources)}")
    return 0


def _baseline_errors(path: Path) -> tuple[str, ...]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        return (f"benchmark baseline cannot be read: {path}: {error}",)
    except json.JSONDecodeError as error:
        return (f"benchmark baseline is not valid JSON: {path}: {error}",)
    if isinstance(payload, dict):
        return () if payload else (f"benchmark baseline object is empty: {path}",)
    if isinstance(payload, list):
        return () if payload else (f"benchmark baseline list is empty: {path}",)
    return (f"benchmark baseline must be a JSON object or list: {path}",)


if __name__ == "__main__":
    raise SystemExit(_main())
