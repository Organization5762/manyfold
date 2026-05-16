"""Reference example suite declared by RFC section 23."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Callable

try:
    from ._repo_paths import ensure_repo_import_paths
except ImportError:
    python_dir = Path(__file__).resolve().parent.parent
    if str(python_dir) not in sys.path:
        sys.path.insert(0, str(python_dir))
    from manyfold._repo_paths import ensure_repo_import_paths

ensure_repo_import_paths()

try:
    from examples import (
        REFERENCE_EXAMPLE_NUMBERS,
        ExampleCatalogEntry,
        ReferenceExampleGap,
        reference_example_metadata,
    )
except ModuleNotFoundError as exc:
    if exc.name != "examples":
        raise
    REFERENCE_EXAMPLE_NUMBERS: tuple[int, ...] = ()
    ExampleCatalogEntry = Any
    ReferenceExampleGap = Any
    reference_example_metadata = None

__all__ = (
    "REFERENCE_EXAMPLE_SUITE",
    "ReferenceExample",
    "implemented_reference_examples",
    "reference_example_suite",
)

ExampleRunner = Callable[[], Any]


def reference_example_suite() -> tuple[ReferenceExample, ...]:
    """Return the ordered RFC reference example suite."""

    return REFERENCE_EXAMPLE_SUITE


def implemented_reference_examples() -> tuple[ReferenceExample, ...]:
    """Return the runnable subset of the RFC reference example suite."""

    return _IMPLEMENTED_REFERENCE_EXAMPLES


def _is_blank(value: str) -> bool:
    return not value.strip()


def _require_non_blank_text(value: object, field: str) -> str:
    if not isinstance(value, str) or _is_blank(value):
        raise ValueError(f"reference example {field} must be a non-empty string")
    return value.strip()


@dataclass(frozen=True)
class ReferenceExample:
    """Declarative metadata for one RFC reference example."""

    number: int
    title: str
    summary: str
    implemented: bool
    module_name: str | None = None
    runner: ExampleRunner | None = None

    def __post_init__(self) -> None:
        if isinstance(self.number, bool) or not isinstance(self.number, int):
            raise TypeError("reference example number must be an integer")
        if self.number <= 0:
            raise ValueError("reference example number must be positive")
        object.__setattr__(
            self, "title", _require_non_blank_text(self.title, "title")
        )
        object.__setattr__(
            self, "summary", _require_non_blank_text(self.summary, "summary")
        )
        if not isinstance(self.implemented, bool):
            raise TypeError("reference example implemented flag must be a boolean")
        if self.module_name is not None:
            object.__setattr__(
                self,
                "module_name",
                _require_non_blank_text(self.module_name, "module name"),
            )
        if self.runner is not None and not callable(self.runner):
            raise TypeError("reference example runner must be callable")
        if self.implemented and (self.module_name is None or self.runner is None):
            raise ValueError(
                "implemented reference examples must include module_name and runner"
            )
        if not self.implemented and (
            self.module_name is not None or self.runner is not None
        ):
            raise ValueError(
                "missing reference examples cannot include module_name or runner"
            )


def _example_runner(import_path: str) -> ExampleRunner:
    def run() -> Any:
        return import_module(import_path).run_example()

    return run


def _implemented_reference_example(metadata: ExampleCatalogEntry) -> ReferenceExample:
    if metadata.reference_number is None or metadata.reference_title is None:
        raise ValueError(
            "implemented reference example metadata must include "
            "reference_number and reference_title"
        )
    return ReferenceExample(
        number=metadata.reference_number,
        title=metadata.reference_title,
        summary=metadata.summary,
        implemented=True,
        module_name=metadata.module_name,
        runner=_example_runner(metadata.import_path),
    )


def _missing_reference_example(metadata: ReferenceExampleGap) -> ReferenceExample:
    return ReferenceExample(
        number=metadata.reference_number,
        title=metadata.reference_title,
        summary=metadata.summary,
        implemented=False,
    )


def _reference_example(
    metadata: ExampleCatalogEntry | ReferenceExampleGap,
) -> ReferenceExample:
    if getattr(metadata, "module_name", None) is not None:
        return _implemented_reference_example(metadata)
    return _missing_reference_example(metadata)


if reference_example_metadata is None:
    REFERENCE_EXAMPLE_SUITE: tuple[ReferenceExample, ...] = ()
else:
    REFERENCE_EXAMPLE_SUITE = tuple(
        _reference_example(reference_example_metadata(number))
        for number in REFERENCE_EXAMPLE_NUMBERS
    )

_IMPLEMENTED_REFERENCE_EXAMPLES: tuple[ReferenceExample, ...] = tuple(
    example for example in REFERENCE_EXAMPLE_SUITE if example.implemented
)
