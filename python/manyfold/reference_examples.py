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
        if not isinstance(self.title, str) or not self.title.strip():
            raise ValueError("reference example title must be a non-empty string")
        if not isinstance(self.summary, str) or not self.summary.strip():
            raise ValueError("reference example summary must be a non-empty string")
        if not isinstance(self.implemented, bool):
            raise TypeError("reference example implemented flag must be a boolean")
        if self.module_name is not None and (
            not isinstance(self.module_name, str) or not self.module_name.strip()
        ):
            raise ValueError("reference example module name must be a non-empty string")
        if self.runner is not None and not callable(self.runner):
            raise TypeError("reference example runner must be callable")


def _example_runner(import_path: str) -> ExampleRunner:
    def run() -> Any:
        return import_module(import_path).run_example()

    return run


def _implemented_reference_example(metadata: ExampleCatalogEntry) -> ReferenceExample:
    assert metadata.reference_number is not None
    assert metadata.reference_title is not None
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


REFERENCE_EXAMPLE_SUITE: tuple[ReferenceExample, ...] = tuple(
    _reference_example(reference_example_metadata(number))
    for number in REFERENCE_EXAMPLE_NUMBERS
)

_IMPLEMENTED_REFERENCE_EXAMPLES: tuple[ReferenceExample, ...] = tuple(
    example for example in REFERENCE_EXAMPLE_SUITE if example.implemented
)


def reference_example_suite() -> tuple[ReferenceExample, ...]:
    """Return the ordered RFC reference example suite."""

    return REFERENCE_EXAMPLE_SUITE


def implemented_reference_examples() -> tuple[ReferenceExample, ...]:
    """Return the runnable subset of the RFC reference example suite."""

    return _IMPLEMENTED_REFERENCE_EXAMPLES
