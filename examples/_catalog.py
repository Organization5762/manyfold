"""Shared metadata for executable and RFC reference Manyfold examples."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from ._exports import CATALOG_EXPORTS

T = TypeVar("T")

__all__ = [
    *CATALOG_EXPORTS,
]

_README_INTRO_LINES = (
    "The `examples/` directory is organized as a short path through the mental",
    "model. Start with a route, derive values, add explicit demand, then move",
    "into joins, watermarks, planning, consensus, and taint-aware runtime behavior. The supported",
    "examples are validated by the regular `unittest` run so they do not drift",
    "away from the API.",
)

_README_OUTRO_LINES = (
    "More involved operator, query, transport, mesh, and security coverage stays",
    "in [tests/test_graph_reactive.py](tests/test_graph_reactive.py), with archived exploratory scripts kept",
    "under [examples/archived/](examples/archived/). The example manifest, README featured-example",
    "list, and RFC reference suite all derive from the shared example catalog,",
    "so supported versus archived status lives in one place.",
)

_README_EXAMPLE_GROUPS = {
    "simple_latest": "Start here: publish changing state and read the latest value",
    "average_temperature": "Layer computation: publish derived values",
    "rate_matched_sensor": "Control the flow: make downstream demand visible",
    "imu_fusion_join": "Fuse streams: coordinate independent sensors",
    "rolling_window_aggregate": "Reason in time: release data by watermark progress",
    "cross_partition_join": "Scale the graph: plan repartition work explicitly",
    "raft_demo": "Capstone: wire a Raft-shaped consensus component",
    "ephemeral_entropy_stream": "Audit the hard parts: mark nondeterminism on purpose",
}


@dataclass(frozen=True)
class ExampleCatalogEntry:
    """Describe one example module and how it should surface in the repo."""

    module_name: str
    summary: str
    archived: bool = False
    readme_order: int | None = None
    reference_number: int | None = None
    reference_title: str | None = None

    @property
    def is_reference_example(self) -> bool:
        return self.reference_number is not None and self.reference_title is not None

    @property
    def import_path(self) -> str:
        """Fully qualified module import path for this example."""

        return f"examples.{self.module_name}"

    @property
    def file_path(self) -> Path:
        """Filesystem path for this example module inside the examples tree."""

        return _module_path(self.module_name)


@dataclass(frozen=True)
class ReferenceExampleGap:
    """Describe one RFC reference example that is still intentionally missing."""

    reference_number: int
    reference_title: str
    summary: str


EXAMPLE_CATALOG: tuple[ExampleCatalogEntry, ...] = (
    ExampleCatalogEntry(
        "simple_latest",
        "Smallest changing-signal publish/read-back example.",
        readme_order=1,
    ),
    ExampleCatalogEntry(
        "average_temperature",
        "Compute and publish a rolling average from temperature samples.",
        readme_order=2,
    ),
    ExampleCatalogEntry("observe_publish", "Observe a route while publishing updates."),
    ExampleCatalogEntry("pipe_route", "Pipe an Rx source directly into a route."),
    ExampleCatalogEntry(
        "read_then_write_next_epoch_step",
        "Install a read/transform/write step with a shared write stream.",
    ),
    ExampleCatalogEntry(
        "write_binding", "Bind a request stream to a desired-write route."
    ),
    ExampleCatalogEntry(
        "uart_temperature_sensor",
        "A raw UART sensor emits metadata and a smoothed logical temperature stream.",
        reference_number=1,
        reference_title="UART temperature sensor",
    ),
    ExampleCatalogEntry(
        "imu_fusion_join",
        "Capacitors stage accelerometer and gyro streams before an event-time join.",
        readme_order=4,
        reference_number=2,
        reference_title="IMU fusion join",
    ),
    ExampleCatalogEntry(
        "lazy_lidar_payload",
        "A resistor gates metadata before a selected LiDAR payload is opened.",
        reference_number=3,
        reference_title="Lazy LiDAR payload",
    ),
    ExampleCatalogEntry(
        "closed_counter_loop",
        "Desired/reported/effective shadow semantics for a counter write-back loop.",
        reference_number=4,
        reference_title="Closed counter loop",
    ),
    ExampleCatalogEntry(
        "brightness_control",
        "Logical brightness requests translate into raw PWM writes.",
        reference_number=5,
        reference_title="Brightness control",
    ),
    ExampleCatalogEntry(
        "mailbox_bridge",
        "An explicit mailbox boundary declares overflow behavior between async domains.",
        reference_number=6,
        reference_title="Mailbox bridge",
    ),
    ExampleCatalogEntry(
        "cross_partition_join",
        "A repartition join with skew metrics and planner output.",
        readme_order=6,
        reference_number=7,
        reference_title="Cross-partition join",
    ),
    ExampleCatalogEntry(
        "broadcast_mirror",
        "Deterministic capacitor fan-out mirrors state to multiple observers.",
        reference_number=8,
        reference_title="Broadcast mirror",
    ),
    ExampleCatalogEntry(
        "raft_demo",
        "The Consensus component wires Raft election defaults from graph primitives.",
        readme_order=7,
        reference_number=9,
        reference_title="Raft demo",
    ),
    ExampleCatalogEntry(
        "ephemeral_entropy_stream",
        "Per-request entropy derivation that taints determinism explicitly.",
        readme_order=8,
        reference_number=10,
        reference_title="Ephemeral entropy stream",
    ),
    ExampleCatalogEntry(
        "rate_matched_sensor",
        "A one-slot capacitor coalesces bursty reads behind explicit demand.",
        readme_order=3,
    ),
    ExampleCatalogEntry(
        "rolling_window_aggregate",
        "A capacitor discharges samples behind explicit event-time watermarks.",
        readme_order=5,
    ),
    ExampleCatalogEntry(
        "archived.windowed_join",
        "Superseded exploratory script that combined rolling windows with latest-join state.",
        archived=True,
    ),
)


def _catalog_entries(
    *,
    archived: bool | None = None,
    reference_only: bool = False,
) -> tuple[ExampleCatalogEntry, ...]:
    entries = EXAMPLE_CATALOG
    if archived is not None:
        entries = tuple(entry for entry in entries if entry.archived is archived)
    if reference_only:
        entries = tuple(entry for entry in entries if entry.is_reference_example)
    return entries


def _entry_modules(entries: tuple[ExampleCatalogEntry, ...]) -> tuple[str, ...]:
    return tuple(entry.module_name for entry in entries)


def _reference_entries(
    entries: tuple[ExampleCatalogEntry, ...],
) -> tuple[ExampleCatalogEntry, ...]:
    return tuple(entry for entry in entries if entry.is_reference_example)


def _sorted_readme_entries(
    entries: tuple[ExampleCatalogEntry, ...],
) -> tuple[ExampleCatalogEntry, ...]:
    return tuple(
        sorted(
            (entry for entry in entries if entry.readme_order is not None),
            key=lambda entry: entry.readme_order if entry.readme_order is not None else -1,
        )
    )


SUPPORTED_EXAMPLE_ENTRIES: tuple[ExampleCatalogEntry, ...] = _catalog_entries(
    archived=False
)

ARCHIVED_EXAMPLE_ENTRIES: tuple[ExampleCatalogEntry, ...] = _catalog_entries(
    archived=True
)

SUPPORTED_EXAMPLE_MODULES: tuple[str, ...] = _entry_modules(SUPPORTED_EXAMPLE_ENTRIES)

ARCHIVED_EXAMPLE_MODULES: tuple[str, ...] = _entry_modules(ARCHIVED_EXAMPLE_ENTRIES)

REFERENCE_EXAMPLE_ENTRIES: tuple[ExampleCatalogEntry, ...] = _reference_entries(
    _catalog_entries(
        archived=False,
        reference_only=True,
    )
)

README_EXAMPLE_ENTRIES: tuple[ExampleCatalogEntry, ...] = _sorted_readme_entries(
    SUPPORTED_EXAMPLE_ENTRIES
)

README_EXAMPLE_MODULES: tuple[str, ...] = _entry_modules(README_EXAMPLE_ENTRIES)

README_FEATURED_EXAMPLES_START = "<!-- manyfold:featured-examples:start -->"
README_FEATURED_EXAMPLES_END = "<!-- manyfold:featured-examples:end -->"

REFERENCE_EXAMPLE_GAPS: tuple[ReferenceExampleGap, ...] = ()

REFERENCE_EXAMPLE_NUMBERS: tuple[int, ...] = tuple(
    sorted(
        number
        for number in (
            *(entry.reference_number for entry in REFERENCE_EXAMPLE_ENTRIES),
            *(gap.reference_number for gap in REFERENCE_EXAMPLE_GAPS),
        )
        if number is not None
    )
)

REFERENCE_EXAMPLE_IMPLEMENTED_COUNT = len(REFERENCE_EXAMPLE_ENTRIES)

if REFERENCE_EXAMPLE_GAPS:
    REFERENCE_EXAMPLE_PROGRESS_DETAIL = (
        "The repository now ships a named RFC reference example suite, with "
        f"runnable coverage for {REFERENCE_EXAMPLE_IMPLEMENTED_COUNT} of "
        f"{len(REFERENCE_EXAMPLE_NUMBERS)} examples; the remaining gap is "
        f"{REFERENCE_EXAMPLE_GAPS[0].reference_title}."
    )
else:
    REFERENCE_EXAMPLE_PROGRESS_DETAIL = (
        "The repository now ships a named RFC reference example suite, with "
        f"runnable coverage for all {len(REFERENCE_EXAMPLE_NUMBERS)} examples."
    )

EXAMPLE_CATALOG_BY_MODULE: dict[str, ExampleCatalogEntry] = {
    entry.module_name: entry for entry in EXAMPLE_CATALOG
}

REFERENCE_EXAMPLE_ENTRY_BY_NUMBER: dict[int, ExampleCatalogEntry] = {
    entry.reference_number: entry
    for entry in REFERENCE_EXAMPLE_ENTRIES
    if entry.reference_number is not None
}

REFERENCE_EXAMPLE_GAP_BY_NUMBER: dict[int, ReferenceExampleGap] = {
    gap.reference_number: gap for gap in REFERENCE_EXAMPLE_GAPS
}


def catalog_entry(module_name: str) -> ExampleCatalogEntry:
    """Return one catalog entry by module name."""

    try:
        return EXAMPLE_CATALOG_BY_MODULE[module_name]
    except KeyError as error:
        raise KeyError(f"unknown example module {module_name!r}") from error


def reference_example_metadata(
    number: int,
) -> ExampleCatalogEntry | ReferenceExampleGap:
    """Return implemented or gap metadata for one RFC reference example number."""

    if entry := REFERENCE_EXAMPLE_ENTRY_BY_NUMBER.get(number):
        return entry
    if gap := REFERENCE_EXAMPLE_GAP_BY_NUMBER.get(number):
        return gap
    raise KeyError(f"unknown RFC reference example {number}")


def render_readme_featured_examples() -> str:
    """Render the README featured-example block from catalog metadata."""

    lines = [*_README_INTRO_LINES]
    for entry in README_EXAMPLE_ENTRIES:
        if group := _README_EXAMPLE_GROUPS.get(entry.module_name):
            lines.append("")
            lines.append(f"**{group}**")
        example_path = f"examples/{entry.module_name.replace('.', '/')}.py"
        lines.append(
            f"- [{example_path}]({example_path}): {entry.summary}"
        )
    lines.append("")
    lines.extend(_README_OUTRO_LINES)
    return "\n".join(lines)


def sync_readme_featured_examples(readme_text: str) -> str:
    """Replace the generated featured-example block inside README content."""

    generated = render_readme_featured_examples()
    start_marker = README_FEATURED_EXAMPLES_START
    end_marker = README_FEATURED_EXAMPLES_END
    start = readme_text.find(start_marker)
    end = readme_text.find(end_marker)
    if start != readme_text.rfind(start_marker) or end != readme_text.rfind(end_marker):
        raise ValueError("README featured-example markers must appear exactly once")
    if start == -1 or end == -1 or end < start:
        raise ValueError("README featured-example markers are missing or out of order")
    start += len(start_marker)
    replacement = f"\n{generated}\n"
    return f"{readme_text[:start]}{replacement}{readme_text[end:]}"


_EXAMPLES_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _EXAMPLES_DIR.parent
_README_PATH = _REPO_ROOT / "README.md"
_IGNORED_MODULE_NAMES = {"__init__", "_catalog", "_exports", "_shared", "catalog"}


def _module_path(module_name: str) -> Path:
    parts = module_name.split(".")
    return _EXAMPLES_DIR.joinpath(*parts).with_suffix(".py")


def _module_name_from_path(path: Path) -> str:
    return path.relative_to(_EXAMPLES_DIR).with_suffix("").as_posix().replace("/", ".")


def _discover_manifestable_modules() -> tuple[str, ...]:
    discovered: list[str] = []
    for path in _EXAMPLES_DIR.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        if path.stem in _IGNORED_MODULE_NAMES:
            continue
        discovered.append(_module_name_from_path(path))
    return tuple(
        sorted(
            discovered,
            key=lambda module_name: (module_name.startswith("archived."), module_name),
        )
    )


def _manifest_for_list_mode(mode: str) -> tuple[str, ...]:
    if mode == "supported":
        return SUPPORTED_EXAMPLE_MODULES
    if mode == "archived":
        return ARCHIVED_EXAMPLE_MODULES
    if mode == "reference":
        return tuple(entry.module_name for entry in REFERENCE_EXAMPLE_ENTRIES)
    if mode == "readme":
        return README_EXAMPLE_MODULES
    raise ValueError(f"unknown catalog list mode {mode!r}")


def _duplicate_values(values: Iterable[T]) -> tuple[T, ...]:
    seen: set[T] = set()
    duplicates: set[T] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        else:
            seen.add(value)
    return tuple(sorted(duplicates))


def _joined_values(values: Iterable[object]) -> str:
    return ", ".join(str(value) for value in values)


def _validate_catalog() -> None:
    module_names = [entry.module_name for entry in EXAMPLE_CATALOG]
    supported_entries = _catalog_entries(archived=False)
    reference_entries = _reference_entries(supported_entries)
    readme_entries = _sorted_readme_entries(supported_entries)
    duplicate_module_names = _duplicate_values(module_names)
    if duplicate_module_names:
        raise ValueError(
            "example catalog contains duplicate module names: "
            f"{_joined_values(duplicate_module_names)}"
        )

    for entry in EXAMPLE_CATALOG:
        has_reference_number = entry.reference_number is not None
        has_reference_title = entry.reference_title is not None
        if has_reference_number != has_reference_title:
            raise ValueError(
                f"example catalog entry {entry.module_name!r} must define reference number and title together"
            )
        if entry.archived and entry.is_reference_example:
            raise ValueError(
                f"archived example catalog entry {entry.module_name!r} cannot also be a reference example"
            )
        if entry.archived and entry.readme_order is not None:
            raise ValueError(
                f"archived example catalog entry {entry.module_name!r} cannot be featured in the README"
            )
        if not entry.file_path.is_file():
            raise ValueError(
                f"example catalog entry {entry.module_name!r} does not map to an existing example file"
            )

    readme_orders = [
        entry.readme_order
        for entry in readme_entries
        if entry.readme_order is not None
    ]
    if any(order < 1 for order in readme_orders):
        raise ValueError("README example entries must use positive readme_order values")
    duplicate_readme_orders = _duplicate_values(readme_orders)
    if duplicate_readme_orders:
        raise ValueError(
            "README example entries contain duplicate readme_order values: "
            f"{_joined_values(duplicate_readme_orders)}"
        )
    if readme_orders and readme_orders != list(range(1, len(readme_orders) + 1)):
        raise ValueError("README example entries must use contiguous readme_order values")

    referenced_numbers = [entry.reference_number for entry in reference_entries]
    duplicate_reference_numbers = _duplicate_values(referenced_numbers)
    if duplicate_reference_numbers:
        raise ValueError(
            "reference example entries contain duplicate RFC example numbers: "
            f"{_joined_values(duplicate_reference_numbers)}"
        )

    gap_numbers = [gap.reference_number for gap in REFERENCE_EXAMPLE_GAPS]
    duplicate_gap_numbers = _duplicate_values(gap_numbers)
    if duplicate_gap_numbers:
        raise ValueError(
            "reference example gaps contain duplicate RFC example numbers: "
            f"{_joined_values(duplicate_gap_numbers)}"
        )

    overlap = set(referenced_numbers).intersection(gap_numbers)
    if overlap:
        overlap_numbers = ", ".join(str(number) for number in sorted(overlap))
        raise ValueError(
            f"reference example numbers overlap with gaps: {overlap_numbers}"
        )
    all_reference_numbers = sorted(
        number
        for number in (*referenced_numbers, *gap_numbers)
        if number is not None
    )
    expected_reference_numbers = list(range(1, len(all_reference_numbers) + 1))
    if all_reference_numbers != expected_reference_numbers:
        raise ValueError(
            "reference example numbers must be contiguous starting at 1"
        )

    discovered_modules = set(_discover_manifestable_modules())
    manifest_modules = set(module_names)
    missing_modules = tuple(sorted(discovered_modules - manifest_modules))
    if missing_modules:
        missing = ", ".join(missing_modules)
        raise ValueError(f"example files missing from catalog: {missing}")
    extra_modules = tuple(sorted(manifest_modules - discovered_modules))
    if extra_modules:
        extra = ", ".join(extra_modules)
        raise ValueError(f"example catalog entries missing files: {extra}")


_validate_catalog()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate the catalog manifest and generated README block",
    )
    parser.add_argument(
        "--check-manifest",
        action="store_true",
        help="validate the example catalog against the filesystem",
    )
    parser.add_argument(
        "--check-readme",
        action="store_true",
        help="validate the generated README featured-example block",
    )
    parser.add_argument(
        "--list",
        choices=("supported", "archived", "reference", "readme"),
        help="print one example module name per line from the selected manifest",
    )
    args = parser.parse_args(argv)

    _validate_catalog()
    if args.list is not None:
        print("\n".join(_manifest_for_list_mode(args.list)))
        return 0
    if args.check_manifest and not (args.check or args.check_readme):
        return 0

    readme = _README_PATH.read_text(encoding="utf-8")
    updated = sync_readme_featured_examples(readme)
    if args.check or args.check_readme:
        return 0 if updated == readme else 1

    if updated != readme:
        _README_PATH.write_text(updated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
