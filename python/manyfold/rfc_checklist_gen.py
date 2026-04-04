"""Generate an RFC implementation checklist."""

from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from typing import Iterator
from typing import TypeVar

import reactivex as rx
from reactivex import operators as ops

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent.parent
RFC_PATH = REPO_ROOT / "docs" / "rfc" / "wiregraph_rfc_rev2.md"
CHECKLIST_PATH = REPO_ROOT / "docs" / "rfc" / "implementation_checklist.md"

CHECKLIST_STATUS = {
    "6": ("x", "Typed identity objects, routes, ports, and producer/runtime refs are in the scaffold."),
    "7": (" ", "Descriptor shapes exist, but most RFC buckets still need semantic enforcement."),
    "8": (" ", "Closed/opened envelope scaffolding exists; lazy payload and audit behavior remain to implement."),
    "9": (" ", "Read/write namespace and shadow-route scaffolding exist; coherence rules are still incomplete."),
    "10": (" ", "Clock domains, taints, and schedule guard types exist; time semantics are still partial."),
    "11": (" ", "WriteBinding and shadow surfaces exist; closed-loop behavior is still a stub."),
    "12": (" ", "In-memory graph, mailboxes, and control-loop ticking exist; scheduler semantics are still incomplete."),
    "13": (" ", "Credit and backpressure flow control are not implemented yet."),
    "14": (" ", "Stateful operators, windows, and joins are not implemented yet."),
    "15": ("x", "Join planning now distinguishes local, repartition, broadcast-mirror, and lookup joins with explicit visible repartition nodes."),
    "16": ("x", "Middleware is now a first-class registration surface with RFC-aligned preservation checks."),
    "17": ("x", "Transport links now expose typed capability declarations that mesh bindings validate against."),
    "18": ("x", "Bridge-style mesh primitives now register as explicit graph topology instead of hidden wiring."),
    "19": ("x", "Query request/response routes and well-known debug event routes are now modeled as streams."),
    "20": ("x", "Route export visibility and per-principal capabilities now gate metadata, payload, replay, debug, and validation access."),
    "21": ("x", "Embedded firmware profile helpers and embedded-rule validation now cover scalar and bulk device routes."),
    "22": ("x", "The public API already prefers typed refs over ad hoc string identities."),
    "23": ("x", "The repository now ships a named RFC reference example suite, with runnable coverage for the currently supported examples."),
    "24": ("x", "Python wrapper ergonomics now include Rx-style observe/publish composition helpers."),
}

APPENDIX_STATUS = {
    "Common framework for flashed devices over serial, BLE, radio, USB, shared memory, and IP links": (
        "x",
        "Link registration now covers the RFC transport families as typed capability-bearing scaffolds.",
    ),
    "Raw and logical stream handling": ("x", "Raw/logical route layers are modeled in the current API."),
    "Real-time debugging and coherent flow exposure": (
        "x",
        "Query request/response and debug event routes now surface coherent runtime flow as streams.",
    ),
    "Retry, filtering, backpressure, overflow, and rate matching": (
        " ",
        "Overflow enums exist; runtime operators and flow control are still pending.",
    ),
    "Windows, aggregations, and streaming joins": (" ", "Pending operator implementation."),
    "Middleware as a first-class composition surface": ("x", "Middleware now registers explicitly with RFC preservation constraints."),
    "Transport-flexible mesh building blocks": ("x", "Links and mesh primitives now compose explicitly in the graph surface."),
    "Explicit support for write-back loops and shadow semantics": (
        " ",
        "WriteBinding and shadow routes exist, but loop semantics are still partial.",
    ),
    "Randomness and determinism explicitly modeled": (
        " ",
        "Taint domains exist, but determinism propagation rules are still incomplete.",
    ),
    "Scheduling and out-of-order bugs made harder to express": (
        " ",
        "Schedule guard/control-loop scaffolding exists, but semantics are incomplete.",
    ),
    "Metadata/payload split with lazy payload opening": (
        " ",
        "Envelope split exists, but lazy payload demand is still pending.",
    ),
    "Query plane modeled as streams": ("x", "Query request/response routes now behave like normal streams."),
    "No ad hoc strings in the typed runtime API": ("x", "Typed refs are already the primary runtime surface."),
    "Protobuf wire schema appendix": ("x", "The extracted schema scaffold is in the repository."),
    "Glossary, examples, appendices, and normative language": ("x", "The RFC document already includes these sections."),
}


T = TypeVar("T")


@dataclass(frozen=True)
class SectionStatus:
    number: str
    title: str
    checkbox: str
    detail: str


@dataclass(frozen=True)
class AppendixStatus:
    title: str
    checkbox: str
    detail: str


def _collect(observable) -> Iterator[T]:
    items: deque[T] = deque()
    observable.subscribe(items.append)
    return iter(items)


def parse_rfc_sections(rfc_path: Path = RFC_PATH) -> tuple[list[SectionStatus], list[AppendixStatus]]:
    lines = rfc_path.read_text().splitlines()
    section_matches = _collect(
        rx.from_iterable(lines).pipe(
            ops.filter(lambda line: line.startswith("## ")),
            ops.map(_parse_section_heading),
            ops.filter(lambda item: item is not None),
        )
    )
    appendix_matches = _collect(
        rx.from_iterable(lines).pipe(
            ops.filter(lambda line: line.startswith("- [")),
            ops.map(_parse_appendix_item),
            ops.filter(lambda item: item is not None),
        )
    )
    return section_matches, appendix_matches


def _parse_section_heading(line: str) -> SectionStatus | None:
    if "Appendix" in line:
        return None
    prefix, _, title = line[3:].partition(". ")
    if not prefix.isdigit():
        return None
    if prefix not in CHECKLIST_STATUS:
        return None
    checkbox, detail = CHECKLIST_STATUS[prefix]
    return SectionStatus(number=prefix, title=title, checkbox=checkbox, detail=detail)


def _parse_appendix_item(line: str) -> AppendixStatus | None:
    title = line.split("] ", 1)[-1].strip()
    status = APPENDIX_STATUS.get(title)
    if status is None:
        return None
    checkbox, detail = status
    return AppendixStatus(title=title, checkbox=checkbox, detail=detail)


def render_checklist(sections: Iterable[SectionStatus], appendix_items: Iterable[AppendixStatus]) -> str:
    section_lines = _collect(
        rx.from_iterable(sections).pipe(
            ops.map(
                lambda section: (
                    f"- [{section.checkbox}] {section.number}. {section.title}"
                    f" ({section.detail})"
                )
            ),
        )
    )
    appendix_lines = _collect(
        rx.from_iterable(appendix_items).pipe(
            ops.map(
                lambda item: f"- [{item.checkbox}] {item.title} ({item.detail})"
            ),
        )
    )
    lines = [
        "# RFC Implementation Checklist",
        "",
        "Generated from `docs/rfc/wiregraph_rfc_rev2.md` by `manyfold-rfc-checklist`.",
        "",
        "## Core Sections",
        *section_lines,
        "",
        "## Appendix F Acceptance Criteria",
        *appendix_lines,
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="validate generated file without rewriting it")
    args = parser.parse_args(argv)

    sections, appendix_items = parse_rfc_sections()
    checklist_content = render_checklist(sections, appendix_items)

    if args.check:
        current_checklist = CHECKLIST_PATH.read_text() if CHECKLIST_PATH.exists() else ""
        return 0 if current_checklist == checklist_content else 1

    CHECKLIST_PATH.write_text(checklist_content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
