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
    "7": ("x", "Descriptors now derive route-specific policies for identity, time, flow, retention, visibility, and debug behavior."),
    "8": ("x", "Closed envelopes stay metadata-only while payload bytes live in a separate store and are joined on demand with route audit events."),
    "9": ("x", "Route and write-binding validation now enforce read/write plane semantics and write.shadow coherence contracts."),
    "10": ("x", "Writes now carry clock-domain metadata plus determinism/coherence/scheduling taints, and control loops emit epoch guards."),
    "11": (" ", "WriteBinding and shadow surfaces exist; closed-loop behavior is still a stub."),
    "12": (" ", "In-memory graph, mailboxes, and control-loop ticking exist; scheduler semantics are still incomplete."),
    "13": (" ", "Credit and backpressure flow control are not implemented yet."),
    "14": (" ", "Stateful operators, windows, and joins are not implemented yet."),
    "15": (" ", "Cross-partition joins are not implemented yet."),
    "16": (" ", "Middleware is not implemented yet."),
    "17": (" ", "Transport and link capabilities are not implemented yet."),
    "18": (" ", "Mesh primitives are not implemented yet."),
    "19": (" ", "Catalog/latest/topology/validation helpers exist; stream-shaped query/debug planes still need work."),
    "20": (" ", "Third-party export and capability security model are not implemented yet."),
    "21": ("x", "Embedded firmware profile helpers and embedded-rule validation now cover scalar and bulk device routes."),
    "22": ("x", "The public API already prefers typed refs over ad hoc string identities."),
    "23": ("x", "The repository now ships a named RFC reference example suite, with runnable coverage for the currently supported examples."),
    "24": ("x", "Python wrapper ergonomics now include Rx-style observe/publish composition helpers."),
}

APPENDIX_STATUS = {
    "Common framework for flashed devices over serial, BLE, radio, USB, shared memory, and IP links": (
        " ",
        "Transport scaffolding is not implemented yet.",
    ),
    "Raw and logical stream handling": ("x", "Raw/logical route layers are modeled in the current API."),
    "Real-time debugging and coherent flow exposure": (
        " ",
        "Debug/query helpers exist, but coherent debug streams are not implemented yet.",
    ),
    "Retry, filtering, backpressure, overflow, and rate matching": (
        " ",
        "Overflow enums exist; runtime operators and flow control are still pending.",
    ),
    "Windows, aggregations, and streaming joins": (" ", "Pending operator implementation."),
    "Middleware as a first-class composition surface": (" ", "Pending middleware implementation."),
    "Transport-flexible mesh building blocks": (" ", "Pending transport and mesh implementation."),
    "Explicit support for write-back loops and shadow semantics": (
        "x",
        "Write bindings now validate the write.shadow desired/reported/effective/ack contract.",
    ),
    "Randomness and determinism explicitly modeled": (
        "x",
        "Determinism taints are now attached to ephemeral/non-replayable writes as part of the time model.",
    ),
    "Scheduling and out-of-order bugs made harder to express": (
        "x",
        "Control-loop writes carry control-epoch metadata and not-before-next-epoch guards.",
    ),
    "Metadata/payload split with lazy payload opening": (
        "x",
        "Payload bytes now live outside ClosedEnvelope metadata and are opened lazily from the payload store.",
    ),
    "Query plane modeled as streams": (" ", "Query helpers exist, but the query plane is not yet stream-native."),
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
