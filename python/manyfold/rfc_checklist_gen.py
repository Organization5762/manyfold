"""Generate an RFC implementation checklist."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Protocol

try:
    from ._repo_paths import ensure_repo_import_paths
except ImportError:
    PACKAGE_DIR = Path(__file__).resolve().parent
    python_dir = str(PACKAGE_DIR.parent)
    if python_dir not in sys.path:
        sys.path.insert(0, python_dir)
    from manyfold._repo_paths import ensure_repo_import_paths

ensure_repo_import_paths()

from examples import REFERENCE_EXAMPLE_PROGRESS_DETAIL

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent.parent
RFC_PATH = REPO_ROOT / "docs" / "rfc" / "wiregraph_rfc_rev2.md"
CHECKLIST_PATH = REPO_ROOT / "docs" / "rfc" / "implementation_checklist.md"
CHECKBOX_STATES = frozenset({" ", "x"})

CHECKLIST_STATUS = {
    "6": (
        "x",
        "Typed identity objects, routes, ports, and producer/runtime refs are in the scaffold.",
    ),
    "7": (
        " ",
        "Route descriptions now expose RFC-shaped identity/schema/time/ordering/flow/retention/security/visibility/environment/debug buckets, and the Python layer now supports explicit per-route replay/retention overrides, but most other bucket semantics are still defaults.",
    ),
    "8": (
        " ",
        "Closed/opened envelope scaffolding exists, lazy payload open-on-demand and route-level payload-demand accounting now work, replay retention now follows explicit per-route policy, payload-store eviction now honors route-level retention policy in the Python layer, route-local audit snapshots now summarize producers, subscribers, related writes, and taint repairs, and retained lineage now tracks trace/causality/correlation ids with trace-query support, but broader envelope semantics remain partial.",
    ),
    "9": (
        " ",
        "Read/write namespace and shadow-route scaffolding exist, and shadow snapshots now derive explicit coherence taints for pending, stale, unmatched, and stable states; broader namespace coherence rules are still incomplete.",
    ),
    "10": (
        " ",
        "Clock domains, taints, guarded scheduler release, validated event-time progress, idempotent watermark releases, and explicit taint-repair operators now exist, but broader time semantics and repair coverage are still partial.",
    ),
    "11": (
        " ",
        "WriteBinding/shadow state exists, guarded writes now support typed retry/ack policies, shadow reconciliation now derives explicit coherence taints, and unsafe direct feedback loops are validated, but fuller loop semantics remain partial.",
    ),
    "12": (
        " ",
        "In-memory graph, mailboxes, and guarded scheduler/control-loop ticking exist, but execution semantics are still incomplete.",
    ),
    "13": (
        " ",
        "Mailbox credit snapshots, queue depth, overflow behavior, first-class filtering, typed guarded-write retry policies, and explicit demand-driven rate matching now exist, but broader flow control remains partial.",
    ),
    "14": (
        " ",
        "Basic stateful_map, filter, sliding window, partition-scoped rolling and event-time windows/aggregations, trigger-driven rolling window aggregation, latest-join, stream-table lookup join, and bounded event-time interval-join operators now exist in the Python layer, but richer repartitioned/event-time semantics are still pending.",
    ),
    "15": (
        "x",
        "Join planning now distinguishes local, repartition, broadcast-mirror, and lookup joins with explicit visible repartition nodes.",
    ),
    "16": (
        "x",
        "Middleware is now a first-class registration surface with RFC-aligned preservation checks.",
    ),
    "17": (
        "x",
        "Transport links now expose typed capability declarations that mesh bindings validate against.",
    ),
    "18": (
        "x",
        "Bridge-style mesh primitives now register as explicit graph topology instead of hidden wiring.",
    ),
    "19": (
        "x",
        "Query request/response routes and well-known debug event routes are now modeled as streams.",
    ),
    "20": (
        "x",
        "Route export visibility and per-principal capabilities now gate metadata, payload, replay, debug, and validation access.",
    ),
    "21": (
        "x",
        "Embedded firmware profile helpers and embedded-rule validation now cover scalar and bulk device routes.",
    ),
    "22": (
        "x",
        "The public API already prefers typed refs over ad hoc string identities.",
    ),
    "23": (
        "x",
        REFERENCE_EXAMPLE_PROGRESS_DETAIL,
    ),
    "24": (
        "x",
        "Python wrapper ergonomics now include Rx-style observe/publish composition helpers.",
    ),
}

APPENDIX_STATUS = {
    "Common framework for flashed devices over serial, BLE, radio, USB, shared memory, and IP links": (
        "x",
        "Link registration now covers the RFC transport families as typed capability-bearing scaffolds.",
    ),
    "Raw and logical stream handling": (
        "x",
        "Raw/logical route layers are modeled in the current API.",
    ),
    "Real-time debugging and coherent flow exposure": (
        "x",
        "Query request/response and debug event routes now surface coherent runtime flow as streams.",
    ),
    "Retry, filtering, backpressure, overflow, and rate matching": (
        " ",
        "First-class value filtering, mailbox overflow behavior, route credit snapshots, typed guarded-write retry policies, and demand-driven rate matching now exist, but broader flow-control semantics are still pending.",
    ),
    "Windows, aggregations, and streaming joins": (
        " ",
        "Basic filter, sliding windows, partition-scoped watermark-aware event-time windows and aggregations, trigger-driven rolling window aggregation, latest-join composition, stream-table lookup joins, and bounded event-time interval joins now exist, but richer repartitioned/event-time semantics are still pending.",
    ),
    "Middleware as a first-class composition surface": (
        "x",
        "Middleware now registers explicitly with RFC preservation constraints.",
    ),
    "Transport-flexible mesh building blocks": (
        "x",
        "Links and mesh primitives now compose explicitly in the graph surface.",
    ),
    "Explicit support for write-back loops and shadow semantics": (
        " ",
        "WriteBinding and shadow routes exist, typed guarded-write retry/ack policies are now available, shadow reconciliation now derives explicit coherence taints, and direct unsafe feedback loops are validated, but fuller loop semantics remain partial.",
    ),
    "Randomness and determinism explicitly modeled": (
        " ",
        "Taint domains, stream-bound taint query inspection with repair notes, and explicit repair operators now exist, but determinism propagation rules are still incomplete.",
    ),
    "Scheduling and out-of-order bugs made harder to express": (
        " ",
        "Guarded scheduler release, validated event-time progress, and idempotent watermark releases now exist, but semantics are still incomplete.",
    ),
    "Metadata/payload split with lazy payload opening": (
        " ",
        "Envelope split exists, lazy payload open-on-demand now works, route-level payload-demand accounting is available, both replay retention and payload-store eviction now follow explicit per-route policy in the Python layer, route-local audit snapshots now summarize producers, subscribers, related writes, and taint repairs, and retained lineage now tracks trace/causality/correlation ids with trace-query support, but broader envelope semantics are still pending.",
    ),
    "Query plane modeled as streams": (
        "x",
        "Query request/response routes now behave like normal streams.",
    ),
    "No ad hoc strings in the typed runtime API": (
        "x",
        "Typed refs are already the primary runtime surface.",
    ),
    "Protobuf wire schema appendix": (
        "x",
        "The RFC appendix remains the source for the schema sketch.",
    ),
    "Glossary, examples, appendices, and normative language": (
        "x",
        "The RFC document already includes these sections.",
    ),
}


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


class _TextPath(Protocol):
    """Minimal path protocol used by the generated-file writer."""

    def exists(self) -> bool: ...

    def read_text(self, *, encoding: str) -> str: ...

    def write_text(self, content: str, *, encoding: str) -> int: ...


def parse_rfc_sections(
    rfc_path: Path = RFC_PATH,
) -> tuple[list[SectionStatus], list[AppendixStatus]]:
    lines = rfc_path.read_text(encoding="utf-8").splitlines()
    section_matches = [
        section
        for line in lines
        if line.startswith("## ")
        for section in [_parse_section_heading(line)]
        if section is not None
    ]
    appendix_matches = [
        item
        for line in lines
        if line.startswith("- [")
        for item in [_parse_appendix_item(line)]
        if item is not None
    ]
    return section_matches, appendix_matches


def _parse_section_heading(line: str) -> SectionStatus | None:
    if line.startswith("## Appendix"):
        return None
    prefix, _, title = line[3:].partition(". ")
    if not prefix.isdigit():
        return None
    if prefix not in CHECKLIST_STATUS:
        return None
    checkbox, detail = CHECKLIST_STATUS[prefix]
    return SectionStatus(number=prefix, title=title, checkbox=checkbox, detail=detail)


def _parse_appendix_item(line: str) -> AppendixStatus | None:
    marker, separator, title = line.partition("] ")
    if separator == "":
        return None
    checkbox = marker.removeprefix("- [")
    if checkbox not in CHECKBOX_STATES:
        return None
    title = title.strip()
    status = APPENDIX_STATUS.get(title)
    if status is None:
        return None
    checkbox, detail = status
    return AppendixStatus(title=title, checkbox=checkbox, detail=detail)


def render_checklist(
    sections: Iterable[SectionStatus], appendix_items: Iterable[AppendixStatus]
) -> str:
    section_lines = [
        f"- [{section.checkbox}] {section.number}. {section.title} ({section.detail})"
        for section in sections
    ]
    appendix_lines = [
        f"- [{item.checkbox}] {item.title} ({item.detail})"
        for item in appendix_items
    ]
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


def _write_if_changed(path: _TextPath, content: str) -> bool:
    """Write generated content only when the target bytes would change."""

    if path.exists() and path.read_text(encoding="utf-8") == content:
        return False
    path.write_text(content, encoding="utf-8")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate generated file without rewriting it",
    )
    args = parser.parse_args(argv)

    sections, appendix_items = parse_rfc_sections()
    checklist_content = render_checklist(sections, appendix_items)

    if args.check:
        current_checklist = (
            CHECKLIST_PATH.read_text(encoding="utf-8")
            if CHECKLIST_PATH.exists()
            else ""
        )
        return 0 if current_checklist == checklist_content else 1

    _write_if_changed(CHECKLIST_PATH, checklist_content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
