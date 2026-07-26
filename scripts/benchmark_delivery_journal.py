"""Measure full-synchronous SQLite append and latest-slot journal writes."""

from __future__ import annotations

import argparse
import json
import tempfile
from collections import deque
from pathlib import Path
from time import perf_counter_ns

from manyfold.architecture._transport_delivery_journal import (
    _DeliveryJournal,
    _OutboxRecord,
)

_MIN_STORAGE_BYTES = 1024 * 1024
_MAX_LATENCY_SAMPLES = 100_000


def _benchmark(
    root: Path,
    *,
    iterations: int,
    payload_bytes: int,
    semantics: str,
    latest_sources: int,
) -> dict[str, int | float | str]:
    path = root / f"{semantics}.sqlite3"
    max_items = iterations if semantics == "append" else latest_sources
    max_storage_bytes = max(
        _MIN_STORAGE_BYTES,
        iterations * (payload_bytes + 512),
    )
    journal = _DeliveryJournal(
        path,
        max_outbox_items=max_items,
        max_inbox_items=1,
        max_storage_bytes=max_storage_bytes,
    )
    payload = b"x" * payload_bytes
    latencies_ns: deque[int] = deque(maxlen=_MAX_LATENCY_SAMPLES)
    started = perf_counter_ns()
    try:
        for index in range(iterations):
            operation_started = perf_counter_ns()
            message_id = journal.next_message_id()
            journal.insert_outbox(
                _OutboxRecord(
                    message_id=message_id,
                    topic="benchmark.topic",
                    semantics=semantics,
                    source_key=(
                        f"source-{index % latest_sources}"
                        if semantics == "latest"
                        else None
                    ),
                    frame_kind=3,
                    channel="benchmark.topic",
                    correlation_id=None,
                    payload=payload,
                    attempts=0,
                    max_attempts=64,
                ),
                created_at=float(index),
                expires_at=float(iterations + 1),
                topic_item_limit=max_items,
                topic_byte_limit=max_storage_bytes,
                soft_limit_ratio=0.7,
            )
            latencies_ns.append(perf_counter_ns() - operation_started)
        elapsed_ns = perf_counter_ns() - started
        stats = journal.stats()
    finally:
        journal.close()
    expected_retained_items = (
        min(iterations, latest_sources) if semantics == "latest" else iterations
    )
    if stats.outbox_items != expected_retained_items:
        raise RuntimeError(
            f"{semantics} retained {stats.outbox_items} records; "
            f"expected {expected_retained_items}"
        )
    ordered = sorted(latencies_ns)
    return {
        "semantics": semantics,
        "iterations": iterations,
        "payload_bytes": payload_bytes,
        "latency_samples": len(ordered),
        "latest_sources": latest_sources if semantics == "latest" else 0,
        "expected_retained_items": expected_retained_items,
        "elapsed_seconds": elapsed_ns / 1_000_000_000,
        "operations_per_second": iterations * 1_000_000_000 / elapsed_ns,
        "latency_p50_ms": ordered[len(ordered) // 2] / 1_000_000,
        "latency_p95_ms": ordered[int(len(ordered) * 0.95)] / 1_000_000,
        "retained_items": stats.outbox_items,
        "logical_storage_bytes": stats.logical_bytes,
        "database_file_bytes": path.stat().st_size,
    }


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=_positive_integer, default=1000)
    parser.add_argument("--latest-sources", type=_positive_integer, default=1)
    parser.add_argument("--payload-bytes", type=_positive_integer, default=256)
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory() as temporary_directory:
        root = Path(temporary_directory)
        results = [
            _benchmark(
                root,
                iterations=arguments.iterations,
                payload_bytes=arguments.payload_bytes,
                semantics=semantics,
                latest_sources=arguments.latest_sources,
            )
            for semantics in ("append", "latest")
        ]
    print(json.dumps({"results": results}, indent=2, sort_keys=True))


if __name__ == "__main__":
    _main()
