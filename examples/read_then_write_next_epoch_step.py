from __future__ import annotations

from collections import deque
from typing import TypedDict

import reactivex as rx
from manyfold import (
    Graph,
    Layer,
    OwnerName,
    Plane,
    ReadThenWriteNextEpochStep,
    Schema,
    StreamFamily,
    StreamName,
    Variant,
    route,
)


class ReadThenWriteNextEpochStepExampleResult(TypedDict):
    mirrored_writes: tuple[bytes, ...]
    latest_payload: bytes
    latest_seq: int


def run_example() -> ReadThenWriteNextEpochStepExampleResult:
    """Install a shared-stream step and verify the graph sees the same writes."""
    graph = Graph()
    command_route = route(
        plane=Plane.Write,
        layer=Layer.Logical,
        owner=OwnerName("motor"),
        family=StreamFamily("speed"),
        stream=StreamName("next_epoch_command"),
        variant=Variant.Request,
        schema=Schema.bytes(name="SpeedCommand"),
    )
    staged_route = route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("motor"),
        family=StreamFamily("speed"),
        stream=StreamName("staged_next_epoch_command"),
        variant=Variant.Meta,
        schema=Schema.bytes(name="SpeedCommand"),
    )
    step = ReadThenWriteNextEpochStep.map(
        name="ReadThenWriteNextEpochSpeedStep",
        read=rx.from_iterable([b"slow", b"fast"]),
        output=staged_route,
        transform=bytes.upper,
    )

    mirrored_writes: deque[bytes] = deque()

    def on_write(value: bytes) -> None:
        mirrored_writes.append(value)

    graph.capacitor(
        source=staged_route,
        sink=command_route,
        capacity=1,
        immediate=True,
    )
    step_subscription = step.write.subscribe(on_write)
    graph.install(step)

    latest = graph.latest(command_route)
    step_subscription.dispose()

    assert latest is not None
    mirrored_writes_tuple = tuple(mirrored_writes)
    assert mirrored_writes_tuple == (b"SLOW", b"FAST")
    assert latest.value == b"FAST"
    assert latest.closed.seq_source == 2

    return {
        "mirrored_writes": mirrored_writes_tuple,
        "latest_payload": latest.value,
        "latest_seq": latest.closed.seq_source,
    }


if __name__ == "__main__":
    print(run_example())
