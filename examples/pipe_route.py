from __future__ import annotations

from typing import TypedDict

import reactivex as rx
from manyfold import (
    Graph,
    Layer,
    OwnerName,
    Plane,
    Schema,
    StreamFamily,
    StreamName,
    Variant,
    route,
)

from examples._shared import require_latest


def run_example() -> PipeRouteExampleResult:
    """Pipe an Rx stream into a writable route and inspect the latest value."""
    graph = Graph()
    command_route = route(
        plane=Plane.Write,
        layer=Layer.Logical,
        owner=OwnerName("motor"),
        family=StreamFamily("speed"),
        stream=StreamName("command"),
        variant=Variant.Request,
        schema=Schema.bytes(name="SpeedCommand"),
    )
    staged_route = route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("motor"),
        family=StreamFamily("speed"),
        stream=StreamName("staged_command"),
        variant=Variant.Meta,
        schema=Schema.bytes(name="SpeedCommand"),
    )

    graph.capacitor(
        source=staged_route,
        sink=command_route,
        capacity=1,
        immediate=True,
    )
    graph.pipe(rx.from_iterable([b"slow", b"fast"]), staged_route)

    latest = require_latest(graph, command_route, "pipe_route command route")
    if latest.value != b"fast" or latest.closed.seq_source != 2:
        raise RuntimeError("pipe_route did not publish the expected latest command")

    return {
        "latest_payload": latest.value,
        "latest_seq": latest.closed.seq_source,
    }


class PipeRouteExampleResult(TypedDict):
    latest_payload: bytes
    latest_seq: int


if __name__ == "__main__":
    print(run_example())
