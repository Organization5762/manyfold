from __future__ import annotations

from typing import TypedDict

import reactivex as rx

from manyfold import Graph
from manyfold import Layer
from manyfold import OwnerName
from manyfold import Plane
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold import Variant
from manyfold import route


class PipeRouteExampleResult(TypedDict):
    latest_payload: bytes
    latest_seq: int


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
        schema=Schema.bytes("SpeedCommand"),
    )
    staged_route = route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("motor"),
        family=StreamFamily("speed"),
        stream=StreamName("staged_command"),
        variant=Variant.Meta,
        schema=Schema.bytes("SpeedCommand"),
    )

    graph.capacitor(
        source=staged_route,
        sink=command_route,
        capacity=1,
        immediate=True,
    )
    graph.pipe(rx.from_iterable([b"slow", b"fast"]), staged_route)

    latest = graph.latest(command_route)
    assert latest is not None
    assert latest.value == b"fast"
    assert latest.closed.seq_source == 2

    return {
        "latest_payload": latest.value,
        "latest_seq": latest.closed.seq_source,
    }


if __name__ == "__main__":
    print(run_example())
