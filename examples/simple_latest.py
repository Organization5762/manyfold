from __future__ import annotations

from typing import TypedDict

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


class SimpleLatestExampleResult(TypedDict):
    latest_payload: bytes
    latest_seq: int


def run_example() -> SimpleLatestExampleResult:
    """Smallest end-to-end example: publish one value, then read it back."""
    graph = Graph()
    route_ref = route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("demo"),
        family=StreamFamily("example"),
        stream=StreamName("latest"),
        variant=Variant.Meta,
        schema=Schema.bytes("DemoLatest"),
    )

    graph.publish(route_ref, b"hello")
    latest = graph.latest(route_ref)
    assert latest is not None

    return {
        "latest_payload": latest.value,
        "latest_seq": latest.closed.seq_source,
    }


if __name__ == "__main__":
    print(run_example())
