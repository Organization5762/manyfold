from __future__ import annotations

from collections import deque
from typing import TypedDict

from manyfold import Graph
from manyfold import Layer
from manyfold import OwnerName
from manyfold import Plane
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold import Variant
from manyfold import route


class BroadcastMirrorExampleResult(TypedDict):
    mirror_a: tuple[bytes, ...]
    mirror_b: tuple[bytes, ...]


def run_example() -> BroadcastMirrorExampleResult:
    graph = Graph()
    state_route = route(
        plane=Plane.State,
        layer=Layer.Logical,
        owner=OwnerName("cluster"),
        family=StreamFamily("mirror"),
        stream=StreamName("state"),
        variant=Variant.State,
        schema=Schema.bytes("MirrorState"),
    )
    mirror_a_route = route(
        plane=Plane.State,
        layer=Layer.Logical,
        owner=OwnerName("cluster"),
        family=StreamFamily("mirror"),
        stream=StreamName("mirror_a"),
        variant=Variant.State,
        schema=Schema.bytes("MirrorState"),
    )
    mirror_b_route = route(
        plane=Plane.State,
        layer=Layer.Logical,
        owner=OwnerName("cluster"),
        family=StreamFamily("mirror"),
        stream=StreamName("mirror_b"),
        variant=Variant.State,
        schema=Schema.bytes("MirrorState"),
    )

    mirror_a: deque[bytes] = deque()
    mirror_b: deque[bytes] = deque()
    graph.capacitor(
        source=state_route,
        sink=mirror_a_route,
        capacity=1,
        immediate=True,
    )
    graph.capacitor(
        source=state_route,
        sink=mirror_b_route,
        capacity=1,
        immediate=True,
    )
    sub_a = graph.observe(mirror_a_route, replay_latest=False).subscribe(
        lambda envelope: mirror_a.append(envelope.value)
    )
    sub_b = graph.observe(mirror_b_route, replay_latest=False).subscribe(
        lambda envelope: mirror_b.append(envelope.value)
    )
    graph.publish(state_route, b"v1")
    graph.publish(state_route, b"v2")
    sub_a.dispose()
    sub_b.dispose()

    return {
        "mirror_a": tuple(mirror_a),
        "mirror_b": tuple(mirror_b),
    }


if __name__ == "__main__":
    print(run_example())
