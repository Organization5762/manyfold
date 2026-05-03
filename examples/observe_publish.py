from __future__ import annotations

from collections import deque
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


class ObservePublishExampleResult(TypedDict):
    observed_payloads: tuple[bytes, ...]
    latest_payload: bytes
    latest_seq: int


def run_example() -> ObservePublishExampleResult:
    """Observe a route, then publish a second value and inspect the replay."""
    graph = Graph()
    accel_route = route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("imu"),
        family=StreamFamily("sensor"),
        stream=StreamName("accel"),
        variant=Variant.Meta,
        schema=Schema.bytes(name="Accel"),
    )
    observed_route = route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("imu"),
        family=StreamFamily("sensor"),
        stream=StreamName("accel_observed"),
        variant=Variant.Meta,
        schema=Schema.bytes(name="Accel"),
    )
    graph.capacitor(
        source=accel_route,
        sink=observed_route,
        capacity=1,
        immediate=True,
    )

    graph.publish(accel_route, b"first")

    observed_payloads: deque[bytes] = deque()

    def on_next(envelope) -> None:
        observed_payloads.append(envelope.value)

    subscription = graph.observe(observed_route).subscribe(on_next)
    graph.publish(accel_route, b"second")
    subscription.dispose()

    latest = graph.latest(accel_route)
    assert latest is not None
    observed_payloads_tuple = tuple(observed_payloads)
    assert observed_payloads_tuple == (b"first", b"second")
    assert latest.value == b"second"
    assert latest.closed.seq_source == 2

    return {
        "observed_payloads": observed_payloads_tuple,
        "latest_payload": latest.value,
        "latest_seq": latest.closed.seq_source,
    }


if __name__ == "__main__":
    print(run_example())
