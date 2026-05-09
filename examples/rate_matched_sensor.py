from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, Layer, Plane, Schema, Variant

from ._shared import example_route, int_schema


def run_example() -> RateMatchedSensorExampleResult:
    graph = Graph()
    source = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="sensor",
        stream="accel",
        variant=Variant.Meta,
        schema=int_schema("Accel"),
    )
    demand = example_route(
        plane=Plane.Read,
        layer=Layer.Internal,
        owner="scheduler",
        family="tick",
        stream="drain",
        variant=Variant.Event,
        schema=Schema.bytes(name="DrainTick"),
    )
    sampled = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="sensor",
        stream="accel_sampled",
        variant=Variant.Meta,
        schema=int_schema("Accel"),
    )

    emitted_values: list[int] = []
    graph.capacitor(
        source=source,
        sink=sampled,
        capacity=1,
        demand=demand,
    )
    subscription = graph.observe(sampled, replay_latest=False).subscribe(
        lambda envelope: emitted_values.append(envelope.value)
    )

    graph.publish(source, 10)
    graph.publish(source, 11)
    graph.publish(demand, b"tick-1")
    graph.publish(source, 12)
    graph.publish(source, 13)
    graph.publish(demand, b"tick-2")
    subscription.dispose()

    return {"emitted_values": tuple(emitted_values)}


class RateMatchedSensorExampleResult(TypedDict):
    emitted_values: tuple[int, ...]


if __name__ == "__main__":
    print(run_example())
