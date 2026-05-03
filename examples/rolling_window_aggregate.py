from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, Layer, Plane, Schema, Variant

from ._shared import example_route, int_schema


class RollingWindowAggregateExampleResult(TypedDict):
    rolling_sums: tuple[int, ...]


def run_example() -> RollingWindowAggregateExampleResult:
    graph = Graph()
    temperature = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="sensor",
        stream="temperature",
        variant=Variant.Meta,
        schema=int_schema("Temperature"),
    )
    watermark = example_route(
        plane=Plane.Read,
        layer=Layer.Internal,
        owner="scheduler",
        family="tick",
        stream="watermark",
        variant=Variant.Event,
        schema=Schema.bytes("WatermarkTick"),
    )
    sampled_temperature = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="sensor",
        stream="temperature_sampled",
        variant=Variant.Meta,
        schema=int_schema("Temperature"),
    )

    rolling_sums: list[int] = []
    graph.capacitor(
        source=temperature,
        sink=sampled_temperature,
        capacity=1,
        demand=watermark,
    )
    subscription = graph.window_aggregate_by_time(
        sampled_temperature,
        width=3,
        watermark=watermark,
        aggregate=sum,
    ).subscribe(rolling_sums.append)

    graph.publish(temperature, 20, control_epoch=20)
    graph.publish(watermark, b"tick-20", control_epoch=20)
    graph.publish(temperature, 21, control_epoch=21)
    graph.publish(watermark, b"tick-21", control_epoch=21)
    graph.publish(temperature, 22, control_epoch=22)
    graph.publish(watermark, b"tick-22", control_epoch=22)
    graph.publish(temperature, 23, control_epoch=23)
    graph.publish(watermark, b"tick-23", control_epoch=23)
    subscription.dispose()

    return {"rolling_sums": tuple(rolling_sums)}


if __name__ == "__main__":
    print(run_example())
