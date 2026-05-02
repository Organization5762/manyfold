from __future__ import annotations

from typing import TypedDict

from manyfold import Graph
from manyfold import Layer
from manyfold import Plane
from manyfold import Variant

from .._shared import example_route
from .._shared import int_schema


class WindowedJoinExampleResult(TypedDict):
    rolling_windows: tuple[tuple[int, ...], ...]
    joined_values: tuple[int, ...]


def run_example() -> WindowedJoinExampleResult:
    graph = Graph()
    accel = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="sensor",
        stream="accel",
        variant=Variant.Meta,
        schema=int_schema("Accel"),
    )
    gyro = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="sensor",
        stream="gyro",
        variant=Variant.Meta,
        schema=int_schema("Gyro"),
    )
    staged_accel = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="sensor",
        stream="staged_accel",
        variant=Variant.Meta,
        schema=int_schema("Accel"),
    )

    rolling_windows: list[tuple[int, ...]] = []
    joined_values: list[int] = []

    graph.capacitor(
        source=accel,
        sink=staged_accel,
        capacity=2,
        immediate=True,
    )
    graph.window(staged_accel, size=2).subscribe(
        lambda values: rolling_windows.append(tuple(values))
    )
    graph.join_latest(
        staged_accel,
        gyro,
        combine=lambda left, right: left + right,
    ).subscribe(joined_values.append)

    graph.publish(accel, 1)
    graph.publish(gyro, 10)
    graph.publish(accel, 2)
    graph.publish(gyro, 11)

    return {
        "rolling_windows": tuple(rolling_windows),
        "joined_values": tuple(joined_values),
    }


if __name__ == "__main__":
    print(run_example())
