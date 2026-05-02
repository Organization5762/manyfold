from __future__ import annotations

import reactivex as rx
from typing import TypedDict

from manyfold import EmbeddedDeviceProfile
from manyfold import Graph
from manyfold import Layer
from manyfold import OwnerName
from manyfold import StreamFamily
from manyfold import StreamName

from ._shared import int_schema
from ._shared import sibling_route


class UartTemperatureSensorExampleResult(TypedDict):
    raw_latest: int
    smoothed_latest: int
    profile_issues: tuple[str, ...]


def run_example() -> UartTemperatureSensorExampleResult:
    graph = Graph()
    profile = EmbeddedDeviceProfile()
    raw_sensor = profile.scalar_sensor(
        owner=OwnerName("uart-temp"),
        family=StreamFamily("sensor"),
        stream=StreamName("temperature_raw"),
        schema=int_schema("Temperature"),
    )
    smoothed_route = sibling_route(
        raw_sensor.metadata_route,
        layer=Layer.Logical,
        stream="temperature_smoothed",
    )

    raw_values = (21, 25, 24)
    graph.capacitor(
        source=raw_sensor.metadata_route,
        sink=smoothed_route,
        capacity=1,
        immediate=True,
    )
    graph.pipe(rx.from_iterable(raw_values), raw_sensor.metadata_route)

    raw_latest = graph.latest(raw_sensor.metadata_route)
    smoothed_latest = graph.latest(smoothed_route)
    assert raw_latest is not None
    assert smoothed_latest is not None

    return {
        "raw_latest": raw_latest.value,
        "smoothed_latest": smoothed_latest.value,
        "profile_issues": raw_sensor.validate(),
    }


if __name__ == "__main__":
    print(run_example())
