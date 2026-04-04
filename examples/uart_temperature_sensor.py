from __future__ import annotations

from typing import TypedDict

import reactivex as rx

from manyfold import Graph
from manyfold import Layer
from manyfold import OwnerName
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold import route
from manyfold.embedded import EmbeddedDeviceProfile


class UartTemperatureSensorExampleResult(TypedDict):
    raw_latest: int
    smoothed_latest: int
    profile_issues: tuple[str, ...]


def _int_schema(schema_id: str) -> Schema[int]:
    return Schema(
        schema_id=schema_id,
        version=1,
        encode=lambda value: str(value).encode("ascii"),
        decode=lambda payload: int(payload.decode("ascii")),
    )


def run_example() -> UartTemperatureSensorExampleResult:
    graph = Graph()
    profile = EmbeddedDeviceProfile()
    raw_sensor = profile.scalar_sensor(
        owner=OwnerName("uart-temp"),
        family=StreamFamily("sensor"),
        stream=StreamName("temperature_raw"),
        schema=_int_schema("Temperature"),
    )
    smoothed_route = route(
        plane=raw_sensor.metadata_route.plane,
        layer=Layer.Logical,
        owner=raw_sensor.metadata_route.owner,
        family=raw_sensor.metadata_route.family,
        stream=StreamName("temperature_smoothed"),
        variant=raw_sensor.metadata_route.variant,
        schema=raw_sensor.metadata_route.schema,
    )

    raw_values = (21, 25, 24)
    smoothed_values = (21, 23, 24)
    graph.pipe(rx.from_iterable(raw_values), raw_sensor.metadata_route)
    graph.pipe(rx.from_iterable(smoothed_values), smoothed_route)

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

