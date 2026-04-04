from __future__ import annotations

from typing import TypedDict

import reactivex as rx

from manyfold import Graph
from manyfold import Layer
from manyfold import OwnerName
from manyfold import Plane
from manyfold import ReadThenWriteNextEpochStep
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold import Variant
from manyfold import route


class BrightnessControlExampleResult(TypedDict):
    pwm_latest: bytes
    pwm_seq: int


def run_example() -> BrightnessControlExampleResult:
    graph = Graph()
    pwm_route = route(
        plane=Plane.Write,
        layer=Layer.Raw,
        owner=OwnerName("led"),
        family=StreamFamily("pwm"),
        stream=StreamName("duty_cycle"),
        variant=Variant.Request,
        schema=Schema.bytes("PwmDutyCycle"),
    )
    step = ReadThenWriteNextEpochStep.map(
        name="BrightnessToPwm",
        read=rx.from_iterable([0, 32, 255]),
        output=pwm_route,
        transform=lambda value: bytes([value]),
    )

    graph.install(step)
    latest = graph.latest(pwm_route)
    assert latest is not None

    return {
        "pwm_latest": latest.value,
        "pwm_seq": latest.closed.seq_source,
    }


if __name__ == "__main__":
    print(run_example())

