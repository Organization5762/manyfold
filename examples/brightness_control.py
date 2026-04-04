from __future__ import annotations

from typing import TypedDict

import reactivex as rx

from manyfold import Graph
from manyfold import ReadThenWriteNextEpochStep
from manyfold import Variant
from manyfold import route
from manyfold.primitives import RouteIdentity
from manyfold.primitives import RouteNamespace
from manyfold import Layer
from manyfold import Plane


class BrightnessControlExampleResult(TypedDict):
    pwm_latest: bytes
    pwm_seq: int


def run_example() -> BrightnessControlExampleResult:
    graph = Graph()
    pwm_route = route(
        namespace=RouteNamespace(plane=Plane.Write, layer=Layer.Raw),
        identity=RouteIdentity.of(
            owner="led",
            family="pwm",
            stream="duty_cycle",
            variant=Variant.Request,
        ),
        schema=bytes,
        schema_id="PwmDutyCycle",
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
