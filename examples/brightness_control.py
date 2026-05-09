from __future__ import annotations

from typing import TypedDict

import reactivex as rx
from manyfold import (
    Graph,
    Layer,
    Plane,
    ReadThenWriteNextEpochStep,
    RouteIdentity,
    RouteNamespace,
    Variant,
    route,
)


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
    staged_pwm_route = route(
        namespace=RouteNamespace(plane=Plane.Write, layer=Layer.Logical),
        identity=RouteIdentity.of(
            owner="led",
            family="pwm",
            stream="staged_duty_cycle",
            variant=Variant.Request,
        ),
        schema=bytes,
        schema_id="PwmDutyCycle",
    )
    step = ReadThenWriteNextEpochStep.map(
        name="BrightnessToPwm",
        read=rx.from_iterable([0, 32, 255]),
        output=staged_pwm_route,
        transform=lambda value: bytes([value]),
    )

    graph.capacitor(
        source=staged_pwm_route,
        sink=pwm_route,
        capacity=1,
        immediate=True,
    )
    graph.install(step)
    latest = graph.latest(pwm_route)
    assert latest is not None

    return {
        "pwm_latest": latest.value,
        "pwm_seq": latest.closed.seq_source,
    }


class BrightnessControlExampleResult(TypedDict):
    pwm_latest: bytes
    pwm_seq: int


if __name__ == "__main__":
    print(run_example())
