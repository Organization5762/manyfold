"""Reference example suite declared by RFC section 23."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Callable

from examples.broadcast_mirror import run_example as run_broadcast_mirror_example
from examples.brightness_control import run_example as run_brightness_control_example
from examples.closed_counter_loop import run_example as run_closed_counter_loop_example
from examples.lazy_lidar_payload import run_example as run_lazy_lidar_payload_example
from examples.mailbox_bridge import run_example as run_mailbox_bridge_example
from examples.uart_temperature_sensor import run_example as run_uart_temperature_sensor_example

ExampleRunner = Callable[[], Any]


@dataclass(frozen=True)
class ReferenceExample:
    number: int
    title: str
    summary: str
    implemented: bool
    runner: ExampleRunner | None = None


REFERENCE_EXAMPLE_SUITE: tuple[ReferenceExample, ...] = (
    ReferenceExample(
        1,
        "UART temperature sensor",
        "A raw UART sensor emits metadata and a smoothed logical temperature stream.",
        True,
        run_uart_temperature_sensor_example,
    ),
    ReferenceExample(
        2,
        "IMU fusion join",
        "Interval join between accelerometer and gyro streams with event-time alignment.",
        False,
    ),
    ReferenceExample(
        3,
        "Lazy LiDAR payload",
        "Metadata filtering happens before a selected LiDAR payload is opened.",
        True,
        run_lazy_lidar_payload_example,
    ),
    ReferenceExample(
        4,
        "Closed counter loop",
        "Desired/reported/effective shadow semantics for a counter write-back loop.",
        True,
        run_closed_counter_loop_example,
    ),
    ReferenceExample(
        5,
        "Brightness control",
        "Logical brightness requests translate into raw PWM writes.",
        True,
        run_brightness_control_example,
    ),
    ReferenceExample(
        6,
        "Mailbox bridge",
        "An explicit mailbox boundary declares overflow behavior between async domains.",
        True,
        run_mailbox_bridge_example,
    ),
    ReferenceExample(
        7,
        "Cross-partition join",
        "A repartition join with skew metrics and planner output.",
        False,
    ),
    ReferenceExample(
        8,
        "Broadcast mirror",
        "Deterministic fan-out mirrors a state update to multiple observers.",
        True,
        run_broadcast_mirror_example,
    ),
    ReferenceExample(
        9,
        "Raft demo",
        "A minimal Raft demo over routed heartbeats, votes, and append entries.",
        False,
    ),
    ReferenceExample(
        10,
        "Ephemeral entropy stream",
        "Per-request entropy derivation that taints determinism explicitly.",
        False,
    ),
)


def reference_example_suite() -> tuple[ReferenceExample, ...]:
    return REFERENCE_EXAMPLE_SUITE


def implemented_reference_examples() -> tuple[ReferenceExample, ...]:
    return tuple(example for example in REFERENCE_EXAMPLE_SUITE if example.implemented)

