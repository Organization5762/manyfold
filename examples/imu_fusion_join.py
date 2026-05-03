from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, Layer, Plane, Schema, Variant

from ._shared import example_route, int_schema


class ImuFusionJoinExampleResult(TypedDict):
    fused_pairs: tuple[tuple[int, int], ...]
    latest_pose: tuple[int, int] | None


def _pose_schema() -> Schema[tuple[int, int]]:
    return Schema(
        schema_id="Pose",
        version=1,
        encode=lambda value: f"{value[0]},{value[1]}".encode("ascii"),
        decode=lambda payload: tuple(
            int(part) for part in payload.decode("ascii").split(",", 1)
        ),
    )


def run_example() -> ImuFusionJoinExampleResult:
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
    pose = example_route(
        plane=Plane.State,
        layer=Layer.Logical,
        owner="imu",
        family="fusion",
        stream="pose",
        variant=Variant.State,
        schema=_pose_schema(),
    )
    staged_accel = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="fusion",
        stream="staged_accel",
        variant=Variant.Meta,
        schema=int_schema("Accel"),
    )
    staged_gyro = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu",
        family="fusion",
        stream="staged_gyro",
        variant=Variant.Meta,
        schema=int_schema("Gyro"),
    )

    fused_pairs: list[tuple[int, int]] = []

    def on_pair(pair: tuple[int, int]) -> None:
        fused_pairs.append(pair)
        graph.publish(pose, pair)

    graph.capacitor(
        source=accel,
        sink=staged_accel,
        capacity=2,
        immediate=True,
    )
    graph.capacitor(
        source=gyro,
        sink=staged_gyro,
        capacity=2,
        immediate=True,
    )
    subscription = graph.interval_join(
        staged_accel,
        staged_gyro,
        within=1,
        combine=lambda accel_value, gyro_value: (accel_value, gyro_value),
    ).subscribe(on_pair)

    graph.publish(accel, 100)
    graph.publish(gyro, 7)
    graph.publish(accel, 101)
    graph.publish(gyro, 8)
    subscription.dispose()

    latest_pose = graph.latest(pose)
    return {
        "fused_pairs": tuple(fused_pairs),
        "latest_pose": None if latest_pose is None else latest_pose.value,
    }


if __name__ == "__main__":
    print(run_example())
