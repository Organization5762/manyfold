from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, JoinInput, Layer, Plane, Schema, Variant

from ._shared import example_route


def run_example() -> CrossPartitionJoinExampleResult:
    graph = Graph()
    accel = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu_left",
        family="sensor",
        stream="accel",
        variant=Variant.Meta,
        schema=Schema.bytes(name="AccelFrame"),
    )
    gyro = example_route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="imu_right",
        family="sensor",
        stream="gyro",
        variant=Variant.Meta,
        schema=Schema.bytes(name="GyroFrame"),
    )

    plan = graph.plan_join(
        "imu_fusion",
        JoinInput(accel, partition_key_semantics="device_id"),
        JoinInput(gyro, partition_key_semantics="axis_id", deterministic_rekey=True),
    )

    return {
        "join_class": plan.join_class,
        "visible_nodes": tuple(node.display() for node in plan.visible_nodes),
        "topology_edges": tuple(graph.topology()),
        "taint_implications": plan.taint_implications,
    }


class CrossPartitionJoinExampleResult(TypedDict):
    join_class: str
    visible_nodes: tuple[str, ...]
    topology_edges: tuple[tuple[str, str], ...]
    taint_implications: tuple[str, ...]


if __name__ == "__main__":
    print(run_example())
