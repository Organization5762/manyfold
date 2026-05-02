from __future__ import annotations

from typing import TypedDict

from manyfold import Graph
from manyfold import OwnerName
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold import EmbeddedDeviceProfile
from manyfold import Layer
from manyfold import Plane
from manyfold import Variant
from manyfold.graph import LazyPayloadSource

from ._shared import sibling_route


class LazyLidarPayloadExampleResult(TypedDict):
    selected_frame: bytes
    metadata_count: int
    matched_frames: tuple[str, ...]
    payload_open_requests: int
    lazy_source_opens: int
    unopened_lazy_payloads: int
    profile_issues: tuple[str, ...]


def _frame_schema() -> Schema[str]:
    return Schema(
        schema_id="LidarFrameMeta",
        version=1,
        encode=lambda value: value.encode("ascii"),
        decode=lambda payload: payload.decode("ascii"),
    )


def run_example() -> LazyLidarPayloadExampleResult:
    graph = Graph()
    profile = EmbeddedDeviceProfile()
    lidar = profile.bulk_sensor(
        owner=OwnerName("lidar"),
        family=StreamFamily("scan"),
        metadata_stream=StreamName("frame_meta"),
        metadata_schema=_frame_schema(),
        payload_stream=StreamName("frame_payload"),
        payload_schema=Schema.bytes("LidarFramePayload"),
    )

    selected_metadata = sibling_route(
        lidar.metadata_route,
        plane=Plane.Read,
        layer=Layer.Logical,
        owner="lidar",
        stream="selected_frame_meta",
        variant=Variant.Meta,
    )
    matched_frames: list[str] = []
    selected_payloads: list[bytes] = []
    graph.resistor(
        source=lidar.metadata_route,
        sink=selected_metadata,
        gate=lambda frame: frame.endswith(":open"),
    )
    graph.observe(selected_metadata, replay_latest=False).subscribe(
        lambda envelope: (
            matched_frames.append(envelope.value),
            selected_payloads.append(graph.open_payload(lidar.payload_route) or b""),
        )
    )

    frames_with_payloads = (
        ("frame-1:drop", b"frame-1-points"),
        ("frame-2:open", b"frame-2-points"),
    )
    for frame, payload in frames_with_payloads:
        graph.publish_lazy(
            lidar.payload_route,
            LazyPayloadSource(lambda payload=payload: payload),
        )
        graph.publish(lidar.metadata_route, frame)

    payload_snapshot = graph.payload_demand_snapshot(lidar.payload_route)

    return {
        "selected_frame": selected_payloads[-1],
        "metadata_count": len(frames_with_payloads),
        "matched_frames": tuple(matched_frames),
        "payload_open_requests": payload_snapshot.payload_open_requests,
        "lazy_source_opens": payload_snapshot.lazy_source_opens,
        "unopened_lazy_payloads": payload_snapshot.unopened_lazy_payloads,
        "profile_issues": lidar.validate(),
    }


if __name__ == "__main__":
    print(run_example())
