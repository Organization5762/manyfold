from __future__ import annotations

from typing import TypedDict

from manyfold import Graph
from manyfold import OwnerName
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold.embedded import EmbeddedDeviceProfile


class LazyLidarPayloadExampleResult(TypedDict):
    selected_frame: bytes
    metadata_count: int
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

    frames = ("frame-1:drop", "frame-2:open")
    selected_payload = b"frame-2-points"
    for frame in frames:
        graph.publish(lidar.metadata_route, frame)
        if frame.endswith(":open"):
            graph.publish(lidar.payload_route, selected_payload)

    latest_payload = graph.latest(lidar.payload_route)
    assert latest_payload is not None

    return {
        "selected_frame": latest_payload.value,
        "metadata_count": len(frames),
        "profile_issues": lidar.validate(),
    }


if __name__ == "__main__":
    print(run_example())

