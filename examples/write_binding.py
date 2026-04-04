from __future__ import annotations

from manyfold import Graph
from manyfold import OwnerName
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold import WriteBindings


def run_example() -> dict[str, bytes]:
    """Publish a write request and inspect the desired shadow route."""

    graph = Graph()
    binding = WriteBindings.logical(
        owner=OwnerName("counter"),
        family=StreamFamily("counter"),
        stream=StreamName("value"),
        schema=Schema.bytes("CounterValue"),
    )

    graph.publish(binding, b"42")
    desired = graph.latest(binding.desired)

    assert desired is not None
    return {
        "request_payload": b"42",
        "desired_payload": bytes(desired.payload_ref.inline_bytes),
    }


if __name__ == "__main__":
    print(run_example())
