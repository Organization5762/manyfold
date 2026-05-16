from __future__ import annotations

from manyfold import Graph, OwnerName, Schema, StreamFamily, StreamName, WriteBindings

from examples._shared import require_latest


def run_example() -> dict[str, bytes]:
    """Publish a write request and inspect the desired shadow route."""

    graph = Graph()
    binding = WriteBindings.logical(
        owner=OwnerName("counter"),
        family=StreamFamily("counter"),
        stream=StreamName("value"),
        schema=Schema.bytes(name="CounterValue"),
    )

    graph.publish(binding, b"42")
    desired = require_latest(graph, binding.desired, "write_binding desired route")

    return {
        "request_payload": b"42",
        "desired_payload": bytes(desired.payload_ref.inline_bytes),
    }


if __name__ == "__main__":
    print(run_example())
