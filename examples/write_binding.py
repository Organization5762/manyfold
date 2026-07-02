from __future__ import annotations

from manyfold import Graph, OwnerName, Schema, StreamFamily, StreamName, WriteBindings


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
    desired = graph.latest(binding.desired)
    desired_payload = graph.open_payload(desired)

    assert desired is not None
    assert desired_payload is not None
    return {
        "request_payload": b"42",
        "desired_payload": desired_payload,
    }


if __name__ == "__main__":
    print(run_example())
