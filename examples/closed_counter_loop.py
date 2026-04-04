from __future__ import annotations

from typing import TypedDict

from manyfold import Graph
from manyfold import OwnerName
from manyfold import Schema
from manyfold import StreamFamily
from manyfold import StreamName
from manyfold import WriteBindings


class ClosedCounterLoopExampleResult(TypedDict):
    desired_latest: bytes
    effective_latest: bytes
    ack_latest: bytes


def run_example() -> ClosedCounterLoopExampleResult:
    graph = Graph()
    binding = WriteBindings.logical(
        owner=OwnerName("counter"),
        family=StreamFamily("loop"),
        stream=StreamName("count"),
        schema=Schema.bytes("CounterValue"),
    )
    graph.register_port(binding.request)
    graph.register_port(binding.desired)
    graph.register_port(binding.reported)
    graph.register_port(binding.effective)
    assert binding.ack is not None
    graph.register_port(binding.ack)

    graph.publish(binding.request, b"2")
    graph.publish(binding.desired, b"2")
    graph.publish(binding.reported, b"2")
    graph.publish(binding.effective, b"2")
    graph.publish(binding.ack, b"ok")

    desired_latest = graph.latest(binding.desired)
    effective_latest = graph.latest(binding.effective)
    ack_latest = graph.latest(binding.ack)
    assert desired_latest is not None
    assert effective_latest is not None
    assert ack_latest is not None

    return {
        "desired_latest": desired_latest.payload_ref.inline_bytes,
        "effective_latest": effective_latest.payload_ref.inline_bytes,
        "ack_latest": ack_latest.payload_ref.inline_bytes,
    }


if __name__ == "__main__":
    print(run_example())

