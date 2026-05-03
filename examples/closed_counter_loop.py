from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, OwnerName, Schema, StreamFamily, StreamName, WriteBindings


class ClosedCounterLoopExampleResult(TypedDict):
    desired_latest: bytes
    effective_latest: bytes
    ack_latest: bytes
    coherence_taints: tuple[str, ...]


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

    graph.publish(binding, b"2")
    shadow = graph.reconcile_write_binding(
        binding,
        reported=b"2",
        effective=b"2",
        ack=b"ok",
    )

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
        "coherence_taints": shadow.coherence_taints,
    }


if __name__ == "__main__":
    print(run_example())
