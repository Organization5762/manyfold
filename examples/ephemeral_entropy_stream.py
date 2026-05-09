from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, Layer, Plane, Schema, TaintDomain, Variant

from ._shared import example_route


def run_example() -> EphemeralEntropyStreamExampleResult:
    graph = Graph()
    entropy = example_route(
        plane=Plane.Read,
        layer=Layer.Ephemeral,
        owner="session",
        family="trace",
        stream="entropy",
        variant=Variant.Event,
        schema=Schema.bytes(name="EntropyBytes"),
    )

    graph.publish(entropy, b"nonce-1")
    latest = graph.latest(entropy)
    assert latest is not None
    descriptor = graph.describe_route(entropy)
    replay = tuple(graph.replay(entropy))

    determinism_taints = tuple(
        taint.value_id
        for taint in latest.closed.taints
        if taint.domain == TaintDomain.Determinism
    )

    return {
        "latest_payload": latest.value,
        "replay_count": len(replay),
        "determinism_taints": determinism_taints,
        "latest_replay_policy": descriptor.retention.latest_replay_policy,
    }


class EphemeralEntropyStreamExampleResult(TypedDict):
    latest_payload: bytes
    replay_count: int
    determinism_taints: tuple[str, ...]
    latest_replay_policy: str


if __name__ == "__main__":
    print(run_example())
