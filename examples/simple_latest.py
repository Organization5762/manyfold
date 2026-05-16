from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, Schema, route

from examples._shared import require_latest


def run_example() -> SimpleLatestExampleResult:
    """Publish changing state, then read back the current value."""
    graph = Graph()
    temperature = route(
        owner="sensor",
        family="environment",
        stream="temperature",
        schema=Schema.bytes(name="Temperature"),
    )

    graph.publish(temperature, b"72.4F")
    graph.publish(temperature, b"72.9F")
    latest = require_latest(graph, temperature, "simple_latest")

    return {
        "latest_payload": latest.value,
        "latest_seq": latest.closed.seq_source,
    }


class SimpleLatestExampleResult(TypedDict):
    latest_payload: bytes
    latest_seq: int


if __name__ == "__main__":
    print(run_example())
