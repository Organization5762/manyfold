from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, Schema, route


class AverageTemperatureExampleResult(TypedDict):
    latest_average: float
    latest_seq: int
    moving_average_node: str
    moving_average_inputs: tuple[str, ...]
    moving_average_outputs: tuple[str, ...]
    moving_average_storage: str
    moving_average_window_size: int


def run_example() -> AverageTemperatureExampleResult:
    """Derive average temperature values from incoming temperature samples."""
    graph = Graph()
    temperature = route(
        owner="sensor",
        family="environment",
        stream="temperature",
        schema=Schema.float(name="Temperature"),
    )
    average_temperature = temperature.derivative_route(
        stream="average_temperature",
        schema=Schema.float(name="AverageTemperature"),
    )

    subscription = graph.observe(temperature, replay_latest=False).moving_average(
        window_size=3
    ).connect(average_temperature)
    graph.publish(temperature, 72.4)
    graph.publish(temperature, 72.9)
    graph.publish(temperature, 73.7)

    latest = graph.latest(average_temperature)
    assert latest is not None
    node = next(
        node
        for node in graph.diagram_nodes()
        if dict(node.metadata).get("statistic") == "moving_average"
    )
    metadata = dict(node.metadata)
    subscription.dispose()

    return {
        "latest_average": latest.value,
        "latest_seq": latest.closed.seq_source,
        "moving_average_node": node.name,
        "moving_average_inputs": node.input_routes,
        "moving_average_outputs": node.output_routes,
        "moving_average_storage": metadata["storage"],
        "moving_average_window_size": int(metadata["window_size"]),
    }


if __name__ == "__main__":
    print(run_example())
