from __future__ import annotations

from typing import TypedDict

from manyfold import Graph, Schema, route


class AverageTemperatureExampleResult(TypedDict):
    samples: tuple[bytes, ...]
    averages: tuple[bytes, ...]
    latest_average: bytes
    latest_seq: int


def _fahrenheit(payload: bytes) -> float:
    return float(payload.decode("ascii").removesuffix("F"))


def _average_payload(values: list[float]) -> bytes:
    return f"{sum(values) / len(values):.1f}F".encode("ascii")


def run_example() -> AverageTemperatureExampleResult:
    """Derive average temperature values from incoming temperature samples."""
    graph = Graph()
    temperature = route(
        owner="sensor",
        family="environment",
        stream="temperature",
        schema=Schema.bytes(name="Temperature"),
    )
    average_temperature = route(
        owner="sensor",
        family="environment",
        stream="average_temperature",
        schema=Schema.bytes(name="AverageTemperature"),
    )

    samples: list[bytes] = []
    averages: list[bytes] = []

    def publish_average(envelope) -> None:
        samples.append(envelope.value)
        average = _average_payload([_fahrenheit(sample) for sample in samples])
        averages.append(average)
        graph.publish(average_temperature, average)

    subscription = graph.observe(temperature, replay_latest=False).subscribe(
        publish_average
    )
    graph.publish(temperature, b"72.4F")
    graph.publish(temperature, b"72.9F")
    graph.publish(temperature, b"73.7F")
    subscription.dispose()

    latest = graph.latest(average_temperature)
    assert latest is not None

    return {
        "samples": tuple(samples),
        "averages": tuple(averages),
        "latest_average": latest.value,
        "latest_seq": latest.closed.seq_source,
    }


if __name__ == "__main__":
    print(run_example())
