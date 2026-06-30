from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict

from manyfold.architecture import PubSub


def run_example() -> SimpleLatestExampleResult:
    """Publish changing state, then read back the current value with SQL."""
    temperature = PubSub()

    temperature.publish(Temperature(degrees=72.4, unit="F"))
    temperature.publish(Temperature(degrees=72.9, unit="F"))
    latest_row = temperature.query_one(
        """
        SELECT pad_name, offset + 1 AS seq_source, degrees, unit, payload
        FROM stream
        ORDER BY event_time DESC, process_sequence DESC
        LIMIT 1
        """
    )
    latest = temperature.latest()
    if latest_row is None or latest is None:
        raise RuntimeError("temperature stream did not contain any rows")

    return {
        "latest_payload": latest_row["payload"],
        "latest_seq": latest_row["seq_source"],
        "latest_degrees": latest.degrees,
        "latest_unit": latest.unit,
        "average_degrees": temperature.average(field="degrees"),
    }


@dataclass(frozen=True)
class Temperature:
    degrees: float
    unit: str


class SimpleLatestExampleResult(TypedDict):
    latest_payload: bytes
    latest_seq: int
    latest_degrees: float
    latest_unit: str
    average_degrees: float | None


if __name__ == "__main__":
    print(run_example())
