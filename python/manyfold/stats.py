"""Graph-visible statistics helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class Average:
    """Moving average over the latest `window_size` values."""

    window_size: int

    def __post_init__(self) -> None:
        if self.window_size <= 0:
            raise ValueError("average window size must be positive")

    def __call__(self, values: Sequence[float]) -> float:
        value_count = len(values)
        if value_count == 0:
            raise ValueError("average requires at least one value")
        start = max(0, value_count - self.window_size)
        window_count = value_count - start
        return sum(values[index] for index in range(start, value_count)) / window_count
