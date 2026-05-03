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
        if not values:
            raise ValueError("average requires at least one value")
        return sum(values) / len(values)
