"""Graph-visible statistics helpers."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

__all__ = ("Average",)


@dataclass(frozen=True)
class Average:
    """Moving average over the latest `window_size` values, or all available values."""

    window_size: int

    def __post_init__(self) -> None:
        if not isinstance(self.window_size, int) or isinstance(self.window_size, bool):
            raise ValueError("average window size must be an integer")
        if self.window_size <= 0:
            raise ValueError("average window size must be positive")

    def __call__(self, values: Sequence[float]) -> float:
        value_count = len(values)
        if value_count == 0:
            raise ValueError("average requires at least one value")
        if self.window_size == 1:
            return _require_finite_number(values[value_count - 1])
        start = max(0, value_count - self.window_size)
        window_count = value_count - start
        # fsum keeps cancellation-heavy windows deterministic without copying
        # the sequence; callers may provide index-only buffers.
        return (
            math.fsum(
                _require_finite_number(values[index])
                for index in range(start, value_count)
            )
            / window_count
        )


def _require_finite_number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("average values must be finite numbers")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("average values must be finite numbers")
    return number
