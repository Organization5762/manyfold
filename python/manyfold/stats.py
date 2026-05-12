"""Graph-visible statistics helpers."""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass

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

    def __call__(self, values: Iterable[float]) -> float:
        """Return the average of the latest window from a sequence or iterable."""
        try:
            value_count = len(values)  # type: ignore[arg-type]
            value_at = values.__getitem__  # type: ignore[attr-defined]
        except (AttributeError, TypeError):
            window = deque(values, maxlen=self.window_size)
            if not window:
                raise ValueError("average requires at least one value")
            return _average_values(window, len(window))

        if value_count == 0:
            raise ValueError("average requires at least one value")
        start = max(0, value_count - self.window_size)
        window_count = value_count - start
        return _average_values(
            (value_at(index) for index in range(start, value_count)),
            window_count,
        )


def _average_values(values: Iterable[object], count: int) -> float:
    # fsum keeps cancellation-heavy windows deterministic. The sequence path
    # avoids copying, while the iterable path retains only the active window.
    return math.fsum(_require_finite_number(value) for value in values) / count


def _require_finite_number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("average values must be finite numbers")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("average values must be finite numbers")
    return number
