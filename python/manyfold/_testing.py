"""Private testing hooks for Manyfold's Python runtime."""

from __future__ import annotations

from .graph import _reset_reactive_threading_state_for_tests

__all__ = ["reset_reactive_threading_state"]


def reset_reactive_threading_state() -> None:
    """Reset process-local reactive runtime state between tests."""

    _reset_reactive_threading_state_for_tests()
