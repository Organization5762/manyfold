"""Rust-backed SQL stream processing primitives for application architecture."""

from __future__ import annotations

from manyfold._manyfold_rust import (
    DataStreamProcessor as DataStreamProcessor,
    DataStreamRecord as DataStreamRecord,
)

__all__ = [
    "DataStreamProcessor",
    "DataStreamRecord",
]
