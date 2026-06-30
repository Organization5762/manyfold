"""Native Manyfold lock primitives."""

from __future__ import annotations

from manyfold._manyfold_rust import (
    ManyFoldLock as ManyFoldLock,
    ManyFoldLockLease as ManyFoldLockLease,
)

__all__ = [
    "ManyFoldLock",
    "ManyFoldLockLease",
]
