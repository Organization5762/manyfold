"""Public module entrypoint for validating and syncing the example catalog."""

from __future__ import annotations

from ._catalog import main

__all__ = ("main",)


if __name__ == "__main__":
    raise SystemExit(main())
