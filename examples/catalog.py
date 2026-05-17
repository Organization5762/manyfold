"""Public module entrypoint for validating and syncing the example catalog."""

from __future__ import annotations

from ._catalog import _main

__all__ = ()

if __name__ == "__main__":
    raise SystemExit(_main())
