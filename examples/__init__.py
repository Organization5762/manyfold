"""Supported and archived executable examples for the Manyfold Python API."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from ._exports import CATALOG_EXPORTS

_EXPORT_CACHE: dict[str, Any] = {}

__all__ = list(CATALOG_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name in _EXPORT_CACHE:
        value = _EXPORT_CACHE[name]
    else:
        value = getattr(import_module("examples._catalog"), name)
        _EXPORT_CACHE[name] = value
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted((*globals(), *__all__))
