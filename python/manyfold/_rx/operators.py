"""Rx operators re-exported through Manyfold."""

import reactivex.operators as _operators

__all__ = tuple(sorted(set(_operators.__all__)))

globals().update((name, getattr(_operators, name)) for name in __all__)
del _operators
