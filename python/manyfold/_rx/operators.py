"""Rx operators re-exported through Manyfold."""

from reactivex.operators import *  # noqa: F403
from reactivex.operators import __all__ as _REACTIVEX_OPERATORS_ALL

# Follow RxPy's declared surface instead of whatever helper names imports leave
# behind in this module.
__all__ = tuple(sorted(set(_REACTIVEX_OPERATORS_ALL)))
