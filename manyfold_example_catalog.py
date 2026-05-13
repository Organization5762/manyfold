"""Repository-root wrapper for the example catalog module entrypoint."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

MODULE_PATH = Path(__file__).resolve()
REPO_ROOT = MODULE_PATH.parent
PYTHON_DIR = REPO_ROOT / "python"
IMPL_PATH = PYTHON_DIR / "manyfold_example_catalog.py"
_IMPL_MODULE_NAME = "_manyfold_example_catalog_impl"
_MISSING_MODULE = object()

_SPEC = importlib.util.spec_from_file_location(
    _IMPL_MODULE_NAME,
    IMPL_PATH,
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_PREVIOUS_MODULE = sys.modules.get(_IMPL_MODULE_NAME, _MISSING_MODULE)
sys.modules[_IMPL_MODULE_NAME] = _MODULE
try:
    _SPEC.loader.exec_module(_MODULE)
except BaseException:
    if _PREVIOUS_MODULE is _MISSING_MODULE:
        sys.modules.pop(_IMPL_MODULE_NAME, None)
    else:
        sys.modules[_IMPL_MODULE_NAME] = _PREVIOUS_MODULE
    raise

main = _MODULE.main

__all__ = ("main",)


if __name__ == "__main__":
    raise SystemExit(main())
