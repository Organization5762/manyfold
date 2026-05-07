"""Repository-root wrapper for the example catalog module entrypoint."""

from __future__ import annotations

import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve()
REPO_ROOT = MODULE_PATH.parent
PYTHON_DIR = REPO_ROOT / "python"
IMPL_PATH = PYTHON_DIR / "manyfold_example_catalog.py"

_SPEC = importlib.util.spec_from_file_location(
    "_manyfold_example_catalog_impl",
    IMPL_PATH,
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

main = _MODULE.main

__all__ = ("main",)


if __name__ == "__main__":
    raise SystemExit(main())
