"""Repository-local entrypoint for validating and syncing the example catalog."""

from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve()
PYTHON_DIR = MODULE_PATH.parent
REPO_ROOT = PYTHON_DIR.parent
REPO_PATHS_MODULE_PATH = PYTHON_DIR / "manyfold" / "_repo_paths.py"

try:
    from manyfold._repo_paths import ensure_repo_import_paths
except ImportError:
    spec = importlib.util.spec_from_file_location(
        "_manyfold_repo_paths",
        REPO_PATHS_MODULE_PATH,
    )
    if spec is None or spec.loader is None:
        raise ImportError(
            f"cannot load repository path helpers from {REPO_PATHS_MODULE_PATH}"
        )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    ensure_repo_import_paths = module.ensure_repo_import_paths

ensure_repo_import_paths()

_catalog_main = importlib.import_module("examples.catalog")._main


def _main(argv: list[str] | None = None) -> int:
    ensure_repo_import_paths()

    return _catalog_main(argv)


if __name__ == "__main__":
    raise SystemExit(_main())
