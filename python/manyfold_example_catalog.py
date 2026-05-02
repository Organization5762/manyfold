"""Repository-local entrypoint for validating and syncing the example catalog."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

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
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    ensure_repo_import_paths = module.ensure_repo_import_paths

ensure_repo_import_paths()


def main(argv: list[str] | None = None) -> int:
    ensure_repo_import_paths()

    from examples.catalog import main as catalog_main

    return catalog_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
