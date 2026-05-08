"""Helpers for repo-local modules that need stable local import paths."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

PACKAGE_DIR = Path(__file__).resolve().parent
PYTHON_DIR = PACKAGE_DIR.parent
REPO_ROOT = PYTHON_DIR.parent


def ensure_path_on_sys_path(path: Path) -> None:
    """Prepend one path to ``sys.path`` exactly once."""

    path_str = _canonical_path_str(path)
    sys.path[:] = [
        existing_path
        for existing_path in sys.path
        if _canonical_path_str(Path(existing_path)) != path_str
    ]
    sys.path.insert(0, path_str)


def ensure_package_dir_on_path() -> None:
    """Allow importing repo-local package modules before editable install exists."""

    ensure_path_on_sys_path(PYTHON_DIR)


def ensure_repo_root_on_path() -> None:
    """Allow repo-local helper modules such as `examples` to be imported."""

    ensure_path_on_sys_path(REPO_ROOT)


def ensure_repo_import_paths() -> None:
    """Install both package and repository roots for local helper imports."""

    ensure_package_dir_on_path()
    ensure_repo_root_on_path()


def load_module_from_path(module_name: str, module_path: Path) -> ModuleType:
    """Load and execute a Python module from an explicit filesystem path.

    The loaded module is registered in ``sys.modules`` during execution so
    normal import-time introspection, such as dataclass type resolution, works.
    """

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to load module {module_name!r} from {module_path}")
    previous_module = sys.modules.get(module_name)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        if previous_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous_module
        raise
    return module


def _canonical_path_str(path: Path) -> str:
    return str(path.expanduser().resolve())
