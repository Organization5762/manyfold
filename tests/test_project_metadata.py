from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CARGO_TOML_PATH = PROJECT_ROOT / "Cargo.toml"
NATIVE_STUB_PATH = PROJECT_ROOT / "python" / "manyfold" / "_manyfold_rust" / "__init__.pyi"
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
PYTHON_SOURCE_ROOTS = (
    PROJECT_ROOT / "examples",
    PROJECT_ROOT / "python",
    PROJECT_ROOT / "tests",
)
PACKAGE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+")


class ProjectMetadataTests(unittest.TestCase):
    def test_python_imports_stay_at_module_scope(self) -> None:
        violations = tuple(
            violation
            for path in _python_source_paths()
            for violation in _function_local_imports(path)
        )

        self.assertEqual(violations, ())

    def test_cargo_dependency_tables_stay_sorted(self) -> None:
        lines = CARGO_TOML_PATH.read_text(encoding="utf-8").splitlines()

        dependencies = _section_keys(lines, "dependencies")
        dev_dependencies = _section_keys(lines, "dev-dependencies")

        self.assertEqual(dependencies, tuple(sorted(dependencies)))
        self.assertEqual(dev_dependencies, tuple(sorted(dev_dependencies)))

    def test_pyproject_metadata_lists_stay_sorted(self) -> None:
        lines = PYPROJECT_PATH.read_text(encoding="utf-8").splitlines()

        keywords = _array_values(lines, "keywords")
        classifiers = _array_values(lines, "classifiers")
        dependencies = _array_values(lines, "dependencies")
        dev_dependencies = _array_values(lines, "dev")
        script_names = _section_keys(lines, "project.scripts")
        url_names = _section_keys(lines, "project.urls")

        self.assertEqual(keywords, tuple(sorted(keywords)))
        self.assertEqual(classifiers, tuple(sorted(classifiers)))
        self.assertEqual(
            dependencies,
            tuple(sorted(dependencies, key=_dependency_name)),
        )
        self.assertEqual(
            dev_dependencies,
            tuple(sorted(dev_dependencies, key=_dependency_name)),
        )
        self.assertEqual(script_names, tuple(sorted(script_names)))
        self.assertEqual(url_names, tuple(sorted(url_names)))

    def test_native_stub_exports_stay_sorted(self) -> None:
        exports = _module_all_assignment(NATIVE_STUB_PATH)

        self.assertEqual(exports, tuple(sorted(exports)))
        self.assertEqual(len(exports), len(set(exports)))


def _array_values(lines: list[str], key: str) -> tuple[str, ...]:
    values: list[str] = []
    in_array = False
    prefix = f"{key} = ["
    for line in lines:
        stripped = line.strip()
        if not in_array:
            in_array = stripped == prefix
            continue
        if stripped == "]":
            return tuple(values)
        values.append(stripped.rstrip(",").strip('"'))
    raise AssertionError(f"pyproject.toml does not define array {key!r}")


def _dependency_name(requirement: str) -> str:
    match = PACKAGE_NAME_RE.match(requirement)
    if match is None:
        raise AssertionError(f"cannot parse dependency name from {requirement!r}")
    return match.group(0).lower()


def _section_keys(lines: list[str], section: str) -> tuple[str, ...]:
    keys: list[str] = []
    in_section = False
    header = f"[{section}]"
    for line in lines:
        stripped = line.strip()
        if stripped == header:
            in_section = True
            continue
        if in_section and stripped.startswith("["):
            return tuple(keys)
        if in_section and stripped and not stripped.startswith("#"):
            keys.append(stripped.split("=", 1)[0].strip())
    if not in_section:
        raise AssertionError(f"pyproject.toml does not define section {section!r}")
    return tuple(keys)


def _module_all_assignment(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets):
            continue
        if not isinstance(node.value, ast.List | ast.Tuple):
            raise AssertionError(f"{path} __all__ must be a literal list or tuple")
        exports: list[str] = []
        for item in node.value.elts:
            if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
                raise AssertionError(f"{path} __all__ must contain only string literals")
            exports.append(item.value)
        return tuple(exports)
    raise AssertionError(f"{path} does not define __all__")


def _python_source_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(path for root in PYTHON_SOURCE_ROOTS for path in root.rglob("*.py"))
    )


def _function_local_imports(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent

    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Import | ast.ImportFrom):
            continue
        parent = parents.get(node)
        while parent is not None:
            if isinstance(parent, ast.FunctionDef | ast.AsyncFunctionDef):
                relative_path = path.relative_to(PROJECT_ROOT)
                violations.append(f"{relative_path}:{node.lineno}")
                break
            parent = parents.get(parent)
    return tuple(violations)
