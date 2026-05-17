from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CARGO_TOML_PATH = PROJECT_ROOT / "Cargo.toml"
NATIVE_STUB_PATH = (
    PROJECT_ROOT / "python" / "manyfold" / "_manyfold_rust" / "__init__.pyi"
)
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
PYTHON_SOURCE_ROOTS = (
    PROJECT_ROOT / "examples",
    PROJECT_ROOT / "python",
    PROJECT_ROOT / "tests",
)
RUNTIME_ASSERT_ROOTS = (
    PROJECT_ROOT / "python" / "manyfold",
    PROJECT_ROOT / "python" / "manyfold_example_catalog.py",
)
PACKAGE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+")
TYPE_ALIAS_VALUE_NAMES = frozenset(
    {
        "Any",
        "Callable",
        "Iterator",
        "Literal",
        "Mapping",
        "Protocol",
        "Sequence",
        "TypeAlias",
        "TypeVar",
        "Union",
    }
)
DECLARATION_ORDER = {
    "public function": 0,
    "public class": 1,
    "private function": 2,
    "private class": 3,
}
DECLARATION_ORDER_EXCEPTIONS = frozenset(
    {
        ("python/manyfold/graph.py", "_ThreadPlaceableNode"),
        ("python/manyfold/reactive_threads.py", "_NoStartingValue"),
    }
)


class ProjectMetadataTests(unittest.TestCase):
    def test_python_imports_stay_at_module_scope(self) -> None:
        violations = tuple(
            violation
            for path in _python_source_paths()
            for violation in _function_local_imports(path)
        )

        self.assertEqual(violations, ())

    def test_manyfold_runtime_uses_explicit_invariant_errors(self) -> None:
        violations = tuple(
            violation
            for path in _runtime_python_source_paths()
            for violation in _assert_statements(path)
        )

        self.assertEqual(violations, ())

    def test_cargo_dependency_tables_stay_sorted(self) -> None:
        cargo = _toml_document(CARGO_TOML_PATH)

        dependencies = tuple(cargo["dependencies"])
        dev_dependencies = tuple(cargo["dev-dependencies"])

        self.assertEqual(dependencies, tuple(sorted(dependencies)))
        self.assertEqual(dev_dependencies, tuple(sorted(dev_dependencies)))

    def test_cargo_feature_dependency_lists_stay_sorted(self) -> None:
        features = _toml_document(CARGO_TOML_PATH)["features"]

        for name, values in features.items():
            with self.subTest(feature=name):
                values = tuple(values)
                self.assertEqual(values, tuple(sorted(values)))
                self.assertEqual(len(values), len(set(values)))

    def test_pyproject_metadata_lists_stay_sorted(self) -> None:
        pyproject = _toml_document(PYPROJECT_PATH)
        project = pyproject["project"]

        keywords = tuple(project["keywords"])
        classifiers = tuple(project["classifiers"])
        dependencies = tuple(project["dependencies"])
        dev_dependencies = tuple(pyproject["dependency-groups"]["dev"])
        script_names = tuple(project["scripts"])
        url_names = tuple(project["urls"])

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

    def test_literal_module_exports_stay_sorted(self) -> None:
        failures: list[str] = []
        for path in _python_source_paths():
            exports = _literal_module_all_assignment(path)
            if exports is None:
                continue
            if exports != tuple(sorted(exports)):
                failures.append(f"{path.relative_to(PROJECT_ROOT)} __all__ is unsorted")
            if len(exports) != len(set(exports)):
                failures.append(
                    f"{path.relative_to(PROJECT_ROOT)} __all__ contains duplicates"
                )

        self.assertEqual(failures, [])

    def test_python_declaration_order_matches_repo_contract(self) -> None:
        violations = tuple(
            violation
            for path in _python_source_paths()
            for violation in _declaration_order_issues(path)
        )

        self.assertEqual(violations, ())


def _dependency_name(requirement: str) -> str:
    match = PACKAGE_NAME_RE.match(requirement)
    if match is None:
        raise AssertionError(f"cannot parse dependency name from {requirement!r}")
    return match.group(0).lower()


def _toml_document(path: Path) -> dict[str, object]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _module_all_assignment(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not _assigns_name(node, "__all__"):
            continue
        if not isinstance(node.value, ast.List | ast.Tuple):
            raise AssertionError(f"{path} __all__ must be a literal list or tuple")
        exports = _literal_string_sequence(node.value)
        if exports is None:
            raise AssertionError(f"{path} __all__ must contain only string literals")
        return exports
    raise AssertionError(f"{path} does not define __all__")


def _literal_module_all_assignment(path: Path) -> tuple[str, ...] | None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not _assigns_name(node, "__all__"):
            continue
        return _literal_string_sequence(node.value)
    return None


def _assigns_name(node: ast.Assign, name: str) -> bool:
    return any(
        isinstance(target, ast.Name) and target.id == name for target in node.targets
    )


def _literal_string_sequence(node: ast.expr) -> tuple[str, ...] | None:
    if not isinstance(node, ast.List | ast.Tuple):
        return None
    values: list[str] = []
    for item in node.elts:
        if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
            return None
        values.append(item.value)
    return tuple(values)


def _declaration_order_issues(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return (
        *_public_main_function_issues(path, tree),
        *_preamble_type_alias_order_issues(path, tree),
        *_module_metadata_order_issues(path, tree),
        *_top_level_declaration_order_issues(path, tree),
        *_class_method_order_issues(path, tree),
    )


def _public_main_function_issues(path: Path, tree: ast.Module) -> tuple[str, ...]:
    return tuple(
        f"{path.relative_to(PROJECT_ROOT)}:{node.lineno} use _main for entrypoints"
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and node.name == "main"
    )


def _preamble_type_alias_order_issues(path: Path, tree: ast.Module) -> tuple[str, ...]:
    issues: list[str] = []
    saw_constant = False
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            continue
        if isinstance(node, ast.Import | ast.ImportFrom):
            continue
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            break
        if not isinstance(node, ast.Assign | ast.AnnAssign):
            continue
        if _is_type_alias_assignment(node):
            if saw_constant:
                issues.append(
                    f"{path.relative_to(PROJECT_ROOT)}:{node.lineno} "
                    "type alias must precede preamble constants"
                )
            continue
        saw_constant = True
    return tuple(issues)


def _module_metadata_order_issues(path: Path, tree: ast.Module) -> tuple[str, ...]:
    declaration_lines = tuple(
        node.lineno
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
    )
    if not declaration_lines:
        return ()
    last_declaration_line = max(declaration_lines)
    return tuple(
        f"{path.relative_to(PROJECT_ROOT)}:{node.lineno} __all__ belongs after declarations"
        for node in tree.body
        if isinstance(node, ast.Assign)
        and _assigns_name(node, "__all__")
        and node.lineno < last_declaration_line
    )


def _top_level_declaration_order_issues(
    path: Path, tree: ast.Module
) -> tuple[str, ...]:
    issues: list[str] = []
    max_seen_order = -1
    max_seen_kind = ""
    for node in tree.body:
        kind = _declaration_kind(node)
        if kind is None:
            continue
        relative_path = str(path.relative_to(PROJECT_ROOT))
        if (relative_path, node.name) in DECLARATION_ORDER_EXCEPTIONS:
            continue
        order = DECLARATION_ORDER[kind]
        if order < max_seen_order:
            issues.append(
                f"{relative_path}:{node.lineno} {kind} {node.name} "
                f"follows {max_seen_kind}"
            )
        if order > max_seen_order:
            max_seen_order = order
            max_seen_kind = kind
    return tuple(issues)


def _declaration_kind(node: ast.stmt) -> str | None:
    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
        return "private function" if node.name.startswith("_") else "public function"
    if isinstance(node, ast.ClassDef):
        return "private class" if node.name.startswith("_") else "public class"
    return None


def _class_method_order_issues(path: Path, tree: ast.Module) -> tuple[str, ...]:
    issues: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        saw_private = False
        for item in node.body:
            if not isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if item.name.startswith("__") and item.name.endswith("__"):
                continue
            if item.name.startswith("_"):
                saw_private = True
                continue
            if saw_private:
                issues.append(
                    f"{path.relative_to(PROJECT_ROOT)}:{item.lineno} "
                    f"{node.name}.{item.name} public method follows private method"
                )
    return tuple(issues)


def _is_type_alias_assignment(node: ast.Assign | ast.AnnAssign) -> bool:
    target = node.target if isinstance(node, ast.AnnAssign) else node.targets[0]
    if not isinstance(target, ast.Name):
        return False
    if isinstance(node, ast.AnnAssign) and _annotation_mentions_type_alias(
        node.annotation
    ):
        return True
    if node.value is None:
        return False
    if isinstance(node.value, ast.Call) and _name_of(node.value.func) == "TypeVar":
        return True
    return _looks_like_type_alias_name(target.id) and _is_type_expression(node.value)


def _annotation_mentions_type_alias(annotation: ast.expr) -> bool:
    if isinstance(annotation, ast.Name):
        return annotation.id == "TypeAlias"
    if isinstance(annotation, ast.Attribute):
        return annotation.attr == "TypeAlias"
    return False


def _looks_like_type_alias_name(name: str) -> bool:
    clean_name = name.lstrip("_")
    return bool(clean_name) and clean_name[0].isupper()


def _is_type_expression(node: ast.expr) -> bool:
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return _is_type_expression(node.left) and _is_type_expression(node.right)
    if isinstance(node, ast.Subscript):
        return _is_type_expression(node.value)
    if isinstance(node, ast.Tuple):
        return all(_is_type_expression(item) for item in node.elts)
    if isinstance(node, ast.Name):
        return node.id in TYPE_ALIAS_VALUE_NAMES or _looks_like_type_alias_name(node.id)
    if isinstance(node, ast.Attribute):
        return node.attr in TYPE_ALIAS_VALUE_NAMES or _looks_like_type_alias_name(
            node.attr
        )
    if isinstance(node, ast.Constant):
        return node.value is None
    return False


def _name_of(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _python_source_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(path for root in PYTHON_SOURCE_ROOTS for path in root.rglob("*.py"))
    )


def _runtime_python_source_paths() -> tuple[Path, ...]:
    paths: list[Path] = []
    for root in RUNTIME_ASSERT_ROOTS:
        if root.is_file():
            paths.append(root)
        else:
            paths.extend(root.rglob("*.py"))
    return tuple(sorted(paths))


def _assert_statements(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return tuple(
        f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Assert)
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
