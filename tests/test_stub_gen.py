from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "python" / "manyfold" / "stub_gen.py"


def load_stub_gen():
    install_reactivex_stub()
    spec = importlib.util.spec_from_file_location("manyfold_stub_gen", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def install_reactivex_stub() -> None:
    if "reactivex" in sys.modules and "reactivex.operators" in sys.modules:
        return

    class Observable:
        def __init__(self, items):
            self._items = list(items)

        def pipe(self, *transforms):
            items = self._items
            for transform in transforms:
                items = transform(items)
            return Observable(items)

        def subscribe(self, on_next):
            for item in self._items:
                on_next(item)

    def from_iterable(items):
        return Observable(items)

    def op_filter(predicate):
        return lambda items: [item for item in items if predicate(item)]

    def op_map(mapper):
        return lambda items: [mapper(item) for item in items]

    def op_distinct():
        def transform(items):
            seen = []
            for item in items:
                if item not in seen:
                    seen.append(item)
            return seen

        return transform

    rx_module = types.ModuleType("reactivex")
    rx_module.from_iterable = from_iterable
    ops_module = types.ModuleType("reactivex.operators")
    ops_module.filter = op_filter
    ops_module.map = op_map
    ops_module.distinct = op_distinct
    rx_module.operators = ops_module
    sys.modules["reactivex"] = rx_module
    sys.modules["reactivex.operators"] = ops_module


class StubGenTests(unittest.TestCase):
    def test_stub_output_matches_repository(self) -> None:
        stub_gen = load_stub_gen()
        surface = stub_gen.parse_public_surface()
        rendered = stub_gen.render_stub(surface)
        self.assertEqual(rendered, (REPO_ROOT / "python" / "manyfold" / "__init__.pyi").read_text())

    def test_checklist_output_matches_repository(self) -> None:
        stub_gen = load_stub_gen()
        sections, appendix_items = stub_gen.parse_rfc_sections()
        rendered = stub_gen.render_checklist(sections, appendix_items)
        self.assertEqual(rendered, (REPO_ROOT / "docs" / "rfc" / "implementation_checklist.md").read_text())


if __name__ == "__main__":
    unittest.main()
