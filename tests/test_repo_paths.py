from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from manyfold import _repo_paths


class RepoPathTests(unittest.TestCase):
    def test_ensure_path_on_sys_path_canonicalizes_relative_paths(self) -> None:
        expected = str(Path("python").resolve())

        with mock.patch.object(sys, "path", ["python", "/tmp/example"]):
            _repo_paths.ensure_path_on_sys_path(Path("python"))

            self.assertEqual(sys.path, [expected, "/tmp/example"])

    def test_ensure_path_on_sys_path_keeps_canonical_path_once(self) -> None:
        expected = str(Path("python").resolve())

        with mock.patch.object(sys, "path", [expected, "/tmp/example", expected]):
            _repo_paths.ensure_path_on_sys_path(Path("python"))

            self.assertEqual(sys.path, [expected, "/tmp/example"])

    def test_load_module_from_path_resolves_relative_module_file(self) -> None:
        module_name = "manyfold_relative_repo_path_test"
        with tempfile.TemporaryDirectory() as temp_dir:
            module_path = Path(temp_dir) / "loaded_module.py"
            module_path.write_text("LOADED_FILE = __file__\n", encoding="utf-8")
            relative_path = Path(os.path.relpath(module_path))

            try:
                loaded = _repo_paths.load_module_from_path(module_name, relative_path)

                self.assertEqual(loaded.LOADED_FILE, str(module_path.resolve()))
                self.assertIs(sys.modules[module_name], loaded)
            finally:
                sys.modules.pop(module_name, None)


if __name__ == "__main__":
    unittest.main()
