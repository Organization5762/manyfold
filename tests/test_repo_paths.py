from __future__ import annotations

import sys
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


if __name__ == "__main__":
    unittest.main()
