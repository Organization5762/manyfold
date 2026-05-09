from __future__ import annotations

import os
import unittest
from unittest import mock

from tests import test_support


class TestSupportTests(unittest.TestCase):
    def test_subprocess_pythonpath_prepends_repo_python_root(self) -> None:
        self.assertEqual(
            test_support._pythonpath_with_repo_python_first(None),
            str(test_support.PYTHON_ROOT),
        )

    def test_subprocess_pythonpath_removes_duplicate_repo_python_root(self) -> None:
        python_root = str(test_support.PYTHON_ROOT)
        current_pythonpath = os.pathsep.join(
            ("/tmp/first", python_root, "/tmp/second", python_root)
        )

        pythonpath = test_support._pythonpath_with_repo_python_first(current_pythonpath)

        self.assertEqual(
            pythonpath,
            os.pathsep.join((python_root, "/tmp/first", "/tmp/second")),
        )

    def test_subprocess_pythonpath_deduplicates_inherited_paths(self) -> None:
        python_root = str(test_support.PYTHON_ROOT)
        current_pythonpath = os.pathsep.join(
            ("/tmp/first", "", "/tmp/second", "/tmp/first")
        )

        pythonpath = test_support._pythonpath_with_repo_python_first(current_pythonpath)

        self.assertEqual(
            pythonpath,
            os.pathsep.join((python_root, "/tmp/first", "/tmp/second")),
        )

    def test_subprocess_pythonpath_removes_relative_repo_python_root(self) -> None:
        python_root = str(test_support.PYTHON_ROOT)
        relative_python_root = os.path.relpath(
            test_support.PYTHON_ROOT,
            start=test_support.REPO_ROOT,
        )
        current_pythonpath = os.pathsep.join((relative_python_root, "/tmp/project"))

        pythonpath = test_support._pythonpath_with_repo_python_first(current_pythonpath)

        self.assertEqual(pythonpath, os.pathsep.join((python_root, "/tmp/project")))

    def test_subprocess_test_env_sets_stable_pythonpath_once(self) -> None:
        python_root = str(test_support.PYTHON_ROOT)
        current_pythonpath = os.pathsep.join(
            (python_root, "/tmp/project", "/tmp/project")
        )
        with mock.patch.dict(
            os.environ,
            {"PYTHONPATH": current_pythonpath},
            clear=False,
        ):
            env = test_support.subprocess_test_env()

        self.assertEqual(env["PYTHONPATH"], os.pathsep.join((python_root, "/tmp/project")))
        self.assertEqual(env["PYTHONPATH"].split(os.pathsep).count(python_root), 1)
        self.assertEqual(env["PYTHONPATH"].split(os.pathsep).count("/tmp/project"), 1)


if __name__ == "__main__":
    unittest.main()
